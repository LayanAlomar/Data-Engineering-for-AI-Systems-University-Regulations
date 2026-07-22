from __future__ import annotations
import json
import shutil

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F, types as T

from src.lineage import lineage_stage
from src.settings import LANDING_FILE, DELTA_ROOT, PROJECT_ROOT

BRONZE = DELTA_ROOT / "bronze"
SILVER = DELTA_ROOT / "silver"
GOLD = DELTA_ROOT / "gold"

SCHEMA = T.StructType([
    T.StructField("regulation_id", T.StringType(), False),
    T.StructField("title", T.StringType(), False),
    T.StructField("category", T.StringType(), False),
    T.StructField("text", T.StringType(), False),
    T.StructField("effective_year", T.IntegerType(), False),
    T.StructField("status", T.StringType(), False),
])

def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("UniversityRegulationsCapstone")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

@lineage_stage(
    "03_delta_bronze_merge_schema",
    ["data/landing/valid_events.jsonl"],
    ["data/delta/bronze", "docs/schema_enforcement_failure.txt"],
)
def build_bronze_merge_and_schema_proof() -> dict:
    spark = get_spark()
    if DELTA_ROOT.exists():
        shutil.rmtree(DELTA_ROOT)
    DELTA_ROOT.mkdir(parents=True, exist_ok=True)

    rows = [
        json.loads(line)
        for line in LANDING_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    incoming = spark.createDataFrame(rows, schema=SCHEMA)

    (
        incoming.withColumn("ingested_at", F.current_timestamp())
        .write.format("delta").mode("overwrite").save(str(BRONZE))
    )

    # Real MERGE keyed on the business key regulation_id.
    updates = spark.createDataFrame([
        (
            "REG002",
            "Academic Warning — Updated",
            "Academic Standing",
            "An academic warning is issued according to the university GPA and academic-standing rules.",
            2026,
            "active",
        ),
        (
            "REG006",
            "Transfer Credit",
            "Registration",
            "Transfer credit is evaluated by the authorized academic unit according to equivalency requirements.",
            2026,
            "active",
        ),
    ], schema=SCHEMA)

    (
        DeltaTable.forPath(spark, str(BRONZE)).alias("target")
        .merge(updates.alias("source"), "target.regulation_id = source.regulation_id")
        .whenMatchedUpdate(set={
            "title": "source.title",
            "category": "source.category",
            "text": "source.text",
            "effective_year": "source.effective_year",
            "status": "source.status",
        })
        .whenNotMatchedInsert(values={
            "regulation_id": "source.regulation_id",
            "title": "source.title",
            "category": "source.category",
            "text": "source.text",
            "effective_year": "source.effective_year",
            "status": "source.status",
            "ingested_at": "current_timestamp()",
        })
        .execute()
    )

    # Required failure-path proof: malformed schema must be refused.
    bad_df = spark.createDataFrame(
        [(
            "REG999",
            "Bad Schema",
            "Testing",
            "This row intentionally uses a string instead of an integer year.",
            "not-an-integer",
            "active",
        )],
        ["regulation_id", "title", "category", "text", "effective_year", "status"],
    )

    schema_rejected = False
    try:
        bad_df.write.format("delta").mode("append").save(str(BRONZE))
    except Exception as exc:
        schema_rejected = True
        evidence = PROJECT_ROOT / "docs/schema_enforcement_failure.txt"
        evidence.write_text(str(exc), encoding="utf-8")
        print("EXPECTED_SCHEMA_ENFORCEMENT_FAILURE")
        print(type(exc).__name__)
        print(str(exc)[:800])

    if not schema_rejected:
        raise RuntimeError("Schema enforcement proof failed: malformed write was accepted")

    result = {
        "bronze_rows_after_merge": spark.read.format("delta").load(str(BRONZE)).count(),
        "schema_rejected": schema_rejected,
    }
    print(json.dumps(result, indent=2))
    spark.stop()
    return result

@lineage_stage(
    "04_delta_silver",
    ["data/delta/bronze"],
    ["data/delta/silver"],
)
def build_silver() -> dict:
    spark = get_spark()
    silver = (
        spark.read.format("delta").load(str(BRONZE))
        .dropDuplicates(["regulation_id"])
        .filter(F.col("status") == "active")
        .withColumn("title_clean", F.trim("title"))
        .withColumn("text_length", F.length("text"))
    )
    silver.write.format("delta").mode("overwrite").save(str(SILVER))
    result = {"silver_rows": silver.count()}
    print(json.dumps(result, indent=2))
    spark.stop()
    return result

@lineage_stage(
    "06_delta_gold",
    ["data/delta/silver"],
    ["data/delta/gold"],
)
def build_gold() -> dict:
    spark = get_spark()
    silver = spark.read.format("delta").load(str(SILVER))
    gold = (
        silver.groupBy("category")
        .agg(
            F.count("*").alias("regulation_count"),
            F.avg("text_length").alias("average_text_length"),
        )
    )
    gold.write.format("delta").mode("overwrite").save(str(GOLD))
    result = {"gold_categories": gold.count()}
    print(json.dumps(result, indent=2))
    spark.stop()
    return result
