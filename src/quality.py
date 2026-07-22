from __future__ import annotations
import json
import os
import great_expectations as gx
import great_expectations.expectations as gxe

from src.delta_pipeline import get_spark, SILVER
from src.lineage import lineage_stage
from src.settings import QUALITY_RESULT

@lineage_stage(
    "05_quality_gate",
    ["data/delta/silver"],
    ["docs/quality_result.json"],
)
def run_quality_gate() -> dict:
    spark = get_spark()
    frame = spark.read.format("delta").load(str(SILVER)).toPandas()
    spark.stop()

    # Controlled failure-path proof for the Airflow gate.
    if os.getenv("FORCE_QUALITY_FAILURE") == "1":
        frame = frame.copy()
        frame.loc[len(frame)] = frame.iloc[0]

    context = gx.get_context()
    data_source = context.data_sources.add_pandas("regulations_source")
    asset = data_source.add_dataframe_asset(name="regulations_asset")
    batch_definition = asset.add_batch_definition_whole_dataframe("whole_dataframe")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": frame})

    suite = context.suites.add(gx.ExpectationSuite(name="regulations_suite"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="regulation_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="regulation_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="status", value_set=["active"]))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="effective_year", min_value=2020, max_value=2035))
    suite.add_expectation(gxe.ExpectColumnValueLengthsToBeBetween(column="text", min_value=20))

    validation = batch.validate(suite)
    result = {
        "success": bool(validation.success),
        "statistics": dict(validation.statistics),
    }
    QUALITY_RESULT.parent.mkdir(parents=True, exist_ok=True)
    QUALITY_RESULT.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))

    # This is the actual gate: Airflow marks the task failed and downstream RAG will not run.
    if not validation.success:
        raise RuntimeError("Great Expectations quality gate failed; downstream stages are blocked")
    return result
