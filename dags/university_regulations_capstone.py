from __future__ import annotations
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.kafka_pipeline import produce_events, consume_validate_route
from src.delta_pipeline import (
    build_bronze_merge_and_schema_proof,
    build_silver,
    build_gold,
)
from src.quality import run_quality_gate
from src.rag_pipeline import build_and_query_rag

with DAG(
    dag_id="university_regulations_capstone",
    description="SDAIA Modern Data Engineering for AI Systems capstone",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["SDAIA", "Kafka", "Delta", "RAG", "OpenLineage"],
) as dag:
    kafka_produce = PythonOperator(
        task_id="kafka_produce",
        python_callable=produce_events,
    )
    kafka_validate = PythonOperator(
        task_id="kafka_consume_validate_dlq",
        python_callable=consume_validate_route,
    )
    bronze_merge = PythonOperator(
        task_id="delta_bronze_merge_schema_enforcement",
        python_callable=build_bronze_merge_and_schema_proof,
    )
    silver = PythonOperator(
        task_id="delta_silver",
        python_callable=build_silver,
    )
    quality_gate = PythonOperator(
        task_id="great_expectations_quality_gate",
        python_callable=run_quality_gate,
    )
    gold = PythonOperator(
        task_id="delta_gold_aggregate",
        python_callable=build_gold,
    )
    rag = PythonOperator(
        task_id="hybrid_rag_with_reranking",
        python_callable=build_and_query_rag,
    )

    kafka_produce >> kafka_validate >> bronze_merge >> silver >> quality_gate
    quality_gate >> [gold, rag]
