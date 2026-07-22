# Data Engineering for AI Systems — University Regulations

**SDAIA Academy Capstone Project**

A production-style data engineering and AI system for ingesting, validating, transforming, governing, and retrieving university regulations.

## Project Information

| Role | Name | Email |
|---|---|---|
| Supervisor | Mohammed Albeladi | — |
| Team Member | Layan Alomar | layanomaralomar@gmail.com |
| Team Member | Wajd Alotaibi | wajd.ashag@hotmail.com |
| Team Member | Alathoob Alosaimi | AlathoobS@outlook.com |
| Team Member | Reema Qublan | reem.qu@icloud.com |

## Project Overview

University regulations are often distributed across documents and difficult to search. This project delivers an end-to-end data engineering pipeline that ingests regulation events through Kafka, validates them with Pydantic, stores them in Delta Lakehouse layers, applies data-quality gates and lineage tracking, and powers a grounded hybrid RAG assistant with citations.

## Exact rubric implementation

| Rubric requirement | Implementation |
|---|---|
| Kafka ingestion | A real Apache Kafka KRaft broker in Docker plus real `kafka-python` producer and consumer |
| Schema validation | Pydantic contract at the ingestion boundary |
| Quarantine / DLQ | A real Kafka dead-letter topic containing the malformed record and rejection reason |
| Delta Lakehouse | Real PySpark + Delta Bronze, Silver, and Gold tables |
| MERGE | Real Delta `MERGE` keyed on business key `regulation_id` |
| Schema enforcement | A deliberately malformed append is rejected and saved as evidence |
| Gold layer | A genuine category-level aggregate, not a copy of Silver |
| RAG | Chunking, embeddings, ChromaDB, BM25, RRF fusion, cross-encoder reranking |
| Grounding | Final answer includes retrieved regulation citations |
| Airflow | A real Apache Airflow DAG executes every pipeline stage |
| Quality gate | Great Expectations raises a task failure; downstream RAG is blocked |
| OpenLineage | START / COMPLETE / FAIL events emitted for every executed stage |
| Failure paths | Malformed Kafka messages, rejected Delta schema, and failed Airflow quality gate are all demonstrated |

## Prerequisites

Install:

1. **Visual Studio Code**
2. **Docker Desktop**
3. **Git**

Open Docker Desktop and wait until Docker Engine is running before starting.

Recommended VS Code extensions are listed in `.vscode/extensions.json`.

## One-time setup in VS Code

1. Extract the ZIP.
2. Open the extracted folder in VS Code:
   `File → Open Folder`.
3. Open the VS Code terminal:
   `Terminal → New Terminal`.
4. Start the services:

   ```powershell
   docker compose up --build -d
   ```

   The first build may take 10–25 minutes.

5. Confirm both containers are running:

   ```powershell
   docker compose ps
   ```

6. Open Airflow:

   `http://localhost:8080`

   Credentials:

   - Username: `admin`
   - Password: `admin`

## Run the complete successful pipeline

From the VS Code PowerShell terminal:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_success.ps1
```

This command:

1. Resets the three Kafka topics.
2. Runs the real Airflow DAG.
3. Produces source records to Kafka.
4. Consumes them and routes malformed records to the DLQ.
5. Builds Delta Bronze/Silver/Gold.
6. Executes Delta MERGE and schema-enforcement proof.
7. Runs Great Expectations successfully.
8. Builds and queries the complete hybrid RAG pipeline.
9. Emits OpenLineage START and COMPLETE events.
10. Saves the Airflow log to `docs/airflow_success_run.log`.

## Prove that the quality gate blocks downstream RAG

Run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_failure.ps1
```

The script intentionally duplicates a business key inside the Great Expectations task. The quality task fails, Airflow blocks the downstream RAG task, and OpenLineage emits a FAIL event.

The evidence is saved to:

```text
docs/airflow_forced_quality_failure.log
```

## Validate all generated submission evidence

After both runs:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify_submission.ps1
```

## Expected evidence files

```text
data/landing/dlq_events.jsonl
data/delta/bronze/_delta_log/
data/delta/silver/_delta_log/
data/delta/gold/_delta_log/
docs/schema_enforcement_failure.txt
docs/quality_result.json
docs/rag_answer.json
docs/airflow_success_run.log
docs/airflow_forced_quality_failure.log
logs/openlineage/events.jsonl
```

## Architecture

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

Airflow dependency chain:

```text
kafka_produce
→ kafka_consume_validate_dlq
→ delta_bronze_merge_schema_enforcement
→ delta_silver
→ great_expectations_quality_gate
→ delta_gold_aggregate
→ hybrid_rag_with_reranking
```

Airflow's default `all_success` trigger rule means neither Gold nor RAG can run after the quality task fails.

## Tests

```powershell
docker compose exec airflow pytest -q /opt/airflow/project/tests
```

## Stop the environment

```powershell
docker compose down
```

To remove all local containers and generated service state:

```powershell
docker compose down -v
```

## GitHub submission

The rubric requires the trainee to publish the project to her own GitHub account and maintain incremental commit history. Suggested commits:

```text
chore: initialize repository structure and documentation
feat: add real Kafka producer consumer and Pydantic DLQ
feat: implement Delta Bronze Silver Gold and MERGE
feat: add hybrid RAG with BM25 RRF and reranking
feat: add Great Expectations quality gate
feat: add OpenLineage stage events
feat: orchestrate full pipeline with Airflow
docs: add architecture and submission evidence guide
```

Do not commit secrets, `.env`, generated Delta tables, Chroma indexes, or runtime logs unless the trainer explicitly requests execution evidence in the repository.
