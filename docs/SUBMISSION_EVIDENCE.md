# Submission evidence guide

Run the following in the VS Code PowerShell terminal:

```powershell
docker compose up --build -d
powershell -ExecutionPolicy Bypass -File scripts/run_success.ps1
powershell -ExecutionPolicy Bypass -File scripts/run_failure.ps1
powershell -ExecutionPolicy Bypass -File scripts/verify_submission.ps1
```

Capture:

1. `docker compose ps` showing Kafka and Airflow running.
2. Airflow DAG graph or CLI success log for the successful run.
3. Kafka DLQ evidence from `data/landing/dlq_events.jsonl`.
4. Delta schema rejection in `docs/schema_enforcement_failure.txt`.
5. Delta `_delta_log` directories for Bronze, Silver, and Gold.
6. Great Expectations successful result in `docs/quality_result.json`.
7. Cited RAG answer in `docs/rag_answer.json`.
8. OpenLineage START / COMPLETE events in `logs/openlineage/events.jsonl`.
9. Controlled Airflow failure log showing the quality task failed and RAG was blocked.
10. OpenLineage FAIL event from the controlled failure.
11. GitHub repository landing page and incremental commit history.
