$ErrorActionPreference = "Stop"

if (Test-Path "docs/ERROR_rag_ran_after_failed_gate.txt") {
  Remove-Item "docs/ERROR_rag_ran_after_failed_gate.txt" -Force
}

powershell -ExecutionPolicy Bypass -File scripts/reset_topics.ps1

docker compose exec airflow bash -lc `
  "rm -f /opt/airflow/project/logs/openlineage/events.jsonl &&
   airflow dags test university_regulations_capstone 2026-07-22 2>&1 |
   tee /opt/airflow/project/docs/airflow_success_run.log"

if ($LASTEXITCODE -ne 0) {
  throw "The successful DAG run failed. Check docs/airflow_success_run.log"
}

Write-Host "Successful DAG run completed."
