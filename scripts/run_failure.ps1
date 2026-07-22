$ErrorActionPreference = "Continue"

if (Test-Path "docs/ERROR_rag_ran_after_failed_gate.txt") {
  Remove-Item "docs/ERROR_rag_ran_after_failed_gate.txt" -Force
}

powershell -ExecutionPolicy Bypass -File scripts/reset_topics.ps1

docker compose exec -e FORCE_QUALITY_FAILURE=1 airflow bash -lc `
  "airflow dags test university_regulations_capstone 2026-07-23 2>&1 |
   tee /opt/airflow/project/docs/airflow_forced_quality_failure.log"

$dagExit = $LASTEXITCODE

if ($dagExit -eq 0) {
  throw "The controlled quality-failure DAG unexpectedly succeeded."
}

if (Test-Path "docs/ERROR_rag_ran_after_failed_gate.txt") {
  throw "RAG ran even though the quality gate failed."
}

$lineage = Get-Content "logs/openlineage/events.jsonl" -Raw
if ($lineage -notmatch '"eventType":"FAIL"' -and $lineage -notmatch '"eventType": "FAIL"') {
  throw "No OpenLineage FAIL event was found."
}

Write-Host "Controlled failure succeeded: quality gate failed, RAG was blocked, and FAIL lineage was emitted."
exit 0
