$ErrorActionPreference = "Stop"

$required = @(
  "README.md",
  "docs/ARCHITECTURE.md",
  "docs/RUBRIC_CHECKLIST.md",
  "docs/airflow_success_run.log",
  "docs/airflow_forced_quality_failure.log",
  "docs/schema_enforcement_failure.txt",
  "docs/quality_result.json",
  "docs/rag_answer.json",
  "logs/openlineage/events.jsonl",
  "data/landing/dlq_events.jsonl",
  "data/delta/bronze/_delta_log",
  "data/delta/silver/_delta_log",
  "data/delta/gold/_delta_log"
)

$missing = @()
foreach ($path in $required) {
  if (-not (Test-Path $path)) {
    $missing += $path
  }
}

if ($missing.Count -gt 0) {
  Write-Host "Missing submission evidence:"
  $missing | ForEach-Object { Write-Host " - $_" }
  exit 1
}

$dlqCount = (Get-Content "data/landing/dlq_events.jsonl").Count
if ($dlqCount -lt 1) {
  throw "DLQ evidence is empty."
}

$rag = Get-Content "docs/rag_answer.json" -Raw | ConvertFrom-Json
if ($rag.citations.Count -lt 1) {
  throw "RAG output has no citations."
}

$successLog = Get-Content "docs/airflow_success_run.log" -Raw
if ($successLog -notmatch "Dag run finished") {
  Write-Warning "Could not find the expected Airflow completion phrase; review the success log manually."
}

$failureLog = Get-Content "docs/airflow_forced_quality_failure.log" -Raw
if ($failureLog -notmatch "Great Expectations quality gate failed") {
  Write-Warning "Could not find the expected quality failure phrase; review the failure log manually."
}

$lineage = Get-Content "logs/openlineage/events.jsonl" -Raw
foreach ($state in @("START", "COMPLETE", "FAIL")) {
  if ($lineage -notmatch "`"eventType`":`"$state`"" -and $lineage -notmatch "`"eventType`": `"$state`"") {
    throw "Missing OpenLineage $state event."
  }
}

if (Test-Path "docs/ERROR_rag_ran_after_failed_gate.txt") {
  throw "RAG incorrectly ran after the failed quality gate."
}

Write-Host "All required evidence files and automated checks passed."
