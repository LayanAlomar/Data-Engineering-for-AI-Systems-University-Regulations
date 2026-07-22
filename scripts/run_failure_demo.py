"""Intentionally makes the quality gate fail to prove downstream blocking.
Run only after the successful DAG run and after backing up data if needed.
"""
from pathlib import Path
import json
from src.settings import LANDING_FILE

rows = [json.loads(line) for line in LANDING_FILE.read_text().splitlines() if line.strip()]
rows.append({
    "regulation_id": rows[0]["regulation_id"],
    "title": "Duplicate",
    "category": "Testing",
    "text": "This duplicate business key intentionally fails the uniqueness expectation.",
    "effective_year": 2026,
    "status": "active"
})
LANDING_FILE.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
print("Failure demo prepared. Re-run the DAG; the quality task should fail and RAG should remain not scheduled.")
