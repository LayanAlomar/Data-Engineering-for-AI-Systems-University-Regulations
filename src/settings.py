from __future__ import annotations
import os
from pathlib import Path

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[1]))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "university-regulations-raw")
VALID_TOPIC = os.getenv("KAFKA_VALID_TOPIC", "university-regulations-valid")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "university-regulations-dlq")
EXPECTED_EVENT_COUNT = int(os.getenv("EXPECTED_EVENT_COUNT", "7"))

SOURCE_FILE = PROJECT_ROOT / "data/source/incoming_events.json"
LANDING_FILE = PROJECT_ROOT / "data/landing/valid_events.jsonl"
DLQ_EVIDENCE_FILE = PROJECT_ROOT / "data/landing/dlq_events.jsonl"
DELTA_ROOT = PROJECT_ROOT / "data/delta"
CHROMA_ROOT = PROJECT_ROOT / "data/chroma"
LINEAGE_LOG = PROJECT_ROOT / "logs/openlineage/events.jsonl"
RAG_RESULT = PROJECT_ROOT / "docs/rag_answer.json"
QUALITY_RESULT = PROJECT_ROOT / "docs/quality_result.json"
