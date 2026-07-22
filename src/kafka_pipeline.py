from __future__ import annotations
import json
import time
from pathlib import Path
from kafka import KafkaConsumer, KafkaProducer
from pydantic import ValidationError

from src.contracts import RegulationEvent
from src.lineage import lineage_stage
from src.settings import (
    KAFKA_BOOTSTRAP_SERVERS, RAW_TOPIC, VALID_TOPIC, DLQ_TOPIC,
    EXPECTED_EVENT_COUNT, SOURCE_FILE, LANDING_FILE, DLQ_EVIDENCE_FILE
)

def _producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=10,
    )

@lineage_stage(
    "01_kafka_produce",
    ["data/source/incoming_events.json"],
    [RAW_TOPIC],
)
def produce_events() -> int:
    events = json.loads(SOURCE_FILE.read_text(encoding="utf-8"))
    producer = _producer()
    for event in events:
        producer.send(RAW_TOPIC, event).get(timeout=30)
    producer.flush()
    producer.close()
    print(f"Produced {len(events)} records to {RAW_TOPIC}")
    return len(events)

@lineage_stage(
    "02_kafka_validate_consume",
    [RAW_TOPIC],
    [VALID_TOPIC, DLQ_TOPIC, "data/landing/valid_events.jsonl", "data/landing/dlq_events.jsonl"],
)
def consume_validate_route() -> dict:
    LANDING_FILE.parent.mkdir(parents=True, exist_ok=True)
    LANDING_FILE.write_text("", encoding="utf-8")
    DLQ_EVIDENCE_FILE.write_text("", encoding="utf-8")

    consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"capstone-validator-{int(time.time())}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=30000,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    producer = _producer()
    valid_count = 0
    dlq_count = 0
    processed = 0

    for message in consumer:
        event = message.value
        processed += 1
        try:
            validated = RegulationEvent.model_validate(event).model_dump()
            producer.send(VALID_TOPIC, validated).get(timeout=30)
            with LANDING_FILE.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(validated) + "\n")
            valid_count += 1
        except ValidationError as exc:
            dlq_record = {
                "original_event": event,
                "rejection_reason": str(exc),
                "source_topic": RAW_TOPIC,
                "source_offset": message.offset,
            }
            producer.send(DLQ_TOPIC, dlq_record).get(timeout=30)
            with DLQ_EVIDENCE_FILE.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(dlq_record) + "\n")
            dlq_count += 1

        if processed >= EXPECTED_EVENT_COUNT:
            break

    producer.flush()
    producer.close()
    consumer.close()

    if processed != EXPECTED_EVENT_COUNT:
        raise RuntimeError(f"Expected {EXPECTED_EVENT_COUNT} events but consumed {processed}")
    if dlq_count == 0:
        raise RuntimeError("Failure-path proof missing: no malformed records reached DLQ")

    result = {"processed": processed, "valid": valid_count, "dlq": dlq_count}
    print(json.dumps(result, indent=2))
    return result
