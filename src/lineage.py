from __future__ import annotations
import json
import uuid
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Callable, Iterable

from openlineage.client.event_v2 import Dataset, Job, Run, RunEvent, RunState
from openlineage.client.serde import Serde
from src.settings import LINEAGE_LOG

def _emit(stage: str, run_id: str, state: RunState, inputs: Iterable[str], outputs: Iterable[str]) -> None:
    LINEAGE_LOG.parent.mkdir(parents=True, exist_ok=True)
    event = RunEvent(
        eventTime=datetime.now(timezone.utc).isoformat(),
        eventType=state,
        run=Run(runId=run_id),
        job=Job(namespace="sdaia-capstone", name=stage),
        inputs=[Dataset(namespace="project", name=name) for name in inputs],
        outputs=[Dataset(namespace="project", name=name) for name in outputs],
        producer="university-regulations-capstone",
    )
    payload = Serde.to_json(event)
    with LINEAGE_LOG.open("a", encoding="utf-8") as handle:
        handle.write(payload + "\n")
    print("OPENLINEAGE_EVENT", payload)

def lineage_stage(stage: str, inputs: list[str], outputs: list[str]):
    def decorator(fn: Callable):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            run_id = str(uuid.uuid4())
            _emit(stage, run_id, RunState.START, inputs, outputs)
            try:
                result = fn(*args, **kwargs)
            except Exception:
                _emit(stage, run_id, RunState.FAIL, inputs, outputs)
                raise
            _emit(stage, run_id, RunState.COMPLETE, inputs, outputs)
            return result
        return wrapper
    return decorator
