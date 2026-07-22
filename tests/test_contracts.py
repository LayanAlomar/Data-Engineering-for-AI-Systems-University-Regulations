import pytest
from pydantic import ValidationError
from src.contracts import RegulationEvent

def test_valid_contract():
    event = RegulationEvent(
        regulation_id="REG001", title="Valid title", category="Registration",
        text="This is a sufficiently long regulation document body.",
        effective_year=2026, status="active"
    )
    assert event.regulation_id == "REG001"

def test_invalid_contract_is_rejected():
    with pytest.raises(ValidationError):
        RegulationEvent(
            regulation_id="", title="Bad", category="Other",
            text="This is a sufficiently long invalid event body.",
            effective_year=1990, status="active"
        )
