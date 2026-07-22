from pydantic import BaseModel, Field, field_validator

class RegulationEvent(BaseModel):
    regulation_id: str = Field(min_length=1)
    title: str = Field(min_length=3)
    category: str = Field(min_length=3)
    text: str = Field(min_length=20)
    effective_year: int
    status: str

    @field_validator("effective_year")
    @classmethod
    def validate_year(cls, value: int) -> int:
        if not 2020 <= value <= 2035:
            raise ValueError("effective_year must be between 2020 and 2035")
        return value

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str) -> str:
        if value not in {"active", "inactive"}:
            raise ValueError("status must be active or inactive")
        return value
