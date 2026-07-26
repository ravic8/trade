from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator

ALLOWED_AGENT_METRICS = {
    "net_profit",
    "revenue",
    "basic_eps",
    "diluted_eps",
    "profit_before_tax",
}


class InvestigationPlan(BaseModel):
    intent: Literal["rank_growth", "compare_companies", "coverage"]
    metric: str
    comparison: Literal["yoy", "qoq"]
    limit: int = Field(default=10, ge=1, le=20)
    scope: Literal["consolidated"] = "consolidated"
    rationale: str = Field(default="", max_length=500)

    @field_validator("metric")
    @classmethod
    def allowed_metric(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in ALLOWED_AGENT_METRICS:
            raise ValueError(f"unsupported filing investigation metric: {normalized}")
        return normalized


class SynthesisClaim(BaseModel):
    text: str = Field(min_length=3, max_length=1_000)
    citation_ids: list[str] = Field(default_factory=list, max_length=8)


class InvestigationSynthesis(BaseModel):
    title: str = Field(min_length=3, max_length=160)
    summary: str = Field(min_length=3, max_length=2_000)
    claims: list[SynthesisClaim] = Field(default_factory=list, max_length=12)
    limitations: list[str] = Field(default_factory=list, max_length=12)
    model_used: bool = False
    provider: str | None = None
    model: str | None = None
    usage: dict[str, int] = Field(default_factory=dict)
