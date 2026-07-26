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

InvestigationIntent = Literal[
    "rank_growth",
    "compare_companies",
    "coverage",
    "capabilities",
    "limitations",
]

SYSTEM_INTENTS = {"coverage", "capabilities", "limitations"}


class InvestigationPlan(BaseModel):
    intent: InvestigationIntent
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


def classify_investigation_intent(question: str) -> InvestigationIntent:
    """Deterministic semantic oracle used independently of the LLM planner."""
    normalized = " ".join(question.lower().replace("’", "'").split())
    capability_terms = (
        "capabilities",
        "capability",
        "capabilites",
        "what can you do",
        "how can you help",
        "what do you support",
        "what is supported",
    )
    limitation_terms = (
        "limitations",
        "limitation",
        "what can't you do",
        "what cant you do",
        "what can you not do",
        "unsupported",
    )
    coverage_terms = (
        "coverage",
        "which stocks",
        "what stocks",
        "which companies",
        "what companies",
        "stocks do you have data",
        "companies do you have data",
        "data do you have",
        "data available",
        "what universe",
        "which universe",
    )
    if any(term in normalized for term in capability_terms):
        return "capabilities"
    if any(term in normalized for term in limitation_terms):
        return "limitations"
    if any(term in normalized for term in coverage_terms):
        return "coverage"
    if "compare" in normalized and "rank" not in normalized:
        return "compare_companies"
    return "rank_growth"


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
