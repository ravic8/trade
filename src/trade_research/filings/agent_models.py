from __future__ import annotations

import re
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


class IntentPolicyDecision(BaseModel):
    """High-confidence deterministic routing evidence, not an answer oracle."""

    intent: InvestigationIntent | None
    confidence: Literal["high", "low"]
    rule_id: str
    enforce: bool


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


def decide_investigation_intent(question: str) -> IntentPolicyDecision:
    """Return a conservative routing decision for unambiguous objectives.

    The policy is deliberately allowed to return no intent. Low-confidence text
    must be interpreted by the structured planner instead of being coerced into
    the financial ranking route.
    """
    normalized = " ".join(question.lower().replace("’", "'").split())
    capability_terms = (
        "capabilities",
        "capability",
        "capabilites",
        "what can you do",
        "how can you help",
        "what do you support",
        "what is supported",
        "what analysis do you support",
        "what can this filing agent do",
        "what can the filing agent do",
    )
    limitation_terms = (
        "limitations",
        "limitation",
        "what can't you do",
        "what cant you do",
        "what can you not do",
        "unsupported",
    )
    if any(term in normalized for term in capability_terms):
        return IntentPolicyDecision(
            intent="capabilities",
            confidence="high",
            rule_id="system.capabilities",
            enforce=True,
        )
    if any(term in normalized for term in limitation_terms):
        return IntentPolicyDecision(
            intent="limitations",
            confidence="high",
            rule_id="system.limitations",
            enforce=True,
        )

    # Explicit analytical verbs take precedence over a secondary request to
    # explain coverage (for example, "rank ... and explain coverage").
    financial_action = re.search(
        r"\b(rank(?:ing|ed)?|leaders?|growth|momentum|strongest|top|"
        r"compare|comparison|year[- ]over[- ]year|quarter[- ]over[- ]quarter|"
        r"yoy|qoq)\b",
        normalized,
    )
    if financial_action:
        compare_only = bool(re.search(r"\bcompar(?:e|ison)\b", normalized)) and not bool(
            re.search(r"\b(rank(?:ing|ed)?|leaders?|growth|momentum|top)\b", normalized)
        )
        return IntentPolicyDecision(
            intent="compare_companies" if compare_only else "rank_growth",
            confidence="high",
            rule_id=("financial.compare_companies" if compare_only else "financial.rank_growth"),
            enforce=True,
        )

    has_subject = bool(
        re.search(r"\b(stocks?|companies|members?|constituents?|universe)\b", normalized)
    )
    has_data_object = bool(
        re.search(r"\b(data|facts?|filings?|filing data|financial data)\b", normalized)
    )
    has_availability = bool(
        re.search(
            r"\b(have|has|available|availability|covered|coverage|represented|support)\b",
            normalized,
        )
    )
    coverage_question = (
        "coverage" in normalized
        or "approved filing data" in normalized
        or "data do you have" in normalized
        or "data is available" in normalized
        or "data are available" in normalized
        or (has_subject and has_availability and (has_data_object or "covered" in normalized))
    )
    if coverage_question:
        return IntentPolicyDecision(
            intent="coverage",
            confidence="high",
            rule_id="system.data_availability",
            enforce=True,
        )

    return IntentPolicyDecision(
        intent=None,
        confidence="low",
        rule_id="policy.no_high_confidence_match",
        enforce=False,
    )


def classify_investigation_intent(question: str) -> InvestigationIntent:
    """Backward-compatible deterministic fallback classification.

    New runtime code should use :func:`decide_investigation_intent` so an
    ambiguous objective can remain unclassified instead of silently becoming a
    ranking request.
    """
    return decide_investigation_intent(question).intent or "rank_growth"


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
