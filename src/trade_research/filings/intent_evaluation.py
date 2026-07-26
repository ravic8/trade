from __future__ import annotations

import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from trade_research.filings.agent_models import InvestigationIntent

DEFAULT_INTENT_DATASET_PATH = Path("evaluations/filings/nifty50_intent_routing_v1.json")


class IntentEvaluationCase(BaseModel):
    case_id: str = Field(min_length=3)
    utterance: str = Field(min_length=3)
    expected_intent: InvestigationIntent


class IntentEvaluationDataset(BaseModel):
    schema_version: Literal[1]
    dataset_id: str = Field(min_length=3)
    cases: list[IntentEvaluationCase] = Field(min_length=1)

    @field_validator("cases")
    @classmethod
    def unique_utterances(
        cls,
        value: list[IntentEvaluationCase],
    ) -> list[IntentEvaluationCase]:
        normalized = [_normalize(item.utterance) for item in value]
        if len(normalized) != len(set(normalized)):
            raise ValueError("intent evaluation utterances must be unique")
        return value


class IntentEvaluationExpectation(BaseModel):
    intent: InvestigationIntent | None
    evaluated: bool
    confidence: Literal["exact", "fuzzy", "none"]
    case_id: str | None = None
    similarity: float = Field(default=0, ge=0, le=1)
    dataset_id: str


def load_intent_evaluation_dataset(
    path: Path = DEFAULT_INTENT_DATASET_PATH,
) -> IntentEvaluationDataset:
    resolved = path.expanduser()
    if not resolved.is_absolute():
        resolved = Path.cwd() / resolved
    return IntentEvaluationDataset.model_validate_json(resolved.read_text())


def evaluate_question_intent(
    question: str,
    *,
    path: Path = DEFAULT_INTENT_DATASET_PATH,
) -> IntentEvaluationExpectation:
    """Resolve a labeled expectation independently of runtime routing policy."""
    dataset = load_intent_evaluation_dataset(path)
    normalized = _normalize(question)
    exact = {_normalize(case.utterance): case for case in dataset.cases}.get(normalized)
    if exact:
        return IntentEvaluationExpectation(
            intent=exact.expected_intent,
            evaluated=True,
            confidence="exact",
            case_id=exact.case_id,
            similarity=1,
            dataset_id=dataset.dataset_id,
        )

    scored = sorted(
        ((_similarity(normalized, _normalize(case.utterance)), case) for case in dataset.cases),
        key=lambda item: item[0],
        reverse=True,
    )
    best_score, best = scored[0]
    runner_up = scored[1][0] if len(scored) > 1 else 0
    if best_score >= 0.90 and best_score - runner_up >= 0.05:
        return IntentEvaluationExpectation(
            intent=best.expected_intent,
            evaluated=True,
            confidence="fuzzy",
            case_id=best.case_id,
            similarity=round(best_score, 4),
            dataset_id=dataset.dataset_id,
        )
    return IntentEvaluationExpectation(
        intent=None,
        evaluated=False,
        confidence="none",
        similarity=round(best_score, 4),
        dataset_id=dataset.dataset_id,
    )


def _normalize(value: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value.lower()).split())


def _similarity(left: str, right: str) -> float:
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    union = left_tokens | right_tokens
    jaccard = len(left_tokens & right_tokens) / len(union) if union else 0
    sequence = SequenceMatcher(None, left, right).ratio()
    return max(jaccard, sequence)
