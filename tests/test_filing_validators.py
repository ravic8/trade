from __future__ import annotations

from datetime import date
from typing import Any

from trade_research.filings.models import ValidationStatus
from trade_research.filings.validators import FilingFactValidator


def _candidate(
    candidate_id: str,
    metric: str,
    value: str,
    *,
    period_start: date = date(2026, 1, 1),
    period_end: date = date(2026, 3, 31),
    period_type: str = "quarter",
) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "canonical_metric": metric,
        "value_decimal": value,
        "currency": "INR",
        "period_start": period_start,
        "period_end": period_end,
        "period_type": period_type,
        "consolidation_scope": "consolidated",
        "evidence_ids": [f"evidence-{candidate_id}"],
        "confidence": 0.995,
    }


def _profit_candidates(
    *,
    tax: str = "20",
    continuing_profit: str | None = "80",
    total_profit: str | None = "1000",
) -> list[dict[str, Any]]:
    candidates = [
        _candidate("pbt", "profit_before_tax", "100"),
        _candidate("tax", "tax_expense", tax),
    ]
    if continuing_profit is not None:
        candidates.append(_candidate("continuing", "net_profit_continuing", continuing_profit))
    if total_profit is not None:
        candidates.append(_candidate("total", "net_profit", total_profit))
    return candidates


def test_profit_after_tax_prefers_continuing_operations_profit() -> None:
    result = FilingFactValidator().validate(
        "run-1",
        _profit_candidates(continuing_profit="80", total_profit="1000"),
    )

    assert result.defects == []
    assert result.requires_review is False
    assert set(result.candidate_statuses.values()) == {ValidationStatus.PASSED}


def test_profit_after_tax_accepts_signed_tax_presentation() -> None:
    result = FilingFactValidator().validate(
        "run-1",
        _profit_candidates(tax="-20", continuing_profit="80"),
    )

    assert result.defects == []
    assert result.requires_review is False


def test_profit_after_tax_falls_back_to_total_profit() -> None:
    result = FilingFactValidator().validate(
        "run-1",
        _profit_candidates(continuing_profit=None, total_profit="80"),
    )

    assert result.defects == []
    assert result.requires_review is False


def test_unreconciled_utility_continuing_profit_remains_reviewable() -> None:
    result = FilingFactValidator().validate(
        "run-1",
        [
            _candidate("pbt", "profit_before_tax", "82670300000"),
            _candidate("tax", "tax_expense", "16973500000"),
            _candidate(
                "continuing",
                "net_profit_continuing",
                "72997700000",
            ),
            _candidate("total", "net_profit", "71966600000"),
        ],
    )

    assert result.requires_review is True
    assert len(result.defects) == 1
    defect = result.defects[0]
    assert defect.rule_code == "accounting.profit_after_tax"
    assert defect.candidate_id == "continuing"
    assert defect.context == {
        "profit_before_tax": "82670300000",
        "tax_expense": "16973500000",
        "target_metric": "net_profit_continuing",
        "profit_after_tax": "72997700000",
        "net_profit": "72997700000",
        "expected": "65696800000",
        "alternate_expected": "99643800000",
    }
    assert result.candidate_statuses["continuing"] == ValidationStatus.REVIEW


def test_accounting_groups_distinct_period_starts_separately() -> None:
    candidates = [
        _candidate(
            "quarter-net",
            "net_profit",
            "80",
            period_start=date(2026, 1, 1),
        ),
        _candidate(
            "ytd-net",
            "net_profit",
            "240",
            period_start=date(2025, 4, 1),
        ),
    ]

    result = FilingFactValidator().validate("run-1", candidates)

    assert result.defects == []
    assert result.requires_review is False
