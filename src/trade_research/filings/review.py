from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from trade_research.filings.models import (
    ConsolidationScope,
    IntelligenceObject,
    PeriodType,
    ReviewDecision,
    ReviewDecisionRequest,
    ReviewItemAction,
    ReviewRequest,
)
from trade_research.filings.validators import FilingFactValidator, ValidationResult

CANDIDATE_EDIT_FIELDS = {
    "canonical_metric",
    "reported_label",
    "value_decimal",
    "currency",
    "unit_scale",
    "period_start",
    "period_end",
    "period_type",
    "consolidation_scope",
}

OBJECT_EDIT_FIELDS = {
    "canonical_name",
    "reported_label",
    "value_decimal",
    "value_text",
    "currency",
    "unit",
    "period_start",
    "period_end",
}


def validate_review_decision(
    *,
    review: ReviewRequest,
    request: ReviewDecisionRequest,
    candidates: list[dict[str, Any]],
    objects: list[dict[str, Any]],
    validator: FilingFactValidator,
) -> ValidationResult | None:
    _validate_review_inventory(review, candidates, objects)
    if request.decision != ReviewDecision.EDIT:
        return None

    expected_candidates = {str(item["candidate_id"]) for item in candidates}
    expected_objects = {str(item["object_id"]) for item in objects}
    supplied_candidates = set(request.candidate_decisions)
    supplied_objects = set(request.object_decisions)
    if supplied_candidates != expected_candidates:
        raise ValueError(
            _coverage_message(
                "candidate",
                expected=expected_candidates,
                supplied=supplied_candidates,
            )
        )
    if supplied_objects != expected_objects:
        raise ValueError(
            _coverage_message(
                "intelligence object",
                expected=expected_objects,
                supplied=supplied_objects,
            )
        )

    reviewed = materialize_candidate_decisions(
        candidates,
        {
            item_id: item.model_dump(mode="json")
            for item_id, item in request.candidate_decisions.items()
        },
    )
    for item in materialize_object_decisions(
        objects,
        {
            item_id: item.model_dump(mode="json")
            for item_id, item in request.object_decisions.items()
        },
    ):
        IntelligenceObject.model_validate(item)

    result = validator.validate(review.run_id, reviewed)
    if result.blocking:
        codes = sorted(
            {
                defect.rule_code
                for defect in result.defects
                if defect.severity.value in {"blocking", "error"}
            }
        )
        raise ValueError(
            "reviewed candidates still fail blocking validation: "
            + ", ".join(codes)
        )
    return result


def materialize_candidate_decisions(
    candidates: list[dict[str, Any]],
    decisions: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    reviewed: list[dict[str, Any]] = []
    for candidate in candidates:
        candidate_id = str(candidate["candidate_id"])
        decision = decisions.get(candidate_id)
        if decision and decision["action"] == ReviewItemAction.REJECT.value:
            continue
        row = dict(candidate)
        if decision and decision["action"] == ReviewItemAction.EDIT.value:
            edits = dict(decision.get("edits") or {})
            _validate_edit_fields(edits, CANDIDATE_EDIT_FIELDS, "candidate")
            row.update(_normalize_candidate_edits(edits))
        _validate_candidate_shape(row)
        reviewed.append(row)
    return reviewed


def materialize_object_decisions(
    objects: list[dict[str, Any]],
    decisions: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    reviewed: list[dict[str, Any]] = []
    for item in objects:
        object_id = str(item["object_id"])
        decision = decisions.get(object_id)
        if decision and decision["action"] == ReviewItemAction.REJECT.value:
            continue
        row = dict(item)
        if decision and decision["action"] == ReviewItemAction.EDIT.value:
            edits = dict(decision.get("edits") or {})
            _validate_edit_fields(edits, OBJECT_EDIT_FIELDS, "intelligence object")
            row.update(_normalize_object_edits(edits))
        reviewed.append(row)
    return reviewed


def serialized_item_decisions(
    decisions: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    return {
        item_id: (
            item.model_dump(mode="json")
            if hasattr(item, "model_dump")
            else dict(item)
        )
        for item_id, item in decisions.items()
    }


def _validate_review_inventory(
    review: ReviewRequest,
    candidates: list[dict[str, Any]],
    objects: list[dict[str, Any]],
) -> None:
    payload_candidates = {
        str(item["candidate_id"])
        for item in review.payload.get("candidate_facts", [])
    }
    payload_objects = {
        str(item["object_id"])
        for item in review.payload.get("intelligence_objects", [])
    }
    current_candidates = {str(item["candidate_id"]) for item in candidates}
    current_objects = {str(item["object_id"]) for item in objects}
    if payload_candidates != current_candidates or payload_objects != current_objects:
        raise ValueError("review inventory changed after the review packet was created")


def _validate_candidate_shape(candidate: Mapping[str, Any]) -> None:
    try:
        value = Decimal(str(candidate["value_decimal"]))
        unit_scale = Decimal(str(candidate["unit_scale"]))
    except (InvalidOperation, KeyError) as exc:
        raise ValueError("candidate edits must contain valid decimal values") from exc
    if not value.is_finite() or not unit_scale.is_finite():
        raise ValueError("candidate edits must contain finite decimal values")
    period_end = candidate.get("period_end")
    period_start = candidate.get("period_start")
    if not isinstance(period_end, date):
        raise ValueError("candidate period_end must be a valid date")
    if period_start is not None and not isinstance(period_start, date):
        raise ValueError("candidate period_start must be a valid date")
    if period_start and period_start > period_end:
        raise ValueError("candidate period_start cannot occur after period_end")
    PeriodType(str(candidate["period_type"]))
    ConsolidationScope(str(candidate["consolidation_scope"]))


def _normalize_candidate_edits(edits: Mapping[str, Any]) -> dict[str, Any]:
    output = dict(edits)
    for key in ("period_start", "period_end"):
        if isinstance(output.get(key), str):
            output[key] = date.fromisoformat(str(output[key]))
    if "period_type" in output:
        output["period_type"] = PeriodType(str(output["period_type"])).value
    if "consolidation_scope" in output:
        output["consolidation_scope"] = ConsolidationScope(
            str(output["consolidation_scope"])
        ).value
    return output


def _normalize_object_edits(edits: Mapping[str, Any]) -> dict[str, Any]:
    output = dict(edits)
    for key in ("period_start", "period_end"):
        if isinstance(output.get(key), str):
            output[key] = date.fromisoformat(str(output[key]))
    return output


def _validate_edit_fields(
    edits: Mapping[str, Any],
    allowed: set[str],
    label: str,
) -> None:
    unsupported = sorted(set(edits) - allowed)
    if unsupported:
        raise ValueError(
            f"unsupported {label} edit fields: {', '.join(unsupported)}"
        )


def _coverage_message(
    label: str,
    *,
    expected: set[str],
    supplied: set[str],
) -> str:
    missing = sorted(expected - supplied)
    unknown = sorted(supplied - expected)
    parts = [f"{label} decisions must cover the complete review packet"]
    if missing:
        parts.append(f"missing={','.join(missing)}")
    if unknown:
        parts.append(f"unknown={','.join(unknown)}")
    return "; ".join(parts)
