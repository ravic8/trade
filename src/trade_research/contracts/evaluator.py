from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, date, datetime
from typing import Any

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

from trade_research.contracts.models import ColumnContract, DataContract
from trade_research.validation.results import ValidationReport, ValidationResult


def _utc_now() -> datetime:
    return datetime.now(UTC)


class ContractEvaluationContext(BaseModel):
    """External time/calendar facts needed for deterministic freshness checks."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    evaluated_at: datetime = Field(default_factory=_utc_now)
    eligible_session_dates: tuple[date, ...] = ()

    @field_validator("evaluated_at")
    @classmethod
    def evaluated_at_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("evaluated_at must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def session_dates_must_be_sorted_and_unique(self) -> ContractEvaluationContext:
        if tuple(sorted(set(self.eligible_session_dates))) != self.eligible_session_dates:
            raise ValueError("eligible_session_dates must be sorted and unique")
        return self


def evaluate_frame_contract(
    frame: pd.DataFrame,
    contract: DataContract,
    *,
    run_id: str,
    scope: dict[str, JsonValue] | None = None,
    context: ContractEvaluationContext | None = None,
) -> ValidationReport:
    """Evaluate generic, executable portions of a registered frame contract."""

    created_at = context.evaluated_at if context is not None else _utc_now()
    evaluation_scope: dict[str, JsonValue] = {
        "data_contract_id": contract.contract_id,
        "dataset_version": contract.dataset_version,
        **(scope or {}),
    }
    columns_by_name = {column.name: column for column in contract.columns}
    registered_columns = set(columns_by_name)
    actual_columns = set(str(column) for column in frame.columns)
    missing_columns = sorted(registered_columns - actual_columns)
    unregistered_columns = sorted(actual_columns - registered_columns)
    missing_column_evidence = _json_string_list(missing_columns)
    required_column_evidence = _json_string_list(sorted(registered_columns))
    unregistered_column_evidence = _json_string_list(unregistered_columns)

    results = [
        _result(
            contract=contract,
            run_id=run_id,
            scope=evaluation_scope,
            suffix="schema.required_columns",
            severity="error",
            status="failed" if missing_columns else "passed",
            observed_value={
                "column_count": len(actual_columns),
                "missing_columns": missing_column_evidence,
            },
            expected_value={"required_columns": required_column_evidence},
            message=(
                f"Missing required columns: {missing_columns}."
                if missing_columns
                else "All required contract columns are present."
            ),
            created_at=created_at,
        ),
        _result(
            contract=contract,
            run_id=run_id,
            scope=evaluation_scope,
            suffix="schema.unregistered_columns",
            severity="warning",
            status="warning" if unregistered_columns else "passed",
            observed_value={"unregistered_columns": unregistered_column_evidence},
            expected_value={"unregistered_columns": []},
            message=(
                f"Frame contains unregistered columns: {unregistered_columns}."
                if unregistered_columns
                else "Frame contains no unregistered columns."
            ),
            created_at=created_at,
        ),
    ]
    results.extend(
        [
            _type_result(
                frame,
                contract,
                columns_by_name,
                run_id,
                evaluation_scope,
                created_at,
            ),
            _nullability_result(
                frame,
                contract,
                columns_by_name,
                run_id,
                evaluation_scope,
                created_at,
            ),
            _primary_key_result(
                frame,
                contract,
                run_id,
                evaluation_scope,
                created_at,
            ),
            _allowed_values_result(
                frame,
                contract,
                columns_by_name,
                run_id,
                evaluation_scope,
                created_at,
            ),
            _range_result(
                frame,
                contract,
                columns_by_name,
                run_id,
                evaluation_scope,
                created_at,
            ),
            _freshness_result(
                frame,
                contract,
                run_id,
                evaluation_scope,
                context,
                created_at,
            ),
        ]
    )
    return ValidationReport(
        dataset_id=contract.contract_id,
        run_id=run_id,
        results=tuple(results),
        created_at=created_at,
    )


def _result(
    *,
    contract: DataContract,
    run_id: str,
    scope: dict[str, JsonValue],
    suffix: str,
    severity: str,
    status: str,
    observed_value: JsonValue | None,
    expected_value: JsonValue | None,
    message: str,
    created_at: datetime,
    evidence: dict[str, JsonValue] | None = None,
) -> ValidationResult:
    return ValidationResult.model_validate(
        {
            "check_id": f"{contract.contract_id}.{suffix}",
            "dataset_id": contract.contract_id,
            "run_id": run_id,
            "scope": scope,
            "severity": severity,
            "status": status,
            "observed_value": observed_value,
            "expected_value": expected_value,
            "message": message,
            "evidence": evidence or {},
            "created_at": created_at,
        }
    )


def _type_result(
    frame: pd.DataFrame,
    contract: DataContract,
    columns: dict[str, ColumnContract],
    run_id: str,
    scope: dict[str, JsonValue],
    created_at: datetime,
) -> ValidationResult:
    failures: dict[str, JsonValue] = {}
    for name, column in columns.items():
        if name not in frame:
            continue
        invalid_count = _invalid_type_count(frame[name], column)
        if invalid_count:
            failures[name] = {
                "expected_type": column.logical_type,
                "invalid_count": invalid_count,
            }
    return _result(
        contract=contract,
        run_id=run_id,
        scope=scope,
        suffix="schema.logical_types",
        severity="error",
        status="failed" if failures else "passed",
        observed_value={"type_failures": failures},
        expected_value={"invalid_count": 0},
        message=(
            f"Columns contain values incompatible with logical types: {sorted(failures)}."
            if failures
            else "Column values satisfy registered logical types."
        ),
        created_at=created_at,
    )


def _invalid_type_count(series: pd.Series, column: ColumnContract) -> int:
    values = series[series.notna()]
    if values.empty:
        return 0
    if column.logical_type == "string":
        return int((~values.map(lambda value: isinstance(value, str))).sum())
    if column.logical_type == "integer":
        numeric = pd.to_numeric(values, errors="coerce")
        valid = numeric.notna() & np.isfinite(numeric) & numeric.mod(1).eq(0)
        return int((~valid).sum())
    if column.logical_type == "number":
        numeric = pd.to_numeric(values, errors="coerce")
        return int((numeric.isna() | ~np.isfinite(numeric)).sum())
    if column.logical_type == "boolean":
        return int((~values.map(lambda value: isinstance(value, (bool, np.bool_)))).sum())
    if column.logical_type in {"date", "datetime"}:
        parsed = pd.to_datetime(values, errors="coerce", utc=column.logical_type == "datetime")
        return int(parsed.isna().sum())
    return int((~values.map(_is_json_safe)).sum())


def _is_json_safe(value: Any) -> bool:
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return False
    return True


def _nullability_result(
    frame: pd.DataFrame,
    contract: DataContract,
    columns: dict[str, ColumnContract],
    run_id: str,
    scope: dict[str, JsonValue],
    created_at: datetime,
) -> ValidationResult:
    null_counts: dict[str, JsonValue] = {
        name: int(frame[name].isna().sum())
        for name, column in columns.items()
        if name in frame and not column.nullable and frame[name].isna().any()
    }
    return _result(
        contract=contract,
        run_id=run_id,
        scope=scope,
        suffix="schema.nullability",
        severity="error",
        status="failed" if null_counts else "passed",
        observed_value={"null_counts": null_counts},
        expected_value={"null_count": 0},
        message=(
            f"Required columns contain nulls: {sorted(null_counts)}."
            if null_counts
            else "Required columns contain no nulls."
        ),
        created_at=created_at,
    )


def _primary_key_result(
    frame: pd.DataFrame,
    contract: DataContract,
    run_id: str,
    scope: dict[str, JsonValue],
    created_at: datetime,
) -> ValidationResult:
    missing = [name for name in contract.primary_key if name not in frame]
    if missing:
        missing_key_evidence = _json_string_list(missing)
        return _result(
            contract=contract,
            run_id=run_id,
            scope=scope,
            suffix="keys.unique",
            severity="error",
            status="skipped_with_reason",
            observed_value={"missing_key_columns": missing_key_evidence},
            expected_value={"duplicate_key_count": 0},
            message=f"Uniqueness check skipped because key columns are missing: {missing}.",
            created_at=created_at,
        )
    duplicate_count = int(frame.duplicated(list(contract.primary_key)).sum())
    return _result(
        contract=contract,
        run_id=run_id,
        scope=scope,
        suffix="keys.unique",
        severity="error",
        status="failed" if duplicate_count else "passed",
        observed_value={"duplicate_key_count": duplicate_count},
        expected_value={"duplicate_key_count": 0},
        message=(
            f"Frame contains {duplicate_count} duplicate primary-key rows."
            if duplicate_count
            else "Primary-key rows are unique."
        ),
        created_at=created_at,
    )


def _allowed_values_result(
    frame: pd.DataFrame,
    contract: DataContract,
    columns: dict[str, ColumnContract],
    run_id: str,
    scope: dict[str, JsonValue],
    created_at: datetime,
) -> ValidationResult:
    violations: dict[str, JsonValue] = {}
    for name, column in columns.items():
        if name not in frame or not column.allowed_values:
            continue
        values = frame[name].dropna()
        invalid = values[~values.isin(column.allowed_values)]
        if not invalid.empty:
            violations[name] = {
                "invalid_count": int(len(invalid)),
                "examples": _json_examples(invalid),
            }
    return _result(
        contract=contract,
        run_id=run_id,
        scope=scope,
        suffix="values.allowed",
        severity="error",
        status="failed" if violations else "passed",
        observed_value={"violations": violations},
        expected_value={"invalid_count": 0},
        message=(
            f"Columns contain values outside registered enums: {sorted(violations)}."
            if violations
            else "Enumerated column values satisfy the contract."
        ),
        created_at=created_at,
    )


def _range_result(
    frame: pd.DataFrame,
    contract: DataContract,
    columns: dict[str, ColumnContract],
    run_id: str,
    scope: dict[str, JsonValue],
    created_at: datetime,
) -> ValidationResult:
    violations: dict[str, JsonValue] = {}
    for name, column in columns.items():
        if name not in frame or (column.minimum is None and column.maximum is None):
            continue
        numeric = pd.to_numeric(frame[name], errors="coerce")
        mask = pd.Series(False, index=frame.index)
        if column.minimum is not None:
            mask |= (
                numeric.le(column.minimum)
                if column.exclusive_minimum
                else numeric.lt(column.minimum)
            )
        if column.maximum is not None:
            mask |= (
                numeric.ge(column.maximum)
                if column.exclusive_maximum
                else numeric.gt(column.maximum)
            )
        invalid = frame.loc[mask.fillna(False), name]
        if not invalid.empty:
            violations[name] = {
                "invalid_count": int(len(invalid)),
                "minimum": column.minimum,
                "maximum": column.maximum,
                "exclusive_minimum": column.exclusive_minimum,
                "exclusive_maximum": column.exclusive_maximum,
                "examples": _json_examples(invalid),
            }
    return _result(
        contract=contract,
        run_id=run_id,
        scope=scope,
        suffix="values.ranges",
        severity="error",
        status="failed" if violations else "passed",
        observed_value={"violations": violations},
        expected_value={"invalid_count": 0},
        message=(
            f"Columns contain out-of-range values: {sorted(violations)}."
            if violations
            else "Bounded numeric values satisfy the contract."
        ),
        created_at=created_at,
    )


def _freshness_result(
    frame: pd.DataFrame,
    contract: DataContract,
    run_id: str,
    scope: dict[str, JsonValue],
    context: ContractEvaluationContext | None,
    created_at: datetime,
) -> ValidationResult:
    freshness = contract.freshness
    basis = freshness.basis_column
    if freshness.mode in {"event_driven", "immutable", "run_scoped"}:
        return _result(
            contract=contract,
            run_id=run_id,
            scope=scope,
            suffix="freshness",
            severity="info",
            status="passed",
            observed_value={"mode": freshness.mode},
            expected_value={"mode": freshness.mode},
            message=f"Freshness is governed by {freshness.mode} policy, not an age threshold.",
            created_at=created_at,
        )
    if basis is None or basis not in frame:
        return _result(
            contract=contract,
            run_id=run_id,
            scope=scope,
            suffix="freshness",
            severity="error",
            status="skipped_with_reason",
            observed_value={"basis_column": basis},
            expected_value={"freshness_mode": freshness.mode},
            message="Freshness check skipped because its basis column is unavailable.",
            created_at=created_at,
        )
    if freshness.mode == "latest_completed_session":
        return _session_freshness_result(
            frame,
            contract,
            run_id,
            scope,
            context,
            created_at,
        )
    return _wall_clock_freshness_result(
        frame,
        contract,
        run_id,
        scope,
        context,
        created_at,
    )


def _session_freshness_result(
    frame: pd.DataFrame,
    contract: DataContract,
    run_id: str,
    scope: dict[str, JsonValue],
    context: ContractEvaluationContext | None,
    created_at: datetime,
) -> ValidationResult:
    freshness = contract.freshness
    if context is None or not context.eligible_session_dates:
        return _result(
            contract=contract,
            run_id=run_id,
            scope=scope,
            suffix="freshness",
            severity="error",
            status="skipped_with_reason",
            observed_value={"eligible_session_count": 0},
            expected_value={"eligible_session_calendar": "required"},
            message="Session freshness skipped because no eligible-session calendar was supplied.",
            created_at=created_at,
        )
    basis = str(freshness.basis_column)
    parsed = pd.to_datetime(frame[basis], errors="coerce").dt.date.dropna()
    if parsed.empty:
        observed_date = None
        lag_sessions = None
        future = False
        passed = False
    else:
        observed_date = max(parsed)
        expected_date = context.eligible_session_dates[-1]
        lag_sessions = sum(
            observed_date < session_date <= expected_date
            for session_date in context.eligible_session_dates
        )
        future = observed_date > expected_date
        passed = not future and lag_sessions <= int(freshness.max_lag_sessions or 0)
    expected_date = context.eligible_session_dates[-1]
    return _result(
        contract=contract,
        run_id=run_id,
        scope=scope,
        suffix="freshness",
        severity="error",
        status="passed" if passed else "failed",
        observed_value={
            "latest_observed_session": observed_date.isoformat() if observed_date else None,
            "lag_sessions": lag_sessions,
            "future_session": future,
        },
        expected_value={
            "latest_eligible_session": expected_date.isoformat(),
            "max_lag_sessions": freshness.max_lag_sessions,
            "grace_minutes": freshness.grace_minutes,
        },
        message=(
            "Dataset satisfies session freshness."
            if passed
            else "Dataset is empty, stale, or contains a future session."
        ),
        created_at=created_at,
    )


def _wall_clock_freshness_result(
    frame: pd.DataFrame,
    contract: DataContract,
    run_id: str,
    scope: dict[str, JsonValue],
    context: ContractEvaluationContext | None,
    created_at: datetime,
) -> ValidationResult:
    freshness = contract.freshness
    basis = str(freshness.basis_column)
    parsed = pd.to_datetime(frame[basis], errors="coerce", utc=True).dropna()
    evaluated_at = context.evaluated_at if context is not None else created_at
    if parsed.empty:
        latest = None
        age_minutes = None
        future = False
        passed = False
    else:
        latest = max(parsed).to_pydatetime()
        age_minutes = (evaluated_at - latest).total_seconds() / 60
        future = age_minutes < 0
        allowed_age = int(freshness.max_age_minutes or 0) + freshness.grace_minutes
        passed = not future and age_minutes <= allowed_age
    return _result(
        contract=contract,
        run_id=run_id,
        scope=scope,
        suffix="freshness",
        severity="error",
        status="passed" if passed else "failed",
        observed_value={
            "latest_observed_at": latest.isoformat() if latest else None,
            "age_minutes": age_minutes,
            "future_timestamp": future,
        },
        expected_value={
            "max_age_minutes": freshness.max_age_minutes,
            "grace_minutes": freshness.grace_minutes,
        },
        message=(
            "Dataset satisfies wall-clock freshness."
            if passed
            else "Dataset is empty, stale, or contains a future freshness timestamp."
        ),
        created_at=created_at,
    )


def _json_examples(series: pd.Series, limit: int = 5) -> list[JsonValue]:
    examples: list[JsonValue] = []
    for value in series.drop_duplicates().head(limit):
        if isinstance(value, np.generic):
            value = value.item()
        if isinstance(value, (date, datetime, pd.Timestamp)):
            value = value.isoformat()
        if _is_json_safe(value):
            examples.append(value)
        else:
            examples.append(str(value))
    return examples


def _json_string_list(values: Iterable[str]) -> list[JsonValue]:
    result: list[JsonValue] = []
    for value in values:
        result.append(value)
    return result
