from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import numpy as np
import pandas as pd
from pydantic import JsonValue

from trade_research.contracts import (
    ContractEvaluationContext,
    evaluate_frame_contract,
    get_data_contract,
)
from trade_research.exchange_sessions import build_materialized_exchange_sessions
from trade_research.targets.daily_forward import DAILY_FORWARD_TARGET_COLUMNS_V1_0
from trade_research.validation import ValidationReport, ValidationResult


@dataclass(frozen=True)
class PublicationContractEvidence:
    report: ValidationReport
    report_path: Path


def enforce_publication_contract(
    frame: pd.DataFrame,
    *,
    contract_id: str,
    report_path: Path,
    run_id: str | None = None,
    scope: Mapping[str, JsonValue] | None = None,
    eligible_session_dates: Iterable[date] = (),
    additional_results: Collection[ValidationResult] = (),
    accepted_warning_check_ids: Collection[str] = (),
    evaluated_at: datetime | None = None,
) -> PublicationContractEvidence:
    """Write contract evidence and fail closed before a frame is published."""

    publication_run_id = run_id or f"publication-{uuid4()}"
    evaluation_time = _as_utc(evaluated_at or datetime.now(UTC))
    context = ContractEvaluationContext(
        evaluated_at=evaluation_time,
        eligible_session_dates=tuple(sorted(set(eligible_session_dates))),
    )
    generic_report = evaluate_frame_contract(
        frame,
        get_data_contract(contract_id),
        run_id=publication_run_id,
        scope={"boundary": "pre_publication", **dict(scope or {})},
        context=context,
    )
    report = ValidationReport(
        dataset_id=generic_report.dataset_id,
        run_id=generic_report.run_id,
        results=(*generic_report.results, *additional_results),
        created_at=generic_report.created_at,
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report.model_dump_json(indent=2) + "\n")
    report.require_downstream_ready(
        accepted_warning_check_ids=accepted_warning_check_ids
    )
    return PublicationContractEvidence(report=report, report_path=report_path)


def normalize_daily_ohlcv_publication_frame(
    frame: pd.DataFrame,
    *,
    exchange: str,
    source: str,
    fetched_at: datetime | None = None,
) -> pd.DataFrame:
    """Map a provider frame to the exact daily-OHLCV publication schema.

    Rows are intentionally not dropped or deduplicated here. Invalid input must
    remain visible to the contract gate rather than disappearing during storage
    normalization.
    """

    columns = {
        "instrument_key": _column(frame, "instrument_key", "InstrumentKey"),
        "symbol": _string_column(frame, "symbol", "Symbol", uppercase=True),
        "exchange": pd.Series(exchange.upper(), index=frame.index, dtype="object"),
        "source": pd.Series(source.lower(), index=frame.index, dtype="object"),
        "date": pd.to_datetime(
            _column(frame, "date", "Date"), errors="coerce"
        ).dt.date,
        "open": pd.to_numeric(_column(frame, "open", "Open"), errors="coerce"),
        "high": pd.to_numeric(_column(frame, "high", "High"), errors="coerce"),
        "low": pd.to_numeric(_column(frame, "low", "Low"), errors="coerce"),
        "close": pd.to_numeric(_column(frame, "close", "Close"), errors="coerce"),
        "volume": pd.to_numeric(_column(frame, "volume", "Volume"), errors="coerce"),
        "open_interest": pd.to_numeric(
            _column(frame, "open_interest", "OpenInterest"), errors="coerce"
        ),
    }
    normalized = pd.DataFrame(columns, index=frame.index)
    suspicious = (
        normalized[["open", "high", "low", "close", "volume"]].isna().any(axis=1)
        | normalized["open"].le(0)
        | normalized["high"].le(0)
        | normalized["low"].le(0)
        | normalized["close"].le(0)
        | normalized["volume"].lt(0)
        | normalized["high"].lt(normalized["low"])
        | normalized["high"].lt(normalized["open"])
        | normalized["high"].lt(normalized["close"])
        | normalized["low"].gt(normalized["open"])
        | normalized["low"].gt(normalized["close"])
    )
    normalized["fetched_at"] = _as_utc(fetched_at or datetime.now(UTC))
    normalized["quality_status"] = np.where(suspicious, "suspicious", "ok")
    return normalized.reset_index(drop=True)


def expected_publication_sessions(
    frame: pd.DataFrame,
    *,
    exchange: str,
    expected_end: date | None = None,
) -> tuple[date, ...]:
    """Resolve exchange sessions spanning a publication frame and its expected end."""

    if "date" not in frame:
        return ()
    observed = pd.to_datetime(frame["date"], errors="coerce").dt.date.dropna()
    if observed.empty:
        return ()
    start = min(observed)
    end = expected_end or max(observed)
    if end < start:
        return ()
    return tuple(
        row.session_date
        for row in build_materialized_exchange_sessions(exchange, start, end)
        if row.is_trading_day
    )


def daily_ohlcv_invariant_result(
    frame: pd.DataFrame,
    *,
    run_id: str,
    scope: Mapping[str, JsonValue] | None = None,
    created_at: datetime | None = None,
) -> ValidationResult:
    violations = {
        "high_below_low": int(frame["high"].lt(frame["low"]).fillna(False).sum()),
        "high_below_open": int(frame["high"].lt(frame["open"]).fillna(False).sum()),
        "high_below_close": int(frame["high"].lt(frame["close"]).fillna(False).sum()),
        "low_above_open": int(frame["low"].gt(frame["open"]).fillna(False).sum()),
        "low_above_close": int(frame["low"].gt(frame["close"]).fillna(False).sum()),
    }
    violation_count = sum(violations.values())
    violation_evidence: dict[str, JsonValue] = dict(violations)
    return ValidationResult(
        check_id="market_data.ohlcv_daily.v1.invariants.ohlc_ordering",
        dataset_id="market_data.ohlcv_daily.v1",
        run_id=run_id,
        scope={"boundary": "pre_publication", **dict(scope or {})},
        severity="error",
        status="failed" if violation_count else "passed",
        observed_value={
            "violation_count": violation_count,
            "violations": violation_evidence,
        },
        expected_value={"violation_count": 0},
        message=(
            "Daily candles contain invalid cross-column OHLC ordering."
            if violation_count
            else "Daily candles satisfy cross-column OHLC ordering."
        ),
        created_at=_as_utc(created_at or datetime.now(UTC)),
    )


def feature_target_leakage_result(
    frame: pd.DataFrame,
    *,
    run_id: str,
    scope: Mapping[str, JsonValue] | None = None,
    created_at: datetime | None = None,
) -> ValidationResult:
    forbidden = sorted(set(frame.columns).intersection(DAILY_FORWARD_TARGET_COLUMNS_V1_0))
    return ValidationResult(
        check_id="feature.daily_technical.v1.invariants.target_columns_absent",
        dataset_id="feature.daily_technical.v1",
        run_id=run_id,
        scope={"boundary": "pre_publication", **dict(scope or {})},
        severity="error",
        status="failed" if forbidden else "passed",
        observed_value={"target_columns": forbidden},
        expected_value={"target_columns": []},
        message=(
            f"Feature publication contains target columns: {forbidden}."
            if forbidden
            else "Feature publication contains no registered forward-target columns."
        ),
        created_at=_as_utc(created_at or datetime.now(UTC)),
    )


def failed_quality_rows_result(
    frame: pd.DataFrame,
    *,
    contract_id: str,
    run_id: str,
    scope: Mapping[str, JsonValue] | None = None,
    created_at: datetime | None = None,
) -> ValidationResult:
    failed_count = (
        int(frame["quality_status"].eq("failed").sum())
        if "quality_status" in frame
        else 0
    )
    return ValidationResult(
        check_id=f"{contract_id}.quality.failed_rows",
        dataset_id=contract_id,
        run_id=run_id,
        scope={"boundary": "pre_publication", **dict(scope or {})},
        severity="error",
        status="failed" if failed_count else "passed",
        observed_value={"failed_row_count": failed_count},
        expected_value={"failed_row_count": 0},
        message=(
            f"Publication contains {failed_count} failed-quality rows."
            if failed_count
            else "Publication contains no failed-quality rows."
        ),
        created_at=_as_utc(created_at or datetime.now(UTC)),
    )


def _column(frame: pd.DataFrame, normalized: str, provider: str) -> pd.Series:
    if normalized in frame:
        return frame[normalized]
    if provider in frame:
        return frame[provider]
    return pd.Series(pd.NA, index=frame.index, dtype="object")


def _string_column(
    frame: pd.DataFrame,
    normalized: str,
    provider: str,
    *,
    uppercase: bool,
) -> pd.Series:
    values = _column(frame, normalized, provider).copy()
    mask = values.notna()
    values.loc[mask] = values.loc[mask].map(str)
    if uppercase:
        values.loc[mask] = values.loc[mask].str.upper()
    return values


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
