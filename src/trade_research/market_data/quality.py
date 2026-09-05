from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from typing import Any, overload
from zoneinfo import ZoneInfo

from sqlalchemy import Engine, insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from trade_research.control_plane.tables import market_data_quality_outcomes_table
from trade_research.market_data.contracts import MarketCandle, ProviderRequest
from trade_research.market_data.validation import (
    CandleValidationIssue,
    CandleValidationResult,
    ValidationSeverity,
)

_NSE_TIMEZONE = ZoneInfo("Asia/Kolkata")
_NSE_OPEN = time(9, 15)
_NSE_CLOSE = time(15, 30)


class MarketDataQualityStatus(StrEnum):
    VALID = "valid"
    MISSING = "missing"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    DUPLICATE = "duplicate"
    INVALID = "invalid"
    OUTSIDE_SESSION = "outside_session"
    STALE = "stale"


@dataclass(frozen=True)
class MarketDataQualityOutcome:
    source_run_id: str
    request_id: str
    provider: str
    exchange: str
    interval: str
    instrument_id: str
    provider_symbol: str
    session_date: date
    status: MarketDataQualityStatus
    reason_code: str
    severity: str
    expected: bool
    retryable: bool
    observed_at: datetime
    candle_timestamp: datetime | None = None
    raw_artifact_id: str | None = None
    details: dict[str, Any] | None = None
    workspace_id: str = "default"

    @property
    def quality_outcome_id(self) -> str:
        identity = {
            "workspace_id": self.workspace_id,
            "source_run_id": self.source_run_id,
            "request_id": self.request_id,
            "instrument_id": self.instrument_id,
            "interval": self.interval,
            "session_date": self.session_date.isoformat(),
            "candle_timestamp": (
                _as_utc(self.candle_timestamp).isoformat()
                if self.candle_timestamp is not None
                else None
            ),
            "reason_code": self.reason_code,
        }
        encoded = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()

    def row(self, at: datetime) -> dict[str, Any]:
        row = asdict(self)
        row["quality_outcome_id"] = self.quality_outcome_id
        row["status"] = self.status.value
        row["observed_at"] = _as_utc(self.observed_at)
        row["candle_timestamp"] = (
            _as_utc(self.candle_timestamp) if self.candle_timestamp is not None else None
        )
        row["details"] = self.details or {}
        row["created_at"] = at
        row["updated_at"] = at
        return row


@dataclass(frozen=True)
class DailyExpectedWindow:
    instrument_id: str
    provider_symbol: str
    window_start: date
    window_end: date
    provider_available: bool = True
    reason_code: str | None = None
    retryable: bool = True


class MarketDataQualityRepository:
    """Idempotent PostgreSQL ledger for candle-level quality explanations."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def record(self, outcomes: Iterable[MarketDataQualityOutcome]) -> int:
        observed_at = datetime.now(UTC)
        rows = [outcome.row(observed_at) for outcome in outcomes]
        if not rows:
            return 0
        dialect = self._engine.dialect.name
        with self._engine.begin() as connection:
            for offset in range(0, len(rows), 1_000):
                chunk = rows[offset : offset + 1_000]
                if dialect == "postgresql":
                    statement: Any = postgresql_insert(
                        market_data_quality_outcomes_table
                    ).values(
                        chunk
                    )
                elif dialect == "sqlite":
                    statement = sqlite_insert(market_data_quality_outcomes_table).values(chunk)
                else:
                    connection.execute(
                        insert(market_data_quality_outcomes_table).values(chunk)
                    )
                    continue
                statement = statement.on_conflict_do_update(
                    index_elements=["quality_outcome_id"],
                    set_={
                        column: getattr(statement.excluded, column)
                        for column in (
                            "status",
                            "severity",
                            "expected",
                            "retryable",
                            "raw_artifact_id",
                            "details",
                            "observed_at",
                            "updated_at",
                        )
                    },
                )
                connection.execute(statement)
        return len(rows)


def validation_quality_outcomes(
    *,
    request: ProviderRequest,
    source_run_id: str,
    result: CandleValidationResult,
) -> list[MarketDataQualityOutcome]:
    candles = [*result.accepted, *result.rejected]
    by_identity = {candle.identity: candle for candle in candles}
    outcomes = [
        _candle_outcome(
            request=request,
            source_run_id=source_run_id,
            candle=candle,
            status=MarketDataQualityStatus.VALID,
            reason_code="accepted",
            severity="info",
            retryable=False,
            details={},
        )
        for candle in result.accepted
    ]
    for issue in result.issues:
        identity = (issue.instrument_id, request.interval, issue.session_date, issue.timestamp)
        candle = by_identity[identity]
        outcomes.append(
            _candle_outcome(
                request=request,
                source_run_id=source_run_id,
                candle=candle,
                status=_status_for_issue(issue),
                reason_code=issue.code,
                severity=issue.severity.value,
                retryable=_issue_is_retryable(issue),
                details={"message": issue.message},
            )
        )
    return outcomes


def daily_missing_quality_outcomes(
    *,
    request: ProviderRequest,
    source_run_id: str,
    windows: Sequence[DailyExpectedWindow],
    eligible_sessions: set[date],
    accepted: Sequence[MarketCandle],
) -> list[MarketDataQualityOutcome]:
    observed = {(candle.instrument_id, candle.session_date) for candle in accepted}
    outcomes: list[MarketDataQualityOutcome] = []
    for window in windows:
        for session_date in sorted(eligible_sessions):
            if not window.window_start <= session_date <= window.window_end:
                continue
            if (window.instrument_id, session_date) in observed:
                continue
            status = (
                MarketDataQualityStatus.MISSING
                if window.provider_available
                else MarketDataQualityStatus.PROVIDER_UNAVAILABLE
            )
            reason_code = window.reason_code or (
                "candle_absent" if window.provider_available else "provider_request_failed"
            )
            outcomes.append(
                MarketDataQualityOutcome(
                    source_run_id=source_run_id,
                    request_id=request.request_id,
                    provider=request.provider,
                    exchange=request.exchange,
                    interval=request.interval.value,
                    instrument_id=window.instrument_id,
                    provider_symbol=window.provider_symbol,
                    session_date=session_date,
                    status=status,
                    reason_code=reason_code,
                    severity="error" if window.provider_available else "warning",
                    expected=True,
                    retryable=window.retryable,
                    observed_at=request.retrieved_at,
                    details={
                        "window_start": window.window_start.isoformat(),
                        "window_end": window.window_end.isoformat(),
                    },
                )
            )
    return outcomes


def nse_minute_missing_quality_outcomes(
    *,
    request: ProviderRequest,
    source_run_id: str,
    canonical_instrument_ids: Mapping[str, str],
    eligible_sessions: set[date],
    accepted: Sequence[MarketCandle],
    unavailable_provider_symbols: set[str] | None = None,
) -> list[MarketDataQualityOutcome]:
    unavailable = unavailable_provider_symbols or set()
    observed = {
        (candle.instrument_id, _as_utc(candle.timestamp))
        for candle in accepted
        if candle.timestamp is not None
    }
    outcomes: list[MarketDataQualityOutcome] = []
    for provider_symbol, instrument_id in sorted(canonical_instrument_ids.items()):
        provider_unavailable = provider_symbol in unavailable
        for session_date in sorted(eligible_sessions):
            for timestamp in _nse_minute_grid(session_date, request):
                if (instrument_id, timestamp) in observed:
                    continue
                outcomes.append(
                    MarketDataQualityOutcome(
                        source_run_id=source_run_id,
                        request_id=request.request_id,
                        provider=request.provider,
                        exchange=request.exchange,
                        interval=request.interval.value,
                        instrument_id=instrument_id,
                        provider_symbol=provider_symbol,
                        session_date=session_date,
                        candle_timestamp=timestamp,
                        status=(
                            MarketDataQualityStatus.PROVIDER_UNAVAILABLE
                            if provider_unavailable
                            else MarketDataQualityStatus.MISSING
                        ),
                        reason_code=(
                            "provider_request_failed"
                            if provider_unavailable
                            else "candle_absent"
                        ),
                        severity="warning" if provider_unavailable else "error",
                        expected=True,
                        retryable=True,
                        observed_at=request.retrieved_at,
                        details={"session_timezone": "Asia/Kolkata"},
                    )
                )
    return outcomes


def _nse_minute_grid(session_date: date, request: ProviderRequest) -> list[datetime]:
    current = datetime.combine(session_date, _NSE_OPEN, tzinfo=_NSE_TIMEZONE).astimezone(UTC)
    close = datetime.combine(session_date, _NSE_CLOSE, tzinfo=_NSE_TIMEZONE).astimezone(UTC)
    timestamps: list[datetime] = []
    while current < close:
        if request.window_start <= current < request.window_end:
            timestamps.append(current)
        current += timedelta(minutes=1)
    return timestamps


def _candle_outcome(
    *,
    request: ProviderRequest,
    source_run_id: str,
    candle: MarketCandle,
    status: MarketDataQualityStatus,
    reason_code: str,
    severity: str,
    retryable: bool,
    details: dict[str, Any],
) -> MarketDataQualityOutcome:
    return MarketDataQualityOutcome(
        source_run_id=source_run_id,
        request_id=request.request_id,
        provider=request.provider,
        exchange=request.exchange,
        interval=request.interval.value,
        instrument_id=candle.instrument_id,
        provider_symbol=candle.provider_symbol,
        session_date=candle.session_date,
        candle_timestamp=candle.timestamp,
        status=status,
        reason_code=reason_code,
        severity=severity,
        expected=True,
        retryable=retryable,
        raw_artifact_id=candle.raw_artifact_id,
        observed_at=request.retrieved_at,
        details=details,
    )


def _status_for_issue(issue: CandleValidationIssue) -> MarketDataQualityStatus:
    if issue.code == "duplicate":
        return MarketDataQualityStatus.DUPLICATE
    if issue.code == "stale_provider_timestamp":
        return MarketDataQualityStatus.STALE
    if issue.code in {
        "ineligible_session",
        "outside_exchange_session",
        "outside_request_window",
        "session_date_mismatch",
    }:
        return MarketDataQualityStatus.OUTSIDE_SESSION
    return MarketDataQualityStatus.INVALID


def _issue_is_retryable(issue: CandleValidationIssue) -> bool:
    return issue.severity is ValidationSeverity.WARNING or issue.code in {
        "future_provider_timestamp",
        "stale_provider_timestamp",
    }


@overload
def _as_utc(value: datetime) -> datetime: ...


@overload
def _as_utc(value: None) -> None: ...


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
