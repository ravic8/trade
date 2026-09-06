from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import Engine, insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from trade_research.control_plane.tables import (
    market_data_availability_observations_table,
)
from trade_research.market_data.contracts import ProviderRequest

_NSE_TIMEZONE = ZoneInfo("Asia/Kolkata")


@dataclass(frozen=True)
class MarketDataAvailabilityObservation:
    source_run_id: str
    request_id: str
    provider: str
    exchange: str
    interval: str
    instrument_id: str
    provider_symbol: str
    requested_start: datetime
    requested_end: datetime
    eligible_sessions: tuple[date, ...]
    observed_sessions: tuple[date, ...]
    observed_row_count: int
    status: str
    reason_code: str
    retryable: bool
    observed_at: datetime
    observed_first_timestamp: datetime | None = None
    observed_last_timestamp: datetime | None = None
    raw_artifact_id: str | None = None
    workspace_id: str = "default"

    @property
    def availability_observation_id(self) -> str:
        return _digest(self.evidence_payload())

    def evidence_payload(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "source_run_id": self.source_run_id,
            "request_id": self.request_id,
            "provider": self.provider,
            "exchange": self.exchange,
            "interval": self.interval,
            "instrument_id": self.instrument_id,
            "provider_symbol": self.provider_symbol,
            "requested_start": _as_utc(self.requested_start).isoformat(),
            "requested_end": _as_utc(self.requested_end).isoformat(),
            "eligible_sessions": [item.isoformat() for item in self.eligible_sessions],
            "observed_sessions": [item.isoformat() for item in self.observed_sessions],
            "observed_first_timestamp": (
                _as_utc(self.observed_first_timestamp).isoformat()
                if self.observed_first_timestamp is not None
                else None
            ),
            "observed_last_timestamp": (
                _as_utc(self.observed_last_timestamp).isoformat()
                if self.observed_last_timestamp is not None
                else None
            ),
            "observed_row_count": self.observed_row_count,
            "status": self.status,
            "reason_code": self.reason_code,
            "retryable": self.retryable,
            "raw_artifact_id": self.raw_artifact_id,
        }

    def row(self) -> dict[str, Any]:
        values = asdict(self)
        values.update(
            {
                "availability_observation_id": self.availability_observation_id,
                "requested_start": _as_utc(self.requested_start),
                "requested_end": _as_utc(self.requested_end),
                "eligible_sessions": [
                    item.isoformat() for item in self.eligible_sessions
                ],
                "observed_sessions": [
                    item.isoformat() for item in self.observed_sessions
                ],
                "observed_first_timestamp": (
                    _as_utc(self.observed_first_timestamp)
                    if self.observed_first_timestamp is not None
                    else None
                ),
                "observed_last_timestamp": (
                    _as_utc(self.observed_last_timestamp)
                    if self.observed_last_timestamp is not None
                    else None
                ),
                "observed_at": _as_utc(self.observed_at),
                "created_at": _as_utc(self.observed_at),
            }
        )
        return values


class MarketDataAvailabilityRepository:
    """Append-only observations of what a provider returned for a request."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def record(self, observations: Sequence[MarketDataAvailabilityObservation]) -> int:
        rows = [observation.row() for observation in observations]
        if not rows:
            return 0
        dialect = self._engine.dialect.name
        with self._engine.begin() as connection:
            for row in rows:
                if dialect == "postgresql":
                    statement: Any = postgresql_insert(
                        market_data_availability_observations_table
                    ).values(**row)
                    statement = statement.on_conflict_do_nothing(
                        index_elements=["availability_observation_id"]
                    )
                elif dialect == "sqlite":
                    statement = sqlite_insert(
                        market_data_availability_observations_table
                    ).values(**row)
                    statement = statement.on_conflict_do_nothing(
                        index_elements=["availability_observation_id"]
                    )
                else:
                    statement = insert(
                        market_data_availability_observations_table
                    ).values(**row)
                connection.execute(statement)
        return len(rows)


def observe_nse_minute_availability(
    *,
    request: ProviderRequest,
    source_run_id: str,
    raw_frame: pd.DataFrame,
    canonical_instrument_ids: Mapping[str, str],
    eligible_sessions: set[date],
    unavailable_provider_symbols: set[str] | None = None,
    raw_artifact_id: str | None = None,
) -> list[MarketDataAvailabilityObservation]:
    """Build per-instrument evidence from the provider response, not a retention claim."""

    unavailable = unavailable_provider_symbols or set()
    timestamps = _timestamps(raw_frame)
    symbols = _provider_symbols(raw_frame)
    observations: list[MarketDataAvailabilityObservation] = []
    for provider_symbol, instrument_id in sorted(canonical_instrument_ids.items()):
        failed = provider_symbol in unavailable
        symbol_mask = symbols.eq(_normalize_symbol(provider_symbol))
        selected = timestamps[
            symbol_mask
            & timestamps.notna()
            & timestamps.ge(request.window_start)
            & timestamps.lt(request.window_end)
        ]
        selected_dates = selected.dt.tz_convert(_NSE_TIMEZONE).dt.date
        observed_sessions = tuple(
            sorted(set(selected_dates[selected_dates.isin(eligible_sessions)].tolist()))
        )
        first = selected.min() if not selected.empty else None
        last = selected.max() if not selected.empty else None
        if failed:
            status = "request_failed"
            reason_code = "provider_request_failed"
        elif selected.empty:
            status = "empty"
            reason_code = "provider_returned_no_data"
        else:
            status = "observed"
            reason_code = "availability_observed"
        observations.append(
            MarketDataAvailabilityObservation(
                source_run_id=source_run_id,
                request_id=request.request_id,
                provider=request.provider,
                exchange=request.exchange,
                interval=request.interval.value,
                instrument_id=instrument_id,
                provider_symbol=provider_symbol,
                requested_start=request.window_start,
                requested_end=request.window_end,
                eligible_sessions=tuple(sorted(eligible_sessions)),
                observed_sessions=observed_sessions,
                observed_first_timestamp=(
                    first.to_pydatetime() if first is not None else None
                ),
                observed_last_timestamp=(
                    last.to_pydatetime() if last is not None else None
                ),
                observed_row_count=int(len(selected)),
                status=status,
                reason_code=reason_code,
                retryable=status != "observed",
                observed_at=request.retrieved_at,
                raw_artifact_id=raw_artifact_id,
            )
        )
    return observations


def availability_session_maps(
    observations: Sequence[MarketDataAvailabilityObservation],
) -> tuple[dict[str, set[date]], dict[str, str]]:
    sessions = {
        observation.provider_symbol: set(observation.observed_sessions)
        for observation in observations
    }
    reasons = {
        observation.provider_symbol: observation.reason_code
        for observation in observations
        if observation.status != "observed"
    }
    return sessions, reasons


def _timestamps(frame: pd.DataFrame) -> pd.Series:
    if frame.empty or "Timestamp" not in frame.columns:
        return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
    return pd.to_datetime(frame["Timestamp"], errors="coerce", utc=True)


def _provider_symbols(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series("", index=frame.index, dtype="string")
    for column in ("TradingSymbol", "ProviderSymbol", "Symbol"):
        if column in frame.columns:
            return frame[column].astype("string").map(_normalize_symbol)
    return pd.Series("", index=frame.index, dtype="string")


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _digest(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
