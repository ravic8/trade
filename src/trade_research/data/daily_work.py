from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid5

WORK_PRIORITIES = {
    "daily_incremental": 10,
    "daily_incremental_retry": 20,
    "new_symbol_backfill": 30,
    "gap_repair": 40,
    "initial_backfill": 50,
}

DURABLE_RETRY_DELAYS = (
    timedelta(minutes=5),
    timedelta(minutes=15),
    timedelta(hours=1),
    timedelta(hours=4),
    timedelta(hours=12),
    timedelta(hours=24),
)


def durable_retry_delay(attempt_count: int) -> timedelta:
    return DURABLE_RETRY_DELAYS[min(max(int(attempt_count), 1) - 1, len(DURABLE_RETRY_DELAYS) - 1)]


@dataclass(frozen=True)
class DailyInstrument:
    canonical_instrument_id: str
    provider_symbol: str
    exchange: str

    @property
    def instrument_key(self) -> str:
        return f"YF|{self.provider_symbol}"


@dataclass(frozen=True)
class DailyWorkItem:
    work_item_id: str
    idempotency_key: str
    work_type: str
    provider: str
    exchange: str
    canonical_instrument_id: str
    provider_symbol: str
    interval: str
    window_start: date
    window_end: date
    priority: int
    status: str
    attempt_count: int
    max_attempts: int
    next_attempt_at: datetime
    created_at: datetime
    updated_at: datetime
    parent_work_item_id: str | None = None

    def as_row(self) -> dict[str, Any]:
        return asdict(self)


class DailyWorkPlanner:
    """Build idempotent daily Yahoo work from calendars and stored coverage."""

    def __init__(self, *, provider: str = "yfinance", max_attempts: int = 9) -> None:
        self.provider = provider
        self.max_attempts = max_attempts

    def plan_incremental(
        self,
        instruments: Sequence[DailyInstrument],
        sessions: Sequence[date],
        latest_dates: Mapping[str, date],
        *,
        overlap_sessions: int,
        now: datetime | None = None,
    ) -> list[DailyWorkItem]:
        ordered_sessions = sorted(set(sessions))
        if not ordered_sessions:
            return []
        end = ordered_sessions[-1]
        session_index = {session: index for index, session in enumerate(ordered_sessions)}
        observed_at = _as_utc(now or datetime.now(UTC))
        work: list[DailyWorkItem] = []
        for instrument in instruments:
            latest = latest_dates.get(instrument.instrument_key)
            if latest is None or latest >= end:
                continue
            preceding_index = max(
                _session_index_at_or_before(ordered_sessions, session_index, latest)
                - max(overlap_sessions, 0),
                0,
            )
            work.append(
                self._item(
                    instrument,
                    work_type="daily_incremental",
                    window_start=ordered_sessions[preceding_index],
                    window_end=end,
                    now=observed_at,
                )
            )
        return work

    def plan_initial_backfill(
        self,
        instruments: Sequence[DailyInstrument],
        sessions: Sequence[date],
        stored_dates: Mapping[str, set[date]],
        *,
        now: datetime | None = None,
    ) -> list[DailyWorkItem]:
        return self._plan_missing_windows(
            instruments,
            sessions,
            stored_dates,
            work_type="initial_backfill",
            now=now,
        )

    def plan_new_symbol_backfill(
        self,
        instruments: Sequence[DailyInstrument],
        sessions: Sequence[date],
        stored_dates: Mapping[str, set[date]],
        *,
        now: datetime | None = None,
    ) -> list[DailyWorkItem]:
        return self._plan_missing_windows(
            instruments,
            sessions,
            stored_dates,
            work_type="new_symbol_backfill",
            now=now,
        )

    def plan_gap_repair(
        self,
        instruments: Sequence[DailyInstrument],
        sessions: Sequence[date],
        stored_dates: Mapping[str, set[date]],
        *,
        now: datetime | None = None,
    ) -> list[DailyWorkItem]:
        return self._plan_missing_windows(
            instruments,
            sessions,
            stored_dates,
            work_type="gap_repair",
            now=now,
        )

    def _plan_missing_windows(
        self,
        instruments: Sequence[DailyInstrument],
        sessions: Sequence[date],
        stored_dates: Mapping[str, set[date]],
        *,
        work_type: str,
        now: datetime | None,
    ) -> list[DailyWorkItem]:
        ordered_sessions = sorted(set(sessions))
        if not ordered_sessions:
            return []
        observed_at = _as_utc(now or datetime.now(UTC))
        work: list[DailyWorkItem] = []
        for instrument in instruments:
            present = stored_dates.get(instrument.instrument_key, set())
            missing = [session for session in ordered_sessions if session not in present]
            for window_start, window_end in _contiguous_session_windows(ordered_sessions, missing):
                work.append(
                    self._item(
                        instrument,
                        work_type=work_type,
                        window_start=window_start,
                        window_end=window_end,
                        now=observed_at,
                    )
                )
        return work

    def _item(
        self,
        instrument: DailyInstrument,
        *,
        work_type: str,
        window_start: date,
        window_end: date,
        now: datetime,
        parent_work_item_id: str | None = None,
    ) -> DailyWorkItem:
        key = work_item_idempotency_key(
            provider=self.provider,
            work_type=work_type,
            canonical_instrument_id=instrument.canonical_instrument_id,
            interval="1d",
            window_start=window_start,
            window_end=window_end,
        )
        return DailyWorkItem(
            work_item_id=str(uuid5(NAMESPACE_URL, key)),
            idempotency_key=key,
            work_type=work_type,
            provider=self.provider,
            exchange=instrument.exchange.upper(),
            canonical_instrument_id=instrument.canonical_instrument_id,
            provider_symbol=instrument.provider_symbol,
            interval="1d",
            window_start=window_start,
            window_end=window_end,
            priority=WORK_PRIORITIES[work_type],
            status="queued",
            attempt_count=0,
            max_attempts=self.max_attempts,
            next_attempt_at=now,
            created_at=now,
            updated_at=now,
            parent_work_item_id=parent_work_item_id,
        )


def work_item_idempotency_key(
    *,
    provider: str,
    work_type: str,
    canonical_instrument_id: str,
    interval: str,
    window_start: date,
    window_end: date,
) -> str:
    return "|".join(
        (
            provider,
            work_type,
            canonical_instrument_id,
            interval,
            window_start.isoformat(),
            window_end.isoformat(),
        )
    )


def _contiguous_session_windows(
    ordered_sessions: Sequence[date],
    missing_sessions: Iterable[date],
) -> list[tuple[date, date]]:
    missing = set(missing_sessions)
    windows: list[tuple[date, date]] = []
    start: date | None = None
    end: date | None = None
    for session in ordered_sessions:
        if session in missing:
            start = session if start is None else start
            end = session
        elif start is not None and end is not None:
            windows.append((start, end))
            start = end = None
    if start is not None and end is not None:
        windows.append((start, end))
    return windows


def _session_index_at_or_before(
    sessions: Sequence[date], session_index: Mapping[date, int], value: date
) -> int:
    exact = session_index.get(value)
    if exact is not None:
        return exact
    for index in range(len(sessions) - 1, -1, -1):
        if sessions[index] <= value:
            return index
    return 0


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
