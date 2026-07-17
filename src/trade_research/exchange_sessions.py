from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from importlib.metadata import version
from typing import Any, Protocol
from zoneinfo import ZoneInfo

import pandas_market_calendars as market_calendars

from trade_research.exchanges import canonical_equity_exchange
from trade_research.market_calendar import (
    EXCHANGE_CONFIGS,
    ExchangeHolidays,
    expected_trading_dates,
)

PANDAS_MARKET_CALENDARS_SOURCE_URL = (
    "https://pandas-market-calendars.readthedocs.io/en/latest/"
)
_CALENDAR_NAMES = {"NSE": "NSE", "TSX": "TSX", "US": "NYSE"}


class ExchangeSessionError(RuntimeError):
    pass


class ExchangeSessionStore(Protocol):
    def exchange_sessions(
        self,
        exchange: str,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        ...

    def exchange_holidays(
        self,
        exchange: str,
        year: int,
        max_age_days: int | None = None,
    ) -> dict[str, Any] | None:
        ...


@dataclass(frozen=True)
class MaterializedExchangeSession:
    exchange: str
    session_date: date
    is_trading_day: bool
    market_open_utc: datetime | None
    market_close_utc: datetime | None
    is_early_close: bool
    source_url: str
    calendar_version: str
    validation_status: str
    generated_at: datetime

    def as_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExchangeSessionValidation:
    accepted: bool
    row_count: int
    trading_day_count: int
    closed_day_count: int
    early_close_count: int
    open_days_by_year: dict[int, int]
    errors: tuple[str, ...]


@dataclass(frozen=True)
class ExchangeSessionShadowComparison:
    legacy_only_dates: tuple[date, ...]
    materialized_only_dates: tuple[date, ...]
    compared_years: tuple[int, ...]

    @property
    def discrepancy_count(self) -> int:
        return len(self.legacy_only_dates) + len(self.materialized_only_dates)


@dataclass(frozen=True)
class ExpectedSessionResolution:
    dates: tuple[date, ...]
    source: str


def build_materialized_exchange_sessions(
    exchange: str,
    start_date: date,
    end_date: date,
    *,
    holiday_overrides: Mapping[int, ExchangeHolidays] | None = None,
    observed_special_open_dates: set[date] | frozenset[date] | None = None,
    generated_at: datetime | None = None,
) -> list[MaterializedExchangeSession]:
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")
    canonical_exchange = canonical_equity_exchange(exchange)
    config = EXCHANGE_CONFIGS[canonical_exchange]
    calendar_name = _CALENDAR_NAMES[canonical_exchange]
    calendar = market_calendars.get_calendar(calendar_name)
    schedule = calendar.schedule(start_date=start_date, end_date=end_date)
    schedule_by_date = {
        index.date(): row
        for index, row in schedule.iterrows()
    }
    overrides = holiday_overrides or {}
    special_open_dates = observed_special_open_dates or frozenset()
    generated = _as_utc(generated_at or datetime.now(UTC))
    calendar_version = (
        f"pandas_market_calendars:{version('pandas_market_calendars')}:{calendar_name}"
    )

    rows: list[MaterializedExchangeSession] = []
    current = start_date
    while current <= end_date:
        override = overrides.get(current.year)
        schedule_row = schedule_by_date.get(current)
        is_calendar_override_closed = (
            override is not None and current in override.closed_dates
        )
        is_observed_special_open = current in special_open_dates and (
            schedule_row is None or is_calendar_override_closed
        )
        is_forced_closed = is_calendar_override_closed and not is_observed_special_open
        if schedule_row is None or is_forced_closed or is_observed_special_open:
            if is_observed_special_open:
                rows.append(
                    MaterializedExchangeSession(
                        exchange=canonical_exchange,
                        session_date=current,
                        is_trading_day=True,
                        market_open_utc=_local_time_as_utc(
                            current,
                            config.open_time,
                            config.timezone,
                        ),
                        market_close_utc=_local_time_as_utc(
                            current,
                            config.close_time,
                            config.timezone,
                        ),
                        is_early_close=False,
                        source_url="stored_ohlcv_observation",
                        calendar_version=f"{calendar_version}:observed_special_open",
                        validation_status="valid_observed_special_session",
                        generated_at=generated,
                    )
                )
                current += timedelta(days=1)
                continue
            rows.append(
                MaterializedExchangeSession(
                    exchange=canonical_exchange,
                    session_date=current,
                    is_trading_day=False,
                    market_open_utc=None,
                    market_close_utc=None,
                    is_early_close=False,
                    source_url=PANDAS_MARKET_CALENDARS_SOURCE_URL,
                    calendar_version=calendar_version,
                    validation_status="valid",
                    generated_at=generated,
                )
            )
            current += timedelta(days=1)
            continue

        market_open = _timestamp_as_utc(schedule_row["market_open"])
        market_close = _timestamp_as_utc(schedule_row["market_close"])
        forced_early_close = override is not None and current in override.early_close_dates
        if forced_early_close and config.early_close_time is not None:
            market_close = _local_time_as_utc(
                current,
                config.early_close_time,
                config.timezone,
            )
        local_close = market_close.astimezone(ZoneInfo(config.timezone)).timetz().replace(
            tzinfo=None
        )
        rows.append(
            MaterializedExchangeSession(
                exchange=canonical_exchange,
                session_date=current,
                is_trading_day=True,
                market_open_utc=market_open,
                market_close_utc=market_close,
                is_early_close=local_close < config.close_time,
                source_url=PANDAS_MARKET_CALENDARS_SOURCE_URL,
                calendar_version=calendar_version,
                validation_status="valid",
                generated_at=generated,
            )
        )
        current += timedelta(days=1)
    return rows


def validate_materialized_exchange_sessions(
    sessions: Sequence[MaterializedExchangeSession | Mapping[str, Any]],
    start_date: date,
    end_date: date,
    *,
    minimum_open_days_per_full_year: int = 220,
    maximum_open_days_per_full_year: int = 260,
) -> ExchangeSessionValidation:
    errors: list[str] = []
    normalized = [_session_mapping(row) for row in sessions]
    expected_row_count = (end_date - start_date).days + 1
    dates = [row["session_date"] for row in normalized]
    if len(normalized) != expected_row_count:
        errors.append(
            f"incomplete_date_range:{len(normalized)}!={expected_row_count}"
        )
    if len(set(dates)) != len(dates):
        errors.append("duplicate_session_dates")
    if dates and (min(dates) != start_date or max(dates) != end_date):
        errors.append("session_date_bounds_mismatch")

    open_days_by_year: dict[int, int] = {}
    invalid_open_rows = 0
    invalid_closed_rows = 0
    invalid_early_rows = 0
    for row in normalized:
        session_date = row["session_date"]
        if row["is_trading_day"]:
            open_days_by_year[session_date.year] = (
                open_days_by_year.get(session_date.year, 0) + 1
            )
            market_open = row["market_open_utc"]
            market_close = row["market_close_utc"]
            if (
                market_open is None
                or market_close is None
                or market_open.tzinfo is None
                or market_close.tzinfo is None
                or market_open >= market_close
            ):
                invalid_open_rows += 1
        elif row["market_open_utc"] is not None or row["market_close_utc"] is not None:
            invalid_closed_rows += 1
        if row["is_early_close"] and not row["is_trading_day"]:
            invalid_early_rows += 1
    if invalid_open_rows:
        errors.append(f"invalid_open_rows:{invalid_open_rows}")
    if invalid_closed_rows:
        errors.append(f"invalid_closed_rows:{invalid_closed_rows}")
    if invalid_early_rows:
        errors.append(f"invalid_early_close_rows:{invalid_early_rows}")

    for year in range(start_date.year, end_date.year + 1):
        if start_date <= date(year, 1, 1) and end_date >= date(year, 12, 31):
            count = open_days_by_year.get(year, 0)
            if not minimum_open_days_per_full_year <= count <= maximum_open_days_per_full_year:
                errors.append(
                    "open_day_count_out_of_range:"
                    f"{year}:{count}:"
                    f"{minimum_open_days_per_full_year}-{maximum_open_days_per_full_year}"
                )

    trading_days = sum(bool(row["is_trading_day"]) for row in normalized)
    early_closes = sum(bool(row["is_early_close"]) for row in normalized)
    return ExchangeSessionValidation(
        accepted=not errors,
        row_count=len(normalized),
        trading_day_count=trading_days,
        closed_day_count=len(normalized) - trading_days,
        early_close_count=early_closes,
        open_days_by_year=open_days_by_year,
        errors=tuple(errors),
    )


def shadow_compare_exchange_sessions(
    sessions: Sequence[MaterializedExchangeSession | Mapping[str, Any]],
    holiday_records: Mapping[int, ExchangeHolidays],
) -> ExchangeSessionShadowComparison:
    materialized_by_year: dict[int, set[date]] = {}
    for row in (_session_mapping(item) for item in sessions):
        if row["is_trading_day"]:
            materialized_by_year.setdefault(row["session_date"].year, set()).add(
                row["session_date"]
            )
    legacy_only: set[date] = set()
    materialized_only: set[date] = set()
    compared_years: list[int] = []
    for year, holidays in sorted(holiday_records.items()):
        if year not in materialized_by_year:
            continue
        compared_years.append(year)
        legacy = set(
            expected_trading_dates(
                _session_exchange(sessions),
                date(year, 1, 1),
                date(year, 12, 31),
                holidays,
            )
        )
        materialized = materialized_by_year.get(year, set())
        legacy_only.update(legacy - materialized)
        materialized_only.update(materialized - legacy)
    return ExchangeSessionShadowComparison(
        legacy_only_dates=tuple(sorted(legacy_only)),
        materialized_only_dates=tuple(sorted(materialized_only)),
        compared_years=tuple(compared_years),
    )


def resolve_expected_session_dates(
    store: ExchangeSessionStore,
    exchange: str,
    start_date: date,
    end_date: date,
    *,
    use_materialized_sessions: bool,
) -> ExpectedSessionResolution:
    canonical_exchange = canonical_equity_exchange(exchange)
    if use_materialized_sessions:
        rows = store.exchange_sessions(canonical_exchange, start_date, end_date)
        expected_count = (end_date - start_date).days + 1
        if len(rows) != expected_count or any(
            not str(row.get("validation_status") or "").startswith("valid")
            for row in rows
        ):
            raise ExchangeSessionError(
                f"Materialized {canonical_exchange} sessions are incomplete or invalid for "
                f"{start_date} through {end_date}."
            )
        return ExpectedSessionResolution(
            dates=tuple(
                row["session_date"] for row in rows if row.get("is_trading_day")
            ),
            source="materialized_exchange_sessions",
        )

    holidays = _stored_holidays(store, canonical_exchange, start_date, end_date)
    return ExpectedSessionResolution(
        dates=tuple(
            expected_trading_dates(
                canonical_exchange,
                start_date,
                end_date,
                holidays=holidays,
            )
        ),
        source=(
            "stored_exchange_holidays" if holidays is not None else "weekdays_only_fallback"
        ),
    )


def expected_dates_for_instrument(
    expected_dates: Sequence[date],
    *,
    coverage_start: date,
    first_trade_date: date | None,
) -> tuple[date, ...]:
    instrument_start = max(coverage_start, first_trade_date) if first_trade_date else coverage_start
    return tuple(value for value in expected_dates if value >= instrument_start)


def classify_exchange_session(
    session: MaterializedExchangeSession | Mapping[str, Any],
    *,
    at: datetime,
    provider_grace_minutes: int,
) -> str:
    row = _session_mapping(session)
    if not row["is_trading_day"]:
        return "weekend" if row["session_date"].weekday() >= 5 else "holiday"
    market_close = row["market_close_utc"]
    if market_close is None:
        return "calendar_invalid"
    current = _as_utc(at)
    if current < market_close:
        return "market_not_closed"
    if current < market_close + timedelta(minutes=provider_grace_minutes):
        return "provider_pending"
    return "expected"


def _stored_holidays(
    store: ExchangeSessionStore,
    exchange: str,
    start_date: date,
    end_date: date,
) -> ExchangeHolidays | None:
    closed_dates: set[date] = set()
    early_close_dates: set[date] = set()
    source_url = ""
    for year in range(start_date.year, end_date.year + 1):
        row = store.exchange_holidays(exchange, year)
        if row is None:
            return None
        source_url = str(row.get("source_url") or source_url)
        closed_dates.update(date.fromisoformat(value) for value in row.get("closed_dates", []))
        early_close_dates.update(
            date.fromisoformat(value) for value in row.get("early_close_dates", [])
        )
    return ExchangeHolidays(
        closed_dates=frozenset(closed_dates),
        early_close_dates=frozenset(early_close_dates),
        source_url=source_url,
    )


def _session_mapping(
    session: MaterializedExchangeSession | Mapping[str, Any],
) -> dict[str, Any]:
    return session.as_row() if isinstance(session, MaterializedExchangeSession) else dict(session)


def _session_exchange(
    sessions: Sequence[MaterializedExchangeSession | Mapping[str, Any]],
) -> str:
    if not sessions:
        raise ValueError("sessions must not be empty")
    return str(_session_mapping(sessions[0])["exchange"])


def _timestamp_as_utc(value: Any) -> datetime:
    timestamp = value.to_pydatetime() if hasattr(value, "to_pydatetime") else value
    if not isinstance(timestamp, datetime):
        raise TypeError(f"Expected datetime-like calendar value, received {type(value)!r}")
    return _as_utc(timestamp)


def _local_time_as_utc(value: date, local_time: time, timezone: str) -> datetime:
    return datetime.combine(value, local_time, tzinfo=ZoneInfo(timezone)).astimezone(UTC)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
