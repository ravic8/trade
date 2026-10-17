"""Completed NSE session helper without research artifact dependencies."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from trade_data.market_calendar import (
    EXCHANGE_CONFIGS,
    ExchangeHolidays,
    fetch_exchange_holidays,
)


@dataclass(frozen=True)
class LatestTradingDate:
    current_local_time: datetime
    latest_expected_trading_date: date
    reason: str
    calendar_source: str


def resolve_latest_expected_trading_date(
    at: datetime | None = None,
    holidays: ExchangeHolidays | None = None,
    *,
    holiday_loader: Callable[[str, int], ExchangeHolidays] | None = None,
) -> LatestTradingDate:
    config = EXCHANGE_CONFIGS["NSE"]
    now = at or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    local_time = now.astimezone(ZoneInfo(config.timezone))
    if holidays is None:
        try:
            holidays = (holiday_loader or fetch_exchange_holidays)("NSE", local_time.year)
            source = holidays.source_url
        except Exception as exc:
            holidays = ExchangeHolidays(frozenset(), frozenset(), config.holiday_source_url)
            source = f"calendar_unavailable:{exc};weekend_only_fallback"
    else:
        source = holidays.source_url

    candidate = local_time.date()
    market_close = datetime.combine(candidate, config.close_time, tzinfo=local_time.tzinfo)
    if not _is_trading_day(candidate, holidays) or local_time <= market_close:
        candidate -= timedelta(days=1)

    while not _is_trading_day(candidate, holidays):
        candidate -= timedelta(days=1)

    if local_time.date() == candidate:
        reason = "current trading day is completed"
    elif local_time.weekday() >= 5:
        reason = "today is weekend; using previous NSE trading session"
    elif local_time.date() in holidays.closed_dates:
        reason = "today is NSE holiday; using previous NSE trading session"
    else:
        reason = "current NSE trading session is not closed; using previous completed session"
    return LatestTradingDate(
        current_local_time=local_time,
        latest_expected_trading_date=candidate,
        reason=reason,
        calendar_source=source,
    )


def _is_trading_day(value: date, holidays: ExchangeHolidays) -> bool:
    return value.weekday() < 5 and value not in holidays.closed_dates
