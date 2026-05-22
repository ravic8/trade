from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

from trade_research.market_calendar import EXCHANGE_CONFIGS, ExchangeHolidays


@dataclass(frozen=True)
class HourlyCandleWindow:
    exchange: str
    window_start: datetime
    window_end: datetime


def expected_hourly_candle_windows(
    exchange: str,
    holidays: ExchangeHolidays,
    scan_days: int,
    min_candle_lag: timedelta,
    at: datetime | None = None,
) -> list[HourlyCandleWindow]:
    """Return completed exchange-aligned hourly candle windows in UTC."""
    config = EXCHANGE_CONFIGS[exchange.upper()]
    current = at or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    local_now = current.astimezone(ZoneInfo(config.timezone))
    first_day = local_now.date() - timedelta(days=max(scan_days - 1, 0))

    windows: list[HourlyCandleWindow] = []
    for day_offset in range(scan_days):
        local_day = first_day + timedelta(days=day_offset)
        if local_day.weekday() >= 5 or local_day in holidays.closed_dates:
            continue

        close_time = _close_time(config.close_time, config.early_close_time, local_day, holidays)
        cursor = datetime.combine(local_day, config.open_time, tzinfo=local_now.tzinfo)
        market_close = datetime.combine(local_day, close_time, tzinfo=local_now.tzinfo)
        while cursor < market_close:
            window_end = min(cursor + timedelta(hours=1), market_close)
            if window_end + min_candle_lag <= local_now:
                windows.append(
                    HourlyCandleWindow(
                        exchange=config.exchange,
                        window_start=cursor.astimezone(UTC),
                        window_end=window_end.astimezone(UTC),
                    )
                )
            cursor += timedelta(hours=1)
    return windows


def _close_time(
    normal_close: time,
    early_close: time | None,
    local_day,
    holidays: ExchangeHolidays,
) -> time:
    if early_close and local_day in holidays.early_close_dates:
        return early_close
    return normal_close
