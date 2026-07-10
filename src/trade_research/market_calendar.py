from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import httpx

NSE_HOLIDAY_URL = "https://www.nseindia.com/api/holiday-master?type=trading"
NSE_HOLIDAYS_PAGE_URL = "https://www.nseindia.com/resources/exchange-communication-holidays/"
NSE_TIMINGS_URL = "https://www.nseindia.com/static/market-data/market-timings"
TMX_CALENDAR_URL = "https://www.tsx.com/en/trading/calendars-and-trading-hours/calendar"

NSE_FALLBACK_CLOSED_DATES = {
    2025: frozenset(
        {
            date(2025, 2, 26),
            date(2025, 3, 14),
            date(2025, 3, 31),
            date(2025, 4, 10),
            date(2025, 4, 14),
            date(2025, 4, 18),
            date(2025, 5, 1),
            date(2025, 8, 15),
            date(2025, 8, 27),
            date(2025, 10, 2),
            date(2025, 10, 21),
            date(2025, 10, 22),
            date(2025, 11, 5),
            date(2025, 12, 25),
        }
    ),
}


@dataclass(frozen=True)
class ExchangeSessionConfig:
    exchange: str
    timezone: str
    open_time: time
    close_time: time
    early_close_time: time | None
    holiday_source_url: str
    timings_source_url: str
    post_close_fetch_window: timedelta = timedelta(minutes=90)


@dataclass(frozen=True)
class ExchangeHolidays:
    closed_dates: frozenset[date]
    early_close_dates: frozenset[date]
    source_url: str


@dataclass(frozen=True)
class SessionDecision:
    exchange: str
    is_trading_day: bool
    should_fetch: bool
    reason: str
    local_time: datetime
    market_open: datetime | None
    market_close: datetime | None
    source_url: str


EXCHANGE_CONFIGS = {
    "NSE": ExchangeSessionConfig(
        exchange="NSE",
        timezone="Asia/Kolkata",
        open_time=time(9, 15),
        close_time=time(15, 30),
        early_close_time=None,
        holiday_source_url=NSE_HOLIDAY_URL,
        timings_source_url=NSE_TIMINGS_URL,
    ),
    "TSX": ExchangeSessionConfig(
        exchange="TSX",
        timezone="America/Toronto",
        open_time=time(9, 30),
        close_time=time(16, 0),
        early_close_time=time(13, 0),
        holiday_source_url=TMX_CALENDAR_URL,
        timings_source_url="https://www.tsx.com/en/trading/calendars-and-trading-hours",
    ),
}


class MarketCalendarError(RuntimeError):
    pass


def expected_trading_dates(
    exchange: str,
    start: date,
    end: date,
    holidays: ExchangeHolidays | None = None,
) -> list[date]:
    if start > end:
        raise ValueError("start must be on or before end")
    _config(exchange)
    closed_dates = holidays.closed_dates if holidays else frozenset()
    current = start
    dates: list[date] = []
    while current <= end:
        if current.weekday() < 5 and current not in closed_dates:
            dates.append(current)
        current += timedelta(days=1)
    return dates


def session_decision(
    exchange: str,
    at: datetime | None = None,
    holidays: ExchangeHolidays | None = None,
) -> SessionDecision:
    config = _config(exchange)
    current = at or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    local_time = current.astimezone(ZoneInfo(config.timezone))
    if holidays is None:
        try:
            holidays = fetch_exchange_holidays(config.exchange, local_time.year)
        except Exception as exc:
            return _closed_decision(config, local_time, f"calendar_unavailable:{exc}")
    local_date = local_time.date()

    if local_time.weekday() >= 5:
        return _closed_decision(config, local_time, "weekend")

    if local_date in holidays.closed_dates:
        return _closed_decision(config, local_time, "exchange_holiday")

    close_time = (
        config.early_close_time
        if local_date in holidays.early_close_dates and config.early_close_time
        else config.close_time
    )
    market_open = datetime.combine(local_date, config.open_time, tzinfo=local_time.tzinfo)
    market_close = datetime.combine(local_date, close_time, tzinfo=local_time.tzinfo)
    fetch_deadline = market_close + config.post_close_fetch_window
    should_fetch = market_open <= local_time <= fetch_deadline
    reason = "market_session" if should_fetch else "outside_market_session"
    return SessionDecision(
        exchange=config.exchange,
        is_trading_day=True,
        should_fetch=should_fetch,
        reason=reason,
        local_time=local_time,
        market_open=market_open,
        market_close=market_close,
        source_url=holidays.source_url,
    )


def fetch_exchange_holidays(exchange: str, year: int) -> ExchangeHolidays:
    normalized = exchange.upper()
    if normalized == "NSE":
        return fetch_nse_holidays(year)
    if normalized == "TSX":
        return fetch_tsx_holidays(year)
    raise ValueError("exchange must be NSE or TSX")


def fetch_nse_holidays(year: int) -> ExchangeHolidays:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Referer": NSE_HOLIDAYS_PAGE_URL,
    }
    with httpx.Client(timeout=30, headers=headers, follow_redirects=True) as client:
        client.get(NSE_HOLIDAYS_PAGE_URL)
        response = client.get(NSE_HOLIDAY_URL)
        response.raise_for_status()

    payload = response.json()
    rows = payload.get("CM") or payload.get("CBM") or []
    if not rows:
        rows = [row for value in payload.values() if isinstance(value, list) for row in value]

    closed_dates = {
        parsed
        for row in rows
        if (parsed := _parse_holiday_date(str(row.get("tradingDate", "")))) and parsed.year == year
    }
    if not closed_dates:
        closed_dates = set(NSE_FALLBACK_CLOSED_DATES.get(year, frozenset()))
    return ExchangeHolidays(
        closed_dates=frozenset(closed_dates),
        early_close_dates=frozenset(),
        source_url=NSE_HOLIDAY_URL,
    )


def fetch_tsx_holidays(year: int) -> ExchangeHolidays:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/html,*/*"}
    with httpx.Client(timeout=30, headers=headers, follow_redirects=True) as client:
        response = client.get(TMX_CALENDAR_URL)
        response.raise_for_status()

    return parse_tsx_holidays(response.text, year)


def parse_tsx_holidays(html: str, year: int) -> ExchangeHolidays:
    year_marker = f"{year} Stock Market Holidays"
    next_year_marker = f"{year - 1} Stock Market Holidays"
    start = html.find(year_marker)
    if start == -1:
        raise MarketCalendarError(f"Could not find TSX holiday section for {year}")

    end = html.find(next_year_marker, start + len(year_marker))
    section = html[start : end if end != -1 else len(html)]
    us_holidays_start = section.find("U.S. Holidays")
    canadian_section = section[:us_holidays_start] if us_holidays_start != -1 else section

    closed_dates: set[date] = set()
    early_close_dates: set[date] = set()
    pattern = re.compile(
        r"(?P<name>[A-Za-z'’.\s]+?)\s*-\s*"
        r"(?P<weekday>Monday|Tuesday|Wednesday|Thursday|Friday),\s*"
        r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})(?P<early>\*)?",
    )
    for match in pattern.finditer(canadian_section):
        parsed = _parse_month_date(
            match.group("month"),
            int(match.group("day")),
            int(match.group("year")),
        )
        if parsed.year != year:
            continue
        name = match.group("name").strip().lower()
        is_early_close = bool(match.group("early")) or "christmas eve" in name
        if is_early_close:
            early_close_dates.add(parsed)
        else:
            closed_dates.add(parsed)

    return ExchangeHolidays(
        closed_dates=frozenset(closed_dates),
        early_close_dates=frozenset(early_close_dates),
        source_url=TMX_CALENDAR_URL,
    )


def _closed_decision(
    config: ExchangeSessionConfig,
    local_time: datetime,
    reason: str,
) -> SessionDecision:
    return SessionDecision(
        exchange=config.exchange,
        is_trading_day=False,
        should_fetch=False,
        reason=reason,
        local_time=local_time,
        market_open=None,
        market_close=None,
        source_url=config.holiday_source_url,
    )


def _config(exchange: str) -> ExchangeSessionConfig:
    try:
        return EXCHANGE_CONFIGS[exchange.upper()]
    except KeyError as exc:
        raise ValueError("exchange must be NSE or TSX") from exc


def _parse_holiday_date(value: str) -> date | None:
    stripped = value.strip()
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(stripped, fmt).date()
        except ValueError:
            continue
    return None


def _parse_month_date(month: str, day: int, year: int) -> date:
    return datetime.strptime(f"{month} {day} {year}", "%B %d %Y").date()
