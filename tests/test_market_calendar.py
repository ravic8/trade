from datetime import datetime
from zoneinfo import ZoneInfo

from trade_research.market_calendar import (
    ExchangeHolidays,
    fetch_exchange_holidays,
    parse_tsx_holidays,
    session_decision,
)


def test_parse_tsx_official_calendar_section_marks_christmas_eve_early_close() -> None:
    html = """
    <h2>2026 Stock Market Holidays - Stock Markets Closed</h2>
    <h3>Canadian Holidays</h3>
    * New Year's Day - Thursday, January 1, 2026
    * Christmas Eve - Thursday, December 24, 2026*
    * Christmas Day - Friday, December 25, 2026
    * Closing at 1:00 PM (TSX/TSXV)
    <h3>U.S. Holidays**</h3>
    * Memorial Day - Monday, May 25, 2026
    <h2>2025 Stock Market Holidays - Stock Markets Closed</h2>
    """

    holidays = parse_tsx_holidays(html, 2026)

    assert datetime(2026, 1, 1).date() in holidays.closed_dates
    assert datetime(2026, 12, 25).date() in holidays.closed_dates
    assert datetime(2026, 12, 24).date() in holidays.early_close_dates
    assert datetime(2026, 5, 25).date() not in holidays.closed_dates


def test_session_decision_skips_exchange_holiday(monkeypatch) -> None:
    def fake_holidays(exchange: str, year: int) -> ExchangeHolidays:
        assert exchange == "NSE"
        assert year == 2026
        return ExchangeHolidays(
            closed_dates=frozenset({datetime(2026, 1, 26).date()}),
            early_close_dates=frozenset(),
            source_url="test",
        )

    monkeypatch.setattr(
        "trade_research.market_calendar.fetch_exchange_holidays",
        fake_holidays,
    )
    decision = session_decision(
        "NSE",
        datetime(2026, 1, 26, 11, 0, tzinfo=ZoneInfo("Asia/Kolkata")),
    )

    assert decision.should_fetch is False
    assert decision.reason == "exchange_holiday"


def test_session_decision_allows_post_close_hourly_fetch(monkeypatch) -> None:
    def fake_holidays(exchange: str, year: int) -> ExchangeHolidays:
        return ExchangeHolidays(
            closed_dates=frozenset(),
            early_close_dates=frozenset(),
            source_url="test",
        )

    monkeypatch.setattr(
        "trade_research.market_calendar.fetch_exchange_holidays",
        fake_holidays,
    )
    decision = session_decision(
        "TSX",
        datetime(2026, 5, 21, 16, 45, tzinfo=ZoneInfo("America/Toronto")),
    )

    assert decision.should_fetch is True
    assert decision.market_close is not None
    assert decision.market_close.hour == 16


def test_fetch_exchange_holidays_rejects_unknown_exchange() -> None:
    try:
        fetch_exchange_holidays("ABC", 2026)
    except ValueError as exc:
        assert "NSE or TSX" in str(exc)
    else:
        raise AssertionError("expected ValueError")
