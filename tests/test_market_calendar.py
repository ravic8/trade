from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

import trade_research.market_calendar as market_calendar_module
from trade_research.market_calendar import (
    NSE_FALLBACK_CLOSED_DATES,
    ExchangeHolidays,
    MarketCalendarError,
    build_us_exchange_holidays,
    expected_trading_dates,
    fetch_exchange_holidays,
    fetch_nse_holidays,
    parse_tsx_holidays,
    session_decision,
    validated_exchange_calendar_years,
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


def test_expected_trading_dates_skip_weekends_and_holidays() -> None:
    holidays = ExchangeHolidays(
        closed_dates=frozenset({datetime(2026, 1, 5).date()}),
        early_close_dates=frozenset(),
        source_url="test",
    )

    dates = expected_trading_dates(
        "NSE",
        datetime(2026, 1, 1).date(),
        datetime(2026, 1, 6).date(),
        holidays=holidays,
    )

    assert dates == [
        datetime(2026, 1, 1).date(),
        datetime(2026, 1, 2).date(),
        datetime(2026, 1, 6).date(),
    ]


def test_us_exchange_holidays_include_observed_independence_day() -> None:
    holidays = build_us_exchange_holidays(2026)

    assert datetime(2026, 1, 1).date() in holidays.closed_dates
    assert datetime(2026, 4, 3).date() in holidays.closed_dates
    assert datetime(2026, 7, 3).date() in holidays.closed_dates
    assert datetime(2026, 11, 27).date() in holidays.early_close_dates
    assert datetime(2026, 12, 24).date() in holidays.early_close_dates


def test_us_exchange_holidays_include_prior_year_new_year_observed_day() -> None:
    holidays = build_us_exchange_holidays(2021)

    assert datetime(2021, 12, 31).date() in holidays.closed_dates


def test_expected_trading_dates_support_us_exchange_code() -> None:
    dates = expected_trading_dates(
        "US",
        datetime(2026, 7, 1).date(),
        datetime(2026, 7, 3).date(),
        holidays=build_us_exchange_holidays(2026),
    )

    assert dates == [
        datetime(2026, 7, 1).date(),
        datetime(2026, 7, 2).date(),
    ]


def test_fetch_exchange_holidays_supports_ca_alias(monkeypatch) -> None:
    def fake_tsx_holidays(year: int) -> ExchangeHolidays:
        assert year == 2026
        return ExchangeHolidays(
            closed_dates=frozenset({datetime(2026, 7, 1).date()}),
            early_close_dates=frozenset(),
            source_url="tmx",
        )

    monkeypatch.setattr(
        "trade_research.market_calendar.fetch_tsx_holidays",
        fake_tsx_holidays,
    )

    holidays = fetch_exchange_holidays("CA", 2026)

    assert datetime(2026, 7, 1).date() in holidays.closed_dates
    assert holidays.source_url == "tmx"


def test_nse_fallback_holidays_include_2025_trading_closures() -> None:
    holidays = NSE_FALLBACK_CLOSED_DATES[2025]
    assert datetime(2025, 8, 15).date() in holidays
    assert datetime(2025, 8, 27).date() in holidays
    assert datetime(2025, 10, 2).date() in holidays
    assert datetime(2025, 12, 25).date() in holidays


def test_exchange_calendar_range_rejects_year_one() -> None:
    with pytest.raises(ValueError, match="earlier than 1990"):
        validated_exchange_calendar_years(
            date(1, 1, 1),
            date(2026, 1, 1),
            reference_date=date(2026, 7, 17),
        )


def test_exchange_calendar_range_rejects_excessive_span() -> None:
    with pytest.raises(ValueError, match="at most 21 calendar years"):
        validated_exchange_calendar_years(
            date(1990, 1, 1),
            date(2011, 1, 1),
            reference_date=date(2026, 7, 17),
        )


def test_nse_holiday_fetch_rejects_an_unavailable_year(monkeypatch) -> None:
    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"CM": [{"tradingDate": "26-Jan-2026"}]}

    class FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            return None

        def get(self, url: str) -> FakeResponse:
            return FakeResponse()

    monkeypatch.setattr(market_calendar_module.httpx, "Client", FakeClient)

    with pytest.raises(MarketCalendarError, match="unavailable for 2024"):
        fetch_nse_holidays(2024)


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
        assert "NSE, TSX, CA, or US" in str(exc)
    else:
        raise AssertionError("expected ValueError")
