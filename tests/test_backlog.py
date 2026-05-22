from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from trade_research.backlog import expected_hourly_candle_windows
from trade_research.market_calendar import ExchangeHolidays


def test_expected_hourly_candle_windows_follow_nse_open_alignment() -> None:
    windows = expected_hourly_candle_windows(
        exchange="NSE",
        holidays=ExchangeHolidays(
            closed_dates=frozenset(),
            early_close_dates=frozenset(),
            source_url="test",
        ),
        scan_days=1,
        min_candle_lag=timedelta(minutes=20),
        at=datetime(2026, 5, 22, 12, 0, tzinfo=ZoneInfo("Asia/Kolkata")),
    )

    starts = [
        item.window_start.astimezone(ZoneInfo("Asia/Kolkata")).strftime("%H:%M")
        for item in windows
    ]

    assert starts == ["09:15", "10:15"]


def test_expected_hourly_candle_windows_skip_exchange_holidays() -> None:
    local_date = datetime(2026, 5, 22).date()
    windows = expected_hourly_candle_windows(
        exchange="TSX",
        holidays=ExchangeHolidays(
            closed_dates=frozenset({local_date}),
            early_close_dates=frozenset(),
            source_url="test",
        ),
        scan_days=1,
        min_candle_lag=timedelta(minutes=20),
        at=datetime(2026, 5, 22, 15, 0, tzinfo=ZoneInfo("America/Toronto")),
    )

    assert windows == []
