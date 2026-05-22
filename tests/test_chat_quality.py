from datetime import UTC, datetime
from types import SimpleNamespace

from trade_research.chat.quality import evaluate_quality_badge


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        chat_quality_nse_complete_threshold=0.95,
        chat_quality_tsx_complete_threshold=0.90,
        chat_stale_intervals_threshold=2,
    )


def test_exchange_close_does_not_age_complete_final_candle_to_stale() -> None:
    badge, warnings = evaluate_quality_badge(
        _settings(),
        exchange="NSE",
        active_symbols=100,
        latest_candle_symbols=100,
        latest_candle_ts=datetime(2026, 5, 22, 9, 45, tzinfo=UTC),
        latest_expected_candle_ts=datetime(2026, 5, 22, 9, 45, tzinfo=UTC),
    )

    assert badge == "complete"
    assert warnings == []


def test_exchange_expected_candle_marks_old_market_window_stale() -> None:
    badge, warnings = evaluate_quality_badge(
        _settings(),
        exchange="NSE",
        active_symbols=100,
        latest_candle_symbols=100,
        latest_candle_ts=datetime(2026, 5, 22, 6, 45, tzinfo=UTC),
        latest_expected_candle_ts=datetime(2026, 5, 22, 9, 45, tzinfo=UTC),
    )

    assert badge == "stale"
    assert warnings == ["Latest candle appears stale relative to expected hourly cadence."]
