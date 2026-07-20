from datetime import UTC, date, datetime

import pandas as pd
import pytest

from trade_research.targets import (
    DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
    DailyOpportunityTargetBuilder,
    audit_daily_opportunity_targets,
)


def _row(
    session_date: date,
    *,
    open_price: float,
    high: float,
    low: float,
    close: float,
    instrument_key: str = "NSE_EQ|AAA",
    symbol: str = "AAA",
    source: str = "yfinance",
) -> dict:
    return {
        "Date": session_date,
        "Open": open_price,
        "High": high,
        "Low": low,
        "Close": close,
        "Volume": 100_000,
        "OpenInterest": 0,
        "InstrumentKey": instrument_key,
        "Symbol": symbol,
        "Exchange": "NSE",
        "Source": source,
    }


def test_opportunity_targets_match_pdf_definitions() -> None:
    frame = pd.DataFrame(
        [
            _row(date(2026, 1, 2), open_price=98, high=102, low=97, close=100),
            _row(date(2026, 1, 5), open_price=105, high=112, low=101, close=108),
        ]
    )
    computed_at = datetime(2026, 1, 6, tzinfo=UTC)

    result = DailyOpportunityTargetBuilder(computed_at=computed_at).build(frame)
    row = result.iloc[1]

    assert row["target_version"] == DAILY_OPPORTUNITY_TARGET_VERSION_V1_0
    assert row["previous_close"] == 100
    assert row["session_return"] == pytest.approx((108 - 105) / 105)
    assert row["gap"] == pytest.approx((105 - 100) / 105)
    assert row["true_return"] == pytest.approx((108 - 100) / 105)
    assert row["upside"] == pytest.approx((112 - 105) / 105)
    assert row["downside"] == pytest.approx((105 - 101) / 105)
    assert row["giveback"] == pytest.approx((112 - 108) / 105)
    assert row["recovery"] == pytest.approx((108 - 101) / 105)
    assert row["session_range"] == pytest.approx((112 - 101) / 105)
    assert row["true_upside"] == pytest.approx((112 - 100) / 105)
    assert row["true_downside"] == pytest.approx((105 - 101) / 105)
    assert row["true_range"] == pytest.approx((112 - 100 + 105 - 101) / 105)
    assert row["quality_status"] == "passed"


def test_opportunity_targets_use_previous_trading_row_not_calendar_day() -> None:
    frame = pd.DataFrame(
        [
            _row(date(2026, 1, 2), open_price=98, high=102, low=97, close=100),
            _row(date(2026, 1, 5), open_price=101, high=104, low=99, close=103),
        ]
    )

    result = DailyOpportunityTargetBuilder().build(frame)

    assert result.iloc[1]["previous_close"] == 100
    assert result.iloc[1]["gap"] == pytest.approx(1 / 101)


def test_opportunity_targets_do_not_cross_sources_or_instruments() -> None:
    frame = pd.DataFrame(
        [
            _row(date(2026, 1, 2), open_price=98, high=102, low=97, close=100),
            _row(
                date(2026, 1, 3),
                open_price=198,
                high=202,
                low=197,
                close=200,
                source="upstox",
            ),
            _row(
                date(2026, 1, 5),
                open_price=50,
                high=52,
                low=49,
                close=51,
                instrument_key="NSE_EQ|BBB",
                symbol="BBB",
            ),
        ]
    )

    result = DailyOpportunityTargetBuilder().build(frame)

    assert result["previous_close"].isna().all()
    assert set(result["quality_status"]) == {"warning"}


def test_opportunity_true_range_is_additive_project_definition() -> None:
    frame = pd.DataFrame(
        [
            _row(date(2026, 1, 2), open_price=100, high=101, low=99, close=100),
            _row(date(2026, 1, 5), open_price=95, high=103, low=92, close=100),
        ]
    )

    row = DailyOpportunityTargetBuilder().build(frame).iloc[1]

    assert row["true_upside"] == pytest.approx((103 - 95) / 95)
    assert row["true_downside"] == pytest.approx((100 - 92) / 95)
    assert row["true_range"] == pytest.approx(16 / 95)


def test_opportunity_target_audit_reports_first_session_warning() -> None:
    frame = pd.DataFrame(
        [
            _row(date(2026, 1, 2), open_price=98, high=102, low=97, close=100),
            _row(date(2026, 1, 5), open_price=101, high=104, low=99, close=103),
        ]
    )
    targets = DailyOpportunityTargetBuilder().build(frame)

    audit, summary = audit_daily_opportunity_targets(targets)

    assert summary.row_count == 2
    assert summary.warning_rows == 1
    assert summary.passed_rows == 1
    assert summary.duplicate_key_count == 0
    assert set(audit["target"]) == {
        "session_return",
        "gap",
        "true_return",
        "upside",
        "downside",
        "giveback",
        "recovery",
        "session_range",
        "true_upside",
        "true_downside",
        "true_range",
    }
