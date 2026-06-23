from datetime import date, timedelta

import pandas as pd
import pytest

from trade_research.targets import (
    DAILY_FORWARD_TARGET_VERSION_V1_0,
    DailyForwardTargetBuilder,
    audit_daily_forward_targets,
)


def _daily_rows(symbol: str, key: str, close_start: float = 100.0, days: int = 80) -> list[dict]:
    rows = []
    for offset in range(days):
        close = close_start + offset
        rows.append(
            {
                "Date": date(2025, 1, 1) + timedelta(days=offset),
                "Open": close - 0.5,
                "High": close + 1.0,
                "Low": close - 1.0,
                "Close": close,
                "Volume": 100_000 + offset,
                "OpenInterest": 0,
                "InstrumentKey": key,
                "Symbol": symbol,
                "Source": "upstox",
            }
        )
    return rows


def test_daily_forward_targets_use_future_closes_only() -> None:
    frame = pd.DataFrame(_daily_rows("AAA", "NSE_EQ|AAA", days=70))

    targets = DailyForwardTargetBuilder().build(frame)
    row = targets[targets["date"].eq(date(2025, 1, 1))].iloc[0]

    assert row["target_version"] == DAILY_FORWARD_TARGET_VERSION_V1_0
    assert row["forward_ret_1d"] == pytest.approx(101 / 100 - 1)
    assert row["forward_ret_5d"] == pytest.approx(105 / 100 - 1)
    assert row["forward_ret_20d"] == pytest.approx(120 / 100 - 1)
    assert row["forward_ret_60d"] == pytest.approx(160 / 100 - 1)


def test_daily_forward_targets_warn_when_future_window_is_incomplete() -> None:
    frame = pd.DataFrame(_daily_rows("AAA", "NSE_EQ|AAA", days=3))

    targets = DailyForwardTargetBuilder().build(frame)

    assert targets.iloc[0]["quality_status"] == "warning"
    assert pd.isna(targets.iloc[0]["forward_ret_5d"])
    assert pd.isna(targets.iloc[-1]["forward_ret_1d"])


def test_daily_forward_targets_compute_universe_relative_and_top_quantile() -> None:
    rows = []
    rows.extend(_daily_rows("AAA", "NSE_EQ|AAA", close_start=100.0, days=25))
    rows.extend(_daily_rows("BBB", "NSE_EQ|BBB", close_start=200.0, days=25))
    frame = pd.DataFrame(rows)
    frame.loc[(frame["Symbol"] == "AAA") & (frame["Date"] == date(2025, 1, 21)), "Close"] = 150.0
    frame.loc[(frame["Symbol"] == "AAA") & (frame["Date"] == date(2025, 1, 21)), "Open"] = 149.5
    frame.loc[(frame["Symbol"] == "AAA") & (frame["Date"] == date(2025, 1, 21)), "High"] = 151.0
    frame.loc[(frame["Symbol"] == "AAA") & (frame["Date"] == date(2025, 1, 21)), "Low"] = 149.0

    targets = DailyForwardTargetBuilder(top_quantile=0.80).build(frame)
    first_day = targets[targets["date"].eq(date(2025, 1, 1))].sort_values("symbol")

    aaa = first_day[first_day["symbol"].eq("AAA")].iloc[0]
    bbb = first_day[first_day["symbol"].eq("BBB")].iloc[0]

    assert aaa["forward_ret_20d"] > bbb["forward_ret_20d"]
    assert aaa["forward_outperform_universe_20d"] > 0
    assert bbb["forward_outperform_universe_20d"] < 0
    assert bool(aaa["top_quantile_forward_return_20d"]) is True
    assert bool(bbb["top_quantile_forward_return_20d"]) is False


def test_daily_forward_target_audit_summary() -> None:
    frame = pd.DataFrame(_daily_rows("AAA", "NSE_EQ|AAA", days=3))
    targets = DailyForwardTargetBuilder().build(frame)

    audit, summary = audit_daily_forward_targets(targets, invalid_ohlcv_count=1)

    assert summary.dataset_name == "daily_v1_forward_returns"
    assert summary.target_version == DAILY_FORWARD_TARGET_VERSION_V1_0
    assert summary.row_count == 3
    assert summary.warning_rows == 3
    assert summary.invalid_ohlcv_count == 1
    assert set(audit["target"]) >= {"forward_ret_1d", "forward_ret_20d"}
