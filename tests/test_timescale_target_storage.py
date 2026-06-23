from datetime import date

import pandas as pd
import pytest

from trade_research.storage.timescale import TimescaleStore
from trade_research.targets import DailyForwardTargetBuilder


def test_daily_target_rows_normalize_for_storage() -> None:
    source = pd.DataFrame(
        [
            {
                "Date": date(2025, 1, 1),
                "Open": 99.5,
                "High": 101.0,
                "Low": 99.0,
                "Close": 100.0,
                "Volume": 100_000,
                "OpenInterest": None,
                "InstrumentKey": "NSE_EQ|TEST",
                "Symbol": "test",
                "Source": "upstox",
            },
            {
                "Date": date(2025, 1, 2),
                "Open": 100.5,
                "High": 102.0,
                "Low": 100.0,
                "Close": 101.0,
                "Volume": 100_000,
                "OpenInterest": None,
                "InstrumentKey": "NSE_EQ|TEST",
                "Symbol": "test",
                "Source": "upstox",
            },
        ]
    )
    targets = DailyForwardTargetBuilder().build(source)

    rows = TimescaleStore._daily_target_rows(targets)

    assert len(rows) == 2
    row = rows[0]
    assert row["instrument_key"] == "NSE_EQ|TEST"
    assert row["date"] == date(2025, 1, 1)
    assert row["symbol"] == "TEST"
    assert row["exchange"] == "NSE"
    assert row["forward_ret_1d"] == pytest.approx(0.01)
    assert row["forward_ret_5d"] is None
    assert row["top_quantile_forward_return_20d"] is None
    assert row["quality_status"] == "warning"
