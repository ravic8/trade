from datetime import date

import pandas as pd

from trade_research.features import DailyTechnicalFeatureBuilder
from trade_research.storage.timescale import TimescaleStore


def test_daily_feature_rows_normalize_for_storage() -> None:
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
            }
        ]
    )
    features = DailyTechnicalFeatureBuilder().build(source)

    rows = TimescaleStore._daily_feature_rows(features)

    assert len(rows) == 1
    row = rows[0]
    assert row["instrument_key"] == "NSE_EQ|TEST"
    assert row["date"] == date(2025, 1, 1)
    assert row["symbol"] == "TEST"
    assert row["exchange"] == "NSE"
    assert row["open_interest"] is None
    assert row["ret_1d"] is None
    assert row["quality_status"] == "warning"
