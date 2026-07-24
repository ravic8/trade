from datetime import date

import pandas as pd

from trade_research.storage.timescale import TimescaleStore, _deduplicate_rows


def _duplicate_daily_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "InstrumentKey": "YF|AAA",
                "Symbol": "AAA",
                "Date": "2026-07-23",
                "Open": 10,
                "High": 12,
                "Low": 9,
                "Close": 11,
                "AdjClose": 10.5,
                "Volume": 100,
            },
            {
                "InstrumentKey": "YF|AAA",
                "Symbol": "AAA",
                "Date": "2026-07-23",
                "Open": 10,
                "High": 13,
                "Low": 9,
                "Close": 12,
                "AdjClose": 11.5,
                "Volume": 200,
            },
        ]
    )


def test_daily_ohlcv_rows_deduplicate_database_conflict_keys() -> None:
    rows = TimescaleStore._daily_ohlcv_rows(
        _duplicate_daily_frame(),
        exchange="NSE",
        source="yfinance",
    )

    assert len(rows) == 1
    assert rows[0]["date"] == date(2026, 7, 23)
    assert rows[0]["close"] == 12
    assert rows[0]["volume"] == 200


def test_daily_adjustment_rows_deduplicate_database_conflict_keys() -> None:
    rows = TimescaleStore._daily_price_adjustment_rows(
        _duplicate_daily_frame(),
        exchange="NSE",
        source="yfinance",
    )

    assert len(rows) == 1
    assert rows[0]["adjusted_close"] == 11.5


def test_snapshot_rows_deduplicate_composite_conflict_keys() -> None:
    rows = _deduplicate_rows(
        [
            {
                "snapshot_id": "snapshot-1",
                "canonical_instrument_id": "eq_aaa",
                "provider_symbol": "AAA-old",
            },
            {
                "snapshot_id": "snapshot-1",
                "canonical_instrument_id": "eq_aaa",
                "provider_symbol": "AAA",
            },
        ],
        ("snapshot_id", "canonical_instrument_id"),
    )

    assert rows == [
        {
            "snapshot_id": "snapshot-1",
            "canonical_instrument_id": "eq_aaa",
            "provider_symbol": "AAA",
        }
    ]
