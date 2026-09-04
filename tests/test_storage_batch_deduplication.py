from datetime import UTC, date, datetime

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


def test_intraday_rows_deduplicate_database_conflict_keys() -> None:
    timestamp = datetime(2026, 7, 23, 9, 30, tzinfo=UTC)
    frame = pd.DataFrame(
        [
            {
                "InstrumentKey": "DUKAS|EURUSD",
                "Symbol": "EUR/USD",
                "Timestamp": timestamp,
                "Interval": "5m",
                "AssetClass": "fx",
                "Open": 1.1,
                "High": 1.2,
                "Low": 1.0,
                "Close": 1.15,
                "Volume": 100,
            },
            {
                "InstrumentKey": "DUKAS|EURUSD",
                "Symbol": "EUR/USD",
                "Timestamp": timestamp,
                "Interval": "5m",
                "AssetClass": "fx",
                "Open": 1.1,
                "High": 1.25,
                "Low": 1.0,
                "Close": 1.2,
                "Volume": 200,
            },
        ]
    )

    rows = TimescaleStore._intraday_ohlcv_rows(
        frame,
        exchange="GLOBAL",
        source="dukascopy",
    )

    assert len(rows) == 1
    assert rows[0]["ts"] == timestamp
    assert rows[0]["close"] == 1.2
    assert rows[0]["volume"] == 200


def test_research_rows_deduplicate_database_conflict_keys() -> None:
    feature_rows = TimescaleStore._daily_feature_rows(
        pd.DataFrame(
            [
                {
                    "instrument_key": "NSE_EQ|AAA",
                    "date": date(2026, 7, 23),
                    "feature_version": "features-v1",
                    "symbol": "AAA",
                    "ret_1d": 0.01,
                },
                {
                    "instrument_key": "NSE_EQ|AAA",
                    "date": date(2026, 7, 23),
                    "feature_version": "features-v1",
                    "symbol": "AAA",
                    "ret_1d": 0.02,
                },
            ]
        )
    )
    target_rows = TimescaleStore._daily_target_rows(
        pd.DataFrame(
            [
                {
                    "instrument_key": "NSE_EQ|AAA",
                    "date": date(2026, 7, 23),
                    "target_version": "targets-v1",
                    "symbol": "AAA",
                    "forward_ret_1d": 0.01,
                },
                {
                    "instrument_key": "NSE_EQ|AAA",
                    "date": date(2026, 7, 23),
                    "target_version": "targets-v1",
                    "symbol": "AAA",
                    "forward_ret_1d": 0.03,
                },
            ]
        )
    )

    assert len(feature_rows) == 1
    assert feature_rows[0]["ret_1d"] == 0.02
    assert len(target_rows) == 1
    assert target_rows[0]["forward_ret_1d"] == 0.03


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
