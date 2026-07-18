from datetime import UTC, date, datetime

import pandas as pd

from trade_research.features import DailyTechnicalFeatureBuilder
from trade_research.storage.timescale import TimescaleStore, metadata


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


def test_replace_daily_features_removes_old_provider_keys_atomically() -> None:
    store = TimescaleStore("sqlite://")
    metadata.create_all(store.engine)
    old = DailyTechnicalFeatureBuilder().build(
        pd.DataFrame(
            [
                {
                    "Date": date(2025, 1, 1),
                    "Open": 99.0,
                    "High": 101.0,
                    "Low": 98.0,
                    "Close": 100.0,
                    "Volume": 100,
                    "InstrumentKey": "NSE_EQ|TEST",
                    "Symbol": "TEST",
                }
            ]
        )
    )
    new = old.copy()
    new["instrument_key"] = "YF|TEST.NS"
    store.upsert_daily_features(old)

    deleted, inserted = store.replace_daily_features(
        new, "daily_v1_ohlcv_technical_v1_0"
    )
    frame = store.daily_feature_frame("daily_v1_ohlcv_technical_v1_0")

    assert deleted == 1
    assert inserted == 1
    assert frame["instrument_key"].tolist() == ["YF|TEST.NS"]


def test_stock_coverage_rows_normalize_for_storage() -> None:
    coverage = pd.DataFrame(
        [
            {
                "window_months": 6,
                "instrument_key": "NSE_EQ|TEST",
                "symbol": "test",
                "window_start": "2026-01-01",
                "window_end": "2026-06-25",
                "first_date": "2026-01-02",
                "last_date": "2026-06-25",
                "expected_date_count": 120,
                "observed_date_count": 119,
                "missing_date_count": 1,
                "coverage_pct": 119 / 120,
                "has_latest_expected_date": True,
                "latest_date_lag_days": 0,
                "coverage_status": "pass",
            }
        ]
    )

    rows = TimescaleStore._stock_coverage_rows(
        coverage,
        run_id="dagster-run",
        source="upstox",
        exchange="NSE",
        created_at=datetime(2026, 6, 28, tzinfo=UTC),
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["run_id"] == "dagster-run"
    assert row["window_months"] == 6
    assert row["instrument_key"] == "NSE_EQ|TEST"
    assert row["symbol"] == "TEST"
    assert row["coverage_status"] == "pass"


def test_daily_ohlcv_fetch_coverage_rows_normalize_for_storage() -> None:
    coverage = pd.DataFrame(
        [
            {
                "instrument_key": "NSE_EQ|TEST",
                "symbol": "test",
                "latest_stored_date": "2026-06-20",
                "fetch_start": "2026-06-21",
                "fetch_end": "2026-06-25",
                "should_fetch": True,
                "fetch_status": "failed",
                "rows_fetched": 0,
                "skip_reason": "",
                "error": "rate limited",
            }
        ]
    )

    rows = TimescaleStore._daily_ohlcv_fetch_coverage_rows(
        coverage,
        run_id="ingestion-run",
        source="upstox",
        exchange="NSE",
        created_at=datetime(2026, 6, 28, tzinfo=UTC),
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["run_id"] == "ingestion-run"
    assert row["instrument_key"] == "NSE_EQ|TEST"
    assert row["symbol"] == "TEST"
    assert row["status"] == "failed"
    assert row["error_message"] == "rate limited"


def test_daily_price_adjustment_rows_normalize_for_storage() -> None:
    rows = TimescaleStore._daily_price_adjustment_rows(
        pd.DataFrame(
            [
                {
                    "Date": date(2026, 7, 1),
                    "InstrumentKey": "YF|AAPL",
                    "Symbol": "aapl",
                    "Close": 100.0,
                    "AdjClose": 95.0,
                },
                {
                    "Date": date(2026, 7, 2),
                    "InstrumentKey": "YF|MSFT",
                    "Symbol": "msft",
                    "Close": 0.0,
                    "AdjClose": 10.0,
                },
            ]
        ),
        exchange="US",
        source="yfinance",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["instrument_key"] == "YF|AAPL"
    assert row["symbol"] == "AAPL"
    assert row["date"] == date(2026, 7, 1)
    assert row["raw_close"] == 100.0
    assert row["adjusted_close"] == 95.0
    assert row["adjustment_factor"] == 0.95


def test_corporate_action_rows_normalize_for_storage() -> None:
    rows = TimescaleStore._corporate_action_rows(
        pd.DataFrame(
            [
                {
                    "InstrumentKey": "YF|AAPL",
                    "Symbol": "aapl",
                    "ActionDate": date(2026, 8, 1),
                    "ActionType": "Dividend",
                    "Value": 0.25,
                    "Currency": "USD",
                    "Raw": {"kind": "cash_dividend"},
                },
                {
                    "InstrumentKey": "YF|MSFT",
                    "Symbol": "msft",
                    "ActionDate": date(2026, 8, 1),
                },
            ]
        ),
        exchange="US",
        source="yfinance",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["source"] == "yfinance"
    assert row["instrument_key"] == "YF|AAPL"
    assert row["symbol"] == "AAPL"
    assert row["action_date"] == date(2026, 8, 1)
    assert row["action_type"] == "dividend"
    assert row["value"] == 0.25
    assert row["currency"] == "USD"
    assert row["raw"] == {"kind": "cash_dividend"}


def test_intraday_ohlcv_rows_normalize_for_storage() -> None:
    rows = TimescaleStore._intraday_ohlcv_rows(
        pd.DataFrame(
            [
                {
                    "Timestamp": datetime(2026, 7, 1, 9, 30, tzinfo=UTC),
                    "InstrumentKey": "DUKAS|EURUSD",
                    "Symbol": "eur/usd",
                    "AssetClass": "fx",
                    "Interval": "5m",
                    "Open": 1.1,
                    "High": 1.2,
                    "Low": 1.0,
                    "Close": 1.15,
                    "Volume": 10.5,
                },
                {
                    "Timestamp": None,
                    "InstrumentKey": "DUKAS|GBPUSD",
                    "Symbol": "gbp/usd",
                    "AssetClass": "fx",
                    "Interval": "5m",
                    "Open": 1.1,
                    "High": 1.2,
                    "Low": 1.0,
                    "Close": 1.15,
                    "Volume": 10.5,
                },
            ]
        ),
        exchange="GLOBAL",
        source="dukascopy",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["instrument_key"] == "DUKAS|EURUSD"
    assert row["source"] == "dukascopy"
    assert row["interval"] == "5m"
    assert row["symbol"] == "EUR/USD"
    assert row["exchange"] == "GLOBAL"
    assert row["asset_class"] == "fx"
    assert row["volume"] == 10.5
    assert row["quality_status"] == "ok"


def test_provider_request_log_rows_normalize_for_storage() -> None:
    rows = TimescaleStore._provider_request_log_rows(
        [
            {
                "run_id": "run-1",
                "provider": "UPSTOX",
                "endpoint_group": "historical",
                "request_key": "NSE_EQ|TEST:1d:2026-06-21:2026-06-25",
                "instrument_key": "NSE_EQ|TEST",
                "symbol": "test",
                "interval": "1d",
                "window_start": "2026-06-21",
                "window_end": date(2026, 6, 25),
                "status": "success",
                "retry_count": 0,
                "rate_limited": True,
                "wait_seconds": 0.25,
                "duration_ms": 123.4,
                "created_at": datetime(2026, 6, 28, tzinfo=UTC),
            }
        ]
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["run_id"] == "run-1"
    assert row["provider"] == "upstox"
    assert row["endpoint_group"] == "historical"
    assert row["instrument_key"] == "NSE_EQ|TEST"
    assert row["symbol"] == "test"
    assert row["window_start"] == date(2026, 6, 21)
    assert row["window_end"] == date(2026, 6, 25)
    assert row["rate_limited"] is True
    assert row["wait_seconds"] == 0.25


def test_provider_request_log_rows_skip_incomplete_records() -> None:
    rows = TimescaleStore._provider_request_log_rows(
        [
            {
                "provider": "upstox",
                "endpoint_group": "historical",
                "status": "success",
            },
            {
                "provider": "upstox",
                "endpoint_group": "historical",
                "request_key": "key",
                "status": "success",
            },
        ]
    )

    assert len(rows) == 1
    assert rows[0]["request_key"] == "key"
