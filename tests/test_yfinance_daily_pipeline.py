from datetime import date

import pandas as pd

import trade_research.pipelines.yfinance_daily as yfinance_daily
from trade_research.config import Settings
from trade_research.data.rate_limits import RateLimitDecision
from trade_research.pipelines.yfinance_daily import (
    _fetch_yfinance_daily_batches_with_controls,
    _yfinance_batches,
    _yfinance_mapping,
    run_yfinance_daily_ohlcv_pipeline,
)


class _RecordingLimiter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def acquire(self, provider: str, endpoint_group: str) -> RateLimitDecision:
        self.calls.append((provider, endpoint_group))
        return RateLimitDecision(backend="memory", wait_seconds=0.0, rate_limited=False)


class _RecordingStore:
    def __init__(self) -> None:
        self.logs: list[dict] = []

    def insert_provider_request_logs(self, logs) -> int:
        self.logs.extend(logs)
        return len(logs)


class _FakeYFinanceProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], date, date]] = []

    def fetch_daily_ohlcv(
        self,
        symbols: list[dict[str, str]],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        self.calls.append(([item["yahoo_symbol"] for item in symbols], start, end))
        rows = []
        for item in symbols:
            rows.append(
                {
                    "Date": start,
                    "Open": 100.0,
                    "High": 101.0,
                    "Low": 99.0,
                    "Close": 100.5,
                    "Volume": 1000,
                    "OpenInterest": None,
                    "InstrumentKey": item["instrument_key"],
                    "Symbol": item["symbol"],
                    "TradingSymbol": item["yahoo_symbol"],
                    "Source": "yfinance",
                }
            )
        return pd.DataFrame(rows)


def test_yfinance_mapping_uses_seed_symbols() -> None:
    mapping = _yfinance_mapping("canada_seed")

    assert mapping.iloc[0]["symbol"] == "SHOP"
    assert mapping.iloc[0]["instrument_key"] == "YF|SHOP.TO"
    assert mapping.iloc[0]["yahoo_symbol"] == "SHOP.TO"


def test_yfinance_batches_group_by_window_and_batch_size() -> None:
    rows = [
        {"symbol": "A", "fetch_start": "2026-07-01", "fetch_end": "2026-07-03"},
        {"symbol": "B", "fetch_start": "2026-07-01", "fetch_end": "2026-07-03"},
        {"symbol": "C", "fetch_start": "2026-07-02", "fetch_end": "2026-07-03"},
    ]

    batches = _yfinance_batches(rows, batch_size=1)

    assert [[row["symbol"] for row in batch] for batch in batches] == [["A"], ["B"], ["C"]]


def test_fetch_yfinance_batches_limits_and_logs_request() -> None:
    provider = _FakeYFinanceProvider()
    limiter = _RecordingLimiter()
    store = _RecordingStore()
    rows = [
        {
            "symbol": "AAPL",
            "instrument_key": "YF|AAPL",
            "yahoo_symbol": "AAPL",
            "fetch_start": "2026-07-01",
            "fetch_end": "2026-07-03",
        },
        {
            "symbol": "MSFT",
            "instrument_key": "YF|MSFT",
            "yahoo_symbol": "MSFT",
            "fetch_start": "2026-07-01",
            "fetch_end": "2026-07-03",
        },
    ]

    frames, failures = _fetch_yfinance_daily_batches_with_controls(
        provider=provider,
        rows=rows,
        limiter=limiter,
        db=store,
        run_id="run-1",
        batch_size=2,
    )

    assert failures == []
    assert len(frames) == 1
    assert provider.calls == [(["AAPL", "MSFT"], date(2026, 7, 1), date(2026, 7, 3))]
    assert limiter.calls == [("yfinance", "download")]
    assert len(store.logs) == 1
    log = store.logs[0]
    assert log["provider"] == "yfinance"
    assert log["endpoint_group"] == "download"
    assert log["request_key"] == "AAPL,MSFT:1d:2026-07-01:2026-07-03"
    assert log["status"] == "success"


def test_run_yfinance_daily_pipeline_writes_artifacts_without_db(tmp_path, monkeypatch) -> None:
    provider = _FakeYFinanceProvider()
    monkeypatch.setattr(
        yfinance_daily,
        "get_settings",
        lambda: Settings(data_dir=tmp_path, provider_rate_limit_backend="none"),
    )

    result = run_yfinance_daily_ohlcv_pipeline(
        universe="us_seed",
        from_date="2026-07-01",
        to_date="2026-07-03",
        limit=2,
        batch_size=2,
        store_db=False,
        provider=provider,
    )

    assert result.name == "yfinance_us_seed_daily_ohlcv"
    assert result.status == "pass"
    assert result.rows == 2
    assert result.metrics["exchange"] == "US"
    assert result.metrics["fetch_symbols"] == 2
    assert result.metrics["store_db"] is False
    assert result.artifacts["ohlcv"].exists()
    assert result.artifacts["daily_audit"].exists()
    assert result.artifacts["fetch_coverage"].exists()
