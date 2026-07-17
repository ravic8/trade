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
    run_yfinance_missing_ohlcv_pipeline,
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


class _FakeTimescaleStore:
    instances: list["_FakeTimescaleStore"] = []

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.logs: list[dict] = []
        self.ohlcv_rows = 0
        self.price_adjustment_rows = 0
        self.coverage_rows = 0
        self.finished_runs: list[dict] = []
        _FakeTimescaleStore.instances.append(self)

    def initialize(self) -> None:
        return None

    def exchange_holidays(self, exchange: str, year: int) -> dict:
        return {
            "exchange": exchange,
            "year": year,
            "closed_dates": [],
            "early_close_dates": [],
            "source_url": "test-calendar",
        }

    def upsert_exchange_holidays(
        self,
        exchange: str,
        year: int,
        closed_dates,
        early_close_dates,
        source_url: str,
    ) -> int:
        return 1

    def daily_ohlcv_dates_by_instrument(
        self,
        instrument_keys: list[str],
        start_date: date,
        end_date: date,
        source: str = "yfinance",
        exchange: str = "US",
    ) -> dict[str, set[date]]:
        del start_date, end_date, source, exchange
        dates = {
            "YF|AAPL": {date(2026, 7, 6), date(2026, 7, 7)},
            "YF|MSFT": {date(2026, 7, 6), date(2026, 7, 7)},
        }
        return {key: dates.get(key, set()) for key in instrument_keys}

    def first_daily_ohlcv_dates_by_instrument(
        self,
        instrument_keys: list[str],
        source: str = "yfinance",
        exchange: str = "US",
    ) -> dict[str, date]:
        del source, exchange
        dates = {"YF|AAPL": date(2026, 7, 6), "YF|MSFT": date(2026, 7, 6)}
        return {key: dates[key] for key in instrument_keys if key in dates}

    def daily_ohlcv_average_turnover_by_instrument(
        self,
        instrument_keys: list[str],
        start_date: date,
        end_date: date,
        source: str = "yfinance",
        exchange: str = "US",
    ) -> dict[str, float]:
        del start_date, end_date, source, exchange
        values = {"YF|AAPL": 500.0, "YF|MSFT": 100.0}
        return {key: values[key] for key in instrument_keys if key in values}

    def start_ingestion_run(
        self,
        job_name: str,
        exchange: str,
        source: str,
        items_requested: int,
        run_metadata: dict,
    ) -> str:
        del job_name, exchange, source, run_metadata
        assert items_requested == 1
        return "run-missing"

    def insert_provider_request_logs(self, logs) -> int:
        self.logs.extend(logs)
        return len(logs)

    def upsert_daily_ohlcv(
        self,
        frame: pd.DataFrame,
        exchange: str = "US",
        source: str = "yfinance",
    ) -> int:
        del exchange, source
        self.ohlcv_rows += len(frame)
        return len(frame)

    def upsert_daily_price_adjustments(
        self,
        frame: pd.DataFrame,
        exchange: str = "US",
        source: str = "yfinance",
    ) -> int:
        del exchange, source
        self.price_adjustment_rows += len(frame)
        return len(frame)

    def insert_data_quality_audits(
        self,
        frame: pd.DataFrame,
        dataset_name: str,
        source: str,
        interval: str,
    ) -> int:
        del dataset_name, source, interval
        return len(frame)

    def insert_daily_ohlcv_fetch_coverage(
        self,
        run_id: str,
        frame: pd.DataFrame,
        source: str = "yfinance",
        exchange: str = "US",
    ) -> int:
        del run_id, source, exchange
        self.coverage_rows += len(frame)
        return len(frame)

    def daily_ohlcv_frame(
        self,
        exchange: str = "US",
        source: str = "yfinance",
    ) -> pd.DataFrame:
        del exchange, source
        return pd.DataFrame()

    def finish_ingestion_run(
        self,
        run_id: str,
        status: str,
        items_processed: int,
        items_succeeded: int,
        items_failed: int,
        error_message: str | None = None,
    ) -> None:
        self.finished_runs.append(
            {
                "run_id": run_id,
                "status": status,
                "items_processed": items_processed,
                "items_succeeded": items_succeeded,
                "items_failed": items_failed,
                "error_message": error_message,
            }
        )


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
                    "AdjClose": 99.5,
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
    assert result.metrics["timescale_price_adjustment_rows"] == 0
    assert result.artifacts["ohlcv"].exists()
    assert result.artifacts["daily_audit"].exists()
    assert result.artifacts["fetch_coverage"].exists()


def test_run_yfinance_missing_pipeline_filters_and_fetches_missing_window(
    tmp_path,
    monkeypatch,
) -> None:
    provider = _FakeYFinanceProvider()
    _FakeTimescaleStore.instances = []
    monkeypatch.setattr(
        yfinance_daily,
        "get_settings",
        lambda: Settings(
            database_url="postgresql://test/test",
            data_dir=tmp_path,
            provider_rate_limit_backend="none",
        ),
    )
    monkeypatch.setattr(yfinance_daily, "TimescaleStore", _FakeTimescaleStore)

    result = run_yfinance_missing_ohlcv_pipeline(
        universe="us_seed",
        from_date="2026-07-06",
        to_date="2026-07-08",
        coverage_status="partial",
        min_avg_daily_turnover=200,
        min_coverage_pct=0.5,
        limit=10,
        batch_size=5,
        export_db_snapshot=False,
        provider=provider,
    )

    store = _FakeTimescaleStore.instances[0]
    assert result.name == "yfinance_us_seed_missing_daily_ohlcv"
    assert result.status == "pass"
    assert result.metrics["fetch_symbols"] == 1
    assert result.metrics["fetch_windows"] == 1
    assert result.metrics["timescale_rows"] == 1
    assert result.metrics["timescale_price_adjustment_rows"] == 1
    assert result.metrics["timescale_fetch_coverage_rows"] == 1
    assert provider.calls == [(["AAPL"], date(2026, 7, 8), date(2026, 7, 8))]
    assert store.logs[0]["request_key"] == "AAPL:1d:2026-07-08:2026-07-08"
    assert store.finished_runs[0]["status"] == "completed"
