from datetime import UTC, datetime

import pandas as pd

import trade_research.pipelines.yfinance_intraday as yfinance_intraday
from trade_research.config import Settings
from trade_research.pipelines.yfinance_intraday import run_yfinance_intraday_ohlcv_pipeline
from trade_research.universe import YFinanceIntradayInstrument


class _FakeYFinanceIntradayProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], datetime, datetime, str]] = []

    def fetch_intraday_ohlcv(
        self,
        instruments: list[YFinanceIntradayInstrument],
        start: datetime,
        end: datetime,
        interval: str = "5m",
    ) -> pd.DataFrame:
        self.calls.append(([item.symbol for item in instruments], start, end, interval))
        rows = []
        for item in instruments:
            rows.append(
                {
                    "Timestamp": start,
                    "Open": 1.1,
                    "High": 1.2,
                    "Low": 1.0,
                    "Close": 1.15,
                    "Volume": 0.0,
                    "InstrumentKey": item.instrument_key,
                    "Symbol": item.symbol,
                    "TradingSymbol": item.yahoo_symbol,
                    "Exchange": item.exchange,
                    "AssetClass": item.asset_class,
                    "Interval": interval,
                    "Source": "yfinance",
                }
            )
        return pd.DataFrame(rows)


class _FakeTimescaleStore:
    instances: list["_FakeTimescaleStore"] = []

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.logs: list[dict] = []
        self.upserted_rows = 0
        self.finished_runs: list[dict] = []
        _FakeTimescaleStore.instances.append(self)

    def initialize(self) -> None:
        return None

    def start_ingestion_run(
        self,
        job_name: str,
        exchange: str,
        source: str,
        items_requested: int,
        run_metadata: dict,
    ) -> str:
        assert job_name == "yfinance_fx_crypto_5m_5m_ohlcv"
        assert exchange == "GLOBAL"
        assert source == "yfinance"
        assert items_requested == 1
        assert run_metadata["instrument"] == "EUR/USD"
        return "yf-intraday-run"

    def insert_provider_request_logs(self, logs) -> int:
        self.logs.extend(logs)
        return len(logs)

    def upsert_intraday_ohlcv(
        self,
        frame: pd.DataFrame,
        exchange: str,
        source: str,
    ) -> int:
        assert exchange == "GLOBAL"
        assert source == "yfinance"
        self.upserted_rows += len(frame)
        return len(frame)

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


def test_run_yfinance_intraday_pipeline_logs_and_upserts(tmp_path, monkeypatch) -> None:
    provider = _FakeYFinanceIntradayProvider()
    _FakeTimescaleStore.instances = []
    monkeypatch.setattr(
        yfinance_intraday,
        "get_settings",
        lambda: Settings(
            database_url="postgresql://test/test",
            data_dir=tmp_path,
            provider_rate_limit_backend="none",
        ),
    )
    monkeypatch.setattr(yfinance_intraday, "TimescaleStore", _FakeTimescaleStore)

    result = run_yfinance_intraday_ohlcv_pipeline(
        from_datetime="2026-07-01T00:00:00Z",
        to_datetime="2026-07-01T01:00:00Z",
        instrument="EUR/USD",
        store_db=True,
        provider=provider,
    )

    store = _FakeTimescaleStore.instances[0]
    assert result.status == "pass"
    assert result.rows == 1
    assert result.metrics["source"] == "yfinance"
    assert result.metrics["timescale_rows"] == 1
    assert provider.calls == [
        (
            ["EUR/USD"],
            datetime(2026, 7, 1, tzinfo=UTC),
            datetime(2026, 7, 1, 1, tzinfo=UTC),
            "5m",
        )
    ]
    assert len(store.logs) == 1
    assert store.logs[0]["provider"] == "yfinance"
    assert store.logs[0]["endpoint_group"] == "intraday_download"
    assert store.finished_runs[0]["status"] == "completed"
    assert result.artifacts["ohlcv"].exists()
    assert result.artifacts["fetch_failures"].exists()


def test_run_yfinance_intraday_pipeline_rejects_unknown_instrument(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        yfinance_intraday,
        "get_settings",
        lambda: Settings(data_dir=tmp_path, provider_rate_limit_backend="none"),
    )

    try:
        run_yfinance_intraday_ohlcv_pipeline(
            from_datetime="2026-07-01T00:00:00Z",
            to_datetime="2026-07-01T01:00:00Z",
            instrument="USD/CNY",
            store_db=False,
            provider=_FakeYFinanceIntradayProvider(),
        )
    except ValueError as exc:
        assert "Unsupported yfinance intraday instrument" in str(exc)
        assert "USD/CNH" in str(exc)
    else:
        raise AssertionError("Expected invalid instrument to raise ValueError")
