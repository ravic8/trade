from datetime import datetime

import pandas as pd

import trade_research.pipelines.dukascopy_intraday as dukascopy_pipeline
from trade_research.config import Settings
from trade_research.pipelines.dukascopy_intraday import (
    run_dukascopy_intraday_ohlcv_pipeline,
)
from trade_research.universe import DukascopyInstrument


class _FakeDukascopyProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, datetime]] = []

    def fetch_hour_ticks(
        self,
        instrument: DukascopyInstrument,
        hour_start: datetime,
    ) -> pd.DataFrame:
        self.calls.append((instrument.symbol, hour_start))
        if hour_start.hour != 0:
            return pd.DataFrame()
        return pd.DataFrame(
            [
                {
                    "Timestamp": hour_start,
                    "Price": 1.1,
                    "Volume": 2.0,
                    "InstrumentKey": instrument.instrument_key,
                    "Symbol": instrument.symbol,
                    "TradingSymbol": instrument.dukascopy_id.upper(),
                    "AssetClass": instrument.asset_class,
                    "Source": "dukascopy",
                },
                {
                    "Timestamp": hour_start + pd.Timedelta(minutes=1),
                    "Price": 1.2,
                    "Volume": 3.0,
                    "InstrumentKey": instrument.instrument_key,
                    "Symbol": instrument.symbol,
                    "TradingSymbol": instrument.dukascopy_id.upper(),
                    "AssetClass": instrument.asset_class,
                    "Source": "dukascopy",
                },
            ]
        )


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
        assert job_name == "dukascopy_fx_crypto_5m_5m_ohlcv"
        assert exchange == "GLOBAL"
        assert source == "dukascopy"
        assert items_requested == 1
        assert run_metadata["mapped_symbols"] == 1
        assert run_metadata["instrument"] == "EURUSD"
        assert run_metadata["max_hours"] == 1
        assert run_metadata["timeout_seconds"] == 5.0
        return "dukas-run"

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
        assert source == "dukascopy"
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


def test_run_dukascopy_intraday_pipeline_logs_and_upserts(tmp_path, monkeypatch) -> None:
    provider = _FakeDukascopyProvider()
    _FakeTimescaleStore.instances = []
    monkeypatch.setattr(
        dukascopy_pipeline,
        "get_settings",
        lambda: Settings(
            database_url="postgresql://test/test",
            data_dir=tmp_path,
            provider_rate_limit_backend="none",
        ),
    )
    monkeypatch.setattr(dukascopy_pipeline, "TimescaleStore", _FakeTimescaleStore)

    result = run_dukascopy_intraday_ohlcv_pipeline(
        from_date="2026-07-01",
        to_date="2026-07-01",
        instrument="EURUSD",
        max_hours=1,
        timeout_seconds=5.0,
        store_db=True,
        provider=provider,
    )

    store = _FakeTimescaleStore.instances[0]
    assert result.status == "pass"
    assert result.rows == 1
    assert result.metrics["instrument"] == "EURUSD"
    assert result.metrics["requested_hours"] == 1
    assert result.metrics["max_hours"] == 1
    assert result.metrics["timeout_seconds"] == 5.0
    assert result.metrics["timescale_rows"] == 1
    assert len(provider.calls) == 1
    assert len(store.logs) == 1
    assert store.logs[0]["provider"] == "dukascopy"
    assert store.logs[0]["endpoint_group"] == "historical"
    assert store.logs[0]["interval"] == "5m"
    assert store.finished_runs[0]["status"] == "completed"
    assert result.artifacts["ohlcv"].exists()
    assert result.artifacts["fetch_failures"].exists()


def test_run_dukascopy_intraday_pipeline_rejects_unknown_instrument(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        dukascopy_pipeline,
        "get_settings",
        lambda: Settings(data_dir=tmp_path, provider_rate_limit_backend="none"),
    )

    try:
        run_dukascopy_intraday_ohlcv_pipeline(
            from_date="2026-07-01",
            to_date="2026-07-01",
            instrument="USD/CNY",
            store_db=False,
            provider=_FakeDukascopyProvider(),
        )
    except ValueError as exc:
        assert "Unsupported Dukascopy instrument" in str(exc)
        assert "USD/CNH" in str(exc)
    else:
        raise AssertionError("Expected invalid instrument to raise ValueError")
