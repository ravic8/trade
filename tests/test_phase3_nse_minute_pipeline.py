from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace

import pandas as pd

import trade_research.pipelines.yfinance_nse_minute as nse_minute
from trade_research.config import Settings
from trade_research.pipelines.yfinance_nse_minute import run_yfinance_nse_minute_pipeline
from trade_research.universe import YFinanceIntradayInstrument


class _Provider:
    def fetch_intraday_ohlcv(
        self,
        instruments: list[YFinanceIntradayInstrument],
        start: datetime,
        end: datetime,
        interval: str = "1m",
    ) -> pd.DataFrame:
        assert len(instruments) == 1
        assert interval == "1m"
        return pd.DataFrame(
            [
                _row(datetime(2026, 9, 4, 3, 45, tzinfo=UTC)),
                _row(datetime(2026, 9, 5, 3, 45, tzinfo=UTC)),
            ]
        )


class _Store:
    instances: list[_Store] = []

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.engine = object()
        self.request_logs: list[dict] = []
        self.finished: list[dict] = []
        self.__class__.instances.append(self)

    def initialize(self) -> None:
        return None

    def exchange_sessions(self, exchange: str, start: date, end: date) -> list[dict]:
        assert exchange == "NSE"
        assert start <= date(2026, 9, 4) <= end
        return [
            {
                "session_date": date(2026, 9, 4),
                "is_trading_day": True,
                "validation_status": "valid",
                "market_close_utc": datetime(2026, 9, 4, 10, tzinfo=UTC),
            },
            {
                "session_date": date(2026, 9, 5),
                "is_trading_day": True,
                "validation_status": "valid",
                "market_close_utc": datetime(2026, 9, 5, 10, tzinfo=UTC),
            },
        ]

    def active_yfinance_daily_instruments(self, exchange: str) -> list[dict]:
        assert exchange == "NSE"
        return [
            {
                "canonical_instrument_id": "nse-reliance",
                "exchange_symbol": "RELIANCE",
                "provider_symbol": "RELIANCE.NS",
                "provider_instrument_key": "YF|RELIANCE.NS",
                "name": "Reliance Industries",
            }
        ]

    def start_ingestion_run(self, **kwargs) -> str:
        assert kwargs["job_name"] == "yfinance_nse_1m_ohlcv"
        assert kwargs["exchange"] == "NSE"
        return "minute-run"

    def insert_provider_request_logs(self, rows) -> int:
        self.request_logs.extend(rows)
        return len(rows)

    def finish_ingestion_run(self, run_id: str, **kwargs) -> None:
        self.finished.append({"run_id": run_id, **kwargs})


def _row(timestamp: datetime) -> dict:
    return {
        "Timestamp": timestamp,
        "InstrumentKey": "YF|RELIANCE.NS",
        "TradingSymbol": "RELIANCE.NS",
        "Symbol": "RELIANCE",
        "Exchange": "NSE",
        "Open": 100.0,
        "High": 101.0,
        "Low": 99.0,
        "Close": 100.5,
        "Volume": 1000,
        "Interval": "1m",
        "Source": "yfinance",
    }


def _settings() -> Settings:
    return Settings(
        _env_file=None,
        database_url="postgresql://test/test",
        provider_rate_limit_backend="none",
        clickhouse_enabled=True,
        clickhouse_write_enabled=True,
        clickhouse_password="writer-password",
        object_store_enabled=True,
        object_store_write_enabled=True,
        object_store_access_key_id="writer",
        object_store_secret_access_key="writer-secret",
        phase3_market_data_enabled=True,
        yfinance_nse_minute_enabled=True,
    )


def test_nse_minute_pipeline_snapshots_raw_but_replicates_completed_sessions(
    monkeypatch,
) -> None:
    _Store.instances = []
    captured: dict = {}
    monkeypatch.setattr(nse_minute, "get_settings", _settings)
    monkeypatch.setattr(nse_minute, "TimescaleStore", _Store)

    def prepare(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            request=object(),
            frame=kwargs["frame"],
            candles=(object(),),
            raw_snapshot=SimpleNamespace(
                storage_uri="s3://trade-raw/minute.json",
                artifact_manifest_id="artifact-minute",
            ),
        )

    monkeypatch.setattr(nse_minute, "prepare_yfinance_batch", prepare)
    monkeypatch.setattr(
        nse_minute,
        "observe_nse_minute_availability",
        lambda **_kwargs: [],
    )

    class AvailabilityRepository:
        def __init__(self, _engine) -> None:
            pass

        def record(self, observations) -> int:
            return len(observations)

    monkeypatch.setattr(
        nse_minute,
        "MarketDataAvailabilityRepository",
        AvailabilityRepository,
    )
    monkeypatch.setattr(
        nse_minute,
        "nse_minute_missing_quality_outcomes",
        lambda **_kwargs: [],
    )

    class QualityRepository:
        def __init__(self, _engine) -> None:
            pass

        def record(self, outcomes) -> int:
            return len(outcomes)

    monkeypatch.setattr(nse_minute, "MarketDataQualityRepository", QualityRepository)
    monkeypatch.setattr(
        nse_minute,
        "replicate_validated_batch",
        lambda _settings, batch, **_kwargs: len(batch.candles),
    )

    result = run_yfinance_nse_minute_pipeline(
        from_datetime="2026-09-04T00:00:00Z",
        to_datetime="2026-09-05T12:00:00Z",
        symbol_limit=1,
        provider=_Provider(),
        at=datetime(2026, 9, 5, 9, tzinfo=UTC),
    )

    assert len(captured["raw_frame"]) == 2
    assert len(captured["frame"]) == 1
    assert result.status == "pass"
    assert result.rows == 1
    assert result.metrics["raw_rows"] == 2
    assert result.metrics["validated_rows"] == 1
    assert result.metrics["eligible_sessions"] == 1
    assert result.metrics["raw_snapshot_uri"] == "s3://trade-raw/minute.json"
    assert _Store.instances[0].finished[0]["status"] == "completed"


def test_nse_minute_pipeline_rejects_window_beyond_configured_retention(monkeypatch) -> None:
    monkeypatch.setattr(nse_minute, "get_settings", _settings)

    try:
        run_yfinance_nse_minute_pipeline(
            from_datetime="2026-08-01T00:00:00Z",
            to_datetime="2026-09-05T00:00:00Z",
            provider=_Provider(),
        )
    except ValueError as exc:
        assert "exceeds the configured request safety limit" in str(exc)
    else:
        raise AssertionError("Expected an out-of-retention request to be rejected")
