from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from importlib import import_module
from types import SimpleNamespace

from fastapi.testclient import TestClient

from trade_research.market_data.aggregation import AggregatedMarketCandle
from trade_research.market_data.contracts import CandleInterval

api_module = import_module("trade_research.api.app")


class _AggregateRepository:
    def __init__(self) -> None:
        self.request = None

    def aggregate_nse_intraday(self, request):
        self.request = request
        return [
            AggregatedMarketCandle(
                workspace_id=request.workspace_id,
                instrument_id=request.instrument_id,
                exchange="NSE",
                symbol="RELIANCE",
                provider_symbol="RELIANCE.NS",
                currency="INR",
                candle_timestamp=datetime(2026, 9, 5, 3, 45, tzinfo=UTC),
                session_date=date(2026, 9, 5),
                interval=request.interval,
                open=Decimal("100"),
                high=Decimal("105"),
                low=Decimal("99"),
                close=Decimal("104"),
                volume=1000,
                provider="yfinance",
                provider_timestamp=datetime(2026, 9, 5, 12, tzinfo=UTC),
                source_rows=15,
                expected_source_rows=15,
                complete=True,
                source_digest="a" * 64,
                source_run_ids=("run-1",),
                raw_artifact_ids=("artifact-1",),
            )
        ]


def test_aggregate_api_returns_lineage_and_completeness(monkeypatch) -> None:
    repository = _AggregateRepository()
    monkeypatch.setattr(
        api_module,
        "get_settings",
        lambda: SimpleNamespace(
            phase3_market_data_enabled=True,
            clickhouse_enabled=True,
        ),
    )
    monkeypatch.setattr(
        api_module,
        "_clickhouse_market_data_repository",
        lambda: repository,
    )
    start = datetime(2026, 9, 5, 3, 45, tzinfo=UTC)

    with TestClient(api_module.app) as client:
        response = client.get(
            "/api/data/candles/aggregate",
            params={
                "instrument_id": "NSE_EQ|RELIANCE",
                "interval": "15m",
                "window_start": start.isoformat(),
                "window_end": (start + timedelta(hours=1)).isoformat(),
                "complete_only": "true",
            },
            headers={"X-Workspace-ID": "workspace-1"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["workspace_id"] == "workspace-1"
    assert payload["source_interval"] == "1m"
    assert payload["interval"] == "15m"
    assert payload["rows"][0]["complete"] is True
    assert payload["rows"][0]["source_rows"] == 15
    assert payload["rows"][0]["expected_source_rows"] == 15
    assert payload["rows"][0]["source_run_ids"] == ["run-1"]
    assert payload["rows"][0]["raw_artifact_ids"] == ["artifact-1"]
    assert repository.request.interval is CandleInterval.FIFTEEN_MINUTES
    assert repository.request.workspace_id == "workspace-1"


def test_aggregate_api_fails_closed_when_phase3_is_disabled(monkeypatch) -> None:
    monkeypatch.setattr(
        api_module,
        "get_settings",
        lambda: SimpleNamespace(
            phase3_market_data_enabled=False,
            clickhouse_enabled=True,
        ),
    )
    start = datetime(2026, 9, 5, 3, 45, tzinfo=UTC)

    with TestClient(api_module.app) as client:
        response = client.get(
            "/api/data/candles/aggregate",
            params={
                "instrument_id": "NSE_EQ|RELIANCE",
                "interval": "5m",
                "window_start": start.isoformat(),
                "window_end": (start + timedelta(minutes=5)).isoformat(),
            },
        )

    assert response.status_code == 503
    assert response.json()["detail"] == "Phase 3 market data is unavailable"


def test_aggregate_api_rejects_naive_windows(monkeypatch) -> None:
    monkeypatch.setattr(
        api_module,
        "get_settings",
        lambda: SimpleNamespace(
            phase3_market_data_enabled=True,
            clickhouse_enabled=True,
        ),
    )

    with TestClient(api_module.app) as client:
        response = client.get(
            "/api/data/candles/aggregate",
            params={
                "instrument_id": "NSE_EQ|RELIANCE",
                "interval": "30m",
                "window_start": "2026-09-05T03:45:00",
                "window_end": "2026-09-05T04:15:00",
            },
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "aggregation window must be timezone-aware"
