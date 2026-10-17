from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import SQLAlchemyError

from trade_data.api import app as api_module


class EmptyStore:
    def market_status(self) -> list[dict]:
        return []

    def candles(self, ticker: str) -> list[dict]:
        return []

    def latest_runs(self) -> list[dict]:
        return []


class FailingStore(EmptyStore):
    def market_status(self) -> list[dict]:
        raise SQLAlchemyError("database unavailable")


@pytest.fixture
def production(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        api_module,
        "get_settings",
        lambda: SimpleNamespace(app_env="production"),
    )


def test_production_market_status_never_returns_demo_rows(
    monkeypatch: pytest.MonkeyPatch,
    production: None,
) -> None:
    monkeypatch.setattr(api_module, "_store", lambda: EmptyStore())
    assert api_module.market_status() == []

    monkeypatch.setattr(api_module, "_store", lambda: FailingStore())
    with pytest.raises(HTTPException) as exc_info:
        api_module.market_status()
    assert exc_info.value.status_code == 503


def test_daily_api_excludes_discarded_and_research_routes() -> None:
    from fastapi.testclient import TestClient
    with TestClient(api_module.app) as client:
        for path in (
            "/api/chat/health", "/api/filings/health", "/api/research/notes",
            "/api/opportunities/daily", "/api/screeners/intraday-range/latest",
            "/api/symbols/MISSING.NS/candles",
        ):
            assert client.get(path).status_code == 404, path


def test_local_endpoints_do_not_generate_market_or_job_data(monkeypatch) -> None:
    monkeypatch.setattr(api_module, "get_settings", lambda: SimpleNamespace(app_env="local"))
    monkeypatch.setattr(api_module, "_store", lambda: EmptyStore())
    assert api_module.market_status() == []
    assert api_module.latest_jobs() == []
    monkeypatch.setattr(api_module, "_store", lambda: FailingStore())
    with pytest.raises(HTTPException) as unavailable_error:
        api_module.market_status()
    assert unavailable_error.value.status_code == 503
