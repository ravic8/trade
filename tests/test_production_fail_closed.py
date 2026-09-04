from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import SQLAlchemyError

from trade_research.api import app as api_module


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


def test_production_demo_only_endpoints_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    production: None,
) -> None:
    monkeypatch.setattr(api_module, "_store", lambda: EmptyStore())

    with pytest.raises(HTTPException) as screener_error:
        api_module.latest_intraday_range()
    assert screener_error.value.status_code == 503

    with pytest.raises(HTTPException) as candle_error:
        api_module.symbol_candles("MISSING.NS")
    assert candle_error.value.status_code == 404

    assert api_module.latest_jobs() == []
    assert api_module.research_notes() == []
