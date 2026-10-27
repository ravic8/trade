from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from trade_data.api import app as api
from trade_data.storage.timescale import ohlcv_daily_table


def test_empty_storage_is_empty_and_unreachable_storage_is_unknown(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    ohlcv_daily_table.create(engine)
    monkeypatch.setattr(api, "_store", lambda: SimpleNamespace(engine=engine))
    assert api.market_status() == []

    def unavailable():
        raise SQLAlchemyError("private connection detail")

    monkeypatch.setattr(api, "_store", unavailable)
    with pytest.raises(HTTPException) as error:
        api.market_status()
    assert error.value.status_code == 503
    assert "private" not in error.value.detail


def test_api_has_no_legacy_provider_mutations_or_research_routes():
    with TestClient(api.app) as client:
        for path in (
            "/api/chat/health",
            "/api/filings/health",
            "/api/research/notes",
            "/api/admin/provider-credentials/upstox/status",
            "/api/data/provider-history",
            "/api/data/bulk-fetch",
        ):
            assert client.get(path).status_code == 404
