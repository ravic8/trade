import json
from datetime import date
from importlib import import_module
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

api = import_module("trade_data.api.app")
PARAMS = {
    "instrument_key": "YF|RELIANCE.NS",
    "start_date": "2026-10-15",
    "end_date": "2026-10-16",
}


def test_daily_candle_read_matches_consumer_fixture_and_scopes_storage(monkeypatch):
    fixture_path = Path(__file__).parent / "fixtures/daily_candles_response.json"
    fixture = json.loads(fixture_path.read_text())
    calls = []

    class Store:
        def daily_ohlcv_frame(self, **kwargs):
            calls.append(kwargs)
            return pd.DataFrame(fixture["rows"])

    monkeypatch.setattr(api, "_store", Store)
    with TestClient(api.app) as client:
        response = client.get("/api/data/candles", params=PARAMS)
    assert response.status_code == 200
    assert response.json() == fixture
    assert calls == [{
        "exchange": "NSE", "source": "yfinance",
        "start_date": date(2026, 10, 15), "end_date": date(2026, 10, 16),
        "instrument_keys": ["YF|RELIANCE.NS"],
    }]


@pytest.mark.parametrize("params,status", [
    ({"end_date": "2026-10-14"}, 400),
    ({"start_date": "2000-01-01"}, 400),
    ({"start_date": "0001-01-01"}, 400),
    ({"instrument_key": " "}, 400),
    ({"exchange": "FX"}, 400),
    ({"provider": "upstox", "exchange": "US"}, 400),
    ({"provider": "unknown"}, 422),
])
def test_unbounded_or_unsupported_reads_fail_before_storage(monkeypatch, params, status):
    monkeypatch.setattr(api, "_store", lambda: pytest.fail("Unexpected storage access"))
    with TestClient(api.app) as client:
        assert client.get("/api/data/candles", params={**PARAMS, **params}).status_code == status


def test_daily_candle_read_empty_is_honest_and_failure_is_unavailable(monkeypatch):
    class Store:
        def daily_ohlcv_frame(self, **kwargs):
            return pd.DataFrame()

    monkeypatch.setattr(api, "_store", Store)
    with TestClient(api.app) as client:
        assert client.get("/api/data/candles", params=PARAMS).json()["rows"] == []

        def unavailable():
            raise SQLAlchemyError("private connection details")

        monkeypatch.setattr(api, "_store", unavailable)
        response = client.get("/api/data/candles", params=PARAMS)
        assert response.status_code == 503
        assert "private" not in response.text


def test_official_nse_source_is_explicit_and_never_falls_back_to_yahoo(monkeypatch):
    calls = []

    class Store:
        def daily_ohlcv_frame(self, **kwargs):
            calls.append(kwargs)
            return pd.DataFrame()

    monkeypatch.setattr(api, "_store", Store)
    with TestClient(api.app) as client:
        response = client.get("/api/data/candles", params={
            **PARAMS, "provider": "nse_official_bhavcopy",
        })
        assert response.status_code == 200
        assert response.json()["provider"] == "nse_official_bhavcopy"
        assert response.json()["rows"] == []
        assert calls[0]["source"] == "nse_official_bhavcopy"
        assert client.get("/api/data/candles", params={
            **PARAMS, "provider": "nse_official_bhavcopy", "exchange": "US",
        }).status_code == 400
        assert len(calls) == 1
