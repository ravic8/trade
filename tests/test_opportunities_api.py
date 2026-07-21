from datetime import date

from fastapi.testclient import TestClient

from trade_research.api.app import app
from trade_research.targets import DAILY_OPPORTUNITY_TARGET_VERSION_V1_0


class OpportunityStore:
    def __init__(self) -> None:
        self.params: dict | None = None

    def opportunity_targets_page(self, **params) -> dict:
        self.params = params
        if params["sort_by"] == "not_a_target":
            raise ValueError("Unsupported opportunity sort column: not_a_target")
        return {
            "exchange": params["exchange"],
            "source": params["source"],
            "target_version": params["target_version"],
            "session_date": params["session_date"] or date(2026, 7, 17),
            "total": 1,
            "summary": {
                "average_return": 0.02,
                "positive_sessions": 1,
                "positive_session_ratio": 1.0,
            },
            "rows": [
                {
                    "instrument_key": "YF|SHOP.TO",
                    "source": "yfinance",
                    "date": date(2026, 7, 17),
                    "target_version": params["target_version"],
                    "symbol": "SHOP.TO",
                    "exchange": "TSX",
                    "open": 100.0,
                    "high": 105.0,
                    "low": 99.0,
                    "close": 102.0,
                    "previous_close": 101.0,
                    "session_return": 0.02,
                    "gap": -0.01,
                    "true_return": 0.01,
                    "upside": 0.05,
                    "downside": 0.01,
                    "giveback": 0.03,
                    "recovery": 0.03,
                    "session_range": 0.06,
                    "true_upside": 0.05,
                    "true_downside": 0.02,
                    "true_range": 0.07,
                }
            ],
        }


def test_daily_opportunities_exposes_pdf_target_variables(monkeypatch) -> None:
    store = OpportunityStore()
    monkeypatch.setattr("trade_research.api.app._store", lambda: store)

    with TestClient(app) as client:
        response = client.get(
            "/api/opportunities/daily",
            params={
                "exchange": "TSX",
                "session_date": "2026-07-17",
                "symbol": "SHOP",
                "sort_by": "upside",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["target_version"] == DAILY_OPPORTUNITY_TARGET_VERSION_V1_0
    assert payload["rows"][0]["true_range"] == 0.07
    assert store.params == {
        "target_version": DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
        "exchange": "TSX",
        "source": "yfinance",
        "session_date": date(2026, 7, 17),
        "symbol": "SHOP",
        "sort_by": "upside",
        "direction": "desc",
        "limit": 100,
        "offset": 0,
        "minimum_session_coverage": 0.95,
    }


def test_daily_opportunities_rejects_forex_and_unknown_sort(monkeypatch) -> None:
    store = OpportunityStore()
    monkeypatch.setattr("trade_research.api.app._store", lambda: store)

    with TestClient(app) as client:
        forex = client.get("/api/opportunities/daily?exchange=FOREX")
        bad_sort = client.get("/api/opportunities/daily?sort_by=not_a_target")

    assert forex.status_code == 400
    assert "NSE, TSX, or US" in forex.json()["detail"]
    assert bad_sort.status_code == 400
