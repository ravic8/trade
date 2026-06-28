from fastapi.testclient import TestClient

from trade_research.api.app import app


def test_research_progress_endpoint() -> None:
    with TestClient(app) as client:
        response = client.get("/api/research/progress")

    assert response.status_code == 200
    payload = response.json()
    assert "steps" in payload
    assert payload["step_count"] >= 7


def test_research_factor_ic_rejects_bad_direction() -> None:
    with TestClient(app) as client:
        response = client.get("/api/research/factors/ic?direction=sideways")

    assert response.status_code == 400
