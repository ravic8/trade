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


def test_research_ml_summary_endpoint() -> None:
    with TestClient(app) as client:
        response = client.get("/api/research/ml/summary")

    assert response.status_code == 200
    payload = response.json()
    assert "assumptions" in payload
    assert "model_runs" in payload


def test_research_ml_model_metrics_rejects_bad_run() -> None:
    with TestClient(app) as client:
        response = client.get("/api/research/ml/model-metrics?run=xgboost")

    assert response.status_code == 400


def test_research_ml_latest_candidates_endpoint() -> None:
    with TestClient(app) as client:
        response = client.get("/api/research/ml/latest-candidates?run=baselines&top_n=5")

    assert response.status_code == 200
    payload = response.json()
    assert "models" in payload
    assert payload["run"] == "baselines"


def test_research_ml_robustness_rejects_bad_group() -> None:
    with TestClient(app) as client:
        response = client.get("/api/research/ml/robustness?group=xgboost")

    assert response.status_code == 400
