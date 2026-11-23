import sqlite3
from contextlib import closing
from importlib import import_module

from fastapi.testclient import TestClient

from trade_data.config import Settings

api = import_module("trade_data.api.app")


def test_job_history_filters_retired_jobs_and_bounds_results(tmp_path, monkeypatch):
    history = tmp_path / "history"
    history.mkdir()
    with closing(sqlite3.connect(history / "runs.db")) as connection, connection:
        connection.execute(
            """
            CREATE TABLE runs (
                id INTEGER PRIMARY KEY, run_id TEXT, pipeline_name TEXT, status TEXT,
                start_time REAL, end_time REAL, create_timestamp TEXT
            )
            """
        )
        connection.executemany(
            "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (1, "daily", "nse_daily_bhavcopy_job", "SUCCESS", 1, 4, "1970-01-01 00:00:00"),
                (2, "repair", "nse_one_year_repair_job", "QUEUED", None, None, "1970-01-02"),
                (3, "old", "yfinance_daily_work_worker_job", "FAILURE", 6, 8, "1970-01-02"),
            ],
        )
    monkeypatch.setattr(
        api, "get_settings", lambda: Settings(_env_file=None, dagster_readonly_home=tmp_path)
    )
    with TestClient(api.app) as client:
        response = client.get("/api/jobs/latest", params={"limit": 1})
        assert response.status_code == 200
        assert response.json() == [
            {
                "id": "repair",
                "name": "nse_one_year_repair_job",
                "status": "queued",
                "created_at": "1970-01-02T00:00:00Z",
                "started_at": None,
                "finished_at": None,
            }
        ]
        assert [run["id"] for run in client.get("/api/jobs/latest").json()] == ["repair", "daily"]
        assert client.get("/api/jobs/latest", params={"limit": 51}).status_code == 422


def test_jobs_reports_unavailable_storage(tmp_path, monkeypatch):
    monkeypatch.setattr(
        api, "get_settings", lambda: Settings(_env_file=None, dagster_readonly_home=tmp_path)
    )
    with TestClient(api.app) as client:
        response = client.get("/api/jobs/latest")
    assert response.status_code == 503
    assert response.json() == {"detail": "Dagster run history unavailable"}
