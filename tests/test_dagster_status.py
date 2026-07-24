import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from trade_research.dagster.status import read_dagster_schedule_statuses


def test_status_reader_reports_stale_origin_ticks_and_runs(tmp_path: Path) -> None:
    home = tmp_path / "dagster"
    schedules_path = home / "schedules/schedules.db"
    runs_path = home / "history/runs.db"
    schedules_path.parent.mkdir(parents=True)
    runs_path.parent.mkdir(parents=True)
    (home / "schedule_current_origin.json").write_text(
        json.dumps({"repository_origin_id": "current-origin"}),
        encoding="utf-8",
    )

    with sqlite3.connect(schedules_path) as connection:
        connection.executescript(
            """
            CREATE TABLE jobs (
                job_origin_id TEXT,
                selector_id TEXT,
                repository_origin_id TEXT,
                status TEXT,
                job_type TEXT,
                job_body TEXT,
                update_timestamp TEXT
            );
            CREATE TABLE job_ticks (
                job_origin_id TEXT,
                selector_id TEXT,
                status TEXT,
                timestamp REAL
            );
            """
        )
        connection.execute(
            "INSERT INTO jobs VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "old-worker-origin",
                "worker-selector",
                "stale-origin",
                "RUNNING",
                "SCHEDULE",
                json.dumps(
                    {
                        "origin": {
                            "instigator_name": "yfinance_daily_work_worker_schedule"
                        }
                    }
                ),
                "2026-07-24 06:00:00",
            ),
        )
        connection.execute(
            "INSERT INTO job_ticks VALUES (?, ?, ?, ?)",
            (
                "old-worker-origin",
                "worker-selector",
                "SUCCESS",
                datetime(2026, 7, 24, 6, 5, tzinfo=UTC).timestamp(),
            ),
        )

    with sqlite3.connect(runs_path) as connection:
        connection.execute(
            """
            CREATE TABLE runs (
                id INTEGER PRIMARY KEY,
                pipeline_name TEXT,
                status TEXT,
                start_time REAL,
                end_time REAL,
                create_timestamp TEXT
            )
            """
        )
        connection.executemany(
            "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    1,
                    "yfinance_daily_work_worker_job",
                    "SUCCESS",
                    None,
                    datetime(2026, 7, 24, 6, 0, tzinfo=UTC).timestamp(),
                    "2026-07-24 06:00:00",
                ),
                (
                    2,
                    "yfinance_daily_work_worker_job",
                    "FAILURE",
                    None,
                    datetime(2026, 7, 24, 6, 5, tzinfo=UTC).timestamp(),
                    "2026-07-24 06:05:00",
                ),
            ],
        )

    statuses = read_dagster_schedule_statuses(
        home,
        {
            "yfinance_daily_work_worker_schedule": (
                "yfinance_daily_work_worker_job"
            )
        },
    )

    worker = statuses["yfinance_daily_work_worker_schedule"]
    assert worker.actual_status == "running"
    assert worker.origin_health == "stale"
    assert worker.origin_drift is True
    assert worker.last_tick_status == "success"
    assert worker.last_run_status == "failure"
    assert worker.last_successful_run_at == datetime(
        2026,
        7,
        24,
        6,
        0,
        tzinfo=UTC,
    )


def test_status_reader_returns_empty_when_storage_is_unavailable(
    tmp_path: Path,
) -> None:
    assert read_dagster_schedule_statuses(
        tmp_path / "missing",
        {"schedule": "job"},
    ) == {}
