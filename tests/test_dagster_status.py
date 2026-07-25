import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from trade_research.dagster.status import (
    _instigator_name,
    _readonly_sqlite,
    read_dagster_schedule_statuses,
)


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
                            "__class__": "ExternalJobOrigin",
                            "external_repository_origin": {
                                "__class__": "ExternalRepositoryOrigin",
                                "repository_name": "__repository__",
                            },
                            "job_name": "yfinance_daily_work_worker_schedule",
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
                "2026-07-24 06:05:00.000000",
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
            ),
            "stopped_by_default_schedule": "stopped_by_default_job",
        },
    )

    worker = statuses["yfinance_daily_work_worker_schedule"]
    assert worker.actual_status == "running"
    assert worker.origin_health == "stale"
    assert worker.origin_drift is True
    assert worker.last_tick_status == "success"
    assert worker.last_tick_at == datetime(2026, 7, 24, 6, 5, tzinfo=UTC)
    assert worker.last_run_status == "failure"
    assert worker.last_successful_run_at == datetime(
        2026,
        7,
        24,
        6,
        0,
        tzinfo=UTC,
    )
    stopped = statuses["stopped_by_default_schedule"]
    assert stopped.actual_status == "stopped"
    assert stopped.origin_health == "current"
    assert stopped.origin_drift is False
    assert stopped.stored_origin_count == 0
    assert stopped.active_origin_count == 0


def test_status_reader_returns_empty_when_storage_is_unavailable(
    tmp_path: Path,
) -> None:
    assert read_dagster_schedule_statuses(
        tmp_path / "missing",
        {"schedule": "job"},
    ) == {}


def test_instigator_name_supports_legacy_serialized_shape() -> None:
    assert (
        _instigator_name(
            json.dumps(
                {
                    "origin": {
                        "instigator_name": "legacy_schedule",
                    }
                }
            )
        )
        == "legacy_schedule"
    )


def test_status_reader_ignores_invalid_tick_timestamp(tmp_path: Path) -> None:
    home = tmp_path / "dagster"
    schedules_path = home / "schedules/schedules.db"
    schedules_path.parent.mkdir(parents=True)
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
                timestamp TEXT
            );
            """
        )
        connection.execute(
            "INSERT INTO jobs VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "worker-origin",
                "worker-selector",
                "repository-origin",
                "RUNNING",
                "SCHEDULE",
                json.dumps(
                    {
                        "origin": {
                            "job_name": "worker_schedule",
                        }
                    }
                ),
                "2026-07-24 06:00:00",
            ),
        )
        connection.execute(
            "INSERT INTO job_ticks VALUES (?, ?, ?, ?)",
            (
                "worker-origin",
                "worker-selector",
                "SUCCESS",
                "not-a-timestamp",
            ),
        )

    worker = read_dagster_schedule_statuses(
        home,
        {"worker_schedule": "worker_job"},
    )["worker_schedule"]

    assert worker.actual_status == "running"
    assert worker.last_tick_status == "success"
    assert worker.last_tick_at is None


def test_status_reader_follows_ticks_across_origin_migration(tmp_path: Path) -> None:
    home = tmp_path / "dagster"
    schedules_path = home / "schedules/schedules.db"
    schedules_path.parent.mkdir(parents=True)
    (home / "schedule_current_origin.json").write_text(
        json.dumps({"repository_origin_id": "current-repository-origin"}),
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
                timestamp TEXT
            );
            """
        )
        connection.execute(
            "INSERT INTO jobs VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "current-worker-origin",
                "stable-worker-selector",
                "current-repository-origin",
                "RUNNING",
                "SCHEDULE",
                json.dumps(
                    {
                        "origin": {
                            "job_name": "worker_schedule",
                        }
                    }
                ),
                "2026-07-25 17:38:00",
            ),
        )
        connection.execute(
            "INSERT INTO job_ticks VALUES (?, ?, ?, ?)",
            (
                "stale-worker-origin",
                "stable-worker-selector",
                "SUCCESS",
                "2026-07-25 17:40:00",
            ),
        )

    worker = read_dagster_schedule_statuses(
        home,
        {"worker_schedule": "worker_job"},
    )["worker_schedule"]

    assert worker.origin_health == "current"
    assert worker.origin_drift is False
    assert worker.last_tick_status == "success"
    assert worker.last_tick_at == datetime(2026, 7, 25, 17, 40, tzinfo=UTC)


def test_readonly_sqlite_opens_wal_database_on_readonly_mount(
    tmp_path: Path,
) -> None:
    database_dir = tmp_path / "readonly"
    database_dir.mkdir()
    database_path = database_dir / "dagster.db"
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        connection.execute("CREATE TABLE values_table (value INTEGER)")
        connection.execute("INSERT INTO values_table VALUES (1)")
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{database_path}{suffix}")
        if sidecar.exists():
            sidecar.unlink()
    database_path.chmod(0o444)
    database_dir.chmod(0o555)
    try:
        with _readonly_sqlite(database_path) as connection:
            assert connection.execute("SELECT value FROM values_table").fetchone()[
                0
            ] == 1
    finally:
        database_dir.chmod(0o755)
        database_path.chmod(0o644)
