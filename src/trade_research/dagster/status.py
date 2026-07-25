from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

CURRENT_ORIGIN_MARKER = "schedule_current_origin.json"
RUNNING_STATUSES = {"RUNNING", "AUTOMATICALLY_RUNNING"}


@dataclass(frozen=True)
class DagsterScheduleStatus:
    actual_status: str
    origin_health: str
    origin_drift: bool | None
    stored_origin_count: int
    active_origin_count: int
    last_tick_status: str | None = None
    last_tick_at: datetime | None = None
    last_run_status: str | None = None
    last_run_at: datetime | None = None
    last_successful_run_at: datetime | None = None


def read_dagster_schedule_statuses(
    dagster_home: Path | str | None,
    schedule_jobs: Mapping[str, str],
) -> dict[str, DagsterScheduleStatus]:
    if dagster_home is None:
        return {}
    home = Path(dagster_home)
    schedules_path = home / "schedules/schedules.db"
    if not schedules_path.exists():
        return {}

    current_repository_origin_id = _read_current_repository_origin(home)
    try:
        with closing(_readonly_sqlite(schedules_path)) as connection:
            stored_rows = connection.execute(
                """
                SELECT
                    job_origin_id,
                    selector_id,
                    repository_origin_id,
                    status,
                    job_body,
                    update_timestamp
                FROM jobs
                WHERE job_type = 'SCHEDULE'
                """
            ).fetchall()
            ticks = _latest_ticks(connection)
    except (sqlite3.Error, OSError, ValueError):
        return {}

    rows_by_name: dict[str, list[sqlite3.Row]] = {}
    for row in stored_rows:
        name = _instigator_name(row["job_body"])
        if name:
            rows_by_name.setdefault(name, []).append(row)

    run_statuses = _read_run_statuses(home / "history/runs.db", schedule_jobs)
    result: dict[str, DagsterScheduleStatus] = {}
    for schedule_name, job_name in schedule_jobs.items():
        rows = rows_by_name.get(schedule_name, [])
        active_rows = [row for row in rows if str(row["status"]) in RUNNING_STATUSES]
        if active_rows:
            actual_status = "running"
        else:
            # All repository schedules are stopped by definition default. If a
            # managed schedule has no stored instigator row, Dagster is using
            # that stopped default rather than an unknown runtime state.
            actual_status = "stopped"
        origin_health = _origin_health(
            rows,
            active_rows,
            current_repository_origin_id,
        )
        latest_tick = max(
            (
                ticks[str(row["selector_id"])]
                for row in rows
                if str(row["selector_id"]) in ticks
            ),
            key=lambda item: _timestamp_sort_key(item["timestamp"]),
            default=None,
        )
        job_runs = run_statuses.get(job_name, {})
        result[schedule_name] = DagsterScheduleStatus(
            actual_status=actual_status,
            origin_health=origin_health,
            origin_drift=(
                origin_health in {"stale", "mixed"}
                if origin_health != "unknown"
                else None
            ),
            stored_origin_count=len(
                {str(row["repository_origin_id"]) for row in rows}
            ),
            active_origin_count=len(
                {str(row["repository_origin_id"]) for row in active_rows}
            ),
            last_tick_status=(
                str(latest_tick["status"]).lower() if latest_tick else None
            ),
            last_tick_at=(
                _timestamp_to_datetime(latest_tick["timestamp"])
                if latest_tick
                else None
            ),
            last_run_status=job_runs.get("last_run_status"),
            last_run_at=job_runs.get("last_run_at"),
            last_successful_run_at=job_runs.get("last_successful_run_at"),
        )
    return result


def _readonly_sqlite(path: Path) -> sqlite3.Connection:
    # Dagster configures these databases in WAL mode. Even a read-only SQLite
    # connection otherwise tries to create a shared-memory sidecar, which fails
    # when production mounts DAGSTER_HOME into the API container as read-only.
    # Each API request opens a fresh connection, so immutable mode still observes
    # the latest checkpointed database snapshot without weakening the mount.
    connection = sqlite3.connect(
        f"file:{path.resolve()}?mode=ro&immutable=1",
        uri=True,
        timeout=1,
    )
    connection.row_factory = sqlite3.Row
    return connection


def _read_current_repository_origin(home: Path) -> str | None:
    marker = home / CURRENT_ORIGIN_MARKER
    if not marker.exists():
        return None
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    value = payload.get("repository_origin_id")
    return str(value) if value else None


def _instigator_name(serialized: str | bytes | None) -> str | None:
    if not serialized:
        return None
    try:
        payload = json.loads(serialized)
    except (json.JSONDecodeError, TypeError):
        return None
    value = _find_named_value(payload, "instigator_name")
    if value is None and isinstance(payload, dict):
        # Current Dagster serde names schedules through ExternalJobOrigin.
        # Older releases used ``instigator_name`` elsewhere in the payload, so
        # retain that lookup first and fall back to the production shape.
        value = _find_named_value(payload.get("origin"), "job_name")
    return str(value) if value else None


def _find_named_value(value: Any, name: str) -> Any:
    if isinstance(value, dict):
        if name in value:
            return value[name]
        for nested in value.values():
            found = _find_named_value(nested, name)
            if found is not None:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = _find_named_value(nested, name)
            if found is not None:
                return found
    return None


def _latest_ticks(connection: sqlite3.Connection) -> dict[str, sqlite3.Row]:
    rows = connection.execute(
        """
        SELECT selector_id, status, timestamp
        FROM job_ticks
        """
    ).fetchall()
    latest: dict[str, sqlite3.Row] = {}
    for row in rows:
        selector_id = str(row["selector_id"])
        existing = latest.get(selector_id)
        if existing is None or _timestamp_sort_key(
            row["timestamp"]
        ) > _timestamp_sort_key(existing["timestamp"]):
            latest[selector_id] = row
    return latest


def _origin_health(
    rows: list[sqlite3.Row],
    active_rows: list[sqlite3.Row],
    current_repository_origin_id: str | None,
) -> str:
    if current_repository_origin_id is None:
        return "unknown"
    if not rows:
        # No stored state means the current definition's stopped default is in
        # effect and there is no stale origin capable of launching ticks.
        return "current"
    relevant = active_rows or rows
    origins = {str(row["repository_origin_id"]) for row in relevant}
    if origins == {current_repository_origin_id}:
        return "current"
    if current_repository_origin_id in origins:
        return "mixed"
    return "stale"


def _read_run_statuses(
    runs_path: Path,
    schedule_jobs: Mapping[str, str],
) -> dict[str, dict[str, Any]]:
    if not runs_path.exists():
        return {}
    job_names = sorted(set(schedule_jobs.values()))
    if not job_names:
        return {}
    placeholders = ",".join("?" for _ in job_names)
    try:
        with closing(_readonly_sqlite(runs_path)) as connection:
            rows = connection.execute(
                f"""
                SELECT pipeline_name, status, start_time, end_time, create_timestamp
                FROM runs
                WHERE pipeline_name IN ({placeholders})
                ORDER BY id DESC
                """,
                job_names,
            ).fetchall()
    except (sqlite3.Error, OSError):
        return {}

    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        job_name = str(row["pipeline_name"])
        entry = result.setdefault(job_name, {})
        observed_at = _run_observed_at(row)
        if "last_run_status" not in entry:
            entry["last_run_status"] = str(row["status"]).lower()
            entry["last_run_at"] = observed_at
        if str(row["status"]) == "SUCCESS" and "last_successful_run_at" not in entry:
            entry["last_successful_run_at"] = observed_at
    return result


def _run_observed_at(row: sqlite3.Row) -> datetime | None:
    for column in ("end_time", "start_time"):
        value = row[column]
        if value is not None:
            return _timestamp_to_datetime(value)
    raw = row["create_timestamp"]
    if raw:
        try:
            parsed = datetime.fromisoformat(str(raw))
            return parsed.replace(tzinfo=parsed.tzinfo or UTC).astimezone(UTC)
        except ValueError:
            return None
    return None


def _timestamp_to_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=value.tzinfo or UTC).astimezone(UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            pass
        else:
            return parsed.replace(tzinfo=parsed.tzinfo or UTC).astimezone(UTC)
    try:
        return datetime.fromtimestamp(float(value), tz=UTC)
    except (TypeError, ValueError, OverflowError):
        return None


def _timestamp_sort_key(value: Any) -> datetime:
    return _timestamp_to_datetime(value) or datetime.min.replace(tzinfo=UTC)
