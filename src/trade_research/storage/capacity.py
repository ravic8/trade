from __future__ import annotations

import json
import os
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text


def project_analytical_capacity(
    *,
    instrument_count: int,
    feature_count: int,
    retention_years: int,
    sessions_per_year: int = 252,
) -> dict[str, int]:
    values = (instrument_count, feature_count, retention_years, sessions_per_year)
    if any(value < 1 for value in values):
        raise ValueError("capacity projection inputs must be positive")
    feature_rows = instrument_count * feature_count * retention_years * sessions_per_year
    prediction_rows = instrument_count * retention_years * sessions_per_year
    return {
        "feature_rows": feature_rows,
        "prediction_rows_per_model": prediction_rows,
        "uncompressed_feature_bytes_lower": feature_rows * 48,
        "uncompressed_feature_bytes_upper": feature_rows * 120,
    }


def _directory_size(path: Path) -> int | None:
    if not path.exists():
        return None
    completed = subprocess.run(
        ["du", "-sk", str(path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if completed.returncode != 0:
        return None
    return int(completed.stdout.split()[0]) * 1024


def _disk(path: Path) -> dict[str, Any]:
    candidate = path
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    usage = shutil.disk_usage(candidate)
    return {
        "configured_path": str(path),
        "measured_at": str(candidate),
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "directory_bytes": _directory_size(path),
    }


def _container_metrics() -> list[dict[str, Any]]:
    try:
        completed = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return []
    if completed.returncode != 0:
        return []
    return [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]


def _memory_total_bytes() -> int | None:
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError, AttributeError):
        return None


def collect_capacity_report(
    *,
    database_url: str,
    paths: dict[str, Path],
    projected_feature_count: int,
    retention_years: int,
) -> dict[str, Any]:
    engine = create_engine(database_url, pool_pre_ping=True, hide_parameters=True)
    with engine.begin() as connection:
        database = connection.execute(
            text(
                """
                SELECT
                    current_database() AS database_name,
                    pg_database_size(current_database()) AS database_bytes,
                    pg_total_relation_size('ohlcv_daily') AS ohlcv_table_bytes,
                    COUNT(*) AS ohlcv_rows,
                    COUNT(DISTINCT instrument_key) AS instrument_count,
                    COUNT(DISTINCT date) AS session_count,
                    MIN(date) AS first_session,
                    MAX(date) AS latest_session
                FROM ohlcv_daily
                """
            )
        ).mappings().one()
    engine.dispose()

    database_metrics = {
        key: value.isoformat() if hasattr(value, "isoformat") else value
        for key, value in database.items()
    }
    projection = project_analytical_capacity(
        instrument_count=max(1, int(database_metrics["instrument_count"])),
        feature_count=projected_feature_count,
        retention_years=retention_years,
    )
    with ThreadPoolExecutor(max_workers=len(paths)) as executor:
        measured = executor.map(_disk, paths.values())
    disks = dict(zip(paths, measured, strict=True))
    persisted_bytes = sum(
        metric["directory_bytes"] or 0
        for name, metric in disks.items()
        if name in {"postgres", "clickhouse", "minio"}
    )
    backup_required_bytes = max(
        persisted_bytes,
        int(database_metrics["database_bytes"] or 0),
    )
    backup_free_bytes = disks["backups"]["free_bytes"]
    return {
        "schema_version": 1,
        "captured_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "host": {
            "cpu_count": os.cpu_count(),
            "load_average": list(os.getloadavg()) if hasattr(os, "getloadavg") else None,
            "memory_total_bytes": _memory_total_bytes(),
        },
        "postgresql": database_metrics,
        "disk": disks,
        "containers": _container_metrics(),
        "projection": {
            **projection,
            "feature_count": projected_feature_count,
            "retention_years": retention_years,
        },
        "backup": {
            "estimated_full_backup_bytes": backup_required_bytes,
            "backup_filesystem_free_bytes": backup_free_bytes,
            "two_copy_headroom_available": backup_free_bytes >= backup_required_bytes * 2,
            "note": "Estimate excludes compression and requires measured drill duration.",
        },
    }
