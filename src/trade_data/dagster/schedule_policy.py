from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from trade_data.config import Settings

MANIFEST_PATH = Path(__file__).with_name("manifests") / "production.json"


@dataclass(frozen=True)
class SchedulePolicy:
    schedule_name: str
    job_name: str
    cron_schedule: str
    execution_timezone: str
    exchange: str
    desired_status: str
    freshness_sla_minutes: int
    upstream_dependencies: tuple[str, ...]
    alert_owner: str
    notes: str


def load_schedule_manifest(path: Path = MANIFEST_PATH) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1 or payload.get("environment") != "production":
        raise ValueError("Unsupported Dagster schedule manifest")
    schedules = payload.get("schedules")
    if not isinstance(schedules, list) or not schedules:
        raise ValueError("Dagster schedule manifest must contain schedules")
    names = [str(row.get("schedule_name")) for row in schedules]
    if len(names) != len(set(names)):
        raise ValueError("Dagster schedule manifest contains duplicate schedule names")
    return payload


def schedule_policy(settings: Settings) -> list[SchedulePolicy]:
    manifest = load_schedule_manifest()
    conditions = _enabled_conditions(settings)
    rows: list[SchedulePolicy] = []
    for raw in manifest["schedules"]:
        condition = str(raw["enabled_when"])
        if condition not in conditions:
            raise ValueError(f"Unknown schedule enabled_when condition: {condition}")
        rows.append(
            SchedulePolicy(
                schedule_name=str(raw["schedule_name"]),
                job_name=str(raw["job_name"]),
                cron_schedule=str(raw["cron_schedule"]),
                execution_timezone=str(raw["execution_timezone"]),
                exchange=str(raw["exchange"]),
                desired_status=_status(conditions[condition]),
                freshness_sla_minutes=int(raw["freshness_sla_minutes"]),
                upstream_dependencies=tuple(str(value) for value in raw["upstream_dependencies"]),
                alert_owner=str(raw["alert_owner"]),
                notes=str(raw["notes"]),
            )
        )
    return rows


def desired_schedule_statuses(settings: Settings) -> dict[str, str]:
    return {row.schedule_name: row.desired_status for row in schedule_policy(settings)}


def _enabled_conditions(settings: Settings) -> dict[str, bool]:
    return {
        "nse_daily": settings.nse_daily_enabled,
        "nse_sessions": settings.materialized_exchange_sessions_enabled,
        "never": False,
    }


def _status(enabled: bool) -> str:
    return "running" if enabled else "stopped"
