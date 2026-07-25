from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dagster._core.definitions.run_request import InstigatorType

from trade_research.config import Settings
from trade_research.dagster.schedule_policy import desired_schedule_statuses
from trade_research.dagster.status import CURRENT_ORIGIN_MARKER


@dataclass(frozen=True)
class ScheduleReconciliationAction:
    action: str
    schedule_name: str
    origin_id: str
    selector_id: str


@dataclass(frozen=True)
class ScheduleReconciliationPlan:
    repository_origin_id: str
    repository_selector_id: str
    actions: tuple[ScheduleReconciliationAction, ...]
    unmanaged_active_schedules: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "repository_origin_id": self.repository_origin_id,
            "repository_selector_id": self.repository_selector_id,
            "actions": [asdict(action) for action in self.actions],
            "unmanaged_active_schedules": list(self.unmanaged_active_schedules),
        }


def build_schedule_reconciliation_plan(
    instance: Any,
    remote_repository: Any,
    settings: Settings,
) -> ScheduleReconciliationPlan:
    remote_schedules = {
        schedule.name: schedule for schedule in remote_repository.get_schedules()
    }
    desired = desired_schedule_statuses(settings)
    stored_states = list(
        instance.all_instigator_state(instigator_type=InstigatorType.SCHEDULE)
    )
    actions: list[ScheduleReconciliationAction] = []

    for schedule_name, remote_schedule in remote_schedules.items():
        desired_status = desired.get(schedule_name, "stopped")
        current_origin_id = remote_schedule.get_remote_origin_id()
        current_state = next(
            (
                state
                for state in stored_states
                if state.instigator_origin_id == current_origin_id
            ),
            None,
        )
        current_running = bool(current_state and current_state.is_running)
        if desired_status == "running" and not current_running:
            actions.append(
                ScheduleReconciliationAction(
                    action="start_current",
                    schedule_name=schedule_name,
                    origin_id=current_origin_id,
                    selector_id=remote_schedule.selector_id,
                )
            )
        elif desired_status == "stopped" and current_running:
            actions.append(
                ScheduleReconciliationAction(
                    action="stop_current",
                    schedule_name=schedule_name,
                    origin_id=current_origin_id,
                    selector_id=remote_schedule.selector_id,
                )
            )

        stale_states = [
            state
            for state in stored_states
            if state.instigator_name == schedule_name
            and state.instigator_origin_id != current_origin_id
        ]
        actions.extend(
            ScheduleReconciliationAction(
                action="stop_stale",
                schedule_name=schedule_name,
                origin_id=state.instigator_origin_id,
                selector_id=state.selector_id,
            )
            for state in stale_states
            if state.is_running
        )
        actions.extend(
            ScheduleReconciliationAction(
                action="delete_stale",
                schedule_name=schedule_name,
                origin_id=state.instigator_origin_id,
                selector_id=state.selector_id,
            )
            for state in stale_states
        )

    managed_names = set(remote_schedules)
    unmanaged_active = sorted(
        {
            state.instigator_name
            for state in stored_states
            if state.is_running and state.instigator_name not in managed_names
        }
    )
    actions.sort(
        key=lambda action: (
            {
                "stop_stale": 0,
                "delete_stale": 1,
                "start_current": 2,
                "stop_current": 3,
            }[action.action],
            action.schedule_name,
            action.origin_id,
        )
    )
    return ScheduleReconciliationPlan(
        repository_origin_id=remote_repository.get_remote_origin_id(),
        repository_selector_id=remote_repository.selector_id,
        actions=tuple(actions),
        unmanaged_active_schedules=tuple(unmanaged_active),
    )


def apply_schedule_reconciliation_plan(
    instance: Any,
    remote_repository: Any,
    plan: ScheduleReconciliationPlan,
    *,
    marker_path: Path,
) -> None:
    remote_schedules = {
        schedule.name: schedule for schedule in remote_repository.get_schedules()
    }
    for action in plan.actions:
        remote_schedule = remote_schedules[action.schedule_name]
        if action.action == "start_current":
            instance.start_schedule(remote_schedule)
        elif action.action == "stop_current":
            instance.stop_schedule(
                action.origin_id,
                action.selector_id,
                remote_schedule,
            )
        elif action.action == "stop_stale":
            instance.stop_schedule(
                action.origin_id,
                action.selector_id,
                None,
            )
        elif action.action == "delete_stale":
            instance.delete_instigator_state(
                action.origin_id,
                action.selector_id,
            )
        else:
            raise ValueError(f"Unsupported schedule reconciliation action: {action.action}")
    write_current_origin_marker(
        marker_path,
        plan,
        remote_schedules,
    )


def write_current_origin_marker(
    marker_path: Path,
    plan: ScheduleReconciliationPlan,
    remote_schedules: dict[str, Any],
) -> None:
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "repository_origin_id": plan.repository_origin_id,
        "repository_selector_id": plan.repository_selector_id,
        "schedules": {
            name: {
                "origin_id": schedule.get_remote_origin_id(),
                "selector_id": schedule.selector_id,
            }
            for name, schedule in sorted(remote_schedules.items())
        },
    }
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = marker_path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(marker_path)


def default_origin_marker_path(dagster_home: Path) -> Path:
    return dagster_home / CURRENT_ORIGIN_MARKER


def recent_daemon_heartbeats(
    instance: Any,
    *,
    maximum_age_seconds: int = 90,
    now: datetime | None = None,
) -> list[str]:
    observed_at = now or datetime.now(UTC)
    recent: list[str] = []
    for daemon_type, heartbeat in instance.get_daemon_heartbeats().items():
        heartbeat_at = datetime.fromtimestamp(float(heartbeat.timestamp), tz=UTC)
        if (observed_at - heartbeat_at).total_seconds() <= maximum_age_seconds:
            recent.append(str(daemon_type))
    return sorted(recent)
