import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from trade_research.config import Settings
from trade_research.dagster.reconcile import (
    apply_schedule_reconciliation_plan,
    build_schedule_reconciliation_plan,
    recent_daemon_heartbeats,
)


class _Schedule:
    def __init__(self, name: str, origin_id: str, selector_id: str) -> None:
        self.name = name
        self._origin_id = origin_id
        self.selector_id = selector_id

    def get_remote_origin_id(self) -> str:
        return self._origin_id


class _Repository:
    def __init__(self, schedules: list[_Schedule]) -> None:
        self._schedules = schedules
        self.selector_id = "current-repository-selector"

    def get_schedules(self) -> list[_Schedule]:
        return self._schedules

    def get_remote_origin_id(self) -> str:
        return "current-repository-origin"


class _Instance:
    def __init__(self, states: list[SimpleNamespace]) -> None:
        self.states = states
        self.started: list[str] = []
        self.stopped: list[tuple[str, str, str | None]] = []

    def all_instigator_state(self, **_kwargs):
        return self.states

    def start_schedule(self, schedule: _Schedule) -> None:
        self.started.append(schedule.name)

    def stop_schedule(self, origin_id, selector_id, schedule) -> None:
        self.stopped.append(
            (
                origin_id,
                selector_id,
                schedule.name if schedule is not None else None,
            )
        )


def _state(
    name: str,
    origin_id: str,
    selector_id: str,
    *,
    running: bool,
) -> SimpleNamespace:
    return SimpleNamespace(
        instigator_name=name,
        instigator_origin_id=origin_id,
        selector_id=selector_id,
        is_running=running,
    )


def test_reconciliation_starts_current_before_stopping_stale_origin(
    tmp_path: Path,
) -> None:
    worker = _Schedule(
        "yfinance_daily_work_worker_schedule",
        "current-worker-origin",
        "current-worker-selector",
    )
    repository = _Repository([worker])
    instance = _Instance(
        [
            _state(
                worker.name,
                "stale-worker-origin",
                "stale-worker-selector",
                running=True,
            )
        ]
    )
    settings = Settings(
        _env_file=None,
        yfinance_daily_enabled=True,
        yfinance_full_us_enabled=True,
    )

    plan = build_schedule_reconciliation_plan(instance, repository, settings)

    assert [action.action for action in plan.actions] == [
        "start_current",
        "stop_stale",
    ]
    marker = tmp_path / "schedule_current_origin.json"
    apply_schedule_reconciliation_plan(
        instance,
        repository,
        plan,
        marker_path=marker,
    )
    assert instance.started == [worker.name]
    assert instance.stopped == [
        ("stale-worker-origin", "stale-worker-selector", None)
    ]
    payload = json.loads(marker.read_text(encoding="utf-8"))
    assert payload["repository_origin_id"] == "current-repository-origin"
    assert payload["schedules"][worker.name]["origin_id"] == "current-worker-origin"


def test_reconciliation_does_not_delete_unmanaged_active_schedules() -> None:
    repository = _Repository([])
    instance = _Instance(
        [
            _state(
                "removed_schedule",
                "removed-origin",
                "removed-selector",
                running=True,
            )
        ]
    )

    plan = build_schedule_reconciliation_plan(
        instance,
        repository,
        Settings(_env_file=None),
    )

    assert plan.actions == ()
    assert plan.unmanaged_active_schedules == ("removed_schedule",)


def test_recent_daemon_heartbeat_blocks_live_reconciliation() -> None:
    now = datetime(2026, 7, 24, 8, 0, tzinfo=UTC)
    instance = SimpleNamespace(
        get_daemon_heartbeats=lambda: {
            "SCHEDULER": SimpleNamespace(timestamp=now.timestamp())
        }
    )

    assert recent_daemon_heartbeats(instance, now=now) == ["SCHEDULER"]
