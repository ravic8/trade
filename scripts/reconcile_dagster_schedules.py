#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from dagster import DagsterInstance
from dagster import __version__ as dagster_version
from dagster._cli.workspace.cli_target import (
    RepositoryOpts,
    get_repository_from_cli_opts,
)
from dagster_shared.cli import WorkspaceOpts

from trade_research.config import get_settings
from trade_research.dagster.reconcile import (
    apply_schedule_reconciliation_plan,
    build_schedule_reconciliation_plan,
    default_origin_marker_path,
    recent_daemon_heartbeats,
)

CONFIRMATION = "APPLY_SCHEDULE_RECONCILIATION"
DEFINITIONS_MODULE = "trade_research.dagster.definitions"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Preview or apply desired Dagster schedule state while stopping stale "
            "repository-origin records without deleting history."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the displayed plan. The default is read-only preview.",
    )
    parser.add_argument(
        "--confirm",
        help=f"Required with --apply; must equal {CONFIRMATION}.",
    )
    parser.add_argument(
        "--marker-path",
        type=Path,
        help="Override the current-origin marker path.",
    )
    args = parser.parse_args()

    dagster_home_value = os.environ.get("DAGSTER_HOME")
    if not dagster_home_value:
        parser.error("DAGSTER_HOME must be set.")
    dagster_home = Path(dagster_home_value)
    marker_path = args.marker_path or default_origin_marker_path(dagster_home)
    workspace_opts = WorkspaceOpts(module_name=(DEFINITIONS_MODULE,))
    repository_opts = RepositoryOpts()

    with (
        DagsterInstance.get() as instance,
        get_repository_from_cli_opts(
            instance=instance,
            version=dagster_version,
            workspace_opts=workspace_opts,
            repository_opts=repository_opts,
        ) as repository,
    ):
        plan = build_schedule_reconciliation_plan(
            instance,
            repository,
            get_settings(),
        )
        print(json.dumps(plan.as_dict(), indent=2))
        if not args.apply:
            print("Preview only; no schedule state was changed.")
            return 0
        if args.confirm != CONFIRMATION:
            parser.error(f"--apply requires --confirm {CONFIRMATION}")
        if plan.unmanaged_active_schedules:
            parser.error(
                "Refusing to apply while unmanaged schedules are active: "
                + ", ".join(plan.unmanaged_active_schedules)
            )
        active_daemons = recent_daemon_heartbeats(instance)
        if active_daemons:
            parser.error(
                "Recent Dagster daemon heartbeats detected. Stop the daemon before "
                "applying reconciliation: "
                + ", ".join(active_daemons)
            )
        apply_schedule_reconciliation_plan(
            instance,
            repository,
            plan,
            marker_path=marker_path,
        )
        print(
            f"Applied {len(plan.actions)} schedule actions and wrote {marker_path}."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
