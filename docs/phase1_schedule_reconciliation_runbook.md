---
document_status: current
last_verified_commit: e543a5799f3afc7ec8dcf115f4ae3cfb68d37cd8
last_verified_date: 2026-07-24
owner: trade-research-platform
replaced_by: null
---

# Phase 1 Dagster Schedule Reconciliation Runbook

## Purpose

This runbook moves active schedules from stale Dagster repository origins to
the current deployed origin without deleting schedule history. It also applies
the desired schedule policy derived from production feature flags.

The reconciler is preview-only by default. Applying it is a production state
change and must not be bundled into an ordinary deployment.

## Preconditions

Do not apply reconciliation until all of the following are true:

1. the Phase 1 release has passed backend, frontend, Compose, and migration
   validation;
2. a fresh encrypted production backup includes PostgreSQL, `dagster_home`,
   application data, and artifacts;
3. that backup has passed a restore test in an isolated location;
4. no manually launched Dagster run is still active;
5. the operator has a rollback window and access to the production host.

## Commands

Define the production Compose command without printing or sourcing secrets:

```bash
cd /opt/trade/app
compose=(
  docker compose
  --env-file /opt/trade/.env
  -f /opt/trade/app/docker-compose.prod.yml
)
```

Create and verify the backup using the repository backup procedure. Record its
path, checksum, and restore-test evidence in the deployment record.

Stop the daemon so no schedule tick can race with reconciliation:

```bash
"${compose[@]}" stop dagster-daemon
```

Preview the exact actions:

```bash
"${compose[@]}" run --rm --no-deps dagster-daemon \
  python /app/scripts/reconcile_dagster_schedules.py
```

Review every `start_current`, `stop_current`, and `stop_stale` action. Unknown
active schedules appear under `unmanaged_active_schedules`; the reconciler
reports but never deletes or stops them.

Apply only after the preview is approved:

```bash
"${compose[@]}" run --rm --no-deps dagster-daemon \
  python /app/scripts/reconcile_dagster_schedules.py \
  --apply \
  --confirm APPLY_SCHEDULE_RECONCILIATION
```

The script refuses to apply if it detects a recent daemon heartbeat. Once
successful, it writes `schedule_current_origin.json` in `DAGSTER_HOME`; the API
uses this marker and read-only SQLite access to distinguish current, stale, and
mixed origins.

Restart the daemon:

```bash
"${compose[@]}" up -d dagster-daemon
```

## Verification

Verify all three evidence layers:

```bash
"${compose[@]}" exec -T dagster-daemon \
  dagster schedule list -m trade_research.dagster.definitions
"${compose[@]}" exec -T dagster-daemon dagster schedule debug
curl -fsS http://127.0.0.1:8080/api/data/schedules/status
```

Expected results:

- desired running schedules are running under the current origin;
- no stale-origin schedule remains active;
- `status_drift` and `origin_drift` are false for managed schedules;
- TSX and US completed-session Opportunity schedules are running when their
  yfinance exchange flags are enabled;
- stopped FX and legacy North America direct schedules remain stopped;
- historical ticks and runs are still queryable.

Observe at least one planner tick, one bounded worker tick, and the next
exchange-appropriate Opportunity target window. Verify PostgreSQL business
outcomes as well as Dagster run status.

## Rollback

If verification fails:

1. stop `dagster-daemon`;
2. preserve the failed post-change `dagster_home` for diagnosis;
3. restore the pre-change `dagster_home` backup;
4. restore the previous application release if the failure is code-related;
5. restart the daemon;
6. rerun the read-only production audit.

Do not use `dagster schedule wipe`. It deletes schedule state/history and is
not part of this procedure.
