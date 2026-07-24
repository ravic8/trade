---
document_status: current
last_verified_commit: afbc5dc1f78803752d013a6db99a76293d01d61e
last_verified_date: 2026-07-24
owner: trade-research-platform
replaced_by: null
---

# Current Repository and Production State

This is the canonical starting context for repository work. When another
document conflicts with this one, use this document and
`docs/phase0_production_audit.md`, then correct the conflicting document.

## Verification boundary

Two kinds of truth are deliberately separated:

- **Repository verified** means the claim was checked against commit
  `afbc5dc1f78803752d013a6db99a76293d01d61e`.
- **Production verified** means the claim was checked directly on the Ubuntu
  host or its authenticated application against timestamped evidence.

As of 24 July 2026, the repository audit, authenticated production UI audit,
and direct read-only Ubuntu/PostgreSQL/Dagster audit are complete. Production
is deployed from clean `main` at commit
`afbc5dc1f78803752d013a6db99a76293d01d61e`; Alembic is at
`20260720_0009 (head)`. UI schedule badges are still not Dagster proof because
they report configured intent rather than instance state.

## Current product position

The repository is a production-deployed market-data and early quantitative
research platform for NSE, TSX, and US equities. It contains:

- FastAPI and React applications;
- PostgreSQL/TimescaleDB market and operational storage;
- Redis-backed provider rate limiting;
- a durable yfinance work queue;
- Dagster assets, jobs, schedules, daemon, and optional webserver;
- yfinance and Upstox daily-equity paths;
- exchange-session and universe materialization;
- Opportunities analytics;
- generated feature, target, factor, ML, prediction, and backtest artifacts;
- optional BigQuery analytics export;
- Qdrant and chat foundations;
- production Docker Compose and automated Ubuntu deployment.

The repository is not yet a scientifically validated trading system. In
particular, the current v1 ML dataset records a static full-history universe
assumption that must be replaced with point-in-time eligibility.

## Authoritative systems today

| Domain | Current authoritative system | Qualification |
|---|---|---|
| Provider work, retries, lifecycle, and run metadata | PostgreSQL | Durable transactional state |
| Exchange sessions and universe snapshots | PostgreSQL | Materialization is feature-gated |
| Daily OHLCV | PostgreSQL/TimescaleDB | Provider/source is part of the key |
| Opportunity targets | PostgreSQL | `/api/opportunities/daily` requires yfinance |
| Features and forward targets | PostgreSQL plus generated Parquet snapshots | Dagster writes both; research APIs do not query these tables directly |
| Research Progress | Local generated artifacts | Not durable application truth |
| Factor UI/API | Local JSON/CSV artifacts | Not durable application truth |
| Models UI/API | Local JSON/Parquet artifacts | Not durable application truth |
| BigQuery analytics | Outbound replica | Feature-gated; never authoritative |
| Qdrant | Retrieval experiment store | End-to-end document ingestion remains incomplete |

ClickHouse and S3-compatible object storage are proposed in
`docs/stabilization_validation_workflow_implementation_plan.md`; they are not
present in the current repository or production Compose topology.

## Provider state

The distinction between implemented capability, repository default, configured
intent, and actual production state is mandatory.

| Exchange/data | Implemented path | Repository default | Production state |
|---|---|---|---|
| NSE daily equities | Provider-neutral asset; Upstox inline primary or durable yfinance primary after readiness gate | Upstox primary; legacy Upstox enabled; yfinance NSE disabled | Both providers are active: yfinance durable runs persist current candles; an Upstox run is present and the artifact research path identifies Upstox as its source |
| TSX daily equities | yfinance direct asset and durable yfinance work queue | Full TSX yfinance disabled | yfinance worker active; candles current through 23 Jul; substantial queued work remains |
| US daily equities | yfinance direct asset and durable yfinance work queue | Full US yfinance disabled | yfinance worker active; candles current through 23 Jul; substantial queued work remains |
| NSE Opportunities | yfinance-only targets | Minimum session coverage 95% | Current through 23 Jul with 2,378 rows and 99.0% displayed coverage |
| TSX Opportunities | yfinance-only targets | Minimum session coverage 95% | Stale at 17 Jul while candles are current through 23 Jul |
| US Opportunities | yfinance-only targets | Minimum session coverage 95% | Stale at 17 Jul while candles are current through 23 Jul; 3 rows have incomplete previous-close inputs |
| FX/crypto intraday | Dukascopy and yfinance assets | Forex pipelines disabled | Both schedules stopped; no active production path found |
| BigQuery export | PostgreSQL outbound sync | All gates disabled | Enabled in production; latest 23 Jul run completed with 103,714 source/destination rows and no rejects |

Production flags directly verified as enabled: durable yfinance daily, full
NSE/TSX/US, materialized exchange sessions, provider-history evidence, and
BigQuery production sync. `NSE_DAILY_PRIMARY_SOURCE=upstox` and
`LEGACY_UPSTOX_NSE_ENABLED=true`, confirming mixed NSE authority.

## Dagster state

The repository defines 17 schedules. Every schedule has
`DefaultScheduleStatus.STOPPED`.

| Schedule | Job | Repository default |
|---|---|---|
| `daily_research_schedule` | `daily_research_pipeline_job` | stopped |
| `north_america_daily_yfinance_schedule` | `north_america_daily_yfinance_job` | stopped |
| `fx_intraday_dukascopy_schedule` | `fx_intraday_dukascopy_job` | stopped |
| `yfinance_fx_intraday_schedule` | `yfinance_fx_intraday_job` | stopped |
| `nse_universe_refresh_schedule` | `nse_universe_refresh_job` | stopped |
| `tsx_universe_refresh_schedule` | `tsx_universe_refresh_job` | stopped |
| `us_universe_refresh_schedule` | `us_universe_refresh_job` | stopped |
| `yfinance_daily_work_planner_schedule` | `yfinance_daily_work_planner_job` | stopped |
| `yfinance_nse_completed_session_work_planner_schedule` | `yfinance_nse_completed_session_work_planner_job` | stopped |
| `yfinance_daily_work_worker_schedule` | `yfinance_daily_work_worker_job` | stopped |
| `nse_completed_session_opportunity_targets_schedule` | `nse_completed_session_opportunity_targets_job` | stopped |
| `tsx_completed_session_opportunity_targets_schedule` | `tsx_completed_session_opportunity_targets_job` | stopped |
| `us_completed_session_opportunity_targets_schedule` | `us_completed_session_opportunity_targets_job` | stopped |
| `bigquery_daily_sync_schedule` | `bigquery_export_sync_job` | stopped |
| `nse_exchange_sessions_schedule` | `nse_exchange_sessions_job` | stopped |
| `tsx_exchange_sessions_schedule` | `tsx_exchange_sessions_job` | stopped |
| `us_exchange_sessions_schedule` | `us_exchange_sessions_job` | stopped |

The `/api/data/schedules/status` endpoint reports desired state derived from
application settings and, when `DAGSTER_READONLY_HOME` is mounted, reads actual
schedule, tick, run, and repository-origin state from Dagster SQLite in
read-only mode. Until schedule reconciliation writes the current-origin marker,
actual running/stopped state is available but origin health remains unknown.

The two North America completed-session target schedules are Phase 1 additions.
They remain stopped by repository default and become desired-running only when
their exchange-specific durable yfinance flags are enabled.

Direct audit found 12 running schedule records and 3 stopped schedules:

- running: daily research, BigQuery sync, NSE/TSX/US universe refresh,
  NSE/TSX/US exchange-session materialization, yfinance daily planner and
  worker, NSE completed-session planner, and NSE Opportunity targets;
- stopped: North America direct yfinance and both FX/intraday schedules.

Dagster control state is fragmented. Nine running schedules belong to an older
repository origin and continue to launch current code, while a fresh
`dagster schedule list` reports their current selectors as stopped. Only
BigQuery, the NSE completed-session planner, and NSE Opportunity targets are
running under the current repository origin. `dagster schedule debug`, tick
storage, and daemon logs prove the older entries remain active.

In the preceding 24 hours:

- the yfinance worker had 288 successful Dagster ticks;
- the planner, NSE/TSX universe refresh, and BigQuery jobs succeeded;
- the daily research job failed;
- the US universe refresh failed;
- NSE Opportunity targets had six successful Dagster runs.

Detailed evidence is in `docs/phase0_production_audit.md`.

## Production deployment topology in the repository

`docker-compose.prod.yml` defines:

- `web`;
- `api`;
- `filing-worker`;
- `dagster-daemon`;
- optional profile `dagster-webserver`;
- `postgres`;
- `redis`;
- `qdrant`;
- `minio` and one-shot `minio-init`;
- `otel-collector`;
- `prometheus`;
- `cloudbeaver`.

Important boundaries:

- the public web entrypoint binds to host loopback;
- Cloudflare Tunnel/Access is the public identity boundary;
- Dagster webserver is optional and intended for private admin access;
- CloudBeaver has no directly published production port;
- MinIO, OpenTelemetry, and Prometheus have no directly published production
  ports;
- PostgreSQL uses a loopback host port for SSH-tunneled administration;
- API, filing worker, Dagster daemon, and Dagster webserver share the API image;
- deployment applies Alembic migrations before starting the full stack.

The GitHub deployment workflow joins Tailscale and uses SSH to synchronize and
deploy `main` on the Ubuntu host.

## Implemented daily paths

### Durable yfinance daily path

```text
exchange sessions
  -> accepted universe snapshots
  -> yfinance daily work planner
  -> PostgreSQL durable work items
  -> yfinance daily work worker
  -> validated PostgreSQL daily OHLCV
  -> completed-session Opportunity targets
```

The durable planner/worker implements leases, heartbeats, bounded attempts,
adaptive provider rate controls, listing boundaries, retries, lifecycle
evidence, and quarantine.

### NSE research path

```text
nse_daily_ohlcv
  -> daily_features_v1
  -> daily_targets_v1
  -> processed_dataset_validation
  -> nse_opportunity_targets_v1
  -> ml_dataset_v1
  -> factor_research_v1
  -> daily_pipeline_health
```

`nse_daily_ohlcv` is provider-neutral:

- with Upstox primary, it downloads and stores Upstox data inline;
- with yfinance primary, it does not download inline; it validates the durable
  yfinance dataset against the Upstox comparison gate.

The Opportunity asset always requests yfinance. Therefore, selecting Upstox as
NSE research primary does not by itself populate the yfinance Opportunity
source.

### North America direct path

`north_america_daily_yfinance_job` runs direct seeded-universe yfinance assets
for US and Canada and then builds US/TSX Opportunity targets. This exists
alongside the durable planner/worker path and should be consolidated in Phase 3.

## Page-to-data-source truth

| Page | Backend | Current behavior |
|---|---|---|
| Dashboard | `/api/market/status`, `/api/opportunities/daily` | Market status uses PostgreSQL, then synthetic fallback; Opportunities is strict PostgreSQL |
| Data | `/api/data/*` | Operational rows are PostgreSQL; schedule status is configured intent, not Dagster actual state |
| Opportunities | `/api/opportunities/daily` | Strict PostgreSQL yfinance Opportunity targets; no mock fallback |
| Research chat | `/api/chat/*`, `/api/research/notes` | Chat foundation is real; research notes are hard-coded |
| Research Progress | `/api/research/progress` | Reads local generated artifacts |
| Factors | `/api/research/factors/*` | Reads local JSON/CSV artifacts |
| Models | `/api/research/ml/*` | Reads local JSON/Parquet artifacts and may compute views from them |
| Jobs | `/api/jobs/latest` | Uses PostgreSQL provider runs, then synthetic fallback |
| Provider Settings | `/api/admin/provider-credentials/upstox/*` | Reads/writes encrypted Upstox credential state with environment fallback |
| Screeners | `/api/screeners/intraday-range/latest` | Hard-coded response |
| Symbol | `/api/symbols/{ticker}/candles`, research notes | PostgreSQL candles then synthetic fallback; notes are hard-coded |

The frontend also supplies fallback objects for many non-strict requests. Only
the Opportunities client currently uses strict fetch behavior.

Detailed evidence is in `docs/phase0_page_data_source_inventory.md`.

## Production mutation surfaces

The current repository still permits production mutation outside Dagster:

- mutating Typer CLI commands;
- `POST /api/data/pipeline-requests`, which runs an Upstox fetch/validation
  request inline in the API process;
- Upstox credential save/test admin routes;
- deployment and database administration scripts.

Phase 1 will add a production mutation guard and remove inline provider
execution from normal application operation. Phase 0 records these paths but
does not change behavior.

See `docs/phase0_cli_and_mutation_inventory.md`.

## Known high-priority risks

### P0 — Dagster schedule identity drifts across deployments

Nine active schedules are stored under an older repository origin, while the
same names appear stopped under the current origin. Schedule listing and
control by current selector can therefore misreport or fail to control the
functionally active schedule. The daemon logged 110 temporary code-server
heartbeat warnings in the inspected 24-hour container log window.

### P0 — Daily research products are not advancing together

At audit time, yfinance OHLCV was current through 23 July for NSE, TSX, and US.
NSE Opportunity targets were also current, but TSX and US Opportunity targets
were still at 17 July. The Upstox-backed NSE research artifact chain was at
22 July, while factor/model artifacts were absent. Direct Dagster evidence
shows the daily research run fails at `ml_dataset_v1` because
`daily_pipeline_stock_coverage.parquet` is missing. TSX/US target products are
stale because the durable worker stores candles but no active schedule builds
their Opportunity targets; the North America target job is stopped.

### P0 — US universe refresh fails deterministically

The latest US universe job failed with a PostgreSQL cardinality violation:
duplicate constrained universe-member values are sent in a single
`ON CONFLICT DO UPDATE` statement. The same error appears in some yfinance work
attempts and requires input deduplication before upsert.

### P0 — Production pages can present synthetic data

Dashboard market status, Jobs, Symbol candles, Screeners, and Research notes
can return fabricated/fallback values. Production must fail closed in later
phases.

### P0 — NSE provider authority can be internally inconsistent

Repository defaults select Upstox for the NSE research asset, while
Opportunities requires yfinance. Production evidence confirms both providers
remain active. The user has selected yfinance as the future standard, so
Upstox must be reduced to an explicit comparison path before retirement.

### P1 — Dashboard freshness is materially incorrect

The production Dashboard reports all 8,885 symbols stale, June/epoch freshness
timestamps, and 0% quality while the Data Console shows July yfinance candles.
It also labels the combined NSE+TSX+US total as “NSE + TSX tracked symbols.”
The Dashboard must not be used as a current operational health source.

### P1 — Queue backlog and run failures are material

The production Data Console showed 50-run success rates of 68% for NSE, 92% for
TSX, and 74% for US. It also showed 602 TSX open items and 5,336 US open items
at 11:45 IST. Active ingestion is not equivalent to a healthy steady state.
Direct database evidence later in the audit showed the backlog draining, but
the current worker still produced partial business outcomes while its Dagster
runs were marked successful.

### P1 — Backup recovery is unproven

The newest backup is from 19 July, no automatic backup timer was found, and no
restore script or restore-test evidence exists. The host has 238 GB free, so
capacity is not the immediate constraint.

### P1 — Factors, Models, and Progress are artifact-backed

These pages depend on files mounted under `/app/data`; they do not query a
durable analytical control plane.

### P1 — Current ML universe is not point-in-time

The v1 static full-history coverage rule can introduce future-aware selection.

### P1 — Direct and durable yfinance paths coexist

North America direct assets and the durable yfinance planner/worker can both
serve daily data. One authoritative execution path is required.

### P2 — Central modules are oversized

`api/app.py` and `storage/timescale.py` contain multiple domains. New work
should use domain modules and migrate existing behavior incrementally.

### P2 — CI is not a full integration gate

The current CI lacks real Timescale/PostgreSQL migration coverage, Redis
integration, frontend unit/browser tests, coverage enforcement, and mypy
execution.

## Current readiness

| Readiness | Status | Reason |
|---|---|---|
| Repository truth | verified | Phase 0 repository inventory complete |
| Production UI truth | verified | Authenticated read-only audit completed 24 Jul 2026 |
| Production host/DB/Dagster truth | verified | Direct read-only audit completed 24 Jul 2026 |
| Baseline ML mechanics | implemented | Local artifact workflow exists |
| Serious research | not ready | Point-in-time, durable lineage, and scientific gates incomplete |
| Production research workflow builder | not ready | Durable research stores and orchestration contracts incomplete |
| Live trading | out of scope | No authorization or validation |

## Canonical companion documents

- `docs/phase0_repository_inventory.md`
- `docs/phase0_page_data_source_inventory.md`
- `docs/phase0_cli_and_mutation_inventory.md`
- `docs/phase0_production_audit.md`
- `docs/phase0_exit_report.md`
- `docs/stabilization_validation_workflow_implementation_plan.md`
- `docs/research_platform_milestone_execution_plan.md`
