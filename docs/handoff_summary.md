# Handoff Summary

Use this as the first context file for a new Codex chat.

## Current Direction

This repo is now a local-first Upstox research repo. Production deployment
packaging has been removed. `docker-compose.yml` starts the full local stack:
API, web, Dagster webserver/daemon, TimescaleDB, Redis, Qdrant, and optional
CloudBeaver. Qdrant is included for the planned document/research retrieval
integration even though that workflow is not complete yet.

## Latest Verified Pipeline State

Latest successful Dagster daily run:

```text
dagster_run_id: 0a854e73-6572-45de-8aad-8e6f627382ae
latest_expected_trading_date: 2026-06-25
overall_status: warn
baseline_ml_ready: true
serious_research_ready: false
production_ready: false
```

Latest verified incremental OHLCV ingestion run:

```text
ingestion_run_id: e361cce3-57ea-4d56-bc90-f490812444e0
status: completed_empty
mapped_symbols: 261
skipped_current_symbols: 261
retry_candidates: 0
```

Latest verified retry run:

```text
retry_run_id: 15a16c9b-0e35-4ba7-9edb-9da05557009e
source_coverage_run_id: e361cce3-57ea-4d56-bc90-f490812444e0
statuses: failed, no_rows
status: completed_empty
candidate_rows: 0
```

Current Timescale row counts:

```text
ohlcv_daily: 126,704
features_daily: 126,703
targets_daily: 126,703
daily_ohlcv_fetch_coverage: 261
stock_coverage_runs: 2
stock_coverage_by_window: 3,132
```

## Implemented Shape

Daily Dagster job:

```text
upstox_daily_ohlcv
  -> daily_features_v1
  -> daily_targets_v1
  -> processed_dataset_validation
  -> factor_research_v1
  -> daily_pipeline_health
```

Important behavior:

- OHLCV ingest is incremental by symbol and writes run-scoped fetch coverage.
- `daily_ohlcv_fetch_coverage` is the retry source of truth.
- `trade-research retry-upstox-nse-daily` retries only `failed` and `no_rows`
  windows from the latest or specified coverage run.
- Features compute incrementally with a 320-calendar-day warmup and export a
  full post-upsert snapshot.
- Targets recompute a 90-calendar-day dirty window and export a full post-upsert
  snapshot.
- Full-history stock coverage and rolling windows are persisted per Dagster run.

## Local-Only Cleanup Completed

Removed deployment/server packaging:

```text
deploy/
docker-compose.prod.yml
Dockerfile.api
.env.prod.example
apps/web/Dockerfile
apps/web/nginx.conf
dagster_home/
```

Previously removed yfinance/Yahoo active paths:

```text
src/trade_research/data/yahoo.py
src/trade_research/dagster/assets.py
src/trade_research/dagster/sensors.py
tests/test_yahoo_provider.py
```

No `yfinance` / `Yahoo` stale references remain in README, docs, src, tests, or
`pyproject.toml`.

## Commands To Know

Start the local stack:

```bash
docker compose up --build
```

Initialize the database:

```bash
docker compose exec api trade-research init-db
```

Run the daily pipeline:

```bash
docker compose exec dagster-webserver \
  dagster job execute -m trade_research.dagster.definitions -j daily_research_pipeline_job
```

Retry failed/no-row daily OHLCV fetches:

```bash
trade-research retry-upstox-nse-daily
```

Run verification:

```bash
.venv/bin/python -m ruff check src tests
.venv/bin/python -m pytest
git diff --check
```

## Next Recommended Work

Build `ml_dataset_v1` as a separate layer after features and targets. It should
join cleaned OHLCV, features, targets, coverage flags, listing/history metadata,
and explicit exclusion reasons. Do not fold ML dataset logic back into features
or targets.
