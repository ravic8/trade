# Daily market data

This repository acquires, validates, and maintains daily equity OHLCV data.
The active scope is **all NSE EQ stocks using yfinance**, with a rolling ten-year
acquisition window, daily incremental updates, and historical gap repair.
There is no liquidity, turnover, or top-N filter at ingestion. Newly listed
stocks and provider history limitations are recorded as coverage exceptions.
Existing observations older than the acquisition window are preserved.

PostgreSQL/TimescaleDB stores accepted observations and operational state.
ClickHouse holds the reconciled analytical copy. Redis coordinates provider
limits. S3-compatible storage retains raw evidence; Dagster runs the durable
planner and worker. Success requires validated data and matching replica counts
and digests. Missing data remains visible and retryable where appropriate.

TSX and US **daily** adapters remain available for later activation. Their
schedules are currently disabled. Upstox history is preserved with its source
label; it is not the selected NSE provider.

## Recovery status

The ten-year NSE history exists in PostgreSQL: the pre-repair audit found
4,391,994 observations spanning July 2016–September 2026. That span does not
establish complete coverage for every stock. ClickHouse was a partial mirror.
Storage access and encryption have been repaired. A production snapshot now
contains 4,450,906 accepted rows in each database, with all 124 monthly partition
checks matching. Current-session catch-up has run; coverage exceptions remain
visible, and historical gap repair is queued.

See the [implementation journal](output/nse_daily_review_2026-10-16/implementation_status.md)
for verified results and outstanding work, and the
[provider decision](output/nse_daily_review_2026-10-16/provider_decision_and_scope.md)
for the comparison and user-confirmed scope.

## Repository boundaries

The sibling `trade-web` owns the operations frontend. `trade-research` owns
features, labels, modeling and backtesting, and consumes versioned daily snapshot
files. Neither requires this checkout to build. `trade-fundamentals` and
`trade-market-experiments` hold archival migration material; they are not active
applications. Existing lens_fa and nifty_stream repositories are untouched.

The daily Python package is `trade_data`, its distribution and command are
`trade-data`. Stored database names, stable instrument IDs, applied migration
revision IDs and historical table definitions remain compatible. The old README
is preserved in `../trade-research/migration`.

## Local development

Use Python 3.11+ and install the project with its development dependencies:

```sh
uv sync --extra dev
uv run pytest -q
uv run ruff check src tests scripts
```

Copy `.env.example` and supply service endpoints and credentials privately.
Production configuration is documented in `.env.prod.example` and
`docker-compose.prod.yml`. Secrets must not be committed. Fresh configuration
leaves collection disabled until the operator enables the intended NSE flags.

For NSE daily operation, select `NSE_DAILY_PRIMARY_SOURCE=yfinance`, enable
`YFINANCE_DAILY_ENABLED` and `YFINANCE_NSE_ENABLED`, scope the shared worker with
`YFINANCE_DAILY_WORKER_EXCHANGE=NSE`, and keep TSX/US and legacy Upstox disabled. Enable the object-store and ClickHouse write
roles for the worker. The API uses read credentials.

After validating configuration and a small real canary, reconcile Dagster to
these five schedules: NSE sessions, NSE universe, daily work planner,
NSE completed-session planner, and daily work worker. Stop the daemon before
applying `scripts/reconcile_dagster_schedules.py`; its heartbeat guard prevents
concurrent schedule-origin changes. Resume the daemon after reconciliation.

The operational UI is the independent sibling repository `trade-web`. Trade has
no frontend source or Node build step. Run the API and frontend independently;
see [frontend integration](docs/frontend_integration.md). Older documents may
still describe features being extracted; they do not expand the scope above.

Export versioned daily inputs with `trade-data export-daily-snapshot`; see the
[snapshot contract](docs/research_snapshot_contract.md). Daily health views exclude
historical minute ledgers. Generic observability remains; Alertmanager delivery
requires an operator-configured receiver.

Deployment now preserves existing infrastructure by default and refuses active
Dagster jobs before changing the code location. Storage upgrades require
`PROD_UPGRADE_INFRASTRUCTURE=true`. Historical `RESEARCH_STORAGE_*` flags and the
`research` Compose profile still control daily storage for compatibility. No
rollout deletes orphan containers or stored volumes. The deployment script assumes
core infrastructure already exists; fresh-stack checks live in
`scripts/verify_fresh_setup.py`.

The active source now contains the daily backend and preserved historical schema
definitions. Isolated infrastructure verification and production acceptance are
tracked separately. Production remains on the repaired NSE image. See the
[implementation status](docs/repository_split_progress.md) before deployment.
