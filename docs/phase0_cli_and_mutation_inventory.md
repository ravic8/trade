---
document_status: current
last_verified_commit: afbc5dc1f78803752d013a6db99a76293d01d61e
last_verified_date: 2026-07-24
owner: trade-research-platform
replaced_by: null
---

# Phase 0 CLI and Mutation Inventory

## Purpose

The user has selected UI-created workflows and Dagster-orchestrated execution
as the production target. This inventory records every current first-party
surface that can mutate production data or operational state. Phase 0 does not
remove these surfaces; later phases guard, replace, or retire them.

## Classification

- **Read only:** inspection or readiness only.
- **Database/admin mutation:** changes database schema, roles, or credentials.
- **Provider ingestion:** calls an external market-data provider and persists
  data or operational evidence.
- **Derived-data build:** creates features, targets, datasets, models,
  predictions, validation, or research artifacts.
- **Planner/worker mutation:** creates or executes durable work.
- **Deployment mutation:** changes the running application or infrastructure.

## Typer CLI

All commands are defined in `src/trade_research/cli.py`.

| Command | Class | Primary writes/effects | Dagster equivalent or target |
|---|---|---|---|
| `universe` | read only | none | UI/query API |
| `refresh-equity-universe` | provider ingestion | universe snapshots/members/lifecycle | universe refresh jobs |
| `tsx-reconciliation-status` | read only | none | Data UI |
| `market-session` | read only | none | UI/query API |
| `refresh-exchange-sessions` | derived/database build | exchange sessions/holidays | exchange-session jobs |
| `init-db` | database/admin mutation | schema initialization | Alembic deployment step |
| `verify-bigquery-environment` | read only | external readiness probes | deployment/readiness check |
| `bigquery-canary-readiness` | read only | readiness evidence | Dagster/operations UI |
| `create-analyst-role` | database/admin mutation | PostgreSQL role/grants | controlled admin migration |
| `revoke-analyst-role` | database/admin mutation | PostgreSQL login/grants | controlled admin migration |
| `provider-request-log` | read only | none | Data UI |
| `fetch-nifty-futures-history` | provider ingestion | artifacts/provider requests | future Dagster asset |
| `fetch-upstox-instruments` | provider ingestion | instrument master | comparison-only Dagster asset, then retire |
| `map-liquid-nse-upstox` | derived-data build | provider mappings/artifacts | comparison-only Dagster asset, then retire |
| `fetch-upstox-nse-daily` | provider ingestion | OHLCV, runs, audits, artifacts | legacy NSE comparison job, then retire |
| `retry-upstox-nse-daily` | provider ingestion | retry OHLCV/run evidence | legacy comparison job, then retire |
| `fetch-yfinance-daily` | provider ingestion | direct OHLCV/artifacts | durable planner/worker only |
| `plan-yfinance-daily-work` | planner/worker mutation | durable work items | planner job |
| `run-yfinance-daily-worker` | planner/worker mutation | provider calls, OHLCV, attempts/runs | worker job |
| `plan-yfinance-tsx-canary` | planner/worker mutation | canary work items | controlled workflow request |
| `plan-yfinance-nse-canary` | planner/worker mutation | canary work items | controlled workflow request |
| `check-nse-yfinance-cutover` | read only | none | validation gate/UI |
| `refresh-yfinance-history-evidence` | provider/derived mutation | history evidence | validation asset |
| `provider-history-status` | read only | none | Data UI |
| `fetch-yfinance-missing` | provider ingestion | missing OHLCV and run evidence | repair workflow |
| `fetch-dukascopy-intraday` | provider ingestion | intraday OHLCV/artifacts | intraday Dagster job |
| `fetch-yfinance-intraday` | provider ingestion | intraday OHLCV/artifacts | intraday Dagster job |
| `build-daily-features` | derived-data build | features, audits, artifacts | feature asset |
| `build-daily-targets` | derived-data build | targets, audits, artifacts | target asset |
| `build-opportunity-targets` | derived-data build | Opportunity targets | per-exchange target assets |
| `build-factor-research` | derived-data build | factor CSV/JSON | factor research asset |
| `validate-processed-datasets` | derived-data build | validation artifacts | validation asset |
| `build-ml-dataset-v1` | derived-data build | ML dataset/artifacts | dataset build asset |
| `build-walk-forward-folds-v1` | derived-data build | fold manifests | dataset/fold asset |
| `run-baseline-predictions-v1` | derived-data build | metrics/predictions | experiment job |
| `run-lightgbm-predictions-v1` | derived-data build | model/metrics/predictions | experiment job |
| `run-prediction-backtest-v1` | derived-data build | backtest series/metrics | backtest job |
| `run-latest-predictions-v1` | derived-data build | inference artifacts | inference job |
| `validate-daily-pipeline-health` | derived-data build | health report/evidence | health asset |

## HTTP mutations

| Method and route | Authorization | Effect | Disposition |
|---|---|---|---|
| `POST /api/admin/provider-credentials/upstox/test` | admin | Calls Upstox credential validation | Keep during comparison period; audit calls |
| `POST /api/admin/provider-credentials/upstox/token` | admin | Encrypts and persists Upstox token | Keep during comparison period; retire with provider |
| `POST /api/data/coverage/preview` | application access | May materialize exchange holidays while calculating preview | Make preview strictly read-only |
| `POST /api/data/pipeline-requests` | application access | Executes Upstox OHLCV fetch and validation inline in API | Replace with workflow request + Dagster launch |
| `POST /api/chat/query` | application access | Persists/query-side chat or LLM operational effects depending on implementation | Keep as product operation |
| `POST /api/chat/feedback` | application access | Persists feedback | Keep as product operation |

The inline data pipeline route is the largest architectural conflict. A web
request can hold provider credentials, call the provider, and persist market
data without a durable orchestration boundary.

## Dagster and deployment mutations

Normal, intended production mutations include:

- Dagster daemon schedule/tick execution;
- Dagster job/asset launches;
- PostgreSQL and TimescaleDB writes by assets;
- Redis adaptive-rate coordination;
- BigQuery outbound writes when explicitly enabled;
- Alembic migrations during deployment;
- Docker Compose deployment from GitHub Actions;
- encrypted provider credential updates by an authorized admin.

These are acceptable only when each action has durable run/request lineage,
idempotency, explicit authorization, and an auditable initiator.

## Scripts and operational tools

Current script categories:

- provider-rate probes;
- Upstox raw-to-processed validation;
- liquid NSE universe selection;
- deployment and database administration helpers referenced by CI/docs.

Provider probes and validation scripts must never run automatically against
production. Deployment scripts remain authorized operational surfaces and need
the same commit/image/migration evidence captured by the production audit.

## UI coupling to CLI

The production Research Progress page displays CLI commands for missing steps,
including:

- `trade-research fetch-upstox-instruments`;
- `trade-research fetch-upstox-nse-daily`;
- `trade-research build-daily-features --store-db`;
- `trade-research build-daily-targets --store-db`;
- dataset, fold, prediction, backtest, and factor commands.

This confirms that Factors and Models are not yet UI/Dagster workflow products.
The page is a read-only artifact checklist for a manual workflow.

## Guard policy for later phases

Phase 1 should add a production mutation policy:

1. default-deny mutating CLI commands when `APP_ENV=production`;
2. allow an explicit break-glass environment flag for controlled recovery;
3. emit an audit record with command, operator, reason, and run ID;
4. remove inline provider execution from the API;
5. make UI actions create durable workflow requests;
6. launch Dagster from the control plane with idempotency keys;
7. keep inspection/readiness commands available;
8. keep migrations and tightly scoped admin operations separate from research
   workflow execution.

## Phase 0 conclusion

The repository does not depend on CLI for all ingestion: the durable yfinance
worker is demonstrably active in production. However, the full research,
feature, model, prediction, and backtest chain still depends on CLI-created
local artifacts. Dagster assets cover only part of that chain. Removing CLI
mutation safely therefore requires implementing durable workflow and artifact
contracts first, not simply deleting commands.
