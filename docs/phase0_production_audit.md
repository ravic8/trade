---
document_status: current
last_verified_commit: afbc5dc1f78803752d013a6db99a76293d01d61e
last_verified_date: 2026-07-24
owner: trade-research-platform
replaced_by: null
---

# Phase 0 Production Audit

## Scope and evidence boundary

This read-only audit covered:

- the authenticated application at `https://trade.chain8.org` between 11:35
  and 11:56 IST;
- the production Ubuntu host through Tailscale/SSH between 12:07 and 12:15 IST;
- Docker Compose services and images;
- Alembic state;
- redacted production feature flags;
- PostgreSQL aggregate and diagnostic queries;
- Dagster schedule storage, ticks, runs, failure events, and daemon logs;
- disk and backup state.

No schedule, flag, row, service, credential, file, or deployment state was
changed. Secret values were not printed.

Direct host facts:

| Item | Verified value |
|---|---|
| Host OS | Ubuntu 24.04.2 LTS |
| Deployed branch/commit | clean `main` at `afbc5dc1f78803752d013a6db99a76293d01d61e` |
| Alembic | `20260720_0009 (head)` |
| PostgreSQL database size | 13 GB |
| Host disk | 455 GB total, 195 GB used, 238 GB available |
| API/web health | `/api/health` returned `ok` directly and through the web proxy |

## Executive conclusion

The daily incremental platform is **active but not fully healthy**.

- yfinance durable work is running repeatedly and storing current NSE, TSX, and
  US candles through 23 July.
- The last 50 exchange-attributed runs include material partial failures:
  68.0% success for NSE, 92.0% for TSX, and 74.0% for US.
- Queue backlog is material, especially US.
- NSE Opportunity targets are current, but TSX and US Opportunity targets are
  seven calendar days behind their current candles.
- Upstox remains configured and its research artifacts/run evidence remain in
  production, so the platform is not yet yfinance-only.
- Factors and Models are not operational products; their required artifacts
  are absent.
- Dashboard market freshness is inconsistent with the Data Console and should
  not be trusted as an operational health view.
- Twelve Dagster schedules are functionally active, but nine are attached to a
  stale repository origin and appear stopped when schedules are listed against
  the current repository selector.
- The latest daily research run and US universe refresh run failed.
- Backups exist, but the newest is five days old and no automatic timer or
  restore-test evidence was found.

## Production data overview

Observed Data Console values:

| Exchange | Active universe | Stored candles | Symbols with data | Latest candle session | Fetched | Open work | Retry wait | Suspicious |
|---|---:|---:|---:|---|---|---:|---:|---:|
| NSE | 2,387 | 4,291,594 | 2,405 | 23 Jul 2026 | 24 Jul 01:20 | 22 | 19 | 1 |
| TSX | 885 | 1,341,900 | 645 | 23 Jul 2026 | 24 Jul 11:45 | 602 | 0 | 0 |
| US | 5,613 | 10,357,583 | 5,612 | 23 Jul 2026 | 24 Jul 11:45 | 5,336 | 9 | 0 |

The count of symbols with historical data is not the same as the accepted
active universe. It can be greater when deactivated or historical symbols are
retained, as in NSE, or lower when current universe members lack data, as in
TSX and US.

## Incremental worker evidence

The Runs view returned persisted `Yfinance Daily Work Queue` records labelled
`dagster`. Recent combined runs included:

- 24 Jul 11:35: completed, 100 items;
- 24 Jul 11:40: completed, 100 items;
- 24 Jul 11:45: completed, 100 items;
- 24 Jul 11:50: completed with failures, 100 items;
- 24 Jul 11:55: running at audit time.

Exchange-attributed summaries over 50 visible runs:

| Exchange | Processed | Success rate | Failed/partial runs | Average finished duration |
|---|---:|---:|---:|---:|
| NSE | 4,402 | 68.0% | 16 | 34s |
| TSX | 529 | 92.0% | 4 | 51s |
| US | 4,117 | 74.0% | 13 | 49s |

This proves recurring worker execution and persisted outcomes. It does not
by itself prove that the Dagster worker schedule is enabled.

Direct Dagster evidence closes that gap:

- 288 worker ticks in the preceding 24 hours, all tick status `SUCCESS`;
- daemon logs show evaluation and scheduled launch every five minutes;
- 288 Dagster worker runs have status `SUCCESS`.

Dagster success means the asset code returned a `PipelineRunResult`; it does not
mean every market-data work item succeeded. PostgreSQL recorded 63 fully
completed and 40 partially failed yfinance business runs in the same 24-hour
window, with 155 failed items. The orchestration and business-success models
are therefore misaligned.

## Opportunity target freshness

| Exchange | Latest target session | Target rows | Displayed coverage | Candle session | Finding |
|---|---|---:|---:|---|---|
| NSE | 23 Jul 2026 | 2,378 | 99.0% | 23 Jul 2026 | Current |
| TSX | 17 Jul 2026 | 644 | 100.0% | 23 Jul 2026 | Stale by four market sessions |
| US | 17 Jul 2026 | 5,408 | 96.4% | 23 Jul 2026 | Stale by four market sessions; 3 incomplete previous-close inputs |

This resolves the earlier “partial versus correct” confusion:

- **coverage** describes how many eligible rows exist for the selected session;
- **freshness** describes whether the newest expected session has been
  materialized;
- an old session can correctly be 100% complete while the product is stale;
- changing exchange/session triggers an asynchronous request, so the transient
  zero/loading view is not an empty persisted session.

## Why no NSE session necessarily shows 100%

The latest NSE target session contains 2,378 Opportunity rows. The current
accepted universe is 2,387, producing the displayed 99.0% coverage.

The denominator and row set can differ because:

- the accepted universe changes over time;
- symbols can be newly listed, suspended, delisted, or otherwise ineligible;
- a provider may not return a valid daily bar;
- a row may fail OHLCV or previous-close validation;
- listing-boundary and exchange-session rules can exclude a symbol;
- durable retry/quarantine work may remain unresolved;
- the UI rounds the percentage.

Direct SQL identified the exact nine accepted-universe symbols without a
23 July target:

```text
CMLL.NS
CPL.NS
EUROTEXIND.NS
KALYANI.NS
NIRAJISPAT.NS
PKTEA.NS
SAYAJIHOTL.NS
SEMAC.NS
THACKER.NS
```

The causes are concrete:

- CMLL is a new symbol for which Yahoo returned no valid candles; it remains in
  retry wait.
- CPL reached the terminal attempt limit with the same new-listing/provider-lag
  condition.
- Six symbols have data through 22 July but Yahoo did not return the expected
  23 July session.
- THACKER is only current through 20 July.
- KALYANI, NIRAJISPAT, SAYAJIHOTL, and THACKER also have retry attempts failing
  with a PostgreSQL cardinality violation caused by duplicate keys in a single
  upsert batch.

## Provider evidence

### yfinance

yfinance is the source shown by the Data and Opportunities pages. Recurring
durable worker records and current stored candles prove active production use
for NSE, TSX, and US. Direct production flags confirm daily yfinance and full
NSE/TSX/US are enabled.

Direct database totals:

| Exchange | First yfinance session | Latest session | Rows | Historical symbols |
|---|---|---|---:|---:|
| NSE | 18 Jul 2016 | 23 Jul 2026 | 4,291,594 | 2,405 |
| TSX | 18 Jul 2016 | 23 Jul 2026 | 1,341,950 | 648 |
| US | 18 Jul 2016 | 23 Jul 2026 | 10,358,028 | 5,612 |

### Upstox

Upstox is still used:

- Provider Settings reports a valid database-backed Upstox credential, updated
  15 July 2026 by `accounts@chain8.org`.
- Research Progress identifies the current NSE daily OHLCV artifact as produced
  by `trade-research fetch-upstox-nse-daily`.
- That artifact contains 131,520 rows across 261 symbols from 1 January 2024
  through 22 July 2026.
- Production run history observed during the audit session included a completed
  `upstox_nse_daily_ohlcv` run on 23 July at 19:30 with 261 items.

Therefore, production is a mixed-provider system today. The requested
yfinance-only target is not yet implemented. Direct flags confirm
`NSE_DAILY_PRIMARY_SOURCE=upstox`,
`LEGACY_UPSTOX_NSE_ENABLED=true`, and `YFINANCE_NSE_ENABLED=true`.

## Research artifact state

Research Progress reported:

- 15 steps;
- 3 completed;
- 12 missing;
- 53 tracked artifacts;
- overall warning.

The partial Upstox-backed chain has:

| Product | Rows | Symbols | Date range | State |
|---|---:|---:|---|---|
| Daily OHLCV | 131,520 | 261 | 2024-01-01 to 2026-07-22 | done |
| Daily technical features | 131,519 | 261 | 2024-01-01 to 2026-07-22 | warning; 51,641 warnings |
| Daily forward targets | 131,519 | 261 | 2024-01-01 to 2026-07-22 | warning; 15,660 warnings |
| Processed validation | n/a | n/a | n/a | missing |
| ML dataset | n/a | n/a | n/a | missing |
| Factor outputs | n/a | n/a | n/a | missing |
| Model/backtest outputs | n/a | n/a | n/a | missing |

Factors independently showed zero rows/features/IC results. Models reported no
ML dataset, zero models, and no predictions or backtests.

Dagster run history shows the daily research job failed in each recent audited
run. The latest failure is at `ml_dataset_v1`:

```text
FileNotFoundError:
/app/data/processed/validation/daily_pipeline_stock_coverage.parquet
```

Earlier assets in the job still materialize, which is why Progress shows OHLCV,
features, and targets while the overall Dagster run is failed.

## Dashboard inconsistency

The Dashboard reported:

- universe 8,885 labelled “NSE + TSX tracked symbols,” although the value is
  NSE + TSX + US;
- quality 0.0%;
- all 8,885 symbols stale;
- NSE/TSX OHLCV freshness in June;
- US OHLCV and all Opportunity refresh timestamps at Unix epoch.

Those values conflict with the current July data shown by the Data and
Opportunities pages. This is a product defect, not evidence that all stored
candles are stale.

## Actual Dagster schedule state

The Data page displayed these rows as `running`:

- yfinance daily work planner;
- yfinance daily work worker;
- NSE/TSX/US universe refresh;
- NSE/TSX/US exchange-session materialization.

The backend implementation derives UI states from settings and does not query
Dagster. Direct instance inspection found:

| Schedule | Functional state | Repository origin | Latest 24h evidence |
|---|---|---|---|
| `daily_research_schedule` | running | stale | 1 tick; Dagster run failed |
| `north_america_daily_yfinance_schedule` | stopped | current/default | no active record |
| `fx_intraday_dukascopy_schedule` | stopped | current/default | no active record |
| `yfinance_fx_intraday_schedule` | stopped | current/default | no active record |
| `nse_universe_refresh_schedule` | running | stale | 1 successful run |
| `tsx_universe_refresh_schedule` | running | stale | 1 successful run |
| `us_universe_refresh_schedule` | running | stale | 1 failed run |
| `yfinance_daily_work_planner_schedule` | running | stale | 1 successful run |
| `yfinance_daily_work_worker_schedule` | running | stale | 288 successful ticks/runs |
| `nse_exchange_sessions_schedule` | running | stale | monthly; no 24h tick |
| `tsx_exchange_sessions_schedule` | running | stale | monthly; no 24h tick |
| `us_exchange_sessions_schedule` | running | stale | monthly; no 24h tick |
| `bigquery_daily_sync_schedule` | running | current | 1 successful run |
| `yfinance_nse_completed_session_work_planner_schedule` | running | current | enabled after its 24h evaluation point |
| `nse_completed_session_opportunity_targets_schedule` | running | current | 6 successful runs |

Nine active schedule records belong to repository origin
`63a6f79ffb22770d7674fb5bae7e046523eb6332`; three current records belong to
origin `295965ac7d6fd7fad2eece0f88a23f0034a01b86`.

Consequences:

- `dagster schedule debug` reports 12 running records;
- a fresh `dagster schedule list -m ...` reports only the three current-origin
  schedules as running and the stale-origin names as stopped;
- the stale-origin worker nevertheless launches every five minutes;
- schedule controls can target the wrong selector after deployment.

The latest US universe run failed because duplicate constrained universe-member
values were passed to one PostgreSQL upsert. The daemon also emitted 110
temporary code-server “No heartbeat received” warnings in the inspected log
window.

## Runtime and recovery state

All eight production Compose services were up. API, PostgreSQL, and CloudBeaver
reported healthy; web and Dagster services were running. API, daemon, and
Dagster webserver used the same API image ID. PostgreSQL and Qdrant use floating
`latest` tags, which weakens reproducibility.

Backups occupy 3.6 GB. The newest captured set is
`20260719T073524Z`, approximately five days old at audit time. No user crontab,
relevant systemd timer, restore script, or documented restore-test evidence was
found. Backup creation exists, but recoverability is not validated.

## Risk register

| Priority | Risk | Evidence | Phase |
|---|---|---|---|
| P0 | Schedule identity drifts across deployments | Nine active stale-origin records; current list says stopped | Phase 1 |
| P0 | TSX/US Opportunity targets are stale | 17 Jul targets vs 23 Jul candles | Phase 1 |
| P0 | Mixed yfinance/Upstox authority | Settings, runs, and artifact chain | Phase 1/3 |
| P0 | Daily research job fails | Missing stock-coverage Parquet at `ml_dataset_v1` | Phase 1 |
| P0 | US universe refresh fails | Duplicate-key cardinality violation | Phase 1 |
| P1 | Dagster success hides partial business failure | 288 successful Dagster runs vs 40 partial ingestion runs | Phase 1 |
| P1 | Material incremental failures | 68% NSE and 74% US 50-run success | Phase 1 |
| P1 | Large queue backlog | 602 TSX and 5,336 US open items | Phase 1 |
| P1 | Dashboard health is incorrect | June/epoch vs July data | Phase 1 |
| P1 | Backup recovery is unproven | Latest backup 19 Jul; no timer/restore test | Phase 1 |
| P1 | Factor/model pages have no durable products | Zero/missing artifacts | Phase 4+ |
| P1 | Research page still instructs CLI mutation | Progress command labels | Phase 2+ |
| P2 | Slow exchange transitions can look empty | Multi-second loading states | UI hardening |
| P2 | Host security maintenance is overdue | 217 available updates; 31 standard security updates | Operations |
| P2 | Infrastructure images are not fully pinned | Timescale/Qdrant `latest` tags | Phase 1 |

## Audit disposition

Production UI audit: **complete**.
Ubuntu/container audit: **complete**.
Direct PostgreSQL audit: **complete**.
Direct Dagster schedule/tick/run audit: **complete**.

Phase 0 truth collection is complete. The findings are a Phase 1 remediation
backlog; no production mutation was performed during this audit.
