---
document_status: current
last_verified_commit: afbc5dc1f78803752d013a6db99a76293d01d61e
last_verified_date: 2026-08-25
owner: trade-research-platform
replaced_by: null
---

# Trade Research Platform V1 Specification

## 1. Authority and purpose

This document is the governing target contract for Trade V1. It defines what
V1 is, the architecture decisions that apply to it, and the release gates that
must pass. It does not claim that the current repository or production system
already satisfies the contract.

When documents disagree, use this order:

1. this V1 specification;
2. `docs/current_state.md` and `docs/phase0_exit_report.md` for audited current
   behavior;
3. `docs/stabilization_validation_workflow_implementation_plan.md` for detailed
   implementation guidance consistent with this specification;
4. `docs/research_platform_milestone_execution_plan.md` for work-package
   detail, narrowed to NSE for V1;
5. historical or superseded documents only as background.

Changing a frozen V1 decision requires an explicit amendment to this document.

## 2. Product contract

V1 lets an authenticated user complete this journey without CLI or database
access:

```text
Select NSE data and universe
  -> define or select features
  -> select a target
  -> configure a model
  -> configure a walk-forward backtest
  -> validate and launch the workflow
  -> monitor progress and failures
  -> inspect and compare standardized results
  -> inspect complete lineage
```

V1 is a quantitative research and backtesting product. It does not place,
route, recommend, or autonomously execute live trades.

## 3. Frozen scope

### 3.1 Included

- NSE cash equities from versioned, point-in-time universe snapshots.
- Daily OHLCV.
- yfinance `1m` OHLCV within provider-available retention; actual retention and
  gaps must be displayed rather than implied.
- Requested `5m`, `15m`, `30m`, and `1h` aggregates derived from validated
  minute candles.
- Curated, versioned feature and target registries.
- Immutable, point-in-time dataset snapshots.
- Naive/ranking baseline, linear/ridge model, and LightGBM.
- Purged chronological walk-forward evaluation with configurable embargo.
- Long-only top-N portfolio construction for the first supported strategy.
- Transaction costs, slippage, turnover, benchmark comparison, factor
  diagnostics, ML diagnostics, and fold stability.
- Authenticated multi-user workspaces with isolation, quotas, audit history,
  workflow drafts, immutable versions, launch, cancellation, rerun, clone,
  comparison, and lineage.

### 3.2 Explicit non-goals

- TSX, US, FX, crypto, or other exchanges.
- Live trading, paper-trading integration, broker execution, order routing, or
  portfolio custody.
- Tick, L2, order-book, or full-market-depth data.
- Options, futures, and other derivatives.
- Arbitrary user-supplied Python, Rust, SQL, binaries, or containers.
- Kafka, Kubernetes, or a broad microservice decomposition.
- Rewriting FastAPI, Dagster, provider networking, or ML training in Rust.
- Qlib, NautilusTrader, LEAN, FinRL, or other framework integration.
- A large model marketplace or user-uploaded model plugins.
- Autonomous AI strategy approval or deployment.

Existing non-NSE paths may remain temporarily for safe migration, but they are
not V1 deliverables and must not expand V1 acceptance scope.

## 4. Architecture decision set

| ID | Decision | V1 rule |
|---|---|---|
| V1-ADR-001 | Product architecture | Modular monolith with explicit domain boundaries; no greenfield rewrite. |
| V1-ADR-002 | Orchestration | Dagster is the only normal production authority for long-running ingestion and research mutations. |
| V1-ADR-003 | API execution | Launch is asynchronous: accepted requests return `202` with a stable `workflow_run_id`. |
| V1-ADR-004 | Provider | yfinance is the V1 market-data provider after a measured NSE cutover. Upstox is comparison-only until its retirement gate passes. |
| V1-ADR-005 | Operational truth | PostgreSQL owns identity, authorization, workflow/run state, schedules, catalog, ingestion state, universes, and audit records. |
| V1-ADR-006 | Candle ownership | PostgreSQL/TimescaleDB remains the canonical provider-ingestion ledger; validated research candles and aggregates are promoted to ClickHouse with reconciliation and watermarks. |
| V1-ADR-007 | Analytical truth | ClickHouse owns query-ready candles, features, targets, predictions, factor statistics, experiment metrics, positions, and backtest series. |
| V1-ADR-008 | Immutable artifacts | S3-compatible object storage owns raw responses, dataset snapshots, models, preprocessors, manifests, reports, and plots. MinIO is the default self-hosted implementation. |
| V1-ADR-009 | Cache/coordination | Redis is non-authoritative and may hold locks, rate limits, short-lived coordination, and caches only. |
| V1-ADR-010 | Python/Rust boundary | Python owns workflow semantics and ML; Rust may implement bounded deterministic compute kernels behind typed Python interfaces. |
| V1-ADR-011 | Data exchange | Columnar Arrow/NumPy buffers cross Python/Rust boundaries; per-row calls and JSON compute boundaries are prohibited. |
| V1-ADR-012 | Research eligibility | All V1 research claims use point-in-time membership and availability. Static full-history eligibility is migration evidence only. |
| V1-ADR-013 | Evaluation | Random row splits are prohibited. V1 uses chronological walk-forward splits with purge and embargo. |
| V1-ADR-014 | Production fallbacks | Production fails closed. Synthetic, mock, or developer-local artifacts cannot be returned as successful production truth. |
| V1-ADR-015 | Versioning | Workflows, features, targets, datasets, models, metric formulas, and artifacts are immutable and versioned. |
| V1-ADR-016 | AI boundary | AI may propose a `WorkflowSpec`; the same deterministic validator and authorization checks used by the UI must approve it. |
| V1-ADR-017 | Compatibility paths | Every temporary duplicate path has an owner, consumers, comparison evidence, rollback rule, and removal gate. |

### 4.1 Target component boundary

```text
React UI
  -> FastAPI control/query API
       -> PostgreSQL control plane
       -> ClickHouse read-only analytical queries
       -> object-manifest reads
       -> Dagster run request
            -> provider adapter and ingestion services
            -> Python research/application services
            -> optional Rust compute kernels
            -> PostgreSQL / ClickHouse / object storage
```

The API must not run provider ingestion, dataset construction, training, or
backtesting inside an HTTP request.

## 5. Data and universe contract

### 5.1 Instrument scope

The V1 universe contains NSE cash equities with a stable internal
`instrument_id`. Symbols are display attributes and must not be used as durable
identity. Each universe version records effective dates, membership source,
listing state, filters, and digest.

Benchmark series, such as a declared Nifty benchmark, are read-only evaluation
inputs and are not part of the tradable universe.

### 5.2 Daily and minute data

Every candle includes at least:

```text
instrument_id, exchange, interval, session_date, timestamp,
open, high, low, close, volume, provider, provider_version,
quality_status, source_run_id, materialized_at
```

Required validation includes:

- exchange calendar and timezone alignment;
- completed-session eligibility;
- positive OHLC prices and valid high/low relationships;
- non-negative volume;
- duplicate-key rejection;
- stale-instrument and listing-boundary handling;
- missing-session classification;
- reconciliation between planned, fetched, validated, and stored rows;
- idempotent repeat ingestion.

Minute retention is a discovered provider capability, not a promised fixed
history. The product records the earliest/latest available timestamps and any
gaps for each instrument and interval.

### 5.3 Data quality states

Use explicit states:

```text
passed | warning | failed | quarantined | missing | stale
```

Every missing candle must have an explainable status, such as non-trading
session, not listed, provider unavailable, rejected, quarantined, or pending.
Unknown absence is a quality failure and cannot be silently treated as zero.

## 6. Feature and target contract

Every feature and target definition contains:

- stable ID, name, semantic version, family, owner, and lifecycle state;
- formula implementation reference and calculation-engine version;
- typed parameters and safe bounds;
- required inputs and dependencies;
- frequency support and minimum lookback;
- decision-time availability;
- null, warmup, and outlier policy;
- supported target type where applicable;
- deterministic golden fixtures and floating tolerance.

Lifecycle:

```text
draft -> validated -> active -> deprecated
```

Only active versions may be used by scheduled V1 workflows.

Initial curated feature families are:

- returns and momentum;
- volatility and downside risk;
- volume and liquidity;
- trend;
- mean reversion;
- cross-sectional ranks.

The required initial target is next-session forward return. Additional curated
forward-return or classification targets may enter V1 only through the same
registry, availability, leakage, and validation contract.

No feature may use information after its declared decision timestamp. Target
columns must never enter the model feature list.

## 7. Dataset contract

Users configure:

- universe version;
- daily or supported minute frequency;
- date range and decision timestamp;
- feature-set version;
- target version;
- point-in-time filters;
- expanding, rolling, or purged walk-forward periods;
- purge and embargo sessions.

The builder produces an immutable Parquet snapshot in object storage plus a
manifest registered in PostgreSQL. The manifest contains:

- dataset ID and version;
- object URI and SHA-256 digest;
- schema fingerprint, row count, instrument count, and date range;
- universe, feature, target, calendar, provider, and code versions;
- ClickHouse source watermarks;
- fold boundaries, purge, and embargo settings;
- inclusion/exclusion counts and reasons;
- validation and leakage results;
- parent artifact IDs and creation time.

Training reads the registered snapshot, never a mutable live query. Identical
specification, source watermarks, and code versions must produce the same
digest. A dataset is unusable until key-integrity, point-in-time, leakage, and
quality gates pass.

## 8. Model, experiment, and backtest contract

The canonical domain is:

```text
Experiment
  |- DatasetSnapshot
  |- FeatureSetVersion
  |- TargetVersion
  |- ModelRun
  |- Predictions
  |- BacktestRun
  |- Metrics
  `- ArtifactManifests
```

### 8.1 Supported models

- Naive or feature-ranking baseline.
- Linear/ridge regression baseline.
- LightGBM with allow-listed, typed, bounded hyperparameters.

Every model plugin declares accepted target types, seed behavior, dependency
versions, resource estimates, serialization format, and training/prediction
interfaces. A run records the exact hyperparameters and random seed.

### 8.2 Initial portfolio/backtest behavior

The required V1 strategy ranks predictions within the eligible NSE universe
and forms a long-only, equal-weight top-N portfolio. Configuration includes:

- top N;
- rebalance frequency;
- maximum per-name weight;
- minimum liquidity rule;
- benchmark;
- commission/fee assumptions;
- slippage model;
- initial capital.

Orders are applied no earlier than the first price permitted by the target and
decision-time contract. Positions, pre-trade weights, post-trade weights,
traded notional, costs, gross returns, and net returns are retained.

The first preferred Rust component is the deterministic backtest kernel. Its
Python implementation remains the golden reference until parity and
end-to-end performance gates pass. The recorded experiment identifies the
engine and engine version.

### 8.3 Failure and promotion

A failed run preserves logs and valid partial evidence but does not publish
incomplete datasets, models, predictions, backtests, or result summaries as
successful. Promotion requires reproducible inputs, leakage-safe evaluation,
declared minimum sample/trade counts, costs, baseline comparison, and bounded
fold concentration.

`research_candidate` is a research lifecycle state only. It never authorizes
paper or live trading.

## 9. Standard metric contract

Metric outputs store `metric_name`, `metric_version`, formula parameters,
evaluation window, frequency, benchmark, gross/net flag, fold/segment, and
observation count. The UI must not compare metrics computed under incompatible
contracts.

Unless a workflow declares otherwise, daily annualization uses `A = 252`, the
risk-free return and minimum acceptable return are zero, and results are net of
configured costs.

### 9.1 Portfolio metrics

Let `r_t` be the net portfolio return for session `t`, and
`V_t = V_(t-1) * (1 + r_t)`.

| Metric | `v1` definition |
|---|---|
| Total return | `V_T / V_0 - 1` |
| CAGR | `(V_T / V_0)^(A / N) - 1`, where `N` is the number of observed return sessions |
| Annualized volatility | `sample_stddev(r_t) * sqrt(A)` |
| Sharpe | `mean(r_t - rf_t) / sample_stddev(r_t - rf_t) * sqrt(A)` |
| Sortino | `mean(r_t - MAR_t) / sqrt(mean(min(r_t - MAR_t, 0)^2)) * sqrt(A)` |
| Drawdown | `V_t / max(V_0...V_t) - 1` |
| Maximum drawdown | Positive magnitude of the minimum drawdown |
| Calmar | `CAGR / maximum_drawdown`, undefined when maximum drawdown is zero |
| Turnover | `0.5 * sum_i(abs(target_weight_i - pretrade_weight_i))` at each rebalance; report mean and total |
| Transaction costs | Sum of traded notional multiplied by the configured fee and slippage models |

Undefined ratios remain null with a reason; they are never converted to zero
or infinity.

### 9.2 Benchmark-relative metrics

On the common aligned evaluation window, report:

- benchmark total return and CAGR;
- excess total return and excess CAGR;
- active return `r_t - benchmark_r_t`;
- tracking error `sample_stddev(active_return) * sqrt(A)`;
- information ratio `mean(active_return) / sample_stddev(active_return) * sqrt(A)`;
- up/down capture when the common sample is sufficient.

### 9.3 Factor and ML metrics

- Pearson IC: daily cross-sectional Pearson correlation between the feature or
  prediction and the registered forward target.
- Rank IC: daily cross-sectional Spearman correlation.
- Factor evidence: IC/rank-IC mean, standard deviation, information
  coefficient ratio, positive-session rate, quantile returns/spreads, hit rate,
  coverage, and monthly/fold stability.
- Regression: MAE, RMSE, `R2`, Pearson IC, rank IC, and directional hit rate.
- Classification, when an active target supports it: log loss, ROC AUC,
  precision, recall, F1, and calibration evidence.
- Fold stability: per-fold values plus mean, median, standard deviation,
  minimum, maximum, positive-fold proportion, and worst-fold result.

Each IC observation records the cross-sectional sample size. Aggregation must
apply a declared minimum sample threshold.

### 9.4 Fitness

Fitness is never an unexplained scalar. The required first formula is:

```text
fitness_v1 =
    net_sharpe
  - drawdown_penalty_coefficient * maximum_drawdown
  - turnover_penalty_coefficient * annualized_turnover
  - instability_penalty_coefficient * fold_metric_stddev
```

The workflow stores every coefficient, the selected fold metric, and all input
values. Model ranking is allowed only on the same evaluation window, target,
universe, portfolio rules, cost model, benchmark, and fitness version.

## 10. Workflow specification

A workflow definition has a stable identity. Editing creates an immutable new
version. A completed run always points to the exact version it executed.

Minimum V1 shape:

```json
{
  "schema_version": "workflow_spec_v1",
  "workflow_id": "uuid",
  "workflow_version": 1,
  "workspace_id": "uuid",
  "name": "NSE next-session ranking experiment",
  "data": {
    "exchange": "NSE",
    "provider": "yfinance",
    "frequency": "1d",
    "start": "2020-01-01",
    "end": "2026-06-30"
  },
  "universe_definition_id": "uuid",
  "feature_set_version_id": "uuid",
  "target_version_id": "uuid",
  "dataset": {
    "as_of_policy": "point_in_time",
    "minimum_listing_sessions": 252,
    "minimum_coverage": 0.98,
    "filters": []
  },
  "split": {
    "strategy": "purged_walk_forward",
    "minimum_train_sessions": 504,
    "validation_sessions": 63,
    "test_sessions": 21,
    "purge_sessions": 1,
    "embargo_sessions": 1
  },
  "model": {
    "family": "lightgbm_regressor",
    "hyperparameters": {},
    "seed": 42
  },
  "portfolio": {
    "construction": "long_top_n_equal_weight",
    "top_n": 10,
    "rebalance": "daily",
    "transaction_cost_bps": 10,
    "slippage_model": "fixed_bps_v1",
    "benchmark_id": "registered-benchmark-id"
  },
  "evaluation": {
    "metric_version": "metrics_v1",
    "fitness_version": "fitness_v1",
    "annualization_sessions": 252
  },
  "schedule": null
}
```

The server validates identity, permissions, registry compatibility, resource
limits, data availability, target/feature timing, split feasibility, and
portfolio bounds before storing a validated version or launching it.

## 11. Execution, identity, and lineage

### 11.1 Run lifecycle

```text
draft -> validated -> queued -> running -> succeeded
                               |          -> degraded
                               |          -> failed
                               `----------> cancelled
```

`degraded` is allowed only when a declared non-critical output fails and the
run contract identifies which results remain valid. Partial business failure
must not appear as success merely because a Dagster process exited normally.

### 11.2 Idempotency

The launch idempotency key is derived from workspace, workflow version, code
version, registry versions, source watermarks, and configuration digest.
Repeating an identical request returns the existing successful result or
creates an explicitly linked retry. It must not create ambiguous duplicates.

### 11.3 Required lineage

Every run records:

- user and workspace;
- workflow and workflow version;
- API request and Dagster run IDs;
- code commit, environment, dependency, and compute-engine versions;
- provider adapter, calendar, universe, feature, and target versions;
- source watermarks and dataset digest;
- model configuration, seed, and artifact digests;
- predictions, portfolio, backtest, metric, and report IDs;
- validation results, timestamps, status transitions, and retry parent.

The UI provides a read-only lineage view from source data through the displayed
result.

## 12. Required product capabilities

### 12.1 Workflow product

- Authentication and workspace isolation.
- Draft creation and immutable version history.
- Deterministic pre-launch validation.
- Dataset-size and compute estimate.
- Launch, cancel, rerun, and clone.
- Per-user concurrency and storage quotas.
- Structured progress, logs, errors, and retry guidance.
- Dataset preview.
- Two-to-four experiment comparison on compatible evaluation contracts.
- Read-only lineage and downloadable manifests/reports.

### 12.2 Page truth

| Product surface | V1 authoritative source |
|---|---|
| Data and ingestion operations | PostgreSQL plus reconciled ClickHouse freshness |
| Research Progress | PostgreSQL run, validation, lineage, and manifest records |
| Factors | ClickHouse factor statistics and feature observations |
| Models | PostgreSQL model/experiment registry, ClickHouse metrics, object manifests |
| Workflow Builder | PostgreSQL workflow definitions and immutable versions |
| Backtest/Evaluation | ClickHouse series and metrics plus registered reports |

No V1 production page depends on a developer-local `latest.json`, CSV,
Parquet, or generated plot as application truth.

## 13. Security and operational contract

- All objects are scoped to a workspace and checked server-side.
- Cross-workspace identifiers must not reveal metadata or existence.
- UI/API identities cannot write ClickHouse analytical tables directly.
- Dagster receives bounded write credentials; API query credentials are
  read-only with timeouts and quotas.
- Object storage uses least-privilege workspace/run prefixes and encryption.
- Secrets are supplied through the deployment secret mechanism and never
  returned by the API or committed.
- Workflow creation, launch, cancellation, promotion, schedule, and permission
  changes are audited.
- User input cannot execute arbitrary code, SQL, paths, or object-store URIs.
- Backup and restore objectives exist for every authoritative store and are
  proven by drills.

## 14. Service and quality objectives

V1 release targets:

- daily completeness at least `99.5%` across eligible NSE instruments, with
  every missing candle classified;
- `100%` of published research runs have manifests and complete lineage;
- `100%` of published V1 experiments use point-in-time datasets;
- no unexplained authoritative duplicate keys;
- no successful production response silently substitutes synthetic or local
  artifact data;
- schedule identity drift detected within 10 minutes;
- common metadata API p95 below 500 ms under the agreed load profile;
- common analytical queries complete within approximately 2 seconds under the
  agreed load profile;
- failed quality gates prevent downstream promotion;
- a worker crash or retry does not create duplicate published output.

Minute-data completeness is measured against the provider-observed available
window and cannot be represented as equivalent to daily historical coverage.

## 15. Delivery phases and gates

| Phase | Release | Required outcome | Exit gate summary |
|---|---|---|---|
| 0 | Planning gate | V1 contract and architecture locked | This specification accepted; NSE scope, formulas, ownership, boundaries, and non-goals frozen. |
| 1 | v0.1 | Stable modular foundation | Daily pipeline passes; schedule identity repaired; failures are truthful; restore tested; API work is asynchronous; CI enforces core boundaries. |
| 2 | v0.2 | Analytical storage foundation | Pinned ClickHouse and governed object storage; migrations, roles, registries, reconciliation, backup, and restore pass on a canary. |
| 3 | v0.3 | Reliable NSE data platform | yfinance daily/minute contract; freshness and gaps visible; ingestion idempotent; provider cutover and retirement evidence complete. |
| 4 | v0.4 | Feature and target platform | Versioned, deterministic, leakage-safe registries; Factors reads durable analytical truth. |
| 5 | v0.5 | Point-in-time dataset builder | Reproducible immutable snapshot, digest, preview, watermarks, exclusions, and leakage report. |
| 6 | v0.7 | Experiments and backtests | Supported models and standardized backtests run through Dagster; comparable durable results and full manifests. |
| 7 | v0.9 | Multi-user workflow product | Isolated users can create, validate, launch, monitor, cancel, rerun, clone, compare, and inspect lineage without CLI access. |
| 8 | v1.0 | Hardened production release | Load, resilience, restore, security, tenant isolation, observability, compatibility, documentation, and end-to-end acceptance pass. |

Phases 2 and 3 may partially overlap after Phase 1 gates pass. Infrastructure
deployment, provider cutover, or destructive cleanup must not bypass its
rollback and reconciliation gate.

## 16. End-to-end V1 acceptance test

V1 is accepted only when a non-admin test user can:

1. authenticate and enter an isolated workspace;
2. select an NSE universe and available daily or minute data;
3. select compatible registered features and a target;
4. configure a supported model and bounded hyperparameters;
5. configure chronological walk-forward folds, purge, embargo, portfolio,
   costs, slippage, and benchmark;
6. receive deterministic validation and a resource estimate;
7. launch and receive `202 + workflow_run_id` without blocking the request;
8. observe queued, running, degraded/failed, cancelled, and succeeded states
   with structured evidence;
9. inspect the immutable dataset, folds, model, predictions, positions,
   backtest series, standardized metrics, factor/ML stability, and costs;
10. compare compatible experiments without mismatched windows or formulas;
11. reproduce the result from its manifest and digests;
12. verify that another workspace cannot read or mutate any part of the run.

Release is blocked by any critical security or data-integrity defect, an
unreproducible published experiment, an unclassified material data gap, an
unrestorable authoritative store, or a successful UI result sourced from a
mock/local fallback.

## 17. Current-state migration notes

The audited repository already provides useful foundations: authenticated UI,
PostgreSQL/TimescaleDB, Dagster daily pipelines, durable yfinance work,
features, targets, factor research, ML utilities, and a basic backtest.

The primary gaps are trustworthy product contracts and durable application
truth:

- schedule identity and failure semantics need repair;
- provider and execution paths overlap;
- Progress, Factors, and Models still depend on local artifacts;
- the current ML universe is not point-in-time;
- ClickHouse is not yet the analytical research plane;
- MinIO exists in deployment topology but is not yet the governed V1 artifact
  plane;
- model and backtest stages are not yet one durable Dagster experiment flow;
- the multi-user workflow builder and quotas do not yet satisfy this contract.

Implementation must migrate these foundations incrementally, with shadow
comparison and rollback, rather than introducing another permanent parallel
path.
