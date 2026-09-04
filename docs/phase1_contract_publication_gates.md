---
document_status: current
last_verified_commit: working-tree
last_verified_date: 2026-08-25
owner: trade-research-platform
replaced_by: null
---

# Phase 1 Contract Publication Gates

## Purpose

The first hard adopters of `data_contract.v1` and `validation_report.v1` are the
NSE daily OHLCV, technical-feature, and forward-target publication boundaries.
Each boundary evaluates the frame before it writes canonical data to PostgreSQL
or a Parquet replica. A blocking result raises `ValidationContractError`; the
data write does not run.

The validation report is written before readiness is enforced. Failed runs
therefore retain exact machine-readable evidence instead of only an exception
message.

## Boundaries

| Pipeline | Contract | Generic checks | Named checks |
|---|---|---:|---|
| Upstox NSE daily fetch and retry | `market_data.ohlcv_daily.v1` | 8 | Cross-column OHLC ordering |
| Daily technical features | `feature.daily_technical.v1` | 8 | Target-column absence; failed-quality rows |
| Daily forward targets | `target.daily_forward_returns.v1` | 8 | Failed-quality rows |

No generic warning is accepted at these boundaries. Missing columns,
unregistered columns, incompatible types, null-policy violations, duplicate
keys, enum/range failures, failed freshness, and skipped checks all block.

## OHLCV normalization

Provider rows are mapped to the registered lower-case publication schema before
evaluation. The mapping intentionally preserves every row, including malformed
and duplicate rows. Storage normalization may not silently discard evidence
before the gate runs.

After the gate passes, the same normalized frame is written to the incremental
Parquet artifact and passed to the Timescale upsert. Storage preserves its
`fetched_at`, `source`, `exchange`, and `quality_status` values.

The named OHLC check rejects:

- high below low;
- high below open or close;
- low above open or close.

Positive prices, non-negative volume, nulls, and duplicates remain generic
contract checks.

## Freshness evidence

The publication context uses locally generated exchange sessions from the
repository's exchange-session implementation:

- ingestion compares the fetched batch with the requested window end;
- feature and target publication compare the output with the latest source
  OHLCV session.

This is a publication-batch freshness gate. Per-instrument eligible-session
coverage, listing boundaries, provider grace, suspensions, and explained
missingness remain WP1.3 and must not be inferred from this report.

## Dagster evidence

Successful `PipelineRunResult` values expose:

- `contract_validation` artifact path;
- `contract_validation_run_id`;
- `contract_validation_status`;
- `contract_validation_checks`.

The existing Dagster result recorder publishes those artifacts and scalar
metrics as asset metadata. A blocking gate raises before a result is returned,
so Dagster marks the asset execution failed while the JSON report remains on
disk.

## Verification

Regression fixtures prove:

- provider normalization exactly matches the registered OHLCV schema;
- duplicate and invalid-OHLC rows remain visible and fail together;
- a failed report is written before the exception is raised;
- feature target-column leakage and failed-quality rows fail;
- feature and target pipelines publish passed contract evidence.

WP1.3 is the next data-quality increment: eligible-session numerator and
denominator semantics with per-instrument explanations.
