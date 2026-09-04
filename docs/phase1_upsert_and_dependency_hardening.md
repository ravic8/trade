---
document_status: current
last_verified_commit: working-tree
last_verified_date: 2026-08-25
owner: trade-research-platform
replaced_by: null
---

# Phase 1 Upsert and Research Dependency Hardening

## Purpose

Phase 0 observed PostgreSQL cardinality failures when a single bulk
`INSERT ... ON CONFLICT DO UPDATE` statement contained the same conflict key
more than once. It also observed the ML dataset step starting without its
required stock-coverage artifact.

This contract closes both failure classes at their boundaries.

## Bulk-write invariant

Every duplicate-prone bulk row builder emits at most one row per database
conflict or primary key before rows are chunked into SQL statements. This
covers:

- canonical and tradable universe members, symbols, and provider instruments;
- daily, hourly, and intraday OHLCV and daily price adjustments;
- corporate actions;
- exchange sessions, provider work, and provider-history evidence;
- daily features, forward targets, and Opportunity targets;
- stock-coverage and daily-fetch-coverage rows; and
- provider request logs.

If an input batch contains repeated keys, the final observation in that batch
wins. The returned or persisted row count is the deduplicated count. Database
conflict handling remains necessary for rows that already exist from an older
transaction; batch deduplication prevents PostgreSQL from attempting to update
the same existing row twice within one statement.

## Research artifact invariant

`processed_dataset_validation` publishes the generated `stock_coverage`
artifact in its `PipelineRunResult`. The `ml_dataset_v1` Dagster asset:

1. requires that named artifact to be present;
2. requires the published path to be an existing file; and
3. passes that exact path to the ML dataset pipeline.

The pipeline retains its default artifact name for standalone CLI/library use,
but orchestrated execution consumes the artifact produced by its declared
upstream asset. Dagster execution order alone is not treated as proof that a
filesystem dependency exists.

## Acceptance checks

- Repeated universe, candle, feature, or target keys do not produce a
  PostgreSQL cardinality or unique-key error.
- Daily and intraday candle deduplication use their full database keys,
  including source and interval where applicable.
- The ML asset does not start when validation omits `stock_coverage`.
- The ML asset does not start when the published coverage path is missing.
- A valid upstream coverage artifact is read from the exact published path,
  even when the default coverage location is absent.

## Production verification

After deployment, rerun the previously failing universe and yfinance batches,
then verify:

1. no run contains PostgreSQL SQLSTATE `21000` cardinality violations;
2. persisted row counts match distinct database conflict-key counts;
3. the daily research run records the validation-produced coverage path; and
4. `ml_dataset_v1` either consumes that path or fails before model-dataset
   construction with a required-artifact error.

These checks do not change provider selection, schedule state, retry policy,
or production data authority.
