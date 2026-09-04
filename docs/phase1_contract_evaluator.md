---
document_status: current
last_verified_commit: working-tree
last_verified_date: 2026-08-25
owner: trade-research-platform
replaced_by: null
---

# Phase 1 Generic Data-Contract Evaluator

## Purpose

`evaluate_frame_contract` evaluates the executable portions of a registered
`data_contract.v1` against a Pandas frame and emits a `validation_report.v1`.
The evaluator is generic: contract-specific column names, types, keys, enums,
ranges, and freshness policies come from the registry rather than conditional
pipeline code.

## Emitted checks

Each evaluation emits eight stable checks beneath the contract ID:

1. `schema.required_columns`
2. `schema.unregistered_columns`
3. `schema.logical_types`
4. `schema.nullability`
5. `keys.unique`
6. `values.allowed`
7. `values.ranges`
8. `freshness`

Missing required columns, incompatible values, required nulls, duplicate keys,
enum violations, and range violations fail. Unregistered additive columns warn
and require explicit downstream acceptance.

If a key column is missing, uniqueness is `skipped_with_reason` rather than
reported as passed. Session freshness is also skipped and remains blocking
when no eligible-session calendar is supplied.

## Freshness context

`ContractEvaluationContext` carries a timezone-aware evaluation timestamp and
an ordered, unique tuple of eligible session dates.

For `latest_completed_session`, callers supply the sessions whose exchange and
provider grace periods have expired. The evaluator compares the latest
observed session with that calendar, measures lag in eligible sessions, and
rejects both excessive lag and future sessions.

For `wall_clock`, the evaluator compares the newest registered basis timestamp
with the configured maximum age plus grace. Empty, stale, and future-dated
inputs fail.

`event_driven`, `immutable`, and `run_scoped` contracts do not invent an age
threshold and report their declared policy explicitly.

## Type and range behavior

- Null values are handled by the separate nullability check.
- Numeric types reject non-numeric and infinite values.
- Integer types reject fractional values.
- Date and datetime values must parse successfully.
- JSON values must serialize with the standard JSON encoder.
- Bounds may be inclusive or exclusive; daily OHLC prices use an exclusive
  zero lower bound.
- Evidence contains counts and a bounded set of JSON-safe examples.

## Boundary

The generic evaluator does not claim to execute prose cross-column or temporal
invariants such as OHLC ordering, leakage checks, or point-in-time eligibility.
Those remain named dataset-specific checks and must accompany generic evidence
before a hard consumer accepts the full contract.

Hard adoption at ingestion, feature, and target publication boundaries is
documented in `docs/phase1_contract_publication_gates.md`. Eligible-session
coverage semantics are the next increment.
