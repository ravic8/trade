---
document_status: current
last_verified_commit: working-tree
last_verified_date: 2026-08-25
owner: trade-research-platform
replaced_by: null
---

# Phase 1 Data-Contract Registry

## Purpose

The code-backed registry is the authoritative inventory of dataset interfaces.
It separates a dataset's logical contract from its current storage location and
prevents key, type, units, nullability, freshness, or compatibility assumptions
from remaining implicit in pipeline code.

Each `data_contract.v1` record defines:

- a versioned contract and dataset identity;
- domain and owner;
- lifecycle and current authoritative store;
- physical table or artifact location and replicas;
- ordered primary key;
- columns, logical types, units, nullability, allowed values, and ranges;
- freshness basis, threshold, and grace period;
- cross-column or temporal invariants;
- semantic compatibility rules; and
- known current limitations.

Contracts are strict and immutable. Duplicate contracts, duplicate columns,
nullable or missing key columns, unknown freshness columns, invalid ranges,
and empty registries are rejected during import or CI.

## Registered contracts

| Contract | Current authority | Lifecycle |
|---|---|---|
| `calendar.exchange_sessions.v1` | PostgreSQL `exchange_sessions` | current |
| `universe.snapshots.v1` | PostgreSQL `universe_snapshots` | current |
| `universe.snapshot_members.v1` | PostgreSQL `universe_snapshot_members` | current |
| `instrument.canonical_identity.v1` | PostgreSQL `symbols` | current |
| `market_data.ohlcv_daily.v1` | PostgreSQL `ohlcv_daily` | current |
| `target.opportunity_daily.v1` | PostgreSQL `opportunity_targets_daily` | current |
| `feature.daily_technical.v1` | PostgreSQL plus active Parquet replica | transitional |
| `target.daily_forward_returns.v1` | PostgreSQL plus active Parquet replica | transitional |
| `research.ml_inputs.nse_daily.v1` | Processed filesystem bundle | transitional |
| `dataset.ml_daily.v1` | Local Parquet | transitional |
| `prediction.daily_rankings.v1` | Local Parquet | transitional |
| `backtest.daily_returns.v1` | Local CSV | transitional |

The transitional labels are deliberate current-state evidence. They do not
claim that the planned object-storage or experiment-registry authority already
exists.

## Compatibility policy

All contracts use semantic compatibility rules:

- adding a nullable column is a minor change;
- adding a required column, removing a column, or changing type/units is a
  major change;
- changing the primary key requires a new contract identity; and
- consumers must migrate explicitly before incompatible publication.

## Validation linkage

Processed validation uses `research.ml_inputs.nse_daily.v1` as its
`dataset_id`. Its summary records the contract ID, registry schema version, and
dataset version, and every structured validation check carries the same
contract identity.

PostgreSQL contract tests compare registered primary keys and column names with
the SQLAlchemy metadata. Frozen feature, target, Opportunity, and ML dataset
versions are also checked against their implementation constants.

## Executable evaluation

`docs/phase1_contract_evaluator.md` defines the generic evaluator for
registered column, key, nullability, enum, range, and freshness rules. Prose
cross-column and temporal invariants still require named dataset-specific
checks.
