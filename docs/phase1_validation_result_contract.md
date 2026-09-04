---
document_status: current
last_verified_commit: working-tree
last_verified_date: 2026-08-25
owner: trade-research-platform
replaced_by: null
---

# Phase 1 Validation Result Contract

## Contract

Every migrated validation check emits `validation_result.v1` with these
required fields:

| Field | Meaning |
|---|---|
| `check_id` | Stable semantic identifier used by downstream policies. |
| `dataset_id` | Versioned identity of the validated dataset contract. |
| `run_id` | Validation execution identity, preferably the Dagster run ID. |
| `scope` | JSON-safe exchange, source, partition, or time-window dimensions. |
| `severity` | `info`, `warning`, or `error`. |
| `status` | `passed`, `warning`, `failed`, or `skipped_with_reason`. |
| `observed_value` | JSON-safe observed measurement or state. |
| `expected_value` | JSON-safe contract expectation. |
| `message` | Human-readable outcome. |
| `evidence` | Structured diagnostic evidence. |
| `created_at` | Timezone-aware timestamp normalized to UTC. |

Unknown fields, invalid identifiers, naive timestamps, and non-JSON evidence
are rejected. A `validation_report.v1` contains a unique set of checks for one
`dataset_id` and `run_id`; mixed identities and duplicate `check_id` values are
invalid. Empty reports are invalid so absence of checks cannot aggregate to a
false pass.

## Downstream policy

A hard dependency accepts `passed` checks by default. A `warning` is accepted
only when the consumer names that exact `check_id` in its contract. `failed`
and `skipped_with_reason` always block hard downstream use and cannot be
converted into safe warnings.

This policy makes warning acceptance reviewable and prevents a broad
"continue on warning" switch from admitting a new, unrelated warning.

## First adoption

Processed-dataset validation now publishes:

- `processed_dataset_validation_results_v1.json`;
- `validation_contract_version`, `validation_run_id`, `validation_status`, and
  `validation_results_path` in its compatibility summary; and
- the structured report on `ProcessedDatasetValidationResult`.

The initial report covers required processed input, unique keys, cleaned-row
preservation, feature and target contracts, cross-dataset key alignment,
baseline ML readiness, and a transitional compatibility-advisory check. The
legacy `overall_status`, `blocking_issues`, and `warnings` fields remain during
incremental migration.

The compatibility-advisory check is intentionally not declared safe for hard
dependencies. Its messages must be migrated to stable granular checks before a
consumer may accept selected warnings.

## Acceptance checks

- Every result is strict, versioned, UTC-normalized, and JSON serializable.
- Reports reject duplicate checks or mixed dataset/run identity.
- Unnamed warnings block downstream policy evaluation.
- Named warnings may pass only for that consumer.
- Failed and skipped checks remain blocking under every warning policy.
- The Dagster run ID flows into processed validation when available.
- Existing summary consumers continue to receive their current fields.

## Next migration boundary

Define the versioned data-contract registry, then replace the transitional
processed-dataset advisory with granular checks whose safe-warning decisions
are owned by individual consumers.
