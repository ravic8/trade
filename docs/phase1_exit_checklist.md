---
document_status: current
last_verified_date: 2026-09-04
owner: trade-research-platform
replaced_by: null
---

# Phase 1 Exit Checklist

## Decision

The Phase 1 repository implementation is ready for review and CI. Phase 1 is
not yet a production-complete milestone: the service-backed CI jobs must pass,
then the migration and observe-only checks must be performed on the production
host without changing provider authority.

## Repository evidence

| Gate | Status | Evidence |
|---|---|---|
| Unified validation results | passed locally | shared result/status model and regression tests |
| Versioned data contracts | passed locally | registry, evaluator, compatibility and publication-gate tests |
| Eligible-session coverage | passed locally | explicit valid numerator, eligible denominator, invalid/off-calendar counts |
| Production CLI guard | passed locally | complete command inventory, default-deny classification, exit-code tests |
| Durable API-to-Dagster mutation boundary | passed locally | `202` workflow submission, idempotent store, stopped sensor, lineage tests |
| Desired schedule manifest and drift | passed locally | required metadata, read-only actual-state join, stale/drift tests |
| Opportunities regressions | passed locally | storage/API tests plus desktop and Pixel 5 Playwright smoke tests |
| Python suite | passed locally | 595 passed, 2 service-gated skips |
| Branch-aware coverage | passed locally | 72%, threshold 70% |
| Ruff and mypy ratchet | passed locally | exact CI commands |
| Web unit/lint/build/browser | passed locally | Vitest, ESLint, TypeScript/Vite, Chromium desktop/mobile |
| Dependency audit | passed locally | Python and npm reported no known vulnerabilities |
| Whitespace/doc checks | passed locally | `git diff --check` and current-state checker |

The two local skips are intentional PostgreSQL/Timescale and Redis integration
tests. CI provisions those services and also verifies an empty-database upgrade
and the supported `0012` to `0013` migration path. Secret, source, dependency,
and built-image scans are CI gates because the local host has no Docker daemon.

## Required production evidence

These steps remain before Phase 1 can be marked complete:

1. Pass every required CI job on the pushed commit.
2. Take a fresh production backup and demonstrate a restore in an isolated
   destination before applying migration `20260904_0013`.
3. Deploy the validation, schedule-drift, workflow-request, and CLI-guard
   changes with the new workflow sensor still stopped.
4. Run read-only schedule and validation checks and confirm mutating CLI
   commands terminate before opening a database or provider connection.
5. Observe one normal production cycle and reconcile API, PostgreSQL, Dagster,
   and Opportunities evidence.
6. Record an owner and remediation decision or explicit risk acceptance for
   each Phase 0 P0 finding. Do not change provider authority in Phase 1.

## Push and merge rule

The working branch may be pushed to open a review and execute CI. Do not merge
or label Phase 1 complete until CI is green. Do not deploy or apply the new
migration until the backup/restore prerequisite and production runbook review
are complete.
