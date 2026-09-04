---
document_status: current
last_verified_commit: 59ce2de
last_verified_date: 2026-08-25
owner: trade-research-platform
replaced_by: null
---

# Phase 1 Pipeline Outcome Contract

## Purpose

Dagster process success must not conceal a failed or partially failed business
operation. Every `PipelineRunResult` therefore maps its internal status to a
canonical business outcome:

| Pipeline status | Business outcome | Meaning |
|---|---|---|
| `pass` | `succeeded` | Required work completed without business warnings. |
| `warn` | `degraded` | Work completed partially or a declared non-critical gate/warning remains. |
| `fail` | `failed` | A blocking invariant failed or required output is unavailable. |

Any other pipeline status is invalid at the Dagster boundary.

## Dagster behavior

All `PipelineRunResult` assets construct and attach these output metadata
fields before their boundary policy is applied:

```text
pipeline
status
business_outcome
rows
warnings
blocking_issues
artifacts and scalar metrics
```

Successful and permitted-degraded materializations retain the metadata on the
asset output. Failed steps retain the outcome and details in the Dagster error
log and exception even though no successful output is published.

Dagster does not provide a native degraded run status. V1 therefore uses two
explicit policies:

1. `failed` always raises and fails the asset/run.
2. `degraded` either:
   - fails a strict data-producing asset; or
   - records `business_outcome=degraded` and a warning for a declared
     non-critical or gated no-op.

No asset may return a `failed` result as a successful Dagster materialization.

## Strict degraded policy

A degraded result fails Dagster when continuing could publish incomplete or
ambiguous data. This includes:

- durable yfinance worker batches;
- Upstox or direct yfinance daily ingestion;
- the provider-neutral NSE daily boundary;
- intraday ingestion and gap validation;
- feature and target materialization;
- ML dataset materialization;
- direct Opportunity target materialization;
- a coverage-ready completed-session Opportunity build that reports failed
  target rows.

The durable yfinance worker also persists the operational run as
`completed_with_failures` before the Dagster asset raises. PostgreSQL business
evidence is therefore retained for diagnosis and retry.

## Permitted degraded policy

A degraded result may remain a successful Dagster process only when no unsafe
materialization was published and the degraded state is explicit. Current
cases are:

- exchange-session warnings that do not violate a blocking calendar rule;
- processed-dataset validation warnings that are allowed by its declared
  threshold contract;
- the final daily health report when it reports warnings but no blocking issue;
- a completed-session Opportunity check whose source coverage is not ready, so
  target materialization is deliberately skipped;
- non-blocking factor/diagnostic evidence.

These cases emit warning logs and `business_outcome=degraded`. A blocking issue
still fails immediately.

## Upstream behavior

Downstream assets reject an upstream `failed` result. Strict assets already
raise at their own boundary, so Dagster does not schedule dependent
materializations. Degraded validation/health results may flow only where the
downstream contract explicitly accepts their warning thresholds.

## Acceptance checks

- A work item in `retry_wait`, `terminal`, or with a lost claim makes the
  durable worker result degraded and the Dagster run fail.
- Heartbeat failure makes the worker result degraded and the Dagster run fail.
- A rejected universe snapshot fails its Dagster asset.
- Missing required validation input fails its Dagster asset.
- Failed feature, target, or ML rows cannot be published behind Dagster
  success.
- A coverage-gated no-op is visible as degraded and does not publish targets.
- Result metadata always contains the canonical business outcome.
- Unknown result statuses fail contract validation.

## Production verification

After deployment, compare PostgreSQL ingestion runs and Dagster runs over the
same observation window:

1. every `completed_with_failures` worker run has a corresponding failed
   Dagster run;
2. fully completed worker runs remain successful;
3. coverage-gated no-ops show degraded metadata and write no targets;
4. failed assets retain warnings, blocking issues, and operational run IDs;
5. retries remain idempotent and do not duplicate candles or published
   artifacts.

This verification changes no production retry, provider, or schedule policy.
