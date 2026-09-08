# Phase 3 NSE Data Platform

## Status

Phase 3 is in progress on `codex/phase-3-nse-data-platform`. This phase follows
the approved V1 boundary: NSE only, research/backtesting only, daily candles and
yfinance minute candles within provider-available retention.

The implemented repository slices are not activated in production. Existing
PostgreSQL daily authority remains unchanged while the Phase 3 feature gate is
disabled.

## Implemented foundation

- A provider-independent request and candle contract carries provider,
  instrument, exchange, currency, interval, provider timestamp, request ID,
  adapter version, and raw-artifact lineage.
- Daily and minute candles pass a common fail-closed validator before a trusted
  Phase 3 write. It checks request identity, requested symbols, positive prices,
  OHLC containment, non-negative volume, completed-session eligibility,
  duplicates, provider timestamps, and NSE regular-session timestamps in
  `Asia/Kolkata`.
- Exact duplicate identities are removed idempotently. Conflicting duplicates
  and invalid candles fail the batch.
- Full normalized provider responses are stored as immutable,
  content-addressed JSON in the raw object-store namespace before filtered rows
  are validated. The object is registered in PostgreSQL `artifact_manifests`.
- ClickHouse migration `0002_market_data_intraday.sql` adds raw-request lineage
  to daily candles and creates the normalized `ohlcv_intraday` table.
- The existing durable yfinance daily worker uses the new snapshot, validation,
  and ClickHouse-replication boundary when Phase 3 is enabled.
- PostgreSQL now stores an idempotent candle-quality ledger. Accepted candles
  and validation findings are classified as `valid`, `duplicate`, `invalid`,
  `outside_session`, or `stale`; every absent expected daily or NSE minute
  candle is classified as `missing` or `provider_unavailable` with its reason.
- Each ClickHouse write records a durable replication checkpoint with the
  source and destination row count, deterministic candle digest, watermark,
  watermark lag, write latency, and terminal `reconciled`, `mismatch`, or
  `failed` state. A mismatch fails the ingestion attempt closed.
- A bounded NSE `1m` pipeline resolves the persisted active NSE universe,
  enforces the configured request-safety window, retains the unfiltered raw
  response, accepts only completed materialized sessions, validates timestamps
  in IST, and writes accepted rows to ClickHouse. Operators can select an
  explicit reviewed symbol set for a bounded canary; the selection is validated
  against the active universe and retained in run metadata.
- The configured minute lookback is no longer treated as a provider retention
  guarantee. Every NSE `1m` run stores content-addressed availability evidence
  per instrument: requested and observed sessions, first/last returned
  timestamp, returned row count, empty response or request-failure reason, raw
  artifact lineage, and observation time. Missing candles use these observed
  sessions to distinguish an unexplained in-session gap from a session the
  provider did not return.
- Minute completeness follows the V1 provider-observed-window rule. Minutes
  outside each returned session boundary are classified as
  `provider_unavailable`; absent minutes inside the observed boundary remain
  blocking `missing` outcomes.
- Multi-year daily replication is split into ClickHouse inserts spanning no
  more than 50 monthly partitions, keeping initial backfills below the server's
  partition-per-insert safety limit.
- The NSE minute path is available through
  `trade-research fetch-yfinance-nse-minute` and the
  `yfinance_nse_minute_job` Dagster job. Its schedule is stopped by default.
- Validated `1m` rows can now be aggregated on request into `5m`, `15m`,
  `30m`, or `1h` candles through ClickHouse and
  `GET /api/data/candles/aggregate`. Buckets are anchored to the NSE 09:15 IST
  open, carry source-run/raw-artifact lineage and a deterministic source
  digest, and expose source-versus-expected minute counts. Incomplete buckets
  are excluded by default and can be requested explicitly for diagnosis. The
  Python golden implementation covers the same OHLCV and final-partial-bucket
  rules used by the ClickHouse query.
- The authenticated Data Console now has an NSE Data view backed by
  `GET /api/data/operations/market-data-health`. It reports the latest quality
  run for each interval, observed/session/candle freshness, the Phase 3
  `>=99.5%` completeness measure, unexplained gaps, provider-unavailable rows,
  quarantined duplicate/invalid/stale/off-session rows, immutable raw-manifest
  IDs and digests, and the latest ClickHouse count/digest/watermark/latency
  checkpoint. Object-store locations are deliberately not returned.
- Historical NSE yfinance daily data can now be reconciled one ClickHouse
  calendar-month partition at a time with the manual-only
  `nse_daily_clickhouse_partition_reconciliation_job`. Audit mode compares
  PostgreSQL authority with the ClickHouse `FINAL` view using a deterministic
  business-field digest and records a durable checkpoint. Repair mode is
  bounded and only upserts missing or value-divergent authoritative rows;
  unexpected destination-only rows are never silently deleted and keep the
  checkpoint in `mismatch` for manual review.
- Every completed Upstox-versus-yfinance readiness comparison is now retained
  as content-addressed, append-only evidence. Repeating an identical comparison
  reuses the same evidence ID instead of increasing the observation count.
- Cutover eligibility requires five consecutive distinct passing session
  windows by default. An authenticated administrator must approve the exact
  evidence-bundle digest; the approval and its actor, reason, timestamp, and
  decision digest are retained in both the decision and audit ledgers.
- Setting `NSE_DAILY_PRIMARY_SOURCE=yfinance` is not sufficient by itself. The
  daily boundary fails closed and reports Upstox as the effective primary until
  an active approval exists. A stale-safe authenticated rollback decision
  immediately restores Upstox as the effective provider.
- The NSE Data view exposes configured versus effective primary, eligibility,
  evidence history, approval identity, and approval/rollback controls. The
  read endpoint is `GET /api/data/operations/nse-provider-cutover`; mutations
  use the admin endpoints under `/api/admin/nse-provider-cutover/` and require
  `X-Idempotency-Key`.
- A locked, language-neutral aggregation fixture now covers all eight
  `5m`/`15m`/`30m`/`1h` complete/incomplete cases, including the NSE 09:15
  anchor, a missing source minute, final 15-minute session buckets, exact OHLCV,
  source digests, and raw lineage. The read-only
  `verify-market-data-aggregation-golden` command validates the Python
  authority or independently produced candidate output before any Rust runtime
  path can be considered. See `docs/phase3_aggregation_golden_contract.md`.
- A fail-closed production-readiness gate now records content-addressed bounded
  canary and rollback/restore evidence. Canary assessment verifies daily and
  minute quality at `>=99.5%`, a maximum instrument scope, raw lineage,
  ClickHouse count/digest/watermark equality, observed provider sessions, and
  an independent minute rerun with the same business-row digest. The final
  gate also requires the provider-comparison window and a reviewed rollback to
  Upstox followed by explicit restoration approval.
- Readiness is exposed through `GET /api/data/operations/phase3-readiness`, the
  read-only `trade-research phase3-readiness` command, and the NSE Data view.
  Authenticated evidence recording is available under
  `/api/admin/phase3-readiness/`; these operations do not enable the feature.

## Fail-closed activation

Phase 3 cannot be enabled with only one storage plane. Settings validation
requires both of these to be writable:

```text
CLICKHOUSE_ENABLED=true
CLICKHOUSE_WRITE_ENABLED=true
OBJECT_STORE_ENABLED=true
OBJECT_STORE_WRITE_ENABLED=true
PHASE3_MARKET_DATA_ENABLED=true
PHASE3_PRODUCTION_ACTIVATION_ENABLED=false
```

The data-plane flags permit manual canary jobs. Production scheduling remains
stopped while `PHASE3_PRODUCTION_ACTIVATION_ENABLED=false`. Minute ingestion is
a separate rollout gate:

```text
YFINANCE_NSE_MINUTE_ENABLED=true
YFINANCE_NSE_MINUTE_LOOKBACK_DAYS=7
YFINANCE_NSE_MINUTE_MAX_SYMBOLS_PER_RUN=100
```

`YFINANCE_NSE_MINUTE_LOOKBACK_DAYS` is a request-size safety limit. It does not
assert that Yahoo retains or returns that entire window.

Production uses the corresponding `PROD_` variables. Only after the readiness
endpoint reports ready should production activation be enabled; that causes
schedule reconciliation to desire `yfinance_nse_minute_schedule` as running.
The production deployment script enforces the same rule after PostgreSQL
migration and stops the deployment if activation is requested while any
durable readiness gate is blocked.

The provider observation gate defaults to:

```text
NSE_CUTOVER_REQUIRED_PASSING_WINDOWS=5
PHASE3_CANARY_MAX_INSTRUMENTS=25
PHASE3_MINIMUM_COMPLETENESS=0.995
PHASE3_REQUIRED_OBSERVED_SESSIONS=5
```

This counts distinct comparison-window end sessions, not command invocations.

## Data flow

```text
Persisted NSE universe + materialized completed sessions
  -> bounded yfinance request
  -> immutable raw response snapshot + PostgreSQL manifest
  -> provider-independent candle adapter
  -> OHLC/session/timezone/duplicate validation
  -> PostgreSQL candle-quality outcomes
  -> PostgreSQL per-instrument observed-availability evidence
  -> PostgreSQL daily canonical commit (daily only)
  -> validated ClickHouse daily or 1m replica
  -> PostgreSQL replication checkpoint (count + digest + watermark)
  -> request-time NSE session aggregation (5m / 15m / 30m / 1h)
  -> content-addressed Upstox/yfinance comparison evidence
  -> authenticated evidence-bundle approval or explicit rollback
```

ClickHouse remains a replica. It cannot overwrite PostgreSQL daily candles.

## Remaining Phase 3 work

- Implement a Rust candidate only if profiling justifies it, then require its
  independently produced output to pass the locked aggregation fixture.
- The bounded daily/minute canary and independent minute rerun pass in the local
  operational environment. Production still requires its own canary evidence,
  the reviewed rollback/restore drill, and five distinct passing live
  provider-comparison windows. Implementation alone intentionally leaves the
  production gate blocked until those live evidence records pass.

## Exit gate

Phase 3 is not complete until daily completeness meets the approved threshold
(target `>=99.5%`), every missing candle is classified, duplicate ingestion is
idempotent, daily and minute freshness are visible in the UI, ClickHouse
reconciliation passes, and any Rust candidate matches the Python authority on
the locked golden dataset.
