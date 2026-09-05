# Phase 3 NSE Data Platform

## Status

Phase 3 is in progress on `codex/phase-3-nse-data-platform`. This phase follows
the approved V1 boundary: NSE only, research/backtesting only, daily candles and
yfinance minute candles within provider-available retention.

The first repository slice is implemented but not activated in production.
Existing PostgreSQL daily authority remains unchanged while the Phase 3 feature
gate is disabled.

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
  enforces the configured yfinance retention window, retains the unfiltered raw
  response, accepts only completed materialized sessions, validates timestamps
  in IST, and writes accepted rows to ClickHouse.
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

## Fail-closed activation

Phase 3 cannot be enabled with only one storage plane. Settings validation
requires both of these to be writable:

```text
CLICKHOUSE_ENABLED=true
CLICKHOUSE_WRITE_ENABLED=true
OBJECT_STORE_ENABLED=true
OBJECT_STORE_WRITE_ENABLED=true
PHASE3_MARKET_DATA_ENABLED=true
```

Minute ingestion is a separate rollout gate:

```text
YFINANCE_NSE_MINUTE_ENABLED=true
YFINANCE_NSE_MINUTE_LOOKBACK_DAYS=7
YFINANCE_NSE_MINUTE_MAX_SYMBOLS_PER_RUN=100
```

Production uses the corresponding `PROD_` variables. Enabling the minute gate
causes schedule reconciliation to desire `yfinance_nse_minute_schedule` as
running. Start with a lower symbol maximum for the canary.

## Data flow

```text
Persisted NSE universe + materialized completed sessions
  -> bounded yfinance request
  -> immutable raw response snapshot + PostgreSQL manifest
  -> provider-independent candle adapter
  -> OHLC/session/timezone/duplicate validation
  -> PostgreSQL candle-quality outcomes
  -> PostgreSQL daily canonical commit (daily only)
  -> validated ClickHouse daily or 1m replica
  -> PostgreSQL replication checkpoint (count + digest + watermark)
  -> request-time NSE session aggregation (5m / 15m / 30m / 1h)
```

ClickHouse remains a replica. It cannot overwrite PostgreSQL daily candles.

## Remaining Phase 3 work

- Add daily Upstox-versus-yfinance reconciliation evidence and a signed NSE
  cutover/rollback gate for the agreed observation window.
- Expose daily/minute freshness, unexplained gaps, duplicate counts, quarantine,
  raw lineage, and ClickHouse replication lag in the authenticated UI.
- Add historical partition-level PostgreSQL-to-ClickHouse reconciliation and
  repair; the current checkpoint proves equality for each newly committed
  validated daily batch.
- Record observed yfinance minute availability instead of treating the
  configured retention bound as a provider guarantee.
- Build Python/Rust golden datasets before considering a Rust hot path.
- Run bounded canary, restore, rollback, and observation-window evidence.

## Exit gate

Phase 3 is not complete until daily completeness meets the approved threshold
(target `>=99.5%`), every missing candle is classified, duplicate ingestion is
idempotent, daily and minute freshness are visible in the UI, ClickHouse
reconciliation passes, and Python/Rust shadow results match on golden datasets.
