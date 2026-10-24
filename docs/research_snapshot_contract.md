# Daily snapshot boundary

The independent trade-research application consumes immutable directories exported
by Trade's `export-daily-snapshot` command. This is a file boundary, not a shared
Python library, research service or second ingestion feed.

```sh
trade-data export-daily-snapshot /path/to/new-snapshot \
  --start-date 2016-10-16 --end-date 2026-10-16
```

The backend CLI is trade-data; run it in Trade's own virtual environment. It is classified
read-only for the database; it writes the requested local output bundle. It does
not call Yahoo or initialize/create database schemas. It uses the configured
PostgreSQL endpoint and existing read privileges. Never put credentials in a
shared snapshot or manifest.

A PostgreSQL repeatable-read, read-only transaction selects the complete validated
materialized calendar for the requested window and then accepted NSE/yfinance
candles on its trading sessions. Missing calendar dates fail the export. Data is
written in bounded chunks (50,000 rows by default, at most 100,000). A failed read
publishes no completed bundle. An existing output directory is refused.

The directory contains manifest.json, sessions.parquet and ohlcv-NNNNN.parquet.
The manifest carries:

- contract daily.ohlcv.snapshot.v1 and schema version 1;
- snapshot ID, transaction as-of timestamp and export timestamp;
- provider yfinance, exchange NSE, interval 1d and inclusive date bounds;
- provider instrument identity, stored quality and adjustment semantics;
- calendar completeness assertion, candle row count, per-file counts and SHA-256.

Candles retain instrument_key, symbol, exchange, source, date, open/high/low/close,
volume, open interest when present, fetched_at and quality_status. Prices retain
the stored yfinance auto_adjust=False semantics; this is not a total-return series.
The export includes accepted records actually stored in the requested window.
It does not fabricate data, force every stock to have ten years of history, or
select stocks using liquidity. Reusing a historical artifact requires its own
provenance; the current canonical provider scope is deliberately NSE/yfinance.

Research verifies paths, digests, row totals, identity/provider/quality, OHLCV and
session membership before computation. Its input checksum and snapshot ID follow
research outputs in provenance.json. Research computes its historical full-window
coverage eligibility using the declared calendar; new listings can therefore be
excluded by the preserved v1 research policy. That research eligibility rule is
not a collection filter in Trade.

The snapshot writer is tested through a controlled transaction source. An actual
PostgreSQL export, including a repeated read under concurrent ingestion, remains
an integration acceptance check. No new historical download or production
snapshot was triggered for this local extraction.


## Whole-history validation and corrections

`trade-data export-validated-history /new/directory --start-date 2016-07-18
--end-date 2026-10-23` creates a single `nse_daily_validated.parquet`, separate
`quarantined.parquet` and `review_flags.parquet`, instrument coverage, session
provenance and a checksummed manifest. The original export is never overwritten.
This command is read-only and uses a repeatable-read transaction with bounded
batches. It rechecks prices and volume instead of trusting legacy `ok` labels.
Hard failures, unknown exchange sessions and known non-stock rights/partly paid
symbols are excluded from the accepted file. Zero volume, extreme returns and
missing or inconsistent adjustment factors are independent review flags.

Adjustment fields preserve Yahoo's `auto_adjust=False` conventions; they are not
independent corporate-action verification. Special exchange sessions are retained
using calendar membership. Historical coverage does not prove a complete listed
universe or absence of survivorship bias. Research selection belongs to the
research repository; ingestion applies no trading-liquidity filter.

Authoritatively verified corrections use `source_verified_correction` outcomes
in the existing quality ledger. Each records the instrument key, original and
corrected OHLCV, source URL and source archive checksum. Raw provider snapshots
remain unchanged. The correction is replayed before canonical validation and
replication, and at legacy storage entry points. A different future provider
price basis fails closed for re-verification rather than silently applying stale
prices. Unresolved hard anomalies retain their original values with suspicious
acceptance and reasoned quarantine evidence. Internal consistency alone does not
establish agreement with every NSE observation; inspect the audit manifest.
