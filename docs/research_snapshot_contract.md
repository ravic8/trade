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
