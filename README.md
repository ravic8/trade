# Trade — NSE daily data backend

Trade collects official NSE final bhavcopies, validates ordinary-equity OHLCV,
stores it in PostgreSQL/TimescaleDB, and verifies replication into ClickHouse.
The API serves only `nse_official_bhavcopy`; there is no alternate-provider fallback.
Frontend, fundamental analysis, ML and backtesting belong outside this repository.

## Data and coverage

The collector includes fully paid ordinary equities in EQ, BE, BZ, SM, ST and SZ
series, with explicit fund, debt, rights and partly paid exclusions. Volume is
exact integer exchange-reported shares; prices are unadjusted as traded. Every
eligible record in a final file must match both databases before that session is
marked complete. An absent exchange print is not a missing ingestion record.

The scheduled job checks after publication, retries unavailable/failed files,
and catches recent missed sessions. A separate, unscheduled job verifies and
completes the rolling past year through the latest eligible completed session.
Existing database history is preserved under its original source labels.

The immutable audited export remains at:

```
data/audited/nse_daily_2025-10-24_2026-10-23.parquet
data/audited/manifest.json
```

It contains 574,811 NSE bars for 2,612 main-market stocks across 247 sessions.
It is a frozen historical scope, not the live pipeline's coverage denominator.
No other generated exports, source archives or accumulated reports belong in
this checkout. Database storage, backups and credentials must not be deleted.

## Development

Python 3.11+:

```sh
uv sync --extra dev
uv run pytest -q
uv run ruff check
python scripts/check_current_state_docs.py
uv run trade-data doctor --check-services
```

Keep credentials in private environment configuration. See `.env.example`,
`.env.prod.example` and [operations](docs/operations.md). Historical schema
revision IDs, tables, instrument aliases and storage names remain compatible;
these do not enable legacy collectors.
