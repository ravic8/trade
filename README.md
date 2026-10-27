# Trade — NSE daily data backend

Trade owns daily NSE equity OHLCV collection, validation, PostgreSQL/TimescaleDB
storage, ClickHouse replication and operational health monitoring. Frontend,
fundamental analysis, ML and backtesting are outside this repository's scope.

The intended daily source is the official NSE final bhavcopy. **The source-code
and scheduled-collector migration is still pending.** This file cleanup does not
switch the running collectors or redeploy production.

## Retained audited data

The only retained market-data export is:

```
data/audited/nse_daily_2025-10-24_2026-10-23.parquet
data/audited/manifest.json
```

It contains 574,811 audited NSE bars for 2,612 ordinary main-market stocks across
247 sessions. Prices are unadjusted as traded; volume is exact exchange-reported
integer shares. Provenance, security IDs and archive checksums are included in
the rows. The manifest records its checksum and existing audit results.

Other generated exports, raw downloads, historical reports and temporary files
have been removed from this checkout rather than archived elsewhere. PostgreSQL,
ClickHouse, existing database dumps, secrets and infrastructure state are not
cleanup targets. Existing server data is outside this local cleanup.

## Development

Python 3.11+:

```sh
uv sync --extra dev
uv run pytest -q
uv run ruff check src tests scripts
python scripts/check_current_state_docs.py
```

Keep credentials in private environment configuration. See `.env.example`,
`.env.prod.example` and [operations](docs/operations.md). Infrastructure/schema
compatibility files remain in `deploy/`, `migrations/`, `clickhouse/` and Compose.
Do not run volume deletion or retention commands as part of repository cleanup.
