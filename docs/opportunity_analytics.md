# Opportunity Analytics

The Opportunities page is a completed-session analytics surface. It replaces
the mock-backed Screeners navigation item and uses PostgreSQL/TimescaleDB as its
serving source.

## Target contract

For each instrument and completed trading session:

- `O`: open
- `H`: high
- `L`: low
- `C`: close
- `P`: the same instrument and provider's previous observed trading-session close

Version `daily_opportunity_outcomes_v1_0` implements the supplied definitions
exactly:

| UI label | Stored column | Formula |
| --- | --- | --- |
| Return | `session_return` | `(C - O) / O` |
| Gap | `gap` | `(O - P) / O` |
| True Return | `true_return` | `(C - P) / O` |
| Upside | `upside` | `(H - O) / O` |
| Downside | `downside` | `(O - L) / O` |
| Giveback | `giveback` | `(H - C) / O` |
| Recovery | `recovery` | `(C - L) / O` |
| Range | `session_range` | `(H - L) / O` |
| True Upside | `true_upside` | `(H - min(O, P)) / O` |
| True Downside | `true_downside` | `(max(O, P) - L) / O` |
| True Range | `true_range` | `True Upside + True Downside` |

The Gap denominator is intentionally `O`, matching the supplied definition.
The project-specific True Range is additive and is not Wilder True Range.

## Timing and leakage boundary

These values describe a realized session. `H`, `L`, and `C` are not known
before that session finishes, so the values must never be presented as
pre-session signals or included as same-session model inputs.

A later Forecast view may predict one or more of these outcomes. Its features
must be lagged to information known at prediction time, and the UI must keep
forecast values visually and semantically separate from realized values.

## Data flow

```text
PostgreSQL/TimescaleDB ohlcv_daily (source of truth)
  -> bounded instrument batches in the Dagster Opportunity target asset
  -> opportunity_targets_daily (idempotent natural key)
  -> GET /api/opportunities/daily
  -> React /opportunities
```

The natural key is `(instrument_key, source, date, target_version)`. The
pipeline groups previous closes by `(source, instrument_key)`, preventing a
provider or instrument boundary from supplying `P`.

The builder discovers instrument keys first and loads at most `batch_size`
instruments at a time (50 by default). Every batch is independently and
atomically upserted, and `target_runs.summary_json` records the completed batch
count. A failed invocation is safe to restart with the same arguments because
the natural-key upsert is idempotent. Incremental runs also load the exact last
valid row before the dirty window for every instrument, so the first recomputed
session retains the correct previous close without loading all history.

The `analytics.opportunity_targets_daily` read-only view exposes the same
dataset to DBeaver and CloudBeaver. BigQuery and a possible future ClickHouse
replica are downstream consumers, not application-serving authorities.

## Deployment and first build

The standard deployment applies Alembic revision `20260720_0009`. After the
application image is deployed, build each exchange from its available daily
OHLCV history:

```bash
DC=(docker compose --env-file /opt/trade/.env -f /opt/trade/app/docker-compose.prod.yml)

"${DC[@]}" run --rm --no-deps api \
  trade-research build-opportunity-targets --exchange NSE --ohlcv-source yfinance \
  --full-rebuild --keep-existing --batch-size 50

"${DC[@]}" run --rm --no-deps api \
  trade-research build-opportunity-targets --exchange TSX --ohlcv-source yfinance \
  --full-rebuild --keep-existing --batch-size 50

"${DC[@]}" run --rm --no-deps api \
  trade-research build-opportunity-targets --exchange US --ohlcv-source yfinance \
  --full-rebuild --keep-existing --batch-size 50
```

Subsequent runs use `--incremental`, which recomputes a bounded dirty window so
corrected OHLCV rows and previous-close relationships are updated
idempotently. The v1 Opportunities contract uses the active cross-exchange
`yfinance` daily feed for NSE, TSX, and US; PostgreSQL remains authoritative.

For long production builds, run the command in `tmux`. If a process or host
fails, repeat the same `--keep-existing` command; already committed batches are
updated rather than duplicated. Use `--replace-exchange` only for an explicitly
destructive clean rebuild because it deletes the selected exchange/source/version
before the first batch and therefore restarts from empty after a failure.

Each run writes per-batch Parquet artifacts below
`processed/opportunities/<exchange>_daily_targets/<run_id>/` and an exchange
manifest containing batch row counts and paths. The latest aggregate audit and
summary remain at stable exchange-specific paths.

## Verification

```sql
SELECT exchange, source, max(date) AS latest_session, count(*) AS rows
FROM public.opportunity_targets_daily
GROUP BY exchange, source
ORDER BY exchange, source;

SELECT
    count(*) AS rows,
    count(*) FILTER (WHERE true_range <> true_upside + true_downside) AS bad_true_range,
    count(*) FILTER (WHERE quality_status = 'passed' AND previous_close IS NULL) AS bad_quality
FROM public.opportunity_targets_daily;

SELECT
    run_id,
    status,
    rows,
    symbols,
    summary_json->>'completed_batches' AS completed_batches,
    summary_json->>'batch_count' AS batch_count,
    started_at,
    finished_at
FROM public.target_runs
WHERE dataset_name = 'daily_opportunity_outcomes'
ORDER BY started_at DESC
LIMIT 20;
```

Then verify the API and UI:

```bash
curl -fsS 'http://127.0.0.1:8081/api/opportunities/daily?exchange=NSE&limit=5'
```

Open `/opportunities`. The legacy `/screeners` path redirects there.
