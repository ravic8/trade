# Trade Research Agent

Production-grade V1 scaffold for a market research agent. The project turns the
original NSE/TSX notebooks into reusable pipelines for:

- exchange universes
- OHLCV ingestion
- market data quality checks
- range/volume/overnight feature engineering
- intraday range screening
- scraped research documents
- OpenAI embeddings
- Qdrant vector search

The notebooks remain useful for exploration. Production code lives under `src/`.

## Lens Vision

Lens is the trader-facing reasoning agent for this research stack. Its job is
not to echo a small set of canned database summaries. It should understand
natural questions from the trader's point of view, infer the analytical intent,
pick the right bounded tools, gather relevant evidence, reason carefully over
that evidence, and explain the result without bias or trade-pushing language.

For example, a prompt such as:

```text
When were highs and lows established for each stock, and what does that say
about distribution and market momentum?
```

should be understood as a market-structure and breadth question. Lens should be
able to plan the evidence it needs, use quantitative tools over the available
OHLCV data to locate per-stock high/low timing and distribution, compare those
distributions across the selected universe and sessions, validate data quality,
and return a grounded analysis of momentum or distribution with caveats and
provenance.

The target Lens is a function-heavy quant analyst:

- LLM reasoning should understand intent, plan analysis, compare evidence, and
  communicate uncertainty.
- Quant tools should compute facts such as OHLCV aggregates, high/low timing,
  breadth, returns, RSI, Bollinger Bands, model features, and backtest metrics.
- Fundamental tools should fetch structured financial facts while retrieval
  tools surface cited filings, transcripts, notes, and research context.
- Answers should remain unbiased: evidence, quality warnings, counter-signals,
  and uncertainty come before conclusions.
- Lens should improve daily through ingestion, training or extraction jobs where
  appropriate, trace review, prompt and tool evaluation loops, regression
  datasets, and feedback-driven quality checks.

The initial environments are intentionally limited to `local` and `production`.
Local development should make experiments easy and observable. Production work
should focus on data licensing, secrets, reliability, cost controls, monitoring,
and safe analyst workflows before any broader environment matrix is added.

## Lens Progress

Lens has a working V1 chat path and a strong data-pipeline base:

- TimescaleDB stores NSE and TSX symbols, hourly OHLCV data, exchange calendars,
  feed health, ingestion runs, and backlog windows.
- Dagster runs exchange-aware hourly ingestion, backlog detection, and recovery
  jobs.
- The FastAPI chat API exposes safe, bounded TimescaleDB and Qdrant gateway
  tools instead of raw SQL.
- The React research page shows Lens answers, data-quality badges, freshness,
  citations, provenance, and a debug audit view.
- Session summary, symbol time-series, data-quality, and research-retrieval
  tool surfaces exist.
- LLM answer rewriting has been wired behind provider settings, with OpenAI
  embeddings available for research document retrieval.
- Freshness evaluation now checks the latest exchange-aligned candle expected
  from the session calendar, so closed-market data is not incorrectly labeled
  stale just because wall-clock time advanced.

V1 still has deliberate limitations that the roadmap addresses:

- The current planner is a small Python intent router, not a strong
  natural-language LLM planner.
- Prompt text does not yet reliably resolve symbols, exchange scope, windows,
  indicators, model-building requests, or market-structure questions.
- The present tool set is safe but narrow, so difficult questions can fall back
  to generic summaries instead of evidence-rich analysis.
- Current answer synthesis is more template-and-rewrite than deep multi-step
  reasoning.
- Fundamentals ingestion, hybrid document retrieval, ML experiment workflows,
  and continuous agent evaluation are not yet complete.

## Lens Roadmap

Future Lens work should land as focused feature branches. Each branch should add
tests, audit visibility, and eval cases for the new question class it supports.

1. `feature/lens-direct-gemini-provider`
   - Move Lens runtime generation to the direct Gemini API for the initial
     cost-conscious provider path and remove OpenRouter from the active local
     and production chat path.
   - Add bounded output tokens, timeouts, retries, provider health, usage/cost
     telemetry, and failure visibility in debug audit.
2. `feature/lens-evals-and-traces`
   - Create the first reasoning evaluation dataset and per-turn traces for
     intent, plan, tools, evidence, citations, quality warnings, LLM status,
     latency, and regressions.
3. `feature/lens-query-understanding`
   - Resolve natural-language exchange scopes, tickers, time windows, trader
     concepts, indicator names, comparisons, and ML/fundamental intents before
     planning.
4. `feature/lens-structured-planner`
   - Replace shallow keyword routing with a schema-validated Gemini planner that
     selects only allowlisted bounded tools and has deterministic fallbacks for
     simple safe requests.
5. `feature/lens-market-structure-tools`
   - Add the quant evidence needed for prompts about when highs/lows were
     established, breadth distribution, intraday/session concentration,
     momentum participation, top/bottom cohorts, and market-wide confirmation.
6. `feature/lens-technical-analysis-tools`
   - Add deterministic RSI, Bollinger Bands, moving averages, MACD, ATR,
     crossovers, volatility, and technical-state tools over validated series.
7. `feature/lens-technical-reasoning`
   - Add evidence-grounded synthesis for technical and market-structure
     questions with charts, caveats, citations, and counter-signal checks.
8. `feature/lens-fundamentals-schema-ingestion`
   - Add structured company, statement, ratio, valuation, peer-group, and
     fiscal-period storage so changing financial facts are queried rather than
     memorized by an LLM.
9. `feature/lens-fundamental-doc-rag`
   - Ingest filings, annual reports, transcripts, notes, and research with
     finance-aware chunks, metadata, provenance, and embeddings.
10. `feature/lens-hybrid-fundamental-retrieval`
    - Combine structured metrics, vector search, keyword search, metadata
      filters, table-aware retrieval, and reranking for finance-grade evidence.
11. `feature/lens-hybrid-analyst-reasoning`
    - Answer hard cross-evidence prompts by comparing technical, fundamental,
      research, freshness, and counter-evidence before composing conclusions.
12. `feature/lens-ml-experiment-tools`
    - Add reproducible dataset, feature, baseline, LSTM, walk-forward
      validation, leakage-check, evaluation, backtest, and artifact tools.
13. `feature/lens-agent-graph-orchestration`
    - Add graph-style orchestration for stateful multi-step reasoning once the
      tools and evals are strong enough: planner, tool router, quant analyst,
      fundamental analyst, research retrieval, evidence validator, and answer
      composer nodes.
14. `feature/lens-production-hardening`
    - Add local/production configuration boundaries, secrets discipline,
      licensed data-provider decisions, rate limits, caching, cost budgets,
      monitoring, citation enforcement, graceful degradation, and deployment
      runbooks.

## Stack

- Python 3.11+
- FastAPI-ready package structure
- pandas/Parquet/DuckDB for analytical workflows
- PostgreSQL/TimescaleDB via `docker-compose.yml`
- Qdrant for low-latency vector retrieval
- Dagster for scheduled, observable data assets
- OpenAI embeddings for scraped research documents
- Typer CLI for V1 operations

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
docker compose up -d qdrant postgres redis
```

## Docker

Run the full V1 app stack:

```bash
docker compose up --build
```

Then open:

```text
http://localhost:5173
```

The compose stack starts:

- `web`: production-built React UI served by nginx
- `api`: FastAPI backend on `http://localhost:8000`
- `postgres`: TimescaleDB/PostgreSQL
- `redis`: job/cache broker
- `qdrant`: vector database
- `dagster-webserver`: orchestration UI on `http://localhost:3000`
- `dagster-daemon`: schedule runner for exchange-aware hourly NSE and TSX ingestion

## Databases

- **TimescaleDB/PostgreSQL** is the source of truth for structured market data:
  exchange symbols, hourly OHLCV candles, ingestion runs, and later screener
  outputs. Hourly candles are keyed by `(ticker, ts, source)`, so repeated
  hourly runs upsert the same candle instead of duplicating it.
- **Qdrant** is the vector index for the knowledge database: embedded document
  chunks from filings, exchange announcements, scraped research, and internal
  notes. Postgres should keep document metadata/provenance; Qdrant should power
  semantic retrieval with symbol/date/source filters.
- **Redis** is supporting infrastructure for jobs/cache style workloads. It is
  not currently the durable store for market data or knowledge.

Dagster currently defines four V1 market-data assets:

- `nse_universe`: fetches the official NSE equity list and stores symbols
- `nse_hourly_ohlcv`: fetches hourly Yahoo candles and upserts them into TimescaleDB
- `tsx_universe`: fetches the configured TSX symbol list and stores symbols
- `tsx_hourly_ohlcv`: fetches hourly Yahoo candles and upserts them into TimescaleDB

Dagster uses separate exchange schedules:

- `nse_hourly_schedule`: `45 9-16 * * 1-5`, timezone `Asia/Kolkata`
- `tsx_hourly_schedule`: `45 9-16 * * 1-5`, timezone `America/Toronto`

Dagster also runs exchange backlog sensors while the daemon is online:

- `nse_hourly_backlog_sensor`: scans completed recent NSE hourly windows
- `tsx_hourly_backlog_sensor`: scans completed recent TSX hourly windows

The sensors compare expected exchange-aligned hourly windows with stored
`ohlcv_hourly` symbol coverage. Missing or low-coverage windows are recorded in
`hourly_backlog_windows` and queue bounded recovery jobs. Recovery jobs fetch
the configured historical Yahoo hourly lookback, upsert candles, and rescore
the specific backlog window before marking it `recovered` or `partial`.

Each scheduled asset checks exchange sessions before fetching. NSE holidays come
from the official NSE trading holiday API, and TSX holidays come from the
official TMX trading calendar. Holidays are cached in TimescaleDB and refreshed
monthly by default (`CALENDAR_REFRESH_DAYS=30`); if no fresh cached calendar is
available and the official source cannot be reached, the guard fails closed and
skips the fetch instead of ingesting on an unverified session. The symbol
universes are also cached in TimescaleDB and refreshed weekly by default
(`UNIVERSE_REFRESH_DAYS=7`).

Docker defaults to ingesting the full cached NSE and TSX universes. Set
`NSE_INGEST_LIMIT` or `TSX_INGEST_LIMIT` only when you want a smaller smoke
run. Scheduled hourly assets use `HOURLY_REALTIME_LOOKBACK_DAYS=1`, so the
realtime pipelines refresh the current Yahoo hourly trading-day window instead
of downloading the last 10 days on every run.

Hourly candles are upserted by candle timestamp, but the scheduled pipeline is
not a gap-aware backlog replayer. If Docker or Dagster is down, the next
scheduled run fetches only the configured realtime lookback. Run an explicit
historical refresh with `trade-research backfill-hourly EXCHANGE` when you want
to refill a wider Yahoo hourly window after downtime or for historical storage.
That command uses `HOURLY_HISTORY_LOOKBACK_DAYS=10` by default and still lets
you override it with `--lookback-days`.

Automatic recent-gap recovery is controlled separately:

- `HOURLY_BACKLOG_ENABLED=true`
- `HOURLY_BACKLOG_SCAN_DAYS=10`
- `HOURLY_BACKLOG_COVERAGE_THRESHOLD=0.5`
- `HOURLY_BACKLOG_MAX_WINDOWS_PER_TICK=1`
- `HOURLY_BACKLOG_MAX_ATTEMPTS=3`
- `HOURLY_BACKLOG_MIN_CANDLE_LAG_MINUTES=20`
- `HOURLY_BACKLOG_STALE_RECOVERY_MINUTES=30`

The candle lag prevents a just-finished Yahoo hourly candle from being called
missing too early. The coverage threshold is intentionally less than `1.0`
because Yahoo does not serve every exchange symbol consistently; feed-health
filtering still controls which cached symbols are considered fetchable.
Queued or running recovery windows that stop making progress are eligible for a
new bounded attempt after the stale-recovery interval.

Yahoo feed health is tracked per symbol in TimescaleDB. Each hourly run skips
symbols whose `next_retry_at` is still in the future, marks successful symbols
`active`, backs off temporary failures, and marks repeated failures
`unsupported` for a weekly retry. Tune this with
`FEED_HEALTH_FAILURE_THRESHOLD`, `FEED_HEALTH_MAX_BACKOFF_HOURS`, and
`FEED_HEALTH_UNSUPPORTED_RETRY_DAYS`.

Yahoo fetching is bounded and conservative because it is a free development
source. The default fetcher uses batch size `20`, at most `2` concurrent batch
workers, `1s` between batch submissions, and retry/backoff/jitter settings from
`YFINANCE_RETRY_ATTEMPTS`, `YFINANCE_RETRY_BASE_SECONDS`, and
`YFINANCE_JITTER_SECONDS`.

Preview an exchange universe:

```bash
trade-research universe NSE --limit 10
trade-research universe TSX --limit 10
trade-research market-session NSE
trade-research market-session TSX
trade-research feed-health
```

Run the V1 screener with a small smoke-test universe:

```bash
trade-research run-screener NSE --limit 50
```

Outputs are written under `data/` as Parquet files.

Initialize the Timescale schema or run one-off hourly ingestions from the CLI:

```bash
trade-research init-db
trade-research ingest-hourly NSE --limit 20
trade-research ingest-hourly TSX --limit 20
trade-research backfill-hourly NSE --lookback-days 10
trade-research backfill-hourly TSX --lookback-days 10
```

Inside Docker:

```bash
docker compose exec api trade-research init-db
docker compose exec api trade-research ingest-hourly NSE --limit 20
docker compose exec api trade-research ingest-hourly TSX --limit 20
docker compose exec api trade-research backfill-hourly NSE --lookback-days 10
docker compose exec api trade-research backfill-hourly TSX --lookback-days 10
```

## Web UI

The V1 UI lives in `apps/web`. It is a Vite + React + TypeScript research
console with market status, job, symbol, screener, research, and Lens chat
surfaces. Some non-chat endpoints still keep sample-data fallbacks when live
backend data is unavailable.

```bash
cd apps/web
npm install
npm run dev
```

Open the URL printed by Vite, usually `http://localhost:5173`.

When the backend API is running on port `8000`, Vite proxies `/api/*` requests
to it. Without the backend, the UI still loads with sample market research data.

## Production Notes

Yahoo Finance is included only as a development/fallback data source. For
reliable production market data, add licensed providers behind the market data
provider interface and keep source metadata, fetch time, and quality flags.
