# Trade Analyst Chatbot v1 - Frozen Implementation Contract

## 1. Purpose
Build a production-safe analyst chatbot that:
- answers market-data questions from TimescaleDB,
- answers research-context questions from Qdrant,
- supports hybrid answers across both,
- always provides provenance and quality/freshness signaling.

This document is the implementation contract for v1. API, schemas, safety boundaries, and badge logic below are considered frozen unless explicitly revised.

## 2. Current System Baseline (As Of 2026-05-22)
- NSE and TSX universe pipelines exist.
- Hourly OHLCV ingestion runs via yfinance.
- TimescaleDB stores symbols, hourly OHLCV, feed health, ingestion runs, exchange holidays, and backlog windows.
- Dagster schedules run exchange-aware realtime ingestion.
- Realtime schedules use 1-day lookback.
- Historical recovery uses backfill jobs with 10-day lookback.
- Automatic backlog sensors detect missing/partial windows and trigger recovery jobs.
- Feed health and row-quality flags exist.
- Data quality snapshot:
  - NSE: active universe 2364, latest complete-session stocks 2172, latest candle 2026-05-22 15:15 IST.
  - TSX: active universe 660, latest complete-session stocks 408, latest candle 2026-05-21 15:30 America/Toronto.
- FastAPI and React scaffolds exist.
- Qdrant + embedding foundations exist.

## 3. Non-Goals (v1)
- No order execution or autonomous trade actions.
- No direct raw SQL generation/execution from user text.
- No uncited factual claims.

## 4. Architecture
1. Chat API Orchestrator (FastAPI)
- Endpoint entrypoint for chat turns.
- Handles request validation, policy checks, routing, and response assembly.

2. Tool Gateway (Safety Boundary)
- Exposes only whitelisted, typed, bounded read operations.
- No DB credentials or SQL text exposed to model.

3. Timescale Query Layer
- Implements approved query templates over existing tables.
- Returns structured results and query provenance.

4. Qdrant Retrieval Layer
- Performs vector search with metadata filters.
- Returns ranked chunks with source metadata for citations.

5. LLM Orchestration
- Planner stage: intent + tool-call planning.
- Answer stage: evidence-grounded response assembly with mandatory citations.

6. UI Chat Surface (React)
- Chat interaction, scope controls, quality badge display, and source drawer.

## 5. Backend Module Mapping
- API router: `src/trade_research/api/app.py`
- Schemas: `src/trade_research/schemas.py`
- Timescale store: `src/trade_research/storage/timescale.py`
- Vector store: `src/trade_research/storage/vector.py`
- Settings: `src/trade_research/config.py`
- New package: `src/trade_research/chat/`
  - `orchestrator.py`
  - `policy.py`
  - `tools.py`
  - `provenance.py`
  - `quality.py`

## 6. API Contract
All routes are under existing API namespace.

### 6.1 POST /api/chat/query
Request:
```json
{
  "message": "How did NSE banks perform in the latest complete session?",
  "context": {
    "exchange": "NSE",
    "symbols": [],
    "timezone": "Asia/Kolkata"
  },
  "options": {
    "max_latency_ms": 8000,
    "strict_quality": true
  },
  "session_id": "sess_123",
  "user_id": "user_abc"
}
```

Response:
```json
{
  "response_id": "resp_456",
  "session_id": "sess_123",
  "answer": {
    "text": "NSE bank names were broadly positive in the latest complete session...",
    "quality_badge": "partial",
    "freshness": {
      "market_data_as_of": "2026-05-22T15:15:00+05:30",
      "research_data_as_of": "2026-05-20T12:00:00+05:30"
    },
    "warnings": [
      "Coverage is partial: 2172/2364 active NSE symbols had complete latest-session candles."
    ],
    "follow_ups": [
      "Show top/bottom 10 by % change within banks.",
      "Compare with previous 5 complete sessions."
    ]
  },
  "citations": [
    {
      "id": "c1",
      "type": "timescale_query",
      "label": "Session breadth and returns",
      "provenance_ref": "prov_ts_001"
    },
    {
      "id": "c2",
      "type": "qdrant_chunk",
      "label": "Banking sector memo (May 2026)",
      "provenance_ref": "prov_qd_003"
    }
  ],
  "trace_id": "trace_xyz"
}
```

Errors:
- 400: invalid request shape, scope, or parameter values.
- 422: unsupported/unsafe request under policy.
- 503: dependency unavailable or timeout with no safe fallback.

### 6.2 GET /api/chat/sources/{response_id}
Purpose: fetch full provenance for UI transparency.

Response:
```json
{
  "response_id": "resp_456",
  "sources": {
    "timescale": [
      {
        "provenance_ref": "prov_ts_001",
        "template_id": "session_summary_v1",
        "parameters": {
          "exchange": "NSE",
          "session_date": "2026-05-22"
        },
        "time_range": {
          "start": "2026-05-22T09:15:00+05:30",
          "end": "2026-05-22T15:15:00+05:30"
        },
        "row_count": 2172,
        "executed_at": "2026-05-22T15:40:04+05:30"
      }
    ],
    "qdrant": [
      {
        "provenance_ref": "prov_qd_003",
        "collection": "market_research_documents",
        "doc_id": "doc_981",
        "chunk_id": "chunk_12",
        "score": 0.83,
        "as_of_date": "2026-05-20",
        "title": "India Financials Weekly"
      }
    ]
  }
}
```

### 6.3 POST /api/chat/feedback
Request:
```json
{
  "response_id": "resp_456",
  "rating": "down",
  "reason": "Missing TSX comparison requested in question",
  "user_id": "user_abc"
}
```

Response:
```json
{
  "status": "accepted"
}
```

### 6.4 GET /api/chat/health
Returns readiness and dependency state for chat stack.

## 7. Internal Tool Gateway Contract
The model can call only these internal tools via orchestrator.

1. `market_data.get_latest_candles`
- Params: `exchange`, `symbols[]`, `lookback_hours`
- Limits: symbols <= 200, lookback_hours <= 72

2. `market_data.get_session_summary`
- Params: `exchange`, optional `session_date`, optional `universe_filter`

3. `market_data.get_symbol_timeseries`
- Params: `exchange`, `symbol`, `start_time`, `end_time`, `interval`
- v1 interval: `1h` only

4. `market_data.get_data_quality`
- Params: `exchange`, `date|time_range`
- Returns completeness and backlog/health indicators

5. `research.search_docs`
- Params: `query`, optional `exchange`, optional `symbols[]`, `top_k`, optional `as_of_after`
- Limits: `top_k <= 12`

Hard safety rules:
- No raw SQL tool.
- No arbitrary table names or joins from model output.
- Read-only DB role.
- Per-tool timeout budget enforced.
- Max rows and max time-range clamps enforced.

## 8. Intent Taxonomy and Orchestration
Intent classes:
- `price_lookup`
- `session_summary`
- `relative_performance`
- `data_quality_check`
- `research_lookup`
- `hybrid_explain`

Planner output must be strict JSON with explicit tool calls.
Answer generator receives only planner output + tool results and must emit:
- direct answer,
- warnings,
- citations.

No-citation final answer is a policy failure and must not be returned to user as factual output.

## 9. Provenance Contract
Every citation must map to a stable provenance record.

Citation types:
- `timescale_query`
- `qdrant_chunk`

Timescale provenance includes:
- template_id
- typed parameters
- time range
- row count
- execution timestamp

Qdrant provenance includes:
- collection
- doc_id
- chunk_id
- score
- as_of_date
- title

## 10. Quality, Freshness, and Badge Policy
Badges:
- `complete`
- `partial`
- `stale`

Default thresholds:
- NSE `complete` if latest-session completeness ratio >= 0.95
- TSX `complete` if latest-session completeness ratio >= 0.90
- Else `partial`

`stale` conditions:
- latest expected exchange-hour candle delayed by >2 intervals,
- and not explained by exchange holiday/expected closure.

Timezone semantics:
- NSE evaluated in `Asia/Kolkata`
- TSX evaluated in `America/Toronto`

If coverage is insufficient for a requested claim scope, response must include explicit warning and avoid overconfident language.

## 11. Refusal and Degraded-Mode Policy
Refuse:
- raw SQL requests,
- prompt/policy exfiltration attempts,
- requests to execute trades/orders,
- unsupported out-of-bounds data requests.

Degraded behavior:
- If Qdrant fails and market data succeeds: answer with market-only warning.
- If Timescale fails for requested factual market claim: return cannot-verify message; no fabricated output.

## 12. Observability and Audit
Per chat turn capture:
- `trace_id`, `response_id`, `session_id`, `user_id`
- intent + planned tool calls
- tool status/latency/error
- final badge and warnings
- citation coverage metadata

Persist audit payload:
- request
- planner result
- tool outputs (sanitized)
- final response
- provenance bundle

## 13. Frontend Contract (React)
Files to extend:
- `apps/web/src/api/types.ts`
- `apps/web/src/api/client.ts`
- `apps/web/src/api/hooks.ts`
- `apps/web/src/pages/ResearchPage.tsx` (or dedicated Chat page)

UI requirements:
- chat input,
- exchange scope controls: NSE, TSX, Both,
- optional symbol scope,
- quality badge with freshness,
- warnings surface,
- expandable source drawer grouped by Market Data / Research,
- clickable provenance rows tied to `/api/chat/sources/{response_id}`.

## 14. Test and Acceptance Requirements
Backend tests:
- chat endpoint contract tests,
- policy refusal tests,
- tool parameter clamp/validation tests,
- degraded-path tests,
- citation-required tests.

Frontend tests:
- rendering of badge/warnings,
- source drawer behavior,
- fallback messaging in degraded responses.

Acceptance gates:
1. Factual answers always include at least one citation.
2. Unsafe requests are refused deterministically.
3. Badge/warning output aligns with live completeness/freshness.
4. Degraded dependencies never produce fabricated claims.

## 15. PR Sequence
1. PR1: doc + schema + config constants.
2. PR2: tool gateway + Timescale query templates + Qdrant retrieval wrapper.
3. PR3: orchestration + policy + quality evaluator.
4. PR4: API routes + backend tests.
5. PR5: UI integration + frontend tests.
6. PR6: observability/audit hardening.

## 16. Open Decisions To Resolve Before PR2
1. Citation strictness mode default:
- strict fail-closed always,
- or allow non-factual conversational fallback without citations.

2. Cross-exchange default in ambiguous prompts:
- default to user-selected scope only,
- or infer Both when unspecified.

3. Max latency target for `/api/chat/query`:
- recommended p95 target <= 8 seconds in v1.

