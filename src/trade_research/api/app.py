import logging
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from math import cos, sin
from time import monotonic

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import SQLAlchemyError

from trade_research.api.security import ChatRateLimitMiddleware, cors_origins
from trade_research.chat import ChatLLMClient, ChatOrchestrator, ChatPolicy, ChatToolGateway
from trade_research.config import Settings, get_settings
from trade_research.research.embeddings import OpenAIEmbeddingClient
from trade_research.schemas import (
    ChatFeedbackRequest,
    ChatFeedbackResponse,
    ChatQueryRequest,
    ChatQueryResponse,
    ChatSourcesResponse,
    ScreenerResult,
    SourcesPayload,
)
from trade_research.storage.timescale import TimescaleStore
from trade_research.storage.vector import QdrantVectorStore

app = FastAPI(title="Trade Research API", version="0.1.0")
logger = logging.getLogger(__name__)
settings = get_settings()

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins(settings.api_cors_origins),
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)
app.add_middleware(ChatRateLimitMiddleware, settings_getter=get_settings)


@app.middleware("http")
async def log_request_summary(request: Request, call_next):
    started_at = monotonic()
    response = await call_next(request)
    logger.info(
        "api request method=%s path=%s status=%s duration_ms=%s",
        request.method,
        request.url.path,
        response.status_code,
        max(int((monotonic() - started_at) * 1000), 0),
    )
    return response


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok", "time": datetime.now(UTC).isoformat()}


@app.post("/api/chat/query", response_model=ChatQueryResponse)
def chat_query(request: ChatQueryRequest) -> ChatQueryResponse:
    settings = get_settings()
    if not settings.chat_enabled:
        raise HTTPException(status_code=503, detail="Chat is disabled")
    try:
        return _chat_orchestrator().handle_query(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/chat/sources/{response_id}", response_model=ChatSourcesResponse)
def chat_sources(response_id: str) -> ChatSourcesResponse:
    settings = get_settings()
    if not settings.chat_enabled:
        raise HTTPException(status_code=503, detail="Chat is disabled")
    sources = _chat_orchestrator().get_sources(response_id)
    return ChatSourcesResponse(response_id=response_id, sources=SourcesPayload(**sources))


@app.get("/api/chat/audit/{response_id}")
def chat_audit(response_id: str) -> dict:
    settings = get_settings()
    if not settings.chat_enabled:
        raise HTTPException(status_code=503, detail="Chat is disabled")
    payload = _chat_orchestrator().get_audit(response_id)
    if not payload:
        raise HTTPException(status_code=404, detail="Audit record not found")
    return payload


@app.post("/api/chat/feedback", response_model=ChatFeedbackResponse)
def chat_feedback(_request: ChatFeedbackRequest) -> ChatFeedbackResponse:
    settings = get_settings()
    if not settings.chat_enabled:
        raise HTTPException(status_code=503, detail="Chat is disabled")
    return ChatFeedbackResponse(status="accepted")


@app.get("/api/chat/health")
def chat_health() -> dict[str, object]:
    settings = get_settings()
    return {
        "enabled": settings.chat_enabled,
        "strictCitationRequired": settings.chat_strict_citation_required,
        "qdrantConfigured": bool(settings.qdrant_url),
        "embeddingConfigured": bool(settings.openai_api_key),
        "llmProvider": "gemini",
        "llmConfigured": bool(_llm_api_key_present(settings)),
        "llmAnswerEnabled": settings.chat_use_llm_answer,
        "checkedAt": datetime.now(UTC).isoformat(),
    }


@app.get("/api/market/status")
def market_status() -> list[dict]:
    try:
        rows = _store().market_status()
        if rows:
            return [
                {
                    "exchange": row["exchange"],
                    "universeSize": row["universe_size"],
                    "lastOhlcvRun": _iso(row["last_ohlcv_run"]),
                    "lastScreenerRun": None,
                    "staleSymbols": max(row["universe_size"] - row["candle_symbols"], 0),
                    "dataQualityScore": 100.0 if row["latest_candle"] else 0.0,
                    "latestCandle": _iso(row["latest_candle"]),
                    "lastOhlcvStatus": row["last_ohlcv_status"],
                }
                for row in rows
            ]
    except SQLAlchemyError:
        pass

    now = datetime.now(UTC)
    return [
        {
            "exchange": "NSE",
            "universeSize": 2365,
            "lastOhlcvRun": (now - timedelta(minutes=35)).isoformat(),
            "lastScreenerRun": (now - timedelta(minutes=8)).isoformat(),
            "staleSymbols": 18,
            "dataQualityScore": 98.2,
        },
        {
            "exchange": "TSX",
            "universeSize": 687,
            "lastOhlcvRun": (now - timedelta(minutes=52)).isoformat(),
            "lastScreenerRun": (now - timedelta(minutes=16)).isoformat(),
            "staleSymbols": 11,
            "dataQualityScore": 96.8,
        },
    ]


@app.get("/api/screeners/intraday-range/latest")
def latest_intraday_range() -> list[dict]:
    now = datetime.now(UTC)
    rows = [
        {
            "ticker": "RELIANCE.NS",
            "exchange": "NSE",
            "company": "Reliance Industries",
            "signal": "Intraday range expansion",
            "liquidity": 2_145_000_000,
            "d5Up0100": 4,
            "d5Dn0100": 3,
            "d5ClUp0200": 1,
            "d5VUp0200": 2,
            "matchedAt": now.isoformat(),
        },
        {
            "ticker": "INFY.NS",
            "exchange": "NSE",
            "company": "Infosys",
            "signal": "Range expansion without close shock",
            "liquidity": 1_128_000_000,
            "d5Up0100": 3,
            "d5Dn0100": 4,
            "d5ClUp0200": 0,
            "d5VUp0200": 2,
            "matchedAt": now.isoformat(),
        },
        {
            "ticker": "WDO.TO",
            "exchange": "TSX",
            "company": "Wesdome Gold Mines",
            "signal": "Two-way intraday expansion",
            "liquidity": 43_800_000,
            "d5Up0100": 3,
            "d5Dn0100": 3,
            "d5ClUp0200": 2,
            "d5VUp0200": 1,
            "matchedAt": now.isoformat(),
        },
    ]
    return [
        ScreenerResult(**_to_screener_result(row)).model_dump(mode="json") | row
        for row in rows
    ]


@app.get("/api/symbols/{ticker}/candles")
def symbol_candles(ticker: str) -> list[dict]:
    try:
        rows = _store().candles(ticker)
        if rows:
            return [
                {
                    "time": _iso(row["ts"]),
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "ticker": row["ticker"],
                }
                for row in rows
            ]
    except SQLAlchemyError:
        pass

    start = datetime.now(UTC).date() - timedelta(days=90)
    candles = []
    for index in range(70):
        base = 100 + sin(index / 5) * 4 + index * 0.08
        open_price = round(base + sin(index) * 0.8, 2)
        close = round(base + cos(index / 2) * 0.9, 2)
        high = round(max(open_price, close) + 1.2 + (index % 4) * 0.18, 2)
        low = round(min(open_price, close) - 1.1 - (index % 3) * 0.15, 2)
        candles.append(
            {
                "time": (start + timedelta(days=index)).isoformat(),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": 850_000 + index * 12_000 + (index % 5) * 70_000,
                "ticker": ticker,
            }
        )
    return candles


@app.get("/api/research/notes")
def research_notes(ticker: str | None = None) -> list[dict]:
    now = datetime.now(UTC)
    notes = [
        {
            "id": "note-1",
            "ticker": "RELIANCE.NS",
            "title": "Energy and telecom subsidiaries remain primary catalyst cluster",
            "sourceType": "agent",
            "publishedAt": (now - timedelta(minutes=12)).isoformat(),
            "summary": (
                "The signal is price-behavior driven. Current retrieved context points "
                "to sector flows rather than a single confirmed company-specific event."
            ),
            "confidence": 0.74,
        },
        {
            "id": "note-2",
            "ticker": "INFY.NS",
            "title": "IT sector range widening with muted close-to-open follow-through",
            "sourceType": "news",
            "publishedAt": (now - timedelta(minutes=44)).isoformat(),
            "summary": (
                "Recent documents mention broad IT rotation. The screener pattern suggests "
                "active intraday participation without large overnight shock."
            ),
            "confidence": 0.69,
        },
        {
            "id": "note-3",
            "ticker": "WDO.TO",
            "title": "Gold miners screen as volatility candidates",
            "sourceType": "exchange",
            "publishedAt": (now - timedelta(hours=1)).isoformat(),
            "summary": (
                "Commodity-linked names are clustering in the range screen. Confirm "
                "source-level news before attributing the move."
            ),
            "confidence": 0.66,
        },
    ]
    if ticker:
        return [note for note in notes if note["ticker"].upper() == ticker.upper()]
    return notes


@app.get("/api/jobs/latest")
def latest_jobs() -> list[dict]:
    try:
        rows = _store().latest_runs()
        if rows:
            return [
                {
                    "id": row["run_id"],
                    "name": row["job_name"],
                    "status": row["status"],
                    "startedAt": _iso(row["started_at"]),
                    "durationSeconds": _duration_seconds(row["started_at"], row["finished_at"]),
                    "itemsProcessed": row["items_processed"],
                    "itemsSucceeded": row["items_succeeded"],
                    "itemsFailed": row["items_failed"],
                    "exchange": row["exchange"],
                    "source": row["source"],
                    "errorMessage": row["error_message"],
                }
                for row in rows
            ]
    except SQLAlchemyError:
        pass

    now = datetime.now(UTC)
    return [
        {
            "id": "job-101",
            "name": "NSE OHLCV ingestion",
            "status": "completed",
            "startedAt": (now - timedelta(minutes=50)).isoformat(),
            "durationSeconds": 615,
            "itemsProcessed": 2347,
        },
        {
            "id": "job-102",
            "name": "Intraday range screener",
            "status": "completed",
            "startedAt": (now - timedelta(minutes=18)).isoformat(),
            "durationSeconds": 18,
            "itemsProcessed": 1654,
        },
        {
            "id": "job-103",
            "name": "Research document embedding",
            "status": "running",
            "startedAt": (now - timedelta(minutes=4)).isoformat(),
            "durationSeconds": None,
            "itemsProcessed": 128,
        },
    ]


def _to_screener_result(row: dict) -> dict:
    return {
        "ticker": row["ticker"],
        "strategy": "intraday_range_v1",
        "matched_at": datetime.fromisoformat(row["matchedAt"]),
        "metrics": {
            "exchange": row["exchange"],
            "company": row["company"],
            "signal": row["signal"],
            "liquidity": row["liquidity"],
            "d5Up0100": row["d5Up0100"],
            "d5Dn0100": row["d5Dn0100"],
            "d5ClUp0200": row["d5ClUp0200"],
            "d5VUp0200": row["d5VUp0200"],
        },
    }


def _store() -> TimescaleStore:
    settings = get_settings()
    return TimescaleStore(settings.database_url)


@lru_cache(maxsize=1)
def _chat_orchestrator() -> ChatOrchestrator:
    settings = get_settings()
    embedding_client = (
        OpenAIEmbeddingClient(
            api_key=settings.openai_api_key,
            model=settings.openai_embedding_model,
            base_url=settings.openai_base_url,
        )
        if settings.openai_api_key
        else None
    )
    gateway = ChatToolGateway(
        settings=settings,
        timescale_store=TimescaleStore(settings.database_url),
        vector_store=QdrantVectorStore(
            url=settings.qdrant_url,
            api_key=settings.qdrant_api_key,
            collection=settings.qdrant_collection,
        ),
        embedding_client=embedding_client,
    )
    llm_client = None
    if settings.chat_use_llm_answer and _llm_api_key_present(settings):
        llm_client = ChatLLMClient(settings=settings)
    return ChatOrchestrator(
        settings=settings,
        tools=gateway,
        llm_client=llm_client,
        policy=ChatPolicy(),
    )


def _llm_api_key_present(settings: Settings) -> bool:
    return bool(settings.gemini_api_key)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _duration_seconds(started_at: datetime | None, finished_at: datetime | None) -> int | None:
    if not started_at or not finished_at:
        return None
    return int((finished_at - started_at).total_seconds())
