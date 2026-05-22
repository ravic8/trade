from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

from qdrant_client.http import models

from trade_research.config import Settings
from trade_research.research.embeddings import OpenAIEmbeddingClient
from trade_research.schemas import ProvenanceQdrant, ProvenanceTimeRange, ProvenanceTimescale
from trade_research.storage.timescale import TimescaleStore
from trade_research.storage.vector import QdrantVectorStore


class ChatToolGateway:
    """Safe, bounded tool surface for chatbot market/research retrieval."""

    def __init__(
        self,
        settings: Settings,
        timescale_store: TimescaleStore,
        vector_store: QdrantVectorStore,
        embedding_client: OpenAIEmbeddingClient | None = None,
    ) -> None:
        self.settings = settings
        self.timescale_store = timescale_store
        self.vector_store = vector_store
        self.embedding_client = embedding_client

    def get_latest_candles(
        self,
        exchange: str,
        symbols: list[str] | None = None,
        lookback_hours: int = 24,
    ) -> dict:
        normalized_exchange = _normalize_single_exchange(exchange)
        normalized_symbols = self._normalize_symbols(symbols or [])
        bounded_lookback = min(lookback_hours, self.settings.chat_max_lookback_hours)
        rows = self.timescale_store.latest_candles(
            exchange=normalized_exchange,
            symbols=normalized_symbols or None,
            lookback_hours=bounded_lookback,
        )
        now = datetime.now(UTC)
        return {
            "data": rows,
            "provenance": ProvenanceTimescale(
                provenance_ref=_provenance_ref("ts"),
                template_id="latest_candles_v1",
                parameters={
                    "exchange": normalized_exchange,
                    "symbols": normalized_symbols,
                    "lookback_hours": bounded_lookback,
                },
                time_range=ProvenanceTimeRange(
                    start=now.replace(microsecond=0),
                    end=now.replace(microsecond=0),
                ),
                row_count=len(rows),
                executed_at=now,
            ).model_dump(mode="json"),
        }

    def get_session_summary(
        self,
        exchange: str,
        session_date: date | None = None,
    ) -> dict:
        normalized_exchange = _normalize_single_exchange(exchange)
        summary = self.timescale_store.session_summary(
            exchange=normalized_exchange,
            session_date=session_date,
        )
        return {
            "data": summary,
            "provenance": ProvenanceTimescale(
                provenance_ref=_provenance_ref("ts"),
                template_id="session_summary_v1",
                parameters={
                    "exchange": normalized_exchange,
                    "session_date": session_date.isoformat() if session_date else None,
                },
                time_range=None,
                row_count=1 if summary else 0,
                executed_at=datetime.now(UTC),
            ).model_dump(mode="json"),
        }

    def get_symbol_timeseries(
        self,
        exchange: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: str = "1h",
    ) -> dict:
        if interval != "1h":
            raise ValueError("Only 1h interval is supported in v1")
        normalized_exchange = _normalize_single_exchange(exchange)
        normalized_symbol = symbol.upper().strip()
        bounded_start, bounded_end = self._bound_time_range(start_time, end_time)
        rows = self.timescale_store.symbol_timeseries(
            exchange=normalized_exchange,
            symbol=normalized_symbol,
            start_time=bounded_start,
            end_time=bounded_end,
        )
        return {
            "data": rows,
            "provenance": ProvenanceTimescale(
                provenance_ref=_provenance_ref("ts"),
                template_id="symbol_timeseries_v1",
                parameters={
                    "exchange": normalized_exchange,
                    "symbol": normalized_symbol,
                    "interval": interval,
                },
                time_range=ProvenanceTimeRange(start=bounded_start, end=bounded_end),
                row_count=len(rows),
                executed_at=datetime.now(UTC),
            ).model_dump(mode="json"),
        }

    def get_data_quality(self, exchange: str) -> dict:
        normalized_exchange = _normalize_single_exchange(exchange)
        payload = self.timescale_store.data_quality_snapshot(exchange=normalized_exchange)
        return {
            "data": payload,
            "provenance": ProvenanceTimescale(
                provenance_ref=_provenance_ref("ts"),
                template_id="data_quality_snapshot_v1",
                parameters={"exchange": normalized_exchange},
                time_range=None,
                row_count=1 if payload else 0,
                executed_at=datetime.now(UTC),
            ).model_dump(mode="json"),
        }

    def search_research_docs(
        self,
        query: str,
        exchange: str | None = None,
        symbols: list[str] | None = None,
        top_k: int = 8,
    ) -> dict:
        if not query.strip():
            raise ValueError("query must not be empty")
        if self.embedding_client is None:
            raise RuntimeError("embedding client is required for research search")

        bounded_top_k = min(top_k, self.settings.chat_max_research_top_k)
        query_vector = self.embedding_client.embed_texts([query])[0]
        query_filter = _build_qdrant_filter(exchange=exchange, symbols=symbols)
        hits = self.vector_store.search(
            query_vector=query_vector,
            limit=bounded_top_k,
            query_filter=query_filter,
        )

        rows = [_serialize_scored_point(hit) for hit in hits]
        provenance = [
            ProvenanceQdrant(
                provenance_ref=_provenance_ref("qd"),
                collection=self.vector_store.collection,
                doc_id=row["id"],
                chunk_id=row["id"],
                score=row["score"],
                as_of_date=_parse_as_of_date(row.get("published_at")),
                title=row.get("title"),
            ).model_dump(mode="json")
            for row in rows
        ]
        return {"data": rows, "provenance": provenance}

    def _normalize_symbols(self, symbols: list[str]) -> list[str]:
        max_symbols = self.settings.chat_max_symbols_per_request
        unique_symbols: list[str] = []
        seen: set[str] = set()
        for symbol in symbols:
            normalized = symbol.upper().strip()
            if not normalized or normalized in seen:
                continue
            unique_symbols.append(normalized)
            seen.add(normalized)
            if len(unique_symbols) >= max_symbols:
                break
        return unique_symbols

    def _bound_time_range(self, start_time: datetime, end_time: datetime) -> tuple[datetime, datetime]:
        if start_time > end_time:
            raise ValueError("start_time must be less than or equal to end_time")
        max_hours = self.settings.chat_max_lookback_hours
        bounded_start = _as_utc(start_time)
        bounded_end = _as_utc(end_time)
        max_span = max_hours * 3600
        if (bounded_end - bounded_start).total_seconds() > max_span:
            bounded_start = bounded_end - timedelta(hours=max_hours)
        return bounded_start, bounded_end


def _normalize_single_exchange(exchange: str) -> str:
    normalized = exchange.upper().strip()
    if normalized not in {"NSE", "TSX"}:
        raise ValueError("exchange must be NSE or TSX")
    return normalized


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _provenance_ref(prefix: str) -> str:
    return f"prov_{prefix}_{uuid4().hex[:10]}"


def _build_qdrant_filter(
    exchange: str | None,
    symbols: list[str] | None,
) -> models.Filter | None:
    conditions: list[models.FieldCondition] = []
    if exchange:
        normalized_exchange = exchange.upper().strip()
        if normalized_exchange in {"NSE", "TSX"}:
            conditions.append(
                models.FieldCondition(
                    key="exchange",
                    match=models.MatchValue(value=normalized_exchange),
                )
            )
    if symbols:
        normalized_symbols = [symbol.upper().strip() for symbol in symbols if symbol.strip()]
        if normalized_symbols:
            conditions.append(
                models.FieldCondition(
                    key="symbol",
                    match=models.MatchAny(any=normalized_symbols),
                )
            )
    if not conditions:
        return None
    return models.Filter(must=conditions)


def _serialize_scored_point(point: models.ScoredPoint) -> dict:
    payload = dict(point.payload or {})
    payload["id"] = str(point.id)
    payload["score"] = float(point.score or 0.0)
    return payload


def _parse_as_of_date(value: object) -> date | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        return None
