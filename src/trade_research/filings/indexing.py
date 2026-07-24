from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.http import models

from trade_research.config import Settings
from trade_research.filings.models import FilingDocument, ParsedDocument
from trade_research.filings.store import FilingStore, stable_id
from trade_research.filings.telemetry import operation_span
from trade_research.research.embeddings import OpenAIEmbeddingClient


@dataclass(frozen=True)
class FilingChunk:
    chunk_id: str
    text: str
    page: int | None
    section_path: str


class FilingEvidenceIndexer:
    def __init__(
        self,
        *,
        settings: Settings,
        store: FilingStore,
        vector_client: QdrantClient | None = None,
        embedding_client: OpenAIEmbeddingClient | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.vector_client = vector_client or QdrantClient(
            url=settings.qdrant_url,
            api_key=settings.qdrant_api_key,
        )
        self.embedding_client = embedding_client or OpenAIEmbeddingClient(
            api_key=settings.openai_api_key,
            model=settings.openai_embedding_model,
            base_url=settings.openai_base_url,
        )

    def index(
        self,
        *,
        run_id: str,
        document: FilingDocument,
        parsed: ParsedDocument,
    ) -> int:
        chunks = build_filing_chunks(
            parsed,
            chunk_size=self.settings.filing_chunk_size,
            overlap=self.settings.filing_chunk_overlap,
            max_chunks=self.settings.filing_index_max_chunks,
        )
        self.store.upsert_index_run(
            run_id=run_id,
            workspace_id=document.workspace_id,
            company_id=document.company_id,
            filing_id=document.filing_id,
            filing_version=document.version,
            index_version=self.settings.filing_index_version,
            embedding_model=self.settings.openai_embedding_model,
            collection_name=self.settings.filing_qdrant_collection,
            status="running",
            chunk_count=0,
        )
        try:
            self._ensure_collection()
            indexed = 0
            for offset in range(0, len(chunks), self.settings.filing_embedding_batch_size):
                batch = chunks[offset : offset + self.settings.filing_embedding_batch_size]
                with operation_span(
                    self.settings,
                    "filing.embedding.batch",
                    observation_type="embedding",
                    metadata={
                        "run_id": run_id,
                        "filing_id": document.filing_id,
                        "batch_size": len(batch),
                        "embedding_model": self.settings.openai_embedding_model,
                        "index_version": self.settings.filing_index_version,
                    },
                ):
                    vectors = self.embedding_client.embed_texts(
                        [chunk.text for chunk in batch]
                    )
                points = [
                    models.PointStruct(
                        id=chunk.chunk_id,
                        vector=vector,
                        payload={
                            "workspace_id": document.workspace_id,
                            "company_id": document.company_id,
                            "symbol": document.symbol,
                            "exchange": document.exchange,
                            "filing_id": document.filing_id,
                            "filing_version": document.version,
                            "period_end": (
                                document.period_end.isoformat()
                                if document.period_end
                                else None
                            ),
                            "content_type": document.content_type,
                            "categories": document.categories,
                            "source_hash": document.sha256,
                            "chunk_id": chunk.chunk_id,
                            "page": chunk.page,
                            "section_path": chunk.section_path,
                            "index_version": self.settings.filing_index_version,
                            "embedding_model": self.settings.openai_embedding_model,
                            "text": chunk.text,
                        },
                    )
                    for chunk, vector in zip(batch, vectors, strict=True)
                ]
                self.vector_client.upsert(
                    collection_name=self.settings.filing_qdrant_collection,
                    points=points,
                    wait=True,
                )
                indexed += len(points)
            self.store.upsert_index_run(
                run_id=run_id,
                workspace_id=document.workspace_id,
                company_id=document.company_id,
                filing_id=document.filing_id,
                filing_version=document.version,
                index_version=self.settings.filing_index_version,
                embedding_model=self.settings.openai_embedding_model,
                collection_name=self.settings.filing_qdrant_collection,
                status="completed",
                chunk_count=indexed,
            )
            return indexed
        except Exception as exc:
            self.store.upsert_index_run(
                run_id=run_id,
                workspace_id=document.workspace_id,
                company_id=document.company_id,
                filing_id=document.filing_id,
                filing_version=document.version,
                index_version=self.settings.filing_index_version,
                embedding_model=self.settings.openai_embedding_model,
                collection_name=self.settings.filing_qdrant_collection,
                status="failed",
                chunk_count=0,
                error_message=str(exc)[:2_000],
            )
            raise

    def search(
        self,
        *,
        query: str,
        workspace_id: str,
        company_id: str,
        limit: int = 8,
        filing_id: str | None = None,
    ) -> list[dict[str, Any]]:
        query_vector = self.embedding_client.embed_texts([query])[0]
        must = [
            models.FieldCondition(
                key="workspace_id", match=models.MatchValue(value=workspace_id)
            ),
            models.FieldCondition(
                key="company_id", match=models.MatchValue(value=company_id)
            ),
            models.FieldCondition(
                key="index_version",
                match=models.MatchValue(value=self.settings.filing_index_version),
            ),
        ]
        if filing_id:
            must.append(
                models.FieldCondition(
                    key="filing_id", match=models.MatchValue(value=filing_id)
                )
            )
        response = self.vector_client.query_points(
            collection_name=self.settings.filing_qdrant_collection,
            query=query_vector,
            query_filter=models.Filter(must=must),
            limit=min(max(limit, 1), 50),
            with_payload=True,
            with_vectors=False,
        )
        return [
            {
                "point_id": str(point.id),
                "score": point.score,
                "payload": point.payload or {},
            }
            for point in response.points
        ]

    def _ensure_collection(self) -> None:
        names = {
            collection.name
            for collection in self.vector_client.get_collections().collections
        }
        if self.settings.filing_qdrant_collection not in names:
            self.vector_client.create_collection(
                collection_name=self.settings.filing_qdrant_collection,
                vectors_config=models.VectorParams(
                    size=self.settings.filing_embedding_vector_size,
                    distance=models.Distance.COSINE,
                    on_disk=True,
                ),
                on_disk_payload=True,
            )
        for field in (
            "workspace_id",
            "company_id",
            "filing_id",
            "filing_version",
            "period_end",
            "index_version",
            "categories",
        ):
            schema = (
                models.PayloadSchemaType.INTEGER
                if field == "filing_version"
                else models.PayloadSchemaType.KEYWORD
            )
            self.vector_client.create_payload_index(
                collection_name=self.settings.filing_qdrant_collection,
                field_name=field,
                field_schema=schema,
                wait=True,
            )


def build_filing_chunks(
    parsed: ParsedDocument,
    *,
    chunk_size: int,
    overlap: int,
    max_chunks: int,
) -> list[FilingChunk]:
    chunks: list[FilingChunk] = []
    if parsed.pages:
        for page in parsed.pages:
            for index, text in enumerate(_chunk_text(page.text, chunk_size, overlap)):
                chunks.append(
                    FilingChunk(
                        chunk_id=stable_id(
                            "filing-chunk",
                            parsed.filing_id,
                            parsed.parser_version,
                            page.page,
                            index,
                            text,
                        ),
                        text=text,
                        page=page.page,
                        section_path=f"pdf/page/{page.page}",
                    )
                )
                if len(chunks) >= max_chunks:
                    return chunks
    elif parsed.xbrl_facts:
        lines = [
            f"{fact.concept} [{fact.context_ref}] = {fact.value_text}"
            for fact in parsed.xbrl_facts
        ]
        for index, text in enumerate(
            _chunk_text("\n".join(lines), chunk_size, overlap)
        ):
            chunks.append(
                FilingChunk(
                    chunk_id=stable_id(
                        "filing-chunk",
                        parsed.filing_id,
                        parsed.parser_version,
                        index,
                        text,
                    ),
                    text=text,
                    page=None,
                    section_path="xbrl/facts",
                )
            )
            if len(chunks) >= max_chunks:
                break
    return chunks


def _chunk_text(text: str, chunk_size: int, overlap: int) -> list[str]:
    normalized = " ".join(text.split())
    if not normalized:
        return []
    chunks: list[str] = []
    start = 0
    while start < len(normalized):
        end = min(start + chunk_size, len(normalized))
        if end < len(normalized):
            boundary = normalized.rfind(" ", start + chunk_size // 2, end)
            if boundary > start:
                end = boundary
        chunks.append(normalized[start:end].strip())
        if end >= len(normalized):
            break
        start = max(end - overlap, start + 1)
    return chunks
