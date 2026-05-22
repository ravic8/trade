from collections.abc import Sequence

from qdrant_client import QdrantClient
from qdrant_client.http import models

from trade_research.schemas import ResearchDocument


class QdrantVectorStore:
    def __init__(
        self,
        url: str,
        collection: str,
        api_key: str | None = None,
        vector_size: int = 1536,
    ) -> None:
        self.collection = collection
        self.vector_size = vector_size
        self.client = QdrantClient(url=url, api_key=api_key)

    def ensure_collection(self) -> None:
        existing = {item.name for item in self.client.get_collections().collections}
        if self.collection in existing:
            return

        self.client.create_collection(
            collection_name=self.collection,
            vectors_config=models.VectorParams(
                size=self.vector_size,
                distance=models.Distance.COSINE,
            ),
        )
        for field in ["symbol", "exchange", "source_type", "published_at"]:
            self.client.create_payload_index(
                collection_name=self.collection,
                field_name=field,
                field_schema=models.PayloadSchemaType.KEYWORD
                if field != "published_at"
                else models.PayloadSchemaType.DATETIME,
            )

    def upsert_documents(
        self,
        documents: Sequence[ResearchDocument],
        embeddings: Sequence[Sequence[float]],
    ) -> None:
        if len(documents) != len(embeddings):
            raise ValueError("documents and embeddings must have the same length")

        self.ensure_collection()
        points = []
        for document, embedding in zip(documents, embeddings, strict=True):
            payload = document.model_dump(mode="json")
            payload.pop("text", None)
            payload["text_preview"] = document.text[:1000]
            points.append(
                models.PointStruct(
                    id=document.id,
                    vector=list(embedding),
                    payload=payload,
                )
            )
        self.client.upsert(collection_name=self.collection, points=points)

    def search(
        self,
        query_vector: Sequence[float],
        limit: int = 10,
        query_filter: models.Filter | None = None,
    ) -> list[models.ScoredPoint]:
        return self.client.search(
            collection_name=self.collection,
            query_vector=list(query_vector),
            query_filter=query_filter,
            limit=limit,
        )
