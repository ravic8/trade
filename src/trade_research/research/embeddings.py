from collections.abc import Sequence

from openai import OpenAI


class OpenAIEmbeddingClient:
    def __init__(
        self,
        api_key: str | None,
        model: str = "text-embedding-3-small",
        base_url: str | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("OPENAI_API_KEY is required to create embeddings")
        self.client = OpenAI(api_key=api_key, base_url=base_url)
        self.model = model

    def embed_texts(self, texts: Sequence[str]) -> list[list[float]]:
        if not texts:
            return []
        response = self.client.embeddings.create(
            model=self.model,
            input=list(texts),
            encoding_format="float",
        )
        return [item.embedding for item in response.data]
