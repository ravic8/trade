from datetime import UTC, datetime

import httpx
import trafilatura

from trade_research.research.documents import build_document_id
from trade_research.schemas import ResearchDocument


def fetch_article_text(
    url: str,
    symbol: str | None = None,
    exchange: str | None = None,
) -> ResearchDocument:
    headers = {"User-Agent": "trade-research/0.1"}
    with httpx.Client(timeout=30, follow_redirects=True, headers=headers) as client:
        response = client.get(url)
        response.raise_for_status()

    extracted = trafilatura.extract(response.text, include_comments=False, include_tables=False)
    text = extracted or response.text
    fetched_at = datetime.now(UTC)
    return ResearchDocument(
        id=build_document_id(url, fetched_at.isoformat()),
        symbol=symbol,
        exchange=exchange,
        source_type="web",
        url=url,
        fetched_at=fetched_at,
        text=text,
        metadata={"content_length": len(response.text)},
    )
