import hashlib


def build_document_id(*parts: str) -> str:
    joined = "|".join(part for part in parts if part)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def chunk_text(text: str, max_chars: int = 3_000, overlap: int = 300) -> list[str]:
    normalized = " ".join(text.split())
    if not normalized:
        return []
    if max_chars <= overlap:
        raise ValueError("max_chars must be greater than overlap")

    chunks: list[str] = []
    start = 0
    while start < len(normalized):
        end = min(start + max_chars, len(normalized))
        chunks.append(normalized[start:end])
        if end == len(normalized):
            break
        start = end - overlap
    return chunks
