from trade_research.research.documents import build_document_id, chunk_text


def test_document_id_is_stable() -> None:
    assert build_document_id("A", "B") == build_document_id("A", "B")
    assert build_document_id("A", "B") != build_document_id("A", "C")


def test_chunk_text_overlaps() -> None:
    text = " ".join(str(i) for i in range(200))

    chunks = chunk_text(text, max_chars=100, overlap=10)

    assert len(chunks) > 1
    assert all(chunk for chunk in chunks)
