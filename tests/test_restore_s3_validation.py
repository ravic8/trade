from __future__ import annotations

from typing import Any

import pytest

from trade_research.filings.restore_validation import validate_restored_s3


class _Paginator:
    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self.pages = pages
        self.calls: list[dict[str, str]] = []

    def paginate(self, **kwargs: str) -> list[dict[str, Any]]:
        self.calls.append(kwargs)
        return self.pages


class _S3Client:
    def __init__(
        self,
        *,
        versioning_status: str | None,
        pages: list[dict[str, Any]],
    ) -> None:
        self.versioning_status = versioning_status
        self.paginator = _Paginator(pages)
        self.head_calls: list[dict[str, str]] = []

    def get_bucket_versioning(self, *, Bucket: str) -> dict[str, str]:
        if self.versioning_status is None:
            return {}
        return {"Status": self.versioning_status}

    def get_paginator(self, operation: str) -> _Paginator:
        assert operation == "list_objects_v2"
        return self.paginator

    def head_object(self, **kwargs: str) -> None:
        self.head_calls.append(kwargs)


def test_validate_restored_s3_lists_and_heads_parsed_documents() -> None:
    client = _S3Client(
        versioning_status="Enabled",
        pages=[
            {
                "Contents": [
                    {"Key": "parsed/a/parsed_document.json"},
                    {"Key": "parsed/a/other.json"},
                ]
            },
            {"Contents": [{"Key": "parsed/b/parsed_document.json"}]},
        ],
    )

    result = validate_restored_s3(
        client,
        bucket="lens-filings",
        prefix="/parsed/",
    )

    assert result.versioning_status == "Enabled"
    assert result.object_count == 2
    assert client.paginator.calls == [
        {"Bucket": "lens-filings", "Prefix": "parsed/"}
    ]
    assert client.head_calls == [
        {"Bucket": "lens-filings", "Key": "parsed/a/parsed_document.json"},
        {"Bucket": "lens-filings", "Key": "parsed/b/parsed_document.json"},
    ]


@pytest.mark.parametrize("status", [None, "Suspended"])
def test_validate_restored_s3_rejects_non_enabled_versioning(
    status: str | None,
) -> None:
    client = _S3Client(versioning_status=status, pages=[])

    with pytest.raises(
        RuntimeError,
        match="restored MinIO bucket versioning is not enabled",
    ):
        validate_restored_s3(
            client,
            bucket="lens-filings",
            prefix="parsed",
        )
