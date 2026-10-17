from __future__ import annotations

import hashlib
import io
from typing import Any

import pytest

from trade_data.storage.object_store import ArtifactNamespace
from trade_data.storage.restore_validation import validate_artifact_buckets


class _Paginator:
    def __init__(self, client: _S3Client) -> None:
        self.client = client

    def paginate(self, *, Bucket: str) -> list[dict[str, Any]]:
        contents = [
            {"Key": key}
            for (bucket, key), _content in self.client.objects.items()
            if bucket == Bucket
        ]
        return [{"Contents": contents}]


class _S3Client:
    def __init__(self) -> None:
        self.versioning = {f"trade-{item.value}": "Enabled" for item in ArtifactNamespace}
        self.objects: dict[tuple[str, str], bytes] = {}

    def get_bucket_versioning(self, *, Bucket: str) -> dict[str, str]:
        return {"Status": self.versioning[Bucket]}

    def get_paginator(self, operation_name: str) -> _Paginator:
        assert operation_name == "list_objects_v2"
        return _Paginator(self)

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        content = self.objects[(Bucket, Key)]
        return {"Metadata": {"sha256": hashlib.sha256(content).hexdigest()}}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}


def _buckets() -> dict[ArtifactNamespace, str]:
    return {item: f"trade-{item.value}" for item in ArtifactNamespace}


def test_validate_artifact_buckets_checks_all_buckets_and_object_digests() -> None:
    client = _S3Client()
    client.objects[("trade-datasets", "canary/data.parquet")] = b"dataset"

    results = validate_artifact_buckets(client, buckets=_buckets())

    assert set(results) == {item.value for item in ArtifactNamespace}
    assert results["datasets"].object_count == 1
    assert results["datasets"].digest_verified_count == 1
    assert results["raw"].object_count == 0


def test_validate_artifact_buckets_rejects_disabled_versioning() -> None:
    client = _S3Client()
    client.versioning["trade-raw"] = "Suspended"

    with pytest.raises(RuntimeError, match="versioning is not enabled: trade-raw"):
        validate_artifact_buckets(client, buckets=_buckets())


def test_validate_artifact_buckets_enforces_object_bound() -> None:
    client = _S3Client()
    client.objects[("trade-raw", "one.json")] = b"one"
    client.objects[("trade-raw", "two.json")] = b"two"

    with pytest.raises(RuntimeError, match="exceeds restore-validation bound"):
        validate_artifact_buckets(
            client,
            buckets=_buckets(),
            maximum_objects_per_bucket=1,
        )


def test_validate_only_configured_daily_bucket():
    results = validate_artifact_buckets(_S3Client(), buckets={ArtifactNamespace.RAW: "trade-raw"})
    assert set(results) == {"raw"}
    with pytest.raises(ValueError, match="At least one artifact bucket"):
        validate_artifact_buckets(_S3Client(), buckets={})
