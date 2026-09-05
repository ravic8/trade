from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from typing import Any, BinaryIO, Protocol

import boto3

from trade_research.storage.object_store import ArtifactNamespace


class S3RestoreClient(Protocol):
    def get_bucket_versioning(self, *, Bucket: str) -> dict[str, Any]: ...

    def get_paginator(self, operation_name: str) -> Any: ...

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]: ...

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]: ...


@dataclass(frozen=True)
class RestoredResearchBucket:
    bucket: str
    versioning_status: str
    object_count: int
    digest_verified_count: int


def validate_research_buckets(
    client: S3RestoreClient,
    *,
    buckets: dict[ArtifactNamespace, str],
    maximum_objects_per_bucket: int = 10_000,
) -> dict[str, RestoredResearchBucket]:
    """Verify every configured research bucket and every bounded object digest."""

    if maximum_objects_per_bucket < 1:
        raise ValueError("maximum_objects_per_bucket must be positive")
    results: dict[str, RestoredResearchBucket] = {}
    for namespace in ArtifactNamespace:
        bucket = buckets[namespace]
        versioning = client.get_bucket_versioning(Bucket=bucket).get("Status")
        if versioning != "Enabled":
            raise RuntimeError(f"research bucket versioning is not enabled: {bucket}")

        object_count = 0
        digest_verified_count = 0
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for item in page.get("Contents", []):
                object_count += 1
                if object_count > maximum_objects_per_bucket:
                    raise RuntimeError(
                        f"research bucket exceeds restore-validation bound: {bucket}"
                    )
                key = str(item["Key"])
                head = client.head_object(Bucket=bucket, Key=key)
                expected_digest = str(head.get("Metadata", {}).get("sha256", ""))
                if not expected_digest:
                    raise RuntimeError(f"research object has no SHA-256 metadata: {bucket}/{key}")
                body: BinaryIO = client.get_object(Bucket=bucket, Key=key)["Body"]
                digest = hashlib.sha256()
                while chunk := body.read(1024 * 1024):
                    digest.update(chunk)
                if digest.hexdigest() != expected_digest:
                    raise RuntimeError(f"research object digest mismatch: {bucket}/{key}")
                digest_verified_count += 1

        results[namespace.value] = RestoredResearchBucket(
            bucket=bucket,
            versioning_status=versioning,
            object_count=object_count,
            digest_verified_count=digest_verified_count,
        )
    return results


def main() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["OBJECT_STORE_ENDPOINT_URL"],
        region_name=os.environ["OBJECT_STORE_REGION"],
        aws_access_key_id=os.environ["OBJECT_STORE_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["OBJECT_STORE_SECRET_ACCESS_KEY"],
    )
    results = validate_research_buckets(
        client,
        buckets={
            ArtifactNamespace.RAW: os.environ["OBJECT_STORE_RAW_BUCKET"],
            ArtifactNamespace.DATASETS: os.environ["OBJECT_STORE_DATASETS_BUCKET"],
            ArtifactNamespace.MODELS: os.environ["OBJECT_STORE_MODELS_BUCKET"],
            ArtifactNamespace.EXPERIMENTS: os.environ["OBJECT_STORE_EXPERIMENTS_BUCKET"],
            ArtifactNamespace.EXPORTS: os.environ["OBJECT_STORE_EXPORTS_BUCKET"],
        },
        maximum_objects_per_bucket=int(
            os.environ.get("OBJECT_STORE_RESTORE_MAX_OBJECTS_PER_BUCKET", "10000")
        ),
    )
    print(
        json.dumps(
            {name: asdict(result) for name, result in results.items()},
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
