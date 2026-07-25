from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from typing import Any

import boto3


@dataclass(frozen=True)
class RestoredS3Validation:
    versioning_status: str
    object_count: int


def validate_restored_s3(
    client: Any,
    *,
    bucket: str,
    prefix: str,
) -> RestoredS3Validation:
    versioning_status = client.get_bucket_versioning(Bucket=bucket).get("Status")
    if versioning_status != "Enabled":
        raise RuntimeError("restored MinIO bucket versioning is not enabled")

    normalized_prefix = prefix.strip("/")
    if normalized_prefix:
        normalized_prefix = f"{normalized_prefix}/"
    object_count = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=normalized_prefix):
        for item in page.get("Contents", []):
            key = str(item["Key"])
            if not key.endswith("/parsed_document.json"):
                continue
            client.head_object(Bucket=bucket, Key=key)
            object_count += 1

    return RestoredS3Validation(
        versioning_status=versioning_status,
        object_count=object_count,
    )


def main() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["FILING_S3_ENDPOINT_URL"],
        region_name=os.environ["FILING_S3_REGION"],
        aws_access_key_id=os.environ["FILING_S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["FILING_S3_SECRET_ACCESS_KEY"],
    )
    result = validate_restored_s3(
        client,
        bucket=os.environ["FILING_S3_BUCKET"],
        prefix=os.environ["FILING_S3_PREFIX"],
    )
    print(json.dumps(asdict(result), separators=(",", ":")))


if __name__ == "__main__":
    main()
