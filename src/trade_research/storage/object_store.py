from __future__ import annotations

import hashlib
import io
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Any, BinaryIO, Protocol

from trade_research.config import Settings

_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")


class ArtifactNamespace(StrEnum):
    RAW = "raw"
    DATASETS = "datasets"
    MODELS = "models"
    EXPERIMENTS = "experiments"
    EXPORTS = "exports"


class ObjectStoreReadOnlyError(PermissionError):
    pass


class ObjectStoreIntegrityError(RuntimeError):
    pass


class ObjectClient(Protocol):
    def head_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def put_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class StoredArtifact:
    namespace: ArtifactNamespace
    bucket: str
    key: str
    storage_uri: str
    sha256: str
    size_bytes: int
    media_type: str
    version_id: str | None


def create_object_store_client(settings: Settings) -> ObjectClient:
    if not settings.object_store_enabled:
        raise RuntimeError("object storage is disabled")
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=settings.object_store_endpoint_url,
        region_name=settings.object_store_region,
        aws_access_key_id=settings.object_store_access_key_id,
        aws_secret_access_key=settings.object_store_secret_access_key,
    )


class ObjectArtifactStore:
    """Immutable, digest-verified research artifact storage.

    Deletion is deliberately absent. Lifecycle deletion remains disabled until
    an isolated restore drill has passed for every research bucket.
    """

    def __init__(
        self,
        client: ObjectClient,
        *,
        buckets: Mapping[ArtifactNamespace, str],
        write_enabled: bool = False,
        server_side_encryption: str = "AES256",
    ) -> None:
        missing = set(ArtifactNamespace) - set(buckets)
        if missing:
            names = ", ".join(sorted(namespace.value for namespace in missing))
            raise ValueError(f"missing object-store buckets: {names}")
        self._client = client
        self._buckets = dict(buckets)
        self._write_enabled = write_enabled
        self._server_side_encryption = server_side_encryption

    @classmethod
    def from_settings(cls, settings: Settings) -> ObjectArtifactStore:
        return cls(
            create_object_store_client(settings),
            buckets={
                ArtifactNamespace.RAW: settings.object_store_raw_bucket,
                ArtifactNamespace.DATASETS: settings.object_store_datasets_bucket,
                ArtifactNamespace.MODELS: settings.object_store_models_bucket,
                ArtifactNamespace.EXPERIMENTS: settings.object_store_experiments_bucket,
                ArtifactNamespace.EXPORTS: settings.object_store_exports_bucket,
            },
            write_enabled=settings.object_store_write_enabled,
            server_side_encryption=settings.object_store_server_side_encryption,
        )

    def put_bytes(
        self,
        namespace: ArtifactNamespace,
        key: str,
        content: bytes,
        *,
        media_type: str = "application/octet-stream",
        metadata: Mapping[str, str] | None = None,
    ) -> StoredArtifact:
        if not self._write_enabled:
            raise ObjectStoreReadOnlyError("object artifact store is read-only")
        normalized_key = self._normalize_key(key)
        digest = hashlib.sha256(content).hexdigest()
        bucket = self._buckets[namespace]
        existing = self._head_or_none(bucket, normalized_key)
        if existing is not None:
            return self._validate_existing(
                namespace,
                bucket,
                normalized_key,
                digest,
                len(content),
                media_type,
                existing,
            )
        object_metadata = {str(k): str(v) for k, v in (metadata or {}).items()}
        object_metadata["sha256"] = digest
        response = self._client.put_object(
            Bucket=bucket,
            Key=normalized_key,
            Body=io.BytesIO(content),
            ContentLength=len(content),
            ContentType=media_type,
            Metadata=object_metadata,
            ServerSideEncryption=self._server_side_encryption,
        )
        stored = self._head_or_none(bucket, normalized_key)
        if stored is None:
            raise ObjectStoreIntegrityError("object was not readable after upload")
        result = self._validate_existing(
            namespace,
            bucket,
            normalized_key,
            digest,
            len(content),
            media_type,
            stored,
        )
        return replace(
            result,
            version_id=response.get("VersionId") or result.version_id,
        )

    def get_bytes(self, artifact: StoredArtifact) -> bytes:
        request: dict[str, Any] = {"Bucket": artifact.bucket, "Key": artifact.key}
        if artifact.version_id:
            request["VersionId"] = artifact.version_id
        response = self._client.get_object(**request)
        body: BinaryIO = response["Body"]
        content = body.read()
        actual = hashlib.sha256(content).hexdigest()
        if actual != artifact.sha256 or len(content) != artifact.size_bytes:
            raise ObjectStoreIntegrityError(
                f"artifact digest/size mismatch for {artifact.storage_uri}"
            )
        return content

    def _head_or_none(self, bucket: str, key: str) -> Mapping[str, Any] | None:
        try:
            return self._client.head_object(Bucket=bucket, Key=key)
        except Exception as error:
            response = getattr(error, "response", {})
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            code = response.get("Error", {}).get("Code")
            if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

    @staticmethod
    def _normalize_key(key: str) -> str:
        normalized = key.strip().strip("/")
        if (
            not normalized
            or not _SAFE_SEGMENT.fullmatch(normalized)
            or any(part in {"", ".", ".."} for part in normalized.split("/"))
        ):
            raise ValueError(f"unsafe object key: {key!r}")
        return normalized

    @staticmethod
    def _validate_existing(
        namespace: ArtifactNamespace,
        bucket: str,
        key: str,
        digest: str,
        size_bytes: int,
        media_type: str,
        head: Mapping[str, Any],
    ) -> StoredArtifact:
        existing_digest = str(head.get("Metadata", {}).get("sha256", ""))
        existing_size = int(head.get("ContentLength", -1))
        if existing_digest != digest or existing_size != size_bytes:
            raise ObjectStoreIntegrityError(
                f"immutable object key already contains different content: s3://{bucket}/{key}"
            )
        return StoredArtifact(
            namespace=namespace,
            bucket=bucket,
            key=key,
            storage_uri=f"s3://{bucket}/{key}",
            sha256=digest,
            size_bytes=size_bytes,
            media_type=str(head.get("ContentType") or media_type),
            version_id=head.get("VersionId"),
        )
