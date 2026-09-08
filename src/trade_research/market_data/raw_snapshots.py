from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Protocol

from trade_research.market_data.contracts import ProviderRequest
from trade_research.storage.object_store import ArtifactNamespace, ObjectArtifactStore

_SAFE_KEY_PART = re.compile(r"[^A-Za-z0-9._-]+")


class ArtifactRegistrar(Protocol):
    def register(
        self,
        *,
        artifact_type: str,
        storage_uri: str,
        sha256: str,
        size_bytes: int,
        media_type: str,
        object_version_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class StoredRawSnapshot:
    artifact_manifest_id: str | None
    storage_uri: str
    sha256: str
    size_bytes: int
    object_version_id: str | None


class RawSnapshotWriter:
    """Write immutable, content-addressed provider evidence to the raw bucket."""

    def __init__(
        self,
        store: ObjectArtifactStore,
        *,
        registrar: ArtifactRegistrar | None = None,
    ) -> None:
        self._store = store
        self._registrar = registrar

    def write(self, request: ProviderRequest, payload: Any) -> StoredRawSnapshot:
        content = serialize_raw_snapshot(request, payload)
        digest = hashlib.sha256(content).hexdigest()
        retrieved = request.retrieved_at
        key = "/".join(
            (
                _key_part(request.provider.lower()),
                _key_part(request.exchange.lower()),
                request.interval.value,
                f"{retrieved:%Y/%m/%d}",
                f"{_key_part(request.request_id)}-{digest}.json",
            )
        )
        artifact = self._store.put_bytes(
            ArtifactNamespace.RAW,
            key,
            content,
            media_type="application/json",
            metadata={
                "provider": request.provider,
                "exchange": request.exchange,
                "interval": request.interval.value,
                "request_id": request.request_id,
                "adapter_version": request.adapter_version,
            },
        )
        manifest_id = None
        if self._registrar is not None:
            manifest = self._registrar.register(
                artifact_type="market_data_raw_response",
                storage_uri=artifact.storage_uri,
                sha256=artifact.sha256,
                size_bytes=artifact.size_bytes,
                media_type=artifact.media_type,
                object_version_id=artifact.version_id,
                metadata={
                    "provider": request.provider,
                    "exchange": request.exchange,
                    "interval": request.interval.value,
                    "request_id": request.request_id,
                    "adapter_version": request.adapter_version,
                    "provider_symbols": list(request.provider_symbols),
                    "retrieved_at": request.retrieved_at.isoformat(),
                },
            )
            manifest_id = str(manifest["artifact_manifest_id"])
        return StoredRawSnapshot(
            artifact_manifest_id=manifest_id,
            storage_uri=artifact.storage_uri,
            sha256=artifact.sha256,
            size_bytes=artifact.size_bytes,
            object_version_id=artifact.version_id,
        )


def serialize_raw_snapshot(request: ProviderRequest, payload: Any) -> bytes:
    envelope = {
        "schema_version": "market-data-raw-v1",
        "request": {
            "request_id": request.request_id,
            "provider": request.provider,
            "exchange": request.exchange,
            "interval": request.interval.value,
            "window_start": request.window_start.isoformat(),
            "window_end": request.window_end.isoformat(),
            "provider_symbols": list(request.provider_symbols),
            "retrieved_at": request.retrieved_at.isoformat(),
            "adapter_version": request.adapter_version,
            "parameters": request.parameters,
        },
        "payload": _json_payload(payload),
    }
    return json.dumps(
        envelope,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")


def _json_payload(payload: Any) -> Any:
    to_dict = getattr(payload, "to_dict", None)
    if callable(to_dict):
        return to_dict(orient="records")
    return payload


def _key_part(value: str) -> str:
    normalized = _SAFE_KEY_PART.sub("-", value.strip()).strip("-.")
    if not normalized:
        raise ValueError("raw snapshot key component is empty")
    return normalized
