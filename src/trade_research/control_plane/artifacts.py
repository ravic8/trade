from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import Engine, select
from sqlalchemy.dialects.postgresql import insert

from trade_research.control_plane.tables import artifact_manifests_table

_SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")


class ArtifactManifestConflictError(RuntimeError):
    """Raised when a storage URI is reused for different content."""


class ArtifactManifestRepository:
    """Append-only PostgreSQL registry for immutable object-store artifacts."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

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
    ) -> dict[str, Any]:
        if not _SHA256.fullmatch(sha256):
            raise ValueError("sha256 must be a 64-character hexadecimal digest")
        values = {
            "artifact_manifest_id": str(uuid4()),
            "artifact_type": artifact_type,
            "storage_uri": storage_uri,
            "sha256": sha256.lower(),
            "size_bytes": size_bytes,
            "media_type": media_type,
            "object_version_id": object_version_id,
            "manifest_metadata": metadata or {},
            "created_at": datetime.now(UTC),
        }
        statement = (
            insert(artifact_manifests_table)
            .values(**values)
            .on_conflict_do_nothing(index_elements=["storage_uri"])
        )
        with self._engine.begin() as connection:
            connection.execute(statement)
            row = (
                connection.execute(
                    select(artifact_manifests_table).where(
                        artifact_manifests_table.c.storage_uri == storage_uri
                    )
                )
                .mappings()
                .one()
            )
        if row["sha256"] != values["sha256"] or row["size_bytes"] != size_bytes:
            raise ArtifactManifestConflictError(
                f"artifact URI already registered with different content: {storage_uri}"
            )
        return dict(row)
