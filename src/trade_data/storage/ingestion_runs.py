"""Persistence of ingestion-run lifecycle state.

The table is supplied by the current Timescale schema module so this focused
repository can be extracted without changing migrations or public store APIs.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Table, select
from sqlalchemy.engine import Engine


class IngestionRunRepository:
    def __init__(self, engine: Engine, table: Table) -> None:
        self._engine = engine
        self._table = table

    def start(
        self,
        job_name: str,
        exchange: str,
        source: str,
        items_requested: int,
        run_metadata: Mapping[str, Any] | None = None,
    ) -> UUID:
        run_id = uuid4()
        with self._engine.begin() as connection:
            connection.execute(
                self._table.insert().values(
                    run_id=str(run_id),
                    job_name=job_name,
                    status="running",
                    exchange=exchange,
                    source=source,
                    started_at=datetime.now(UTC),
                    items_requested=items_requested,
                    items_processed=0,
                    items_succeeded=0,
                    items_failed=0,
                    run_metadata=dict(run_metadata or {}),
                )
            )
        return run_id

    def finish(
        self,
        run_id: UUID,
        status: str,
        items_processed: int,
        items_succeeded: int,
        items_failed: int,
        error_message: str | None = None,
        run_metadata_patch: Mapping[str, Any] | None = None,
    ) -> None:
        values: dict[str, Any] = {
            "status": status,
            "finished_at": datetime.now(UTC),
            "items_processed": items_processed,
            "items_succeeded": items_succeeded,
            "items_failed": items_failed,
            "error_message": error_message,
        }
        with self._engine.begin() as connection:
            if run_metadata_patch:
                existing_metadata = connection.execute(
                    select(self._table.c.run_metadata).where(self._table.c.run_id == str(run_id))
                ).scalar_one_or_none()
                merged_metadata = (
                    dict(existing_metadata) if isinstance(existing_metadata, Mapping) else {}
                )
                merged_metadata.update(dict(run_metadata_patch))
                values["run_metadata"] = merged_metadata
            connection.execute(
                self._table.update().where(self._table.c.run_id == str(run_id)).values(**values)
            )
