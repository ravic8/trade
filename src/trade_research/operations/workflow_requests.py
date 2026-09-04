from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import Engine, create_engine, insert, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from trade_research.operations.tables import workflow_requests_table


@dataclass(frozen=True)
class WorkflowRequest:
    workflow_id: str
    workflow_type: str
    idempotency_key: str
    request_payload: Mapping[str, Any]
    status: str
    requested_by: str
    dagster_run_id: str | None
    result_run_id: str | None
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    updated_at: datetime


class WorkflowRequestStore:
    def __init__(self, database_url: str | None = None, *, engine: Engine | None = None) -> None:
        if engine is None and database_url is None:
            raise ValueError("database_url or engine is required")
        self.engine = engine or create_engine(str(database_url), pool_pre_ping=True)

    def submit(
        self,
        *,
        workflow_type: str,
        request_payload: Mapping[str, Any],
        idempotency_key: str,
        requested_by: str,
        now: datetime | None = None,
    ) -> tuple[WorkflowRequest, bool]:
        normalized_payload = json.loads(
            json.dumps(request_payload, sort_keys=True, separators=(",", ":"), default=str)
        )
        request_hash = hashlib.sha256(
            json.dumps(normalized_payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        workflow_id = str(uuid5(NAMESPACE_URL, f"workflow:{idempotency_key}"))
        observed_at = now or datetime.now(UTC)
        values = {
            "workflow_id": workflow_id,
            "workflow_type": workflow_type,
            "idempotency_key": idempotency_key,
            "request_hash": request_hash,
            "request_payload": normalized_payload,
            "status": "queued",
            "requested_by": requested_by,
            "created_at": observed_at,
            "updated_at": observed_at,
        }
        statement: Any
        if self.engine.dialect.name == "postgresql":
            statement = postgresql_insert(workflow_requests_table).values(values)
            statement = statement.on_conflict_do_nothing(
                index_elements=["idempotency_key"]
            )
        elif self.engine.dialect.name == "sqlite":
            statement = sqlite_insert(workflow_requests_table).values(values)
            statement = statement.on_conflict_do_nothing(
                index_elements=["idempotency_key"]
            )
        else:
            statement = insert(workflow_requests_table).values(values)
        with self.engine.begin() as connection:
            result = connection.execute(statement)
            row = connection.execute(
                select(workflow_requests_table).where(
                    workflow_requests_table.c.idempotency_key == idempotency_key
                )
            ).mappings().one()
        if str(row["request_hash"]) != request_hash or str(row["workflow_type"]) != workflow_type:
            raise ValueError("Idempotency key is already bound to a different workflow request")
        return _workflow_request(dict(row)), bool(result.rowcount)

    def get(self, workflow_id: str) -> WorkflowRequest | None:
        with self.engine.begin() as connection:
            row = connection.execute(
                select(workflow_requests_table).where(
                    workflow_requests_table.c.workflow_id == workflow_id
                )
            ).mappings().first()
        return _workflow_request(dict(row)) if row is not None else None

    def queued(self, workflow_type: str, *, limit: int = 25) -> list[WorkflowRequest]:
        query = (
            select(workflow_requests_table)
            .where(workflow_requests_table.c.workflow_type == workflow_type)
            .where(workflow_requests_table.c.status == "queued")
            .order_by(workflow_requests_table.c.created_at)
            .limit(max(1, min(limit, 100)))
        )
        with self.engine.begin() as connection:
            return [
                _workflow_request(dict(row))
                for row in connection.execute(query).mappings()
            ]

    def mark_running(self, workflow_id: str, dagster_run_id: str) -> None:
        now = datetime.now(UTC)
        self._update(
            workflow_id,
            status="running",
            dagster_run_id=dagster_run_id,
            started_at=now,
            updated_at=now,
        )

    def mark_completed(self, workflow_id: str, *, result_run_id: str) -> None:
        now = datetime.now(UTC)
        self._update(
            workflow_id,
            status="succeeded",
            result_run_id=result_run_id,
            completed_at=now,
            updated_at=now,
        )

    def mark_failed(self, workflow_id: str, *, error_message: str) -> None:
        now = datetime.now(UTC)
        self._update(
            workflow_id,
            status="failed",
            error_message=error_message[:4000],
            completed_at=now,
            updated_at=now,
        )

    def _update(self, workflow_id: str, **values: Any) -> None:
        statement = (
            update(workflow_requests_table)
            .where(workflow_requests_table.c.workflow_id == workflow_id)
            .values(**values)
        )
        with self.engine.begin() as connection:
            result = connection.execute(statement)
        if result.rowcount != 1:
            raise ValueError(f"Workflow request not found: {workflow_id}")


def _workflow_request(row: Mapping[str, Any]) -> WorkflowRequest:
    return WorkflowRequest(
        workflow_id=str(row["workflow_id"]),
        workflow_type=str(row["workflow_type"]),
        idempotency_key=str(row["idempotency_key"]),
        request_payload=dict(row["request_payload"]),
        status=str(row["status"]),
        requested_by=str(row["requested_by"]),
        dagster_run_id=str(row["dagster_run_id"]) if row.get("dagster_run_id") else None,
        result_run_id=str(row["result_run_id"]) if row.get("result_run_id") else None,
        error_message=str(row["error_message"]) if row.get("error_message") else None,
        created_at=row["created_at"],
        started_at=row.get("started_at"),
        completed_at=row.get("completed_at"),
        updated_at=row["updated_at"],
    )
