from sqlalchemy import JSON, Column, DateTime, String, Table, Text, UniqueConstraint

from trade_research.storage.timescale import metadata

workflow_requests_table = Table(
    "workflow_requests",
    metadata,
    Column("workflow_id", String, primary_key=True),
    Column("workflow_type", String, nullable=False),
    Column("idempotency_key", String, nullable=False),
    Column("request_hash", String, nullable=False),
    Column("request_payload", JSON, nullable=False),
    Column("status", String, nullable=False),
    Column("requested_by", String, nullable=False),
    Column("dagster_run_id", String),
    Column("result_run_id", String),
    Column("error_message", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("started_at", DateTime(timezone=True)),
    Column("completed_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("idempotency_key", name="uq_workflow_requests_idempotency_key"),
)
