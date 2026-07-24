"""Add Lens NSE filing intelligence M1 business tables.

Revision ID: 20260724_0010
Revises: 20260720_0009
Create Date: 2026-07-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260724_0010"
down_revision: str | None = "20260720_0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "filing_documents",
        sa.Column("filing_id", sa.String(), primary_key=True),
        sa.Column("workspace_id", sa.String(), nullable=False),
        sa.Column("company_id", sa.String(), nullable=False),
        sa.Column("symbol", sa.String(), nullable=False),
        sa.Column("exchange", sa.String(), nullable=False),
        sa.Column("company_name", sa.String(), nullable=False),
        sa.Column("categories", sa.JSON(), nullable=False),
        sa.Column("title", sa.Text()),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("source_apis", sa.JSON(), nullable=False),
        sa.Column("filing_date", sa.DateTime(timezone=True)),
        sa.Column("period_end", sa.Date()),
        sa.Column("consolidation_scope", sa.String(), nullable=False),
        sa.Column("audited", sa.Boolean()),
        sa.Column("submission_type", sa.String()),
        sa.Column("relative_path", sa.Text(), nullable=False),
        sa.Column("object_uri", sa.Text(), nullable=False),
        sa.Column("filename", sa.String(), nullable=False),
        sa.Column("byte_size", sa.BigInteger(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("content_type", sa.String(), nullable=False),
        sa.Column("document_key", sa.String(), nullable=False),
        sa.Column("version", sa.BigInteger(), nullable=False),
        sa.Column("supersedes_filing_id", sa.String()),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("parse_quality", sa.Float()),
        sa.Column("source_metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "workspace_id",
            "company_id",
            "sha256",
            name="uq_filing_documents_workspace_company_hash",
        ),
        sa.UniqueConstraint(
            "workspace_id",
            "document_key",
            "version",
            name="uq_filing_documents_workspace_key_version",
        ),
    )
    op.create_index(
        "idx_filing_documents_company_period",
        "filing_documents",
        ["workspace_id", "company_id", "period_end"],
    )
    op.create_index(
        "idx_filing_documents_key_current",
        "filing_documents",
        ["workspace_id", "document_key", "is_current"],
    )

    op.create_table(
        "filing_runs",
        sa.Column("run_id", sa.String(), primary_key=True),
        sa.Column("thread_id", sa.String(), nullable=False),
        sa.Column("workspace_id", sa.String(), nullable=False),
        sa.Column("company_id", sa.String(), nullable=False),
        sa.Column("filing_id", sa.String(), nullable=False),
        sa.Column("workflow_type", sa.String(), nullable=False),
        sa.Column("idempotency_key", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("current_node", sa.String()),
        sa.Column("progress", sa.Float(), nullable=False),
        sa.Column("attempt_count", sa.BigInteger(), nullable=False),
        sa.Column("max_attempts", sa.BigInteger(), nullable=False),
        sa.Column("cancel_requested", sa.Boolean(), nullable=False),
        sa.Column("input_payload", sa.JSON(), nullable=False),
        sa.Column("output_payload", sa.JSON(), nullable=False),
        sa.Column("error_code", sa.String()),
        sa.Column("error_message", sa.Text()),
        sa.Column("worker_id", sa.String()),
        sa.Column("trace_id", sa.String()),
        sa.Column("queued_at", sa.DateTime(timezone=True)),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True)),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("waiting_review_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "workspace_id",
            "idempotency_key",
            name="uq_filing_runs_workspace_idempotency",
        ),
    )
    op.create_index(
        "idx_filing_runs_workspace_status",
        "filing_runs",
        ["workspace_id", "status", "updated_at"],
    )
    op.create_index(
        "idx_filing_runs_lease",
        "filing_runs",
        ["status", "lease_expires_at"],
    )

    op.create_table(
        "filing_evidence",
        sa.Column("evidence_id", sa.String(), primary_key=True),
        sa.Column("workspace_id", sa.String(), nullable=False),
        sa.Column("company_id", sa.String(), nullable=False),
        sa.Column("filing_id", sa.String(), nullable=False),
        sa.Column("filing_version", sa.BigInteger(), nullable=False),
        sa.Column("page", sa.BigInteger()),
        sa.Column("section_path", sa.Text()),
        sa.Column("table_name", sa.Text()),
        sa.Column("row_label", sa.Text()),
        sa.Column("column_label", sa.Text()),
        sa.Column("xbrl_concept", sa.String()),
        sa.Column("context_ref", sa.String()),
        sa.Column("chunk_id", sa.String()),
        sa.Column("source_hash", sa.String(length=64), nullable=False),
        sa.Column("snippet", sa.Text()),
        sa.Column("effective_date", sa.Date()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "workspace_id",
            "filing_id",
            "evidence_id",
            name="uq_filing_evidence_workspace_filing",
        ),
    )
    op.create_index(
        "idx_filing_evidence_filing",
        "filing_evidence",
        ["workspace_id", "filing_id"],
    )

    op.create_table(
        "filing_candidate_facts",
        sa.Column("candidate_id", sa.String(), primary_key=True),
        sa.Column("run_id", sa.String(), nullable=False),
        sa.Column("workspace_id", sa.String(), nullable=False),
        sa.Column("company_id", sa.String(), nullable=False),
        sa.Column("canonical_metric", sa.String(), nullable=False),
        sa.Column("reported_label", sa.String(), nullable=False),
        sa.Column("value_decimal", sa.String(), nullable=False),
        sa.Column("currency", sa.String()),
        sa.Column("unit_scale", sa.String(), nullable=False),
        sa.Column("period_start", sa.Date()),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("period_type", sa.String(), nullable=False),
        sa.Column("consolidation_scope", sa.String(), nullable=False),
        sa.Column("source_filing_id", sa.String(), nullable=False),
        sa.Column("source_filing_version", sa.BigInteger(), nullable=False),
        sa.Column("evidence_ids", sa.JSON(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("validation_status", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("extractor_version", sa.String(), nullable=False),
        sa.Column("prompt_version", sa.String()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_filing_candidate_run_metric",
        "filing_candidate_facts",
        ["run_id", "canonical_metric"],
    )

    op.create_table(
        "filing_approved_facts",
        sa.Column("fact_id", sa.String(), primary_key=True),
        sa.Column("candidate_id", sa.String(), nullable=False),
        sa.Column("run_id", sa.String(), nullable=False),
        sa.Column("workspace_id", sa.String(), nullable=False),
        sa.Column("company_id", sa.String(), nullable=False),
        sa.Column("canonical_metric", sa.String(), nullable=False),
        sa.Column("reported_label", sa.String(), nullable=False),
        sa.Column("value_decimal", sa.String(), nullable=False),
        sa.Column("currency", sa.String()),
        sa.Column("unit_scale", sa.String(), nullable=False),
        sa.Column("period_start", sa.Date()),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("period_type", sa.String(), nullable=False),
        sa.Column("consolidation_scope", sa.String(), nullable=False),
        sa.Column("source_filing_id", sa.String(), nullable=False),
        sa.Column("source_filing_version", sa.BigInteger(), nullable=False),
        sa.Column("evidence_ids", sa.JSON(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("validation_status", sa.String(), nullable=False),
        sa.Column("review_status", sa.String(), nullable=False),
        sa.Column("extractor_version", sa.String(), nullable=False),
        sa.Column("prompt_version", sa.String()),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("approved_by", sa.String(), nullable=False),
        sa.Column("supersedes_fact_id", sa.String()),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.UniqueConstraint(
            "workspace_id",
            "company_id",
            "canonical_metric",
            "period_end",
            "period_type",
            "consolidation_scope",
            "source_filing_id",
            "source_filing_version",
            name="uq_filing_approved_fact_business_key",
        ),
    )
    op.create_index(
        "idx_filing_approved_company_metric_period",
        "filing_approved_facts",
        ["workspace_id", "company_id", "canonical_metric", "period_end"],
    )

    op.create_table(
        "filing_intelligence_objects",
        sa.Column("object_id", sa.String(), primary_key=True),
        sa.Column("run_id", sa.String(), nullable=False),
        sa.Column("workspace_id", sa.String(), nullable=False),
        sa.Column("company_id", sa.String(), nullable=False),
        sa.Column("object_type", sa.String(), nullable=False),
        sa.Column("canonical_name", sa.String(), nullable=False),
        sa.Column("reported_label", sa.Text()),
        sa.Column("value_decimal", sa.String()),
        sa.Column("value_text", sa.Text()),
        sa.Column("currency", sa.String()),
        sa.Column("unit", sa.String()),
        sa.Column("period_start", sa.Date()),
        sa.Column("period_end", sa.Date()),
        sa.Column("source_filing_id", sa.String(), nullable=False),
        sa.Column("source_filing_version", sa.BigInteger(), nullable=False),
        sa.Column("evidence_ids", sa.JSON(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("review_status", sa.String(), nullable=False),
        sa.Column("extractor_version", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "filing_validation_defects",
        sa.Column("defect_id", sa.String(), primary_key=True),
        sa.Column("run_id", sa.String(), nullable=False),
        sa.Column("candidate_id", sa.String()),
        sa.Column("rule_code", sa.String(), nullable=False),
        sa.Column("severity", sa.String(), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("context", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "filing_review_requests",
        sa.Column("review_id", sa.String(), primary_key=True),
        sa.Column("run_id", sa.String(), nullable=False),
        sa.Column("workspace_id", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("decision_payload", sa.JSON(), nullable=False),
        sa.Column("reviewer_id", sa.String()),
        sa.Column("reason", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("run_id", "status", name="uq_filing_review_run_status"),
    )
    op.create_index(
        "idx_filing_review_workspace_status",
        "filing_review_requests",
        ["workspace_id", "status", "created_at"],
    )

    op.create_table(
        "filing_analysis_runs",
        sa.Column("analysis_id", sa.String(), primary_key=True),
        sa.Column("workspace_id", sa.String(), nullable=False),
        sa.Column("company_id", sa.String(), nullable=False),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("answer", sa.Text(), nullable=False),
        sa.Column("citations", sa.JSON(), nullable=False),
        sa.Column("tool_calls", sa.JSON(), nullable=False),
        sa.Column("warnings", sa.JSON(), nullable=False),
        sa.Column("trace_id", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "filing_index_runs",
        sa.Column("index_run_id", sa.String(), primary_key=True),
        sa.Column("run_id", sa.String(), nullable=False),
        sa.Column("workspace_id", sa.String(), nullable=False),
        sa.Column("company_id", sa.String(), nullable=False),
        sa.Column("filing_id", sa.String(), nullable=False),
        sa.Column("filing_version", sa.BigInteger(), nullable=False),
        sa.Column("index_version", sa.String(), nullable=False),
        sa.Column("embedding_model", sa.String(), nullable=False),
        sa.Column("collection_name", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("chunk_count", sa.BigInteger(), nullable=False),
        sa.Column("error_message", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "workspace_id",
            "filing_id",
            "filing_version",
            "index_version",
            name="uq_filing_index_version",
        ),
    )
    op.create_index(
        "idx_filing_index_runs_status",
        "filing_index_runs",
        ["workspace_id", "status", "updated_at"],
    )

    op.create_table(
        "filing_audit_events",
        sa.Column("event_id", sa.String(), primary_key=True),
        sa.Column("workspace_id", sa.String(), nullable=False),
        sa.Column("actor_id", sa.String(), nullable=False),
        sa.Column("action", sa.String(), nullable=False),
        sa.Column("target_type", sa.String(), nullable=False),
        sa.Column("target_id", sa.String(), nullable=False),
        sa.Column("before_payload", sa.JSON(), nullable=False),
        sa.Column("after_payload", sa.JSON(), nullable=False),
        sa.Column("reason", sa.Text()),
        sa.Column("trace_id", sa.String()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("filing_audit_events")
    op.drop_index("idx_filing_index_runs_status", table_name="filing_index_runs")
    op.drop_table("filing_index_runs")
    op.drop_table("filing_analysis_runs")
    op.drop_index("idx_filing_review_workspace_status", table_name="filing_review_requests")
    op.drop_table("filing_review_requests")
    op.drop_table("filing_validation_defects")
    op.drop_table("filing_intelligence_objects")
    op.drop_index(
        "idx_filing_approved_company_metric_period",
        table_name="filing_approved_facts",
    )
    op.drop_table("filing_approved_facts")
    op.drop_index(
        "idx_filing_candidate_run_metric",
        table_name="filing_candidate_facts",
    )
    op.drop_table("filing_candidate_facts")
    op.drop_index("idx_filing_evidence_filing", table_name="filing_evidence")
    op.drop_table("filing_evidence")
    op.drop_index("idx_filing_runs_lease", table_name="filing_runs")
    op.drop_index("idx_filing_runs_workspace_status", table_name="filing_runs")
    op.drop_table("filing_runs")
    op.drop_index(
        "idx_filing_documents_key_current",
        table_name="filing_documents",
    )
    op.drop_index(
        "idx_filing_documents_company_period",
        table_name="filing_documents",
    )
    op.drop_table("filing_documents")
