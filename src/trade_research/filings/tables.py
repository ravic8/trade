from __future__ import annotations

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Index,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
)

filing_metadata = MetaData()


filing_documents_table = Table(
    "filing_documents",
    filing_metadata,
    Column("filing_id", String, primary_key=True),
    Column("workspace_id", String, nullable=False),
    Column("company_id", String, nullable=False),
    Column("symbol", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("company_name", String, nullable=False),
    Column("categories", JSON, nullable=False),
    Column("title", Text),
    Column("source_url", Text, nullable=False),
    Column("source_apis", JSON, nullable=False),
    Column("filing_date", DateTime(timezone=True)),
    Column("period_end", Date),
    Column("consolidation_scope", String, nullable=False),
    Column("audited", Boolean),
    Column("submission_type", String),
    Column("relative_path", Text, nullable=False),
    Column("object_uri", Text, nullable=False),
    Column("filename", String, nullable=False),
    Column("byte_size", BigInteger, nullable=False),
    Column("sha256", String(64), nullable=False),
    Column("content_type", String, nullable=False),
    Column("document_key", String, nullable=False),
    Column("version", BigInteger, nullable=False),
    Column("supersedes_filing_id", String),
    Column("is_current", Boolean, nullable=False),
    Column("status", String, nullable=False),
    Column("parse_quality", Float),
    Column("source_metadata", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint(
        "workspace_id",
        "company_id",
        "sha256",
        name="uq_filing_documents_workspace_company_hash",
    ),
    UniqueConstraint(
        "workspace_id",
        "document_key",
        "version",
        name="uq_filing_documents_workspace_key_version",
    ),
)


filing_runs_table = Table(
    "filing_runs",
    filing_metadata,
    Column("run_id", String, primary_key=True),
    Column("thread_id", String, nullable=False),
    Column("workspace_id", String, nullable=False),
    Column("company_id", String, nullable=False),
    Column("filing_id", String, nullable=False),
    Column("workflow_type", String, nullable=False),
    Column("idempotency_key", String, nullable=False),
    Column("status", String, nullable=False),
    Column("current_node", String),
    Column("progress", Float, nullable=False),
    Column("attempt_count", BigInteger, nullable=False),
    Column("max_attempts", BigInteger, nullable=False),
    Column("cancel_requested", Boolean, nullable=False),
    Column("input_payload", JSON, nullable=False),
    Column("output_payload", JSON, nullable=False),
    Column("error_code", String),
    Column("error_message", Text),
    Column("worker_id", String),
    Column("trace_id", String),
    Column("queued_at", DateTime(timezone=True)),
    Column("started_at", DateTime(timezone=True)),
    Column("heartbeat_at", DateTime(timezone=True)),
    Column("lease_expires_at", DateTime(timezone=True)),
    Column("waiting_review_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint(
        "workspace_id",
        "idempotency_key",
        name="uq_filing_runs_workspace_idempotency",
    ),
)


filing_evidence_table = Table(
    "filing_evidence",
    filing_metadata,
    Column("evidence_id", String, primary_key=True),
    Column("workspace_id", String, nullable=False),
    Column("company_id", String, nullable=False),
    Column("filing_id", String, nullable=False),
    Column("filing_version", BigInteger, nullable=False),
    Column("page", BigInteger),
    Column("section_path", Text),
    Column("table_name", Text),
    Column("row_label", Text),
    Column("column_label", Text),
    Column("xbrl_concept", String),
    Column("context_ref", String),
    Column("chunk_id", String),
    Column("source_hash", String(64), nullable=False),
    Column("snippet", Text),
    Column("effective_date", Date),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint(
        "workspace_id",
        "filing_id",
        "evidence_id",
        name="uq_filing_evidence_workspace_filing",
    ),
)


filing_candidate_facts_table = Table(
    "filing_candidate_facts",
    filing_metadata,
    Column("candidate_id", String, primary_key=True),
    Column("run_id", String, nullable=False),
    Column("workspace_id", String, nullable=False),
    Column("company_id", String, nullable=False),
    Column("canonical_metric", String, nullable=False),
    Column("reported_label", String, nullable=False),
    Column("value_decimal", String, nullable=False),
    Column("currency", String),
    Column("unit_scale", String, nullable=False),
    Column("period_start", Date),
    Column("period_end", Date, nullable=False),
    Column("period_type", String, nullable=False),
    Column("consolidation_scope", String, nullable=False),
    Column("source_filing_id", String, nullable=False),
    Column("source_filing_version", BigInteger, nullable=False),
    Column("evidence_ids", JSON, nullable=False),
    Column("confidence", Float, nullable=False),
    Column("validation_status", String, nullable=False),
    Column("status", String, nullable=False),
    Column("extractor_version", String, nullable=False),
    Column("prompt_version", String),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)


filing_approved_facts_table = Table(
    "filing_approved_facts",
    filing_metadata,
    Column("fact_id", String, primary_key=True),
    Column("candidate_id", String, nullable=False),
    Column("run_id", String, nullable=False),
    Column("workspace_id", String, nullable=False),
    Column("company_id", String, nullable=False),
    Column("canonical_metric", String, nullable=False),
    Column("reported_label", String, nullable=False),
    Column("value_decimal", String, nullable=False),
    Column("currency", String),
    Column("unit_scale", String, nullable=False),
    Column("period_start", Date),
    Column("period_end", Date, nullable=False),
    Column("period_type", String, nullable=False),
    Column("consolidation_scope", String, nullable=False),
    Column("source_filing_id", String, nullable=False),
    Column("source_filing_version", BigInteger, nullable=False),
    Column("evidence_ids", JSON, nullable=False),
    Column("confidence", Float, nullable=False),
    Column("validation_status", String, nullable=False),
    Column("review_status", String, nullable=False),
    Column("extractor_version", String, nullable=False),
    Column("prompt_version", String),
    Column("approved_at", DateTime(timezone=True), nullable=False),
    Column("approved_by", String, nullable=False),
    Column("supersedes_fact_id", String),
    Column("is_current", Boolean, nullable=False),
    UniqueConstraint(
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


filing_intelligence_objects_table = Table(
    "filing_intelligence_objects",
    filing_metadata,
    Column("object_id", String, primary_key=True),
    Column("run_id", String, nullable=False),
    Column("workspace_id", String, nullable=False),
    Column("company_id", String, nullable=False),
    Column("object_type", String, nullable=False),
    Column("canonical_name", String, nullable=False),
    Column("reported_label", Text),
    Column("value_decimal", String),
    Column("value_text", Text),
    Column("currency", String),
    Column("unit", String),
    Column("period_start", Date),
    Column("period_end", Date),
    Column("source_filing_id", String, nullable=False),
    Column("source_filing_version", BigInteger, nullable=False),
    Column("evidence_ids", JSON, nullable=False),
    Column("confidence", Float, nullable=False),
    Column("review_status", String, nullable=False),
    Column("extractor_version", String, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)


filing_validation_defects_table = Table(
    "filing_validation_defects",
    filing_metadata,
    Column("defect_id", String, primary_key=True),
    Column("run_id", String, nullable=False),
    Column("candidate_id", String),
    Column("rule_code", String, nullable=False),
    Column("severity", String, nullable=False),
    Column("message", Text, nullable=False),
    Column("context", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)


filing_review_requests_table = Table(
    "filing_review_requests",
    filing_metadata,
    Column("review_id", String, primary_key=True),
    Column("run_id", String, nullable=False),
    Column("workspace_id", String, nullable=False),
    Column("status", String, nullable=False),
    Column("payload", JSON, nullable=False),
    Column("decision_payload", JSON, nullable=False),
    Column("reviewer_id", String),
    Column("reason", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("decided_at", DateTime(timezone=True)),
    UniqueConstraint("run_id", "status", name="uq_filing_review_run_status"),
)


filing_analysis_runs_table = Table(
    "filing_analysis_runs",
    filing_metadata,
    Column("analysis_id", String, primary_key=True),
    Column("workspace_id", String, nullable=False),
    Column("company_id", String, nullable=False),
    Column("question", Text, nullable=False),
    Column("status", String, nullable=False),
    Column("answer", Text, nullable=False),
    Column("citations", JSON, nullable=False),
    Column("tool_calls", JSON, nullable=False),
    Column("warnings", JSON, nullable=False),
    Column("trace_id", String, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)


filing_universe_snapshots_table = Table(
    "filing_universe_snapshots",
    filing_metadata,
    Column("snapshot_id", String, primary_key=True),
    Column("workspace_id", String, nullable=False),
    Column("universe_id", String, nullable=False),
    Column("effective_date", Date, nullable=False),
    Column("source_url", Text, nullable=False),
    Column("source_hash", String(64), nullable=False),
    Column("members", JSON, nullable=False),
    Column("member_count", BigInteger, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint(
        "workspace_id",
        "universe_id",
        "source_hash",
        name="uq_filing_universe_snapshot_source",
    ),
)


filing_investigation_runs_table = Table(
    "filing_investigation_runs",
    filing_metadata,
    Column("analysis_id", String, primary_key=True),
    Column("thread_id", String, nullable=False),
    Column("workspace_id", String, nullable=False),
    Column("universe_id", String, nullable=False),
    Column("universe_snapshot_id", String),
    Column("question", Text, nullable=False),
    Column("status", String, nullable=False),
    Column("current_node", String, nullable=False),
    Column("progress", Float, nullable=False),
    Column("request_payload", JSON, nullable=False),
    Column("plan_payload", JSON, nullable=False),
    Column("result_payload", JSON, nullable=False),
    Column("error_code", String),
    Column("error_message", Text),
    Column("trace_id", String),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
)


filing_investigation_events_table = Table(
    "filing_investigation_events",
    filing_metadata,
    Column("event_id", String, primary_key=True),
    Column("analysis_id", String, nullable=False),
    Column("workspace_id", String, nullable=False),
    Column("sequence", BigInteger, nullable=False),
    Column("node", String, nullable=False),
    Column("status", String, nullable=False),
    Column("detail", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint(
        "analysis_id",
        "sequence",
        name="uq_filing_investigation_event_sequence",
    ),
)


filing_investigation_evaluations_table = Table(
    "filing_investigation_evaluations",
    filing_metadata,
    Column("evaluation_id", String, primary_key=True),
    Column("analysis_id", String, nullable=False),
    Column("workspace_id", String, nullable=False),
    Column("dataset_id", String, nullable=False),
    Column("evaluator_version", String, nullable=False),
    Column("status", String, nullable=False),
    Column("score", Float, nullable=False),
    Column("report_payload", JSON, nullable=False),
    Column("trace_id", String),
    Column("created_at", DateTime(timezone=True), nullable=False),
)


filing_index_runs_table = Table(
    "filing_index_runs",
    filing_metadata,
    Column("index_run_id", String, primary_key=True),
    Column("run_id", String, nullable=False),
    Column("workspace_id", String, nullable=False),
    Column("company_id", String, nullable=False),
    Column("filing_id", String, nullable=False),
    Column("filing_version", BigInteger, nullable=False),
    Column("index_version", String, nullable=False),
    Column("embedding_model", String, nullable=False),
    Column("collection_name", String, nullable=False),
    Column("status", String, nullable=False),
    Column("chunk_count", BigInteger, nullable=False),
    Column("error_message", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint(
        "workspace_id",
        "filing_id",
        "filing_version",
        "index_version",
        name="uq_filing_index_version",
    ),
)


filing_audit_events_table = Table(
    "filing_audit_events",
    filing_metadata,
    Column("event_id", String, primary_key=True),
    Column("workspace_id", String, nullable=False),
    Column("actor_id", String, nullable=False),
    Column("action", String, nullable=False),
    Column("target_type", String, nullable=False),
    Column("target_id", String, nullable=False),
    Column("before_payload", JSON, nullable=False),
    Column("after_payload", JSON, nullable=False),
    Column("reason", Text),
    Column("trace_id", String),
    Column("created_at", DateTime(timezone=True), nullable=False),
)


Index(
    "idx_filing_documents_company_period",
    filing_documents_table.c.workspace_id,
    filing_documents_table.c.company_id,
    filing_documents_table.c.period_end,
)
Index(
    "idx_filing_index_runs_status",
    filing_index_runs_table.c.workspace_id,
    filing_index_runs_table.c.status,
    filing_index_runs_table.c.updated_at,
)
Index(
    "idx_filing_documents_key_current",
    filing_documents_table.c.workspace_id,
    filing_documents_table.c.document_key,
    filing_documents_table.c.is_current,
)
Index(
    "idx_filing_runs_workspace_status",
    filing_runs_table.c.workspace_id,
    filing_runs_table.c.status,
    filing_runs_table.c.updated_at,
)
Index(
    "idx_filing_runs_lease",
    filing_runs_table.c.status,
    filing_runs_table.c.lease_expires_at,
)
Index(
    "idx_filing_candidate_run_metric",
    filing_candidate_facts_table.c.run_id,
    filing_candidate_facts_table.c.canonical_metric,
)
Index(
    "idx_filing_approved_company_metric_period",
    filing_approved_facts_table.c.workspace_id,
    filing_approved_facts_table.c.company_id,
    filing_approved_facts_table.c.canonical_metric,
    filing_approved_facts_table.c.period_end,
)
Index(
    "idx_filing_evidence_filing",
    filing_evidence_table.c.workspace_id,
    filing_evidence_table.c.filing_id,
)
Index(
    "idx_filing_review_workspace_status",
    filing_review_requests_table.c.workspace_id,
    filing_review_requests_table.c.status,
    filing_review_requests_table.c.created_at,
)
Index(
    "idx_filing_universe_workspace_effective",
    filing_universe_snapshots_table.c.workspace_id,
    filing_universe_snapshots_table.c.universe_id,
    filing_universe_snapshots_table.c.effective_date,
)
Index(
    "idx_filing_investigation_workspace_status",
    filing_investigation_runs_table.c.workspace_id,
    filing_investigation_runs_table.c.status,
    filing_investigation_runs_table.c.updated_at,
)
Index(
    "idx_filing_investigation_events",
    filing_investigation_events_table.c.analysis_id,
    filing_investigation_events_table.c.sequence,
)
Index(
    "idx_filing_investigation_evaluation_latest",
    filing_investigation_evaluations_table.c.workspace_id,
    filing_investigation_evaluations_table.c.analysis_id,
    filing_investigation_evaluations_table.c.created_at,
)
