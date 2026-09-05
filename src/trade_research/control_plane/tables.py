from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
)

from trade_research.storage.timescale import metadata

feature_definitions_table = Table(
    "feature_definitions",
    metadata,
    Column("feature_definition_id", String(36), primary_key=True),
    Column("feature_key", String(200), nullable=False),
    Column("version", String(64), nullable=False),
    Column("definition", JSON, nullable=False),
    Column("owner", String(200), nullable=False),
    Column("status", String(32), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("feature_key", "version", name="uq_feature_definitions_key_version"),
)

target_definitions_table = Table(
    "target_definitions",
    metadata,
    Column("target_definition_id", String(36), primary_key=True),
    Column("target_key", String(200), nullable=False),
    Column("version", String(64), nullable=False),
    Column("definition", JSON, nullable=False),
    Column("owner", String(200), nullable=False),
    Column("status", String(32), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("target_key", "version", name="uq_target_definitions_key_version"),
)

workflow_definitions_table = Table(
    "workflow_definitions",
    metadata,
    Column("workflow_definition_id", String(36), primary_key=True),
    Column("workflow_key", String(200), nullable=False, unique=True),
    Column("workflow_kind", String(64), nullable=False),
    Column("owner", String(200), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

workflow_versions_table = Table(
    "workflow_versions",
    metadata,
    Column("workflow_version_id", String(36), primary_key=True),
    Column(
        "workflow_definition_id",
        String(36),
        ForeignKey("workflow_definitions.workflow_definition_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("version", Integer, nullable=False),
    Column("definition_sha256", String(64), nullable=False),
    Column("specification", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint(
        "workflow_definition_id",
        "version",
        name="uq_workflow_versions_definition_version",
    ),
)

workflow_schedules_table = Table(
    "workflow_schedules",
    metadata,
    Column("workflow_schedule_id", String(36), primary_key=True),
    Column(
        "workflow_version_id",
        String(36),
        ForeignKey("workflow_versions.workflow_version_id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("schedule_key", String(200), nullable=False, unique=True),
    Column("cron_expression", String(100), nullable=False),
    Column("timezone", String(64), nullable=False),
    Column("enabled", Boolean, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

artifact_manifests_table = Table(
    "artifact_manifests",
    metadata,
    Column("artifact_manifest_id", String(36), primary_key=True),
    Column("artifact_type", String(64), nullable=False),
    Column("storage_uri", Text, nullable=False, unique=True),
    Column("sha256", String(64), nullable=False),
    Column("size_bytes", BigInteger, nullable=False),
    Column("media_type", String(200), nullable=False),
    Column("object_version_id", String(512)),
    Column("manifest_metadata", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("sha256", "storage_uri", name="uq_artifact_manifests_digest_uri"),
)

dataset_snapshots_table = Table(
    "dataset_snapshots",
    metadata,
    Column("dataset_snapshot_id", String(36), primary_key=True),
    Column("dataset_key", String(200), nullable=False),
    Column("version", String(64), nullable=False),
    Column(
        "artifact_manifest_id",
        String(36),
        ForeignKey("artifact_manifests.artifact_manifest_id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("as_of", DateTime(timezone=True), nullable=False),
    Column("row_count", BigInteger, nullable=False),
    Column("snapshot_metadata", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("dataset_key", "version", name="uq_dataset_snapshots_key_version"),
)

model_versions_table = Table(
    "model_versions",
    metadata,
    Column("model_version_id", String(36), primary_key=True),
    Column("model_key", String(200), nullable=False),
    Column("version", String(64), nullable=False),
    Column(
        "dataset_snapshot_id",
        String(36),
        ForeignKey("dataset_snapshots.dataset_snapshot_id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column(
        "artifact_manifest_id",
        String(36),
        ForeignKey("artifact_manifests.artifact_manifest_id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("parameters", JSON, nullable=False),
    Column("status", String(32), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("model_key", "version", name="uq_model_versions_key_version"),
)

workflow_runs_table = Table(
    "workflow_runs",
    metadata,
    Column("workflow_run_id", String(36), primary_key=True),
    Column(
        "workflow_version_id",
        String(36),
        ForeignKey("workflow_versions.workflow_version_id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column(
        "workflow_request_id",
        String,
        ForeignKey("workflow_requests.workflow_id", ondelete="SET NULL"),
    ),
    Column("dagster_run_id", String(255), unique=True),
    Column("status", String(32), nullable=False),
    Column("parameters", JSON, nullable=False),
    Column("outputs", JSON),
    Column("started_at", DateTime(timezone=True)),
    Column("completed_at", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

experiment_runs_table = Table(
    "experiment_runs",
    metadata,
    Column("experiment_run_id", String(36), primary_key=True),
    Column("experiment_key", String(200), nullable=False),
    Column("workflow_run_id", String(36), ForeignKey("workflow_runs.workflow_run_id")),
    Column(
        "dataset_snapshot_id",
        String(36),
        ForeignKey("dataset_snapshots.dataset_snapshot_id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("model_version_id", String(36), ForeignKey("model_versions.model_version_id")),
    Column("status", String(32), nullable=False),
    Column("parameters", JSON, nullable=False),
    Column("summary_metrics", JSON, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

validation_runs_table = Table(
    "validation_runs",
    metadata,
    Column("validation_run_id", String(36), primary_key=True),
    Column("subject_type", String(64), nullable=False),
    Column("subject_id", String(255), nullable=False),
    Column("validator_key", String(200), nullable=False),
    Column("validator_version", String(64), nullable=False),
    Column("status", String(32), nullable=False),
    Column("summary", JSON, nullable=False),
    Column("details", JSON, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

audit_events_table = Table(
    "audit_events",
    metadata,
    Column("audit_event_id", String(36), primary_key=True),
    Column("actor", String(255), nullable=False),
    Column("action", String(200), nullable=False),
    Column("entity_type", String(64), nullable=False),
    Column("entity_id", String(255), nullable=False),
    Column("request_id", String(255)),
    Column("event_metadata", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
