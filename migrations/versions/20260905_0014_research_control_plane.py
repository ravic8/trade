"""Add the Phase 2 research control-plane schema.

Revision ID: 20260905_0014
Revises: 20260904_0013
Create Date: 2026-09-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260905_0014"
down_revision: str | None = "20260904_0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _timestamps() -> tuple[sa.Column, sa.Column]:
    return (
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )


def upgrade() -> None:
    op.create_table(
        "feature_definitions",
        sa.Column("feature_definition_id", sa.String(length=36), primary_key=True),
        sa.Column("feature_key", sa.String(length=200), nullable=False),
        sa.Column("version", sa.String(length=64), nullable=False),
        sa.Column("definition", sa.JSON(), nullable=False),
        sa.Column("owner", sa.String(length=200), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        *_timestamps(),
        sa.UniqueConstraint("feature_key", "version", name="uq_feature_definitions_key_version"),
    )
    op.create_table(
        "target_definitions",
        sa.Column("target_definition_id", sa.String(length=36), primary_key=True),
        sa.Column("target_key", sa.String(length=200), nullable=False),
        sa.Column("version", sa.String(length=64), nullable=False),
        sa.Column("definition", sa.JSON(), nullable=False),
        sa.Column("owner", sa.String(length=200), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        *_timestamps(),
        sa.UniqueConstraint("target_key", "version", name="uq_target_definitions_key_version"),
    )
    op.create_table(
        "workflow_definitions",
        sa.Column("workflow_definition_id", sa.String(length=36), primary_key=True),
        sa.Column("workflow_key", sa.String(length=200), nullable=False, unique=True),
        sa.Column("workflow_kind", sa.String(length=64), nullable=False),
        sa.Column("owner", sa.String(length=200), nullable=False),
        *_timestamps(),
    )
    op.create_table(
        "workflow_versions",
        sa.Column("workflow_version_id", sa.String(length=36), primary_key=True),
        sa.Column(
            "workflow_definition_id",
            sa.String(length=36),
            sa.ForeignKey("workflow_definitions.workflow_definition_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("definition_sha256", sa.String(length=64), nullable=False),
        sa.Column("specification", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "workflow_definition_id",
            "version",
            name="uq_workflow_versions_definition_version",
        ),
    )
    op.create_table(
        "workflow_schedules",
        sa.Column("workflow_schedule_id", sa.String(length=36), primary_key=True),
        sa.Column(
            "workflow_version_id",
            sa.String(length=36),
            sa.ForeignKey("workflow_versions.workflow_version_id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("schedule_key", sa.String(length=200), nullable=False, unique=True),
        sa.Column("cron_expression", sa.String(length=100), nullable=False),
        sa.Column("timezone", sa.String(length=64), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        *_timestamps(),
    )
    op.create_table(
        "artifact_manifests",
        sa.Column("artifact_manifest_id", sa.String(length=36), primary_key=True),
        sa.Column("artifact_type", sa.String(length=64), nullable=False),
        sa.Column("storage_uri", sa.Text(), nullable=False, unique=True),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("media_type", sa.String(length=200), nullable=False),
        sa.Column("object_version_id", sa.String(length=512)),
        sa.Column("manifest_metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("sha256", "storage_uri", name="uq_artifact_manifests_digest_uri"),
    )
    op.create_index("idx_artifact_manifests_sha256", "artifact_manifests", ["sha256"])
    op.create_table(
        "dataset_snapshots",
        sa.Column("dataset_snapshot_id", sa.String(length=36), primary_key=True),
        sa.Column("dataset_key", sa.String(length=200), nullable=False),
        sa.Column("version", sa.String(length=64), nullable=False),
        sa.Column(
            "artifact_manifest_id",
            sa.String(length=36),
            sa.ForeignKey("artifact_manifests.artifact_manifest_id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("row_count", sa.BigInteger(), nullable=False),
        sa.Column("snapshot_metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("dataset_key", "version", name="uq_dataset_snapshots_key_version"),
    )
    op.create_table(
        "model_versions",
        sa.Column("model_version_id", sa.String(length=36), primary_key=True),
        sa.Column("model_key", sa.String(length=200), nullable=False),
        sa.Column("version", sa.String(length=64), nullable=False),
        sa.Column(
            "dataset_snapshot_id",
            sa.String(length=36),
            sa.ForeignKey("dataset_snapshots.dataset_snapshot_id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "artifact_manifest_id",
            sa.String(length=36),
            sa.ForeignKey("artifact_manifests.artifact_manifest_id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("parameters", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("model_key", "version", name="uq_model_versions_key_version"),
    )
    op.create_table(
        "workflow_runs",
        sa.Column("workflow_run_id", sa.String(length=36), primary_key=True),
        sa.Column(
            "workflow_version_id",
            sa.String(length=36),
            sa.ForeignKey("workflow_versions.workflow_version_id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "workflow_request_id",
            sa.String(),
            sa.ForeignKey("workflow_requests.workflow_id", ondelete="SET NULL"),
        ),
        sa.Column("dagster_run_id", sa.String(length=255), unique=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("parameters", sa.JSON(), nullable=False),
        sa.Column("outputs", sa.JSON()),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        *_timestamps(),
    )
    op.create_index("idx_workflow_runs_status_created", "workflow_runs", ["status", "created_at"])
    op.create_table(
        "experiment_runs",
        sa.Column("experiment_run_id", sa.String(length=36), primary_key=True),
        sa.Column("experiment_key", sa.String(length=200), nullable=False),
        sa.Column(
            "workflow_run_id",
            sa.String(length=36),
            sa.ForeignKey("workflow_runs.workflow_run_id"),
        ),
        sa.Column(
            "dataset_snapshot_id",
            sa.String(length=36),
            sa.ForeignKey("dataset_snapshots.dataset_snapshot_id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "model_version_id",
            sa.String(length=36),
            sa.ForeignKey("model_versions.model_version_id"),
        ),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("parameters", sa.JSON(), nullable=False),
        sa.Column("summary_metrics", sa.JSON(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_experiment_runs_key_created",
        "experiment_runs",
        ["experiment_key", "created_at"],
    )
    op.create_table(
        "validation_runs",
        sa.Column("validation_run_id", sa.String(length=36), primary_key=True),
        sa.Column("subject_type", sa.String(length=64), nullable=False),
        sa.Column("subject_id", sa.String(length=255), nullable=False),
        sa.Column("validator_key", sa.String(length=200), nullable=False),
        sa.Column("validator_version", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("summary", sa.JSON(), nullable=False),
        sa.Column("details", sa.JSON(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_validation_runs_subject_created",
        "validation_runs",
        ["subject_type", "subject_id", "created_at"],
    )
    op.create_table(
        "audit_events",
        sa.Column("audit_event_id", sa.String(length=36), primary_key=True),
        sa.Column("actor", sa.String(length=255), nullable=False),
        sa.Column("action", sa.String(length=200), nullable=False),
        sa.Column("entity_type", sa.String(length=64), nullable=False),
        sa.Column("entity_id", sa.String(length=255), nullable=False),
        sa.Column("request_id", sa.String(length=255)),
        sa.Column("event_metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_audit_events_entity_created",
        "audit_events",
        ["entity_type", "entity_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_audit_events_entity_created", table_name="audit_events")
    op.drop_table("audit_events")
    op.drop_index("idx_validation_runs_subject_created", table_name="validation_runs")
    op.drop_table("validation_runs")
    op.drop_index("idx_experiment_runs_key_created", table_name="experiment_runs")
    op.drop_table("experiment_runs")
    op.drop_index("idx_workflow_runs_status_created", table_name="workflow_runs")
    op.drop_table("workflow_runs")
    op.drop_table("model_versions")
    op.drop_table("dataset_snapshots")
    op.drop_index("idx_artifact_manifests_sha256", table_name="artifact_manifests")
    op.drop_table("artifact_manifests")
    op.drop_table("workflow_schedules")
    op.drop_table("workflow_versions")
    op.drop_table("workflow_definitions")
    op.drop_table("target_definitions")
    op.drop_table("feature_definitions")
