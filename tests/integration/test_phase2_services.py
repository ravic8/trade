from __future__ import annotations

import os
from datetime import date

import boto3
import pytest
from botocore.exceptions import ClientError

from trade_research.storage.object_store import ArtifactNamespace, ObjectArtifactStore

clickhouse_connect = pytest.importorskip("clickhouse_connect")
DatabaseError = pytest.importorskip(
    "clickhouse_connect.driver.exceptions"
).DatabaseError

pytestmark = pytest.mark.skipif(
    os.environ.get("PHASE2_SERVICE_TESTS") != "1",
    reason="requires the Phase 2 ClickHouse and MinIO services",
)


def _clickhouse(user_variable: str, password_variable: str):
    return clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username=os.environ[user_variable],
        password=os.environ[password_variable],
        database="research",
    )


def _s3(access_key_variable: str, secret_key_variable: str):
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        region_name="us-east-1",
        aws_access_key_id=os.environ[access_key_variable],
        aws_secret_access_key=os.environ[secret_key_variable],
    )


def test_clickhouse_roles_migrations_and_retry_reconciliation() -> None:
    migration = _clickhouse("CLICKHOUSE_MIGRATION_USER", "CLICKHOUSE_MIGRATION_PASSWORD")
    tables = {
        row[0]
        for row in migration.query(
            "SELECT name FROM system.tables WHERE database = 'research'"
        ).result_rows
    }
    assert {
        "ohlcv_daily",
        "feature_observations_daily",
        "target_observations_daily",
        "factor_statistics",
        "feature_distributions",
        "predictions_daily",
        "backtest_returns_daily",
        "backtest_positions_daily",
        "experiment_metrics",
        "data_quality_results",
        "schema_migrations",
    } <= tables

    writer = _clickhouse("CLICKHOUSE_DAGSTER_USER", "CLICKHOUSE_DAGSTER_PASSWORD")
    columns = [
        "workspace_id",
        "feature_key",
        "feature_version",
        "instrument_id",
        "session_date",
        "value",
        "dataset_snapshot_id",
        "source_run_id",
        "content_sha256",
        "version",
    ]
    row = [
        "phase2-ci",
        "momentum_20d",
        "v1",
        "NSE:INFY",
        date(2026, 9, 4),
        1.25,
        "snapshot-ci",
        "run-ci",
        "a" * 64,
        1,
    ]
    writer.insert("feature_observations_daily", [row, row], column_names=columns)

    reader = _clickhouse("CLICKHOUSE_API_USER", "CLICKHOUSE_API_PASSWORD")
    reconciled = reader.query(
        """
        SELECT count(), any(value)
        FROM feature_observations_daily FINAL
        WHERE workspace_id = 'phase2-ci' AND feature_key = 'momentum_20d'
        """
    ).first_row
    assert reconciled == (1, 1.25)
    with pytest.raises(DatabaseError):
        reader.command(
            "INSERT INTO feature_observations_daily "
            "(workspace_id, feature_key, feature_version, instrument_id, session_date, "
            "value, dataset_snapshot_id, source_run_id, content_sha256, version) "
            "VALUES ('denied', 'x', 'v1', 'NSE:X', '2026-09-04', 0, 's', 'r', "
            f"'{'b' * 64}', 1)"
        )

    analyst = _clickhouse("CLICKHOUSE_ANALYST_USER", "CLICKHOUSE_ANALYST_PASSWORD")
    assert analyst.query("SELECT count() FROM feature_observations_daily").first_row[0] >= 1
    with pytest.raises(DatabaseError):
        analyst.command("TRUNCATE TABLE feature_observations_daily")


def test_object_store_roles_versioning_integrity_and_deletion_denial() -> None:
    writer_client = _s3(
        "OBJECT_STORE_DAGSTER_ACCESS_KEY_ID",
        "OBJECT_STORE_DAGSTER_SECRET_ACCESS_KEY",
    )
    reader_client = _s3(
        "OBJECT_STORE_API_ACCESS_KEY_ID",
        "OBJECT_STORE_API_SECRET_ACCESS_KEY",
    )
    buckets = {namespace: f"trade-{namespace.value}" for namespace in ArtifactNamespace}
    for bucket in buckets.values():
        assert writer_client.get_bucket_versioning(Bucket=bucket)["Status"] == "Enabled"

    store = ObjectArtifactStore(writer_client, buckets=buckets, write_enabled=True)
    artifact = store.put_bytes(
        ArtifactNamespace.DATASETS,
        "ci/phase2-canary.parquet",
        b"phase2-canary",
        media_type="application/vnd.apache.parquet",
    )
    readonly = ObjectArtifactStore(reader_client, buckets=buckets)
    assert readonly.get_bytes(artifact) == b"phase2-canary"

    with pytest.raises(ClientError) as put_error:
        reader_client.put_object(
            Bucket=buckets[ArtifactNamespace.DATASETS],
            Key="ci/denied",
            Body=b"denied",
        )
    assert put_error.value.response["Error"]["Code"] == "AccessDenied"

    with pytest.raises(ClientError) as delete_error:
        writer_client.delete_object(
            Bucket=artifact.bucket,
            Key=artifact.key,
        )
    assert delete_error.value.response["Error"]["Code"] == "AccessDenied"
