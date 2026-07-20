from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
from alembic import command
from alembic.config import Config
from google.cloud import bigquery
from pydantic import ValidationError
from sqlalchemy import create_engine, inspect

from trade_research.analytics import bigquery as bigquery_module
from trade_research.analytics.bigquery import (
    ENTITY_SPECS,
    BigQuerySyncResult,
    GoogleBigQueryGateway,
    _validate_credentials_file,
    evaluate_bigquery_tsx_canary_readiness,
    run_bigquery_sync,
    run_bigquery_tsx_canary,
)
from trade_research.config import Settings


class RecordingClient:
    def __init__(self, *, reporting_location: str = "US") -> None:
        self.reporting_location = reporting_location
        self.operations: list[str] = []

    def get_dataset(self, dataset_id, *, dataset_view):
        assert dataset_view == bigquery.enums.DatasetView.METADATA
        self.operations.append(f"get:{dataset_id}")
        location = self.reporting_location if dataset_id.endswith("_reporting") else "US"
        return SimpleNamespace(location=location)

    def create_table(self, table, *, exists_ok):
        assert exists_ok is True
        self.operations.append(f"create:{table.table_id}")


def _gateway(client: RecordingClient) -> GoogleBigQueryGateway:
    gateway = GoogleBigQueryGateway.__new__(GoogleBigQueryGateway)
    gateway._bigquery = bigquery
    gateway._client = client
    gateway.project_id = "tradechain8"
    gateway.core_dataset = "trade_chain8_analytics"
    gateway.reporting_dataset = "trade_chain8_reporting"
    gateway.location = "US"
    gateway.authenticated_principal = (
        "trade-chain8-bigquery-exporter@tradechain8.iam.gserviceaccount.com"
    )
    return gateway


def test_preflight_checks_both_datasets_before_any_table_write() -> None:
    client = RecordingClient()
    verification = _gateway(client).ensure_foundation([ENTITY_SPECS["ohlcv_daily"]])

    assert client.operations[:2] == [
        "get:tradechain8.trade_chain8_analytics",
        "get:tradechain8.trade_chain8_reporting",
    ]
    assert client.operations[2] == "create:ohlcv_daily"
    assert verification.core_dataset_location == "US"
    assert verification.reporting_dataset_location == "US"


def test_phase9_2b_migration_adds_canary_reconciliation_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'phase9-2b.sqlite'}"
    monkeypatch.setenv("DATABASE_URL", database_url)
    command.upgrade(Config("alembic.ini"), "head")
    inspector = inspect(create_engine(database_url))

    run_columns = {
        column["name"] for column in inspector.get_columns("bigquery_sync_runs")
    }
    partition_columns = {
        column["name"]
        for column in inspector.get_columns("bigquery_sync_partitions")
    }
    assert {
        "reporting_dataset",
        "authenticated_principal",
        "staging_row_count",
        "merged_row_count",
        "duplicate_business_key_count",
    }.issubset(run_columns)
    assert {
        "staging_row_count",
        "merged_row_count",
        "duplicate_business_key_count",
        "source_min_date",
        "source_max_date",
        "destination_min_date",
        "destination_max_date",
    }.issubset(partition_columns)


def test_location_mismatch_fails_before_any_write() -> None:
    client = RecordingClient(reporting_location="EU")

    with pytest.raises(RuntimeError, match="reporting dataset location is EU"):
        _gateway(client).ensure_foundation([ENTITY_SPECS["ohlcv_daily"]])

    assert all(not operation.startswith("create:") for operation in client.operations)


def test_service_account_file_validation_is_fail_closed(tmp_path: Path) -> None:
    credential = tmp_path / "service-account.json"
    credential.write_text("non-secret test fixture", encoding="utf-8")
    credential.chmod(0o644)
    with pytest.raises(RuntimeError, match="permissions must be 0600"):
        _validate_credentials_file(credential)

    credential.chmod(0o600)
    _validate_credentials_file(credential)

    empty = tmp_path / "empty.json"
    empty.touch(mode=0o600)
    with pytest.raises(RuntimeError, match="missing or unreadable"):
        _validate_credentials_file(empty)


def test_service_account_identity_is_required_when_enabled() -> None:
    with pytest.raises(ValidationError, match="BIGQUERY_EXPECTED_SERVICE_ACCOUNT_EMAIL"):
        Settings(
            _env_file=None,
            bigquery_enabled=True,
            bigquery_project_id="tradechain8",
            bigquery_auth_method="service_account_file",
            bigquery_credentials_path="/run/secrets/gcp/service-account.json",
        )


def test_master_canary_and_production_gates_are_independent() -> None:
    settings = Settings(
        _env_file=None,
        bigquery_enabled=True,
        bigquery_project_id="tradechain8",
    )
    assert run_bigquery_sync(settings=settings).status == "gated"
    assert run_bigquery_sync(settings=settings, mode="canary").status == "gated"


def test_tsx_canary_scope_is_fixed_to_latest_completed_year(monkeypatch) -> None:
    captured = {}

    def fake_run(**kwargs):
        captured.update(kwargs)
        return BigQuerySyncResult(run_id="canary", status="completed")

    monkeypatch.setattr(bigquery_module, "run_bigquery_sync", fake_run)
    result = run_bigquery_tsx_canary(today=date(2026, 7, 20))

    assert result.status == "completed"
    assert captured["exchange"] == "TSX"
    assert captured["year"] == 2025
    assert captured["entities"] == ("ohlcv_daily",)
    assert captured["mode"] == "canary"


class CanaryState:
    def bigquery_sync_runs(self, *, limit):
        assert limit == 100
        return [
            {
                "run_id": run_id,
                "trigger": "dagster_tsx_canary",
                "status": "completed",
                "exchange": "TSX",
                "year": 2025,
                "entities": ["ohlcv_daily"],
            }
            for run_id in ("second", "first")
        ]

    def bigquery_sync_partitions(self, *, run_id, limit):
        assert limit == 10
        return [
            {
                "run_id": run_id,
                "status": "completed",
                "source_row_count": 100,
                "destination_row_count": 100,
                "count_difference": 0,
                "rejected_rows": 0,
                "duplicate_business_key_count": 0,
                "source_watermark": "2025-12-31",
                "destination_watermark": "2025-12-31",
                "source_min_date": date(2025, 1, 2),
                "source_max_date": date(2025, 12, 31),
                "destination_min_date": date(2025, 1, 2),
                "destination_max_date": date(2025, 12, 31),
                "schema_drift": {},
            }
        ]


def test_two_equivalent_canaries_are_required_for_production_readiness() -> None:
    readiness = evaluate_bigquery_tsx_canary_readiness(
        CanaryState(),  # type: ignore[arg-type]
        today=date(2026, 7, 20),
    )
    assert readiness.ready_for_production is True
    assert readiness.successful_run_ids == ("second", "first")
