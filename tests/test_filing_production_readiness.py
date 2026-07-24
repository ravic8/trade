from __future__ import annotations

from pathlib import Path

from trade_research.config import Settings
from trade_research.filings.production import verify_filing_production


def _settings(tmp_path: Path, **overrides: object) -> Settings:
    manifest = tmp_path / "data/filings/nse/INFY/manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text("{}", encoding="utf-8")
    golden = tmp_path / "evaluations/filings/infy_m1_golden.json"
    golden.parent.mkdir(parents=True)
    golden.write_text("{}", encoding="utf-8")
    values: dict[str, object] = {
        "app_env": "production",
        "database_url": "postgresql+psycopg://trade:secret@postgres/trade",
        "redis_url": "redis://redis:6379/0",
        "admin_emails": "admin@example.test",
        "filing_enabled": True,
        "filing_manifest_path": manifest,
        "filing_golden_dataset_path": golden,
        "filing_queue_mode": "celery",
        "filing_artifact_backend": "s3",
        "filing_s3_endpoint_url": "http://minio:9000",
        "filing_s3_access_key_id": "lensfilings",
        "filing_s3_secret_access_key": "application-secret",
        "filing_require_workspace_header": True,
        "langfuse_enabled": True,
        "langfuse_public_key": "public-key",
        "langfuse_secret_key": "secret-key",
        "otel_enabled": True,
        "otel_exporter_otlp_endpoint": "http://otel-collector:4318",
    }
    values.update(overrides)
    return Settings(**values)


def _passing_probes() -> dict[str, object]:
    return {
        "postgresql_migration": lambda: "migration verified",
        "redis": lambda: "Redis verified",
        "object_store": lambda: "object store verified",
        "filing_worker": lambda: "worker verified",
        "otel_collector": lambda: "collector verified",
    }


def test_readiness_passes_only_when_every_production_gate_passes(
    tmp_path: Path,
) -> None:
    report = verify_filing_production(
        _settings(tmp_path),
        probes=_passing_probes(),
        application_root=tmp_path,
    )

    assert report.passed is True
    assert all(check.passed for check in report.checks)
    assert {check.name for check in report.checks} == {
        "runtime_configuration",
        "authentication",
        "source_manifest",
        "golden_dataset",
        "langfuse",
        "postgresql_migration",
        "redis",
        "object_store",
        "filing_worker",
        "otel_collector",
    }


def test_readiness_fails_closed_without_auth_telemetry_or_worker(
    tmp_path: Path,
) -> None:
    probes = _passing_probes()

    def failed_worker() -> str:
        raise RuntimeError("redis://user:top-secret@redis:6379 must not reach the report")

    probes["filing_worker"] = failed_worker
    report = verify_filing_production(
        _settings(
            tmp_path,
            admin_emails="",
            filing_require_workspace_header=False,
            langfuse_enabled=False,
            otel_enabled=False,
        ),
        probes=probes,
        application_root=tmp_path,
    )
    serialized = report.model_dump_json()

    assert report.passed is False
    assert "top-secret" not in serialized
    failed = {check.name for check in report.checks if not check.passed}
    assert failed == {
        "runtime_configuration",
        "authentication",
        "langfuse",
        "filing_worker",
    }


def test_readiness_reports_missing_corpus_without_importing_it(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    settings.filing_manifest_path.unlink()
    settings.filing_golden_dataset_path.unlink()

    report = verify_filing_production(
        settings,
        probes=_passing_probes(),
        application_root=tmp_path,
    )

    assert report.passed is False
    failed = {check.name for check in report.checks if not check.passed}
    assert failed == {"source_manifest", "golden_dataset"}
