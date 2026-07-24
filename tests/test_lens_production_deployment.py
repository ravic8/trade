from __future__ import annotations

import os
import subprocess
from pathlib import Path

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _production_compose() -> dict[str, object]:
    return yaml.safe_load((REPOSITORY_ROOT / "docker-compose.prod.yml").read_text(encoding="utf-8"))


def test_production_api_and_worker_use_durable_filing_runtime() -> None:
    compose = _production_compose()
    services = compose["services"]
    api = services["api"]
    worker = services["filing-worker"]

    for service in (api, worker):
        environment = service["environment"]
        assert environment["APP_ENV"] == "production"
        assert environment["FILING_QUEUE_MODE"] == "celery"
        assert environment["FILING_ARTIFACT_BACKEND"] == "s3"
        assert environment["FILING_S3_ENDPOINT_URL"] == "http://minio:9000"
        assert environment["FILING_REQUIRE_WORKSPACE_HEADER"] == (
            "${PROD_FILING_REQUIRE_WORKSPACE_HEADER:-true}"
        )
        assert environment["OTEL_ENABLED"] == "${PROD_OTEL_ENABLED:-true}"
        assert environment["OTEL_EXPORTER_OTLP_ENDPOINT"] == ("http://otel-collector:4318")
        assert service["depends_on"]["postgres"]["condition"] == "service_healthy"
        assert service["depends_on"]["minio-init"]["condition"] == (
            "service_completed_successfully"
        )

    assert worker["image"] == api["image"]
    assert worker["command"][0:4] == [
        "celery",
        "-A",
        "trade_research.filings.tasks:celery_app",
        "worker",
    ]
    assert worker["healthcheck"]["test"][0] == "CMD-SHELL"
    assert "inspect ping" in worker["healthcheck"]["test"][1]
    assert worker["volumes"] == ["${PROD_TRADE_DATA_DIR:-./data}:/app/data:ro"]


def test_production_minio_is_private_versioned_and_uses_app_identity() -> None:
    services = _production_compose()["services"]
    minio = services["minio"]
    bootstrap = services["minio-init"]

    assert "ports" not in minio
    assert minio["volumes"] == [
        "${PROD_MINIO_DATA_DIR:-/opt/trade/minio}:/data"
    ]
    assert "/minio/health/live" in minio["healthcheck"]["test"][-1]
    assert bootstrap["depends_on"]["minio"]["condition"] == "service_healthy"

    command = bootstrap["command"][0]
    assert "mc version enable" in command
    assert "arn:aws:s3:::$$FILING_S3_BUCKET/*" in command
    assert "mc admin policy create lens filing-artifacts" in command
    assert '"s3:DeleteObject"' not in command
    assert "mc admin user add" in command
    assert "mc admin user info" in command
    assert "mc admin policy attach lens filing-artifacts" in command
    assert "$$MINIO_ROOT_PASSWORD" in command
    assert "$$FILING_S3_SECRET_ACCESS_KEY" in command


def test_production_observability_is_internal_and_persistent() -> None:
    services = _production_compose()["services"]
    collector = services["otel-collector"]
    prometheus = services["prometheus"]

    assert "ports" not in collector
    assert collector["expose"] == ["4317", "4318", "9464"]
    assert collector["volumes"] == [
        "./ops/observability/otel-collector.yaml:/etc/otelcol-contrib/config.yaml:ro"
    ]
    assert "ports" not in prometheus
    assert (
        "${PROD_PROMETHEUS_DATA_DIR:-/opt/trade/prometheus}:/prometheus"
        in prometheus["volumes"]
    )
    assert prometheus["depends_on"]["otel-collector"]["condition"] == ("service_started")


def test_production_env_and_deploy_fail_closed_for_filing_secrets() -> None:
    env_example = (REPOSITORY_ROOT / ".env.prod.example").read_text(encoding="utf-8")
    deploy = (REPOSITORY_ROOT / "deploy/deploy.sh").read_text(encoding="utf-8")

    for name in (
        "PROD_FILING_S3_ACCESS_KEY_ID",
        "PROD_FILING_S3_SECRET_ACCESS_KEY",
        "PROD_MINIO_ROOT_USER",
        "PROD_MINIO_ROOT_PASSWORD",
        "PROD_FILING_REQUIRE_WORKSPACE_HEADER=true",
        "PROD_OTEL_ENABLED=true",
    ):
        assert name in env_example

    assert 'require_secure_value "PROD_MINIO_ROOT_USER"' in deploy
    assert "filing storage must not use the MinIO root identity" in deploy
    assert "production filing workspace enforcement must remain enabled" in deploy
    assert '"queue_mode":"celery"' in deploy
    assert '"artifact_backend":"s3"' in deploy
    assert '"workspace_header_required":true' in deploy
    assert "ps --status running -q filing-worker" in deploy
    assert "ps --status running -q minio" in deploy
    assert "ps --status running -q otel-collector" in deploy
    assert "ps --status running -q prometheus" in deploy


def test_deploy_rejects_placeholder_filing_secrets_before_docker(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    for command in ("docker", "git", "curl"):
        executable = fake_bin / command
        executable.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
        executable.chmod(0o755)
    env_file = tmp_path / "production.env"
    env_file.write_text(
        "\n".join(
            [
                "PROD_FILING_ENABLED=true",
                "PROD_MINIO_ROOT_USER=trade-minio-admin",
                "PROD_MINIO_ROOT_PASSWORD=replace-with-root-secret",
                "PROD_FILING_S3_ACCESS_KEY_ID=lensfilings",
                "PROD_FILING_S3_SECRET_ACCESS_KEY=replace-with-app-secret",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{fake_bin}:{environment['PATH']}",
            "TRADE_APP_DIR": str(REPOSITORY_ROOT),
            "TRADE_ENV_FILE": str(env_file),
        }
    )

    completed = subprocess.run(
        ["bash", str(REPOSITORY_ROOT / "deploy/deploy.sh")],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert completed.returncode == 1
    assert "PROD_MINIO_ROOT_PASSWORD must be set" in completed.stderr


def test_api_image_contains_locked_filing_evaluation() -> None:
    dockerfile = (REPOSITORY_ROOT / "Dockerfile.api").read_text(encoding="utf-8")

    assert "COPY evaluations ./evaluations" in dockerfile


def test_backup_captures_minio_with_worker_quiesced() -> None:
    backup = (REPOSITORY_ROOT / "deploy/backup.sh").read_text(encoding="utf-8")

    stop_worker = backup.index("stop filing-worker")
    stop_minio = backup.index("stop minio")
    archive = backup.index('"minio.tgz"')
    restart = backup.index("restart_filing_storage\n", archive)
    assert stop_worker < stop_minio < archive < restart
