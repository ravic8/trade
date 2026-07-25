from __future__ import annotations

import os
import subprocess
from pathlib import Path

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


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

    assert api["environment"]["FILING_ALERTMANAGER_URL"] == "http://alertmanager:9093"
    assert api["environment"]["FILING_ALERT_WEBHOOK_TOKEN_FILE"] == (
        "/run/secrets/alertmanager_webhook_token"
    )
    assert any(
        "/run/secrets/alertmanager_webhook_token:ro" in volume
        for volume in api["volumes"]
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
    alertmanager = services["alertmanager"]

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
    assert (
        "./ops/observability/filing-alerts.yaml:/etc/prometheus/rules/filing-alerts.yaml:ro"
        in prometheus["volumes"]
    )
    assert prometheus["depends_on"]["otel-collector"]["condition"] == ("service_started")
    assert prometheus["depends_on"]["alertmanager"]["condition"] == ("service_started")
    assert "ports" not in alertmanager
    assert alertmanager["expose"] == ["9093"]
    assert (
        "./ops/observability/alertmanager.yaml:/etc/alertmanager/alertmanager.yaml:ro"
        in alertmanager["volumes"]
    )
    assert any(
        "/run/secrets/alertmanager_webhook_token:ro" in volume
        for volume in alertmanager["volumes"]
    )
    assert alertmanager["depends_on"]["api"]["condition"] == "service_healthy"


def test_production_env_and_deploy_fail_closed_for_filing_secrets() -> None:
    env_example = (REPOSITORY_ROOT / ".env.prod.example").read_text(encoding="utf-8")
    deploy = (REPOSITORY_ROOT / "deploy/deploy.sh").read_text(encoding="utf-8")

    for name in (
        "PROD_FILING_S3_ACCESS_KEY_ID",
        "PROD_FILING_S3_SECRET_ACCESS_KEY",
        "PROD_MINIO_ROOT_USER",
        "PROD_MINIO_ROOT_PASSWORD",
        "PROD_RESILIENCE_REPORT_DIR=/opt/trade/resilience-reports",
        "PROD_HUMAN_REVIEW_REPORT_DIR=/opt/trade/human-review-reports",
        "PROD_ALERT_REPORT_DIR=/opt/trade/alert-reports",
        "PROD_ALERT_WEBHOOK_TOKEN_FILE=/opt/trade/secrets/alertmanager-webhook-token",
        "PROD_ALERTMANAGER_DATA_DIR=/opt/trade/alertmanager",
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
    assert "ps --status running -q alertmanager" in deploy
    assert "openssl rand -hex 32" in deploy
    assert "alert webhook token must contain at least 32 characters" in deploy


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


def test_backup_is_atomic_and_quiesces_mutable_services() -> None:
    backup = (REPOSITORY_ROOT / "deploy/backup.sh").read_text(encoding="utf-8")

    stops = [
        backup.index(f"stop {service}")
        for service in (
            "filing-worker",
            "dagster-daemon",
            "dagster-webserver",
            "cloudbeaver",
            "qdrant",
            "minio",
        )
    ]
    database_dump = backup.index("pg_dump")
    archives = [
        backup.index(f'"{name}.tgz"')
        for name in (
            "data",
            "artifacts",
            "qdrant",
            "dagster_home",
            "minio",
            "cloudbeaver",
        )
    ]
    restart = backup.rindex("\nrestart_quiesced_services\n")
    finalize = backup.index('mv "$BACKUP_DIR" "$FINAL_BACKUP_DIR"')

    assert max(stops) < database_dump < min(archives)
    assert max(archives) < restart < finalize
    assert 'BACKUP_DIR="$BACKUP_ROOT/.incomplete-$STAMP"' in backup
    assert "sha256sum postgres.dump > SHA256SUMS" in backup


def test_failed_backup_remains_incomplete_and_restarts_services(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    docker_log = tmp_path / "docker.log"
    backup_root = tmp_path / "backups"
    env_file = tmp_path / "production.env"

    persistent_directories = {
        "PROD_TRADE_DATA_DIR": tmp_path / "data",
        "PROD_TRADE_ARTIFACTS_DIR": tmp_path / "artifacts",
        "PROD_QDRANT_DATA_DIR": tmp_path / "qdrant",
        "PROD_DAGSTER_HOME_DIR": tmp_path / "dagster-home",
        "PROD_MINIO_DATA_DIR": tmp_path / "minio",
        "PROD_CLOUDBEAVER_WORKSPACE_DIR": tmp_path / "cloudbeaver",
    }
    for directory in persistent_directories.values():
        directory.mkdir()
        (directory / "state").write_text("state", encoding="utf-8")

    env_file.write_text(
        "\n".join(f"{name}={path}" for name, path in persistent_directories.items())
        + "\n",
        encoding="utf-8",
    )
    _write_executable(
        fake_bin / "docker",
        """#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_DOCKER_LOG"
if [[ "$*" == *"ps --status running -q"* ]]; then
  printf '%s\n' 'running-container'
elif [[ "$*" == *"exec -T postgres pg_dump"* ]]; then
  printf '%s\n' 'fake-postgres-dump'
fi
exit 0
""",
    )
    _write_executable(
        fake_bin / "tar",
        "#!/usr/bin/env bash\nexit 23\n",
    )

    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{fake_bin}:{environment['PATH']}",
            "TRADE_APP_DIR": str(REPOSITORY_ROOT),
            "TRADE_ENV_FILE": str(env_file),
            "TRADE_BACKUP_DIR": str(backup_root),
            "FAKE_DOCKER_LOG": str(docker_log),
        }
    )
    completed = subprocess.run(
        ["bash", str(REPOSITORY_ROOT / "deploy/backup.sh")],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert completed.returncode == 23
    backups = list(backup_root.iterdir())
    assert len(backups) == 1
    assert backups[0].name.startswith(".incomplete-")

    docker_calls = docker_log.read_text(encoding="utf-8")
    for service in (
        "filing-worker",
        "dagster-daemon",
        "dagster-webserver",
        "cloudbeaver",
        "qdrant",
        "minio",
    ):
        assert f"stop {service}" in docker_calls
    for service in ("minio", "qdrant", "cloudbeaver"):
        assert f"up -d --no-deps {service}" in docker_calls
    for service in ("filing-worker", "dagster-daemon", "dagster-webserver"):
        assert f"up -d {service}" in docker_calls
