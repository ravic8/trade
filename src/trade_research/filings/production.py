from __future__ import annotations

import socket
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import boto3
import httpx
import redis
from alembic.config import Config
from alembic.script import ScriptDirectory
from botocore.config import Config as BotoConfig
from pydantic import BaseModel
from sqlalchemy import create_engine, text

from trade_research.config import Settings

Probe = Callable[[], str]


class ProductionReadinessCheck(BaseModel):
    name: str
    passed: bool
    detail: str


class ProductionReadinessReport(BaseModel):
    generated_at: datetime
    passed: bool
    checks: list[ProductionReadinessCheck]


def verify_filing_production(
    settings: Settings,
    *,
    timeout_seconds: float = 5.0,
    probes: Mapping[str, Probe] | None = None,
    application_root: Path | None = None,
) -> ProductionReadinessReport:
    """Run read-only production filing checks without returning secret values."""
    root = (application_root or Path.cwd()).resolve()
    checks = [
        _configuration_check(settings),
        _authentication_check(settings),
        _path_check("source_manifest", _container_path(settings.filing_manifest_path, root)),
        _path_check(
            "golden_dataset",
            _container_path(settings.filing_golden_dataset_path, root),
        ),
        _langfuse_check(settings),
        _alerting_check(settings),
    ]
    active_probes = probes or _default_probes(
        settings,
        timeout_seconds=timeout_seconds,
        application_root=root,
    )
    for name in (
        "postgresql_migration",
        "redis",
        "object_store",
        "filing_worker",
        "otel_collector",
        "alertmanager",
    ):
        probe = active_probes.get(name)
        if probe is None:
            checks.append(
                ProductionReadinessCheck(
                    name=name,
                    passed=False,
                    detail="probe is not configured",
                )
            )
            continue
        checks.append(_run_probe(name, probe))
    return ProductionReadinessReport(
        generated_at=datetime.now(UTC),
        passed=all(check.passed for check in checks),
        checks=checks,
    )


def _configuration_check(settings: Settings) -> ProductionReadinessCheck:
    defects: list[str] = []
    if settings.app_env != "production":
        defects.append("APP_ENV must be production")
    if not settings.filing_enabled:
        defects.append("filing runtime is disabled")
    if settings.filing_queue_mode != "celery":
        defects.append("queue mode must be celery")
    if settings.filing_artifact_backend != "s3":
        defects.append("artifact backend must be s3")
    if not settings.otel_enabled:
        defects.append("OpenTelemetry must be enabled")
    return ProductionReadinessCheck(
        name="runtime_configuration",
        passed=not defects,
        detail="; ".join(defects) if defects else "durable production runtime configured",
    )


def _authentication_check(settings: Settings) -> ProductionReadinessCheck:
    defects: list[str] = []
    if not settings.filing_require_workspace_header:
        defects.append("workspace header enforcement is disabled")
    if not settings.admin_emails.strip():
        defects.append("admin allowlist is empty")
    return ProductionReadinessCheck(
        name="authentication",
        passed=not defects,
        detail="; ".join(defects) if defects else "workspace and admin controls configured",
    )


def _langfuse_check(settings: Settings) -> ProductionReadinessCheck:
    if not settings.langfuse_enabled:
        return ProductionReadinessCheck(
            name="langfuse",
            passed=False,
            detail="Langfuse is disabled",
        )
    if not settings.langfuse_public_key or not settings.langfuse_secret_key:
        return ProductionReadinessCheck(
            name="langfuse",
            passed=False,
            detail="Langfuse credentials are incomplete",
        )
    return ProductionReadinessCheck(
        name="langfuse",
        passed=True,
        detail="Langfuse tracing is configured",
    )


def _alerting_check(settings: Settings) -> ProductionReadinessCheck:
    defects: list[str] = []
    endpoint = urlparse(settings.filing_alertmanager_url)
    if endpoint.scheme not in {"http", "https"} or not endpoint.hostname:
        defects.append("Alertmanager URL is invalid")
    token_file = settings.filing_alert_webhook_token_file
    if token_file is None or not token_file.is_file():
        defects.append("alert webhook token file is missing")
    else:
        try:
            token = token_file.read_text(encoding="utf-8").strip()
        except OSError:
            defects.append("alert webhook token file is unreadable")
        else:
            if len(token) < 32:
                defects.append("alert webhook token is too short")
    return ProductionReadinessCheck(
        name="alerting",
        passed=not defects,
        detail=(
            "; ".join(defects)
            if defects
            else "Alertmanager routing and webhook authentication are configured"
        ),
    )


def _path_check(name: str, path: Path) -> ProductionReadinessCheck:
    return ProductionReadinessCheck(
        name=name,
        passed=path.is_file(),
        detail="file is present" if path.is_file() else "required file is missing",
    )


def _run_probe(name: str, probe: Probe) -> ProductionReadinessCheck:
    try:
        detail = probe()
    except Exception:
        return ProductionReadinessCheck(
            name=name,
            passed=False,
            detail="probe failed; inspect service logs for the protected error",
        )
    return ProductionReadinessCheck(name=name, passed=True, detail=detail)


def _default_probes(
    settings: Settings,
    *,
    timeout_seconds: float,
    application_root: Path,
) -> dict[str, Probe]:
    return {
        "postgresql_migration": lambda: _probe_postgresql(
            settings,
            application_root=application_root,
        ),
        "redis": lambda: _probe_redis(settings, timeout_seconds=timeout_seconds),
        "object_store": lambda: _probe_object_store(
            settings,
            timeout_seconds=timeout_seconds,
        ),
        "filing_worker": lambda: _probe_worker(timeout_seconds=timeout_seconds),
        "otel_collector": lambda: _probe_otel(
            settings,
            timeout_seconds=timeout_seconds,
        ),
        "alertmanager": lambda: _probe_alertmanager(
            settings,
            timeout_seconds=timeout_seconds,
        ),
    }


def _probe_postgresql(settings: Settings, *, application_root: Path) -> str:
    alembic_config = Config(str(application_root / "alembic.ini"))
    alembic_config.set_main_option(
        "script_location",
        str(application_root / "migrations"),
    )
    expected_head = ScriptDirectory.from_config(alembic_config).get_current_head()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            deployed_head = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
    finally:
        engine.dispose()
    if deployed_head != expected_head:
        raise RuntimeError("database migration does not match application head")
    return f"database and migration head {deployed_head} verified"


def _probe_redis(settings: Settings, *, timeout_seconds: float) -> str:
    client = redis.Redis.from_url(
        settings.redis_url,
        socket_connect_timeout=timeout_seconds,
        socket_timeout=timeout_seconds,
    )
    if not client.ping():
        raise RuntimeError("Redis ping failed")
    return "Redis broker responded"


def _probe_object_store(settings: Settings, *, timeout_seconds: float) -> str:
    client = boto3.client(
        "s3",
        endpoint_url=settings.filing_s3_endpoint_url,
        region_name=settings.filing_s3_region,
        aws_access_key_id=settings.filing_s3_access_key_id,
        aws_secret_access_key=settings.filing_s3_secret_access_key,
        config=BotoConfig(
            connect_timeout=timeout_seconds,
            read_timeout=timeout_seconds,
            retries={"max_attempts": 1},
        ),
    )
    client.head_bucket(Bucket=settings.filing_s3_bucket)
    versioning = client.get_bucket_versioning(Bucket=settings.filing_s3_bucket)
    if versioning.get("Status") != "Enabled":
        raise RuntimeError("filing bucket versioning is not enabled")
    return "versioned filing bucket is reachable with application credentials"


def _probe_worker(*, timeout_seconds: float) -> str:
    from trade_research.filings.tasks import celery_app

    responses = celery_app.control.ping(timeout=timeout_seconds)
    if not responses:
        raise RuntimeError("no filing worker responded")
    return f"{len(responses)} Celery worker(s) responded"


def _probe_otel(settings: Settings, *, timeout_seconds: float) -> str:
    endpoint = urlparse(settings.otel_exporter_otlp_endpoint or "")
    if not endpoint.hostname or not endpoint.port:
        raise RuntimeError("invalid OTLP endpoint")
    with socket.create_connection(
        (endpoint.hostname, endpoint.port),
        timeout=timeout_seconds,
    ):
        pass
    return "OpenTelemetry collector endpoint is reachable"


def _probe_alertmanager(settings: Settings, *, timeout_seconds: float) -> str:
    endpoint = settings.filing_alertmanager_url.rstrip("/")
    response = httpx.get(f"{endpoint}/-/ready", timeout=timeout_seconds)
    response.raise_for_status()
    if response.text.strip().upper() != "OK":
        raise RuntimeError("Alertmanager readiness response was unexpected")
    return "Alertmanager is ready to route notifications"


def _container_path(path: Path, application_root: Path) -> Path:
    if path.is_absolute():
        return path
    return application_root / path
