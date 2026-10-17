#!/usr/bin/env python3
"""Verify the documented core bootstrap in a fresh, isolated Compose project."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from uuid import uuid4

ROOT_FILES = (
    "pyproject.toml",
    "uv.lock",
    "README.md",
    "alembic.ini",
    "Dockerfile.api",
    "docker-compose.yml",
    "docker-compose.external.yml",
    ".env.external.example",
    ".dockerignore",
    ".env.example",
)
SOURCE_DIRS = ("src", "migrations", "clickhouse", "evaluations", "scripts")
EXCLUDED = {
    "node_modules",
    "dist",
    ".vite",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".git",
    ".venv",
}
CORE = {"api", "postgres", "redis"}


def copy_source(root: Path, destination: Path) -> str:
    """Copy only build inputs, never the operator's .env, data, or generated output."""
    files = [root / name for name in ROOT_FILES]
    for name in SOURCE_DIRS:
        for directory, directories, filenames in os.walk(root / name, followlinks=False):
            directories[:] = sorted(set(directories) - EXCLUDED)
            if any((Path(directory) / item).is_symlink() for item in directories):
                raise RuntimeError("Build input contains a symlink directory")
            files.extend(Path(directory) / filename for filename in filenames)
    digest = hashlib.sha256()
    for source in sorted(files):
        relative = source.relative_to(root)
        if EXCLUDED.intersection(relative.parts) or source.suffix == ".pyc":
            continue
        if source.name.startswith(".env") and relative.as_posix() not in {
            ".env.example",
            ".env.external.example",
        }:
            continue
        if source.is_symlink():
            raise RuntimeError(f"Build input is a symlink: {relative}")
        content = source.read_bytes()
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        digest.update(str(relative).encode() + b"\0" + content + b"\0")
    shutil.copyfile(destination / ".env.example", destination / ".env")
    return digest.hexdigest()


def docker_environment() -> dict[str, str]:
    """Retain Docker access, excluding inherited application flags and credentials."""
    names = {
        "PATH",
        "HOME",
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "DOCKER_CONFIG",
        "DOCKER_TLS_VERIFY",
        "DOCKER_CERT_PATH",
        "SSH_AUTH_SOCK",
        "TMPDIR",
    }
    return {key: value for key, value in os.environ.items() if key in names}


def isolated_config(config: dict, project: str) -> dict:
    if set(config["services"]) != CORE:
        raise RuntimeError("Default Compose configuration must contain exactly three core services")
    for name, service in config["services"].items():
        if service.get("container_name"):
            raise RuntimeError("Core containers must be scoped by their Compose project")
        for port in service.get("ports", []):
            port.update(published="0", host_ip="127.0.0.1")
        if name == "api":
            service["volumes"] = [
                {"type": "volume", "source": "bootstrap_data", "target": "/app/data"},
                {"type": "volume", "source": "bootstrap_artifacts", "target": "/app/artifacts"},
            ]
            # The sample environment is the bootstrap contract. Any accidentally
            # enabled provider/optional capability makes this verification fail.
            environment = service["environment"]
            policy_flags = {
                "FILING_AUTO_APPROVE_XBRL",
                "CHAT_RATE_LIMIT_ENABLED",
                "LEGACY_UPSTOX_NSE_ENABLED",
            }
            for key, value in environment.items():
                if (
                    key.endswith("_ENABLED")
                    and key not in policy_flags
                    and str(value).lower() in {"true", "1", "yes", "on"}
                ):
                    raise RuntimeError(f"Sample core configuration unexpectedly enables {key}")
            for key in (
                "UPSTOX_ACCESS_TOKEN",
                "POLYGON_API_KEY",
                "OPENAI_API_KEY",
                "GEMINI_API_KEY",
                "QDRANT_API_KEY",
                "LANGFUSE_SECRET_KEY",
            ):
                if str(environment.get(key) or "").strip():
                    raise RuntimeError(f"Sample core configuration must leave {key} empty")
            expected = {
                "DATABASE_URL": "postgresql+psycopg://trade:trade@postgres:5432/trade_research",
                "REDIS_URL": "redis://redis:6379/0",
            }
            if any(environment.get(key) != value for key, value in expected.items()):
                raise RuntimeError("Sample endpoints must target the isolated core services")
    for kind in ("volumes", "networks"):
        for resource in config.get(kind, {}).values():
            if resource.get("external") or not resource.get("name", "").startswith(project + "_"):
                raise RuntimeError(f"Verification {kind} must belong to the isolated project")
    config["volumes"].update(
        {
            "bootstrap_data": {"name": f"{project}_bootstrap_data"},
            "bootstrap_artifacts": {"name": f"{project}_bootstrap_artifacts"},
        }
    )
    return config


READ_ONLY_SNAPSHOT = """
import json
from sqlalchemy import create_engine, text
from trade_data.config import get_settings
engine = create_engine(get_settings().database_url, hide_parameters=True)
with engine.connect() as connection, connection.begin():
    connection.execute(text("SET TRANSACTION READ ONLY"))
    tables = connection.execute(text(
        "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename"
    )).scalars().all()
    result = {"read_only": connection.execute(text(
        "SHOW transaction_read_only")).scalar_one() == "on", "tables": tables}
    if "alembic_version" in tables:
        result["revision"] = connection.execute(text(
            "SELECT version_num FROM alembic_version")).scalars().all()
        names = ("ohlcv_daily", "pipeline_work_items", "provider_request_log",
                 "ingestion_runs", "universe_snapshots", "exchange_sessions")
        result["row_counts"] = {name: connection.execute(text(
            "SELECT count(*) FROM " + name)).scalar_one() for name in names}
        result["latest_daily_date"] = connection.execute(text(
            "SELECT max(date) FROM ohlcv_daily")).scalar_one()
        result["timescale_version"] = connection.execute(text(
            "SELECT extversion FROM pg_extension WHERE extname='timescaledb'"
        )).scalar_one()
print(json.dumps(result, default=str))
engine.dispose()
"""
EXPECTED_HEAD = """
import json
from alembic.config import Config
from alembic.script import ScriptDirectory
print(json.dumps(ScriptDirectory.from_config(Config("alembic.ini")).get_heads()))
"""


class Verifier:
    def __init__(self, workspace: Path, project: str, log: Path):
        self.workspace, self.project, self.log = workspace, project, log
        self.environment = docker_environment()
        self.base = [
            "docker",
            "compose",
            "--env-file",
            str(workspace / ".env"),
            "-p",
            project,
            "-f",
            str(workspace / "docker-compose.yml"),
        ]

    def run(self, args: list[str], *, timeout: int = 300) -> str:
        capture = args[0] not in {"build", "up", "down", "logs"}
        with self.log.open("a") as log:
            result = subprocess.run(
                self.base + args,
                cwd=self.workspace,
                env=self.environment,
                text=True,
                stdout=subprocess.PIPE if capture else log,
                stderr=log,
                timeout=timeout,
            )
            if capture:
                log.write(result.stdout)
        if result.returncode:
            raise RuntimeError(f"Compose {args[0]} failed; see the verification log")
        return result.stdout or ""

    def snapshot(self) -> dict:
        return json.loads(self.run(["exec", "-T", "api", "python", "-c", READ_ONLY_SNAPSHOT]))

    def http(self, service: str, target: int, path: str, expected: int = 200) -> dict:
        binding = self.run(["port", service, str(target)]).strip()
        url = f"http://{binding}{path}"
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                with urlopen(url, timeout=5) as response:
                    status, body = response.status, response.read().decode()
            except HTTPError as error:
                status, body = error.code, error.read().decode()
            except (URLError, TimeoutError, ConnectionError):
                time.sleep(1)
                continue
            if status != expected:
                raise RuntimeError(f"{service}{path} returned {status}; expected {expected}")
            return {"url": url, "status": status, "body": body}
        raise RuntimeError(f"{service}{path} did not become available")


def check_fresh_core(verifier: Verifier, checks: dict, expected_services: set[str]) -> None:
    """Shared bootstrap, schema, diagnostics, and read-only HTTP acceptance."""
    before = verifier.snapshot()
    if not before["read_only"] or before["tables"]:
        raise RuntimeError("Fresh database is not empty before migrations")
    checks["empty_before_migrations"] = before
    print("Applying migrations and checking idempotent upgrade", flush=True)
    verifier.run(["exec", "-T", "api", "python", "-m", "alembic", "upgrade", "head"])
    first = verifier.snapshot()
    expected = json.loads(verifier.run(["exec", "-T", "api", "python", "-c", EXPECTED_HEAD]))
    verifier.run(["exec", "-T", "api", "python", "-m", "alembic", "upgrade", "head"])
    second = verifier.snapshot()
    if first != second or second["revision"] != expected:
        raise RuntimeError("Migrations did not reach a stable current head")
    if any(second["row_counts"].values()) or second["latest_daily_date"] is not None:
        raise RuntimeError("Fresh setup unexpectedly contains data or ingestion activity")
    checks["schema_and_empty_data"] = second
    doctor = json.loads(
        verifier.run(["exec", "-T", "api", "trade-data", "doctor", "--check-services"])
    )
    statuses = {check["name"]: check["status"] for check in doctor["checks"]}
    expected_statuses = {
        "postgresql": "pass",
        "redis": "pass",
        "daily_provider": "not_configured",
        "clickhouse": "disabled",
        "object_store": "disabled",
    }
    if not doctor["ready"] or any(
        statuses.get(key) != value for key, value in expected_statuses.items()
    ):
        raise RuntimeError("Required services or disabled capability diagnostics failed")
    checks["doctor"] = doctor
    print("Checking API and honest empty-data behavior", flush=True)
    checks["api_health"] = verifier.http("api", 8000, "/api/health")
    market = verifier.http("api", 8000, "/api/market/status")
    if json.loads(market["body"]) != []:
        raise RuntimeError("Fresh market status must be empty")
    checks["empty_market_status"] = market
    checks["filings_unmounted"] = verifier.http("api", 8000, "/api/filings/health", 404)
    final = verifier.snapshot()
    if final != second:
        raise RuntimeError("Read-only bootstrap checks changed database contents")
    rows = verifier.run(["ps", "--format", "json"])
    services = {json.loads(line)["Service"] for line in rows.splitlines() if line}
    if services != expected_services:
        raise RuntimeError("Unexpected services in acceptance project")
    checks["running_services"] = sorted(services)


def verify(root: Path, report: Path) -> bool:
    project = "trade-bootstrap-" + uuid4().hex[:12]
    report = report.resolve()
    report.parent.mkdir(parents=True, exist_ok=True)
    log = report.with_suffix(".log")
    log.write_text("")
    evidence = {
        "status": "failed",
        "project": project,
        "started_at": datetime.now(UTC).isoformat(),
        "checks": {},
    }
    # Keep the acceptance checkout inside a Docker-shareable workspace.
    scratch = root / "tmp" / "fresh-setup"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=project + "-", dir=scratch) as temporary:
        workspace = Path(temporary)
        verifier = Verifier(workspace, project, log)
        cleanup_needed = False
        try:
            print("Copying isolated source and sample configuration", flush=True)
            evidence["source_sha256"] = copy_source(root, workspace)
            verifier.run(["version"])
            config = json.loads(verifier.run(["config", "--format", "json"]))
            config = isolated_config(config, project)
            config_file = workspace / "bootstrap.compose.json"
            config_file.write_text(json.dumps(config))
            verifier.base[-1] = str(config_file)
            verifier.run(["config", "--quiet"])
            print("Building core API from isolated source", flush=True)
            cleanup_needed = True
            verifier.run(["build", "api"], timeout=900)
            print("Starting four core services on fresh volumes", flush=True)
            verifier.run(["up", "-d", "--no-build", "--wait", "--wait-timeout", "150"])
            check_fresh_core(verifier, evidence["checks"], CORE)
            evidence["status"] = "passed"
        except Exception as error:
            evidence["error"] = str(error)
            if cleanup_needed:
                try:
                    verifier.run(["logs", "--no-color", "--tail", "100"])
                except Exception:
                    pass  # Preserve the original failure and still attempt cleanup.
        finally:
            if cleanup_needed:
                print("Cleaning up only the isolated verification project", flush=True)
                try:
                    verifier.run(["down", "--volumes", "--rmi", "local"])
                    evidence["cleanup"] = "passed"
                except Exception:
                    evidence["cleanup"] = "failed"
                    evidence["status"] = "failed"
                    evidence["cleanup_project"] = project
            evidence["finished_at"] = datetime.now(UTC).isoformat()
            report.write_text(json.dumps(evidence, indent=2) + "\n")
    print(f"Fresh setup: {evidence['status']}; report: {report}", flush=True)
    return evidence["status"] == "passed"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=Path("output/fresh-setup/report.json"))
    args = parser.parse_args()
    raise SystemExit(0 if verify(Path(__file__).resolve().parents[1], args.report) else 1)


if __name__ == "__main__":
    main()
