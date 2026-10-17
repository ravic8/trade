#!/usr/bin/env python3
"""Prove external core endpoints using infrastructure owned by a separate project."""

from __future__ import annotations

import argparse
import json
import secrets
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

try:
    from .verify_fresh_setup import Verifier, check_fresh_core, copy_source
except ImportError:  # Direct script execution.
    from verify_fresh_setup import Verifier, check_fresh_core, copy_source

APP_SERVICES = {"api"}


def fixture_config(project: str, database_password: str, redis_password: str) -> dict:
    """Only acceptance-owned infrastructure; no operator endpoints or volumes."""
    return {
        "services": {
            "database": {
                "image": "timescale/timescaledb:latest-pg16",
                "environment": {
                    "POSTGRES_USER": "external_user",
                    "POSTGRES_DB": "external_trade",
                    "POSTGRES_PASSWORD": database_password,
                },
                "volumes": [
                    {
                        "type": "volume",
                        "source": "database_data",
                        "target": "/var/lib/postgresql/data",
                    }
                ],
                "networks": {"default": {"aliases": ["database.service"]}},
                "healthcheck": {
                    "test": ["CMD-SHELL", "pg_isready -U external_user -d external_trade"],
                    "interval": "5s",
                    "timeout": "3s",
                    "retries": 20,
                },
            },
            "cache": {
                "image": "redis:7-alpine",
                "command": ["redis-server", "--requirepass", redis_password],
                "networks": {"default": {"aliases": ["cache.service"]}},
                "healthcheck": {
                    "test": ["CMD", "redis-cli", "-a", redis_password, "ping"],
                    "interval": "5s",
                    "timeout": "3s",
                    "retries": 20,
                },
            },
        },
        "volumes": {"database_data": {"name": f"{project}_database_data"}},
        "networks": {"default": {"name": f"{project}_services"}},
    }


def isolated_application(config: dict, project: str, infrastructure_project: str) -> dict:
    if set(config["services"]) != APP_SERVICES:
        raise RuntimeError("External application must resolve to exactly the API")
    if config["services"]["api"].get("depends_on"):
        raise RuntimeError("External API must not depend on local infrastructure services")
    for service in config["services"].values():
        if service.get("container_name"):
            raise RuntimeError("Acceptance containers must be scoped by project")
        for port in service.get("ports", []):
            port.update(published="0", host_ip="127.0.0.1")
    for kind in ("volumes", "networks"):
        for resource in config.get(kind, {}).values():
            if resource.get("external") or not resource.get("name", "").startswith(project + "_"):
                raise RuntimeError("Application resources must belong to the acceptance project")
    config["services"]["api"]["volumes"] = [
        {"type": "volume", "source": "acceptance_data", "target": "/app/data"},
        {"type": "volume", "source": "acceptance_artifacts", "target": "/app/artifacts"},
    ]
    config.setdefault("volumes", {}).update(
        {
            name: {"name": f"{project}_{name}"}
            for name in ("acceptance_data", "acceptance_artifacts")
        }
    )
    config.setdefault("networks", {})["infrastructure"] = {
        "external": True,
        "name": f"{infrastructure_project}_services",
    }
    config["services"]["api"]["networks"] = {"default": None, "infrastructure": None}
    return config


def private_json(path: Path, value: dict) -> None:
    path.touch(mode=0o600)
    path.write_text(json.dumps(value))


def resolved_application(verifier: Verifier) -> dict:
    """Resolve secrets without copying the resolved environment into the log."""
    import subprocess

    result = subprocess.run(
        verifier.base + ["config", "--format", "json"],
        cwd=verifier.workspace,
        env=verifier.environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode:
        raise RuntimeError(
            "External Compose configuration failed; check required endpoints/env file"
        )
    return json.loads(result.stdout)


def doctor(verifier: Verifier, failed_service: str | None = None) -> dict:
    # The wrapper records the real CLI exit status while allowing the runner to
    # inspect expected failures. CLI stderr is deliberately not printed.
    code = """
import json, subprocess
result = subprocess.run(["trade-data", "doctor", "--check-services"],
                        capture_output=True, text=True)
print(json.dumps({"exit_code": result.returncode, "report": json.loads(result.stdout)}))
"""
    result = json.loads(verifier.run(["exec", "-T", "api", "python", "-c", code]))
    statuses = {check["name"]: check["status"] for check in result["report"]["checks"]}
    for name in ("postgresql", "redis"):
        if statuses[name] != ("fail" if name == failed_service else "pass"):
            raise RuntimeError("Service failure was not diagnosed accurately")
    if result["report"]["ready"] != (failed_service is None):
        raise RuntimeError("Diagnostics incorrectly reported readiness")
    if (result["exit_code"] == 0) != (failed_service is None):
        raise RuntimeError("Doctor exit status does not reflect service availability")
    return result


def verify(root: Path, report: Path) -> bool:
    token = uuid4().hex[:12]
    project, infrastructure_project = f"trade-external-{token}", f"trade-services-{token}"
    report = report.resolve()
    report.parent.mkdir(parents=True, exist_ok=True)
    log = report.with_suffix(".log")
    log.write_text("")
    passwords = [secrets.token_hex(24) + ":@%/", secrets.token_hex(24) + ":@%/"]
    evidence = {
        "status": "failed",
        "project": project,
        "infrastructure_project": infrastructure_project,
        "started_at": datetime.now(UTC).isoformat(),
        "checks": {},
    }
    scratch = root / "tmp" / "external-services"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=project + "-", dir=scratch) as temporary:
        workspace = Path(temporary)
        application = Verifier(workspace, project, log)
        infrastructure = Verifier(workspace, infrastructure_project, log)
        app_cleanup = infra_cleanup = False
        try:
            print("Copying source; generating separate authenticated infrastructure", flush=True)
            evidence["source_sha256"] = copy_source(root, workspace)
            database_url = (
                f"postgresql+psycopg://external_user:{quote(passwords[0], safe='')}"
                "@database.service:5432/external_trade"
            )
            redis_url = f"redis://:{quote(passwords[1], safe='')}@cache.service:6379/0"
            env = workspace / ".env.external"
            env.touch(mode=0o600)
            # Start with the core sample's disabled provider flags. Compose's
            # external overlay replaces the .env env_file, including all secrets.
            env.write_text(
                (workspace / ".env").read_text()
                + f"\nCOMPOSE_DATABASE_URL={database_url}\nCOMPOSE_REDIS_URL={redis_url}\n"
            )
            application.base[3] = str(env)
            application.base.extend(["-f", str(workspace / "docker-compose.external.yml")])
            config = resolved_application(application)
            api_environment = config["services"]["api"]["environment"]
            if (
                api_environment["DATABASE_URL"] != database_url
                or api_environment["REDIS_URL"] != redis_url
            ):
                raise RuntimeError("External endpoints were overridden or ignored")
            config = isolated_application(config, project, infrastructure_project)
            app_file, infra_file = workspace / "application.json", workspace / "services.json"
            private_json(app_file, config)
            private_json(infra_file, fixture_config(infrastructure_project, *passwords))
            application.base = application.base[:6] + ["-f", str(app_file)]
            infrastructure.base[-1] = str(infra_file)
            application.run(["config", "--quiet"])
            infrastructure.run(["config", "--quiet"])
            infra_cleanup = True
            infrastructure.run(["up", "-d", "--wait", "--wait-timeout", "120"])
            app_cleanup = True
            print("Building and starting API without local database or Redis", flush=True)
            application.run(["build", "api"], timeout=900)
            application.run(["up", "-d", "--no-build", "--wait", "--wait-timeout", "150"])
            check_fresh_core(application, evidence["checks"], APP_SERVICES)
            baseline = application.snapshot()
            print("Testing owned external-service outages and recovery", flush=True)
            infrastructure.run(["stop", "database"])
            evidence["checks"]["database_outage"] = doctor(application, "postgresql")
            evidence["checks"]["database_unavailable_api"] = application.http(
                "api", 8000, "/api/market/status", 503
            )
            infrastructure.run(["up", "-d", "--wait", "--wait-timeout", "120", "database"])
            evidence["checks"]["database_recovery"] = doctor(application)
            infrastructure.run(["stop", "cache"])
            evidence["checks"]["redis_outage"] = doctor(application, "redis")
            infrastructure.run(["up", "-d", "--wait", "--wait-timeout", "120", "cache"])
            evidence["checks"]["redis_recovery"] = doctor(application)
            if application.snapshot() != baseline:
                raise RuntimeError("External-service checks changed database contents")
            if json.loads(application.http("api", 8000, "/api/market/status")["body"]) != []:
                raise RuntimeError("Recovered market status must remain empty")
            evidence["checks"]["separate_ownership"] = {
                "application_services": sorted(APP_SERVICES),
                "infrastructure_services": ["cache", "database"],
                "authenticated_endpoints_forwarded": True,
                "schema_and_data_unchanged_after_outages": True,
            }
            evidence["status"] = "passed"
        except Exception as error:
            evidence["error"] = str(error)
            for verifier, needed in ((application, app_cleanup), (infrastructure, infra_cleanup)):
                if needed:
                    try:
                        verifier.run(["logs", "--no-color", "--tail", "100"])
                    except Exception:
                        pass
        finally:
            print("Cleaning application first, then only owned infrastructure", flush=True)
            evidence["cleanup"] = {}
            for verifier, needed in ((application, app_cleanup), (infrastructure, infra_cleanup)):
                if needed:
                    try:
                        verifier.run(["down", "--volumes", "--rmi", "local"])
                        evidence["cleanup"][verifier.project] = "passed"
                    except Exception:
                        evidence["cleanup"][verifier.project] = "failed"
                        evidence["status"] = "failed"
            # Even a failure log must not retain generated endpoint credentials.
            log_content = log.read_text()
            for password in passwords + [quote(value, safe="") for value in passwords]:
                log_content = log_content.replace(password, "[redacted]")
            log.write_text(log_content)
            evidence["finished_at"] = datetime.now(UTC).isoformat()
            serialized = json.dumps(evidence, indent=2)
            for password in passwords + [quote(value, safe="") for value in passwords]:
                serialized = serialized.replace(password, "[redacted]")
            report.write_text(serialized + "\n")
    print(f"External endpoints: {evidence['status']}; report: {report}", flush=True)
    return evidence["status"] == "passed"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=Path("output/external-services/report.json"))
    args = parser.parse_args()
    raise SystemExit(0 if verify(Path(__file__).resolve().parents[1], args.report) else 1)


if __name__ == "__main__":
    main()
