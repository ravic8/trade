#!/usr/bin/env python3
"""Prove core backup, isolated restore, previous-schema upgrade, and volume retention."""

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
    from .core_recovery import FILESYSTEM, CoreRecovery
    from .verify_external_services import fixture_config, private_json
    from .verify_fresh_setup import CORE, EXPECTED_HEAD, Verifier, copy_source, isolated_config
except ImportError:
    from core_recovery import FILESYSTEM, CoreRecovery
    from verify_external_services import fixture_config, private_json
    from verify_fresh_setup import CORE, EXPECTED_HEAD, Verifier, copy_source, isolated_config

PREVIOUS_REVISION = "20260726_0012"
CHECKPOINT = r"""
import pathlib
from sqlalchemy import create_engine, text
from trade_data.config import get_settings
engine = create_engine(get_settings().database_url)
with engine.begin() as c:
    c.execute(text("CREATE SCHEMA recovery_proof"))
    c.execute(text("CREATE TABLE recovery_proof.checkpoint "
                   "(id text PRIMARY KEY, note text NOT NULL)"))
    c.execute(text("INSERT INTO recovery_proof.checkpoint "
                   "VALUES ('m0', 'operational recovery marker')"))
for root in ('/app/data', '/app/artifacts'):
    directory = pathlib.Path(root) / 'recovery-proof'
    directory.mkdir()
    (directory / 'checkpoint.bin').write_bytes(b'\x00Milestone 0 recovery marker\xff\n')
engine.dispose()
"""
READ_CHECKPOINT = """
import json
from sqlalchemy import create_engine, text
from trade_data.config import get_settings
engine = create_engine(get_settings().database_url)
with engine.connect() as c, c.begin():
    c.execute(text("SET TRANSACTION READ ONLY"))
    print(json.dumps(c.execute(text("SELECT id, note FROM recovery_proof.checkpoint ORDER BY id"))
                     .mappings().all(), default=dict))
engine.dispose()
"""
LOSE_CHECKPOINT = """
import pathlib
from sqlalchemy import create_engine, text
from trade_data.config import get_settings
engine = create_engine(get_settings().database_url)
with engine.begin() as c:
    c.execute(text("DROP SCHEMA recovery_proof CASCADE"))
for root in ('/app/data', '/app/artifacts'):
    (pathlib.Path(root) / 'recovery-proof' / 'checkpoint.bin').unlink()
engine.dispose()
"""


def storage_identity(mounts: dict) -> dict:
    return {
        service: [
            {key: mount.get(key) for key in ("Type", "Name", "Source", "Destination", "RW")}
            for mount in entries
        ]
        for service, entries in mounts.items()
    }


def verify(root: Path, report: Path, *, external: bool = False) -> bool:
    token = uuid4().hex[:12]
    projects = ["trade-recovery-source-" + token, "trade-recovery-target-" + token]
    infrastructure_project = "trade-recovery-services-" + token
    passwords = [secrets.token_hex(24) + ":@%/", secrets.token_hex(24) + ":@%/"]
    report = report.resolve()
    report.parent.mkdir(parents=True, exist_ok=True)
    log = report.with_suffix(".log")
    log.write_text("")
    evidence = {
        "status": "failed",
        "started_at": datetime.now(UTC).isoformat(),
        "projects": projects,
        "topology": "external" if external else "local",
        "checks": {},
    }
    scratch = root / "tmp" / "core-recovery"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="recovery-" + token + "-", dir=scratch) as temporary:
        workspace = Path(temporary)
        verifiers = [Verifier(workspace, project, log) for project in projects]
        cleanup = []
        try:
            print("Building isolated core recovery source", flush=True)
            evidence["source_sha256"] = copy_source(root, workspace)
            infrastructure = None
            if external:
                infrastructure = Verifier(workspace, infrastructure_project, log)
                infra_config = fixture_config(infrastructure_project, *passwords)
                infra_config["services"]["target_database"] = json.loads(
                    json.dumps(infra_config["services"]["database"])
                )
                target_db = infra_config["services"]["target_database"]
                target_db["networks"]["default"]["aliases"] = ["target-database.service"]
                target_db["volumes"][0]["source"] = "target_database_data"
                infra_config["volumes"]["target_database_data"] = {
                    "name": infrastructure_project + "_target_database_data"
                }
                infra_path = workspace / "external-infrastructure.json"
                private_json(infra_path, infra_config)
                infrastructure.base[-1] = str(infra_path)
                cleanup.append(infrastructure)
                infrastructure.run(["up", "-d", "--wait", "--wait-timeout", "150"])
                evidence["infrastructure_project"] = infrastructure_project
            files = []
            for index, verifier in enumerate(verifiers):
                config = isolated_config(
                    json.loads(verifier.run(["config", "--format", "json"])), verifier.project
                )
                if external:
                    for service in ("postgres", "redis"):
                        del config["services"][service]
                    config["volumes"].pop("postgres_data", None)
                    api = config["services"]["api"]
                    api.pop("depends_on", None)
                    hostname = "database.service" if index == 0 else "target-database.service"
                    api["environment"]["DATABASE_URL"] = (
                        f"postgresql+psycopg://external_user:{quote(passwords[0], safe='')}"
                        f"@{hostname}:5432/external_trade"
                    )
                    api["environment"]["REDIS_URL"] = (
                        f"redis://:{quote(passwords[1], safe='')}@cache.service:6379/0"
                    )
                    api["networks"] = {"default": None, "infrastructure": None}
                    config["networks"]["infrastructure"] = {
                        "external": True,
                        "name": infrastructure_project + "_services",
                    }
                path = workspace / (verifier.project + ".json")
                private_json(path, config)
                verifier.base[-1] = str(path)
                files.append(path)
            source, target = verifiers
            cleanup.append(source)
            source.run(["build", "api"], timeout=900)
            source.run(["up", "-d", "--no-build", "--wait", "--wait-timeout", "150"])
            source.run(
                ["exec", "-T", "api", "python", "-m", "alembic", "upgrade", PREVIOUS_REVISION]
            )
            source.run(["exec", "-T", "api", "python", "-c", CHECKPOINT])
            source_tool, target_tool = [
                CoreRecovery(workspace, project, workspace / ".env", [file])
                for project, file in zip(projects, files, strict=True)
            ]
            source_checkpoint = json.loads(source_tool.python(READ_CHECKPOINT))
            bundle = workspace / "checkpoint-backup"
            print("Backing up previous schema and operational file/DB markers", flush=True)
            manifest = source_tool.backup(bundle, quiesced=True)
            evidence["checks"]["backup"] = {
                "format_version": manifest["format_version"],
                "revision": manifest["database"]["revision"],
                "payload_sha256": {
                    key: value["sha256"] for key, value in manifest["files"].items()
                },
            }
            source_tool.python(LOSE_CHECKPOINT)
            if source_tool.runtime()["state"] == manifest["database"]:
                raise RuntimeError("Recovery drill failed to simulate checkpoint loss")
            evidence["checks"]["source_loss_simulated"] = True
            source_image = (
                source_tool.run(
                    ["docker", "inspect", "--format", "{{.Image}}", source_tool.api_id()]
                )
                .decode()
                .strip()
            )
            source_tool.run(["docker", "tag", source_image, projects[1] + "-api:latest"])
            cleanup.append(target)
            print("Restoring into fresh database and fresh data/artifact mounts", flush=True)
            target.run(["up", "-d", "--no-build", "--wait", "--wait-timeout", "150"])
            evidence["checks"]["target_before_restore"] = target_tool.runtime()["state"]
            evidence["checks"]["restore"] = target_tool.restore(bundle, quiesced=True)
            if json.loads(target_tool.python(READ_CHECKPOINT)) != source_checkpoint:
                raise RuntimeError("Restored operational row values differ from checkpoint")
            evidence["checks"]["operational_row_values_verified"] = True
            mounts = target_tool.inventory()["mounts"]
            filesystem = json.loads(target_tool.python(FILESYSTEM))
            print("Upgrading restored previous schema and checking idempotence", flush=True)
            target.run(["exec", "-T", "api", "python", "-m", "alembic", "upgrade", "head"])
            upgraded = target_tool.runtime()["state"]
            expected_head = json.loads(
                target.run(["exec", "-T", "api", "python", "-c", EXPECTED_HEAD])
            )
            if upgraded["revision"] != expected_head:
                raise RuntimeError("Recovered previous schema did not upgrade to current head")
            target.run(["exec", "-T", "api", "python", "-m", "alembic", "upgrade", "head"])
            if target_tool.runtime()["state"] != upgraded:
                raise RuntimeError("Repeat recovered-schema upgrade changed state")
            for key, count in manifest["database"]["row_counts"].items():
                if upgraded["row_counts"].get(key) != count:
                    raise RuntimeError("Previous-schema upgrade changed recovered row counts")
            evidence["checks"]["upgrade"] = {
                "from": PREVIOUS_REVISION,
                "to": expected_head,
                "recovered_row_counts_preserved": True,
                "repeat_upgrade_unchanged": True,
            }
            print("Recreating containers and verifying named-volume persistence", flush=True)
            target.run(
                ["up", "-d", "--no-build", "--force-recreate", "--wait", "--wait-timeout", "150"]
            )
            if infrastructure is not None:
                infrastructure.run(
                    ["up", "-d", "--force-recreate", "--wait", "--wait-timeout", "150"]
                )
            if target_tool.runtime()["state"] != upgraded:
                raise RuntimeError("Container recreation changed restored database contents")
            after_mounts = target_tool.inventory()["mounts"]
            if storage_identity(after_mounts) != storage_identity(mounts):
                evidence["checks"]["mounts_before"] = mounts
                evidence["checks"]["mounts_after"] = after_mounts
                raise RuntimeError("Container recreation changed storage ownership/mounts")
            if json.loads(target_tool.python(FILESYSTEM)) != filesystem:
                raise RuntimeError("Container recreation changed restored file bytes")
            if json.loads(target_tool.python(READ_CHECKPOINT)) != source_checkpoint:
                raise RuntimeError("Upgrade/recreation changed operational marker values")
            evidence["checks"]["volume_retention"] = True
            print("Round-tripping the current schema into fresh owned source storage", flush=True)
            current_bundle = workspace / "current-schema-backup"
            current_manifest = target_tool.backup(current_bundle, quiesced=True)
            source.run(["down", "--volumes"])
            if infrastructure is not None:
                for statement in ("DROP DATABASE external_trade", "CREATE DATABASE external_trade"):
                    infrastructure.run(
                        [
                            "exec",
                            "-T",
                            "database",
                            "psql",
                            "-U",
                            "external_user",
                            "-d",
                            "postgres",
                            "-v",
                            "ON_ERROR_STOP=1",
                            "-c",
                            statement,
                        ]
                    )
            source.run(["up", "-d", "--no-build", "--wait", "--wait-timeout", "150"])
            current_restore = source_tool.restore(current_bundle, quiesced=True)
            if current_restore["database"]["revision"] != expected_head:
                raise RuntimeError("Current-schema checkpoint did not restore its revision")
            if json.loads(source_tool.python(READ_CHECKPOINT)) != source_checkpoint:
                raise RuntimeError("Current-schema restore changed operational row values")
            if json.loads(source_tool.python(FILESYSTEM)) != filesystem:
                raise RuntimeError("Current-schema restore changed file bytes")
            evidence["checks"]["current_schema_restore"] = {
                "revision": current_manifest["database"]["revision"],
                "all_user_table_counts_and_objects_verified": True,
                "operational_row_values_and_file_bytes_verified": True,
            }
            evidence["checks"]["doctor"] = json.loads(
                target.run(["exec", "-T", "api", "trade-data", "doctor", "--check-services"])
            )
            if not evidence["checks"]["doctor"]["ready"]:
                raise RuntimeError("Recovered core is not ready")
            evidence["checks"]["api_health"] = target.http("api", 8000, "/api/health")
            evidence["checks"]["api_health"] = target.http("api", 8000, "/api/health")
            market = target.http("api", 8000, "/api/market/status")
            if json.loads(market["body"]) != []:
                raise RuntimeError("Recovery drill must not fabricate market data")
            evidence["checks"]["empty_market_status"] = True
            rows = target.run(["ps", "--format", "json"])
            if {json.loads(line)["Service"] for line in rows.splitlines() if line} != (
                {"api"} if external else CORE
            ):
                raise RuntimeError("Recovery drill started unexpected services")
            evidence["status"] = "passed"
        except Exception as error:
            evidence["error"] = str(error)
            for verifier in cleanup:
                try:
                    verifier.run(["logs", "--no-color", "--tail", "100"])
                except Exception:
                    pass
        finally:
            print("Cleaning only owned recovery projects", flush=True)
            evidence["cleanup"] = {}
            for verifier in reversed(cleanup):
                try:
                    verifier.run(["down", "--volumes", "--rmi", "local"])
                    evidence["cleanup"][verifier.project] = "passed"
                except Exception:
                    evidence["cleanup"][verifier.project] = "failed"
                    evidence["status"] = "failed"
            evidence["finished_at"] = datetime.now(UTC).isoformat()
            serialized = json.dumps(evidence, indent=2)
            log_content = log.read_text()
            for password in passwords + [quote(value, safe="") for value in passwords]:
                serialized = serialized.replace(password, "[redacted]")
                log_content = log_content.replace(password, "[redacted]")
            log.write_text(log_content)
            report.write_text(serialized + "\n")
    print(f"Core recovery: {evidence['status']}; report: {report}", flush=True)
    return evidence["status"] == "passed"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=Path("output/core-recovery/report.json"))
    parser.add_argument(
        "--external", action="store_true", help="Use separately owned authenticated infrastructure"
    )
    args = parser.parse_args()
    raise SystemExit(
        0 if verify(Path(__file__).resolve().parents[1], args.report, external=args.external) else 1
    )


if __name__ == "__main__":
    main()
