from __future__ import annotations

import json
import os
import subprocess
import tarfile
from hashlib import sha256
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
RESTORE_SCRIPT = REPOSITORY_ROOT / "deploy" / "restore-drill.sh"


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _create_archive(backup_dir: Path, source: Path) -> None:
    with tarfile.open(backup_dir / f"{source.name}.tgz", "w:gz") as stream:
        stream.add(source, arcname=source.name)


def _create_backup(tmp_path: Path) -> Path:
    payload_root = tmp_path / "payload"
    data_dir = payload_root / "data"
    manifest_dir = data_dir / "filings" / "nse" / "INFY"
    documents_dir = manifest_dir / "documents"
    documents_dir.mkdir(parents=True)
    document = documents_dir / "filing.xml"
    document.write_text("<xbrl>restored</xbrl>", encoding="utf-8")
    manifest = {
        "candidate_count": 2,
        "document_count": 1,
        "failed_download_count": 1,
        "documents": [
            {
                "relative_path": "documents/filing.xml",
                "bytes": document.stat().st_size,
                "sha256": sha256(document.read_bytes()).hexdigest(),
                "acquisition_status": "downloaded",
                "error": None,
            },
            {
                "relative_path": "documents/missing.pdf",
                "sha256": None,
                "acquisition_status": "failed",
                "error": "curl exit code 22",
            },
        ]
    }
    (manifest_dir / "manifest.json").write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    for name in ("minio", "qdrant", "artifacts", "dagster_home", "cloudbeaver"):
        directory = payload_root / name
        directory.mkdir()
        (directory / "state").write_text(name, encoding="utf-8")

    backup_dir = tmp_path / "backup"
    backup_dir.mkdir()
    (backup_dir / "postgres.dump").write_bytes(b"custom-format-dump")
    (backup_dir / "README.txt").write_text("restore fixture\n", encoding="utf-8")
    for directory in payload_root.iterdir():
        _create_archive(backup_dir, directory)
    checksum_names = ["postgres.dump", *sorted(path.name for path in backup_dir.glob("*.tgz"))]
    (backup_dir / "SHA256SUMS").write_text(
        "".join(f"{'0' * 64}  {name}\n" for name in checksum_names),
        encoding="utf-8",
    )
    return backup_dir


def _create_fake_tools(tmp_path: Path) -> tuple[Path, Path]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    docker_log = tmp_path / "docker.log"
    _write_executable(
        fake_bin / "sha256sum",
        """#!/usr/bin/env bash
if [[ "${FAKE_CHECKSUM_FAIL:-false}" == "true" ]]; then
  printf '%s\n' 'postgres.dump: FAILED' >&2
  exit 1
fi
if [[ "${1:-}" == "-c" ]]; then
  while read -r _hash filename; do
    printf '%s: OK\n' "$filename"
  done < "${2:-SHA256SUMS}"
  exit 0
fi
exit 2
""",
    )
    _write_executable(
        fake_bin / "docker",
        """#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_DOCKER_LOG"
call="$*"
if [[ "$call" == *"pg_restore"* && "${FAKE_PG_RESTORE_FAIL:-false}" == "true" ]]; then
  exit 9
fi
if [[ "$call" == *"network rm"* && "${FAKE_NETWORK_RM_FAIL:-false}" == "true" ]]; then
  exit 7
fi
if [[ "$call" == *"--network none --user 0:0"* && "${FAKE_CLEANUP_FAIL:-false}" == "true" ]]; then
  exit 8
fi
if [[ "$call" == *"SELECT extversion FROM pg_extension"* ]]; then
  printf '%s\n' '2.17.2'
elif [[ "$call" == *"SELECT version_num FROM alembic_version"* ]]; then
  printf '%s\n' '20260726_0011'
elif [[ "$call" == *"COUNT(*) FROM filing_documents"* ]]; then
  printf '%s\n' '123'
elif [[ "$call" == *"COUNT(*) FROM filing_approved_facts"* ]]; then
  printf '%s\n' '1186'
elif [[ "$call" == *"alembic heads"* ]]; then
  printf '%s\n' '20260726_0011 (head)'
elif [[ "$call" == *"trade_research.filings.restore_validation"* ]]; then
  printf '%s\n' '{"versioning_status":"Enabled","object_count":26}'
elif [[ "$call" == *"evaluate-filing-golden"* ]]; then
  cat <<'JSON'
{
  "dataset_id": "infy-m1-13q-core-v1",
  "case_count": 13,
  "expected_fact_count": 52,
  "matched_fact_count": 52,
  "value_correct_count": 52,
  "evidence_correct_count": 52,
  "passed": true,
  "defects": []
}
JSON
elif [[ "$call" == *"network create"* || "$call" == *"run -d"* ]]; then
  printf '%s\n' 'fake-container-id'
fi
exit 0
""",
    )
    return fake_bin, docker_log


def _environment(
    tmp_path: Path,
    fake_bin: Path,
    docker_log: Path,
) -> tuple[dict[str, str], Path, Path]:
    env_file = tmp_path / "production.env"
    env_file.write_text(
        "\n".join(
            (
                "PROD_API_IMAGE=trade-research-api:test",
                "PROD_MINIO_ROOT_USER=restore-root",
                "PROD_MINIO_ROOT_PASSWORD=restore-root-password",
                "PROD_FILING_S3_ACCESS_KEY_ID=restore-app",
                "PROD_FILING_S3_SECRET_ACCESS_KEY=restore-app-password",
                "PROD_FILING_S3_BUCKET=lens-filings",
                "PROD_FILING_S3_PREFIX=parsed",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    restore_root = tmp_path / "restore-drills"
    report_root = tmp_path / "restore-reports"
    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{fake_bin}:{environment['PATH']}",
            "TRADE_APP_DIR": str(REPOSITORY_ROOT),
            "TRADE_ENV_FILE": str(env_file),
            "TRADE_RESTORE_ROOT": str(restore_root),
            "TRADE_RESTORE_REPORT_DIR": str(report_root),
            "TRADE_RESTORE_MIN_SOURCE_DOCUMENTS": "1",
            "FAKE_DOCKER_LOG": str(docker_log),
        }
    )
    return environment, restore_root, report_root


def test_restore_drill_is_isolated_and_emits_passing_report(tmp_path: Path) -> None:
    backup_dir = _create_backup(tmp_path)
    fake_bin, docker_log = _create_fake_tools(tmp_path)
    environment, restore_root, report_root = _environment(
        tmp_path,
        fake_bin,
        docker_log,
    )

    completed = subprocess.run(
        ["bash", str(RESTORE_SCRIPT), str(backup_dir)],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert completed.returncode == 0, completed.stderr
    reports = list(report_root.glob("*.json"))
    assert len(reports) == 1
    report = json.loads(reports[0].read_text(encoding="utf-8"))
    assert report["status"] == "passed"
    assert report["stage"] == "completed"
    assert report["integrity"]["checksums_verified"] is True
    assert report["source_manifest"] == {
        "document_count": 1,
        "verified_document_count": 1,
        "failed_skipped_count": 1,
    }
    assert report["postgresql"]["migration_revision"] == "20260726_0011"
    assert report["postgresql"]["timescaledb_version"] == "2.17.2"
    assert report["postgresql"]["approved_fact_count"] == 1186
    assert report["object_store"] == {
        "object_count": 26,
        "versioning_verified": True,
    }
    assert report["qdrant"]["reachable"] is True
    assert report["golden_evaluation"]["passed"] is True
    assert report["isolation"]["host_ports_published"] is False
    assert report["isolation"]["production_compose_used"] is False
    assert not list(restore_root.iterdir())

    docker_calls = docker_log.read_text(encoding="utf-8")
    assert "docker compose" not in docker_calls
    assert "--publish" not in docker_calls
    assert " -p " not in docker_calls
    assert "/opt/trade/data" not in docker_calls
    assert "timescaledb_pre_restore()" in docker_calls
    assert "timescaledb_post_restore()" in docker_calls
    assert "network rm trade-restore-" in docker_calls
    assert docker_calls.count("rm -f trade-restore-") == 3
    assert "--network none --user 0:0" in docker_calls
    assert "--entrypoint python" in docker_calls
    assert ":/restore-work" in docker_calls


def test_checksum_failure_never_starts_restore_containers(tmp_path: Path) -> None:
    backup_dir = _create_backup(tmp_path)
    fake_bin, docker_log = _create_fake_tools(tmp_path)
    environment, restore_root, report_root = _environment(
        tmp_path,
        fake_bin,
        docker_log,
    )
    environment["FAKE_CHECKSUM_FAIL"] = "true"

    completed = subprocess.run(
        ["bash", str(RESTORE_SCRIPT), str(backup_dir)],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert completed.returncode == 1
    assert not docker_log.exists()
    report_path = next(report_root.glob("*.json"))
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["stage"] == "checksum_verification"
    assert report["integrity"]["checksums_verified"] is False
    assert report["isolation"]["work_dir_retained"] is True
    assert Path(report["isolation"]["work_dir"]).is_dir()
    assert len(list(restore_root.iterdir())) == 1


def test_failed_database_restore_cleans_containers_and_retains_evidence(
    tmp_path: Path,
) -> None:
    backup_dir = _create_backup(tmp_path)
    fake_bin, docker_log = _create_fake_tools(tmp_path)
    environment, _restore_root, report_root = _environment(
        tmp_path,
        fake_bin,
        docker_log,
    )
    environment["FAKE_PG_RESTORE_FAIL"] = "true"

    completed = subprocess.run(
        ["bash", str(RESTORE_SCRIPT), str(backup_dir)],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert completed.returncode == 9
    report_path = next(report_root.glob("*.json"))
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["stage"] == "postgresql_restore"
    assert report["exit_code"] == 9
    assert report["isolation"]["work_dir_retained"] is True
    assert Path(report["isolation"]["work_dir"]).is_dir()

    docker_calls = docker_log.read_text(encoding="utf-8")
    assert "rm -f trade-restore-" in docker_calls
    assert "network rm trade-restore-" in docker_calls


def test_restore_drill_rejects_incomplete_backup(tmp_path: Path) -> None:
    incomplete = tmp_path / ".incomplete-20260725T070005Z"
    incomplete.mkdir()
    fake_bin, docker_log = _create_fake_tools(tmp_path)
    environment, _restore_root, _report_root = _environment(
        tmp_path,
        fake_bin,
        docker_log,
    )

    completed = subprocess.run(
        ["bash", str(RESTORE_SCRIPT), str(incomplete)],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert completed.returncode == 1
    assert "refusing to restore an incomplete backup" in completed.stderr
    assert not docker_log.exists()


def test_cleanup_failure_changes_passing_drill_to_failure(tmp_path: Path) -> None:
    backup_dir = _create_backup(tmp_path)
    fake_bin, docker_log = _create_fake_tools(tmp_path)
    environment, _restore_root, report_root = _environment(
        tmp_path,
        fake_bin,
        docker_log,
    )
    environment["FAKE_NETWORK_RM_FAIL"] = "true"

    completed = subprocess.run(
        ["bash", str(RESTORE_SCRIPT), str(backup_dir)],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert completed.returncode == 1
    report_path = next(report_root.glob("*.json"))
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["stage"] == "cleanup"
    assert report["isolation"]["work_dir_retained"] is True
    assert Path(report["isolation"]["work_dir"]).is_dir()


def test_privileged_restore_data_cleanup_failure_is_reported(tmp_path: Path) -> None:
    backup_dir = _create_backup(tmp_path)
    fake_bin, docker_log = _create_fake_tools(tmp_path)
    environment, _restore_root, report_root = _environment(
        tmp_path,
        fake_bin,
        docker_log,
    )
    environment["FAKE_CLEANUP_FAIL"] = "true"

    completed = subprocess.run(
        ["bash", str(RESTORE_SCRIPT), str(backup_dir)],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert completed.returncode == 1
    report_path = next(report_root.glob("*.json"))
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["stage"] == "cleanup"
    assert report["isolation"]["work_dir_retained"] is True
    assert Path(report["isolation"]["work_dir"]).is_dir()
    assert "failed to remove restore data" in completed.stderr


def test_restore_drill_is_documented_and_has_configured_isolated_paths() -> None:
    script = RESTORE_SCRIPT.read_text(encoding="utf-8")
    example = (REPOSITORY_ROOT / ".env.prod.example").read_text(encoding="utf-8")
    acceptance = (
        REPOSITORY_ROOT / "docs" / "lens_m1_production_acceptance.md"
    ).read_text(encoding="utf-8")

    assert "PROD_RESTORE_ROOT=/opt/trade/restore-drills" in example
    assert "PROD_RESTORE_REPORT_DIR=/opt/trade/restore-reports" in example
    assert "deploy/restore-drill.sh /opt/trade/backups/<timestamp>" in acceptance
    assert "timescaledb_pre_restore()" in acceptance
    assert "TRADE_RESTORE_KEEP=true" in acceptance
    assert "account for one failed download" in acceptance
    assert "trade_research.filings.restore_validation" in script
    assert "mc find" not in script
    assert "--print" not in script
    assert "| grep" not in script
