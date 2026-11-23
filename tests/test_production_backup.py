import os
import subprocess
from pathlib import Path


def test_backup_resumes_minio_without_recreating_it(tmp_path: Path) -> None:
    repository = Path(__file__).resolve().parents[1]
    docker = tmp_path / "docker"
    docker.write_text(
        """#!/usr/bin/env bash
printf '%s\n' "$*" >> "$DOCKER_TEST_LOG"
case "$*" in
  *' ps --status running -q minio') echo minio-container ;;
  *' exec -T postgres pg_dump '*) printf 'test dump' ;;
esac
""",
        encoding="utf-8",
    )
    docker.chmod(0o700)
    env_file = tmp_path / "backup.env"
    archived_paths = (
        "TRADE_DATA_DIR",
        "TRADE_ARTIFACTS_DIR",
        "DAGSTER_HOME_DIR",
        "MINIO_DATA_DIR",
        "CLICKHOUSE_DATA_DIR",
        "KES_CONFIG_DIR",
        "KES_CERT_DIR",
        "KES_KEY_DIR",
        "KES_SECRET_DIR",
        "CLOUDBEAVER_WORKSPACE_DIR",
    )
    env_file.write_text(
        "".join(f"PROD_{name}={tmp_path / 'absent'}\n" for name in archived_paths),
        encoding="utf-8",
    )
    call_log = tmp_path / "docker.log"
    subprocess.run(
        ["bash", str(repository / "deploy/backup.sh")],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "TRADE_APP_DIR": str(repository),
            "TRADE_ENV_FILE": str(env_file),
            "TRADE_BACKUP_DIR": str(tmp_path / "backups"),
            "DOCKER_TEST_LOG": str(call_log),
        },
        check=True,
        capture_output=True,
        text=True,
    )
    calls = call_log.read_text(encoding="utf-8").splitlines()
    minio_calls = [
        call.split("docker-compose.prod.yml ", 1)[-1]
        for call in calls
        if call.endswith((" minio", " minio-container"))
    ]
    assert minio_calls == [
        "ps --status running -q minio",
        "stop minio-container",
        "start minio-container",
    ]
