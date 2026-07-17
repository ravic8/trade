from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("dagster_running", [False, True])
def test_deploy_refreshes_only_an_already_running_dagster_webserver(
    tmp_path: Path,
    dagster_running: bool,
) -> None:
    repository_root = Path(__file__).resolve().parents[1]
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    docker_log = tmp_path / "docker.log"
    curl_log = tmp_path / "curl.log"

    _write_executable(
        fake_bin / "git",
        "#!/usr/bin/env bash\nexit 0\n",
    )
    _write_executable(
        fake_bin / "docker",
        """#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_DOCKER_LOG"
if [[ "$*" == *"ps --status running -q dagster-webserver"* ]] && \
   [[ "$FAKE_DAGSTER_RUNNING" == "true" ]]; then
  printf '%s\n' 'fake-dagster-container-id'
fi
exit 0
""",
    )
    _write_executable(
        fake_bin / "curl",
        """#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_CURL_LOG"
exit 0
""",
    )

    env_file = tmp_path / "production.env"
    env_file.write_text(
        "\n".join(
            [
                f"PROD_TRADE_DATA_DIR={tmp_path / 'data'}",
                f"PROD_TRADE_ARTIFACTS_DIR={tmp_path / 'artifacts'}",
                f"PROD_POSTGRES_DATA_DIR={tmp_path / 'postgres'}",
                f"PROD_REDIS_DATA_DIR={tmp_path / 'redis'}",
                f"PROD_QDRANT_DATA_DIR={tmp_path / 'qdrant'}",
                f"PROD_DAGSTER_HOME_DIR={tmp_path / 'dagster-home'}",
                "PROD_WEB_PORT=8081",
                "PROD_DAGSTER_WEB_PORT=3300",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{fake_bin}:{env['PATH']}",
            "TRADE_APP_DIR": str(repository_root),
            "TRADE_ENV_FILE": str(env_file),
            "FAKE_DOCKER_LOG": str(docker_log),
            "FAKE_CURL_LOG": str(curl_log),
            "FAKE_DAGSTER_RUNNING": str(dagster_running).lower(),
        }
    )

    subprocess.run(
        ["bash", str(repository_root / "deploy/deploy.sh")],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    docker_calls = docker_log.read_text(encoding="utf-8")
    curl_calls = curl_log.read_text(encoding="utf-8")
    admin_build = "--profile admin build dagster-webserver"
    admin_recreate = (
        "--profile admin up -d --no-deps --force-recreate dagster-webserver"
    )

    assert (admin_build in docker_calls) is dagster_running
    assert (admin_recreate in docker_calls) is dagster_running
    assert ("http://127.0.0.1:3300" in curl_calls) is dagster_running
    assert "http://localhost:8081/api/health" in curl_calls


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)
