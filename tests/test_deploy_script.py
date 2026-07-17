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
    env.pop("TRADE_DEPLOY_REEXECUTED", None)
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

    docker_call_lines = docker_calls.splitlines()
    build_index = _call_index(docker_call_lines, " build")
    postgres_start_index = _call_index(docker_call_lines, "up -d postgres")
    postgres_ready_index = _call_index(
        docker_call_lines,
        "exec -T postgres pg_isready",
    )
    migration_index = _call_index(
        docker_call_lines,
        "run --rm --no-deps api alembic -c /app/alembic.ini upgrade head",
    )
    application_start_index = _call_index(
        docker_call_lines,
        "up -d --remove-orphans",
    )
    assert (
        build_index
        < postgres_start_index
        < postgres_ready_index
        < migration_index
        < application_start_index
    )


def test_deploy_aborts_before_application_replacement_when_migration_fails(
    tmp_path: Path,
) -> None:
    repository_root = Path(__file__).resolve().parents[1]
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    docker_log = tmp_path / "docker.log"

    _write_executable(fake_bin / "git", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(
        fake_bin / "docker",
        """#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_DOCKER_LOG"
if [[ "$*" == *"run --rm --no-deps api alembic"* ]]; then
  exit 42
fi
exit 0
""",
    )
    _write_executable(fake_bin / "curl", "#!/usr/bin/env bash\nexit 0\n")

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
        }
    )

    completed = subprocess.run(
        ["bash", str(repository_root / "deploy/deploy.sh")],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert completed.returncode == 42
    docker_calls = docker_log.read_text(encoding="utf-8")
    assert "run --rm --no-deps api alembic" in docker_calls
    assert "up -d --remove-orphans" not in docker_calls


def test_deploy_reexecutes_synchronized_script_after_revision_change(
    tmp_path: Path,
) -> None:
    repository_root = Path(__file__).resolve().parents[1]
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    revision_state = tmp_path / "revision"
    docker_log = tmp_path / "docker.log"

    _write_executable(
        fake_bin / "git",
        """#!/usr/bin/env bash
if [[ "$1" == "rev-parse" && "$2" == "HEAD" ]]; then
  if [[ -f "$FAKE_REVISION_STATE" ]]; then
    printf '%s\n' 'new-revision'
  else
    printf '%s\n' 'old-revision'
  fi
elif [[ "$1" == "pull" ]]; then
  printf '%s\n' 'new-revision' > "$FAKE_REVISION_STATE"
fi
exit 0
""",
    )
    _write_executable(
        fake_bin / "docker",
        """#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_DOCKER_LOG"
exit 0
""",
    )
    _write_executable(fake_bin / "curl", "#!/usr/bin/env bash\nexit 0\n")

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
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env = os.environ.copy()
    env.pop("TRADE_DEPLOY_REEXECUTED", None)
    env.update(
        {
            "PATH": f"{fake_bin}:{env['PATH']}",
            "TRADE_APP_DIR": str(repository_root),
            "TRADE_ENV_FILE": str(env_file),
            "FAKE_REVISION_STATE": str(revision_state),
            "FAKE_DOCKER_LOG": str(docker_log),
        }
    )

    completed = subprocess.run(
        ["bash", str(repository_root / "deploy/deploy.sh")],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    assert completed.stdout.count(
        "restarting deployment with synchronized script at new-revision"
    ) == 1
    docker_calls = docker_log.read_text(encoding="utf-8")
    assert docker_calls.count(" build\n") == 1
    assert docker_calls.count("up -d --remove-orphans") == 1


def test_deploy_workflow_syncs_main_before_invoking_deploy_script() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    workflow = (repository_root / ".github/workflows/deploy.yml").read_text(
        encoding="utf-8"
    )

    fetch = workflow.index("git fetch origin main")
    pull = workflow.index("git pull --ff-only origin main")
    deploy = workflow.index("./deploy/deploy.sh")
    assert fetch < pull < deploy


def test_api_image_contains_alembic_runtime_files() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    dockerfile = (repository_root / "Dockerfile.api").read_text(encoding="utf-8")

    config_copy = dockerfile.index("COPY alembic.ini ./")
    migrations_copy = dockerfile.index("COPY migrations ./migrations")
    install = dockerfile.index("RUN pip install -e .")
    assert config_copy < install
    assert migrations_copy < install


def _call_index(calls: list[str], fragment: str) -> int:
    return next(index for index, call in enumerate(calls) if fragment in call)


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)
