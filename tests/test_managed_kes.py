from __future__ import annotations

import os
import subprocess
from pathlib import Path


def test_managed_kes_prepare_generates_config_and_updates_env(tmp_path: Path) -> None:
    repository_root = Path(__file__).resolve().parents[1]
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker_log = tmp_path / "docker.log"
    fake_docker = bin_dir / "docker"
    fake_docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$FAKE_DOCKER_LOG"
cat <<'OUT'
Your API key:

   kes:v1:test-api-key

Your Identity:

   0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
OUT
""",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    env_file = tmp_path / "production.env"
    env_file.write_text(
        "\n".join(
            [
                "PROD_SELF_MANAGED_KES_ENABLED=true",
                f"PROD_KES_CONFIG_DIR={tmp_path / 'kes/config'}",
                f"PROD_KES_CERT_DIR={tmp_path / 'kes/certs'}",
                f"PROD_KES_KEY_DIR={tmp_path / 'kes/keys'}",
                f"PROD_KES_SECRET_DIR={tmp_path / 'kes/secrets'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "TRADE_APP_DIR": str(repository_root),
            "TRADE_ENV_FILE": str(env_file),
            "FAKE_DOCKER_LOG": str(docker_log),
        }
    )

    subprocess.run(
        ["bash", str(repository_root / "deploy/managed-kes.sh"), "prepare"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    configured = env_file.read_text(encoding="utf-8")
    assert "PROD_MINIO_KMS_ENABLED=true" in configured
    assert "PROD_MINIO_KMS_SERVER=https://kes:7373" in configured
    assert "PROD_MINIO_KMS_API_KEY=kes:v1:test-api-key" in configured
    config = (tmp_path / "kes/config/config.yaml").read_text(encoding="utf-8")
    assert "- /v1/key/generate/*" in config
    assert "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" in config
    assert (tmp_path / "kes/certs/server.key").exists()
    assert (tmp_path / "kes/certs/server.crt").exists()
