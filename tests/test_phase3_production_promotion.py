from __future__ import annotations

import os
import subprocess
from pathlib import Path


def test_status_reports_flags_without_exposing_credentials(tmp_path: Path) -> None:
    repository_root = Path(__file__).resolve().parents[1]
    env_file = tmp_path / "production.env"
    secret = "must-not-appear-in-output"
    env_file.write_text(
        "\n".join(
            [
                "PROD_MINIO_KMS_ENABLED=true",
                "PROD_SELF_MANAGED_KES_ENABLED=false",
                "PROD_RESEARCH_STORAGE_DEPLOY_ENABLED=false",
                "PROD_RESEARCH_STORAGE_ENABLED=false",
                "PROD_PHASE3_MARKET_DATA_ENABLED=false",
                "PROD_YFINANCE_NSE_MINUTE_ENABLED=false",
                "PROD_PHASE3_PRODUCTION_ACTIVATION_ENABLED=false",
                f"PROD_CLICKHOUSE_ADMIN_PASSWORD={secret}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env = os.environ.copy()
    env.update(
        {
            "TRADE_APP_DIR": str(repository_root),
            "TRADE_ENV_FILE": str(env_file),
        }
    )

    completed = subprocess.run(
        ["bash", str(repository_root / "deploy/phase3-production.sh"), "status"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    assert "PROD_RESEARCH_STORAGE_ENABLED=false" in completed.stdout
    assert "PROD_CLICKHOUSE_ADMIN_PASSWORD=ready" in completed.stdout
    assert secret not in completed.stdout


def test_unsupported_operation_fails_closed(tmp_path: Path) -> None:
    repository_root = Path(__file__).resolve().parents[1]
    env_file = tmp_path / "production.env"
    env_file.write_text("PROD_MINIO_KMS_ENABLED=true\n", encoding="utf-8")
    env = os.environ.copy()
    env.update(
        {
            "TRADE_APP_DIR": str(repository_root),
            "TRADE_ENV_FILE": str(env_file),
        }
    )

    completed = subprocess.run(
        ["bash", str(repository_root / "deploy/phase3-production.sh"), "unknown"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert completed.returncode != 0
    assert "unsupported operation" in completed.stderr


def test_readiness_returns_nonzero_when_gate_is_blocked(tmp_path: Path) -> None:
    repository_root = Path(__file__).resolve().parents[1]
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    docker_log = tmp_path / "docker.log"
    docker = fake_bin / "docker"
    docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$FAKE_DOCKER_LOG"
if [[ "$*" == *"trade-research phase3-readiness"* ]]; then
  printf '%s\n' 'Phase 3 production readiness: BLOCKED'
  exit 1
fi
exit 0
""",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    env_file = tmp_path / "production.env"
    env_file.write_text(
        "\n".join(
            [
                "PROD_MINIO_KMS_ENABLED=true",
                "PROD_SELF_MANAGED_KES_ENABLED=true",
                "PROD_RESEARCH_STORAGE_DEPLOY_ENABLED=true",
                "PROD_RESEARCH_STORAGE_ENABLED=true",
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
        ["bash", str(repository_root / "deploy/phase3-production.sh"), "readiness"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert completed.returncode == 1
    assert "Phase 3 production readiness: BLOCKED" in completed.stdout
    assert "readiness remains blocked" in completed.stdout


def test_bootstrap_stages_storage_before_enabling_canary_plane(tmp_path: Path) -> None:
    repository_root = Path(__file__).resolve().parents[1]
    fake_app = tmp_path / "app"
    fake_deploy = fake_app / "deploy"
    fake_deploy.mkdir(parents=True)
    deployment_log = tmp_path / "deployments.log"
    deploy_script = fake_deploy / "deploy.sh"
    deploy_script.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
source "$TRADE_ENV_FILE"
printf '%s,%s,%s,%s,%s\n' \
  "$PROD_RESEARCH_STORAGE_DEPLOY_ENABLED" \
  "$PROD_RESEARCH_STORAGE_ENABLED" \
  "$PROD_PHASE3_MARKET_DATA_ENABLED" \
  "$PROD_YFINANCE_NSE_MINUTE_ENABLED" \
  "$PROD_PHASE3_PRODUCTION_ACTIVATION_ENABLED" >> "$FAKE_DEPLOYMENT_LOG"
""",
        encoding="utf-8",
    )
    deploy_script.chmod(0o755)
    env_file = tmp_path / "production.env"
    env_file.write_text(
        "\n".join(
            [
                "PROD_MINIO_KMS_ENABLED=true",
                "PROD_SELF_MANAGED_KES_ENABLED=false",
                "PROD_MINIO_KMS_SERVER=https://kms.internal:7373",
                "PROD_MINIO_KMS_ENCLAVE=trade-production",
                "PROD_MINIO_KMS_API_KEY=secure-kms-api-key",
                "PROD_MINIO_KMS_SSE_KEY=trade-research-sse",
                f"PROD_DEPLOY_STATE_DIR={tmp_path / 'deploy-state'}",
                "PROD_RESEARCH_STORAGE_DEPLOY_ENABLED=false",
                "PROD_RESEARCH_STORAGE_ENABLED=false",
                "PROD_PHASE3_MARKET_DATA_ENABLED=false",
                "PROD_YFINANCE_NSE_MINUTE_ENABLED=false",
                "PROD_PHASE3_PRODUCTION_ACTIVATION_ENABLED=false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env = os.environ.copy()
    env.update(
        {
            "TRADE_APP_DIR": str(fake_app),
            "TRADE_ENV_FILE": str(env_file),
            "FAKE_DEPLOYMENT_LOG": str(deployment_log),
        }
    )

    subprocess.run(
        [
            "bash",
            str(repository_root / "deploy/phase3-production.sh"),
            "bootstrap-canary",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    assert deployment_log.read_text(encoding="utf-8").splitlines() == [
        "true,false,false,false,false",
        "true,true,true,true,false",
    ]
    configured = env_file.read_text(encoding="utf-8")
    assert "PROD_RESEARCH_STORAGE_ENABLED=true" in configured
    assert "PROD_PHASE3_MARKET_DATA_ENABLED=true" in configured
    assert "PROD_YFINANCE_NSE_MINUTE_ENABLED=true" in configured
    assert "PROD_PHASE3_PRODUCTION_ACTIVATION_ENABLED=false" in configured
    assert len(list((tmp_path / "deploy-state/phase3-env-backups").iterdir())) == 1


def test_bootstrap_enables_managed_kes_when_external_kms_is_absent(
    tmp_path: Path,
) -> None:
    repository_root = Path(__file__).resolve().parents[1]
    fake_app = tmp_path / "app"
    fake_deploy = fake_app / "deploy"
    fake_deploy.mkdir(parents=True)
    deployment_log = tmp_path / "deployments.log"
    deploy_script = fake_deploy / "deploy.sh"
    deploy_script.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
source "$TRADE_ENV_FILE"
printf '%s,%s,%s,%s,%s,%s\n' \
  "$PROD_SELF_MANAGED_KES_ENABLED" \
  "$PROD_MINIO_KMS_ENABLED" \
  "$PROD_RESEARCH_STORAGE_DEPLOY_ENABLED" \
  "$PROD_RESEARCH_STORAGE_ENABLED" \
  "$PROD_PHASE3_MARKET_DATA_ENABLED" \
  "$PROD_PHASE3_PRODUCTION_ACTIVATION_ENABLED" >> "$FAKE_DEPLOYMENT_LOG"
""",
        encoding="utf-8",
    )
    deploy_script.chmod(0o755)
    env_file = tmp_path / "production.env"
    env_file.write_text(
        "\n".join(
            [
                f"PROD_DEPLOY_STATE_DIR={tmp_path / 'deploy-state'}",
                "PROD_MINIO_KMS_ENABLED=false",
                "PROD_RESEARCH_STORAGE_DEPLOY_ENABLED=false",
                "PROD_RESEARCH_STORAGE_ENABLED=false",
                "PROD_PHASE3_MARKET_DATA_ENABLED=false",
                "PROD_YFINANCE_NSE_MINUTE_ENABLED=false",
                "PROD_PHASE3_PRODUCTION_ACTIVATION_ENABLED=false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env = os.environ.copy()
    env.update(
        {
            "TRADE_APP_DIR": str(fake_app),
            "TRADE_ENV_FILE": str(env_file),
            "FAKE_DEPLOYMENT_LOG": str(deployment_log),
        }
    )

    subprocess.run(
        [
            "bash",
            str(repository_root / "deploy/phase3-production.sh"),
            "bootstrap-canary",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    assert deployment_log.read_text(encoding="utf-8").splitlines() == [
        "true,true,true,false,false,false",
        "true,true,true,true,true,false",
    ]
    configured = env_file.read_text(encoding="utf-8")
    assert "PROD_SELF_MANAGED_KES_ENABLED=true" in configured
    assert "PROD_MINIO_KMS_ENABLED=true" in configured
    assert "PROD_MINIO_KMS_SERVER=https://kes:7373" in configured
    assert "PROD_MINIO_KMS_ENCLAVE=trade-production" in configured
    assert "PROD_MINIO_KMS_SSE_KEY=trade-research-sse" in configured
