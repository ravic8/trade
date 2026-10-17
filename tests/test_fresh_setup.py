"""Isolation and failure handling for the real fresh-install acceptance runner."""

import copy
import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/verify_fresh_setup.py"
spec = importlib.util.spec_from_file_location("fresh_setup", SCRIPT)
assert spec and spec.loader
bootstrap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bootstrap)


def core_config():
    return {
        "services": {
            "api": {
                "ports": [{"target": 8000, "published": "8000"}],
                "environment": {
                    "DATABASE_URL": "postgresql+psycopg://trade:trade@postgres:5432/trade_research",
                    "REDIS_URL": "redis://redis:6379/0",
                    "CHAT_ENABLED": "false",
                    "CHAT_RATE_LIMIT_ENABLED": "true",
                    "LEGACY_UPSTOX_NSE_ENABLED": "true",
                },
                "volumes": [{"type": "bind", "source": "/operator/data"}],
            },
            "postgres": {},
            "redis": {},
        },
        "volumes": {"postgres_data": {"name": "test_postgres_data"}},
        "networks": {"default": {"name": "test_default"}},
    }


def test_source_snapshot_excludes_operator_state_and_nested_environment(tmp_path):
    root, target = tmp_path / "repo", tmp_path / "snapshot"
    root.mkdir()
    for name in bootstrap.ROOT_FILES:
        (root / name).write_text("sample")
    (root / ".env").write_text("operator-secret")
    for name in ("apps/web/node_modules/a", "apps/web/.env.local", "src/__pycache__/a.pyc"):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("operator-secret")
    source = root / "src/trade_data/data/provider.py"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("pass")
    digest = bootstrap.copy_source(root, target)
    assert (target / ".env").read_text() == "sample"
    assert (target / "src/trade_data/data/provider.py").read_text() == "pass"
    assert not (target / "apps/web/.env.local").exists()
    assert not (target / "apps/web/node_modules").exists()
    assert len(digest) == 64
    source.write_text("changed")
    assert bootstrap.copy_source(root, tmp_path / "second") != digest


def test_environment_cannot_inherit_application_overrides(monkeypatch):
    for key in ("COMPOSE_PROFILES", "COMPOSE_DATABASE_URL", "UPSTOX_ACCESS_TOKEN", "API_PORT"):
        monkeypatch.setenv(key, "operator-secret")
    monkeypatch.setenv("DOCKER_HOST", "unix:///docker.sock")
    environment = bootstrap.docker_environment()
    assert environment["DOCKER_HOST"] == "unix:///docker.sock"
    assert "operator-secret" not in environment.values()


def test_isolation_uses_ephemeral_loopback_ports_and_owned_volumes():
    config = bootstrap.isolated_config(core_config(), "test")
    api = config["services"]["api"]
    assert api["ports"] == [{"target": 8000, "published": "0", "host_ip": "127.0.0.1"}]
    assert all(volume["type"] == "volume" for volume in api["volumes"])
    assert config["volumes"]["bootstrap_data"]["name"] == "test_bootstrap_data"


@pytest.mark.parametrize(
    "mutation", ["service", "endpoint", "enabled", "credential", "external", "volume"]
)
def test_unsafe_configuration_fails_before_start(mutation):
    config = copy.deepcopy(core_config())
    if mutation == "service":
        config["services"]["worker"] = {}
    elif mutation == "endpoint":
        config["services"]["api"]["environment"]["DATABASE_URL"] = "operator-secret"
    elif mutation == "enabled":
        config["services"]["api"]["environment"]["YFINANCE_DAILY_ENABLED"] = "true"
    elif mutation == "credential":
        config["services"]["api"]["environment"]["UPSTOX_ACCESS_TOKEN"] = "operator-secret"
    elif mutation == "external":
        config["volumes"]["postgres_data"]["external"] = True
    else:
        config["volumes"]["postgres_data"]["name"] = "operator_data"
    with pytest.raises(RuntimeError) as error:
        bootstrap.isolated_config(config, "test")
    assert "operator-secret" not in str(error.value)


def test_failed_build_still_cleans_owned_project_and_writes_report(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(bootstrap, "copy_source", lambda *args: "digest")

    def run(self, args, **kwargs):
        calls.append(args)
        assert self.workspace.is_relative_to(tmp_path / "tmp/fresh-setup")
        if args == ["config", "--format", "json"]:
            config = core_config()
            config["volumes"] = {}
            config["networks"] = {}
            return json.dumps(config)
        if args[0] == "build":
            raise RuntimeError("Build failed")
        return ""

    monkeypatch.setattr(bootstrap.Verifier, "run", run)
    report = tmp_path / "report.json"
    assert not bootstrap.verify(tmp_path, report)
    evidence = json.loads(report.read_text())
    assert evidence["error"] == "Build failed"
    assert evidence["cleanup"] == "passed"
    assert ["down", "--volumes", "--rmi", "local"] in calls


def test_cleanup_failure_cannot_report_acceptance_success(tmp_path, monkeypatch):
    monkeypatch.setattr(bootstrap, "copy_source", lambda *args: "digest")

    def run(self, args, **kwargs):
        if args == ["config", "--format", "json"]:
            config = core_config()
            config["volumes"] = {}
            config["networks"] = {}
            return json.dumps(config)
        if args[0] in {"build", "down"}:
            raise RuntimeError("Docker unavailable")
        return ""

    monkeypatch.setattr(bootstrap.Verifier, "run", run)
    report = tmp_path / "report.json"
    assert not bootstrap.verify(tmp_path, report)
    evidence = json.loads(report.read_text())
    assert evidence["status"] == "failed"
    assert evidence["cleanup"] == "failed"
    assert evidence["cleanup_project"].startswith("trade-bootstrap-")
