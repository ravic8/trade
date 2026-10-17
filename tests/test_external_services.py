"""External topology, credential isolation, and owned acceptance cleanup."""

import copy
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/verify_external_services.py"
spec = importlib.util.spec_from_file_location("external_services", SCRIPT)
assert spec and spec.loader
external = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(SCRIPT.parent))
try:
    spec.loader.exec_module(external)
finally:
    sys.path.pop(0)


def application_config():
    return {
        "services": {
            "api": {"environment": {}, "ports": [{"published": "8000", "target": 8000}]},
        },
        "volumes": {"acceptance_test_volume": {"name": "app_test_volume"}},
        "networks": {"default": {"name": "app_default"}},
    }


def test_fixture_authenticates_both_services_without_exposing_host_ports():
    config = external.fixture_config("owned", "database-secret", "redis-secret")
    assert config["services"]["database"]["environment"]["POSTGRES_PASSWORD"] == "database-secret"
    assert config["services"]["cache"]["command"][-2:] == ["--requirepass", "redis-secret"]
    assert all("ports" not in service for service in config["services"].values())
    assert config["networks"]["default"]["name"] == "owned_services"


def test_application_borrows_infrastructure_network_and_owns_its_storage():
    config = external.isolated_application(application_config(), "app", "infra")
    assert config["networks"]["infrastructure"] == {"external": True, "name": "infra_services"}
    assert all(volume["type"] == "volume" for volume in config["services"]["api"]["volumes"])
    assert config["services"]["api"]["ports"][0]["host_ip"] == "127.0.0.1"
    assert config["services"]["api"]["ports"][0]["published"] == "0"


def test_application_accepts_compose_omitting_unused_volume_definitions():
    config = application_config()
    del config["volumes"]
    isolated = external.isolated_application(config, "app", "infra")
    assert set(isolated["volumes"]) == {"acceptance_data", "acceptance_artifacts"}


@pytest.mark.parametrize("mutation", ["database", "dependency", "volume", "external_network"])
def test_application_rejects_local_dependencies_and_unowned_resources(mutation):
    config = copy.deepcopy(application_config())
    if mutation == "database":
        config["services"]["postgres"] = {}
    elif mutation == "dependency":
        config["services"]["api"]["depends_on"] = {"postgres": {}}
    elif mutation == "volume":
        config["volumes"]["acceptance_test_volume"]["name"] = "operator_modules"
    else:
        config["networks"]["default"]["external"] = True
    with pytest.raises(RuntimeError):
        external.isolated_application(config, "app", "infra")


def test_resolution_failure_does_not_echo_compose_secrets(tmp_path, monkeypatch):
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1, stdout="", stderr="credential-marker"
        ),
    )
    verifier = external.Verifier(tmp_path, "app", tmp_path / "log")
    with pytest.raises(RuntimeError) as error:
        external.resolved_application(verifier)
    assert "credential-marker" not in str(error.value)
    assert not verifier.log.exists()


@pytest.mark.parametrize("failed_service", [None, "postgresql", "redis"])
def test_doctor_accepts_only_correct_status_and_exit_code(failed_service):
    report = {
        "ready": failed_service is None,
        "checks": [
            {"name": name, "status": "fail" if name == failed_service else "pass"}
            for name in ("postgresql", "redis")
        ],
    }
    result = {"exit_code": int(failed_service is not None), "report": report}
    verifier = SimpleNamespace(run=lambda args: json.dumps(result))
    assert external.doctor(verifier, failed_service) == result
    result["exit_code"] = 1 - result["exit_code"]
    with pytest.raises(RuntimeError, match="exit status"):
        external.doctor(verifier, failed_service)


def test_failed_build_cleans_app_before_infrastructure_and_redacts_evidence(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(external.secrets, "token_hex", lambda length: "credential-marker")

    def snapshot(root, workspace):
        (workspace / ".env").write_text("YFINANCE_DAILY_ENABLED=false\n")
        return "digest"

    def resolved(verifier):
        config = application_config()
        config["volumes"] = {}
        config["networks"] = {}
        config["services"]["api"]["environment"] = {
            "DATABASE_URL": "postgresql+psycopg://external_user:credential-marker%3A%40%25%2F"
            "@database.service:5432/external_trade",
            "REDIS_URL": "redis://:credential-marker%3A%40%25%2F@cache.service:6379/0",
        }
        return config

    def run(self, args, **kwargs):
        calls.append((self.project, args[0]))
        assert self.workspace.is_relative_to(tmp_path / "tmp/external-services")
        with self.log.open("a") as stream:
            stream.write("credential-marker:@%/\ncredential-marker%3A%40%25%2F\n")
        if args[0] == "build":
            raise RuntimeError("credential-marker:@%/ build failed")
        return ""

    monkeypatch.setattr(external, "copy_source", snapshot)
    monkeypatch.setattr(external, "resolved_application", resolved)
    monkeypatch.setattr(external.Verifier, "run", run)
    report = tmp_path / "report.json"
    assert not external.verify(tmp_path, report)
    evidence = json.loads(report.read_text())
    assert list(evidence["cleanup"].values()) == ["passed", "passed"]
    assert [(project, action) for project, action in calls if action == "down"] == [
        (evidence["project"], "down"),
        (evidence["infrastructure_project"], "down"),
    ]
    assert "credential-marker" not in report.read_text()
    assert "credential-marker" not in report.with_suffix(".log").read_text()


def test_real_compose_overlay_replaces_env_file_and_requires_both_endpoints(tmp_path):
    """Config resolution needs the Compose CLI but no running Docker daemon."""
    import shutil

    if shutil.which("docker") is None:
        pytest.skip("Compose CLI unavailable")
    root = SCRIPT.parents[1]
    for name in ("docker-compose.yml", "docker-compose.external.yml"):
        shutil.copyfile(root / name, tmp_path / name)
    (tmp_path / ".env").write_text(
        "OPENAI_API_KEY=operator-secret\nOPERATOR_ONLY=operator-secret\n"
    )
    env = tmp_path / ".env.external"
    database_url = "postgresql+psycopg://owner:encoded%25password@different.db:5432/application"
    redis_url = "redis://:encoded%25password@different.cache:6379/4"
    env.write_text(f"COMPOSE_DATABASE_URL={database_url}\nCOMPOSE_REDIS_URL={redis_url}\n")
    verifier = external.Verifier(tmp_path, "app", tmp_path / "log")
    verifier.base[3] = str(env)
    verifier.base.extend(["-f", str(tmp_path / "docker-compose.external.yml")])
    config = external.resolved_application(verifier)
    assert set(config["services"]) == {"api"}
    api = config["services"]["api"]
    assert not api.get("depends_on")
    assert api["environment"]["DATABASE_URL"] == database_url
    assert api["environment"]["REDIS_URL"] == redis_url
    assert "operator-secret" not in json.dumps(config)
    assert not verifier.log.exists()
    for content in (f"COMPOSE_REDIS_URL={redis_url}\n", f"COMPOSE_DATABASE_URL={database_url}\n"):
        env.write_text(content)
        with pytest.raises(RuntimeError, match="required endpoints"):
            external.resolved_application(verifier)
