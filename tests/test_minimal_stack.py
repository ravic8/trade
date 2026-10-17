"""Keep the core deployable without optional infrastructure or feature runtimes."""

from pathlib import Path

import pytest
import yaml


def test_default_stack_and_orchestration_need_only_core_infrastructure() -> None:
    services = yaml.safe_load(
        (Path(__file__).resolve().parents[1] / "docker-compose.yml").read_text()
    )["services"]
    core = {name for name, spec in services.items() if not spec.get("profiles")}
    assert core == {"postgres", "redis", "api"}
    for name in core | {"dagster-webserver", "dagster-daemon"}:
        assert set(services[name].get("depends_on", {})) <= core


@pytest.mark.parametrize(
    "profiles",
    [
        {"filings"},
        {"research"},
        {"ingestion"},
        {"ai"},
        {"observability"},
        {"admin"},
        {"filings", "research", "ingestion"},
    ],
)
def test_profile_dependencies_are_available(profiles: set[str]) -> None:
    services = yaml.safe_load(
        (Path(__file__).resolve().parents[1] / "docker-compose.yml").read_text()
    )["services"]
    active = {
        name
        for name, spec in services.items()
        if not spec.get("profiles") or profiles.intersection(spec["profiles"])
    }
    for name in active:
        assert set(services[name].get("depends_on", {})) <= active
