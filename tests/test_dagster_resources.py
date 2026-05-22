from importlib import import_module
from types import SimpleNamespace

import pytest

dagster = pytest.importorskip("dagster")
resources = import_module("trade_research.dagster.resources")


class FakeTimescaleStore:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.initialize_calls = 0

    def initialize(self) -> None:
        self.initialize_calls += 1


def test_timescale_store_initializes_without_resource_config(monkeypatch) -> None:
    monkeypatch.setattr(
        resources,
        "get_settings",
        lambda: SimpleNamespace(database_url="postgresql://scheduled-run"),
    )
    monkeypatch.setattr(resources, "TimescaleStore", FakeTimescaleStore)

    store = resources.timescale_store(dagster.build_init_resource_context())

    assert store.database_url == "postgresql://scheduled-run"
    assert store.initialize_calls == 1
