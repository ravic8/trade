from types import SimpleNamespace

import pytest
import typer
from typer.testing import CliRunner

from trade_research import cli
from trade_research.cli_guard import COMMAND_EFFECTS, CommandEffect, enforce_cli_policy


def test_every_registered_cli_command_has_an_effect_classification() -> None:
    registered = {command.name for command in cli.app.registered_commands}
    assert None not in registered
    assert registered == set(COMMAND_EFFECTS)


def test_mutating_command_is_rejected_in_production() -> None:
    with pytest.raises(typer.Exit) as raised:
        enforce_cli_policy("init-db", app_env="production")
    assert raised.value.exit_code == 78


def test_unclassified_command_fails_closed_in_production() -> None:
    with pytest.raises(typer.Exit) as raised:
        enforce_cli_policy("new-unreviewed-command", app_env="production")
    assert raised.value.exit_code == 78


def test_read_only_command_is_allowed_in_production() -> None:
    enforce_cli_policy("market-session", app_env="production")


def test_mutating_command_is_allowed_outside_production() -> None:
    enforce_cli_policy("init-db", app_env="local")


def test_cli_callback_blocks_before_mutating_command_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    initialized = False

    def _unexpected_initialize() -> None:
        nonlocal initialized
        initialized = True

    monkeypatch.setattr(cli, "get_settings", lambda: SimpleNamespace(app_env="production"))
    monkeypatch.setattr(cli.TimescaleStore, "initialize", _unexpected_initialize)
    result = CliRunner().invoke(cli.app, ["init-db"])
    assert result.exit_code == 78
    assert "Blocked mutating CLI command 'init-db'" in result.stderr
    assert initialized is False


def test_inventory_contains_both_policy_classes() -> None:
    assert set(COMMAND_EFFECTS.values()) == {CommandEffect.READ_ONLY, CommandEffect.MUTATING}
