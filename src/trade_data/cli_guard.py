"""Fail-closed policy for invoking the maintenance CLI in production."""

from __future__ import annotations

from enum import StrEnum

import typer


class CommandEffect(StrEnum):
    READ_ONLY = "read_only"
    MUTATING = "mutating"


# This is deliberately explicit. A new command is blocked in production until
# its effects have been reviewed and added to this inventory.
COMMAND_EFFECTS: dict[str, CommandEffect] = {
    "doctor": CommandEffect.READ_ONLY,
    "init-db": CommandEffect.MUTATING,
    "refresh-exchange-sessions": CommandEffect.MUTATING,
    "fetch-nse-daily": CommandEffect.MUTATING,
    "repair-nse-year": CommandEffect.MUTATING,
}


def enforce_cli_policy(command: str | None, *, app_env: str) -> None:
    """Reject mutating and unclassified commands in production.

    There is intentionally no general-purpose break-glass flag. Database
    migrations and incident recovery remain separate audited paths.
    """

    if command is None or app_env.strip().lower() != "production":
        return
    effect = COMMAND_EFFECTS.get(command)
    if effect is CommandEffect.READ_ONLY:
        return
    classification = effect.value if effect else "unclassified"
    typer.echo(
        f"Blocked {classification} CLI command '{command}' in production. "
        "Submit normal mutations through Dagster; use the audited deployment "
        "or incident procedure for administrative recovery.",
        err=True,
    )
    raise typer.Exit(code=78)
