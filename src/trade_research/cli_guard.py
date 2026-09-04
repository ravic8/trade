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
    "universe": CommandEffect.READ_ONLY,
    "refresh-equity-universe": CommandEffect.MUTATING,
    "tsx-reconciliation-status": CommandEffect.READ_ONLY,
    "market-session": CommandEffect.READ_ONLY,
    "refresh-exchange-sessions": CommandEffect.MUTATING,
    "init-db": CommandEffect.MUTATING,
    "import-filing-manifest": CommandEffect.MUTATING,
    "import-filing-universe": CommandEffect.MUTATING,
    "import-filing-universe-pack": CommandEffect.MUTATING,
    "run-filing-intelligence": CommandEffect.MUTATING,
    "run-filing-investigation": CommandEffect.MUTATING,
    "review-filing-intelligence": CommandEffect.MUTATING,
    "evaluate-filing-golden": CommandEffect.READ_ONLY,
    "verify-filing-production": CommandEffect.READ_ONLY,
    "verify-bigquery-environment": CommandEffect.READ_ONLY,
    "bigquery-canary-readiness": CommandEffect.READ_ONLY,
    "verify-bigquery-backfill": CommandEffect.READ_ONLY,
    "create-analyst-role": CommandEffect.MUTATING,
    "revoke-analyst-role": CommandEffect.MUTATING,
    "provider-request-log": CommandEffect.READ_ONLY,
    "fetch-nifty-futures-history": CommandEffect.MUTATING,
    "fetch-upstox-instruments": CommandEffect.MUTATING,
    "map-liquid-nse-upstox": CommandEffect.MUTATING,
    "fetch-upstox-nse-daily": CommandEffect.MUTATING,
    "retry-upstox-nse-daily": CommandEffect.MUTATING,
    "fetch-yfinance-daily": CommandEffect.MUTATING,
    "plan-yfinance-daily-work": CommandEffect.MUTATING,
    "run-yfinance-daily-worker": CommandEffect.MUTATING,
    "plan-yfinance-tsx-canary": CommandEffect.MUTATING,
    "plan-yfinance-nse-canary": CommandEffect.MUTATING,
    "check-nse-yfinance-cutover": CommandEffect.READ_ONLY,
    "refresh-yfinance-history-evidence": CommandEffect.MUTATING,
    "provider-history-status": CommandEffect.READ_ONLY,
    "fetch-yfinance-missing": CommandEffect.MUTATING,
    "fetch-dukascopy-intraday": CommandEffect.MUTATING,
    "fetch-yfinance-intraday": CommandEffect.MUTATING,
    "build-daily-features": CommandEffect.MUTATING,
    "build-daily-targets": CommandEffect.MUTATING,
    "build-opportunity-targets": CommandEffect.MUTATING,
    "build-factor-research": CommandEffect.MUTATING,
    "validate-processed-datasets": CommandEffect.MUTATING,
    "build-ml-dataset-v1": CommandEffect.MUTATING,
    "build-walk-forward-folds-v1": CommandEffect.MUTATING,
    "run-baseline-predictions-v1": CommandEffect.MUTATING,
    "run-lightgbm-predictions-v1": CommandEffect.MUTATING,
    "run-prediction-backtest-v1": CommandEffect.MUTATING,
    "run-latest-predictions-v1": CommandEffect.MUTATING,
    "validate-daily-pipeline-health": CommandEffect.MUTATING,
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
