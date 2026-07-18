from __future__ import annotations

from datetime import datetime

from trade_research.config import Settings, get_settings
from trade_research.exchanges import canonical_equity_exchange
from trade_research.pipelines.base import PipelineRunResult
from trade_research.storage import TimescaleStore
from trade_research.universe import (
    NSEUniverseProvider,
    PersistedUniverseService,
    TSXUniverseProvider,
    UniverseValidationPolicy,
    YFinanceUSUniverseProvider,
)
from trade_research.universe.base import UniverseProvider
from trade_research.universe.persisted import UniverseSnapshotRepository


def run_equity_universe_snapshot_pipeline(
    exchange: str,
    *,
    provider: UniverseProvider | None = None,
    repository: UniverseSnapshotRepository | None = None,
    allow_large_change: bool = False,
    trigger: str = "pipeline",
    fetched_at: datetime | None = None,
    snapshot_id: str | None = None,
) -> PipelineRunResult:
    settings = get_settings()
    canonical_exchange = canonical_equity_exchange(exchange)
    resolved_provider = provider or equity_universe_provider(canonical_exchange)
    resolved_repository = repository
    if resolved_repository is None:
        store = TimescaleStore(settings.database_url)
        store.initialize()
        resolved_repository = store

    service = PersistedUniverseService(
        resolved_repository,
        missing_snapshots_before_inactive=(
            settings.equity_universe_missing_snapshots_before_inactive
        ),
    )
    exchange_enabled = _yfinance_exchange_enabled(settings, canonical_exchange)
    result = service.refresh(
        resolved_provider,
        universe_validation_policy(settings, canonical_exchange),
        allow_large_change=allow_large_change,
        fetched_at=fetched_at,
        snapshot_id=snapshot_id,
        enqueue_backfills=exchange_enabled,
    )
    validation_errors = list(result.validation.errors) if result.validation else []
    validation_warnings = list(result.validation.warnings) if result.validation else []
    error_message = result.error_message or ""
    blocking_issues = validation_errors or ([error_message] if error_message else [])
    diagnostics = getattr(resolved_provider, "diagnostics", None)
    source_diagnostics = diagnostics() if callable(diagnostics) else {}
    return PipelineRunResult(
        name=f"{canonical_exchange.lower()}_universe_snapshot",
        status="pass" if result.status == "accepted" else "fail",
        rows=result.symbol_count,
        metrics={
            "snapshot_id": result.snapshot_id,
            "exchange": result.exchange,
            "source": result.source,
            "snapshot_status": result.status,
            "symbol_count": result.symbol_count,
            "events_written": result.events_written,
            "work_items_queued": result.work_items_queued,
            "backfill_planning_enabled": exchange_enabled,
            "backfill_execution_enabled": (
                settings.yfinance_daily_enabled and exchange_enabled
            ),
            "trigger": trigger,
            "allow_large_change": allow_large_change,
            "source_diagnostics": source_diagnostics,
        },
        warnings=validation_warnings,
        blocking_issues=blocking_issues,
    )


def equity_universe_provider(exchange: str) -> UniverseProvider:
    canonical_exchange = canonical_equity_exchange(exchange)
    if canonical_exchange == "NSE":
        return NSEUniverseProvider()
    if canonical_exchange == "TSX":
        return TSXUniverseProvider()
    return YFinanceUSUniverseProvider()


def universe_validation_policy(
    settings: Settings,
    exchange: str,
) -> UniverseValidationPolicy:
    canonical_exchange = canonical_equity_exchange(exchange)
    minimums = {
        "NSE": settings.equity_universe_minimum_nse_symbols,
        "TSX": settings.equity_universe_minimum_tsx_symbols,
        "US": settings.equity_universe_minimum_us_symbols,
    }
    return UniverseValidationPolicy(
        minimum_symbol_count=minimums[canonical_exchange],
        maximum_change_ratio=settings.equity_universe_maximum_change_ratio,
    )


def _yfinance_exchange_enabled(settings: Settings, exchange: str) -> bool:
    return {
        "NSE": settings.yfinance_nse_enabled,
        "TSX": settings.yfinance_full_tsx_enabled,
        "US": settings.yfinance_full_us_enabled,
    }[canonical_equity_exchange(exchange)]
