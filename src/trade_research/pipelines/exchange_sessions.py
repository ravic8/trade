from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date, datetime, timedelta
from typing import Any, Protocol

from trade_research.config import get_settings
from trade_research.exchange_sessions import (
    ExchangeSessionShadowComparison,
    build_materialized_exchange_sessions,
    shadow_compare_exchange_sessions,
    validate_materialized_exchange_sessions,
)
from trade_research.exchanges import canonical_equity_exchange
from trade_research.market_calendar import ExchangeHolidays, fetch_exchange_holidays
from trade_research.pipelines.base import PipelineRunResult
from trade_research.storage import TimescaleStore


class ExchangeSessionRepository(Protocol):
    def exchange_holidays(
        self,
        exchange: str,
        year: int,
        max_age_days: int | None = None,
    ) -> dict[str, Any] | None:
        ...

    def upsert_exchange_holidays(
        self,
        exchange: str,
        year: int,
        closed_dates,
        early_close_dates,
        source_url: str,
        fetched_at: datetime | None = None,
    ) -> int:
        ...

    def upsert_exchange_sessions(self, sessions) -> int:
        ...

    def delete_exchange_sessions(
        self,
        exchange: str,
        start_date: date,
        end_date: date,
    ) -> int:
        ...

    def observed_daily_session_dates(
        self,
        exchange: str,
        start_date: date,
        end_date: date,
        *,
        minimum_instruments: int = 10,
    ) -> set[date]:
        ...


def run_exchange_session_materialization_pipeline(
    exchange: str,
    *,
    repository: ExchangeSessionRepository | None = None,
    as_of_date: date | None = None,
    history_years: int | None = None,
    future_years: int | None = None,
    holiday_loader: Callable[[str, int], ExchangeHolidays] = fetch_exchange_holidays,
    generated_at: datetime | None = None,
    trigger: str = "pipeline",
) -> PipelineRunResult:
    settings = get_settings()
    canonical_exchange = canonical_equity_exchange(exchange)
    current_date = as_of_date or date.today()
    history = (
        settings.exchange_session_history_years
        if history_years is None
        else history_years
    )
    future = (
        settings.exchange_session_future_years
        if future_years is None
        else future_years
    )
    if history < 0 or future < 0:
        raise ValueError("history_years and future_years must be non-negative")
    start_date = date(current_date.year - history, 1, 1)
    current_year_end = date(current_date.year, 12, 31)
    requested_end_date = date(current_date.year + future, 12, 31)

    resolved_repository = repository
    if resolved_repository is None:
        store = TimescaleStore(settings.database_url)
        store.initialize()
        resolved_repository = store

    holiday_records = _stored_holiday_records(
        resolved_repository,
        canonical_exchange,
        start_date.year,
        requested_end_date.year,
    )
    warnings: list[str] = []
    try:
        official = holiday_loader(canonical_exchange, current_date.year)
        holiday_records[current_date.year] = official
        resolved_repository.upsert_exchange_holidays(
            exchange=canonical_exchange,
            year=current_date.year,
            closed_dates=official.closed_dates,
            early_close_dates=official.early_close_dates,
            source_url=official.source_url,
        )
    except Exception as exc:
        warnings.append(f"Official current-year holiday refresh failed: {exc}")

    observed_loader = getattr(resolved_repository, "observed_daily_session_dates", None)
    observed_dates = (
        observed_loader(
            canonical_exchange,
            start_date,
            requested_end_date,
            minimum_instruments=(
                settings.exchange_session_observed_open_minimum_instruments
            ),
        )
        if observed_loader is not None
        else set()
    )
    sessions = build_materialized_exchange_sessions(
        canonical_exchange,
        start_date,
        current_year_end,
        holiday_overrides=holiday_records,
        observed_special_open_dates=observed_dates,
        generated_at=generated_at,
    )
    validation = validate_materialized_exchange_sessions(
        sessions,
        start_date,
        current_year_end,
        minimum_open_days_per_full_year=(
            settings.exchange_session_minimum_open_days_per_year
        ),
        maximum_open_days_per_full_year=(
            settings.exchange_session_maximum_open_days_per_year
        ),
    )
    future_years_skipped: dict[int, list[str]] = {}
    if validation.accepted:
        for year in range(current_date.year + 1, requested_end_date.year + 1):
            future_start = date(year, 1, 1)
            future_end = date(year, 12, 31)
            future_sessions = build_materialized_exchange_sessions(
                canonical_exchange,
                future_start,
                future_end,
                holiday_overrides=holiday_records,
                observed_special_open_dates=observed_dates,
                generated_at=generated_at,
            )
            future_validation = validate_materialized_exchange_sessions(
                future_sessions,
                future_start,
                future_end,
                minimum_open_days_per_full_year=(
                    settings.exchange_session_minimum_open_days_per_year
                ),
                maximum_open_days_per_full_year=(
                    settings.exchange_session_maximum_open_days_per_year
                ),
            )
            if not future_validation.accepted:
                future_years_skipped[year] = list(future_validation.errors)
                warnings.append(
                    f"Skipped incomplete future calendar year {year}: "
                    f"{', '.join(future_validation.errors)}"
                )
                break
            sessions.extend(future_sessions)

    end_date = sessions[-1].session_date if sessions else current_year_end
    validation = validate_materialized_exchange_sessions(
        sessions,
        start_date,
        end_date,
        minimum_open_days_per_full_year=(
            settings.exchange_session_minimum_open_days_per_year
        ),
        maximum_open_days_per_full_year=(
            settings.exchange_session_maximum_open_days_per_year
        ),
    )
    shadow = shadow_compare_exchange_sessions(sessions, holiday_records)
    observed_special_sessions = [
        item.session_date
        for item in sessions
        if item.validation_status == "valid_observed_special_session"
    ]
    blocking_issues = list(validation.errors)
    if shadow.discrepancy_count > settings.exchange_session_shadow_max_discrepancies:
        blocking_issues.append(
            "calendar_shadow_discrepancies_exceed_limit:"
            f"{shadow.discrepancy_count}>"
            f"{settings.exchange_session_shadow_max_discrepancies}"
        )
    elif shadow.discrepancy_count:
        warnings.append(_shadow_warning(shadow))

    rows_written = 0
    future_rows_deleted = 0
    if not blocking_issues:
        rows_written = resolved_repository.upsert_exchange_sessions(
            [item.as_row() for item in sessions]
        )
        if end_date < requested_end_date:
            delete_sessions = getattr(
                resolved_repository,
                "delete_exchange_sessions",
                None,
            )
            if delete_sessions is not None:
                future_rows_deleted = delete_sessions(
                    canonical_exchange,
                    end_date + timedelta(days=1),
                    requested_end_date,
                )

    return PipelineRunResult(
        name=f"{canonical_exchange.lower()}_exchange_sessions",
        status="fail" if blocking_issues else ("warn" if warnings else "pass"),
        rows=rows_written,
        metrics={
            "exchange": canonical_exchange,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "requested_end_date": requested_end_date.isoformat(),
            "future_years_skipped": future_years_skipped,
            "future_rows_deleted": future_rows_deleted,
            "calendar_version": sessions[0].calendar_version if sessions else None,
            "session_rows": validation.row_count,
            "trading_days": validation.trading_day_count,
            "closed_days": validation.closed_day_count,
            "early_closes": validation.early_close_count,
            "observed_special_sessions": [
                value.isoformat() for value in observed_special_sessions
            ],
            "open_days_by_year": validation.open_days_by_year,
            "shadow_compared_years": list(shadow.compared_years),
            "shadow_discrepancy_count": shadow.discrepancy_count,
            "shadow_legacy_only_dates": [
                value.isoformat() for value in shadow.legacy_only_dates
            ],
            "shadow_materialized_only_dates": [
                value.isoformat() for value in shadow.materialized_only_dates
            ],
            "planning_enabled": settings.materialized_exchange_sessions_enabled,
            "trigger": trigger,
        },
        warnings=warnings,
        blocking_issues=blocking_issues,
    )


def _stored_holiday_records(
    repository: ExchangeSessionRepository,
    exchange: str,
    start_year: int,
    end_year: int,
) -> dict[int, ExchangeHolidays]:
    records: dict[int, ExchangeHolidays] = {}
    for year in range(start_year, end_year + 1):
        row = repository.exchange_holidays(exchange, year)
        if row is None:
            continue
        records[year] = _holiday_record(row)
    return records


def _holiday_record(row: Mapping[str, Any]) -> ExchangeHolidays:
    return ExchangeHolidays(
        closed_dates=frozenset(
            date.fromisoformat(value) for value in row.get("closed_dates", [])
        ),
        early_close_dates=frozenset(
            date.fromisoformat(value) for value in row.get("early_close_dates", [])
        ),
        source_url=str(row.get("source_url") or "stored_exchange_holidays"),
    )


def _shadow_warning(comparison: ExchangeSessionShadowComparison) -> str:
    return (
        f"Calendar shadow comparison found {comparison.discrepancy_count} date differences "
        f"across years {list(comparison.compared_years)}."
    )
