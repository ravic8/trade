from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Protocol, TypedDict

from trade_research.config import get_settings
from trade_research.exchange_sessions import (
    resolve_expected_session_dates,
)
from trade_research.market_calendar import (
    ExchangeHolidays,
    expected_trading_dates,
    validated_exchange_calendar_years,
)
from trade_research.validation.coverage import evaluate_eligible_session_coverage


class DailyCoverageStore(Protocol):
    def resolve_provider_instruments(
        self,
        symbols: list[str],
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> list[dict]:
        ...

    def daily_ohlcv_dates_by_instrument(
        self,
        instrument_keys: list[str],
        start_date: date,
        end_date: date,
        source: str = "upstox",
        exchange: str = "NSE",
        *,
        valid_only: bool = False,
    ) -> dict[str, set[date]]:
        ...

    def exchange_holidays(
        self,
        exchange: str,
        year: int,
        max_age_days: int | None = None,
    ) -> dict | None:
        ...

    def first_daily_ohlcv_dates_by_instrument(
        self,
        instrument_keys: list[str],
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> dict[str, date]:
        ...


@dataclass(frozen=True)
class CoveragePreviewInput:
    provider: str
    exchange: str
    symbols: tuple[str, ...]
    unit: str
    interval: int
    start_date: date
    end_date: date


class ResolvedCoverageInstrument(TypedDict):
    symbol: str
    trading_symbol: str
    instrument_key: str
    name: Any
    isin: Any


class CoverageInstrumentResolution(TypedDict):
    resolved: list[ResolvedCoverageInstrument]
    unresolved_symbols: set[str]
    ambiguous_symbols: set[str]


def build_daily_coverage_preview(
    request: CoveragePreviewInput,
    store: DailyCoverageStore,
) -> dict:
    _validate_daily_request(request)
    requested_symbols = _normalize_symbols(request.symbols)
    instruments = store.resolve_provider_instruments(
        requested_symbols,
        source=request.provider,
        exchange=request.exchange,
    )
    resolution = _resolve_unique_instruments(requested_symbols, instruments)
    resolved = resolution["resolved"]
    instrument_keys = [item["instrument_key"] for item in resolved]
    settings = get_settings()
    holidays = None
    if settings.materialized_exchange_sessions_enabled:
        session_resolution = resolve_expected_session_dates(
            store,
            request.exchange,
            request.start_date,
            request.end_date,
            use_materialized_sessions=True,
        )
        expected_dates = list(session_resolution.dates)
        calendar_source = session_resolution.source
    else:
        holidays = _stored_holidays(
            store,
            request.exchange,
            request.start_date,
            request.end_date,
        )
        expected_dates = expected_trading_dates(
            request.exchange,
            request.start_date,
            request.end_date,
            holidays=holidays,
        )
        calendar_source = (
            "stored_exchange_holidays" if holidays is not None else "weekdays_only_fallback"
        )
    provider_eligible_dates = _provider_eligible_dates(
        store,
        expected_dates,
        provider=request.provider,
        exchange=request.exchange,
        grace_minutes=(
            settings.yfinance_provider_grace_minutes
            if request.provider.lower() == "yfinance"
            else 0
        ),
    )
    stored_dates = store.daily_ohlcv_dates_by_instrument(
        instrument_keys,
        request.start_date,
        request.end_date,
        source=request.provider,
        exchange=request.exchange,
    )
    valid_stored_dates = store.daily_ohlcv_dates_by_instrument(
        instrument_keys,
        request.start_date,
        request.end_date,
        source=request.provider,
        exchange=request.exchange,
        valid_only=True,
    )

    tasks = []
    coverage_evidence = []
    total_requested_sessions = 0
    total_expected = 0
    total_present = 0
    total_invalid = 0
    total_missing = 0
    total_explained_missing = 0
    total_actionable_missing = 0
    total_off_calendar = 0
    total_exclusions: defaultdict[str, int] = defaultdict(int)
    for item in resolved:
        key = item["instrument_key"]
        coverage = evaluate_eligible_session_coverage(
            requested_start=request.start_date,
            requested_end=request.end_date,
            exchange_session_dates=expected_dates,
            provider_eligible_session_dates=provider_eligible_dates,
            stored_dates=tuple(stored_dates.get(key, set())),
            valid_stored_dates=tuple(valid_stored_dates.get(key, set())),
        )
        evidence = {
            "symbol": item["symbol"],
            "instrument_key": key,
            **coverage.as_evidence(),
        }
        coverage_evidence.append(evidence)
        total_requested_sessions += coverage.requested_exchange_sessions
        total_expected += coverage.expected_eligible_sessions
        total_present += coverage.valid_stored_eligible_sessions
        total_invalid += coverage.invalid_stored_eligible_sessions
        total_missing += coverage.missing_eligible_sessions
        total_explained_missing += coverage.explained_missing_sessions
        total_actionable_missing += coverage.actionable_missing_sessions
        total_off_calendar += len(coverage.off_calendar_stored_dates)
        for reason, count in coverage.exclusion_counts.items():
            total_exclusions[reason] += count
        for window_start, window_end, dates in _contiguous_windows(
            list(coverage.actionable_missing_dates)
        ):
            tasks.append(
                {
                    "symbol": item["symbol"],
                    "trading_symbol": item["trading_symbol"],
                    "instrument_key": key,
                    "fetch_start": window_start,
                    "fetch_end": window_end,
                    "missing_rows": len(dates),
                    "status": "queued",
                }
            )

    warnings = []
    if resolution["unresolved_symbols"]:
        warnings.append(
            "Unresolved symbols: " + ", ".join(sorted(resolution["unresolved_symbols"]))
        )
    if resolution["ambiguous_symbols"]:
        warnings.append(
            "Ambiguous symbols: " + ", ".join(sorted(resolution["ambiguous_symbols"]))
        )
    if not expected_dates:
        warnings.append("No expected trading sessions in the requested date range.")
    if not settings.materialized_exchange_sessions_enabled and holidays is None:
        warnings.append(
            "No stored exchange holiday calendar found; preview uses weekdays only."
        )

    return {
        "provider": request.provider,
        "exchange": request.exchange,
        "unit": request.unit,
        "interval": request.interval,
        "start_date": request.start_date,
        "end_date": request.end_date,
        "calendar_source": calendar_source,
        "symbols_requested": len(requested_symbols),
        "symbols_resolved": len(resolved),
        "unresolved_symbols": sorted(resolution["unresolved_symbols"]),
        "ambiguous_symbols": sorted(resolution["ambiguous_symbols"]),
        "requested_exchange_sessions": total_requested_sessions,
        "expected_eligible_sessions": total_expected,
        "valid_stored_eligible_sessions": total_present,
        "invalid_stored_eligible_sessions": total_invalid,
        "missing_eligible_sessions": total_missing,
        "explained_missing_sessions": total_explained_missing,
        "actionable_missing_sessions": total_actionable_missing,
        "off_calendar_stored_sessions": total_off_calendar,
        "coverage_ratio": total_present / total_expected if total_expected else 0.0,
        "eligibility_exclusion_counts": dict(sorted(total_exclusions.items())),
        "coverage": coverage_evidence,
        "expected_rows": total_expected,
        "already_present_rows": total_present,
        "missing_rows": total_missing,
        "estimated_provider_calls": len(tasks),
        "tasks": tasks,
        "warnings": warnings,
    }


def _validate_daily_request(request: CoveragePreviewInput) -> None:
    provider = request.provider.lower()
    exchange = request.exchange.upper()
    if provider not in {"upstox", "yfinance"}:
        raise ValueError("provider must be upstox or yfinance.")
    if provider == "upstox" and exchange != "NSE":
        raise ValueError("provider=upstox supports only exchange=NSE.")
    if provider == "yfinance" and exchange not in {"NSE", "TSX", "US"}:
        raise ValueError("provider=yfinance supports exchange=NSE, TSX, or US.")
    if request.unit != "days" or request.interval != 1:
        raise ValueError("Only daily candles are supported for the MVP.")
    if request.start_date > request.end_date:
        raise ValueError("start_date must be on or before end_date.")
    validated_exchange_calendar_years(request.start_date, request.end_date)
    if not _normalize_symbols(request.symbols):
        raise ValueError("At least one symbol is required.")


def _normalize_symbols(symbols: tuple[str, ...]) -> list[str]:
    return sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()})


def _resolve_unique_instruments(
    requested_symbols: list[str],
    instruments: list[dict],
) -> CoverageInstrumentResolution:
    by_symbol: dict[str, list[dict]] = defaultdict(list)
    for row in instruments:
        trading_symbol = str(row.get("trading_symbol") or "").strip().upper()
        yahoo_symbol = str(row.get("yahoo_symbol") or "").strip().upper()
        for alias in {trading_symbol, yahoo_symbol} - {""}:
            by_symbol[alias].append(row)

    resolved: list[ResolvedCoverageInstrument] = []
    unresolved = set()
    ambiguous = set()
    for symbol in requested_symbols:
        matches = by_symbol.get(symbol, [])
        if not matches:
            unresolved.add(symbol)
            continue
        keys = {str(row.get("instrument_key") or "") for row in matches}
        if len(keys) > 1:
            ambiguous.add(symbol)
            continue
        row = matches[0]
        resolved.append(
            {
                "symbol": symbol,
                "trading_symbol": str(row.get("trading_symbol") or symbol).upper(),
                "instrument_key": str(row["instrument_key"]),
                "name": row.get("name"),
                "isin": row.get("isin"),
            }
        )
    return {
        "resolved": resolved,
        "unresolved_symbols": unresolved,
        "ambiguous_symbols": ambiguous,
    }


def _stored_holidays(
    store: DailyCoverageStore,
    exchange: str,
    start: date,
    end: date,
) -> ExchangeHolidays | None:
    closed_dates: set[date] = set()
    early_close_dates: set[date] = set()
    source_url = ""
    for year in range(start.year, end.year + 1):
        row = store.exchange_holidays(exchange, year)
        if row is None:
            return None
        source_url = str(row.get("source_url") or source_url)
        closed_dates.update(date.fromisoformat(value) for value in row.get("closed_dates", []))
        early_close_dates.update(
            date.fromisoformat(value) for value in row.get("early_close_dates", [])
        )
    return ExchangeHolidays(
        closed_dates=frozenset(closed_dates),
        early_close_dates=frozenset(early_close_dates),
        source_url=source_url,
    )


def _provider_eligible_dates(
    store: DailyCoverageStore,
    expected_dates: list[date],
    *,
    provider: str,
    exchange: str,
    grace_minutes: int,
) -> list[date]:
    if not expected_dates:
        return []
    resolver = getattr(store, "latest_provider_eligible_exchange_session", None)
    if resolver is None:
        return expected_dates
    latest = resolver(
        exchange,
        provider_grace_minutes=grace_minutes,
    )
    if latest is None:
        return []
    latest_date = latest.get("session_date")
    if not isinstance(latest_date, date):
        raise ValueError(
            f"Latest provider-eligible {provider} session has no valid session_date."
        )
    return [value for value in expected_dates if value <= latest_date]


def _contiguous_windows(missing_dates: list[date]) -> list[tuple[date, date, list[date]]]:
    if not missing_dates:
        return []
    windows = []
    current = [missing_dates[0]]
    for missing_date in missing_dates[1:]:
        if missing_date == current[-1] + timedelta(days=1):
            current.append(missing_date)
            continue
        windows.append((current[0], current[-1], current))
        current = [missing_date]
    windows.append((current[0], current[-1], current))
    return windows
