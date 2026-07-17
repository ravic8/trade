from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Protocol

from trade_research.config import get_settings
from trade_research.exchange_sessions import (
    expected_dates_for_instrument,
    resolve_expected_session_dates,
)
from trade_research.market_calendar import ExchangeHolidays, expected_trading_dates


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
    stored_dates = store.daily_ohlcv_dates_by_instrument(
        instrument_keys,
        request.start_date,
        request.end_date,
        source=request.provider,
        exchange=request.exchange,
    )
    first_date_loader = getattr(
        store,
        "first_daily_ohlcv_dates_by_instrument",
        None,
    )
    first_trade_dates = (
        first_date_loader(
            instrument_keys,
            source=request.provider,
            exchange=request.exchange,
        )
        if first_date_loader is not None
        else {
            key: min(values)
            for key, values in stored_dates.items()
            if values
        }
    )

    tasks = []
    total_expected = 0
    total_present = 0
    total_missing = 0
    for item in resolved:
        key = item["instrument_key"]
        instrument_expected_dates = expected_dates_for_instrument(
            expected_dates,
            coverage_start=request.start_date,
            first_trade_date=first_trade_dates.get(key),
        )
        instrument_expected_set = set(instrument_expected_dates)
        present_dates = stored_dates.get(key, set())
        present_expected = instrument_expected_set.intersection(present_dates)
        missing_dates = sorted(instrument_expected_set.difference(present_dates))
        total_expected += len(instrument_expected_dates)
        total_present += len(present_expected)
        total_missing += len(missing_dates)
        for window_start, window_end, dates in _contiguous_windows(missing_dates):
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
        "expected_rows": total_expected,
        "already_present_rows": total_present,
        "missing_rows": total_missing,
        "estimated_provider_calls": len(tasks),
        "tasks": tasks,
        "warnings": warnings,
    }


def _validate_daily_request(request: CoveragePreviewInput) -> None:
    if request.provider.lower() != "upstox":
        raise ValueError("Only provider=upstox is supported for the MVP.")
    if request.exchange.upper() != "NSE":
        raise ValueError("Only exchange=NSE is supported for the MVP.")
    if request.unit != "days" or request.interval != 1:
        raise ValueError("Only daily candles are supported for the MVP.")
    if request.start_date > request.end_date:
        raise ValueError("start_date must be on or before end_date.")
    if not _normalize_symbols(request.symbols):
        raise ValueError("At least one symbol is required.")


def _normalize_symbols(symbols: tuple[str, ...]) -> list[str]:
    return sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()})


def _resolve_unique_instruments(
    requested_symbols: list[str],
    instruments: list[dict],
) -> dict[str, list[dict] | set[str]]:
    by_symbol: dict[str, list[dict]] = defaultdict(list)
    for row in instruments:
        trading_symbol = str(row.get("trading_symbol") or "").strip().upper()
        if trading_symbol:
            by_symbol[trading_symbol].append(row)

    resolved = []
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
