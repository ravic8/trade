"""Read-only evidence for a bounded, real-data daily ingestion canary."""

from __future__ import annotations

from datetime import date
from typing import Any, Protocol


class DailyCanaryStore(Protocol):
    def latest_provider_eligible_exchange_session(
        self, exchange: str, *, provider_grace_minutes: int
    ) -> dict[str, Any] | None: ...

    def active_yfinance_daily_instruments(self, exchange: str) -> list[dict[str, Any]]: ...

    def exchange_sessions(
        self, exchange: str, start_date: date, end_date: date
    ) -> list[dict[str, Any]]: ...

    def daily_ohlcv_dates_by_instrument(
        self,
        instrument_keys: list[str],
        start_date: date,
        end_date: date,
        source: str,
        exchange: str,
        *,
        valid_only: bool,
    ) -> dict[str, set[date]]: ...


class DailyCanaryRunStore(DailyCanaryStore, Protocol):
    def ingestion_run(self, run_id: str) -> dict[str, Any] | None: ...

    def pipeline_work_items_for_run(
        self, run_id: str, *, claimed_work_item_ids: list[str], exchange: str
    ) -> list[dict[str, Any]]: ...

    def provider_request_log_summary(
        self, run_id: str, provider: str
    ) -> list[dict[str, Any]]: ...


def daily_canary_snapshot(
    store: DailyCanaryStore,
    *,
    exchange: str,
    provider_symbols: list[str],
    start_date: date,
    end_date: date,
    provider_grace_minutes: int = 0,
) -> dict[str, Any]:
    """Report valid stored sessions; never fetch or fabricate provider data."""

    exchange_code = exchange.strip().upper()
    if exchange_code not in {"NSE", "TSX", "US"}:
        raise ValueError("exchange must be NSE, TSX, or US")
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")
    if (end_date - start_date).days > 30:
        raise ValueError("canary window must be at most 31 calendar days")
    if provider_grace_minutes < 0:
        raise ValueError("provider_grace_minutes must be non-negative")
    symbols = [value.strip().upper() for value in provider_symbols]
    if not 1 <= len(symbols) <= 5 or any(not value for value in symbols):
        raise ValueError("provide between one and five non-empty provider symbols")
    if len(set(symbols)) != len(symbols):
        raise ValueError("provider symbols must be unique")

    eligible = store.latest_provider_eligible_exchange_session(
        exchange_code, provider_grace_minutes=provider_grace_minutes
    )
    if eligible is None:
        raise ValueError(f"no provider-eligible {exchange_code} session is materialized")
    eligible_date = eligible["session_date"]
    if end_date > eligible_date:
        raise ValueError(
            f"canary end_date must not exceed the latest eligible session {eligible_date}"
        )

    active = store.active_yfinance_daily_instruments(exchange_code)
    by_symbol = {
        str(row["provider_symbol"]).upper(): row
        for row in active
        if row.get("provider_symbol")
    }
    missing_symbols = sorted(set(symbols) - by_symbol.keys())
    if missing_symbols:
        raise ValueError(
            "symbols absent from the accepted active snapshot: " + ", ".join(missing_symbols)
        )

    session_rows = store.exchange_sessions(exchange_code, start_date, end_date)
    sessions = sorted(
        {
            row["session_date"]
            for row in session_rows
            if row["is_trading_day"] and str(row["validation_status"]).startswith("valid")
        }
    )
    if not sessions:
        raise ValueError("no validated trading sessions exist in the requested window")

    keys = [
        str(by_symbol[symbol].get("provider_instrument_key") or f"YF|{symbol}")
        for symbol in symbols
    ]
    observed = store.daily_ohlcv_dates_by_instrument(
        keys, start_date, end_date, source="yfinance", exchange=exchange_code, valid_only=True
    )
    instruments = []
    for symbol, key in zip(symbols, keys, strict=True):
        row = by_symbol[symbol]
        effective_at = row.get("listing_status_effective_at")
        eligible_sessions = sessions
        if row.get("listing_status") == "active" and effective_at is not None:
            eligible_sessions = [
                session for session in sessions if session >= effective_at.date()
            ]
        if not eligible_sessions:
            raise ValueError(f"{symbol} has no eligible sessions in the requested window")
        stored = set(observed.get(key, set())) & set(eligible_sessions)
        missing = [session for session in eligible_sessions if session not in stored]
        instruments.append(
            {
                "provider_symbol": symbol,
                "instrument_key": key,
                "eligible_sessions": [session.isoformat() for session in eligible_sessions],
                "valid_session_count": len(stored),
                "missing_sessions": [session.isoformat() for session in missing],
            }
        )
    return {
        "schema_version": 1,
        "provider": "yfinance",
        "exchange": exchange_code,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "provider_grace_minutes": provider_grace_minutes,
        "latest_eligible_session": eligible_date.isoformat(),
        "validated_sessions": [session.isoformat() for session in sessions],
        "complete": all(not item["missing_sessions"] for item in instruments),
        "instruments": instruments,
    }


def daily_canary_run_report(
    store: DailyCanaryRunStore,
    *,
    run_id: str,
    provider_grace_minutes: int,
) -> dict[str, Any]:
    """Summarize one real NSE canary run without provider calls or mutations."""
    run = store.ingestion_run(run_id)
    if run is None:
        raise ValueError("ingestion run not found")
    if (
        run.get("job_name") != "yfinance_daily_work_queue"
        or run.get("exchange") != "NSE"
        or run.get("source") != "yfinance"
    ):
        raise ValueError("run is not an NSE yfinance daily worker run")
    metadata = run.get("run_metadata") or {}
    work_type = metadata.get("work_type_filter")
    if work_type not in {"bounded_canary", "daily_incremental"}:
        raise ValueError("run did not use a bounded NSE canary work-type filter")
    claimed_ids = list(metadata.get("claimed_work_item_ids") or [])
    if not 1 <= len(claimed_ids) <= 5:
        raise ValueError("run must have one to five claimed canary work items")
    work = store.pipeline_work_items_for_run(
        run_id, claimed_work_item_ids=claimed_ids, exchange="NSE"
    )
    if len(work) != len(claimed_ids) or any(
        item.get("work_type") != work_type or item.get("provider") != "yfinance"
        for item in work
    ):
        raise ValueError("claimed work no longer matches the scoped NSE canary run")
    symbols = sorted({str(item["provider_symbol"]) for item in work})
    if not 1 <= len(symbols) <= 5:
        raise ValueError("run has no bounded provider-symbol scope")
    start = min(item["window_start"] for item in work)
    end = max(item["window_end"] for item in work)
    coverage = daily_canary_snapshot(
        store,
        exchange="NSE",
        provider_symbols=symbols,
        start_date=start,
        end_date=end,
        provider_grace_minutes=provider_grace_minutes,
    )
    requests: list[dict[str, Any]] = [
        {
            "endpoint_group": str(row["endpoint_group"]),
            "status": str(row["status"]),
            "requests": int(row["requests"]),
            "rate_limited_requests": int(row["rate_limited_requests"] or 0),
        }
        for row in store.provider_request_log_summary(run_id, provider="yfinance")
    ]
    items = [
        {
            "work_item_id": str(item["work_item_id"]),
            "provider_symbol": str(item["provider_symbol"]),
            "window_start": item["window_start"].isoformat(),
            "window_end": item["window_end"].isoformat(),
            "current_status": str(item["status"]),
            "attempt_count": int(item["attempt_count"]),
        }
        for item in work
    ]
    return {
        "schema_version": 1,
        "run_id": run_id,
        "run_status": str(run["status"]),
        "work_type": work_type,
        "items_requested": int(run["items_requested"]),
        "items_succeeded": int(run["items_succeeded"]),
        "items_failed": int(run["items_failed"]),
        "work_items": items,
        "provider_requests": requests,
        "coverage": coverage,
        "complete": (
            run["status"] == "completed"
            and int(run["items_failed"]) == 0
            and int(run["items_succeeded"]) == len(items)
            and all(item["current_status"] == "succeeded" for item in items)
            and any(
                request["status"] == "success" and int(request["requests"]) > 0
                for request in requests
            )
            and coverage["complete"]
        ),
    }
