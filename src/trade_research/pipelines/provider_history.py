from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from trade_research.config import get_settings
from trade_research.data.provider_history import (
    build_provider_daily_history_evidence,
    expected_sessions_for_work_item,
)
from trade_research.pipelines.base import PipelineRunResult
from trade_research.storage import TimescaleStore

SUPPORTED_EQUITY_EXCHANGES = frozenset({"NSE", "TSX", "US"})


def run_yfinance_provider_history_evidence_bootstrap(
    exchange: str,
    *,
    symbol_limit: int | None = None,
    provider_symbols: Iterable[str] | None = None,
    trigger: str = "pipeline",
    at: datetime | None = None,
) -> PipelineRunResult:
    """Build durable provider-history evidence from successful stored backfills."""
    exchange_code = exchange.upper()
    if exchange_code not in SUPPORTED_EQUITY_EXCHANGES:
        raise ValueError("exchange must be NSE, TSX, or US")
    if symbol_limit is not None and symbol_limit < 1:
        raise ValueError("symbol_limit must be positive when provided")

    settings = get_settings()
    db = TimescaleStore(settings.database_url)
    db.initialize()
    observed_at = _as_utc(at or datetime.now(UTC))
    requested_symbols = {
        value.strip().upper()
        for value in (provider_symbols or ())
        if value and value.strip()
    }
    instruments = db.active_yfinance_daily_instruments(exchange_code)
    if exchange_code == "TSX":
        instruments = [
            row
            for row in instruments
            if row.get("reconciliation_status") == "official_eligible"
        ]
    if requested_symbols:
        instruments = [
            row
            for row in instruments
            if str(row.get("provider_symbol") or "").upper() in requested_symbols
        ]
    if symbol_limit is not None:
        instruments = instruments[:symbol_limit]
    if not instruments:
        raise ValueError(
            f"No active yfinance instruments matched the {exchange_code} evidence request."
        )

    instrument_by_id = {
        str(row["canonical_instrument_id"]): row for row in instruments
    }
    completed_work = db.succeeded_daily_backfill_work_items(instrument_by_id)
    if not completed_work:
        return PipelineRunResult(
            name="yfinance_provider_history_evidence_bootstrap",
            status="warn",
            rows=0,
            metrics={
                "trigger": trigger,
                "exchange": exchange_code,
                "instruments_selected": len(instruments),
                "successful_backfill_windows": 0,
                "evidence_rows_written": 0,
                "classifications": {},
                "quarantined_symbols": [],
                "pending_work_cancelled": 0,
            },
            warnings=["No successful daily backfill work is available to classify."],
        )

    minimum_start = min(row["window_start"] for row in completed_work)
    maximum_end = max(row["window_end"] for row in completed_work)
    sessions = [
        row["session_date"]
        for row in db.exchange_sessions(exchange_code, minimum_start, maximum_end)
        if row["is_trading_day"]
        and str(row["validation_status"]).startswith("valid")
    ]
    instrument_keys = [
        str(
            row.get("provider_instrument_key")
            or f"YF|{row['provider_symbol']}"
        )
        for row in completed_work
    ]
    stored_dates = db.daily_ohlcv_dates_by_instrument(
        list(dict.fromkeys(instrument_keys)),
        minimum_start,
        maximum_end,
        source="yfinance",
        exchange=exchange_code,
        valid_only=True,
    )

    evidence_rows: list[dict[str, Any]] = []
    for work_item in completed_work:
        instrument_key = str(
            work_item.get("provider_instrument_key")
            or f"YF|{work_item['provider_symbol']}"
        )
        expected = expected_sessions_for_work_item(work_item, sessions)
        observed = [
            candle_date
            for candle_date in stored_dates.get(instrument_key, set())
            if work_item["window_start"] <= candle_date <= work_item["window_end"]
        ]
        evidence = build_provider_daily_history_evidence(
            work_item,
            expected_sessions=expected,
            observed_dates=observed,
            run_id=str(work_item.get("run_id") or f"bootstrap:{observed_at.isoformat()}"),
            at=observed_at,
            sparse_minimum_expected_rows=(
                settings.yfinance_sparse_history_minimum_expected_rows
            ),
            sparse_maximum_observed_rows=(
                settings.yfinance_sparse_history_maximum_observed_rows
            ),
        )
        if evidence is not None:
            evidence_rows.append(evidence.as_row())

    written = db.upsert_provider_daily_history_evidence(evidence_rows)
    classifications = Counter(
        str(row["classification"]) for row in evidence_rows
    )
    quarantined_symbols = sorted(
        {
            str(row["provider_symbol"])
            for row in evidence_rows
            if row["classification"] == "quarantined_sparse"
        }
    )
    quarantined_ids = {
        str(row["canonical_instrument_id"])
        for row in evidence_rows
        if row["classification"] == "quarantined_sparse"
    }
    cancelled = db.cancel_pending_pipeline_work_for_instruments(
        quarantined_ids,
        reason="provider_history_quarantined",
        message=(
            "Pending Yahoo work was cancelled because successful provider history "
            "was implausibly sparse for an established listing."
        ),
        at=observed_at,
    )
    return PipelineRunResult(
        name="yfinance_provider_history_evidence_bootstrap",
        status="warn" if quarantined_symbols else "pass",
        rows=written,
        metrics={
            "trigger": trigger,
            "exchange": exchange_code,
            "instruments_selected": len(instruments),
            "successful_backfill_windows": len(completed_work),
            "evidence_rows_written": written,
            "classifications": dict(sorted(classifications.items())),
            "quarantined_symbols": quarantined_symbols,
            "pending_work_cancelled": cancelled,
        },
        warnings=(
            [
                "Sparse provider history was quarantined for: "
                + ", ".join(quarantined_symbols)
            ]
            if quarantined_symbols
            else []
        ),
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
