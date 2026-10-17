from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from typing import Any

VERIFIED_HISTORY_CLASSIFICATIONS = frozenset(
    {"verified_complete", "verified_partial"}
)


@dataclass(frozen=True)
class ProviderDailyHistoryEvidence:
    evidence_id: str
    provider: str
    instrument_key: str
    exchange: str
    canonical_instrument_id: str
    provider_symbol: str
    interval: str
    work_type: str
    requested_start: date
    requested_end: date
    coverage_start: date
    coverage_end: date
    first_available_date: date
    last_available_date: date
    expected_rows: int
    observed_rows: int
    missing_rows: int
    coverage_ratio: float
    classification: str
    quarantine_reason: str | None
    evidence_run_id: str
    verified_at: datetime
    created_at: datetime
    updated_at: datetime
    status: str = "active"

    def as_row(self) -> dict[str, Any]:
        return asdict(self)


def build_provider_daily_history_evidence(
    work_item: Mapping[str, Any],
    *,
    expected_sessions: Sequence[date],
    observed_dates: Sequence[date],
    run_id: str,
    at: datetime | None = None,
    sparse_minimum_expected_rows: int = 220,
    sparse_maximum_observed_rows: int = 5,
) -> ProviderDailyHistoryEvidence | None:
    """Classify one successful Yahoo work window as durable coverage evidence."""
    expected = sorted(set(expected_sessions))
    observed = sorted(set(observed_dates))
    if not expected or not observed:
        return None

    now = _as_utc(at or datetime.now(UTC))
    requested_start = _as_date(work_item["window_start"])
    requested_end = _as_date(work_item["window_end"])
    first_available = observed[0]
    last_available = observed[-1]
    expected_set = set(expected)
    observed_expected = expected_set.intersection(observed)
    expected_rows = len(expected)
    observed_rows = len(observed_expected)
    missing_rows = max(expected_rows - observed_rows, 0)
    listing_effective_at = work_item.get("listing_status_effective_at")
    listing_start = (
        _as_date(listing_effective_at)
        if str(work_item.get("listing_status") or "active") == "active"
        and listing_effective_at is not None
        else None
    )
    sparse_old_listing = bool(
        listing_start is not None
        and (requested_end - listing_start).days >= 365
        and expected_rows >= sparse_minimum_expected_rows
        and observed_rows <= sparse_maximum_observed_rows
    )
    if sparse_old_listing:
        classification = "quarantined_sparse"
        quarantine_reason = "implausibly_sparse_history_for_established_listing"
    else:
        classification = "verified_complete" if missing_rows == 0 else "verified_partial"
        quarantine_reason = None

    return ProviderDailyHistoryEvidence(
        evidence_id=str(work_item["work_item_id"]),
        provider=str(work_item.get("provider") or "yfinance"),
        instrument_key=str(
            work_item.get("provider_instrument_key")
            or f"YF|{work_item['provider_symbol']}"
        ),
        exchange=str(work_item["exchange"]).upper(),
        canonical_instrument_id=str(work_item["canonical_instrument_id"]),
        provider_symbol=str(work_item["provider_symbol"]),
        interval=str(work_item.get("interval") or "1d"),
        work_type=str(work_item["work_type"]),
        requested_start=requested_start,
        requested_end=requested_end,
        coverage_start=expected[0],
        coverage_end=expected[-1],
        first_available_date=first_available,
        last_available_date=last_available,
        expected_rows=expected_rows,
        observed_rows=observed_rows,
        missing_rows=missing_rows,
        coverage_ratio=(observed_rows / expected_rows if expected_rows else 0.0),
        classification=classification,
        quarantine_reason=quarantine_reason,
        evidence_run_id=run_id,
        verified_at=now,
        created_at=now,
        updated_at=now,
    )


def verified_provider_history_start(
    evidence: Sequence[Mapping[str, Any]],
) -> date | None:
    starts = [
        _as_date(row["first_available_date"])
        for row in evidence
        if row.get("classification") in VERIFIED_HISTORY_CLASSIFICATIONS
        and row.get("status") == "active"
        and row.get("work_type") in {"initial_backfill", "new_symbol_backfill"}
        and row.get("first_available_date") is not None
    ]
    return min(starts) if starts else None


def verified_provider_coverage_windows(
    evidence: Sequence[Mapping[str, Any]],
) -> list[tuple[date, date]]:
    return [
        (_as_date(row["coverage_start"]), _as_date(row["coverage_end"]))
        for row in evidence
        if row.get("classification") in VERIFIED_HISTORY_CLASSIFICATIONS
        and row.get("status") == "active"
        and row.get("work_type")
        in {"initial_backfill", "new_symbol_backfill", "gap_repair"}
    ]


def provider_history_is_quarantined(evidence: Sequence[Mapping[str, Any]]) -> bool:
    return any(
        row.get("classification") == "quarantined_sparse"
        and row.get("status") == "active"
        for row in evidence
    )


def expected_sessions_for_work_item(
    work_item: Mapping[str, Any],
    sessions: Sequence[date],
) -> list[date]:
    """Restrict calendar sessions to the requested and known lifecycle window."""
    window_start = _as_date(work_item["window_start"])
    window_end = _as_date(work_item["window_end"])
    eligible = [session for session in sessions if window_start <= session <= window_end]
    effective_at = work_item.get("listing_status_effective_at")
    if effective_at is None:
        return eligible
    effective_date = _as_date(effective_at)
    listing_status = str(work_item.get("listing_status") or "active")
    if listing_status == "active":
        return [session for session in eligible if session >= effective_date]
    if listing_status in {"halted", "suspended", "delisted"}:
        return [session for session in eligible if session <= effective_date]
    return eligible


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
