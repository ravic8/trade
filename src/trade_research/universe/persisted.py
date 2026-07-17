from __future__ import annotations

import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid4, uuid5

from trade_research.data.daily_work import work_item_idempotency_key
from trade_research.exchanges import canonical_equity_exchange
from trade_research.schemas import Symbol
from trade_research.universe.base import UniverseProvider

_PROVIDER_SYMBOL_PATTERNS = {
    "NSE": re.compile(r"^[A-Z0-9&][A-Z0-9&.-]*\.NS$"),
    "TSX": re.compile(r"^[A-Z0-9][A-Z0-9-]*\.TO$"),
    "US": re.compile(r"^[A-Z0-9][A-Z0-9-]*$"),
}


@dataclass(frozen=True)
class UniverseValidationPolicy:
    minimum_symbol_count: int
    maximum_change_ratio: float = 0.20

    def __post_init__(self) -> None:
        if self.minimum_symbol_count < 1:
            raise ValueError("minimum_symbol_count must be positive")
        if not 0 <= self.maximum_change_ratio <= 1:
            raise ValueError("maximum_change_ratio must be between 0 and 1")


@dataclass(frozen=True)
class UniverseValidationResult:
    accepted: bool
    symbol_count: int
    previous_symbol_count: int | None
    change_ratio: float | None
    errors: tuple[str, ...]
    warnings: tuple[str, ...] = ()

    def as_json(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "symbol_count": self.symbol_count,
            "previous_symbol_count": self.previous_symbol_count,
            "change_ratio": self.change_ratio,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class UniverseInstrumentState:
    canonical_instrument_id: str
    symbol: str
    exchange: str
    provider_symbol: str | None
    name: str | None
    currency: str | None
    source: str
    source_url: str | None
    first_seen_at: datetime
    last_seen_at: datetime
    is_active: bool
    inactive_at: datetime | None
    inactive_reason: str | None
    consecutive_missing_refreshes: int
    last_universe_snapshot_id: str
    present_in_snapshot: bool


@dataclass(frozen=True)
class UniverseLifecycleEvent:
    event_id: str
    canonical_instrument_id: str
    exchange: str
    event_type: str
    old_value: dict[str, Any] | None
    new_value: dict[str, Any] | None
    snapshot_id: str
    created_at: datetime


@dataclass(frozen=True)
class UniverseBackfillWorkItem:
    work_item_id: str
    idempotency_key: str
    canonical_instrument_id: str
    provider_symbol: str
    exchange: str
    window_start: date
    window_end: date
    created_at: datetime


@dataclass(frozen=True)
class UniverseReconciliationPlan:
    snapshot_id: str
    exchange: str
    fetched_at: datetime
    instruments: tuple[UniverseInstrumentState, ...]
    events: tuple[UniverseLifecycleEvent, ...]
    work_items: tuple[UniverseBackfillWorkItem, ...]

    @property
    def members(self) -> tuple[UniverseInstrumentState, ...]:
        return tuple(item for item in self.instruments if item.present_in_snapshot)


@dataclass(frozen=True)
class UniverseRefreshResult:
    snapshot_id: str
    exchange: str
    source: str
    status: str
    symbol_count: int
    validation: UniverseValidationResult | None
    events_written: int = 0
    work_items_queued: int = 0
    error_message: str | None = None


class UniverseSnapshotRepository(Protocol):
    def latest_accepted_universe_snapshot(self, exchange: str) -> Mapping[str, Any] | None:
        ...

    def universe_symbol_state(self, exchange: str) -> list[Mapping[str, Any]]:
        ...

    def record_universe_snapshot(
        self,
        *,
        snapshot_id: str,
        exchange: str,
        source: str,
        status: str,
        fetched_at: datetime,
        symbol_count: int,
        validation_json: Mapping[str, Any],
        error_message: str | None,
    ) -> None:
        ...

    def persist_accepted_universe_snapshot(
        self,
        *,
        plan: UniverseReconciliationPlan,
        source: str,
        validation_json: Mapping[str, Any],
    ) -> None:
        ...


def validate_universe_snapshot(
    symbols: Sequence[Symbol],
    exchange: str,
    policy: UniverseValidationPolicy,
    previous_symbol_count: int | None = None,
    *,
    allow_large_change: bool = False,
) -> UniverseValidationResult:
    canonical_exchange = canonical_equity_exchange(exchange)
    errors: list[str] = []
    warnings: list[str] = []
    symbol_count = len(symbols)

    if symbol_count < policy.minimum_symbol_count:
        errors.append(
            f"symbol_count_below_minimum:{symbol_count}<{policy.minimum_symbol_count}"
        )

    normalized_symbols = [_normalize_exchange_symbol(item.symbol) for item in symbols]
    provider_symbols = [_normalize_provider_symbol(item.yahoo_symbol) for item in symbols]
    duplicate_symbols = _duplicates(normalized_symbols)
    duplicate_provider_symbols = _duplicates(
        [item for item in provider_symbols if item is not None]
    )
    if duplicate_symbols:
        errors.append(f"duplicate_exchange_symbols:{','.join(duplicate_symbols[:20])}")
    if duplicate_provider_symbols:
        errors.append(
            f"duplicate_provider_symbols:{','.join(duplicate_provider_symbols[:20])}"
        )

    exchange_mismatches = sorted(
        {
            item.exchange
            for item in symbols
            if _canonical_exchange_or_none(item.exchange) != canonical_exchange
        }
    )
    if exchange_mismatches:
        errors.append(f"exchange_mismatch:{','.join(exchange_mismatches)}")

    missing_mappings = [
        normalized_symbols[index]
        for index, provider_symbol in enumerate(provider_symbols)
        if provider_symbol is None
    ]
    if missing_mappings:
        errors.append(f"missing_provider_symbol:{','.join(missing_mappings[:20])}")

    pattern = _PROVIDER_SYMBOL_PATTERNS[canonical_exchange]
    invalid_mappings = sorted(
        {
            provider_symbol
            for provider_symbol in provider_symbols
            if provider_symbol is not None and pattern.fullmatch(provider_symbol) is None
        }
    )
    if invalid_mappings:
        errors.append(f"invalid_provider_symbol:{','.join(invalid_mappings[:20])}")

    change_ratio = None
    if previous_symbol_count is not None and previous_symbol_count > 0:
        change_ratio = abs(symbol_count - previous_symbol_count) / previous_symbol_count
        if change_ratio > policy.maximum_change_ratio:
            message = (
                "symbol_count_change_exceeds_limit:"
                f"{change_ratio:.6f}>{policy.maximum_change_ratio:.6f}"
            )
            if allow_large_change:
                warnings.append(f"override:{message}")
            else:
                errors.append(message)

    return UniverseValidationResult(
        accepted=not errors,
        symbol_count=symbol_count,
        previous_symbol_count=previous_symbol_count,
        change_ratio=change_ratio,
        errors=tuple(errors),
        warnings=tuple(warnings),
    )


def reconcile_universe_snapshot(
    symbols: Sequence[Symbol],
    existing_rows: Sequence[Mapping[str, Any] | UniverseInstrumentState],
    *,
    exchange: str,
    snapshot_id: str,
    fetched_at: datetime,
    missing_snapshots_before_inactive: int = 2,
) -> UniverseReconciliationPlan:
    if missing_snapshots_before_inactive < 2:
        raise ValueError("missing_snapshots_before_inactive must be at least 2")
    canonical_exchange = canonical_equity_exchange(exchange)
    observed_at = _as_utc(fetched_at)
    existing = {
        state.symbol: state
        for row in existing_rows
        if (state := _state_from_existing(row, canonical_exchange)) is not None
    }
    current = {
        _normalize_exchange_symbol(symbol.symbol): symbol
        for symbol in symbols
    }
    instruments: list[UniverseInstrumentState] = []
    events: list[UniverseLifecycleEvent] = []
    work_items: list[UniverseBackfillWorkItem] = []

    for exchange_symbol in sorted(current):
        symbol = current[exchange_symbol]
        provider_symbol = _normalize_provider_symbol(symbol.yahoo_symbol)
        previous = existing.get(exchange_symbol)
        canonical_id = (
            previous.canonical_instrument_id
            if previous is not None
            else canonical_instrument_id(canonical_exchange, exchange_symbol)
        )
        first_seen_at = previous.first_seen_at if previous is not None else observed_at
        state = UniverseInstrumentState(
            canonical_instrument_id=canonical_id,
            symbol=exchange_symbol,
            exchange=canonical_exchange,
            provider_symbol=provider_symbol,
            name=symbol.name,
            currency=symbol.currency,
            source=symbol.source,
            source_url=symbol.source_url,
            first_seen_at=first_seen_at,
            last_seen_at=observed_at,
            is_active=True,
            inactive_at=None,
            inactive_reason=None,
            consecutive_missing_refreshes=0,
            last_universe_snapshot_id=snapshot_id,
            present_in_snapshot=True,
        )
        instruments.append(state)

        if previous is None or not previous.last_universe_snapshot_id:
            events.append(_event("added", state, snapshot_id, observed_at, None))
            if provider_symbol:
                work_items.append(_new_symbol_backfill(state, observed_at))
        elif not previous.is_active:
            events.append(
                _event(
                    "reactivated",
                    state,
                    snapshot_id,
                    observed_at,
                    _state_json(previous),
                )
            )
            if provider_symbol:
                work_items.append(_new_symbol_backfill(state, observed_at))
        elif previous.consecutive_missing_refreshes > 0:
            events.append(
                _event(
                    "active_confirmed",
                    state,
                    snapshot_id,
                    observed_at,
                    _state_json(previous),
                )
            )

        if (
            previous is not None
            and previous.provider_symbol
            and previous.provider_symbol != provider_symbol
        ):
            events.append(
                _event(
                    "provider_mapping_changed",
                    state,
                    snapshot_id,
                    observed_at,
                    {"provider_symbol": previous.provider_symbol},
                    {"provider_symbol": provider_symbol},
                )
            )

    for exchange_symbol in sorted(set(existing) - set(current)):
        previous = existing[exchange_symbol]
        if not previous.last_universe_snapshot_id:
            continue
        missing_count = previous.consecutive_missing_refreshes + 1
        deactivate = missing_count >= missing_snapshots_before_inactive
        state = UniverseInstrumentState(
            canonical_instrument_id=previous.canonical_instrument_id,
            symbol=previous.symbol,
            exchange=canonical_exchange,
            provider_symbol=previous.provider_symbol,
            name=previous.name,
            currency=previous.currency,
            source=previous.source,
            source_url=previous.source_url,
            first_seen_at=previous.first_seen_at,
            last_seen_at=previous.last_seen_at,
            is_active=not deactivate,
            inactive_at=(
                previous.inactive_at
                if deactivate and not previous.is_active and previous.inactive_at is not None
                else observed_at if deactivate else None
            ),
            inactive_reason=(
                "absent_from_consecutive_snapshots" if deactivate else "suspected_inactive"
            ),
            consecutive_missing_refreshes=missing_count,
            last_universe_snapshot_id=snapshot_id,
            present_in_snapshot=False,
        )
        instruments.append(state)
        if missing_count == 1:
            events.append(
                _event(
                    "suspected_inactive",
                    state,
                    snapshot_id,
                    observed_at,
                    _state_json(previous),
                )
            )
        elif deactivate and previous.is_active:
            events.append(
                _event(
                    "deactivated",
                    state,
                    snapshot_id,
                    observed_at,
                    _state_json(previous),
                )
            )

    return UniverseReconciliationPlan(
        snapshot_id=snapshot_id,
        exchange=canonical_exchange,
        fetched_at=observed_at,
        instruments=tuple(instruments),
        events=tuple(events),
        work_items=tuple(work_items),
    )


class PersistedUniverseService:
    def __init__(
        self,
        repository: UniverseSnapshotRepository,
        *,
        missing_snapshots_before_inactive: int = 2,
    ) -> None:
        self.repository = repository
        self.missing_snapshots_before_inactive = missing_snapshots_before_inactive

    def refresh(
        self,
        provider: UniverseProvider,
        policy: UniverseValidationPolicy,
        *,
        allow_large_change: bool = False,
        fetched_at: datetime | None = None,
        snapshot_id: str | None = None,
    ) -> UniverseRefreshResult:
        exchange = canonical_equity_exchange(provider.exchange)
        observed_at = _as_utc(fetched_at or datetime.now(UTC))
        resolved_snapshot_id = snapshot_id or str(uuid4())
        source = str(getattr(provider, "source", provider.__class__.__name__))
        try:
            symbols = provider.fetch()
        except Exception as exc:
            message = str(exc)
            self.repository.record_universe_snapshot(
                snapshot_id=resolved_snapshot_id,
                exchange=exchange,
                source=source,
                status="failed",
                fetched_at=observed_at,
                symbol_count=0,
                validation_json={"accepted": False, "errors": ["source_fetch_failed"]},
                error_message=message,
            )
            return UniverseRefreshResult(
                snapshot_id=resolved_snapshot_id,
                exchange=exchange,
                source=source,
                status="failed",
                symbol_count=0,
                validation=None,
                error_message=message,
            )

        previous = self.repository.latest_accepted_universe_snapshot(exchange)
        previous_count = int(previous["symbol_count"]) if previous is not None else None
        validation = validate_universe_snapshot(
            symbols,
            exchange,
            policy,
            previous_count,
            allow_large_change=allow_large_change,
        )
        if not validation.accepted:
            message = "; ".join(validation.errors)
            self.repository.record_universe_snapshot(
                snapshot_id=resolved_snapshot_id,
                exchange=exchange,
                source=source,
                status="rejected",
                fetched_at=observed_at,
                symbol_count=len(symbols),
                validation_json=validation.as_json(),
                error_message=message,
            )
            return UniverseRefreshResult(
                snapshot_id=resolved_snapshot_id,
                exchange=exchange,
                source=source,
                status="rejected",
                symbol_count=len(symbols),
                validation=validation,
                error_message=message,
            )

        plan = reconcile_universe_snapshot(
            symbols,
            self.repository.universe_symbol_state(exchange),
            exchange=exchange,
            snapshot_id=resolved_snapshot_id,
            fetched_at=observed_at,
            missing_snapshots_before_inactive=self.missing_snapshots_before_inactive,
        )
        self.repository.persist_accepted_universe_snapshot(
            plan=plan,
            source=source,
            validation_json=validation.as_json(),
        )
        return UniverseRefreshResult(
            snapshot_id=resolved_snapshot_id,
            exchange=exchange,
            source=source,
            status="accepted",
            symbol_count=len(plan.members),
            validation=validation,
            events_written=len(plan.events),
            work_items_queued=len(plan.work_items),
        )


def canonical_instrument_id(exchange: str, symbol: str) -> str:
    identity = f"trade-research:equity:{canonical_equity_exchange(exchange)}:{symbol.upper()}"
    return f"eq_{uuid5(NAMESPACE_URL, identity).hex}"


def _state_from_existing(
    row: Mapping[str, Any] | UniverseInstrumentState,
    exchange: str,
) -> UniverseInstrumentState | None:
    if isinstance(row, UniverseInstrumentState):
        return row if row.exchange == exchange else None
    row_exchange = _canonical_exchange_or_none(str(row.get("exchange") or ""))
    if row_exchange != exchange:
        return None
    symbol = _normalize_exchange_symbol(str(row.get("symbol") or ""))
    if not symbol:
        return None
    first_seen = row.get("first_seen_at") or row.get("fetched_at") or datetime.now(UTC)
    last_seen = row.get("last_seen_at") or row.get("fetched_at") or first_seen
    return UniverseInstrumentState(
        canonical_instrument_id=str(
            row.get("canonical_instrument_id") or canonical_instrument_id(exchange, symbol)
        ),
        symbol=symbol,
        exchange=exchange,
        provider_symbol=_normalize_provider_symbol(row.get("yahoo_symbol")),
        name=_optional_string(row.get("name")),
        currency=_optional_string(row.get("currency")),
        source=str(row.get("source") or "unknown"),
        source_url=_optional_string(row.get("source_url")),
        first_seen_at=_as_utc(first_seen),
        last_seen_at=_as_utc(last_seen),
        is_active=bool(row.get("is_active", True)),
        inactive_at=_as_utc(row["inactive_at"]) if row.get("inactive_at") else None,
        inactive_reason=_optional_string(row.get("inactive_reason")),
        consecutive_missing_refreshes=int(row.get("consecutive_missing_refreshes") or 0),
        last_universe_snapshot_id=str(row.get("last_universe_snapshot_id") or ""),
        present_in_snapshot=False,
    )


def _event(
    event_type: str,
    state: UniverseInstrumentState,
    snapshot_id: str,
    created_at: datetime,
    old_value: dict[str, Any] | None,
    new_value: dict[str, Any] | None = None,
) -> UniverseLifecycleEvent:
    event_identity = f"{snapshot_id}:{state.canonical_instrument_id}:{event_type}"
    return UniverseLifecycleEvent(
        event_id=str(uuid5(NAMESPACE_URL, event_identity)),
        canonical_instrument_id=state.canonical_instrument_id,
        exchange=state.exchange,
        event_type=event_type,
        old_value=old_value,
        new_value=new_value or _state_json(state),
        snapshot_id=snapshot_id,
        created_at=created_at,
    )


def _new_symbol_backfill(
    state: UniverseInstrumentState,
    created_at: datetime,
) -> UniverseBackfillWorkItem:
    window_end = created_at.date()
    window_start = _subtract_years(window_end, 10)
    idempotency_key = work_item_idempotency_key(
        provider="yfinance",
        work_type="new_symbol_backfill",
        canonical_instrument_id=state.canonical_instrument_id,
        interval="1d",
        window_start=window_start,
        window_end=window_end,
    )
    return UniverseBackfillWorkItem(
        work_item_id=str(uuid5(NAMESPACE_URL, idempotency_key)),
        idempotency_key=idempotency_key,
        canonical_instrument_id=state.canonical_instrument_id,
        provider_symbol=str(state.provider_symbol),
        exchange=state.exchange,
        window_start=window_start,
        window_end=window_end,
        created_at=created_at,
    )


def _state_json(state: UniverseInstrumentState) -> dict[str, Any]:
    payload = asdict(state)
    for key in ("first_seen_at", "last_seen_at", "inactive_at"):
        value = payload[key]
        payload[key] = value.isoformat() if value is not None else None
    return payload


def _duplicates(values: Sequence[str]) -> list[str]:
    return sorted(value for value, count in Counter(values).items() if count > 1)


def _normalize_exchange_symbol(value: str) -> str:
    return value.strip().upper()


def _normalize_provider_symbol(value: object) -> str | None:
    normalized = str(value or "").strip().upper()
    return normalized or None


def _canonical_exchange_or_none(value: str) -> str | None:
    try:
        return canonical_equity_exchange(value)
    except ValueError:
        return None


def _optional_string(value: object) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _subtract_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(month=2, day=28, year=value.year - years)
