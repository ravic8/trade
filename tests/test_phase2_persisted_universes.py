from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from trade_research.config import Settings
from trade_research.schemas import Symbol
from trade_research.universe.persisted import (
    PersistedUniverseService,
    UniverseInstrumentState,
    UniverseValidationPolicy,
    canonical_instrument_id,
    reconcile_universe_snapshot,
    validate_universe_snapshot,
)


def _symbol(
    symbol: str,
    *,
    exchange: str = "US",
    provider_symbol: str | None = None,
) -> Symbol:
    currencies = {"NSE": "INR", "TSX": "CAD", "US": "USD"}
    return Symbol(
        symbol=symbol,
        exchange=exchange,
        yahoo_symbol=provider_symbol or symbol.replace(".", "-"),
        name=f"{symbol} Incorporated",
        currency=currencies[exchange],
        source="test_directory",
        source_url="https://example.test/symbols",
    )


class MutableProvider:
    exchange = "US"
    source = "test_directory"

    def __init__(self, symbols: list[Symbol]) -> None:
        self.symbols = symbols
        self.error: Exception | None = None

    def fetch(self) -> list[Symbol]:
        if self.error is not None:
            raise self.error
        return self.symbols


class MemoryRepository:
    def __init__(self) -> None:
        self.snapshots: list[dict[str, Any]] = []
        self.state: list[dict[str, Any]] = []
        self.plans = []

    def latest_accepted_universe_snapshot(self, exchange: str):
        accepted = [
            row
            for row in self.snapshots
            if row["exchange"] == exchange and row["status"] == "accepted"
        ]
        return accepted[-1] if accepted else None

    def universe_symbol_state(self, exchange: str):
        return [row for row in self.state if row["exchange"] == exchange]

    def record_universe_snapshot(self, **kwargs) -> None:
        self.snapshots.append(dict(kwargs))

    def persist_accepted_universe_snapshot(self, *, plan, source, validation_json) -> None:
        self.snapshots.append(
            {
                "snapshot_id": plan.snapshot_id,
                "exchange": plan.exchange,
                "source": source,
                "status": "accepted",
                "symbol_count": len(plan.members),
                "validation_json": dict(validation_json),
            }
        )
        self.state = []
        for item in plan.instruments:
            row = asdict(item)
            row["yahoo_symbol"] = row.pop("provider_symbol")
            row["fetched_at"] = plan.fetched_at
            self.state.append(row)
        self.plans.append(plan)


def test_phase2_validation_defaults_are_conservative() -> None:
    settings = Settings(_env_file=None)

    assert settings.equity_universe_minimum_nse_symbols == 1_000
    assert settings.equity_universe_minimum_tsx_symbols == 500
    assert settings.equity_universe_minimum_us_symbols == 3_000
    assert settings.equity_universe_maximum_change_ratio == 0.20
    assert settings.equity_universe_missing_snapshots_before_inactive == 2
    assert settings.yfinance_daily_enabled is False


def test_snapshot_validation_rejects_truncation_duplicates_and_bad_mappings() -> None:
    policy = UniverseValidationPolicy(minimum_symbol_count=2, maximum_change_ratio=0.20)
    symbols = [
        _symbol("AAPL"),
        _symbol("AAPL", provider_symbol="INVALID.TO"),
    ]

    result = validate_universe_snapshot(
        symbols,
        "US",
        policy,
        previous_symbol_count=10,
    )

    assert result.accepted is False
    assert any(error.startswith("duplicate_exchange_symbols") for error in result.errors)
    assert any(error.startswith("invalid_provider_symbol") for error in result.errors)
    assert any(error.startswith("symbol_count_change_exceeds_limit") for error in result.errors)


def test_large_change_override_does_not_bypass_other_validation_rules() -> None:
    policy = UniverseValidationPolicy(minimum_symbol_count=3, maximum_change_ratio=0.10)

    result = validate_universe_snapshot(
        [_symbol("AAPL"), _symbol("MSFT")],
        "US",
        policy,
        previous_symbol_count=10,
        allow_large_change=True,
    )

    assert result.accepted is False
    assert result.errors == ("symbol_count_below_minimum:2<3",)
    assert result.warnings[0].startswith("override:symbol_count_change_exceeds_limit")


def test_two_missing_snapshots_deactivate_and_reappearance_reactivates() -> None:
    repository = MemoryRepository()
    provider = MutableProvider([_symbol("AAPL"), _symbol("MSFT")])
    service = PersistedUniverseService(repository)
    policy = UniverseValidationPolicy(minimum_symbol_count=1, maximum_change_ratio=1.0)
    times = [
        datetime(2026, 7, 14, tzinfo=UTC),
        datetime(2026, 7, 15, tzinfo=UTC),
        datetime(2026, 7, 16, tzinfo=UTC),
        datetime(2026, 7, 17, tzinfo=UTC),
    ]

    first = service.refresh(provider, policy, fetched_at=times[0], snapshot_id="snapshot-1")
    provider.symbols = [_symbol("AAPL")]
    second = service.refresh(provider, policy, fetched_at=times[1], snapshot_id="snapshot-2")
    third = service.refresh(provider, policy, fetched_at=times[2], snapshot_id="snapshot-3")
    provider.symbols = [_symbol("AAPL"), _symbol("MSFT")]
    fourth = service.refresh(provider, policy, fetched_at=times[3], snapshot_id="snapshot-4")

    assert first.status == second.status == third.status == fourth.status == "accepted"
    assert first.work_items_queued == 2
    assert repository.plans[1].events[0].event_type == "suspected_inactive"
    missing_once = next(item for item in repository.plans[1].instruments if item.symbol == "MSFT")
    assert missing_once.is_active is True
    assert missing_once.consecutive_missing_refreshes == 1
    missing_twice = next(item for item in repository.plans[2].instruments if item.symbol == "MSFT")
    assert missing_twice.is_active is False
    assert missing_twice.inactive_reason == "absent_from_consecutive_snapshots"
    assert any(event.event_type == "deactivated" for event in repository.plans[2].events)
    still_missing = reconcile_universe_snapshot(
        [_symbol("AAPL")],
        repository.plans[2].instruments,
        exchange="US",
        snapshot_id="snapshot-still-missing",
        fetched_at=times[3],
    )
    still_inactive = next(item for item in still_missing.instruments if item.symbol == "MSFT")
    assert still_inactive.inactive_at == missing_twice.inactive_at
    assert not any(event.event_type == "deactivated" for event in still_missing.events)
    reactivated = next(item for item in repository.plans[3].instruments if item.symbol == "MSFT")
    assert reactivated.is_active is True
    assert reactivated.consecutive_missing_refreshes == 0
    assert any(event.event_type == "reactivated" for event in repository.plans[3].events)
    assert fourth.work_items_queued == 1


def test_rejected_snapshot_cannot_advance_missing_counts_or_deactivate() -> None:
    repository = MemoryRepository()
    provider = MutableProvider([_symbol("AAPL"), _symbol("MSFT")])
    service = PersistedUniverseService(repository)
    accepted_policy = UniverseValidationPolicy(minimum_symbol_count=2)
    service.refresh(provider, accepted_policy, snapshot_id="accepted")
    state_before = list(repository.state)

    provider.symbols = [_symbol("AAPL")]
    result = service.refresh(provider, accepted_policy, snapshot_id="truncated")

    assert result.status == "rejected"
    assert repository.state == state_before
    assert len(repository.plans) == 1
    assert repository.snapshots[-1]["status"] == "rejected"


def test_source_failure_is_recorded_without_reconciliation() -> None:
    repository = MemoryRepository()
    provider = MutableProvider([_symbol("AAPL")])
    provider.error = RuntimeError("provider unavailable")

    result = PersistedUniverseService(repository).refresh(
        provider,
        UniverseValidationPolicy(minimum_symbol_count=1),
        snapshot_id="failed",
    )

    assert result.status == "failed"
    assert result.error_message == "provider unavailable"
    assert repository.snapshots[0]["status"] == "failed"
    assert repository.plans == []


def test_mapping_change_preserves_canonical_identity_and_emits_event() -> None:
    fetched_at = datetime(2026, 7, 17, tzinfo=UTC)
    canonical_id = canonical_instrument_id("US", "BRK.B")
    existing = UniverseInstrumentState(
        canonical_instrument_id=canonical_id,
        symbol="BRK.B",
        exchange="US",
        provider_symbol="BRK.B",
        name="Berkshire Hathaway",
        currency="USD",
        source="test_directory",
        source_url="test",
        first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        last_seen_at=datetime(2026, 7, 16, tzinfo=UTC),
        is_active=True,
        inactive_at=None,
        inactive_reason=None,
        consecutive_missing_refreshes=0,
        last_universe_snapshot_id="previous",
        present_in_snapshot=True,
    )

    plan = reconcile_universe_snapshot(
        [_symbol("BRK.B", provider_symbol="BRK-B")],
        [existing],
        exchange="US",
        snapshot_id="current",
        fetched_at=fetched_at,
    )

    assert plan.members[0].canonical_instrument_id == canonical_id
    mapping_event = next(
        event for event in plan.events if event.event_type == "provider_mapping_changed"
    )
    assert mapping_event.old_value == {"provider_symbol": "BRK.B"}
    assert mapping_event.new_value == {"provider_symbol": "BRK-B"}
