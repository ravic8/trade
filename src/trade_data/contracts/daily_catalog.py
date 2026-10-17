"""Daily-data contracts independent of research and model packages."""

from __future__ import annotations

from trade_data.contracts.models import (
    ColumnContract,
    DataContract,
    FreshnessContract,
    LogicalType,
)
from trade_data.contracts.registry import DataContractRegistry


def _column(
    name: str,
    logical_type: LogicalType,
    description: str,
    *,
    nullable: bool = False,
    units: str | None = None,
    allowed_values: tuple[str | int | float | bool, ...] = (),
    minimum: float | None = None,
    maximum: float | None = None,
    exclusive_minimum: bool = False,
    exclusive_maximum: bool = False,
) -> ColumnContract:
    return ColumnContract(
        name=name,
        logical_type=logical_type,
        nullable=nullable,
        description=description,
        units=units,
        allowed_values=allowed_values,
        minimum=minimum,
        maximum=maximum,
        exclusive_minimum=exclusive_minimum,
        exclusive_maximum=exclusive_maximum,
    )


IDENTITY_COLUMNS = (
    _column("instrument_key", "string", "Provider-qualified instrument identity."),
    _column("symbol", "string", "Exchange display symbol."),
    _column(
        "exchange",
        "string",
        "Canonical exchange code.",
        allowed_values=("NSE", "TSX", "US"),
    ),
    _column("source", "string", "Market-data provider identity."),
    _column("date", "date", "Exchange-local trading session date."),
)

OHLCV_VALUE_COLUMNS = (
    _column(
        "open",
        "number",
        "Session open price.",
        units="quote_currency",
        minimum=0,
        exclusive_minimum=True,
    ),
    _column(
        "high",
        "number",
        "Session high price.",
        units="quote_currency",
        minimum=0,
        exclusive_minimum=True,
    ),
    _column(
        "low",
        "number",
        "Session low price.",
        units="quote_currency",
        minimum=0,
        exclusive_minimum=True,
    ),
    _column(
        "close",
        "number",
        "Session close price.",
        units="quote_currency",
        minimum=0,
        exclusive_minimum=True,
    ),
    _column("volume", "integer", "Session traded volume.", units="shares", minimum=0),
    _column(
        "open_interest",
        "integer",
        "Session open interest when supplied by the provider.",
        nullable=True,
        units="contracts",
        minimum=0,
    ),
)

DAILY_DATA_CONTRACTS = (
    DataContract(
        contract_id="calendar.exchange_sessions.v1",
        dataset_version="exchange_sessions_v1",
        domain="calendar",
        owner="trade-research-platform",
        description="Materialized exchange trading sessions and UTC market boundaries.",
        lifecycle="current",
        authoritative_store="postgresql",
        storage_name="exchange_sessions",
        primary_key=("exchange", "session_date"),
        columns=(
            _column("exchange", "string", "Canonical exchange code."),
            _column("session_date", "date", "Exchange-local calendar date."),
            _column("is_trading_day", "boolean", "Whether the exchange trades."),
            _column("market_open_utc", "datetime", "Session open in UTC.", nullable=True),
            _column("market_close_utc", "datetime", "Session close in UTC.", nullable=True),
            _column("is_early_close", "boolean", "Whether the session closes early."),
            _column("source_url", "string", "Calendar provenance URL."),
            _column("calendar_version", "string", "Calendar generator version."),
            _column(
                "validation_status",
                "string",
                "Calendar materialization validation status.",
                allowed_values=("passed", "warning", "failed"),
            ),
            _column("generated_at", "datetime", "UTC materialization timestamp."),
        ),
        freshness=FreshnessContract(
            mode="event_driven",
            basis_column="generated_at",
            description=(
                "Calendar rows are refreshed by scheduled materialization and rule changes."
            ),
        ),
        invariants=(
            "trading sessions require non-null UTC open and close timestamps",
            "market_close_utc must be later than market_open_utc",
            "session identity is unique by exchange and date",
        ),
    ),
    DataContract(
        contract_id="universe.snapshots.v1",
        dataset_version="canonical_universe_snapshot_v1",
        domain="universe",
        owner="trade-research-platform",
        description="Accepted or rejected canonical exchange-universe snapshots.",
        lifecycle="current",
        authoritative_store="postgresql",
        storage_name="universe_snapshots",
        primary_key=("snapshot_id",),
        columns=(
            _column("snapshot_id", "string", "Immutable snapshot identity."),
            _column("exchange", "string", "Canonical exchange code."),
            _column("source", "string", "Universe source identity."),
            _column(
                "status",
                "string",
                "Snapshot acceptance outcome.",
                allowed_values=("accepted", "rejected", "failed"),
            ),
            _column("fetched_at", "datetime", "UTC source retrieval timestamp."),
            _column("symbol_count", "integer", "Snapshot member count.", minimum=0),
            _column("validation_json", "json", "Structured reconciliation evidence."),
            _column("error_message", "string", "Blocking failure detail.", nullable=True),
            _column("created_at", "datetime", "UTC persistence timestamp."),
        ),
        freshness=FreshnessContract(
            mode="wall_clock",
            basis_column="fetched_at",
            max_age_minutes=2160,
            description="Each exchange requires a recently fetched canonical snapshot.",
        ),
        invariants=(
            "only accepted snapshots may become current universe authority",
            "accepted symbol_count must equal persisted member count",
            "snapshot_id is immutable",
        ),
    ),
    DataContract(
        contract_id="universe.snapshot_members.v1",
        dataset_version="canonical_universe_members_v1",
        domain="universe",
        owner="trade-research-platform",
        description="Point-in-time membership of canonical universe snapshots.",
        lifecycle="current",
        authoritative_store="postgresql",
        storage_name="universe_snapshot_members",
        primary_key=("snapshot_id", "canonical_instrument_id"),
        columns=(
            _column("snapshot_id", "string", "Parent universe snapshot identity."),
            _column("canonical_instrument_id", "string", "Stable canonical instrument identity."),
            _column("exchange_symbol", "string", "Symbol observed on the exchange."),
            _column("provider_symbol", "string", "Provider-specific symbol.", nullable=True),
            _column("name", "string", "Instrument display name.", nullable=True),
            _column("raw_metadata", "json", "Source-specific membership metadata."),
        ),
        freshness=FreshnessContract(
            mode="immutable",
            description="Membership rows are immutable within a snapshot.",
        ),
        invariants=(
            "membership is unique by snapshot and canonical instrument",
            "members must reference an existing snapshot",
        ),
    ),
    DataContract(
        contract_id="instrument.canonical_identity.v1",
        dataset_version="canonical_instrument_lifecycle_v1",
        domain="instrument",
        owner="trade-research-platform",
        description="Canonical instrument identity, lifecycle, and provider eligibility.",
        lifecycle="current",
        authoritative_store="postgresql",
        storage_name="symbols",
        primary_key=("symbol", "exchange"),
        columns=(
            _column("symbol", "string", "Canonical exchange symbol."),
            _column("exchange", "string", "Canonical exchange code."),
            _column(
                "canonical_instrument_id", "string", "Stable instrument identity.", nullable=True
            ),
            _column("provider_instrument_key", "string", "Active provider key.", nullable=True),
            _column(
                "listing_status",
                "string",
                "Evidence-backed listing lifecycle state.",
                allowed_values=("active", "inactive", "unknown"),
            ),
            _column(
                "listing_status_effective_at",
                "datetime",
                "Lifecycle effective time.",
                nullable=True,
            ),
            _column(
                "pipeline_eligibility",
                "string",
                "Permitted provider-work scope.",
                allowed_values=("backfill", "incremental", "none"),
            ),
            _column("is_active", "boolean", "Whether the symbol is active in current operations."),
            _column("fetched_at", "datetime", "Latest source refresh timestamp."),
        ),
        freshness=FreshnessContract(
            mode="event_driven",
            basis_column="fetched_at",
            description="Identity changes only through accepted universe reconciliation.",
        ),
        invariants=(
            "canonical instrument identity cannot be reassigned silently",
            "inactive instruments cannot remain pipeline eligible",
            "provider key changes require alias history",
        ),
        limitations=("Legacy rows may not yet have canonical_instrument_id populated.",),
    ),
    DataContract(
        contract_id="market_data.ohlcv_daily.v1",
        dataset_version="ohlcv_daily_source_keyed_v1",
        domain="market_data",
        owner="trade-research-platform",
        description="Provider-keyed daily adjusted or raw OHLCV observations.",
        lifecycle="current",
        authoritative_store="postgresql",
        storage_name="ohlcv_daily",
        replicas=("processed/equities/*.parquet",),
        primary_key=("instrument_key", "source", "date"),
        columns=(
            *IDENTITY_COLUMNS,
            *OHLCV_VALUE_COLUMNS,
            _column("fetched_at", "datetime", "UTC provider retrieval timestamp."),
            _column(
                "quality_status",
                "string",
                "Row-level OHLCV quality state.",
                allowed_values=("ok", "suspicious"),
            ),
        ),
        freshness=FreshnessContract(
            mode="latest_completed_session",
            basis_column="date",
            max_lag_sessions=1,
            grace_minutes=240,
            description="Eligible instruments must cover the latest completed NSE session.",
        ),
        invariants=(
            "open high low and close must be positive",
            "high must be at least open close and low",
            "low must be at most open close and high",
            "volume and open interest cannot be negative",
            "row keys must be unique within and across batches",
        ),
    ),
)

DAILY_DATA_CONTRACT_REGISTRY = DataContractRegistry(DAILY_DATA_CONTRACTS)
