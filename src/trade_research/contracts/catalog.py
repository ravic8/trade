from __future__ import annotations

from trade_research.contracts.models import (
    ColumnContract,
    DataContract,
    FreshnessContract,
    LogicalType,
)
from trade_research.contracts.registry import DataContractRegistry
from trade_research.features import FEATURE_VERSION_V1_0
from trade_research.features.daily_technical import FEATURE_COLUMNS_V1_0
from trade_research.modeling.ml_dataset_v1 import ML_DATASET_VERSION_V1
from trade_research.targets import (
    DAILY_FORWARD_TARGET_COLUMNS_V1_0,
    DAILY_FORWARD_TARGET_VERSION_V1_0,
    DAILY_OPPORTUNITY_TARGET_COLUMNS_V1_0,
    DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
)

ML_INPUTS_CONTRACT_ID = "research.ml_inputs.nse_daily.v1"


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


def _feature_unit(name: str) -> str:
    if name.startswith(("ret_", "log_ret_", "volatility_")):
        return "decimal_return"
    if name.startswith(("sma_", "ema_", "true_range", "atr_")) and not name.startswith("atr_pct"):
        return "quote_currency"
    if name.startswith("volume_avg"):
        return "shares"
    if name.startswith("turnover") and "ratio" not in name:
        return "quote_currency"
    return "ratio"


FEATURE_VALUE_COLUMNS = tuple(
    _column(
        name,
        "number",
        f"Frozen daily technical feature {name}.",
        nullable=True,
        units=_feature_unit(name),
    )
    for name in FEATURE_COLUMNS_V1_0
)

FORWARD_TARGET_VALUE_COLUMNS = tuple(
    _column(
        name,
        "boolean" if name == "top_quantile_forward_return_20d" else "number",
        f"Forward-looking research target {name}.",
        nullable=True,
        units=None if name == "top_quantile_forward_return_20d" else "decimal_return",
    )
    for name in DAILY_FORWARD_TARGET_COLUMNS_V1_0
)

OPPORTUNITY_VALUE_COLUMNS = tuple(
    _column(
        name,
        "number",
        f"Completed-session Opportunity outcome {name}.",
        nullable=True,
        units="decimal_return",
    )
    for name in DAILY_OPPORTUNITY_TARGET_COLUMNS_V1_0
)


DATA_CONTRACTS = (
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
    DataContract(
        contract_id="target.opportunity_daily.v1",
        dataset_version=DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
        domain="target",
        owner="trade-research-platform",
        description="Completed-session Opportunity outcomes derived from OHLC and previous close.",
        lifecycle="current",
        authoritative_store="postgresql",
        storage_name="opportunity_targets_daily",
        primary_key=("instrument_key", "source", "date", "target_version"),
        columns=(
            *IDENTITY_COLUMNS,
            *OHLCV_VALUE_COLUMNS,
            _column(
                "previous_close",
                "number",
                "Previous valid session close.",
                nullable=True,
                units="quote_currency",
            ),
            _column("target_version", "string", "Frozen Opportunity formula version."),
            _column("computed_at", "datetime", "UTC computation timestamp."),
            _column(
                "quality_status",
                "string",
                "Target row quality state.",
                allowed_values=("passed", "warning", "failed"),
            ),
            *OPPORTUNITY_VALUE_COLUMNS,
        ),
        freshness=FreshnessContract(
            mode="latest_completed_session",
            basis_column="date",
            max_lag_sessions=1,
            grace_minutes=300,
            description="Opportunity targets follow coverage-ready daily OHLCV.",
        ),
        invariants=(
            "outcomes use open as denominator",
            "previous_close is from the prior valid source-keyed session",
            "targets are unavailable before session close",
        ),
    ),
    DataContract(
        contract_id="feature.daily_technical.v1",
        dataset_version=FEATURE_VERSION_V1_0,
        domain="feature",
        owner="trade-research-platform",
        description="Frozen V1 daily technical feature layer.",
        lifecycle="transitional",
        authoritative_store="postgresql",
        storage_name="features_daily",
        replicas=("processed/features/daily_v1_ohlcv_technical.parquet",),
        primary_key=("instrument_key", "date", "feature_version"),
        columns=(
            *IDENTITY_COLUMNS,
            *OHLCV_VALUE_COLUMNS,
            _column("feature_version", "string", "Frozen feature formula version."),
            _column("computed_at", "datetime", "UTC computation timestamp."),
            _column(
                "quality_status",
                "string",
                "Feature row quality state.",
                allowed_values=("passed", "warning", "failed"),
            ),
            *FEATURE_VALUE_COLUMNS,
        ),
        freshness=FreshnessContract(
            mode="latest_completed_session",
            basis_column="date",
            max_lag_sessions=1,
            grace_minutes=360,
            description="Feature materialization follows validated daily OHLCV.",
        ),
        invariants=(
            "feature keys must align with validated OHLCV keys",
            "feature values cannot be infinite",
            "rolling-window nulls must be represented by warning quality status",
            "target columns cannot appear in the feature set",
        ),
        limitations=("The ML pipeline still consumes the Parquet replica.",),
    ),
    DataContract(
        contract_id="target.daily_forward_returns.v1",
        dataset_version=DAILY_FORWARD_TARGET_VERSION_V1_0,
        domain="target",
        owner="trade-research-platform",
        description="Frozen V1 forward-return and cross-sectional labels.",
        lifecycle="transitional",
        authoritative_store="postgresql",
        storage_name="targets_daily",
        replicas=("processed/targets/daily_v1_forward_returns.parquet",),
        primary_key=("instrument_key", "date", "target_version"),
        columns=(
            *IDENTITY_COLUMNS,
            _column("target_version", "string", "Frozen target formula version."),
            _column("computed_at", "datetime", "UTC computation timestamp."),
            _column(
                "quality_status",
                "string",
                "Target row quality state.",
                allowed_values=("passed", "warning", "failed"),
            ),
            *FORWARD_TARGET_VALUE_COLUMNS,
        ),
        freshness=FreshnessContract(
            mode="latest_completed_session",
            basis_column="date",
            max_lag_sessions=1,
            grace_minutes=360,
            description="Target rows are generated for each validated OHLCV session.",
        ),
        invariants=(
            "target keys must align with validated OHLCV keys",
            "target values cannot be infinite",
            "horizon-end nulls are expected and must remain non-trainable",
        ),
        limitations=("The ML pipeline still consumes the Parquet replica.",),
    ),
    DataContract(
        contract_id=ML_INPUTS_CONTRACT_ID,
        dataset_version="nse_daily_ml_inputs_v1",
        domain="dataset",
        owner="trade-research-platform",
        description="Aligned validated OHLCV, feature, target, and stock-coverage inputs.",
        lifecycle="transitional",
        authoritative_store="dataset_bundle",
        storage_name="processed/{validated,features,targets,validation}",
        primary_key=("instrument_key", "date"),
        columns=IDENTITY_COLUMNS,
        freshness=FreshnessContract(
            mode="latest_completed_session",
            basis_column="date",
            max_lag_sessions=1,
            grace_minutes=360,
            description="All ML inputs must align through the latest accepted research session.",
        ),
        invariants=(
            "component keys are unique",
            "feature and target keys align with cleaned OHLCV",
            "stock coverage is generated in the same validation run",
            "baseline ML readiness is explicitly validated",
        ),
        limitations=(
            "This is a transitional filesystem bundle rather than a registered artifact manifest.",
            "Coverage eligibility is still static full-history rather than point-in-time.",
        ),
    ),
    DataContract(
        contract_id="dataset.ml_daily.v1",
        dataset_version=ML_DATASET_VERSION_V1,
        domain="dataset",
        owner="trade-research-platform",
        description="Leakage-aware daily ML dataset with chronological split metadata.",
        lifecycle="transitional",
        authoritative_store="local_parquet",
        storage_name="processed/ml/ml_dataset_v1.parquet",
        primary_key=("instrument_key", "date", "ml_dataset_version"),
        columns=(
            *IDENTITY_COLUMNS,
            _column("ml_dataset_version", "string", "Frozen dataset-builder version."),
            _column("feature_version", "string", "Feature contract version.", nullable=True),
            _column("target_version", "string", "Target contract version.", nullable=True),
            _column("coverage_policy", "string", "Universe coverage eligibility policy."),
            _column(
                "coverage_pct_full_history",
                "number",
                "Static full-history coverage ratio.",
                units="ratio",
                minimum=0,
                maximum=1,
            ),
            _column("coverage_status", "string", "Coverage validation state."),
            _column(
                "split",
                "string",
                "Chronological evaluation split.",
                allowed_values=("train_seed", "validation_seed", "walk_forward_eval"),
            ),
            _column("is_trainable", "boolean", "Whether the row satisfies training requirements."),
            _column("exclusion_reasons", "string", "Semicolon-delimited exclusion reasons."),
            _column("feature_quality_status", "string", "Upstream feature quality.", nullable=True),
            _column("target_quality_status", "string", "Upstream target quality.", nullable=True),
            *FEATURE_VALUE_COLUMNS,
            _column(
                "forward_ret_1d",
                "number",
                "Next-session realized return target.",
                nullable=True,
                units="decimal_return",
            ),
            _column(
                "next_day_positive", "boolean", "Next-session positive-return label.", nullable=True
            ),
            _column(
                "next_day_top_decile", "boolean", "Next-session top-decile label.", nullable=True
            ),
            _column(
                "next_day_bottom_decile",
                "boolean",
                "Next-session bottom-decile label.",
                nullable=True,
            ),
            _column(
                "daily_forward_ret_1d_rank",
                "number",
                "Cross-sectional next-session return rank.",
                nullable=True,
                units="rank",
            ),
        ),
        freshness=FreshnessContract(
            mode="latest_completed_session",
            basis_column="date",
            max_lag_sessions=1,
            grace_minutes=420,
            description="Dataset follows validated features and targets.",
        ),
        invariants=(
            "feature columns exclude identifiers and all target columns",
            "only chronological splits are permitted",
            "rows with null targets or invalid features are non-trainable",
            "dataset keys are unique",
        ),
        limitations=(
            "Local Parquet is transitional application truth.",
            "Point-in-time universe eligibility is not implemented.",
        ),
    ),
    DataContract(
        contract_id="prediction.daily_rankings.v1",
        dataset_version="latest_predictions_v1",
        domain="prediction",
        owner="trade-research-platform",
        description="Per-model daily instrument scores and ranks.",
        lifecycle="transitional",
        authoritative_store="local_parquet",
        storage_name="processed/ml/latest_predictions_v1/latest_predictions.parquet",
        primary_key=("run_id", "model_id", "prediction_date", "instrument_key"),
        columns=(
            _column("run_id", "string", "Model execution family identity."),
            _column("model_id", "string", "Model or baseline identity."),
            _column("prediction_date", "date", "Feature session used for prediction."),
            _column("instrument_key", "string", "Predicted instrument identity."),
            _column("symbol", "string", "Exchange display symbol."),
            _column("date", "date", "Underlying dataset row date."),
            _column("fold_id", "string", "Walk-forward fold identity."),
            _column("score", "number", "Model ranking score.", units="model_score"),
            _column(
                "rank",
                "number",
                "Cross-sectional ascending rank position.",
                units="rank",
                minimum=1,
            ),
            _column(
                "realized_forward_ret_1d",
                "number",
                "Realized next-session return when known.",
                nullable=True,
                units="decimal_return",
            ),
            _column("selected_top_5", "boolean", "Whether selected in top five."),
            _column("selected_top_10", "boolean", "Whether selected in top ten."),
            _column("selected_top_20", "boolean", "Whether selected in top twenty."),
        ),
        freshness=FreshnessContract(
            mode="latest_completed_session",
            basis_column="prediction_date",
            max_lag_sessions=1,
            grace_minutes=480,
            description="Latest rankings follow the newest feature-complete session.",
        ),
        invariants=(
            "scores are finite",
            "rank is unique within model and prediction date",
            "prediction rows reference a versioned dataset and model run",
        ),
        limitations=(
            "Predictions are not yet stored in the experiment registry or object storage.",
        ),
    ),
    DataContract(
        contract_id="backtest.daily_returns.v1",
        dataset_version="prediction_backtest_v1",
        domain="backtest",
        owner="trade-research-platform",
        description="Daily gross and net returns for equal-weight top-N prediction portfolios.",
        lifecycle="transitional",
        authoritative_store="local_file",
        storage_name="processed/ml/backtests_v1/daily_portfolio_returns.csv",
        primary_key=("model_id", "top_n", "prediction_date"),
        columns=(
            _column("model_id", "string", "Evaluated model identity."),
            _column(
                "top_n", "integer", "Requested portfolio size.", units="instruments", minimum=1
            ),
            _column("prediction_date", "date", "Portfolio formation session."),
            _column(
                "selected_count",
                "integer",
                "Actual selected instruments.",
                units="instruments",
                minimum=0,
            ),
            _column(
                "gross_return", "number", "Equal-weight gross daily return.", units="decimal_return"
            ),
            _column(
                "turnover",
                "number",
                "One-way portfolio turnover ratio.",
                units="ratio",
                minimum=0,
                maximum=1,
            ),
            _column(
                "transaction_cost",
                "number",
                "Applied daily transaction cost.",
                units="decimal_return",
                minimum=0,
            ),
            _column(
                "net_return",
                "number",
                "Gross return less transaction cost.",
                units="decimal_return",
            ),
        ),
        freshness=FreshnessContract(
            mode="run_scoped",
            description=(
                "Backtest evidence is immutable for a versioned dataset, model, and config run."
            ),
        ),
        invariants=(
            "selected_count cannot exceed top_n",
            "net_return equals gross_return minus transaction_cost",
            "future rows cannot contribute to portfolio formation",
        ),
        limitations=("Backtest artifacts do not yet have a durable experiment-run identity.",),
    ),
)

DATA_CONTRACT_REGISTRY = DataContractRegistry(DATA_CONTRACTS)


def get_data_contract(contract_id: str) -> DataContract:
    return DATA_CONTRACT_REGISTRY.get(contract_id)
