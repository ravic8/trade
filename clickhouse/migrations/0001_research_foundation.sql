CREATE TABLE IF NOT EXISTS {{database}}.ohlcv_daily
(
    workspace_id String DEFAULT 'default',
    instrument_id String,
    exchange LowCardinality(String),
    symbol String,
    session_date Date,
    open Decimal(20, 8),
    high Decimal(20, 8),
    low Decimal(20, 8),
    close Decimal(20, 8),
    volume UInt64,
    source LowCardinality(String),
    source_run_id String,
    content_sha256 FixedString(64),
    version UInt64,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(session_date)
ORDER BY (workspace_id, exchange, instrument_id, session_date, source);

CREATE TABLE IF NOT EXISTS {{database}}.feature_observations_daily
(
    workspace_id String DEFAULT 'default',
    feature_key String,
    feature_version String,
    instrument_id String,
    session_date Date,
    value Nullable(Float64),
    dataset_snapshot_id String,
    source_run_id String,
    content_sha256 FixedString(64),
    version UInt64,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(session_date)
ORDER BY (workspace_id, feature_key, feature_version, instrument_id, session_date);

CREATE TABLE IF NOT EXISTS {{database}}.target_observations_daily
(
    workspace_id String DEFAULT 'default',
    target_key String,
    target_version String,
    instrument_id String,
    session_date Date,
    horizon_sessions UInt16,
    value Nullable(Float64),
    dataset_snapshot_id String,
    source_run_id String,
    content_sha256 FixedString(64),
    version UInt64,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(session_date)
ORDER BY (
    workspace_id,
    target_key,
    target_version,
    horizon_sessions,
    instrument_id,
    session_date
);

CREATE TABLE IF NOT EXISTS {{database}}.factor_statistics
(
    workspace_id String DEFAULT 'default',
    experiment_run_id String,
    factor_key String,
    factor_version String,
    as_of_date Date,
    universe_key String,
    statistic LowCardinality(String),
    value Nullable(Float64),
    sample_count UInt64,
    source_run_id String,
    version UInt64,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(as_of_date)
ORDER BY (
    workspace_id,
    experiment_run_id,
    factor_key,
    factor_version,
    as_of_date,
    universe_key,
    statistic
);

CREATE TABLE IF NOT EXISTS {{database}}.feature_distributions
(
    workspace_id String DEFAULT 'default',
    dataset_snapshot_id String,
    feature_key String,
    feature_version String,
    as_of_date Date,
    universe_key String,
    count UInt64,
    missing_count UInt64,
    mean Nullable(Float64),
    stddev Nullable(Float64),
    minimum Nullable(Float64),
    p25 Nullable(Float64),
    median Nullable(Float64),
    p75 Nullable(Float64),
    maximum Nullable(Float64),
    version UInt64,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(as_of_date)
ORDER BY (
    workspace_id,
    dataset_snapshot_id,
    feature_key,
    feature_version,
    as_of_date,
    universe_key
);

CREATE TABLE IF NOT EXISTS {{database}}.predictions_daily
(
    workspace_id String DEFAULT 'default',
    model_version_id String,
    dataset_snapshot_id String,
    instrument_id String,
    prediction_date Date,
    horizon_sessions UInt16,
    score Float64,
    rank Nullable(UInt32),
    source_run_id String,
    content_sha256 FixedString(64),
    version UInt64,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(prediction_date)
ORDER BY (
    workspace_id,
    model_version_id,
    prediction_date,
    horizon_sessions,
    instrument_id
);

CREATE TABLE IF NOT EXISTS {{database}}.backtest_returns_daily
(
    workspace_id String DEFAULT 'default',
    experiment_run_id String,
    session_date Date,
    gross_return Float64,
    net_return Float64,
    turnover Float64,
    transaction_cost Float64,
    equity Float64,
    source_run_id String,
    version UInt64,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(session_date)
ORDER BY (workspace_id, experiment_run_id, session_date);

CREATE TABLE IF NOT EXISTS {{database}}.backtest_positions_daily
(
    workspace_id String DEFAULT 'default',
    experiment_run_id String,
    session_date Date,
    instrument_id String,
    quantity Float64,
    price Decimal(20, 8),
    market_value Float64,
    weight Float64,
    source_run_id String,
    version UInt64,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(session_date)
ORDER BY (workspace_id, experiment_run_id, session_date, instrument_id);

CREATE TABLE IF NOT EXISTS {{database}}.experiment_metrics
(
    workspace_id String DEFAULT 'default',
    experiment_run_id String,
    metric_key String,
    metric_value Nullable(Float64),
    metric_text Nullable(String),
    split LowCardinality(String),
    step Nullable(UInt64),
    measured_at DateTime64(6, 'UTC'),
    source_run_id String,
    version UInt64,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(measured_at)
ORDER BY (
    workspace_id,
    experiment_run_id,
    metric_key,
    split,
    measured_at,
    source_run_id
);

CREATE TABLE IF NOT EXISTS {{database}}.data_quality_results
(
    workspace_id String DEFAULT 'default',
    validation_run_id String,
    check_key String,
    check_version String,
    subject_type LowCardinality(String),
    subject_id String,
    status LowCardinality(String),
    observed_value Nullable(Float64),
    expected_value Nullable(Float64),
    details_json String,
    measured_at DateTime64(6, 'UTC'),
    source_run_id String,
    version UInt64,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(measured_at)
ORDER BY (
    workspace_id,
    validation_run_id,
    check_key,
    subject_type,
    subject_id,
    measured_at
);
