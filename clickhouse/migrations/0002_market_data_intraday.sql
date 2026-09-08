ALTER TABLE {{database}}.ohlcv_daily
    ADD COLUMN IF NOT EXISTS provider_symbol String AFTER symbol;

ALTER TABLE {{database}}.ohlcv_daily
    ADD COLUMN IF NOT EXISTS currency LowCardinality(String) AFTER provider_symbol;

ALTER TABLE {{database}}.ohlcv_daily
    ADD COLUMN IF NOT EXISTS provider_timestamp DateTime64(6, 'UTC') AFTER source;

ALTER TABLE {{database}}.ohlcv_daily
    ADD COLUMN IF NOT EXISTS request_id String AFTER provider_timestamp;

ALTER TABLE {{database}}.ohlcv_daily
    ADD COLUMN IF NOT EXISTS raw_artifact_id String AFTER request_id;

ALTER TABLE {{database}}.ohlcv_daily
    ADD COLUMN IF NOT EXISTS adapter_version String AFTER raw_artifact_id;

CREATE TABLE IF NOT EXISTS {{database}}.ohlcv_intraday
(
    workspace_id String DEFAULT 'default',
    instrument_id String,
    exchange LowCardinality(String),
    symbol String,
    provider_symbol String,
    currency LowCardinality(String),
    candle_timestamp DateTime64(6, 'UTC'),
    session_date Date,
    interval LowCardinality(String),
    open Decimal(20, 8),
    high Decimal(20, 8),
    low Decimal(20, 8),
    close Decimal(20, 8),
    volume UInt64,
    source LowCardinality(String),
    provider_timestamp DateTime64(6, 'UTC'),
    request_id String,
    raw_artifact_id String,
    adapter_version String,
    source_run_id String,
    content_sha256 FixedString(64),
    version UInt64,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(session_date)
ORDER BY (
    workspace_id,
    exchange,
    instrument_id,
    interval,
    candle_timestamp,
    source
);
