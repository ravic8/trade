-- Run with an administrator identity, never with the exporter service account.
-- Both datasets must already exist in US. This script does not create datasets.

CREATE OR REPLACE VIEW `tradechain8.trade_chain8_reporting.ohlcv_daily` AS
SELECT * FROM `tradechain8.trade_chain8_analytics.ohlcv_daily`;

CREATE OR REPLACE VIEW `tradechain8.trade_chain8_reporting.symbols` AS
SELECT * FROM `tradechain8.trade_chain8_analytics.symbols`;

CREATE OR REPLACE VIEW `tradechain8.trade_chain8_reporting.exchange_sessions` AS
SELECT * FROM `tradechain8.trade_chain8_analytics.exchange_sessions`;

CREATE OR REPLACE VIEW `tradechain8.trade_chain8_reporting.pipeline_health` AS
SELECT * FROM `tradechain8.trade_chain8_analytics.pipeline_health`;

CREATE OR REPLACE VIEW `tradechain8.trade_chain8_reporting.ingestion_runs` AS
SELECT * FROM `tradechain8.trade_chain8_analytics.ingestion_runs`;

CREATE OR REPLACE VIEW `tradechain8.trade_chain8_reporting.provider_health` AS
SELECT * FROM `tradechain8.trade_chain8_analytics.provider_health`;

CREATE OR REPLACE VIEW `tradechain8.trade_chain8_reporting.universe_lifecycle` AS
SELECT * FROM `tradechain8.trade_chain8_analytics.universe_lifecycle`;
