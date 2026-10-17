# Daily software and infrastructure boundaries

The [runtime contract](runtime_contract.md) defines service ownership. The daily
storage port lives in `src/trade_data/storage/daily_contract.py`; the worker's
composition point lives in `src/trade_data/pipelines/daily_runtime.py`.

PostgreSQL/TimescaleDB owns accepted OHLCV, universe lifecycle, exchange sessions,
work identity, claims, leases, retry state and provenance. Redis owns transient
rate-limit counters. A process restart must not lose queued daily work. SQLite
unit tests exercise bounded logic, but do not prove PostgreSQL locking or Timescale
behavior.

The worker's ClickHouse role can append accepted daily observations and reconcile
the resulting FINAL state. The API uses read credentials. Count/digest/watermark
proofs identify their source run and partition. Ingestion and historical replica
repair must preserve the serialization and lineage rules in the implementation
journal.

Object storage retains immutable, digest-verified raw evidence and optional export
artifacts. Existing bucket names and historical namespace definitions remain
compatible. They do not register research execution. Preserve managed KES keys,
configuration and certificates with encrypted object recovery; external KMS
recovery is the operator's responsibility.

The deployed physical PostgreSQL database and ClickHouse schema names remain
`trade_research` and `research`. Stable instrument IDs and applied migration
revision IDs also remain unchanged. Renaming the Python package does not rename
stored data or authorize dropping historical tables.

See [daily scope](v1_spec.md), [recovery](core_recovery.md),
[frontend contract](frontend_integration.md) and [snapshot contract](research_snapshot_contract.md).
