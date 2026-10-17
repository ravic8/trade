---
document_status: current
last_verified_commit: 32cac48a91324483253b9ab054c5c97cb4aab04e
last_verified_date: 2026-10-16
owner: trade-data
replaced_by: null
---

# Daily data repository status

This document describes the local working tree on 16 October 2026, including
uncommitted split changes on the base commit above. It does not describe a
production deployment of those changes.

Trade now owns a Python daily equity data backend (`trade_data`, command
`trade-data`), a FastAPI operations API and Dagster planner/worker roles. The active
provider scope is all NSE EQ stocks through yfinance, without a liquidity filter.
The acquisition window is ten years. Incremental work, listing history, gap repair,
validation and replica reconciliation share the durable daily queue.

PostgreSQL/TimescaleDB is authoritative. ClickHouse is the validated analytical
replica, Redis supplies rate limits and S3-compatible storage retains raw evidence.
Historical table definitions, migration revision IDs, stable instrument identities
and existing data remain compatible. Old research storage profile/variable names
are retained for configuration compatibility; they do not enable research models.

The operations frontend lives in trade-web. Models, features, targets and backtests
live in trade-research, using versioned file snapshots. Both build independently.
The two other sibling repositories hold archival material. lens_fa and nifty_stream
are independent and untouched. Filing, chat, ML and intraday execution are absent
from the daily application. Retired specifications are preserved under
trade-research/migration/docs-before-daily-cleanup.

TSX/US daily adapters remain dormant. Exactly five NSE schedules are desired:
NSE sessions, NSE universe, shared daily planner, NSE completed-session planner,
and shared daily worker. All code schedule defaults remain stopped; reconciled
instance state controls activation. No provider work is enabled by API startup.

The running server remains on the separately repaired NSE recovery image. Its
previously verified accepted row count was 4,450,906 in each database; that is a
point-in-time audit, not a live total. The source split has not been deployed.
For current verification and outstanding acceptance gates, use
[the implementation journal](repository_split_progress.md).

Operational entrypoints: [minimal stack](minimal_stack.md), [runtime contract](runtime_contract.md),
[frontend](frontend_integration.md), [research export](research_snapshot_contract.md),
[daily canary](daily_canary.md), and [recovery](core_recovery.md).
