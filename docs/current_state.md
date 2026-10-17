---
document_status: current
last_verified_commit: 312680b274bb895f9307bc42929ace49a39fb29c
last_verified_date: 2026-10-17
owner: trade-data
replaced_by: null
---

# Daily data repository status

The daily-only backend and independent frontend were deployed on 17 October
2026. This document describes the verified source revision above and that
production cutover. Operational counts below are point-in-time observations.

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

The production backend uses `trade_data`; the independently deployed frontend
is [trade-web](https://github.com/ravic8/trade-web). Research is
[trade-research](https://github.com/ravic8/trade-research); archival source is in
[trade-fundamentals](https://github.com/ravic8/trade-fundamentals) and
[trade-market-experiments](https://github.com/ravic8/trade-market-experiments).
All four repositories are private and have independent initial commits/remotes.

The cutover snapshot contains 4,450,979 accepted NSE/yfinance daily rows across
2,597 instruments, from 18 July 2016 through 16 October 2026. On that last session,
2,554 eligible active instruments have accepted data, against 2,606 eligible active
instruments. The queue continues historical acquisition and gap repair; this does
not establish complete ten-year coverage for every instrument. Older accepted
history remains stored as the acquisition window rolls forward.

The live release passed bounded latest-session ingestion, an idempotent planner
rerun, historical replay, gateway/admin authorization and desktop/mobile browser
checks. PostgreSQL restoration and isolated encrypted MinIO/KES and ClickHouse
restoration passed. Recovery of 75,349 verified infrastructure-failed queue tasks
preserved their attempts and errors; provider exceptions remain explicit. Read
[the implementation journal](repository_split_progress.md) for final replica,
scheduled worker, release and recovery evidence.

Operational entrypoints: [minimal stack](minimal_stack.md), [runtime contract](runtime_contract.md),
[frontend](frontend_integration.md), [research export](research_snapshot_contract.md),
[daily canary](daily_canary.md), and [recovery](core_recovery.md).
