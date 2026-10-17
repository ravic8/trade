---
document_status: current
last_verified_commit: 32cac48a91324483253b9ab054c5c97cb4aab04e
last_verified_date: 2026-10-16
owner: trade-data
replaced_by: null
---

# Daily equity data scope

This is the current scope agreed during the repository split. Earlier research,
filing and UI specifications are archival and do not govern this backend.

- Acquire all NSE EQ symbols through yfinance, with no liquidity or top-N filter.
- Maintain a rolling ten-year acquisition window, daily increments, new listings
  and historical gap repair. Preserve older stored observations.
- Retain canonical identity, provider provenance, validated exchange calendars,
  quality quarantine, request evidence and explicit coverage exceptions.
- Use PostgreSQL/TimescaleDB as authority and reconcile ClickHouse by counts and
  digests. Durable queue claims and retries must survive process restarts.
- Serve operational HTTP reads and guarded daily-work requests. Frontend and
  research builds must be independent, consuming HTTP and versioned file snapshots.
- Preserve historical migrations, data, encrypted objects and required keys.
  No repository split operation authorizes dropping production data or volumes.
- Keep TSX/US daily support dormant until separately activated. Keep old Upstox
  daily data source-labeled. Do not add ML, backtests, filings, chat or intraday work.

Acceptance requires unit/contract checks, independently installed packages,
container builds, isolated restore and fresh/upgrade migrations, validated snapshot
export, bounded real NSE canaries and verified live schedule state. The exact
completed and pending checks are in [the journal](repository_split_progress.md).
