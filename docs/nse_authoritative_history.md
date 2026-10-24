# NSE-authoritative historical OHLCV

For 24 October 2025–23 October 2026 the approved one-year dataset uses
`source=nse_official_bhavcopy` in PostgreSQL and ClickHouse. It contains the actual
exchange observations for the current eligible ordinary main-market universe.
Original yfinance history is retained under its own source, including disputed
observations. Choose the NSE source for this historical period.

Prices are **unadjusted, as traded**. Volume is the original integer traded-share
count. Missing exchange prints are not filled with Yahoo placeholders. The scope
excludes a separate SME universe and does not claim every historical delisting.
Corporate-action-adjusted returns are outside this dataset's contract.

Read a stock through the existing backend:

```
GET /api/data/candles?provider=nse_official_bhavcopy&exchange=NSE&instrument_key=YF%7CRELIANCE.NS&start_date=2025-10-24&end_date=2026-10-23
```

The instrument key is an existing internal alias; `provider` identifies the source
of the returned values. Explicit NSE reads do not fall back to Yahoo outside the
published window or for dates without exchange prints. The existing endpoint's
provider default remains yfinance for compatibility; consumers of the corrected
one-year history must select `nse_official_bhavcopy` explicitly.

`operations.nse_authority.verify_nse_dataset` checks the approved artifact hash,
validated exchange calendar, source/basis, unique stock/date identities and every
OHLCV value against its original checksummed official NSE archive. Batch preparation
requires an unambiguous canonical identity and the same current ISIN as the approved
mapping, and stores raw snapshots with archive provenance. Imports are bounded and
reconciled by monthly source-specific row counts and digests. The export is read
back from PostgreSQL and checked against the exchange artifact before publication.
No yfinance adjustment factors are applied to exchange prices.

This is a historical import, not a new scheduled NSE collector. The five daily
NSE/yfinance schedules continue; TSX, US and legacy Upstox collectors remain off.
