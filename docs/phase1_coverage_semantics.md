---
document_status: current
last_verified_commit: working-tree
last_verified_date: 2026-08-25
owner: trade-research-platform
replaced_by: null
---

# Phase 1 Eligible-Session Coverage Semantics

## Contract

Daily coverage is:

```text
valid stored eligible sessions / expected eligible sessions
```

Both counts are first-class evidence. `coverage_ratio` is derived from them and
is `0.0` when the denominator is zero; a zero denominator must be interpreted
with its exclusion evidence rather than as complete coverage.

`evaluate_eligible_session_coverage` is the deterministic reference model. It
returns exact date sets as well as counts so callers can generate missing-work
windows without reverse-engineering a percentage.

## Denominator

The base set is exchange sessions inside the requested date range. Rules are
applied in this order, with one exclusion reason assigned to each removed
session:

1. before evidence-backed listing start;
2. after evidence-backed delisting end;
3. outside point-in-time universe membership;
4. inside the provider grace period;
5. evidence-backed halt or suspension.

Provider unavailability does not shrink the denominator. It classifies a
missing eligible session as explained; the session remains missing. This keeps
provider limitations visible instead of converting absence into apparent
coverage.

The first stored candle is not a listing boundary. Deriving the denominator
from first stored data is circular and can hide missing early history.

## Numerator

Only stored dates that are both eligible and quality-valid enter the numerator.
For current daily OHLCV storage, quality-valid means `quality_status = 'ok'`.
The evidence separately reports:

- invalid stored eligible sessions;
- off-calendar stored sessions;
- explained missing sessions;
- actionable missing sessions.

Invalid stored dates remain missing for coverage purposes and therefore remain
actionable unless separate evidence explains their absence.

## Coverage preview API

`GET /api/data/coverage` and `POST /api/data/coverage/preview` now expose, both
in aggregate and per instrument:

- `requested_exchange_sessions`;
- `expected_eligible_sessions`;
- `valid_stored_eligible_sessions`;
- `invalid_stored_eligible_sessions`;
- `missing_eligible_sessions`;
- `explained_missing_sessions`;
- `actionable_missing_sessions`;
- `off_calendar_stored_sessions`;
- `coverage_ratio`;
- exclusion counts by reason.

The existing `expected_rows`, `already_present_rows`, and `missing_rows` fields
remain compatibility aliases for the explicit denominator, numerator, and
missing count. Fetch-preview tasks are generated only for actionable missing
dates.

## Current adoption boundary

This increment adopts the reference model in the read-only coverage preview
API and provides lifecycle, point-in-time membership, grace, invalid-row,
off-calendar, and explained-missing fixtures.

The broader `/api/data/availability` PostgreSQL aggregate queries still expose
legacy names. The seeded Yahoo query already calendar-matches stored dates and
separates provider-unavailable rows; the Upstox aggregate still needs the same
explicit expected-session relation before those endpoints can claim the full
WP1.3 contract. That migration is intentionally recorded rather than hidden.
