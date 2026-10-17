# Real-data daily ingestion canary

The first local run and its observed evidence are recorded in
[`daily_canary_2026-10-02.md`](daily_canary_2026-10-02.md).

This procedure checks a small, explicitly selected set of real provider symbols.
It does not create sample market data. Choose the exchange, provider entitlement,
and exact symbols before enabling a provider call. The current bounded CLI path
is NSE/yfinance only; TSX and US are not ready merely because their adapters exist.

## Before a provider call

1. Run `trade-data doctor --check-services` and ensure migrations are at
   head. Confirm an accepted universe snapshot and provider-eligible exchange
   sessions exist for the chosen exchange.
2. Record the selected symbols and a completed, at-most-31-calendar-day window.
   Do not use a future or still-open market session.
3. Capture a read-only baseline, for example:

   ```sh
   trade-data daily-canary-snapshot NSE --symbols SYMBOL1.NS,SYMBOL2.NS \
     --from-date YYYY-MM-DD --to-date YYYY-MM-DD
   ```

   The snapshot reads the accepted active universe, validated exchange
   sessions, and stored `yfinance` daily rows with `quality_status=ok`. It makes
   no provider request and writes no work item. `missing_sessions` is a stored
   coverage gap, not proof that the provider offers history for that session.
   The latest eligible session uses the same configured provider grace period
   as the worker, so a just-closed session is not treated as ready prematurely.

4. Preview the bounded work plan with
   `trade-data plan-yfinance-nse-canary --symbols SYMBOL1.NS,SYMBOL2.NS
   --symbol-limit 2 --session-count 5 --dry-run`. The canary command now uses
   only the selected number of recent eligible sessions. A dry run does not
   enqueue work, but the planner may initialize database objects; use a
   disposable local environment when proving strict read-only behavior.

## Authorized execution and evidence

Only after reviewing the scope, enable the dedicated NSE canary and daily
worker flags for that local execution. Enqueue the same bounded plan and run a
worker with a small claim size. The worker may process other already queued
items, so inspect queue contents first and do not use a shared or production
queue for this procedure. Capture the CLI output, ingestion-run records,
provider errors/rate limits, and a second coverage snapshot for the same
window. Turn the flags off afterward.

For incremental behavior, wait until a newly completed session has been
materialized, then use the incremental planner under a separately reviewed
bounded scope. Its configured overlap intentionally re-fetches recent sessions
to catch revisions; it does not promise a fetch of *only* absent dates. A
repeat run with no newer session should create no incremental work for a
symbol already current, while unique work-item keys prevent duplicate enqueue.

The scoped NSE incremental path requires one to five explicit provider symbols:

```sh
trade-data plan-yfinance-nse-incremental-canary \
  --symbols RELIANCE.NS,COALINDIA.NS,ONGC.NS --dry-run
```

Review `latest_eligible_session` and every `planned_windows` entry. The
command plans no historical backfill or gap repair. With no new eligible
session, it reports zero work. It rejects symbols without valid seeded daily
coverage and any incremental fetch window longer than 31 calendar days; those
cases require a separately reviewed repair/backfill. To enqueue, enable
`YFINANCE_NSE_CANARY_ENABLED` only for that command and use `--enqueue`;
rerunning the same plan cannot insert duplicate work-item keys. The planner
does not run exchange-wide work
cancellations when exact symbols are supplied.

After verifying the queue, run the matching scoped worker with both
`YFINANCE_NSE_CANARY_ENABLED` and `YFINANCE_DAILY_ENABLED` enabled only for the
command process:

```sh
trade-data run-yfinance-nse-incremental-canary \
  --symbols RELIANCE.NS,COALINDIA.NS,ONGC.NS
```

This worker claims at most as many items as selected symbols per invocation and
filters claims and stale-lock recovery by exchange, work type, and provider
symbols. It may pick an older ready incremental item for one of those symbols;
inspect the queue if multiple windows exist. It does not process another
symbol, exchange, or work type. Save its run ID and compare the before/after
coverage snapshot and provider request logs. The read-only
`trade-data daily-canary-run-report RUN_ID` command combines the claimed
work, current item statuses, provider request summary, and selected-window
stored coverage without a provider call. A non-passing worker status exits
nonzero.

Never report `complete=true` as proof of authenticity, provider entitlement,
or that every expected instrument is present. The snapshot checks only the
selected stored rows and locally validated sessions. Keep provider provenance,
row quality, queue outcomes, and failure/retry evidence together with the
result. No synthetic bars may be substituted for gaps or provider failures.
