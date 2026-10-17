# NSE/yfinance daily canary — 2026-10-02

This is a local real-provider ingestion record, not a performance benchmark or
an assertion that yfinance is suitable for live trading. The working tree was
uncommitted; base commit `32cac48` alone does not reproduce these code changes.

| Item | Observed result |
| --- | --- |
| Runtime | Host Python virtual environment; PostgreSQL/TimescaleDB in local Colima Compose |
| Provider adapter | yfinance 1.7.0 |
| Exchange and symbols | NSE: `RELIANCE.NS`, `COALINDIA.NS`, `ONGC.NS` |
| Completed session window | 2026-09-25 through 2026-10-01; five validated sessions |
| Before | 0 valid stored yfinance sessions for each selected symbol |
| Dry plan | 3 bounded-canary items generated; 0 enqueued |
| Enqueue | Exactly 3 items, each 2026-09-25 through 2026-10-01 |
| Scoped worker run | `448f58c8-3967-411b-a883-c47bc267707d` |
| Worker result | 3 claimed, 3 succeeded on first attempt; 15 OHLCV rows and 15 adjustment rows written; 0 retry/terminal/cancelled/lost claims |
| Provider request log | 3 yfinance `download` requests, all `success`, none marked rate-limited |
| After | 5 valid stored sessions for each symbol; no selected-session gaps |
| Same-window re-enqueue | 3 generated, 0 inserted; queue remained at 3 succeeded canary items |
| Incremental dry plan | 0 generated for these three symbols while 2026-10-01 remained the latest provider-eligible session |

The worker was invoked with `exchange="NSE"`, `work_type="bounded_canary"`,
and `claim_size=3`, after confirming there were no other queued yfinance items.
The provider and canary flags were set only in the two command processes; the
repository `.env` and running API container were not enabled for continuous
Yahoo execution. The stored-coverage snapshot checks `source="yfinance"` and
`quality_status="ok"`, not provider authenticity, exchange-official prices,
corporate-action accuracy, or entitlement. Those checks and a second run after
a *new* eligible session remain future work. No synthetic bars were used.

On October 2, the materialized calendar marked NSE closed. The next validated
session in that calendar is October 5, so there was no new daily incremental
observation to ingest on October 2. The snapshot command now applies the same
120-minute provider grace period as the worker when deciding eligibility.
The exchange's [2026 equity holiday calendar](https://www.nseindia.com/resources/exchange-communication-holidays)
also lists October 2 as a trading holiday.
