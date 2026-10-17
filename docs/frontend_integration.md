# Independent operations frontend

`trade-web` is the local sibling `/Users/raviteja/Downloads/projects/trade-web`.
It owns the React application, npm lockfile, browser tests, Caddy SPA/API proxy,
Dockerfile and JavaScript CI. Trade owns the HTTP API and Python/Dagster workers.
Neither repository imports source from the other or requires the other's checkout
for its build. The repositories do not yet have new remotes or commits.

For development, start Trade's API on port 8000, then run `npm ci` and
`npm run dev` in trade-web. Vite proxies /api using `VITE_API_PROXY_TARGET`.
Its default is http://localhost:8000. `VITE_API_BASE_URL` can instead point to a
public API URL, with the frontend origin allowed by Trade's CORS setting.
Provider credentials remain server-side.

For deployment, build the frontend image from trade-web's own Dockerfile and set
`API_UPSTREAM` to a separately reachable backend. Trade's production Compose no
longer builds or starts a frontend. The API listener uses loopback
`PROD_API_PORT` (8000 by default). CloudBeaver stays on the private Docker network
at cloudbeaver:8978 without a published host port. The independent gateway
preserves the existing server entrypoint on loopback port 8081.
Trade-web's proxy does not provision CloudBeaver or authenticate users; retain the
existing trusted gateway/admin identity boundary for administrative operations.

Trade-web exposes the NSE daily Data Console, quality/replication views, coverage,
work queue, run details, listing lifecycle, daily candles, Jobs and optional
Upstox credential controls. TSX/US selectors are disabled until activation is
approved separately. Research, Opportunity, chat, filing-agent, intraday and
BigQuery pages and calls are absent from the independent app.

## Contract changes

Export the API from Trade with:

```sh
python scripts/export_web_contract.py --output /path/to/trade-web/contracts/openapi.json
```

Then run `npm run generate:api`, lint, tests, build and browser checks in trade-web.
The daily candle response uses generated OpenAPI types. Other retained client DTOs
remain the existing hand-written types; they are not all generated or exhaustively
validated. `tests/fixtures/daily_candles_response.json` is verified against the
real API handler with isolated fake storage and copied into the frontend consumer
fixtures. Daily reads are scoped to one instrument, provider, exchange and an
ordered date range of at most 3,660 days. Empty storage returns an empty array;
service failures return 503. This endpoint never requests provider data.

## Verification and deployment status

Browser checks use deterministic responses and run on desktop and mobile Chromium.
They cover navigation, daily availability, daily quality/replication views,
candlestick rendering, and denied admin access. Backend tests verify read scoping,
date bounds and errors. Frontend tests verify encoded requests, backend URL
configuration, validation errors and idempotent request headers.

The source split is local. A live end-to-end test with databases, the trusted
auth gateway, independent container builds and coordinated production deployment
remain acceptance gates. The local Docker daemon was unavailable during this
batch; Compose configuration validation does not prove an image build.

## Daily status compatibility

`/api/market/status` now reads accepted daily observations for the latest accepted
active universe and the selected daily source. Latest-session symbol counts exclude
inactive instruments, rejected universe snapshots, invalid rows and other sources.
The legacy dataQualityScore field reports latest available session coverage as a
percentage; detailed quality and expected-session freshness remain in the dedicated
Data Console endpoints. The legacy lastScreenerRun field stays null for response
compatibility. No screener executes in this backend.
