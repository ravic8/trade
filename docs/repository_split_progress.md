# Repository split: implementation progress

Updated 2026-10-17. Source extraction and production cutover are complete;
see Batch 7 for acceptance and the continuing acquisition backlog.

## Batch 1 — daily import boundaries (completed locally)

- Preserved the post-repair dirty working tree: 497 non-ignored files, SHA-256
  manifest, source archive and working-tree patch. Recovery artifacts are under
  `output/repository_split_implementation_2026-10-16/` and excluded from Git.
- Added a reproducible ownership inventory script and initial file/import manifest.
  Mixed files are explicitly marked for symbol-level classification; the inventory
  does not authorize blind moves or mark those files as extracted.
- Froze historical feature/target column names in `storage/legacy_schema.py` so
  daily storage and publication checks no longer import research implementations.
- Extracted five daily calendar/universe/instrument/OHLCV contracts into
  `contracts/daily_catalog.py`. The transitional full catalog reuses those objects.
- Extracted completed-session calendar logic into `validation/calendar.py`; the
  old research-health entrypoint delegates to it and remains test-compatible.
- Removed eager research/vector/discarded-provider imports from package entry
  points. Legacy public names resolve only when explicitly requested, until their
  API/CLI/Dagster callers are removed. These bridges are temporary.
- Removed a runtime intraday-universe import used only for yfinance type hints.
- Added a fresh-process import boundary test that blocks research, agents, vector
  stores, discarded intraday modules and external ML/agent libraries while loading
  daily pipelines, calendar, contracts, storage and canary operations.

## Batch 1 evidence

- **826 tests passed, seven skipped.** Six integration tests and the real filing
  pack test remain skipped under the current environment/data ownership.
- Repository-wide Ruff and whitespace checks passed.
- Generated PostgreSQL table and index definitions match the saved pre-change
  source for all **37** tables in storage metadata; zero differences.
- This schema comparison is not a fresh PostgreSQL migration or restore rehearsal.
  Those checks remain production-cutover gates.
- Production services, schedules, database contents and deployed images were not
  changed by this implementation batch. lens_fa and nifty_stream were untouched.

## Batch 2 — daily application registrations (completed locally)

- Daily API startup, CLI startup and the Dagster code location now pass the
  fresh-process boundary test with research, agents, vector storage, BigQuery and
  discarded intraday modules blocked. This is an import isolation check, not a
  clean installation of the final minimal package.
- Active code registers **33 API operations, 27 CLI commands, 18 assets, 16 jobs
  and 12 schedules**. The desired NSE-only production policy still enables exactly
  the same five schedules. TSX/US definitions and manual Upstox daily controls
  remain; the legacy North America direct schedule remains stopped.
- Removed research/Opportunity, filing-agent, intraday, aggregation and BigQuery
  registrations. The inactive North America direct job now selects daily OHLCV
  only; it previously also selected Opportunity targets.
- Removed the old symbol-candle endpoint because it queried hourly storage.
  A bounded daily-candle read contract remains to be implemented with the frontend
  extraction. Existing upstream provider capabilities still describe their full
  provider capabilities; that does not register intraday ingestion in Trade.
- Availability rejects non-daily intervals before database access. Generic API
  telemetry is independent of filing intelligence. CLI init-db no longer creates
  filing schemas; Timescale metadata retains historical tables for compatibility.
- Removed obsolete schedule-policy conditions and synchronized the production
  desired manifest and current source schedule inventory.
- Preserved originals, SHA-256 manifests and removed-symbol inventories under
  `output/repository_split_implementation_2026-10-16/batch02/`. Additional recovery
  snapshots are in trade-research/migration; these mixed snapshots are evidence,
  not independently runnable research modules or new applications.
- Moved obsolete endpoint/command/asset registration tests out of active collection
  into the snapshots. Mixed tests retain daily assertions. Legacy filing unit
  tests use their own router rather than registering filings in the daily API.
  Remaining legacy implementation tests stay until their owning source moves.

## Batch 2 evidence

- **784 tests passed, seven skipped.** The lower count reflects extraction of
  obsolete registration tests, not additional skips. Existing integration and
  filing fixture skips are unchanged. Ruff and whitespace checks passed.
- Retained Dagster asset function ASTs match their pre-batch snapshots exactly.
  Retained CLI/API functions match except the reviewed init-db and daily-only
  availability changes. Registration inventories and comparison evidence are in
  the batch recovery folder.
- Generated PostgreSQL definitions remain identical for all **37 tables** and
  indexes. No applied migration, database content, volume or production schedule
  changed. A real restore rehearsal and fresh-database migration remain pending.

## Batch 3 — independent operations frontend (completed locally)

- Created the local sibling **trade-web** with its own npm package/lockfile, CI,
  API URL configuration, Dockerfile and Caddy SPA/API proxy. It builds without a
  Trade checkout. No remote or initial commit was created.
- Preserved and hash-verified all 45 current frontend source/configuration files
  before extraction. Exact originals remain in the private batch03 source archive
  and trade-research/migration/ui-before-split. Provenance records include the
  original revision and dirty-file hashes; this did not import filtered Git history.
- Retained Data Console, coverage, daily quality and ClickHouse reconciliation,
  work queues, runs, listing lifecycle, Jobs and optional provider credentials.
  TSX/US selectors remain disabled. Removed research/Opportunity, agents,
  intraday/minute and BigQuery UI and client calls.
- Added bounded single-instrument daily candle inspection. Trade now has **34 API
  operations**. Reads require an instrument and ordered dates, reject ranges over
  3,660 days, and query stored daily OHLCV without provider requests. The chart uses
  only rows with accepted quality status and reports excluded rows.
- Exported OpenAPI and generated the daily candle TypeScript contract; verified
  one exact API response fixture on both sides. Other retained client DTOs remain
  hand-written; there is no claim of exhaustive contract coverage.
- Removed apps/web, Dockerfile.web and the old combined Caddy proxy from Trade
  after verified copying and frontend checks. Backend Compose, CI, setup/recovery
  verification and deploy builds no longer build or start the frontend.
- Production source configuration now uses separate loopback API and CloudBeaver
  listeners. This requires coordinated gateway/tunnel updates at deployment;
  no deployed routing was changed. See [frontend integration](frontend_integration.md).

## Batch 3 evidence

- **793 backend tests passed, seven skipped**; Ruff, whitespace and current-state
  manifest checks passed. Existing integration/filing-fixture skips are unchanged.
- Independent frontend npm ci, lint, five client tests, TypeScript/Vite build and
  desktop/mobile Chromium smoke checks (two browser cases) passed after removal
  of the original frontend directory. Screenshots were visually checked.
- Browser checks use deterministic API fixtures, not live databases/Yahoo or the
  production authentication gateway. Admin denial and server validation/error
  handling are covered; an authorized live mutation remains a cutover check.
- Backend wheel built without frontend sources. Default/production/KMS Compose
  configurations validate, and the deployment shell passes syntax checks.
  Container image builds were not run because the local Docker daemon is absent.
- The **37 Timescale metadata tables** still have identical DDL/index definitions.
  The **56 tables loaded after full API startup**, including control-plane
  compatibility schemas, also match the exact pre-batch source snapshot.
- No production deployment, remote service, database migration, provider choice,
  schedule or stored market data was changed by this batch.

## Batch 4 — independent research package and source removal (completed locally)

- trade-research is independently installable with its own pyproject, uv.lock,
  CLI, tests, environment and CI. The runtime reads versioned file snapshots and
  persists research outputs as Parquet/CSV/JSON; it has no ingestion database or
  provider clients and no imports from Trade internals.
- Preserved feature/forward-target/Opportunity/model calculations. All **16**
  feature/target/model module ASTs match the pre-batch source. Research retains
  dataset, folds, baseline, LightGBM, latest-prediction, factor and backtest artifact
  workflows. Old database-coupled orchestration, screening and research UI remain
  explicitly inactive migration material, not another active app.
- Added Trade's read-only export-daily-snapshot CLI command. It requires a complete
  validated materialized calendar and uses one PostgreSQL repeatable-read,
  read-only transaction with bounded Parquet chunks, date/provider/identity/quality
  metadata, snapshot as-of and file hashes. It makes no provider requests.
  Research verifies the bundle before computation and writes input/output provenance.
  See [snapshot contract](research_snapshot_contract.md).
- Removed **39** verified research/source compatibility modules from Trade and
  **48** standalone discarded modules, including filings, chat, scraping, vectors,
  BigQuery execution and minute/intraday pipeline entrypoints. Removed the retained
  provider classes for intraday yfinance and NIFTY futures acquisition as well.
- Removed research/BigQuery CRUD methods from TimescaleStore and research-only
  contract validation. Historical table/index definitions remain frozen in place.
- Replaced temporary lazy public exports with explicit daily exports. The five
  daily contracts remain; no research catalog import remains in the daily package.
- Removed ML, agent, document and BigQuery dependencies and regenerated Trade's
  lockfile. The current daily environment and installed wheel run without sklearn,
  LightGBM, OpenAI, LangGraph, Celery or Qdrant packages.
- Tests that belong to research are active there. Retired database orchestration
  and discarded-functionality tests are preserved with hashes in the private
  batch04 recovery folder; mixed tests retain daily assertions. The skipped real
  filing fixture test left the retained suite with its discarded owner.
- An initial copied-test settings mismatch wrote eight local research outputs in
  the historical artifact folder. They were restored from exact hash-verified
  originals, the environment handling was corrected, and all 36 historical
  research artifact hashes were rechecked. Tests now isolate outputs in temporary
  directories; installed-wheel tests run outside either checkout.

## Batch 4 evidence

- **550 backend tests passed, six integration tests skipped**, with **72.15%**
  coverage (the existing 70% gate passes). Ruff and the existing 16-file mypy
  boundary pass. The reduced count reflects removed/moved ownership, not more skips.
- Research's clean installed wheel passed **58 tests with no skips**, including a
  synthetic bundle exported by the backend writer, artifact build, corruption/path
  rejection and the preserved model/backtest checks. No source PYTHONPATH or
  sibling package is supplied during installed verification.
- A separately installed backend wheel loaded API, CLI and Dagster with ML/agent
  packages absent. It exposes **34 API operations, 28 CLI commands, 12 registered
  schedules** and the unchanged desired set of exactly **five NSE schedules**.
- All **56 tables/index definitions loaded by API startup** match the source from
  the exact pre-batch archive. This includes the 37 Timescale tables and historical
  control-plane compatibility schema. No migration version files changed.
- Hash manifests, both source archives, old storage code, test ownership, algorithm
  AST comparison, schema comparison and installed verification are recorded under
  output/repository_split_implementation_2026-10-16/batch04/.
- No production service, provider call, database migration, stored row or live
  schedule was changed. A real PostgreSQL snapshot concurrency test and isolated
  restore/migration rehearsal remain acceptance checks.

## Batch 5 — deployment cleanup and daily package identity (completed locally)

- Renamed the installed backend package to **trade_data**, distribution and CLI
  to **trade-data**. API, Dagster, scripts, CI, migrations, Docker and workspace
  imports use the new namespace. No compatibility package or namespace shim exists.
  Research retains trade_research/trade-research; the two wheels coexist cleanly.
- Removed **117 unused settings**, discarded API DTOs and chat rate-limiting code.
  Retained the bounded daily canary setting after verifying its daily caller.
  Old environment variables are ignored by Settings; they do not restore removed
  features. Stored database/schema names, stable instrument IDs and historical
  storage flags/profile names are preserved deliberately.
- Removed filing-worker, Qdrant, filing-only initialization and obsolete deployment
  drills, webhook rules, secret mounts, example settings and evaluation fixtures.
  Exact originals remain in the private batch05 archive. Existing containers,
  persistent volumes, buckets and database rows were not deleted.
- App rollouts preserve infrastructure versions by default. Planned storage
  upgrades require PROD_UPGRADE_INFRASTRUCTURE=true. PostgreSQL is started with
  --no-recreate; application services use --no-deps. Removed --remove-orphans so
  the independent frontend and archived services are not implicitly deleted.
- Deployment refuses active Dagster runs, stops an idle daemon, preserves the old
  workspace, installs the new module origin and waits out daemon heartbeats before
  reconciling schedules. Existing run storage remains mounted. Mocked deployment
  tests cover absent/idle/busy daemon states, MinIO downgrade rejection and rollback.
- Backup retains daily storage and includes managed KES configuration, certificates,
  keys and secrets when present. Backup files are private. The new PostgreSQL
  restore tool verifies checksums first, creates uniquely named Docker resources
  with no network or published ports, restores with error checking, records row
  counts/revisions and removes only its own disposable resources. Its mocked
  tests cover corruption, path escape, successful restore and failure cleanup.
- Daily health reads exclude historical minute quality, availability and replica
  ledgers. Historical data remains stored. The frontend OpenAPI contract matches
  exactly and requires no client changes.
- Generic observability remains. The removed filing-only alert receiver is replaced
  with an operator-configured receiver without outbound delivery; notification
  routing must be configured separately before relying on it.

## Batch 5 evidence

- **545 backend tests passed, six integration tests skipped**, with the existing
  70% coverage gate passing at **71.49%**. Final results are in batch05/final-tests.txt.
- Ruff, the existing **16-file** mypy boundary, current-state documentation checker,
  shell syntax, Alembic revision graph, and default/production/external-KMS/managed-KES
  Compose configuration checks pass. Compose checks use placeholder credentials;
  they do not start services or prove container builds.
- A clean installed backend wheel loads **34 API operations, 28 CLI commands and
  12 schedules**, with exactly **five desired NSE schedules**, without research,
  sklearn, LightGBM, OpenAI, LangGraph, Qdrant or Celery installed. After installing
  the independent research wheel alongside it, both namespaces load from
  site-packages and **all 58 research tests pass** outside both checkouts.
- All **56 API-loaded PostgreSQL table/index definitions match** the pre-batch
  archive. One historical migration file changes its Python package import only;
  its revision IDs and operations are byte-equivalent after that substitution.
  Every other migration version file is unchanged. This comparison does not prove
  execution against a fresh or restored PostgreSQL database.
- Recovery archive, hashes, source-change manifest, schema comparison, migration
  exception, wheel checks and verification outputs are under
  output/repository_split_implementation_2026-10-16/batch05/.
- No production service, provider request, database migration, stored row or live
  schedule was changed. lens_fa and nifty_stream remain untouched.

## Batch 6 — daily storage cleanup and real infrastructure checks

- Removed unused hourly backlog, feed-health, hourly/intraday reads and writes,
  seeded intraday availability and row adapters from TimescaleStore. Removed the
  minute aggregation and availability modules and their discarded tests.
- Removed ClickHouse feature/experiment repositories and minute aggregation.
  Canonical requests/candles and analytical writes now reject non-daily intervals.
  Daily OHLCV, raw evidence, quality quarantine, audit lineage, and count/digest
  reconciliation remain. TSX/US daily adapters remain dormant.
- Fixed a retained live bug: /api/market/status queried hourly rows and hourly job
  names. It now uses the latest accepted active universe, accepted daily rows and
  daily provider runs. It counts symbols on the latest available session, excluding
  inactive instruments, rejected snapshots, invalid rows and other sources. The
  legacy dataQualityScore field reports that session's coverage percentage.
- Removed research-only storage canary/capacity modules, scripts and tests into
  inert trade-research/migration material. Daily raw-bucket restore validation is
  retained and accepts only configured buckets. Five more unused settings left
  the runtime: three old feed-health controls and model/experiment bucket fields.
- Runtime object-store namespaces and bootstrap are raw, datasets and exports.
  Existing model/experiment buckets and all historical database data remain
  untouched. Historical infrastructure profile/policy names stay compatible.
- Archived all 63 pre-cleanup documents with hashes, removed 16 non-daily
  specifications from Trade, replaced current state/architecture/scope and core
  operations guides, and marked earlier audits as historical. The ownership
  inventory now reports present files rather than absent historical Git entries.
- Docker and CI test jobs install the committed uv.lock with pinned uv and frozen
  resolution. API, worker and Dagster UI still share one backend image. No Node
  or sibling repository is required to build it.

## Batch 6 source evidence

- **529 backend tests passed, five service integration tests skipped**, with
  **72.71%** coverage. The smaller suite reflects retired intraday/research tests;
  one discarded minute-aggregation integration test accounts for the reduced skip
  count. Added regressions cover daily status scoping and non-daily rejection.
- Ruff, the existing 16-file mypy boundary, documentation checks and four Compose
  configurations pass. The frontend OpenAPI schema is unchanged.
- The clean installed backend loads 34 API operations, 28 CLI commands and 12
  schedules with exactly five desired NSE schedules and no ML/agent/research
  packages. The independent research wheel coinstalls and passes all 58 tests.
- All 56 API-loaded table/index definitions match the pre-batch archive. Every
  PostgreSQL and ClickHouse migration version file is byte-identical; TimescaleStore
  initialization/bootstrap AST is identical. Daily candle and business hashes,
  PostgreSQL row mappings and ClickHouse row mappings match before/after fixtures.
- Real server rehearsal successfully created an empty isolated PostgreSQL/Timescale
  database, migrated to 20260726_0012, inserted a daily test row, upgraded to
  20260906_0018 and verified an unchanged repeated upgrade. All 72 public tables
  were included in the repeat/count check. This used the existing immutable
  application runtime image with current source/migrations mounted read-only;
  it is not the new-image build proof. Temporary database resources were removed.
- The first disposable migration attempt hit PostgreSQL's temporary-init socket
  readiness race before TCP opened. The retry waits for TCP, passes and cleans up;
  restore tooling now uses the same corrected readiness check. Initial failure
  evidence is retained alongside the passing proof.

## Batch 6 infrastructure evidence

- The saved **2.6 GB PostgreSQL backup restored successfully** using its matching
  immutable PostgreSQL/Timescale image. It contains **16,685,751 daily table rows
  across the whole saved database**, not just accepted NSE/yfinance rows. The dump
  SHA-256 matches the recovery checkpoint and its Alembic head is 20260906_0018.
  The disposable restore used no network/published ports and was capped at one
  CPU/1 GB RAM. Its container and volume were removed after the passing report.
- Built **trade-data-api:split-rehearsal-dbbd567d6d85**, image ID
  sha256:ba378291527a25f78c44b4f55189db4594a638b4a113a59efab0315505d1d632,
  with frozen dependencies in a unique BuildKit builder capped at one CPU/2 GB.
  The image loads API, CLI and Dagster, retains exactly five desired NSE schedules,
  and contains no research/ML/agent libraries. The temporary builder was removed;
  the tagged validation image remains available. It has not replaced production.
- Exercised that new image against a second isolated PostgreSQL and Redis pair:
  fresh migrations and repeated upgrade passed, API health/daily candle reads
  returned HTTP 200, missing-universe status stayed honestly empty, and discarded
  filing/chat routes returned 404. Only synthetic fixture data was used in this
  disposable database; no live provider requests were made.
- A real PostgreSQL snapshot concurrency check confirmed transaction_read_only=on
  and repeatable-read isolation. A separate writer committed a close-price update
  after the calendar read; the export retained the earlier price, while the
  database and API exposed the committed new price. This validates the consistency
  boundary that dummy-storage unit tests cannot establish.
- Final resource checks found no rehearsal containers, volumes or build workers.
  Production API health returned HTTP 200. No production service, schedule, data
  row, provider setting or stored volume was replaced. Disk space returned after
  disposable restore cleanup. lens_fa and nifty_stream remain untouched.
- Image provenance includes exact staged hashes. Runtime src/, Dockerfile, lockfile
  and migrations match the final source; the documentation and ownership-inventory
  helper continued to be updated after staging and must be included in the final
  rollout build. These verification images are not a substitute for deployment
  from the finalized checkout.
- Source archives, transfer/ownership manifests, schema/hash comparison, full test
  output, installed-wheel checks, restore report, migration report, Docker build
  log and new-image acceptance proof are recorded privately under
  output/repository_split_implementation_2026-10-16/batch06/.

## Batch 7 — recovery and production cutover (17 October 2026)

The daily backend and independent frontend now serve production. The API and
Dagster daemon use `trade_data`; the frontend is deployed as the independent
`trade-web` Compose project. The existing loopback gateway on port 8081 and
Cloudflare routing remain compatible, including the separate SQL-console host.
Exactly five current-origin NSE schedules are running. TSX/US and legacy Upstox
execution remain disabled. Repository separation does not delete historical data.

### Verified release

- **533 backend tests passed, six service integration tests skipped locally**,
  with **72.71% coverage**. CI separately exercises real PostgreSQL/Timescale,
  Redis, ClickHouse and encrypted MinIO. Ruff, the mypy contract/validation
  boundary, documentation checks and production Compose checks pass.
- Fixed ClickHouse's unbounded batch-verification scan by filtering immutable
  replacement-key scope before FINAL and filtering mutable run lineage afterward.
  Per-query memory and thread bounds keep real reconciliation within the server's
  4 GB ClickHouse limit.
- Disabled high-volume diagnostic collectors and detached their existing tables
  without deleting files. The large trace/text/metric logs had exhausted server
  memory and blocked ordinary business queries. User data remains attached.
- Explicitly round analytical Decimal(20,8) prices before insertion. The driver
  previously truncated extra digits while PostgreSQL's audit rounded them. The
  regression reproduces the actual historical replay values.
- Deduplicate quality outcome IDs within a bulk upsert, retaining sequential
  last-observation semantics. Overlapping queue windows previously caused
  PostgreSQL's ON CONFLICT cardinality error and a whole batch retry. A real
  PostgreSQL integration regression covers repeated overlapping observations.
- Fixed fresh-setup copying of uv.lock, idle yfinance configuration, and external
  acceptance handling of optional Compose volume maps. MinIO's old published
  images are unavailable; fresh builds use pinned upstream community source.
  Existing production MinIO/KES images are preserved under explicit cached tags
  and in a private Docker image archive.

### Storage and data acceptance

- A fresh private pre-cutover PostgreSQL dump is 2,769,399,540 bytes, SHA-256
  `a1a7a4f05e14399dae05ff842c27af0b72eaeea7801c597deca22deca48f6a49`.
  The earlier matching-schema full backup restored successfully in Batch 6.
- An isolated recovery of the complete MinIO/KES archive verified **six buckets,
  882 object versions and 339 encrypted versions**, including versioning, delete
  markers, byte digests and encryption-key recovery. All **12 ClickHouse user
  tables** matched their source canonical counts/digests. Diagnostic logs were
  retained on the source volume and intentionally excluded from user-data restore.
  All disposable clone containers, volumes and networks were removed.
- The serialized pre-worker audit reconciled **124 monthly partitions** containing
  **4,450,979 accepted NSE/yfinance daily rows** in both databases. All business
  digests matched. Recent replica repair corrected **46,246** rows affected by
  the previous precision behavior; no authoritative rows were deleted.
- Stored accepted history spans **18 July 2016–16 October 2026**, across **2,597
  instruments**. The latest session has **2,554** eligible active instruments with
  accepted data against **2,606** eligible active instruments. This is coverage
  evidence, not a claim of complete ten-year history for every stock.
- Bounded real refresh of RELIANCE.NS/TCS.NS succeeded (four daily rows). The
  incremental planner rerun generated zero work. A separate RELIANCE.NS January
  2018 historical replay succeeded (22 rows), with raw-object persistence and
  PostgreSQL/ClickHouse verification. The fixed precision release repeated both.
- Requeued **75,349** tasks attributable to verified old MinIO HeadObject failures
  or ClickHouse memory errors, limited to active NSE instruments with accepted
  history and current acquisition windows. Attempts and error fields were
  preserved; exact before-state rows are privately archived. Provider exceptions,
  inactive instruments and expired windows were excluded. The historical queue
  continues in production and is not expected to drain during release acceptance.

### Independent repositories and gateway

- Private repositories have initial commits and remotes:
  [trade-web](https://github.com/ravic8/trade-web),
  [trade-research](https://github.com/ravic8/trade-research),
  [trade-fundamentals](https://github.com/ravic8/trade-fundamentals), and
  [trade-market-experiments](https://github.com/ravic8/trade-market-experiments).
  Web and research CI passed independently. The other two hold inert archives.
- The first fixed automatic worker tick processed 100 tasks: **98 succeeded**
  and two SAB.NS windows were explicitly quarantined for invalid provider candles.
  Its **696 validated rows** reconciled by count/digest. Dagster reports this batch
  as degraded/failure because of those terminal exceptions; it is not hidden as
  an all-success tick. A subsequent serialized audit of August–October passed
  without repairs, confirming that normal ingestion preserves replica precision.
  The next automatic tick (`a86e55d4-727c-41ae-978a-fa668276dbfb`) succeeded:
  **100 tasks succeeded, zero failed/retried/terminal**, ingestion run
  `434d2ee8-dd6b-4734-8515-858c452e078a`. Retry timers for the 196 tasks affected
  by the now-fixed quality insert were resumed without clearing their history.
- Production browser checks use the real gateway/API on desktop and mobile.
  Coverage, daily candle chart, jobs and unauthorized Settings behavior passed
  without mocks or JavaScript errors. API health and the SQL-console route returned
  200; admin access returned 403 without identity and 200 for an authorized identity.
- Backend image/configuration, source hashes, original container specifications,
  environment/workspace backups and old application images remain recoverable.
  The old frontend is retained stopped as a gateway rollback option. Candidate
  services are removed after final acceptance. lens_fa/nifty_stream are untouched.

### Release evidence and rollback

Private local evidence is under
`output/repository_split_implementation_2026-10-17/batch07/`; the server checkpoint
is `/opt/trade/maintenance/20261017-daily-cutover-20f0a42a1344`. These paths contain
sensitive recovery material and are excluded from Git.

The initial cutover backend image ID was
`sha256:ba2aa99ad8c0c58a9034931c31d6043927aba4a283172c4bf8d2a036e682eb34`.
The independent frontend is `trade-web:daily-split-20261017`, ID
`sha256:e0f3fc14e060e7d39a9b973dbace39a1ad9edb2587cf99a93c746949014577da`.
The server's `production-release.json` records the current application image ID and
release tag. `api.inspect.private.json`, `dagster-daemon.inspect.private.json`,
`web.inspect.private.json`, `.env.before` and `workspace.yaml.before` retain the
previous application configuration. Rollback stops the daemon between runs,
restores the previous image/environment/workspace and reconciles only the five
saved NSE schedules. Stop the independent frontend before restarting the retained
old gateway on port 8081. Application rollback requires no database downgrade or
stored-data deletion. Keep private storage backups, keys and immutable image
archives together; never roll back or rebuild encryption keys from source alone.

Automatic deployment is held during this coordinated cutover and is re-enabled
after the merged source passes CI. The backend source checkout is synchronized
with main; frontend deployment source remains independent. Future backend builds
use revision tags unless the operator explicitly pins an image.
