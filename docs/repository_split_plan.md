# Repository split plan

Status: proposed; revised after scope clarification and online research on
2026-10-16. The user instructed discarding legacy Lens/fundamentals and
streaming/F&O functionality instead of migrating it into lens_fa or nifty_stream.
Those independent repositories remain untouched. This plan does not claim a
fresh production audit. See `repository_split_research.md` for techniques and sources.

## Target ownership

| Repository | Owns | Does not own |
| --- | --- | --- |
| trade | Daily listed-equity acquisition, universe/mappings, calendars, adjustments, validation, raw evidence, historical maintenance, PostgreSQL authority, ClickHouse replication, work queues, scheduling, operational API | Frontend, features, labels, screening, ML, backtesting, filings, agents, intraday/streaming |
| trade-web | Daily-data operational UI: coverage, freshness, runs, errors, exceptions, provider settings, authorized job requests | Direct database access, provider credentials, pipeline execution, research or Lens application |
| trade-research | Features, forward targets, Opportunity analytics/screening, factors, datasets, models, backtesting, research-specific interfaces | Authoritative daily-data ingestion or writes to Trade-owned tables |
| lens_fa | Independent existing project; no changes in this split | Legacy Trade fundamentals migration |
| nifty_stream | Independent existing project; no changes in this split | Legacy Trade streaming/experimental migration |
| trade-market-experiments | Existing recovery/archive copies only; not an active application | A production dependency or new development target |
| trade-fundamentals | Existing recovery/archive copies only; legacy functionality is discarded | An application or migration destination for Lens |

The current Python project is still named trade-research and the core package is
trade_research. After extracting consumers, rename the daily distribution to
trade-data and its package to trade_data. Keep the repository name trade.
The research application can then own its own trade_research package without
namespace sharing. Do not make core startup load sibling source directories.

## Daily-data contract

- Active acquisition: yfinance, all accepted NSE EQ stocks, no liquidity/top-N
  filter. TSX/US daily adapters remain dormant for later activation.
- Maintain a rolling ten-year acquisition window plus incremental collection.
  Retain older existing observations. Backfill, newly listed stocks and gap repair
  are execution modes of this same service, not separate applications.
- Preserve source identity, adjustment semantics, calendar, quality and coverage
  exceptions. Never fill missing sessions with invented prices.
- PostgreSQL/Timescale is the authority; ClickHouse is a reconciled read copy.
  Trade owns writes, migrations and credentials for its domain.
- Minimal operational API supports the web client and external consumers.
  Expose source, quality, as-of time, pagination and bounded request windows.
- Job requests are durable, authorized, idempotent requests returning job IDs.
  The scheduler/worker executes them outside the HTTP request process.

## Boundaries between repositories

Use versioned HTTP/OpenAPI contracts for UI/operational integration. Research can
consume versioned dataset exports or documented read-only data views; it must not
import Trade's pipeline/storage internals. Do not create a shared Python library
until a demonstrated stable common abstraction justifies one.

Each application gets distinct write credentials and owns its own tables/schema.
Existing database servers may be shared. Existing mixed tables remain intact
through this split. New research writes use an explicitly owned schema/database,
not Trade-owned market-data tables. Database/schema migration is
a separate reviewed step from source movement.

Application images, tests and lockfiles belong to their respective repositories.
Deployment may share the existing host, network, database servers and monitoring;
use explicit endpoints and pinned image versions. There is no requirement to
introduce Kubernetes, a new infrastructure repository, or additional databases.

## Current migration state

- trade-web is proposed and has not been created.
- Three approved local sibling repositories exist: trade-research,
  trade-fundamentals and trade-market-experiments.
- Research/fundamentals source copies are verified; originals remain coupled in
  Trade. Research still needs independent packaging. Legacy fundamentals copies
  now serve only as recovery/archive material.
- Local research artifacts, inactive filing files, notebooks/native prototypes
  and intraday artifacts have moved with hash manifests and recovery originals.
- lens_fa's README says its accepted Stage 1 specification is complete and
  application implementation has not started. This is context only: no legacy
  Trade functionality will be imported into it.
- nifty_stream already owns a separate Upstox F&O recorder and ingestion stack.
  Do not merge the generic native experiments into it by default.

## Ordered implementation

### 1. Freeze the baseline and write the ownership manifest

Preserve the current dirty tree, commit/reference, untracked source, destination
copies, transfer hashes, operational configuration, database backup and deployed
image references. Reconcile any destination edits before transferring again.
Classify every module, route, CLI command, asset, schedule, UI page, test, fixture,
document, dependency, deployment service and ignored dataset.

Deliverable: a path/symbol-to-owner manifest with states copied, detached,
validated, and removed. Copies alone do not count as extraction.

Gate: every removal has a verified destination or an explicit preserved archive;
no unrelated working-tree change is discarded.

### 2. Make the daily backend independent

First untangle the actual coupling points:

- pipelines/__init__.py: remove eager research/intraday exports.
- storage/__init__.py: remove eager vector-store loading.
- storage/timescale.py: separate daily persistence from research persistence;
  remove imports of feature/target builders and column definitions.
- contracts/catalog.py and pipelines/contract_gate.py: separate daily contracts
  from feature/target/ML publication contracts.
- validation/daily_pipeline.py: retain calendar/daily validation helpers in Trade;
  move feature/model rebuild and research-readiness orchestration out.
- api/app.py: split daily operational routes from chat, filings, Opportunities,
  factors and models. Move shared generic telemetry out of the filings package.
- cli.py and Dagster definitions/assets/manifests: register only daily ingestion
  and maintenance capabilities in Trade.
- config/schemas: separate active daily settings/DTOs from consumer-specific ones.

Applied migrations must not disappear. Where an old baseline migration imports
live metadata, freeze the required legacy schema definitions first. Preserve the
existing migration chain and database objects; new schema changes must be additive.
Move research persistence methods into the research owner, not a reverse import
from Trade into an optional research application.

Gate: daily package, CLI, API and Dagster load in a clean environment without
OpenAI, LangGraph, Qdrant, Celery, LightGBM or scikit-learn installed. Daily tests
pass without sibling directories on PYTHONPATH or data from another application.
Retain the current scheduler/provider execution design during extraction.

### 3. Extract the frontend into trade-web

Transfer apps/web and its relevant build, lint, unit, browser tests, lockfile and
container configuration with hashes. Retain DataPipelinePage, JobsPage,
ProviderSettingsPage and useful daily-candle/operational dashboard components.

Remove Lens, research chat, factors, Models, Opportunities, screening and
research-progress navigation from the operations app. Preserve their code in the
appropriate owner/archive; do not start building multiple new UIs during the split.

Configure a backend base URL and documented authentication. Keep provider secrets
and all database credentials on the backend. Use an OpenAPI snapshot/client
contract that is checked against the backend, rather than copying internal Python
schemas or connecting the browser to PostgreSQL/ClickHouse.

Gate: trade-web installs/builds/tests by itself; Trade builds without Node or
apps/web. Browser checks exercise coverage, runs, errors and bounded job requests
against the extracted backend. Both images can deploy independently.

### 4. Complete the research extraction

Move features, targets, modeling, screeners, research artifact readers, research
validation, research persistence, pipeline entry points, configuration, tests,
fixtures and documentation into trade-research. Include Opportunity analytics.
Retain provenance for existing datasets/models and do not put large/private
artifacts into Git.

Give the destination its own package, lockfile, CLI and owned persistence boundary.
Declare its daily-data input contract: instrument identity, session date, provider,
OHLCV, adjustment policy, quality, as-of time and dataset version/snapshot.
Do not install the entire old monolith as its hidden runtime dependency.

Gate: independent installation and representative tests for feature/target
construction and dataset/backtest plumbing using declared fixtures. Existing model
code is preserved, not claimed to be scientifically validated by the migration.
Trade has no research imports or model dependencies.

### 5. Discard legacy fundamentals and streaming functionality

Remove the obsolete filings, chat, scraping, agents, vector implementation,
Lens/research-agent UI, F&O/intraday execution, native prototypes and their entry
points, tests, dependencies and deployment services from the active daily project.
Do not merge any of these into lens_fa or nifty_stream, and do not change those
independent repositories. Preserve existing snapshots as recovery archives.

The existing trade-fundamentals and trade-market-experiments copies are archives,
not applications that must be made functional. Retain stored data and applied
migration compatibility through this source split; any database/table/volume
purge is a separate explicit action. Remove legacy application service definitions
without deleting infrastructure shared with the retained daily backend.

Gate: no discarded functionality, routes, CLI commands, jobs or dependencies are
reachable from the daily backend. Neither independent Lens nor F&O repo changes.

### 6. Simplify packaging, deployment and documentation

After code separation, remove unused dependencies and regenerate the daily-only
lockfile. Rename the daily package/distribution and update imports, CLI scripts,
Docker commands, CI, tests, API title and operator documentation together.

Trade's application deployment contains its API, daily worker/scheduler and optional
scheduler UI. Move the web application service to trade-web and remove obsolete
fundamentals application service definitions. Existing PostgreSQL, ClickHouse,
Redis, MinIO/KES and monitoring may remain
operator-managed shared infrastructure; isolate credentials and volumes by owner.
Do not stop/delete shared infrastructure because an application is extracted.
Keep existing Qdrant/filing storage intact pending a separate explicit decision
on stored-data retention; it is not required by the daily application.

Keep the five currently desired NSE schedules. Remove research/intraday schedules
from active definitions while preserving historical run data. Package/module
renames can change Dagster origins: stop the daemon for origin reconciliation,
use its heartbeat guard, reconcile desired state, then restart. Preserve prior
images for historical execution/rollback compatibility.

Update README, architecture, current_state documents and current_state_manifest
as one coherent change so the old mixed-platform inventory is no longer canonical.
Replace old terminology and document TSX/US activation requirements separately.

Gate: a clean checkout of Trade installs, migrates and runs the daily backend
alone; no frontend build step, agent services or implicit sibling imports remain.

### 7. Deploy and prove the split

Build immutable images and run isolated checks before replacing the active image.
Preserve runtime settings and verified existing data volumes. Avoid mixing the
source split with a provider switch, scheduler replacement, infrastructure move
or destructive schema cleanup.

Verify configuration, migration compatibility, five desired schedules, API health,
a bounded real NSE incremental run, a bounded gap repair, idempotent rerun and
PostgreSQL/ClickHouse equality. Recheck exception reporting and source provenance.
Observe an actual scheduled worker tick after deployment. Verify TSX/US and other
application schedules remain inactive.

Serialize monthly mirror repair and ingestion: the current strict run-lineage
replication check can fail when overlapping writers replace each other's lineage.
Give each fresh reconciliation snapshot a fresh source-run ID. Database backup
listing validation is not a restore rehearsal; verify restorability in isolation
before any future destructive database migration.

Deploy trade-web separately against the tested API contract. Roll back application
images/configuration on failure; retain migration baselines and avoid table drops.
Remove source originals only after the corresponding independent destination and
clean daily-backend gates pass.

## Reviewable change batches

1. Ownership manifest, baseline fixtures and legacy schema compatibility.
2. Daily backend import/API/CLI/Dagster isolation.
3. Operational frontend extraction and API contract checks.
4. Research package/persistence/tests extraction.
5. Discard legacy fundamentals/streaming functionality and preserve recovery archives.
6. Daily package/dependency/deployment/docs cleanup.
7. Production acceptance evidence and final transfer verification.

Each batch must leave the daily backend working. A destination may be inactive
while extraction proceeds, but this must be explicit in its README and manifest.
No destination is described as functional until its independent checks pass.

## Completion definition

- Trade contains only the daily backend, its operational API, tests, contracts,
  deployment instructions and necessary migration compatibility.
- No frontend, research/ML, fundamentals/agents or intraday execution remains.
- No runtime imports from sibling repositories or shared Python namespace tricks.
- Destination repos own their code, fixtures, artifacts, dependencies and tests;
  discarded legacy content has a recovery archive without becoming an application.
- Existing market data, source labels, database migrations and run history survive.
- Daily ingestion and history maintenance work and replicate correctly in both
  databases; missing/invalid/provider-limited data remains explicit.
- Independent deployment and rollback work. The operations frontend uses the
  backend's documented contract and has no direct database/provider credentials.
