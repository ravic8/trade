# Implementation plan: simple daily backend, frontend and research

Status: implementation in progress, 2026-10-16. This document describes the sequence;
writing it does not create trade-web, move additional application code, discard
existing directories or change production. It incorporates the user's final scope.

## Fixed scope

- trade: daily equity backend, operational API, ingestion/repair, quality, storage.
- trade-web: operational React frontend; reuse the existing app.
- trade-research: research, features/targets, Opportunity analytics, ML/backtesting.
- lens_fa and nifty_stream: untouched independent projects.
- Legacy fundamentals/agents and streaming/intraday/native functionality: discard
  from active Trade code. Existing recovery archives are sufficient; do not build
  trade-fundamentals or trade-market-experiments into applications.
- NSE/yfinance is the active scope. Keep TSX/US daily adapters dormant. No liquidity
  filter. Keep ten-year acquisition, new listings, incremental and gap repair.
- Keep PostgreSQL/Timescale, ClickHouse, existing scheduling and provider controls.
  Do not change provider, scheduler, database hosts or retention during extraction.

## Stage 1 — Baseline and ownership

Reuse the existing snapshots, copy manifests and recovery evidence. Capture the
current post-repair changes as an additional baseline. Inspect existing destination
edits before transferring anything again. Classify every code path and deployment
entry as keep, move to web, move to research, or discard.

Record package/route/CLI/job/schedule inventories, existing data/migration heads,
application image references, runtime configuration locations and current tests.
Secrets remain in private runtime storage. Validate relevant snapshot hashes.

Deliverables: ownership manifest, recoverable source baseline, baseline test/build
results and rollback references. No implementation depends on an uncommitted file
that exists only in a sibling checkout.

Pass: every removal has a verified destination or existing recovery archive, and
pre-existing unrelated working-tree changes are accounted for.

## Stage 2 — Daily backend isolation

Work through these existing coupling points, without replacing working pipelines:

1. pipelines/__init__.py and storage/__init__.py: daily-only exports and imports.
2. storage/timescale.py: daily persistence independent of feature/target code.
   Research persistence moves to the research package. Preserve required historical
   schema definitions as frozen migration compatibility, not computation imports.
3. contracts/catalog.py, contract_gate and validation: keep daily contracts/helpers;
   remove feature/model validation and rebuild orchestration from the daily package.
4. api/app.py: keep coverage, universe, provider settings, runs, exceptions, job
   requests, health and daily candle endpoints. Remove research/Lens/intraday routes.
5. cli.py and Dagster: daily commands/assets only. Keep the five intended NSE
   schedules and dormant TSX/US daily capabilities. Preserve historical run records.
6. Separate active daily settings and DTOs from research/discarded application code.

Use narrow seams at actual dependencies. Do not introduce a generic framework,
plugin system, shared namespace or separate service for each internal module.

Deliverables: one daily application with API and worker/scheduler process roles;
a small declared HTTP contract; daily tests and minimal startup checks.

Pass: API/CLI/Dagster start with only declared daily dependencies, without model,
agent, document/vector or frontend packages. Applied migration history still loads;
fresh-database migration and existing-head compatibility are verified in isolation.

## Stage 3 — Frontend extraction

Create local sibling trade-web. Reconcile and transfer apps/web, lockfile, relevant
shared components, tests, build/container configuration and provenance. Preserve
useful Git history in a separate extraction clone where practical; explicitly
include the current dirty changes and do not rewrite the active checkout.

Retain Data, Jobs, Provider Settings, coverage/quality views and useful daily-candle
inspection. Keep an operational dashboard only if it uses the daily backend.
Move research/Opportunity pages to their research owner as preserved UI source;
discard Lens, chat, intraday and streaming pages/navigation.

Point the frontend at a configured backend URL. Keep database/provider secrets on
the backend. Check the typed API client against the backend OpenAPI contract and
exercise representative coverage, error and authorized job-request interactions.
Do not add a gateway, BFF service or contract broker.

Deliverables: independent trade-web package/image; Trade has no frontend build step.

Pass: web installs, builds, lints and tests alone; browser checks work with the
backend; backend builds with no Node/frontend checkout. Remove apps/web only after
its destination and the retained backend pass these checks.

## Stage 4 — Research extraction

Complete the already staged move of features, targets, screeners/Opportunity,
factors, modeling, backtesting, artifact readers, research persistence, entry
points, configuration, tests and fixtures. Reconcile destination copies before
removing originals. Do not reimplement models as part of this split.

Give trade-research an independently installable package, lockfile and documented
CLI. Inputs are declared daily-data exports or documented read-only interfaces
with provider, instrument identity, session, OHLCV, adjustment semantics, quality,
as-of time and snapshot/version. It must not import Trade's internal modules,
acquire its own duplicate canonical daily feed or write ingestion tables.

Keep existing artifact provenance. Move the fixture-dependent filing test out of
the retained suite with discarded functionality; do not inflate success by simply
skipping tests after moving fixtures. Report retained/moved/discarded tests by owner.

Deliverables: independent research package and verified transfer records.

Pass: representative feature/target, dataset and backtest-plumbing checks pass in
a clean environment with no sibling PYTHONPATH. Daily backend has no research imports.
Existing research UI source can remain explicitly inactive; creating a research
product frontend is outside this extraction.

## Stage 5 — Remove discarded functionality

Remove filings, chat, scraping, agents, vectors, Lens UI, minute/intraday/F&O
execution and native/experimental modules plus their commands, routes, jobs,
configuration, dependencies, fixtures and application service definitions.

Use existing archives rather than migrating anything to lens_fa or nifty_stream.
Mark trade-fundamentals and trade-market-experiments as recovery/archive-only.
Neither is a required runtime/development dependency. Do not remove those archives
or purge existing database records/volumes as an incidental cleanup action.

Deliverables: no reachable discarded functionality in Trade and no changes to the
independent Lens/F&O repos. Preserve applied migrations and necessary legacy DDL.

Pass: ownership/import/CLI/route/job checks contain no discarded execution paths.

## Stage 6 — Packaging and developer workflow

Rename the daily package to trade_data and distribution/CLI to trade-data after
extraction. Update all retained imports, Docker entrypoints, Dagster configuration,
CI and docs together. Keep trade-research's package distinct.

Remove unused dependencies and regenerate each lockfile. Build and test installed
artifacts outside source directories. Add lightweight import-boundary checks so
research/agent dependencies cannot return. Do not create a shared-library repo.

Provide a small common developer command vocabulary: bootstrap, run, test, build.
Each repo documents its exact commands. Ordinary unit tests use local controlled
fixtures and require neither a full history download nor unrelated applications.
Offer integration checks separately against configured services.

Deliverables: independent clean-checkout builds and accurate README/architecture,
current_state and manifest documentation. Trade has no apps/web or Node build
requirements, no ML/agent dependencies and no imports from sibling filesystems.

Pass: all three declared builds work independently. Relevant tests/lint and a
small frontend/backend interaction suite pass; transfer manifests are reconciled.

## Stage 7 — Production cutover and acceptance

Build immutable images; validate configuration and migrations before deployment.
Preserve the existing server, volumes, endpoints, provider settings and credential
roles. Shared infrastructure remains operator-managed with explicit ownership.
Do not restart/remove PostgreSQL, ClickHouse, Redis, MinIO/KES or monitoring merely
because application repositories move. Preserve Qdrant/filing volumes pending a
separate stored-data retention decision.

Deploy the daily backend first. If package/module renaming changes Dagster origins,
stop the daemon, respect its heartbeat guard, reconcile exactly the intended NSE
schedules and restart. Preserve previous images and historical run data. Then
switch the frontend deployment to the independent trade-web image/API endpoint.

Acceptance evidence:

- API health and retained operational endpoints.
- Exactly five intended NSE schedules; TSX/US inactive.
- A bounded real daily incremental run and historical gap repair.
- Idempotent rerun, retries and explicit provider/invalid-data exceptions.
- PostgreSQL/ClickHouse reconciliation for affected data.
- An actual scheduled worker tick after cutover.
- Frontend coverage, jobs, errors and authorized repair request workflow.
- Application image/configuration rollback references.

Serialize replica backfill and ingestion while proof depends on latest run lineage.
Use fresh execution/snapshot IDs for new reconciliation attempts. Code deletion
must not imply database/table deletion. A backup listing is not a restore rehearsal;
verify an isolated restore before any later destructive database migration.

Pass: the extracted deployment completes these checks and can roll back without
restoring databases or reversing a destructive schema change.

## Change management

One reviewable change batch per stage; split Stage 2 by coupling point if needed.
Each batch leaves the daily backend working. Finish removal after verification:
copied-but-still-imported modules do not count as a completed split.

Stop a batch on a failed gate, fix it or restore that batch's baseline, and continue
independent planning/documentation work. Do not ask for repeated permission for
routine authorized extraction; surface genuinely missing decisions explicitly.

Final result: three focused active repos, one daily backend application, independent
builds, documented data/API contracts, preserved market data and clear ownership.
