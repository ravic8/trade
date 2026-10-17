# Research findings: splitting Trade into independent repositories

Research date: 2026-10-16. Scope: repository separation, Python boundaries,
frontend/backend contracts, migration safety and daily-ingestion reliability.
Recommendations below are judgments for this project, not a universal ranking.
No production services or application code were changed for this research.

## Final ownership recommendation

Three active repositories: trade (daily backend), trade-web (operations frontend),
and trade-research (research consumer). Keep the daily backend as one application
with internal modules and API/worker process roles. Separate repositories do not
require separate microservices for universe, validation, storage and scheduling.
Fowler describes the extra operational prerequisites for microservices and the
value of learning boundaries before subdividing further:
[Microservice prerequisites](https://martinfowler.com/bliki/MicroservicePrerequisites.html),
[Monolith first](https://martinfowler.com/bliki/MonolithFirst.html).

The user's newer instruction supersedes the earlier plan: discard the legacy
fundamentals/agents and streaming/F&O implementation from this project. Do not
migrate it into lens_fa or nifty_stream, and do not change either independent repo.
Existing snapshots can preserve discarded code; it need not become another app.
Do not conflate application removal with dropping existing tables or data.

## Techniques and fit

### 1. Incremental extraction: use immediately

The Strangler Fig approach modernizes through smaller replacement boundaries.
For Trade, first isolate daily interfaces and imports, then extract one owner at
a time. No routing proxy or new runtime service is required merely to apply this
idea to code extraction. Delete discarded functionality rather than rebuilding it.

Complete a vertical slice: source, imports, entry points, tests, fixtures,
dependencies, configuration, deployment and documentation. Copying directories
alone is not completion.
Source: [Strangler Fig](https://martinfowler.com/bliki/StranglerFigApplication.html).

### 2. Branch by abstraction: use at existing coupling points

Introduce a narrow interface where a daily caller reaches mixed implementation;
move callers onto it, then remove the old dependency. Suitable seams are daily
persistence, provider execution and operational job submission. Avoid introducing
interfaces for every class. Temporary adapters need explicit removal criteria.

In particular, daily storage must not import feature/target computation to obtain
legacy table columns. Freeze necessary migration/schema compatibility independently
and move research persistence/computation to its owner.
Source: [Branch by abstraction](https://martinfowler.com/bliki/BranchByAbstraction.html).

### 3. Enforced architecture boundaries: use immediately

Import Linter supports forbidden dependencies, layers, independence and cycle
checks. Use architecture checks in CI so dependencies cannot quietly return.
Proposed rules: core data logic cannot import API/Dagster entry points; daily
modules cannot import research, modeling, filings, chat or vector implementations;
no cycles between sibling core modules. Add explicit checks for prohibited external
packages and imports from sibling working directories.

This complements a clean environment test with only declared daily dependencies.
A successful test run in the current all-purpose development environment does not
prove the daily application is independently packaged.
Source: [Import Linter](https://import-linter.readthedocs.io/en/stable/).

### 4. OpenAPI plus executable consumer contracts: use a lightweight version now

OpenAPI describes the HTTP interface independently of implementation language.
Publish Trade's operational contract and generate/check the frontend's typed
client against it. Exercise concrete coverage, job-request and error responses
against the real provider implementation in CI.

Pact's documentation distinguishes consumer/provider behavior tests from static
schema checks. For one frontend and one backend, start with OpenAPI plus a small
set of executable interaction tests. Pact is an option if coordination grows;
a Pact Broker is not a prerequisite for this split. Schema validation alone does
not prove the frontend's assumptions are satisfied.
Sources: [OpenAPI specification](https://spec.openapis.org/oas/v3.0.4.html),
[Pact introduction](https://docs.pact.io/),
[How Pact works](https://docs.pact.io/getting_started/how_pact_works).

### 5. Expand, migrate, contract: use when an interface actually changes

Add a replacement interface, migrate its consumers, then remove the original.
Use this for frontend API changes and any necessary storage refactoring.
Do not preserve discarded Lens/streaming behavior behind permanent compatibility
flags. Database destructive cleanup is a separate step after ownership and data
retention are settled.

Alembic specifically separates schema migrations from more application-specific
data migration concerns. Preserve applied revisions, freeze migration dependencies
on current application metadata, and use separate bounded data-transfer scripts
if data relocation is later needed. Avoid dual writing simply to move source code.
Sources: [Parallel change](https://martinfowler.com/bliki/ParallelChange.html),
[Alembic data-migration guidance](https://alembic.sqlalchemy.org/en/latest/cookbook.html#data-migrations-general-techniques).

### 6. Independent package/build verification: use immediately

Give each active repository its own package definition, lockfile, tests, fixtures
and image build. Trade's package should become trade_data, leaving trade_research
for the extracted research application. Retain src layout and test an installed
wheel from outside the source checkout, with sibling directories unavailable.
Also run clean CLI/API/Dagster startup checks with no ML/agent dependencies.

PyPA explains how src layout helps prevent accidentally testing the development
copy instead of the installed distribution. Remove sibling PYTHONPATH hacks and
do not share a Python namespace between these applications.
Source: [PyPA src layout discussion](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/).

### 7. History-preserving extraction: use selectively

GitHub documents extracting a folder using git-filter-repo in a fresh clone.
This fits apps/web particularly well because it has a clear directory boundary.
Research code is scattered; use a deliberate path list and provenance manifest.
Do not rewrite the active dirty checkout or overwrite the existing sibling copies.

A clone does not capture uncommitted working-tree edits, ignored datasets or
untracked files. Preserve and reconcile those separately using the existing
snapshots and transfer hashes. Filtered history does not itself complete import,
packaging, deployment or credential separation. No force-push is needed here.
Source: [GitHub folder extraction guide](https://docs.github.com/en/get-started/using-git/splitting-a-subfolder-out-into-a-new-repository).

### 8. Idempotent work and bounded retries: preserve during the split

AWS explains retry safety through stable request identifiers and idempotent
operations, and recommends limiting retries. Apply this to job-request IDs,
work-window identities, canonical candle keys, retry/backoff and replica checks.
Keep a job's logical identity separate from the ID of each fresh execution or
reconciliation snapshot. Do not claim end-to-end exactly-once processing merely
because an upsert or request token is present.

Retain raw evidence, invalid-data quarantine, explicit coverage exceptions and
independent PostgreSQL/ClickHouse reconciliation. Test rerunning an identical
window and retrying a partially completed operation. Serialize overlapping writes
while the existing replication proof depends on the latest row's run lineage.
Do not replace the current scheduler or provider as part of extraction.
Sources: [AWS idempotent APIs](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/),
[AWS retry limits](https://docs.aws.amazon.com/wellarchitected/latest/framework/rel_mitigate_interaction_failure_limit_retries.html).

## Techniques to defer

- Many small runtime services: extra operations without a demonstrated need.
- A generic shared library repo: risks recreating coupling across all owners.
- A frontend-specific gateway/BFF: unnecessary until the existing operational API
  cannot serve actual frontend needs cleanly.
- Event buses, Kubernetes, service meshes and event sourcing: not required for
  this repository extraction.
- A contract broker: optional tooling, not a prerequisite for API checks.
- Simultaneous database relocation, scheduler rewrite or provider switch: separate
  changes after source boundaries are proven.

## Revised execution order and evidence

1. Confirm keep/move/discard manifest; extend existing backups and transfer records.
2. Define daily behavior checks and import rules; freeze legacy schema compatibility.
3. Isolate daily API/CLI/Dagster/storage and remove discarded Lens/streaming entry points.
4. Extract operational trade-web with API interaction checks.
5. Complete research extraction against declared data contracts.
6. Remove obsolete code/config/services/dependencies; rename and package daily backend.
7. Test installed artifacts in clean environments and deploy bounded acceptance runs.

Completion requires working independent builds, enforced boundaries, transferred
fixtures and tests, preserved daily data/migrations, API compatibility, a real
incremental and gap-repair run, idempotent rerun and matching replicas. Count
passing/skipped tests by owner so moving fixtures does not silently reduce coverage.
