# Database Migrations

Production schema changes are managed with Alembic. `TimescaleStore.initialize()`
remains available for local/test bootstrap compatibility, but deployments must
run migrations before starting the API, Dagster daemon, or workers.

The first revision is a transition from the repository's pre-Alembic schema. It
adds Phase 1 tables and symbol lifecycle columns without recreating or deleting
existing market-data tables.

```bash
alembic upgrade head
alembic current
```

`DATABASE_URL` is loaded through `trade_research.config.Settings`, so the same
database configuration is used by the application and migrations.

For a new local database, `trade-research init-db` remains the compatibility
bootstrap during Phase 1; then stamp/apply the Alembic head as appropriate. A
later cleanup phase can move the full legacy schema into Alembic once every
supported environment has crossed the transition revision.

During this transition, Alembic autogeneration ignores reflected indexes that
are not declared in SQLAlchemy metadata. Those legacy performance indexes are
still owned by `TimescaleStore.initialize()` and must not be proposed for
deletion. New Phase 1 indexes are declared in metadata and remain fully checked.
