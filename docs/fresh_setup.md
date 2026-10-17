# Fresh daily backend acceptance

Run `python scripts/verify_fresh_setup.py` on a host with Docker and Buildx. The
verifier copies the application into temporary storage, uses a uniquely named
Compose project and loopback ports, and removes only resources it created.
It requires no provider credentials or provider execution.

The proof builds and starts the three core services, confirms an empty database,
applies migrations to the current Alembic head twice, checks idempotence and
Timescale availability, verifies required-service diagnostics and API health,
and confirms empty market status with no generated observations or work.
Discarded filing routes must return 404. It records source/image/schema evidence
and cleanup status in `output/fresh-setup/`.

This verifier covers the backend. trade-web owns browser and proxy acceptance.
External database/rate-limit endpoints use the separate
[external-services proof](external_services.md). Recovery and upgrade acceptance
are described in [core recovery](core_recovery.md).

Reports from earlier revisions are historical. See
[the implementation journal](repository_split_progress.md) for checks actually
performed against the current split; a local source test does not prove that
this Docker acceptance ran.
