# Backend image builds

Dockerfile.api installs the committed uv.lock with a pinned uv version and frozen
resolution. Development packages are excluded from the runtime environment.
The dependency stage is cached independently of source; BuildKit caches apt, pip
and uv downloads. CI test jobs use the same lock with the development extra enabled.

The placeholder package in the dependency stage installs metadata and the
trade-data entrypoint. Runtime imports use /app/src after actual source is copied.
No frontend checkout, Node build or sibling repository is required.

API, Dagster daemon and optional Dagster webserver share one image tag in the
production Compose file. The rollout builds it once and recreates only the
application roles. Existing storage services retain their versions unless an
infrastructure upgrade is explicitly selected. No orphan-container deletion occurs.

A local wheel test does not establish a Docker image build. Refer to
[the implementation journal](repository_split_progress.md) for executed build
and deployment evidence.
