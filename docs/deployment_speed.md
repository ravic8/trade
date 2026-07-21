# Deployment Speed and Build Caching

Production currently deploys from the Ubuntu checkout. This phase keeps that
operating model and makes its repeated builds cache-efficient without changing
database migration or health-check safety.

## Implemented cache architecture

### Python image

`Dockerfile.api` now has a stable dependency stage:

```text
pyproject.toml + README
  -> cached Python virtual environment
  -> application source copied into the small runtime image
```

Changing Python source no longer invalidates the expensive third-party
dependency installation layer. BuildKit cache mounts retain pip and apt package
downloads when the dependency layer genuinely changes. Build-only compilers
remain outside the runtime image.

### Web image

The lockfile is copied before application source, and npm uses a persistent
BuildKit cache mount. A source-only change rebuilds the Vite output without
redownloading unchanged packages.

### Shared API/Dagster image

FastAPI, Dagster daemon, and the optional Dagster webserver execute the same
Python application image. Compose now tags all three with `PROD_API_IMAGE`
(default `trade-research-api:local`), and deployment builds that image once
through the `api` service.

### Build context and CI

The Docker context excludes local market datasets, dumps, generated output,
tests, caches, and developer environments. GitHub Actions already uses the
official pip and npm cache integrations; the Python cache is now explicitly
keyed by `pyproject.toml`.

The deployment log records image-build, migration, and total deployment
durations. Use those numbers for warm-versus-cold comparisons; do not infer a
speedup without production measurements.

These choices follow Docker's guidance on ordering stable layers before
frequently changing source and using cache mounts for package managers:
[Docker build cache optimization](https://docs.docker.com/build/cache/optimize/).
The CI dependency caches use GitHub's supported setup-action mechanism:
[GitHub dependency caching](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching).

## Production prerequisites

- Docker Engine with BuildKit support.
- Docker Compose v2.
- Enough disk for reusable BuildKit layers.
- Do not run routine `docker builder prune --all` between deployments; it
  intentionally removes the cache.

Before merging, validate the server once:

```bash
docker version
docker compose version
docker buildx version
```

The existing deployment sequence is unchanged:

```text
sync revision
  -> validate Compose
  -> build API once and web once
  -> start/check PostgreSQL
  -> Alembic migration using the new API image
  -> recreate changed services
  -> application, CloudBeaver, and optional Dagster health checks
```

## Measure after deployment

Capture two consecutive deployments of the same revision or a documentation-
only revision. Compare these log lines:

```text
[trade-deploy] production image build completed in ...s
[trade-deploy] database migrations completed in ...s
[trade-deploy] deployment completed in ...s
```

Also inspect cache reuse with:

```bash
docker compose --env-file /opt/trade/.env \
  -f /opt/trade/app/docker-compose.prod.yml \
  build --progress=plain api web
```

Warm builds should show cached dependency steps. Exact timing depends on server
CPU, disk, network, and whether dependency manifests changed.

## Recommended second phase: immutable registry images

The best long-term design moves compilation away from the production server:

```text
GitHub Actions after successful CI
  -> buildx API and web images
  -> GitHub Actions or registry cache
  -> push immutable GHCR tags by commit SHA
Ubuntu deployment
  -> authenticate read-only to GHCR
  -> pull exact SHA-tagged images
  -> run Alembic from that API image
  -> health check and retain previous image for rollback
```

Docker documents registry-backed external caches for ephemeral CI builders in
the same [cache optimization guide](https://docs.docker.com/build/cache/optimize/).
Compose supports explicit image tags alongside build definitions:
[Compose build specification](https://docs.docker.com/reference/compose-file/build/).

This second phase needs an explicit decision and credentials:

- GHCR organization/repository and package visibility.
- A GitHub Actions token with package-write permission.
- A production read-only package token if the images remain private.
- Retention policy for SHA tags and build-cache tags.
- Rollback retention target, recommended at least the previous two releases.

Do not place registry credentials in Git, Dockerfiles, Compose, or image
layers. Until those details are supplied, production continues using its local
BuildKit cache safely.
