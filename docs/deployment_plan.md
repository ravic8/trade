# Deployment Plan

This document captures the deployment direction for the Trade Research app.
The repo remains local-first for development, and production is now deployed on
the maintainer's Ubuntu machine behind Cloudflare Tunnel and Cloudflare Access.

## Current Deployment Status

Production is live:

```text
Public URL:       https://trade.chain8.org
Public access:    Cloudflare Access
Tunnel target:    http://localhost:8081
Ubuntu app path:  /opt/trade/app
Server env file:  /opt/trade/.env
Compose project:  trade-prod
```

The production compose project is named `trade-prod` so it can run separately
from the local development stack. It exposes only the web entrypoint on
`127.0.0.1:${PROD_WEB_PORT:-8080}`; the deployed Ubuntu host currently sets
`PROD_WEB_PORT=8081`. Internal services are reachable only inside
the Docker network unless an admin profile or tunnel exposes them.

Production compose variables use a `PROD_` prefix to avoid accidentally loading
local development secrets from `.env` during validation.

Implemented deployment files:

```text
Dockerfile.web
docker-compose.prod.yml
deploy/caddy/Caddyfile
.env.prod.example
deploy/deploy.sh
deploy/backup.sh
.github/workflows/ci.yml
.github/workflows/deploy.yml
```

Admin Upstox token management is implemented and verified in production:

```text
/settings/providers
GET  /api/admin/provider-credentials/upstox/status
POST /api/admin/provider-credentials/upstox/test
POST /api/admin/provider-credentials/upstox/token
provider_credentials table
```

Admin access is controlled by `ADMIN_EMAILS` / `PROD_ADMIN_EMAILS` and trusted
identity headers such as `cf-access-authenticated-user-email`. The Upstox token
is encrypted before it is stored, and the raw token is never returned by the API.
The production token source shows as `database` after saving through the
provider settings page.

Production recovery after power loss is configured:

```text
BIOS AC BACK: Always On
docker.service: enabled
tailscaled.service: enabled
cloudflared.service: enabled
trade-prod.service: enabled
```

The `trade-prod.service` systemd unit runs `docker compose up -d` from
`/opt/trade/app`. It is expected to show `active (exited)` because it is a
oneshot service that starts containers and then exits successfully.

## Goals

- Deploy the app on the Ubuntu desktop/server owned by the project maintainer.
- Keep local development on feature branches, with PR review before merge.
- Deploy only after changes are merged to `main`.
- Support a small trusted user group: the maintainer, one engineer contributor,
  and one external research user.
- Avoid public router port forwarding because the Ubuntu machine uses normal
  consumer internet and may be behind NAT, dynamic IP, or CGNAT.
- Let the maintainer update the daily Upstox access token from the browser
  without editing server files or restarting containers.
- Keep databases, queues, vector stores, orchestration UIs, and raw admin tools
  private.

## Target Architecture

```text
External users
  -> https://trade.chain8.org
  -> Cloudflare Access
  -> Cloudflare Tunnel
  -> http://localhost:8081 on Ubuntu
  -> Caddy
  -> React app and /api reverse proxy
  -> FastAPI
  -> TimescaleDB, Redis, Qdrant, data/, artifacts/

Maintainer/admin
  -> Tailscale
  -> SSH, deployment commands, Dagster admin, database tools
```

Cloudflare Tunnel is the public application path. Tailscale is the private
admin and deployment path.

## Production Runtime

Production should use a separate compose file instead of exposing the current
development stack directly.

Deployment files:

```text
Dockerfile.web                 implemented
docker-compose.prod.yml        implemented
deploy/caddy/Caddyfile         implemented
.env.prod.example              implemented
deploy/deploy.sh               implemented
deploy/backup.sh               implemented
.github/workflows/ci.yml       implemented
.github/workflows/deploy.yml   implemented
```

Production services:

```text
caddy             public HTTP entrypoint behind Cloudflare Tunnel
web               built React static files
api               FastAPI application
postgres          TimescaleDB/PostgreSQL
redis             internal cache/queue dependency
qdrant            internal vector store
dagster-daemon    internal scheduled job runner
dagster-webserver private admin UI, reachable only over Tailscale
```

The production frontend should be built with:

```bash
npm run build
```

This produces static HTML, JavaScript, and CSS files. It does not make the app
fake or data-static. The built frontend still calls the live backend through
`/api/...`.

Local development:

```text
Vite dev server -> FastAPI
```

Production:

```text
Caddy static files -> FastAPI
```

The user-facing app should look the same after a PR is merged and deployed,
assuming production has the same database/artifact state as local.

## Internal-Only Services

Internal-only means a service is reachable by other containers or by the admin
over Tailscale, but not directly from the public internet.

Do not expose these publicly:

```text
Postgres/Timescale 5432
Redis 6379
Qdrant 6333/6334
Dagster webserver 3000
CloudBeaver or other database admin tools
```

The public surface should be only:

```text
https://trade.chain8.org
```

All application API traffic should go through the reverse proxy:

```text
https://trade.chain8.org/api/...
```

## Access Model

Use Cloudflare Access for the public app:

- Allow the configured maintainer/admin emails.
- Allow the configured engineer/research user emails.
- Block everyone else.

Unauthenticated terminal requests to `https://trade.chain8.org` are expected to
return a Cloudflare Access redirect such as `302 Found`. Authenticated browser
sessions should reach the app.

Use Tailscale for private admin access:

- SSH to the Ubuntu machine.
- Deployment commands from GitHub Actions or maintainer laptop.
- Dagster admin UI.
- Database administration.
- Debugging private service endpoints.

## Secrets Model

GitHub Secrets should store deployment credentials only:

```text
TAILSCALE_AUTHKEY
DEPLOY_HOST
DEPLOY_USER
DEPLOY_PATH
DEPLOY_SSH_PRIVATE_KEY
```

Optional GitHub Secrets if Cloudflare automation is added later:

```text
CLOUDFLARE_API_TOKEN
CLOUDFLARE_ACCOUNT_ID
```

Runtime application secrets should live on the Ubuntu server, not in GitHub:

```text
/opt/trade/.env
```

Examples:

```text
PROD_POSTGRES_PASSWORD=...
PROD_DATABASE_URL=...
PROD_OPENAI_API_KEY=...
PROD_GEMINI_API_KEY=...
PROD_UPSTOX_ACCESS_TOKEN=...
PROD_APP_SECRET_KEY=...
PROD_ADMIN_EMAILS=you@example.com
PROD_API_CORS_ORIGINS=https://trade.chain8.org
```

The committed `.env.prod.example` must stay secret-free.

## Browser-Based Upstox Token Updates

The maintainer needs to update the Upstox access token daily. The production app
provides an admin-only browser workflow for this.

Admin route:

```text
/settings/providers
```

Backend endpoints:

```text
GET  /api/admin/provider-credentials/upstox/status
POST /api/admin/provider-credentials/upstox/test
POST /api/admin/provider-credentials/upstox/token
```

Expected behavior:

- The browser submits a new token to the backend.
- The backend validates the token against Upstox.
- The backend stores the token encrypted server-side.
- The raw token is never returned to the browser.
- Data pipelines read the latest stored token.
- `.env` remains a fallback, not the daily update mechanism.

Do not store the Upstox token in:

```text
React localStorage
browser cookies
frontend source code
GitHub Secrets
committed .env files
```

Recommended storage:

```text
provider_credentials
```

Suggested fields:

```text
provider
credential_type
encrypted_value
updated_at
updated_by
last_validated_at
validation_status
validation_message
```

Use `APP_SECRET_KEY` from the server `.env` to encrypt/decrypt the stored token.

Token resolution order:

```text
1. encrypted provider_credentials token
2. UPSTOX_ACCESS_TOKEN from server .env
3. clear missing-token error
```

Only the maintainer/admin should be able to update provider credentials.
Read-only users should not see raw secrets.

## CI Workflow

PRs should run checks before merge:

```bash
.venv/bin/python -m pytest
.venv/bin/python -m ruff check
cd apps/web && npm ci && npm run lint && npm run build
docker compose -f docker-compose.prod.yml config
```

The exact Python environment in CI may differ from local `.venv`; the important
contract is that tests, linting, frontend build, and production compose config
all pass before merge.

## Deployment Workflow

After a PR is merged to `main`:

```text
GitHub Actions CI succeeds on main
  -> joins Tailscale
  -> SSHes to Ubuntu
  -> runs deploy/deploy.sh
```

The deploy script should:

```bash
git fetch origin
git checkout main
git pull --ff-only
docker compose -f docker-compose.prod.yml build
docker compose -f docker-compose.prod.yml up -d
curl -f http://localhost:${PROD_WEB_PORT:-8080}/api/health
```

If the health check fails, the deployment should fail loudly. A later iteration
can add image tagging and automatic rollback.

The deploy workflow is implemented in `.github/workflows/deploy.yml`. It runs
after the `CI` workflow succeeds on `main`, and it can also be started manually
with `workflow_dispatch`.

The implemented script defaults to:

```bash
TRADE_APP_DIR=/opt/trade/app
TRADE_ENV_FILE=/opt/trade/.env
TRADE_DEPLOY_BRANCH=main
deploy/deploy.sh
```

It loads `/opt/trade/.env`, creates the configured persistent directories,
validates compose config, builds images, starts the stack, and checks
`http://localhost:${PROD_WEB_PORT:-8080}/api/health`.

The deployed Ubuntu host currently uses:

```text
PROD_WEB_PORT=8081
PROD_API_CORS_ORIGINS=https://trade.chain8.org
```

## Persistent Server Paths

Use stable server-owned paths outside the git checkout:

```text
/opt/trade/app
/opt/trade/.env
/opt/trade/data
/opt/trade/artifacts
/opt/trade/postgres
/opt/trade/qdrant
/opt/trade/dagster_home
/opt/trade/backups
```

Generated datasets and artifacts should not be committed to git. They should be
mounted into containers as persistent server data.

The deployed Dagster home also contains runtime config files required by the
daemon:

```text
/opt/trade/dagster_home/workspace.yaml
/opt/trade/dagster_home/dagster.yaml
```

## Backup Plan

Initial backup target can be another local SSD on the Ubuntu machine.

Back up:

```text
Timescale/Postgres dump
data/
artifacts/
dagster_home/
encrypted provider credentials
```

Do not back up raw decrypted secrets. If encrypted provider credentials are
backed up, `APP_SECRET_KEY` must be backed up separately and securely or the
credentials cannot be restored.

The implemented backup script defaults to:

```bash
TRADE_APP_DIR=/opt/trade/app
TRADE_ENV_FILE=/opt/trade/.env
PROD_BACKUP_DIR=/opt/trade/backups
deploy/backup.sh
```

It writes a timestamped backup directory containing a Postgres custom-format
dump plus archives for `data/`, `artifacts/`, `qdrant/`, and `dagster_home/`
when those directories exist.

## Implementation Phases

### Phase 1: Production Packaging

- Add production Docker and Caddy files.
- Keep current `docker-compose.yml` for local development.
- Verify production compose locally.

### Phase 2: Admin Upstox Token Feature

- Implemented: encrypted provider credential storage.
- Implemented: admin status/test/save endpoints.
- Implemented: `/settings/providers` frontend page.
- Implemented: `.env` token fallback.

### Phase 3: Pipeline Token Integration

- Implemented: API-triggered and batch daily OHLCV paths prefer stored encrypted
  credentials.
- Implemented: `.env` fallback remains available for bootstrap.
- Remaining: expose richer expired-token status after live validation failures.

### Phase 4: CI

- Implemented: PR and `main` checks for Python, frontend, and production
  compose configuration.

### Phase 5: Manual Ubuntu Deployment

- Implemented: installed Docker, Tailscale, and Cloudflared on Ubuntu.
- Implemented: created `/opt/trade` paths.
- Implemented: added server `.env`.
- Implemented: ran first production deployment manually with `deploy/deploy.sh`.
- Implemented: verified `/api/health` through localhost and through the
  Cloudflare URL.

### Phase 6: Cloudflare And Tailscale

- Implemented: Cloudflare Tunnel routes `trade.chain8.org` to
  `http://localhost:8081`.
- Implemented: Cloudflare Access protects `trade.chain8.org`.
- Implemented: Tailscale private access supports deploy/admin SSH.

### Phase 7: Auto Deploy On Merge

- Implemented: GitHub Actions deploy workflow.
- Implemented: use Tailscale plus SSH to run `deploy/deploy.sh`.
- Implemented: deploy only after CI succeeds on merged `main`; manual dispatch
  is available for operator-controlled retries.

### Phase 8: Backups

- Implemented: first-pass backup script.
- Schedule backups.
- Test restore on a non-production path before relying on backups.

### Phase 9: Power-Loss Recovery

- Implemented: BIOS `AC BACK` set to `Always On`.
- Implemented: Docker, Tailscale, Cloudflared, and `trade-prod.service` enabled
  at boot.
- Verified: after service startup, local health passes at
  `http://localhost:8081/api/health`.
- Verified: public unauthenticated `https://trade.chain8.org/api/health`
  returns Cloudflare Access redirect, which confirms the app is protected.

## Open Decisions

- Whether Cloudflare Access email headers remain sufficient for admin
  authorization long term, or whether the app should add its own auth layer.
- Whether Dagster webserver should run continuously or only be started when
  admin access is needed.
- Whether backups should remain local-only first or include offsite storage
  immediately.
- Whether deploys should keep building images on the Ubuntu server or move image
  builds into GitHub Actions later.
- Whether to migrate the GitHub deploy workflow from Tailscale auth key to
  Tailscale OAuth client credentials. The current auth-key workflow works, but
  GitHub Actions shows a Tailscale deprecation warning recommending OAuth.
