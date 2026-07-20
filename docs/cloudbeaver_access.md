# Browser analytics access with CloudBeaver

CloudBeaver provides browser-based SQL access to curated PostgreSQL analytics
views. It does not expose PostgreSQL to the internet and it does not replace
PostgreSQL as the operational source of truth.

```text
analyst browser
  -> https://sql.chain8.org
  -> Cloudflare Access (explicit email allowlist)
  -> existing Cloudflare Tunnel
  -> loopback-only Caddy entrypoint
  -> CloudBeaver on the production Compose network
  -> postgres:5432 with an individual read-only analyst role
```

CloudBeaver has no published host port. Caddy routes only the configured
`PROD_CLOUDBEAVER_HOST` to `cloudbeaver:8978`. CloudBeaver, Caddy, and
PostgreSQL share a dedicated internal Docker network; CloudBeaver is not on the
application network and has no outbound network route. PostgreSQL retains its
existing loopback-only host binding and must not be opened in the server
firewall.

## Production variables

Add these values to `/opt/trade/.env`:

```text
PROD_CLOUDBEAVER_HOST=sql.chain8.org
PROD_CLOUDBEAVER_IMAGE=dbeaver/cloudbeaver:26.1.2
PROD_CLOUDBEAVER_WORKSPACE_DIR=/opt/trade/cloudbeaver
PROD_CLOUDBEAVER_SESSION_IDLE_MS=1800000
```

The image is version-pinned so routine deployments do not silently adopt a
new CloudBeaver release. Upgrade the version through a reviewed pull request.

The deploy script creates the persistent workspace and installs the
secret-free shared connection policy from `deploy/cloudbeaver/`. It never
installs a database username or password. When that policy changes, a running
CloudBeaver service is restarted so the updated permissions take effect; an
unchanged policy does not add a restart to routine deployments.

## Cloudflare configuration

Keep the existing `trade.chain8.org` public hostname unchanged. Add a second
public hostname to the same tunnel:

```text
Hostname: sql.chain8.org
Service:  http://localhost:8080
```

Use the actual loopback `PROD_WEB_PORT` when it is not `8080`. Both hostnames
reach Caddy, which selects the upstream from the HTTP Host header.

Create a separate Cloudflare Access self-hosted application for
`sql.chain8.org`:

1. Add one Allow policy containing only the maintainer and approved analyst
   email addresses.
2. Use the configured Google identity provider and require its MFA control.
3. Do not use `Include Everyone`, an email-domain wildcard, or a Bypass policy.
4. Use a bounded Access session duration, such as eight hours.
5. Confirm an unapproved browser is denied before inviting the analyst.

CloudBeaver deliberately uses its own local login after Cloudflare Access.
Caddy removes Cloudflare identity/JWT headers before proxying to CloudBeaver,
so CloudBeaver does not implicitly trust an unvalidated forwarded identity.

## First deployment and CloudBeaver bootstrap

Deploy normally after the variables and Cloudflare route exist:

```bash
cd /opt/trade/app
./deploy/deploy.sh
```

Deployment succeeds only after both the application API and CloudBeaver respond
through Caddy on their respective host routes.

Verify that only the existing web and PostgreSQL loopback ports are published:

```bash
docker compose --env-file /opt/trade/.env \
  -f /opt/trade/app/docker-compose.prod.yml ps

ss -lnt | grep -E ':(8080|5433)\b'
ss -lnt | grep ':8978\b' && echo 'unexpected CloudBeaver host port'
```

Open `https://sql.chain8.org` as the maintainer. On first launch, complete the
CloudBeaver setup wizard and create a strong local administrator password. Do
not reuse the PostgreSQL application password.

In CloudBeaver administration:

1. Confirm anonymous access is disabled.
2. Confirm private/custom connections are disabled for non-admin users.
3. Confirm both global and user database credential saving are disabled.
4. Create one local CloudBeaver account per analyst.
5. Grant the account the normal `user` team only; do not grant administrator
   permissions.

The shared `Trade Analytics PostgreSQL` connection is installed automatically:

```text
Host:       postgres
Port:       5432
Database:   trade_research
Project:    Shared
Credentials: not stored
```

## Create an analyst database role

Create a separate PostgreSQL role for each person. The command prompts without
echoing the password:

```bash
DC=(docker compose \
  --env-file /opt/trade/.env \
  -f /opt/trade/app/docker-compose.prod.yml)

"${DC[@]}" run --rm --no-deps api \
  trade-research create-analyst-role analyst_name
```

The policy grants only the `analytics` schema and enforces:

```text
default_transaction_read_only = on
connection limit = 2
statement timeout = 5 minutes
idle transaction timeout = 1 minute
lock timeout = 5 seconds
search_path = analytics
```

Give the database password to the analyst through a secure password manager,
not email, chat, Git, a shell argument, or a committed file. The analyst enters
it when opening the shared connection; the password remains session-only.

## Verification

Run this from CloudBeaver:

```sql
SELECT
    current_database(),
    current_user,
    current_setting('transaction_read_only'),
    current_setting('statement_timeout'),
    current_setting('idle_in_transaction_session_timeout'),
    current_setting('lock_timeout');
```

Verify the curated views:

```sql
SELECT table_name
FROM information_schema.views
WHERE table_schema = 'analytics'
ORDER BY table_name;

SELECT exchange, source, COUNT(*) AS rows, MAX(date) AS latest_date
FROM analytics.ohlcv_daily
GROUP BY exchange, source
ORDER BY exchange, source;
```

Verify direct application-table access and writes fail:

```sql
SELECT * FROM public.provider_credentials;
CREATE TABLE cloudbeaver_write_test (id integer);
```

Expected results are permission denied for the application table and a
read-only transaction error for the write.

## Revocation

Disable all three access layers:

1. Remove the email from the `sql.chain8.org` Cloudflare Access policy.
2. Disable or delete the person's CloudBeaver account.
3. Disable the PostgreSQL login and terminate its active sessions:

```bash
"${DC[@]}" run --rm --no-deps api \
  trade-research revoke-analyst-role analyst_name
```

Confirm the role cannot log in:

```sql
SELECT rolname, rolcanlogin, rolconnlimit
FROM pg_roles
WHERE rolname = 'analyst_name';

SELECT COUNT(*)
FROM pg_stat_activity
WHERE usename = 'analyst_name';
```

The expected values are `rolcanlogin = false` and zero active sessions.

## Backup and restore

`deploy/backup.sh` briefly stops CloudBeaver, archives the workspace as
`cloudbeaver.tgz`, and restarts it if it was previously running. This preserves
local CloudBeaver users and settings consistently. The shared database
connection policy also remains recoverable from Git.

Test workspace restoration on a non-production path before relying on the
backup. Database analyst roles are part of the PostgreSQL dump, not the
CloudBeaver workspace.

## Desktop DBeaver remains supported

CloudBeaver is the preferred browser path for remote analysts. Desktop DBeaver
can still use its existing SSH-tunnel model from
`docs/database_access_with_dbeaver.md`; no public PostgreSQL port is required
for either workflow.
