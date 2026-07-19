# Database access with DBeaver

DBeaver can query the production Timescale/PostgreSQL database without exposing
PostgreSQL to the public internet. The production Compose configuration binds
the container to server loopback only:

```text
127.0.0.1:${PROD_POSTGRES_HOST_PORT:-5433}:5432
```

Do not open TCP 5432 or 5433 in the server firewall. Do not give analysts the
application's `trade` database account.

## One-time server configuration

Set the host port in `/opt/trade/.env`:

```text
PROD_POSTGRES_HOST_PORT=5433
```

Apply the port mapping without recreating the database volume:

```bash
cd /opt/trade/app

DC=(docker compose \
  --env-file /opt/trade/.env \
  -f /opt/trade/app/docker-compose.prod.yml)

"${DC[@]}" up -d --no-deps --force-recreate postgres

ss -lnt | grep '127.0.0.1:5433'
```

Apply migrations first; Phase 9.2A creates the curated `analytics` schema and
views. Create one read-only login per person. The CLI prompts twice without
echoing the password:

```bash
"${DC[@]}" run --rm --no-deps api \
  trade-research create-analyst-role analyst_name
```

For automation, pipe a single value directly from a secret manager; do not use
a literal command-line argument or committed file:

```bash
secret-manager read trade/analysts/analyst_name \
  | "${DC[@]}" run -T --rm --no-deps api \
      trade-research create-analyst-role analyst_name --password-stdin
```

The command creates or rotates the individual login, sets
`default_transaction_read_only = on`, revokes public-schema creation and
grants `SELECT` only on curated analytics views. Analysts do not receive direct
access to application tables or credentials. The migration also removes the
legacy `PUBLIC` create privilege from the `public` schema; the application
owner retains its owner privileges.

## DBeaver with its built-in SSH tunnel

Create a PostgreSQL connection in DBeaver.

Main connection settings:

```text
Host:       127.0.0.1
Port:       5433
Database:   trade_research
Username:   analyst_name
Password:   the analyst database password
```

Enable the connection's SSH tunnel and enter:

```text
SSH host:       production server hostname or IP
SSH port:       22, or the server's configured SSH port
SSH user:       the person's own Linux account
Authentication: the person's own SSH private key
```

DBeaver creates the tunnel and connects from the server to
`127.0.0.1:5433`. A collaborator in another country uses the same setup;
geography does not change the database configuration. Each collaborator must
have an individually revocable SSH key and database role.

If DBeaver's SSH tunnel is not used, create a local tunnel first:

```bash
ssh -N \
  -L 15432:127.0.0.1:5433 \
  analyst-linux-user@production-server
```

Then configure DBeaver with host `127.0.0.1` and port `15432`.

## Verification

Run this query in DBeaver:

```sql
SELECT
    current_database(),
    current_user,
    current_setting('transaction_read_only'),
    inet_server_addr(),
    inet_server_port();
```

`current_user` must be the analyst role and `transaction_read_only` must be
`on`. Verify that writes fail:

```sql
CREATE TABLE dbeaver_write_test (id integer);
```

The expected result is a read-only transaction error. Do not override the
read-only setting.

Useful starting queries are:

```sql
SELECT exchange, source, COUNT(*) AS rows, MAX(date) AS latest_date
FROM analytics.ohlcv_daily
GROUP BY exchange, source
ORDER BY exchange, source;

SELECT exchange, work_type, status, COUNT(*) AS items
FROM analytics.pipeline_work_state
GROUP BY exchange, work_type, status
ORDER BY exchange, work_type, status;

SELECT provider, current_rpm, current_concurrency, circuit_state,
       recent_error_rate, cooldown_until, updated_at
FROM analytics.provider_health
ORDER BY provider;
```

The available views are `analytics.ohlcv_daily`, `analytics.symbol_state`,
`analytics.pipeline_work_state`, `analytics.ingestion_runs`,
`analytics.provider_health`, and `analytics.universe_lifecycle`.

## Revocation

Revoke the database login through the same one-off Compose invocation used for
creation:

```bash
"${DC[@]}" run --rm --no-deps api \
  trade-research revoke-analyst-role analyst_name
```

Also remove the person's SSH public key or disable their Linux account. Confirm
revocation with `SELECT rolcanlogin FROM pg_roles WHERE rolname =
'analyst_name';` as an administrator. Prefer `NOLOGIN` because it is immediate,
auditable, and reversible; drop the role only after checking dependencies.
