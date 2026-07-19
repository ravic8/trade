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

Create one read-only login per person. Enter its password interactively so it
does not appear in shell history:

```bash
"${DC[@]}" exec postgres \
  psql \
  -U "${PROD_POSTGRES_USER:-trade}" \
  -d "${PROD_POSTGRES_DB:-trade_research}"
```

Run the following inside `psql`, replacing `analyst_name` with a unique role:

```sql
CREATE ROLE analyst_name LOGIN;
\password analyst_name

GRANT CONNECT ON DATABASE trade_research TO analyst_name;
GRANT USAGE ON SCHEMA public TO analyst_name;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst_name;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO analyst_name;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO analyst_name;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON SEQUENCES TO analyst_name;

ALTER ROLE analyst_name SET default_transaction_read_only = on;
```

The default-privilege grants apply to objects subsequently created by the role
that executes those statements. Re-run the explicit `GRANT SELECT ON ALL
TABLES` after migrations if a migration owner differs from that role.

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
FROM ohlcv_daily
GROUP BY exchange, source
ORDER BY exchange, source;

SELECT exchange, work_type, status, COUNT(*) AS items
FROM pipeline_work_items
GROUP BY exchange, work_type, status
ORDER BY exchange, work_type, status;

SELECT provider, current_rpm, current_concurrency, circuit_state,
       recent_error_rate, cooldown_until, updated_at
FROM adaptive_rate_state
ORDER BY provider;
```

## Revocation

Remove the person's SSH public key or disable their Linux account, then revoke
the database login:

```sql
ALTER ROLE analyst_name NOLOGIN;
```

After confirming there are no dependent grants or owned objects, the role can
be dropped. Prefer `NOLOGIN` first because it is immediate and reversible.
