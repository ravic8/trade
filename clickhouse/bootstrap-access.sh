#!/bin/sh
set -eu

required_variables="
CLICKHOUSE_ADMIN_USER CLICKHOUSE_ADMIN_PASSWORD
CLICKHOUSE_MIGRATION_USER CLICKHOUSE_MIGRATION_PASSWORD
CLICKHOUSE_DAGSTER_USER CLICKHOUSE_DAGSTER_PASSWORD
CLICKHOUSE_API_USER CLICKHOUSE_API_PASSWORD
CLICKHOUSE_ANALYST_USER CLICKHOUSE_ANALYST_PASSWORD
"
for variable_name in $required_variables; do
  eval "variable_value=\${$variable_name:-}"
  if [ -z "$variable_value" ]; then
    printf 'missing required ClickHouse bootstrap variable: %s\n' "$variable_name" >&2
    exit 1
  fi
done

client() {
  clickhouse-client \
    --host clickhouse \
    --user "$CLICKHOUSE_ADMIN_USER" \
    --password "$CLICKHOUSE_ADMIN_PASSWORD" \
    "$@"
}

validate_identifier() {
  case "$1" in
    "" | [0-9]* | *[!A-Za-z0-9_]*)
      printf 'invalid ClickHouse identifier: %s\n' "$1" >&2
      exit 1
      ;;
  esac
}

client --multiquery --query "
CREATE DATABASE IF NOT EXISTS research;
CREATE ROLE IF NOT EXISTS migration;
CREATE ROLE IF NOT EXISTS dagster_writer;
CREATE ROLE IF NOT EXISTS api_reader;
CREATE ROLE IF NOT EXISTS analyst_reader;
GRANT CREATE DATABASE ON *.* TO migration;
GRANT ALL ON research.* TO migration;
GRANT SELECT, INSERT ON research.* TO dagster_writer;
GRANT SELECT ON research.* TO api_reader;
GRANT SELECT ON research.* TO analyst_reader;
"

upsert_user() {
  user_name="$1"
  user_password="$2"
  role_name="$3"
  max_seconds="$4"
  max_memory="$5"
  readonly_value="$6"
  validate_identifier "$user_name"
  validate_identifier "$role_name"
  case "$max_seconds:$max_memory:$readonly_value" in
    *[!0-9:]* | *::* | :* | *:)
      printf 'invalid numeric ClickHouse role limit\n' >&2
      exit 1
      ;;
  esac
  client \
    --param_user_password "$user_password" \
    --multiquery \
    --query "
      CREATE USER IF NOT EXISTS $user_name
        IDENTIFIED WITH sha256_password BY {user_password:String};
      ALTER USER $user_name
        IDENTIFIED WITH sha256_password BY {user_password:String}
        SETTINGS
          max_execution_time = $max_seconds,
          max_memory_usage = $max_memory,
          readonly = $readonly_value;
      GRANT $role_name TO $user_name;
      ALTER USER $user_name DEFAULT ROLE $role_name;
    "
}

upsert_user \
  "$CLICKHOUSE_MIGRATION_USER" "$CLICKHOUSE_MIGRATION_PASSWORD" \
  migration 120 1073741824 0
upsert_user \
  "$CLICKHOUSE_DAGSTER_USER" "$CLICKHOUSE_DAGSTER_PASSWORD" \
  dagster_writer 60 1073741824 0
upsert_user \
  "$CLICKHOUSE_API_USER" "$CLICKHOUSE_API_PASSWORD" \
  api_reader 15 268435456 1
upsert_user \
  "$CLICKHOUSE_ANALYST_USER" "$CLICKHOUSE_ANALYST_PASSWORD" \
  analyst_reader 60 536870912 1

printf 'ClickHouse research identities are reconciled.\n'
