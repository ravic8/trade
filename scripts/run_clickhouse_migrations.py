#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

import clickhouse_connect

from trade_research.config import get_settings
from trade_research.storage.clickhouse_migrations import (
    ClickHouseMigrator,
    discover_migrations,
)


def main() -> None:
    settings = get_settings()
    if not settings.clickhouse_enabled:
        raise SystemExit("CLICKHOUSE_ENABLED must be true to run migrations")
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_username,
        password=settings.clickhouse_password,
        secure=settings.clickhouse_secure,
        connect_timeout=settings.clickhouse_connect_timeout_seconds,
        send_receive_timeout=settings.clickhouse_query_timeout_seconds,
    )
    directory = Path(__file__).resolve().parents[1] / "clickhouse" / "migrations"
    migrations = discover_migrations(directory, database=settings.clickhouse_database)
    applied = ClickHouseMigrator(client, database=settings.clickhouse_database).apply(migrations)
    print(f"Applied {len(applied)} ClickHouse migration(s): {applied}")


if __name__ == "__main__":
    main()
