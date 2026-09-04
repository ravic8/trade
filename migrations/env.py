from __future__ import annotations

from logging.config import fileConfig
from typing import Any

from alembic import context
from sqlalchemy import engine_from_config, pool

from trade_research.config import get_settings
from trade_research.control_plane import tables as control_plane_tables
from trade_research.operations.tables import workflow_requests_table as workflow_requests_table
from trade_research.storage.timescale import metadata

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

config.set_main_option("sqlalchemy.url", get_settings().database_url)
target_metadata = metadata

# Import registration is intentional: the tables share the operational
# metadata used by Alembic autogeneration.
_control_plane_tables = control_plane_tables


def include_object(
    _object: Any,
    _name: str | None,
    object_type: str,
    reflected: bool,
    compare_to: Any,
) -> bool:
    # The pre-Alembic schema creates a set of performance indexes explicitly in
    # TimescaleStore.initialize(). Until those indexes are moved into metadata,
    # autogenerate must not interpret them as candidates for removal.
    if object_type == "index" and reflected and compare_to is None:
        return False
    return True


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            include_object=include_object,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
