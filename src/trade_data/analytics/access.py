from __future__ import annotations

import re
from collections.abc import Sequence

from psycopg import sql
from sqlalchemy import create_engine, text

_ROLE_PATTERN = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def validate_analyst_role_name(role_name: str) -> str:
    normalized = role_name.strip().lower()
    if not _ROLE_PATTERN.fullmatch(normalized):
        raise ValueError(
            "Role names must start with a lowercase letter or underscore and contain "
            "only lowercase letters, digits, and underscores (maximum 63 characters)."
        )
    return normalized


def analyst_role_statements(role_name: str, database_name: str) -> Sequence[str]:
    """Return the auditable SQL policy applied after a role login is created."""
    role = validate_analyst_role_name(role_name)
    database = _validated_database_name(database_name)
    return (
        f"GRANT CONNECT ON DATABASE {database} TO {role}",
        f"REVOKE CREATE ON SCHEMA public FROM {role}",
        f"GRANT USAGE ON SCHEMA analytics TO {role}",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO {role}",
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO {role}",
        f"ALTER ROLE {role} CONNECTION LIMIT 2",
        f"ALTER ROLE {role} SET default_transaction_read_only = on",
        f"ALTER ROLE {role} SET statement_timeout = '5min'",
        f"ALTER ROLE {role} SET idle_in_transaction_session_timeout = '1min'",
        f"ALTER ROLE {role} SET lock_timeout = '5s'",
        f"ALTER ROLE {role} SET search_path = analytics",
    )


def create_or_update_analyst_role(
    database_url: str,
    role_name: str,
    password: str,
) -> None:
    """Create an individual login or rotate it, then enforce analytics-only access."""
    role = validate_analyst_role_name(role_name)
    if not password:
        raise ValueError("An analyst password is required.")
    engine = create_engine(database_url, hide_parameters=True, pool_pre_ping=True)
    if engine.dialect.name != "postgresql":
        raise ValueError("Analyst roles can only be managed on PostgreSQL.")
    database_name = str(engine.url.database or "")
    statements = analyst_role_statements(role, database_name)
    raw_connection = engine.raw_connection()
    try:
        with raw_connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
            verb = sql.SQL("ALTER") if cursor.fetchone() else sql.SQL("CREATE")
            cursor.execute(
                sql.SQL("{} ROLE {} LOGIN PASSWORD {}").format(
                    verb,
                    sql.Identifier(role),
                    sql.Literal(password),
                )
            )
            for statement in statements:
                cursor.execute(statement)
        raw_connection.commit()
    except Exception:
        raw_connection.rollback()
        raise
    finally:
        raw_connection.close()
        engine.dispose()


def revoke_analyst_role(database_url: str, role_name: str) -> None:
    """Immediately disable an analyst login without destroying audit history."""
    role = validate_analyst_role_name(role_name)
    engine = create_engine(database_url, hide_parameters=True, pool_pre_ping=True)
    if engine.dialect.name != "postgresql":
        raise ValueError("Analyst roles can only be managed on PostgreSQL.")
    with engine.begin() as connection:
        connection.execute(text(f"ALTER ROLE {role} NOLOGIN"))
        connection.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE usename = :role_name AND pid <> pg_backend_pid()"
            ),
            {"role_name": role},
        )
    engine.dispose()


def _validated_database_name(database_name: str) -> str:
    if not _ROLE_PATTERN.fullmatch(database_name):
        raise ValueError("Database name is not a safe unquoted PostgreSQL identifier.")
    return database_name
