from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

_MIGRATION_NAME = re.compile(r"^(?P<version>[0-9]{4})_(?P<name>[a-z0-9_]+)\.sql$")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ClickHouseMigrationError(RuntimeError):
    pass


class MigrationClient(Protocol):
    def command(self, query: str, parameters: dict[str, Any] | None = None) -> Any: ...

    def query(self, query: str, parameters: dict[str, Any] | None = None) -> Any: ...

    def insert(
        self,
        table: str,
        data: list[list[Any]],
        column_names: list[str],
    ) -> Any: ...


@dataclass(frozen=True)
class ClickHouseMigration:
    version: int
    name: str
    path: Path
    sha256: str
    sql: str


def discover_migrations(directory: Path, *, database: str) -> list[ClickHouseMigration]:
    if not _IDENTIFIER.fullmatch(database):
        raise ValueError(f"invalid ClickHouse database identifier: {database!r}")
    migrations: list[ClickHouseMigration] = []
    for path in sorted(directory.glob("*.sql")):
        match = _MIGRATION_NAME.fullmatch(path.name)
        if match is None:
            raise ClickHouseMigrationError(f"invalid migration filename: {path.name}")
        source = path.read_text(encoding="utf-8")
        rendered = source.replace("{{database}}", database)
        migrations.append(
            ClickHouseMigration(
                version=int(match.group("version")),
                name=match.group("name"),
                path=path,
                sha256=hashlib.sha256(source.encode("utf-8")).hexdigest(),
                sql=rendered,
            )
        )
    versions = [migration.version for migration in migrations]
    if len(versions) != len(set(versions)):
        raise ClickHouseMigrationError("duplicate ClickHouse migration version")
    return migrations


def split_statements(sql: str) -> list[str]:
    """Split repository-owned DDL where semicolons occur only between statements."""

    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    previous = ""
    for character in sql:
        if character == "'" and previous != "\\":
            in_single_quote = not in_single_quote
        if character == ";" and not in_single_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(character)
        previous = character
    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)
    if in_single_quote:
        raise ClickHouseMigrationError("unterminated single-quoted string in migration")
    return statements


class ClickHouseMigrator:
    def __init__(self, client: MigrationClient, *, database: str) -> None:
        if not _IDENTIFIER.fullmatch(database):
            raise ValueError(f"invalid ClickHouse database identifier: {database!r}")
        self._client = client
        self._database = database

    def initialize(self) -> None:
        self._client.command(f"CREATE DATABASE IF NOT EXISTS {self._database}")
        self._client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {self._database}.schema_migrations
            (
                version UInt32,
                name String,
                sha256 FixedString(64),
                applied_at DateTime64(6, 'UTC') DEFAULT now64(6)
            )
            ENGINE = ReplacingMergeTree(applied_at)
            ORDER BY version
            """
        )

    def apply(self, migrations: list[ClickHouseMigration]) -> list[int]:
        self.initialize()
        result = self._client.query(
            f"""
            SELECT version, argMax(name, applied_at), argMax(sha256, applied_at)
            FROM {self._database}.schema_migrations
            GROUP BY version
            """
        )
        applied = {int(row[0]): (str(row[1]), str(row[2])) for row in result.result_rows}
        completed: list[int] = []
        for migration in migrations:
            recorded = applied.get(migration.version)
            if recorded is not None:
                if recorded != (migration.name, migration.sha256):
                    raise ClickHouseMigrationError(
                        f"migration {migration.version:04d} checksum/name changed"
                    )
                continue
            for statement in split_statements(migration.sql):
                self._client.command(statement)
            self._client.insert(
                f"{self._database}.schema_migrations",
                [[migration.version, migration.name, migration.sha256]],
                column_names=["version", "name", "sha256"],
            )
            completed.append(migration.version)
        return completed
