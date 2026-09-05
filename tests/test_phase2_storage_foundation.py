from __future__ import annotations

import io
from pathlib import Path
from types import SimpleNamespace

import pytest

from trade_research.config import Settings
from trade_research.storage.clickhouse import (
    ClickHouseFeatureRepository,
    ResearchStorageReadOnlyError,
)
from trade_research.storage.clickhouse_migrations import (
    ClickHouseMigrationError,
    ClickHouseMigrator,
    discover_migrations,
)
from trade_research.storage.object_store import (
    ArtifactNamespace,
    ObjectArtifactStore,
    ObjectStoreIntegrityError,
    ObjectStoreReadOnlyError,
)


class FakeClickHouse:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.inserts: list[tuple[str, list[list[object]], list[str]]] = []
        self.applied: list[tuple[int, str, str]] = []

    def command(self, query: str, parameters=None) -> None:
        self.commands.append(query.strip())

    def query(self, query: str, parameters=None) -> SimpleNamespace:
        if "schema_migrations" in query:
            return SimpleNamespace(result_rows=list(self.applied), column_names=[])
        return SimpleNamespace(result_rows=[], column_names=[])

    def insert(self, table: str, data: list[list[object]], column_names: list[str]) -> None:
        self.inserts.append((table, data, column_names))
        if table.endswith("schema_migrations"):
            self.applied.extend(tuple(row) for row in data)


class MissingObjectError(RuntimeError):
    response = {
        "ResponseMetadata": {"HTTPStatusCode": 404},
        "Error": {"Code": "NoSuchKey"},
    }


class FakeObjectClient:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict] = {}
        self.put_calls = 0

    def head_object(self, *, Bucket: str, Key: str) -> dict:
        try:
            return self.objects[(Bucket, Key)]["head"]
        except KeyError as error:
            raise MissingObjectError from error

    def put_object(self, *, Bucket: str, Key: str, Body, **kwargs) -> dict:
        self.put_calls += 1
        content = Body.read()
        self.objects[(Bucket, Key)] = {
            "content": content,
            "head": {
                "ContentLength": len(content),
                "ContentType": kwargs["ContentType"],
                "Metadata": kwargs["Metadata"],
                "VersionId": "version-1",
            },
        }
        return {"VersionId": "version-1"}

    def get_object(self, *, Bucket: str, Key: str, **kwargs) -> dict:
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)]["content"])}


def _buckets() -> dict[ArtifactNamespace, str]:
    return {namespace: f"trade-{namespace.value}" for namespace in ArtifactNamespace}


def test_clickhouse_migrations_are_complete_and_idempotent() -> None:
    directory = Path(__file__).parents[1] / "clickhouse" / "migrations"
    migrations = discover_migrations(directory, database="research")
    assert [migration.version for migration in migrations] == [1]
    required_tables = {
        "ohlcv_daily",
        "feature_observations_daily",
        "target_observations_daily",
        "factor_statistics",
        "feature_distributions",
        "predictions_daily",
        "backtest_returns_daily",
        "backtest_positions_daily",
        "experiment_metrics",
        "data_quality_results",
    }
    assert required_tables <= {
        statement.split(".", 1)[1].split()[0]
        for statement in migrations[0].sql.split("CREATE TABLE IF NOT EXISTS ")[1:]
    }

    client = FakeClickHouse()
    migrator = ClickHouseMigrator(client, database="research")
    assert migrator.apply(migrations) == [1]
    command_count = len(client.commands)
    assert migrator.apply(migrations) == []
    assert len(client.commands) == command_count + 2  # idempotent bootstrap only


def test_clickhouse_migration_checksum_drift_is_rejected() -> None:
    directory = Path(__file__).parents[1] / "clickhouse" / "migrations"
    migrations = discover_migrations(directory, database="research")
    client = FakeClickHouse()
    client.applied = [(1, "research_foundation", "0" * 64)]
    with pytest.raises(ClickHouseMigrationError, match="checksum/name changed"):
        ClickHouseMigrator(client, database="research").apply(migrations)


def test_clickhouse_repository_denies_writes_by_default() -> None:
    client = FakeClickHouse()
    repository = ClickHouseFeatureRepository(client)
    with pytest.raises(ResearchStorageReadOnlyError):
        repository.insert_observations([{"feature_key": "momentum"}])
    assert client.inserts == []


def test_object_store_is_immutable_digest_verified_and_retry_safe() -> None:
    client = FakeObjectClient()
    store = ObjectArtifactStore(client, buckets=_buckets(), write_enabled=True)
    artifact = store.put_bytes(
        ArtifactNamespace.DATASETS,
        "daily/nse.parquet",
        b"parquet-content",
        media_type="application/vnd.apache.parquet",
    )
    replay = store.put_bytes(
        ArtifactNamespace.DATASETS,
        "daily/nse.parquet",
        b"parquet-content",
        media_type="application/vnd.apache.parquet",
    )
    assert replay == artifact
    assert client.put_calls == 1
    assert store.get_bytes(artifact) == b"parquet-content"

    with pytest.raises(ObjectStoreIntegrityError, match="different content"):
        store.put_bytes(
            ArtifactNamespace.DATASETS,
            "daily/nse.parquet",
            b"different",
        )


def test_object_store_denies_writes_and_unsafe_keys_by_default() -> None:
    readonly = ObjectArtifactStore(FakeObjectClient(), buckets=_buckets())
    with pytest.raises(ObjectStoreReadOnlyError):
        readonly.put_bytes(ArtifactNamespace.RAW, "safe.json", b"{}")

    writable = ObjectArtifactStore(FakeObjectClient(), buckets=_buckets(), write_enabled=True)
    with pytest.raises(ValueError, match="unsafe object key"):
        writable.put_bytes(ArtifactNamespace.RAW, "../secrets", b"no")


def test_phase2_settings_fail_closed() -> None:
    with pytest.raises(ValueError, match="CLICKHOUSE_PASSWORD"):
        Settings(clickhouse_enabled=True)
    with pytest.raises(ValueError, match="writes require"):
        Settings(clickhouse_write_enabled=True)
    with pytest.raises(ValueError, match="OBJECT_STORE_ACCESS_KEY_ID"):
        Settings(object_store_enabled=True)
    with pytest.raises(ValueError, match="writes require"):
        Settings(object_store_write_enabled=True)


def test_clickhouse_role_limits_use_validated_numeric_literals() -> None:
    script = (
        Path(__file__).parents[1] / "clickhouse" / "bootstrap-access.sh"
    ).read_text(encoding="utf-8")

    assert "invalid numeric ClickHouse role limit" in script
    assert "{max_seconds:UInt64}" not in script
    assert "{max_memory:UInt64}" not in script
    assert "{readonly_value:UInt8}" not in script
    assert "max_execution_time = $max_seconds" in script
