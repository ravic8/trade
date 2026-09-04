import pytest
from pydantic import ValidationError

from trade_research.contracts import (
    DATA_CONTRACT_REGISTRY,
    ML_INPUTS_CONTRACT_ID,
    ColumnContract,
    DataContract,
    DataContractRegistry,
    FreshnessContract,
    get_data_contract,
)
from trade_research.features import FEATURE_VERSION_V1_0
from trade_research.modeling.ml_dataset_v1 import ML_DATASET_VERSION_V1
from trade_research.storage.timescale import metadata
from trade_research.targets import (
    DAILY_FORWARD_TARGET_VERSION_V1_0,
    DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
)

EXPECTED_CONTRACT_IDS = {
    "calendar.exchange_sessions.v1",
    "universe.snapshots.v1",
    "universe.snapshot_members.v1",
    "instrument.canonical_identity.v1",
    "market_data.ohlcv_daily.v1",
    "target.opportunity_daily.v1",
    "feature.daily_technical.v1",
    "target.daily_forward_returns.v1",
    ML_INPUTS_CONTRACT_ID,
    "dataset.ml_daily.v1",
    "prediction.daily_rankings.v1",
    "backtest.daily_returns.v1",
}


def _minimal_contract(**overrides: object) -> DataContract:
    values: dict[str, object] = {
        "contract_id": "dataset.example.v1",
        "dataset_version": "example_v1",
        "domain": "dataset",
        "owner": "test-owner",
        "description": "Test data contract.",
        "lifecycle": "current",
        "authoritative_store": "local_parquet",
        "storage_name": "example.parquet",
        "primary_key": ("id",),
        "columns": (
            ColumnContract(
                name="id",
                logical_type="string",
                nullable=False,
                description="Stable identifier.",
            ),
        ),
        "freshness": FreshnessContract(
            mode="run_scoped",
            description="Immutable test run.",
        ),
        "invariants": ("id is unique",),
    }
    values.update(overrides)
    return DataContract.model_validate(values)


def test_default_registry_covers_phase1_required_domains() -> None:
    assert set(DATA_CONTRACT_REGISTRY.contract_ids) == EXPECTED_CONTRACT_IDS
    assert {contract.domain for contract in DATA_CONTRACT_REGISTRY.contracts} >= {
        "calendar",
        "universe",
        "instrument",
        "market_data",
        "feature",
        "target",
        "dataset",
        "prediction",
        "backtest",
    }


def test_postgresql_contract_keys_and_columns_match_physical_tables() -> None:
    for contract in DATA_CONTRACT_REGISTRY.contracts:
        if contract.authoritative_store != "postgresql":
            continue
        table = metadata.tables[contract.storage_name]
        physical_key = tuple(column.name for column in table.primary_key.columns)
        physical_columns = set(table.columns.keys())

        assert contract.primary_key == physical_key
        assert {column.name for column in contract.columns} <= physical_columns


def test_catalog_versions_follow_frozen_implementation_versions() -> None:
    assert get_data_contract("feature.daily_technical.v1").dataset_version == (FEATURE_VERSION_V1_0)
    assert get_data_contract("target.daily_forward_returns.v1").dataset_version == (
        DAILY_FORWARD_TARGET_VERSION_V1_0
    )
    assert get_data_contract("target.opportunity_daily.v1").dataset_version == (
        DAILY_OPPORTUNITY_TARGET_VERSION_V1_0
    )
    assert get_data_contract("dataset.ml_daily.v1").dataset_version == (ML_DATASET_VERSION_V1)


def test_contracts_are_strict_json_serializable_and_keys_are_non_nullable() -> None:
    for contract in DATA_CONTRACT_REGISTRY.contracts:
        payload = contract.model_dump(mode="json")
        columns = {column.name: column for column in contract.columns}

        assert payload["schema_version"] == "data_contract.v1"
        assert all(not columns[name].nullable for name in contract.primary_key)


def test_contract_rejects_nullable_or_missing_primary_key_columns() -> None:
    nullable_id = ColumnContract(
        name="id",
        logical_type="string",
        nullable=True,
        description="Nullable identifier.",
    )
    with pytest.raises(ValidationError, match="cannot be nullable"):
        _minimal_contract(columns=(nullable_id,))

    with pytest.raises(ValidationError, match="references missing columns"):
        _minimal_contract(primary_key=("missing_id",))


def test_contract_rejects_duplicate_columns_and_unknown_freshness_basis() -> None:
    identifier = ColumnContract(
        name="id",
        logical_type="string",
        nullable=False,
        description="Identifier.",
    )
    with pytest.raises(ValidationError, match="duplicate columns"):
        _minimal_contract(columns=(identifier, identifier))

    with pytest.raises(ValidationError, match="is not registered"):
        _minimal_contract(
            freshness=FreshnessContract(
                mode="wall_clock",
                basis_column="generated_at",
                max_age_minutes=60,
                description="Fresh for one hour.",
            )
        )


def test_column_contract_rejects_non_numeric_bounds() -> None:
    with pytest.raises(ValidationError, match="numeric bounds"):
        ColumnContract(
            name="status",
            logical_type="string",
            nullable=False,
            description="Status value.",
            minimum=0,
        )


def test_registry_is_unique_nonempty_and_fails_closed_for_unknown_ids() -> None:
    contract = _minimal_contract()
    with pytest.raises(ValueError, match="cannot be empty"):
        DataContractRegistry(())
    with pytest.raises(ValueError, match="Duplicate data contract"):
        DataContractRegistry((contract, contract))
    with pytest.raises(KeyError, match="Unknown data contract"):
        DATA_CONTRACT_REGISTRY.get("dataset.missing.v1")
