from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

DATA_CONTRACT_SCHEMA_VERSION: Literal["data_contract.v1"] = "data_contract.v1"

LogicalType = Literal["string", "integer", "number", "boolean", "date", "datetime", "json"]
ContractDomain = Literal[
    "calendar",
    "universe",
    "instrument",
    "market_data",
    "feature",
    "target",
    "dataset",
    "prediction",
    "backtest",
]
StorageKind = Literal[
    "postgresql",
    "local_parquet",
    "local_file",
    "object_storage",
    "dataset_bundle",
]
ContractLifecycle = Literal["current", "transitional", "proposed"]
FreshnessMode = Literal[
    "latest_completed_session",
    "wall_clock",
    "event_driven",
    "immutable",
    "run_scoped",
]


class ColumnContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9_]*$")
    logical_type: LogicalType
    nullable: bool
    description: str = Field(min_length=1)
    units: str | None = None
    allowed_values: tuple[str | int | float | bool, ...] = ()
    minimum: float | None = None
    maximum: float | None = None
    exclusive_minimum: bool = False
    exclusive_maximum: bool = False

    @model_validator(mode="after")
    def range_and_allowed_values_must_be_coherent(self) -> ColumnContract:
        if len(set(self.allowed_values)) != len(self.allowed_values):
            raise ValueError(f"Column {self.name!r} has duplicate allowed values.")
        if self.minimum is not None or self.maximum is not None:
            if self.logical_type not in {"integer", "number"}:
                raise ValueError(
                    f"Column {self.name!r} has numeric bounds but is {self.logical_type!r}."
                )
            if (
                self.minimum is not None
                and self.maximum is not None
                and self.minimum > self.maximum
            ):
                raise ValueError(f"Column {self.name!r} has minimum greater than maximum.")
        if self.exclusive_minimum and self.minimum is None:
            raise ValueError(
                f"Column {self.name!r} cannot have an exclusive minimum without a minimum."
            )
        if self.exclusive_maximum and self.maximum is None:
            raise ValueError(
                f"Column {self.name!r} cannot have an exclusive maximum without a maximum."
            )
        return self


class FreshnessContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: FreshnessMode
    basis_column: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_]*$")
    max_lag_sessions: int | None = Field(default=None, ge=0)
    max_age_minutes: int | None = Field(default=None, ge=1)
    grace_minutes: int = Field(default=0, ge=0)
    description: str = Field(min_length=1)

    @model_validator(mode="after")
    def mode_requires_matching_threshold(self) -> FreshnessContract:
        if self.mode == "latest_completed_session":
            if self.basis_column is None or self.max_lag_sessions is None:
                raise ValueError(
                    "latest_completed_session freshness requires basis_column and max_lag_sessions."
                )
        elif self.mode == "wall_clock":
            if self.basis_column is None or self.max_age_minutes is None:
                raise ValueError("wall_clock freshness requires basis_column and max_age_minutes.")
        elif self.max_lag_sessions is not None or self.max_age_minutes is not None:
            raise ValueError(f"Freshness mode {self.mode!r} cannot define lag or age thresholds.")
        return self


class CompatibilityPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    policy: Literal["semantic_versioning"] = "semantic_versioning"
    additive_nullable_columns: Literal["minor"] = "minor"
    additive_required_columns: Literal["major"] = "major"
    column_removal: Literal["major"] = "major"
    type_or_units_change: Literal["major"] = "major"
    primary_key_change: Literal["new_contract"] = "new_contract"
    consumer_migration_required: bool = True


class DataContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["data_contract.v1"] = DATA_CONTRACT_SCHEMA_VERSION
    contract_id: str = Field(
        min_length=1,
        pattern=r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+\.v[1-9][0-9]*$",
    )
    dataset_version: str = Field(min_length=1)
    domain: ContractDomain
    owner: str = Field(min_length=1)
    description: str = Field(min_length=1)
    lifecycle: ContractLifecycle
    authoritative_store: StorageKind
    storage_name: str = Field(min_length=1)
    replicas: tuple[str, ...] = ()
    primary_key: tuple[str, ...] = Field(min_length=1)
    columns: tuple[ColumnContract, ...] = Field(min_length=1)
    freshness: FreshnessContract
    invariants: tuple[str, ...] = Field(min_length=1)
    compatibility: CompatibilityPolicy = Field(default_factory=CompatibilityPolicy)
    limitations: tuple[str, ...] = ()

    @model_validator(mode="after")
    def schema_identity_must_be_coherent(self) -> DataContract:
        columns_by_name = {column.name: column for column in self.columns}
        if len(columns_by_name) != len(self.columns):
            raise ValueError(f"Contract {self.contract_id!r} contains duplicate columns.")
        if len(set(self.primary_key)) != len(self.primary_key):
            raise ValueError(f"Contract {self.contract_id!r} contains duplicate key columns.")
        missing_key_columns = sorted(set(self.primary_key) - set(columns_by_name))
        if missing_key_columns:
            raise ValueError(
                f"Contract {self.contract_id!r} primary key references missing columns: "
                f"{missing_key_columns}."
            )
        nullable_key_columns = [name for name in self.primary_key if columns_by_name[name].nullable]
        if nullable_key_columns:
            raise ValueError(
                f"Contract {self.contract_id!r} primary key columns cannot be nullable: "
                f"{nullable_key_columns}."
            )
        if (
            self.freshness.basis_column is not None
            and self.freshness.basis_column not in columns_by_name
        ):
            raise ValueError(
                f"Contract {self.contract_id!r} freshness basis column "
                f"{self.freshness.basis_column!r} is not registered."
            )
        if len(set(self.invariants)) != len(self.invariants):
            raise ValueError(f"Contract {self.contract_id!r} contains duplicate invariants.")
        return self
