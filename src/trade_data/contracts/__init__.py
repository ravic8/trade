"""Daily data contracts and fail-closed publication evaluation."""


from trade_data.contracts.daily_catalog import (
    DAILY_DATA_CONTRACT_REGISTRY,
    DAILY_DATA_CONTRACTS,
)
from trade_data.contracts.evaluator import (
    ContractEvaluationContext,
    evaluate_frame_contract,
)
from trade_data.contracts.models import (
    DATA_CONTRACT_SCHEMA_VERSION,
    ColumnContract,
    CompatibilityPolicy,
    ContractDomain,
    ContractLifecycle,
    DataContract,
    FreshnessContract,
    FreshnessMode,
    LogicalType,
    StorageKind,
)
from trade_data.contracts.registry import DataContractRegistry

__all__ = [
    "DAILY_DATA_CONTRACT_REGISTRY",
    "DAILY_DATA_CONTRACTS",
    "DATA_CONTRACT_REGISTRY",
    "DATA_CONTRACT_SCHEMA_VERSION",
    "DATA_CONTRACTS",
    "ColumnContract",
    "CompatibilityPolicy",
    "ContractEvaluationContext",
    "ContractDomain",
    "ContractLifecycle",
    "DataContract",
    "DataContractRegistry",
    "FreshnessContract",
    "FreshnessMode",
    "LogicalType",
    "StorageKind",
    "get_data_contract",
    "evaluate_frame_contract",
]


DATA_CONTRACTS = DAILY_DATA_CONTRACTS
DATA_CONTRACT_REGISTRY = DAILY_DATA_CONTRACT_REGISTRY


def get_data_contract(contract_id: str) -> DataContract:
    return DATA_CONTRACT_REGISTRY.get(contract_id)
