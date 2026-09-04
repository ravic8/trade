from __future__ import annotations

from collections.abc import Iterable
from types import MappingProxyType

from trade_research.contracts.models import ContractDomain, DataContract


class DataContractRegistry:
    """Immutable lookup registry for versioned data contracts."""

    def __init__(self, contracts: Iterable[DataContract]) -> None:
        by_id: dict[str, DataContract] = {}
        for contract in contracts:
            if contract.contract_id in by_id:
                raise ValueError(f"Duplicate data contract: {contract.contract_id}")
            by_id[contract.contract_id] = contract
        if not by_id:
            raise ValueError("Data-contract registry cannot be empty.")
        self._by_id = MappingProxyType(by_id)

    @property
    def contracts(self) -> tuple[DataContract, ...]:
        return tuple(self._by_id[key] for key in sorted(self._by_id))

    @property
    def contract_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._by_id))

    def get(self, contract_id: str) -> DataContract:
        try:
            return self._by_id[contract_id]
        except KeyError as exc:
            raise KeyError(f"Unknown data contract: {contract_id}") from exc

    def list(self, *, domain: ContractDomain | None = None) -> tuple[DataContract, ...]:
        contracts = self.contracts
        if domain is None:
            return contracts
        return tuple(contract for contract in contracts if contract.domain == domain)

    def contains(self, contract_id: str) -> bool:
        return contract_id in self._by_id
