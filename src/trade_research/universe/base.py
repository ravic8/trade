from abc import ABC, abstractmethod

from trade_research.schemas import Symbol


class UniverseProvider(ABC):
    exchange: str

    @abstractmethod
    def fetch(self) -> list[Symbol]:
        """Return the tradable symbol universe."""
