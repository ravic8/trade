from __future__ import annotations

from dataclasses import dataclass

from trade_research.schemas import ChatQueryRequest


_UNSAFE_PATTERNS = (
    "drop table",
    "delete from",
    "truncate table",
    "alter table",
    "raw sql",
    "ignore previous instructions",
    "system prompt",
    "execute trade",
    "place order",
    "buy now",
    "sell now",
)


@dataclass(slots=True)
class PolicyDecision:
    allowed: bool
    reason: str | None = None


class ChatPolicy:
    def evaluate(self, request: ChatQueryRequest) -> PolicyDecision:
        text = request.message.lower()
        for pattern in _UNSAFE_PATTERNS:
            if pattern in text:
                return PolicyDecision(
                    allowed=False,
                    reason=(
                        "Request is outside chatbot policy boundaries. "
                        "I can help with read-only analysis and cited market/research answers."
                    ),
                )
        return PolicyDecision(allowed=True)
