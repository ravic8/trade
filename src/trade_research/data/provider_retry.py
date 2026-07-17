from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

from tenacity import RetryCallState, wait_random_exponential


@dataclass(frozen=True)
class ProviderFailureClassification:
    code: str
    retryable: bool
    affects_provider_health: bool
    status_code: int | None = None
    retry_after_seconds: float | None = None


class RetryableProviderFailure(RuntimeError):
    def __init__(
        self,
        message: str,
        classification: ProviderFailureClassification,
    ) -> None:
        super().__init__(message)
        self.classification = classification


class RetryAfterOrExponentialWait:
    """Full-jitter exponential wait that also honors a provider Retry-After."""

    def __init__(self, multiplier: float = 2.0, maximum: float = 15.0) -> None:
        self._fallback = wait_random_exponential(multiplier=multiplier, max=maximum)

    def __call__(self, retry_state: RetryCallState) -> float:
        fallback = float(self._fallback(retry_state))
        exception = retry_state.outcome.exception() if retry_state.outcome else None
        classification = getattr(exception, "classification", None)
        retry_after = getattr(classification, "retry_after_seconds", None)
        return max(fallback, float(retry_after or 0.0))


def classify_provider_failure(exc: BaseException) -> ProviderFailureClassification:
    status_code = _status_code(exc)
    message = str(exc).strip().lower()
    retry_after = _retry_after_seconds(exc)
    type_name = type(exc).__name__.lower()

    if status_code == 429 or any(
        marker in message
        for marker in ("too many requests", "rate limit", "ratelimit", "yf rate limit")
    ):
        return ProviderFailureClassification(
            code="rate_limited",
            retryable=True,
            affects_provider_health=True,
            status_code=429 if status_code is None else status_code,
            retry_after_seconds=retry_after,
        )
    if status_code in {500, 502, 503, 504}:
        return ProviderFailureClassification(
            code="provider_5xx",
            retryable=True,
            affects_provider_health=True,
            status_code=status_code,
            retry_after_seconds=retry_after,
        )
    if status_code == 404 or any(
        marker in message
        for marker in ("invalid ticker", "invalid symbol", "symbol may be delisted")
    ):
        return ProviderFailureClassification(
            code="invalid_symbol",
            retryable=False,
            affects_provider_health=False,
            status_code=status_code or 404,
        )
    if isinstance(exc, TimeoutError) or "timeout" in type_name or "timed out" in message:
        return ProviderFailureClassification(
            code="timeout",
            retryable=True,
            affects_provider_health=True,
            status_code=status_code,
        )
    if any(
        marker in type_name or marker in message
        for marker in (
            "connection",
            "network",
            "dns",
            "proxy",
            "curlerror",
            "connection reset",
        )
    ):
        return ProviderFailureClassification(
            code="network_error",
            retryable=True,
            affects_provider_health=True,
            status_code=status_code,
        )
    return ProviderFailureClassification(
        code="provider_error",
        retryable=True,
        affects_provider_health=True,
        status_code=status_code,
        retry_after_seconds=retry_after,
    )


def empty_response_classification() -> ProviderFailureClassification:
    return ProviderFailureClassification(
        code="empty_response",
        retryable=True,
        affects_provider_health=False,
    )


def _status_code(exc: BaseException) -> int | None:
    for candidate in (exc, getattr(exc, "response", None)):
        if candidate is None:
            continue
        value = getattr(candidate, "status_code", None)
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            continue
    match = re.search(r"(?:http(?: status)?\s*)?\b(404|429|500|502|503|504)\b", str(exc), re.I)
    return int(match.group(1)) if match else None


def _retry_after_seconds(exc: BaseException) -> float | None:
    response = getattr(exc, "response", None)
    headers: Any = getattr(response, "headers", None)
    if not headers:
        return None
    value = headers.get("Retry-After") or headers.get("retry-after")
    if value is None:
        return None
    try:
        return max(float(value), 0.0)
    except (TypeError, ValueError):
        pass
    try:
        retry_at = parsedate_to_datetime(str(value))
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        return max((retry_at - datetime.now(UTC)).total_seconds(), 0.0)
    except (TypeError, ValueError, OverflowError):
        return None
