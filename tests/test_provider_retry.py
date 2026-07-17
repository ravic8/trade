from __future__ import annotations

from trade_research.data.provider_retry import classify_provider_failure


class _Response:
    def __init__(self, status_code: int, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.headers = headers or {}


class _HttpError(RuntimeError):
    def __init__(self, status_code: int, headers: dict[str, str] | None = None) -> None:
        super().__init__(f"HTTP {status_code}")
        self.response = _Response(status_code, headers)


def test_rate_limit_classification_honors_retry_after() -> None:
    failure = classify_provider_failure(_HttpError(429, {"Retry-After": "17"}))

    assert failure.code == "rate_limited"
    assert failure.retryable is True
    assert failure.affects_provider_health is True
    assert failure.status_code == 429
    assert failure.retry_after_seconds == 17


def test_provider_5xx_and_timeout_are_retryable_health_failures() -> None:
    server_failure = classify_provider_failure(_HttpError(503))
    timeout_failure = classify_provider_failure(TimeoutError("request timed out"))

    assert server_failure.code == "provider_5xx"
    assert server_failure.retryable is True
    assert timeout_failure.code == "timeout"
    assert timeout_failure.affects_provider_health is True


def test_invalid_symbol_is_terminal_without_reducing_global_rate() -> None:
    failure = classify_provider_failure(RuntimeError("symbol may be delisted"))

    assert failure.code == "invalid_symbol"
    assert failure.retryable is False
    assert failure.affects_provider_health is False

