from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from threading import Lock
from uuid import uuid4

from trade_research.config import Settings


@dataclass(frozen=True)
class RateLimitWindow:
    name: str
    limit: int
    window_seconds: int


@dataclass(frozen=True)
class RateLimitDecision:
    backend: str
    wait_seconds: float
    rate_limited: bool


class ProviderRateLimiter:
    def acquire(
        self,
        provider: str,
        endpoint_group: str,
        weight: int = 1,
    ) -> RateLimitDecision:
        raise NotImplementedError

    def update_rate_per_minute(self, provider: str, limit: int) -> None:
        """Update the process-local enforced rate used by future acquisitions."""


class NoopProviderRateLimiter(ProviderRateLimiter):
    def acquire(
        self,
        provider: str,
        endpoint_group: str,
        weight: int = 1,
    ) -> RateLimitDecision:
        _validate_weight(weight)
        return RateLimitDecision(backend="none", wait_seconds=0.0, rate_limited=False)

    def update_rate_per_minute(self, provider: str, limit: int) -> None:
        _validate_limit(limit)


class InMemoryProviderRateLimiter(ProviderRateLimiter):
    def __init__(self, limits: dict[tuple[str, str], tuple[RateLimitWindow, ...]]) -> None:
        self._limits = limits
        self._lock = Lock()
        self._hits: dict[str, deque[float]] = {}

    def acquire(
        self,
        provider: str,
        endpoint_group: str,
        weight: int = 1,
    ) -> RateLimitDecision:
        _validate_weight(weight)
        windows = _windows_for(self._limits, provider, endpoint_group)
        if not windows:
            return RateLimitDecision(backend="memory", wait_seconds=0.0, rate_limited=False)
        _validate_weight_fits_windows(weight, windows)

        total_wait = 0.0
        rate_limited = False
        while True:
            wait_seconds = self._reserve_or_wait(
                provider,
                endpoint_group,
                windows,
                weight,
            )
            if wait_seconds <= 0:
                return RateLimitDecision(
                    backend="memory",
                    wait_seconds=total_wait,
                    rate_limited=rate_limited,
                )
            rate_limited = True
            total_wait += wait_seconds
            time.sleep(wait_seconds)

    def _reserve_or_wait(
        self,
        provider: str,
        endpoint_group: str,
        windows: tuple[RateLimitWindow, ...],
        weight: int,
    ) -> float:
        now = time.monotonic()
        with self._lock:
            wait_seconds = 0.0
            for window in windows:
                key = _limit_key(provider, endpoint_group, window.name)
                hits = self._hits.setdefault(key, deque())
                cutoff = now - window.window_seconds
                while hits and hits[0] <= cutoff:
                    hits.popleft()
                required_expirations = len(hits) + weight - window.limit
                if required_expirations > 0:
                    limiting_hit = hits[required_expirations - 1]
                    wait_seconds = max(
                        wait_seconds,
                        limiting_hit + window.window_seconds - now,
                    )
            if wait_seconds > 0:
                return max(wait_seconds, 0.001)
            for window in windows:
                hits = self._hits[_limit_key(provider, endpoint_group, window.name)]
                hits.extend([now] * weight)
            return 0.0

    def update_rate_per_minute(self, provider: str, limit: int) -> None:
        _validate_limit(limit)
        with self._lock:
            self._limits = _updated_minute_limits(self._limits, provider, limit)


class RedisProviderRateLimiter(ProviderRateLimiter):
    _SCRIPT = """
local now_ms = tonumber(ARGV[1])
local member = ARGV[2]
local weight = tonumber(ARGV[3])
local wait_ms = 0
for i = 1, #KEYS do
  local limit = tonumber(ARGV[3 + ((i - 1) * 2) + 1])
  local window_ms = tonumber(ARGV[3 + ((i - 1) * 2) + 2])
  redis.call('ZREMRANGEBYSCORE', KEYS[i], 0, now_ms - window_ms)
  local count = redis.call('ZCARD', KEYS[i])
  local required_expirations = count + weight - limit
  if required_expirations > 0 then
    local limiting = redis.call(
      'ZRANGE', KEYS[i], required_expirations - 1, required_expirations - 1, 'WITHSCORES'
    )
    if limiting[2] then
      local candidate = tonumber(limiting[2]) + window_ms - now_ms
      if candidate > wait_ms then
        wait_ms = candidate
      end
    end
  end
end
if wait_ms > 0 then
  return wait_ms
end
for i = 1, #KEYS do
  local window_ms = tonumber(ARGV[3 + ((i - 1) * 2) + 2])
  for token = 1, weight do
    redis.call('ZADD', KEYS[i], now_ms, member .. ':' .. i .. ':' .. token)
  end
  redis.call('PEXPIRE', KEYS[i], window_ms + 60000)
end
return 0
"""

    def __init__(
        self,
        redis_url: str,
        limits: dict[tuple[str, str], tuple[RateLimitWindow, ...]],
    ) -> None:
        try:
            import redis
        except ImportError as exc:  # pragma: no cover - depends on optional runtime package
            raise RuntimeError("Install redis to use Redis provider rate limiting.") from exc

        self._client = redis.Redis.from_url(redis_url, decode_responses=True)
        self._client.ping()
        self._limits = limits
        self._script = self._client.register_script(self._SCRIPT)

    def acquire(
        self,
        provider: str,
        endpoint_group: str,
        weight: int = 1,
    ) -> RateLimitDecision:
        _validate_weight(weight)
        windows = _windows_for(self._limits, provider, endpoint_group)
        if not windows:
            return RateLimitDecision(backend="redis", wait_seconds=0.0, rate_limited=False)
        _validate_weight_fits_windows(weight, windows)

        total_wait = 0.0
        rate_limited = False
        keys = [_limit_key(provider, endpoint_group, window.name) for window in windows]
        while True:
            now_ms = int(time.time() * 1000)
            args: list[str | int] = [now_ms, str(uuid4()), weight]
            for window in windows:
                args.extend([window.limit, window.window_seconds * 1000])
            wait_ms = int(self._script(keys=keys, args=args))
            if wait_ms <= 0:
                return RateLimitDecision(
                    backend="redis",
                    wait_seconds=total_wait,
                    rate_limited=rate_limited,
                )
            rate_limited = True
            wait_seconds = max(wait_ms / 1000, 0.001)
            total_wait += wait_seconds
            time.sleep(wait_seconds)

    def update_rate_per_minute(self, provider: str, limit: int) -> None:
        _validate_limit(limit)
        self._limits = _updated_minute_limits(self._limits, provider, limit)


def build_provider_rate_limiter(settings: Settings) -> ProviderRateLimiter:
    limits = provider_rate_limit_windows(settings)
    backend = settings.provider_rate_limit_backend.strip().lower()
    require_redis = settings.provider_rate_limit_require_redis or backend == "redis"
    if backend == "none":
        return NoopProviderRateLimiter()
    if backend in {"auto", "redis"}:
        try:
            return RedisProviderRateLimiter(settings.redis_url, limits)
        except Exception:
            if require_redis:
                raise
    return InMemoryProviderRateLimiter(limits)


def provider_rate_limit_windows(
    settings: Settings,
) -> dict[tuple[str, str], tuple[RateLimitWindow, ...]]:
    return {
        ("upstox", "historical"): (
            RateLimitWindow("1s", settings.upstox_rate_per_second, 1),
            RateLimitWindow("1m", settings.upstox_rate_per_minute, 60),
            RateLimitWindow("30m", settings.upstox_rate_per_30_minutes, 30 * 60),
        ),
        ("yfinance", "download"): (
            RateLimitWindow("1m", settings.yfinance_initial_rpm, 60),
        ),
        ("yfinance", "intraday_download"): (
            RateLimitWindow("1m", settings.yfinance_initial_rpm, 60),
        ),
        ("dukascopy", "historical"): (
            RateLimitWindow("1m", settings.dukascopy_rate_per_minute, 60),
        ),
    }


def _windows_for(
    limits: dict[tuple[str, str], tuple[RateLimitWindow, ...]],
    provider: str,
    endpoint_group: str,
) -> tuple[RateLimitWindow, ...]:
    return limits.get((provider.strip().lower(), endpoint_group.strip().lower()), ())


def _limit_key(provider: str, endpoint_group: str, window_name: str) -> str:
    normalized_provider = provider.strip().lower()
    if normalized_provider == "yfinance":
        return "provider-rate-limit:yfinance:all"
    return (
        f"provider-rate-limit:{normalized_provider}:"
        f"{endpoint_group.strip().lower()}:{window_name}"
    )


def _updated_minute_limits(
    limits: dict[tuple[str, str], tuple[RateLimitWindow, ...]],
    provider: str,
    limit: int,
) -> dict[tuple[str, str], tuple[RateLimitWindow, ...]]:
    normalized_provider = provider.strip().lower()
    updated = dict(limits)
    for key, windows in limits.items():
        if key[0] != normalized_provider:
            continue
        updated[key] = tuple(
            RateLimitWindow(window.name, limit, window.window_seconds)
            if window.name == "1m"
            else window
            for window in windows
        )
    return updated


def _validate_weight(weight: int) -> None:
    if isinstance(weight, bool) or not isinstance(weight, int) or weight < 1:
        raise ValueError("Rate-limit weight must be a positive integer.")


def _validate_limit(limit: int) -> None:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("Rate limit must be a positive integer.")


def _validate_weight_fits_windows(
    weight: int,
    windows: tuple[RateLimitWindow, ...],
) -> None:
    for window in windows:
        if weight > window.limit:
            raise ValueError(
                f"Rate-limit weight {weight} exceeds {window.name} limit {window.limit}."
            )
