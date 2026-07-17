from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from threading import Lock
from time import monotonic, sleep
from typing import Any, Protocol

from trade_research.config import Settings
from trade_research.data.provider_retry import ProviderFailureClassification
from trade_research.data.rate_limits import ProviderRateLimiter

logger = logging.getLogger(__name__)


class AdaptiveRateStore(Protocol):
    def adaptive_rate_state(self, provider: str) -> dict[str, Any] | None:
        ...

    def upsert_adaptive_rate_state(self, state: dict[str, Any]) -> int:
        ...


@dataclass(frozen=True)
class AdaptiveRateSnapshot:
    provider: str
    mode: str
    enforced_rpm: int
    recommended_rpm: int
    last_safe_rpm: int | None
    minimum_rpm: int
    maximum_rpm: int
    enforced_concurrency: int
    recommended_concurrency: int
    consecutive_healthy_windows: int
    circuit_state: str
    cooldown_until: datetime | None
    last_429_at: datetime | None
    recent_error_rate: float
    latency_baseline_ms: float | None


class YahooAdaptiveRateGovernor:
    """Observe or enforce conservative Yahoo rate recommendations."""

    provider = "yfinance"

    def __init__(
        self,
        settings: Settings,
        limiter: ProviderRateLimiter,
        store: AdaptiveRateStore | None = None,
        *,
        monotonic_clock: Callable[[], float] = monotonic,
        utc_clock: Callable[[], datetime] | None = None,
        sleep_fn: Callable[[float], None] = sleep,
    ) -> None:
        self._settings = settings
        self._limiter = limiter
        self._store = store
        self._monotonic_clock = monotonic_clock
        self._utc_clock = utc_clock or (lambda: datetime.now(UTC))
        self._sleep = sleep_fn
        self._lock = Lock()
        self._window_started_at = self._monotonic_clock()
        self._window_requests = 0
        self._window_health_errors = 0
        self._window_5xx = 0
        self._window_latency_ms: list[float] = []

        persisted = self._load_state()
        last_safe = _nullable_int(persisted.get("last_safe_rpm")) if persisted else None
        startup_rpm = (
            _clamp(
                int(last_safe * 0.8),
                settings.yfinance_minimum_rpm,
                settings.yfinance_maximum_rpm,
            )
            if last_safe
            else settings.yfinance_initial_rpm
        )
        persisted_concurrency = (
            _nullable_int(persisted.get("current_concurrency")) if persisted else None
        )
        startup_concurrency = _clamp(
            persisted_concurrency or settings.yfinance_initial_concurrency,
            1,
            settings.yfinance_maximum_concurrency,
        )
        self._recommended_rpm = startup_rpm
        self._recommended_concurrency = startup_concurrency
        self._last_safe_rpm = last_safe
        self._healthy_windows = (
            int(persisted.get("consecutive_healthy_windows") or 0) if persisted else 0
        )
        self._circuit_state = (
            str(persisted.get("circuit_state") or "closed") if persisted else "closed"
        )
        self._cooldown_until = (
            _nullable_datetime(persisted.get("cooldown_until")) if persisted else None
        )
        self._last_429_at = _nullable_datetime(persisted.get("last_429_at")) if persisted else None
        self._recent_error_rate = (
            float(persisted.get("recent_error_rate") or 0.0) if persisted else 0.0
        )
        self._latency_baseline_ms = (
            float(persisted["latency_baseline_ms"])
            if persisted and persisted.get("latency_baseline_ms") is not None
            else None
        )

        if settings.yfinance_adaptive_rate_mode == "adaptive":
            self._enforced_rpm = startup_rpm
            self._enforced_concurrency = startup_concurrency
            self._limiter.update_rate_per_minute(self.provider, startup_rpm)
        else:
            self._enforced_rpm = settings.yfinance_initial_rpm
            self._enforced_concurrency = settings.yfinance_initial_concurrency
        self._persist()

    @property
    def concurrency(self) -> int:
        return self._enforced_concurrency

    def wait_for_availability(self) -> float:
        cooldown_expired = False
        with self._lock:
            now = self._utc_clock()
            wait_seconds = (
                max((self._cooldown_until - now).total_seconds(), 0.0)
                if self._cooldown_until is not None
                else 0.0
            )
            if self._cooldown_until is not None and wait_seconds == 0:
                self._cooldown_until = None
                self._circuit_state = "closed"
                cooldown_expired = True
        if cooldown_expired:
            self._persist()
        if wait_seconds > 0:
            self._sleep(wait_seconds)
        return wait_seconds

    def report(
        self,
        classification: ProviderFailureClassification | None,
        duration_ms: float,
    ) -> None:
        with self._lock:
            now = self._utc_clock()
            self._window_requests += 1
            self._window_latency_ms.append(max(float(duration_ms), 0.0))
            if classification and classification.affects_provider_health:
                self._window_health_errors += 1
            if classification and classification.code == "provider_5xx":
                self._window_5xx += 1
            if classification and classification.code == "rate_limited":
                self._apply_rate_limit(classification, now)
            if (
                self._monotonic_clock() - self._window_started_at
                >= self._settings.yfinance_adaptive_evaluation_window_seconds
            ):
                self._evaluate_window(now)

    def snapshot(self) -> AdaptiveRateSnapshot:
        with self._lock:
            return self._snapshot()

    def _apply_rate_limit(
        self,
        classification: ProviderFailureClassification,
        now: datetime,
    ) -> None:
        self._recommended_rpm = _clamp(
            int(self._recommended_rpm * 0.25),
            self._settings.yfinance_minimum_rpm,
            self._settings.yfinance_maximum_rpm,
        )
        self._recommended_concurrency = max(self._recommended_concurrency - 1, 1)
        self._healthy_windows = 0
        self._circuit_state = "open"
        cooldown_seconds = max(
            float(classification.retry_after_seconds or 0.0),
            float(self._settings.yfinance_adaptive_cooldown_seconds),
        )
        self._cooldown_until = now + timedelta(seconds=cooldown_seconds)
        self._last_429_at = now
        self._apply_if_adaptive()
        self._persist()

    def _evaluate_window(self, now: datetime) -> None:
        requests = max(self._window_requests, 1)
        error_rate = self._window_health_errors / requests
        average_latency = (
            sum(self._window_latency_ms) / len(self._window_latency_ms)
            if self._window_latency_ms
            else 0.0
        )
        self._recent_error_rate = error_rate
        if average_latency > 0:
            if self._latency_baseline_ms is None:
                self._latency_baseline_ms = average_latency
            else:
                self._latency_baseline_ms = (
                    (self._latency_baseline_ms * 0.8) + (average_latency * 0.2)
                )

        if self._window_5xx:
            self._recommended_rpm = _clamp(
                int(self._recommended_rpm * 0.75),
                self._settings.yfinance_minimum_rpm,
                self._settings.yfinance_maximum_rpm,
            )
            self._recommended_concurrency = max(self._recommended_concurrency - 1, 1)
            self._healthy_windows = 0
        elif error_rate >= self._settings.yfinance_adaptive_error_threshold:
            self._recommended_rpm = _clamp(
                int(self._recommended_rpm * 0.70),
                self._settings.yfinance_minimum_rpm,
                self._settings.yfinance_maximum_rpm,
            )
            self._recommended_concurrency = max(self._recommended_concurrency - 1, 1)
            self._healthy_windows = 0
        elif self._window_health_errors == 0:
            self._healthy_windows += 1
            self._last_safe_rpm = self._recommended_rpm
            if (
                self._healthy_windows
                >= self._settings.yfinance_adaptive_healthy_windows_before_increase
            ):
                self._recommended_rpm = _clamp(
                    self._recommended_rpm + self._settings.yfinance_adaptive_increase_rpm,
                    self._settings.yfinance_minimum_rpm,
                    self._settings.yfinance_maximum_rpm,
                )
                self._recommended_concurrency = min(
                    self._recommended_concurrency + 1,
                    self._settings.yfinance_maximum_concurrency,
                )
                self._healthy_windows = 0

        if self._cooldown_until and now >= self._cooldown_until:
            self._circuit_state = "closed"
            self._cooldown_until = None
        self._apply_if_adaptive()
        self._persist()
        self._window_started_at = self._monotonic_clock()
        self._window_requests = 0
        self._window_health_errors = 0
        self._window_5xx = 0
        self._window_latency_ms = []

    def _apply_if_adaptive(self) -> None:
        if self._settings.yfinance_adaptive_rate_mode != "adaptive":
            return
        self._enforced_rpm = self._recommended_rpm
        self._enforced_concurrency = self._recommended_concurrency
        self._limiter.update_rate_per_minute(self.provider, self._enforced_rpm)

    def _snapshot(self) -> AdaptiveRateSnapshot:
        return AdaptiveRateSnapshot(
            provider=self.provider,
            mode=self._settings.yfinance_adaptive_rate_mode,
            enforced_rpm=self._enforced_rpm,
            recommended_rpm=self._recommended_rpm,
            last_safe_rpm=self._last_safe_rpm,
            minimum_rpm=self._settings.yfinance_minimum_rpm,
            maximum_rpm=self._settings.yfinance_maximum_rpm,
            enforced_concurrency=self._enforced_concurrency,
            recommended_concurrency=self._recommended_concurrency,
            consecutive_healthy_windows=self._healthy_windows,
            circuit_state=self._circuit_state,
            cooldown_until=self._cooldown_until,
            last_429_at=self._last_429_at,
            recent_error_rate=self._recent_error_rate,
            latency_baseline_ms=self._latency_baseline_ms,
        )

    def _load_state(self) -> dict[str, Any] | None:
        if self._store is None or not hasattr(self._store, "adaptive_rate_state"):
            return None
        try:
            return self._store.adaptive_rate_state(self.provider)
        except Exception as exc:  # pragma: no cover - defensive observability path
            logger.warning("Unable to load Yahoo adaptive-rate state: %s", exc)
            return None

    def _persist(self) -> None:
        if self._store is None or not hasattr(self._store, "upsert_adaptive_rate_state"):
            return
        snapshot = asdict(self._snapshot())
        state = {
            "provider": self.provider,
            "current_rpm": snapshot["recommended_rpm"],
            "last_safe_rpm": snapshot["last_safe_rpm"],
            "minimum_rpm": snapshot["minimum_rpm"],
            "maximum_rpm": snapshot["maximum_rpm"],
            "current_concurrency": snapshot["recommended_concurrency"],
            "consecutive_healthy_windows": snapshot["consecutive_healthy_windows"],
            "circuit_state": snapshot["circuit_state"],
            "cooldown_until": snapshot["cooldown_until"],
            "last_429_at": snapshot["last_429_at"],
            "recent_error_rate": snapshot["recent_error_rate"],
            "latency_baseline_ms": snapshot["latency_baseline_ms"],
            "updated_at": self._utc_clock(),
        }
        try:
            self._store.upsert_adaptive_rate_state(state)
        except Exception as exc:  # pragma: no cover - defensive observability path
            logger.warning("Unable to persist Yahoo adaptive-rate state: %s", exc)


def _clamp(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(value, maximum))


def _nullable_int(value: Any) -> int | None:
    return int(value) if value is not None else None


def _nullable_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None
