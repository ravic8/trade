"""Read-only checks for the software-to-infrastructure boundary."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from redis import Redis
from sqlalchemy import create_engine, text

from trade_data.config import Settings


@dataclass(frozen=True)
class RuntimeCheck:
    name: str
    status: str
    detail: str


@dataclass(frozen=True)
class RuntimeReport:
    checks: tuple[RuntimeCheck, ...]

    @property
    def ready(self) -> bool:
        return all(check.status != "fail" for check in self.checks)

    def as_dict(self) -> dict[str, object]:
        return {"ready": self.ready, "checks": [asdict(check) for check in self.checks]}


def inspect_runtime(settings: Settings, *, check_services: bool = False) -> RuntimeReport:
    """Inspect configuration and optionally probe required services without mutations.

    Provider credentials are deliberately not tested here: provider APIs may
    charge, rate-limit, or require market-specific consent. Missing optional
    providers do not prevent the application from starting.
    """

    checks = [
        RuntimeCheck(
            "postgresql", "configured", "DATABASE_URL configuration is valid; service not probed"
        ),
        RuntimeCheck("redis", "configured", "REDIS_URL configuration is valid; service not probed"),
    ]
    if check_services:
        try:
            engine = create_engine(settings.database_url, connect_args={"connect_timeout": 3})
            try:
                with engine.connect() as connection:
                    connection.execute(text("SELECT 1"))
            finally:
                engine.dispose()
            checks[0] = RuntimeCheck("postgresql", "pass", "Read-only query succeeded")
        except Exception:
            checks[0] = RuntimeCheck(
                "postgresql", "fail", "Connection failed; check DATABASE_URL and service health"
            )

        try:
            client = Redis.from_url(settings.redis_url, socket_connect_timeout=3, socket_timeout=3)
            try:
                client.ping()
            finally:
                client.close()
            checks[1] = RuntimeCheck("redis", "pass", "PING succeeded")
        except Exception:
            checks[1] = RuntimeCheck(
                "redis", "fail", "Connection failed; check REDIS_URL and service health"
            )

    daily_provider_configured = bool(
        settings.yfinance_daily_enabled or settings.upstox_access_token
    )
    checks.append(
        RuntimeCheck(
            "daily_provider",
            "configured" if daily_provider_configured else "not_configured",
            "At least one daily provider is enabled or credentialed"
            if daily_provider_configured
            else "No daily provider is enabled; configure a real provider before ingestion",
        )
    )
    checks.append(
        RuntimeCheck(
            "clickhouse",
            "configured" if settings.clickhouse_enabled else "disabled",
            "Optional analytical store; not probed"
            if settings.clickhouse_enabled
            else "Optional analytical store is disabled",
        )
    )
    checks.append(
        RuntimeCheck(
            "object_store",
            "configured" if settings.object_store_enabled else "disabled",
            "Optional object store; not probed"
            if settings.object_store_enabled
            else "Optional object store is disabled",
        )
    )
    return RuntimeReport(tuple(checks))
