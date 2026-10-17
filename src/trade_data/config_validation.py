"""Configuration-only validation. Never connect or include supplied values in errors."""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlsplit

from sqlalchemy.engine import make_url

if TYPE_CHECKING:
    from trade_data.config import Settings


class ConfigurationError(RuntimeError):
    """A startup failure whose message is safe to print."""


def validate_runtime_configuration(settings: Settings) -> None:
    issues: list[str] = []

    def endpoint(name: str, value: str | None, schemes: set[str]) -> None:
        try:
            parsed = urlsplit(value or "")
            if (
                parsed.scheme not in schemes
                or not parsed.hostname
                or any(char.isspace() for char in value or "")
            ):
                raise ValueError
            if parsed.port is not None and not 1 <= parsed.port <= 65535:
                raise ValueError
            if parsed.fragment:
                raise ValueError
            if parsed.scheme in {"redis", "rediss"}:
                values = parse_qs(parsed.query).get("db", [])
                if parsed.path not in {"", "/"}:
                    values.append(parsed.path.removeprefix("/"))
                if any(not value.isascii() or not value.isdigit() for value in values):
                    raise ValueError
        except (ValueError, TypeError):
            issues.append(f"{name}: supply an absolute URL with a host and a valid port")

    try:
        database = make_url(settings.database_url)
        if database.get_backend_name() == "sqlite":
            pass  # Retained for local fixtures; PostgreSQL is the core deployment store.
        elif (
            database.get_backend_name() != "postgresql"
            or not database.host
            or not database.database
            or any(c.isspace() for c in database.host)
            or (database.port is not None and not 1 <= database.port <= 65535)
        ):
            raise ValueError
    except Exception:
        # SQLAlchemy parser errors can contain the original URL. Never propagate them.
        issues.append("DATABASE_URL: supply a PostgreSQL URL with a host and database name")
    endpoint("REDIS_URL", settings.redis_url, {"redis", "rediss"})
    if settings.clickhouse_enabled and (
        not settings.clickhouse_host.strip()
        or any(c.isspace() or c in "/?#@" for c in settings.clickhouse_host)
    ):
        issues.append(
            "CLICKHOUSE_HOST: supply a hostname or IP address when CLICKHOUSE_ENABLED=true"
        )
    if settings.object_store_enabled:
        # None uses AWS's regional default; a configured custom endpoint must be valid.
        if settings.object_store_endpoint_url is not None:
            endpoint(
                "OBJECT_STORE_ENDPOINT_URL", settings.object_store_endpoint_url, {"http", "https"}
            )
        for field in ("raw", "datasets", "exports"):
            if not getattr(settings, f"object_store_{field}_bucket").strip():
                issues.append(f"OBJECT_STORE_{field.upper()}_BUCKET: must not be empty")
    if settings.otel_enabled:
        endpoint(
            "OTEL_EXPORTER_OTLP_ENDPOINT", settings.otel_exporter_otlp_endpoint, {"http", "https"}
        )
    if issues:
        raise ValueError("; ".join(issues))
