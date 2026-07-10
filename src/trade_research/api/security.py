from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable
from math import ceil
from threading import Lock
from time import monotonic
from typing import Protocol

from fastapi import HTTPException, Request, status
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


class RateLimitSettings(Protocol):
    chat_rate_limit_enabled: bool
    chat_rate_limit_requests: int
    chat_rate_limit_window_seconds: int
    chat_rate_limit_trust_forwarded_for: bool


class AdminSettings(Protocol):
    admin_emails: str
    admin_email_headers: str


class SlidingWindowRateLimiter:
    def __init__(self) -> None:
        self._lock = Lock()
        self._hits: dict[str, deque[float]] = {}

    def check(self, key: str, limit: int, window_seconds: int) -> tuple[bool, int]:
        now = monotonic()
        cutoff = now - window_seconds
        with self._lock:
            hits = self._hits.setdefault(key, deque())
            while hits and hits[0] <= cutoff:
                hits.popleft()
            if len(hits) >= limit:
                return False, max(ceil(hits[0] + window_seconds - now), 1)
            hits.append(now)
            if not hits:
                self._hits.pop(key, None)
            return True, 0


class ChatRateLimitMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        settings_getter: Callable[[], RateLimitSettings],
        limiter: SlidingWindowRateLimiter | None = None,
    ) -> None:
        self.app = app
        self.settings_getter = settings_getter
        self.limiter = limiter or SlidingWindowRateLimiter()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not _is_chat_query(scope):
            await self.app(scope, receive, send)
            return

        settings = self.settings_getter()
        if not settings.chat_rate_limit_enabled:
            await self.app(scope, receive, send)
            return

        allowed, retry_after = self.limiter.check(
            _client_ip(scope, settings.chat_rate_limit_trust_forwarded_for),
            settings.chat_rate_limit_requests,
            settings.chat_rate_limit_window_seconds,
        )
        if allowed:
            await self.app(scope, receive, send)
            return

        response = JSONResponse(
            {"detail": "Chat query rate limit exceeded. Retry after the cooldown."},
            status_code=429,
            headers={"Retry-After": str(retry_after)},
        )
        await response(scope, receive, send)


def cors_origins(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def require_admin_request(request: Request, settings: AdminSettings) -> str:
    allowed = _normalized_items(settings.admin_emails)
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access is not configured.",
        )
    email = authenticated_email(request, settings.admin_email_headers)
    if email is None or email.lower() not in allowed:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required.",
        )
    return email.lower()


def authenticated_email(request: Request, header_names: str) -> str | None:
    for name in _normalized_items(header_names):
        value = request.headers.get(name)
        if value and "@" in value:
            return value.strip()
    return None


def _normalized_items(value: str | Iterable[str]) -> set[str]:
    if isinstance(value, str):
        items = value.split(",")
    else:
        items = value
    return {item.strip().lower() for item in items if item and item.strip()}


def _is_chat_query(scope: Scope) -> bool:
    return scope.get("method") == "POST" and scope.get("path") == "/api/chat/query"


def _client_ip(scope: Scope, trust_forwarded_for: bool) -> str:
    if trust_forwarded_for:
        forwarded_for = _header(scope, b"x-forwarded-for")
        if forwarded_for:
            return forwarded_for.split(",", maxsplit=1)[0].strip()
    client = scope.get("client")
    return str(client[0]) if client else "unknown"


def _header(scope: Scope, name: bytes) -> str | None:
    for header_name, header_value in scope.get("headers", []):
        if header_name.lower() == name:
            return header_value.decode("latin-1")
    return None
