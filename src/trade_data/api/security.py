from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from fastapi import HTTPException, Request, status


class AdminSettings(Protocol):
    admin_emails: str
    admin_email_headers: str


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


