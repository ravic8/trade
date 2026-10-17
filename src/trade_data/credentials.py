from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from cryptography.fernet import Fernet, InvalidToken


class CredentialStore(Protocol):
    def provider_credential(
        self,
        provider: str,
        credential_type: str,
    ) -> dict | None:
        ...


@dataclass(frozen=True)
class ProviderCredentialStatus:
    provider: str
    credential_type: str
    configured: bool
    source: str
    updated_at: datetime | None = None
    updated_by: str | None = None
    last_validated_at: datetime | None = None
    validation_status: str | None = None
    validation_message: str | None = None


class CredentialEncryptionError(RuntimeError):
    """Raised when credentials cannot be encrypted or decrypted."""


def encrypt_secret(secret: str, app_secret_key: str | None) -> str:
    if not secret.strip():
        raise CredentialEncryptionError("Secret value is empty.")
    return _fernet(app_secret_key).encrypt(secret.encode("utf-8")).decode("ascii")


def decrypt_secret(encrypted_secret: str, app_secret_key: str | None) -> str:
    try:
        return _fernet(app_secret_key).decrypt(encrypted_secret.encode("ascii")).decode("utf-8")
    except InvalidToken as exc:
        raise CredentialEncryptionError("Stored credential could not be decrypted.") from exc


def resolve_provider_token(
    store: CredentialStore,
    provider: str,
    fallback_token: str | None,
    app_secret_key: str | None,
    credential_type: str = "access_token",
) -> str | None:
    row = store.provider_credential(provider=provider, credential_type=credential_type)
    if row and row.get("encrypted_value"):
        return decrypt_secret(str(row["encrypted_value"]), app_secret_key)
    return fallback_token


def provider_credential_status(
    store: CredentialStore,
    provider: str,
    fallback_token: str | None,
    credential_type: str = "access_token",
) -> ProviderCredentialStatus:
    row = store.provider_credential(provider=provider, credential_type=credential_type)
    if row:
        return ProviderCredentialStatus(
            provider=provider,
            credential_type=credential_type,
            configured=True,
            source="database",
            updated_at=row.get("updated_at"),
            updated_by=row.get("updated_by"),
            last_validated_at=row.get("last_validated_at"),
            validation_status=row.get("validation_status"),
            validation_message=row.get("validation_message"),
        )
    return ProviderCredentialStatus(
        provider=provider,
        credential_type=credential_type,
        configured=bool(fallback_token),
        source="env" if fallback_token else "missing",
    )


def _fernet(app_secret_key: str | None) -> Fernet:
    if not app_secret_key or not app_secret_key.strip():
        raise CredentialEncryptionError("APP_SECRET_KEY is required for stored credentials.")
    digest = hashlib.sha256(app_secret_key.encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))
