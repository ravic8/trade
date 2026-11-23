"""Approval-only access requests for trade.chain8.org.

Anyone may submit a request from the public landing page. Only an admin
(ADMIN_EMAILS, e.g. ravi@chain8.org) may list, approve or reject them.
Approving grants role "user"; only ADMIN_EMAILS ever map to role "admin".
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, select
from sqlalchemy.engine import Engine

metadata = MetaData()

access_requests = Table(
    "access_requests",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(200), nullable=False),
    Column("email", String(320), nullable=False, unique=True),
    Column("use_case", String(1000), nullable=False, default=""),
    Column("status", String(20), nullable=False, default="pending"),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("decided_at", DateTime(timezone=True)),
    Column("decided_by", String(320)),
)


class AccessRequests:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        metadata.create_all(engine)

    def submit(self, name: str, email: str, use_case: str) -> dict:
        email = email.strip().lower()
        now = datetime.now(UTC)
        with self.engine.begin() as conn:
            existing = conn.execute(
                select(access_requests).where(access_requests.c.email == email)
            ).mappings().first()
            if existing:
                return dict(existing)
            result = conn.execute(
                access_requests.insert().values(
                    name=name.strip(), email=email, use_case=use_case.strip(),
                    status="pending", created_at=now,
                )
            )
            row = conn.execute(
                select(access_requests).where(access_requests.c.id == result.inserted_primary_key[0])
            ).mappings().one()
            return dict(row)

    def list_all(self) -> list[dict]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(access_requests).order_by(access_requests.c.created_at.desc())
            ).mappings()
            return [dict(r) for r in rows]

    def decide(self, request_id: int, status: str, decided_by: str) -> dict | None:
        if status not in {"approved", "rejected"}:
            raise ValueError("status must be approved or rejected")
        with self.engine.begin() as conn:
            result = conn.execute(
                access_requests.update()
                .where(access_requests.c.id == request_id)
                .values(status=status, decided_at=datetime.now(UTC), decided_by=decided_by.lower())
            )
            if result.rowcount == 0:
                return None
            row = conn.execute(
                select(access_requests).where(access_requests.c.id == request_id)
            ).mappings().one()
            return dict(row)
