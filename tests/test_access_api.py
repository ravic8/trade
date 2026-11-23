import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine

from trade_data.api import app as api
from trade_data.api.access import AccessRequests

ADMIN = {"cf-access-authenticated-user-email": "ravi@chain8.org"}
USER = {"cf-access-authenticated-user-email": "teammate@chain8.org"}


@pytest.fixture()
def client(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path}/access.db")
    repo = AccessRequests(engine)
    monkeypatch.setattr(api, "_access_repo", lambda: repo)
    monkeypatch.setenv("ADMIN_EMAILS", "ravi@chain8.org")
    with TestClient(api.app) as c:
        yield c


def test_me_requires_auth(client):
    assert client.get("/api/me").status_code == 401


def test_me_roles(client):
    assert client.get("/api/me", headers=ADMIN).json() == {
        "email": "ravi@chain8.org",
        "role": "admin",
    }
    assert client.get("/api/me", headers=USER).json()["role"] == "user"


def test_access_request_flow(client):
    r = client.post(
        "/api/access-requests",
        json={"name": "Teammate", "email": "Teammate@Chain8.org", "use_case": "Nifty options"},
    )
    assert r.status_code == 201
    assert r.json()["status"] == "pending"
    request_id = r.json()["id"]

    # Non-admin cannot list or decide.
    assert client.get("/api/admin/access-requests", headers=USER).status_code == 403
    assert (
        client.post(f"/api/admin/access-requests/{request_id}/approve", headers=USER).status_code
        == 403
    )
    assert client.get("/api/admin/access-requests").status_code == 403

    # Admin lists, approves, and the decision is recorded.
    rows = client.get("/api/admin/access-requests", headers=ADMIN).json()
    assert [row["email"] for row in rows] == ["teammate@chain8.org"]
    decided = client.post(f"/api/admin/access-requests/{request_id}/approve", headers=ADMIN)
    assert decided.status_code == 200
    assert decided.json()["status"] == "approved"
    assert decided.json()["decided_by"] == "ravi@chain8.org"


def test_reject_and_missing(client):
    r = client.post("/api/access-requests", json={"name": "X", "email": "x@example.com"})
    rid = r.json()["id"]
    assert (
        client.post(f"/api/admin/access-requests/{rid}/reject", headers=ADMIN).json()["status"]
        == "rejected"
    )
    assert client.post("/api/admin/access-requests/9999/approve", headers=ADMIN).status_code == 404


def test_invalid_email_rejected(client):
    assert (
        client.post("/api/access-requests", json={"name": "X", "email": "nope"}).status_code == 422
    )
