from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from trade_research.api.security import ChatRateLimitMiddleware, cors_origins


def _settings(**overrides) -> SimpleNamespace:
    values = {
        "chat_rate_limit_enabled": True,
        "chat_rate_limit_requests": 1,
        "chat_rate_limit_window_seconds": 60,
        "chat_rate_limit_trust_forwarded_for": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _app(settings: SimpleNamespace) -> FastAPI:
    app = FastAPI()
    app.add_middleware(ChatRateLimitMiddleware, settings_getter=lambda: settings)

    @app.post("/api/chat/query")
    def query() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/chat/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


def test_chat_query_rate_limit_rejects_excess_posts() -> None:
    with TestClient(_app(_settings())) as client:
        assert client.post("/api/chat/query").status_code == 200
        response = client.post("/api/chat/query")
        assert response.status_code == 429
        assert int(response.headers["Retry-After"]) >= 1
        assert client.get("/api/chat/health").status_code == 200


def test_forwarded_ip_is_used_only_when_enabled() -> None:
    with TestClient(_app(_settings(chat_rate_limit_trust_forwarded_for=True))) as client:
        first = client.post("/api/chat/query", headers={"X-Forwarded-For": "198.51.100.1"})
        second = client.post("/api/chat/query", headers={"X-Forwarded-For": "198.51.100.2"})
        assert first.status_code == 200
        assert second.status_code == 200


def test_cors_origins_drop_blank_values() -> None:
    assert cors_origins(" https://lens.example.com, ,http://localhost:5173 ") == [
        "https://lens.example.com",
        "http://localhost:5173",
    ]
