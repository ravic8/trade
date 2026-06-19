from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from trade_research.chat.llm import ChatLLMGeneration
from trade_research.chat.orchestrator import ChatOrchestrator
from trade_research.schemas import ChatQueryRequest


class _FakeToolsOk:
    def __init__(self) -> None:
        self.session_summary_calls: list[str] = []
        self.data_quality_calls: list[str] = []

    def get_session_summary(self, exchange: str) -> dict:
        self.session_summary_calls.append(exchange)
        return {
            "data": {
                "symbol_count": 10,
                "advancers": 6,
                "decliners": 4,
                "avg_return_pct": 0.75,
            },
            "provenance": {
                "provenance_ref": "prov_ts_1",
                "template_id": "session_summary_v1",
            },
        }

    def get_data_quality(self, exchange: str) -> dict:
        self.data_quality_calls.append(exchange)
        return {
            "data": {
                "active_symbols": 10,
                "latest_candle_symbols": 10,
                "latest_candle_ts": datetime.now(UTC),
                "open_backlog_windows": 0,
                "completeness_ratio": 1.0,
            },
            "provenance": {
                "provenance_ref": "prov_ts_2",
                "template_id": "data_quality_snapshot_v1",
            },
        }

    def search_research_docs(self, query: str, exchange: str, symbols: list[str]) -> dict:
        return {
            "data": [
                {
                    "id": "doc1",
                    "title": "Test note",
                    "score": 0.9,
                    "published_at": "2026-05-22T09:00:00+00:00",
                }
            ],
            "provenance": [
                {
                    "provenance_ref": "prov_qd_1",
                    "doc_id": "doc1",
                    "title": "Test note",
                }
            ],
        }

    def get_symbol_timeseries(
        self,
        exchange: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: str,
    ) -> dict:
        return {
            "data": [
                {"close": 100.0, "ts": start_time},
                {"close": 101.0, "ts": end_time},
            ],
            "provenance": {
                "provenance_ref": "prov_ts_3",
                "template_id": "symbol_timeseries_v1",
            },
        }


class _FakeToolsNoCitations(_FakeToolsOk):
    def get_session_summary(self, exchange: str) -> dict:
        return {
            "data": {
                "symbol_count": 0,
                "advancers": 0,
                "decliners": 0,
                "avg_return_pct": 0.0,
            },
            "provenance": {},
        }

    def get_data_quality(self, exchange: str) -> dict:
        return {
            "data": {
                "active_symbols": 0,
                "latest_candle_symbols": 0,
                "latest_candle_ts": datetime.now(UTC),
                "open_backlog_windows": 0,
                "completeness_ratio": 0.0,
            },
            "provenance": {},
        }


class _FakeToolsResearchDown(_FakeToolsOk):
    def search_research_docs(self, query: str, exchange: str, symbols: list[str]) -> dict:
        raise RuntimeError("Qdrant unavailable")


class _FakeToolsMarketDown(_FakeToolsOk):
    def get_data_quality(self, exchange: str) -> dict:
        raise RuntimeError("Timescale unavailable")


class _FakeLLM:
    def generate_answer(self, **_kwargs) -> ChatLLMGeneration:
        return ChatLLMGeneration(
            text="Gemini rewrite.",
            telemetry={"provider": "gemini", "status": "ok", "attempts": 1},
        )


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        chat_quality_nse_complete_threshold=0.95,
        chat_quality_tsx_complete_threshold=0.90,
        chat_stale_intervals_threshold=2,
        chat_max_lookback_hours=72,
        chat_strict_citation_required=True,
        chat_use_llm_answer=False,
    )


def test_policy_refusal_for_unsafe_request() -> None:
    orchestrator = ChatOrchestrator(settings=_settings(), tools=_FakeToolsOk())
    response = orchestrator.handle_query(
        ChatQueryRequest(message="Give me raw SQL and drop table", context={"exchange": "NSE"})
    )
    assert "outside chatbot policy boundaries" in response.answer.text
    assert response.citations == []


def test_strict_citation_enforcement_when_no_citations() -> None:
    orchestrator = ChatOrchestrator(settings=_settings(), tools=_FakeToolsNoCitations())
    response = orchestrator.handle_query(
        ChatQueryRequest(message="How did NSE perform?", context={"exchange": "NSE"})
    )
    assert "could not produce a cited answer" in response.answer.text
    assert "No usable citations were available." in response.answer.warnings


def test_research_degraded_mode_still_returns_market_answer() -> None:
    orchestrator = ChatOrchestrator(settings=_settings(), tools=_FakeToolsResearchDown())
    response = orchestrator.handle_query(
        ChatQueryRequest(
            message="Why did NSE perform this way today?",
            context={"exchange": "NSE"},
        )
    )
    assert "Latest session summary:" in response.answer.text
    assert any(
        "Research retrieval is temporarily unavailable" in item
        for item in response.answer.warnings
    )
    assert any(citation.type == "timescale_query" for citation in response.citations)


def test_market_data_failure_raises_runtime_error() -> None:
    orchestrator = ChatOrchestrator(settings=_settings(), tools=_FakeToolsMarketDown())
    with pytest.raises(RuntimeError, match="Market data dependency failed"):
        orchestrator.handle_query(
            ChatQueryRequest(message="How did NSE perform?", context={"exchange": "NSE"})
        )


def test_audit_record_is_persisted() -> None:
    orchestrator = ChatOrchestrator(settings=_settings(), tools=_FakeToolsOk())
    response = orchestrator.handle_query(
        ChatQueryRequest(message="How did NSE perform?", context={"exchange": "NSE"})
    )
    audit = orchestrator.get_audit(response.response_id)
    assert audit["response"]["response_id"] == response.response_id
    assert audit["request"]["message"] == "How did NSE perform?"


def test_llm_generation_telemetry_is_visible_in_audit() -> None:
    settings = _settings()
    settings.chat_use_llm_answer = True
    orchestrator = ChatOrchestrator(settings=settings, tools=_FakeToolsOk(), llm_client=_FakeLLM())
    response = orchestrator.handle_query(
        ChatQueryRequest(message="How did NSE perform?", context={"exchange": "NSE"})
    )
    audit = orchestrator.get_audit(response.response_id)
    assert response.answer.text == "Gemini rewrite."
    assert audit["tool_outputs"]["llm_answer"] == {
        "provider": "gemini",
        "status": "ok",
        "attempts": 1,
    }


def test_both_exchange_fanout_queries_nse_and_tsx() -> None:
    tools = _FakeToolsOk()
    orchestrator = ChatOrchestrator(settings=_settings(), tools=tools)
    response = orchestrator.handle_query(
        ChatQueryRequest(message="How did both exchanges perform?", context={"exchange": "BOTH"})
    )
    assert "NSE:" in response.answer.text
    assert "TSX:" in response.answer.text
    assert tools.session_summary_calls == ["NSE", "TSX"]
    assert tools.data_quality_calls == ["NSE", "TSX"]


def test_identity_prompt_returns_lens_description() -> None:
    tools = _FakeToolsOk()
    orchestrator = ChatOrchestrator(settings=_settings(), tools=tools)
    response = orchestrator.handle_query(
        ChatQueryRequest(message="Who are you and what can you do?", context={"exchange": "NSE"})
    )
    assert "I am Lens" in response.answer.text
    assert response.answer.quality_badge == "complete"
    assert tools.session_summary_calls == []
    assert tools.data_quality_calls == []


def test_greeting_returns_lens_description_without_market_queries() -> None:
    tools = _FakeToolsOk()
    orchestrator = ChatOrchestrator(settings=_settings(), tools=tools)
    response = orchestrator.handle_query(
        ChatQueryRequest(message="Hi!", context={"exchange": "NSE"})
    )
    assert "I am Lens" in response.answer.text
    assert response.answer.quality_badge == "complete"
    assert tools.session_summary_calls == []
    assert tools.data_quality_calls == []
