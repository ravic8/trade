from __future__ import annotations

import httpx

from trade_research.chat.llm import ChatLLMClient
from trade_research.config import Settings


def _settings(**overrides) -> Settings:
    return Settings(
        _env_file=None,
        gemini_api_key="gemini-test-key",
        chat_answer_model="gemini-2.5-flash",
        chat_llm_retry_base_seconds=0,
        **overrides,
    )


def test_gemini_answer_generation_caps_output_and_reports_usage() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "candidates": [
                    {
                        "content": {"parts": [{"text": "Grounded answer."}]},
                        "finishReason": "STOP",
                    }
                ],
                "usageMetadata": {
                    "promptTokenCount": 41,
                    "candidatesTokenCount": 12,
                    "totalTokenCount": 53,
                },
            },
    )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        llm_client = ChatLLMClient(_settings(chat_answer_max_output_tokens=321), client)
        generation = llm_client.generate_answer(
            question="What happened?",
            deterministic_answer="Draft answer.",
            warnings=["Partial data."],
            citations=["timescale_query: Session summary"],
        )

    payload = httpx.Request.read(requests[0]).decode()
    assert generation.text == "Grounded answer."
    assert '"maxOutputTokens":321' in payload
    assert '"thinkingBudget":0' in payload
    assert requests[0].headers["x-goog-api-key"] == "gemini-test-key"
    assert generation.telemetry["status"] == "ok"
    assert generation.telemetry["finish_reason"] == "STOP"
    assert generation.telemetry["usage"] == {
        "prompt_tokens": 41,
        "response_tokens": 12,
        "total_tokens": 53,
    }


def test_gemini_answer_generation_retries_and_reports_failure() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"error": {"message": "busy"}})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        generation = ChatLLMClient(_settings(chat_llm_retry_attempts=2), client).generate_answer(
            question="What happened?",
            deterministic_answer="Draft answer.",
            warnings=[],
            citations=[],
        )

    assert calls == 2
    assert generation.text is None
    assert generation.telemetry["status"] == "failed"
    assert generation.telemetry["attempts"] == 2
    assert generation.telemetry["error_type"] == "HTTPStatusError"
    assert generation.telemetry["http_status"] == 503
