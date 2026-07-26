from __future__ import annotations

import json
from typing import Any

import httpx

from trade_research.config import Settings
from trade_research.filings.agent_llm import FilingAgentLLM
from trade_research.filings.agent_models import InvestigationPlan


def _settings(*, provider: str, model: str) -> Settings:
    credentials: dict[str, str] = (
        {"openai_api_key": "test-openai-key"}
        if provider == "openai"
        else {"gemini_api_key": "test-gemini-key"}
    )
    return Settings(
        app_env="test",
        filing_agent_llm_enabled=True,
        filing_agent_llm_provider=provider,
        filing_agent_llm_model=model,
        filing_agent_retry_attempts=1,
        langfuse_enabled=False,
        otel_enabled=False,
        **credentials,
    )


def _request_json(request: httpx.Request) -> dict[str, Any]:
    return json.loads(request.content.decode("utf-8"))


def test_gpt_5_4_uses_current_token_parameter_and_strict_plan_schema() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "intent": "rank_growth",
                                    "metric": "net_profit",
                                    "comparison": "yoy",
                                    "limit": 10,
                                    "scope": "consolidated",
                                    "rationale": "Rank comparable approved facts.",
                                }
                            )
                        }
                    }
                ],
                "usage": {
                    "prompt_tokens": 100,
                    "completion_tokens": 20,
                    "total_tokens": 120,
                },
            },
        )

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = FilingAgentLLM(
        _settings(provider="openai", model="gpt-5.4-nano"),
        http_client=http_client,
    )

    plan, telemetry = client.plan(
        question="Rank Nifty 50 companies by year-over-year net-profit growth.",
        requested_comparison="yoy",
    )
    http_client.close()

    assert telemetry["status"] == "ok"
    assert telemetry["fallback"] is False
    assert plan.limit == 10
    payload = _request_json(requests[0])
    assert payload["max_completion_tokens"] == 1_200
    assert "max_tokens" not in payload
    assert "temperature" not in payload
    response_format = payload["response_format"]
    assert response_format["type"] == "json_schema"
    assert response_format["json_schema"]["name"] == "investigation_plan"
    assert response_format["json_schema"]["strict"] is True
    schema = response_format["json_schema"]["schema"]
    assert schema["properties"]["limit"]["type"] == "integer"
    assert schema["properties"]["rationale"]["type"] == "string"
    assert schema["additionalProperties"] is False


def test_legacy_openai_model_retains_deterministic_temperature() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "intent": "coverage",
                                    "metric": "revenue",
                                    "comparison": "qoq",
                                    "limit": 10,
                                    "scope": "consolidated",
                                    "rationale": "Measure coverage.",
                                }
                            )
                        }
                    }
                ],
                "usage": {},
            },
        )

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = FilingAgentLLM(
        _settings(provider="openai", model="gpt-4o-mini"),
        http_client=http_client,
    )

    _, telemetry = client.plan(
        question="Show revenue coverage.",
        requested_comparison="qoq",
    )
    http_client.close()

    assert telemetry["status"] == "ok"
    payload = _request_json(requests[0])
    assert payload["temperature"] == 0
    assert payload["max_completion_tokens"] == 1_200


def test_gemini_receives_strict_json_schema_for_plan() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": json.dumps(
                                        {
                                            "intent": "rank_growth",
                                            "metric": "net_profit",
                                            "comparison": "yoy",
                                            "limit": 10,
                                            "scope": "consolidated",
                                            "rationale": "Rank approved facts.",
                                        }
                                    )
                                }
                            ]
                        }
                    }
                ],
                "usageMetadata": {},
            },
        )

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = FilingAgentLLM(
        _settings(provider="gemini", model="gemini-flash-lite-latest"),
        http_client=http_client,
    )

    plan, telemetry = client.plan(
        question="Rank net-profit growth.",
        requested_comparison="yoy",
    )
    http_client.close()

    assert telemetry["status"] == "ok"
    assert plan.limit == 10
    payload = _request_json(requests[0])
    generation_config = payload["generationConfig"]
    assert generation_config["responseMimeType"] == "application/json"
    assert generation_config["responseJsonSchema"]["properties"]["limit"] == {
        "type": "integer",
        "minimum": 1,
        "maximum": 20,
    }


def test_synthesis_uses_narrow_schema_without_server_metadata() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "title": "Nifty 50 comparison",
                                    "summary": "One eligible company was ranked.",
                                    "claims": [
                                        {
                                            "text": "Infosys reported 10% growth.",
                                            "citation_ids": ["c1", "c2"],
                                        }
                                    ],
                                    "limitations": [],
                                }
                            )
                        }
                    }
                ],
                "usage": {},
            },
        )

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = FilingAgentLLM(
        _settings(provider="openai", model="gpt-5.4-nano"),
        http_client=http_client,
    )
    synthesis = client.synthesize(
        question="Rank net-profit growth.",
        plan=InvestigationPlan(
            intent="rank_growth",
            metric="net_profit",
            comparison="yoy",
        ),
        comparison={
            "rows": [],
            "citations": [
                {"citation_id": "c1", "label": "current"},
                {"citation_id": "c2", "label": "prior"},
            ],
        },
        coverage={"represented_company_count": 1, "member_count": 50},
    )
    http_client.close()

    assert synthesis.model_used is True
    payload = _request_json(requests[0])
    json_schema = payload["response_format"]["json_schema"]
    assert json_schema["name"] == "investigation_synthesis"
    assert set(json_schema["schema"]["properties"]) == {
        "title",
        "summary",
        "claims",
        "limitations",
    }


def test_provider_error_telemetry_excludes_message_and_request_data() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            json={
                "error": {
                    "message": "Sensitive provider detail",
                    "type": "invalid_request_error",
                    "param": "max_tokens",
                    "code": "unsupported_parameter",
                }
            },
        )

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = FilingAgentLLM(
        _settings(provider="openai", model="gpt-5.4-nano"),
        http_client=http_client,
    )

    plan, telemetry = client.plan(
        question="Rank net-profit growth.",
        requested_comparison="yoy",
    )
    http_client.close()

    assert telemetry["status"] == "invalid"
    assert telemetry["fallback"] is True
    assert telemetry["http_status"] == 400
    assert telemetry["provider_error_type"] == "invalid_request_error"
    assert telemetry["provider_error_param"] == "max_tokens"
    assert telemetry["provider_error_code"] == "unsupported_parameter"
    assert "message" not in telemetry
    assert plan.rationale.startswith("Deterministic safe plan")
