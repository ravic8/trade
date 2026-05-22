from __future__ import annotations

import logging
from dataclasses import dataclass
from time import monotonic, sleep
from typing import Any

import httpx

from trade_research.config import Settings


@dataclass(frozen=True)
class ChatLLMGeneration:
    text: str | None
    telemetry: dict[str, Any]


class ChatLLMClient:
    def __init__(self, settings: Settings, http_client: httpx.Client | None = None) -> None:
        self.settings = settings
        self._logger = logging.getLogger(__name__)
        if not settings.gemini_api_key:
            raise ValueError("GEMINI_API_KEY is required for Gemini answer generation")
        self.client = http_client or httpx.Client(timeout=settings.chat_llm_timeout_seconds)
        self._owns_client = http_client is None

    def generate_answer(
        self,
        question: str,
        deterministic_answer: str,
        warnings: list[str],
        citations: list[str],
    ) -> ChatLLMGeneration:
        prompt = self._build_prompt(question, deterministic_answer, warnings, citations)
        started_at = monotonic()
        telemetry: dict[str, Any] = {
            "provider": "gemini",
            "model": self.settings.chat_answer_model,
            "status": "failed",
            "attempts": 0,
            "max_output_tokens": self.settings.chat_answer_max_output_tokens,
            "thinking_budget": self.settings.chat_llm_thinking_budget,
        }
        for attempt in range(1, self.settings.chat_llm_retry_attempts + 1):
            telemetry["attempts"] = attempt
            try:
                response = self.client.post(
                    self._generate_url(),
                    headers={
                        "Content-Type": "application/json",
                        "x-goog-api-key": self.settings.gemini_api_key,
                    },
                    json=self._request_payload(prompt),
                )
                if response.status_code in {408, 429} or response.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        "Gemini returned a retryable status",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                payload = response.json()
                content = self._response_text(payload)
                telemetry |= {
                    "status": "ok" if content else "empty",
                    "latency_ms": self._elapsed_ms(started_at),
                    "usage": self._usage(payload),
                    "finish_reason": self._finish_reason(payload),
                }
                return ChatLLMGeneration(text=content, telemetry=telemetry)
            except (httpx.HTTPError, ValueError) as exc:
                retryable = self._retryable(exc)
                telemetry |= {
                    "latency_ms": self._elapsed_ms(started_at),
                    "error_type": type(exc).__name__,
                }
                if isinstance(exc, httpx.HTTPStatusError):
                    telemetry["http_status"] = exc.response.status_code
                if not retryable or attempt == self.settings.chat_llm_retry_attempts:
                    self._logger.warning("Gemini answer generation failed: %s", exc)
                    return ChatLLMGeneration(text=None, telemetry=telemetry)
                sleep(self.settings.chat_llm_retry_base_seconds * (2 ** (attempt - 1)))
        return ChatLLMGeneration(text=None, telemetry=telemetry)

    def close(self) -> None:
        if self._owns_client:
            self.client.close()

    def _generate_url(self) -> str:
        base_url = self.settings.gemini_base_url.rstrip("/")
        model = self.settings.chat_answer_model.removeprefix("models/")
        return f"{base_url}/models/{model}:generateContent"

    def _request_payload(self, prompt: str) -> dict[str, Any]:
        return {
            "systemInstruction": {
                "parts": [
                    {
                        "text": (
                            "You are a trade analyst assistant. "
                            "Only restate and synthesize provided evidence. "
                            "Do not add uncited facts."
                        )
                    }
                ]
            },
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.2,
                "maxOutputTokens": self.settings.chat_answer_max_output_tokens,
                "thinkingConfig": {
                    "thinkingBudget": self.settings.chat_llm_thinking_budget,
                },
            },
        }

    @staticmethod
    def _response_text(payload: dict[str, Any]) -> str | None:
        candidates = payload.get("candidates") or []
        if not candidates:
            return None
        parts = candidates[0].get("content", {}).get("parts") or []
        text = "".join(str(part.get("text") or "") for part in parts).strip()
        return text or None

    @staticmethod
    def _usage(payload: dict[str, Any]) -> dict[str, int]:
        usage = payload.get("usageMetadata") or {}
        fields = {
            "prompt_tokens": usage.get("promptTokenCount"),
            "response_tokens": usage.get("candidatesTokenCount"),
            "thought_tokens": usage.get("thoughtsTokenCount"),
            "total_tokens": usage.get("totalTokenCount"),
        }
        return {key: value for key, value in fields.items() if isinstance(value, int)}

    @staticmethod
    def _finish_reason(payload: dict[str, Any]) -> str | None:
        candidates = payload.get("candidates") or []
        finish_reason = candidates[0].get("finishReason") if candidates else None
        return str(finish_reason) if finish_reason else None

    @staticmethod
    def _retryable(exc: Exception) -> bool:
        if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
            return True
        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code
            return status_code in {408, 429} or status_code >= 500
        return False

    @staticmethod
    def _elapsed_ms(started_at: float) -> int:
        return max(int((monotonic() - started_at) * 1000), 0)

    @staticmethod
    def _build_prompt(
        question: str,
        deterministic_answer: str,
        warnings: list[str],
        citations: list[str],
    ) -> str:
        warning_text = "\n".join(f"- {item}" for item in warnings) if warnings else "- None"
        citation_text = "\n".join(f"- {item}" for item in citations) if citations else "- None"
        return (
            f"Question:\n{question}\n\n"
            f"Evidence-grounded draft answer:\n{deterministic_answer}\n\n"
            f"Warnings:\n{warning_text}\n\n"
            f"Citations:\n{citation_text}\n\n"
            "Rewrite the answer to be concise and analyst-friendly while preserving these warnings."
        )
