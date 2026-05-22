from __future__ import annotations

import logging

from openai import OpenAI

from trade_research.config import Settings


class ChatLLMClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._logger = logging.getLogger(__name__)
        self.client = OpenAI(
            api_key=self._api_key(),
            base_url=self._base_url(),
            default_headers=self._default_headers(),
        )

    def generate_answer(
        self,
        question: str,
        deterministic_answer: str,
        warnings: list[str],
        citations: list[str],
    ) -> str | None:
        prompt = self._build_prompt(question, deterministic_answer, warnings, citations)
        try:
            response = self.client.chat.completions.create(
                model=self.settings.chat_answer_model,
                temperature=0.2,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a trade analyst assistant. "
                            "Only restate and synthesize provided evidence. "
                            "Do not add uncited facts."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
            )
            content = response.choices[0].message.content if response.choices else None
            if isinstance(content, str) and content.strip():
                return content.strip()
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("LLM answer generation failed: %s", exc)
        return None

    def _api_key(self) -> str:
        provider = self.settings.llm_provider.lower()
        if provider == "openrouter":
            if not self.settings.openrouter_api_key:
                raise ValueError("OPENROUTER_API_KEY is required when LLM_PROVIDER=openrouter")
            return self.settings.openrouter_api_key
        if not self.settings.openai_api_key:
            raise ValueError("OPENAI_API_KEY is required when LLM_PROVIDER=openai")
        return self.settings.openai_api_key

    def _base_url(self) -> str | None:
        provider = self.settings.llm_provider.lower()
        if provider == "openrouter":
            return self.settings.openrouter_base_url
        return self.settings.openai_base_url

    def _default_headers(self) -> dict[str, str] | None:
        if self.settings.llm_provider.lower() != "openrouter":
            return None
        headers: dict[str, str] = {}
        if self.settings.openrouter_referer:
            headers["HTTP-Referer"] = self.settings.openrouter_referer
        if self.settings.openrouter_title:
            headers["X-Title"] = self.settings.openrouter_title
        return headers or None

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
