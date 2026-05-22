from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from trade_research.chat.policy import ChatPolicy
from trade_research.chat.quality import evaluate_quality_badge
from trade_research.chat.tools import ChatToolGateway
from trade_research.config import Settings
from trade_research.schemas import (
    ChatAnswer,
    ChatQueryRequest,
    ChatQueryResponse,
    Citation,
    FreshnessInfo,
    PlannerPlan,
    ToolCallSpec,
)


class ChatOrchestrator:
    """Policy-gated orchestrator with bounded planning and grounded responses."""

    def __init__(
        self,
        settings: Settings,
        tools: ChatToolGateway,
        llm_client: object | None = None,
        policy: ChatPolicy | None = None,
    ) -> None:
        self.settings = settings
        self.tools = tools
        self.llm_client = llm_client
        self.policy = policy or ChatPolicy()
        self._sources: dict[str, dict] = {}
        self._audit: dict[str, dict] = {}
        self._logger = logging.getLogger(__name__)

    def handle_query(self, request: ChatQueryRequest) -> ChatQueryResponse:
        decision = self.policy.evaluate(request)
        if not decision.allowed:
            response = self._refusal_response(request, decision.reason or "Request not allowed.")
            self._record_audit(request, None, {"timescale": [], "qdrant": []}, response)
            return response

        plan = self._plan(request)
        tool_outputs = self._execute_plan(request, plan)
        response = self._compose_response(request, plan, tool_outputs)
        self._sources[response.response_id] = tool_outputs
        self._record_audit(request, plan, tool_outputs, response)
        return response

    def get_sources(self, response_id: str) -> dict:
        payload = self._sources.get(response_id, {})
        return {
            "timescale": list(payload.get("timescale", [])),
            "qdrant": list(payload.get("qdrant", [])),
        }

    def get_audit(self, response_id: str) -> dict:
        return self._audit.get(response_id, {})

    def _plan(self, request: ChatQueryRequest) -> PlannerPlan:
        text = request.message.lower()
        exchange = _resolve_exchange_scope(request.context.exchange)
        tool_calls: list[ToolCallSpec] = []

        if any(
            token in text
            for token in (
                "who are you",
                "what are you",
                "what can you do",
                "help",
                "capabilities",
                "how should i ask",
            )
        ):
            return PlannerPlan(
                intent="smalltalk_or_identity",
                requires_market_data=False,
                requires_research=False,
                tool_calls=[],
            )

        if any(token in text for token in ("quality", "missing", "stale", "coverage")):
            return PlannerPlan(
                intent="data_quality_check",
                requires_market_data=True,
                requires_research=False,
                tool_calls=[
                    ToolCallSpec(
                        tool="market_data.get_data_quality",
                        arguments={"exchange": exchange},
                    )
                ],
            )

        if any(token in text for token in ("note", "research", "why", "memo", "filing", "news")):
            tool_calls.append(
                ToolCallSpec(
                    tool="research.search_docs",
                    arguments={
                        "query": request.message,
                        "exchange": exchange,
                        "symbols": request.context.symbols,
                    },
                )
            )
            if any(token in text for token in ("price", "session", "perform", "return", "move")):
                tool_calls.append(
                    ToolCallSpec(
                        tool="market_data.get_session_summary",
                        arguments={"exchange": exchange},
                    )
                )
                tool_calls.append(
                    ToolCallSpec(
                        tool="market_data.get_data_quality",
                        arguments={"exchange": exchange},
                    )
                )
                return PlannerPlan(
                    intent="hybrid_explain",
                    requires_market_data=True,
                    requires_research=True,
                    tool_calls=tool_calls,
                )
            return PlannerPlan(
                intent="research_lookup",
                requires_market_data=False,
                requires_research=True,
                tool_calls=tool_calls,
            )

        if request.context.symbols:
            symbol = request.context.symbols[0]
            end_time = datetime.now(UTC)
            start_time = end_time - timedelta(hours=min(24, self.settings.chat_max_lookback_hours))
            return PlannerPlan(
                intent="price_lookup",
                requires_market_data=True,
                requires_research=False,
                tool_calls=[
                    ToolCallSpec(
                        tool="market_data.get_symbol_timeseries",
                        arguments={
                            "exchange": exchange,
                            "symbol": symbol,
                            "start_time": start_time,
                            "end_time": end_time,
                            "interval": "1h",
                        },
                    ),
                    ToolCallSpec(
                        tool="market_data.get_data_quality",
                        arguments={"exchange": exchange},
                    ),
                ],
            )

        return PlannerPlan(
            intent="session_summary",
            requires_market_data=True,
            requires_research=False,
            tool_calls=[
                ToolCallSpec(
                    tool="market_data.get_session_summary",
                    arguments={"exchange": exchange},
                ),
                ToolCallSpec(
                    tool="market_data.get_data_quality",
                    arguments={"exchange": exchange},
                ),
            ],
        )

    def _execute_plan(self, request: ChatQueryRequest, plan: PlannerPlan) -> dict:
        outputs = {"timescale": [], "qdrant": [], "errors": []}
        for call in plan.tool_calls:
            try:
                if call.tool == "market_data.get_session_summary":
                    summaries: dict[str, dict] = outputs.setdefault("market_session_summary", {})
                    for exchange in _expand_market_exchanges(call.arguments.get("exchange")):
                        result = self.tools.get_session_summary(exchange=exchange)
                        outputs["timescale"].append(result["provenance"])
                        summaries[exchange] = result["data"]
                elif call.tool == "market_data.get_data_quality":
                    quality_by_exchange: dict[str, dict] = outputs.setdefault(
                        "market_data_quality", {}
                    )
                    for exchange in _expand_market_exchanges(call.arguments.get("exchange")):
                        result = self.tools.get_data_quality(exchange=exchange)
                        outputs["timescale"].append(result["provenance"])
                        quality_by_exchange[exchange] = result["data"]
                elif call.tool == "market_data.get_symbol_timeseries":
                    series_by_exchange: dict[str, list[dict]] = outputs.setdefault(
                        "market_symbol_timeseries", {}
                    )
                    arguments = dict(call.arguments)
                    for exchange in _expand_market_exchanges(arguments.get("exchange")):
                        arguments["exchange"] = exchange
                        result = self.tools.get_symbol_timeseries(**arguments)
                        outputs["timescale"].append(result["provenance"])
                        series_by_exchange[exchange] = result["data"]
                elif call.tool == "research.search_docs":
                    result = self.tools.search_research_docs(**call.arguments)
                    outputs["qdrant"].extend(result["provenance"])
                    outputs["research_docs"] = result["data"]
            except Exception as exc:  # noqa: BLE001
                is_research = call.tool.startswith("research.")
                outputs["errors"].append({"tool": call.tool, "message": str(exc)})
                self._logger.warning("Chat tool failure: %s: %s", call.tool, exc)
                if not is_research:
                    raise RuntimeError(f"Market data dependency failed: {call.tool}") from exc
        return outputs

    def _compose_response(
        self,
        request: ChatQueryRequest,
        plan: PlannerPlan,
        outputs: dict,
    ) -> ChatQueryResponse:
        quality_payload = outputs.get("market_data_quality") or {}
        if plan.intent == "smalltalk_or_identity":
            badge, warnings, market_freshness = "complete", [], None
        else:
            badge, warnings, market_freshness = _evaluate_quality_bundle(
                self.settings,
                quality_payload,
            )

        answer_text = _render_answer_text(plan.intent, outputs)
        tool_errors = outputs.get("errors", [])
        if tool_errors:
            for error in tool_errors:
                if str(error.get("tool", "")).startswith("research."):
                    warnings.append(
                        "Research retrieval is temporarily unavailable; answer uses market data."
                    )
                else:
                    warnings.append("Some dependencies failed during retrieval.")
        citations = _build_citations(outputs)
        if (
            plan.intent != "smalltalk_or_identity"
            and self.settings.chat_strict_citation_required
            and not citations
        ):
            answer_text = (
                "I could not produce a cited answer from currently available sources. "
                "Please retry in a moment."
            )
            warnings.append("No usable citations were available.")
        elif self.settings.chat_use_llm_answer and self.llm_client is not None:
            generation = self.llm_client.generate_answer(
                question=request.message,
                deterministic_answer=answer_text,
                warnings=warnings,
                citations=[f"{item.type}: {item.label}" for item in citations],
            )
            outputs["llm_answer"] = generation.telemetry
            if generation.text:
                answer_text = generation.text

        freshness = FreshnessInfo(
            market_data_as_of=market_freshness,
            research_data_as_of=_latest_research_ts(outputs.get("research_docs", [])),
        )
        response_id = f"resp_{uuid4().hex[:12]}"
        return ChatQueryResponse(
            response_id=response_id,
            session_id=request.session_id,
            answer=ChatAnswer(
                text=answer_text,
                quality_badge=badge,
                freshness=freshness,
                warnings=warnings,
                follow_ups=_follow_ups(plan.intent),
            ),
            citations=citations,
            trace_id=f"trace_{uuid4().hex[:12]}",
        )

    def _record_audit(
        self,
        request: ChatQueryRequest,
        plan: PlannerPlan | None,
        outputs: dict,
        response: ChatQueryResponse,
    ) -> None:
        self._audit[response.response_id] = {
            "recorded_at": datetime.now(UTC).isoformat(),
            "request": request.model_dump(mode="json"),
            "plan": plan.model_dump(mode="json") if plan else None,
            "tool_outputs": outputs,
            "response": response.model_dump(mode="json"),
        }

    def _refusal_response(self, request: ChatQueryRequest, reason: str) -> ChatQueryResponse:
        response_id = f"resp_{uuid4().hex[:12]}"
        return ChatQueryResponse(
            response_id=response_id,
            session_id=request.session_id,
            answer=ChatAnswer(
                text=reason,
                quality_badge="partial",
                freshness=FreshnessInfo(),
                warnings=["Request refused by policy guardrails."],
                follow_ups=[
                    "Ask for a read-only market summary by exchange.",
                    "Ask for cited research context by symbol or sector.",
                ],
            ),
            citations=[],
            trace_id=f"trace_{uuid4().hex[:12]}",
        )


def _resolve_exchange_scope(exchange: str) -> str:
    normalized = (exchange or "BOTH").upper()
    if normalized in {"NSE", "TSX", "BOTH"}:
        return normalized
    return "BOTH"


def _render_answer_text(intent: str, outputs: dict) -> str:
    if intent == "smalltalk_or_identity":
        return (
            "I am Lens, your Trade Analyst Assistant. I can answer exchange-scoped questions "
            "using TimescaleDB market data and Qdrant research context with provenance. "
            "I support NSE, TSX, and BOTH scope, and I report quality/freshness warnings "
            "when coverage is partial or stale."
        )

    if intent == "data_quality_check":
        quality_map = outputs.get("market_data_quality") or {}
        if not quality_map:
            return "No quality snapshot data is currently available."
        lines = []
        for exchange, quality in quality_map.items():
            lines.append(
                f"{exchange}: active={quality.get('active_symbols', 0)}, "
                f"latest-candle symbols={quality.get('latest_candle_symbols', 0)}, "
                f"completeness={quality.get('completeness_ratio', 0.0):.2%}, "
                f"open backlog windows={quality.get('open_backlog_windows', 0)}"
            )
        return (
            "Latest quality snapshot: " + "; ".join(lines) + "."
        )

    if intent == "research_lookup":
        docs = outputs.get("research_docs") or []
        if not docs:
            return "No matching research documents were found for this query."
        top = docs[0]
        return (
            f"Top research match: {top.get('title') or top.get('id')} "
            f"(score {top.get('score', 0.0):.2f})."
        )

    if intent == "price_lookup":
        series_map = outputs.get("market_symbol_timeseries") or {}
        rows = _first_non_empty_series(series_map.values())
        if not rows:
            return "No hourly candles found for the requested symbol and window."
        first = rows[0]
        last = rows[-1]
        change = 0.0
        if first["close"] > 0:
            change = ((last["close"] - first["close"]) / first["close"]) * 100.0
        return (
            f"Hourly move over requested window: {change:.2f}% "
            f"(from {first['close']:.2f} to {last['close']:.2f})."
        )

    summary_map = outputs.get("market_session_summary") or {}
    if not summary_map:
        return "No session summary data is currently available for this request."
    parts = []
    for exchange, summary in summary_map.items():
        parts.append(
            f"{exchange}: symbols={summary.get('symbol_count', 0)}, "
            f"advancers={summary.get('advancers', 0)}, "
            f"decliners={summary.get('decliners', 0)}, "
            f"avg return={float(summary.get('avg_return_pct') or 0.0):.2f}%"
        )
    return "Latest session summary: " + "; ".join(parts) + "."


def _build_citations(outputs: dict) -> list[Citation]:
    citations: list[Citation] = []
    index = 1
    for row in outputs.get("timescale", []):
        provenance_ref = row.get("provenance_ref")
        if not provenance_ref:
            continue
        citations.append(
            Citation(
                id=f"c{index}",
                type="timescale_query",
                label=str(row.get("template_id", "Timescale query")),
                provenance_ref=str(provenance_ref),
            )
        )
        index += 1
    for row in outputs.get("qdrant", []):
        provenance_ref = row.get("provenance_ref")
        if not provenance_ref:
            continue
        citations.append(
            Citation(
                id=f"c{index}",
                type="qdrant_chunk",
                label=str(row.get("title") or row.get("doc_id") or "Research document"),
                provenance_ref=str(provenance_ref),
            )
        )
        index += 1
    return citations


def _latest_research_ts(rows: list[dict]) -> datetime | None:
    latest: datetime | None = None
    for row in rows:
        value = row.get("published_at")
        if not isinstance(value, str) or not value:
            continue
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        if latest is None or parsed > latest:
            latest = parsed
    return latest


def _expand_market_exchanges(exchange: object) -> list[str]:
    normalized = str(exchange or "BOTH").upper()
    if normalized == "BOTH":
        return ["NSE", "TSX"]
    if normalized in {"NSE", "TSX"}:
        return [normalized]
    return ["NSE", "TSX"]


def _first_non_empty_series(series_collection: Iterable[list[dict]]) -> list[dict]:
    for series in series_collection:
        if series:
            return series
    return []


def _evaluate_quality_bundle(
    settings: Settings,
    quality_payload: dict,
) -> tuple[str, list[str], datetime | None]:
    if not quality_payload:
        return "stale", ["No market data quality payload was available."], None

    badges: list[str] = []
    warnings: list[str] = []
    freshness_values: list[datetime] = []
    for exchange, payload in quality_payload.items():
        latest_candle_ts = payload.get("latest_candle_ts")
        if isinstance(latest_candle_ts, datetime):
            freshness_values.append(latest_candle_ts)
        badge, exchange_warnings = evaluate_quality_badge(
            settings,
            exchange=exchange,
            active_symbols=int(payload.get("active_symbols") or 0),
            latest_candle_symbols=int(payload.get("latest_candle_symbols") or 0),
            latest_candle_ts=latest_candle_ts,
            latest_expected_candle_ts=payload.get("latest_expected_candle_ts"),
        )
        badges.append(badge)
        warnings.extend(exchange_warnings)

    overall_badge = "complete"
    if "stale" in badges:
        overall_badge = "stale"
    elif "partial" in badges:
        overall_badge = "partial"

    freshness = min(freshness_values) if freshness_values else None
    return overall_badge, warnings, freshness


def _follow_ups(intent: str) -> list[str]:
    if intent == "smalltalk_or_identity":
        return [
            "How did NSE perform in the latest complete session?",
            "Compare BOTH exchanges quality snapshot.",
            "Why did NSE move today?",
        ]
    if intent == "data_quality_check":
        return [
            "Show missing windows by hour for this exchange.",
            "Compare completeness versus the previous complete session.",
        ]
    if intent == "research_lookup":
        return [
            "Filter research to a specific symbol.",
            "Combine this research with latest session breadth.",
        ]
    return [
        "Break down top and bottom movers.",
        "Add research context for the strongest movers.",
    ]
