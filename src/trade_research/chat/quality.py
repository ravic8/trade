from __future__ import annotations

from datetime import UTC, datetime, timedelta

from trade_research.config import Settings
from trade_research.schemas import QualityBadge


def evaluate_quality_badge(
    settings: Settings,
    exchange: str,
    active_symbols: int,
    latest_candle_symbols: int,
    latest_candle_ts: datetime | None,
) -> tuple[QualityBadge, list[str]]:
    warnings: list[str] = []
    ratio = 0.0
    if active_symbols > 0:
        ratio = latest_candle_symbols / active_symbols

    threshold = (
        settings.chat_quality_nse_complete_threshold
        if exchange.upper() == "NSE"
        else settings.chat_quality_tsx_complete_threshold
    )
    badge: QualityBadge = "complete" if ratio >= threshold else "partial"
    if badge == "partial":
        warnings.append(
            f"Coverage is partial: {latest_candle_symbols}/{active_symbols} symbols "
            f"for {exchange.upper()} latest session."
        )

    if _is_stale(settings, latest_candle_ts):
        badge = "stale"
        warnings.append("Latest candle appears stale relative to expected hourly cadence.")
    return badge, warnings


def _is_stale(settings: Settings, latest_candle_ts: datetime | None) -> bool:
    if latest_candle_ts is None:
        return True
    candle_ts = latest_candle_ts if latest_candle_ts.tzinfo else latest_candle_ts.replace(tzinfo=UTC)
    stale_after = datetime.now(UTC) - timedelta(hours=settings.chat_stale_intervals_threshold)
    return candle_ts < stale_after
