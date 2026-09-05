from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from zoneinfo import ZoneInfo

from trade_research.market_data.contracts import CandleInterval, MarketCandle, ProviderRequest

_NSE_TIMEZONE = ZoneInfo("Asia/Kolkata")
_NSE_OPEN = time(9, 15)
_NSE_CLOSE = time(15, 30)


class ValidationSeverity(StrEnum):
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True)
class CandleValidationIssue:
    code: str
    severity: ValidationSeverity
    instrument_id: str
    session_date: date
    timestamp: datetime | None
    message: str


@dataclass(frozen=True)
class CandleValidationResult:
    accepted: tuple[MarketCandle, ...]
    rejected: tuple[MarketCandle, ...]
    issues: tuple[CandleValidationIssue, ...]

    @property
    def valid(self) -> bool:
        return not any(issue.severity is ValidationSeverity.ERROR for issue in self.issues)

    @property
    def duplicate_count(self) -> int:
        return sum(issue.code == "duplicate" for issue in self.issues)


class MarketDataValidationError(RuntimeError):
    def __init__(self, result: CandleValidationResult) -> None:
        self.result = result
        codes = sorted(
            {issue.code for issue in result.issues if issue.severity is ValidationSeverity.ERROR}
        )
        super().__init__("market-data validation failed: " + ", ".join(codes))


def validate_candle_batch(
    request: ProviderRequest,
    candles: list[MarketCandle] | tuple[MarketCandle, ...],
    *,
    eligible_sessions: set[date] | None = None,
    observed_at: datetime | None = None,
    maximum_provider_lag: timedelta | None = None,
) -> CandleValidationResult:
    """Validate and deterministically deduplicate a provider candle batch.

    Exact duplicate identities retain the first row and produce a warning. Any
    contradictory duplicate or invalid candle is rejected and produces an
    error, allowing ingestion to fail closed before a canonical write.
    """

    now = (observed_at or datetime.now(UTC)).astimezone(UTC)
    accepted: list[MarketCandle] = []
    rejected: list[MarketCandle] = []
    issues: list[CandleValidationIssue] = []
    seen: dict[tuple[str, CandleInterval, date, datetime | None], MarketCandle] = {}

    for candle in candles:
        row_issues = _validate_candle(
            request,
            candle,
            eligible_sessions=eligible_sessions,
            observed_at=now,
            maximum_provider_lag=maximum_provider_lag,
        )
        previous = seen.get(candle.identity)
        if previous is not None:
            is_exact = previous.canonical_payload() == candle.canonical_payload()
            row_issues.append(
                _issue(
                    candle,
                    code="duplicate" if is_exact else "conflicting_duplicate",
                    severity=(ValidationSeverity.WARNING if is_exact else ValidationSeverity.ERROR),
                    message=(
                        "Exact duplicate candle was removed."
                        if is_exact
                        else "Duplicate candle identity contains conflicting values."
                    ),
                )
            )
            rejected.append(candle)
            issues.extend(row_issues)
            continue
        seen[candle.identity] = candle
        issues.extend(row_issues)
        if any(issue.severity is ValidationSeverity.ERROR for issue in row_issues):
            rejected.append(candle)
        else:
            accepted.append(candle)

    accepted.sort(key=_sort_key)
    rejected.sort(key=_sort_key)
    return CandleValidationResult(tuple(accepted), tuple(rejected), tuple(issues))


def _validate_candle(
    request: ProviderRequest,
    candle: MarketCandle,
    *,
    eligible_sessions: set[date] | None,
    observed_at: datetime,
    maximum_provider_lag: timedelta | None,
) -> list[CandleValidationIssue]:
    issues: list[CandleValidationIssue] = []
    if (
        candle.provider.lower() != request.provider.lower()
        or candle.exchange.upper() != request.exchange.upper()
        or candle.interval is not request.interval
        or candle.request_id != request.request_id
        or candle.adapter_version != request.adapter_version
    ):
        issues.append(
            _issue(
                candle,
                "request_mismatch",
                ValidationSeverity.ERROR,
                "Candle identity does not match its provider request.",
            )
        )
    if candle.provider_symbol not in request.provider_symbols:
        issues.append(
            _issue(
                candle,
                "unexpected_symbol",
                ValidationSeverity.ERROR,
                "Candle symbol was not included in the provider request.",
            )
        )
    if candle.interval.is_intraday:
        if candle.timestamp is not None and not (
            request.window_start <= candle.timestamp.astimezone(UTC) < request.window_end
        ):
            issues.append(
                _issue(
                    candle,
                    "outside_request_window",
                    ValidationSeverity.ERROR,
                    "Intraday candle timestamp is outside the requested window.",
                )
            )
    elif not (
        request.window_start.date() <= candle.session_date < request.window_end.date()
    ):
        issues.append(
            _issue(
                candle,
                "outside_request_window",
                ValidationSeverity.ERROR,
                "Daily candle session is outside the requested window.",
            )
        )
    if any(value <= 0 for value in (candle.open, candle.high, candle.low, candle.close)):
        issues.append(
            _issue(
                candle,
                "non_positive_price",
                ValidationSeverity.ERROR,
                "OHLC values must be positive.",
            )
        )
    if candle.high < max(candle.open, candle.low, candle.close) or candle.low > min(
        candle.open, candle.high, candle.close
    ):
        issues.append(
            _issue(
                candle,
                "invalid_ohlc_range",
                ValidationSeverity.ERROR,
                "High/low values do not contain the open and close.",
            )
        )
    if candle.volume < 0:
        issues.append(
            _issue(
                candle,
                "negative_volume",
                ValidationSeverity.ERROR,
                "Volume must be non-negative.",
            )
        )
    if eligible_sessions is not None and candle.session_date not in eligible_sessions:
        issues.append(
            _issue(
                candle,
                "ineligible_session",
                ValidationSeverity.ERROR,
                "Candle is not part of the completed-session eligibility set.",
            )
        )
    if candle.provider_timestamp.astimezone(UTC) > observed_at + timedelta(minutes=1):
        issues.append(
            _issue(
                candle,
                "future_provider_timestamp",
                ValidationSeverity.ERROR,
                "Provider timestamp is in the future.",
            )
        )
    if maximum_provider_lag is not None and (
        observed_at - candle.provider_timestamp.astimezone(UTC) > maximum_provider_lag
    ):
        issues.append(
            _issue(
                candle,
                "stale_provider_timestamp",
                ValidationSeverity.WARNING,
                "Provider response exceeded the configured freshness lag.",
            )
        )
    if candle.interval.is_intraday:
        issues.extend(_validate_intraday_session(candle))
    return issues


def _validate_intraday_session(candle: MarketCandle) -> list[CandleValidationIssue]:
    if candle.timestamp is None:  # Guarded by MarketCandle; retained for type narrowing.
        return []
    if candle.exchange.upper() != "NSE":
        return []
    local_timestamp = candle.timestamp.astimezone(_NSE_TIMEZONE)
    local_time = local_timestamp.time().replace(tzinfo=None)
    issues: list[CandleValidationIssue] = []
    if local_timestamp.date() != candle.session_date:
        issues.append(
            _issue(
                candle,
                "session_date_mismatch",
                ValidationSeverity.ERROR,
                "Intraday timestamp does not resolve to the declared NSE session date.",
            )
        )
    if not (_NSE_OPEN <= local_time < _NSE_CLOSE):
        issues.append(
            _issue(
                candle,
                "outside_exchange_session",
                ValidationSeverity.ERROR,
                "Intraday candle is outside the NSE regular session.",
            )
        )
    if candle.timestamp.second or candle.timestamp.microsecond:
        issues.append(
            _issue(
                candle,
                "unaligned_minute",
                ValidationSeverity.ERROR,
                "Intraday candle timestamp must align to a whole minute.",
            )
        )
    return issues


def _issue(
    candle: MarketCandle,
    code: str,
    severity: ValidationSeverity,
    message: str,
) -> CandleValidationIssue:
    return CandleValidationIssue(
        code=code,
        severity=severity,
        instrument_id=candle.instrument_id,
        session_date=candle.session_date,
        timestamp=candle.timestamp,
        message=message,
    )


def _sort_key(candle: MarketCandle) -> tuple[str, date, datetime]:
    return (
        candle.instrument_id,
        candle.session_date,
        candle.timestamp or datetime.combine(candle.session_date, time.min, tzinfo=UTC),
    )
