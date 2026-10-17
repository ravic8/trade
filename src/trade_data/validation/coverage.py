from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Literal

CoverageExclusionReason = Literal[
    "before_listing",
    "after_delisting",
    "outside_universe_membership",
    "provider_grace",
    "evidence_halted",
    "evidence_suspended",
]

EvidenceExclusionReason = Literal["evidence_halted", "evidence_suspended"]


@dataclass(frozen=True)
class DateWindow:
    start: date
    end: date

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError("DateWindow start must be on or before end.")

    def contains(self, value: date) -> bool:
        return self.start <= value <= self.end


@dataclass(frozen=True)
class EligibleSessionCoverage:
    requested_start: date
    requested_end: date
    exchange_session_dates: tuple[date, ...]
    expected_eligible_dates: tuple[date, ...]
    valid_stored_eligible_dates: tuple[date, ...]
    invalid_stored_eligible_dates: tuple[date, ...]
    missing_eligible_dates: tuple[date, ...]
    explained_missing_dates: tuple[date, ...]
    actionable_missing_dates: tuple[date, ...]
    off_calendar_stored_dates: tuple[date, ...]
    exclusion_counts: Mapping[CoverageExclusionReason, int] = field(default_factory=dict)

    @property
    def requested_exchange_sessions(self) -> int:
        return len(self.exchange_session_dates)

    @property
    def expected_eligible_sessions(self) -> int:
        return len(self.expected_eligible_dates)

    @property
    def valid_stored_eligible_sessions(self) -> int:
        return len(self.valid_stored_eligible_dates)

    @property
    def invalid_stored_eligible_sessions(self) -> int:
        return len(self.invalid_stored_eligible_dates)

    @property
    def missing_eligible_sessions(self) -> int:
        return len(self.missing_eligible_dates)

    @property
    def explained_missing_sessions(self) -> int:
        return len(self.explained_missing_dates)

    @property
    def actionable_missing_sessions(self) -> int:
        return len(self.actionable_missing_dates)

    @property
    def coverage_ratio(self) -> float:
        denominator = self.expected_eligible_sessions
        return self.valid_stored_eligible_sessions / denominator if denominator else 0.0

    def as_evidence(self) -> dict[str, object]:
        return {
            "requested_exchange_sessions": self.requested_exchange_sessions,
            "expected_eligible_sessions": self.expected_eligible_sessions,
            "valid_stored_eligible_sessions": self.valid_stored_eligible_sessions,
            "invalid_stored_eligible_sessions": self.invalid_stored_eligible_sessions,
            "missing_eligible_sessions": self.missing_eligible_sessions,
            "explained_missing_sessions": self.explained_missing_sessions,
            "actionable_missing_sessions": self.actionable_missing_sessions,
            "off_calendar_stored_sessions": len(self.off_calendar_stored_dates),
            "coverage_ratio": self.coverage_ratio,
            "exclusion_counts": dict(self.exclusion_counts),
        }


def evaluate_eligible_session_coverage(
    *,
    requested_start: date,
    requested_end: date,
    exchange_session_dates: Sequence[date],
    provider_eligible_session_dates: Sequence[date] | None = None,
    stored_dates: Sequence[date] = (),
    valid_stored_dates: Sequence[date] | None = None,
    listing_start: date | None = None,
    listing_end: date | None = None,
    universe_membership_windows: Sequence[DateWindow] = (),
    evidence_exclusions: Mapping[date, EvidenceExclusionReason] | None = None,
    explained_unavailable_dates: Sequence[date] = (),
) -> EligibleSessionCoverage:
    """Evaluate valid stored sessions over an explicit eligibility denominator.

    Provider unavailability explains missingness but does not remove an otherwise
    eligible session from the denominator. Only lifecycle, point-in-time universe,
    completed provider grace, and evidence-backed halt/suspension rules can do so.
    """

    if requested_start > requested_end:
        raise ValueError("requested_start must be on or before requested_end.")
    sessions = tuple(
        sorted(
            {
                value
                for value in exchange_session_dates
                if requested_start <= value <= requested_end
            }
        )
    )
    provider_eligible = (
        set(sessions)
        if provider_eligible_session_dates is None
        else set(provider_eligible_session_dates)
    )
    exclusions = evidence_exclusions or {}
    exclusion_counts: Counter[CoverageExclusionReason] = Counter()
    eligible: list[date] = []
    for session in sessions:
        reason = _exclusion_reason(
            session,
            provider_eligible=provider_eligible,
            listing_start=listing_start,
            listing_end=listing_end,
            universe_membership_windows=universe_membership_windows,
            evidence_exclusions=exclusions,
        )
        if reason is None:
            eligible.append(session)
        else:
            exclusion_counts[reason] += 1

    stored = {
        value for value in stored_dates if requested_start <= value <= requested_end
    }
    valid = stored if valid_stored_dates is None else set(valid_stored_dates).intersection(stored)
    eligible_set = set(eligible)
    valid_eligible = eligible_set.intersection(valid)
    invalid_eligible = eligible_set.intersection(stored).difference(valid)
    missing = eligible_set.difference(valid_eligible)
    explained = missing.intersection(explained_unavailable_dates)
    actionable = missing.difference(explained)
    return EligibleSessionCoverage(
        requested_start=requested_start,
        requested_end=requested_end,
        exchange_session_dates=sessions,
        expected_eligible_dates=tuple(eligible),
        valid_stored_eligible_dates=tuple(sorted(valid_eligible)),
        invalid_stored_eligible_dates=tuple(sorted(invalid_eligible)),
        missing_eligible_dates=tuple(sorted(missing)),
        explained_missing_dates=tuple(sorted(explained)),
        actionable_missing_dates=tuple(sorted(actionable)),
        off_calendar_stored_dates=tuple(sorted(stored.difference(sessions))),
        exclusion_counts=dict(sorted(exclusion_counts.items())),
    )


def _exclusion_reason(
    session: date,
    *,
    provider_eligible: set[date],
    listing_start: date | None,
    listing_end: date | None,
    universe_membership_windows: Sequence[DateWindow],
    evidence_exclusions: Mapping[date, EvidenceExclusionReason],
) -> CoverageExclusionReason | None:
    if listing_start is not None and session < listing_start:
        return "before_listing"
    if listing_end is not None and session > listing_end:
        return "after_delisting"
    if universe_membership_windows and not any(
        window.contains(session) for window in universe_membership_windows
    ):
        return "outside_universe_membership"
    if session not in provider_eligible:
        return "provider_grace"
    return evidence_exclusions.get(session)
