from datetime import date

import pytest

from trade_research.validation.coverage import (
    DateWindow,
    evaluate_eligible_session_coverage,
)


def test_eligible_coverage_applies_lifecycle_grace_and_evidence_exclusions() -> None:
    sessions = tuple(
        date(2026, 1, day)
        for day in (1, 2, 5, 6, 7, 8)
    )

    coverage = evaluate_eligible_session_coverage(
        requested_start=date(2026, 1, 1),
        requested_end=date(2026, 1, 8),
        exchange_session_dates=sessions,
        provider_eligible_session_dates=sessions[:-1],
        stored_dates=(
            date(2026, 1, 2),
            date(2026, 1, 4),
            date(2026, 1, 5),
            date(2026, 1, 6),
        ),
        valid_stored_dates=(
            date(2026, 1, 2),
            date(2026, 1, 4),
            date(2026, 1, 6),
        ),
        listing_start=date(2026, 1, 2),
        evidence_exclusions={date(2026, 1, 6): "evidence_halted"},
        explained_unavailable_dates=(date(2026, 1, 7),),
    )

    assert coverage.expected_eligible_dates == (
        date(2026, 1, 2),
        date(2026, 1, 5),
        date(2026, 1, 7),
    )
    assert coverage.valid_stored_eligible_sessions == 1
    assert coverage.invalid_stored_eligible_dates == (date(2026, 1, 5),)
    assert coverage.missing_eligible_dates == (
        date(2026, 1, 5),
        date(2026, 1, 7),
    )
    assert coverage.explained_missing_dates == (date(2026, 1, 7),)
    assert coverage.actionable_missing_dates == (date(2026, 1, 5),)
    assert coverage.off_calendar_stored_dates == (date(2026, 1, 4),)
    assert coverage.exclusion_counts == {
        "before_listing": 1,
        "evidence_halted": 1,
        "provider_grace": 1,
    }
    assert coverage.coverage_ratio == 1 / 3


def test_point_in_time_membership_and_delisting_bound_the_denominator() -> None:
    coverage = evaluate_eligible_session_coverage(
        requested_start=date(2026, 1, 1),
        requested_end=date(2026, 1, 8),
        exchange_session_dates=tuple(
            date(2026, 1, day) for day in (1, 2, 5, 6, 7, 8)
        ),
        listing_end=date(2026, 1, 7),
        universe_membership_windows=(DateWindow(date(2026, 1, 5), date(2026, 1, 8)),),
    )

    assert coverage.expected_eligible_dates == (
        date(2026, 1, 5),
        date(2026, 1, 6),
        date(2026, 1, 7),
    )
    assert coverage.exclusion_counts == {
        "after_delisting": 1,
        "outside_universe_membership": 2,
    }


def test_provider_unavailability_explains_but_does_not_shrink_denominator() -> None:
    coverage = evaluate_eligible_session_coverage(
        requested_start=date(2026, 1, 5),
        requested_end=date(2026, 1, 6),
        exchange_session_dates=(date(2026, 1, 5), date(2026, 1, 6)),
        explained_unavailable_dates=(date(2026, 1, 5), date(2026, 1, 6)),
    )

    assert coverage.expected_eligible_sessions == 2
    assert coverage.explained_missing_sessions == 2
    assert coverage.actionable_missing_sessions == 0
    assert coverage.coverage_ratio == 0.0


def test_coverage_rejects_reversed_requested_or_membership_windows() -> None:
    with pytest.raises(ValueError, match="requested_start"):
        evaluate_eligible_session_coverage(
            requested_start=date(2026, 1, 2),
            requested_end=date(2026, 1, 1),
            exchange_session_dates=(),
        )
    with pytest.raises(ValueError, match="DateWindow"):
        DateWindow(date(2026, 1, 2), date(2026, 1, 1))
