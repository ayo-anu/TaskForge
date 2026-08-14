"""Pure retry-policy resolution, exhaustion, and backoff tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from taskforge.retries.domain import (
    MAX_CONFIGURED_RETRY_ATTEMPTS,
    MAX_CONFIGURED_RETRY_DELAY_SECONDS,
    MAX_PERSISTED_ATTEMPT_NUMBER,
    InvalidPersistedRetryPolicy,
    JSONMapping,
    JSONValue,
    RetryCalculationError,
    RetryDecisionKind,
    RetryPolicy,
    decide_retry,
    parse_persisted_retry_policy,
    resolve_persisted_retry_policy,
    validate_retry_policy_configuration,
)


def policy_json(**overrides: JSONValue) -> dict[str, JSONValue]:
    value: dict[str, JSONValue] = {
        "maximum_attempts": 4,
        "initial_delay_seconds": 10,
        "multiplier": 2,
        "maximum_delay_seconds": 300,
    }
    value.update(overrides)
    return value


def policy(**overrides: JSONValue) -> RetryPolicy:
    return parse_persisted_retry_policy(policy_json(**overrides))


COMPLETED_AT = datetime(2026, 8, 14, 12, 30, 0, 123456, tzinfo=UTC)


def test_configuration_limits_are_explicit_and_inclusive() -> None:
    configured, issues = validate_retry_policy_configuration(
        policy_json(
            maximum_attempts=MAX_CONFIGURED_RETRY_ATTEMPTS,
            initial_delay_seconds=MAX_CONFIGURED_RETRY_DELAY_SECONDS,
            maximum_delay_seconds=MAX_CONFIGURED_RETRY_DELAY_SECONDS,
            multiplier=1.5,
        )
    )
    assert issues == ()
    assert configured is not None
    assert configured.multiplier == Decimal("1.5")


@pytest.mark.parametrize(
    ("overrides", "code"),
    (
        (
            {"maximum_attempts": MAX_CONFIGURED_RETRY_ATTEMPTS + 1},
            "invalid_retry_maximum_attempts",
        ),
        (
            {"initial_delay_seconds": MAX_CONFIGURED_RETRY_DELAY_SECONDS + 1},
            "invalid_retry_initial_delay_seconds",
        ),
        (
            {"maximum_delay_seconds": MAX_CONFIGURED_RETRY_DELAY_SECONDS + 1},
            "invalid_retry_maximum_delay_seconds",
        ),
    ),
)
def test_configuration_rejects_values_above_authoring_limits(
    overrides: dict[str, JSONValue], code: str
) -> None:
    _, issues = validate_retry_policy_configuration(policy_json(**overrides))
    assert code in [issue.code for issue in issues]


def test_persisted_parser_does_not_retroactively_apply_authoring_limits() -> None:
    persisted = parse_persisted_retry_policy(
        policy_json(
            maximum_attempts=MAX_CONFIGURED_RETRY_ATTEMPTS + 1,
            initial_delay_seconds=MAX_CONFIGURED_RETRY_DELAY_SECONDS + 1,
            maximum_delay_seconds=MAX_CONFIGURED_RETRY_DELAY_SECONDS + 1,
        )
    )
    assert persisted.maximum_attempts == MAX_CONFIGURED_RETRY_ATTEMPTS + 1
    assert persisted.maximum_delay_seconds == MAX_CONFIGURED_RETRY_DELAY_SECONDS + 1


def test_persisted_parser_enforces_postgresql_attempt_number_limit() -> None:
    assert (
        parse_persisted_retry_policy(
            policy_json(maximum_attempts=MAX_PERSISTED_ATTEMPT_NUMBER)
        ).maximum_attempts
        == MAX_PERSISTED_ATTEMPT_NUMBER
    )
    with pytest.raises(InvalidPersistedRetryPolicy) as error:
        parse_persisted_retry_policy(
            policy_json(maximum_attempts=MAX_PERSISTED_ATTEMPT_NUMBER + 1)
        )
    assert [issue.code for issue in error.value.issues] == [
        "invalid_retry_maximum_attempts"
    ]


@pytest.mark.parametrize(
    "value",
    (
        [],
        policy_json(maximum_attempts=0),
        policy_json(initial_delay_seconds=-1),
        policy_json(multiplier=True),
        policy_json(multiplier=float("inf")),
        policy_json(multiplier=float("-inf")),
        policy_json(multiplier=float("nan")),
        policy_json(maximum_delay_seconds=-1),
        policy_json(initial_delay_seconds=11, maximum_delay_seconds=10),
        {**policy_json(), "jitter": True},
    ),
)
def test_persisted_parser_fails_closed_for_malformed_policy(value: object) -> None:
    with pytest.raises(InvalidPersistedRetryPolicy):
        parse_persisted_retry_policy(value)  # type: ignore[arg-type]


def test_resolution_uses_atomic_step_over_workflow_precedence() -> None:
    workflow: JSONMapping = {"retry_policy": policy_json(maximum_attempts=5)}
    step: JSONMapping = {"retry_policy": policy_json(maximum_attempts=2)}
    assert resolve_persisted_retry_policy(workflow, step) == policy(maximum_attempts=2)
    assert resolve_persisted_retry_policy(workflow, {"future": True}) == policy(
        maximum_attempts=5
    )
    assert resolve_persisted_retry_policy(None, step) == policy(maximum_attempts=2)
    assert resolve_persisted_retry_policy(None, None) is None


def test_malformed_present_step_policy_never_falls_back_to_workflow() -> None:
    with pytest.raises(InvalidPersistedRetryPolicy):
        resolve_persisted_retry_policy(
            {"retry_policy": policy_json()},
            {"retry_policy": {"maximum_attempts": 2}},
        )


def test_no_policy_and_explicit_zero_retries_are_distinct() -> None:
    no_policy = decide_retry(
        policy=None, failed_attempt_number=1, completed_at=COMPLETED_AT
    )
    exhausted = decide_retry(
        policy=policy(maximum_attempts=1),
        failed_attempt_number=1,
        completed_at=COMPLETED_AT,
    )
    assert no_policy.kind is RetryDecisionKind.NO_POLICY
    assert exhausted.kind is RetryDecisionKind.EXHAUSTED
    assert no_policy.next_eligible_at is exhausted.next_eligible_at is None


@pytest.mark.parametrize(
    ("attempt_number", "delay"),
    ((1, 10), (2, 20), (3, 40), (4, 80)),
)
def test_attempt_n_uses_exponent_n_minus_one(attempt_number: int, delay: int) -> None:
    decision = decide_retry(
        policy=policy(maximum_attempts=5),
        failed_attempt_number=attempt_number,
        completed_at=COMPLETED_AT,
    )
    assert decision.kind is RetryDecisionKind.RETRY_ALLOWED
    assert decision.next_attempt_number == attempt_number + 1
    assert decision.delay_seconds == delay
    assert decision.next_eligible_at == COMPLETED_AT + timedelta(seconds=delay)


def test_fractional_backoff_uses_ceiling_and_preserves_timestamp_precision() -> None:
    decision = decide_retry(
        policy=policy(initial_delay_seconds=3, multiplier=1.5),
        failed_attempt_number=2,
        completed_at=COMPLETED_AT,
    )
    assert decision.delay_seconds == 5
    assert decision.next_eligible_at == datetime(
        2026, 8, 14, 12, 30, 5, 123456, tzinfo=UTC
    )


@pytest.mark.parametrize(
    ("initial_delay", "multiplier", "attempt_number", "cap"),
    (
        (3, 1.125, 6, 300),
        (7, 1.01, 10, 300),
        (1, 1.5, 7, 300),
        (9, 1.0001, 16, 300),
    ),
)
def test_optimized_backoff_matches_decimal_reference_for_small_exponents(
    initial_delay: int,
    multiplier: float,
    attempt_number: int,
    cap: int,
) -> None:
    expected_raw = min(
        Decimal(cap),
        Decimal(initial_delay) * Decimal(str(multiplier)) ** (attempt_number - 1),
    )
    expected = int(expected_raw.to_integral_value(rounding="ROUND_CEILING"))

    decision = decide_retry(
        policy=policy(
            maximum_attempts=attempt_number + 1,
            initial_delay_seconds=initial_delay,
            multiplier=multiplier,
            maximum_delay_seconds=cap,
        ),
        failed_attempt_number=attempt_number,
        completed_at=COMPLETED_AT,
    )

    assert decision.delay_seconds == expected


def test_backoff_saturates_during_accumulator_multiplication() -> None:
    decision = decide_retry(
        policy=policy(
            maximum_attempts=7,
            initial_delay_seconds=3,
            multiplier=2,
            maximum_delay_seconds=100,
        ),
        failed_attempt_number=6,
        completed_at=COMPLETED_AT,
    )
    assert decision.delay_seconds == 96

    capped = decide_retry(
        policy=policy(
            maximum_attempts=8,
            initial_delay_seconds=3,
            multiplier=2,
            maximum_delay_seconds=100,
        ),
        failed_attempt_number=7,
        completed_at=COMPLETED_AT,
    )
    assert capped.delay_seconds == 100


def test_backoff_saturates_squared_factor_before_it_is_consumed() -> None:
    decision = decide_retry(
        policy=policy(
            maximum_attempts=12,
            initial_delay_seconds=1,
            multiplier=10,
            maximum_delay_seconds=99,
        ),
        failed_attempt_number=11,
        completed_at=COMPLETED_AT,
    )
    assert decision.delay_seconds == 99


@pytest.mark.parametrize(
    ("multiplier", "expected"),
    ((1.979, 99), (2.001, 100)),
)
def test_fractional_backoff_boundary_around_cap(
    multiplier: float, expected: int
) -> None:
    decision = decide_retry(
        policy=policy(
            initial_delay_seconds=50,
            multiplier=multiplier,
            maximum_delay_seconds=100,
        ),
        failed_attempt_number=2,
        completed_at=COMPLETED_AT,
    )
    assert decision.delay_seconds == expected


def test_zero_delay_multiplier_one_and_cap_are_deterministic() -> None:
    zero = decide_retry(
        policy=policy(initial_delay_seconds=0, maximum_delay_seconds=0),
        failed_attempt_number=3,
        completed_at=COMPLETED_AT,
    )
    constant = decide_retry(
        policy=policy(multiplier=1),
        failed_attempt_number=3,
        completed_at=COMPLETED_AT,
    )
    capped = decide_retry(
        policy=policy(maximum_delay_seconds=25),
        failed_attempt_number=3,
        completed_at=COMPLETED_AT,
    )
    assert (zero.delay_seconds, zero.next_eligible_at) == (0, COMPLETED_AT)
    assert constant.delay_seconds == 10
    assert capped.delay_seconds == 25


def test_exhaustion_occurs_exactly_at_maximum_attempts() -> None:
    permitted = decide_retry(
        policy=policy(maximum_attempts=3),
        failed_attempt_number=2,
        completed_at=COMPLETED_AT,
    )
    exhausted = decide_retry(
        policy=policy(maximum_attempts=3),
        failed_attempt_number=3,
        completed_at=COMPLETED_AT,
    )
    beyond = decide_retry(
        policy=policy(maximum_attempts=3),
        failed_attempt_number=4,
        completed_at=COMPLETED_AT,
    )
    assert permitted.next_attempt_number == 3
    assert exhausted.kind is beyond.kind is RetryDecisionKind.EXHAUSTED


@pytest.mark.parametrize(
    "attempt_number", (True, 0, -1, MAX_PERSISTED_ATTEMPT_NUMBER + 1)
)
def test_decision_rejects_invalid_attempt_numbers(attempt_number: object) -> None:
    with pytest.raises(RetryCalculationError):
        decide_retry(
            policy=policy(),
            failed_attempt_number=attempt_number,  # type: ignore[arg-type]
            completed_at=COMPLETED_AT,
        )


def test_decision_rejects_naive_completion_timestamp() -> None:
    with pytest.raises(RetryCalculationError, match="timezone-aware"):
        decide_retry(
            policy=policy(),
            failed_attempt_number=1,
            completed_at=COMPLETED_AT.replace(tzinfo=None),
        )


def test_decision_reports_timestamp_overflow_stably() -> None:
    with pytest.raises(RetryCalculationError, match="cannot be represented"):
        decide_retry(
            policy=policy(initial_delay_seconds=1, maximum_delay_seconds=1),
            failed_attempt_number=1,
            completed_at=datetime.max.replace(tzinfo=UTC),
        )


def test_capped_calculation_handles_huge_attempt_exponent_without_float_overflow() -> (
    None
):
    decision = decide_retry(
        policy=policy(
            maximum_attempts=MAX_PERSISTED_ATTEMPT_NUMBER,
            multiplier=1.0000000000000002,
            maximum_delay_seconds=300,
        ),
        failed_attempt_number=MAX_PERSISTED_ATTEMPT_NUMBER - 1,
        completed_at=COMPLETED_AT,
    )
    assert decision.delay_seconds == 11
