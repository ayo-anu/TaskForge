"""Deterministic retry-policy resolution and capped backoff calculations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_CEILING, ROUND_HALF_EVEN, Decimal, localcontext
from enum import StrEnum
from math import isfinite
from uuid import UUID

from taskforge.worker.results import TaskExecutionFailureKind

type JSONValue = (
    bool | int | float | str | list[JSONValue] | dict[str, JSONValue] | None
)
type JSONMapping = dict[str, JSONValue]

MAX_CONFIGURED_RETRY_ATTEMPTS = 32
MAX_CONFIGURED_RETRY_DELAY_SECONDS = 31_536_000
MAX_PERSISTED_ATTEMPT_NUMBER = 2_147_483_647
MAX_TIMEDELTA_SECONDS = timedelta.max.days * 86_400 + timedelta.max.seconds

_RETRY_POLICY_FIELDS = frozenset(
    {
        "maximum_attempts",
        "initial_delay_seconds",
        "multiplier",
        "maximum_delay_seconds",
    }
)


@dataclass(frozen=True)
class RetryPolicy:
    maximum_attempts: int
    initial_delay_seconds: int
    multiplier: Decimal
    maximum_delay_seconds: int


@dataclass(frozen=True)
class RetryPolicyValidationIssue:
    code: str
    path: tuple[str, ...]
    message: str


class InvalidPersistedRetryPolicy(ValueError):
    """An immutable retry-policy snapshot cannot be interpreted safely."""

    def __init__(self, issues: tuple[RetryPolicyValidationIssue, ...]) -> None:
        self.issues = issues
        super().__init__("persisted retry policy is invalid")


class RetryCalculationError(ValueError):
    """A retry decision cannot be represented safely."""


class RetryDecisionKind(StrEnum):
    NO_POLICY = "no_policy"
    EXHAUSTED = "exhausted"
    RETRY_ALLOWED = "retry_allowed"


class RetryEventType(StrEnum):
    RETRY_SCHEDULED = "retry_scheduled"
    RETRY_DISPATCHED = "retry_dispatched"
    RETRY_NOT_SCHEDULED = "retry_not_scheduled"


class RetryNotScheduledReason(StrEnum):
    NO_POLICY = "no_policy"
    EXHAUSTED = "exhausted"


@dataclass(frozen=True)
class RetryEventCursor:
    task_run_id: UUID
    occurred_at: datetime
    event_id: UUID

    def __post_init__(self) -> None:
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("retry event cursor timestamp must be timezone-aware")
        object.__setattr__(self, "occurred_at", self.occurred_at.astimezone(UTC))


@dataclass(frozen=True)
class InspectedRetryEvent:
    id: UUID
    workflow_run_id: UUID
    task_run_id: UUID
    event_type: RetryEventType
    failed_attempt_id: UUID | None
    failed_attempt_number: int | None
    retry_attempt_id: UUID | None
    retry_attempt_number: int | None
    next_eligible_at: datetime | None
    decision_reason: RetryNotScheduledReason | None
    failure_kind: TaskExecutionFailureKind | None
    occurred_at: datetime

    def __post_init__(self) -> None:
        for field in ("next_eligible_at", "occurred_at"):
            value = getattr(self, field)
            if value is not None:
                if value.tzinfo is None or value.utcoffset() is None:
                    raise ValueError("retry event timestamps must be timezone-aware")
                object.__setattr__(self, field, value.astimezone(UTC))


@dataclass(frozen=True)
class InspectedRetryEventPage:
    items: tuple[InspectedRetryEvent, ...]
    next_cursor: RetryEventCursor | None


@dataclass(frozen=True)
class RetryDecision:
    kind: RetryDecisionKind
    failed_attempt_number: int
    next_attempt_number: int | None = None
    delay_seconds: int | None = None
    next_eligible_at: datetime | None = None


def validate_retry_policy_configuration(
    value: JSONValue,
) -> tuple[RetryPolicy | None, tuple[RetryPolicyValidationIssue, ...]]:
    """Validate a newly configured policy using current product limits."""
    return _validate_retry_policy(
        value,
        maximum_attempts_limit=MAX_CONFIGURED_RETRY_ATTEMPTS,
        maximum_delay_limit=MAX_CONFIGURED_RETRY_DELAY_SECONDS,
    )


def parse_persisted_retry_policy(value: JSONValue) -> RetryPolicy:
    """Parse an immutable snapshot using only intrinsic runtime safety limits."""
    policy, issues = _validate_retry_policy(
        value,
        maximum_attempts_limit=MAX_PERSISTED_ATTEMPT_NUMBER,
        maximum_delay_limit=MAX_TIMEDELTA_SECONDS,
    )
    if issues:
        raise InvalidPersistedRetryPolicy(issues)
    assert policy is not None
    return policy


def resolve_persisted_retry_policy(
    workflow_execution_policy: JSONMapping | None,
    step_execution_policy: JSONMapping | None,
) -> RetryPolicy | None:
    """Resolve a complete step policy over a complete workflow policy."""
    if step_execution_policy is not None and "retry_policy" in step_execution_policy:
        return parse_persisted_retry_policy(step_execution_policy["retry_policy"])
    if (
        workflow_execution_policy is not None
        and "retry_policy" in workflow_execution_policy
    ):
        return parse_persisted_retry_policy(workflow_execution_policy["retry_policy"])
    return None


def decide_retry(
    *,
    policy: RetryPolicy | None,
    failed_attempt_number: int,
    completed_at: datetime,
) -> RetryDecision:
    """Decide retry eligibility without consulting a process clock or persistence."""
    if (
        type(failed_attempt_number) is not int
        or not 1 <= failed_attempt_number <= MAX_PERSISTED_ATTEMPT_NUMBER
    ):
        raise RetryCalculationError("failed attempt number is invalid")
    if completed_at.tzinfo is None or completed_at.utcoffset() is None:
        raise RetryCalculationError(
            "result completion timestamp must be timezone-aware"
        )
    if policy is None:
        return RetryDecision(RetryDecisionKind.NO_POLICY, failed_attempt_number)
    if failed_attempt_number >= policy.maximum_attempts:
        return RetryDecision(RetryDecisionKind.EXHAUSTED, failed_attempt_number)

    delay_seconds = _calculate_delay_seconds(policy, failed_attempt_number)
    try:
        next_eligible_at = completed_at + timedelta(seconds=delay_seconds)
    except OverflowError as error:
        raise RetryCalculationError(
            "retry eligibility timestamp cannot be represented"
        ) from error
    return RetryDecision(
        RetryDecisionKind.RETRY_ALLOWED,
        failed_attempt_number,
        next_attempt_number=failed_attempt_number + 1,
        delay_seconds=delay_seconds,
        next_eligible_at=next_eligible_at,
    )


def _calculate_delay_seconds(policy: RetryPolicy, failed_attempt_number: int) -> int:
    cap = Decimal(policy.maximum_delay_seconds)
    value = Decimal(policy.initial_delay_seconds)
    if value == 0 or policy.multiplier == 1 or failed_attempt_number == 1:
        return min(policy.maximum_delay_seconds, int(value.to_integral_value()))

    exponent = failed_attempt_number - 1
    precision = (
        len(cap.as_tuple().digits)
        + len(policy.multiplier.as_tuple().digits)
        + 2 * exponent.bit_length()
        + 8
    )
    with localcontext() as context:
        # Each squaring can at most double accumulated rounding error. Decimal
        # input significance plus two guard digits per binary operation keeps
        # that error below the least represented input place; half-even avoids
        # systematically biasing intermediate products toward the final ceil.
        context.prec = precision
        context.rounding = ROUND_HALF_EVEN
        factor = _saturating_multiply(Decimal(1), policy.multiplier, cap)
        while exponent:
            if exponent & 1:
                value = _saturating_multiply(value, factor, cap)
            exponent >>= 1
            if exponent:
                factor = _saturating_multiply(factor, factor, cap)
            if value >= cap:
                return policy.maximum_delay_seconds
        rounded = value.to_integral_value(rounding=ROUND_CEILING)
    return min(policy.maximum_delay_seconds, int(rounded))


def _saturating_multiply(a: Decimal, b: Decimal, cap: Decimal) -> Decimal:
    """Return min(cap, a * b) without constructing an oversized product."""
    if a == 0 or b == 0:
        return Decimal(0)

    with localcontext() as context:
        context.rounding = ROUND_CEILING
        saturation_threshold = cap / a
    if b >= saturation_threshold:
        return cap

    return min(cap, a * b)


def _validate_retry_policy(
    value: JSONValue,
    *,
    maximum_attempts_limit: int,
    maximum_delay_limit: int,
) -> tuple[RetryPolicy | None, tuple[RetryPolicyValidationIssue, ...]]:
    if not isinstance(value, dict):
        return None, (
            RetryPolicyValidationIssue(
                "invalid_retry_policy", (), "Retry policy must be an object."
            ),
        )
    issues: list[RetryPolicyValidationIssue] = []
    if set(value) != _RETRY_POLICY_FIELDS:
        issues.append(
            RetryPolicyValidationIssue(
                "invalid_retry_policy_fields",
                (),
                "Retry policy must contain exactly the supported fields.",
            )
        )

    maximum_attempts = value.get("maximum_attempts")
    if (
        type(maximum_attempts) is not int
        or not 1 <= maximum_attempts <= maximum_attempts_limit
    ):
        issues.append(
            RetryPolicyValidationIssue(
                "invalid_retry_maximum_attempts",
                ("maximum_attempts",),
                "Maximum attempts must be a positive bounded integer including attempt 1.",
            )
        )
    initial_delay = value.get("initial_delay_seconds")
    if type(initial_delay) is not int or not 0 <= initial_delay <= maximum_delay_limit:
        issues.append(
            RetryPolicyValidationIssue(
                "invalid_retry_initial_delay_seconds",
                ("initial_delay_seconds",),
                "Initial retry delay must be a non-negative bounded integer.",
            )
        )
    multiplier = value.get("multiplier")
    parsed_multiplier: Decimal | None = None
    if type(multiplier) is int and multiplier >= 1:
        parsed_multiplier = Decimal(multiplier)
    elif type(multiplier) is float and isfinite(multiplier) and multiplier >= 1:
        parsed_multiplier = Decimal(str(multiplier))
    if parsed_multiplier is None:
        issues.append(
            RetryPolicyValidationIssue(
                "invalid_retry_multiplier",
                ("multiplier",),
                "Retry multiplier must be a finite number of at least one.",
            )
        )
    maximum_delay = value.get("maximum_delay_seconds")
    if type(maximum_delay) is not int or not 0 <= maximum_delay <= maximum_delay_limit:
        issues.append(
            RetryPolicyValidationIssue(
                "invalid_retry_maximum_delay_seconds",
                ("maximum_delay_seconds",),
                "Maximum retry delay must be a non-negative bounded integer.",
            )
        )
    if (
        type(initial_delay) is int
        and 0 <= initial_delay <= maximum_delay_limit
        and type(maximum_delay) is int
        and 0 <= maximum_delay <= maximum_delay_limit
        and maximum_delay < initial_delay
    ):
        issues.append(
            RetryPolicyValidationIssue(
                "invalid_retry_delay_order",
                ("maximum_delay_seconds",),
                "Maximum retry delay cannot be less than the initial delay.",
            )
        )
    if issues:
        return None, tuple(issues)
    assert isinstance(maximum_attempts, int) and not isinstance(maximum_attempts, bool)
    assert isinstance(initial_delay, int) and not isinstance(initial_delay, bool)
    assert isinstance(maximum_delay, int) and not isinstance(maximum_delay, bool)
    assert parsed_multiplier is not None
    return (
        RetryPolicy(
            maximum_attempts,
            initial_delay,
            parsed_multiplier,
            maximum_delay,
        ),
        (),
    )
