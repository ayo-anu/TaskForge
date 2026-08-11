"""Task claim acquisition result invariants."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from taskforge.claims.domain import (
    IssuedTaskClaim,
    TaskClaimLease,
    TaskClaimOutcome,
    TaskClaimRejected,
    TaskClaimRejectionReason,
    TaskClaimRenewalOutcome,
    TaskClaimRenewalRequest,
    TaskClaimRenewalResult,
    TaskClaimResult,
    TaskClaimResultAuthority,
)


def test_claim_lease_normalizes_timestamps_and_preserves_explicit_outcome() -> None:
    acquired = datetime.now(UTC)
    lease = TaskClaimLease(
        uuid4(), 2, uuid4(), acquired, acquired + timedelta(seconds=1)
    )
    result = TaskClaimResult(TaskClaimOutcome.REPLAYED_EXPIRED, lease)
    assert result.outcome is TaskClaimOutcome.REPLAYED_EXPIRED
    assert result.claim.generation == 2
    assert result.claim.acquired_at.tzinfo is UTC


@pytest.mark.parametrize("generation", [0, -1])
def test_claim_generation_must_be_positive(generation: int) -> None:
    acquired = datetime.now(UTC)
    with pytest.raises(ValueError, match="generation"):
        TaskClaimLease(
            uuid4(), generation, uuid4(), acquired, acquired + timedelta(seconds=1)
        )


def test_claim_lease_requires_ordered_aware_timestamps() -> None:
    acquired = datetime.now(UTC)
    with pytest.raises(ValueError, match="expire"):
        TaskClaimLease(uuid4(), 1, uuid4(), acquired, acquired)
    with pytest.raises(ValueError, match="timezone-aware"):
        TaskClaimLease(uuid4(), 1, uuid4(), acquired.replace(tzinfo=None), acquired)


def test_renewal_request_rejects_naive_expiry_and_normalizes_aware_value() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        TaskClaimRenewalRequest(uuid4(), 1, uuid4(), datetime.now())

    expected = datetime.now(UTC)
    request = TaskClaimRenewalRequest(uuid4(), 1, uuid4(), expected)
    assert request.expected_lease_expires_at.tzinfo is UTC


def test_renewal_result_preserves_explicit_outcome() -> None:
    acquired = datetime.now(UTC)
    lease = TaskClaimLease(
        uuid4(), 1, uuid4(), acquired, acquired + timedelta(seconds=60)
    )
    result = TaskClaimRenewalResult(TaskClaimRenewalOutcome.ACTIVE_UNCHANGED, lease)
    assert result.outcome is TaskClaimRenewalOutcome.ACTIVE_UNCHANGED


def test_issued_claim_requires_authority_only_for_active_outcomes() -> None:
    acquired = datetime.now(UTC)
    lease = TaskClaimLease(
        uuid4(), 1, uuid4(), acquired, acquired + timedelta(seconds=60)
    )
    authority = TaskClaimResultAuthority("tf_claim_result_v1." + "a" * 43)
    IssuedTaskClaim(TaskClaimOutcome.ACQUIRED_ACTIVE, lease, authority)
    IssuedTaskClaim(TaskClaimOutcome.REPLAYED_EXPIRED, lease, None)
    with pytest.raises(ValueError, match="require result authority"):
        IssuedTaskClaim(TaskClaimOutcome.REPLAYED_ACTIVE, lease, None)
    with pytest.raises(ValueError, match="require result authority"):
        IssuedTaskClaim(TaskClaimOutcome.REPLAYED_EXPIRED, lease, authority)


def test_result_authority_and_renewal_request_reject_invalid_identity_fields() -> None:
    with pytest.raises(ValueError, match="invalid claim result authority"):
        TaskClaimResultAuthority("wrong-format")
    with pytest.raises(ValueError, match="generation"):
        TaskClaimRenewalRequest(uuid4(), 0, uuid4(), datetime.now(UTC))


@pytest.mark.parametrize(
    "presented_value",
    (
        "tf_claim_result_v1.",
        "tf_claim_result_v1." + "a" * 42,
        "tf_claim_result_v1." + "a" * 44,
        "tf_claim_result_v1." + "a" * 42 + "=",
        "tf_claim_result_v1." + "a" * 42 + "+",
        "tf_claim_result_v1." + "a" * 42 + "/",
        "tf_claim_result_v1." + "a" * 42 + " ",
        "tf_claim_result_v1." + "a" * 42 + "\n",
        "tf_claim_result_v1." + "a" * 21 + "." + "a" * 21,
        "prefix.tf_claim_result_v1." + "a" * 43,
        "tf_claim_result_v1." + "a" * 43 + ".suffix",
        "tf_claim_result_v1." + "a" * 42 + "é",
    ),
)
def test_result_authority_rejects_noncanonical_bearer_format(
    presented_value: str,
) -> None:
    with pytest.raises(ValueError, match="invalid claim result authority"):
        TaskClaimResultAuthority(presented_value)


def test_result_authority_accepts_canonical_bearer_format() -> None:
    authority = TaskClaimResultAuthority(
        "tf_claim_result_v1.ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopq"
    )

    assert authority.presented_value == (
        "tf_claim_result_v1.ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopq"
    )


def test_claim_rejection_has_stable_reason_and_safe_fixed_message() -> None:
    rejection = TaskClaimRejected(TaskClaimRejectionReason.ALREADY_AUTHORITATIVE)

    assert rejection.reason is TaskClaimRejectionReason.ALREADY_AUTHORITATIVE
    assert str(rejection) == "task claim acquisition rejected"
    rendered = repr(rejection)
    assert "worker-session-secret" not in rendered
    assert "already_authoritative" not in rendered
