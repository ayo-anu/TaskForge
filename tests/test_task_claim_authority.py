"""Result-specific task claim authority tests."""

from uuid import uuid4

import pytest

from taskforge.claims.authority import TaskClaimResultAuthorityIssuer


def test_result_authority_is_stable_bound_and_redacted() -> None:
    issuer = TaskClaimResultAuthorityIssuer(b"a" * 32)
    identity_id, session_id, attempt_id = uuid4(), uuid4(), uuid4()
    authority = issuer.issue(
        worker_identity_id=identity_id,
        worker_session_id=session_id,
        task_attempt_id=attempt_id,
        generation=7,
    )

    assert authority == issuer.issue(
        worker_identity_id=identity_id,
        worker_session_id=session_id,
        task_attempt_id=attempt_id,
        generation=7,
    )
    assert authority.presented_value.startswith("tf_claim_result_v1.")
    assert authority.presented_value not in repr(authority)
    assert authority.presented_value not in str(authority)
    assert "a" * 32 not in repr(issuer)


def test_result_authority_domain_binding_rejects_altered_claim_facts() -> None:
    issuer = TaskClaimResultAuthorityIssuer(b"b" * 32)
    identity_id, session_id, attempt_id = uuid4(), uuid4(), uuid4()
    authority = issuer.issue(
        worker_identity_id=identity_id,
        worker_session_id=session_id,
        task_attempt_id=attempt_id,
        generation=3,
    )

    assert issuer.verify(
        authority,
        worker_identity_id=identity_id,
        worker_session_id=session_id,
        task_attempt_id=attempt_id,
        generation=3,
    )
    assert not issuer.verify(
        authority,
        worker_identity_id=uuid4(),
        worker_session_id=session_id,
        task_attempt_id=attempt_id,
        generation=3,
    )
    assert not issuer.verify(
        authority,
        worker_identity_id=identity_id,
        worker_session_id=uuid4(),
        task_attempt_id=attempt_id,
        generation=3,
    )
    assert not issuer.verify(
        authority,
        worker_identity_id=identity_id,
        worker_session_id=session_id,
        task_attempt_id=uuid4(),
        generation=3,
    )
    assert not issuer.verify(
        authority,
        worker_identity_id=identity_id,
        worker_session_id=session_id,
        task_attempt_id=attempt_id,
        generation=4,
    )


@pytest.mark.parametrize("secret", [b"", b"short"])
def test_result_authority_requires_minimum_secret_length(secret: bytes) -> None:
    with pytest.raises(ValueError, match="32 bytes"):
        TaskClaimResultAuthorityIssuer(secret)


@pytest.mark.parametrize("generation", [0, -1, 2**63])
def test_result_authority_requires_positive_bigint_generation(generation: int) -> None:
    issuer = TaskClaimResultAuthorityIssuer(b"c" * 32)
    with pytest.raises(ValueError, match="BIGINT"):
        issuer.issue(
            worker_identity_id=uuid4(),
            worker_session_id=uuid4(),
            task_attempt_id=uuid4(),
            generation=generation,
        )
