"""Shared capability-name contract tests."""

from taskforge.capabilities import is_valid_capability_name


def test_capability_contract_accepts_canonical_boundary_names() -> None:
    assert is_valid_capability_name("a")
    assert is_valid_capability_name("a.b-c_d9")
    assert is_valid_capability_name("a" + "z" * 127)


def test_capability_contract_rejects_noncanonical_names() -> None:
    for value in ("", "A", "1worker", " worker", "worker ", "a/b", "a" * 129):
        assert not is_valid_capability_name(value)
