"""Behavioral tests for the repository security-gate policy."""

from __future__ import annotations

import subprocess
from datetime import date
from pathlib import Path

import pytest
from scripts.security_scan import (
    GITLEAKS_IMAGE,
    SecurityPolicy,
    SecurityPolicyError,
    VulnerabilityFinding,
    evaluate_findings,
    format_finding,
    load_gitleaks_fingerprints,
    load_policy,
    run_dependency_scan,
    validate_gitleaks_bijection,
)

FINGERPRINT = (
    "0123456789abcdef0123456789abcdef01234567:tests/test_fixture.py:generic-api-key:10"
)


def exception_fields(
    *, introduced_on: str = "2026-08-01", expires_on: str = "2026-09-01"
) -> str:
    return f'''justification = "Reviewed temporary exception"
responsible_role = "Taskforge maintainers"
introduced_on = "{introduced_on}"
expires_on = "{expires_on}"
tracking = "SEC-123"
'''


def vulnerability_policy(
    *,
    advisory_id: str = "CVE-2026-1234",
    package: str = "example_package",
    introduced_on: str = "2026-08-01",
    expires_on: str = "2026-09-01",
) -> str:
    return f'''schema_version = 1

[[vulnerability]]
advisory_id = "{advisory_id}"
package = "{package}"
{exception_fields(introduced_on=introduced_on, expires_on=expires_on)}'''


def gitleaks_policy(
    *,
    fingerprint: str = FINGERPRINT,
    introduced_on: str = "2026-08-01",
    expires_on: str = "2026-09-01",
) -> str:
    return f'''schema_version = 1

[[gitleaks]]
fingerprint = "{fingerprint}"
{exception_fields(introduced_on=introduced_on, expires_on=expires_on)}'''


def write_policy(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "security-exceptions.toml"
    path.write_text(content, encoding="utf-8")
    return path


def finding(
    *, package: str = "example-package", advisory_id: str = "CVE-2026-1234"
) -> VulnerabilityFinding:
    return VulnerabilityFinding(
        package=package,
        version="1.2.3",
        advisory_id=advisory_id,
        aliases=("GHSA-1111-2222-3333",),
        fix_versions=("1.2.4",),
    )


def test_empty_exception_policy_is_valid(tmp_path: Path) -> None:
    policy = load_policy(
        write_policy(tmp_path, "schema_version = 1\n"),
        evaluation_date=date(2026, 8, 15),
    )

    assert policy == SecurityPolicy((), ())
    validate_gitleaks_bijection(policy, ())


def test_exact_advisory_and_normalized_package_match(tmp_path: Path) -> None:
    policy = load_policy(
        write_policy(tmp_path, vulnerability_policy()),
        evaluation_date=date(2026, 8, 15),
    )

    evaluation = evaluate_findings((finding(),), policy)

    assert not evaluation.blocking
    assert evaluation.excepted[0][1].package == "example-package"


def test_advisory_alias_matches_only_for_same_package(tmp_path: Path) -> None:
    policy = load_policy(
        write_policy(
            tmp_path,
            vulnerability_policy(advisory_id="GHSA-1111-2222-3333"),
        ),
        evaluation_date=date(2026, 8, 15),
    )

    assert not evaluate_findings((finding(),), policy).blocking

    with pytest.raises(SecurityPolicyError, match="unused vulnerability"):
        evaluate_findings((finding(package="different-package"),), policy)


@pytest.mark.parametrize("value", ["CVE-*", "GHSA-????", "[all]"])
def test_wildcard_advisory_is_rejected(tmp_path: Path, value: str) -> None:
    with pytest.raises(SecurityPolicyError, match="must be exact"):
        load_policy(
            write_policy(tmp_path, vulnerability_policy(advisory_id=value)),
            evaluation_date=date(2026, 8, 15),
        )


def test_duplicate_vulnerability_exception_is_rejected(tmp_path: Path) -> None:
    entry = vulnerability_policy().split("\n", maxsplit=1)[1]
    content = f"schema_version = 1\n{entry}{entry}"

    with pytest.raises(SecurityPolicyError, match="duplicate vulnerability"):
        load_policy(
            write_policy(tmp_path, content),
            evaluation_date=date(2026, 8, 15),
        )


@pytest.mark.parametrize("value", ["2026/08/01", "2026-02-30", "August 1"])
def test_malformed_dates_are_rejected(tmp_path: Path, value: str) -> None:
    with pytest.raises(SecurityPolicyError, match="malformed introduced_on"):
        load_policy(
            write_policy(tmp_path, vulnerability_policy(introduced_on=value)),
            evaluation_date=date(2026, 8, 15),
        )


def test_future_introduced_on_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(SecurityPolicyError, match="future introduced_on"):
        load_policy(
            write_policy(
                tmp_path,
                vulnerability_policy(
                    introduced_on="2026-08-16", expires_on="2026-09-01"
                ),
            ),
            evaluation_date=date(2026, 8, 15),
        )


def test_exception_is_valid_day_before_expiry_and_expired_on_date(
    tmp_path: Path,
) -> None:
    path = write_policy(tmp_path, vulnerability_policy())

    assert load_policy(path, evaluation_date=date(2026, 8, 31)).vulnerabilities
    with pytest.raises(SecurityPolicyError, match="expired on 2026-09-01 UTC"):
        load_policy(path, evaluation_date=date(2026, 9, 1))


def test_ninety_day_period_is_allowed_and_ninety_one_is_rejected(
    tmp_path: Path,
) -> None:
    allowed = write_policy(
        tmp_path,
        vulnerability_policy(introduced_on="2026-01-01", expires_on="2026-04-01"),
    )
    assert load_policy(allowed, evaluation_date=date(2026, 1, 1)).vulnerabilities

    rejected = write_policy(
        tmp_path,
        vulnerability_policy(introduced_on="2026-01-01", expires_on="2026-04-02"),
    )
    with pytest.raises(SecurityPolicyError, match="exceeds 90 days"):
        load_policy(rejected, evaluation_date=date(2026, 1, 1))


def test_leap_day_uses_calendar_date_arithmetic(tmp_path: Path) -> None:
    path = write_policy(
        tmp_path,
        vulnerability_policy(introduced_on="2024-02-29", expires_on="2024-05-29"),
    )

    assert load_policy(path, evaluation_date=date(2024, 3, 1)).vulnerabilities


def test_unused_vulnerability_exception_is_rejected(tmp_path: Path) -> None:
    policy = load_policy(
        write_policy(tmp_path, vulnerability_policy()),
        evaluation_date=date(2026, 8, 15),
    )

    with pytest.raises(SecurityPolicyError, match="unused vulnerability"):
        evaluate_findings((), policy)


def test_fixed_version_output_is_actionable() -> None:
    output = format_finding(finding())

    assert "package=example-package" in output
    assert "version=1.2.3" in output
    assert "advisory=CVE-2026-1234" in output
    assert "aliases=GHSA-1111-2222-3333" in output
    assert "fixed_versions=1.2.4" in output


def test_scanner_or_service_failure_remains_nonzero() -> None:
    calls = 0

    def failed_runner(
        *args: object, **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr=""
            )
        return subprocess.CompletedProcess(
            args=[], returncode=2, stdout="", stderr="service unavailable"
        )

    assert run_dependency_scan(SecurityPolicy((), ()), runner=failed_runner) == 2


def test_scanner_commands_are_locked_hashed_and_redacted() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "scripts/security_scan.py"
    ).read_text(encoding="utf-8")

    assert '"--locked"' in source
    assert '"--all-groups"' in source
    assert '"--all-extras"' in source
    assert '"--require-hashes"' in source
    assert '"--disable-pip"' in source
    assert '"--redact"' in source
    assert "v8.30.1@sha256:" in GITLEAKS_IMAGE


def test_gitleaks_suppressions_require_exact_bijection(tmp_path: Path) -> None:
    policy = load_policy(
        write_policy(tmp_path, gitleaks_policy()),
        evaluation_date=date(2026, 8, 15),
    )

    validate_gitleaks_bijection(policy, (FINGERPRINT,))

    with pytest.raises(SecurityPolicyError, match="orphan fingerprints"):
        validate_gitleaks_bijection(SecurityPolicy((), ()), (FINGERPRINT,))
    with pytest.raises(SecurityPolicyError, match="orphan metadata"):
        validate_gitleaks_bijection(policy, ())


def test_duplicate_gitleaks_metadata_is_rejected(tmp_path: Path) -> None:
    entry = gitleaks_policy().split("\n", maxsplit=1)[1]
    with pytest.raises(SecurityPolicyError, match="duplicate Gitleaks"):
        load_policy(
            write_policy(tmp_path, f"schema_version = 1\n{entry}{entry}"),
            evaluation_date=date(2026, 8, 15),
        )


def test_duplicate_or_nonfingerprint_gitleaks_ignore_is_rejected(
    tmp_path: Path,
) -> None:
    ignored = tmp_path / ".gitleaksignore"
    ignored.write_text(f"{FINGERPRINT}\n{FINGERPRINT}\n", encoding="utf-8")
    with pytest.raises(SecurityPolicyError, match="duplicate fingerprints"):
        load_gitleaks_fingerprints(ignored)

    ignored.write_text("tests/**\n", encoding="utf-8")
    with pytest.raises(SecurityPolicyError, match="exact fingerprints only"):
        load_gitleaks_fingerprints(ignored)


def test_expired_gitleaks_metadata_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(SecurityPolicyError, match="expired"):
        load_policy(
            write_policy(tmp_path, gitleaks_policy(expires_on="2026-08-15")),
            evaluation_date=date(2026, 8, 15),
        )
