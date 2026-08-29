"""Run Taskforge's repository security gates with expiring exceptions."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = PROJECT_ROOT / "security" / "security-exceptions.toml"
GITLEAKS_IGNORE_PATH = PROJECT_ROOT / ".gitleaksignore"
GITLEAKS_IMAGE = (
    "ghcr.io/gitleaks/gitleaks:v8.30.1@"
    "sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f"
)
LOCAL_COMMAND = "make security"
MAX_EXCEPTION_DAYS = 90
_DATE_PATTERN = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_FINGERPRINT_PATTERN = re.compile(
    r"^[0-9a-f]{40}:[^:*?\[\]\r\n]+:[A-Za-z0-9_-]+:[1-9][0-9]*$"
)
_WILDCARD_PATTERN = re.compile(r"[*?\[\]]")


class SecurityPolicyError(ValueError):
    """Raised when repository security policy is invalid or inconsistent."""


@dataclass(frozen=True)
class ExceptionMetadata:
    justification: str
    responsible_role: str
    introduced_on: date
    expires_on: date
    tracking: str


@dataclass(frozen=True)
class VulnerabilityException:
    advisory_id: str
    package: str
    metadata: ExceptionMetadata


@dataclass(frozen=True)
class GitleaksException:
    fingerprint: str
    metadata: ExceptionMetadata


@dataclass(frozen=True)
class SecurityPolicy:
    vulnerabilities: tuple[VulnerabilityException, ...]
    gitleaks: tuple[GitleaksException, ...]


@dataclass(frozen=True)
class VulnerabilityFinding:
    package: str
    version: str
    advisory_id: str
    aliases: tuple[str, ...]
    fix_versions: tuple[str, ...]


@dataclass(frozen=True)
class AuditEvaluation:
    blocking: tuple[VulnerabilityFinding, ...]
    excepted: tuple[tuple[VulnerabilityFinding, VulnerabilityException], ...]


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def utc_policy_date() -> date:
    """Return the UTC calendar date used for exception evaluation."""

    return datetime.now(UTC).date()


def normalize_package_name(value: str) -> str:
    normalized = re.sub(r"[-_.]+", "-", value.strip().lower())
    if not normalized or _WILDCARD_PATTERN.search(normalized):
        raise SecurityPolicyError(f"invalid exact package name: {value!r}")
    return normalized


def _required_string(record: dict[str, Any], field: str, kind: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise SecurityPolicyError(f"{kind} requires non-empty {field}")
    return value.strip()


def _parse_date(record: dict[str, Any], field: str, kind: str) -> date:
    value = record.get(field)
    if not isinstance(value, str) or not _DATE_PATTERN.fullmatch(value):
        raise SecurityPolicyError(f"{kind} has malformed {field}; use YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise SecurityPolicyError(f"{kind} has malformed {field}") from error


def _metadata(
    record: dict[str, Any], *, kind: str, evaluation_date: date
) -> ExceptionMetadata:
    introduced_on = _parse_date(record, "introduced_on", kind)
    expires_on = _parse_date(record, "expires_on", kind)
    if introduced_on > evaluation_date:
        raise SecurityPolicyError(f"{kind} has future introduced_on")
    if expires_on <= introduced_on:
        raise SecurityPolicyError(f"{kind} expires_on must follow introduced_on")
    if (expires_on - introduced_on).days > MAX_EXCEPTION_DAYS:
        raise SecurityPolicyError(f"{kind} review period exceeds 90 days")
    if evaluation_date >= expires_on:
        raise SecurityPolicyError(f"{kind} expired on {expires_on.isoformat()} UTC")
    return ExceptionMetadata(
        justification=_required_string(record, "justification", kind),
        responsible_role=_required_string(record, "responsible_role", kind),
        introduced_on=introduced_on,
        expires_on=expires_on,
        tracking=_required_string(record, "tracking", kind),
    )


def load_policy(
    path: Path = POLICY_PATH, *, evaluation_date: date | None = None
) -> SecurityPolicy:
    policy_date = evaluation_date or utc_policy_date()
    try:
        document = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise SecurityPolicyError(
            f"cannot read security exception policy: {error}"
        ) from error

    if document.get("schema_version") != 1:
        raise SecurityPolicyError(
            "security exception policy requires schema_version = 1"
        )
    allowed_top_level = {"schema_version", "vulnerability", "gitleaks"}
    unexpected = set(document) - allowed_top_level
    if unexpected:
        raise SecurityPolicyError(f"unknown policy sections: {sorted(unexpected)}")

    vulnerabilities: list[VulnerabilityException] = []
    vulnerability_keys: set[tuple[str, str]] = set()
    for index, raw in enumerate(document.get("vulnerability", []), start=1):
        if not isinstance(raw, dict):
            raise SecurityPolicyError(
                f"vulnerability exception {index} must be a table"
            )
        kind = f"vulnerability exception {index}"
        advisory_id = _required_string(raw, "advisory_id", kind).upper()
        if _WILDCARD_PATTERN.search(advisory_id):
            raise SecurityPolicyError(f"{kind} advisory_id must be exact")
        package = normalize_package_name(_required_string(raw, "package", kind))
        key = (package, advisory_id)
        if key in vulnerability_keys:
            raise SecurityPolicyError(f"duplicate vulnerability exception: {key}")
        vulnerability_keys.add(key)
        vulnerabilities.append(
            VulnerabilityException(
                advisory_id=advisory_id,
                package=package,
                metadata=_metadata(raw, kind=kind, evaluation_date=policy_date),
            )
        )

    gitleaks: list[GitleaksException] = []
    gitleaks_keys: set[str] = set()
    for index, raw in enumerate(document.get("gitleaks", []), start=1):
        if not isinstance(raw, dict):
            raise SecurityPolicyError(f"Gitleaks exception {index} must be a table")
        kind = f"Gitleaks exception {index}"
        fingerprint = _required_string(raw, "fingerprint", kind)
        if not _FINGERPRINT_PATTERN.fullmatch(fingerprint):
            raise SecurityPolicyError(f"{kind} requires one exact finding fingerprint")
        if fingerprint in gitleaks_keys:
            raise SecurityPolicyError(f"duplicate Gitleaks fingerprint: {fingerprint}")
        gitleaks_keys.add(fingerprint)
        gitleaks.append(
            GitleaksException(
                fingerprint=fingerprint,
                metadata=_metadata(raw, kind=kind, evaluation_date=policy_date),
            )
        )
    return SecurityPolicy(tuple(vulnerabilities), tuple(gitleaks))


def load_gitleaks_fingerprints(path: Path = GITLEAKS_IGNORE_PATH) -> tuple[str, ...]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise SecurityPolicyError(f"cannot read .gitleaksignore: {error}") from error
    fingerprints = tuple(line.strip() for line in lines if line.strip())
    if len(fingerprints) != len(set(fingerprints)):
        raise SecurityPolicyError(".gitleaksignore contains duplicate fingerprints")
    invalid = [
        item for item in fingerprints if not _FINGERPRINT_PATTERN.fullmatch(item)
    ]
    if invalid:
        raise SecurityPolicyError(".gitleaksignore permits exact fingerprints only")
    return fingerprints


def validate_gitleaks_bijection(
    policy: SecurityPolicy, fingerprints: Sequence[str]
) -> None:
    ignored = set(fingerprints)
    described = {entry.fingerprint for entry in policy.gitleaks}
    orphan_ignores = sorted(ignored - described)
    orphan_metadata = sorted(described - ignored)
    if orphan_ignores or orphan_metadata:
        raise SecurityPolicyError(
            "Gitleaks suppression mismatch: "
            f"orphan fingerprints={orphan_ignores}, orphan metadata={orphan_metadata}"
        )


def parse_pip_audit_output(output: str) -> tuple[VulnerabilityFinding, ...]:
    try:
        document = json.loads(output)
    except json.JSONDecodeError as error:
        raise SecurityPolicyError("pip-audit did not return valid JSON") from error
    dependencies = document.get("dependencies")
    if not isinstance(dependencies, list):
        raise SecurityPolicyError("pip-audit JSON is missing dependencies")
    findings: list[VulnerabilityFinding] = []
    for dependency in dependencies:
        if not isinstance(dependency, dict):
            raise SecurityPolicyError("pip-audit dependency entry is malformed")
        package = normalize_package_name(str(dependency.get("name", "")))
        version = str(dependency.get("version", "unknown"))
        vulnerabilities = dependency.get("vulns", [])
        if not isinstance(vulnerabilities, list):
            raise SecurityPolicyError("pip-audit vulnerability list is malformed")
        for vulnerability in vulnerabilities:
            if not isinstance(vulnerability, dict):
                raise SecurityPolicyError("pip-audit vulnerability entry is malformed")
            advisory_id = str(vulnerability.get("id", "")).strip().upper()
            if not advisory_id:
                raise SecurityPolicyError("pip-audit finding has no advisory ID")
            aliases = tuple(
                str(item).strip().upper()
                for item in vulnerability.get("aliases", [])
                if str(item).strip()
            )
            fixes = tuple(str(item) for item in vulnerability.get("fix_versions", []))
            findings.append(
                VulnerabilityFinding(package, version, advisory_id, aliases, fixes)
            )
    return tuple(findings)


def evaluate_findings(
    findings: Sequence[VulnerabilityFinding], policy: SecurityPolicy
) -> AuditEvaluation:
    blocking: list[VulnerabilityFinding] = []
    excepted: list[tuple[VulnerabilityFinding, VulnerabilityException]] = []
    used: set[VulnerabilityException] = set()
    for finding in findings:
        identifiers = {finding.advisory_id, *finding.aliases}
        matches = [
            entry
            for entry in policy.vulnerabilities
            if entry.package == finding.package and entry.advisory_id in identifiers
        ]
        if len(matches) > 1:
            raise SecurityPolicyError(
                f"multiple exceptions match {finding.package} {finding.advisory_id}"
            )
        if matches:
            used.add(matches[0])
            excepted.append((finding, matches[0]))
        else:
            blocking.append(finding)
    unused = [entry for entry in policy.vulnerabilities if entry not in used]
    if unused:
        values = [f"{entry.package}:{entry.advisory_id}" for entry in unused]
        raise SecurityPolicyError(f"unused vulnerability exceptions: {values}")
    return AuditEvaluation(tuple(blocking), tuple(excepted))


def format_finding(finding: VulnerabilityFinding) -> str:
    aliases = ", ".join(finding.aliases) or "none"
    fixes = ", ".join(finding.fix_versions) or "none published"
    return (
        f"package={finding.package} version={finding.version} "
        f"advisory={finding.advisory_id} aliases={aliases} fixed_versions={fixes}"
    )


def run_dependency_scan(
    policy: SecurityPolicy, *, runner: CommandRunner = subprocess.run
) -> int:
    with tempfile.TemporaryDirectory(prefix="taskforge-security-") as directory:
        requirements = Path(directory) / "locked-requirements.txt"
        exported = runner(
            [
                "uv",
                "export",
                "--locked",
                "--all-groups",
                "--all-extras",
                "--no-emit-project",
                "--output-file",
                str(requirements),
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if exported.returncode != 0:
            print("dependency scan failed while exporting uv.lock", file=sys.stderr)
            print(exported.stderr, file=sys.stderr)
            return 2
        audited = runner(
            [
                sys.executable,
                "-m",
                "pip_audit",
                "--requirement",
                str(requirements),
                "--require-hashes",
                "--disable-pip",
                "--format",
                "json",
                "--aliases",
                "on",
                "--progress-spinner",
                "off",
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    if audited.returncode not in {0, 1}:
        print("dependency scanner or advisory service failed", file=sys.stderr)
        print(audited.stderr, file=sys.stderr)
        return 2
    try:
        evaluation = evaluate_findings(parse_pip_audit_output(audited.stdout), policy)
    except SecurityPolicyError as error:
        print(f"dependency security policy error: {error}", file=sys.stderr)
        return 2
    for finding, exception in evaluation.excepted:
        print(
            f"EXCEPTED {format_finding(finding)} "
            f"expires_on={exception.metadata.expires_on.isoformat()} UTC"
        )
    if evaluation.blocking:
        print("Blocking dependency vulnerabilities:", file=sys.stderr)
        for finding in evaluation.blocking:
            print(f"- {format_finding(finding)}", file=sys.stderr)
        print(f"Reproduce locally with: {LOCAL_COMMAND}", file=sys.stderr)
        return 1
    print("Dependency audit passed: no unexcepted known vulnerabilities.")
    return 0


def run_gitleaks_scan(*, runner: CommandRunner = subprocess.run) -> int:
    command = [
        "docker",
        "run",
        "--rm",
        "--volume",
        f"{PROJECT_ROOT}:/repo:ro",
        "--workdir",
        "/repo",
        GITLEAKS_IMAGE,
        "git",
        "--redact",
        "--no-banner",
        "--gitleaks-ignore-path",
        "/repo/.gitleaksignore",
        "/repo",
    ]
    completed = runner(command, cwd=PROJECT_ROOT, check=False)
    if completed.returncode != 0:
        print(
            "Gitleaks failed. Output is redacted; rotate/revoke any real credential. "
            "Exceptions are allowed only for proven non-secret material.",
            file=sys.stderr,
        )
        print(f"Reproduce locally with: {LOCAL_COMMAND}", file=sys.stderr)
        return completed.returncode or 1
    print("Gitleaks passed: full Git history contains no unexcepted secret findings.")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--evaluation-date",
        type=date.fromisoformat,
        help="UTC policy date override for deterministic tests (YYYY-MM-DD)",
    )
    arguments = parser.parse_args(argv)
    try:
        policy = load_policy(evaluation_date=arguments.evaluation_date)
        fingerprints = load_gitleaks_fingerprints()
        validate_gitleaks_bijection(policy, fingerprints)
    except SecurityPolicyError as error:
        print(f"security exception policy error: {error}", file=sys.stderr)
        return 2
    dependency_status = run_dependency_scan(policy)
    gitleaks_status = run_gitleaks_scan()
    return dependency_status or gitleaks_status


if __name__ == "__main__":
    raise SystemExit(main())
