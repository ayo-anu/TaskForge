# Taskforge security checks

Taskforge uses two repository security gates. `pip-audit 2.10.1` scans a
hashed export of every runtime, development, build, and security-tool
dependency resolved in `uv.lock`. Gitleaks 8.30.1 scans the complete Git
history for committed credentials and always redacts findings.

Run the same checks locally with:

```console
make security
```

The dependency gate blocks every active known vulnerability by default; it
does not use severity filtering, update dependencies, or perform a second pip
resolution. The secret gate blocks every finding not represented by an exact
reviewed fingerprint. CI runs both gates on pull requests, pushes to `main`,
manual dispatch, and a weekly schedule so an unchanged lockfile is checked
against newly published advisories.

## Temporary exceptions

Exceptions are reviewed repository data in
`security/security-exceptions.toml`. A vulnerability exception identifies one
exact advisory and normalized package. A Gitleaks exception identifies one
exact fingerprint also present in `.gitleaksignore`; the files must have a
one-to-one correspondence. Wildcards and package-, path-, rule-, or
regular-expression-wide suppressions are prohibited.

Every exception includes a justification, responsible role, UTC introduction
and expiry dates, and a remediation or tracking reference. It is valid only
while the current UTC date is strictly earlier than `expires_on`, cannot be
introduced in the future, and cannot span more than 90 days. Expired,
malformed, duplicate, orphaned, or unused exceptions fail the gate.

## Remediation

For a vulnerable dependency, review the advisory and aliases, update the
narrowest applicable constraint and `uv.lock`, then rerun `make security` and
the affected tests. Direct, transitive, development, build, and security-tool
dependencies follow the same policy. Use a temporary exception only when an
immediate safe upgrade is unavailable and senior review accepts the documented
risk.

Treat a Gitleaks finding as an exposed credential: rotate or revoke it, remove
it from current content, and assess the Git history. Add a fingerprint
exception only after proving the value is non-secret test material. Never put
raw secret material in exception metadata.

These gates detect known Python package vulnerabilities and likely committed
secrets. They are not comprehensive SAST, DAST, container scanning, dependency
freshness enforcement, license review, penetration testing, or an artifact
signing/attestation system.
