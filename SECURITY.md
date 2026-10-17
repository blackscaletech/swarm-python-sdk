# Security Policy

## Supported Versions

This checkout prepares version 0.1.0. Until a release/support policy is announced,
security fixes target the latest reviewed development version only.

## Reporting A Vulnerability

Use the repository's private security advisory form:
https://github.com/blackscaletech/swarm-python-sdk/security/advisories/new

Do not open a public issue containing credentials, customer data, exploit details
or private infrastructure references. If private reporting is not enabled, contact
the maintainers through an existing private channel before sharing details.
Include the SDK/Python versions, minimal synthetic reproduction, expected versus
actual behavior, and impact. Never include live tokens. No response-time or
disclosure-date guarantee is established by this preparation.

## Integration Boundaries

Keep credentials outside source control and use HTTPS and least privilege.
Idempotency keys, client normalization and lease handling do not grant authority.
Preserve generation/lease fencing and reconcile ambiguous effects using the
original intent. Custom synchronous transports and callbacks must cooperate with
cancellation and deadlines. Digest agreement without an independently expected
digest is not a freshness or authenticity guarantee.

Publication requires a reviewed tag, human approval on the protected `pypi`
environment, and a repository-bound PyPI Trusted Publisher. Configure those
controls before enabling the manual workflow; do not add long-lived API tokens.
