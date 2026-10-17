# Contributing

Use Python 3.11 or newer. The runtime has no third-party dependencies. Work from
this repository root; tests and packaging need no other repository or service.

## Tests And Artifacts

```bash
python -B scripts/test-unit.py
python -B scripts/test-packaged.py --dist-dir dist --evidence .local-receipts/packaged
```

Both output directories must be fresh. The full unit suite needs loopback sockets
for native HTTP tests; the installed-artifact gate prohibits all sockets and DNS.
All tests must pass with zero skips. Do not replace canonical assertions with
environment-based skips. The gate uses a pre-provisioned Python environment with
the exact pins in `scripts/requirements-packaging.txt`; it does not install build
dependencies or contact an index. If provisioning is necessary, use a separate
virtual environment and an approved wheelhouse:

```bash
python -m pip install --no-index --find-links /path/to/wheelhouse -r scripts/requirements-packaging.txt
```

Keep authority normalization, operation policies, retry identities, lease fencing,
uncertain-effect handling and exported signatures compatible. Add synthetic tests
for behavior changes. The bundled client contract is a reviewed public snapshot,
not a live introspection tool. Update its digest assertion only after coordinated
public contract review. Never add private source reads, credentials, service
dependencies or machine-specific paths to tests or package metadata.

The sdist includes public tests, scripts and the local contract. Local receipts,
environments, private handoff material and build outputs must not be exported.

## Release Checklist

1. Review the complete source/export boundary and approve Apache-2.0 licensing.
2. Confirm the intended public repository URLs in metadata and security guidance.
3. Run unit tests and the offline gate on every supported CI interpreter.
4. Review the changelog, version and built distribution contents.
5. Configure a protected `pypi` GitHub environment with required human reviewers
   and restricted release tags. Configure the matching PyPI Trusted Publisher for
   `publish.yml` and that environment; do not create an API token.
6. After separate release authorization, select the reviewed matching `v<version>`
   tag and manually dispatch `publish.yml` with `publish` set to `true`. The input
   defaults to `false`, which verifies the artifacts without publishing them.

Neither pushes, tags nor GitHub releases automatically publish. The workflow is
limited to transferring the exact tested wheel and sdist to the protected publish
job; that job does not check out source or rebuild artifacts. OIDC permission is
scoped to the publish job only. The workflow is
only scaffolding until those external controls are configured. Do not dispatch it
as part of local preparation. Dependency/action upgrades require explicit review
of the new versions and immutable action commit pins.
