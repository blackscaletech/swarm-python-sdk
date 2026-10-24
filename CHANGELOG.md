# Changelog

## Unreleased

## 0.1.3 - 2026-10-24

- Merge additional query parameters without corrupting existing URL queries or fragments.
- Preserve effective-query retry identity and align duplicate query keys with the TypeScript SDK.

## 0.1.2 - 2026-10-20

- Encode identifiers as individual path segments in typed endpoint helpers.
- Return `False` for malformed webhook signatures, including non-ASCII input.
- Add regression coverage without changing exported signatures, contracts or retry keys.

## 0.1.1 - 2026-10-17

- Publish registry installation guidance and a typed discovery quickstart.
- Remove obsolete release-preparation claims from the public package description.
- Preserve all runtime source and public API behavior.

## 0.1.0 - 2026-10-17

- Release the standalone `swarm-sdk` distribution at version 0.1.0 with
  the existing `swarm_sdk` API and runtime behavior.
- Bundle the reviewed public client contract and Python typing marker.
- Replace private-source test dependencies with public contract assertions.
- Add offline sdist-to-installed-wheel checks and release documentation.
- Prepare pinned CI and a separately approved, manual Trusted PyPI workflow.
- Keep publishing opt-in through a single default-false input, a matching version
  tag and the protected PyPI environment, transferring tested artifacts unchanged.
