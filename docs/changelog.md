# Changelog

The release history lives in [`CHANGELOG.md`](../CHANGELOG.md) at the
repository root, one entry per published version. Releases are tagged
`vX.Y.Z` on GitHub and published to PyPI as
[`skaidb`](https://pypi.org/project/skaidb/) by the release workflow.

## Versioning

The driver follows [Semantic Versioning](https://semver.org/): a major bump
for a breaking API change, minor for new features, patch for fixes. The
version is defined once in `pyproject.toml`; `skaidb.__version__` reports it
and the driver sends it to the server in the Hello frame, so the server's
`drivers` table shows exactly which release each client runs.

## Releasing

A release is a tag: bump `version` in `pyproject.toml` (and the fallback
literal in `skaidb/__init__.py`), add the `## [X.Y.Z]` section to
`CHANGELOG.md`, commit, then `git tag vX.Y.Z && git push origin main vX.Y.Z`.
The `Publish to PyPI` workflow runs the tests, builds the sdist and wheel,
uploads them to PyPI and creates the GitHub Release; nothing is done by hand.

## 1.0.2 — 2026-09-20

Release automation: published from GitHub Actions. No code changes.
[`CHANGELOG.md`](../CHANGELOG.md#102---2026-09-20).

## 1.0.1 — 2026-09-19

Documentation only: absolute README links so they work on PyPI, and the
`drivers`-table note in [api.md](api.md#connect). No code changes.
[`CHANGELOG.md`](../CHANGELOG.md#101---2026-09-19).

## 1.0.0 — 2026-09-19

First standalone release on PyPI; the full feature list is in the root
[`CHANGELOG.md`](../CHANGELOG.md#100---2026-09-19).
