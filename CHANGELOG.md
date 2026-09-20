# Changelog

All notable changes to the skaidb Python driver. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/).

## [1.0.2] - 2026-09-20

1.0.2 — release automation: published from GitHub Actions.

### Changed
- Pushing a `vX.Y.Z` tag now runs the tests, builds the sdist and wheel,
  uploads them to PyPI and creates the GitHub Release (with this changelog
  section as its notes) from GitHub Actions, with no manual step. The publish
  workflow refuses a tag that does not match `pyproject.toml` or has no
  changelog entry, and skips the PyPI upload with a warning instead of failing
  when the `PYPI_API_TOKEN` secret is absent.
- No code changes.

## [1.0.1] - 2026-09-19

### Documentation
- Absolute README links for PyPI: every link in `README.md` that pointed at
  a file in the repository (`docs/`, `CHANGELOG.md`, `LICENSE`, `examples/`)
  is now an absolute GitHub URL, because PyPI renders the README verbatim and
  does not rewrite relative links.
- `docs/api.md` notes that the server records the driver name and version
  from the Hello frame asynchronously, so a `SELECT` on the `drivers` table
  right after connecting may not show the row yet.
- No code changes.

## [1.0.0] - 2026-09-19

First release as a standalone package on PyPI (`pip install skaidb`). The
driver previously lived in the skaidb monorepo under `drivers/python`; its
history is carried over.

### Added
- DB-API 2.0 (PEP 249) surface: `connect()`, `Connection`, `Cursor`,
  `qmark` parameters, `fetchone/fetchmany/fetchall`, iteration, `description`,
  `rowcount`, `nextset()` for multi-result-set `CALL`s.
- SCRAM-SHA-256 handshake with mutual authentication; anonymous connections.
- Typed parameter binding over server-side prepared statements (arrays and
  nested documents bind natively), per-connection prepared-statement cache,
  client-side text-binding fallback for statement kinds the server will not
  prepare.
- `executemany()` in one round-trip (`ExecuteBatch`), with a per-row fallback
  on older servers.
- `Connection.stream()` / `RowStream`: chunked result streaming with the
  drain-on-abandon rule.
- `Connection.subscribe()`: poll a `CREATE STREAM` log as an event iterator.
- Multi-seed failover (`seeds=`), `database=`, separate dial/read timeouts,
  `is_usable()`, `ping()`, `reconnect()`.
- TLS (`tls=`, `tls_ca=`, `tls_insecure=`, `tls_server_name=`).
- `ConnectionPool` / `skaidb.pool()`.
- Tunable consistency (`ONE`, `QUORUM`, `ALL`) per connection and per cursor.
- Hello frame: the driver reports its name and package version to the server.

### Changed
- The version is defined once, in `pyproject.toml`; `skaidb.__version__` is
  read from the installed metadata, so the Hello frame always carries the
  real package version.
- PEP 639 license metadata (`SSPL-1.0`), `py.typed`, complete project URLs.
- `InterfaceError` and `__version__` are exported in `__all__`.

[1.0.2]: https://github.com/porcupin26/skaidb-python/releases/tag/v1.0.2
[1.0.1]: https://github.com/porcupin26/skaidb-python/releases/tag/v1.0.1
[1.0.0]: https://github.com/porcupin26/skaidb-python/releases/tag/v1.0.0
