# Changelog

All notable changes to the skaidb Python driver. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/).

## [1.5.2] - 2026-11-16

### Fixed
- **A transaction begun with a `BEGIN` statement is no longer carried onto a
  new connection.** Only `conn.transaction()` kept the automatic reconnect
  away from its transaction. After `conn.execute("BEGIN")`, a lost
  connection was replaced like any other: the rest of the transaction ran on
  a new session, each statement committing on its own, and the `COMMIT` was
  answered `no transaction in progress` — half a transaction applied. The
  connection now notes a transaction, from a `BEGIN` the server accepted
  (`BEGIN`, `BEGIN TRANSACTION`, `BEGIN ATOMIC …`) until `COMMIT` or
  `ROLLBACK` is answered, and does not re-dial while one is open. When the
  connection is lost, the statement under which it is lost raises
  `OperationalError`, and so does every later one, without being sent. The
  application ends the transaction where it is: `ROLLBACK` succeeds without
  being sent — the server discarded the transaction with the connection —
  and `COMMIT` raises an `OperationalError` that says nothing was committed.
  A connection lost under the `COMMIT` itself is an unknown outcome, and
  that error says so instead. After either, the next statement re-dials as
  usual.
- **`conn.commit()` and `conn.rollback()` end the transaction a `BEGIN`
  statement opened.** Both sent nothing: after `conn.execute("BEGIN")`,
  `conn.commit()` returned without an error and the transaction stayed open
  — no other connection ever saw its rows, and they were gone when the
  connection closed. With a transaction open they now send `COMMIT` /
  `ROLLBACK` and raise what the statement raises; with none open they are
  accepted and send nothing, so calling them as a matter of routine costs no
  round trip.
- **`SET CONSISTENCY` and `SET SCAN BUDGET` survive a re-dial.** Like the
  session database, they live on the connection, and a new connection has
  neither: after `SET SCAN BUDGET ROWS 1` a scan was refused, the member
  restarted while the client was idle, and the same scan ran — the limit
  silently gone; after `SET CONSISTENCY ALL` the statements read at the
  request's own level. The connection now notes the last of each the server
  accepted (one statement, any letter case, an optional `;`, `--` comments
  skipped) and sends it again on every later dial, after the database and
  before the statement; `… DEFAULT` clears the note. A setting refused on
  the new connection fails the statement with that error: nothing runs
  under other settings than the application set.
- **A `USE` is not held hostage by the database it leaves.** The connection
  re-entered its noted database on every re-dial — also when the statement
  that re-dialed was itself a `USE`. Once that database had been dropped,
  every statement failed at the re-dial (rightly: it must not run
  elsewhere), `USE other` included, and the only way out was a new
  connection. A re-dial made for a `USE` now sends the noted settings and
  then the `USE`, whose answer decides the session database; the same for a
  `SET` and the setting it replaces. If that statement is refused, the
  connection is marked broken rather than left in the default database.
- **A connection the server closed while it was idle costs no statement.** A
  member's restart, a failover or an idle timeout closes idle connections.
  Over a real network the next write to such a socket still succeeds and
  only the read fails, after the statement left: a stream raised
  `OperationalError`, a statement under `auto_reconnect=False` failed
  although nothing was wrong with it, a retried write could run twice, and
  a frame nobody asked for on the idle socket was read as the next
  statement's reply. Before a statement's first request the driver now
  looks at the socket — a readiness poll that does not wait, no round trip;
  under TLS the transport's socket — and replaces a connection that is
  closed, reset or holds stray bytes before anything is written to it.
  `is_usable()` takes the same look, so a pool no longer hands out a
  connection its member closed: the checkout gives a working one, with or
  without `auto_reconnect`. Inside a transaction the loss is reported, as
  above.
- **A dial whose `Hello` is not answered fails, and the next endpoint is
  tried.** A transport failure under the `Hello` was ignored, and
  `connect()` returned the connection as usable. With the reply late or
  missing past the read timeout, the first statement was written and then
  failed on the read: under `auto_reconnect` it was sent again elsewhere —
  an `UPDATE … SET n = n + 1` reached two members — and without it the
  caller got an error for a statement the member had received. The same
  now holds for the `USE` and the settings that put a session back: a
  member lost under them is one more endpoint that did not work, not the
  end of the dial. (What a server answers is unchanged: an old server's
  error to `Hello` is ignored, and a database that does not exist ends the
  dial.)
- **A transaction conflict in a batch row is a `TransactionConflict`.** The
  error of an `executemany` batch starts with the failing row's name —
  `batch row 1: <error> (1 rows applied before it)` — so a conflict there
  did not match the `transaction conflict:` prefix and stayed a plain
  `ProgrammingError`. The row's own error is now what is read; the message
  is unchanged.

### Changed
- **A pool hands every borrower a connection in the pool's own session
  state:** in the pool's database (`database=`, or `default` without it),
  with no consistency override, no scan budget and no transaction open.
  Before, a borrower's `SET CONSISTENCY` and `SET SCAN BUDGET` stayed on
  the connection for the next borrower — which read at another consistency,
  or was refused under a scan limit it never set — and so did a transaction
  left open. What a borrower changed is put back when its connection comes
  back (`ROLLBACK`, `USE`, `SET SCAN BUDGET DEFAULT`, `SET CONSISTENCY
  DEFAULT` — only the ones needed; a borrower that changed nothing costs no
  round trip), and a connection that cannot be put back is closed instead
  of pooled. **Upgrade note:** on a pool without `database=`, a borrower's
  `USE` no longer carries over to later borrowers — every checkout starts
  in `default`. Give the pool its `database=`, or send `USE` at the start
  of each checkout. `SET CONSISTENCY DEFAULT` needs server 0.349: on an
  older server a connection whose borrower set a consistency is closed when
  it comes back, and the next borrower gets a new one.
- `conn.rollback()` no longer raises when no transaction is open: there is
  nothing to roll back, and the server answers a `ROLLBACK` with nothing
  open the same way. (It raised `OperationalError` in every state, also as
  the clean-up after a failed `commit()`, where it replaced the error the
  caller had to see.) Inside `transaction()` it still raises, and
  `commit()` still sends nothing there: the helper ends its transaction.
- `conn.reconnect()` raises `ProgrammingError` in the middle of a
  transaction begun with a `BEGIN` statement, as it does inside
  `transaction()`: end the transaction first. Once the connection is lost
  it is accepted, and ends the lost transaction as it dials — run the
  transaction again from `BEGIN`.
- When the body of `transaction()` swallows the loss of its connection and
  returns, the error of the helper's unsent `COMMIT` now says that nothing
  was committed.

### Documentation
- The failure table of `docs/tls.md` was checked against servers serving
  TLS, and three of its four rows corrected: TLS dialed to a plaintext port
  is `EOF occurred in violation of protocol`, not `WRONG_VERSION_NUMBER`
  (which is TLS dialed to a plaintext REST port); plaintext dialed to a
  server that requires TLS is `Connection reset by peer`; `bad handshake
  challenge` is neither of the causes it was given. The refusals of the
  certificate login are listed.
- "Session state" (README, `docs/api.md`) and "Session state and the pool"
  (`docs/pooling.md`) replace the sections on the session database;
  transactions written by hand, `commit()` / `rollback()` and the look at
  an idle socket are described.
- A `RowStream` that is just dropped is closed when the interpreter
  collects it — at once on CPython, later on PyPy (`docs/streaming.md`).

## [1.5.1] - 2026-11-16

### Fixed
- **A statement interrupted between its request and its reply no longer
  hands that reply to the next statement.** A `KeyboardInterrupt`, a
  `SystemExit` or any other exception raised by a signal handler, a green
  thread's timeout — anything but a socket error — left the reply unread on
  a connection that still looked healthy, and replies carry no request id:
  the next statement got the interrupted one's rows, and every statement
  after it the reply of the one before. Whatever stops a round trip now
  marks the connection broken and closes its socket, then propagates
  unchanged; the next statement re-dials as after any transport failure.
  This covers `execute()`, `executemany()`, `stream()` and its iteration
  (including an interrupt while a chunk is decoded, which used to skip the
  chunk's rows), the drain in `RowStream.close()`, and a `reconnect()`
  interrupted half way through the handshake. `stream()` also no longer
  releases the connection as healthy when the header frame cannot be parsed.
- **The session database survives a re-dial when a `USE` statement set
  it.** Only the `database=` given to `connect()` was entered again on a new
  connection. After `conn.execute("USE app")` and a failover — the member
  restarted while the client was idle — the new session was in no database,
  and the next statement ran against the default database's table of the
  same name. The connection now remembers the database of the last `USE` the
  server accepted (`USE [DATABASE] name`, the name bare or in double quotes,
  an optional `;`; `--` comments around it are skipped, as the server skips
  them, so `USE app -- the app database` counts too) and enters it on every
  later dial before anything else is sent.
- **A re-dial that does not finish leaves the connection broken.** When the
  server refused the `USE` on a new connection (the database dropped
  meanwhile), the error was raised once and the connection then stayed open
  in the default database, where the following statements ran. It is now
  closed, and each statement fails with the server's error until the
  database can be entered: a statement never runs in another database. The
  same holds for a `reconnect()` with no endpoint to reach, after which
  `is_usable()` stayed `True`.
- **A connection marked broken closes its socket at once**, not at the next
  statement or at `close()`. After a read timeout the server is alive and
  kept the session — with a transaction open in it, or the rest of a stream
  it was still sending — for as long as the application held the connection
  unused. The same for a `RowStream` left open by the body of
  `transaction()`: its transaction could be neither committed nor rolled
  back, and stayed open on the server.
- **A `RowStream` reads only the socket it was opened on.** After
  `conn.reconnect()` with a stream still alive, closing or dropping the old
  stream drained the NEW socket — it waited out the read timeout and marked
  the healthy connection broken, or swallowed the rows of a stream opened
  since — and iterating it yielded the new statement's rows. A stream whose
  connection was closed or re-dialed now raises `OperationalError` once the
  rows it had already received are used up, and closing it does nothing.
- **A cached prepared statement is never sent by an id of an earlier
  connection.** The first parameterised statement after a failure the driver
  does not retry on the spot (a stream that lost its connection, a statement
  inside `transaction()`, an abandoned stream too long to drain) failed with
  `unknown prepared statement id N`: it re-dialed, then sent the id it had
  cached on the connection that was gone. The cache now goes with the
  socket, and the statement is prepared again before it runs.
- **The server's prepared-statement table no longer fills after parameter
  errors.** A statement beyond the 240 the connection caches is closed after
  its one use — except when it was given the wrong number of parameters,
  which left its slot taken on the server. Sixteen of those and every
  statement the connection had not cached was refused with `too many
  prepared statements (max 256); close some first`.
- **A statement beyond the prepared-statement cache fails over like any
  other.** When its connection failed, closing the statement re-dialed and
  the failed statement was then not retried: `execute()` and `executemany()`
  raised `OperationalError` on a failover every cached statement survives.
- **`conn.reconnect()` inside `transaction()` no longer applies half a
  transaction.** A body that answered a lost connection by re-dialing ran
  its remaining statements on a session with no transaction, each
  committing on its own, while the statements before the loss were gone;
  the helper then sent its `COMMIT` there. `reconnect()` now raises
  `ProgrammingError` while `transaction()` has its transaction open: the
  error ends the transaction, and the caller runs it again.
- **`conn.rollback()` no longer says the database has no transactions.** It
  raised "skaidb does not support rollback (auto-commit only)". The DB-API
  `commit()` and `rollback()` still send nothing — `commit()` is accepted,
  `rollback()` raises `OperationalError` — and now say so, and name what
  ends a transaction: `conn.transaction()`, or `conn.execute("COMMIT")` /
  `conn.execute("ROLLBACK")`.

### Changed
- A pool made with `database=` hands every connection out in that database.
  One a borrower left in another database with a `USE` is put back with one
  round trip at its next checkout (or replaced if that fails); before, the
  next borrower ran in the last borrower's database. A pool made without
  `database=` has none to restore: a connection stays where its last
  borrower took it.

### Documentation
- `docs/api.md` shows `connect()` with `tls_client_cert`, `tls_client_key`,
  `auth_mechanism` and `auto_reconnect`, and `stream()` with `params`;
  `docs/pooling.md` describes the retry the driver does on its own
  (`auto_reconnect`) instead of saying there is none; the README no longer
  says the driver presents no client certificate. New sections describe
  broken connections and the session database. `docs/changelog.md` points
  to this file.

## [1.5.0] - 2026-11-15

### Added
- **`conn.transaction(body, attempts=3, begin="BEGIN")`** runs a transaction
  on one connection: it sends `begin`, calls `body(conn)`, sends `COMMIT`
  and returns what the body returned. A body that raises gets a `ROLLBACK`
  — whatever the `ROLLBACK` itself answers is ignored — and its exception
  back unchanged. A `COMMIT` the server refuses for a conflict runs the
  whole transaction again from `begin`, at once, up to `attempts` runs in
  all, so the body must be safe to run more than once. Nothing else is
  retried: a refused `begin` (the body then never runs), an exception from
  the body, any other `COMMIT` error and a transport failure are raised as
  they are. `begin` is sent verbatim (`BEGIN ATOMIC`, `BEGIN ATOMIC
  PARTITION (col = lit)` on a cluster). While the transaction is open the
  connection does not re-dial, whatever `auto_reconnect` says: on a fresh
  connection a retried statement would run outside the transaction.
  `pool.transaction(body, ...)` checks out one connection for the whole
  transaction and returns it afterwards.
- **`skaidb.TransactionConflict`**, a `ProgrammingError` subclass raised for
  a server error whose message starts with `transaction conflict:` — a
  `COMMIT` refused because another writer changed a row the transaction
  writes after its `BEGIN` (standalone server >= 0.346). It is raised on
  every statement path with the server's message unchanged, so code that
  catches `ProgrammingError` keeps working. It is a statement error: the
  connection stays usable and is not re-dialed, and a pool keeps the
  connection and counts nothing against its member.

## [1.4.1] - 2026-10-31

### Fixed
- One slow round trip at the start can no longer get a healthy member
  ejected for latency. A member's first round trip set its average
  outright, so a statement that stalled in the client — a CPU-bound
  application holding the thread while it starts — put seconds into it;
  ten round trips later the member was compared with the others, and the
  average had barely decayed: ejected (`latency 4153.7 ms vs median
  0.2 ms`) while it answered in milliseconds. The fastest of a member's
  first three round trips now seeds its average (a round trip is only ever
  lengthened), and later ones move it as before. Until then the member is
  not measured: `pool.members()` reports its `latency_ms` as `None` until
  its third round trip.
- A latency ejection's warning names the slowest of the member's last 10
  round trips alongside its average and the median, which tells a slow
  member from one slow statement.

## [1.4.0] - 2026-10-29

### Fixed
- A streamed `CALL` whose procedure body `EMIT`s result sets returns them.
  The server answers such a call with every set in one `ResultSets` frame
  (tag 8) even to a stream request, and `conn.stream()`, with or without
  parameters, refused it with `InterfaceError` ("this statement returns
  multiple result sets, which stream() cannot carry"). The frame is now
  decoded by the code `Cursor.execute()` uses: `stream()` returns an
  already-finished `RowStream` on the first set, and the new
  `RowStream.nextset()` walks the rest exactly as `Cursor.nextset()` does
  (`True` while another set is current, `None` after the last; always
  `None` for a streamed result). The connection is free at once.

## [1.3.0] - 2026-10-29

### Added
- **Parameterised streaming**: `conn.stream(sql, params)` binds `?`
  placeholders exactly as `Cursor.execute(sql, params)` does, over
  `OP_QUERY_STREAM_PARAMS` (server >= 0.330) — a range-bounded export no
  longer needs its literals interpolated into the SQL. Without parameters
  the request is the original `OP_QUERY_STREAM`, so it works against every
  server; `stream(sql, "ONE")` (a consistency passed positionally) still
  means the consistency.

## [1.2.0] - 2026-10-25

### Added
- **Pool load balancing across members** (the rules every skaidb driver
  shares, PROTOCOL.md §6.1): `load_balancing="round_robin"` /
  `"least_latency"` (power of two choices on the round-trip average ×
  checked-out connections; `"random"` stays the default), `idle_order="fifo"`,
  health ejection (consecutive transport failures, or a latency average far
  above the other members') with a background probe that re-admits the
  member, `discover=True` (members from `SHOW ENDPOINTS`, server ≥ 0.327:
  joiners added, leavers dropped, resyncing/unreachable members skipped), and
  `read_your_writes=<seconds>` with `getconn(affinity=key)` /
  `connection(affinity=key)` — reads follow the member that took the key's
  last write. `pool.members()` reports each member's state, latency and
  connections.

### Changed
- A pooled connection that re-dials does so in the pool's order (usable
  members first, the one it lost last), and the pool builds the TLS context
  once instead of per connection.

## [1.1.1] - 2026-09-26

### Fixed
- Documentation: the automatic-reconnect note said a retried `INSERT` on an
  existing primary key fails. A plain `INSERT` replaces that row (upsert),
  so the retry is harmless; the statements that can apply twice are the
  ones that compute from the current row (`SET n = n + 1`, `ON CONFLICT DO
  UPDATE` increments) and sequence-keyed inserts. No code changes.

## [1.1.0] - 2026-09-26

### Added
- **Automatic reconnect** (`connect(auto_reconnect=True)`, the default): a
  statement whose connection fails in transit re-dials and runs once more,
  and a broken connection re-dials before its next statement. Re-dials try
  the other endpoints before the one that failed — a member going down in a
  rolling restart used to accept the fresh dial and fail it again, ending a
  long `executemany` backfill. `executemany` re-prepares on the new
  connection. At-least-once on failover; `auto_reconnect=False` restores the
  old behaviour.
- **Certificate login**: `auth_mechanism="certificate"` with
  `tls_client_cert` / `tls_client_key` authenticates with the TLS client
  certificate (wire mechanism EXTERNAL; its Common Name is the user). The default
  user asserts no name; pass `user` to require a specific Common Name.
- The shared **wire-protocol conformance suite**
  (`conformance/vectors.json`, generated from the server's reference
  encoders and published at https://skaidb.org/conformance/vectors.json)
  runs in CI through the public API against a fake server that sends the
  reference bytes. CI also fails when the vendored copy differs from the
  published one.

### Fixed
- `Decimal` values wider than 28 digits decoded rounded (the default
  `decimal` context precision): they are now built exactly from the wire
  mantissa (up to 38 digits). Caught by the conformance suite.
- `tls=True` without `tls_ca` now loads the system trust store, as
  documented; before, the context had no CAs and every verification failed.

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
