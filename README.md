# skaidb — Python driver

[![PyPI](https://img.shields.io/pypi/v/skaidb.svg)](https://pypi.org/project/skaidb/)
[![CI](https://github.com/porcupin26/skaidb-python/actions/workflows/ci.yml/badge.svg)](https://github.com/porcupin26/skaidb-python/actions/workflows/ci.yml)
[![Python](https://img.shields.io/pypi/pyversions/skaidb.svg)](https://pypi.org/project/skaidb/)

The official Python driver for [skaidb](https://skaidb.org). A
[DB-API 2.0 (PEP 249)](https://peps.python.org/pep-0249/) driver: if you have
used `sqlite3` or `psycopg2`, you already know this API. **Pure standard
library** (`socket`, `ssl`, `hashlib`, `hmac`) — no dependencies, one module,
Python 3.8 through 3.14.

- Package: [`skaidb` on PyPI](https://pypi.org/project/skaidb/)
- Full reference: [`docs/`](https://github.com/porcupin26/skaidb-python/tree/main/docs) — [getting started](https://github.com/porcupin26/skaidb-python/blob/main/docs/getting-started.md),
  [API reference](https://github.com/porcupin26/skaidb-python/blob/main/docs/api.md), [TLS](https://github.com/porcupin26/skaidb-python/blob/main/docs/tls.md),
  [streaming](https://github.com/porcupin26/skaidb-python/blob/main/docs/streaming.md), [pooling](https://github.com/porcupin26/skaidb-python/blob/main/docs/pooling.md),
  [changelog](https://github.com/porcupin26/skaidb-python/blob/main/CHANGELOG.md)
- Server documentation: <https://skaidb.org/docs/>
- Wire protocol the driver speaks: <https://skaidb.org/docs/PROTOCOL.html>

## Install

```sh
pip install skaidb
```

## Connect and query

```python
import skaidb

conn = skaidb.connect(host="localhost", port=7000,
                      user="skaidb", password="secret", database="app")
cur = conn.cursor()

cur.execute("CREATE TABLE users (PRIMARY KEY (id))")
cur.execute("INSERT INTO users (id, name, tags) VALUES (?, ?, ?)",
            (1, "Ada", ["math", "eng"]))

cur.execute("SELECT id, name, tags FROM users WHERE id = ?", (1,))
print(cur.fetchone())      # (1, 'Ada', ['math', 'eng'])
print(cur.description)     # [('id', None, ...), ('name', None, ...), ('tags', ...)]

conn.close()
```

Connections and cursors are context managers, and cursors are iterable:

```python
with skaidb.connect(host="localhost", user="skaidb", password="secret") as conn:
    for row in conn.execute("SELECT id, name FROM users ORDER BY id"):
        print(row)
```

## Table of contents

- [Connecting](#connecting) — host/port, seeds and failover, session state
  (`database=`, `USE`, `SET`), timeouts, TLS, consistency, health checks
- [Statements and parameters](#statements-and-parameters) — `?` binding,
  prepared statements, type mapping, `executemany`
- [Cursors](#cursors) — fetch methods, `description`, `rowcount`, multiple
  result sets
- [Streaming large results](#streaming-large-results) — `RowStream` and the
  abandon/drain rule
- [Connection pool](#connection-pool)
- [Streams (`CREATE STREAM`)](#streams-create-stream) — `subscribe()`
- [Transactions](#transactions) — `transaction()`, conflicts and the retry,
  `BEGIN` … `COMMIT` by hand, `commit()` / `rollback()`, the two rules
- [Errors](#errors)
- [Thread safety](#thread-safety)
- [Compatibility](#compatibility)

## Connecting

```python
skaidb.connect(
    host="localhost", port=7000,
    user="anonymous", password="",
    consistency="QUORUM",         # "ONE" | "QUORUM" | "ALL" (or skaidb.Consistency.*)
    timeout=10.0,                 # default for both dial and read; None = no timeout
    database=None,                # run USE <database> as part of connecting
    seeds=None,                   # ["db1", "db2:7000", ...] tried in random order
    connect_timeout=None,         # override the dial timeout only
    read_timeout=None,            # override the read timeout only
    tls=False, tls_ca=None, tls_insecure=False, tls_server_name="skaidb",
    tls_client_cert=None, tls_client_key=None,  # present a client certificate
    auth_mechanism="scram",       # or "certificate": the client cert is the login
    auto_reconnect=True,          # re-dial + retry once after a transport failure
) -> skaidb.Connection
```

`connect()` dials, runs the SCRAM-SHA-256 handshake (with mutual
authentication — the server proves it knows your password too), sends a
`Hello` naming the driver and its version, and runs `USE` if a `database`
was given. A seed that fails anywhere on the way — the dial, the handshake,
an unanswered `Hello` — is passed over for the next one. Omit
`user`/`password` for a server with authentication disabled.

### Seeds and failover

skaidb is leaderless: every node accepts every read and write. Pass the
cluster's addresses as `seeds`; they are tried in **randomized order** until
one connects, which also spreads a fleet of clients across the nodes.

```python
conn = skaidb.connect(seeds=["db1", "db2:7000", "db3"], database="app")
```

Each seed is `"host"` or `"host:port"` (the port after the last colon wins;
for an IPv6 literal use `host=`/`port=` instead). `conn.reconnect()` re-dials
across the same seeds, re-authenticates and puts the connection's
[session state](#session-state) back, discarding its prepared statements.

**Automatic reconnect** (`auto_reconnect=True`, the default). When a node
goes away (a rolling restart, a crash), the statement whose connection
failed re-dials and runs once more, trying the OTHER seeds before the one
that just failed; a connection already broken re-dials before its next
statement. Prepared statements are re-prepared on the new connection, so
`executemany` survives a failover too, and the new connection gets the
[session state](#session-state) of the old one — its database, its
consistency override, its scan budget — before anything else is sent.

A connection the server closed while it sat **idle** — a restart, a
failover, an idle timeout — costs no statement at all: before a statement's
first request the driver looks at the socket (no round trip), and replaces
a connection that is closed, reset or holds bytes nobody asked for before
anything is written to it.

The retry after a failure in transit repeats the whole statement, so one
the old node had already applied can run twice (at-least-once on failover).
Most writes are idempotent — a plain `INSERT` replaces the row with the same
primary key, and `DELETE` or an `UPDATE` that sets fixed values lands the
same row twice — but a statement that computes from the current row does
not: `UPDATE … SET n = n + 1`, `INSERT … ON CONFLICT DO UPDATE SET n = n +
1`, or a row keyed by a sequence. Run those with `auto_reconnect=False` (or
make them idempotent). A stream cannot be retried mid-way, and nothing
re-dials while a [transaction](#transactions) is open — `conn.transaction()`'s
or one begun with a `BEGIN` statement: it lives on the connection that
failed. Pass `auto_reconnect=False` to handle `OperationalError` yourself.

**Certificate login.** With `auth_mechanism="certificate"` and
`tls_client_cert` / `tls_client_key`, the TLS client certificate is the
credential: its Common Name is the user (pass `user` only to assert it), and
no password is sent. The server needs `auth.x509_enabled` and a CA that
signed the certificate.

### Session state

A session on the server carries state beyond its statements: the database it
is in, a consistency override and a scan budget. All of it lives on the
connection, and a new connection has none — so the driver notes what the
server accepted and puts it back on every re-dial, before anything else is
sent.

```python
conn = skaidb.connect(host="db1", database="app")
conn.execute("USE archive")              # the connection is in `archive` from here on
conn.execute("SET CONSISTENCY ALL")      # every statement runs at ALL …
conn.execute("SET SCAN BUDGET ROWS 5000")  # … and may examine 5000 rows at most
```

- **The database** — the one given to `connect()`, then the one named by the
  last `USE` the server accepted. A `USE` is recognised when it is the whole
  statement: `USE [DATABASE] name`, the name bare or in double quotes (`""`
  for a quote inside).
- **`SET CONSISTENCY { ONE | QUORUM | ALL }`** — the last one the server
  accepted; `SET CONSISTENCY DEFAULT` (server >= 0.349) clears it.
- **`SET SCAN BUDGET …`** — the last one the server accepted; `SET SCAN
  BUDGET DEFAULT` clears it.

Each is read when it is exactly one statement, in any letter case, with an
optional trailing `;` — `--` comments around it are skipped, as the server
skips them. After a re-dial the new connection enters the database, then
gets the consistency override, then the scan budget. If one of them is
refused there, the statement fails with that error and the connection stays
broken: nothing runs in another database, at another consistency or past the
limit you set.

A `USE` is never held up by the database it leaves: when the statement that
re-dials is itself a `USE` (or a `SET` that replaces a setting), the driver
does not first put back what the statement is about to replace. So after the
session's database was dropped, every statement fails with the `USE` error —
and `USE other` gets out of it.

### Timeouts

`timeout` is the default for both the TCP dial and every read; `connect_timeout`
and `read_timeout` override each independently, so a read timeout can sit above
the server's statement timeout without slowing dial failures:

```python
conn = skaidb.connect(host="db1", connect_timeout=2, read_timeout=120)
```

`timeout=None` disables **both** bounds: a dial to a black-holed address and a
read from a live-but-silent peer will block forever. That includes the drain
an abandoned stream performs on `close()` (see [streaming](#streaming-large-results)).
A read timeout surfaces as `OperationalError` and marks the connection
broken, which closes its socket at once (see [health checks](#health-checks)).

### TLS

```python
conn = skaidb.connect(host="db1", tls=True)                     # system trust store
conn = skaidb.connect(host="db1", tls_ca="/etc/skaidb/ca.crt")  # the cluster CA
conn = skaidb.connect(host="db1", tls_insecure=True)            # dev only: no verification
```

Any of the three enables TLS. The server name presented as SNI and checked
against the certificate is `tls_server_name` (default `"skaidb"`, the SAN the
server's certificate carries). SCRAM runs inside the TLS session.
`tls_client_cert` / `tls_client_key` (PEM files) present a client
certificate — for a server that verifies its clients, and for the
certificate login above — and enable TLS as well. Details: [docs/tls.md](https://github.com/porcupin26/skaidb-python/blob/main/docs/tls.md).

### Consistency

skaidb has tunable consistency; the driver's default is `QUORUM`. Set it per
connection, and override per cursor or per stream:

```python
conn = skaidb.connect(host="db1", consistency="ONE")
cur = conn.cursor()
cur.set_consistency("ALL")                     # subsequent executes on this cursor
rows = conn.stream("SELECT ...", consistency="ONE")
```

`skaidb.Consistency.ONE / QUORUM / ALL` are the integer values `0 / 1 / 2`;
`Consistency.resolve()` accepts either form and raises `ValueError` otherwise.
DDL is always run at quorum by the server regardless of this setting.

### Health checks

- `conn.is_usable()` — no round-trip. `True` unless the connection is closed,
  was left out of sync by a transport error or an undrainable abandoned
  stream, is currently mid-stream, or was closed by the server while it sat
  idle (a look at the socket that does not wait; a connection found so is
  marked broken).
- `conn.ping()` — a real round-trip (`SHOW DATABASES`); `False` when it
  fails (a transport failure also marks the connection broken).
- `conn.reconnect()` — re-dial (failing over across seeds), re-authenticate
  and put the session state back. Refused while a transaction is open.
- `conn.closed` — `True` after `close()`.

A connection is marked **broken** when its socket is dead or out of step with
the server: a transport error or a read timeout, a close or stray bytes found
on it while it was idle, an abandoned stream too long to drain, a re-dial
that did not finish, or a statement stopped between its request and its
reply by anything else — a `KeyboardInterrupt`, an exception raised by a
signal handler — which propagates unchanged. The socket is closed
at that moment, not at the next statement: replies carry no request id, so
one left unread would answer the next statement, and until the socket closes
the server keeps the session and any transaction open in it. With
`auto_reconnect` the next statement re-dials first; without it, statements
raise `OperationalError` until `reconnect()`. Either way a connection that
broke with a [transaction](#transactions) open first has that transaction
ended by the application.

## Statements and parameters

Placeholders use `?` (`paramstyle = "qmark"`, like `sqlite3`). Pass parameters
as a tuple or list:

```python
cur.execute("SELECT * FROM users WHERE name = ? AND age > ?", ("O'Brien", 30))
```

### How binding works

A parameterized statement is **prepared on the server** and its values are
sent as typed values over the binary protocol. Nothing is interpolated into
SQL text, so `"O'Brien"` needs no escaping, and `?` can carry a `list`
(→ Array) or a `dict` (→ Document), which have no SQL literal form:

```python
cur.execute("INSERT INTO docs (id, meta) VALUES (?, ?)",
            (7, {"city": "London", "tags": ["a", "b"]}))
cur.execute("SELECT id FROM users WHERE id IN (?)", ([1, 2, 3],))   # set membership
```

Prepared statements are cached per connection (up to 240 entries, under the
server's 256-per-connection limit) and reused; past the cap a statement is
prepared, executed and closed again, so the server's table never fills. A
prepared statement belongs to the connection that prepared it: the cache is
dropped with the socket, and after a re-dial a statement is prepared again
before it runs.

Statement kinds the server refuses to prepare (DDL and session control such
as `USE`) fall back to **client-side text binding** for scalar parameters: a
string is quoted with `''` escaping, a `datetime` becomes epoch milliseconds,
`bytes` become a hex string literal, and lists/dicts raise `ProgrammingError`
on this path. Mismatched placeholder/parameter counts raise
`ProgrammingError` on either path.

### Type mapping

| skaidb value | Python → bind                          | Python ← result            |
|--------------|----------------------------------------|----------------------------|
| Null         | `None`                                 | `None`                     |
| Bool         | `bool` (checked before `int`)          | `bool`                     |
| Int          | `int` (must fit a signed 64-bit)       | `int`                      |
| Float        | `float` (finite only)                  | `float`                    |
| Decimal      | `decimal.Decimal` (finite, 128-bit mantissa) | `decimal.Decimal`    |
| String       | `str`                                  | `str`                      |
| Bytes        | `bytes`, `bytearray`                   | `bytes`                    |
| Uuid         | `uuid.UUID`                            | `uuid.UUID`                |
| Timestamp    | `datetime.datetime` (ms precision)     | `datetime.datetime` (UTC, tz-aware) |
| Array        | `list`, `tuple`                        | `list`                     |
| Document     | `dict` with `str` keys                 | `dict`                     |

Semantics worth knowing:

- An `int` outside the signed 64-bit range raises `struct.error` from the
  codec (not a `skaidb.Error`); an `int` subclass other than `bool` binds as Int.
- `NaN`/`Infinity` floats and non-finite `Decimal`s raise `ProgrammingError`.
  A `Decimal` whose digits exceed a signed 128-bit mantissa raises too; a
  positive exponent is folded into the mantissa (scale is unsigned).
- A **naive `datetime` is interpreted as local time** (Python's
  `astimezone()` rule); pass tz-aware values to be explicit. Timestamps are
  millisecond precision — microseconds are truncated. Results always come
  back tz-aware in UTC.
- Any other type raises `ProgrammingError("cannot bind value of type ...")`.
  Convert e.g. `date` or `Enum` values yourself.

### `executemany` — bulk writes in one round-trip

```python
cur.executemany("INSERT INTO t (id, v) VALUES (?, ?)", [(1, "a"), (2, "b"), (3, "c")])
print(cur.rowcount)   # total rows affected
```

The statement is prepared once and **every parameter row ships in a single
frame** (`ExecuteBatch`). Outside a transaction each row commits on its own:
on a failure the `ProgrammingError` names the row (`batch row 1: … (1 rows
applied before it)`) and earlier rows stay applied. Servers
without the batch opcode, and statements that cannot be prepared, fall back to
a per-row loop automatically.

## Cursors

`Cursor` follows PEP 249:

- `execute(sql, params=None) -> Cursor` (returns itself, so
  `conn.execute(...).fetchall()` works — `Connection.execute()` is a shortcut
  that creates a cursor).
- `fetchone()`, `fetchmany(size=cursor.arraysize)`, `fetchall()`, iteration.
- `description`: a list of 7-tuples, `(name, None, None, None, None, None, None)`
  — only the column name is populated (results are dynamically typed).
  `None` for non-row statements.
- `rowcount`: the number of rows for a row-returning statement, rows
  affected for `INSERT`/`UPDATE`/`DELETE`, `-1` for DDL.
- `nextset()`: a `CALL` whose procedure `EMIT`s several result sets returns
  them all; the first is current after `execute()`, `nextset()` advances and
  returns `True`, or `None` when there are no more.
- `set_consistency(level)`, `close()`, context-manager support.

A cursor holds its whole result in memory; for results that do not fit,
use [streaming](#streaming-large-results).

## Streaming large results

`conn.stream(sql, params=None, consistency=None)` runs the statement over the streaming
opcode and returns a `RowStream` — an iterator that holds **one chunk** of
rows at a time. Column names arrive in the header, before any row:

```python
with conn.stream("SELECT id, pad FROM big ORDER BY id") as rows:
    print(rows.columns)              # ['id', 'pad']
    for row in rows:
        if enough(row):
            break                    # leaving the block drains the tail
```

`params` bind `?` placeholders exactly as `Cursor.execute` does (server >=
0.330; a parameterless stream works against any server). A non-row statement returns
an already-finished, empty stream whose `affected` holds the mutation count. A `CALL`
whose body `EMIT`s result sets is answered with all of them in one frame even when
streamed: the stream starts on the first set and `rows.nextset()` walks the rest, as
`Cursor.nextset()` does.

**The abandon/drain rule.** The connection is busy for the whole stream: any
other statement on it raises `ProgrammingError` until the stream ends (by
reaching the end, by `close()`, or by being garbage-collected). `close()`
drains the frames the server has already queued so the socket is left at a
request boundary — but only up to **64 frames**. Past that, the remainder is
big enough that draining costs more than a reconnect, so the connection is
**marked broken** instead and its socket closed: `is_usable()` turns `False`,
the next statement re-dials (without `auto_reconnect` it raises
`OperationalError`), and a pool discards the connection. Rule of thumb:
iterate to the end, or stop early inside a `with` block and expect the
connection to be recycled if a lot was still in flight. A stream belongs to
the socket it was opened on: once the connection has been closed or re-dialed
it raises `OperationalError` after the rows it had already received, and
closing it reads nothing. Details: [docs/streaming.md](https://github.com/porcupin26/skaidb-python/blob/main/docs/streaming.md).

## Connection pool

```python
pool = skaidb.pool(seeds=["db1", "db2", "db3"], database="app", maxsize=8)

with pool.connection() as conn:          # checked out, returned on exit
    conn.execute("SELECT ... WHERE id IN (?)", ([1, 2, 3],))

conn = pool.getconn(); ...; pool.putconn(conn)   # the explicit form
pool.close()                             # closes idle connections
```

`ConnectionPool` is thread-safe and accepts every `connect()` keyword, plus
load balancing across members: `load_balancing="round_robin"` or
`"least_latency"`, `discover=True` (members from `SHOW ENDPOINTS`, resyncing
ones skipped), health ejection with background re-probe, `idle_order="fifo"`
and `read_your_writes=<seconds>` with `pool.connection(affinity=key)`;
`pool.members()` shows what it sees.
`maxsize` bounds the number of **idle** connections retained; checkout never
blocks — when no idle connection is available a new one is dialed, and a
returned connection beyond `maxsize` is closed. Connections are validated with
`is_usable()` on checkout and check-in, so one broken by a transport error or
an undrained stream, or closed by its member while it sat idle, is closed
and replaced transparently. Every borrower gets a connection in the pool's
own [session state](#session-state): in the pool's database (`database=`, or
`default` without it), with no consistency override, no scan budget and no
transaction open — what a borrower changed is put back when its connection
comes back. Details:
[docs/pooling.md](https://github.com/porcupin26/skaidb-python/blob/main/docs/pooling.md).

## Streams (`CREATE STREAM`)

`conn.subscribe(name, after=None, poll=0.5)` yields a stream's events forever
as dicts with `id`, `op`, `k`, `ts`, `doc`. It polls the stream's log with a
keyset cursor (500 events per page, sleeping `poll` seconds when caught up),
so it needs no MQTT client. `id` is the position: persist the last one you
handled and pass it as `after=` to resume exactly there.

```python
for ev in conn.subscribe("big_orders", after=checkpoint):
    handle(ev["doc"])
    checkpoint = ev["id"]
```

For push delivery, subscribe to `$stream/<db>/<name>` with any MQTT client
instead; the events are identical.

## Transactions

A transaction is ordinary statements on one connection: `BEGIN`, the
statements, then `COMMIT` or `ROLLBACK`. Outside a transaction every
statement autocommits. `conn.transaction(body, attempts=3, begin="BEGIN")`
runs the whole sequence and returns what the body returned:

```python
def move(conn):
    conn.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?", (10, 1))
    conn.execute("UPDATE accounts SET balance = balance + ? WHERE id = ?", (10, 2))
    return conn.execute("SELECT balance FROM accounts WHERE id = ?", (1,)).fetchone()[0]

balance = conn.transaction(move)          # BEGIN, move(conn), COMMIT
```

- If the body raises, `ROLLBACK` is sent and the body's exception propagates
  unchanged; whatever the `ROLLBACK` itself answers is ignored.
- If the server refuses the `COMMIT` for a conflict (below), the whole
  transaction runs again from `BEGIN` — at once, with no pause — up to
  `attempts` runs in all, and the last run's conflict is raised. **The body
  must be safe to run more than once**, and should do nothing outside the
  database that a retry would repeat.
- Nothing else is retried: an exception from the body (a conflict included),
  any other `COMMIT` error and a transport failure are raised as they are.
  `attempts` below 1 raises `ValueError` before anything is sent.
- `begin` is the opening statement, sent verbatim: plain `BEGIN` on a
  standalone server, `BEGIN ATOMIC` or `BEGIN ATOMIC PARTITION (col = lit)`
  on a cluster.
- Finish or close any stream the body opens before it returns. With one
  still open neither `COMMIT` nor `ROLLBACK` can be sent: the call raises,
  and the connection's socket is closed, which is what ends the transaction
  on the server.

`pool.transaction(body, attempts=3, begin="BEGIN")` does the same from a
[pool](#connection-pool): it checks out one connection for the whole
transaction and returns it afterwards.

**Conflicts.** A standalone server (>= 0.346) refuses a `COMMIT` when another
writer changed a row the transaction writes after its `BEGIN`. Nothing was
applied, the transaction is over, and the connection is back in autocommit
and stays usable, so running the transaction again from `BEGIN` is safe. The
driver raises this as `skaidb.TransactionConflict` — a `ProgrammingError`,
so code that catches that keeps working — for any server error that starts
with `transaction conflict:`; the message is the server's.

The same transaction written by hand:

```python
conn.execute("BEGIN")                     # if this raises, stop here
try:
    conn.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?", (10, 1))
    conn.execute("UPDATE accounts SET balance = balance + ? WHERE id = ?", (10, 2))
except BaseException:
    try:
        conn.execute("ROLLBACK")
    except skaidb.Error:
        pass                              # keep the error that brought us here
    raise
try:
    conn.execute("COMMIT")
except skaidb.TransactionConflict:
    ...                                   # nothing was applied: run it again from BEGIN
```

`conn.commit()` and `conn.rollback()`, the DB-API methods, end a transaction
begun that way: with one open they send `COMMIT` / `ROLLBACK` and raise what
the statement raises; with none open they are accepted and send nothing —
every statement has committed on its own — so calling them as a matter of
routine costs nothing. (Inside `transaction()` the helper ends its
transaction: `commit()` sends nothing there, and `rollback()` raises — a
body rolls back by raising.)

Two rules hold for both forms:

- **One connection for the whole transaction.** The transaction belongs to
  the connection that sent `BEGIN`; a statement on any other connection is
  outside it and commits on its own. From a pool, keep one checkout for the
  duration (`pool.transaction(body)`, or one `with pool.connection() as conn:`
  block) and use only the connection you were given. The automatic reconnect
  would break this rule — a statement retried on a fresh connection runs
  outside the transaction — so the connection notes a transaction, from a
  `BEGIN` the server accepted until `COMMIT` or `ROLLBACK` is answered, and
  does not re-dial while one is open (nor accept `conn.reconnect()`:
  `ProgrammingError`). That holds for `transaction()` and for a `BEGIN` you
  send yourself alike.
- **Stop if `BEGIN` fails.** When the opening statement raises, no
  transaction is open and every statement after it would commit on its own.
  `transaction()` raises that error without calling the body.

**When the connection is lost inside a transaction**, the transaction is
gone with it — the server discards it — and nothing of it was committed.
The statement under which it was lost raises `OperationalError`, and so does
every statement after it, unsent: on a new connection each would commit on
its own. `transaction()` reports the loss and is done. A transaction written
by hand is ended where it is: `ROLLBACK` (or `conn.rollback()`) succeeds
without being sent, `COMMIT` (or `conn.commit()`) raises an
`OperationalError` saying that nothing was committed. After either, the next
statement re-dials as usual. The one case with an unknown outcome is a
connection lost under the `COMMIT` itself — the server may have applied it
before its reply went missing — and that error says so instead.

## Errors

```
Exception
└── skaidb.Error
    ├── skaidb.InterfaceError        # malformed/unexpected frames
    └── skaidb.DatabaseError
        ├── skaidb.OperationalError  # dial/auth/transport failures, broken connection,
        │                            # a connection lost inside a transaction
        └── skaidb.ProgrammingError  # the server rejected the statement; bad parameters;
            │                        # closed connection; statement while streaming;
            │                        # reconnect() with a transaction open
            └── skaidb.TransactionConflict  # the server refused a COMMIT: another
                                            # writer changed a row the transaction writes
```

- A server `Error` frame is a **statement** error (`ProgrammingError`); the
  connection stays usable. One whose message starts with `transaction
  conflict:` is a `TransactionConflict` (see [transactions](#transactions)) —
  in a batch, where the server names the failing row first, the row's error
  is what is read.
- A transport failure (`OperationalError`) marks the connection broken and
  closes its socket. With `auto_reconnect` (the default) the failed statement
  re-dials and retries once; otherwise call `reconnect()` or let the pool
  replace it. Inside a transaction nothing re-dials: see
  [transactions](#transactions).
- Anything else that stops a statement between its request and its reply —
  a `KeyboardInterrupt`, a `SystemExit`, an exception raised by a signal
  handler — propagates unchanged and costs the connection in the same way:
  its reply is never read as the answer to a later statement.
- Outside the hierarchy: `ValueError` for an invalid consistency level,
  `maxsize < 1` or `attempts < 1`, and `struct.error` for an `int` beyond
  64 bits.

## Thread safety

`threadsafety = 1`: threads may share the module but not a connection. A
connection serializes its own round-trips with a lock, and a stream claims it
outright, but the intended shape for concurrency is one connection per thread,
which is what a [pool](#connection-pool) gives you.

## Compatibility

- Python 3.8 – 3.14, CPython and PyPy. No dependencies.
- Works with any skaidb server. Where a server predates an opcode the driver
  falls back: prepared statements → client-side text binding, `ExecuteBatch`
  → per-row execution, `Hello` → its refusal ignored. `stream()` needs a
  server with the streaming opcode and raises `ProgrammingError` otherwise.
  `SET CONSISTENCY DEFAULT` needs server 0.349: an older one refuses it, and
  a pool then closes a connection whose borrower set a consistency instead
  of handing it on.
- The wire protocol (framing, SCRAM handshake, value encoding, opcodes) is
  specified at <https://skaidb.org/docs/PROTOCOL.html>. The driver speaks the
  binary protocol on port 7000; the server's REST/JSON gateway (port 7080)
  is a dependency-free alternative for non-Python clients.
- `skaidb.__version__` is the installed package version and is what the
  driver reports to the server (visible in the server's `drivers` table).

## Examples

[`examples/`](https://github.com/porcupin26/skaidb-python/tree/main/examples) contains runnable scripts: `basic.py`, `streaming.py`,
`pool.py`, `tls.py`, `subscribe.py`. Each names its arguments in its
docstring; `basic.py` and `streaming.py` take `[host] [port] [user] [password]`
and default to `localhost:7000`.

```sh
python3 examples/basic.py localhost 7000 skaidb secret
```

## Development

```sh
git clone https://github.com/porcupin26/skaidb-python
cd skaidb-python
python -m pip install pytest
python -m pytest -q          # unit tests; no server needed
```

## License

[SSPL-1.0](https://github.com/porcupin26/skaidb-python/blob/main/LICENSE) (Server Side Public License), the same license as skaidb.
