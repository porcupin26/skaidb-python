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

- [Connecting](#connecting) — host/port, seeds and failover, `database=`,
  timeouts, TLS, consistency, health checks
- [Statements and parameters](#statements-and-parameters) — `?` binding,
  prepared statements, type mapping, `executemany`
- [Cursors](#cursors) — fetch methods, `description`, `rowcount`, multiple
  result sets
- [Streaming large results](#streaming-large-results) — `RowStream` and the
  abandon/drain rule
- [Connection pool](#connection-pool)
- [Streams (`CREATE STREAM`)](#streams-create-stream) — `subscribe()`
- [Transactions](#transactions)
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
) -> skaidb.Connection
```

`connect()` dials, runs the SCRAM-SHA-256 handshake (with mutual
authentication — the server proves it knows your password too), sends a
best-effort `Hello` naming the driver and its version, and runs `USE` if a
`database` was given. Omit `user`/`password` for a server with authentication
disabled.

### Seeds and failover

skaidb is leaderless: every node accepts every read and write. Pass the
cluster's addresses as `seeds`; they are tried in **randomized order** until
one connects, which also spreads a fleet of clients across the nodes.

```python
conn = skaidb.connect(seeds=["db1", "db2:7000", "db3"], database="app")
```

Each seed is `"host"` or `"host:port"` (the port after the last colon wins;
for an IPv6 literal use `host=`/`port=` instead). `conn.reconnect()` re-dials
across the same seeds and re-authenticates, discarding the connection's
prepared statements.

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
A read timeout surfaces as `OperationalError` and marks the connection broken.

### TLS

```python
conn = skaidb.connect(host="db1", tls=True)                     # system trust store
conn = skaidb.connect(host="db1", tls_ca="/etc/skaidb/ca.crt")  # the cluster CA
conn = skaidb.connect(host="db1", tls_insecure=True)            # dev only: no verification
```

Any of the three enables TLS. The server name presented as SNI and checked
against the certificate is `tls_server_name` (default `"skaidb"`, the SAN the
server's certificate carries). SCRAM runs inside the TLS session. The driver
does not present a client certificate. Details: [docs/tls.md](https://github.com/porcupin26/skaidb-python/blob/main/docs/tls.md).

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
  stream, or is currently mid-stream.
- `conn.ping()` — a real round-trip (`SHOW DATABASES`); `False` marks it broken.
- `conn.reconnect()` — re-dial (failing over across seeds) and re-authenticate.
- `conn.closed` — `True` after `close()`.

A broken connection refuses further statements with `OperationalError` rather
than retrying, because its socket may still hold someone else's unread frames.

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
prepared, executed and closed in one go. The cache is dropped on `reconnect()`.

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
frame** (`ExecuteBatch`). Each row autocommits on its own; on a failure the
`ProgrammingError` names the row index and earlier rows stay applied. Servers
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

`conn.stream(sql, consistency=None)` runs the statement over the streaming
opcode and returns a `RowStream` — an iterator that holds **one chunk** of
rows at a time. Column names arrive in the header, before any row:

```python
with conn.stream("SELECT id, pad FROM big ORDER BY id") as rows:
    print(rows.columns)              # ['id', 'pad']
    for row in rows:
        if enough(row):
            break                    # leaving the block drains the tail
```

`stream()` takes SQL text only (no parameters). A non-row statement returns
an already-finished, empty stream whose `affected` holds the mutation count.

**The abandon/drain rule.** The connection is busy for the whole stream: any
other statement on it raises `ProgrammingError` until the stream ends (by
reaching the end, by `close()`, or by being garbage-collected). `close()`
drains the frames the server has already queued so the socket is left at a
request boundary — but only up to **64 frames**. Past that, the remainder is
big enough that draining costs more than a reconnect, so the connection is
**marked broken** instead: `is_usable()` turns `False`, further statements
raise `OperationalError`, and a pool discards it rather than handing on a
socket with rows still queued. Rule of thumb: iterate to the end, or stop
early inside a `with` block and expect the connection to be recycled if a lot
was still in flight. Details: [docs/streaming.md](https://github.com/porcupin26/skaidb-python/blob/main/docs/streaming.md).

## Connection pool

```python
pool = skaidb.pool(seeds=["db1", "db2", "db3"], database="app", maxsize=8)

with pool.connection() as conn:          # checked out, returned on exit
    conn.execute("SELECT ... WHERE id IN (?)", ([1, 2, 3],))

conn = pool.getconn(); ...; pool.putconn(conn)   # the explicit form
pool.close()                             # closes idle connections
```

`ConnectionPool` is thread-safe and accepts every `connect()` keyword.
`maxsize` bounds the number of **idle** connections retained; checkout never
blocks — when no idle connection is available a new one is dialed, and a
returned connection beyond `maxsize` is closed. Connections are validated with
`is_usable()` on checkout and check-in, so one broken by a transport error or
an undrained stream is closed and replaced transparently. Details:
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

skaidb autocommits every statement. `conn.commit()` is an accepted no-op for
DB-API conformance; `conn.rollback()` raises `OperationalError`, since silently
dropping a rollback would be worse than refusing it. Where your server
supports statement-level transaction control, issue `BEGIN`/`COMMIT`/`ROLLBACK`
as ordinary statements; see the server documentation for what your
deployment supports.

## Errors

```
Exception
└── skaidb.Error
    ├── skaidb.InterfaceError        # malformed/unexpected frames; stream() of a multi-set CALL
    └── skaidb.DatabaseError
        ├── skaidb.OperationalError  # dial/auth/transport failures, broken connection, rollback()
        └── skaidb.ProgrammingError  # the server rejected the statement; bad parameters;
                                     # closed connection; statement while streaming
```

- A server `Error` frame is a **statement** error (`ProgrammingError`); the
  connection stays usable.
- A transport failure (`OperationalError`) marks the connection broken;
  call `reconnect()` or let the pool replace it.
- Outside the hierarchy: `ValueError` for an invalid consistency level or
  `maxsize < 1`, and `struct.error` for an `int` beyond 64 bits.

## Thread safety

`threadsafety = 1`: threads may share the module but not a connection. A
connection serializes its own round-trips with a lock, and a stream claims it
outright, but the intended shape for concurrency is one connection per thread,
which is what a [pool](#connection-pool) gives you.

## Compatibility

- Python 3.8 – 3.14, CPython and PyPy. No dependencies.
- Works with any skaidb server. Where a server predates an opcode the driver
  falls back: prepared statements → client-side text binding, `ExecuteBatch`
  → per-row execution, `Hello` → ignored. `stream()` needs a server with the
  streaming opcode and raises `ProgrammingError` otherwise.
- The wire protocol (framing, SCRAM handshake, value encoding, opcodes) is
  specified at <https://skaidb.org/docs/PROTOCOL.html>. The driver speaks the
  binary protocol on port 7000; the server's REST/JSON gateway (port 7080)
  is a dependency-free alternative for non-Python clients.
- `skaidb.__version__` is the installed package version and is what the
  driver reports to the server (visible in the server's `drivers` table).

## Examples

[`examples/`](https://github.com/porcupin26/skaidb-python/tree/main/examples) contains runnable scripts: `basic.py`, `streaming.py`,
`pool.py`, `tls.py`, `subscribe.py`. Each takes the host/port/user/password on
the command line and defaults to `localhost:7000`.

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
