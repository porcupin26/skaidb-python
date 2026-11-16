# API reference

Module `skaidb`, one file, no dependencies. Everything public is listed in
`skaidb.__all__`. The wire protocol these calls speak is specified at
<https://skaidb.org/docs/PROTOCOL.html>; the SQL dialect and server behaviour
at <https://skaidb.org/docs/>.

Contents: [module globals](#module-globals) · [`connect()`](#connect) ·
[`Connection`](#connection) · [`Cursor`](#cursor) · [`RowStream`](#rowstream) ·
[`ConnectionPool` / `pool()`](#connectionpool-and-pool) ·
[`Consistency`](#consistency) · [errors](#errors) ·
[parameters and type mapping](#parameters-and-type-mapping) ·
[limits and constants](#limits-and-constants)

## Module globals

| Name | Value | Meaning |
|------|-------|---------|
| `apilevel` | `"2.0"` | DB-API level |
| `threadsafety` | `1` | threads may share the module, not connections |
| `paramstyle` | `"qmark"` | `?` placeholders, positional |
| `__version__` | e.g. `"1.5.2"` | the installed package version (read from distribution metadata; falls back to the in-module literal when running from an uninstalled checkout). Reported to the server in the Hello frame. |

## `connect()`

```python
skaidb.connect(
    host: str = "localhost",
    port: int = 7000,
    user: str = "anonymous",
    password: str = "",
    consistency: int | str = Consistency.QUORUM,
    timeout: float | None = 10.0,
    database: str | None = None,
    seeds: Sequence[str] | None = None,
    connect_timeout: float | None = None,
    read_timeout: float | None = None,
    tls: bool = False,
    tls_ca: str | None = None,
    tls_insecure: bool = False,
    tls_server_name: str = "skaidb",
    tls_client_cert: str | None = None,
    tls_client_key: str | None = None,
    auth_mechanism: str = "scram",
    auto_reconnect: bool = True,
) -> Connection
```

Opens a connection and authenticates. What happens, in order: resolve the
endpoint list (`seeds`, or the single `host:port`) and shuffle it; for each
endpoint until one succeeds: TCP connect (with `TCP_NODELAY`), optional TLS
wrap, the SCRAM-SHA-256 handshake (or, with `auth_mechanism="certificate"`,
the certificate login), `Hello`, and `USE "<database>"` if `database` was
given. `Hello` carries telemetry and what the server answers is ignored (an
old server answers an error) — but it must answer: an endpoint whose reply
does not come, like one that fails anywhere else on the way, is passed over
for the next.

The server records the driver name and `__version__` carried by `Hello` in
its `drivers` table **asynchronously**: `Hello` is acknowledged before the
row is written, so a `SELECT` on the `drivers` table issued immediately after
`connect()` may not show the new connection yet. Poll briefly if a test or a
health check depends on the row.

| Parameter | Notes |
|-----------|-------|
| `host`, `port` | Used only when `seeds` is not given. |
| `user`, `password` | SCRAM credentials. `"anonymous"` / `""` for a server with auth disabled. The server's signature is verified (mutual auth) whenever a password is set. |
| `consistency` | `"ONE"`, `"QUORUM"`, `"ALL"` (case-insensitive) or `Consistency.ONE/QUORUM/ALL`. Default for every statement on the connection; cursors and streams can override. `ValueError` on anything else. |
| `timeout` | Default for both the dial timeout and the socket read timeout, in seconds. **`None` means no timeout at all**: a dial to a black-holed host and a read from a silent peer both block forever, including the drain an abandoned stream performs. |
| `connect_timeout`, `read_timeout` | Override the dial / read bound separately; each falls back to `timeout` when `None`. |
| `database` | Runs `USE "<database>"` (identifier-quoted) as part of connecting, and of every re-dial. A `USE` statement the server accepts later replaces it: see [session state](#session-state). |
| `seeds` | `"host"` or `"host:port"` strings tried in randomized order; the port after the **last** colon is used, bare names get `port`. IPv6 literals: use `host=`/`port=`. |
| `tls`, `tls_ca`, `tls_insecure`, `tls_server_name` | See [tls.md](tls.md). Any of the first three enables TLS. |
| `tls_client_cert`, `tls_client_key` | PEM files of a client certificate to present over TLS (enables TLS). Needed for `auth_mechanism="certificate"`. Giving one without the other raises `ProgrammingError`. |
| `auth_mechanism` | `"scram"` (default: `user`/`password`) or `"certificate"` (EXTERNAL: the client certificate's Common Name is the user; the server needs `auth.x509_enabled`). |
| `auto_reconnect` | Default `True`. After a transport failure the next statement re-dials first, and a statement whose connection fails in transit re-dials — trying the other endpoints before the failed one — and runs once more (whole statement, prepare included; at-least-once on failover: a plain `INSERT` replaces the same row, but `SET n = n + 1`-style or sequence-keyed writes can apply twice). `False` raises `OperationalError` instead. A stream is never retried, and nothing re-dials while a [transaction](#transactions) is open. A connection found closed while it sat idle is replaced before the statement is sent, so nothing runs twice: see [broken connections](#broken-connections). |

Raises `OperationalError` when no endpoint could be connected and
authenticated (the message lists every endpoint's failure), including
`authentication denied`, `server signature mismatch` and `no reply to
Hello`; `ProgrammingError` for an unknown `auth_mechanism`, or when the
server refuses the `USE` of `database` (what a server answers ends the dial:
it is the same on every member).

## `Connection`

A DB-API connection to one node. Do not construct it directly; use
`connect()` or a pool.

### Statements

```python
Connection.cursor() -> Cursor
Connection.execute(sql: str, params: Sequence | None = None) -> Cursor
```

`execute()` is a shortcut: new cursor, `cursor.execute(sql, params)`, return
the cursor. See [`Cursor.execute`](#cursorexecute).

### Streaming

```python
Connection.stream(sql: str, params: Sequence | None = None,
                  consistency: int | str | None = None) -> RowStream
```

Runs `sql` over the streaming opcode and returns a [`RowStream`](#rowstream).
`params` bind the statement's `?` placeholders as typed values, exactly as
[`Cursor.execute`](#cursorexecute) binds them (server >= 0.330; without
parameters the request is the original streaming opcode, which every server
with streaming knows). `consistency=None` uses the connection's level; a
consistency passed as the second positional argument, `stream(sql, "ONE")`,
is still read as the consistency. Outcomes by what the statement produced:

- rows: a live `RowStream` with `columns` filled from the header;
- a mutation or DDL: an already-finished, empty `RowStream` (`affected`
  holds the mutation's row count);
- a server error before any row: `ProgrammingError`; a server without the
  opcode: `ProgrammingError("server does not support streaming: …")` (or
  `"… parameterised streaming …"` when `params` were given);
- a multi-result-set reply (a `CALL` that `EMIT`s, answered in one frame
  even when streamed): an already-finished `RowStream` on the first set;
  `nextset()` walks the rest, as `Cursor.nextset()` does. The connection is
  free at once.

Raises `ProgrammingError` if the connection is already streaming. A stream
is never retried by the automatic reconnect: a transport failure raises
`OperationalError`.

### Streams (`CREATE STREAM`)

```python
Connection.subscribe(stream: str, after: str | None = None, poll: float = 0.5)
    -> Iterator[dict]
```

An infinite generator over a stream's log (`_stream_<name>`). Pages the log
with a keyset cursor, 500 events per query, and sleeps `poll` seconds when it
catches up. Each event is `{"id", "op", "k", "ts", "doc"}`; `id` is the
position — pass the last processed one as `after` to resume. Uses an ordinary
cursor underneath, so the connection is free between polls. For push delivery
use MQTT on `$stream/<db>/<name>` instead.

### Health and lifecycle

```python
Connection.is_usable() -> bool      # no round-trip
Connection.ping() -> bool           # round-trip (SHOW DATABASES)
Connection.reconnect() -> None      # re-dial across seeds, re-authenticate
Connection.close() -> None
Connection.closed: bool
```

- `is_usable()` is `False` when the connection is closed, **broken** or
  **mid-stream** (a `RowStream` owns it). It also looks at the idle socket,
  without waiting: a connection its peer has closed or reset, or one holding
  bytes nobody asked for, is marked broken there and then. Pools use it on
  checkout and check-in.
- `ping()` returns `False` when the round trip fails for any reason, and on
  a connection that is not usable to begin with; a transport failure also
  marks the connection broken.
- `reconnect()` drops the socket, re-dials — the other endpoints before the
  one it was connected to — re-authenticates, re-sends Hello, puts the
  [session state](#session-state) back and clears the prepared-statement
  cache. Raises `ProgrammingError` on a closed connection, and while a
  [transaction](#transactions) is open. If it does not finish — no endpoint,
  the `USE` or a setting refused, an interrupt — the connection is left
  broken.
- `close()` is idempotent. `with connect(...) as conn:` closes on exit.

### Broken connections

A reply carries no request id: it belongs to a request only because every
earlier reply was read to its end. So the connection is marked broken, and
its socket closed at that moment, whenever the socket is dead or out of step
with the server:

- a transport error or a read timeout — in a statement, in a stream, or
  while an abandoned stream is drained (`OperationalError`);
- a close, a reset or stray bytes found on the socket while the connection
  was idle. Before a statement's first request the driver looks at the
  socket — one readiness poll that does not wait, no round trip; under TLS
  the transport's socket, where TLS's own records are told from a close. A
  server sends nothing on an idle connection, so a readable one is dead: it
  is replaced before anything is written to it, and the statement runs
  once. (A member's restart or an idle timeout closes idle connections;
  over a real network the write to such a socket still succeeds, and only
  the read fails — after the statement left.) A connection in the middle of
  a stream is not idle and is not looked at;
- a frame the driver cannot place (`InterfaceError`), or an abandoned stream
  too long to drain;
- anything else that stops a statement between the first byte of its request
  and the last byte of its reply: a `KeyboardInterrupt`, a `SystemExit` or
  another exception raised by a signal handler, a green thread's timeout.
  It propagates unchanged;
- a re-dial that did not finish, or a `RowStream` left open by the body of
  `transaction()`.

Closing the socket at once, rather than at the next statement, is also what
ends the session on the server — with a transaction open in it, or the rest
of a stream it is still sending. With `auto_reconnect` the next statement
re-dials first; without it the statement raises `OperationalError("connection
is broken …; call reconnect()")`. A pool discards a broken connection. A
connection that broke with a [transaction](#transactions) open does neither
until the application has ended that transaction.

### Session state

A session on the server starts in no database (a bare table name resolves in
the default one), with no consistency override and no scan budget. The
connection keeps a note of each, and every later dial (`reconnect()`, or the
automatic reconnect) puts them back before anything else is sent — in this
order:

| State | Noted from | Sent on a new connection |
|-------|-----------|--------------------------|
| the database | the `database` given to `connect()`, then the last `USE [DATABASE] <name>` the server answered with success | `USE "<name>"` |
| the consistency override | the last `SET CONSISTENCY …` answered with success; cleared by `SET CONSISTENCY DEFAULT` (server >= 0.349) | the statement, as it was sent |
| the scan budget | the last `SET SCAN BUDGET …` answered with success; cleared by `SET SCAN BUDGET DEFAULT` | the statement, as it was sent |

A statement is noted whichever way it was sent — `execute()`, a cursor,
`executemany()` or `stream()` — when it is exactly one such statement:
keywords in any letter case, an optional trailing `;`, and `--` comments,
each running to the end of its line, skipped as the server skips them (`USE
app -- the tenant's database`). A database name is bare (letters, digits,
`_`) or in double quotes with `""` for a quote inside; a `--` inside a
quoted name is part of the name. Of a `SET` the driver reads the first words
and no further. Anything else (two statements in one string) is left alone,
and the connection keeps the notes it has.

When a statement that puts the state back is refused on the new connection,
the statement that needed the connection fails with the server's error
(`ProgrammingError`) and the connection stays broken: it never runs in
another database, at another consistency or without its scan limit.

One exception keeps a session from being held hostage by its own state. When
the statement that makes the driver dial is itself a `USE`, the new
connection does not first enter the database that statement leaves — it may
have been dropped meanwhile, and then no statement could ever succeed, that
`USE` included. The noted settings are sent, then the `USE`; its answer
decides the session database as always. The same for a `SET CONSISTENCY` or
`SET SCAN BUDGET` and the setting it replaces. If the statement is then
refused, the connection is marked broken rather than left in the default
database.

A [pool](#connectionpool-and-pool) hands every borrower a connection in the
pool's own session state, whatever the last borrower set.

### Transactions

```python
Connection.transaction(body: Callable[[Connection], T],
                       attempts: int = 3, begin: str = "BEGIN") -> T
Connection.commit() -> None     # COMMIT, if a transaction is open
Connection.rollback() -> None   # ROLLBACK, if a transaction is open
```

A transaction is statements on one connection: `BEGIN`, the statements, then
`COMMIT` or `ROLLBACK`. Outside a transaction each statement autocommits.
`transaction()` runs one and returns what `body` returned:

1. `attempts < 1` raises `ValueError`; nothing is sent.
2. `begin` is sent verbatim — plain `BEGIN` on a standalone server, `BEGIN
   ATOMIC` or `BEGIN ATOMIC PARTITION (col = lit)` on a cluster. If it
   fails, its error is raised: the body does not run and nothing else is
   sent, because each of the body's statements would commit on its own.
3. `body(conn)` is called with this connection.
4. If it raises — any exception, `KeyboardInterrupt` included — `ROLLBACK`
   is sent and the body's exception propagates unchanged. Whatever the
   `ROLLBACK` answers is ignored (a server before 0.347 answers an error
   when no transaction is open). Nothing is retried, even when the
   exception is a `TransactionConflict`.
5. Otherwise `COMMIT` is sent. On success the body's value is returned. On a
   [`TransactionConflict`](#errors) the call goes back to step 2 while runs
   remain — `attempts` in all, with no pause between them — and raises the
   conflict after the last. Any other `COMMIT` error is raised as it is,
   with no retry and no `ROLLBACK`: the transaction is already over.

The body runs once per attempt, so it must be safe to run more than once and
should do nothing outside the database that a retry would repeat.

Finish or close any `RowStream` the body opens before it returns. While one
is open neither `COMMIT` nor `ROLLBACK` can be sent (`ProgrammingError`, or
the body's own exception), and the connection is marked broken — its socket
closed, which ends the transaction on the server — rather than left inside
an open transaction. The stream ends there too.

#### A transaction is on one connection

The connection notes a transaction wherever it comes from — `transaction()`,
or `BEGIN` / `COMMIT` / `ROLLBACK` sent as ordinary statements: a statement
the server accepted whose first word is `BEGIN` (`BEGIN`, `BEGIN
TRANSACTION`, `BEGIN ATOMIC …`) opens it, and exactly `COMMIT [TRANSACTION]`
or `ROLLBACK [TRANSACTION]` ends it once the server has answered — with
success or with an error, since a refused `COMMIT` has ended the transaction
too.

While one is open the connection does not re-dial, whatever
`auto_reconnect` says, and `reconnect()` raises `ProgrammingError`: a
statement on a fresh connection would run outside the transaction and commit
on its own. (`begin` itself re-dials like any other statement — nothing is
open yet.) When the connection is lost — under a statement, or found closed
by the look at the idle socket — the server discards the transaction with
it, and:

- the statement under which it was lost raises `OperationalError`, and so
  does every statement after it, without being sent;
- `transaction()` raises that error and is done; afterwards the connection
  reconnects as usual;
- a transaction written by hand is ended by the application, where it is:
  `ROLLBACK` — the statement, or `rollback()` — is answered with success
  without being sent, and `COMMIT` — the statement, or `commit()` — raises
  an `OperationalError` saying that the connection was lost and nothing was
  committed. Either one ends the transaction, and the next statement
  re-dials, session state put back;
- a connection lost under the `COMMIT` itself is the one unknown outcome:
  the server may have applied it before its reply went missing. That
  `OperationalError` says so and claims neither; the transaction is over.
  Lost under the `ROLLBACK` itself, the transaction is gone either way, and
  the statement succeeds.

#### `commit()` and `rollback()`

The DB-API methods agree with the transaction the connection has noted.
With one open — after a `BEGIN` statement — `commit()` sends `COMMIT` and
`rollback()` sends `ROLLBACK`, and they raise what the statement raises (a
`TransactionConflict` from `commit()`, for one). With none open both are
accepted and send nothing: every statement has committed on its own, and
there is nothing to roll back — the server answers a `ROLLBACK` with nothing
open the same way. So frameworks that call them as a matter of routine cost
no round trip, and a `rollback()` in the clean-up after a failed `commit()`
does not replace its error.

Inside `transaction()` the helper ends the transaction: `commit()` sends
nothing there, and `rollback()` raises `OperationalError` — a body rolls
back by raising.

## `Cursor`

```python
Cursor.connection: Connection
Cursor.arraysize: int = 1
Cursor.rowcount: int
Cursor.description: list[tuple] | None
```

### `Cursor.execute`

```python
Cursor.execute(sql: str, params: Sequence | None = None) -> Cursor
```

With `params` (non-empty): prepare `sql` on the server (cached per
connection), check the parameter count against what the server reported
(`ProgrammingError("statement expects N parameters, got M")`), bind each
value as a typed value and execute. If the server reports the statement
cannot be prepared (DDL, session control), fall back to client-side text
binding of scalar values (see [text binding](#client-side-text-binding)).

Without `params`: the statement is sent as-is over the one-shot query op. A
`?` outside a string literal with no parameters raises `ProgrammingError`.

After a successful call:

- row-producing statement: `description` is a list of
  `(name, None, None, None, None, None, None)`, `rowcount` is the number of
  rows, and the rows are buffered for the fetch methods;
- `INSERT`/`UPDATE`/`DELETE`: `description = None`, `rowcount` = rows affected;
- DDL: `description = None`, `rowcount = -1`;
- a multi-set `CALL`: the first set is current; see `nextset()`.

Returns `self`, so `conn.execute(sql).fetchall()` reads naturally.

### `Cursor.executemany`

```python
Cursor.executemany(sql: str, seq_of_params: Iterable[Sequence]) -> None
```

Prepares `sql` once and sends every parameter row in **one frame**
(`ExecuteBatch`). `rowcount` is the total rows affected (`-1` if the server
answered DDL). Outside a transaction each row commits on its own: if a row
fails the `ProgrammingError` names it — `batch row 1: <error> (1 rows
applied before it)`, counted from 0 — and earlier rows stay applied. (A row
error that is a transaction conflict raises `TransactionConflict`, as it
does anywhere else.)
Every row's length is validated against the statement's parameter count
before the batch is sent.
Falls back to a per-row `execute()` loop for unpreparable statements and for
servers that predate the opcode; session and transaction control (`USE`,
`SET`, `BEGIN`, `COMMIT`, `ROLLBACK`) goes row by row without a prepare. An
empty sequence sets `rowcount = 0` and sends nothing.

### Fetching

```python
Cursor.fetchone() -> tuple | None
Cursor.fetchmany(size: int | None = None) -> list[tuple]   # default cursor.arraysize
Cursor.fetchall() -> list[tuple]
iter(cursor) / next(cursor)                                 # rows until exhausted
Cursor.nextset() -> bool | None
Cursor.set_consistency(level: int | str) -> None
Cursor.close() -> None                                      # frees the buffered rows
```

`nextset()` advances to the next result set of a multi-set reply and returns
`True`; returns `None` when there is none. Rows are plain tuples in column
order.

Cursors are context managers and iterable. A cursor never talks to the server
on its own after `execute()` returns — the whole result is already in memory.

## `RowStream`

Returned by [`Connection.stream()`](#streaming). See [streaming.md](streaming.md)
for the full contract.

```python
RowStream.columns: list[str]        # from the header, before any row
RowStream.affected: int             # rows affected, when the statement was a mutation
iter(rows) / next(rows) -> tuple    # rows, one server chunk in memory at a time
RowStream.nextset() -> bool | None  # the next set of a CALL that EMITs; None when there is none
RowStream.close() -> None           # finish early: drain up to 64 frames, else mark broken
with conn.stream(sql) as rows: ...  # close() on exit
```

- Iteration ends at the server's end-of-stream frame, which releases the
  connection. A server error after the header raises `ProgrammingError` from
  `next()`; rows already yielded are valid and the connection stays usable.
- A transport error or unparseable frame mid-stream raises
  `OperationalError` / `InterfaceError` and marks the connection
  [broken](#broken-connections); so does anything else that interrupts the
  read or the decoding of a chunk. The stream is over in each case.
- `close()` is idempotent and is also called by `__del__`, so a stream that
  is simply dropped still follows the drain rule (errors there are swallowed).
- A stream belongs to the socket it was opened on. Once the connection has
  closed that socket or re-dialed, iterating past the rows already received
  raises `OperationalError`, and `close()` reads nothing: what is on the
  connection now is another statement's.
- A `CALL` whose body `EMIT`s result sets arrives in one frame: the stream
  starts on the first set and `nextset()` moves `columns` and the iteration
  to the next one, returning `True`, or `None` when there is none (always,
  for a streamed result), as `Cursor.nextset()` does.

## `ConnectionPool` and `pool()`

```python
skaidb.pool(maxsize: int = 10, **kwargs) -> ConnectionPool
skaidb.ConnectionPool(
    maxsize: int = 10,
    *,
    load_balancing: str = "random",       # "random" | "round_robin" | "least_latency"
    idle_order: str = "lifo",             # "lifo" | "fifo"
    discover: bool = False,               # read members from SHOW ENDPOINTS
    refresh_interval: float = 60.0,       # seconds between discovery refreshes
    read_your_writes: float = 0.0,        # seconds; 0 = off
    eject_errors: int = 3,                # consecutive transport failures
    eject_latency_factor: float = 3.0,    # x the other members' median; 0 = off
    eject_latency_floor: float = 0.05,    # seconds above the median, at least
    eject_backoff: float = 5.0,           # first ejection, doubling per repeat
    eject_backoff_max: float = 60.0,
    **connect_kwargs,
)

ConnectionPool.getconn(affinity=None) -> Connection
ConnectionPool.putconn(conn: Connection) -> None
ConnectionPool.connection(affinity=None)   # context manager yielding a Connection
ConnectionPool.transaction(body, attempts=3, begin="BEGIN", affinity=None)
ConnectionPool.members() -> list[dict]     # endpoint, canonical, state, latency_ms,
                                           # in_use, idle, seed
ConnectionPool.close() -> None
ConnectionPool.closed: bool
```

Thread-safe. `connect_kwargs` are `connect()`'s keywords; `host`/`port`/
`seeds` name the starting members and the rest apply to every connection.
`maxsize` (`ValueError` if `< 1`) is the cap on **idle** connections kept;
it does not bound concurrent checkouts and `getconn()` never blocks. An
unknown `load_balancing` or `idle_order` raises `ValueError`. `affinity`
scopes `read_your_writes` (any hashable; `None` = pool-wide).
`transaction()` checks out one connection, runs
[`Connection.transaction`](#transactions) on it — every run a conflict costs
included — and returns the connection to the pool; `attempts < 1` raises
`ValueError` before a connection is taken.

Every borrower gets a connection in the pool's own
[session state](#session-state): in the pool's database (`database=`, or
`default` without it), with no consistency override, no scan budget and no
transaction open. `putconn()` puts back what the connection's notes say its
borrower changed — `ROLLBACK`, `USE`, `SET SCAN BUDGET DEFAULT`, `SET
CONSISTENCY DEFAULT`, only the ones needed — and closes the connection
instead of pooling it if one of them fails. `getconn()` does not hand out a
connection its member closed while it sat idle. See [pooling.md](pooling.md).

## `Consistency`

```python
class Consistency:
    ONE = 0
    QUORUM = 1
    ALL = 2

    @classmethod
    def resolve(cls, value: int | str) -> int
```

`resolve()` accepts `0/1/2` or `"one"/"quorum"/"all"` in any case and raises
`ValueError` otherwise. Every `consistency` argument in the driver goes
through it.

## Errors

```
Exception
└── Error
    ├── InterfaceError
    └── DatabaseError
        ├── OperationalError
        └── ProgrammingError
            └── TransactionConflict
```

| Class | Raised for |
|-------|-----------|
| `Error` | base class; catch this for "anything the driver raised" |
| `InterfaceError` | a truncated or malformed server message, an unknown value/response tag, an unexpected frame during a stream (connection marked broken) |
| `DatabaseError` | base of the two below |
| `OperationalError` | no endpoint reachable (the dial, the handshake or the `Hello` failed on each), authentication denied, mutual-auth signature mismatch, a socket error or read timeout mid-request (connection marked broken), a statement on a broken connection that may not re-dial (`auto_reconnect=False`), a connection lost inside a transaction — every statement until the transaction is ended, and its `COMMIT` — a `RowStream` whose connection was closed or re-dialed, `rollback()` inside `transaction()` |
| `ProgrammingError` | the server rejected the statement (an `Error` frame: bad SQL, constraint violation, unknown table, …), a parameter-count mismatch, an unbindable value type, NaN/Infinity, a non-finite or oversized `Decimal`, non-string document keys, a statement on a closed connection or on a pool that is closed, any statement while a `RowStream` is open, `reconnect()` with a transaction open, an unknown `auth_mechanism` |
| `TransactionConflict` | a server error whose message starts with `transaction conflict:` — a `COMMIT` refused because another writer changed a row the transaction writes after its `BEGIN` (standalone server >= 0.346). Nothing was applied, the transaction is over and the connection is back in autocommit, so the transaction can be run again from `BEGIN`. A `ProgrammingError` subclass, raised on every statement path (`execute()`, `commit()`, `executemany()` — where the prefix is read after the `batch row <n>: ` the server puts before a failing row's error — `stream()` and its iteration) |

Raised from outside the hierarchy: `ValueError` (bad consistency level,
`maxsize < 1`, `attempts < 1`), `struct.error` (an `int` that does not fit a
signed 64-bit integer). Message text from the server is passed through
unchanged.

A statement the server rejected never breaks the connection — a
`TransactionConflict` included: it is not re-dialed, and a pool neither
discards the connection nor counts the error against the member. (A
`ProgrammingError` comes with a broken connection in two cases, neither of
them a rejected statement: a statement of the
[session state](#session-state) refused on a re-dial — or the `USE` or
`SET` that re-dialed in its place — and a `RowStream` left open by the body
of `transaction()`.) An `OperationalError`
from a request (as opposed to from `connect()`) always does: see
[broken connections](#broken-connections).

## Parameters and type mapping

`paramstyle = "qmark"`: positional `?` placeholders, parameters as a tuple or
list. `?` inside a single-quoted literal is not a placeholder.

### Typed binding (the normal path)

Values are encoded with the protocol's value codec and bound to a server-side
prepared statement.

| Python | skaidb | Rules |
|--------|--------|-------|
| `None` | Null | |
| `bool` | Bool | checked before `int` (`bool` is an `int` subclass) |
| `int` | Int | signed 64-bit; **out-of-range raises `struct.error`** |
| `float` | Float | IEEE double; NaN/±Infinity raise `ProgrammingError` |
| `decimal.Decimal` | Decimal | `mantissa × 10^-scale`, mantissa signed 128-bit, scale unsigned 32-bit. A positive exponent is folded into the mantissa. NaN/Infinity or an overflowing mantissa raise `ProgrammingError` |
| `str` | String | UTF-8 |
| `bytes`, `bytearray` | Bytes | |
| `uuid.UUID` | Uuid | 16 raw bytes |
| `datetime.datetime` | Timestamp | milliseconds since the Unix epoch; microseconds truncated. **A naive datetime is interpreted as local time** (`astimezone()` with no argument) — pass tz-aware values to avoid surprises |
| `list`, `tuple` | Array | elements encoded recursively |
| `dict` | Document | keys must be `str` (else `ProgrammingError`); values recursive |
| anything else | — | `ProgrammingError("cannot bind value of type X")` — convert `date`, `time`, `Enum`, `set`, … yourself |

Results decode with the same table in reverse: Timestamp → tz-aware UTC
`datetime`, Array → `list`, Document → `dict`, Decimal → `Decimal`, Bytes →
`bytes`, Uuid → `uuid.UUID`.

### Client-side text binding

Used only when the server reports that the statement cannot be prepared
(DDL, `USE`, and other session control). The `?` placeholders are replaced in
the SQL text, skipping single-quoted literals (`''` escapes a quote):

| Python | SQL text |
|--------|----------|
| `None` | `NULL` |
| `bool` | `TRUE` / `FALSE` |
| `int` | decimal digits (no 64-bit check on this path) |
| `float` | `repr()`; NaN/Infinity raise `ProgrammingError` |
| `decimal.Decimal` | `str()` |
| `str` | single-quoted, `'` doubled |
| `bytes`, `bytearray` | **a single-quoted hex string literal** (`b"\xff"` → `'ff'`), not a bytes value |
| `uuid.UUID` | single-quoted canonical form |
| `datetime.datetime` | epoch milliseconds as an integer (same naive-is-local rule) |
| `list`, `tuple`, `dict` | `ProgrammingError` — no literal form |

Parameter-count mismatches raise `ProgrammingError` on both paths.

## Limits and constants

| Constant | Value | Meaning |
|----------|-------|---------|
| server `MAX_PREPARED_PER_CONN` | 256 | prepared statements a connection may hold on the server |
| driver prepared cache | 240 | statements cached per connection; beyond this a statement is prepared, executed and closed per call |
| stream drain cap | 64 frames | how much of an abandoned stream `close()` reads before marking the connection broken instead |
| `subscribe()` page | 500 events | log rows fetched per poll |
| default port | 7000 | binary protocol; the REST/JSON gateway is on 7080 |
| default timeout | 10 s | dial and read |
| default consistency | `QUORUM` | |
