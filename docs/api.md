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
| `__version__` | e.g. `"1.0.0"` | the installed package version (read from distribution metadata; falls back to the in-module literal when running from an uninstalled checkout). Reported to the server in the Hello frame. |

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
) -> Connection
```

Opens a connection and runs the SCRAM-SHA-256 handshake. What happens, in
order: resolve the endpoint list (`seeds`, or the single `host:port`) and
shuffle it; for each endpoint until one succeeds: TCP connect (with
`TCP_NODELAY`), optional TLS wrap, SCRAM handshake; then send `Hello`
(best-effort, ignored by old servers) and `USE "<database>"` if `database`
was given.

| Parameter | Notes |
|-----------|-------|
| `host`, `port` | Used only when `seeds` is not given. |
| `user`, `password` | SCRAM credentials. `"anonymous"` / `""` for a server with auth disabled. The server's signature is verified (mutual auth) whenever a password is set. |
| `consistency` | `"ONE"`, `"QUORUM"`, `"ALL"` (case-insensitive) or `Consistency.ONE/QUORUM/ALL`. Default for every statement on the connection; cursors and streams can override. `ValueError` on anything else. |
| `timeout` | Default for both the dial timeout and the socket read timeout, in seconds. **`None` means no timeout at all**: a dial to a black-holed host and a read from a silent peer both block forever, including the drain an abandoned stream performs. |
| `connect_timeout`, `read_timeout` | Override the dial / read bound separately; each falls back to `timeout` when `None`. |
| `database` | Runs `USE "<database>"` (identifier-quoted) as part of connecting. |
| `seeds` | `"host"` or `"host:port"` strings tried in randomized order; the port after the **last** colon is used, bare names get `port`. IPv6 literals: use `host=`/`port=`. |
| `tls`, `tls_ca`, `tls_insecure`, `tls_server_name` | See [tls.md](tls.md). Any of the first three enables TLS. |

Raises `OperationalError` when no endpoint could be connected and
authenticated (the message lists every endpoint's failure), including
`authentication denied` and `server signature mismatch`.

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
Connection.stream(sql: str, consistency: int | str | None = None) -> RowStream
```

Runs `sql` over the streaming opcode and returns a [`RowStream`](#rowstream).
No parameters (the streaming opcode carries SQL text). `consistency=None`
uses the connection's level. Outcomes by what the statement produced:

- rows: a live `RowStream` with `columns` filled from the header;
- a mutation or DDL: an already-finished, empty `RowStream` (`affected`
  holds the mutation's row count);
- a server error before any row: `ProgrammingError`; a server without the
  opcode: `ProgrammingError("server does not support streaming: …")`;
- a multi-result-set reply (a `CALL` that `EMIT`s): `InterfaceError` — run it
  with `execute()` instead. The connection is left healthy.

Raises `ProgrammingError` if the connection is already streaming.

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

- `is_usable()` is `False` when the connection is closed, **broken** (a
  transport error, an unparseable frame, or an abandoned stream too long to
  drain left the socket out of sync) or **mid-stream** (a `RowStream` owns
  it). Pools use it on checkout and check-in.
- `ping()` returns `False` and marks the connection broken on any failure.
- `reconnect()` drops the socket, re-dials (shuffled seeds), re-authenticates,
  re-sends Hello and `USE`, and clears the prepared-statement cache. Raises
  `ProgrammingError` on a closed connection.
- A broken connection refuses statements with `OperationalError("connection is
  broken …; call reconnect()")` rather than retrying: its socket may hold
  unread frames, and writing into it would read someone else's answer back.
- `close()` is idempotent. `with connect(...) as conn:` closes on exit.

### Transactions

```python
Connection.commit() -> None     # no-op
Connection.rollback() -> None   # raises OperationalError
```

skaidb autocommits each statement. `commit()` exists for DB-API conformance
and does nothing; `rollback()` raises rather than silently doing nothing.
Statement-level `BEGIN`/`COMMIT`/`ROLLBACK`, where the server supports them,
are sent as ordinary statements.

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
answered DDL). Each row autocommits; if a row fails the `ProgrammingError`
names its index and earlier rows stay applied. Every row's length is
validated against the statement's parameter count before anything is sent.
Falls back to a per-row `execute()` loop for unpreparable statements and for
servers that predate the opcode. An empty sequence sets `rowcount = 0` and
sends nothing.

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
RowStream.close() -> None           # finish early: drain up to 64 frames, else mark broken
with conn.stream(sql) as rows: ...  # close() on exit
```

- Iteration ends at the server's end-of-stream frame, which releases the
  connection. A server error after the header raises `ProgrammingError` from
  `next()`; rows already yielded are valid and the connection stays usable.
- A transport error or unparseable frame mid-stream raises
  `OperationalError` / `InterfaceError` and marks the connection broken.
- `close()` is idempotent and is also called by `__del__`, so a stream that
  is simply dropped still follows the drain rule (errors there are swallowed).

## `ConnectionPool` and `pool()`

```python
skaidb.pool(maxsize: int = 10, **connect_kwargs) -> ConnectionPool
skaidb.ConnectionPool(maxsize: int = 10, **connect_kwargs)

ConnectionPool.getconn() -> Connection
ConnectionPool.putconn(conn: Connection) -> None
ConnectionPool.connection()            # context manager yielding a Connection
ConnectionPool.close() -> None
ConnectionPool.closed: bool
```

Thread-safe. `connect_kwargs` are passed to `connect()` verbatim for every
new connection. `maxsize` (`ValueError` if `< 1`) is the cap on **idle**
connections kept; it does not bound concurrent checkouts and `getconn()`
never blocks. See [pooling.md](pooling.md).

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
```

| Class | Raised for |
|-------|-----------|
| `Error` | base class; catch this for "anything the driver raised" |
| `InterfaceError` | a truncated or malformed server message, an unknown value/response tag, an unexpected frame during a stream (connection marked broken); `stream()` of a multi-result-set statement (connection healthy) |
| `DatabaseError` | base of the two below |
| `OperationalError` | no endpoint reachable, authentication denied, mutual-auth signature mismatch, a socket error or read timeout mid-request (connection marked broken), a statement on a broken connection, `rollback()` |
| `ProgrammingError` | the server rejected the statement (an `Error` frame: bad SQL, constraint violation, unknown table, …), a parameter-count mismatch, an unbindable value type, NaN/Infinity, a non-finite or oversized `Decimal`, non-string document keys, a statement on a closed connection or on a pool that is closed, any statement while a `RowStream` is open |

Raised from outside the hierarchy: `ValueError` (bad consistency level,
`maxsize < 1`), `struct.error` (an `int` that does not fit a signed 64-bit
integer). Message text from the server is passed through unchanged.

A `ProgrammingError` never breaks the connection. An `OperationalError` from
a request (as opposed to from `connect()`) always does.

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
