# Streaming

`Cursor.execute()` receives a result as one frame and holds every row in
memory. `Connection.stream()` receives it in chunks and holds one chunk at a
time. Use it for exports, scans and any result that might not fit in memory
— it is also how you read a large table without tripping the server's
per-statement scan budgets, since the server executes an eligible `SELECT`
page by page.

```python
with conn.stream("SELECT id, pad FROM big ORDER BY id") as rows:
    print(rows.columns)          # ['id', 'pad'] — known before the first row
    for row in rows:
        write(row)
```

## The contract

The streaming exchange is: a header frame carrying the column names, zero or
more row-chunk frames, then an end frame (or an error frame). The protocol
rule that drives everything on this page is:

> The connection is busy for the whole stream: no other request may be sent
> on it until the end frame or an error. A client that abandons a stream
> early must drain the remaining frames before reusing the connection, or
> close it.

The driver enforces it like this:

1. While a `RowStream` is open, **any other statement on that connection
   raises `ProgrammingError`** ("connection is busy streaming; finish or
   close() the RowStream …"). This includes `execute()`, `executemany()`,
   `ping()` and a second `stream()`. `is_usable()` is `False` meanwhile.
2. Iterating to the end releases the connection.
3. Stopping early — `break`, an exception, leaving the `with` block, or just
   dropping the object — calls `close()`, which **drains** the frames the
   server has already sent so the socket is back at a request boundary.
4. The drain is capped at **64 frames**. If the stream still has more than
   that queued, the driver stops reading and **marks the connection broken**:
   further statements raise `OperationalError`, `is_usable()` stays `False`,
   and a pool discards it on check-in and dials a replacement. Draining the
   tail of a small result is far cheaper than a reconnect; draining most of a
   huge one is not, and a broken-but-honest connection is better than a
   desynced one whose next caller reads someone else's rows.

So the rule of thumb: **read to the end when you can; when you cannot, stop
inside a `with` block and expect the connection to be recycled if a lot was
still in flight.** Put a `LIMIT` on the statement if you know you only need
the first N rows — then the drain is short and the connection survives.

## What you get back

```python
rows = conn.stream(sql, consistency=None)
```

| Statement produced | Result |
|--------------------|--------|
| rows | a live `RowStream`; `rows.columns` is filled; iterate for tuples |
| `INSERT`/`UPDATE`/`DELETE` | an already-finished empty stream; `rows.affected` = rows affected |
| DDL | an already-finished empty stream, `affected = 0` |
| an error before any row | `ProgrammingError` from `stream()`; connection stays usable |
| a `CALL` returning several result sets | `InterfaceError` from `stream()`; connection stays usable — use `execute()` + `nextset()` |
| server without the streaming opcode | `ProgrammingError("server does not support streaming: …")` |

`stream()` takes SQL text only; there is no parameter binding on the
streaming opcode. Bind values yourself with care or restructure the query
(for example `WHERE id IN (...)` with a literal list).

## Errors mid-stream

- A server error **after** the header (a node dying mid-scan, a scan budget
  tripping) raises `ProgrammingError` from `next()`. Rows already yielded are
  valid. The error frame ends the exchange, so the connection is at a
  request boundary and stays usable.
- A transport error (socket closed, read timeout) raises `OperationalError`
  and marks the connection broken.
- A frame the driver cannot parse raises `InterfaceError` and marks the
  connection broken.

## Timeouts

Every read in the stream — and the drain in `close()` — is bounded by the
connection's read timeout. With `connect(timeout=None)` (or
`read_timeout=None`) there is no time bound: a peer that is alive but silent
blocks `next()` and `close()` forever. Set a read timeout comfortably above
the time the server needs to produce one chunk.

## Consistency

`stream(sql, consistency="ONE")` overrides the connection's level for this
statement only. `QUORUM` streaming pages are served from a quorum of
replicas per page.

## Pools

A pooled connection returned while its stream is still open is **not** put
back in the idle set: `is_usable()` reports it as mid-stream and the pool
closes it. Finish or close the stream before leaving the
`pool.connection()` block — the natural shape is to nest the `with`s:

```python
with pool.connection() as conn:
    with conn.stream("SELECT ...") as rows:
        for row in rows:
            ...
```

## Memory

The driver holds one chunk at a time: the current chunk's rows are decoded
into tuples and handed out one by one, then the next frame is read. Whether
the *server* also streams internally depends on the statement — a plain
eligible `SELECT` is executed page by page, other shapes are materialised
server-side and delivered in chunks — but the client-side bound is the
portable guarantee.
