# Connection pooling

`threadsafety = 1`: a `Connection` is not meant to be shared between threads.
The pool gives each thread (or each request) its own connection and reuses
them across requests, which also amortises the dial + SCRAM handshake.

```python
import skaidb

pool = skaidb.pool(seeds=["db1", "db2", "db3"], database="app",
                   user="app", password=pw, tls_ca="/etc/skaidb/ca.crt",
                   maxsize=8)

with pool.connection() as conn:
    cur = conn.execute("SELECT id FROM users WHERE id IN (?)", ([1, 2, 3],))
    ids = [r[0] for r in cur]

pool.close()
```

## API

```python
skaidb.pool(maxsize: int = 10, **connect_kwargs) -> ConnectionPool
ConnectionPool.connection()          # context manager: getconn() / putconn()
ConnectionPool.getconn() -> Connection
ConnectionPool.putconn(conn) -> None
ConnectionPool.close() -> None
ConnectionPool.closed: bool
```

`connect_kwargs` are exactly the keywords of `connect()`; every new
connection is dialed with them, so pooled connections inherit multi-seed
failover, `database=`, timeouts, TLS and the default consistency.

## How it behaves

- **Checkout never blocks.** `getconn()` pops an idle connection if there is
  one; otherwise it dials a new one. `maxsize` is the cap on **idle**
  connections retained, not on concurrent checkouts: with 20 threads and
  `maxsize=8`, up to 20 connections can be open at once and 12 of them are
  closed as they come back. Size `maxsize` to your steady-state concurrency.
- **Validation is cheap and happens at both ends.** `is_usable()` (no
  round-trip) is checked on checkout — a stale idle connection is closed and
  the loop tries the next — and on check-in — a broken or mid-stream
  connection is closed instead of pooled. `pool.connection()` does the
  check-in for you, closing the connection if a transport error broke it.
- **Idle connections are not health-checked by round-trip.** A server
  restart leaves idle sockets that look usable until the first statement,
  which then fails with `OperationalError` and marks the connection broken;
  the pool discards it on check-in. If you need a guarantee, call
  `conn.ping()` after checkout (one round-trip) or wrap the first statement
  in a retry that takes a fresh connection.
- **Prepared statements are per connection.** Each pooled connection keeps
  its own cache (240 statements), so a hot statement is prepared once per
  connection, not once per pool.
- **Closing.** `pool.close()` closes every idle connection and marks the pool
  closed; `getconn()` then raises `ProgrammingError`. Connections still
  checked out are closed when they are returned.
- **Thread safety.** The idle list is guarded by a lock. Connections handed
  out are exclusively the caller's until returned.

## Streams and the pool

A `RowStream` owns its connection until it ends. Finish or close the stream
before the `pool.connection()` block exits; a connection returned mid-stream
is closed rather than reused (its socket still has rows queued). A stream
abandoned with more than 64 frames in flight marks the connection broken,
and the pool replaces it. See [streaming.md](streaming.md).

## Retrying

The driver does not retry on its own. A pattern that copes with a node going
away:

```python
def run(sql, params=None, attempts=2):
    for i in range(attempts):
        with pool.connection() as conn:
            try:
                return conn.execute(sql, params).fetchall()
            except skaidb.OperationalError:
                if i == attempts - 1:
                    raise
                # the broken connection is discarded on exit; the next
                # checkout dials a fresh one across the seed list
```

Because skaidb autocommits per statement, retrying a write can apply it
twice unless the statement is idempotent (an `INSERT` with a fixed primary
key, an `UPDATE` to a value, a `DELETE`). Prefer idempotent writes.
