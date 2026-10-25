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
skaidb.pool(maxsize: int = 10, **kwargs) -> ConnectionPool
ConnectionPool.connection(affinity=None)  # context manager: getconn() / putconn()
ConnectionPool.getconn(affinity=None) -> Connection
ConnectionPool.putconn(conn) -> None
ConnectionPool.members() -> list[dict]
ConnectionPool.close() -> None
ConnectionPool.closed: bool
```

`kwargs` are the keywords of `connect()` plus the balancing options below;
every new connection is dialed with the `connect()` ones, so pooled
connections inherit `database=`, timeouts, TLS, the default consistency and
automatic reconnect (a pooled connection re-dials in the pool's order:
usable members first, the one it lost last).

## Balancing across members

By default (`load_balancing="random"`) each new connection dials the seeds in
random order and stays on the member it reached — fine for many clients, but
one process can end up with most of its connections on one member. Pick a
policy to spread a single process's load:

```python
pool = skaidb.pool(
    seeds=["db1", "db2", "db3"], database="app", user="app", password=pw,
    load_balancing="least_latency",  # or "round_robin"
    discover=True,                   # learn members from the cluster
    read_your_writes=2.0,            # seconds; see below
    idle_order="fifo",
)
```

| Option | Default | What it does |
|---|---|---|
| `load_balancing` | `"random"` | `"round_robin"`: checkouts rotate over the usable members. `"least_latency"`: of two members picked at random, the one with the lower round-trip average × checked-out connections (near and idle members win). |
| `idle_order` | `"lifo"` | `"fifo"` hands out the longest-idle connection first, so every idle connection keeps being used. |
| `discover` | `False` | Reads the members with `SHOW ENDPOINTS` (server ≥ 0.327; no privilege needed) when a member is first reached and every `refresh_interval` (60 s) after. New members are added, members that left are dropped (seeds are kept, marked `unlisted`), and members the server reports `resyncing` or `unreachable` get no new connections until they are `ready`. An older or standalone server leaves the seeds as they are. |
| `eject_errors` | `3` | Consecutive transport failures (dial, I/O, timeout, capacity refusal) that eject a member. Statement errors and refused logins do not count. |
| `eject_latency_factor`, `eject_latency_floor` | `3.0`, `0.05` | A member whose round-trip average is above factor × the other members' median **and** more than floor seconds above it is ejected (after 10 samples on it and on another member). `0` disables. |
| `eject_backoff`, `eject_backoff_max` | `5.0`, `60.0` | How long an ejection lasts, doubling per repeat. Afterwards a background thread probes the member and re-admits it. The last usable member is never ejected. |
| `read_your_writes` | `0.0` | Seconds after a write during which checkouts with the same `affinity` go to the member that took the write. |

**Read-your-writes.** The member that coordinated a write reads it back at
any consistency, `ONE` included; another member may not have applied it yet.
With `read_your_writes` set, pass an affinity key — a user or session id —
when you check out:

```python
with pool.connection(affinity=user_id) as conn:
    conn.execute("INSERT INTO notes (id, owner, body) VALUES (?, ?, ?)", (nid, user_id, body))
# ... the next request for the same user, within the window:
with pool.connection(affinity=user_id) as conn:          # same member
    conn.cursor().execute("SELECT body FROM notes WHERE id = ?", (nid,))
```

Checkouts without a key share one pool-wide key, so with constant writes
everything follows the latest writer — pass a key when you balance.

Derive the key without touching the pool. If the key comes from something
that itself queries the database through the same pool — Flask-Login's
`current_user`, for instance, runs your `user_loader` — a helper that checks
out a connection and computes the key inside it re-enters itself. Read the
raw id from the session or token instead:

```python
from flask import session

def db():
    return pool.connection(affinity=session.get("_user_id"))  # no query to get the key
``` A
statement is a write unless it starts with `SELECT`, `SHOW`, `EXPLAIN`,
`DESCRIBE`, `VALUES`, `WITH` or `USE`.

**Watching it.** `pool.members()` returns one dict per member:

```python
[{'endpoint': 'db1:7000', 'canonical': '10.0.0.1:7000', 'state': 'ready',
  'latency_ms': 0.8, 'in_use': 2, 'idle': 3, 'seed': True}, ...]
```

`state` is `ready`, `ejected`, `resyncing`, `unreachable` or `unlisted`.
Ejections and re-admissions are logged at WARNING on the `skaidb.pool`
logger. The rules are the same in every skaidb driver
([PROTOCOL.md §6.1](https://skaidb.org/docs/PROTOCOL.html#61-balancing-a-pool-across-members)).

## How it behaves

- **Checkout never blocks.** `getconn()` takes an idle connection (to the
  member the policy picks, if it picks one) when there is one; otherwise it
  dials a new one. `maxsize` is the cap on **idle**
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
- **Thread safety.** The idle list and the member table are guarded by a
  lock; the probe/discovery thread is a daemon started on first need. Connections handed
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
