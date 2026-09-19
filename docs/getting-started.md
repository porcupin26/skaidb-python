# Getting started

The skaidb Python driver is a DB-API 2.0 (PEP 249) module with no
dependencies. This page takes you from install to a first query, a bulk load
and a streamed read. The [API reference](api.md) has every signature.

## Install

```sh
pip install skaidb
```

Requires Python 3.8 or newer. There is nothing to compile.

## A server to talk to

You need a skaidb node reachable on its binary-protocol port (default
`7000`). See <https://skaidb.org/docs/> for running a node or cluster. The
examples below assume `localhost:7000` with a user `skaidb` / password
`secret`; drop `user`/`password` if authentication is disabled.

## First connection

```python
import skaidb

conn = skaidb.connect(host="localhost", port=7000,
                      user="skaidb", password="secret")
print(conn.execute("SHOW DATABASES").fetchall())
conn.close()
```

`connect()` opens a TCP socket, runs the SCRAM-SHA-256 handshake and returns
a `Connection`. Use it as a context manager so it is closed on the way out:

```python
with skaidb.connect(host="localhost", user="skaidb", password="secret") as conn:
    ...
```

## Pick a database

Either run `USE` yourself or let `connect()` do it:

```python
conn = skaidb.connect(host="localhost", user="skaidb", password="secret",
                      database="app")          # runs USE "app" on connect
```

## Create a table and write rows

Placeholders are `?`. Values are bound as typed values through a server-side
prepared statement, so strings need no escaping and lists / dicts bind as
arrays / documents:

```python
cur = conn.cursor()
cur.execute("CREATE TABLE users (PRIMARY KEY (id))")
cur.execute("INSERT INTO users (id, name, tags, meta) VALUES (?, ?, ?, ?)",
            (1, "Ada", ["math", "eng"], {"city": "London"}))
```

For many rows, `executemany` ships the whole batch in one round-trip:

```python
cur.executemany("INSERT INTO users (id, name) VALUES (?, ?)",
                [(2, "Linus"), (3, "Margaret")])
print(cur.rowcount)        # 2
```

## Read rows

```python
cur.execute("SELECT id, name, tags FROM users WHERE id IN (?) ORDER BY id", ([1, 3],))
print(cur.description)     # column names, one 7-tuple per column
print(cur.fetchone())      # (1, 'Ada', ['math', 'eng'])
for row in cur:            # the rest
    print(row)
```

Rows are tuples. Values come back as native Python types (`int`, `str`,
`list`, `dict`, `decimal.Decimal`, `uuid.UUID`, tz-aware UTC `datetime`, …);
see the [type mapping](api.md#type-mapping).

## Read a big result without loading it

A cursor holds its whole result in memory. For exports and large scans use a
stream, which holds one chunk at a time:

```python
with conn.stream("SELECT id, name FROM users ORDER BY id") as rows:
    print(rows.columns)
    for row in rows:
        process(row)
```

The connection is busy until the stream is finished; if you stop early, the
`with` block drains what is left. Read [streaming.md](streaming.md) before
relying on this in production — abandoning a large stream recycles the
connection.

## Several nodes

skaidb is leaderless, so any node serves any request. Give the driver all of
them and it will connect to one at random and fail over to the others:

```python
conn = skaidb.connect(seeds=["db1", "db2", "db3"], database="app",
                      user="skaidb", password="secret")
```

## Threads

Use a pool: one connection per thread, reused across requests.

```python
pool = skaidb.pool(seeds=["db1", "db2", "db3"], database="app",
                   user="skaidb", password="secret", maxsize=8)
with pool.connection() as conn:
    conn.execute("...")
```

See [pooling.md](pooling.md).

## TLS

```python
conn = skaidb.connect(host="db1", tls_ca="/etc/skaidb/ca.crt",
                      user="skaidb", password="secret")
```

See [tls.md](tls.md) for the three modes and the server-name check.

## Errors

Everything the driver raises derives from `skaidb.Error`. A rejected
statement is a `skaidb.ProgrammingError` and the connection stays usable; a
transport failure is a `skaidb.OperationalError` and the connection must be
reconnected or replaced. See [api.md#errors](api.md#errors).

## Next

- [API reference](api.md)
- [Streaming](streaming.md) · [Pooling](pooling.md) · [TLS](tls.md)
- [Changelog](changelog.md)
- Server docs: <https://skaidb.org/docs/> · Wire protocol:
  <https://skaidb.org/docs/PROTOCOL.html>
