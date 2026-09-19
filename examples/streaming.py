"""Stream a large result without holding it all in memory.

    python3 examples/streaming.py [host] [port] [user] [password]

`conn.stream(sql)` returns a RowStream. Column names arrive before any row
(`rows.columns`). The connection is busy until the stream ends, so stop
early only inside a `with` block (or call `close()`), which drains what the
server already sent and hands the connection back.
"""
import sys

import skaidb

host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
port = int(sys.argv[2]) if len(sys.argv) > 2 else 7000
user = sys.argv[3] if len(sys.argv) > 3 else "anonymous"
pw = sys.argv[4] if len(sys.argv) > 4 else ""

with skaidb.connect(host=host, port=port, user=user, password=pw) as conn:
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS events (PRIMARY KEY (id))")
    cur.executemany(
        "INSERT INTO events (id, kind, payload) VALUES (?, ?, ?)",
        [(i, "click" if i % 2 else "view", {"n": i}) for i in range(1, 10_001)],
    )

    total = 0
    with conn.stream("SELECT id, kind, payload FROM events ORDER BY id") as rows:
        print("columns:", rows.columns)
        for row in rows:
            total += 1
            if total == 100:
                break  # leaving the block drains the tail for you
    print("consumed", total, "rows; connection usable again:", conn.is_usable())

    cur.execute("DROP TABLE events")
