"""A thread-safe connection pool balanced across the cluster's members.

    python3 examples/pool.py [seed1,seed2,...] [user] [password]

Every keyword `connect()` accepts is accepted by `skaidb.pool()`; pooled
connections inherit database=, timeouts and TLS settings. Broken
connections are discarded on check-in and replaced transparently. With
`load_balancing="least_latency"` and `discover=True` the pool spreads its
checkouts over every member it learns from SHOW ENDPOINTS, skipping members
that are resyncing and ejecting ones that fail or turn slow.
"""
import sys
from concurrent.futures import ThreadPoolExecutor

import skaidb

seeds = (sys.argv[1] if len(sys.argv) > 1 else "localhost:7000").split(",")
user = sys.argv[2] if len(sys.argv) > 2 else "anonymous"
pw = sys.argv[3] if len(sys.argv) > 3 else ""

pool = skaidb.pool(
    seeds=seeds, user=user, password=pw, maxsize=4,
    load_balancing="least_latency", discover=True,
)


def count_databases(i: int) -> int:
    with pool.connection() as conn:  # checked out, returned on exit
        cur = conn.execute("SHOW DATABASES")
        return len(cur.fetchall())


with ThreadPoolExecutor(max_workers=8) as ex:
    print(list(ex.map(count_databases, range(16))))

for m in pool.members():  # what the pool sees per member
    print(m["endpoint"], m["state"], m["latency_ms"], m["in_use"], m["idle"])

pool.close()
