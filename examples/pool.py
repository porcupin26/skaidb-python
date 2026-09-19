"""A thread-safe connection pool with multi-seed failover.

    python3 examples/pool.py [seed1,seed2,...] [user] [password]

Every keyword `connect()` accepts is accepted by `skaidb.pool()`; pooled
connections inherit seeds, database=, timeouts and TLS settings. Broken
connections are discarded on check-in and replaced transparently.
"""
import sys
from concurrent.futures import ThreadPoolExecutor

import skaidb

seeds = (sys.argv[1] if len(sys.argv) > 1 else "localhost:7000").split(",")
user = sys.argv[2] if len(sys.argv) > 2 else "anonymous"
pw = sys.argv[3] if len(sys.argv) > 3 else ""

pool = skaidb.pool(seeds=seeds, user=user, password=pw, maxsize=4)


def count_databases(i: int) -> int:
    with pool.connection() as conn:  # checked out, returned on exit
        cur = conn.execute("SHOW DATABASES")
        return len(cur.fetchall())


with ThreadPoolExecutor(max_workers=8) as ex:
    print(list(ex.map(count_databases, range(16))))

pool.close()
