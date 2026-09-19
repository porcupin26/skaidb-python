"""Consume a skaidb stream (CREATE STREAM ...) as events arrive.

    python3 examples/subscribe.py stream_name [host] [port] [user] [password]

`conn.subscribe()` polls the stream's log and yields each event as a dict
with `id`, `op`, `k`, `ts`, `doc`. Persist the last `id` you handled and
pass it back as `after=` to resume exactly where you stopped.
"""
import sys

import skaidb

name = sys.argv[1]
host = sys.argv[2] if len(sys.argv) > 2 else "localhost"
port = int(sys.argv[3]) if len(sys.argv) > 3 else 7000
user = sys.argv[4] if len(sys.argv) > 4 else "anonymous"
pw = sys.argv[5] if len(sys.argv) > 5 else ""

with skaidb.connect(host=host, port=port, user=user, password=pw) as conn:
    last = None
    for ev in conn.subscribe(name, after=last):
        print(ev["id"], ev["op"], ev["k"], ev["doc"])
        last = ev["id"]  # checkpoint this somewhere durable
