"""Connect over TLS.

    python3 examples/tls.py host port user password [ca.crt]

With a CA file the server certificate is verified against that CA (the
cluster's `ca.crt`) and the SNI / expected name is `tls_server_name`
(default "skaidb"). Without one the system trust store is used. For a
self-signed dev server only, `tls_insecure=True` skips verification.
"""
import sys

import skaidb

host, port, user, pw = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
ca = sys.argv[5] if len(sys.argv) > 5 else None

kwargs = {"host": host, "port": port, "user": user, "password": pw}
if ca:
    kwargs["tls_ca"] = ca
else:
    kwargs["tls"] = True

with skaidb.connect(**kwargs) as conn:
    print(conn.execute("SHOW DATABASES").fetchall())
