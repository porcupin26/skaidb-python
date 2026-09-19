# TLS

The binary protocol can run inside TLS. The handshake happens right after the
TCP connect; SCRAM authentication and every request then ride inside the TLS
session. The server side (listener certificate, `client_tls` policy, the
cluster CA) is covered in the server docs at <https://skaidb.org/docs/>.

## Three modes

```python
import skaidb

# 1. Verify against the system trust store (a publicly trusted or
#    OS-installed CA). SNI and the expected name are tls_server_name.
conn = skaidb.connect(host="db1.example.com", tls=True,
                      tls_server_name="db1.example.com")

# 2. Verify against a specific CA — the cluster's own ca.crt. This is the
#    usual production shape.
conn = skaidb.connect(host="db1", tls_ca="/etc/skaidb/ca.crt")

# 3. No verification at all (self-signed dev server). INSECURE: any peer
#    can impersonate the server.
conn = skaidb.connect(host="db1", tls_insecure=True)
```

Setting any of `tls=True`, `tls_ca=...` or `tls_insecure=True` enables TLS.
`tls_insecure` wins over `tls_ca`. Without any of them the connection is
plaintext, even if the server would accept TLS.

## The server name

`tls_server_name` (default `"skaidb"`) is sent as SNI and is the name the
server certificate must match under modes 1 and 2. skaidb's generated
certificates carry `skaidb` as a subject alternative name, which is why the
default is not the host you dialed: the same certificate is valid on every
node whatever its address. If your certificates instead carry real hostnames,
pass the hostname.

Hostname checking uses Python's `ssl` defaults (`check_hostname=True`,
`CERT_REQUIRED`) with `ssl.PROTOCOL_TLS_CLIENT`, so the negotiated protocol
and cipher follow your Python build's policy.

## Pools and seeds

TLS keywords pass through `skaidb.pool()` and apply to every seed:

```python
pool = skaidb.pool(seeds=["db1", "db2", "db3"], tls_ca="/etc/skaidb/ca.crt",
                   user="app", password=pw, database="app")
```

## Client certificates

The driver does not present a client certificate; identity is the SCRAM
user. A server configured to require client certificates on the binary port
will reject this driver's handshake.

## Failure modes

| Symptom | Cause |
|---------|-------|
| `OperationalError: could not connect to any endpoint (…: [SSL: CERTIFICATE_VERIFY_FAILED] …)` | the CA does not sign the server certificate, or `tls_server_name` does not match a SAN |
| `OperationalError: could not connect to any endpoint (…: [SSL: WRONG_VERSION_NUMBER] …)` | the server port is plaintext (TLS not enabled on the listener) |
| `OperationalError: … connection closed by server` right after connect, without TLS | the server requires TLS (`client_tls = required`) and you dialed plaintext |
| `OperationalError: bad handshake challenge` | usually a plaintext dial to a TLS-only port, or the wrong port (7080 is the REST gateway) |

All connect-time failures are reported per endpoint in one
`OperationalError` after every seed was tried.
