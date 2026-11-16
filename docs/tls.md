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

Setting any of `tls=True`, `tls_ca=...` or `tls_insecure=True` enables TLS,
and so does a [client certificate](#client-certificates). `tls_insecure`
wins over `tls_ca`. Without any of them the connection is plaintext, even if
the server would accept TLS.

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

`tls_client_cert` / `tls_client_key` (PEM files) make the driver present a
client certificate in the TLS handshake. A server with the certificate login
enabled (`auth.x509_enabled`) asks for one and checks it against its client
CA; any other server ignores it. On its own the certificate only opens the
TLS session; the login is still the SCRAM user.

With `auth_mechanism="certificate"` the certificate IS the login (wire
mechanism EXTERNAL): the server maps its Common Name to a role and no
password is sent. The server needs `auth.x509_enabled`, and its client CA
must have signed the certificate.

```python
conn = skaidb.connect(host="db1", tls_ca="/etc/skaidb/ca.crt",
                      tls_client_cert="/etc/app/app.crt",
                      tls_client_key="/etc/app/app.key",
                      auth_mechanism="certificate", database="app")
```

Pass `user` only to assert the expected identity: a certificate mapped to a
different role fails the connect. `tls_client_cert` and `tls_client_key` go
together (`ProgrammingError` when only one is given), and
`auth_mechanism="certificate"` without TLS fails the connect.

## Failure modes

All connect-time failures are reported per endpoint in one
`OperationalError` after every seed was tried:
`could not connect to any endpoint (host:port: <symptom>; …)`. The symptoms
below are what a server answers each mistake with; the text in brackets is
OpenSSL's, and varies a little with the Python build.

| Symptom | Cause |
|---------|-------|
| `[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate` | the CA you trust — `tls_ca`, or the system store under `tls=True` — did not sign the server certificate. A cluster's own CA is not in the system store: pass `tls_ca` |
| `[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for '<name>'.` | `tls_server_name` is not a subject alternative name of the server certificate |
| `[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol` (Python 3.8 and 3.9: `TLS/SSL connection has been closed (EOF)`) | TLS dialed to a plaintext port: the server has `client_tls = "off"` and hangs up on the TLS hello |
| `Connection reset by peer` | plaintext dialed to a server with `client_tls = "required"`: it refuses the connection before any handshake frame |
| `timed out`, once the read timeout has passed | plaintext dialed to the REST gateway's port (7080), which waits for an HTTP request |
| `[SSL: WRONG_VERSION_NUMBER] wrong version number`, or `record layer failure` | TLS dialed to a plaintext REST port, which answers in plain HTTP |
| `The read operation timed out` | TLS dialed to the HTTPS REST port (7443): the TLS handshake succeeds, and the protocol behind it is HTTP |
| `[SSL: TLSV1_ALERT_UNKNOWN_CA] tlsv1 alert unknown ca` | the server's client CA (`auth.x509_ca_file`) did not sign the client certificate |
| `authentication denied: certificate authentication needs TLS, auth.x509_enabled, and a client certificate that chains to auth.x509_ca_file and carries a Common Name` | `auth_mechanism="certificate"` with no client certificate given, or against a server without the certificate login |
| `authentication denied: no user matches the client certificate's Common Name` | the certificate is accepted, and the server has no user of that name |
| `authentication denied: the username does not match the client certificate's Common Name` | `user` asserts another name than the certificate's |
| `certificate authentication needs TLS with a client certificate` | `auth_mechanism="certificate"` with no TLS option given at all |
