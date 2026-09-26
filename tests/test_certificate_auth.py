"""Certificate login (wire mechanism EXTERNAL): the AuthStart bytes, the
outcome handling, and that the default user asserts no name."""

import unittest
from unittest import mock

import skaidb


def _enc_str(s):
    b = s.encode()
    return len(b).to_bytes(4, "little") + b


class FakeConn:
    """Stands in for the frame layer of one connection."""

    def __init__(self, outcome):
        self.sent = []
        self.outcome = outcome

    def write(self, frame):
        self.sent.append(bytes(frame))

    def read(self):
        return self.outcome


def _conn(outcome, user):
    c = skaidb.Connection.__new__(skaidb.Connection)
    c._tls_ctx = object()
    fake = FakeConn(outcome)
    c._write_frame = fake.write
    c._read_frame = fake.read
    return c, fake


OK = bytes([13, 1]) + bytes(32)


class CertificateHandshake(unittest.TestCase):
    def test_auth_start_carries_mechanism_external_and_no_nonce(self):
        c, fake = _conn(OK, "app")
        c._handshake_certificate("app")
        self.assertEqual(fake.sent, [bytes([10]) + _enc_str("app") + _enc_str("") + bytes([2])])

    def test_all_zero_signature_is_accepted_without_verification(self):
        c, _ = _conn(OK, "")
        c._handshake_certificate("")

    def test_denied_carries_the_reason(self):
        c, _ = _conn(bytes([13, 0]) + _enc_str("no user matches the client certificate"), "")
        with self.assertRaisesRegex(skaidb.OperationalError, "no user matches"):
            c._handshake_certificate("")

    def test_needs_tls(self):
        c, _ = _conn(OK, "")
        c._tls_ctx = None
        with self.assertRaisesRegex(skaidb.OperationalError, "needs TLS"):
            c._handshake_certificate("")

    def test_default_user_asserts_no_name(self):
        seen = []
        with mock.patch.object(skaidb.Connection, "_handshake_certificate",
                               lambda self, user: seen.append(user) or (_ for _ in ()).throw(OSError("stop"))):
            for user, expected in (("anonymous", ""), ("app", "app")):
                seen.clear()
                with self.assertRaises(skaidb.OperationalError):
                    skaidb.connect(host="127.0.0.1", port=_free_port_listening(), user=user,
                                   auth_mechanism="certificate", tls_insecure=True,
                                   auto_reconnect=False, connect_timeout=2)
                self.assertEqual(seen, [expected])


_listeners = []


def _free_port_listening():
    """A local port that accepts TCP and answers the TLS handshake."""
    import socket, ssl, subprocess, tempfile, threading, os
    d = tempfile.mkdtemp()
    key, crt = os.path.join(d, "k.pem"), os.path.join(d, "c.pem")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key,
                    "-out", crt, "-days", "1", "-subj", "/CN=skaidb"], check=True, capture_output=True)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(crt, key)
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(4)
    _listeners.append(srv)

    def serve():
        while True:
            try:
                s, _ = srv.accept()
                ctx.wrap_socket(s, server_side=True)
            except OSError:
                return
    threading.Thread(target=serve, daemon=True).start()
    return srv.getsockname()[1]


if __name__ == "__main__":
    unittest.main()
