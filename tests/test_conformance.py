"""The shared skaidb wire-protocol conformance suite.

`conformance/vectors.json` is generated from the server's reference encoders
(https://skaidb.org/conformance/vectors.json; contract in
`conformance/README.md`). This harness runs it against the driver's PUBLIC
API through a fake server that sends the reference bytes — never bytes this
driver encoded — and checks the exact bytes the driver sends.

Pure standard library; run with `python -m pytest tests` or
`python -m unittest tests.test_conformance`.
"""

import datetime as _dt
import decimal
import hashlib
import hmac
import json
import os
import socket
import struct
import threading
import unittest
import uuid

import skaidb

HERE = os.path.dirname(os.path.abspath(__file__))
VECTORS_PATH = os.path.join(HERE, "..", "conformance", "vectors.json")
with open(VECTORS_PATH, encoding="utf-8") as f:
    V = json.load(f)

EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)


# ---- tagged JSON <-> Python ---------------------------------------------------


def tagged(v):
    """A value the driver surfaced, in the vectors' tagged form."""
    if v is None:
        return {"null": True}
    if isinstance(v, bool):
        return {"bool": v}
    if isinstance(v, int):
        return {"int": str(v)}
    if isinstance(v, float):
        return {"float": v, "float_bits": struct.pack(">d", v).hex()}
    if isinstance(v, decimal.Decimal):
        sign, digits, exponent = v.as_tuple()
        mantissa = int("".join(map(str, digits)) or "0") * (-1 if sign else 1)
        if exponent > 0:
            mantissa *= 10**exponent
            exponent = 0
        return {"decimal": {"mantissa": str(mantissa), "scale": -exponent}}
    if isinstance(v, str):
        return {"string": v}
    if isinstance(v, (bytes, bytearray)):
        return {"bytes": bytes(v).hex()}
    if isinstance(v, uuid.UUID):
        return {"uuid": str(v)}
    if isinstance(v, _dt.datetime):
        return {"timestamp_ms": str((v - EPOCH) // _dt.timedelta(milliseconds=1))}
    if isinstance(v, (list, tuple)):
        return {"array": [tagged(x) for x in v]}
    if isinstance(v, dict):
        return {"document": [{"key": k, "value": tagged(x)} for k, x in v.items()]}
    raise AssertionError(f"unmapped driver value {v!r}")


def native(t):
    """A tagged value as the Python value the driver binds."""
    if "null" in t:
        return None
    if "bool" in t:
        return t["bool"]
    if "int" in t:
        return int(t["int"])
    if "float_bits" in t:
        return struct.unpack(">d", bytes.fromhex(t["float_bits"]))[0]
    if "decimal" in t:
        m = int(t["decimal"]["mantissa"])
        return decimal.Decimal((1 if m < 0 else 0, tuple(int(d) for d in str(abs(m))), -t["decimal"]["scale"]))
    if "string" in t:
        return t["string"]
    if "bytes" in t:
        return bytes.fromhex(t["bytes"])
    if "uuid" in t:
        return uuid.UUID(t["uuid"])
    if "timestamp_ms" in t:
        return EPOCH + _dt.timedelta(milliseconds=int(t["timestamp_ms"]))
    if "array" in t:
        return [native(x) for x in t["array"]]
    if "document" in t:
        return {e["key"]: native(e["value"]) for e in t["document"]}
    raise AssertionError(f"unknown tagged value {t}")


def rows_json(columns, rows):
    return {"columns": list(columns), "rows": [[tagged(v) for v in r] for r in rows]}


# ---- the scripted fake server ------------------------------------------------


def _read_frame(sock):
    head = _read_exact(sock, 4)
    if head is None:
        return None
    (n,) = struct.unpack(">I", head)
    return _read_exact(sock, n)


def _read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def _send(sock, payload):
    sock.sendall(struct.pack(">I", len(payload)) + payload)


def _str(b, pos):
    (n,) = struct.unpack_from("<I", b, pos)
    return b[pos + 4 : pos + 4 + n], pos + 4 + n


class FakeServer:
    """One connection: the handshake per `outcome`, then `exchanges`
    (list of (request bytes, [response bytes...])). Any mismatch is kept in
    `self.error` for the test to assert."""

    def __init__(self, outcome="ok", exchanges=(), die_on_request=False):
        self.outcome = outcome
        # Close the connection on the first real request instead of
        # answering — a member going down mid-statement.
        self.die_on_request = die_on_request
        self.requests = []
        self.exchanges = list(exchanges)
        self.error = None
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(1)
        self.port = self.sock.getsockname()[1]
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        try:
            conn, _ = self.sock.accept()
            with conn:
                self._serve(conn)
        except Exception as e:  # noqa: BLE001 - surfaced through self.error
            self.error = f"{type(e).__name__}: {e}"

    def _serve(self, conn):
        auth = V["auth"]
        start = _read_frame(conn)
        assert start[0] == 10, "expected AuthStart"
        user, pos = _str(start, 1)
        client_nonce, pos = _str(start, pos)
        salt = bytes.fromhex(auth["challenge"]["salt"])
        iterations = auth["challenge"]["iterations"]
        server_nonce = client_nonce + auth["challenge"]["server_nonce_suffix"].encode()
        challenge = bytes([11]) + struct.pack("<I", len(salt)) + salt + struct.pack("<I", iterations)
        challenge += struct.pack("<I", len(server_nonce)) + server_nonce
        _send(conn, challenge)
        finish = _read_frame(conn)
        assert finish[0] == 12, "expected AuthFinish"
        am = b"\0".join([user, client_nonce, server_nonce, salt.hex().encode(), str(iterations).encode()])
        salted = hashlib.pbkdf2_hmac("sha256", auth["password"].encode(), salt, iterations, 32)
        client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
        client_sig = hmac.new(hashlib.sha256(client_key).digest(), am, hashlib.sha256).digest()
        proof = bytes(a ^ b for a, b in zip(client_key, client_sig))
        if finish[1:33] != proof:
            raise AssertionError("client proof did not verify")
        server_sig = hmac.new(
            hmac.new(salted, b"Server Key", hashlib.sha256).digest(), am, hashlib.sha256
        ).digest()
        if self.outcome == "ok":
            _send(conn, bytes([13, 1]) + server_sig)
        elif self.outcome == "bad_server_signature":
            _send(conn, bytes([13, 1]) + b"\xaa" * 32)
            return
        else:
            denied = next(o for o in auth["outcomes"] if o["name"] == "denied")
            _send(conn, bytes.fromhex(denied["payload"]))
            return
        ddl = bytes.fromhex(V["ignorable_requests"]["ddl_payload"])
        pending = list(self.exchanges)
        while True:
            req = _read_frame(conn)
            if req is None:
                if pending:
                    self.error = f"never received request {pending[0][0].hex()}"
                return
            if req[:1] in (b"\x04", b"\x08"):
                _send(conn, ddl)
                continue
            self.requests.append(req)
            if self.die_on_request:
                return
            if not pending:
                self.error = f"unexpected extra request {req.hex()}"
                return
            want, responses = pending.pop(0)
            if req != want:
                self.error = f"request mismatch:\n  got  {req.hex()}\n  want {want.hex()}"
                return
            for r in responses:
                _send(conn, r)

    def join(self):
        self.thread.join(timeout=10)
        self.sock.close()


def _connect(server):
    return skaidb.connect(
        host="127.0.0.1",
        port=server.port,
        user=V["auth"]["username"],
        password=V["auth"]["password"],
        timeout=5,
        auto_reconnect=False,
    )


# ---- running a case's call through the public API --------------------------


def _outcome(fn):
    try:
        return fn()
    except skaidb.ProgrammingError as e:
        return {"error": str(e)}


def run_call(conn, call):
    method = call["method"]
    if method == "sequence":
        return {"sequence": [run_call(conn, c) for c in call["calls"]]}
    if method == "query":
        cur = conn.cursor()
        cur.set_consistency(call["consistency"])

        def go():
            cur.execute(call["sql"])
            return _cursor_result(cur)

        return _outcome(go)
    if method == "query_stream":
        def go():
            stream = conn.stream(call["sql"])
            if not stream.columns:
                return {"affected": str(stream.affected)}
            rows = []
            try:
                for row in stream:
                    rows.append(row)
            except skaidb.ProgrammingError as e:
                return {"rows_then_error": {"rows": rows_json(stream.columns, rows), "error": str(e)}}
            return {"rows": rows_json(stream.columns, rows)}

        return _outcome(go)
    if method == "execute_prepared":
        cur = conn.cursor()
        cur.set_consistency(call["consistency"])

        def go():
            cur.execute(call["sql"], [native(p) for p in call["params"]])
            return _cursor_result(cur)

        return _outcome(go)
    if method == "execute_batch":
        cur = conn.cursor()
        cur.set_consistency(call["consistency"])

        def go():
            cur.executemany(call["sql"], [[native(p) for p in row] for row in call["rows"]])
            return {"affected": str(cur.rowcount)}

        return _outcome(go)
    raise AssertionError(f"unknown call.method {method}")


def _cursor_result(cur):
    if cur.description is None:
        return {"ddl": True} if cur.rowcount == -1 else {"affected": str(cur.rowcount)}
    sets = []
    while True:
        sets.append(rows_json([d[0] for d in cur.description], cur.fetchall()))
        if not cur.nextset():
            break
    return {"result_sets": sets} if len(sets) > 1 else {"rows": sets[0]}


def matches(expect, got):
    if "error" in expect:
        return "error" in got and expect["error"] in got["error"]
    if "sequence" in expect:
        return "sequence" in got and len(expect["sequence"]) == len(got["sequence"]) and all(
            matches(e, g) for e, g in zip(expect["sequence"], got["sequence"])
        )
    if "rows_then_error" in expect:
        e, g = expect["rows_then_error"], got.get("rows_then_error")
        return g is not None and e["rows"] == g["rows"] and e["error"] in g["error"]
    return expect == got


# ---- the tests -------------------------------------------------------------


class Values(unittest.TestCase):
    def test_every_value_decodes_and_encodes(self):
        for entry in V["values"]:
            with self.subTest(entry["name"]):
                encoded = bytes.fromhex(entry["encoded"])
                decoded = skaidb._decode_value(skaidb._Reader(encoded))
                self.assertEqual(tagged(decoded), entry["value"])
                self.assertEqual(skaidb._encode_value(native(entry["value"])).hex(), entry["encoded"])


class Scram(unittest.TestCase):
    def test_computations(self):
        for s in V["scram"]:
            with self.subTest(s["username"]):
                salt = bytes.fromhex(s["salt"])
                am = "\0".join(
                    [s["username"], s["client_nonce"], s["server_nonce"], salt.hex(), str(s["iterations"])]
                ).encode("utf-8")
                self.assertEqual(am.hex(), s["auth_message"])
                proof, sig = skaidb._scram_proof(s["password"], salt, s["iterations"], am)
                self.assertEqual(proof.hex(), s["client_proof"])
                self.assertEqual(sig.hex(), s["server_signature"])


class AuthOutcomes(unittest.TestCase):
    def test_outcomes(self):
        for o in V["auth"]["outcomes"]:
            with self.subTest(o["name"]):
                server = FakeServer(outcome=o["name"])
                if o["expect"] == "connected":
                    _connect(server).close()
                else:
                    with self.assertRaises(skaidb.OperationalError) as ctx:
                        _connect(server)
                    if "reason" in o:
                        self.assertIn(o["reason"], str(ctx.exception))
                server.join()
                self.assertIsNone(server.error)


class Cases(unittest.TestCase):
    def test_cases(self):
        for case in V["cases"]:
            with self.subTest(case["name"]):
                exchanges = [
                    (bytes.fromhex(e["request"]), [bytes.fromhex(r) for r in e["responses"]])
                    for e in case["exchanges"]
                ]
                server = FakeServer(exchanges=exchanges)
                conn = _connect(server)
                try:
                    got = run_call(conn, case["call"])
                finally:
                    conn.close()
                server.join()
                self.assertIsNone(server.error, case["name"])
                self.assertTrue(
                    matches(case["expect"], got),
                    f"{case['name']}:\n  expected {case['expect']}\n  got      {got}",
                )


if __name__ == "__main__":
    unittest.main()
