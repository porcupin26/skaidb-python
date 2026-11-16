"""Transactions: the typed conflict error and the retrying `transaction()`
helper — against a fake server that records every statement it receives and
answers from a per-test script.

The rules under test are PROTOCOL.md §6: a transaction is statements on ONE
connection (`BEGIN`, the statements, `COMMIT` or `ROLLBACK`), and a server
`Error` whose message starts with `transaction conflict:` is a refused
`COMMIT` — nothing was applied, the connection stays usable, and a retrying
helper retries on exactly that prefix.
"""

import hashlib
import hmac
import os
import re
import select
import shutil
import socket
import ssl
import struct
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock

import skaidb

try:
    from test_conformance import V, _read_frame, _send, _str
    from test_stream import _f_chunk, _f_end, _f_error, _f_header, _f_mutation
except ImportError:  # run as `python -m unittest tests.test_transactions`
    from tests.test_conformance import V, _read_frame, _send, _str
    from tests.test_stream import _f_chunk, _f_end, _f_error, _f_header, _f_mutation

# The server's message today. Only the prefix is contract.
CONFLICT = (
    "transaction conflict: a row of table accounts was changed by another "
    "writer after the transaction began — nothing was committed; run the "
    "transaction again"
)

DEBIT = "UPDATE accounts SET balance = balance - 10 WHERE id = 1"
CREDIT = "UPDATE accounts SET balance = balance + ? WHERE id = ?"  # prepared
SCAN = "SELECT id FROM accounts"

_DDL = bytes([skaidb._RESP_DDL])
# Scripted in place of a reply: close the connection without answering.
DROP = object()
# Scripted in place of a reply: do not answer, and keep the connection open.
STALL = object()


class Late:
    """Scripted in place of a reply: `reply`, sent `delay` seconds late."""

    def __init__(self, reply, delay):
        self.reply = reply
        self.delay = delay


class Then:
    """Scripted in place of a reply: `reply`, and then the server does
    `what` to the connection, which the client is about to leave idle:

    * `HALF_CLOSE` — shuts down its sending side only, and keeps reading
      and discarding whatever still arrives (`Server.discarded`). The client
      reads EOF, and its writes succeed: a closed connection as it looks
      over a real network, where the reset for a write comes back too late
      to fail it;
    * `CLOSE_NOTIFY` — the same, announced with TLS's own alert;
    * `RESET` — closes with a reset (`SO_LINGER` 0);
    * a payload — sends it as one more frame nobody asked for."""

    def __init__(self, reply, what):
        self.reply = reply
        self.what = what


HALF_CLOSE = object()
CLOSE_NOTIFY = object()
RESET = object()
# Scripted in place of a statement's text: the Hello a connection opens with.
HELLO = ("hello",)


def _rows(columns, rows) -> bytes:
    """A Rows frame: the columns, then the rows (as a stream's header and
    chunk lay them out)."""
    return bytes([skaidb._RESP_ROWS]) + _f_header(columns)[1:] + _f_chunk(rows)[1:]


# ---- the fake server --------------------------------------------------------


class Server:
    """A fake skaidb server: SCRAM per the conformance vectors on every
    connection, then each statement is recorded and answered.

    `statements` is every statement received, in order — the text of a
    one-shot or stream request, or the text behind the prepared id of an
    execute; `log` pairs each with the number of the connection it arrived
    on, `connections` counts the connections accepted and `hung_up(n)`
    tells whether the client has closed connection `n`. `ran_in` pairs each
    statement with the database its session was in — the one the last `USE`
    answered with success named, None on a session that entered none — and
    `ran_under` with the session's settings: the level of the last `SET
    CONSISTENCY` and the limits of the last `SET SCAN BUDGET` answered with
    success, None for one never set or cleared with `DEFAULT`.
    `restart()` closes every open connection, as a restarting server does.
    `discarded(n)` is what connection `n` received after the server
    half-closed it (`Then`). With `tls=True` every connection is wrapped in
    TLS, under a certificate made for the run.

    Each connection has the real server's prepared-statement table: at most
    `MAX_PREPARED` statements, a slot freed by the close opcode, a prepare
    beyond that refused — and one of transaction or session control refused
    as a kind that cannot be prepared. `prepared_open(n)` is the number connection `n`
    holds now and `prepared_peak` the most any connection ever held; `ops`
    pairs every statement with the opcode that carried it, and `bound` with
    the typed parameters of an execute. An execute naming a slot the
    connection does not hold is refused and noted in `unknown_ids`.

    `on(sql, *replies)` scripts the answers to the next executions of `sql`
    (or, for `HELLO`, to the next connections' Hello, which is otherwise
    acknowledged and not recorded)
    and `always(sql, reply)` the answer to every later one; a reply is a
    payload, a list of payloads (the frames of a stream), `DROP`, `STALL`, a
    `Late` one of those, or a `Then` (a reply, and what the server does to
    the connection after it).
    Unscripted, `BEGIN …` / `COMMIT` / `ROLLBACK` / `USE …` / `SET …` are
    acknowledged, a `SELECT` answers one row and anything else is a one-row
    mutation."""

    MAX_PREPARED = 256
    TABLE_FULL = "too many prepared statements (max 256); close some first"

    def __init__(self, tls=False):
        self._tls = _server_tls() if tls else None
        self._discarded = {}
        self.log = []
        self.ran_in = []
        self.ran_under = []
        self.ops = []
        self.bound = []
        self.unknown_ids = []
        self.prepared_peak = 0
        self._prepared_open = {}
        self.connections = 0
        self._open = []
        self._hung_up = set()
        self._script = {}
        self._always = {}
        self._lock = threading.Lock()
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(16)
        self.port = self.sock.getsockname()[1]
        self.closed = False
        threading.Thread(target=self._accept, daemon=True).start()

    def on(self, sql, *replies, prepare=False):
        """Answer the next executions of `sql` (with `prepare`, the next
        requests to prepare it) with `replies`, in order."""
        with self._lock:
            self._script.setdefault(("prepare", sql) if prepare else sql, []).extend(replies)

    def always(self, sql, reply):
        with self._lock:
            self._always[sql] = reply

    @property
    def statements(self):
        with self._lock:
            return [sql for _, sql in self.log]

    def hung_up(self, number, within=2.0) -> bool:
        """Whether the client has closed connection `number`. Waits up to
        `within` seconds for this server's thread to see it, so a socket the
        client closed at once reads True, and one it still holds False."""
        deadline = time.monotonic() + within
        while True:
            with self._lock:
                if number in self._hung_up:
                    return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.005)

    def prepared_open(self, number) -> int:
        with self._lock:
            return self._prepared_open.get(number, 0)

    def discarded(self, number) -> bytes:
        """The bytes the client sent on connection `number` after the server
        half-closed it."""
        with self._lock:
            return bytes(self._discarded.get(number, b""))

    def restart(self):
        """Close every connection accepted so far; the listener stays."""
        with self._lock:
            gone, self._open = self._open, []
        for conn in gone:
            try:
                # The transport's own shutdown, also under TLS: the thread
                # serving the connection is reading from it.
                socket.socket.shutdown(conn, socket.SHUT_RDWR)
            except OSError:
                pass

    def close(self):
        self.closed = True
        self.sock.close()

    def _accept(self):
        while not self.closed:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            with self._lock:
                self.connections += 1
                number = self.connections
                self._open.append(conn)
            threading.Thread(target=self._serve_safe, args=(conn, number), daemon=True).start()

    def _serve_safe(self, conn, number):
        try:
            if self._tls is not None:
                conn = self._tls.wrap_socket(conn, server_side=True)
                with self._lock:
                    self._open.append(conn)
            with conn:
                if self._handshake(conn):
                    self._serve(conn, number)
                    return
        except OSError:  # reset: the client closed with a reply still unread
            pass
        # Closed by the client in the middle of the handshake, or reset.
        with self._lock:
            self._hung_up.add(number)

    @staticmethod
    def _handshake(conn) -> bool:
        auth = V["auth"]
        start = _read_frame(conn)
        if start is None:
            return False
        user, pos = _str(start, 1)
        client_nonce, pos = _str(start, pos)
        salt = bytes.fromhex(auth["challenge"]["salt"])
        iterations = auth["challenge"]["iterations"]
        server_nonce = client_nonce + auth["challenge"]["server_nonce_suffix"].encode()
        challenge = bytes([11]) + struct.pack("<I", len(salt)) + salt + struct.pack("<I", iterations)
        challenge += struct.pack("<I", len(server_nonce)) + server_nonce
        _send(conn, challenge)
        if _read_frame(conn) is None:
            return False
        am = b"\0".join([user, client_nonce, server_nonce, salt.hex().encode(), str(iterations).encode()])
        salted = hashlib.pbkdf2_hmac("sha256", auth["password"].encode(), salt, iterations, 32)
        server_sig = hmac.new(
            hmac.new(salted, b"Server Key", hashlib.sha256).digest(), am, hashlib.sha256
        ).digest()
        _send(conn, bytes([13, 1]) + server_sig)
        return True

    def _serve(self, conn, number):
        prepared = {}  # this connection's prepared statements, by slot
        database = None  # the session's database
        settings = {"CONSISTENCY": None, "SCAN BUDGET": None}  # and its settings
        while True:
            req = _read_frame(conn)
            if req is None:  # the client closed the connection
                with self._lock:
                    self._hung_up.add(number)
                return
            op = req[0]
            if op == skaidb._OP_CLOSE:
                prepared.pop(struct.unpack_from("<I", req, 1)[0], None)
                with self._lock:
                    self._prepared_open[number] = len(prepared)
            if op == skaidb._OP_CLOSE:
                _send(conn, _DDL)
                continue
            if op == skaidb._OP_HELLO:
                reply = self._scripted(HELLO)
                if reply is None:
                    reply = _DDL
            elif op == skaidb._OP_PREPARE:
                sql = self._sql(req, 1)
                reply = self._scripted(("prepare", sql))
                if reply is None and self._session_control(_without_comments(sql)):
                    reply = _f_error("statement kind cannot be prepared")
                if reply is None and len(prepared) >= self.MAX_PREPARED:
                    reply = _f_error(self.TABLE_FULL)
                if reply is None:
                    slot = next(i for i in range(len(prepared) + 1) if i not in prepared)
                    prepared[slot] = sql
                    with self._lock:
                        self._prepared_open[number] = len(prepared)
                        self.prepared_peak = max(self.prepared_peak, len(prepared))
                    reply = bytes([skaidb._RESP_PREPARED]) + struct.pack(
                        "<IH", slot, sql.count("?")
                    )
            else:
                if op in (skaidb._OP_EXECUTE, skaidb._OP_EXECUTE_BATCH):
                    slot = struct.unpack_from("<I", req, 2)[0]
                    if slot not in prepared:  # an id this connection never issued
                        with self._lock:
                            self.unknown_ids.append((number, slot))
                        _send(conn, _f_error(f"unknown prepared statement id {slot}"))
                        continue
                    sql = prepared[slot]
                else:  # one-shot query or stream request: the text itself
                    sql = self._sql(req, 2)
                with self._lock:
                    self.log.append((number, sql))
                    self.ran_in.append((database, sql))
                    self.ran_under.append((settings["CONSISTENCY"], settings["SCAN BUDGET"], sql))
                    self.ops.append((op, sql))
                    if op == skaidb._OP_EXECUTE:
                        self.bound.append((sql, self._params(req)))
                reply = self._scripted(sql)
                read = _without_comments(sql)  # what the server parses
                if reply is None:
                    reply = self._default(read)
                use = _USE.match(read)
                if use and reply == _DDL:
                    database = use.group(1).replace('""', '"') if use.group(1) is not None else use.group(2)
                setting = _SET.match(read)
                if setting and reply == _DDL:
                    kind = " ".join(setting.group(1).upper().split())
                    value = " ".join(setting.group(2).upper().split())
                    settings[kind] = None if value == "DEFAULT" else value
            if isinstance(reply, Late):
                time.sleep(reply.delay)
                reply = reply.reply
            then = None
            if isinstance(reply, Then):
                reply, then = reply.reply, reply.what
            if reply is DROP:
                return
            if reply is STALL:
                continue
            for payload in reply if isinstance(reply, list) else [reply]:
                _send(conn, payload)
            if then is RESET:
                conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                return  # closed on the way out: a reset, not a FIN
            if then is HALF_CLOSE or then is CLOSE_NOTIFY:
                self._half_close(conn, number, notify=then is CLOSE_NOTIFY)
                return
            if then is not None:
                _send(conn, then)

    def _half_close(self, conn, number, notify):
        """Stop sending on `conn` — with TLS's close alert first, if
        `notify` — and read on, discarding and counting what arrives, until
        the client closes."""
        with self._lock:
            self._discarded[number] = bytearray()
        if notify:
            conn.settimeout(0)
            try:
                conn.unwrap()  # sends the alert; the client's is not waited for
            except (ssl.SSLWantReadError, ssl.SSLWantWriteError):
                pass
            conn.settimeout(None)
        # The transport's own shutdown: under TLS the session stays up for
        # reading, so what is counted is what the client sent to be read —
        # not the alert a TLS library may answer the close with.
        socket.socket.shutdown(conn, socket.SHUT_WR)
        try:
            while True:
                data = conn.recv(65536)
                if not data:
                    break
                with self._lock:
                    self._discarded[number] += data
        except ssl.SSLError:  # the client's TLS ended the session
            pass
        with self._lock:
            self._hung_up.add(number)

    @staticmethod
    def _params(req) -> list:
        """The typed parameters of an execute request, decoded."""
        (count,) = struct.unpack_from("<H", req, 6)
        pos, values = 8, []
        for _ in range(count):
            (n,) = struct.unpack_from("<I", req, pos)
            values.append(skaidb._decode_value(skaidb._Reader(req[pos + 4 : pos + 4 + n])))
            pos += 4 + n
        return values

    @staticmethod
    def _sql(req, pos) -> str:
        (n,) = struct.unpack_from("<I", req, pos)
        return req[pos + 4 : pos + 4 + n].decode()

    def _scripted(self, key):
        with self._lock:
            queue = self._script.get(key)
            if queue:
                return queue.pop(0)
            return self._always.get(key)

    @staticmethod
    def _session_control(sql) -> bool:
        """Whether `sql` is transaction or session control: acknowledged,
        and — as on the real server — a kind that cannot be prepared."""
        return sql.split()[:1] != [] and sql.split()[0].rstrip(";").upper() in (
            "BEGIN", "COMMIT", "ROLLBACK", "USE", "SET",
        )

    @classmethod
    def _default(cls, sql):
        if cls._session_control(sql):
            return _DDL
        if sql.startswith("SELECT"):
            return _rows(["n"], [(1,)])
        return _f_mutation(1)


# What this server reads as `USE [DATABASE] <name>`: the name in double
# quotes (group 1, `""` for a quote inside) or bare (group 2).
_USE = re.compile(
    r'\s*USE\s+(?:DATABASE\s+)?(?:"((?:[^"]|"")+)"|(\w+))\s*;?\s*\Z', re.IGNORECASE
)


# What it reads as `SET CONSISTENCY <level>` or `SET SCAN BUDGET <limits>`:
# the setting (group 1) and what it is set to (group 2).
_SET = re.compile(
    r"\s*SET\s+(CONSISTENCY|SCAN\s+BUDGET)\s+(.*?)\s*;?\s*\Z", re.IGNORECASE | re.DOTALL
)


def _without_comments(sql) -> str:
    """`sql` as the server parses it: without its `--` comments, each of
    which runs to the end of its line — a `--` inside a double-quoted name
    or a quoted string is not one."""
    return re.sub(
        r"""("(?:[^"]|"")*"|'(?:[^']|'')*')|--[^\n]*""", lambda m: m.group(1) or "", sql
    )


_TLS = []


def _server_tls() -> ssl.SSLContext:
    """A server-side TLS context under a self-signed certificate, made once
    per run with the `openssl` command (as tests/test_certificate_auth.py
    makes its own)."""
    if not _TLS:
        if shutil.which("openssl") is None:
            raise unittest.SkipTest("needs the openssl command to make a certificate")
        d = tempfile.mkdtemp()
        key, crt = os.path.join(d, "k.pem"), os.path.join(d, "c.pem")
        try:
            subprocess.run(
                ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key,
                 "-out", crt, "-days", "1", "-subj", "/CN=skaidb"],
                check=True, capture_output=True,
            )
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(crt, key)
        finally:
            shutil.rmtree(d, ignore_errors=True)
        _TLS.append(ctx)
    return _TLS[0]


def _credentials(server):
    kw = dict(
        host="127.0.0.1",
        port=server.port,
        user=V["auth"]["username"],
        password=V["auth"]["password"],
        timeout=5,
    )
    if server._tls is not None:
        kw["tls_insecure"] = True
    return kw


class _Case(unittest.TestCase):
    """A fake server and one connection to it with the driver's defaults
    (automatic reconnect on): a statement the driver wrongly re-dialed for
    shows up as a second connection."""

    def setUp(self):
        self.server = Server()
        self.addCleanup(self.server.close)
        self.conn = skaidb.connect(**_credentials(self.server))
        self.addCleanup(self.conn.close)

    def assert_usable(self):
        """The connection carries the next statement, on the same socket."""
        self.assertTrue(self.conn.is_usable())
        self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])
        self.assertEqual(self.server.connections, 1)


# ---- the typed error --------------------------------------------------------


class ConflictError(_Case):
    def test_it_is_a_statement_error_and_exported(self):
        self.assertTrue(issubclass(skaidb.TransactionConflict, skaidb.ProgrammingError))
        self.assertIn("TransactionConflict", skaidb.__all__)

    def test_a_refused_commit_raises_it_and_the_connection_stays_usable(self):
        self.server.on("COMMIT", _f_error(CONFLICT))
        self.conn.execute("BEGIN")
        self.conn.execute(DEBIT)
        with self.assertRaises(skaidb.TransactionConflict) as cm:
            self.conn.execute("COMMIT")
        self.assertEqual(str(cm.exception), CONFLICT, "the server's message, unchanged")
        # A statement error, not a connection failure: nothing was re-dialed
        # and the COMMIT was not sent again.
        self.assert_usable()
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "COMMIT", "SELECT 1"])

    def test_code_that_catches_the_statement_error_still_catches_it(self):
        bases = (skaidb.ProgrammingError, skaidb.DatabaseError, skaidb.Error)
        self.server.on("COMMIT", *[_f_error(CONFLICT)] * len(bases))
        for base in bases:
            with self.subTest(base.__name__):
                try:
                    self.conn.execute("COMMIT")
                except base as e:
                    self.assertIsInstance(e, skaidb.TransactionConflict)
                else:
                    self.fail("the refused COMMIT did not raise")

    def test_only_the_prefix_decides(self):
        cases = [
            ("transaction conflict: reworded by a later server", True),
            ("transaction conflict:", True),
            ("row 2: transaction conflict: not at the start", False),
            ("Transaction conflict: another spelling", False),
            ("constraint violated", False),
        ]
        self.server.on("COMMIT", *[_f_error(msg) for msg, _ in cases])
        for msg, conflict in cases:
            with self.subTest(msg):
                with self.assertRaises(skaidb.ProgrammingError) as cm:
                    self.conn.execute("COMMIT")
                self.assertEqual(str(cm.exception), msg)
                self.assertIs(
                    type(cm.exception),
                    skaidb.TransactionConflict if conflict else skaidb.ProgrammingError,
                )

    def test_a_conflict_in_a_batch_row_is_one_too(self):
        # A failed row of an `executemany` batch is named before its error
        # and counted after it: `batch row 1: <error> (1 rows applied
        # before it)`. The prefix that decides is the row's error's.
        batch = "INSERT INTO audit (id) VALUES (?)"
        named = f"batch row 1: {CONFLICT} (1 rows applied before it)"
        cases = [
            (named, True),
            ("batch row 0: transaction conflict: reworded (0 rows applied before it)", True),
            ("batch row 1200: transaction conflict:", True),
            (CONFLICT, True),  # as a server without the batch opcode's rows would say it
            (
                'batch row 1: not null violation: column "v" of table "audit" is null '
                "(1 rows applied before it)",
                False,
            ),
            ("batch row 1: Transaction conflict: another spelling", False),
            ("batch row 1: a transaction conflict: not at the start of the row's error", False),
            ("batch row : transaction conflict: no row is named", False),
            ("batch row x: transaction conflict: no row is named", False),
            ("batch row 1 transaction conflict: not the server's naming", False),
            ("row 1: transaction conflict: not the server's naming", False),
        ]
        self.server.on(batch, *[_f_error(msg) for msg, _ in cases])
        cur = self.conn.cursor()
        for msg, conflict in cases:
            with self.subTest(msg):
                with self.assertRaises(skaidb.ProgrammingError) as cm:
                    cur.executemany(batch, [(1,), (2,)])
                self.assertEqual(str(cm.exception), msg, "the server's message, unchanged")
                self.assertIs(
                    type(cm.exception),
                    skaidb.TransactionConflict if conflict else skaidb.ProgrammingError,
                )
        # Only a batch names a row: the same text answering another
        # statement starts with something else than the prefix.
        self.server.on("COMMIT", _f_error(named))
        self.server.on(CREDIT, _f_error(named))
        for run in (lambda: self.conn.execute("COMMIT"), lambda: self.conn.execute(CREDIT, (10, 2))):
            with self.assertRaises(skaidb.ProgrammingError) as cm:
                run()
            self.assertIs(type(cm.exception), skaidb.ProgrammingError)
        self.assert_usable()

    def test_every_statement_path_raises_it(self):
        conflict = _f_error(CONFLICT)
        batch = "INSERT INTO audit (id) VALUES (?)"
        unprepared = "UPDATE accounts SET balance = ? WHERE id = ?"
        ranged = "SELECT id FROM accounts WHERE id > ?"
        self.server.on("COMMIT", conflict)
        self.server.on(CREDIT, conflict)
        self.server.on(unprepared, conflict, prepare=True)
        self.server.on(batch, conflict)
        self.server.on(SCAN, conflict, [_f_header(["id"]), _f_chunk([(1,)]), conflict])
        self.server.on(ranged, conflict)
        cur = self.conn.cursor()

        def midstream():
            rows = self.conn.stream(SCAN)
            self.assertEqual(next(rows), (1,))
            next(rows)

        paths = [
            ("one-shot query", lambda: cur.execute("COMMIT")),
            ("prepared execute", lambda: cur.execute(CREDIT, (10, 2))),
            ("prepare", lambda: cur.execute(unprepared, (10, 2))),
            ("executemany", lambda: cur.executemany(batch, [(1,), (2,)])),
            ("stream", lambda: self.conn.stream(SCAN)),
            ("stream with parameters", lambda: self.conn.stream(ranged, (1,))),
            ("a stream's later frame", midstream),
        ]
        for name, run in paths:
            with self.subTest(name):
                with self.assertRaises(skaidb.TransactionConflict) as cm:
                    run()
                self.assertEqual(str(cm.exception), CONFLICT)
                self.assertTrue(self.conn.is_usable())
        self.assert_usable()


# ---- the helper -------------------------------------------------------------


class Transaction(_Case):
    def test_success_is_begin_the_body_and_commit_once(self):
        runs = []

        def body(conn):
            self.assertIs(conn, self.conn, "the body gets the connection")
            runs.append(1)
            conn.execute(DEBIT)
            conn.execute(CREDIT, (10, 2))
            return "moved"

        self.assertEqual(self.conn.transaction(body), "moved")
        self.assertEqual(len(runs), 1)
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, CREDIT, "COMMIT"])

    def test_two_conflicts_then_success_runs_the_body_three_times(self):
        self.server.on("COMMIT", _f_error(CONFLICT), _f_error(CONFLICT))
        runs = []

        def body(conn):
            runs.append(len(runs) + 1)
            conn.execute(DEBIT)
            return f"run {runs[-1]}"

        # No backoff: the other writer has already finished.
        with mock.patch.object(skaidb.time, "sleep", side_effect=AssertionError("slept")):
            self.assertEqual(self.conn.transaction(body, attempts=3), "run 3")
        self.assertEqual(runs, [1, 2, 3])
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "COMMIT"] * 3)
        self.assert_usable()

    def test_a_conflict_on_every_commit_is_raised_after_the_last_attempt(self):
        self.server.always("COMMIT", _f_error(CONFLICT))
        runs = []

        def body(conn):
            runs.append(1)
            conn.execute(DEBIT)

        with self.assertRaises(skaidb.TransactionConflict) as cm:
            self.conn.transaction(body, attempts=2)
        self.assertEqual(str(cm.exception), CONFLICT)
        self.assertEqual(len(runs), 2)
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "COMMIT"] * 2)
        self.assert_usable()

    def test_three_attempts_by_default_and_one_means_no_retry(self):
        self.server.always("COMMIT", _f_error(CONFLICT))
        for attempts, kwargs in ((3, {}), (1, {"attempts": 1})):
            with self.subTest(attempts=attempts):
                runs = []
                with self.assertRaises(skaidb.TransactionConflict):
                    self.conn.transaction(lambda conn: runs.append(1), **kwargs)
                self.assertEqual(len(runs), attempts)

    def test_a_body_error_rolls_back_and_propagates(self):
        boom = RuntimeError("boom")

        def body(conn):
            conn.execute(DEBIT)
            raise boom

        with self.assertRaises(RuntimeError) as cm:
            self.conn.transaction(body)
        self.assertIs(cm.exception, boom)
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "ROLLBACK"])
        self.assert_usable()

    def test_a_refused_rollback_does_not_replace_the_body_error(self):
        # What a server before 0.347 answers a ROLLBACK with nothing open.
        self.server.on("ROLLBACK", _f_error("no transaction in progress"))
        boom = RuntimeError("boom")

        def body(conn):
            conn.execute(DEBIT)
            raise boom

        with self.assertRaises(RuntimeError) as cm:
            self.conn.transaction(body)
        self.assertIs(cm.exception, boom)
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "ROLLBACK"])
        self.assert_usable()

    def test_a_statement_failing_in_the_body_rolls_back(self):
        self.server.on(DEBIT, _f_error("no such table accounts"))
        with self.assertRaises(skaidb.ProgrammingError) as cm:
            self.conn.transaction(lambda conn: conn.execute(DEBIT))
        self.assertEqual(str(cm.exception), "no such table accounts")
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "ROLLBACK"])

    def test_a_conflict_raised_by_the_body_is_not_retried(self):
        # Only a refused COMMIT of the helper's own is retried: a conflict
        # that comes out of the body is a body error like any other.
        self.server.always(DEBIT, _f_error(CONFLICT))
        runs = []

        def body(conn):
            runs.append(1)
            conn.execute(DEBIT)

        with self.assertRaises(skaidb.TransactionConflict):
            self.conn.transaction(body, attempts=3)
        self.assertEqual(len(runs), 1)
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "ROLLBACK"])

    def test_an_exception_that_is_not_an_exception_subclass_rolls_back_too(self):
        class Stop(BaseException):
            pass

        def body(conn):
            conn.execute(DEBIT)
            raise Stop()

        with self.assertRaises(Stop):
            self.conn.transaction(body)
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "ROLLBACK"])

    def test_a_refused_begin_runs_nothing(self):
        # Whatever the error says — a conflict prefix included — the body
        # must not run: its statements would each commit on their own.
        for msg in ("permission denied", CONFLICT):
            with self.subTest(msg):
                self.server.log.clear()
                self.server.on("BEGIN", _f_error(msg))
                ran = []
                with self.assertRaises(skaidb.ProgrammingError) as cm:
                    self.conn.transaction(lambda conn: ran.append(1))
                self.assertEqual(str(cm.exception), msg)
                self.assertEqual(ran, [])
                self.assertEqual(self.server.statements, ["BEGIN"], "no COMMIT, no ROLLBACK")

    def test_another_commit_error_is_not_retried_and_not_rolled_back(self):
        self.server.on("COMMIT", _f_error("constraint violated: balance >= 0"))
        runs = []

        def body(conn):
            runs.append(1)
            conn.execute(DEBIT)

        with self.assertRaises(skaidb.ProgrammingError) as cm:
            self.conn.transaction(body)
        self.assertIs(type(cm.exception), skaidb.ProgrammingError)
        self.assertEqual(len(runs), 1)
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "COMMIT"])
        self.assert_usable()

    def test_attempts_below_one_is_refused_before_anything_is_sent(self):
        ran = []
        for attempts in (0, -1):
            with self.subTest(attempts=attempts):
                with self.assertRaises(ValueError):
                    self.conn.transaction(lambda conn: ran.append(1), attempts=attempts)
        self.assertEqual(ran, [])
        self.assertEqual(self.server.statements, [])

    def test_a_custom_begin_is_sent_verbatim_on_every_run(self):
        begin = "BEGIN ATOMIC PARTITION (tenant = 7)"
        self.server.on("COMMIT", _f_error(CONFLICT))
        self.conn.transaction(lambda conn: conn.execute(DEBIT), begin=begin)
        self.assertEqual(self.server.statements, [begin, DEBIT, "COMMIT"] * 2)


class OneConnection(_Case):
    """The transaction lives on the socket it began on: a transport failure
    is the usual OperationalError and nothing of the transaction is sent
    over a fresh connection, where it would commit on its own."""

    def test_a_begin_on_a_lost_connection_redials_like_any_statement(self):
        # Nothing is open yet, so the usual reconnect applies (an idle
        # connection whose server restarted): the transaction then runs
        # whole on the fresh connection.
        self.server.on("BEGIN", DROP)
        self.assertEqual(self.conn.transaction(lambda conn: conn.execute(DEBIT).rowcount), 1)
        self.assertEqual(
            self.server.log, [(1, "BEGIN"), (2, "BEGIN"), (2, DEBIT), (2, "COMMIT")]
        )

    def test_a_connection_lost_in_the_body_is_not_redialed(self):
        self.server.on(DEBIT, DROP)
        with self.assertRaises(skaidb.OperationalError):
            self.conn.transaction(lambda conn: conn.execute(DEBIT))
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT], "not run again, no ROLLBACK")
        self.assertEqual(self.server.connections, 1)
        # Outside the transaction the connection reconnects as it always did.
        self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])
        self.assertEqual(self.server.connections, 2)

    def test_a_body_that_swallows_the_failure_cannot_commit_elsewhere(self):
        self.server.on(DEBIT, DROP)

        def body(conn):
            try:
                conn.execute(DEBIT)
            except skaidb.OperationalError:
                pass

        with self.assertRaises(skaidb.OperationalError):
            self.conn.transaction(body)
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT], "no COMMIT on a fresh connection")
        self.assertEqual(self.server.connections, 1)

    def test_nothing_of_the_body_follows_a_lost_connection_onto_a_fresh_one(self):
        # PROTOCOL.md §6 rule 7, the whole of it: a statement succeeds, the
        # connection is lost under the next one, the body swallows that
        # error, tries a write, swallows that too and returns normally.
        audit = "INSERT INTO audit (id) VALUES (1)"
        writes = [
            ("a statement", lambda conn: conn.execute(audit)),
            ("a prepared statement", lambda conn: conn.execute(CREDIT, (10, 3))),
            ("executemany", lambda conn: conn.cursor().executemany(CREDIT, [(10, 3), (10, 4)])),
            ("a stream", lambda conn: conn.stream(audit)),
        ]
        for name, write in writes:
            with self.subTest(name):
                server = Server()
                self.addCleanup(server.close)
                conn = skaidb.connect(**_credentials(server))
                self.addCleanup(conn.close)
                server.on(DEBIT, DROP)
                raised = []

                def body(conn):
                    conn.execute(CREDIT, (10, 2))  # statement 1: succeeds
                    for statement in (lambda conn: conn.execute(DEBIT), write):
                        try:
                            statement(conn)  # 2: the connection is lost; 3: a write
                        except skaidb.OperationalError as e:
                            raised.append(e)
                    return "done"

                with self.assertRaises(skaidb.OperationalError) as cm:
                    conn.transaction(body)
                self.assertEqual(len(raised), 2, "statement 2 raised, and statement 3 too")
                # Statement 3 was not sent, and neither COMMIT nor ROLLBACK
                # went to a session that never had the transaction.
                self.assertEqual(server.log, [(1, "BEGIN"), (1, CREDIT), (1, DEBIT)])
                self.assertEqual(server.connections, 1)
                # The loss was under a statement of the body, so the COMMIT
                # was never sent and the error can say what that means.
                # (Lost under the COMMIT itself it must not:
                # test_a_connection_lost_at_commit_is_not_retried.)
                self.assertIn("connection lost", str(cm.exception))
                self.assertIn("nothing was committed", str(cm.exception))
                # The same connection carries ordinary statements again, as
                # after any lost connection.
                self.assertEqual(conn.execute("SELECT 1").fetchall(), [(1,)])
                self.assertEqual(server.log[-1], (2, "SELECT 1"))

    def test_the_body_cannot_redial_its_way_out_of_the_transaction(self):
        # A body that answers the loss with reconnect() would run the rest
        # of its statements on a session with no transaction — each one
        # committing on its own — and the helper's COMMIT would go there
        # too. reconnect() is refused until the transaction has ended.
        self.server.on(DEBIT, DROP)
        refused = []

        def body(conn):
            try:
                conn.execute(DEBIT)
            except skaidb.OperationalError:
                try:
                    conn.reconnect()
                except skaidb.ProgrammingError as e:
                    refused.append(str(e))
                    raise
                conn.execute(DEBIT)

        with self.assertRaises(skaidb.ProgrammingError):
            self.conn.transaction(body)
        self.assertEqual(len(refused), 1)
        self.assertIn("transaction()", refused[0])
        self.assertEqual(self.server.log, [(1, "BEGIN"), (1, DEBIT)])
        self.assertEqual(self.server.connections, 1, "nothing was dialed")
        # Once the helper has returned, reconnect() works as it always did.
        self.conn.reconnect()
        self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])
        self.assertEqual(self.server.log[-1], (2, "SELECT 1"))

    def test_reconnect_is_refused_on_a_healthy_connection_inside_the_transaction_too(self):
        def body(conn):
            conn.execute(DEBIT)
            conn.reconnect()

        with self.assertRaises(skaidb.ProgrammingError):
            self.conn.transaction(body)
        # The transaction is still on its socket, so it is rolled back there.
        self.assertEqual(self.server.log, [(1, "BEGIN"), (1, DEBIT), (1, "ROLLBACK")])
        self.assert_usable()

    def test_a_connection_lost_at_commit_is_not_retried(self):
        self.server.on("COMMIT", DROP)
        runs = []

        def body(conn):
            runs.append(1)
            conn.execute(DEBIT)

        with self.assertRaises(skaidb.OperationalError) as cm:
            self.conn.transaction(body)
        self.assertEqual(len(runs), 1)
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "COMMIT"])
        self.assertEqual(self.server.connections, 1)
        # The server may have applied it before its reply went missing: the
        # error says the connection was lost, and claims neither outcome.
        self.assertNotIn("nothing was committed", str(cm.exception))

    def test_a_read_timeout_in_the_body_ends_the_session_at_once(self):
        # The server is alive and still holds the session with its open
        # transaction: only the socket closing ends it there. The helper
        # cannot send a ROLLBACK (the reply to DEBIT may still arrive), and
        # must not leave the socket open until the connection is next used.
        self.server.on(DEBIT, STALL)

        def body(conn):
            conn._sock.settimeout(0.3)  # the read timeout, for this statement
            conn.execute(DEBIT)

        with self.assertRaises(skaidb.OperationalError) as cm:
            self.conn.transaction(body)
        self.assertIn("timed out", str(cm.exception))
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT])
        self.assertTrue(self.server.hung_up(1), "the socket is still open")
        self.assertEqual(self.server.connections, 1, "and nothing was re-dialed")
        # Outside the transaction the connection reconnects as it always did.
        self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])
        self.assertEqual(self.server.connections, 2)

    def test_an_interrupt_inside_a_body_statement_costs_the_connection(self):
        # The reply to DEBIT is still on the socket: a ROLLBACK sent there
        # would read it as its own answer. Closing the socket is what rolls
        # the transaction back.
        stop = KeyboardInterrupt()
        real = self.conn._file

        class Interrupted:
            def read(self, n):
                self.read = real.read
                raise stop

            close = real.close

        def body(conn):
            conn._file = Interrupted()
            conn.execute(DEBIT)

        with self.assertRaises(KeyboardInterrupt) as cm:
            self.conn.transaction(body)
        self.assertIs(cm.exception, stop)
        self.assertTrue(self.server.hung_up(1), "the socket is still open")
        self.assertNotIn("ROLLBACK", self.server.statements, "nothing more on that socket")
        self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])
        self.assertEqual(self.server.log[-1], (2, "SELECT 1"))

    def test_a_stream_the_body_leaves_open_costs_the_connection(self):
        # Neither COMMIT nor ROLLBACK can be sent while a stream owns the
        # connection, so the transaction stays open on that socket: it must
        # not be reused.
        frames = [_f_header(["id"]), _f_chunk([(1,)]), _f_chunk([(2,)]), _f_end()]
        self.server.always(SCAN, frames)
        boom = RuntimeError("boom")
        held = []

        def returns(conn):
            held.append(conn.stream(SCAN))
            return next(held[-1])

        def raises(conn):
            held.append(conn.stream(SCAN))
            raise boom

        for body in (returns, raises):
            with self.subTest(body.__name__):
                self.server.log.clear()
                with self.assertRaises((skaidb.ProgrammingError, RuntimeError)) as cm:
                    self.conn.transaction(body)
                if body is raises:
                    self.assertIs(cm.exception, boom)
                else:
                    self.assertIn("busy streaming", str(cm.exception))
                self.assertEqual(self.server.statements, ["BEGIN", SCAN])
                # Closing the socket is what ends the transaction on the
                # server: at once, not when the connection is next used.
                self.assertTrue(self.server.hung_up(self.server.connections))
                held.pop().close()
                self.assertFalse(self.conn.is_usable(), "still inside an open transaction")
                # The next statement runs on a fresh connection.
                before = self.server.connections
                self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])
                self.assertEqual(self.server.connections, before + 1)


# ---- a transaction the application writes itself ------------------------------


ROW = "INSERT INTO audit (id) VALUES (%d)"
BATCH = "INSERT INTO audit (id) VALUES (?)"


class HandWritten(_Case):
    """A `BEGIN` the application sends itself opens a transaction like the
    helper's: the driver notes it — from a `BEGIN` the server accepted
    until `COMMIT` or `ROLLBACK` is answered — and while it is open
    PROTOCOL.md §6 rule 7 holds. A lost connection is not replaced: on a
    new session the rest of the transaction, and its `COMMIT`, would each
    commit on their own."""

    def lost_after_row_1(self):
        """`BEGIN`, row 1, and the connection is lost under row 2."""
        self.conn.execute("BEGIN")
        self.conn.execute(ROW % 1)
        self.server.on(ROW % 2, DROP)
        with self.assertRaises(skaidb.OperationalError):
            self.conn.execute(ROW % 2)
        return [(1, "BEGIN"), (1, ROW % 1), (1, ROW % 2)]

    def test_without_a_loss_it_is_the_statements_as_written(self):
        self.conn.execute("BEGIN")
        self.conn.execute(ROW % 1)
        self.conn.execute(CREDIT, (10, 2))
        self.conn.execute("COMMIT")
        self.assertEqual(self.server.statements, ["BEGIN", ROW % 1, CREDIT, "COMMIT"])
        self.assert_usable()

    def test_nothing_follows_a_lost_connection_onto_a_new_session(self):
        sent = self.lost_after_row_1()
        # Row 3 fails too, and is not sent — whichever way it is written.
        writes = [
            ("a statement", lambda conn: conn.execute(ROW % 3)),
            ("a prepared statement", lambda conn: conn.execute(BATCH, (3,))),
            ("executemany", lambda conn: conn.cursor().executemany(BATCH, [(3,), (4,)])),
            ("a stream", lambda conn: conn.stream(ROW % 3)),
            ("a read", lambda conn: conn.execute(SCAN)),
        ]
        for name, write in writes:
            with self.subTest(name):
                with self.assertRaises(skaidb.OperationalError) as cm:
                    write(self.conn)
                self.assertIn("connection lost inside a transaction", str(cm.exception))
        # The COMMIT fails, and is not sent: it would reach a session that
        # has no transaction.
        with self.assertRaises(skaidb.OperationalError) as cm:
            self.conn.execute("COMMIT")
        self.assertIn("connection lost", str(cm.exception))
        self.assertIn("nothing was committed", str(cm.exception))
        self.assertEqual(self.server.log, sent)
        self.assertEqual(self.server.connections, 1, "a new session was dialed for the transaction")
        # It ended the transaction: the next statement dials again, as
        # after any lost connection.
        self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])
        self.assertEqual(self.server.log, sent + [(2, "SELECT 1")])

    def test_rollback_after_the_loss_succeeds_and_is_not_sent(self):
        for name, rollback in (
            ("execute", lambda conn: conn.execute("ROLLBACK").rowcount),
            ("a spelling", lambda conn: conn.execute("rollback transaction; -- give up").rowcount),
            ("a stream", lambda conn: list(conn.stream("ROLLBACK"))),
            ("executemany", lambda conn: conn.cursor().executemany("ROLLBACK", [()])),
        ):
            with self.subTest(name):
                self.server = Server()
                self.addCleanup(self.server.close)
                self.conn = skaidb.connect(**_credentials(self.server))
                self.addCleanup(self.conn.close)
                sent = self.lost_after_row_1()
                # The server discarded the transaction with the connection.
                self.assertIn(rollback(self.conn), (-1, [], None))
                self.assertEqual(self.server.log, sent)
                self.assertEqual(self.server.connections, 1)
                self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])
                self.assertEqual(self.server.log, sent + [(2, "SELECT 1")])

    def test_the_session_is_put_back_once_the_transaction_has_been_ended(self):
        self.conn.execute("USE app")
        self.conn.execute("SET SCAN BUDGET ROWS 1")
        self.lost_after_row_1()
        self.conn.execute("ROLLBACK")
        self.conn.execute(SCAN)
        self.assertEqual(
            [sql for n, sql in self.server.log if n == 2],
            ['USE "app"', "SET SCAN BUDGET ROWS 1", SCAN],
        )

    def test_a_connection_lost_under_the_commit_itself_is_an_unknown_outcome(self):
        self.conn.execute("BEGIN")
        self.conn.execute(ROW % 1)
        self.server.on("COMMIT", DROP)
        with self.assertRaises(skaidb.OperationalError) as cm:
            self.conn.execute("COMMIT")
        # The server may have applied it before its reply went missing.
        self.assertIn("outcome is unknown", str(cm.exception))
        self.assertNotIn("nothing was committed", str(cm.exception))
        self.assertEqual(self.server.statements, ["BEGIN", ROW % 1, "COMMIT"], "sent again")
        self.assertEqual(self.server.connections, 1)
        # The transaction is over either way.
        self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])
        self.assertEqual(self.server.log[-1], (2, "SELECT 1"))

    def test_a_connection_lost_under_the_rollback_itself_has_rolled_back(self):
        self.conn.execute("BEGIN")
        self.conn.execute(ROW % 1)
        self.server.on("ROLLBACK", DROP)
        self.assertEqual(self.conn.execute("ROLLBACK").rowcount, -1)
        self.assertEqual(self.server.statements, ["BEGIN", ROW % 1, "ROLLBACK"], "sent again")
        self.assertEqual(self.server.connections, 1)
        self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])
        self.assertEqual(self.server.log[-1], (2, "SELECT 1"))

    def test_a_connection_that_died_while_idle_is_a_lost_transaction_not_a_redial(self):
        # The look that replaces an idle connection its peer closed
        # (tests/test_idle.py) finds this one dead too — and reports it.
        self.conn.execute("BEGIN")
        self.server.on(ROW % 1, Then(_f_mutation(1), HALF_CLOSE))
        self.conn.execute(ROW % 1)
        select.select([self.conn._sock], [], [], 5)
        for statement in (ROW % 2, ROW % 3):
            with self.assertRaises(skaidb.OperationalError) as cm:
                self.conn.execute(statement)
            self.assertIn("connection lost inside a transaction", str(cm.exception))
        self.assertEqual(self.server.discarded(1), b"", "written to the dead connection")
        self.assertEqual(self.server.connections, 1)
        self.assertEqual(self.conn.execute("ROLLBACK").rowcount, -1)
        self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])
        self.assertEqual(self.server.log, [(1, "BEGIN"), (1, ROW % 1), (2, "SELECT 1")])

    def test_every_spelling_of_begin_opens_it(self):
        begins = [
            ("BEGIN", lambda conn, sql: conn.execute(sql)),
            ("begin;", lambda conn, sql: conn.execute(sql)),
            ("BEGIN TRANSACTION", lambda conn, sql: conn.execute(sql)),
            ("BEGIN ATOMIC PARTITION (tenant = 7)", lambda conn, sql: conn.execute(sql)),
            ("-- the transfer\nBEGIN -- now\n", lambda conn, sql: conn.execute(sql)),
            ("BEGIN", lambda conn, sql: conn.stream(sql).close()),
            ("BEGIN", lambda conn, sql: conn.cursor().executemany(sql, [()])),
            ("BEGIN ATOMIC PARTITION (tenant = ?)", lambda conn, sql: conn.execute(sql, (7,))),
        ]
        for begin, send in begins:
            with self.subTest(begin, how=send.__code__.co_names[-1]):
                server = Server()
                self.addCleanup(server.close)
                conn = skaidb.connect(**_credentials(server))
                self.addCleanup(conn.close)
                send(conn, begin)
                server.on(DEBIT, DROP)
                with self.assertRaises(skaidb.OperationalError):
                    conn.execute(DEBIT)
                self.assertEqual(server.connections, 1, "re-dialed inside the transaction")
                self.assertEqual(server.statements.count(DEBIT), 1)

    def test_commit_and_rollback_end_it(self):
        ends = [
            ("COMMIT", lambda conn, sql: conn.execute(sql)),
            ("commit transaction;", lambda conn, sql: conn.execute(sql)),
            ("COMMIT -- done", lambda conn, sql: conn.execute(sql)),
            ("ROLLBACK", lambda conn, sql: conn.execute(sql)),
            ("Rollback  Transaction", lambda conn, sql: conn.execute(sql)),
            ("COMMIT", lambda conn, sql: conn.stream(sql).close()),
            ("ROLLBACK", lambda conn, sql: conn.cursor().executemany(sql, [()])),
        ]
        for end, send in ends:
            with self.subTest(end, how=send.__code__.co_names[-1]):
                server = Server()
                self.addCleanup(server.close)
                conn = skaidb.connect(**_credentials(server))
                self.addCleanup(conn.close)
                conn.execute("BEGIN")
                conn.execute(ROW % 1)
                send(conn, end)
                # Outside a transaction a lost connection is replaced, and
                # the statement runs.
                server.on(DEBIT, DROP)
                self.assertEqual(conn.execute(DEBIT).rowcount, 1)
                self.assertEqual(server.log[-2:], [(1, DEBIT), (2, DEBIT)])

    def test_a_refused_commit_or_rollback_has_ended_it_too(self):
        for end, error in (
            ("COMMIT", CONFLICT),
            ("COMMIT", "constraint violated: balance >= 0"),
            ("ROLLBACK", "no transaction in progress"),
        ):
            for how, send in (
                ("execute", lambda conn, sql: conn.execute(sql)),
                ("a stream", lambda conn, sql: conn.stream(sql)),
            ):
                with self.subTest(end, error=error, how=how):
                    server = Server()
                    self.addCleanup(server.close)
                    conn = skaidb.connect(**_credentials(server))
                    self.addCleanup(conn.close)
                    conn.execute("BEGIN")
                    server.on(end, _f_error(error))
                    with self.assertRaises(skaidb.ProgrammingError):
                        send(conn, end)
                    server.on(DEBIT, DROP)
                    self.assertEqual(conn.execute(DEBIT).rowcount, 1)
                    self.assertEqual(server.connections, 2)

    def test_a_refused_begin_opens_nothing(self):
        self.server.on("BEGIN", _f_error("a transaction is already in progress"))
        with self.assertRaises(skaidb.ProgrammingError):
            self.conn.execute("BEGIN")
        self.server.on(DEBIT, DROP)
        self.assertEqual(self.conn.execute(DEBIT).rowcount, 1)
        self.assertEqual(self.server.connections, 2)

    def test_what_is_not_its_end_leaves_it_open(self):
        self.server.always("SELECT 'COMMIT'", _rows(["v"], [("COMMIT",)]))
        self.server.on("COMMIT WORK", _f_error("parse error"))
        self.server.on(DEBIT, _f_error("no such table accounts"))
        self.conn.execute("BEGIN")
        self.conn.execute("SELECT 'COMMIT'")
        self.conn.execute("ROLLBACK TO SAVEPOINT a")  # not `ROLLBACK [TRANSACTION]`
        for refused in ("COMMIT WORK", DEBIT):  # a statement error ends nothing
            with self.assertRaises(skaidb.ProgrammingError):
                self.conn.execute(refused)
        self.server.on(CREDIT, DROP)
        with self.assertRaises(skaidb.OperationalError):
            self.conn.execute(CREDIT, (10, 2))
        self.assertEqual(self.server.connections, 1, "re-dialed inside the transaction")

    def test_a_commit_that_could_not_be_sent_leaves_it_open(self):
        # A stream owns the connection: the COMMIT is refused before it is
        # written, and the transaction is still open on the server.
        self.server.always(SCAN, [_f_header(["id"]), _f_chunk([(1,)]), _f_end()])
        self.conn.execute("BEGIN")
        rows = self.conn.stream(SCAN)
        with self.assertRaises(skaidb.ProgrammingError) as cm:
            self.conn.execute("COMMIT")
        self.assertIn("busy streaming", str(cm.exception))
        self.assertEqual(list(rows), [(1,)])
        self.server.on(DEBIT, DROP)
        with self.assertRaises(skaidb.OperationalError):
            self.conn.execute(DEBIT)
        self.assertEqual(self.server.connections, 1, "re-dialed inside the transaction")

    def test_reconnect_is_refused_while_it_is_open(self):
        self.conn.execute("BEGIN")
        self.conn.execute(ROW % 1)
        with self.assertRaises(skaidb.ProgrammingError) as cm:
            self.conn.reconnect()
        self.assertIn("transaction open", str(cm.exception))
        self.assertEqual(self.server.connections, 1)
        # Still on its socket: it can be ended there.
        self.conn.execute("ROLLBACK")
        self.assertEqual(self.server.log, [(1, "BEGIN"), (1, ROW % 1), (1, "ROLLBACK")])
        self.conn.reconnect()
        self.assertEqual(self.server.connections, 2)

    def test_reconnect_after_the_loss_ends_the_lost_transaction(self):
        # What an application without the automatic reconnect does about a
        # lost connection: reconnect(), and run the transaction again from
        # BEGIN. That is the application saying the transaction is gone —
        # never the driver replacing a connection behind its back.
        conn = skaidb.connect(**dict(_credentials(self.server), auto_reconnect=False))
        self.addCleanup(conn.close)
        self.server.log.clear()

        def transfer():
            conn.execute("BEGIN")
            conn.execute(ROW % 1)
            conn.execute(ROW % 2)
            conn.execute("COMMIT")

        self.server.on(ROW % 2, DROP)
        with self.assertRaises(skaidb.OperationalError):
            transfer()
        conn.reconnect()
        self.assertFalse(conn._txn_noted)
        transfer()
        self.assertEqual(
            self.server.log,
            [(2, "BEGIN"), (2, ROW % 1), (2, ROW % 2)]
            + [(3, "BEGIN"), (3, ROW % 1), (3, ROW % 2), (3, "COMMIT")],
        )
        # With nothing open it is the next statement's connection to lose.
        self.server.on(DEBIT, DROP)
        with self.assertRaises(skaidb.OperationalError):
            conn.execute(DEBIT)
        conn.reconnect()
        self.assertEqual(conn.execute(DEBIT).rowcount, 1)

    def test_reconnect_after_the_loss_with_the_automatic_reconnect_too(self):
        self.lost_after_row_1()
        self.conn.reconnect()
        self.assertEqual(self.server.connections, 2)
        # The transaction is over: the statement commits on its own.
        self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])
        self.server.on(DEBIT, DROP)
        self.assertEqual(self.conn.execute(DEBIT).rowcount, 1, "still noted open")

    def test_without_automatic_reconnect_the_same_holds(self):
        conn = skaidb.connect(**dict(_credentials(self.server), auto_reconnect=False))
        self.addCleanup(conn.close)
        self.server.log.clear()
        conn.execute("BEGIN")
        self.server.on(ROW % 1, DROP)
        with self.assertRaises(skaidb.OperationalError):
            conn.execute(ROW % 1)
        with self.assertRaises(skaidb.OperationalError) as cm:
            conn.execute(ROW % 2)
        self.assertIn("connection lost inside a transaction", str(cm.exception))
        self.assertEqual(conn.execute("ROLLBACK").rowcount, -1)
        # The transaction is over; the connection is as after any loss.
        with self.assertRaises(skaidb.OperationalError) as cm:
            conn.execute("SELECT 1")
        self.assertIn("reconnect()", str(cm.exception))
        conn.reconnect()
        self.assertEqual(conn.execute("SELECT 1").fetchall(), [(1,)])
        self.assertEqual(self.server.log, [(2, "BEGIN"), (2, ROW % 1), (3, "SELECT 1")])

    def test_the_helper_notes_its_own_transaction_once(self):
        # Its BEGIN and COMMIT are statements like any other: noted, ended.
        self.conn.transaction(lambda conn: conn.execute(DEBIT))
        self.assertFalse(self.conn._txn_noted)
        self.server.on(DEBIT, DROP)
        self.assertEqual(self.conn.execute(DEBIT).rowcount, 1, "left noted open by the helper")
        self.assertEqual(self.server.connections, 2)
        # And a body that fails, or loses its connection, leaves none behind.
        with self.assertRaises(RuntimeError):
            self.conn.transaction(lambda conn: (_ for _ in ()).throw(RuntimeError("boom")))
        self.assertFalse(self.conn._txn_noted)
        self.server.on(DEBIT, DROP)
        with self.assertRaises(skaidb.OperationalError):
            self.conn.transaction(lambda conn: conn.execute(DEBIT))
        self.assertFalse(self.conn._txn_noted)
        self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])

    def test_the_helper_does_not_run_inside_a_transaction_that_is_lost(self):
        self.lost_after_row_1()
        ran = []
        with self.assertRaises(skaidb.OperationalError):
            self.conn.transaction(lambda conn: ran.append(1))
        self.assertEqual(ran, [])
        self.assertEqual(self.server.connections, 1)


class TransactionStatements(unittest.TestCase):
    """Which statements open and end the transaction the driver notes: one
    whose first word is `BEGIN`; exactly `COMMIT [TRANSACTION]` or
    `ROLLBACK [TRANSACTION]` — any case, an optional `;`, `--` comments
    skipped."""

    def test_the_statements_that_open_and_end_a_transaction(self):
        cases = [
            ("BEGIN", "begin"),
            ("begin;", "begin"),
            ("  Begin  ;\n", "begin"),
            ("BEGIN TRANSACTION", "begin"),
            ("BEGIN ATOMIC", "begin"),
            ("BEGIN ATOMIC PARTITION (tenant = 'a;b')", "begin"),
            ("-- the transfer\nBEGIN -- now", "begin"),
            ("COMMIT", "commit"),
            ("commit ;", "commit"),
            ("COMMIT TRANSACTION", "commit"),
            ("commit\ntransaction; -- done", "commit"),
            ("ROLLBACK", "rollback"),
            ("rollback;", "rollback"),
            ("ROLLBACK TRANSACTION;", "rollback"),
            ("BEGINNING", None),
            ("BEGIN;SELECT 1", None),
            ("COMMIT WORK", None),
            ("COMMIT TRANSACTION now", None),
            ("COMMIT; BEGIN", None),
            ("COMMITTED", None),
            ("ROLLBACK TO SAVEPOINT a", None),
            ("ROLLBACK; ROLLBACK", None),
            ("-- COMMIT", None),
            ("SELECT 'COMMIT'", None),
            ("CALL commit()", None),
            ("START TRANSACTION", None),
            ("END", None),
        ]
        for sql, want in cases:
            with self.subTest(sql):
                noted = skaidb._session_statement(sql)
                self.assertEqual(noted and noted[0], want)


# ---- the DB-API methods -----------------------------------------------------


class CommitAndRollbackMethods(_Case):
    """DB-API `commit()` and `rollback()` agree with the transaction the
    driver has noted: they end one that is open, by sending `COMMIT` /
    `ROLLBACK`, and send nothing when none is — where every statement has
    committed on its own, and frameworks call both as a matter of routine."""

    def test_commit_ends_a_transaction_that_is_open(self):
        self.conn.execute("BEGIN")
        self.conn.execute(DEBIT)
        self.assertIsNone(self.conn.commit())
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "COMMIT"])
        # Ended: a second call has nothing to commit.
        self.conn.commit()
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "COMMIT"])
        self.assert_usable()

    def test_rollback_ends_a_transaction_that_is_open(self):
        self.conn.execute("BEGIN")
        self.conn.execute(DEBIT)
        self.assertIsNone(self.conn.rollback())
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "ROLLBACK"])
        self.conn.rollback()
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "ROLLBACK"])
        self.assert_usable()

    def test_neither_sends_anything_when_no_transaction_is_open(self):
        self.conn.commit()
        self.conn.rollback()
        self.conn.execute(DEBIT)  # committed on its own
        self.conn.commit()
        self.conn.rollback()
        self.conn.execute("BEGIN")
        self.conn.execute("ROLLBACK")  # ended by the statement
        self.conn.commit()
        self.conn.rollback()
        self.assertEqual(self.server.statements, [DEBIT, "BEGIN", "ROLLBACK"])

    def test_a_begin_the_server_refused_opened_nothing_to_end(self):
        self.server.on("BEGIN", _f_error("permission denied"))
        with self.assertRaises(skaidb.ProgrammingError):
            self.conn.execute("BEGIN")
        self.conn.commit()
        self.conn.rollback()
        self.assertEqual(self.server.statements, ["BEGIN"])

    def test_commit_raises_what_the_server_answers_and_the_transaction_is_over(self):
        self.server.on("COMMIT", _f_error(CONFLICT))
        self.conn.execute("BEGIN")
        self.conn.execute(DEBIT)
        with self.assertRaises(skaidb.TransactionConflict) as cm:
            self.conn.commit()
        self.assertEqual(str(cm.exception), CONFLICT)
        # The usual clean-up after a failed commit does not replace the
        # conflict with an error of its own, and sends nothing.
        self.conn.rollback()
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "COMMIT"])
        self.assert_usable()

    def test_after_a_lost_connection_rollback_ends_the_transaction_here(self):
        self.conn.execute("BEGIN")
        self.server.on(DEBIT, DROP)
        with self.assertRaises(skaidb.OperationalError):
            self.conn.execute(DEBIT)
        self.assertIsNone(self.conn.rollback())
        self.assertEqual(self.server.log, [(1, "BEGIN"), (1, DEBIT)], "sent to a new session")
        self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])
        self.assertEqual(self.server.log[-1], (2, "SELECT 1"))

    def test_after_a_lost_connection_commit_says_nothing_was_committed(self):
        self.conn.execute("BEGIN")
        self.server.on(DEBIT, DROP)
        with self.assertRaises(skaidb.OperationalError):
            self.conn.execute(DEBIT)
        with self.assertRaises(skaidb.OperationalError) as cm:
            self.conn.commit()
        self.assertIn("nothing was committed", str(cm.exception))
        self.assertEqual(self.server.log, [(1, "BEGIN"), (1, DEBIT)], "sent to a new session")
        self.conn.commit()  # over: nothing to commit, nothing raised
        self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])

    def test_inside_transaction_the_helper_ends_its_transaction(self):
        # A body written with the DB-API habit must not end the helper's
        # transaction half way: commit() leaves it to the helper …
        def commits(conn):
            conn.execute(DEBIT)
            conn.commit()
            conn.execute(CREDIT, (10, 2))

        self.conn.transaction(commits)
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, CREDIT, "COMMIT"])

        # … and rollback() says how a body rolls back: by raising.
        def rolls_back(conn):
            conn.execute(DEBIT)
            conn.rollback()
            conn.execute(CREDIT, (10, 2))  # would commit on its own

        self.server.log.clear()
        with self.assertRaises(skaidb.OperationalError) as cm:
            self.conn.transaction(rolls_back)
        self.assertIn("raise", str(cm.exception))
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "ROLLBACK"])
        self.assert_usable()

    def test_their_documentation_says_what_they_send(self):
        for method, statement in ((self.conn.commit, "COMMIT"), (self.conn.rollback, "ROLLBACK")):
            with self.subTest(method.__name__):
                doc = " ".join((method.__doc__ or "").split())
                self.assertIn(f"``{statement}``", doc)
                self.assertIn("sends nothing", doc)
                self.assertIn(":meth:`transaction`", doc)


# ---- the pool ---------------------------------------------------------------


class Pool(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.addCleanup(self.server.close)
        self.pool = skaidb.pool(**_credentials(self.server))
        self.addCleanup(self.pool.close)

    def member(self):
        return self.pool._members[("127.0.0.1", self.server.port)]

    def counts(self):
        m = self.pool.members()[0]
        return (m["in_use"], m["idle"])

    def test_the_transaction_runs_on_one_checked_out_connection(self):
        def body(conn):
            self.assertEqual(self.counts(), (1, 0), "checked out for the duration")
            self.assertEqual(conn._pool_affinity, "ada")
            conn.execute(DEBIT)
            conn.execute(CREDIT, (10, 2))
            return "moved"

        self.assertEqual(self.pool.transaction(body, affinity="ada"), "moved")
        self.assertEqual(
            self.server.log, [(1, "BEGIN"), (1, DEBIT), (1, CREDIT), (1, "COMMIT")]
        )
        self.assertEqual(self.counts(), (0, 1), "returned to the pool")

    def test_a_conflict_retries_on_the_same_connection_and_is_not_a_member_failure(self):
        self.server.on("COMMIT", _f_error(CONFLICT), _f_error(CONFLICT))
        runs = []

        def body(conn):
            runs.append(1)
            conn.execute(DEBIT)
            return len(runs)

        self.assertEqual(self.pool.transaction(body, begin="BEGIN ATOMIC"), 3)
        self.assertEqual(self.server.statements, ["BEGIN ATOMIC", DEBIT, "COMMIT"] * 3)
        self.assertEqual({number for number, _ in self.server.log}, {1})
        self.assertEqual(self.member().errors, 0)
        self.assertEqual(self.pool.members()[0]["state"], "ready")
        self.assertEqual(self.counts(), (0, 1), "kept, not discarded")

    def test_a_conflict_on_the_last_attempt_still_returns_the_connection(self):
        self.server.always("COMMIT", _f_error(CONFLICT))
        with self.assertRaises(skaidb.TransactionConflict):
            self.pool.transaction(lambda conn: conn.execute(DEBIT), attempts=2)
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "COMMIT"] * 2)
        self.assertEqual(self.member().errors, 0)
        self.assertEqual(self.counts(), (0, 1))
        with self.pool.connection() as conn:  # the same connection, still good
            self.assertEqual(conn.execute("SELECT 1").fetchall(), [(1,)])
        self.assertEqual(self.server.connections, 1)

    def test_a_connection_lost_mid_transaction_is_discarded_not_retried(self):
        self.server.on(DEBIT, DROP)
        with self.assertRaises(skaidb.OperationalError):
            self.pool.transaction(lambda conn: conn.execute(DEBIT))
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT])
        self.assertEqual(self.server.connections, 1)
        self.assertEqual(self.counts(), (0, 0), "the broken connection was closed")

    def test_a_begin_on_a_pooled_connection_lost_while_idle_redials_like_any_statement(self):
        with self.pool.connection() as conn:
            conn.execute("SELECT 1")
        self.server.restart()  # the idle connection is gone; the pool cannot tell yet
        self.assertEqual(self.pool.transaction(lambda conn: conn.execute(DEBIT).rowcount), 1)
        # The BEGIN that found the connection dead was sent again on a new
        # one, and the whole transaction ran there.
        self.assertEqual(self.server.log[1:], [(2, "BEGIN"), (2, DEBIT), (2, "COMMIT")])
        self.assertEqual(self.counts(), (0, 1), "the re-dialed connection went back to the pool")

    def test_attempts_below_one_is_refused_before_a_connection_is_taken(self):
        with self.assertRaises(ValueError):
            self.pool.transaction(lambda conn: None, attempts=0)
        self.assertEqual(self.server.connections, 0)


if __name__ == "__main__":
    unittest.main()
