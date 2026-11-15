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
import socket
import struct
import threading
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
    on, and `connections` counts the connections accepted.

    `on(sql, *replies)` scripts the answers to the next executions of `sql`
    and `always(sql, reply)` the answer to every later one; a reply is a
    payload, a list of payloads (the frames of a stream) or `DROP`.
    Unscripted, `BEGIN …` / `COMMIT` / `ROLLBACK` are acknowledged, a
    `SELECT` answers one row and anything else is a one-row mutation."""

    def __init__(self):
        self.log = []
        self.connections = 0
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
            threading.Thread(target=self._serve_safe, args=(conn, number), daemon=True).start()

    def _serve_safe(self, conn, number):
        try:
            with conn:
                if self._handshake(conn):
                    self._serve(conn, number)
        except OSError:
            pass

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
        prepared = []  # this connection's prepared statements; the id is the index
        while True:
            req = _read_frame(conn)
            if req is None:
                return
            op = req[0]
            if op in (skaidb._OP_CLOSE, skaidb._OP_HELLO):
                _send(conn, _DDL)
                continue
            if op == skaidb._OP_PREPARE:
                sql = self._sql(req, 1)
                reply = self._scripted(("prepare", sql))
                if reply is None:
                    prepared.append(sql)
                    reply = bytes([skaidb._RESP_PREPARED]) + struct.pack(
                        "<IH", len(prepared) - 1, sql.count("?")
                    )
            else:
                if op in (skaidb._OP_EXECUTE, skaidb._OP_EXECUTE_BATCH):
                    sql = prepared[struct.unpack_from("<I", req, 2)[0]]
                else:  # one-shot query or stream request: the text itself
                    sql = self._sql(req, 2)
                with self._lock:
                    self.log.append((number, sql))
                reply = self._scripted(sql)
                if reply is None:
                    reply = self._default(sql)
            if reply is DROP:
                return
            for payload in reply if isinstance(reply, list) else [reply]:
                _send(conn, payload)

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
    def _default(sql):
        if sql.split()[0] in ("BEGIN", "COMMIT", "ROLLBACK"):
            return _DDL
        if sql.startswith("SELECT"):
            return _rows(["n"], [(1,)])
        return _f_mutation(1)


def _credentials(server):
    return dict(
        host="127.0.0.1",
        port=server.port,
        user=V["auth"]["username"],
        password=V["auth"]["password"],
        timeout=5,
    )


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

    def test_a_connection_lost_at_commit_is_not_retried(self):
        self.server.on("COMMIT", DROP)
        runs = []

        def body(conn):
            runs.append(1)
            conn.execute(DEBIT)

        with self.assertRaises(skaidb.OperationalError):
            self.conn.transaction(body)
        self.assertEqual(len(runs), 1)
        self.assertEqual(self.server.statements, ["BEGIN", DEBIT, "COMMIT"])
        self.assertEqual(self.server.connections, 1)

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
                held.pop().close()
                self.assertFalse(self.conn.is_usable(), "still inside an open transaction")
                # The next statement runs on a fresh connection.
                before = self.server.connections
                self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])
                self.assertEqual(self.server.connections, before + 1)


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

    def test_attempts_below_one_is_refused_before_a_connection_is_taken(self):
        with self.assertRaises(ValueError):
            self.pool.transaction(lambda conn: None, attempts=0)
        self.assertEqual(self.server.connections, 0)


if __name__ == "__main__":
    unittest.main()
