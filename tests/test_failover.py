"""Automatic reconnect and endpoint failover.

A member going down during a rolling restart used to cost a whole batch job:
a pooled connection failed with a broken pipe, the fresh dial landed on the
same dying member, and the driver never tried the next seed. Now a
statement whose connection fails in transit re-dials — the other endpoints
first — and runs once more.
"""

import unittest
from unittest import mock

import skaidb
try:
    from test_conformance import V, FakeServer
    from test_stream import _f_error
    from test_transactions import DROP, HELLO, STALL, Late, Server, _credentials
except ImportError:  # run as `python -m unittest tests.test_failover`
    from tests.test_conformance import V, FakeServer
    from tests.test_stream import _f_error
    from tests.test_transactions import DROP, HELLO, STALL, Late, Server, _credentials

ROWS_CASE = next(c for c in V["cases"] if c["name"] == "query_rows_several")
EXCHANGE = (
    bytes.fromhex(ROWS_CASE["exchanges"][0]["request"]),
    [bytes.fromhex(r) for r in ROWS_CASE["exchanges"][0]["responses"]],
)
SQL = ROWS_CASE["call"]["sql"]


def _connect(servers, **kw):
    # Keep the seed order deterministic: the dying member first.
    kw.setdefault("timeout", 5)
    with mock.patch.object(skaidb.random, "shuffle", lambda eps: None):
        return skaidb.connect(
            seeds=[f"127.0.0.1:{s.port}" for s in servers],
            user=V["auth"]["username"],
            password=V["auth"]["password"],
            **kw,
        )


class Failover(unittest.TestCase):
    def test_a_statement_fails_over_to_another_member_and_retries(self):
        dying = FakeServer(die_on_request=True)
        healthy = FakeServer(exchanges=[EXCHANGE])
        conn = _connect([dying, healthy])
        cur = conn.cursor()
        cur.execute(SQL)
        self.assertEqual(len(cur.fetchall()), 3, "answered by the healthy member")
        conn.close()
        dying.join()
        healthy.join()
        self.assertEqual(len(dying.requests), 1, "the dying member saw the statement once")
        self.assertIsNone(healthy.error)

    def test_executemany_fails_over_with_a_fresh_prepare(self):
        case = next(c for c in V["cases"] if c["name"] == "execute_batch")
        exchanges = [
            (bytes.fromhex(e["request"]), [bytes.fromhex(r) for r in e["responses"]])
            for e in case["exchanges"]
        ]
        dying = FakeServer(die_on_request=True)
        healthy = FakeServer(exchanges=exchanges)
        conn = _connect([dying, healthy])
        cur = conn.cursor()
        rows = [[1, "x"], [2, "y"], [3, None]]
        cur.executemany(case["call"]["sql"], rows)
        self.assertEqual(cur.rowcount, 3)
        conn.close()
        dying.join()
        healthy.join()
        self.assertIsNone(healthy.error, "prepare re-ran on the new connection")

    def test_a_broken_connection_redials_before_the_next_statement(self):
        first = FakeServer(die_on_request=True)
        second = FakeServer(exchanges=[EXCHANGE])
        conn = _connect([first, second], auto_reconnect=False)
        with self.assertRaises(skaidb.OperationalError):
            conn.cursor().execute(SQL)
        self.assertFalse(conn.is_usable())
        # Opting back in: the next statement re-dials first (nothing was
        # sent yet, so this is always safe) — to the OTHER member.
        conn._auto_reconnect = True
        cur = conn.cursor()
        cur.execute(SQL)
        self.assertEqual(len(cur.fetchall()), 3)
        conn.close()
        first.join()
        second.join()
        self.assertIsNone(second.error)

    def test_auto_reconnect_off_surfaces_the_failure(self):
        dying = FakeServer(die_on_request=True)
        conn = _connect([dying], auto_reconnect=False)
        with self.assertRaises(skaidb.OperationalError):
            conn.cursor().execute(SQL)
        conn.close()
        dying.join()


class PastThePreparedCache(unittest.TestCase):
    """A statement prepared beyond the cache is closed again after its one
    use. When the connection fails under it there is nothing left to close:
    the prepared id died with the socket, and the statement is retried on a
    fresh connection like any other."""

    UPDATE = "UPDATE accounts SET balance = ? WHERE id = ?"
    INSERT = "INSERT INTO audit (id) VALUES (?)"

    def setUp(self):
        self.server = Server()
        self.addCleanup(self.server.close)
        self.conn = skaidb.connect(**_credentials(self.server))
        self.addCleanup(self.conn.close)
        full = mock.patch.object(skaidb, "_MAX_PREPARED_CACHE", 0)
        full.start()
        self.addCleanup(full.stop)

    def test_a_statement_past_the_cache_fails_over_like_any_other(self):
        self.server.on(self.UPDATE, DROP)
        self.assertEqual(self.conn.execute(self.UPDATE, (10, 2)).rowcount, 1)
        self.assertEqual(self.server.log, [(1, self.UPDATE), (2, self.UPDATE)])
        self.assertEqual(self.server.connections, 2)

    def test_executemany_past_the_cache_fails_over_too(self):
        self.server.on(self.INSERT, DROP)
        cur = self.conn.cursor()
        cur.executemany(self.INSERT, [(1,), (2,)])
        self.assertEqual(cur.rowcount, 1)
        self.assertEqual(self.server.log, [(1, self.INSERT), (2, self.INSERT)])

    def test_without_automatic_reconnect_nothing_is_dialed_to_close_it(self):
        conn = skaidb.connect(**dict(_credentials(self.server), auto_reconnect=False))
        self.addCleanup(conn.close)
        self.server.on(self.UPDATE, DROP)
        with self.assertRaises(skaidb.OperationalError):
            conn.execute(self.UPDATE, (10, 2))
        self.assertFalse(conn.is_usable())
        self.assertEqual(self.server.connections, 2, "setUp's connection and this one")

    def test_an_interrupted_statement_is_not_followed_by_a_dial(self):
        # The interrupt must come back at once: no connect and no round trip
        # on the way out, to close an id the new session never issued.
        self.conn.execute("SELECT 1")
        stop = KeyboardInterrupt()
        real = skaidb.Connection._read_frame
        reads = []

        def read_frame(conn):
            reads.append(1)
            if len(reads) == 2:  # the reply to the execute, after the prepare's
                raise stop
            return real(conn)

        with mock.patch.object(skaidb.Connection, "_read_frame", read_frame):
            with self.assertRaises(KeyboardInterrupt) as cm:
                self.conn.execute(self.UPDATE, (10, 2))
        self.assertIs(cm.exception, stop)
        self.assertEqual(len(reads), 2, "nothing was read after the interrupt")
        self.assertEqual(self.server.connections, 1, "and nothing was dialed")
        self.assertFalse(self.conn.is_usable())
        self.assertEqual(self.conn.execute("SELECT 1").fetchall(), [(1,)])


class UnfinishedHello(unittest.TestCase):
    """The Hello is a round trip like any other: when its reply does not
    come, the socket is dead or has an answer still on its way — a
    connection that looks usable and is not. The dial fails there and moves
    to the next endpoint. (The Hello is telemetry only in what it carries:
    an old server's Error to it is still ignored.)"""

    UPDATE = "UPDATE accounts SET balance = 0 WHERE id = 1"

    def member(self, hello=None):
        server = Server()
        self.addCleanup(server.close)
        if hello is not None:
            server.on(HELLO, hello)
        return server

    def connect(self, members, **kw):
        kw.setdefault("timeout", 0.5)
        conn = _connect(members, **kw)
        self.addCleanup(conn.close)
        return conn

    # The reply never comes within the read timeout — or comes late, to be
    # read as the answer to the first statement — or the member hangs up.
    FAILURES = [
        ("no reply", lambda: STALL),
        ("a late reply", lambda: Late(bytes([skaidb._RESP_DDL]), 1.0)),
        ("the connection closed", lambda: DROP),
    ]

    def test_the_dial_moves_to_the_next_endpoint(self):
        for name, hello in self.FAILURES:
            for auto in (True, False):
                with self.subTest(name, auto_reconnect=auto):
                    failing, healthy = self.member(hello()), self.member()
                    conn = self.connect([failing, healthy], auto_reconnect=auto)
                    self.assertEqual(conn._connected, ("127.0.0.1", healthy.port))
                    self.assertTrue(conn.is_usable())
                    self.assertEqual(conn.execute(self.UPDATE).rowcount, 1)
                    self.assertEqual(healthy.statements, [self.UPDATE])
                    # The member that did not answer was hung up on, and
                    # never received the statement.
                    if name != "the connection closed":
                        self.assertTrue(failing.hung_up(1, within=5), "its socket was left open")
                    self.assertEqual(failing.statements, [], "the statement went to it as well")
                    self.assertEqual(healthy.connections, 1)

    def test_with_nowhere_else_to_go_the_connect_fails(self):
        for name, hello in self.FAILURES:
            with self.subTest(name):
                failing = self.member(hello())
                with self.assertRaises(skaidb.OperationalError) as cm:
                    self.connect([failing])
                self.assertIn("could not connect to any endpoint", str(cm.exception))
                self.assertIn(f"127.0.0.1:{failing.port}: no reply to Hello", str(cm.exception))
                if name != "the connection closed":
                    self.assertTrue(failing.hung_up(1, within=5), "its socket was left open")

    def test_a_reconnect_does_the_same(self):
        first, second = self.member(), self.member()
        conn = self.connect([first, second])
        self.assertEqual(conn._connected, ("127.0.0.1", first.port))
        second.on(HELLO, STALL)  # the member reconnect() tries first
        conn.reconnect()
        self.assertEqual(conn._connected, ("127.0.0.1", first.port))
        self.assertEqual(conn.execute(self.UPDATE).rowcount, 1)
        self.assertEqual(first.log, [(2, self.UPDATE)])
        self.assertEqual(second.statements, [])

    def test_a_pool_counts_it_against_the_member_once(self):
        failing, healthy = self.member(STALL), self.member()
        with mock.patch.object(skaidb.random, "shuffle", lambda eps: None):
            pool = skaidb.pool(
                seeds=[f"127.0.0.1:{failing.port}", f"127.0.0.1:{healthy.port}"],
                user=V["auth"]["username"], password=V["auth"]["password"], timeout=0.5,
            )
            self.addCleanup(pool.close)
            with pool.connection() as conn:
                self.assertEqual(conn.execute(self.UPDATE).rowcount, 1)
        self.assertEqual(healthy.statements, [self.UPDATE])
        self.assertEqual(pool._members[("127.0.0.1", failing.port)].errors, 1)

    def test_an_old_servers_error_to_hello_is_still_ignored(self):
        old = self.member(_f_error("protocol error: unknown opcode 8"))
        conn = self.connect([old])
        self.assertEqual(conn.execute(self.UPDATE).rowcount, 1)
        self.assertEqual(old.log, [(1, self.UPDATE)])


class SessionLostAtTheDial(unittest.TestCase):
    """The same for the statements that put the session back on a new
    connection: a member that fails under them is one more endpoint that
    did not work, not the end of the dial. An answer is another matter — a
    database the server says does not exist is an error everywhere."""

    COUNT = "SELECT count(*) FROM notes"

    def members(self):
        servers = [Server(), Server()]
        for server in servers:
            self.addCleanup(server.close)
        return servers

    def test_a_member_lost_under_the_use_is_not_the_end_of_the_dial(self):
        failing, healthy = self.members()
        failing.on('USE "app"', DROP)
        conn = _connect([failing, healthy], database="app")
        self.addCleanup(conn.close)
        self.assertEqual(conn._connected, ("127.0.0.1", healthy.port))
        conn.execute(self.COUNT)
        self.assertEqual(healthy.ran_in[-1], ("app", self.COUNT))

    def test_a_member_lost_under_a_setting_is_not_either(self):
        first, second = self.members()
        conn = _connect([first, second])
        self.addCleanup(conn.close)
        conn.execute("SET SCAN BUDGET ROWS 1")
        second.on("SET SCAN BUDGET ROWS 1", DROP)  # the member the re-dial tries first
        first.restart()
        conn.execute(self.COUNT)
        self.assertEqual(second.statements, ["SET SCAN BUDGET ROWS 1"])
        self.assertEqual(first.log[-2:], [(2, "SET SCAN BUDGET ROWS 1"), (2, self.COUNT)])
        self.assertEqual(first.ran_under[-1], (None, "ROWS 1", self.COUNT))

    def test_a_database_the_server_refuses_ends_it(self):
        refusing, healthy = self.members()
        refusing.on('USE "app"', _f_error('database "app" does not exist'))
        with self.assertRaises(skaidb.ProgrammingError) as cm:
            _connect([refusing, healthy], database="app")
        self.assertIn("does not exist", str(cm.exception))
        self.assertEqual(healthy.connections, 0)


if __name__ == "__main__":
    unittest.main()
