"""A connection closed while it was idle costs no statement.

A member's restart, a failover or an idle timeout closes idle connections.
On loopback the driver notices at its next write, which the kernel answers
with a reset at once. Over a real network that write succeeds — the reset
has not come back yet — and only the read fails, after the statement left.
So before a statement's first request the driver looks at the socket,
without waiting and without a round trip: on an idle connection the server
sends nothing, and a socket that is readable holds its close, a reset or
stray bytes. Such a connection is replaced before anything is written
(PROTOCOL.md §6 rule 4: it is dead), and a pool does not hand it out.

Against the fake server of `test_transactions`, which can half-close a
connection: it stops sending and keeps reading, so the client reads EOF and
its writes succeed — the real network's case, made deterministic.
"""

import select
import time
import unittest

import skaidb

try:
    from test_stream import _f_chunk, _f_end, _f_header
    from test_transactions import (
        CLOSE_NOTIFY, HALF_CLOSE, RESET, Server, Then, _credentials, _rows,
    )
except ImportError:  # run as `python -m unittest tests.test_idle`
    from tests.test_stream import _f_chunk, _f_end, _f_header
    from tests.test_transactions import (
        CLOSE_NOTIFY, HALF_CLOSE, RESET, Server, Then, _credentials, _rows,
    )

LAST = "SELECT 'the last statement before the connection sat idle'"
COUNT = "SELECT count(*) FROM notes"
INSERT = "INSERT INTO notes (id, body) VALUES (?, ?)"
UPDATE = "UPDATE notes SET body = 'x' WHERE id = 1"
SCAN = "SELECT id FROM notes"

# A reply nobody asked for: what a statement on the connection would read
# as its own answer.
_STRAY = _rows(["n"], [("stray",)])


def _wait_until_readable(conn, within=5.0) -> None:
    """Let what the server did to the connection arrive: its close, its
    reset or its stray bytes make the client's socket readable."""
    ready = select.select([conn._sock], [], [], within)[0]
    assert ready, "nothing arrived on the idle connection"


class _Case(unittest.TestCase):
    tls = False

    def setUp(self):
        self.server = Server(tls=self.tls)
        self.addCleanup(self.server.close)

    def connect(self, **kw):
        conn = skaidb.connect(**dict(_credentials(self.server), **kw))
        self.addCleanup(conn.close)
        return conn

    def idle_then(self, conn, what) -> None:
        """Run a statement, after whose reply the server does `what` to the
        connection; return once that has reached the client."""
        self.server.on(LAST, Then(_rows(["n"], [(1,)]), what))
        conn.execute(LAST)
        _wait_until_readable(conn)

    def on_connection(self, number) -> list:
        return [sql for n, sql in self.server.log if n == number and sql != LAST]


# How a statement can be sent, and what the server then holds.
STATEMENTS = [
    ("a read", COUNT, lambda conn: conn.execute(COUNT).fetchall()),
    ("a write", UPDATE, lambda conn: conn.execute(UPDATE).rowcount),
    ("a write with parameters", INSERT, lambda conn: conn.execute(INSERT, (1, "x")).rowcount),
    ("executemany", INSERT, lambda conn: conn.cursor().executemany(INSERT, [(1, "x"), (2, "y")])),
    ("a stream", SCAN, lambda conn: list(conn.stream(SCAN))),
    ("a stream with parameters", SCAN + " WHERE id > ?", lambda conn: list(conn.stream(SCAN + " WHERE id > ?", (0,)))),
]


class ClosedWhileIdle(_Case):
    def setUp(self):
        super().setUp()
        frames = [_f_header(["id"]), _f_chunk([(1,)]), _f_end()]
        self.server.always(SCAN, frames)
        self.server.always(SCAN + " WHERE id > ?", frames)

    def test_a_half_closed_connection_carries_no_statement(self):
        # The server's close has arrived; a write to the socket would still
        # succeed. Nothing is written to it: the statement runs once, on a
        # second connection, and no error reaches the application.
        for name, sql, run in STATEMENTS:
            with self.subTest(name):
                self.server.log.clear()
                conn = self.connect()
                first = self.server.connections
                self.idle_then(conn, HALF_CLOSE)
                run(conn)
                self.assertEqual(self.server.connections, first + 1)
                self.assertEqual(self.on_connection(first + 1), [sql])
                self.assertEqual(
                    self.server.discarded(first), b"", "written to the connection the server had closed"
                )
                self.assertTrue(self.server.hung_up(first), "the dead socket was left open")
                self.assertTrue(conn.is_usable())

    def test_a_reset_connection_carries_none_either(self):
        for name, sql, run in STATEMENTS:
            with self.subTest(name):
                self.server.log.clear()
                conn = self.connect()
                first = self.server.connections
                self.idle_then(conn, RESET)
                run(conn)
                self.assertEqual(self.server.connections, first + 1)
                self.assertEqual(self.server.log.count((first + 1, sql)), 1)

    def test_a_restarted_server(self):
        conn = self.connect()
        conn.execute(COUNT)
        self.server.restart()
        _wait_until_readable(conn)
        self.assertEqual(conn.execute(UPDATE).rowcount, 1)
        self.assertEqual(self.server.log, [(1, COUNT), (2, UPDATE)])

    def test_stray_bytes_are_never_read_as_a_reply(self):
        # A frame nobody asked for sits on the idle connection: the next
        # statement would take it for its own answer.
        conn = self.connect()
        self.idle_then(conn, _STRAY)
        self.assertEqual(conn.execute(COUNT).fetchall(), [(1,)])
        self.assertEqual(self.server.log[-1], (2, COUNT))
        self.assertTrue(self.server.hung_up(1))

    def test_the_session_is_put_back_on_the_new_connection(self):
        # Exactly as after a failed write: the database, the settings and
        # the prepared statements (PROTOCOL.md §6 rules 9 and 10).
        conn = self.connect(database="app")
        conn.execute("SET CONSISTENCY ALL")
        conn.execute("SET SCAN BUDGET ROWS 1")
        conn.execute(INSERT, (1, "x"))
        self.idle_then(conn, HALF_CLOSE)
        conn.execute(INSERT, (2, "y"))
        self.assertEqual(
            self.on_connection(2),
            ['USE "app"', "SET CONSISTENCY ALL", "SET SCAN BUDGET ROWS 1", INSERT],
        )
        self.assertEqual(self.server.ran_in[-1], ("app", INSERT))
        self.assertEqual(self.server.ran_under[-1], ("ALL", "ROWS 1", INSERT))
        self.assertEqual(self.server.unknown_ids, [], "sent by an id of the connection that was gone")
        self.assertEqual(self.server.bound[-1], (INSERT, [2, "y"]))
        self.assertEqual(self.server.discarded(1), b"")

    def test_without_automatic_reconnect_the_statement_fails_before_it_is_sent(self):
        conn = self.connect(auto_reconnect=False)
        self.idle_then(conn, HALF_CLOSE)
        with self.assertRaises(skaidb.OperationalError) as cm:
            conn.execute(UPDATE)
        self.assertIn("reconnect()", str(cm.exception))
        # Not sent, so not applied: the caller may run it again.
        self.assertEqual(self.server.discarded(1), b"")
        self.assertEqual(self.server.connections, 1)
        self.assertFalse(conn.is_usable())
        conn.reconnect()
        self.assertEqual(conn.execute(UPDATE).rowcount, 1)
        self.assertEqual(self.on_connection(2), [UPDATE])

    def test_is_usable_sees_it_without_a_round_trip(self):
        conn = self.connect()
        self.assertTrue(conn.is_usable())
        self.idle_then(conn, HALF_CLOSE)
        self.assertFalse(conn.is_usable())
        self.assertTrue(self.server.hung_up(1), "found dead, and left open")
        self.assertEqual(self.server.discarded(1), b"", "it asked the server")
        self.assertFalse(conn.ping())

    def test_a_live_connection_is_left_alone(self):
        conn = self.connect()
        for _ in range(3):
            self.assertTrue(conn.is_usable())
            self.assertEqual(conn.execute(COUNT).fetchall(), [(1,)])
            self.assertEqual(conn.execute(INSERT, (1, "x")).rowcount, 1)
        self.assertEqual(self.server.connections, 1)

    def test_a_connection_in_the_middle_of_a_stream_is_not_looked_at(self):
        # It is not idle: what is readable there is the stream's next frame.
        conn = self.connect()
        rows = conn.stream(SCAN)
        _wait_until_readable(conn)
        self.assertFalse(conn.is_usable(), "mid-stream")
        with self.assertRaises(skaidb.ProgrammingError) as cm:
            conn.execute(COUNT)
        self.assertIn("busy streaming", str(cm.exception))
        self.assertEqual(list(rows), [(1,)])
        self.assertTrue(conn.is_usable())
        self.assertEqual(conn.execute(COUNT).fetchall(), [(1,)])
        self.assertEqual(self.server.connections, 1, "the stream's frames were taken for a close")

    def test_inside_transaction_the_loss_is_reported_and_nothing_is_redialed(self):
        # PROTOCOL.md §6 rule 7 as before: the transaction was on the
        # connection that is gone. New: its next statement is not written
        # to the dead socket either.
        conn = self.connect()
        raised = []

        def body(conn):
            self.idle_then(conn, HALF_CLOSE)
            for _ in range(2):
                try:
                    conn.execute(UPDATE)
                except skaidb.OperationalError as e:
                    raised.append(e)

        with self.assertRaises(skaidb.OperationalError):
            conn.transaction(body)
        self.assertEqual(len(raised), 2)
        self.assertEqual(self.server.log, [(1, "BEGIN"), (1, LAST)])
        self.assertEqual(self.server.discarded(1), b"")
        self.assertEqual(self.server.connections, 1)
        # Afterwards the connection reconnects as it always did.
        self.assertEqual(conn.execute(COUNT).fetchall(), [(1,)])
        self.assertEqual(self.server.log[-1], (2, COUNT))

    def test_the_look_does_not_wait(self):
        conn = self.connect()
        started = time.monotonic()
        for _ in range(200):
            self.assertTrue(conn.is_usable())
        self.assertLess(time.monotonic() - started, 1.0)


class ClosedWhileIdleUnderTls(_Case):
    """The same under TLS: the socket looked at is the transport's, and what
    is readable there may be TLS's own."""

    tls = True

    def test_a_connection_closed_at_the_transport_carries_no_statement(self):
        # A FIN with no TLS alert: nothing of it shows in the decrypted
        # stream until a read.
        conn = self.connect()
        self.idle_then(conn, HALF_CLOSE)
        self.assertEqual(conn.execute(UPDATE).rowcount, 1)
        self.assertEqual(self.on_connection(2), [UPDATE])
        self.assertEqual(self.server.discarded(1), b"")
        self.assertEqual(self.server.connections, 2)

    def test_a_connection_closed_with_the_tls_alert_carries_none_either(self):
        conn = self.connect()
        self.idle_then(conn, CLOSE_NOTIFY)
        self.assertEqual(conn.execute(INSERT, (1, "x")).rowcount, 1)
        self.assertEqual(self.on_connection(2), [INSERT])
        self.assertEqual(self.server.discarded(1), b"")

    def test_a_reset_connection(self):
        conn = self.connect()
        self.idle_then(conn, RESET)
        self.assertEqual(conn.execute(UPDATE).rowcount, 1)
        self.assertEqual(self.on_connection(2), [UPDATE])

    def test_stray_data_is_never_read_as_a_reply(self):
        conn = self.connect()
        self.idle_then(conn, _STRAY)
        self.assertEqual(conn.execute(COUNT).fetchall(), [(1,)])
        self.assertEqual(self.server.log[-1], (2, COUNT))

    def test_a_live_connection_is_left_alone(self):
        conn = self.connect()
        for _ in range(3):
            self.assertTrue(conn.is_usable())
            self.assertEqual(conn.execute(COUNT).fetchall(), [(1,)])
        self.assertEqual(self.server.connections, 1)

    def test_records_tls_keeps_to_itself_are_not_a_close(self):
        # After its handshake a TLS 1.3 server sends session tickets: bytes
        # on the transport that are nothing to the application. A socket
        # that holds only those is alive.
        import socket
        import ssl

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        raw = socket.create_connection(("127.0.0.1", self.server.port), timeout=5)
        sock = ctx.wrap_socket(raw, server_hostname="skaidb")
        self.addCleanup(sock.close)
        if not select.select([sock], [], [], 1.0)[0]:
            self.skipTest("this TLS version sends nothing after its handshake")
        conn = skaidb.Connection.__new__(skaidb.Connection)
        conn._sock = sock
        # Each look consumes the tickets that have arrived, until none is
        # left to read: the socket is quiet, and was alive all along.
        looks = 0
        while select.select([sock], [], [], 0.3)[0]:
            self.assertFalse(conn._gone(), "TLS's own records were taken for a close")
            looks += 1
            self.assertLess(looks, 10, "the look does not consume what it reads")
        self.assertGreater(looks, 0)
        self.assertEqual(sock.gettimeout(), 5, "the look changed the read timeout")
        self.assertFalse(conn._gone())


class Pooled(_Case):
    """A pooled connection the peer closed while it sat idle is not handed
    out as live: the checkout gives a working one."""

    def pool(self, **kw):
        pool = skaidb.pool(**dict(_credentials(self.server), **kw))
        self.addCleanup(pool.close)
        return pool

    def idle_in_the_pool_then(self, pool, what):
        with pool.connection() as conn:
            self.idle_then(conn, what)
        return conn

    def test_the_checkout_gives_a_working_connection(self):
        for name, what in (("half-closed", HALF_CLOSE), ("reset", RESET), ("stray bytes", _STRAY)):
            # Also with no automatic reconnect to hide a dead one behind.
            for auto in (True, False):
                with self.subTest(name, auto_reconnect=auto):
                    self.server = Server()
                    self.addCleanup(self.server.close)
                    pool = self.pool(auto_reconnect=auto)
                    dead = self.idle_in_the_pool_then(pool, what)
                    with pool.connection() as conn:
                        self.assertIsNot(conn, dead, "the dead connection was handed out")
                        self.assertEqual(conn.execute(UPDATE).rowcount, 1)
                    self.assertEqual(self.on_connection(2), [UPDATE])
                    self.assertEqual(self.server.discarded(1), b"")
                    self.assertTrue(dead.closed)
                    self.assertEqual(pool.members()[0]["idle"], 1)

    def test_a_connection_that_died_during_its_checkout_is_not_pooled(self):
        pool = self.pool()
        with pool.connection() as conn:
            self.idle_then(conn, HALF_CLOSE)
        self.assertEqual(pool.members()[0]["idle"], 0)
        self.assertTrue(conn.closed)

    def test_a_live_idle_connection_is_handed_out_again(self):
        pool = self.pool()
        with pool.connection() as first:
            first.execute(COUNT)
        with pool.connection() as second:
            self.assertIs(second, first)
            second.execute(COUNT)
        self.assertEqual(self.server.connections, 1)

    def test_a_transaction_on_a_connection_that_died_idle_runs(self):
        pool = self.pool()
        self.idle_in_the_pool_then(pool, HALF_CLOSE)
        self.assertEqual(pool.transaction(lambda conn: conn.execute(UPDATE).rowcount), 1)
        self.assertEqual(self.on_connection(2), ["BEGIN", UPDATE, "COMMIT"])
        self.assertEqual(self.server.discarded(1), b"")


if __name__ == "__main__":
    unittest.main()
