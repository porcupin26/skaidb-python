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
    from test_transactions import DROP, Server, _credentials
except ImportError:  # run as `python -m unittest tests.test_failover`
    from tests.test_conformance import V, FakeServer
    from tests.test_transactions import DROP, Server, _credentials

ROWS_CASE = next(c for c in V["cases"] if c["name"] == "query_rows_several")
EXCHANGE = (
    bytes.fromhex(ROWS_CASE["exchanges"][0]["request"]),
    [bytes.fromhex(r) for r in ROWS_CASE["exchanges"][0]["responses"]],
)
SQL = ROWS_CASE["call"]["sql"]


def _connect(servers, **kw):
    # Keep the seed order deterministic: the dying member first.
    with mock.patch.object(skaidb.random, "shuffle", lambda eps: None):
        return skaidb.connect(
            seeds=[f"127.0.0.1:{s.port}" for s in servers],
            user=V["auth"]["username"],
            password=V["auth"]["password"],
            timeout=5,
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


if __name__ == "__main__":
    unittest.main()
