"""The server's prepared-statement table never fills.

A connection may hold at most 256 prepared statements on the server; the
257th prepare is refused. The driver prepares every parameterised statement,
so it keeps at most 240 of them and closes each one beyond that as soon as it
has run — and a statement must never be answered by the client-side literal
fallback because the table was full: literals type values differently, and
arrays and documents have none.

Against the fake server of `test_transactions`, which keeps the real
server's table per connection: 256 slots, freed by the close opcode.
"""

import datetime
import unittest

import skaidb

try:
    from test_stream import _f_error
    from test_transactions import DROP, Server, _credentials
except ImportError:  # run as `python -m unittest tests.test_prepared`
    from tests.test_stream import _f_error
    from tests.test_transactions import DROP, Server, _credentials

WHEN = datetime.datetime(2026, 11, 16, 12, 30, tzinfo=datetime.timezone.utc)
TAGS = ["a", {"k": [1, 2]}]
BLOB = b"\x00\xff"


def _insert(n: int) -> str:
    """One of many distinct statement texts."""
    return f"INSERT INTO t (id, a{n}, at, raw) VALUES (?, ?, ?, ?)"


class _Case(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.addCleanup(self.server.close)
        self.conn = skaidb.connect(**_credentials(self.server))
        self.addCleanup(self.conn.close)

    def assert_never_full(self):
        self.assertLess(self.server.prepared_peak, Server.MAX_PREPARED)
        self.assertEqual(self.server.prepared_open(1), skaidb._MAX_PREPARED_CACHE)
        self.assertEqual(len(self.conn._prepared), skaidb._MAX_PREPARED_CACHE)
        self.assertEqual(self.server.connections, 1)


class ManyDistinctStatements(_Case):
    def test_three_hundred_statements_are_all_bound_with_typed_parameters(self):
        cur = self.conn.cursor()
        for n in range(300):
            cur.execute(_insert(n), (n, TAGS, WHEN, BLOB))
            self.assertEqual(cur.rowcount, 1)
        # Every one went out as a prepared execute carrying typed values …
        self.assertEqual(self.server.ops, [(skaidb._OP_EXECUTE, _insert(n)) for n in range(300)])
        self.assertEqual(
            self.server.bound, [(_insert(n), [n, TAGS, WHEN, BLOB]) for n in range(300)]
        )
        # … and the statements past the cache were closed as they went.
        self.assert_never_full()

    def test_the_same_statements_again_reuse_what_is_cached(self):
        cur = self.conn.cursor()
        for _ in range(2):
            for n in range(300):
                cur.execute(_insert(n), (n, TAGS, WHEN, BLOB))
        self.assertEqual(len(self.server.ops), 600)
        self.assertEqual({op for op, _ in self.server.ops}, {skaidb._OP_EXECUTE})
        self.assert_never_full()

    def test_executemany_with_three_hundred_statements(self):
        cur = self.conn.cursor()
        for n in range(300):
            cur.executemany(_insert(n), [(n, TAGS, WHEN, BLOB), (n + 1000, TAGS, WHEN, BLOB)])
        self.assertEqual(
            self.server.ops, [(skaidb._OP_EXECUTE_BATCH, _insert(n)) for n in range(300)]
        )
        self.assert_never_full()

    def test_a_statement_past_the_cache_is_closed_whatever_stops_it(self):
        # Each of these fails after the prepare. Its slot must still be
        # given back: sixteen that are not fill the server's table, and
        # every new statement on the connection is refused from then on.
        failures = [
            ("too few parameters", lambda cur, sql: cur.execute(sql, (1, TAGS))),
            ("too many parameters", lambda cur, sql: cur.execute(sql, (1, TAGS, WHEN, BLOB, 5))),
            (
                "a value that cannot be bound",
                lambda cur, sql: cur.execute(sql, (1, TAGS, WHEN, object())),
            ),
            (
                "executemany, a short row",
                lambda cur, sql: cur.executemany(sql, [(1, TAGS, WHEN, BLOB), (2,)]),
            ),
            (
                "executemany, a value that cannot be bound",
                lambda cur, sql: cur.executemany(sql, [(1, TAGS, WHEN, {1: 2})]),
            ),
        ]
        for name, fail in failures:
            with self.subTest(name):
                server = Server()
                self.addCleanup(server.close)
                conn = skaidb.connect(**_credentials(server))
                self.addCleanup(conn.close)
                cur = conn.cursor()
                for n in range(skaidb._MAX_PREPARED_CACHE):
                    cur.execute(_insert(n), (n, TAGS, WHEN, BLOB))
                for n in range(1000, 1020):
                    with self.assertRaises(skaidb.ProgrammingError) as cm:
                        fail(cur, _insert(n))
                    self.assertNotEqual(str(cm.exception), Server.TABLE_FULL)
                self.assertEqual(server.prepared_open(1), skaidb._MAX_PREPARED_CACHE)
                # The connection still prepares and runs new statements.
                cur.execute(_insert(5000), (1, TAGS, WHEN, BLOB))
                self.assertEqual(server.ops[-1], (skaidb._OP_EXECUTE, _insert(5000)))
                self.assertLess(server.prepared_peak, Server.MAX_PREPARED)
                self.assertEqual(server.connections, 1)

    def test_a_server_error_past_the_cache_closes_the_statement_too(self):
        cur = self.conn.cursor()
        for n in range(skaidb._MAX_PREPARED_CACHE):
            cur.execute(_insert(n), (n, TAGS, WHEN, BLOB))
        for n in range(1000, 1020):
            self.server.on(_insert(n), _f_error("constraint violated"))
            with self.assertRaises(skaidb.ProgrammingError):
                cur.execute(_insert(n), (n, TAGS, WHEN, BLOB))
        self.assert_never_full()


class RefusedPrepare(_Case):
    def test_a_full_table_is_a_statement_error_not_a_literal_fallback(self):
        sql = "INSERT INTO t (id, at, raw) VALUES (?, ?, ?)"
        self.server.on(sql, _f_error(Server.TABLE_FULL), prepare=True)
        with self.assertRaises(skaidb.ProgrammingError) as cm:
            self.conn.execute(sql, (1, WHEN, BLOB))
        self.assertEqual(str(cm.exception), Server.TABLE_FULL)
        # Nothing was sent as text with the values written into it.
        self.assertEqual(self.server.ops, [])
        # A statement error: the connection is not broken and not re-dialed.
        self.assertTrue(self.conn.is_usable())
        self.conn.execute(sql, (1, WHEN, BLOB))
        self.assertEqual(self.server.ops, [(skaidb._OP_EXECUTE, sql)])
        self.assertEqual(self.server.connections, 1)

    def test_executemany_does_not_fall_back_either(self):
        sql = "INSERT INTO t (id, at, raw) VALUES (?, ?, ?)"
        self.server.on(sql, _f_error(Server.TABLE_FULL), prepare=True)
        with self.assertRaises(skaidb.ProgrammingError) as cm:
            self.conn.cursor().executemany(sql, [(1, WHEN, BLOB), (2, WHEN, BLOB)])
        self.assertEqual(str(cm.exception), Server.TABLE_FULL)
        self.assertEqual(self.server.ops, [])

    def test_only_a_statement_the_server_will_not_prepare_is_sent_as_text(self):
        # The one refusal that does mean "send it as text": the statement
        # kind cannot be prepared at all.
        sql = "CREATE TABLE t (PRIMARY KEY (id)) WITH (ttl = ?)"
        self.server.on(sql, _f_error("statement kind cannot be prepared"), prepare=True)
        self.conn.execute(sql, (60,))
        self.assertEqual(
            self.server.ops,
            [(skaidb._OP_QUERY, "CREATE TABLE t (PRIMARY KEY (id)) WITH (ttl = 60)")],
        )

    def test_a_reconnect_starts_with_an_empty_table(self):
        cur = self.conn.cursor()
        for n in range(skaidb._MAX_PREPARED_CACHE + 5):
            cur.execute(_insert(n), (n, TAGS, WHEN, BLOB))
        self.server.on(_insert(0), DROP)
        cur.execute(_insert(0), (0, TAGS, WHEN, BLOB))  # lost, re-dialed, run again
        self.assertEqual(self.server.connections, 2)
        self.assertEqual(self.server.prepared_open(2), 1)
        self.assertEqual(len(self.conn._prepared), 1)


# A and B take the same number of parameters and do different things.
UPDATE = "UPDATE t SET v = ? WHERE id = ?"
DELETE = "DELETE FROM t WHERE v = ? AND id = ?"
SCAN = "SELECT id FROM t"


class AfterARedial(_Case):
    """A prepared statement's id is a slot on the connection that prepared
    it. On a new connection the same number is unknown, or names another
    statement prepared there since — so no id crosses a re-dial: the
    connection's cache is emptied with the socket, and a statement is
    prepared again before it runs. The fake server resolves every execute
    through its own table, so a statement run under a stale id would show
    up as the other statement, or in `unknown_ids`."""

    def prepare_both(self, conn):
        conn.execute(UPDATE, ("warm", 9))  # slot 0
        conn.execute(DELETE, ("warm", 9))  # slot 1
        self.assertEqual(conn._prepared, {UPDATE: (0, 2), DELETE: (1, 2)})
        del self.server.bound[:]

    def assert_each_ran_as_itself(self, conn):
        """B first — it takes the first slot of the new connection — then A,
        cached before the loss."""
        self.assertEqual(conn.execute(DELETE, ("old", 1)).rowcount, 1)
        self.assertEqual(conn.execute(UPDATE, ("new", 2)).rowcount, 1)
        self.assertEqual(self.server.bound, [(DELETE, ["old", 1]), (UPDATE, ["new", 2])])
        self.assertEqual(self.server.unknown_ids, [], "an id of the old connection was sent")
        self.assertEqual(conn._prepared, {DELETE: (0, 2), UPDATE: (1, 2)})

    def test_a_connection_lost_while_idle(self):
        self.prepare_both(self.conn)
        self.server.restart()
        self.assert_each_ran_as_itself(self.conn)
        self.assertEqual(self.server.connections, 2)

    def test_a_connection_already_marked_broken(self):
        # Broken by a statement that is not retried (a stream): the next
        # statement re-dials before it is sent, and must not send the id it
        # found in the cache of the connection that is gone.
        self.prepare_both(self.conn)
        self.server.on(SCAN, DROP)
        with self.assertRaises(skaidb.OperationalError):
            self.conn.stream(SCAN)
        self.assertFalse(self.conn.is_usable())
        self.assert_each_ran_as_itself(self.conn)
        self.assertEqual(self.server.connections, 2)

    def test_executemany_on_a_connection_already_marked_broken(self):
        self.prepare_both(self.conn)
        self.server.on(SCAN, DROP)
        with self.assertRaises(skaidb.OperationalError):
            self.conn.stream(SCAN)
        cur = self.conn.cursor()
        cur.executemany(DELETE, [("old", 1), ("old", 2)])
        self.assertEqual(self.server.ops[-1], (skaidb._OP_EXECUTE_BATCH, DELETE))
        self.assertEqual(self.server.unknown_ids, [])

    def test_a_reconnect_by_hand(self):
        for auto in (True, False):
            with self.subTest(auto_reconnect=auto):
                server = Server()
                self.addCleanup(server.close)
                conn = skaidb.connect(**dict(_credentials(server), auto_reconnect=auto))
                self.addCleanup(conn.close)
                self.server = server
                self.prepare_both(conn)
                conn.reconnect()
                self.assertEqual(conn._prepared, {})
                self.assert_each_ran_as_itself(conn)

    def test_without_automatic_reconnect_the_cache_goes_with_the_socket(self):
        conn = skaidb.connect(**dict(_credentials(self.server), auto_reconnect=False))
        self.addCleanup(conn.close)
        self.prepare_both(conn)
        self.server.restart()
        with self.assertRaises(skaidb.OperationalError):
            conn.execute(DELETE, ("old", 1))
        self.assertEqual(conn._prepared, {}, "ids of a socket that is closed")
        conn.reconnect()
        self.assert_each_ran_as_itself(conn)

    def test_each_pooled_connection_prepares_for_itself(self):
        pool = skaidb.pool(**_credentials(self.server))
        self.addCleanup(pool.close)
        first, second = pool.getconn(), pool.getconn()
        first.execute(UPDATE, ("warm", 9))  # slot 0 of its connection
        second.execute(DELETE, ("old", 1))  # slot 0 of the other one
        second.execute(UPDATE, ("new", 2))
        first.execute(DELETE, ("old", 3))
        self.assertEqual(
            self.server.bound,
            [(UPDATE, ["warm", 9]), (DELETE, ["old", 1]), (UPDATE, ["new", 2]), (DELETE, ["old", 3])],
        )
        self.assertEqual(self.server.unknown_ids, [])
        pool.putconn(first)
        pool.putconn(second)


if __name__ == "__main__":
    unittest.main()
