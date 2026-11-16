"""The session database survives a re-dial.

A session's database is the server's knowledge, and a new connection starts
in none. So the driver remembers the database a connection is in — the
`database=` it was opened with, or the last `USE` the server answered with
success — and enters it on every later dial before anything else is sent
(PROTOCOL.md §6 rule 9). Otherwise the statement after a failover runs
against whatever table of the same name the default database holds.

Against the fake server of `test_transactions`, which keeps each session's
database and says which one every statement ran in (`ran_in`).
"""

import unittest

import skaidb

try:
    from test_stream import _f_error
    from test_transactions import Server, _credentials
except ImportError:  # run as `python -m unittest tests.test_database`
    from tests.test_stream import _f_error
    from tests.test_transactions import Server, _credentials

COUNT = "SELECT count(*) FROM notes"
INSERT = "INSERT INTO notes (id, body) VALUES (?, ?)"
GONE = _f_error('database "app" does not exist')


class UseStatement(unittest.TestCase):
    """Which statements name the session database: exactly one
    `USE [DATABASE] <name>`, the name bare or in double quotes, an optional
    `;` — the cases of the Rust driver's `used_database`."""

    def test_the_database_a_statement_names(self):
        cases = [
            ("USE app", "app"),
            ("use app;", "app"),
            ("  USE\n  DATABASE app_2 ;\n", "app_2"),
            ('USE "my db"', "my db"),
            ('USE "my-db"', "my-db"),
            ('USE DATABASE "a""b"', 'a"b'),
            # A database named `database`, as the server reads it.
            ("USE DATABASE", "DATABASE"),
            ("USE DATABASE DATABASE", "DATABASE"),
            ("USE", None),
            ("USEFUL app", None),
            ("USE app; SELECT 1", None),
            ("USE my-db", None),
            ('USE "a"b"', None),
            ('USE ""', None),
            ('USE "', None),
            ("USE 'app'", None),
            ("USE ?", None),
            ("USE app -- the tenant", None),
            ("SELECT 'USE app'", None),
            ("INSERT INTO t (id) VALUES (1)", None),
            ("", None),
        ]
        for sql, want in cases:
            with self.subTest(sql):
                self.assertEqual(skaidb._used_database(sql), want)


class _Case(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.addCleanup(self.server.close)

    def connect(self, **kw):
        conn = skaidb.connect(**dict(_credentials(self.server), **kw))
        self.addCleanup(conn.close)
        return conn

    def lose_the_connection_then_read_and_write(self, conn):
        """The check itself: the server restarts while the client is idle,
        then a read and a write. Returns the database each ran in."""
        before = len(self.server.ran_in)
        self.server.restart()
        conn.execute(COUNT)
        conn.execute(INSERT, (1, "x"))
        return [db for db, sql in self.server.ran_in[before:] if sql in (COUNT, INSERT)]


class AfterARedial(_Case):
    def test_the_database_given_to_connect_is_entered_again(self):
        conn = self.connect(database="app")
        self.assertEqual(self.lose_the_connection_then_read_and_write(conn), ["app", "app"])
        self.assertEqual(self.server.connections, 2)
        # Entered before anything else is sent on the new connection.
        self.assertEqual(self.server.log[-3][1], 'USE "app"')

    def test_the_database_of_a_use_statement_is_entered_again(self):
        ways = [
            ("execute", lambda conn: conn.execute("USE app"), "app"),
            ("a cursor", lambda conn: conn.cursor().execute("use app;"), "app"),
            ("USE DATABASE", lambda conn: conn.execute("USE DATABASE app"), "app"),
            ("a quoted name", lambda conn: conn.execute('USE "my-db"'), "my-db"),
            ("a quote in the name", lambda conn: conn.execute('USE "a""b"'), 'a"b'),
            ("a stream", lambda conn: conn.stream("USE app").close(), "app"),
        ]
        for name, use, database in ways:
            with self.subTest(name):
                conn = self.connect()
                use(conn)
                self.assertEqual(
                    self.lose_the_connection_then_read_and_write(conn),
                    [database, database],
                    "the statements after the re-dial ran in another database",
                )

    def test_the_last_use_wins_over_the_database_given_to_connect(self):
        conn = self.connect(database="app")
        conn.execute("USE archive")
        self.assertEqual(
            self.lose_the_connection_then_read_and_write(conn), ["archive", "archive"]
        )
        # And over an earlier USE, re-dial after re-dial.
        conn.execute("USE app")
        self.assertEqual(self.lose_the_connection_then_read_and_write(conn), ["app", "app"])
        self.assertEqual(self.lose_the_connection_then_read_and_write(conn), ["app", "app"])

    def test_a_refused_use_changes_nothing(self):
        conn = self.connect(database="app")
        self.server.on("USE nosuch", _f_error('database "nosuch" does not exist'))
        with self.assertRaises(skaidb.ProgrammingError):
            conn.execute("USE nosuch")
        self.server.on("USE alsono", _f_error('database "alsono" does not exist'))
        with self.assertRaises(skaidb.ProgrammingError):
            conn.stream("USE alsono")
        self.assertEqual(self.lose_the_connection_then_read_and_write(conn), ["app", "app"])

    def test_a_statement_that_is_not_one_use_changes_nothing(self):
        conn = self.connect(database="app")
        conn.execute("SELECT 'USE archive'")
        conn.execute("UPDATE notes SET body = 'USE archive' WHERE id = 1")
        self.assertEqual(self.lose_the_connection_then_read_and_write(conn), ["app", "app"])

    def test_a_reconnect_by_hand_enters_it_too(self):
        conn = self.connect()
        conn.execute("USE app")
        conn.reconnect()
        conn.execute(COUNT)
        self.assertEqual(self.server.ran_in[-1], ("app", COUNT))

    def test_when_the_database_cannot_be_entered_the_statement_fails_with_that_error(self):
        for name, enter in (
            ("database=", lambda: self.connect(database="app")),
            ("a USE statement", lambda: self.connect().execute("USE app").connection),
        ):
            with self.subTest(name):
                conn = enter()
                before = len(self.server.ran_in)
                self.server.restart()
                self.server.on('USE "app"', GONE)
                with self.assertRaises(skaidb.ProgrammingError) as cm:
                    conn.execute(COUNT)
                self.assertIn('database "app" does not exist', str(cm.exception))
                # It never ran in another database — not then, not after.
                self.server.on('USE "app"', GONE)
                with self.assertRaises(skaidb.ProgrammingError):
                    conn.execute(INSERT, (1, "x"))
                ran = [(db, sql) for db, sql in self.server.ran_in[before:] if sql in (COUNT, INSERT)]
                self.assertEqual(ran, [], "a statement ran outside its database")
                # Once the database can be entered, the connection works again.
                conn.execute(COUNT)
                self.assertEqual(self.server.ran_in[-1], ("app", COUNT))

    def test_inside_a_transaction_nothing_is_redialed_whatever_the_database(self):
        conn = self.connect()
        conn.execute("USE app")

        def body(conn):
            self.server.restart()
            conn.execute(COUNT)

        with self.assertRaises(skaidb.OperationalError):
            conn.transaction(body)
        self.assertEqual(self.server.connections, 1)
        conn.execute(COUNT)
        self.assertEqual(self.server.ran_in[-1], ("app", COUNT))


class Pooled(_Case):
    def pool(self, **kw):
        pool = skaidb.pool(**dict(_credentials(self.server), **kw))
        self.addCleanup(pool.close)
        return pool

    def test_a_pooled_connection_that_redials_enters_the_pools_database(self):
        pool = self.pool(database="app")
        with pool.connection() as conn:
            conn.execute(COUNT)
        # Idle in the pool while the server restarts.
        with pool.connection() as conn:
            self.assertEqual(self.lose_the_connection_then_read_and_write(conn), ["app", "app"])
        self.assertEqual(self.server.connections, 2)

    def test_a_borrowers_use_survives_a_redial_during_its_checkout(self):
        pool = self.pool(database="app")
        with pool.connection() as conn:
            conn.execute("USE archive")
            self.assertEqual(
                self.lose_the_connection_then_read_and_write(conn), ["archive", "archive"]
            )

    def test_the_next_borrower_gets_the_pools_database_not_the_last_borrowers(self):
        pool = self.pool(database="app")
        with pool.connection() as conn:
            conn.execute("USE archive")
            conn.execute(COUNT)
            self.assertEqual(self.server.ran_in[-1], ("archive", COUNT))
        with pool.connection() as conn:
            conn.execute(COUNT)
            self.assertEqual(
                self.server.ran_in[-1], ("app", COUNT), "the last borrower's USE leaked"
            )
            # … also after a re-dial.
            self.assertEqual(self.lose_the_connection_then_read_and_write(conn), ["app", "app"])
        self.assertEqual(self.server.log.count((1, 'USE "app"')), 2, "the dial, and one reset")
        # A borrower that changed nothing costs no round trip.
        with pool.connection() as conn:
            conn.execute(COUNT)
        self.assertEqual(self.server.statements.count('USE "app"'), 3, "+ the re-dial, no more")

    def test_a_connection_that_cannot_return_to_the_pools_database_is_replaced(self):
        pool = self.pool(database="app")
        with pool.connection() as conn:
            conn.execute("USE archive")
        self.server.on('USE "app"', GONE)  # refuses the reset of connection 1
        with pool.connection() as conn:
            conn.execute(COUNT)
        self.assertEqual(self.server.ran_in[-1], ("app", COUNT))
        self.assertEqual(self.server.log[-1], (2, COUNT), "a fresh connection, in the database")
        self.assertTrue(self.server.hung_up(1), "the connection left outside it was closed")
        self.assertEqual(pool.members()[0]["idle"], 1)

    def test_a_pool_without_a_database_leaves_a_borrowers_use_in_place(self):
        # No database of its own to restore: the connection stays where its
        # last borrower took it (enter the database on every checkout, or
        # give the pool one).
        pool = self.pool()
        with pool.connection() as conn:
            conn.execute("USE archive")
        with pool.connection() as conn:
            conn.execute(COUNT)
            self.assertEqual(self.server.ran_in[-1], ("archive", COUNT))
            self.assertEqual(
                self.lose_the_connection_then_read_and_write(conn), ["archive", "archive"]
            )
        self.assertNotIn('USE "app"', self.server.statements)


if __name__ == "__main__":
    unittest.main()
