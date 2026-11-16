"""A session's settings survive a re-dial.

Besides its database, a session on the server carries a consistency
override (`SET CONSISTENCY`) and a scan budget (`SET SCAN BUDGET`). A new
connection has neither. So the driver notes the last of each the server
answered with success and sends it again on every later dial, after the
session database and before the statement — otherwise the statement after a
member's restart reads at another consistency, or past the limit the
application set, with nothing to say the connection changed.

Against the fake server of `test_transactions`, which keeps each session's
settings and says which ones every statement ran under (`ran_under`).
"""

import unittest

import skaidb

try:
    from test_stream import _f_error
    from test_transactions import Server, _credentials
except ImportError:  # run as `python -m unittest tests.test_session`
    from tests.test_stream import _f_error
    from tests.test_transactions import Server, _credentials

COUNT = "SELECT count(*) FROM notes"
INSERT = "INSERT INTO notes (id, body) VALUES (?, ?)"
BUDGET = "SET SCAN BUDGET ROWS 1"
ALL = "SET CONSISTENCY ALL"


class SessionStatement(unittest.TestCase):
    """Which statements the driver notes: exactly one `SET CONSISTENCY …` or
    `SET SCAN BUDGET …` — keywords in any case, free whitespace, an optional
    `;`, `--` comments skipped as the server skips them. It reads the first
    words and no further: the statement's text is the note, and `DEFAULT` as
    the next word clears it."""

    def test_the_setting_a_statement_makes(self):
        cases = [
            ("SET CONSISTENCY ALL", ("consistency", "SET CONSISTENCY ALL")),
            ("set consistency one;", ("consistency", "set consistency one")),
            ("  SET\n CONSISTENCY\tQUORUM ;\n", ("consistency", "SET\n CONSISTENCY\tQUORUM")),
            ("SET CONSISTENCY DEFAULT", ("consistency", None)),
            ("set consistency default;", ("consistency", None)),
            ("SET CONSISTENCY ALL -- every replica", ("consistency", "SET CONSISTENCY ALL")),
            ("-- first\nSET CONSISTENCY ONE; -- then\n", ("consistency", "SET CONSISTENCY ONE")),
            ("SET SCAN BUDGET ROWS 1", ("scan_budget", "SET SCAN BUDGET ROWS 1")),
            ("SET SCAN BUDGET ROWS 5000 BYTES 1048576;", ("scan_budget", "SET SCAN BUDGET ROWS 5000 BYTES 1048576")),
            ("set scan budget bytes 64 rows 2", ("scan_budget", "set scan budget bytes 64 rows 2")),
            ("SET SCAN BUDGET DEFAULT", ("scan_budget", None)),
            ("Set  Scan\nBudget  Default ;", ("scan_budget", None)),
            ("SET SCAN BUDGET -- lifted\n DEFAULT", ("scan_budget", None)),
            # Not exactly one such statement: left alone.
            ("SET SCAN BUDGET ROWS 1; SELECT 1", None),
            ("SET CONSISTENCY ALL; SET SCAN BUDGET ROWS 1", None),
            ("-- SET CONSISTENCY ALL", None),
            ("SELECT 'SET CONSISTENCY ALL'", None),
            ("SET CONFIG storage.scan_row_budget = 5", None),
            ("SET SCAN", None),
            ("SET", None),
            ("SETTLE CONSISTENCY ALL", None),
            ("SET CONSISTENCYALL", None),
            ("RESET CONSISTENCY", None),
            ("UPDATE notes SET consistency = 1", None),
            ("", None),
            # A `USE` is read by its own rules (tests/test_database.py).
            ("USE app", ("use", "app")),
            ("USE app; SET CONSISTENCY ALL", None),
        ]
        for sql, want in cases:
            with self.subTest(sql):
                self.assertEqual(skaidb._session_statement(sql), want)


class _Case(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.addCleanup(self.server.close)

    def connect(self, **kw):
        conn = skaidb.connect(**dict(_credentials(self.server), **kw))
        self.addCleanup(conn.close)
        return conn

    def lose_the_connection_then_run(self, conn, sql=COUNT):
        """The check itself: the server restarts while the client is idle,
        then a statement. Returns what the new connection received, the
        statement last."""
        before = len(self.server.log)
        self.server.restart()
        conn.execute(sql)
        return [sql for _, sql in self.server.log[before:]]

    def ran_under(self, sql=COUNT):
        """The settings the last `sql` ran under: (consistency, scan budget)."""
        consistency, budget, last = self.server.ran_under[-1]
        self.assertEqual(last, sql)
        return (consistency, budget)


class AfterARedial(_Case):
    def test_both_settings_are_sent_again_before_the_statement(self):
        conn = self.connect()
        conn.execute(BUDGET)
        conn.execute(ALL)
        conn.execute(COUNT)
        self.assertEqual(self.ran_under(), ("ALL", "ROWS 1"))
        # The consistency first, then the scan budget, then the statement.
        self.assertEqual(self.lose_the_connection_then_run(conn), [ALL, BUDGET, COUNT])
        self.assertEqual(self.ran_under(), ("ALL", "ROWS 1"), "the settings were lost with the connection")
        self.assertEqual(self.server.connections, 2)

    def test_they_follow_the_session_database(self):
        conn = self.connect(database="app")
        conn.execute(ALL)
        conn.execute(BUDGET)
        self.assertEqual(
            self.lose_the_connection_then_run(conn), ['USE "app"', ALL, BUDGET, COUNT]
        )
        self.assertEqual(self.server.ran_in[-1], ("app", COUNT))
        self.assertEqual(self.ran_under(), ("ALL", "ROWS 1"))

    def test_only_a_setting_that_was_made_is_sent(self):
        for setting, under in ((ALL, ("ALL", None)), (BUDGET, (None, "ROWS 1"))):
            with self.subTest(setting):
                conn = self.connect()
                conn.execute(setting)
                self.assertEqual(self.lose_the_connection_then_run(conn), [setting, COUNT])
                self.assertEqual(self.ran_under(), under)
        # And nothing at all on a connection that set nothing.
        conn = self.connect()
        self.assertEqual(self.lose_the_connection_then_run(conn), [COUNT])

    def test_every_way_of_sending_the_statement_is_noted(self):
        ways = [
            ("execute", lambda conn, sql: conn.execute(sql)),
            ("a cursor", lambda conn, sql: conn.cursor().execute(sql)),
            ("a stream", lambda conn, sql: conn.stream(sql).close()),
            ("executemany", lambda conn, sql: conn.cursor().executemany(sql, [()])),
        ]
        for name, send in ways:
            with self.subTest(name):
                conn = self.connect()
                send(conn, "set scan budget rows 7 bytes 9;  -- for the export")
                send(conn, "SET CONSISTENCY one")
                self.lose_the_connection_then_run(conn)
                self.assertEqual(self.ran_under(), ("ONE", "ROWS 7 BYTES 9"))

    def test_the_last_setting_of_each_kind_wins(self):
        conn = self.connect()
        for sql in (ALL, BUDGET, "SET CONSISTENCY ONE", "SET SCAN BUDGET BYTES 4096"):
            conn.execute(sql)
        self.assertEqual(
            self.lose_the_connection_then_run(conn),
            ["SET CONSISTENCY ONE", "SET SCAN BUDGET BYTES 4096", COUNT],
        )
        self.assertEqual(self.ran_under(), ("ONE", "BYTES 4096"))
        # … re-dial after re-dial.
        self.lose_the_connection_then_run(conn)
        self.assertEqual(self.ran_under(), ("ONE", "BYTES 4096"))

    def test_a_setting_cleared_with_default_is_not_sent_again(self):
        conn = self.connect()
        conn.execute(ALL)
        conn.execute(BUDGET)
        conn.execute("SET SCAN BUDGET DEFAULT")
        self.assertEqual(self.lose_the_connection_then_run(conn), [ALL, COUNT])
        self.assertEqual(self.ran_under(), ("ALL", None))
        conn.execute("set consistency default;")
        self.assertEqual(self.lose_the_connection_then_run(conn), [COUNT])
        self.assertEqual(self.ran_under(), (None, None))

    def test_a_refused_setting_changes_nothing(self):
        conn = self.connect()
        conn.execute(ALL)
        conn.execute(BUDGET)
        self.server.on("SET CONSISTENCY EVENTUAL", _f_error("SET CONSISTENCY takes ONE, QUORUM, ALL, or DEFAULT"))
        with self.assertRaises(skaidb.ProgrammingError):
            conn.execute("SET CONSISTENCY EVENTUAL")
        self.server.on("SET SCAN BUDGET ROWS 0", _f_error("SCAN BUDGET ROWS: a positive integer"))
        with self.assertRaises(skaidb.ProgrammingError):
            conn.stream("SET SCAN BUDGET ROWS 0")
        # Nor does an older server's refusal of DEFAULT clear the override
        # it never lifted.
        self.server.on("SET CONSISTENCY DEFAULT", _f_error("parse error"))
        with self.assertRaises(skaidb.ProgrammingError):
            conn.execute("SET CONSISTENCY DEFAULT")
        self.assertEqual(self.lose_the_connection_then_run(conn), [ALL, BUDGET, COUNT])

    def test_a_statement_that_is_not_one_setting_changes_nothing(self):
        conn = self.connect()
        conn.execute(BUDGET)
        conn.execute("SELECT 'SET SCAN BUDGET DEFAULT'")
        conn.execute("UPDATE notes SET body = 'SET CONSISTENCY ALL' WHERE id = 1")
        self.assertEqual(self.lose_the_connection_then_run(conn), [BUDGET, COUNT])

    def test_a_write_runs_under_them_too(self):
        conn = self.connect()
        conn.execute(ALL)
        conn.execute(BUDGET)
        self.server.restart()
        conn.execute(INSERT, (1, "x"))
        self.assertEqual(self.ran_under(INSERT), ("ALL", "ROWS 1"))

    def test_a_reconnect_by_hand_sends_them_too(self):
        conn = self.connect()
        conn.execute(ALL)
        conn.execute(BUDGET)
        conn.reconnect()
        conn.execute(COUNT)
        self.assertEqual(self.ran_under(), ("ALL", "ROWS 1"))

    def test_when_a_setting_is_refused_the_statement_fails_with_that_error(self):
        refused = _f_error("resource limit: SET SCAN BUDGET is not available")
        conn = self.connect()
        conn.execute(ALL)
        conn.execute(BUDGET)
        before = len(self.server.ran_under)
        self.server.restart()
        self.server.on(BUDGET, refused)
        with self.assertRaises(skaidb.ProgrammingError) as cm:
            conn.execute(COUNT)
        self.assertIn("SET SCAN BUDGET is not available", str(cm.exception))
        # It never ran without its limit — not then, not after.
        self.server.on(BUDGET, refused)
        with self.assertRaises(skaidb.ProgrammingError):
            conn.execute(INSERT, (1, "x"))
        ran = [sql for _, _, sql in self.server.ran_under[before:] if sql in (COUNT, INSERT)]
        self.assertEqual(ran, [], "a statement ran under other settings than the application set")
        self.assertFalse(conn.is_usable())
        # Once the setting is accepted, the connection works again.
        conn.execute(COUNT)
        self.assertEqual(self.ran_under(), ("ALL", "ROWS 1"))

    def test_inside_a_transaction_nothing_is_redialed_whatever_the_settings(self):
        conn = self.connect()
        conn.execute(BUDGET)

        def body(conn):
            self.server.restart()
            conn.execute(COUNT)

        with self.assertRaises(skaidb.OperationalError):
            conn.transaction(body)
        self.assertEqual(self.server.connections, 1)
        conn.execute(COUNT)
        self.assertEqual(self.ran_under(), (None, "ROWS 1"))


class Pooled(_Case):
    def pool(self, **kw):
        pool = skaidb.pool(**dict(_credentials(self.server), **kw))
        self.addCleanup(pool.close)
        return pool

    def test_a_pooled_connection_redialed_while_a_borrower_holds_it_keeps_its_settings(self):
        pool = self.pool(database="app")
        with pool.connection() as conn:
            conn.execute(BUDGET)
            conn.execute(ALL)
            self.assertEqual(
                self.lose_the_connection_then_run(conn), ['USE "app"', ALL, BUDGET, COUNT]
            )
            self.assertEqual(self.ran_under(), ("ALL", "ROWS 1"))
            self.assertEqual(self.server.ran_in[-1], ("app", COUNT))


if __name__ == "__main__":
    unittest.main()
