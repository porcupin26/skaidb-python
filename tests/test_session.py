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
    from test_transactions import DROP, Server, _credentials
except ImportError:  # run as `python -m unittest tests.test_session`
    from tests.test_stream import _f_error
    from tests.test_transactions import DROP, Server, _credentials

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

    def test_a_setting_sent_with_a_parameter_is_noted_as_it_was_sent(self):
        # The server does not prepare a SET (`statement kind cannot be
        # prepared: …`), so the value is written into the text — and that
        # text, not the one with the `?`, is what the server accepted.
        conn = self.connect()
        conn.execute("SET SCAN BUDGET ROWS ?", (7,))
        conn.cursor().executemany("SET CONSISTENCY ALL", [()])
        self.assertEqual(self.ran_under_after(conn), ("ALL", "ROWS 7"))
        self.assertEqual(
            self.lose_the_connection_then_run(conn),
            ["SET CONSISTENCY ALL", "SET SCAN BUDGET ROWS 7", COUNT],
        )
        self.assertEqual(self.ran_under(), ("ALL", "ROWS 7"))

    def ran_under_after(self, conn):
        conn.execute(COUNT)
        return self.ran_under()

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


class EveryBorrowerItsOwnSession(_Case):
    """A pooled connection reaches each borrower in the pool's own session
    state: in the pool's database — its `database=`, or `default`, where
    every new connection starts — with no consistency override, no scan
    budget and no transaction of an earlier borrower. What a borrower
    changed is put back when the connection comes back, before the next
    borrower can get it; a connection that cannot be put back is closed."""

    USE_DEFAULT = 'USE "default"'
    RESET = ["SET SCAN BUDGET DEFAULT", "SET CONSISTENCY DEFAULT"]

    def pool(self, **kw):
        kw.setdefault("maxsize", 1)  # every borrower gets the same connection
        pool = skaidb.pool(**dict(_credentials(self.server), **kw))
        self.addCleanup(pool.close)
        return pool

    def borrow_and_change_everything(self, pool):
        """Returns how much the server had received when the borrower was
        done with the connection."""
        with pool.connection() as conn:
            conn.execute("USE other")
            conn.execute(BUDGET)
            conn.execute(ALL)
            conn.execute(COUNT)
            self.assertEqual(self.server.ran_in[-1], ("other", COUNT))
            self.assertEqual(self.ran_under(), ("ALL", "ROWS 1"))
            return len(self.server.log)

    def test_the_next_borrower_gets_the_pools_state_not_the_last_borrowers(self):
        for kw, home, use in (({}, "default", self.USE_DEFAULT), ({"database": "app"}, "app", 'USE "app"')):
            with self.subTest(pool=kw):
                self.server = Server()
                self.addCleanup(self.server.close)
                pool = self.pool(**kw)
                before = self.borrow_and_change_everything(pool)
                with pool.connection() as conn:
                    conn.execute(COUNT)
                self.assertEqual(self.server.ran_in[-1], (home, COUNT), "the last borrower's USE leaked")
                self.assertEqual(self.ran_under(), (None, None), "the last borrower's settings leaked")
                # On the same connection, put back before the statement.
                self.assertEqual(
                    self.server.log[before:], [(1, use), *[(1, sql) for sql in self.RESET], (1, COUNT)]
                )
                self.assertEqual(self.server.connections, 1)

    def test_a_redial_for_the_next_borrower_restores_the_pools_state_too(self):
        # The notes went with the borrower: nothing of it is sent again.
        for kw, restored in (({}, []), ({"database": "app"}, ['USE "app"'])):
            with self.subTest(pool=kw):
                self.server = Server()
                self.addCleanup(self.server.close)
                pool = self.pool(**kw)
                self.borrow_and_change_everything(pool)
                with pool.connection() as conn:
                    self.assertEqual(self.lose_the_connection_then_run(conn), restored + [COUNT])
                    self.assertEqual(self.ran_under(), (None, None))

    def test_only_what_a_borrower_changed_is_put_back(self):
        pool = self.pool()
        changes = [
            (["USE other"], [self.USE_DEFAULT]),
            ([BUDGET], ["SET SCAN BUDGET DEFAULT"]),
            ([ALL], ["SET CONSISTENCY DEFAULT"]),
            # A borrower that put it back itself, or changed nothing, costs
            # no round trip.
            ([BUDGET, ALL, "SET SCAN BUDGET DEFAULT", "set consistency default;"], []),
            (["USE other", "USE default"], []),
            ([COUNT, INSERT], []),
        ]
        for statements, put_back in changes:
            with self.subTest(statements):
                with pool.connection() as conn:
                    for sql in statements:
                        conn.execute(sql, (1, "x") if sql == INSERT else None)
                    before = len(self.server.log)
                self.assertEqual([sql for _, sql in self.server.log[before:]], put_back)
        self.assertEqual(self.server.connections, 1)

    def test_a_pool_with_a_database_leaves_a_borrower_that_stayed_in_it_alone(self):
        pool = self.pool(database="app")
        with pool.connection() as conn:
            conn.execute(COUNT)
            conn.execute('USE "app"')
            before = len(self.server.log)
        self.assertEqual(self.server.log[before:], [])

    def test_a_transaction_left_open_is_rolled_back_first(self):
        pool = self.pool()
        with pool.connection() as conn:
            conn.execute("BEGIN")
            conn.execute(INSERT, (1, "x"))
            conn.execute(BUDGET)
            before = len(self.server.log)
        self.assertEqual(
            self.server.log[before:], [(1, "ROLLBACK"), (1, "SET SCAN BUDGET DEFAULT")]
        )
        with pool.connection() as conn:
            self.assertFalse(conn._txn_noted)
            # Not inside the last borrower's transaction: a lost connection
            # is replaced as usual.
            self.server.on(COUNT, DROP)
            conn.execute(COUNT)
        self.assertEqual(self.server.log[-2:], [(1, COUNT), (2, COUNT)])

    def test_a_connection_that_cannot_be_put_back_is_closed(self):
        # What an older server answers `SET CONSISTENCY DEFAULT` with, and
        # the same for each of the other statements.
        for refused, change in (
            ("SET CONSISTENCY DEFAULT", ALL),
            ("SET SCAN BUDGET DEFAULT", BUDGET),
            (self.USE_DEFAULT, "USE other"),
            ("ROLLBACK", "BEGIN"),
        ):
            for name, reply in (("refused", _f_error("parse error")), ("the connection lost", DROP)):
                with self.subTest(refused, how=name):
                    self.server = Server()
                    self.addCleanup(self.server.close)
                    pool = self.pool()
                    with pool.connection() as conn:
                        conn.execute(change)
                        self.server.on(refused, reply)
                    self.assertTrue(conn.closed, "pooled in another borrower's state")
                    self.assertEqual(pool.members()[0]["idle"], 0)
                    if reply is not DROP:
                        self.assertTrue(self.server.hung_up(1), "its socket was left open")
                    self.assertEqual(self.server.connections, 1, "dialed again to put it back")
                    with pool.connection() as conn:
                        conn.execute(COUNT)
                    self.assertEqual(self.server.log[-1], (2, COUNT))
                    self.assertEqual(self.server.ran_in[-1], (None, COUNT))
                    self.assertEqual(self.ran_under(), (None, None))
                    self.assertEqual(pool.members()[0], dict(pool.members()[0], in_use=0, idle=1))

    def test_a_connection_lost_inside_a_transaction_is_closed_without_a_statement(self):
        pool = self.pool()
        with pool.connection() as conn:
            conn.execute("BEGIN")
            self.server.on(INSERT, DROP)
            with self.assertRaises(skaidb.OperationalError):
                conn.execute(INSERT, (1, "x"))
        self.assertEqual(self.server.log, [(1, "BEGIN"), (1, INSERT)])
        self.assertEqual(self.server.connections, 1)
        self.assertEqual(pool.members()[0]["idle"], 0)

    def test_an_interrupt_while_it_is_put_back_costs_the_connection_not_the_pool(self):
        pool = self.pool()
        stop = KeyboardInterrupt()
        conn = pool.getconn()
        conn.execute(BUDGET)
        real = skaidb.Connection._read_frame

        def read_frame(self):
            raise stop

        skaidb.Connection._read_frame = read_frame
        try:
            with self.assertRaises(KeyboardInterrupt):
                pool.putconn(conn)
        finally:
            skaidb.Connection._read_frame = real
        self.assertTrue(conn.closed)
        self.assertEqual(pool.members()[0], dict(pool.members()[0], in_use=0, idle=0))
        with pool.connection() as conn:
            conn.execute(COUNT)
        self.assertEqual(self.ran_under(), (None, None))

    def test_the_helper_leaves_nothing_to_put_back(self):
        pool = self.pool(database="app")
        pool.transaction(lambda conn: conn.execute(COUNT))
        self.assertEqual(
            [sql for _, sql in self.server.log], ['USE "app"', "BEGIN", COUNT, "COMMIT"]
        )

    def test_a_closed_pool_sends_nothing_to_a_connection_that_comes_back(self):
        pool = self.pool()
        conn = pool.getconn()
        conn.execute(BUDGET)
        pool.close()
        before = len(self.server.log)
        pool.putconn(conn)
        self.assertEqual(self.server.log[before:], [])
        self.assertTrue(conn.closed)


if __name__ == "__main__":
    unittest.main()
