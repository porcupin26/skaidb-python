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
except ImportError:  # run as `python -m unittest tests.test_failover`
    from tests.test_conformance import V, FakeServer

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


if __name__ == "__main__":
    unittest.main()
