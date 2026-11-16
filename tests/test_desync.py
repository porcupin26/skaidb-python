"""A statement reads its own reply, or none.

The protocol has no request ids: a reply belongs to a request only because
the client read every earlier reply to its end (PROTOCOL.md §6 rule 4 — on
any framing or I/O error the connection is dead). So a socket that is out of
step with the server is never read again, and a `RowStream` reads only the
socket it was opened on.

Against the fake server of `test_transactions`, over real sockets.
"""

import time
import unittest

import skaidb

try:
    from test_stream import _f_chunk, _f_end, _f_header
    from test_transactions import SCAN, _Case, _rows
except ImportError:  # run as `python -m unittest tests.test_desync`
    from tests.test_stream import _f_chunk, _f_end, _f_header
    from tests.test_transactions import SCAN, _Case, _rows

NAMES = "SELECT name FROM accounts"


class StaleStream(_Case):
    """A stream belongs to the socket it was opened on: once the connection
    has re-dialed, the stream reads nothing and releases nothing — what is
    on the connection now is another statement's."""

    def setUp(self):
        super().setUp()
        self.server.always(
            SCAN, [_f_header(["id"]), _f_chunk([(1,), (2,)]), _f_chunk([(3,)]), _f_end()]
        )
        self.server.always(
            NAMES, [_f_header(["name"]), _f_chunk([("ada",)]), _f_chunk([("bob",)]), _f_end()]
        )
        self.server.always("SELECT 'mine'", _rows(["v"], [("mine",)]))

    def stale_stream(self):
        """A stream with one row read and the rest unread, on a connection
        that has re-dialed since."""
        rows = self.conn.stream(SCAN)
        self.assertEqual(next(rows), (1,))
        self.conn.reconnect()
        self.assertEqual(self.server.connections, 2)
        return rows

    def test_closing_it_reads_nothing_from_the_new_socket(self):
        old = self.stale_stream()
        self.assertEqual(self.conn.execute("SELECT 'mine'").fetchall(), [("mine",)])
        started = time.monotonic()
        old.close()
        # Draining would wait out the read timeout for frames the new socket
        # does not have, then mark the healthy connection broken.
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertTrue(self.conn.is_usable())
        self.assertEqual(self.conn.execute("SELECT 'mine'").fetchall(), [("mine",)])
        self.assertEqual(self.server.connections, 2, "nothing had to be re-dialed again")

    def test_closing_it_does_not_drain_or_release_a_newer_stream(self):
        old = self.stale_stream()
        new = self.conn.stream(NAMES)
        old.close()
        # The connection still belongs to the new stream …
        self.assertFalse(self.conn.is_usable())
        with self.assertRaises(skaidb.ProgrammingError) as cm:
            self.conn.execute("SELECT 'mine'")
        self.assertIn("busy streaming", str(cm.exception))
        # … which has every one of its rows.
        self.assertEqual(list(new), [("ada",), ("bob",)])
        self.assertTrue(self.conn.is_usable())
        self.assertEqual(self.conn.execute("SELECT 'mine'").fetchall(), [("mine",)])

    def test_iterating_it_raises_and_never_yields_another_statements_rows(self):
        old = self.stale_stream()
        new = self.conn.stream(NAMES)
        # The rest of the chunk it had already received is its own …
        self.assertEqual(next(old), (2,))
        # … the next frame on the connection is not.
        with self.assertRaises(skaidb.OperationalError) as cm:
            next(old)
        self.assertIn("re-dialed", str(cm.exception))
        self.assertEqual(list(old), [], "finished, like any stream after its error")
        self.assertFalse(self.conn._broken, "the new connection is not at fault")
        self.assertEqual(list(new), [("ada",), ("bob",)])
        old.close()
        self.assertTrue(self.conn.is_usable())

    def test_dropping_it_is_harmless(self):
        old = self.stale_stream()
        new = self.conn.stream(NAMES)
        del old  # no close(): the refcount drop runs __del__
        self.assertEqual(list(new), [("ada",), ("bob",)])
        self.assertTrue(self.conn.is_usable())

    def test_a_stream_whose_connection_was_closed_raises_and_reads_nothing(self):
        rows = self.conn.stream(SCAN)
        self.assertEqual(next(rows), (1,))
        self.conn.close()
        self.assertEqual(next(rows), (2,))
        with self.assertRaises(skaidb.OperationalError):
            next(rows)
        rows.close()

    def test_a_reconnect_that_cannot_dial_does_not_leave_the_connection_streaming(self):
        rows = self.conn.stream(SCAN)
        self.assertEqual(next(rows), (1,))
        self.conn._endpoints = [("127.0.0.1", _closed_port())]
        with self.assertRaises(skaidb.OperationalError):
            self.conn.reconnect()
        # The old socket is gone and no new one came: no stream is in flight,
        # whether or not the stale one is ever closed.
        self.conn._endpoints = [("127.0.0.1", self.server.port)]
        self.assertEqual(self.conn.execute("SELECT 'mine'").fetchall(), [("mine",)])
        rows.close()
        self.assertTrue(self.conn.is_usable())


def _closed_port() -> int:
    """A local port nothing listens on."""
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


if __name__ == "__main__":
    unittest.main()
