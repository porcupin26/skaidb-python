"""A statement reads its own reply, or none.

The protocol has no request ids: a reply belongs to a request only because
the client read every earlier reply to its end (PROTOCOL.md §6 rule 4 — on
any framing or I/O error the connection is dead). So a socket that is out of
step with the server is never read again, and a `RowStream` reads only the
socket it was opened on.

Against the fake server of `test_transactions`, over real sockets.
"""

import gc
import signal
import struct
import threading
import time
import unittest
from asyncio import CancelledError
from unittest import mock

import skaidb

try:
    from test_stream import _f_chunk, _f_end, _f_error, _f_header, _f_mutation
    from test_transactions import (
        CREDIT, DROP, SCAN, STALL, Late, Server, _Case, _credentials, _rows,
    )
except ImportError:  # run as `python -m unittest tests.test_desync`
    from tests.test_stream import _f_chunk, _f_end, _f_error, _f_header, _f_mutation
    from tests.test_transactions import (
        CREDIT, DROP, SCAN, STALL, Late, Server, _Case, _credentials, _rows,
    )

NAMES = "SELECT name FROM accounts"

_HEADER = _f_header(["id"])
_CHUNK = _f_chunk([(1,)])
_UNKNOWN = bytes([99])  # no such response tag
# A result with more frames still to come than an abandoned stream drains.
_TOO_LONG = (
    [_HEADER]
    + [_f_chunk([(i,)]) for i in range(skaidb._STREAM_DRAIN_MAX_FRAMES + 5)]
    + [_f_end()]
)


def _abandon(conn):
    """Open a stream, read one row, close it with the rest unread."""
    rows = conn.stream(SCAN)
    next(rows)
    rows.close()


def _impatient(conn):
    """Shorten the read timeout from here on (the dial and the handshake
    ran under the usual one), for a reply that is not coming."""
    conn._sock.settimeout(0.3)
    return conn


def _abandon_impatient(conn):
    rows = conn.stream(SCAN)
    next(rows)
    _impatient(conn)
    rows.close()


class _Alarm(Exception):
    """An ordinary exception thrown in from outside the driver, as the
    handler of a `signal.alarm` deadline does."""


class _InterruptedRead:
    """Stands in for the socket's read file: the read after `after` more
    raises `exc` instead of reading — what a signal handler that raises
    does to a blocked `recv`. Whatever the server sent stays unread."""

    def __init__(self, conn, exc, after=0):
        self._inner, self._exc, self._after = conn._file, exc, after
        conn._file = self

    def read(self, n):
        if self._exc is not None:
            if self._after == 0:
                exc, self._exc = self._exc, None
                raise exc
            self._after -= 1
        return self._inner.read(n)

    def close(self):
        self._inner.close()


class _InterruptedSend:
    """Stands in for the socket: the next request gets half way out, then
    `exc` is raised."""

    def __init__(self, conn, exc):
        self._inner, self._exc = conn._sock, exc
        conn._sock = self

    def sendall(self, data):
        if self._exc is None:
            return self._inner.sendall(data)
        exc, self._exc = self._exc, None
        self._inner.sendall(data[: len(data) // 2])
        raise exc

    def close(self):
        self._inner.close()


FIRST, SECOND = "SELECT 'first'", "SELECT 'second'"
BATCH = "INSERT INTO audit (id) VALUES (?)"


class Interrupted(unittest.TestCase):
    """Whatever leaves a round trip between the first byte sent and the last
    byte read marks the connection broken and closes its socket, and goes on
    unchanged — a `KeyboardInterrupt`, or anything else a signal handler or
    a green thread throws into the middle of a statement. Otherwise the
    unread reply answers the NEXT statement, and every one after it reads
    the reply of the one before."""

    # Where a statement can be interrupted: what runs first on the
    # connection, the statement, and how many reads succeed before the
    # interrupted one (a frame is two reads: its length, then its body).
    PATHS = [
        ("a query", None, lambda conn: conn.execute(FIRST), 0),
        ("a query, between a frame's length and its body", None, lambda conn: conn.execute(FIRST), 1),
        ("preparing a statement", None, lambda conn: conn.execute(CREDIT, (10, 2)), 0),
        (
            "a prepared statement",
            lambda conn: conn.execute(CREDIT, (10, 1)),
            lambda conn: conn.execute(CREDIT, (10, 2)),
            0,
        ),
        (
            "executemany",
            lambda conn: conn.cursor().executemany(BATCH, [(1,)]),
            lambda conn: conn.cursor().executemany(BATCH, [(2,), (3,)]),
            0,
        ),
        ("opening a stream", None, lambda conn: conn.stream(SCAN), 0),
        ("a stream's next frame", None, lambda conn: list(conn.stream(SCAN)), 2),
        (
            "a stream's next frame, between its length and its body",
            None,
            lambda conn: list(conn.stream(SCAN)),
            3,
        ),
        ("draining an abandoned stream", None, _abandon, 4),
    ]

    # Not one of them is a transport error, and most are not an `Exception`.
    INTERRUPTS = [
        lambda: KeyboardInterrupt(),
        lambda: SystemExit(1),
        lambda: CancelledError(),
        lambda: GeneratorExit(),
        lambda: _Alarm("deadline"),
        lambda: MemoryError(),
    ]

    def server(self):
        server = Server()
        self.addCleanup(server.close)
        server.always(FIRST, _rows(["v"], [("first",)]))
        server.always(SECOND, _rows(["v"], [("second",)]))
        server.always(
            SCAN, [_f_header(["id"]), _f_chunk([(1,)]), _f_chunk([(2,)]), _f_end()]
        )
        return server

    def connect(self, server, **kw):
        conn = skaidb.connect(**dict(_credentials(server), **kw))
        self.addCleanup(conn.close)
        return conn

    def interrupted(self, path, exc, wrap=_InterruptedRead, **kw):
        """A fresh server and a connection on which `path` was interrupted
        with `exc`."""
        _, before, act, after = path
        server = self.server()
        conn = self.connect(server, **kw)
        if before is not None:
            before(conn)
        if wrap is _InterruptedRead:
            wrap(conn, exc, after)
        else:
            wrap(conn, exc)
        with self.assertRaises(type(exc)) as cm:
            act(conn)
        self.assertIs(cm.exception, exc, "the interrupt itself, unchanged")
        return server, conn

    def cases(self, paths=None):
        """Every path interrupted by every kind of exception."""
        return [
            (path, make())
            for path in (self.PATHS if paths is None else paths)
            for make in self.INTERRUPTS
        ]

    def test_the_next_statement_reads_its_own_reply(self):
        for path, exc in self.cases():
            with self.subTest(path[0], interrupt=type(exc).__name__):
                server, conn = self.interrupted(path, exc)
                self.assertEqual(
                    conn.execute(SECOND).fetchall(),
                    [("second",)],
                    "the next statement was handed another statement's reply",
                )
                self.assertEqual(conn.execute(FIRST).fetchall(), [("first",)])

    def test_the_connection_is_broken_and_its_socket_closed_at_once(self):
        for path, exc in self.cases():
            with self.subTest(path[0], interrupt=type(exc).__name__):
                server, conn = self.interrupted(path, exc)
                self.assertFalse(conn.is_usable())
                self.assertTrue(server.hung_up(1), "the socket is still open")
                # The usual recovery: the next statement re-dials first.
                self.assertEqual(server.connections, 1)
                self.assertEqual(conn.execute(SECOND).fetchall(), [("second",)])
                self.assertEqual(server.log[-1], (2, SECOND))

    def test_without_automatic_reconnect_the_next_statement_is_refused(self):
        for path in self.PATHS:
            with self.subTest(path[0]):
                server, conn = self.interrupted(path, KeyboardInterrupt(), auto_reconnect=False)
                with self.assertRaises(skaidb.OperationalError) as cm:
                    conn.execute(SECOND)
                self.assertIn("reconnect()", str(cm.exception))
                self.assertNotIn(SECOND, server.statements, "nothing went out on that socket")
                conn.reconnect()
                self.assertEqual(conn.execute(SECOND).fetchall(), [("second",)])

    def test_an_interrupt_while_the_request_is_being_sent(self):
        # Half a request is on the wire: the next one would be read as the
        # rest of it.
        sending = [p for p in self.PATHS if p[0] in ("a query", "opening a stream")]
        for path, exc in self.cases(sending):
            with self.subTest(path[0], interrupt=type(exc).__name__):
                server, conn = self.interrupted(path, exc, wrap=_InterruptedSend)
                self.assertFalse(conn.is_usable())
                self.assertTrue(server.hung_up(1), "the socket is still open")
                self.assertEqual(conn.execute(SECOND).fetchall(), [("second",)])
                self.assertEqual(server.log, [(2, SECOND)])

    def test_a_stream_header_that_cannot_be_parsed_costs_the_connection(self):
        # The chunks behind the header are on their way and no stream goes
        # out to read them.
        truncated = bytes([skaidb._RESP_ROWS_HEADER]) + struct.pack("<I", 3)
        server = self.server()
        server.always(SCAN, [truncated, _f_chunk([(1,)]), _f_end()])
        conn = self.connect(server)
        with self.assertRaises(skaidb.InterfaceError):
            conn.stream(SCAN)
        self.assertFalse(conn.is_usable())
        self.assertTrue(server.hung_up(1), "the socket is still open")
        self.assertEqual(conn.execute(SECOND).fetchall(), [("second",)])

    def test_an_interrupt_while_a_stream_header_is_parsed(self):
        server = self.server()
        conn = self.connect(server)
        stop = KeyboardInterrupt()
        with mock.patch.object(skaidb._Reader, "text", side_effect=stop):
            with self.assertRaises(KeyboardInterrupt) as cm:
                conn.stream(SCAN)
        self.assertIs(cm.exception, stop)
        self.assertFalse(conn.is_usable())
        self.assertTrue(server.hung_up(1), "the socket is still open")
        self.assertEqual(conn.execute(SECOND).fetchall(), [("second",)])

    def test_an_interrupt_while_a_chunk_is_decoded_ends_the_stream(self):
        # The frame was read whole and its rows are lost: going on would
        # skip them without a word.
        server = self.server()
        conn = self.connect(server)
        rows = conn.stream(SCAN)
        stop = KeyboardInterrupt()
        with mock.patch.object(skaidb, "_decode_value", side_effect=stop):
            with self.assertRaises(KeyboardInterrupt) as cm:
                next(rows)
        self.assertIs(cm.exception, stop)
        self.assertEqual(list(rows), [], "the stream is over, not one chunk short")
        self.assertFalse(conn.is_usable())
        self.assertTrue(server.hung_up(1), "the socket is still open")
        rows.close()
        self.assertEqual(conn.execute(SECOND).fetchall(), [("second",)])

    def test_a_statement_error_still_costs_nothing(self):
        # The line this must not cross: a server Error is a whole reply.
        server = self.server()
        server.on(FIRST, bytes([skaidb._RESP_ERROR]) + struct.pack("<I", 4) + b"nope")
        server.on(SCAN, bytes([skaidb._RESP_ERROR]) + struct.pack("<I", 4) + b"nope")
        server.on(SCAN, _f_mutation(3))
        conn = self.connect(server)
        for run in (lambda: conn.execute(FIRST), lambda: conn.stream(SCAN)):
            with self.assertRaises(skaidb.ProgrammingError):
                run()
            self.assertTrue(conn.is_usable())
        self.assertEqual(conn.stream(SCAN).affected, 3)
        self.assertEqual(conn.execute(SECOND).fetchall(), [("second",)])
        self.assertEqual(server.connections, 1)

    @unittest.skipUnless(
        hasattr(signal, "setitimer") and threading.current_thread() is threading.main_thread(),
        "needs SIGALRM delivered to the thread running the test",
    )
    def test_a_signal_handler_that_raises_into_a_blocked_read(self):
        # The real thing, end to end: the server answers late, the alarm
        # goes off while the statement waits, and the late reply must not
        # answer the next statement.
        server = self.server()
        server.on(FIRST, Late(_rows(["v"], [("first",)]), 0.5))
        conn = self.connect(server)

        def on_alarm(signum, frame):
            raise _Alarm("deadline")

        previous = signal.signal(signal.SIGALRM, on_alarm)
        try:
            signal.setitimer(signal.ITIMER_REAL, 0.1)
            with self.assertRaises(_Alarm):
                conn.execute(FIRST)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)
        self.assertEqual(
            conn.execute(SECOND).fetchall(),
            [("second",)],
            "the next statement was handed the late reply",
        )


def _interrupted_dial(exc, after):
    """Patch the driver so that the read after `after` more frames raises
    `exc`. A dial reads the AuthChallenge, the AuthOutcome, the Hello
    acknowledgement and then the reply to `USE`, in that order."""
    real = skaidb.Connection._read_frame
    pending = [exc]

    def read_frame(self):
        nonlocal after
        if pending:
            if after == 0:
                raise pending.pop()
            after -= 1
        return real(self)

    return mock.patch.object(skaidb.Connection, "_read_frame", read_frame)


class UnfinishedDial(unittest.TestCase):
    """A re-dial that does not finish — interrupted, refused its database,
    or with nowhere to connect — leaves the connection broken, with no
    socket: never one that looks usable half way through the handshake,
    with an acknowledgement still unread, or outside its database."""

    USE = 'USE "app"'
    # The frames a dial reads, in order (see `_interrupted_dial`).
    STEPS = ["the challenge", "the outcome", "the Hello acknowledgement", "the reply to USE"]

    def connected(self):
        """A fake server and a connection to it, in database `app`."""
        server = Server()
        self.addCleanup(server.close)
        server.always(SECOND, _rows(["v"], [("second",)]))
        server.always(self.USE, bytes([skaidb._RESP_DDL]))
        conn = skaidb.connect(**dict(_credentials(server), database="app"))
        self.addCleanup(conn.close)
        return server, conn

    def test_an_interrupted_reconnect_does_not_leave_a_half_open_connection(self):
        for after, step in enumerate(self.STEPS):
            for make in Interrupted.INTERRUPTS:
                exc = make()
                with self.subTest(step, interrupt=type(exc).__name__):
                    server, conn = self.connected()
                    with _interrupted_dial(exc, after):
                        with self.assertRaises(type(exc)) as cm:
                            conn.reconnect()
                    self.assertIs(cm.exception, exc)
                    self.assertFalse(conn.is_usable())
                    self.assertTrue(server.hung_up(2), "the socket is still open")
                    # The next statement dials again, and reads its own reply.
                    self.assertEqual(conn.execute(SECOND).fetchall(), [("second",)])
                    self.assertEqual(server.log[-2:], [(3, self.USE), (3, SECOND)])

    def test_an_interrupted_automatic_reconnect_is_tried_again(self):
        # The same when the re-dial is the driver's own: a connection lost
        # under a statement, and the dial that follows interrupted.
        for after, step in enumerate(self.STEPS):
            with self.subTest(step):
                server, conn = self.connected()
                server.on(SECOND, DROP)
                stop = KeyboardInterrupt()
                with _interrupted_dial(stop, after + 1):  # + the reply that was lost
                    with self.assertRaises(KeyboardInterrupt) as cm:
                        conn.execute(SECOND)
                self.assertIs(cm.exception, stop)
                self.assertFalse(conn.is_usable())
                self.assertTrue(server.hung_up(2), "the socket is still open")
                self.assertEqual(conn.execute(SECOND).fetchall(), [("second",)])
                self.assertEqual(server.log[-2:], [(3, self.USE), (3, SECOND)])

    def test_a_reconnect_refused_its_database_is_not_left_in_another(self):
        server, conn = self.connected()
        server.on(self.USE, _f_error('database "app" does not exist'))
        with self.assertRaises(skaidb.ProgrammingError) as cm:
            conn.reconnect()
        self.assertIn("does not exist", str(cm.exception))
        self.assertEqual(server.connections, 2)
        # Authenticated, but in the server's default database: a statement
        # sent there would read or write the wrong tables.
        self.assertFalse(conn.is_usable())
        self.assertTrue(server.hung_up(2), "the socket is still open")
        self.assertEqual(conn.execute(SECOND).fetchall(), [("second",)])
        self.assertEqual(server.log[-2:], [(3, self.USE), (3, SECOND)])

    def test_a_reconnect_with_nowhere_to_connect_leaves_the_connection_broken(self):
        server, conn = self.connected()
        conn._endpoints = [("127.0.0.1", _closed_port())]
        with self.assertRaises(skaidb.OperationalError):
            conn.reconnect()
        self.assertFalse(conn.is_usable(), "no socket, yet usable")
        conn._endpoints = [("127.0.0.1", server.port)]
        self.assertEqual(conn.execute(SECOND).fetchall(), [("second",)])


class BrokenClosesTheSocket(unittest.TestCase):
    """A connection marked broken closes its socket at once — not at the
    next statement and not at `close()`. Until the socket closes the server
    keeps the session, a transaction open in it and the rest of a stream it
    is still sending."""

    # What the server answers the statement with, what the client does, and
    # what that raises (None: nothing — close() reports through is_usable()).
    CASES = [
        (
            "a read timeout",
            STALL,
            lambda conn: _impatient(conn).execute(SCAN),
            skaidb.OperationalError,
        ),
        (
            "a read timeout opening a stream",
            STALL,
            lambda conn: _impatient(conn).stream(SCAN),
            skaidb.OperationalError,
        ),
        (
            "a read timeout mid-stream",
            [_HEADER, _CHUNK],
            lambda conn: list(_impatient(conn).stream(SCAN)),
            skaidb.OperationalError,
        ),
        (
            "an unknown frame mid-stream",
            [_HEADER, _UNKNOWN],
            lambda conn: list(conn.stream(SCAN)),
            skaidb.InterfaceError,
        ),
        (
            "an unknown reply to a stream request",
            _UNKNOWN,
            lambda conn: conn.stream(SCAN),
            skaidb.InterfaceError,
        ),
        ("an abandoned stream too long to drain", _TOO_LONG, _abandon, None),
        ("an unknown frame while draining", [_HEADER, _CHUNK, _UNKNOWN], _abandon, None),
        ("a read timeout while draining", [_HEADER, _CHUNK], _abandon_impatient, None),
    ]

    def test_the_server_sees_the_client_hang_up_without_another_call(self):
        for name, reply, act, raises in self.CASES:
            with self.subTest(name):
                server = Server()
                self.addCleanup(server.close)
                server.always(SCAN, reply)
                # No automatic reconnect: a re-dial would close the old
                # socket on its way and hide what is under test.
                conn = skaidb.connect(**dict(_credentials(server), auto_reconnect=False))
                self.addCleanup(conn.close)
                if raises is None:
                    act(conn)
                else:
                    with self.assertRaises(raises):
                        act(conn)
                self.assertFalse(conn.is_usable())
                self.assertTrue(server.hung_up(1), "the socket is still open")
                # The connection refuses work until it is re-dialed …
                with self.assertRaises(skaidb.OperationalError) as cm:
                    conn.execute("SELECT 1")
                self.assertIn("reconnect()", str(cm.exception))
                self.assertEqual(server.connections, 1)
                # … and then carries statements again.
                conn.reconnect()
                self.assertEqual(conn.execute("SELECT 1").fetchall(), [(1,)])


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
        del old  # no close(): dropping the last reference runs __del__ …
        gc.collect()  # … at once on CPython, here on an interpreter without refcounts
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
