"""Streaming (`OP_QUERY_STREAM`) tests for the skaidb driver.

Pure standard library (`unittest`) — run with
`python -m unittest tests.test_stream` (or `python -m pytest tests`) from
`drivers/python`. No server required: a fake socket replays canned frames, so
the real framing, drain and connection-state code runs.

The rule under test is PROTOCOL.md §3: the connection is busy for the whole
stream, and a client that abandons one early must drain the remaining frames
or stop using the connection.
"""

import struct
import threading
import unittest

import skaidb
from skaidb import _Reader, _encode_value


# ---- fake transport -------------------------------------------------------


class _FakeFile:
    """Stands in for the socket's read file: a queue of length-prefixed
    frames. A short read returns b"" the way a closed socket would."""

    def __init__(self):
        self.buf = bytearray()

    def feed(self, payload: bytes) -> None:
        self.buf += struct.pack(">I", len(payload)) + payload

    def read(self, n: int) -> bytes:
        if len(self.buf) < n:
            self.buf.clear()
            return b""
        out = bytes(self.buf[:n])
        del self.buf[:n]
        return out

    def close(self):
        pass


class _FakeSock:
    def __init__(self):
        self.sent = bytearray()

    def sendall(self, data: bytes) -> None:
        self.sent += data

    def close(self):
        pass


class _StreamConn(skaidb.Connection):
    """A Connection wired to the fake transport instead of a socket."""

    def __init__(self):  # bypass connect + handshake
        self._lock = threading.Lock()
        self.closed = False
        self._broken = False
        self._streaming = False
        self._prepared = {}
        self._consistency = skaidb.Consistency.QUORUM
        self._sock = _FakeSock()
        self._file = _FakeFile()

    def feed(self, *payloads: bytes) -> "_StreamConn":
        for p in payloads:
            self._file.feed(p)
        return self

    @property
    def unread(self) -> int:
        return len(self._file.buf)


# ---- frame builders (§3 of PROTOCOL.md) -----------------------------------


def _f_header(columns) -> bytes:
    out = bytearray([skaidb._RESP_ROWS_HEADER])
    out += struct.pack("<I", len(columns))
    for c in columns:
        b = c.encode()
        out += struct.pack("<I", len(b)) + b
    return bytes(out)


def _f_chunk(rows) -> bytes:
    out = bytearray([skaidb._RESP_ROWS_CHUNK])
    out += struct.pack("<I", len(rows))
    for row in rows:
        out += struct.pack("<I", len(row))
        for cell in row:
            vb = _encode_value(cell)
            out += struct.pack("<I", len(vb)) + vb
    return bytes(out)


def _f_end() -> bytes:
    return bytes([skaidb._RESP_ROWS_END])


def _f_error(msg: str) -> bytes:
    b = msg.encode()
    return bytes([skaidb._RESP_ERROR]) + struct.pack("<I", len(b)) + b


def _f_mutation(affected: int) -> bytes:
    return bytes([skaidb._RESP_MUTATION]) + struct.pack("<Q", affected)


def _f_result_sets(*sets) -> bytes:
    """ResultSets (a CALL whose body EMITted): the set count, then each set
    laid out as a Rows body — the columns, then the rows."""
    out = bytearray([skaidb._RESP_RESULT_SETS]) + struct.pack("<I", len(sets))
    for columns, rows in sets:
        out += _f_header(columns)[1:]           # u32 ncols + names, as in the header
        out += _f_chunk(rows)[1:]               # u32 nrows + rows, as in a chunk
    return bytes(out)


def _sent_ops(conn) -> list:
    """The opcode of every request frame the fake socket received."""
    sent, ops = bytes(conn._sock.sent), []
    while sent:
        (n,) = struct.unpack(">I", sent[:4])
        ops.append(sent[4])
        sent = sent[4 + n :]
    return ops


class Streaming(unittest.TestCase):
    def test_rows_columns_and_release(self):
        conn = _StreamConn().feed(
            _f_header(["id", "name"]),
            _f_chunk([(1, "ada"), (2, "bob")]),
            _f_chunk([(3, "cyd")]),
            _f_end(),
        )
        rows = conn.stream("SELECT id, name FROM t")
        self.assertEqual(rows.columns, ["id", "name"])
        self.assertEqual(list(rows), [(1, "ada"), (2, "bob"), (3, "cyd")])
        # Exhausting the stream hands the connection back.
        self.assertFalse(conn._streaming)
        self.assertTrue(conn.is_usable())

    def test_consistency_name_is_resolved(self):
        conn = _StreamConn().feed(_f_header([]), _f_end())
        list(conn.stream("SELECT 1", consistency="ONE"))
        # Request frame: u32 length | opcode | consistency byte.
        self.assertEqual(conn._sock.sent[4], skaidb._OP_QUERY_STREAM)
        self.assertEqual(conn._sock.sent[5], skaidb.Consistency.ONE)

    def test_params_travel_in_the_parameterised_opcode(self):
        conn = _StreamConn().feed(_f_header(["id"]), _f_chunk([(7,)]), _f_end())
        rows = list(conn.stream("SELECT id FROM t WHERE ts >= ? AND tag = ?", (5, "a")))
        self.assertEqual(rows, [(7,)])
        sent = bytes(conn._sock.sent[4:])
        self.assertEqual(sent[0], skaidb._OP_QUERY_STREAM_PARAMS)
        self.assertEqual(sent[1], skaidb.Consistency.QUORUM)
        sql = b"SELECT id FROM t WHERE ts >= ? AND tag = ?"
        self.assertEqual(sent[2:6], struct.pack("<I", len(sql)))
        self.assertEqual(sent[6 : 6 + len(sql)], sql)
        tail = sent[6 + len(sql) :]
        expect = struct.pack("<H", 2)
        for v in (5, "a"):
            vb = _encode_value(v)
            expect += struct.pack("<I", len(vb)) + vb
        self.assertEqual(tail, expect, "the same parameter vector OP_EXECUTE carries")

    def test_no_params_keep_the_original_opcode(self):
        conn = _StreamConn().feed(_f_header([]), _f_end())
        list(conn.stream("SELECT 1", []))
        self.assertEqual(conn._sock.sent[4], skaidb._OP_QUERY_STREAM)

    def test_positional_consistency_still_means_consistency(self):
        conn = _StreamConn().feed(_f_header([]), _f_end())
        list(conn.stream("SELECT 1", "ONE"))
        self.assertEqual(conn._sock.sent[4], skaidb._OP_QUERY_STREAM)
        self.assertEqual(conn._sock.sent[5], skaidb.Consistency.ONE)

    def test_an_old_server_refusing_parameterised_streams_says_so(self):
        conn = _StreamConn().feed(_f_error("protocol error: malformed message: unknown opcode"))
        with self.assertRaises(skaidb.ProgrammingError) as cm:
            conn.stream("SELECT id FROM t WHERE id > ?", (1,))
        self.assertIn("parameterised streaming", str(cm.exception))
        self.assertTrue(conn.is_usable(), "a refused request leaves the connection usable")

    def test_abandoned_stream_is_drained_and_connection_resyncs(self):
        conn = _StreamConn().feed(
            _f_header(["id"]),
            _f_chunk([(1,)]),
            _f_chunk([(2,)]),
            _f_chunk([(3,)]),
            _f_end(),
        )
        with conn.stream("SELECT id FROM big") as rows:
            for _ in rows:
                break  # caller stops early
        self.assertEqual(conn.unread, 0, "abandoned stream left frames on the socket")
        self.assertTrue(conn.is_usable())

        # The real symptom of a missed drain: the next statement reads a
        # leftover RowsChunk and reports "unknown response tag 6".
        conn.feed(_f_mutation(1))
        self.assertEqual(conn._query("INSERT INTO t VALUES (9)", 1)[:2], ("mutation", 1))

    def test_garbage_collected_stream_is_drained(self):
        conn = _StreamConn().feed(
            _f_header(["id"]), _f_chunk([(1,)]), _f_chunk([(2,)]), _f_end()
        )
        rows = conn.stream("SELECT id FROM big")
        next(iter(rows))
        del rows  # no close(), no context manager: refcount drop only
        self.assertEqual(conn.unread, 0)
        self.assertFalse(conn._streaming)
        self.assertTrue(conn.is_usable())

    def test_undrainable_stream_marks_connection_broken(self):
        # More frames outstanding than the drain budget: discarding the
        # connection beats reading megabytes nobody wants.
        conn = _StreamConn()
        conn.feed(_f_header(["id"]))
        for i in range(skaidb._STREAM_DRAIN_MAX_FRAMES + 5):
            conn.feed(_f_chunk([(i,)]))
        conn.feed(_f_end())

        rows = conn.stream("SELECT id FROM huge")
        next(iter(rows))
        rows.close()
        self.assertTrue(conn._broken)
        self.assertFalse(conn.is_usable(), "a desynced connection must fail the health check")

        # And it refuses work rather than reading someone else's frames.
        with self.assertRaises(skaidb.OperationalError) as ctx:
            conn._query("SELECT 1", 1)
        self.assertIn("reconnect", str(ctx.exception))

    def test_broken_connection_fails_the_pool_health_check(self):
        conn = _StreamConn()
        conn.feed(_f_header(["id"]))
        for i in range(skaidb._STREAM_DRAIN_MAX_FRAMES + 2):
            conn.feed(_f_chunk([(i,)]))
        rows = conn.stream("SELECT id FROM huge")
        next(iter(rows))
        rows.close()

        p = skaidb.ConnectionPool(maxsize=2)
        p.putconn(conn)
        self.assertEqual(p._idle, [], "a poisoned connection was pooled for reuse")
        self.assertTrue(conn.closed)

    def test_open_stream_fails_the_pool_health_check(self):
        # Returning a connection to the pool mid-stream is the same poisoning
        # path: chunks are still in flight for the next caller to trip over.
        conn = _StreamConn().feed(_f_header(["id"]), _f_chunk([(1,)]), _f_end())
        rows = conn.stream("SELECT id FROM t")
        next(iter(rows))
        self.assertFalse(conn.is_usable())
        p = skaidb.ConnectionPool(maxsize=2)
        p.putconn(conn)
        self.assertEqual(p._idle, [])
        rows.close()

    def test_concurrent_statement_is_refused_not_interleaved(self):
        conn = _StreamConn().feed(_f_header(["id"]), _f_chunk([(1,)]), _f_end())
        rows = conn.stream("SELECT id FROM t")
        next(iter(rows))
        sent_before = bytes(conn._sock.sent)

        # Same thread (a statement run from inside the loop) …
        with self.assertRaises(skaidb.ProgrammingError) as ctx:
            conn._query("SELECT 2", 1)
        self.assertIn("streaming", str(ctx.exception))

        # … and another thread.
        failure = []

        def other():
            try:
                conn._query("SELECT 3", 1)
            except skaidb.Error as e:
                failure.append(e)

        t = threading.Thread(target=other)
        t.start()
        t.join(5)
        self.assertEqual(len(failure), 1)
        self.assertIsInstance(failure[0], skaidb.ProgrammingError)
        # Nothing was written into the middle of the stream.
        self.assertEqual(bytes(conn._sock.sent), sent_before)

        rows.close()
        self.assertTrue(conn.is_usable())  # usable again once the stream ends

    def test_error_frame_midstream_keeps_connection_usable(self):
        conn = _StreamConn().feed(
            _f_header(["id"]), _f_chunk([(1,)]), _f_error("scan budget exceeded")
        )
        rows = conn.stream("SELECT id FROM t")
        it = iter(rows)
        self.assertEqual(next(it), (1,))
        with self.assertRaises(skaidb.ProgrammingError):
            next(it)
        # An Error frame ends the exchange, so the socket is at a request
        # boundary: the connection survives.
        self.assertFalse(conn._broken)
        self.assertTrue(conn.is_usable())

    def test_dead_socket_midstream_breaks_connection(self):
        conn = _StreamConn().feed(_f_header(["id"]), _f_chunk([(1,)]))  # no end
        rows = conn.stream("SELECT id FROM t")
        with self.assertRaises(skaidb.OperationalError):
            list(rows)
        self.assertTrue(conn._broken)
        self.assertFalse(conn.is_usable())

    def test_unknown_frame_breaks_connection(self):
        conn = _StreamConn().feed(_f_header(["id"]), bytes([99]), _f_end())
        rows = conn.stream("SELECT id FROM t")
        with self.assertRaises(skaidb.InterfaceError):
            list(rows)
        self.assertTrue(conn._broken)

    def test_non_row_statement_yields_nothing_and_releases(self):
        conn = _StreamConn().feed(_f_mutation(3))
        rows = conn.stream("INSERT INTO t VALUES (1)")
        self.assertEqual(list(rows), [])
        self.assertEqual(rows.affected, 3)
        self.assertEqual(rows.columns, [])
        self.assertTrue(conn.is_usable())

    def test_streamed_call_returns_its_result_sets(self):
        # A CALL whose body EMITs answers with ONE ResultSets frame, streamed
        # or not: every set, in order, and nothing left on the socket.
        sets = _f_result_sets((["a"], [("x",), ("y",)]), (["b"], [("z",)]))
        conn = _StreamConn().feed(sets, sets, _f_header(["n"]), _f_chunk([(1,)]), _f_end())
        for sql, params in (("CALL report(?)", (5,)), ("CALL report()", None)):
            rows = conn.stream(sql, params)
            self.assertTrue(conn.is_usable(), "the whole reply is in one frame: the stream holds nothing")
            got = []
            while True:
                got.append("".join(rows.columns) + ":" + "".join(r[0] for r in rows))
                if not rows.nextset():
                    break
            self.assertEqual("|".join(got), "a:xy|b:z", sql)
            self.assertIsNone(rows.nextset())
            rows.close()
        self.assertEqual(_sent_ops(conn), [skaidb._OP_QUERY_STREAM_PARAMS, skaidb._OP_QUERY_STREAM])
        # The next statement on the same connection reads its own reply.
        rows = conn.stream("SELECT n FROM t")
        self.assertEqual(list(rows), [(1,)])
        self.assertIsNone(rows.nextset(), "a real stream has one set")
        self.assertEqual(conn.unread, 0)
        self.assertTrue(conn.is_usable())

    def test_statement_error_releases_the_connection(self):
        conn = _StreamConn().feed(_f_error("no such table t"))
        with self.assertRaises(skaidb.ProgrammingError):
            conn.stream("SELECT id FROM t")
        self.assertFalse(conn._streaming)
        self.assertTrue(conn.is_usable())

    def test_old_server_reports_missing_opcode(self):
        conn = _StreamConn().feed(_f_error("unknown opcode 5"))
        with self.assertRaises(skaidb.ProgrammingError) as ctx:
            conn.stream("SELECT 1")
        self.assertIn("does not support streaming", str(ctx.exception))
        self.assertTrue(conn.is_usable())

    def test_close_is_idempotent_and_safe_after_exhaustion(self):
        conn = _StreamConn().feed(_f_header(["id"]), _f_chunk([(1,)]), _f_end())
        rows = conn.stream("SELECT id FROM t")
        self.assertEqual(list(rows), [(1,)])
        rows.close()
        rows.close()
        self.assertTrue(conn.is_usable())

    def test_close_after_connection_close_does_not_read(self):
        conn = _StreamConn().feed(_f_header(["id"]), _f_chunk([(1,)]), _f_end())
        rows = conn.stream("SELECT id FROM t")
        next(iter(rows))
        conn.close()
        rows.close()  # must not touch the dropped socket
        self.assertTrue(conn.closed)


if __name__ == "__main__":
    unittest.main()
