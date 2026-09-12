"""skaidb — Python driver (DB-API 2.0 / PEP 249).

Pure standard library: ``socket``, ``ssl``, ``hashlib``, ``hmac``. No third-party deps.

Quick start::

    import skaidb

    conn = skaidb.connect(host="localhost", port=7000,
                          user="skaidb", password="secret")
    cur = conn.cursor()
    cur.execute("CREATE TABLE users (PRIMARY KEY (id))")
    cur.execute("INSERT INTO users (id, name) VALUES (?, ?)", (1, "Ada"))
    cur.execute("SELECT id, name FROM users WHERE id = ?", (1,))
    print(cur.fetchall())          # [(1, 'Ada')]
    conn.close()

The API mirrors ``sqlite3``/``psycopg2`` so there is almost nothing new to
learn. Placeholders use the ``qmark`` style (``?``), exactly like ``sqlite3``.
"""

from __future__ import annotations

# Reported in the server's `drivers` table via Hello; keep in sync with
# pyproject.toml.
__version__ = "0.1.0"

import datetime as _dt
import decimal
import hashlib
import hmac
import random
import socket
import ssl
import struct
import threading
import uuid
from contextlib import contextmanager
from typing import Any, Iterable, List, Optional, Sequence, Tuple

__all__ = [
    "connect",
    "Connection",
    "ConnectionPool",
    "pool",
    "Cursor",
    "RowStream",
    "Error",
    "DatabaseError",
    "OperationalError",
    "ProgrammingError",
    "Consistency",
    "apilevel",
    "threadsafety",
    "paramstyle",
]

# ---- DB-API 2.0 module globals -------------------------------------------

apilevel = "2.0"
threadsafety = 1  # threads may share the module, not connections
paramstyle = "qmark"  # uses '?' placeholders, like sqlite3

_UTC = _dt.timezone.utc
_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_UTC)


# ---- Exceptions (PEP 249 hierarchy) --------------------------------------


class Error(Exception):
    """Base class for all skaidb errors."""


class InterfaceError(Error):
    """Error related to the driver rather than the database."""


class DatabaseError(Error):
    """Error reported by the database."""


class OperationalError(DatabaseError):
    """Connection / transport problem."""


class ProgrammingError(DatabaseError):
    """A statement failed (bad SQL, constraint, etc.)."""


# ---- Consistency levels ---------------------------------------------------


class Consistency:
    ONE = 0
    QUORUM = 1
    ALL = 2

    _BY_NAME = {"ONE": 0, "QUORUM": 1, "ALL": 2}

    @classmethod
    def resolve(cls, value: "int | str") -> int:
        if isinstance(value, int):
            if value not in (0, 1, 2):
                raise ValueError(f"invalid consistency {value!r}")
            return value
        try:
            return cls._BY_NAME[value.upper()]
        except (AttributeError, KeyError):
            raise ValueError(f"invalid consistency {value!r}") from None


# ---- Value codec (§4 of PROTOCOL.md) --------------------------------------

_TAG_NULL = 0
_TAG_BOOL = 1
_TAG_INT = 2
_TAG_FLOAT = 3
_TAG_DECIMAL = 4
_TAG_STRING = 5
_TAG_BYTES = 6
_TAG_UUID = 7
_TAG_TIMESTAMP = 8
_TAG_ARRAY = 9
_TAG_DOCUMENT = 10


class _Reader:
    __slots__ = ("buf", "pos")

    def __init__(self, buf: bytes):
        self.buf = buf
        self.pos = 0

    def take(self, n: int) -> bytes:
        end = self.pos + n
        if end > len(self.buf):
            raise InterfaceError("truncated server message")
        s = self.buf[self.pos:end]
        self.pos = end
        return s

    def u8(self) -> int:
        return self.take(1)[0]

    def u16(self) -> int:
        return struct.unpack_from("<H", self.take(2))[0]

    def u32(self) -> int:
        return struct.unpack_from("<I", self.take(4))[0]

    def i64(self) -> int:
        return struct.unpack_from("<q", self.take(8))[0]

    def u64(self) -> int:
        return struct.unpack_from("<Q", self.take(8))[0]

    def blob(self) -> bytes:
        return self.take(self.u32())

    def text(self) -> str:
        return self.blob().decode("utf-8")


def _decode_value(r: _Reader) -> Any:
    tag = r.u8()
    if tag == _TAG_NULL:
        return None
    if tag == _TAG_BOOL:
        return r.u8() != 0
    if tag == _TAG_INT:
        return r.i64()
    if tag == _TAG_FLOAT:
        return struct.unpack_from("<d", r.take(8))[0]
    if tag == _TAG_DECIMAL:
        mantissa = int.from_bytes(r.take(16), "little", signed=True)
        scale = r.u32()
        return decimal.Decimal(mantissa).scaleb(-scale)
    if tag == _TAG_STRING:
        return r.text()
    if tag == _TAG_BYTES:
        return r.blob()
    if tag == _TAG_UUID:
        return uuid.UUID(bytes=r.take(16))
    if tag == _TAG_TIMESTAMP:
        return _EPOCH + _dt.timedelta(milliseconds=r.i64())
    if tag == _TAG_ARRAY:
        return [_decode_value(r) for _ in range(r.u32())]
    if tag == _TAG_DOCUMENT:
        out = {}
        for _ in range(r.u32()):
            key = r.text()
            out[key] = _decode_value(r)
        return out
    raise InterfaceError(f"unknown value tag {tag}")


def _decimal_parts(d: decimal.Decimal) -> "tuple[int, int]":
    """A `Decimal` as `(mantissa: i128, scale: u32)` with `value = mantissa *
    10**-scale`, matching skaidb's `Decimal` codec. `scale` is unsigned, so a
    positive exponent is folded into the mantissa."""
    sign, digits, exponent = d.as_tuple()
    if not isinstance(exponent, int):  # 'n'/'N' (NaN) or 'F' (Infinity)
        raise ProgrammingError("cannot bind non-finite Decimal")
    mantissa = 0
    for dig in digits:
        mantissa = mantissa * 10 + dig
    if sign:
        mantissa = -mantissa
    if exponent <= 0:
        scale = -exponent
    else:
        mantissa *= 10**exponent
        scale = 0
    if not -(2**127) <= mantissa < 2**127:
        raise ProgrammingError("Decimal is too large to bind (mantissa exceeds i128)")
    return mantissa, scale


def _encode_value_into(v: Any, out: bytearray) -> None:
    """Encode `v` as a typed skaidb value (tag + payload), the inverse of
    `_decode_value`. Lists/tuples become `Array`, dicts become `Document` —
    the whole point of prepared-statement binding: arrays and nested documents
    that have no SQL literal form travel as typed values, not interpolated
    text. `bool` is checked before `int` (it is an `int` subclass)."""
    if v is None:
        out.append(_TAG_NULL)
    elif isinstance(v, bool):
        out.append(_TAG_BOOL)
        out.append(1 if v else 0)
    elif isinstance(v, int):
        out.append(_TAG_INT)
        out += struct.pack("<q", v)
    elif isinstance(v, float):
        if v != v or v in (float("inf"), float("-inf")):
            raise ProgrammingError("cannot bind NaN/Infinity")
        out.append(_TAG_FLOAT)
        out += struct.pack("<d", v)
    elif isinstance(v, decimal.Decimal):
        mantissa, scale = _decimal_parts(v)
        out.append(_TAG_DECIMAL)
        out += mantissa.to_bytes(16, "little", signed=True)
        out += struct.pack("<I", scale)
    elif isinstance(v, str):
        b = v.encode("utf-8")
        out.append(_TAG_STRING)
        out += struct.pack("<I", len(b))
        out += b
    elif isinstance(v, (bytes, bytearray)):
        b = bytes(v)
        out.append(_TAG_BYTES)
        out += struct.pack("<I", len(b))
        out += b
    elif isinstance(v, uuid.UUID):
        out.append(_TAG_UUID)
        out += v.bytes
    elif isinstance(v, _dt.datetime):
        ms = int((v.astimezone(_UTC) - _EPOCH).total_seconds() * 1000)
        out.append(_TAG_TIMESTAMP)
        out += struct.pack("<q", ms)
    elif isinstance(v, (list, tuple)):
        out.append(_TAG_ARRAY)
        out += struct.pack("<I", len(v))
        for item in v:
            _encode_value_into(item, out)
    elif isinstance(v, dict):
        out.append(_TAG_DOCUMENT)
        out += struct.pack("<I", len(v))
        for k, val in v.items():
            if not isinstance(k, str):
                raise ProgrammingError("document keys must be strings")
            kb = k.encode("utf-8")
            out += struct.pack("<I", len(kb))
            out += kb
            _encode_value_into(val, out)
    else:
        raise ProgrammingError(f"cannot bind value of type {type(v).__name__}")


def _encode_value(v: Any) -> bytes:
    out = bytearray()
    _encode_value_into(v, out)
    return bytes(out)


# ---- Wire opcodes & response tags -----------------------------------------

_OP_QUERY = 1
_OP_PREPARE = 2
_OP_EXECUTE = 3
_OP_CLOSE = 4
_OP_EXECUTE_BATCH = 7
_OP_QUERY_STREAM = 5
_OP_HELLO = 8

_RESP_ROWS = 0
_RESP_MUTATION = 1
_RESP_DDL = 2
_RESP_ERROR = 3
_RESP_PREPARED = 4
_RESP_ROWS_HEADER = 5
_RESP_ROWS_CHUNK = 6
_RESP_ROWS_END = 7
_RESP_RESULT_SETS = 8

# Keep the per-connection prepared-statement cache under the server's
# MAX_PREPARED_PER_CONN (256): statements beyond this are prepared, executed,
# and immediately closed rather than retained.
_MAX_PREPARED_CACHE = 240

# Frames an abandoned stream will read and discard before giving up and
# marking the connection broken. Draining is much cheaper than a reconnect
# for the tail of a small result; past this the remainder is big enough that
# discarding the connection is the better trade (PROTOCOL.md §3: a client
# that abandons a stream early must drain or close).
_STREAM_DRAIN_MAX_FRAMES = 64

_BUSY_STREAMING = (
    "connection is busy streaming; finish or close() the RowStream "
    "(or use a second connection) before running another statement"
)


class _Unpreparable(Exception):
    """Internal: the server reported a statement kind that cannot be prepared
    (DDL / session control). The caller falls back to client-side text binding."""


# ---- Client-side parameter binding (§5) -----------------------------------


def _quote(arg: Any) -> str:
    if arg is None:
        return "NULL"
    if isinstance(arg, bool):
        return "TRUE" if arg else "FALSE"
    if isinstance(arg, int):
        return str(arg)
    if isinstance(arg, float):
        if arg != arg or arg in (float("inf"), float("-inf")):
            raise ProgrammingError("cannot bind NaN/Infinity")
        return repr(arg)
    if isinstance(arg, decimal.Decimal):
        return str(arg)
    if isinstance(arg, str):
        return "'" + arg.replace("'", "''") + "'"
    if isinstance(arg, (bytes, bytearray)):
        return "'" + bytes(arg).hex() + "'"
    if isinstance(arg, uuid.UUID):
        return "'" + str(arg) + "'"
    if isinstance(arg, _dt.datetime):
        ms = int((arg.astimezone(_UTC) - _EPOCH).total_seconds() * 1000)
        return str(ms)
    raise ProgrammingError(f"cannot bind value of type {type(arg).__name__}")


def _bind(sql: str, params: Optional[Sequence[Any]]) -> str:
    if not params:
        if "?" in _strip_strings(sql):
            raise ProgrammingError("query has placeholders but no parameters given")
        return sql
    out = []
    it = iter(params)
    in_str = False
    i = 0
    n = len(sql)
    used = 0
    while i < n:
        ch = sql[i]
        if in_str:
            out.append(ch)
            if ch == "'":
                # doubled '' stays inside the string
                if i + 1 < n and sql[i + 1] == "'":
                    out.append("'")
                    i += 2
                    continue
                in_str = False
            i += 1
            continue
        if ch == "'":
            in_str = True
            out.append(ch)
            i += 1
            continue
        if ch == "?":
            try:
                out.append(_quote(next(it)))
            except StopIteration:
                raise ProgrammingError("more placeholders than parameters") from None
            used += 1
            i += 1
            continue
        out.append(ch)
        i += 1
    remaining = list(it)
    if remaining:
        raise ProgrammingError("more parameters than placeholders")
    return "".join(out)


def _strip_strings(sql: str) -> str:
    """Return sql with single-quoted literals blanked, for placeholder counting."""
    out = []
    in_str = False
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if in_str:
            if ch == "'":
                if i + 1 < n and sql[i + 1] == "'":
                    i += 2
                    continue
                in_str = False
            i += 1
            continue
        if ch == "'":
            in_str = True
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


# ---- SCRAM-SHA-256 handshake (§2) -----------------------------------------


def _scram_proof(password: str, salt: bytes, iterations: int, auth_message: bytes):
    salted = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations, 32)
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    client_sig = hmac.new(stored_key, auth_message, hashlib.sha256).digest()
    proof = bytes(a ^ b for a, b in zip(client_key, client_sig))
    server_key = hmac.new(salted, b"Server Key", hashlib.sha256).digest()
    server_sig = hmac.new(server_key, auth_message, hashlib.sha256).digest()
    return proof, server_sig


# ---- Connection -----------------------------------------------------------


class Connection:
    """A DB-API 2.0 connection to one skaidb node.

    skaidb is non-transactional (each statement commits on its own), so
    ``commit()`` is an accepted no-op for DB-API conformance; ``rollback()``
    raises, since silently dropping a rollback would be worse than refusing
    it. Use ``BEGIN``/``COMMIT``/``ROLLBACK`` as statements instead.
    """

    _nonce_counter = 0
    _nonce_lock = threading.Lock()
    # Class-level defaults: health state must have an answer even on an
    # instance built without __init__ (a subclass wiring its own transport).
    _broken = False
    _streaming = False

    def __init__(
        self,
        endpoints,
        user,
        password,
        consistency,
        connect_timeout,
        read_timeout,
        database=None,
        tls_ctx=None,
        tls_server_name="skaidb",
    ):
        self._consistency = Consistency.resolve(consistency)
        self._lock = threading.Lock()
        self.closed = False
        # Set once a transport error, or an abandoned stream too long to
        # drain, leaves the socket out of sync; a pool checks `is_usable()`
        # and discards the connection instead of reusing it.
        self._broken = False
        # True while a `RowStream` owns the socket. The stream spans many
        # frames and many `next()` calls, so holding `_lock` for its duration
        # would deadlock the common single-threaded shape (a statement run
        # from inside the `for` loop) and would block, not warn, in the
        # threaded one. A flag instead lets any other statement fail loudly
        # with a clear message — PROTOCOL.md §3: no other request may be sent
        # until RowsEnd or Error.
        self._streaming = False
        # sql text -> (prepared_id, nparams) for this connection. Ids are
        # per-connection slot indices assigned by the server, so the cache is
        # cleared whenever the underlying socket is (re)dialed.
        self._prepared: "dict[str, tuple[int, int]]" = {}
        self._endpoints: List[Tuple[str, int]] = list(endpoints)
        self._user = user
        self._password = password
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._database = database
        self._tls_ctx = tls_ctx
        self._tls_server_name = tls_server_name
        self._sock = None
        self._file = None
        self._open()

    def _open(self) -> None:
        """Dial the endpoints in order until one connects and authenticates,
        then `USE` the database if one was requested. Raises
        ``OperationalError`` if no endpoint is reachable."""
        errors = []
        for host, port in self._endpoints:
            try:
                sock = socket.create_connection(
                    (host, port), timeout=self._connect_timeout
                )
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                # TLS wrap (SNI = the server cert's SAN, default 'skaidb') —
                # the handshake runs here; SCRAM then rides inside TLS.
                if self._tls_ctx is not None:
                    sock = self._tls_ctx.wrap_socket(
                        sock, server_hostname=self._tls_server_name
                    )
                sock.settimeout(self._read_timeout)
                self._sock = sock
                self._file = sock.makefile("rb")
                self._handshake(self._user, self._password)
            except (OSError, OperationalError) as e:
                errors.append(f"{host}:{port}: {e}")
                self._drop_socket()
                continue
            # Connected + authenticated: a fresh socket has no prepared
            # statements, no stream in flight and is not broken.
            self._broken = False
            self._streaming = False
            self._prepared.clear()
            self._send_hello()
            if self._database is not None:
                self._use_database(self._database)
            return
        raise OperationalError("could not connect to any endpoint (" + "; ".join(errors) + ")")

    def _drop_socket(self) -> None:
        for closer in (self._file, self._sock):
            try:
                if closer is not None:
                    closer.close()
            except OSError:
                pass
        self._file = None
        self._sock = None

    def _send_hello(self) -> None:
        """Best-effort self-identification (fills the server's ``drivers``
        table ``client_name``/``client_version``). An old server rejects the
        opcode with an error frame, which is ignored — identity is
        telemetry, never load-bearing."""
        try:
            name = b"python"
            ver = __version__.encode()
            req = bytearray([_OP_HELLO])
            req += struct.pack("<I", len(name)) + name
            req += struct.pack("<I", len(ver)) + ver
            self._write_frame(bytes(req))
            self._read_frame()  # Ddl ack, or an old server's Error — either way, done.
        except (OSError, OperationalError):
            pass

    def _use_database(self, db: str) -> None:
        ident = '"' + db.replace('"', '""') + '"'
        self._query("USE " + ident, self._consistency)

    def reconnect(self) -> None:
        """Re-dial (failing over across endpoints) and re-authenticate,
        discarding any per-connection prepared statements. A pool calls this to
        recover a broken connection."""
        if self.closed:
            raise ProgrammingError("connection is closed")
        self._drop_socket()
        self._open()

    def is_usable(self) -> bool:
        """True if the connection can be handed to a new caller — not closed,
        not left out of sync by a transport error or an undrainable abandoned
        stream, and not currently mid-stream. Cheap (no round-trip).

        The mid-stream case matters because this doubles as a pool's health
        check: a connection returned to the pool while a `RowStream` is still
        live has chunks in flight, and the next caller would read them as the
        answer to its own statement."""
        return not self.closed and not self._broken and not self._streaming

    def ping(self) -> bool:
        """Round-trip liveness check; returns False (and marks the connection
        broken) on any transport failure."""
        if not self.is_usable():
            return False
        try:
            self._query("SHOW DATABASES", self._consistency)
            return True
        except (OperationalError, ProgrammingError, InterfaceError):
            return False

    # -- framing --
    def _write_frame(self, payload: bytes) -> None:
        if self._sock is None:
            raise OperationalError("connection is not open")
        self._sock.sendall(struct.pack(">I", len(payload)) + payload)

    def _read_frame(self) -> bytes:
        head = self._read_exact(4)
        (length,) = struct.unpack(">I", head)
        return self._read_exact(length)

    def _read_exact(self, n: int) -> bytes:
        # `close()` can drop the socket while a stream still holds a
        # reference to this connection, so the file may be gone.
        if self._file is None:
            raise OperationalError("connection is not open")
        buf = self._file.read(n)
        if buf is None or len(buf) != n:
            raise OperationalError("connection closed by server")
        return buf

    # -- handshake --
    def _handshake(self, user: str, password: str) -> None:
        with Connection._nonce_lock:
            Connection._nonce_counter += 1
            counter = Connection._nonce_counter
        client_nonce = f"py{id(self)}.{counter}"

        start = bytes([10]) + _enc_str(user) + _enc_str(client_nonce)
        self._write_frame(start)

        r = _Reader(self._read_frame())
        if r.u8() != 11:
            raise OperationalError("bad handshake challenge")
        salt = r.blob()
        iterations = r.u32()
        server_nonce = r.text()

        salt_hex = salt.hex()
        auth_message = "\0".join(
            [user, client_nonce, server_nonce, salt_hex, str(iterations)]
        ).encode("utf-8")
        proof, expected_server_sig = _scram_proof(password, salt, iterations, auth_message)

        self._write_frame(bytes([12]) + proof)

        r = _Reader(self._read_frame())
        if r.u8() != 13:
            raise OperationalError("bad handshake outcome")
        if r.u8() == 1:
            server_sig = r.take(32)
            if password and not hmac.compare_digest(server_sig, expected_server_sig):
                raise OperationalError("server signature mismatch (mutual auth failed)")
        else:
            raise OperationalError(f"authentication denied: {r.text()}")

    # -- response parsing (shared by one-shot query and prepared execute) --
    @staticmethod
    def _parse_result(r: "_Reader"):
        tag = r.u8()
        if tag == _RESP_ROWS:
            ncols = r.u32()
            columns = [r.text() for _ in range(ncols)]
            rows = []
            for _ in range(r.u32()):
                ncells = r.u32()
                row = tuple(_decode_value(_Reader(r.blob())) for _ in range(ncells))
                rows.append(row)
            return ("rows", columns, rows)
        if tag == _RESP_RESULT_SETS:
            # Several result sets (a CALL whose body EMITs): DB-API style —
            # the first set is current, `Cursor.nextset()` walks the rest.
            sets = []
            for _ in range(r.u32()):
                ncols = r.u32()
                columns = [r.text() for _ in range(ncols)]
                rows = []
                for _ in range(r.u32()):
                    ncells = r.u32()
                    rows.append(tuple(_decode_value(_Reader(r.blob())) for _ in range(ncells)))
                sets.append((columns, rows))
            return ("sets", sets, None)
        if tag == _RESP_MUTATION:
            return ("mutation", r.u64(), None)
        if tag == _RESP_DDL:
            return ("ddl", 0, None)
        if tag == _RESP_ERROR:
            raise ProgrammingError(r.text())
        raise InterfaceError(f"unknown response tag {tag}")

    def _roundtrip(self, req: bytes) -> "_Reader":
        """Send one request frame and read one response frame under the
        connection lock. Transport failures surface as `OperationalError` so a
        pool can recycle the connection cleanly."""
        self._check_ready()
        try:
            with self._lock:
                # Re-check under the lock: `_stream_open` claims the
                # connection while holding it, so this is where a statement
                # racing a stream on another thread is ordered.
                if self._streaming:
                    raise ProgrammingError(_BUSY_STREAMING)
                self._write_frame(req)
                return _Reader(self._read_frame())
        except OperationalError:
            # A framing-level error (e.g. server closed the socket) leaves the
            # connection unusable; mark it so pools recycle it.
            self._broken = True
            raise
        except OSError as e:  # mid-query socket error: dead peer, timeout, ...
            self._broken = True
            raise OperationalError(f"query transport failed: {e}") from e

    def _check_ready(self) -> None:
        """Refuse to start a request on a connection that cannot carry one.
        A broken connection is refused rather than retried because its socket
        may hold unread frames: writing into it would read someone else's
        answer back, which is far more confusing than an error here."""
        if self.closed:
            raise ProgrammingError("connection is closed")
        if self._broken:
            raise OperationalError(
                "connection is broken (a transport error or an abandoned "
                "stream left it out of sync); call reconnect()"
            )
        if self._streaming:
            raise ProgrammingError(_BUSY_STREAMING)

    # -- one-shot query (OP_QUERY) --
    def _query(self, sql: str, consistency: int):
        body = sql.encode("utf-8")
        req = bytes([_OP_QUERY, consistency]) + struct.pack("<I", len(body)) + body
        return self._parse_result(self._roundtrip(req))

    # -- prepared statements (OP_PREPARE / OP_EXECUTE / OP_CLOSE) --
    def _prepare(self, sql: str) -> "tuple[int, int, bool]":
        """Prepare `sql`, returning `(id, nparams, cached)`. Reuses a cached id
        when present. Raises `_Unpreparable` for DDL/session statements the
        server refuses to prepare."""
        hit = self._prepared.get(sql)
        if hit is not None:
            return (hit[0], hit[1], True)
        body = sql.encode("utf-8")
        req = bytes([_OP_PREPARE]) + struct.pack("<I", len(body)) + body
        r = self._roundtrip(req)
        tag = r.u8()
        if tag == _RESP_PREPARED:
            stmt_id = r.u32()
            nparams = r.u16()
            if len(self._prepared) < _MAX_PREPARED_CACHE:
                self._prepared[sql] = (stmt_id, nparams)
                return (stmt_id, nparams, True)
            return (stmt_id, nparams, False)
        if tag == _RESP_ERROR:
            msg = r.text()
            if "cannot be prepared" in msg:
                raise _Unpreparable(msg)
            raise ProgrammingError(msg)
        raise InterfaceError(f"unexpected prepare response tag {tag}")

    def _execute_prepared(self, stmt_id: int, params: Sequence[Any], consistency: int):
        req = bytearray()
        req.append(_OP_EXECUTE)
        req.append(consistency)
        req += struct.pack("<I", stmt_id)
        req += struct.pack("<H", len(params))
        for p in params:
            vb = _encode_value(p)
            req += struct.pack("<I", len(vb))
            req += vb
        return self._parse_result(self._roundtrip(bytes(req)))

    def _execute_batch(self, stmt_id: int, rows: "list[Sequence[Any]]", consistency: int):
        """One round-trip executing a prepared statement once per param row
        (the `executemany` wire op). Returns the parsed result (total
        affected). Raises `ProgrammingError` on a row failure (the server
        names the failing row; earlier rows stay applied)."""
        req = bytearray()
        req.append(_OP_EXECUTE_BATCH)
        req.append(consistency)
        req += struct.pack("<I", stmt_id)
        req += struct.pack("<I", len(rows))
        for params in rows:
            req += struct.pack("<H", len(params))
            for p in params:
                vb = _encode_value(p)
                req += struct.pack("<I", len(vb))
                req += vb
        return self._parse_result(self._roundtrip(bytes(req)))

    def _close_prepared(self, stmt_id: int) -> None:
        """Best-effort free of a server-side prepared slot (it replies Ddl)."""
        req = bytes([_OP_CLOSE]) + struct.pack("<I", stmt_id)
        try:
            self._roundtrip(req)
        except (OperationalError, ProgrammingError, InterfaceError):
            pass

    def stream(self, sql: str, consistency: "int | None" = None) -> "RowStream":
        """Run `sql` over `OP_QUERY_STREAM` and return a :class:`RowStream` —
        an iterator of rows that holds one chunk at a time instead of the
        whole result, for exports and large scans.

        The connection is busy for the whole stream: every other statement on
        it raises `ProgrammingError` until the stream finishes. It finishes
        when you iterate to the end, when you `close()` it (which drains what
        the server already has in flight), or when it is garbage-collected.
        A stream abandoned with too much still to come marks the connection
        broken instead of draining, so `is_usable()` turns False and a pool
        discards it rather than handing on a desynced socket.

        Use it as a context manager when you may stop early::

            with conn.stream("SELECT id, pad FROM big") as rows:
                print(rows.columns)
                for row in rows:
                    if enough(row):
                        break

        Takes no parameters: the streaming opcode carries SQL text. Raises
        `ProgrammingError` against a server that does not know the opcode.
        """
        level = (
            self._consistency
            if consistency is None
            else Consistency.resolve(consistency)
        )
        body = sql.encode("utf-8")
        req = bytes([_OP_QUERY_STREAM, level]) + struct.pack("<I", len(body)) + body
        r = self._stream_open(req)
        try:
            tag = r.u8()
            if tag == _RESP_ERROR:
                msg = r.text()
                if "unknown opcode" in msg:
                    raise ProgrammingError(f"server does not support streaming: {msg}")
                raise ProgrammingError(msg)
            if tag == _RESP_MUTATION:
                # Not a row-producing statement: one ordinary frame is the
                # whole reply, so the stream is empty and already finished.
                return RowStream(self, [], r.u64(), done=True)
            if tag == _RESP_DDL:
                return RowStream(self, [], 0, done=True)
            if tag == _RESP_RESULT_SETS:
                # A procedure that EMITs. The server answers any non-row
                # result as ONE ordinary frame even over the stream opcode,
                # so the socket is at a frame boundary and the connection is
                # perfectly healthy — it is only this API that cannot carry
                # the shape. Refuse the call, not the connection: marking it
                # broken here retired a working socket over a `CALL`.
                raise InterfaceError(
                    "this statement returns multiple result sets, which stream() "
                    "cannot carry; run it with execute() instead"
                )
            if tag != _RESP_ROWS_HEADER:
                # An unexpected reply leaves us unable to say how many frames
                # it is made of, so the socket position is unknown.
                self._broken = True
                raise InterfaceError(f"unexpected response tag {tag} to stream request")
            columns = [r.text() for _ in range(r.u32())]
        except BaseException:
            # Nothing will consume the stream, so release the claim here.
            self._stream_release()
            raise
        return RowStream(self, columns, 0, done=False)

    def _stream_open(self, req: bytes) -> "_Reader":
        """Claim the connection for a stream and send `req`, returning its
        first response frame. The claim is released by the `RowStream` (or by
        `stream()` itself if no stream is handed out)."""
        self._check_ready()
        with self._lock:
            if self._streaming:
                raise ProgrammingError(_BUSY_STREAMING)
            self._streaming = True
            try:
                self._write_frame(req)
                return _Reader(self._read_frame())
            except OperationalError:
                self._streaming = False
                self._broken = True
                raise
            except OSError as e:
                self._streaming = False
                self._broken = True
                raise OperationalError(f"query transport failed: {e}") from e

    def _stream_release(self) -> None:
        """Give the connection back once a stream's exchange is over."""
        self._streaming = False

    def _execute_params(self, sql: str, params: Sequence[Any], consistency: int):
        """Run `sql` with `params` bound as typed values over the prepared
        path. Falls back to client-side text interpolation for statement kinds
        the server won't prepare (so scalar params still work on DDL/session
        control, as before)."""
        try:
            stmt_id, nparams, cached = self._prepare(sql)
        except _Unpreparable:
            return self._query(_bind(sql, params), consistency)
        if len(params) != nparams:
            raise ProgrammingError(
                f"statement expects {nparams} parameters, got {len(params)}"
            )
        try:
            return self._execute_prepared(stmt_id, params, consistency)
        finally:
            if not cached:
                self._close_prepared(stmt_id)

    # -- DB-API surface --
    def subscribe(self, stream: str, after: "str | None" = None, poll: float = 0.5):
        """Yield a stream's events as they arrive, forever.

        A thin, dependency-free helper over the stream's log: it pages the
        log with the keyset cursor and yields each event as a dict
        (``id``, ``op``, ``k``, ``ts``, ``doc``). ``id`` is the position —
        keep the last one you processed and pass it as ``after`` to resume
        exactly where you stopped, across restarts.

        This polls rather than pushing, which is why it needs no MQTT
        client. For push delivery subscribe to ``$stream/<db>/<name>`` with
        any MQTT client instead; the events are identical.

            for ev in conn.subscribe("big_orders"):
                handle(ev["doc"])
        """
        import time as _time

        log = "_stream_" + stream
        cur = after
        while True:
            if cur is None:
                sql = f"SELECT id, op, k, ts, doc FROM {log} ORDER BY id LIMIT 500"
                params: tuple = ()
            else:
                sql = (
                    f"SELECT id, op, k, ts, doc FROM {log} "
                    "WHERE id > ? ORDER BY id LIMIT 500"
                )
                params = (cur,)
            cursor = self.cursor()
            cursor.execute(sql, params)
            batch = cursor.fetchall()
            for row in batch:
                cur = row[0]
                yield {"id": row[0], "op": row[1], "k": row[2], "ts": row[3], "doc": row[4]}
            if not batch:
                _time.sleep(poll)

    def cursor(self) -> "Cursor":
        if self.closed:
            raise ProgrammingError("connection is closed")
        return Cursor(self)

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> "Cursor":
        """Convenience: make a cursor, execute, return it."""
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def commit(self) -> None:  # no-op: skaidb auto-commits each statement
        pass

    def rollback(self) -> None:  # refused: there is nothing to roll back
        raise OperationalError("skaidb does not support rollback (auto-commit only)")

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._drop_socket()

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class RowStream:
    """A streamed result set from :meth:`Connection.stream`: iterate it for
    rows, one chunk held in memory at a time.

    It owns the connection until the exchange is over (PROTOCOL.md §3 — no
    other request may be sent until RowsEnd or Error), so any statement run
    on that connection meanwhile raises `ProgrammingError`. Iterating to the
    end releases it; so does `close()`, which drains the frames the server
    already sent, and so does dropping the last reference. `columns` carries
    the result's column names, which arrive in the header before any row.
    """

    def __init__(self, conn: Connection, columns: List[str], affected: int, done: bool):
        self._conn = conn
        self.columns = columns
        # Rows affected, when the statement turned out to be a mutation.
        self.affected = affected
        self._rows = iter(())
        self._done = False
        self._released = False
        if done:
            self._finish()

    def __iter__(self) -> "RowStream":
        return self

    def __next__(self) -> tuple:
        while True:
            try:
                return next(self._rows)
            except StopIteration:
                pass  # current chunk drained; pull the next frame
            if self._done:
                raise StopIteration
            self._next_frame()

    def _finish(self) -> None:
        """Mark the exchange over and hand the connection back. Idempotent,
        because close() and the iterator both reach it."""
        self._done = True
        if not self._released:
            self._released = True
            self._conn._stream_release()

    def _next_frame(self) -> None:
        """Read the next stream frame into the row buffer, or end the stream."""
        conn = self._conn
        try:
            fr = _Reader(conn._read_frame())
            t = fr.u8()
            if t == _RESP_ROWS_CHUNK:
                rows = []
                for _ in range(fr.u32()):
                    rows.append(
                        tuple(_decode_value(_Reader(fr.blob())) for _ in range(fr.u32()))
                    )
                self._rows = iter(rows)
                return
        except OperationalError:
            # Dead socket mid-stream: there is nothing left to drain and the
            # connection cannot carry another statement.
            conn._broken = True
            self._finish()
            raise
        except OSError as e:
            conn._broken = True
            self._finish()
            raise OperationalError(f"stream transport failed: {e}") from e
        except InterfaceError:
            # A frame we could not parse: we no longer know what the peer is
            # sending, so the connection is not reusable.
            conn._broken = True
            self._finish()
            raise
        if t == _RESP_ROWS_END:
            self._finish()
            return
        if t == _RESP_ERROR:
            # Rows already yielded are valid; the statement failed partway.
            # The error frame ends the exchange, so the socket is back at a
            # request boundary and the connection stays usable.
            self._finish()
            raise ProgrammingError(fr.text())
        conn._broken = True
        self._finish()
        raise InterfaceError(f"unexpected frame tag {t} in stream")

    def close(self) -> None:
        """Finish with the stream, draining whatever the server has already
        queued so the connection is left at a request boundary. If the rest of
        the result is longer than `_STREAM_DRAIN_MAX_FRAMES` frames, or the
        drain hits a transport error, the connection is marked broken instead
        — a discarded connection costs a reconnect, a desynced one costs the
        next caller a baffling error. Idempotent.

        The frame cap bounds the drain in FRAMES, and the socket's read
        timeout bounds it in time. With `connect(timeout=None)` there is no
        second bound: a peer that is alive but silent blocks this call
        forever. That is the same exposure every other read on the
        connection has under that setting, and the reason the default is not
        `None`."""
        if self._released:
            return
        self._rows = iter(())
        conn = self._conn
        # Nothing to drain if the stream ran out or the socket is already gone.
        if not self._done and not conn.closed:
            try:
                for _ in range(_STREAM_DRAIN_MAX_FRAMES):
                    payload = conn._read_frame()
                    tag = payload[0] if payload else None
                    if tag in (_RESP_ROWS_END, _RESP_ERROR):
                        break
                    if tag != _RESP_ROWS_CHUNK:
                        conn._broken = True
                        break
                else:
                    conn._broken = True
            except (OperationalError, OSError):
                conn._broken = True
        self._finish()

    def __enter__(self) -> "RowStream":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __del__(self):
        # The abandoned-stream path that has no explicit close: a `for` loop
        # left by `break` or an exception, or a stream that simply went out of
        # scope. Errors here are unraisable, and at interpreter shutdown even
        # module globals may be gone, so nothing is allowed to escape.
        try:
            self.close()
        except Exception:
            pass


def _enc_str(s: str) -> bytes:
    b = s.encode("utf-8")
    return struct.pack("<I", len(b)) + b


# ---- Cursor ---------------------------------------------------------------


class Cursor:
    def __init__(self, connection: Connection):
        self.connection = connection
        self.arraysize = 1
        self.rowcount = -1
        self.description = None
        self._rows: list = []
        # Every result set of the last CALL that EMITted several (DB-API
        # `nextset()` walks them; the first is current after execute()).
        self._result_sets: list = []
        self._pos = 0
        self._consistency = connection._consistency

    def set_consistency(self, consistency) -> None:
        """Override the consistency level for subsequent executes on this cursor."""
        self._consistency = Consistency.resolve(consistency)

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> "Cursor":
        if params:
            # Bind as typed values over the prepared-statement path: `?` can
            # carry arrays (list/tuple) and nested documents (dict), which have
            # no SQL literal form. Falls back to text interpolation for
            # statement kinds the server won't prepare.
            kind, a, b = self.connection._execute_params(
                sql, params, self._consistency
            )
        else:
            # No parameters: one-shot query. `_bind` still rejects a stray `?`.
            kind, a, b = self.connection._query(
                _bind(sql, None), self._consistency
            )
        self._result_sets = []
        if kind == "sets":
            self._result_sets = list(a)
            columns, rows = self._result_sets[0] if self._result_sets else ([], [])
            kind = "rows"
            a, b = columns, rows
        if kind == "rows":
            columns, rows = a, b
            # description: 7-tuples per DB-API; only name is meaningful here
            self.description = [(c, None, None, None, None, None, None) for c in columns]
            self._rows = rows
            self.rowcount = len(rows)
        else:  # mutation / ddl
            self.description = None
            self._rows = []
            self.rowcount = a if kind == "mutation" else -1
        self._pos = 0
        return self

    def executemany(self, sql: str, seq_of_params: Iterable[Sequence[Any]]) -> None:
        rows = [list(p) for p in seq_of_params]
        if not rows:
            self.rowcount = 0
            return
        # One round-trip for the whole batch via the ExecuteBatch wire op
        # (prepare once, ship every param row in a single frame). Falls back
        # to the per-row loop for unpreparable statements and for servers
        # that predate the opcode.
        try:
            stmt_id, nparams, cached = self.connection._prepare(sql)
        except _Unpreparable:
            self._executemany_loop(sql, rows)
            return
        for i, params in enumerate(rows):
            if len(params) != nparams:
                raise ProgrammingError(
                    f"row {i}: statement expects {nparams} parameters, got {len(params)}"
                )
        try:
            kind, a, _ = self.connection._execute_batch(
                stmt_id, rows, self._consistency
            )
        except ProgrammingError as e:
            if "unknown opcode" in str(e):  # pre-batch server
                self._executemany_loop(sql, rows)
                return
            raise
        finally:
            if not cached:
                self.connection._close_prepared(stmt_id)
        self.description = None
        self._rows = []
        self._pos = 0
        self.rowcount = a if kind == "mutation" else -1

    def _executemany_loop(self, sql: str, rows: "list[Sequence[Any]]") -> None:
        total = 0
        for params in rows:
            self.execute(sql, params)
            if self.rowcount and self.rowcount > 0:
                total += self.rowcount
        self.rowcount = total

    def fetchone(self):
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: Optional[int] = None):
        size = self.arraysize if size is None else size
        chunk = self._rows[self._pos:self._pos + size]
        self._pos += len(chunk)
        return chunk

    def fetchall(self):
        chunk = self._rows[self._pos:]
        self._pos = len(self._rows)
        return chunk

    def nextset(self):
        """Advance to the next result set of a multi-set reply (a `CALL`
        whose body `EMIT`ted result sets). Returns True when another set is
        now current, None when there is none (DB-API)."""
        if len(self._result_sets) <= 1:
            self._result_sets = []
            return None
        self._result_sets.pop(0)
        columns, rows = self._result_sets[0]
        self.description = [(c, None, None, None, None, None, None) for c in columns]
        self._rows = rows
        self.rowcount = len(rows)
        self._pos = 0
        return True

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def close(self) -> None:
        self._rows = []

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


# ---- endpoints / module entry point ---------------------------------------


def _parse_endpoint(spec: str, default_port: int) -> Tuple[str, int]:
    """`"host"` or `"host:port"` -> `(host, port)`. The port after the last
    colon wins (bracketless), so bare IPv4/hostnames keep the default port."""
    spec = spec.strip()
    host, sep, port = spec.rpartition(":")
    if sep and port.isdigit():
        return (host, int(port))
    return (spec, default_port)


def _resolve_endpoints(host, port, seeds) -> List[Tuple[str, int]]:
    if seeds:
        eps = [_parse_endpoint(s, port) for s in seeds]
    else:
        eps = [(host, port)]
    # Shuffle so a fleet of clients spreads its initial connections across the
    # seed list rather than hammering the first one. Leaderless: any seed
    # serves any request.
    eps = list(eps)
    random.shuffle(eps)
    return eps


def connect(
    host: str = "localhost",
    port: int = 7000,
    user: str = "anonymous",
    password: str = "",
    consistency: "int | str" = Consistency.QUORUM,
    timeout: Optional[float] = 10.0,
    database: Optional[str] = None,
    seeds: Optional[Sequence[str]] = None,
    connect_timeout: Optional[float] = None,
    read_timeout: Optional[float] = None,
    tls: bool = False,
    tls_ca: Optional[str] = None,
    tls_insecure: bool = False,
    tls_server_name: str = "skaidb",
) -> Connection:
    """Open a connection to skaidb and run the SCRAM handshake.

    ``seeds`` is an optional list of ``"host"`` / ``"host:port"`` endpoints
    tried (in randomized order) until one connects — skaidb is leaderless, so
    any seed serves any request. ``database`` issues ``USE <database>`` as part
    of connecting, so the session starts in the right database. ``timeout`` is
    the default for both dial and reads; ``connect_timeout`` / ``read_timeout``
    override each independently (a read timeout can then sit above the server's
    statement timeout without also slowing dial failures).
    """
    endpoints = _resolve_endpoints(host, port, seeds)
    ct = connect_timeout if connect_timeout is not None else timeout
    rt = read_timeout if read_timeout is not None else timeout
    tls_ctx = _build_tls_context(tls, tls_ca, tls_insecure)
    return Connection(
        endpoints, user, password, consistency, ct, rt, database, tls_ctx, tls_server_name
    )


def _build_tls_context(tls: bool, tls_ca: "str | None", tls_insecure: bool):
    """Build a client ``ssl.SSLContext`` for the binary protocol, or ``None``
    for plaintext. TLS is enabled when ``tls`` is set or a CA / insecure flag
    is given. ``tls_ca`` verifies the server cert against a specific CA (the
    cluster ``ca.crt``); ``tls_insecure`` skips verification (self-signed/dev
    only — INSECURE); otherwise the system trust store is used."""
    if not (tls or tls_ca or tls_insecure):
        return None
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    if tls_insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    elif tls_ca:
        ctx.load_verify_locations(tls_ca)
    return ctx


# ---- connection pool ------------------------------------------------------


class ConnectionPool:
    """A thread-safe pool of skaidb connections.

    Hands out connections from an idle set (creating new ones up to
    ``maxsize`` retained idle) and validates them cheaply on checkout/checkin,
    discarding any left broken by a transport error. All keyword arguments
    accepted by :func:`connect` (``seeds``, ``database``, timeouts, auth, …)
    pass straight through, so pooled connections inherit multi-seed failover.

    Typical use::

        pool = skaidb.pool(seeds=["h1", "h2", "h3"], database="app", maxsize=8)
        with pool.connection() as conn:
            conn.execute("SELECT ...")
    """

    def __init__(self, maxsize: int = 10, **connect_kwargs: Any):
        if maxsize < 1:
            raise ValueError("maxsize must be >= 1")
        self._maxsize = maxsize
        self._kwargs = connect_kwargs
        self._idle: List[Connection] = []
        self._lock = threading.Lock()
        self.closed = False

    def getconn(self) -> Connection:
        """Check out a usable connection, reusing an idle one when possible."""
        while True:
            with self._lock:
                if self.closed:
                    raise ProgrammingError("pool is closed")
                conn = self._idle.pop() if self._idle else None
            if conn is None:
                return connect(**self._kwargs)
            if conn.is_usable():
                return conn
            conn.close()  # discard a stale idle connection, then loop

    def putconn(self, conn: Connection) -> None:
        """Return a connection to the pool, or close it if it is broken or the
        pool is full."""
        with self._lock:
            if not self.closed and conn.is_usable() and len(self._idle) < self._maxsize:
                self._idle.append(conn)
                return
        conn.close()

    @contextmanager
    def connection(self):
        """Context manager: check out a connection and return it on exit
        (closing it instead if a transport error broke it)."""
        conn = self.getconn()
        try:
            yield conn
        finally:
            if conn.is_usable():
                self.putconn(conn)
            else:
                conn.close()

    def close(self) -> None:
        """Close the pool and every idle connection. Checked-out connections
        close when they are returned."""
        with self._lock:
            self.closed = True
            idle, self._idle = self._idle, []
        for conn in idle:
            conn.close()

    def __enter__(self) -> "ConnectionPool":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def pool(maxsize: int = 10, **connect_kwargs: Any) -> ConnectionPool:
    """Create a :class:`ConnectionPool`. Accepts every :func:`connect`
    keyword (``seeds``, ``database``, timeouts, auth, …)."""
    return ConnectionPool(maxsize=maxsize, **connect_kwargs)
