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

import collections
import datetime as _dt
import decimal
import hashlib
import hmac
import logging
import random
import socket
import ssl
import struct
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Any, Iterable, List, Optional, Sequence, Tuple

# The package version lives in ONE place, pyproject.toml. At runtime it is read
# back from the installed distribution's metadata; the literal below is only
# the fallback for running straight from a source checkout that was never
# installed (tests/test_version.py keeps it equal to pyproject.toml). The
# server records it in its `drivers` table via the Hello frame.
_FALLBACK_VERSION = "1.4.1"


def _detect_version() -> str:
    try:
        from importlib.metadata import version as _dist_version

        return _dist_version("skaidb")
    except Exception:  # not installed (source checkout), or no metadata
        return _FALLBACK_VERSION


__version__ = _detect_version()

__all__ = [
    "__version__",
    "connect",
    "Connection",
    "ConnectionPool",
    "pool",
    "Cursor",
    "RowStream",
    "Error",
    "InterfaceError",
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
        # Built from its digits, not `Decimal(m).scaleb(-scale)`: scaleb
        # rounds to the context precision (28 digits by default), which
        # silently truncated wider mantissas (up to 38 digits on the wire).
        digits = tuple(int(d) for d in str(abs(mantissa)))
        return decimal.Decimal((1 if mantissa < 0 else 0, digits, -scale))
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
_OP_QUERY_STREAM_PARAMS = 9

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
    # An instance built without __init__ wires its own transport and cannot
    # be re-dialed, so it does not auto-reconnect.
    _auto_reconnect = False
    _cert_auth = False
    _connected = None
    # The pool that owns this connection (see `ConnectionPool`): told each
    # round trip's latency, each transport failure and each write, and asked
    # for the dial order when the connection re-dials. None outside a pool.
    _observer = None
    _pool_ep = None

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
        auto_reconnect=True,
        auth_mechanism="scram",
        observer=None,
    ):
        self._observer = observer
        self._consistency = Consistency.resolve(consistency)
        # Re-dial transparently after a transport failure (see `_run`).
        self._auto_reconnect = auto_reconnect
        mech = str(auth_mechanism).lower()
        if mech not in ("scram", "password", "certificate", "external", "x509"):
            raise ProgrammingError(
                f"unknown auth_mechanism {auth_mechanism!r} (use 'scram' or 'certificate')"
            )
        self._cert_auth = mech in ("certificate", "external", "x509")
        # The endpoint the live socket is dialed to; a re-dial tries the
        # others first.
        self._connected: "Tuple[str, int] | None" = None
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
                if self._cert_auth:
                    # The default user asserts nothing: the certificate's
                    # Common Name is the login.
                    self._handshake_certificate("" if self._user == "anonymous" else self._user)
                else:
                    self._handshake(self._user, self._password)
            except (OSError, OperationalError) as e:
                errors.append(f"{host}:{port}: {e}")
                self._drop_socket()
                if self._observer is not None and _member_fault(e):
                    self._observer._failed((host, port))
                continue
            # Connected + authenticated: a fresh socket has no prepared
            # statements, no stream in flight and is not broken.
            self._connected = (host, port)
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
        # Try every OTHER endpoint before the one that just failed: a member
        # going down during a rolling restart still accepts a connect for a
        # moment, and re-dialing it first lost the retry (and an 8-hour
        # backfill) in production.
        if self._observer is not None:
            # Pooled: the pool's current order (healthy members first, the
            # one that failed last).
            self._endpoints = self._observer._dial_order(exclude=self._connected)
        elif self._connected in self._endpoints and len(self._endpoints) > 1:
            self._endpoints.remove(self._connected)
            self._endpoints.append(self._connected)
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

    def _run(self, fn):
        """Run one statement; if its connection fails in transit, re-dial
        (walking the other endpoints first) and run it once more.

        The retry repeats the WHOLE statement — prepare included, since
        prepared ids die with the connection — and happens at most once. A
        statement the old node had already applied can therefore run twice
        (at-least-once on failover, as in the Rust driver); an INSERT on an
        existing primary key then fails instead of duplicating. Pass
        ``auto_reconnect=False`` to :func:`connect` to surface the failure
        instead."""
        try:
            return fn()
        except OperationalError:
            if not (self._auto_reconnect and self._broken) or self.closed or self._streaming:
                raise
            self.reconnect()
            return fn()

    # -- handshake --
    def _handshake_certificate(self, user: str) -> None:
        """EXTERNAL: the TLS client certificate is the credential (its
        Common Name is the username; ``user`` may be empty or must equal
        it). No exchange follows AuthStart; the outcome's signature is not
        checked — TLS authenticated the server."""
        if self._tls_ctx is None:
            raise OperationalError("certificate authentication needs TLS with a client certificate")
        self._write_frame(bytes([10]) + _enc_str(user) + _enc_str("") + bytes([2]))
        r = _Reader(self._read_frame())
        if r.u8() != 13:
            raise OperationalError("bad handshake outcome")
        if r.u8() != 1:
            raise OperationalError(f"authentication denied: {r.text()}")

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
    def _parse_rows(r: "_Reader"):
        """A Rows body (PROTOCOL.md §3.2), also one set of ResultSets: `r`
        sits just past the tag. Returns ``(columns, rows)``."""
        ncols = r.u32()
        columns = [r.text() for _ in range(ncols)]
        rows = []
        for _ in range(r.u32()):
            ncells = r.u32()
            rows.append(tuple(_decode_value(_Reader(r.blob())) for _ in range(ncells)))
        return columns, rows

    @staticmethod
    def _parse_sets(r: "_Reader"):
        """A ResultSets body (a CALL whose body EMITs), `r` just past the tag:
        every set as ``(columns, rows)``, in emission order. The buffered path
        and a stream request both receive this frame, and both decode it
        here."""
        return [Connection._parse_rows(r) for _ in range(r.u32())]

    @staticmethod
    def _parse_result(r: "_Reader"):
        tag = r.u8()
        if tag == _RESP_ROWS:
            columns, rows = Connection._parse_rows(r)
            return ("rows", columns, rows)
        if tag == _RESP_RESULT_SETS:
            # Several result sets (a CALL whose body EMITs): DB-API style —
            # the first set is current, `Cursor.nextset()` walks the rest.
            return ("sets", Connection._parse_sets(r), None)
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
        started = time.monotonic()
        try:
            with self._lock:
                # Re-check under the lock: `_stream_open` claims the
                # connection while holding it, so this is where a statement
                # racing a stream on another thread is ordered.
                if self._streaming:
                    raise ProgrammingError(_BUSY_STREAMING)
                self._write_frame(req)
                frame = self._read_frame()
        except OperationalError:
            # A framing-level error (e.g. server closed the socket) leaves the
            # connection unusable; mark it so pools recycle it.
            self._broken = True
            self._observe_failure()
            raise
        except OSError as e:  # mid-query socket error: dead peer, timeout, ...
            self._broken = True
            self._observe_failure()
            raise OperationalError(f"query transport failed: {e}") from e
        if self._observer is not None:
            self._observer._round_trip(self, time.monotonic() - started)
        return _Reader(frame)

    def _observe_failure(self) -> None:
        if self._observer is not None and self._connected is not None:
            self._observer._failed(self._connected)

    def _note_statement(self, sql: str) -> None:
        """A statement succeeded: a pool with read-your-writes on remembers
        which member took a write."""
        if self._observer is not None and _is_write(sql):
            self._observer._wrote(self)

    def _check_ready(self) -> None:
        """Refuse to start a request on a connection that cannot carry one.
        A broken connection is refused rather than retried because its socket
        may hold unread frames: writing into it would read someone else's
        answer back, which is far more confusing than an error here."""
        if self.closed:
            raise ProgrammingError("connection is closed")
        if self._broken and self._auto_reconnect and not self._streaming:
            # Nothing of this request has been sent: re-dialing first is
            # always safe.
            self.reconnect()
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

    def stream(
        self,
        sql: str,
        params: "Optional[Sequence[Any]]" = None,
        consistency: "int | str | None" = None,
    ) -> "RowStream":
        """Run `sql` over `OP_QUERY_STREAM` and return a :class:`RowStream` —
        an iterator of rows that holds one chunk at a time instead of the
        whole result, for exports and large scans.

        `params` bind the statement's ``?`` placeholders in order — typed,
        exactly the values `Cursor.execute` takes (lists, dicts, datetimes,
        …) — over `OP_QUERY_STREAM_PARAMS` (servers >= 0.330)::

            for row in conn.stream("SELECT id, ts FROM events WHERE ts >= ?", (since,)):
                ...

        Without parameters the request is the original `OP_QUERY_STREAM`, so
        a parameterless stream works against every server; a parameterised
        one against an older server raises `ProgrammingError`. (A
        consistency passed positionally, ``stream(sql, "ONE")`` — the
        signature before `params` existed — still means the consistency.)

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

        A `CALL` whose procedure body `EMIT`s result sets is answered with all
        of them in one frame, streamed or not. The stream then owns nothing
        and walks the sets as `Cursor.nextset()` does::

            rows = conn.stream("CALL report()")
            while True:
                for row in rows:
                    ...
                if not rows.nextset():
                    break

        Raises `ProgrammingError` against a server that does not know the
        opcode.
        """
        if consistency is None and isinstance(params, (int, str)):
            # `stream(sql, consistency)`, written before `params` existed.
            consistency, params = params, None
        level = (
            self._consistency
            if consistency is None
            else Consistency.resolve(consistency)
        )
        body = sql.encode("utf-8")
        params = list(params) if params else []
        if params:
            req = bytearray([_OP_QUERY_STREAM_PARAMS, level])
            req += struct.pack("<I", len(body)) + body
            req += struct.pack("<H", len(params))
            for p in params:
                vb = _encode_value(p)
                req += struct.pack("<I", len(vb))
                req += vb
            req = bytes(req)
        else:
            req = bytes([_OP_QUERY_STREAM, level]) + struct.pack("<I", len(body)) + body
        r = self._stream_open(req)
        self._note_statement(sql)
        try:
            tag = r.u8()
            if tag == _RESP_ERROR:
                msg = r.text()
                if "unknown opcode" in msg:
                    what = "parameterised streaming (skaidb >= 0.330)" if params else "streaming"
                    raise ProgrammingError(f"server does not support {what}: {msg}")
                raise ProgrammingError(msg)
            if tag == _RESP_MUTATION:
                # Not a row-producing statement: one ordinary frame is the
                # whole reply, so the stream is empty and already finished.
                return RowStream(self, [], r.u64(), done=True)
            if tag == _RESP_DDL:
                return RowStream(self, [], 0, done=True)
            if tag == _RESP_RESULT_SETS:
                # A procedure that EMITs: the server answers with every set
                # in ONE ordinary frame even over the stream opcode — the
                # frame `Cursor.execute()` receives, decoded by the same
                # code. The exchange is over, so the stream is already
                # finished: the first set is current, `nextset()` walks the
                # rest, as `Cursor.nextset()` does.
                return RowStream(self, [], 0, done=True, sets=self._parse_sets(r))
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
            started = time.monotonic()
            try:
                self._write_frame(req)
                frame = self._read_frame()
            except OperationalError:
                self._streaming = False
                self._broken = True
                self._observe_failure()
                raise
            except OSError as e:
                self._streaming = False
                self._broken = True
                self._observe_failure()
                raise OperationalError(f"query transport failed: {e}") from e
        if self._observer is not None:
            self._observer._round_trip(self, time.monotonic() - started)
        return _Reader(frame)

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
            if self._observer is not None and getattr(self, "_pool_out", False):
                # Closed while checked out instead of returned: the pool
                # stops counting it against its member.
                self._observer._closed_out(self)

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

    A `CALL` whose procedure body `EMIT`s result sets is answered with all of
    them in one frame, streamed or not: such a stream owns nothing, starts on
    the first set, and `nextset()` walks the rest as `Cursor.nextset()` does.
    """

    def __init__(self, conn: Connection, columns: List[str], affected: int, done: bool,
                 sets: "Optional[list]" = None):
        self._conn = conn
        self.columns = columns
        # Rows affected, when the statement turned out to be a mutation.
        self.affected = affected
        self._rows = iter(())
        # The sets after the current one, of a CALL whose body EMITted.
        self._sets: list = []
        if sets:
            self.columns, rows = sets[0]
            self._rows = iter(rows)
            self._sets = list(sets[1:])
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

    def nextset(self):
        """Advance to the next result set of a multi-set reply, as
        `Cursor.nextset()` does: a `CALL` whose procedure body `EMIT`s result
        sets is answered with all of them in one frame, even to a stream
        request. `columns` and the iteration then describe that set. Returns
        True when another set is now current, None when there is none —
        always, for a streamed result (DB-API)."""
        if not self._sets:
            return None
        self.columns, rows = self._sets.pop(0)
        self._rows = iter(rows)
        return True

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
        conn = self.connection
        if params:
            # Bind as typed values over the prepared-statement path: `?` can
            # carry arrays (list/tuple) and nested documents (dict), which have
            # no SQL literal form. Falls back to text interpolation for
            # statement kinds the server won't prepare.
            kind, a, b = conn._run(
                lambda: conn._execute_params(sql, params, self._consistency)
            )
        else:
            # No parameters: one-shot query. `_bind` still rejects a stray `?`.
            text = _bind(sql, None)
            kind, a, b = conn._run(lambda: conn._query(text, self._consistency))
        conn._note_statement(sql)
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
        self.connection._run(lambda: self._executemany_once(sql, rows))
        self.connection._note_statement(sql)

    def _executemany_once(self, sql: str, rows: "list[list[Any]]") -> None:
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
    tls_client_cert: Optional[str] = None,
    tls_client_key: Optional[str] = None,
    auth_mechanism: str = "scram",
    auto_reconnect: bool = True,
) -> Connection:
    """Open a connection to skaidb and run the SCRAM handshake.

    ``seeds`` is an optional list of ``"host"`` / ``"host:port"`` endpoints
    tried (in randomized order) until one connects — skaidb is leaderless, so
    any seed serves any request. ``database`` issues ``USE <database>`` as part
    of connecting, so the session starts in the right database. ``timeout`` is
    the default for both dial and reads; ``connect_timeout`` / ``read_timeout``
    override each independently (a read timeout can then sit above the server's
    statement timeout without also slowing dial failures).

    ``auto_reconnect`` (default True): after a transport failure the next
    statement re-dials first, and a statement whose connection fails in
    transit is retried once on a fresh connection — the other endpoints are
    tried before the one that failed (see :meth:`Connection._run` for the
    at-least-once caveat). ``auth_mechanism="certificate"`` authenticates with
    the TLS client certificate given by ``tls_client_cert`` / ``tls_client_key``
    (its Common Name is the user; the server needs ``auth.x509_enabled``).
    """
    endpoints = _resolve_endpoints(host, port, seeds)
    _, dial = _dialer(
        host, port, seeds, user, password, consistency, timeout, database,
        connect_timeout, read_timeout, tls, tls_ca, tls_insecure, tls_server_name,
        tls_client_cert, tls_client_key, auth_mechanism, auto_reconnect,
    )
    return dial(endpoints)


def _dialer(
    host: str = "localhost",
    port: int = 7000,
    seeds: Optional[Sequence[str]] = None,
    user: str = "anonymous",
    password: str = "",
    consistency: "int | str" = Consistency.QUORUM,
    timeout: Optional[float] = 10.0,
    database: Optional[str] = None,
    connect_timeout: Optional[float] = None,
    read_timeout: Optional[float] = None,
    tls: bool = False,
    tls_ca: Optional[str] = None,
    tls_insecure: bool = False,
    tls_server_name: str = "skaidb",
    tls_client_cert: Optional[str] = None,
    tls_client_key: Optional[str] = None,
    auth_mechanism: str = "scram",
    auto_reconnect: bool = True,
):
    """The configured endpoints (in the order given) and a function that
    dials a :class:`Connection` across an explicit endpoint order with
    :func:`connect`'s options — the TLS context is built once."""
    endpoints = [_parse_endpoint(s, port) for s in seeds] if seeds else [(host, port)]
    ct = connect_timeout if connect_timeout is not None else timeout
    rt = read_timeout if read_timeout is not None else timeout
    tls_ctx = _build_tls_context(tls, tls_ca, tls_insecure, tls_client_cert, tls_client_key)

    def dial(order, observer=None, reconnect=None):
        return Connection(
            order,
            user,
            password,
            consistency,
            ct,
            rt,
            database,
            tls_ctx,
            tls_server_name,
            auto_reconnect=auto_reconnect if reconnect is None else reconnect,
            auth_mechanism=auth_mechanism,
            observer=observer,
        )

    return endpoints, dial


def _build_tls_context(
    tls: bool,
    tls_ca: "str | None",
    tls_insecure: bool,
    client_cert: "str | None" = None,
    client_key: "str | None" = None,
):
    """Build a client ``ssl.SSLContext`` for the binary protocol, or ``None``
    for plaintext. TLS is enabled when ``tls`` is set or a CA / insecure flag
    is given. ``tls_ca`` verifies the server cert against a specific CA (the
    cluster ``ca.crt``); ``tls_insecure`` skips verification (self-signed/dev
    only — INSECURE); otherwise the system trust store is used."""
    if not (tls or tls_ca or tls_insecure or client_cert):
        return None
    if bool(client_cert) != bool(client_key):
        raise ProgrammingError("tls_client_cert and tls_client_key go together")
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    if tls_insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    elif tls_ca:
        ctx.load_verify_locations(tls_ca)
    else:
        ctx.load_default_certs()
    if client_cert:
        ctx.load_cert_chain(client_cert, client_key)
    return ctx


# ---- connection pool ------------------------------------------------------

_log = logging.getLogger("skaidb.pool")

# Leading keywords of statements that never write: everything else counts as
# a write for read-your-writes affinity (a read taken for a write only keeps
# the caller on its member a little longer).
_READ_KEYWORDS = frozenset(
    ("select", "show", "explain", "describe", "desc", "values", "with", "use")
)


def _is_write(sql: str) -> bool:
    i, n = 0, len(sql)
    while i < n:
        if sql[i].isspace() or sql[i] == "(":
            i += 1
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j + 1
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
        else:
            break
    j = i
    while j < n and (sql[j].isalpha() or sql[j] == "_"):
        j += 1
    return sql[i:j].lower() not in _READ_KEYWORDS


def _member_fault(e: BaseException) -> bool:
    """Whether a failed dial says something about the member's health. A
    refused login is the caller's problem, not the member's — except a
    capacity refusal, which is the member being full."""
    msg = str(e)
    if isinstance(e, OSError):
        return True
    if msg.startswith("authentication denied"):
        return "too many connections:" in msg
    return not msg.startswith("server signature mismatch")


def _ep_text(ep: "Tuple[str, int]") -> str:
    return f"{ep[0]}:{ep[1]}"


# Latency EWMA: the fastest of a member's first round trips seeds it (a round
# trip is only ever lengthened — by the member, the network or this process
# stalling — so a slow first statement cannot set it); then each round trip
# moves it 5% toward it, and no sample counts for more than 10x the larger of
# the current average and the ejection floor — one slow query does not eject
# a healthy member, a member that stays slow is ejected within a few dozen
# statements.
_EWMA_SEED_SAMPLES = 3
_EWMA_WEIGHT = 0.05
_EWMA_CLAMP = 10.0
# Round trips a latency ejection's warning reports the slowest of.
_RECENT_SAMPLES = 10
# Round trips a member (and at least one other) needs before its average
# can eject it.
_EWMA_MIN_SAMPLES = 10
# least_latency: a member whose last round trip is older than this scores 0,
# so a lightly loaded pool keeps re-measuring every member instead of
# settling forever on whichever answered fastest once.
_STALE_SAMPLE = 2.0
# How often the background thread checks for members due a re-probe.
_MAINTAIN_TICK = 0.25
# Discovery refresh while a listed member is not ready: a member is often
# `unreachable` or `resyncing` only for seconds (a peer's circuit breaker
# right after it restarts), and waiting a whole refresh interval to let it
# back in leaves the load on the others.
_REFRESH_NOT_READY = 5.0


class _Member:
    """One member as the pool sees it."""

    __slots__ = (
        "endpoint", "seed", "canonical", "listed", "ewma", "firsts", "recent", "samples",
        "last_sample", "errors", "ejected_until", "ejections", "readmitted_at", "in_use", "probing",
    )

    def __init__(self, endpoint: "Tuple[str, int]", seed: bool):
        self.endpoint = endpoint  # what the pool dials
        self.seed = seed  # configured: discovery never drops it
        self.canonical: "str | None" = None  # its own SHOW ENDPOINTS name
        self.listed = "ready"  # ready | resyncing | unreachable | unlisted
        self.ewma: "float | None" = None  # seconds
        self.firsts: "List[float]" = []  # round trips before the average is seeded
        self.recent: "collections.deque[float]" = collections.deque(maxlen=_RECENT_SAMPLES)
        self.samples = 0
        self.last_sample = 0.0
        self.errors = 0  # consecutive transport failures
        self.ejected_until: "float | None" = None  # None = admitted
        self.ejections = 0
        self.readmitted_at = 0.0
        self.in_use = 0
        self.probing = False

    def reset_latency(self) -> None:
        """Measure the member afresh (a re-admission, back to ready)."""
        self.ewma = None
        self.firsts.clear()
        self.recent.clear()
        self.samples = 0


class ConnectionPool:
    """A thread-safe pool of skaidb connections, balanced across members.

    Hands out connections from an idle set (creating new ones up to
    ``maxsize`` retained idle) and validates them cheaply on checkout/checkin,
    discarding any left broken by a transport error. All keyword arguments
    accepted by :func:`connect` (``seeds``, ``database``, timeouts, auth, …)
    pass straight through.

    Load balancing (see docs/pooling.md):

    * ``load_balancing`` — ``"random"`` (default: each new connection dials
      the members in random order), ``"round_robin"`` (checkouts rotate over
      the members) or ``"least_latency"`` (of two members picked at random,
      the one with the lower latency average x checked-out connections).
    * ``idle_order`` — ``"lifo"`` (default: the most recently returned idle
      connection first) or ``"fifo"`` (the longest idle first, so every idle
      connection keeps getting used).
    * ``discover`` — read the members from ``SHOW ENDPOINTS`` (server >=
      0.327) when connections are first made to them and every
      ``refresh_interval`` seconds after: members joining the cluster are
      added, members the server reports ``resyncing`` or ``unreachable``
      get no new connections until they are ``ready`` again.
    * Health ejection: a member with ``eject_errors`` consecutive transport
      failures, or whose latency average is above ``eject_latency_factor``
      times the other members' median and at least ``eject_latency_floor``
      seconds slower, gets no connections for ``eject_backoff`` seconds
      (doubling per repeat, up to ``eject_backoff_max``), then a background
      probe re-admits it. The last usable member is never ejected.
    * ``read_your_writes`` — seconds after a write during which checkouts
      with the same ``affinity`` key go to the member that took it (the
      coordinator that acknowledged a write reads it back even at ``ONE``).
      0 (default) turns it off.

    Typical use::

        pool = skaidb.pool(seeds=["h1", "h2", "h3"], database="app", maxsize=8,
                           load_balancing="least_latency", discover=True)
        with pool.connection() as conn:
            conn.execute("SELECT ...")
    """

    def __init__(
        self,
        maxsize: int = 10,
        *,
        load_balancing: str = "random",
        idle_order: str = "lifo",
        discover: bool = False,
        refresh_interval: float = 60.0,
        read_your_writes: float = 0.0,
        eject_errors: int = 3,
        eject_latency_factor: float = 3.0,
        eject_latency_floor: float = 0.05,
        eject_backoff: float = 5.0,
        eject_backoff_max: float = 60.0,
        **connect_kwargs: Any,
    ):
        if maxsize < 1:
            raise ValueError("maxsize must be >= 1")
        if load_balancing not in ("random", "round_robin", "least_latency"):
            raise ValueError(
                f"unknown load_balancing {load_balancing!r} "
                "(use 'random', 'round_robin' or 'least_latency')"
            )
        if idle_order not in ("lifo", "fifo"):
            raise ValueError(f"unknown idle_order {idle_order!r} (use 'lifo' or 'fifo')")
        if eject_errors < 1:
            raise ValueError("eject_errors must be >= 1")
        self._maxsize = maxsize
        self._policy = load_balancing
        self._fifo = idle_order == "fifo"
        self._discover = discover
        self._refresh_interval = refresh_interval
        self._ryw = read_your_writes
        self._eject_errors = eject_errors
        self._eject_factor = eject_latency_factor
        self._eject_floor = eject_latency_floor
        self._backoff = eject_backoff
        self._backoff_max = eject_backoff_max
        self._kwargs = connect_kwargs
        seeds, self._dial = _dialer(**connect_kwargs)
        self._members: "dict[Tuple[str, int], _Member]" = {}
        for ep in seeds:
            self._members.setdefault(ep, _Member(ep, seed=True))
        self._idle: List[Connection] = []
        self._lock = threading.Lock()
        self._rr = -1
        self._affinity: "dict[Any, Tuple[Tuple[str, int], float]]" = {}
        self._discovery_off = False
        self._next_refresh = 0.0
        self._stop = threading.Event()
        self._maintainer: "threading.Thread | None" = None
        self.closed = False

    # -- checkout / checkin --

    def getconn(self, affinity: Any = None) -> Connection:
        """Check out a usable connection: an idle one to the member the
        policy picks, or a new one dialed to it (falling back to the other
        members). ``affinity`` scopes read-your-writes (None = pool-wide)."""
        while True:
            with self._lock:
                if self.closed:
                    raise ProgrammingError("pool is closed")
                target = self._pick(affinity)
                conn, stale = self._take_idle(target)
                order = None if conn is not None else self._order(first=target)
            for c in stale:
                c.close()
            if conn is None:
                conn = self._dial(order, observer=self)
                conn._pool_ep = conn._connected
                self._discover_through(conn)
                if not conn.is_usable():
                    conn.close()
                    continue
            elif not conn.is_usable():
                conn.close()  # discard a stale idle connection, then loop
                continue
            with self._lock:
                m = self._members.get(conn._pool_ep)
                if m is not None:
                    m.in_use += 1
            conn._pool_affinity = affinity
            conn._pool_out = True
            return conn

    def putconn(self, conn: Connection) -> None:
        """Return a connection to the pool, or close it if it is broken, its
        member is ejected or gone, or the pool is full."""
        with self._lock:
            m = self._members.get(conn._pool_ep)
            if getattr(conn, "_pool_out", False):
                conn._pool_out = False
                if m is not None and m.in_use > 0:
                    m.in_use -= 1
            if (
                not self.closed
                and conn.is_usable()
                and m is not None
                and self._usable(m)
                and len(self._idle) < self._maxsize
            ):
                self._idle.append(conn)
                return
        conn.close()

    @contextmanager
    def connection(self, affinity: Any = None):
        """Context manager: check out a connection and return it on exit
        (closing it instead if a transport error broke it)."""
        conn = self.getconn(affinity)
        try:
            yield conn
        finally:
            self.putconn(conn)

    def members(self) -> "list[dict]":
        """The members as the pool sees them, for monitoring: ``endpoint``,
        ``canonical`` (the member's own name, once discovered), ``state``
        (``ready``, ``ejected``, ``resyncing``, ``unreachable``, ``unlisted``),
        ``latency_ms`` (the round-trip average), ``in_use``, ``idle``,
        ``seed``."""
        with self._lock:
            idle: "dict[Tuple[str, int], int]" = {}
            for c in self._idle:
                idle[c._pool_ep] = idle.get(c._pool_ep, 0) + 1
            return [
                {
                    "endpoint": _ep_text(m.endpoint),
                    "canonical": m.canonical,
                    "state": "ejected" if m.ejected_until is not None else m.listed,
                    "latency_ms": None if m.ewma is None else m.ewma * 1000.0,
                    "in_use": m.in_use,
                    "idle": idle.get(m.endpoint, 0),
                    "seed": m.seed,
                }
                for m in sorted(self._members.values(), key=lambda m: m.endpoint)
            ]

    def close(self) -> None:
        """Close the pool and every idle connection. Checked-out connections
        close when they are returned."""
        with self._lock:
            self.closed = True
            idle, self._idle = self._idle, []
        self._stop.set()
        for conn in idle:
            conn.close()

    def __enter__(self) -> "ConnectionPool":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- choosing a member (callers hold self._lock) --

    @staticmethod
    def _usable(m: _Member) -> bool:
        return m.ejected_until is None and m.listed == "ready"

    def _pick(self, affinity: Any) -> "_Member | None":
        """The member this checkout should go to, or None for "any" (the
        random policy, or no usable member at all)."""
        if self._ryw > 0:
            hit = self._affinity.get(affinity)
            if hit is not None:
                ep, until = hit
                m = self._members.get(ep)
                if time.monotonic() >= until or m is None:
                    del self._affinity[affinity]
                elif self._usable(m):
                    return m
        usable = [m for m in self._members.values() if self._usable(m)]
        if not usable or self._policy == "random":
            return None
        if self._policy == "round_robin":
            usable.sort(key=lambda m: m.endpoint)
            self._rr = (self._rr + 1) % len(usable)
            return usable[self._rr]
        if len(usable) == 1:
            return usable[0]
        a, b = random.sample(usable, 2)
        return a if self._score(a) <= self._score(b) else b

    @staticmethod
    def _score(m: _Member) -> float:
        # A member never measured, or not lately, scores 0: it is tried and
        # measured.
        if m.ewma is None or time.monotonic() - m.last_sample > _STALE_SAMPLE:
            return 0.0
        return m.ewma * (m.in_use + 1)

    def _take_idle(self, target: "_Member | None"):
        """Remove and return an idle connection (to `target`, or to any
        usable member), plus the idle connections found to belong to members
        that are no longer usable (to close outside the lock)."""
        stale = []
        found = None
        idx = range(len(self._idle)) if self._fifo else range(len(self._idle) - 1, -1, -1)
        for i in idx:
            c = self._idle[i]
            m = self._members.get(c._pool_ep)
            if m is None or not self._usable(m):
                stale.append(i)
                continue
            if target is None or m is target:
                found = i
                break
        picked = self._idle[found] if found is not None else None
        drop = set(stale)
        if found is not None:
            drop.add(found)
        stale_conns = [self._idle[i] for i in stale]
        if drop:
            self._idle = [c for i, c in enumerate(self._idle) if i not in drop]
        return picked, stale_conns

    def _order(self, first: "_Member | None" = None, exclude=None) -> "List[Tuple[str, int]]":
        """Endpoints to dial, best first: `first`, the other usable members
        in random order, then the rest (resyncing/unreachable before
        ejected, soonest re-admission first) — a dial never has nowhere to
        go while any member is configured."""
        usable = [m for m in self._members.values() if self._usable(m) and m is not first]
        random.shuffle(usable)
        rest = [m for m in self._members.values() if not self._usable(m) and m is not first]
        rest.sort(key=lambda m: (m.ejected_until is not None, m.ejected_until or 0.0))
        order = ([first] if first is not None else []) + usable + rest
        eps = [m.endpoint for m in order]
        if exclude in eps and len(eps) > 1:
            eps.remove(exclude)
            eps.append(exclude)
        return eps

    # -- what connections report (any thread) --

    def _dial_order(self, exclude=None) -> "List[Tuple[str, int]]":
        """A pooled connection re-dialing: the pool's order, the member it
        lost last."""
        with self._lock:
            return self._order(None, exclude)

    def _round_trip(self, conn: Connection, elapsed: float) -> None:
        with self._lock:
            ep = conn._connected
            if ep != conn._pool_ep:
                # The connection re-dialed to another member mid-checkout.
                old = self._members.get(conn._pool_ep)
                if getattr(conn, "_pool_out", False):
                    if old is not None and old.in_use > 0:
                        old.in_use -= 1
                    new = self._members.get(ep)
                    if new is not None:
                        new.in_use += 1
                conn._pool_ep = ep
            m = self._members.get(ep)
            if m is None:
                return
            m.errors = 0
            m.recent.append(elapsed)
            if m.ewma is None:
                m.firsts.append(elapsed)
                if len(m.firsts) >= _EWMA_SEED_SAMPLES:
                    m.ewma = min(m.firsts)
                    m.firsts.clear()
            else:
                cap = _EWMA_CLAMP * max(m.ewma, self._eject_floor)
                m.ewma += _EWMA_WEIGHT * (min(elapsed, cap) - m.ewma)
            m.samples += 1
            m.last_sample = time.monotonic()
            # Only an admitted member can be ejected for its latency: round
            # trips still finishing on connections checked out before an
            # ejection must not eject it again (and double its backoff).
            if (
                self._usable(m)
                and m.ewma is not None
                and m.samples >= _EWMA_MIN_SAMPLES
                and self._eject_factor > 0
            ):
                others = sorted(
                    o.ewma
                    for o in self._members.values()
                    if o is not m and self._usable(o) and o.ewma is not None
                    and o.samples >= _EWMA_MIN_SAMPLES
                )
                if others:
                    median = others[(len(others) - 1) // 2]
                    if m.ewma > self._eject_factor * median and m.ewma - median > self._eject_floor:
                        self._eject(
                            m,
                            f"latency {m.ewma * 1000:.1f} ms vs median {median * 1000:.1f} ms; "
                            f"slowest of the last {len(m.recent)} round trips {max(m.recent) * 1000:.1f} ms",
                        )

    def _failed(self, ep: "Tuple[str, int]") -> None:
        with self._lock:
            m = self._members.get(ep)
            if m is None:
                return
            m.errors += 1
            if m.errors >= self._eject_errors and m.ejected_until is None:
                self._eject(m, f"{m.errors} consecutive connection failures")

    def _closed_out(self, conn: Connection) -> None:
        with self._lock:
            if getattr(conn, "_pool_out", False):
                conn._pool_out = False
                m = self._members.get(conn._pool_ep)
                if m is not None and m.in_use > 0:
                    m.in_use -= 1

    def _wrote(self, conn: Connection) -> None:
        if self._ryw <= 0 or not getattr(conn, "_pool_out", False):
            return
        now = time.monotonic()
        with self._lock:
            if len(self._affinity) >= 4096:
                self._affinity = {k: v for k, v in self._affinity.items() if v[1] > now}
            self._affinity[getattr(conn, "_pool_affinity", None)] = (conn._connected, now + self._ryw)

    # -- ejection and re-admission --

    def _eject(self, m: _Member, why: str) -> None:
        """Take `m` out of rotation (caller holds the lock) — unless it is
        the last usable member: a slow or flaky member beats none."""
        if not any(self._usable(o) for o in self._members.values() if o is not m):
            return
        now = time.monotonic()
        if now - m.readmitted_at > self._backoff_max:
            m.ejections = 0  # healthy for a while: start the backoff over
        m.ejections += 1
        m.ejected_until = now + self._backoff_for(m)
        _log.warning("skaidb pool: ejecting %s (%s)", _ep_text(m.endpoint), why)
        self._ensure_maintainer()

    def _backoff_for(self, m: _Member) -> float:
        return min(self._backoff * (2 ** max(m.ejections - 1, 0)), self._backoff_max)

    def _ensure_maintainer(self) -> None:
        """Start the background thread (caller holds the lock): it re-probes
        ejected members and refreshes discovery. A daemon, so it never holds
        the interpreter open."""
        if self._maintainer is None and not self.closed:
            t = threading.Thread(target=self._maintain, name="skaidb-pool", daemon=True)
            self._maintainer = t
            t.start()

    def _maintain(self) -> None:
        while not self._stop.wait(_MAINTAIN_TICK):
            now = time.monotonic()
            with self._lock:
                due = [
                    m for m in self._members.values()
                    if m.ejected_until is not None and now >= m.ejected_until and not m.probing
                ]
                for m in due:
                    m.probing = True
                refresh = self._discover and not self._discovery_off and now >= self._next_refresh
            for m in due:
                self._probe(m)
            if refresh:
                self._refresh()

    def _probe(self, m: _Member) -> None:
        """Dial `m` alone and run one statement: success re-admits it (the
        probe connection joins the idle set), failure ejects it again for
        twice as long."""
        conn = None
        ok = False
        try:
            conn = self._dial([m.endpoint], reconnect=False)
            ok = conn.ping()
        except Error:
            ok = False
        with self._lock:
            m.probing = False
            if self.closed:
                ok = False
            elif ok:
                m.ejected_until = None
                m.errors = 0
                m.reset_latency()
                m.readmitted_at = time.monotonic()
                _log.warning("skaidb pool: re-admitted %s", _ep_text(m.endpoint))
                if conn is not None and len(self._idle) < self._maxsize:
                    conn._observer = self
                    conn._pool_ep = m.endpoint
                    conn._auto_reconnect = self._kwargs.get("auto_reconnect", True)
                    self._idle.append(conn)
                    conn = None
            else:
                m.ejections += 1
                m.ejected_until = time.monotonic() + self._backoff_for(m)
        if conn is not None:
            conn.close()

    # -- discovery --

    def _discover_through(self, conn: Connection) -> None:
        """Run discovery on a freshly dialed connection when the member it
        reached has not named itself yet (once per member, so seeds given as
        host names are matched to the endpoints the server reports)."""
        if not self._discover or self._discovery_off:
            return
        with self._lock:
            m = self._members.get(conn._pool_ep)
            if m is None or m.canonical is not None:
                return
            self._ensure_maintainer()
        self._list_through(conn)

    def _refresh(self) -> None:
        try:
            conn = self.getconn()
        except Error:
            with self._lock:
                self._next_refresh = time.monotonic() + self._refresh_interval
            return
        try:
            self._list_through(conn)
        finally:
            self.putconn(conn)

    def _list_through(self, conn: Connection) -> None:
        try:
            cur = conn.cursor()
            cur.execute("SHOW ENDPOINTS")
            rows = cur.fetchall()
        except ProgrammingError:
            # A server that predates the statement: keep the seeds.
            with self._lock:
                self._discovery_off = True
            return
        except Error:
            return
        with self._lock:
            if not rows:
                # A standalone server (or a witness): there are no members to
                # learn, now or later — keep the seeds and stop asking.
                self._discovery_off = True
                return
            self._apply_listing(conn._pool_ep, rows)
            interval = self._refresh_interval
            if any(m.listed in ("resyncing", "unreachable") for m in self._members.values()):
                interval = min(interval, _REFRESH_NOT_READY)
            self._next_refresh = time.monotonic() + interval

    def _apply_listing(self, dialed: "Tuple[str, int]", rows) -> None:
        """Fold a non-empty `SHOW ENDPOINTS` answer (caller holds the lock)."""
        listed = {}
        for endpoint, state, local in rows:
            listed[endpoint] = state
            if local:
                me = self._members.get(dialed)
                if me is not None:
                    me.canonical = endpoint
        # A member reported under the name of one already known (a seed
        # given as a host name) is that member, not a new one.
        by_name: "dict[str, _Member]" = {}
        for m in self._members.values():
            if m.canonical is not None and (m.canonical not in by_name or m.seed):
                by_name[m.canonical] = m
        for key, m in list(self._members.items()):
            if m.canonical is not None and by_name.get(m.canonical) is not m:
                del self._members[key]  # a duplicate of a seed
        port = self._kwargs.get("port", 7000)
        seen = set()
        for endpoint, state in listed.items():
            m = by_name.get(endpoint)
            if m is None:
                ep = _parse_endpoint(endpoint, port)
                m = self._members.get(ep)
                if m is None:
                    m = _Member(ep, seed=False)
                    self._members[ep] = m
                m.canonical = endpoint
            state = state if state in ("ready", "resyncing", "unreachable") else "ready"
            if state == "ready" and m.listed != "ready":
                # Back in rotation: measured afresh, like a re-admission.
                m.reset_latency()
                m.errors = 0
            m.listed = state
            seen.add(id(m))
        for key, m in list(self._members.items()):
            if id(m) in seen or (m.seed and m.canonical is None):
                continue  # listed, or a seed not identified yet
            if m.seed:
                m.listed = "unlisted"  # configured, but no longer a member
            else:
                del self._members[key]


def pool(maxsize: int = 10, **kwargs: Any) -> ConnectionPool:
    """Create a :class:`ConnectionPool`. Accepts every :func:`connect`
    keyword (``seeds``, ``database``, timeouts, auth, …) and the pool's
    load-balancing options."""
    return ConnectionPool(maxsize=maxsize, **kwargs)
