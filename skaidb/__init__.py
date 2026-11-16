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
import select
import socket
import ssl
import struct
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Any, Callable, Iterable, List, Optional, Sequence, Tuple, TypeVar

_T = TypeVar("_T")

# The package version lives in ONE place, pyproject.toml. At runtime it is read
# back from the installed distribution's metadata; the literal below is only
# the fallback for running straight from a source checkout that was never
# installed (tests/test_version.py keeps it equal to pyproject.toml). The
# server records it in its `drivers` table via the Hello frame.
_FALLBACK_VERSION = "1.5.1"


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
    "TransactionConflict",
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


class TransactionConflict(ProgrammingError):
    """The server refused a ``COMMIT``: another writer changed a row the
    transaction writes after its ``BEGIN``.

    Nothing was applied, the transaction is over, and the connection is back
    in autocommit and stays usable, so running the whole transaction again
    from ``BEGIN`` is safe — :meth:`Connection.transaction` does exactly
    that. The message is the server's, unchanged."""


# A server Error whose message starts with this is a refused COMMIT
# (PROTOCOL.md §6). Only the prefix is contract: the rest of the text may
# change.
_CONFLICT_PREFIX = "transaction conflict:"


def _server_error(msg: str) -> ProgrammingError:
    """The exception for a server ``Error`` frame — a statement error, which
    leaves the connection usable. `msg` is the server's message, unchanged."""
    if msg.startswith(_CONFLICT_PREFIX):
        return TransactionConflict(msg)
    return ProgrammingError(msg)


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


# A connection lost with a transaction open on it (PROTOCOL.md §6 rule 7):
# the transaction went with the connection, and nothing of it is sent on
# another.
_LOST_IN_TRANSACTION = (
    "connection lost inside a transaction: the transaction ended with the "
    "connection and nothing of it was committed; this statement was not "
    "sent. ROLLBACK ends the transaction here — then run it again"
)
_LOST_BEFORE_COMMIT = (
    "connection lost inside the transaction: nothing was committed — the "
    "transaction ended with the connection, and COMMIT was not sent; run "
    "the transaction again"
)
_LOST_UNDER_COMMIT = (
    "connection lost under COMMIT: the outcome is unknown — the server may "
    "have committed the transaction before its reply went missing"
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


# ---- The session database ---------------------------------------------------


def _used_database(sql: str) -> "str | None":
    """The database a ``USE [DATABASE] <name>`` statement names, when `sql`
    is exactly one such statement (a trailing ``;`` allowed, ``--`` comments
    skipped): the name as written, or with its double quotes removed and the
    doubled quotes inside undone. None for anything else — every driver
    reads a ``USE`` by these rules (PROTOCOL.md §6 rule 9)."""
    sql = _without_line_comments(sql)
    rest = _strip_keyword(sql.strip().rstrip(";").rstrip(), "USE")
    if rest is None:
        return None
    named = _strip_keyword(rest, "DATABASE")
    if named is not None:
        rest = named
    if rest.startswith('"'):
        inner = rest[1:-1]
        # A lone quote inside would end the name early: not one name.
        if len(rest) < 2 or not rest.endswith('"') or not inner or '"' in inner.replace('""', ""):
            return None
        return inner.replace('""', '"')
    if rest and all(ch.isalnum() or ch == "_" for ch in rest):
        return rest
    return None


def _without_line_comments(sql: str) -> str:
    """`sql` without its ``--`` comments (each runs to the end of its line),
    leaving alone a ``--`` inside a double-quoted name or a quoted string.
    The server accepts ``USE app -- the app database``, and a ``USE`` the
    driver cannot read is one it does not carry to the next connection."""
    if "--" not in sql:
        return sql
    out = []
    quote = None
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if quote is not None:
            out.append(ch)
            if ch == quote:
                quote = None
        elif sql.startswith("--", i):
            i = sql.find("\n", i)
            if i < 0:
                break
            out.append("\n")
        else:
            out.append(ch)
            if ch in "\"'":
                quote = ch
        i += 1
    return "".join(out)


def _strip_keyword(text: str, word: str) -> "str | None":
    """`text` after the leading keyword `word` (any case) and the whitespace
    that must follow it; None when it does not start with one."""
    head, tail = text[: len(word)], text[len(word) :]
    if head.isascii() and head.upper() == word and tail[:1].isspace():
        return tail.lstrip()
    return None


# ---- The session's state ----------------------------------------------------

# The first word of every statement the driver notes (`_session_statement`).
_SESSION_WORDS = frozenset(("USE", "SET", "BEGIN", "COMMIT", "ROLLBACK"))


def _session_statement(sql: str) -> "tuple[str, str | None] | None":
    """What `sql` does to the state of its session, when it is exactly one
    statement that the driver notes — a session's state lives on the
    connection, so the driver keeps a note of it and puts it back on every
    later dial (PROTOCOL.md §6 rule 9). `(kind, value)`:

    * ``("use", database)`` — ``USE [DATABASE] <name>`` (`_used_database`);
    * ``("consistency", text)`` — ``SET CONSISTENCY …``: `text` is the
      statement, comments and the trailing ``;`` stripped, to send again;
      None when the level is ``DEFAULT``, which clears the override;
    * ``("scan_budget", text)`` — ``SET SCAN BUDGET …``, the same way;
    * ``("begin", None)`` — a statement whose first word is ``BEGIN``
      (``BEGIN``, ``BEGIN TRANSACTION``, ``BEGIN ATOMIC …``): it opens a
      transaction, which lives on the connection too (rule 7);
    * ``("commit", None)`` / ``("rollback", None)`` — exactly ``COMMIT
      [TRANSACTION]`` / ``ROLLBACK [TRANSACTION]``, which end it.

    None for anything else: the driver reads the first words and no
    further, and leaves alone what is not exactly one such statement."""
    i, n = 0, len(sql)
    while i < n and sql[i].isspace():
        i += 1
    if not sql.startswith("--", i):
        # Any other statement is told apart by its first word, without a
        # pass over its text.
        j = i
        while j < n and j - i < 9 and sql[j].isalpha():
            j += 1
        if sql[i:j].upper() not in _SESSION_WORDS:
            return None
    database = _used_database(sql)
    if database is not None:
        return ("use", database)
    text = _without_line_comments(sql).strip()
    if text.endswith(";"):
        text = text[:-1].rstrip()
    words = [w.upper() if w.isascii() else "" for w in text.split(None, 4)[:4]]
    if words[:1] == ["BEGIN"]:
        return ("begin", None)
    for end in ("COMMIT", "ROLLBACK"):
        if words in ([end], [end, "TRANSACTION"]):
            return (end.lower(), None)
    if words[:2] == ["SET", "CONSISTENCY"]:
        kind, level = "consistency", words[2:3]
    elif words[:3] == ["SET", "SCAN", "BUDGET"]:
        kind, level = "scan_budget", words[3:4]
    else:
        return None
    if ";" in text:  # more than one statement
        return None
    return (kind, None if level == ["DEFAULT"] else text)


# ---- A look at an idle socket ------------------------------------------------


def _readable(sock) -> bool:
    """Whether a read on `sock` would return at once — data, the peer's
    close or an error — asked of the operating system without waiting."""
    if hasattr(select, "poll"):  # no limit on the descriptor's number
        poller = select.poll()
        poller.register(sock, select.POLLIN)
        return bool(poller.poll(0))
    return bool(select.select([sock], [], [], 0)[0])


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

    Each statement commits on its own unless a transaction is open on the
    connection. :meth:`transaction` runs one — ``BEGIN``, your statements,
    ``COMMIT`` — and runs it again when the server refuses the commit with a
    :class:`TransactionConflict`; ``BEGIN``/``COMMIT``/``ROLLBACK`` can also
    be sent as ordinary statements, and the connection notes the
    transaction they open: while it is open a lost connection is not
    replaced. The DB-API ``commit()`` and ``rollback()`` end that
    transaction, and send nothing when none is open.
    """

    _nonce_counter = 0
    _nonce_lock = threading.Lock()
    # Class-level defaults: health state must have an answer even on an
    # instance built without __init__ (a subclass wiring its own transport).
    _broken = False
    _streaming = False
    # True while `transaction()` has a transaction open here. The transaction
    # lives on this socket, so nothing re-dials meanwhile (see `_run`).
    _txn_open = False
    # True while a transaction the driver has noted is open here: from a
    # `BEGIN` the server accepted until `COMMIT` or `ROLLBACK` is answered
    # (`_note_session`) — whoever sent them, the application or
    # `transaction()`.
    _txn_noted = False
    # An instance built without __init__ wires its own transport and cannot
    # be re-dialed, so it does not auto-reconnect.
    _auto_reconnect = False
    _cert_auth = False
    _connected = None
    _database = None
    _session_consistency = None
    _session_scan_budget = None
    # The part of the session's state the statement being run sets itself
    # (`_stating`), and whether a dial made for it left that part out
    # (`_restore_session`).
    _stating = None
    _unrestored = False
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
        # drain, leaves the socket out of sync — the socket is closed with it
        # (`_mark_broken`); a pool checks `is_usable()` and discards the
        # connection instead of reusing it.
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
        # emptied whenever the socket is dropped (`_drop_socket`).
        self._prepared: "dict[str, tuple[int, int]]" = {}
        self._endpoints: List[Tuple[str, int]] = list(endpoints)
        self._user = user
        self._password = password
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        # The database the session is in, as far as the driver knows: the
        # one asked for here, then the one named by the last `USE` the
        # server answered with success (`_note_session`). Every dial enters
        # it before anything else is sent — a new session starts in none.
        self._database = database
        # The session's other settings, noted the same way: the last
        # `SET CONSISTENCY …` and the last `SET SCAN BUDGET …` the server
        # accepted, as the statement to send again on every dial; None when
        # there is none, or once `… DEFAULT` cleared it. A new session has
        # neither: without the note a statement after a re-dial would read
        # at another consistency, or past the limit the application set.
        self._session_consistency: "str | None" = None
        self._session_scan_budget: "str | None" = None
        # And a transaction, which is not put back: a connection lost while
        # one is open is not replaced until the application has ended it
        # (`_lost_before`).
        self._txn_noted = False
        self._tls_ctx = tls_ctx
        self._tls_server_name = tls_server_name
        self._sock = None
        self._file = None
        self._open()

    def _open(self) -> None:
        """Dial the endpoints in order until one connects and authenticates,
        then put the session's state back (`_restore_session`): its
        database, its consistency override, its scan budget. Raises
        ``OperationalError`` if no endpoint is reachable.

        A dial that does not finish leaves the connection broken, with no
        socket — whatever stopped it: no endpoint, a `USE` or a setting the
        server refused (the session is in its default database, or under
        its default settings, not the ones asked for), or an interrupt half
        way through the handshake or before the Hello was acknowledged (see
        `_exchange`). The next statement then dials again instead of
        running on a socket that is not ready."""
        try:
            self._establish()
        except BaseException:
            self._mark_broken()
            raise

    def _establish(self) -> None:
        errors = []
        for host, port in self._endpoints:
            # Once the handshake is through, a failure is one of a round
            # trip, which reports to the pool itself (`_exchange`).
            authenticated = False
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
                authenticated = True
                # Connected + authenticated: a fresh socket has no prepared
                # statements, no stream in flight and is not broken.
                self._connected = (host, port)
                self._broken = False
                self._streaming = False
                self._prepared.clear()
                # The dial is not done until the Hello is answered and the
                # session is put back. A member that fails under those is
                # one more endpoint that did not work; what a member
                # ANSWERS — a database it does not have — ends the dial.
                self._send_hello()
                self._restore_session()
            except (OSError, OperationalError) as e:
                errors.append(f"{host}:{port}: {e}")
                self._drop_socket()
                if self._observer is not None and not authenticated and _member_fault(e):
                    self._observer._failed((host, port))
                continue
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
        # A stream was in flight on that socket and on no other: its
        # `RowStream` sees the socket gone (`RowStream._stale`) and neither
        # reads nor releases anything from here on.
        self._streaming = False
        # Prepared ids are slots of the session that socket carried: on the
        # next one they are unknown, or name whatever was prepared there
        # since. Emptied here rather than at the next dial, because the
        # statement that re-dials a broken connection looks its id up first
        # (`_execute_params`) and would send the old one to the new session.
        self._prepared.clear()

    def _mark_broken(self) -> None:
        """The socket is dead or out of step with the server: mark the
        connection broken and close the socket now, not at the next
        statement or at `close()`. That ends the session on the server —
        and with it a transaction open there, and the rows it holds — and
        leaves no unread frame for a later statement to take for its own
        reply."""
        self._broken = True
        self._drop_socket()

    def _send_hello(self) -> None:
        """Self-identification (fills the server's ``drivers`` table
        ``client_name``/``client_version``). What the server answers is
        ignored — an old server rejects the opcode with an error frame, and
        identity is telemetry, never load-bearing.

        That it answers is not: the Hello is a round trip like any other.
        A reply that does not come leaves a socket that is dead, or one
        with an answer still on its way, which the first statement would
        find — after it was sent, so a retry runs it twice — or read as its
        own. So a transport failure here is a failed dial
        (``OperationalError``, the socket closed), and the next endpoint
        is tried."""
        name = b"python"
        ver = __version__.encode()
        req = bytearray([_OP_HELLO])
        req += struct.pack("<I", len(name)) + name
        req += struct.pack("<I", len(ver)) + ver
        try:
            self._exchange(bytes(req))  # Ddl ack, or an old server's Error — either way, done.
        except OperationalError as e:
            raise OperationalError(f"no reply to Hello ({e})") from e

    def _restore_session(self) -> None:
        """Put the state of the session back on a connection just dialed,
        before anything else is sent on it: the database, then the noted
        `SET CONSISTENCY`, then the noted `SET SCAN BUDGET` — each only if
        there is one. A statement the server refuses here is raised, and
        the dial has not finished (`_open`): no statement runs in another
        database, or under other settings, than the application set.

        Except the part the statement this dial is made for sets itself
        (`_stating`): a `USE` does not first enter the database it leaves.
        That database may be gone — dropped while the connection was down —
        and then no statement could ever run again, the `USE` that leaves
        it included. The statement's own answer decides that part; until
        it has, the session is short of it (`_unrestored`)."""
        self._unrestored = False
        if self._database is not None:
            if self._stating == "use":
                self._unrestored = True
            else:
                self._use_database(self._database)
        for kind, setting in (
            ("consistency", self._session_consistency),
            ("scan_budget", self._session_scan_budget),
        ):
            if setting is None:
                continue
            if self._stating == kind:
                self._unrestored = True
            else:
                self._query(setting, self._consistency)

    def _use_database(self, db: str) -> None:
        """Enter `db`, and remember it as the session's database."""
        ident = '"' + db.replace('"', '""') + '"'
        self._query("USE " + ident, self._consistency)
        self._database = db

    def reconnect(self) -> None:
        """Re-dial (failing over across endpoints), re-authenticate and
        enter the session's database again, discarding any per-connection
        prepared statements. With ``auto_reconnect`` the connection calls
        this itself, before a statement on a broken connection and after one
        whose connection failed in transit.

        Refused with ``ProgrammingError`` while :meth:`transaction` has its
        transaction open: the transaction lives on the socket it began on,
        so after a re-dial the rest of the body would commit statement by
        statement and the ``COMMIT`` would reach a session with nothing to
        commit."""
        if self.closed:
            raise ProgrammingError("connection is closed")
        if self._txn_open:
            raise ProgrammingError(
                "reconnect() inside transaction(): the transaction is on the "
                "connection it began on, and on a new one the statements "
                "that follow would each commit on their own; let the error "
                "end the transaction and run it again"
            )
        if self._txn_noted:
            raise ProgrammingError(
                "reconnect() with a transaction open: the transaction is on "
                "the connection it began on, and on a new one the statements "
                "that follow would each commit on their own; end it first "
                "(ROLLBACK — answered here once the connection is lost) and "
                "run it again"
            )
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
        stream, not currently mid-stream, and not closed by its peer while
        it sat idle (`_gone`; a connection found so is marked broken).
        Cheap (no round-trip).

        The mid-stream case matters because this doubles as a pool's health
        check: a connection returned to the pool while a `RowStream` is still
        live has chunks in flight, and the next caller would read them as the
        answer to its own statement."""
        if self.closed or self._broken or self._streaming:
            return False
        # Only an idle socket can be looked at: with a round trip in flight
        # (another thread holds the lock) a reply is what is readable.
        lock = getattr(self, "_lock", None)
        if lock is None or not lock.acquire(False):
            return True
        try:
            gone = not self._broken and not self._streaming and self._gone()
            if gone:
                self._mark_broken()
        finally:
            lock.release()
        if gone:
            self._observe_failure()
        return not gone

    def _look(self) -> None:
        """Before a statement's first request: if the peer closed the
        connection while it sat idle, mark it broken now, with nothing
        sent. A member's restart, a failover or an idle timeout closes idle
        connections, and over a real network the write to such a socket
        still succeeds — the reset has not come back yet — so only the read
        fails, after the statement left: one that may not be sent again
        then fails, and one that is sent again may run twice. Found here,
        the connection is replaced exactly as after a failed write, before
        anything is written."""
        with self._lock:
            if self.closed or self._broken or self._streaming:
                return
            if not self._gone():
                return
            self._mark_broken()
        self._observe_failure()

    def _gone(self) -> bool:
        """Whether the socket of this idle connection is dead: one readiness
        poll that does not wait, and no round trip. The server sends nothing
        on an idle connection, so a socket that is readable holds its close,
        a reset, or bytes nobody asked for — after any of them it cannot
        carry a request.

        Under TLS the socket polled is the transport's, and TLS has records
        of its own (a session ticket, a key update): a read that does not
        wait tells them, which it consumes, from a close or stray data.
        Bytes already decrypted and not read count as stray too."""
        sock = self._sock
        if sock is None or not hasattr(sock, "fileno"):
            return False  # no socket of the operating system's to look at
        tls = isinstance(sock, ssl.SSLSocket)
        try:
            if tls and sock.pending() > 0:
                return True
            if not _readable(sock):
                return False
            timeout = sock.gettimeout()
            sock.settimeout(0)  # the read below must not wait either
            try:
                if tls:
                    sock.recv(1)  # b"": the peer closed; a byte: stray data
                else:
                    sock.recv(1, socket.MSG_PEEK)
                return True
            except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
                # Nothing for this connection after all: a spurious wake-up,
                # or records TLS keeps to itself.
                return False
            finally:
                sock.settimeout(timeout)
        except (OSError, ValueError):  # reset, or closed under us
            return True

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

    @contextmanager
    def _statement(self, sql: str):
        """The span of one statement of the application, from before its
        first request to its answer.

        It starts with a look at the socket (`_look`): a connection that
        died while it sat idle is replaced before the statement is written,
        not after it failed.

        A statement that sets part of the session's state itself — a `USE`,
        a `SET CONSISTENCY`, a `SET SCAN BUDGET` — says so for the dials
        made on its behalf, which then leave that part alone
        (`_restore_session`). If such a dial happened and the statement
        was not answered with success, the new session has neither the old
        state nor a new one: the connection is marked broken, so the next
        statement dials again and puts all of it back — or fails with the
        error of what cannot be put back. It never runs outside the
        database, or the settings, the driver has noted."""
        noted = _session_statement(sql)
        what = noted[0] if noted else None
        outer, self._stating = self._stating, what
        try:
            self._look()
            yield what
        except BaseException:
            if self._unrestored:
                self._mark_broken()
            raise
        finally:
            self._stating = outer
            self._unrestored = False

    def _run(self, sql, fn):
        """Run the statement `sql`, which `fn` sends; if its connection
        fails in transit, re-dial (walking the other endpoints first) and
        run it once more.

        The retry repeats the WHOLE statement — prepare included, since
        prepared ids die with the connection — and happens at most once. A
        statement the old node had already applied can therefore run twice
        (at-least-once on failover, as in the Rust driver); an INSERT on an
        existing primary key then fails instead of duplicating. Pass
        ``auto_reconnect=False`` to :func:`connect` to surface the failure
        instead.

        Inside :meth:`transaction` the failure always surfaces: on a fresh
        connection the statement would run outside the transaction and
        commit on its own."""
        with self._statement(sql) as what:
            if self._lost_before(what):
                return ("ddl", 0, None)  # ROLLBACK, answered here
            try:
                return fn()
            except OperationalError as e:
                if self._in_transaction():
                    if self._broken and self._lost_under(what, e):
                        return ("ddl", 0, None)
                    raise
                if (
                    not (self._auto_reconnect and self._broken)
                    or self.closed
                    or self._streaming
                ):
                    raise
                self.reconnect()
                return fn()

    def _in_transaction(self) -> bool:
        """Whether a transaction is open on this connection, as far as the
        driver knows: one it noted (`_txn_noted`), or `transaction()`'s."""
        return self._txn_noted or self._txn_open

    def _lost_before(self, what: "str | None") -> bool:
        """The statement of kind `what` is about to be sent: is this a
        connection lost with a transaction open (PROTOCOL.md §6 rule 7)?
        Then nothing is sent — on this connection or on a new one, where
        the statement would commit on its own — and the application ends
        the transaction here:

        * its ``ROLLBACK`` is answered with success (True is returned):
          the server discarded the transaction with the connection;
        * its ``COMMIT`` fails, saying that nothing was committed;
        * anything else fails with the connection-lost error.

        Either end clears the note, and the next statement dials again as
        after any lost connection. False when there is nothing of the
        kind, and the statement goes out."""
        if not (self._broken and self._in_transaction()) or self.closed:
            return False
        if what == "rollback":
            self._txn_noted = False
            return True
        if what == "commit":
            self._txn_noted = False
            raise OperationalError(_LOST_BEFORE_COMMIT)
        raise OperationalError(_LOST_IN_TRANSACTION)

    def _lost_under(self, what: "str | None", error: OperationalError) -> bool:
        """The connection was lost under a statement of kind `what` with a
        transaction open. It is not replaced, and the statement is not sent
        again. Under the ``COMMIT`` itself the outcome is unknown — the
        server may have applied it before its reply went missing — and the
        error raised says so; under the ``ROLLBACK`` the transaction is
        gone either way, which is what was asked (True is returned). Both
        end the transaction. Under any other statement False is returned
        and the caller raises `error`: nothing was committed, and the
        transaction stays noted until the application ends it
        (`_lost_before`)."""
        if what == "commit":
            self._txn_noted = False
            raise OperationalError(f"{_LOST_UNDER_COMMIT} ({error})") from error
        if what == "rollback":
            self._txn_noted = False
            return True
        return False

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
            raise _server_error(r.text())
        raise InterfaceError(f"unknown response tag {tag}")

    def _roundtrip(self, req: bytes) -> "_Reader":
        """Send one request frame and read one response frame under the
        connection lock. Transport failures surface as `OperationalError` so a
        pool can recycle the connection cleanly."""
        self._check_ready()
        started = time.monotonic()
        with self._lock:
            # Re-check under the lock: `_stream_open` claims the
            # connection while holding it, so this is where a statement
            # racing a stream on another thread is ordered.
            if self._streaming:
                raise ProgrammingError(_BUSY_STREAMING)
            frame = self._exchange(req)
        if self._observer is not None:
            self._observer._round_trip(self, time.monotonic() - started)
        return _Reader(frame)

    def _exchange(self, req: bytes) -> bytes:
        """Send one request frame and read the first frame of its reply (the
        caller holds the lock).

        A reply carries no request id: one that is not read to its end is
        taken for the answer to the next request. So whatever stops this
        between the first byte sent and the last byte read costs the
        connection — it is marked broken and its socket closed before the
        exception goes on. That holds for a transport failure, raised as
        `OperationalError`, and just as much for anything raised into the
        middle of it from outside, which propagates unchanged: a
        `KeyboardInterrupt`, a `SystemExit` or any other exception thrown by
        a signal handler, a green thread's timeout or cancellation."""
        try:
            self._write_frame(req)
            return self._read_frame()
        except OperationalError:
            # A framing-level error (e.g. server closed the socket) leaves the
            # connection unusable; mark it so pools recycle it.
            self._mark_broken()
            self._observe_failure()
            raise
        except OSError as e:  # mid-query socket error: dead peer, timeout, ...
            self._mark_broken()
            self._observe_failure()
            raise OperationalError(f"query transport failed: {e}") from e
        except BaseException:
            self._mark_broken()
            raise

    def _observe_failure(self) -> None:
        if self._observer is not None and self._connected is not None:
            self._observer._failed(self._connected)

    def _note_statement(self, sql: str) -> None:
        """A statement succeeded: a pool with read-your-writes on remembers
        which member took a write."""
        if self._observer is not None and _is_write(sql):
            self._observer._wrote(self)

    def _note_session(self, sql: str, refused: bool = False) -> None:
        """The server answered `sql` — with success, or with an error if
        `refused`: if the statement changes the state of the session
        (`_session_statement`), note it.

        What a statement answered with success set is what every later
        dial of this connection puts back (`_restore_session`) — the
        database a ``USE`` entered, the ``SET CONSISTENCY`` or ``SET SCAN
        BUDGET`` in force (none once ``… DEFAULT`` cleared it). Sent as
        ordinary statements they used to be the server's knowledge alone:
        after a re-dial the session was in no database and under no
        setting, and the next statement ran against whatever table of that
        name the default database holds, at the request's own consistency
        and with no scan limit.

        A transaction is noted too, from a ``BEGIN`` answered with success
        until its ``COMMIT`` or ``ROLLBACK`` is answered at all. It is not
        put back — it cannot be: while one is open the connection is not
        replaced (`_run`, `_check_ready`)."""
        noted = _session_statement(sql)
        if noted is None:
            return
        kind, value = noted
        if kind in ("commit", "rollback"):
            # Answered: the transaction is over, also when the answer is an
            # error — a refused COMMIT has ended it too (rule 6).
            self._txn_noted = False
        elif refused:
            return
        elif kind == "begin":
            self._txn_noted = True
        elif kind == "use":
            self._database = value
        elif kind == "consistency":
            self._session_consistency = value
        elif kind == "scan_budget":
            self._session_scan_budget = value

    def _check_ready(self) -> None:
        """Refuse to start a request on a connection that cannot carry one.
        A broken connection is refused rather than retried because its socket
        was out of step with the server (and is closed, see `_mark_broken`):
        a request written into it would have read someone else's answer
        back, which is far more confusing than an error here."""
        if self.closed:
            raise ProgrammingError("connection is closed")
        if self._broken and self._in_transaction():
            # The transaction died with the old socket: on a new one this
            # request would run outside it, and commit on its own.
            raise OperationalError(_LOST_IN_TRANSACTION)
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
        r = self._roundtrip(req)
        # Noted here, where the text is the one that was sent: a statement
        # with parameters the server does not prepare arrives through
        # `_execute_params` with its values written in.
        try:
            result = self._parse_result(r)
        except ProgrammingError:  # the server's Error frame
            self._note_session(sql, refused=True)
            raise
        self._note_session(sql)
        return result

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
            raise _server_error(msg)
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
        """Best-effort free of a server-side prepared slot (it replies Ddl).
        On a broken connection there is nothing to free — the slot went with
        the socket — and nothing is sent: starting a request would re-dial,
        which would hide the failure from the retry in `_run` and close an
        id the new session never issued."""
        if self._broken:
            return
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
        with self._statement(sql) as what:
            if self._lost_before(what):
                return RowStream(self, [], 0, done=True)  # ROLLBACK, answered here
            try:
                return self._stream(sql, req, bool(params))
            except OperationalError as e:
                if self._in_transaction() and self._broken and self._lost_under(what, e):
                    return RowStream(self, [], 0, done=True)
                raise

    def _stream(self, sql: str, req: bytes, parameterised: bool) -> "RowStream":
        """Send the stream request `req` for `sql` and make the
        :class:`RowStream` of its reply (see `stream`)."""
        r = self._stream_open(req)
        # Only a RowsHeader has more frames behind it; until the first frame
        # turns out to be the whole reply, the rest is still on the socket.
        whole = False
        try:
            self._note_statement(sql)
            tag = r.u8()
            whole = tag in (_RESP_ERROR, _RESP_MUTATION, _RESP_DDL, _RESP_RESULT_SETS)
            if tag == _RESP_ERROR:
                msg = r.text()
                self._note_session(sql, refused=True)
                if "unknown opcode" in msg:
                    what = "parameterised streaming (skaidb >= 0.330)" if parameterised else "streaming"
                    raise ProgrammingError(f"server does not support {what}: {msg}")
                raise _server_error(msg)
            if tag in (_RESP_MUTATION, _RESP_DDL):
                self._note_session(sql)  # session or transaction control, accepted
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
                raise InterfaceError(f"unexpected response tag {tag} to stream request")
            columns = [r.text() for _ in range(r.u32())]
            return RowStream(self, columns, 0, done=False)
        except BaseException:
            if whole:
                # The exchange is over and nothing will consume a stream, so
                # release the claim here.
                self._stream_release()
            else:
                # No stream goes out to read what follows the header — it
                # could not be parsed, or the call was interrupted — or the
                # reply is one this driver does not know, made of nobody
                # knows how many frames: the socket position is lost.
                self._mark_broken()
            raise

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
            # A failure closes the socket, which also ends the claim.
            frame = self._exchange(req)
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
        try:
            if len(params) != nparams:
                raise ProgrammingError(
                    f"statement expects {nparams} parameters, got {len(params)}"
                )
            return self._execute_prepared(stmt_id, params, consistency)
        finally:
            if not cached:
                # Past the cache the statement is not kept, so its slot on
                # the server is given back whatever became of it — also when
                # it was never executed: slots that leak fill the server's
                # table (256 per connection), which then refuses every new
                # statement.
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

    def transaction(
        self,
        body: "Callable[[Connection], _T]",
        attempts: int = 3,
        begin: str = "BEGIN",
    ) -> "_T":
        """Run ``body(conn)`` inside a transaction on this connection and
        return what it returned::

            def move(conn):
                conn.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?", (10, 1))
                conn.execute("UPDATE accounts SET balance = balance + ? WHERE id = ?", (10, 2))

            conn.transaction(move)

        Sends ``begin``, calls ``body`` with this connection, then sends
        ``COMMIT``. ``begin`` is the opening statement, sent verbatim: plain
        ``BEGIN`` on a standalone server, ``BEGIN ATOMIC`` or ``BEGIN ATOMIC
        PARTITION (col = lit)`` on a cluster.

        * If ``begin`` fails, its error is raised and nothing else is sent:
          the body does not run, because each of its statements would commit
          on its own.
        * If the body raises, ``ROLLBACK`` is sent — whatever it answers is
          ignored — and the body's exception propagates unchanged. Nothing
          is retried, whatever the exception is.
        * If the server refuses the ``COMMIT`` with a
          :class:`TransactionConflict` (another writer changed a row the
          transaction writes; nothing was applied), the whole transaction
          runs again from ``begin``, at once, up to ``attempts`` runs in
          all; the last run's conflict is raised. So ``body`` must be safe
          to run more than once, and should do nothing outside the database
          that a retry would repeat.
        * Any other ``COMMIT`` error is raised as it is: the transaction is
          already over, so there is no retry and no ``ROLLBACK``.

        ``attempts`` below 1 raises ``ValueError`` before anything is sent.
        Once ``begin`` has been answered, a transport failure is the usual
        ``OperationalError`` and nothing is retried, by the automatic
        reconnect either: the transaction was on the connection that failed.
        (``begin`` itself re-dials like any statement — nothing is open
        yet.) For the same reason the body cannot call :meth:`reconnect`
        (``ProgrammingError``): let the failure end the transaction and run
        it again. Finish or close any :class:`RowStream` the body opens
        before it returns: while one is open neither ``COMMIT`` nor
        ``ROLLBACK`` can be sent, and the connection is marked broken
        rather than left inside an open transaction.
        """
        if attempts < 1:
            raise ValueError("attempts must be >= 1")
        left = attempts
        while True:
            left -= 1
            # A refused `begin` opened nothing: going on would autocommit
            # each of the body's statements, so its error ends the call.
            self.execute(begin)
            outer, self._txn_open = self._txn_open, True
            try:
                try:
                    result = body(self)
                except BaseException:
                    # The body's error is the one raised, whatever ROLLBACK
                    # answers (a server before 0.347 refuses a ROLLBACK with
                    # nothing open) and even if it cannot be sent.
                    try:
                        self.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise
                try:
                    self.execute("COMMIT")
                except TransactionConflict:
                    # Refused: nothing was applied and the transaction is
                    # over. The other writer has already finished, so the
                    # next run starts at once and reads the rows as they
                    # are now.
                    if left < 1:
                        raise
                else:
                    return result
            finally:
                self._txn_open = outer
                if self._streaming:
                    # A RowStream the body left open kept COMMIT / ROLLBACK
                    # from being sent, so the transaction is still open on
                    # this socket: it must not carry another statement, and
                    # closing it is what ends the transaction on the server.
                    self._mark_broken()
                if self._broken:
                    # Whatever cost the connection ended the transaction
                    # with it, and this call is what reports that: the
                    # connection dials again at its next statement.
                    self._txn_noted = False

    def commit(self) -> None:
        """DB-API ``commit()``: commit the transaction open on this
        connection — one begun with a ``BEGIN`` statement, which the driver
        has noted — by sending ``COMMIT``, and raise what that raises (a
        :class:`TransactionConflict` when the server refuses it; after a
        lost connection, the error that says nothing was committed).

        With no transaction open it is accepted and sends nothing: each
        statement has committed on its own. So it is safe to call as a
        matter of routine, as DB-API frameworks do.

        Inside :meth:`transaction` it sends nothing either: the helper
        commits when the body returns."""
        if self._txn_noted and not self._txn_open:
            self.execute("COMMIT")

    def rollback(self) -> None:
        """DB-API ``rollback()``: roll back the transaction open on this
        connection — one begun with a ``BEGIN`` statement, which the driver
        has noted — by sending ``ROLLBACK``. After a lost connection the
        transaction is gone already, and it is ended here without a
        statement.

        With no transaction open it is accepted and sends nothing, as the
        server accepts a ``ROLLBACK`` with nothing open: there is nothing
        to roll back — every statement has committed on its own — and the
        clean-up after a failed ``COMMIT`` or ``BEGIN`` does not replace
        that error with one of its own.

        Inside :meth:`transaction` it raises ``OperationalError`` and sends
        nothing: the helper ends its transaction, and a body rolls back by
        raising."""
        if self._txn_open:
            raise OperationalError(
                "rollback() inside transaction(): the helper ends its "
                "transaction — raise from the body to roll it back"
            )
        if self._txn_noted:
            self.execute("ROLLBACK")

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

    A stream belongs to the socket it was opened on. Once the connection has
    closed that socket or re-dialed (`close()`, `reconnect()`, the automatic
    reconnect after a failure) the rest of the result is gone: iterating
    past the rows already received raises `OperationalError`, and `close()`
    does nothing — it never reads the connection's next socket.

    A `CALL` whose procedure body `EMIT`s result sets is answered with all of
    them in one frame, streamed or not: such a stream owns nothing, starts on
    the first set, and `nextset()` walks the rest as `Cursor.nextset()` does.
    """

    def __init__(self, conn: Connection, columns: List[str], affected: int, done: bool,
                 sets: "Optional[list]" = None):
        self._conn = conn
        # The read side of the socket the frames arrive on (see `_stale`).
        self._file = conn._file
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

    def _stale(self) -> bool:
        """True once the connection is no longer on the socket this stream
        was opened on: it was closed, or re-dialed. What the connection reads
        now is another statement's reply, and the claim on it — if any —
        another stream's."""
        return self._conn._file is not self._file

    def _finish(self) -> None:
        """Mark the exchange over and hand the connection back. Idempotent,
        because close() and the iterator both reach it."""
        self._done = True
        if not self._released:
            self._released = True
            if not self._stale():
                self._conn._stream_release()

    def _next_frame(self) -> None:
        """Read the next stream frame into the row buffer, or end the stream."""
        conn = self._conn
        if self._stale():
            self._finish()
            raise OperationalError(
                "the connection this stream was reading was closed or "
                "re-dialed; the rest of its result is lost"
            )
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
            conn._mark_broken()
            self._finish()
            raise
        except OSError as e:
            conn._mark_broken()
            self._finish()
            raise OperationalError(f"stream transport failed: {e}") from e
        except BaseException:
            # A frame we could not parse (`InterfaceError`): we no longer
            # know what the peer is sending. Or the read, or the decoding of
            # a chunk, was interrupted from outside (see
            # `Connection._exchange`): part of a frame is still on the
            # socket, or a chunk's rows were dropped. Either way the
            # connection is not reusable and the stream is over — going on
            # would read from the middle of a frame, or skip rows silently.
            conn._mark_broken()
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
            raise _server_error(fr.text())
        conn._mark_broken()
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
        `None`.

        Nothing is read once the connection has closed or re-dialed the
        socket the stream was opened on: the frames there now are not this
        stream's."""
        if self._released:
            return
        self._rows = iter(())
        conn = self._conn
        # Nothing to drain if the stream ran out or its socket is gone.
        if not self._done and not self._stale():
            try:
                for _ in range(_STREAM_DRAIN_MAX_FRAMES):
                    payload = conn._read_frame()
                    tag = payload[0] if payload else None
                    if tag in (_RESP_ROWS_END, _RESP_ERROR):
                        break
                    if tag != _RESP_ROWS_CHUNK:
                        conn._mark_broken()
                        break
                else:
                    conn._mark_broken()
            except (OperationalError, OSError):
                conn._mark_broken()
            except BaseException:
                # Interrupted in the middle of the drain: the socket is not
                # at a request boundary, and a second close() must not
                # pick the drain up from the middle of a frame.
                conn._mark_broken()
                self._finish()
                raise
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
                sql, lambda: conn._execute_params(sql, params, self._consistency)
            )
        else:
            # No parameters: one-shot query. `_bind` still rejects a stray `?`.
            text = _bind(sql, None)
            kind, a, b = conn._run(sql, lambda: conn._query(text, self._consistency))
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
        if _session_statement(sql) is not None:
            # Session and transaction control is never prepared: one
            # statement per row, each handled as `execute` handles it.
            self._executemany_loop(sql, rows)
        else:
            self.connection._run(sql, lambda: self._executemany_once(sql, rows))
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
        try:
            for i, params in enumerate(rows):
                if len(params) != nparams:
                    raise ProgrammingError(
                        f"row {i}: statement expects {nparams} parameters, got {len(params)}"
                    )
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
    of connecting, so the session starts in the right database; the
    connection enters its database again on every re-dial — this one, or the
    one named by the last ``USE`` statement the server accepted — and sends
    again the last ``SET CONSISTENCY`` and ``SET SCAN BUDGET`` statements
    the server accepted, unless ``… DEFAULT`` cleared them. ``timeout`` is
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
    * Every borrower gets a connection in the pool's own session state: in
      the pool's database — ``database`` (a :func:`connect` keyword), or
      ``default`` without it — with no consistency override, no scan budget
      and no transaction open. What a borrower changed with ``USE``, ``SET
      CONSISTENCY``, ``SET SCAN BUDGET`` or ``BEGIN`` is put back when its
      connection comes back, or the connection is closed.

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

    def _home(self, conn: Connection) -> bool:
        """Put a connection that comes back into the pool's own session
        state, so the next borrower gets it as a new connection would be:
        in the pool's database (its ``database=``, or ``default``, where
        every session starts), under no consistency override and no scan
        budget, with no transaction open. Only what the connection's notes
        say its borrower changed is sent — ``ROLLBACK`` first, then ``USE``,
        ``SET SCAN BUDGET DEFAULT``, ``SET CONSISTENCY DEFAULT`` — so a
        borrower that changed nothing costs no round trip. The notes go
        with it: a re-dial for the next borrower restores the pool's state,
        not the last borrower's.

        False when a statement fails — a server before 0.349 refuses ``SET
        CONSISTENCY DEFAULT``, or the connection is gone: the caller closes
        the connection instead of pooling it. Nothing dials here."""
        home = self._kwargs.get("database")
        try:
            if conn._txn_noted:
                conn._query("ROLLBACK", conn._consistency)
            if (conn._database or "default") != (home or "default"):
                conn._use_database(home or "default")
            conn._database = home
            if conn._session_scan_budget is not None:
                conn._query("SET SCAN BUDGET DEFAULT", conn._consistency)
            if conn._session_consistency is not None:
                conn._query("SET CONSISTENCY DEFAULT", conn._consistency)
        except Error:
            return False
        return True

    def putconn(self, conn: Connection) -> None:
        """Return a connection to the pool — in the pool's own session
        state, whatever its borrower did to it (`_home`) — or close it if
        it is broken, cannot be put back in that state, its member is
        ejected or gone, or the pool is full."""
        # Decided before the pool's lock is taken: the answer looks at the
        # socket, a connection found dead reports to the pool, and putting
        # the session back may cost round trips.
        try:
            usable = not self.closed and conn.is_usable() and self._home(conn)
        except BaseException:
            # Interrupted half way: the connection is not pooled, and is
            # no longer counted as checked out.
            self._put(conn, False)
            raise
        self._put(conn, usable)

    def _put(self, conn: Connection, usable: bool) -> None:
        with self._lock:
            m = self._members.get(conn._pool_ep)
            if getattr(conn, "_pool_out", False):
                conn._pool_out = False
                if m is not None and m.in_use > 0:
                    m.in_use -= 1
            if (
                not self.closed
                and usable
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

    def transaction(
        self,
        body: "Callable[[Connection], _T]",
        attempts: int = 3,
        begin: str = "BEGIN",
        affinity: Any = None,
    ) -> "_T":
        """Check out ONE connection, run :meth:`Connection.transaction` on
        it and return the connection to the pool: every statement of the
        transaction, and of every run a conflict costs, goes over that
        connection. ``body`` gets the connection and must use it, never
        another checkout — a statement on another connection is outside the
        transaction. ``affinity`` is the checkout's (see :meth:`getconn`)."""
        if attempts < 1:
            raise ValueError("attempts must be >= 1")
        with self.connection(affinity) as conn:
            return conn.transaction(body, attempts, begin)

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
