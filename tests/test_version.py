"""The package version is defined once, in pyproject.toml. These tests pin the
two places that must agree with it: the in-module fallback literal (used when
running from an uninstalled checkout) and the version the driver reports to
the server in the Hello frame."""

import pathlib
import re
import struct
import threading
import unittest

import skaidb
from skaidb import _OP_HELLO

_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _pyproject_version() -> str:
    text = (_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    m = re.search(r'^version\s*=\s*"([^"]+)"', text, re.M)
    assert m, "pyproject.toml has no version"
    return m.group(1)


class _Sock:
    def __init__(self):
        self.sent = bytearray()

    def sendall(self, data):
        self.sent += data

    def close(self):
        pass


class _File:
    """Answers every read with a single Ddl ack frame (what Hello returns)."""

    def __init__(self):
        self.buf = bytearray()

    def ack(self):
        payload = bytes([2])  # RESP_DDL
        self.buf += struct.pack(">I", len(payload)) + payload

    def read(self, n):
        out = bytes(self.buf[:n])
        del self.buf[:n]
        return out

    def close(self):
        pass


class _HelloConn(skaidb.Connection):
    def __init__(self):  # bypass dial + handshake
        self._lock = threading.Lock()
        self.closed = False
        self._broken = False
        self._streaming = False
        self._prepared = {}
        self._consistency = skaidb.Consistency.QUORUM
        self._sock = _Sock()
        self._file = _File()


class Version(unittest.TestCase):
    def test_fallback_literal_matches_pyproject(self):
        self.assertEqual(skaidb._FALLBACK_VERSION, _pyproject_version())

    def test_dunder_version_is_pep440_and_matches_pyproject(self):
        # Installed (metadata) or checkout (fallback): both must equal pyproject.
        self.assertRegex(skaidb.__version__, r"^\d+\.\d+\.\d+")
        self.assertEqual(skaidb.__version__, _pyproject_version())

    def test_hello_frame_carries_package_version(self):
        conn = _HelloConn()
        conn._file.ack()
        conn._send_hello()
        frame = bytes(conn._sock.sent)
        (length,) = struct.unpack(">I", frame[:4])
        body = frame[4 : 4 + length]
        self.assertEqual(body[0], _OP_HELLO)
        r = skaidb._Reader(body[1:])
        self.assertEqual(r.text(), "python")
        self.assertEqual(r.text(), skaidb.__version__)
        self.assertEqual(r.text.__self__.pos, len(body) - 1)  # nothing trailing

    def test_version_exported(self):
        self.assertIn("__version__", skaidb.__all__)
