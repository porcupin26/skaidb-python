"""Pool load balancing: policies, idle order, health ejection and
re-admission, member discovery (SHOW ENDPOINTS) and read-your-writes
affinity — against fake members that each serve many connections."""

import hashlib
import hmac
import socket
import struct
import threading
import time
import unittest

import skaidb

try:
    from test_conformance import V, _read_frame, _send, _str
except ImportError:  # run as `python -m unittest tests.test_load_balancing`
    from tests.test_conformance import V, _read_frame, _send, _str


def _text(s):
    b = s.encode()
    return struct.pack("<I", len(b)) + b


def _rows(columns, rows):
    out = bytearray([0]) + struct.pack("<I", len(columns))
    for c in columns:
        out += _text(c)
    out += struct.pack("<I", len(rows))
    for row in rows:
        out += struct.pack("<I", len(row))
        for v in row:
            enc = skaidb._encode_value(v)
            out += struct.pack("<I", len(enc)) + enc
    return bytes(out)


class Member:
    """A fake skaidb member: SCRAM per the conformance vectors on every
    connection, then `SELECT …` answers one row naming the member (after
    `delay` seconds), `SHOW ENDPOINTS` answers `listing`, `SHOW DATABASES`
    (the pool's probe) answers no rows, anything else is a one-row
    mutation. `refuse` closes each new connection before the handshake;
    `old_server` answers SHOW ENDPOINTS with a parse error."""

    def __init__(self, name):
        self.name = name
        self.delay = 0.0
        self.refuse = False
        self.old_server = False
        self.listing = None
        self.statements = []
        self.lock = threading.Lock()
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(64)
        self.port = self.sock.getsockname()[1]
        self.endpoint = f"127.0.0.1:{self.port}"
        self.closed = False
        threading.Thread(target=self._accept, daemon=True).start()

    def selects(self):
        with self.lock:
            return sum(1 for s in self.statements if s.startswith("SELECT"))

    def _accept(self):
        while not self.closed:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            if self.refuse:
                conn.close()
                continue
            threading.Thread(target=self._serve_safe, args=(conn,), daemon=True).start()

    def _serve_safe(self, conn):
        try:
            with conn:
                self._serve(conn)
        except OSError:
            pass

    def _serve(self, conn):
        auth = V["auth"]
        start = _read_frame(conn)
        if start is None:
            return
        user, pos = _str(start, 1)
        client_nonce, pos = _str(start, pos)
        salt = bytes.fromhex(auth["challenge"]["salt"])
        iterations = auth["challenge"]["iterations"]
        server_nonce = client_nonce + auth["challenge"]["server_nonce_suffix"].encode()
        challenge = bytes([11]) + struct.pack("<I", len(salt)) + salt + struct.pack("<I", iterations)
        challenge += struct.pack("<I", len(server_nonce)) + server_nonce
        _send(conn, challenge)
        finish = _read_frame(conn)
        if finish is None:
            return
        am = b"\0".join([user, client_nonce, server_nonce, salt.hex().encode(), str(iterations).encode()])
        salted = hashlib.pbkdf2_hmac("sha256", auth["password"].encode(), salt, iterations, 32)
        server_sig = hmac.new(
            hmac.new(salted, b"Server Key", hashlib.sha256).digest(), am, hashlib.sha256
        ).digest()
        _send(conn, bytes([13, 1]) + server_sig)
        ddl = bytes([2])
        while True:
            req = _read_frame(conn)
            if req is None:
                return
            if req[0] != 1:  # hello / close / anything else: an ack
                _send(conn, ddl)
                continue
            (n,) = struct.unpack_from("<I", req, 2)
            sql = req[6 : 6 + n].decode()
            with self.lock:
                self.statements.append(sql)
            if sql == "SHOW ENDPOINTS":
                if self.old_server or self.listing is None:
                    _send(conn, bytes([3]) + _text("parse error: expected TABLES, found ENDPOINTS"))
                else:
                    _send(conn, _rows(["endpoint", "state", "local"], self.listing()))
            elif sql == "SHOW DATABASES":
                _send(conn, _rows(["name"], []))
            elif sql.startswith("SELECT"):
                if self.delay:
                    time.sleep(self.delay)
                _send(conn, _rows(["member"], [[self.name]]))
            else:
                _send(conn, bytes([1]) + struct.pack("<Q", 1))

    def close(self):
        self.closed = True
        self.sock.close()


def _pool(members, **kw):
    kw.setdefault("seeds", [m.endpoint for m in members])
    return skaidb.pool(
        user=V["auth"]["username"],
        password=V["auth"]["password"],
        timeout=5,
        **kw,
    )


def _who(pool, affinity=None):
    with pool.connection(affinity) as conn:
        cur = conn.cursor()
        cur.execute("SELECT member()")
        return cur.fetchone()[0]


class Policies(unittest.TestCase):
    def setUp(self):
        self.members = [Member(n) for n in "abc"]
        self.addCleanup(lambda: [m.close() for m in self.members])

    def test_round_robin_rotates_over_every_member(self):
        p = _pool(self.members, load_balancing="round_robin")
        self.addCleanup(p.close)
        served = [_who(p) for _ in range(9)]
        self.assertEqual(sorted(served), sorted("abc" * 3))
        # Strict rotation: three in a row are three different members.
        for i in range(0, 9, 3):
            self.assertEqual(len(set(served[i : i + 3])), 3, served)

    def test_least_latency_prefers_the_faster_member(self):
        a, b, _ = self.members
        b.delay = 0.02
        p = _pool([a, b], load_balancing="least_latency", eject_latency_factor=0)
        self.addCleanup(p.close)
        served = [_who(p) for _ in range(60)]
        self.assertGreater(served.count("a"), 48, served)
        self.assertGreater(served.count("b"), 0, "b was measured at least once")

    def test_least_latency_re_measures_a_member_it_has_not_used_lately(self):
        a, b, _ = self.members
        b.delay = 0.02
        p = _pool([a, b], load_balancing="least_latency", eject_latency_factor=0)
        self.addCleanup(p.close)
        for _ in range(20):
            _who(p)
        # b is slow, then recovers: an idle pool notices once b's average
        # goes stale instead of never asking b again.
        b.delay = 0.0
        p._members[("127.0.0.1", b.port)].last_sample -= 10
        served = [_who(p) for _ in range(3)]
        self.assertIn("b", served)

    def test_random_policy_keeps_the_1_1_behaviour(self):
        p = _pool(self.members)
        self.addCleanup(p.close)
        # One connection, reused: random never targets a member.
        self.assertEqual(len({_who(p) for _ in range(5)}), 1)

    def test_unknown_policy_is_refused(self):
        with self.assertRaises(ValueError):
            skaidb.pool(load_balancing="nearest")
        with self.assertRaises(ValueError):
            skaidb.pool(idle_order="random")


class IdleOrder(unittest.TestCase):
    def setUp(self):
        self.m = Member("a")
        self.addCleanup(self.m.close)

    def _three(self, **kw):
        p = _pool([self.m], **kw)
        self.addCleanup(p.close)
        conns = [p.getconn() for _ in range(3)]
        for c in conns:
            p.putconn(c)
        return p, conns

    def test_lifo_hands_out_the_most_recently_returned(self):
        p, conns = self._three()
        self.assertIs(p.getconn(), conns[2])

    def test_fifo_hands_out_the_longest_idle(self):
        p, conns = self._three(idle_order="fifo")
        got = [p.getconn() for _ in range(3)]
        self.assertEqual(got, conns)


class Ejection(unittest.TestCase):
    def setUp(self):
        self.members = [Member(n) for n in "abc"]
        self.addCleanup(lambda: [m.close() for m in self.members])

    def _state(self, p, member):
        return next(r["state"] for r in p.members() if r["endpoint"] == member.endpoint)

    def test_a_failing_member_is_ejected_then_readmitted_by_the_probe(self):
        a, b, c = self.members
        b.refuse = True
        p = _pool(self.members, load_balancing="round_robin", eject_errors=2, eject_backoff=0.3)
        self.addCleanup(p.close)
        served = [_who(p) for _ in range(12)]
        self.assertNotIn("b", served)
        self.assertEqual(self._state(p, b), "ejected")
        # Healthy again: the background probe re-admits it after the backoff.
        b.refuse = False
        deadline = time.monotonic() + 5
        while self._state(p, b) != "ready":
            self.assertLess(time.monotonic(), deadline, "never re-admitted")
            time.sleep(0.05)
        served = [_who(p) for _ in range(6)]
        self.assertIn("b", served)

    def test_a_probe_that_fails_doubles_the_backoff(self):
        a, b, c = self.members
        b.refuse = True
        p = _pool(self.members, load_balancing="round_robin", eject_errors=1, eject_backoff=0.2)
        self.addCleanup(p.close)
        for _ in range(6):
            _who(p)
        m = p._members[("127.0.0.1", b.port)]
        deadline = time.monotonic() + 5
        while m.ejections < 2:
            self.assertLess(time.monotonic(), deadline, "the probe never ran")
            time.sleep(0.05)
        self.assertEqual(self._state(p, b), "ejected")
        self.assertGreaterEqual(p._backoff_for(m), 0.4)

    def test_a_consistently_slow_member_is_ejected(self):
        a, b, c = self.members
        b.delay = 0.05
        p = _pool(self.members, load_balancing="round_robin", eject_latency_floor=0.02, eject_backoff=30)
        self.addCleanup(p.close)
        for _ in range(45):
            _who(p)
        self.assertEqual(self._state(p, b), "ejected")
        self.assertNotIn("b", [_who(p) for _ in range(6)])

    def test_an_ejected_member_is_not_ejected_again_by_late_round_trips(self):
        a, b, c = self.members
        b.delay = 0.05
        p = _pool(self.members, load_balancing="round_robin", eject_latency_floor=0.02, eject_backoff=30)
        self.addCleanup(p.close)
        for _ in range(45):
            _who(p)
        m = p._members[("127.0.0.1", b.port)]
        self.assertEqual(self._state(p, b), "ejected")
        until = m.ejected_until
        # Slow round trips still finishing on connections to b that were
        # checked out before the ejection.
        late = type("Late", (), {"_connected": m.endpoint, "_pool_ep": m.endpoint})()
        for _ in range(5):
            p._round_trip(late, 0.05)
        self.assertEqual((m.ejections, m.ejected_until), (1, until))

    def test_one_slow_statement_does_not_eject_a_member(self):
        a, b, c = self.members
        p = _pool(self.members, load_balancing="round_robin", eject_latency_floor=0.05)
        self.addCleanup(p.close)
        for _ in range(36):
            _who(p)
        b.delay = 0.5
        while _who(p) != "b":
            pass
        b.delay = 0.0
        for _ in range(6):
            _who(p)
        self.assertEqual(self._state(p, b), "ready")

    def _fakes(self, p):
        """Stand-ins for connections to each member, to feed the pool's
        latency measurements round trips of a chosen length."""
        return {
            f.name: type("Fake", (), {"_connected": ("127.0.0.1", f.port), "_pool_ep": ("127.0.0.1", f.port)})()
            for f in self.members
        }

    def test_a_slow_first_round_trip_does_not_eject_a_member(self):
        # The first statements on b stall in the client — a CPU-bound
        # application starting up holds the thread — and b answers fast from
        # then on: the fastest of its first round trips seeds its average,
        # so b stays in rotation. (Seeded by its first round trip alone, b's
        # average still read ~3.8 s when the pool first compared it, and b
        # was ejected.)
        a, b, c = self.members
        p = _pool(self.members, load_balancing="round_robin", eject_latency_floor=0.05)
        self.addCleanup(p.close)
        fake = self._fakes(p)
        p._round_trip(fake["b"], 6.6)
        p._round_trip(fake["b"], 3.0)
        for _ in range(12):
            for name in "abc":
                p._round_trip(fake[name], 0.0002)
        self.assertEqual(self._state(p, b), "ready")
        self.assertLess(p._members[("127.0.0.1", b.port)].ewma, 0.05)

    def test_a_member_slow_from_its_first_round_trip_is_ejected_and_the_warning_names_its_slowest(self):
        a, b, c = self.members
        p = _pool(self.members, load_balancing="round_robin", eject_latency_floor=0.05, eject_backoff=30)
        self.addCleanup(p.close)
        fake = self._fakes(p)
        with self.assertLogs("skaidb.pool", level="WARNING") as logs:
            for i in range(12):
                p._round_trip(fake["a"], 0.0002)
                p._round_trip(fake["c"], 0.0002)
                p._round_trip(fake["b"], 0.9 if i == 5 else 0.5)
        self.assertEqual(self._state(p, b), "ejected")
        ejected = [line for line in logs.output if "ejecting" in line]
        self.assertEqual(len(ejected), 1, logs.output)
        self.assertRegex(ejected[0], r"latency 5\d\d\.\d ms vs median 0\.2 ms")
        self.assertIn("slowest of the last 10 round trips 900.0 ms", ejected[0])

    def test_a_readmitted_member_is_seeded_afresh(self):
        a, b, c = self.members
        p = _pool(self.members, load_balancing="round_robin")
        self.addCleanup(p.close)
        m = p._members[("127.0.0.1", b.port)]
        fake = self._fakes(p)
        for t in (0.004, 0.003, 0.002):
            p._round_trip(fake["b"], t)
        self.assertEqual(m.ewma, 0.002)
        m.reset_latency()
        self.assertEqual((m.ewma, m.firsts, list(m.recent), m.samples), (None, [], [], 0))
        p._round_trip(fake["b"], 0.007)
        self.assertIsNone(m.ewma, "not measured until its first three round trips")

    def test_the_last_usable_member_is_never_ejected(self):
        a, b, c = self.members
        b.refuse = c.refuse = True
        p = _pool(self.members, load_balancing="round_robin", eject_errors=1)
        self.addCleanup(p.close)
        for _ in range(6):
            self.assertEqual(_who(p), "a")
        a.refuse = True
        # Idle connections to a still work (how many depends on the dial
        # order above); once they are used up, every dial fails.
        held = []
        with self.assertRaises(skaidb.OperationalError):
            for _ in range(12):
                held.append(p.getconn())
        self.assertEqual(self._state(p, a), "ready")

    def test_a_refused_login_is_not_a_member_fault(self):
        self.assertFalse(skaidb._member_fault(skaidb.OperationalError("authentication denied: bad password")))
        self.assertTrue(skaidb._member_fault(
            skaidb.OperationalError("authentication denied: too many connections: 500")))
        self.assertTrue(skaidb._member_fault(skaidb.OperationalError("connection closed by server")))
        self.assertTrue(skaidb._member_fault(ConnectionRefusedError()))


class Discovery(unittest.TestCase):
    def setUp(self):
        self.members = [Member(n) for n in "abc"]
        self.addCleanup(lambda: [m.close() for m in self.members])
        a, b, c = self.members
        self.states = {a.endpoint: "ready", b.endpoint: "ready", c.endpoint: "resyncing"}

        def listing_for(me):
            return lambda: [(ep, st, ep == me.endpoint) for ep, st in sorted(self.states.items())]

        for m in self.members:
            m.listing = listing_for(m)

    def test_members_are_discovered_and_resyncing_ones_get_no_connections(self):
        a, b, c = self.members
        p = _pool([a], load_balancing="round_robin", discover=True)
        self.addCleanup(p.close)
        served = [_who(p) for _ in range(8)]
        self.assertEqual(set(served), {"a", "b"}, served)
        states = {r["endpoint"]: r["state"] for r in p.members()}
        self.assertEqual(states[c.endpoint], "resyncing")
        # c catches up: the next refresh lets it in, measured afresh.
        m = p._members[("127.0.0.1", c.port)]
        m.ewma, m.samples = 9.9, 50
        self.states[c.endpoint] = "ready"
        conn = p.getconn()
        p._list_through(conn)
        p.putconn(conn)
        self.assertEqual((m.ewma, m.samples), (None, 0))
        self.assertIn("c", [_who(p) for _ in range(6)])

    def test_a_listing_with_a_member_not_ready_refreshes_sooner(self):
        a, b, c = self.members
        p = _pool([a], load_balancing="round_robin", discover=True, refresh_interval=60)
        self.addCleanup(p.close)
        _who(p)
        self.assertLessEqual(p._next_refresh - time.monotonic(), 5.0)  # c resyncing
        self.states[c.endpoint] = "ready"
        conn = p.getconn()
        p._list_through(conn)
        p.putconn(conn)
        self.assertGreater(p._next_refresh - time.monotonic(), 50.0)

    def test_a_seed_given_by_another_name_is_not_counted_twice(self):
        a, b, c = self.members
        p = _pool([a], seeds=[f"localhost:{a.port}"], load_balancing="round_robin", discover=True)
        self.addCleanup(p.close)
        _who(p)
        rows = p.members()
        self.assertEqual(len(rows), 3, rows)
        seed = next(r for r in rows if r["seed"])
        self.assertEqual(seed["endpoint"], f"localhost:{a.port}")
        self.assertEqual(seed["canonical"], a.endpoint)

    def test_a_member_that_leaves_the_listing_is_dropped(self):
        a, b, c = self.members
        p = _pool([a], load_balancing="round_robin", discover=True)
        self.addCleanup(p.close)
        _who(p)
        del self.states[b.endpoint]
        conn = p.getconn()
        p._list_through(conn)
        p.putconn(conn)
        self.assertNotIn(b.endpoint, [r["endpoint"] for r in p.members()])

    def test_a_standalone_server_turns_discovery_off(self):
        a, b, c = self.members
        a.listing = lambda: []
        p = _pool([a], load_balancing="round_robin", discover=True)
        self.addCleanup(p.close)
        self.assertEqual({_who(p) for _ in range(4)}, {"a"})
        self.assertTrue(p._discovery_off)
        self.assertEqual(sum(1 for s in a.statements if s == "SHOW ENDPOINTS"), 1)

    def test_an_old_server_turns_discovery_off_and_keeps_the_seeds(self):
        a, b, c = self.members
        a.old_server = True
        p = _pool([a], load_balancing="round_robin", discover=True)
        self.addCleanup(p.close)
        self.assertEqual({_who(p) for _ in range(4)}, {"a"})
        self.assertTrue(p._discovery_off)
        self.assertEqual(len(p.members()), 1)


class ReadYourWrites(unittest.TestCase):
    def setUp(self):
        self.members = [Member(n) for n in "abc"]
        self.addCleanup(lambda: [m.close() for m in self.members])

    def test_reads_follow_the_member_that_took_the_write(self):
        p = _pool(self.members, load_balancing="round_robin", read_your_writes=0.4)
        self.addCleanup(p.close)
        with p.connection("alice") as conn:
            conn.cursor().execute("INSERT INTO t VALUES (1)")
            wrote = conn._connected
        writer = next(m.name for m in self.members if m.port == wrote[1])
        self.assertEqual({_who(p, "alice") for _ in range(6)}, {writer})
        # Another key, or no key, still rotates.
        self.assertEqual(len({_who(p, "bob") for _ in range(3)}), 3)
        time.sleep(0.5)
        self.assertEqual(len({_who(p, "alice") for _ in range(3)}), 3)

    def test_reads_do_not_set_affinity(self):
        p = _pool(self.members, load_balancing="round_robin", read_your_writes=5)
        self.addCleanup(p.close)
        _who(p, "alice")
        self.assertEqual(len({_who(p, "alice") for _ in range(3)}), 3)

    def test_statement_classification(self):
        for sql in ("SELECT 1", "  select 1", "(SELECT 1)", "-- c\nSHOW TABLES",
                    "/* x */ EXPLAIN SELECT 1", "WITH a AS (SELECT 1) SELECT * FROM a",
                    "DESCRIBE t", "VALUES (1)", "use app"):
            self.assertFalse(skaidb._is_write(sql), sql)
        for sql in ("INSERT INTO t VALUES (1)", "update t set a = 1", "DELETE FROM t",
                    "CREATE TABLE t (id INT PRIMARY KEY)", "COMMIT", "CALL p()", "selectx"):
            self.assertTrue(skaidb._is_write(sql), sql)


if __name__ == "__main__":
    unittest.main()
