# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Apache-2.0

import copy
from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from chimera_admin.cli import _build_parser, main
from chimera_admin.fleet import Failure, Poller, Target, TargetView, load_targets, render_fleet, run_fleet


def fixture(name="snapshot.json"):
    data = json.loads((ROOT / "examples" / name).read_text())
    return {"threads": data["threads_snapshot"], "connections": data["connections_snapshot"]}


def message(snapshots, at=1, done=False):
    return {"kind": "result", "attempt": 1, "finished": at, "snapshots": snapshots, "error": None, "done": done}


class ConfigAndIdentity(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "targets.json"

    def tearDown(self):
        self.temp.cleanup()

    def write(self, targets):
        self.path.write_text(json.dumps({"targets": targets}))

    def test_relative_credentials_target_selection_and_ipv6(self):
        self.write([{"name": "a", "url": "https://[::1]:8443", "token_file": "secrets/a.token", "ca_file": "ca.pem"}, {"name": "b", "url": "http://127.0.0.1:8080"}])
        targets = load_targets(self.path, ["a"])
        self.assertEqual(len(targets), 1)
        self.assertEqual((targets[0].host, targets[0].port), ("::1", 8443))
        self.assertEqual(targets[0].options["token_file"], str(Path(self.temp.name) / "secrets/a.token"))

    def test_duplicate_unknown_names_and_invalid_credentials_are_rejected(self):
        for targets in (
            [{"name": "a", "url": "http://localhost:8080"}] * 2,
            [{"name": "a", "url": "http://user:secret@localhost:8080"}],
            [{"name": "a", "url": "http://localhost:0"}],
            [{"name": "a", "url": "http://localhost:8080", "token_env": "TOKEN", "username": "u", "password_env": "PASS"}],
            [{"name": "a", "url": "http://localhost:8080", "password": "literal-secret"}],
            [{"name": "a", "url": "http://localhost:8080", "timeout_seconds": True}],
        ):
            self.write(targets)
            with self.assertRaises(ValueError):
                load_targets(self.path)
        self.write([{"name": "a", "url": "http://localhost:8080"}])
        with self.assertRaises(ValueError):
            load_targets(self.path, ["unknown"])

    def test_colliding_worker_ids_stay_separate_and_restart_is_local(self):
        a = TargetView(Target("nas-a", "http://a:8080", "a", 8080, False, 3, {}))
        b = TargetView(Target("nas-b", "http://b:8080", "b", 8080, False, 3, {}))
        a.accept(message(fixture())); b.accept(message(fixture()))
        a.accept(message(fixture("snapshot-next.json"), 2))
        restart = fixture("snapshot-next.json")
        for snapshot in restart.values(): snapshot["server_instance_id"] = "restarted-b"
        b.accept(message(restart, 2))
        self.assertEqual(a.meta["rates"]["0"]["rpc"], 500.0)
        self.assertEqual(b.meta["rates"], {})
        self.assertTrue(b.meta["restarted"])
        self.assertFalse(a.meta["restarted"])
        args = _build_parser().parse_args(["inspect", "dashboard"])
        output = render_fleet([a, b], args, 2)
        self.assertIn("nas-a  LIVE", output)
        self.assertIn("nas-a", output)
        self.assertIn("nas-b", output)
        self.assertIn("ABSENT 0:17", output)
        self.assertNotIn("ABSENT 0:17", output.split("nas-b  LIVE", 1)[-1])

    def test_partial_connection_sample_does_not_remove_other_target_state(self):
        views = [TargetView(Target(name, "http://localhost", "localhost", 80, False, 3, {})) for name in ("a", "b")]
        for view in views: view.accept(message(fixture()))
        partial = fixture("snapshot-next.json"); partial["connections"].update(complete=False, missing_workers=[0])
        views[0].accept(message(partial, 2)); views[1].accept(message(fixture("snapshot-next.json"), 2))
        self.assertEqual(views[0].meta["absent"], [])
        self.assertEqual([c["connection_id"] for c in views[1].meta["absent"]], ["0:17"])

    def test_quiet_wide_groups_keep_instance_identity_and_health_status(self):
        a = TargetView(Target("nas-a", "http://a:8080", "a", 8080, False, 3, {}))
        b = TargetView(Target("nas-b", "http://b:8080", "b", 8080, False, 3, {}))
        c = TargetView(Target("nas-c", "http://c:8080", "c", 8080, False, 3, {}))
        a.accept(message(fixture()))
        partial = fixture()
        partial["connections"].update(complete=False, truncated=True)
        c.accept(message(partial))
        b.accept({"kind": "result", "attempt": 1, "finished": 1, "snapshots": None,
                  "error": Failure("unreachable", 1), "done": False})
        options = _build_parser().parse_args(["inspect", "dashboard", "--quiet", "--wide"])
        with patch("chimera_admin.fleet.terminal_width", return_value=80):
            output = render_fleet([a, b, c], options, 3)
        self.assertNotIn("INSPECTION DASHBOARD", output)
        self.assertIn("nas-a  LIVE", output)
        self.assertIn("nas-b  UNREACHABLE", output)
        self.assertIn("nas-c  PARTIAL", output)
        self.assertIn("connections PARTIAL", output)
        self.assertIn(fixture()["threads"]["server_instance_id"], output)
        self.assertIn("Error: unreachable", output)
        self.assertIn("RAW TRANSPORT", output)
        self.assertLessEqual(max(map(len, output.splitlines())), 80)


class FakeClient:
    def __init__(self, blocked=None):
        self.blocked = blocked
        self.index = 0
        self.closed = threading.Event()
        self.owner = None

    def get_threads(self, **kwargs):
        self.owner = threading.current_thread().name
        if self.blocked: self.blocked.wait(4)
        return fixture("snapshot.json" if self.index == 0 else "snapshot-next.json")["threads"]

    def get_connections(self, **kwargs):
        data = fixture("snapshot.json" if self.index == 0 else "snapshot-next.json")["connections"]
        self.index += 1
        return data

    def close(self):
        self.close_owner = threading.current_thread().name
        self.closed.set()


class Scheduling(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.config = Path(self.temp.name) / "targets.json"
        self.config.write_text(json.dumps({"targets": [{"name": "healthy", "url": "http://127.0.0.1:8080", "timeout_seconds": 0.1}, {"name": "blocked", "url": "http://127.0.0.1:8081", "timeout_seconds": 0.1}]}))

    def tearDown(self):
        self.temp.cleanup()

    def setup_pollers(self):
        released = threading.Event()
        clients = {"healthy": FakeClient(), "blocked": FakeClient(released)}
        pollers = []
        def create(target, args, ready):
            poller = Poller(target, args, ready, lambda spec: clients[spec.name])
            pollers.append(poller)
            return poller
        return released, clients, pollers, create

    def test_healthy_target_advances_while_other_request_is_blocked(self):
        released, clients, pollers, create = self.setup_pollers()
        args = _build_parser().parse_args(["inspect", "dashboard", "--targets", str(self.config), "--watch", "--interval", "1", "--iterations", "2", "--json"])
        out = io.StringIO(); started = time.monotonic()
        try:
            with patch("chimera_admin.fleet.Poller", side_effect=create), redirect_stdout(out):
                code = run_fleet(args)
            self.assertLess(time.monotonic() - started, 2.5)
            data = [json.loads(line) for line in out.getvalue().splitlines()]
            last = data[-1]["targets"]
            self.assertEqual(code, 4)
            self.assertEqual(last["healthy"]["status"], "LIVE")
            self.assertEqual(last["healthy"]["comparison"]["rates"]["0"]["rpc"], 500.0)
            self.assertEqual(last["blocked"]["status"], "TIMEOUT")
            self.assertTrue(any(frame["targets"]["healthy"]["status"] == "LIVE" and frame["targets"]["blocked"]["status"] == "PENDING" for frame in data))
        finally:
            released.set()
            for poller in pollers: poller.join(1)
        self.assertEqual(clients["healthy"].owner, clients["healthy"].close_owner)

    def test_ctrl_c_restores_cursor_without_waiting_for_stuck_request(self):
        released, clients, pollers, create = self.setup_pollers()
        class Terminal(io.StringIO):
            def isatty(self): return True
        out = Terminal()
        args = _build_parser().parse_args(["inspect", "dashboard", "--targets", str(self.config), "--watch"])
        started = time.monotonic()
        try:
            with patch("chimera_admin.fleet.Poller", side_effect=create), patch("chimera_admin.fleet.render_fleet", side_effect=KeyboardInterrupt), patch.dict(os.environ, {"TERM": "xterm"}), redirect_stdout(out):
                self.assertEqual(run_fleet(args), 130)
            self.assertLess(time.monotonic() - started, 1)
            self.assertTrue(out.getvalue().endswith("\x1b[?25h"))
        finally:
            released.set()
            for poller in pollers: poller.join(1)

    def test_q_exits_tty_fleet_watch_and_closes_pollers(self):
        target = Target("nas-a", "http://localhost:8080", "localhost", 8080, False, 3, {})
        sample = TargetView(target)
        sample.accept(message(fixture()))

        class Keys:
            active = True
            def __init__(self): self.calls, self.closed = 0, False
            def poll(self):
                self.calls += 1
                return self.calls == 2
            def close(self): self.closed = True

        class Ready:
            def wait(self, timeout): return True

        class Group:
            def __init__(self):
                self.views, self.ready, self.done, self.closed = [sample], Ready(), False, False
            def start(self): pass
            def update(self): pass
            def close(self): self.closed = True

        keys, group = Keys(), Group()
        args = _build_parser().parse_args(["inspect", "dashboard", "--targets", str(self.config), "--watch"])
        class Terminal(io.StringIO):
            def isatty(self): return True
        output = Terminal()
        with patch("chimera_admin.fleet.configured_targets", return_value=[target]), \
                patch("chimera_admin.fleet.PollGroup", return_value=group), \
                patch("chimera_admin.fleet.WatchKeys", return_value=keys), \
                patch("chimera_admin.fleet.render_fleet", return_value="frame"), \
                patch("sys.stdin.isatty", return_value=True), patch.dict(os.environ, {"TERM": "xterm"}), redirect_stdout(output):
            self.assertEqual(run_fleet(args), 0)
        self.assertIn("frame\n", output.getvalue())
        self.assertTrue(output.getvalue().endswith("\x1b[?25h"))
        self.assertTrue(group.closed)
        self.assertTrue(keys.closed)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        target = urlsplit(self.path)
        with self.server.lock:
            self.server.active += 1
            self.server.max_active = max(self.server.max_active, self.server.active)
            self.server.auth_seen.append(self.headers.get("Authorization"))
        try:
            body = fixture()["threads" if target.path.endswith("threads") else "connections"]
            body["server_instance_id"] = self.server.instance
            encoded = json.dumps(body).encode()
            self.send_response(self.server.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
        finally:
            with self.server.lock: self.server.active -= 1

    def log_message(self, *args): pass


class FleetHTTP(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.servers, self.threads = [], []
        for name in ("nas-a", "nas-b"):
            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            server.instance, server.status = name + "-instance", 200
            server.lock, server.auth_seen, server.active, server.max_active = threading.Lock(), [], 0, 0
            thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
            self.servers.append(server); self.threads.append(thread)
        self.config = Path(self.temp.name) / "targets.json"
        self.config.write_text(json.dumps({"targets": [{"name": name, "url": "http://127.0.0.1:{}".format(server.server_port), "token_env": env} for name, server, env in zip(("nas-a", "nas-b"), self.servers, ("FLEET_TEST_A", "FLEET_TEST_B"))]}))

    def tearDown(self):
        for server in self.servers: server.shutdown(); server.server_close()
        for thread in self.threads: thread.join()
        self.temp.cleanup()

    def call(self, *extra):
        output = io.StringIO()
        with patch.dict(os.environ, {"FLEET_TEST_A": "token-a", "FLEET_TEST_B": "token-b", "CHIMERA_ADMIN_TOKEN": "must-not-inherit"}), redirect_stdout(output):
            code = main(["inspect", "dashboard", "--targets", str(self.config), "--json", *extra])
        return code, json.loads(output.getvalue())

    def test_two_actual_servers_keep_auth_ids_and_sessions_separate(self):
        code, data = self.call()
        self.assertEqual(code, 0)
        self.assertEqual(set(data["targets"]), {"nas-a", "nas-b"})
        for i, name in enumerate(("nas-a", "nas-b")):
            target = data["targets"][name]
            self.assertEqual(len(target["snapshots"]["threads"]["threads"]), 8)
            self.assertEqual(target["snapshots"]["connections"]["connections"][0]["connection_id"], "0:17")
            self.assertEqual(set(self.servers[i].auth_seen), {"Bearer token-" + ("a" if i == 0 else "b")})
            self.assertEqual(self.servers[i].max_active, 1)
        self.assertNotIn("token-a", json.dumps(data))
        self.assertNotIn("must-not-inherit", json.dumps(data))

    def test_auth_failure_does_not_hide_healthy_instance(self):
        self.servers[1].status = 401
        code, data = self.call()
        self.assertEqual(code, 4)
        self.assertEqual(data["targets"]["nas-a"]["status"], "LIVE")
        self.assertEqual(data["targets"]["nas-b"]["status"], "AUTH ERROR")

    def test_target_selection_avoids_polling_other_instance(self):
        code, data = self.call("--target", "nas-b")
        self.assertEqual(code, 0)
        self.assertEqual(set(data["targets"]), {"nas-b"})
        self.assertEqual(self.servers[0].auth_seen, [])


if __name__ == "__main__":
    unittest.main()
