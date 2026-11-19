# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Apache-2.0

import ast
import copy
from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import sys
import threading
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from chimera_admin import ChimeraAdminClient, ChimeraAdminError
from chimera_admin.cli import _build_parser, main
from chimera_admin.inspection import WatchState, endpoint, fetch, protocol, render, run_inspection, valid


def fixture(name="snapshot.json"):
    return json.loads((ROOT / "examples" / name).read_text())


def frames(name="snapshot.json"):
    raw = fixture(name)
    return {"threads": raw["threads_snapshot"], "connections": raw["connections_snapshot"]}


def args(*extra):
    return _build_parser().parse_args(["inspect", "dashboard", *extra])


class InspectionSemantics(unittest.TestCase):
    def test_nfs_session_command_and_watch_peak(self):
        parsed = _build_parser().parse_args(["inspect", "nfs", "sessions"])
        self.assertEqual(parsed.inspection_view, "nfs_sessions")
        before = {"nfs_sessions": {"server_instance_id": "instance", "complete": True,
                                   "sessions": [{"session_id": "abc", "client_id": "7",
                                                 "requested_slots": 64, "negotiated_slots": 64,
                                                 "slots_in_progress": 64,
                                                 "replay_hits": "3", "seq_misordered": "2",
                                                 "bad_slot": "1", "delay_errors": "4",
                                                 "slot_in_progress_retries": "3",
                                                 "connections": [{"connection_id": "2:17", "worker_id": 2}],
                                                 "workers": [2]}]}}
        live = {"server_instance_id": "instance", "complete": True,
                "threads": [{"worker_id": 0, "vfs": {"operations": [
                    {"name": "Write", "latency_avg_ns": "280000000",
                     "latency_samples_total": "100"}]}}]}
        before["live"] = live
        after = copy.deepcopy(before)
        after["nfs_sessions"]["sessions"][0]["slots_in_progress"] = 64
        state = WatchState()
        state.update(before)
        meta = state.update(after)
        self.assertEqual(meta["nfs_peaks"]["abc"], 64)
        output = render(after, meta, parsed, width=160)
        self.assertIn("abc", output)
        self.assertIn("CONNS", output)
        self.assertIn("IN-PROGRESS RETRY", output)
        self.assertIn("3", output)
        self.assertIn("64 / 0.280 s = 229 req/s", output)
        self.assertIn("A full slot table can also indicate a slow backend", output)

    def test_nfs_slot_table_input(self):
        parsed = _build_parser().parse_args(["inspect", "nfs", "sessions", "--slots", "abc"])
        snapshot = {"nfs_sessions": {"server_instance_id": "instance", "complete": True,
                                     "slots": [{"slot": 0, "state": "IN_PROGRESS", "seqid": 4,
                                                "age_ns": "1500000000", "operation": None}]}}
        output = render(snapshot, {"nfs_peaks": {}}, parsed, width=160)
        self.assertIn("IN_PROGRESS", output)
        self.assertIn("1500.000 ms", output)

    def test_nfs_clients_saved_input_and_columns(self):
        parsed = _build_parser().parse_args([
            "inspect", "nfs", "clients", "--input",
            str(ROOT / "examples" / "nfs-clients.json"), "--wide"])
        snapshot = fetch(None, parsed)
        output = render(snapshot, {}, parsed, width=160)
        self.assertIn("NFSv4.1+ CLIENTS", output)
        self.assertIn("confirmed", output)
        self.assertIn("10.0.0.5:43210", output)
        self.assertIn("6c696e75782d636c69656e74", output)
        self.assertIn("DELEGATIONS", output)

    def test_nfs_client_wide_addresses_group_ports_and_wrap_without_loss(self):
        parsed = _build_parser().parse_args([
            "inspect", "nfs", "clients", "--input",
            str(ROOT / "examples" / "nfs-clients.json"), "--wide"])
        snapshot = fetch(None, parsed)
        client = snapshot["nfs_clients"]["clients"][0]
        client["remote_addresses"] = [
            "10.0.0.5:43210", "10.0.0.5:43211",
            "[2001:db8::5]:43210", "[2001:db8::5]:43211"]
        client["owner_hex"] = "ab" * 64
        output = render(snapshot, {}, parsed, width=130)
        self.assertIn("10.0.0.5:{43210,43211}", output)
        self.assertIn("[2001:db8::5]:{43210,43211}", output)
        self.assertIn("ab" * 32, output)
        self.assertLessEqual(max(map(len, output.splitlines())), 130)

    def test_dashboard_includes_nfs_sessions_when_nfs_is_enabled(self):
        snapshot = frames()
        snapshot["nfs_sessions"] = {
            "server_instance_id": snapshot["threads"]["server_instance_id"],
            "complete": True,
            "sessions": [{"session_id": "abcdef0123456789", "client_id": "9",
                          "negotiated_slots": 64, "slots_in_progress": 3,
                          "connections": [{"connection_id": "0:7"}]}],
        }
        meta = WatchState().update(snapshot)
        output = render(snapshot, meta, args(), width=160)
        self.assertIn("NFSv4.1+ SESSIONS", output)
        self.assertIn("abcdef01", output)

    def test_scoped_ipv6_endpoint_keeps_interface_and_port(self):
        self.assertEqual(endpoint({"family": "ipv6", "address": "fe80::1", "scope_id": 3, "port": 2049}), "[fe80::1%3]:2049")

    def test_ports_protocols_and_all_eight_workers(self):
        first = frames()
        meta = WatchState().update(first)
        output = render(first, meta, args())
        for i in range(8):
            self.assertIn("10.0.0.20:{}".format(40100 + i), output)
            self.assertIn("chm_core_{}".format(i), output)
        self.assertIn("[2001:db8::20]:50101", output)
        self.assertIn("NFSv4.1", output)
        self.assertIn("SMB/3.1.1", output)
        self.assertIn("Synthetic example data", output)
        self.assertIn("RX MSGS", render(first, meta, args("--wide")))

    def test_quiet_wide_age_and_responsive_layouts(self):
        sample = frames()
        sample["connections"]["connections"][0]["age_ms"] = 537000
        meta = WatchState().update(sample)
        quiet = render(sample, meta, args("--quiet"), width=80)
        self.assertNotIn("INSPECTION", quiet)
        self.assertNotIn("Rates are histogram", quiet)
        self.assertIn("localhost:8080", quiet)
        self.assertIn("WORKER METRICS", quiet)
        self.assertIn("CONNECTION 0:17", quiet)
        self.assertLessEqual(max(map(len, quiet.splitlines())), 80)
        wide = render(sample, meta, args("--wide", "--quiet"), width=120)
        self.assertIn("Instance ID:", wide)
        self.assertIn("8m 57s", wide)
        self.assertIn("[2001:db8::20]:50101", wide)
        self.assertLessEqual(max(map(len, wide.splitlines())), 120)

    def test_rates_use_monotonic_clock_and_absence_uses_full_samples(self):
        state = WatchState()
        self.assertEqual(state.update(frames())["rates"], {})
        result = state.update(frames("snapshot-next.json"))
        self.assertEqual(result["rates"]["0"], {"rpc": 500.0, "vfs": 750.0})
        self.assertEqual([r["connection_id"] for r in result["absent"]], ["0:17"])

    def test_live_worker_cpu_and_busy_percentages_use_deltas(self):
        state = WatchState()
        before = {
            "server_instance_id": "instance",
            "complete": True,
            "sampled_monotonic_ns": "100",
            "threads": [{"worker_id": 0, "cpu_ns": "100", "loop_iters": "1000",
                         "runq_wait_ns": "10", "loop_iters_with_work": "700",
                         "rpc": {"rx_msgs": "1", "rx_bytes": "0", "tx_bytes": "0"}}],
        }
        after = copy.deepcopy(before)
        after["sampled_monotonic_ns"] = "1000000100"
        after["threads"][0].update(cpu_ns="500000100", runq_wait_ns="20000010", loop_iters="1100", loop_iters_with_work="780",
                                    rpc={"rx_msgs": "11", "rx_bytes": "10000000", "tx_bytes": "5000000"})
        state.update({"live": before})
        rates = state.update({"live": after})["live_rates"]["0"]
        self.assertEqual(rates, {"cpu": 50.0, "runq": 2.0, "busy": 80.0, "rpc_s": 10.0, "rx_MB_s": 10.0, "tx_MB_s": 5.0})

    def test_large_counters_do_not_lose_integer_precision(self):
        before, after = frames(), frames("snapshot-next.json")
        before["threads"]["threads"][0]["rpc"]["procedures"][0]["latency_samples_total"] = str(2 ** 60)
        after["threads"]["threads"][0]["rpc"]["procedures"][0]["latency_samples_total"] = str(2 ** 60 + 10)
        state = WatchState(); state.update(before)
        self.assertEqual(state.update(after)["rates"]["0"]["rpc"], 5.0)

    def test_counter_reset_clock_reset_and_context_change_have_no_rate(self):
        for change in ("counter", "clock", "context"):
            before, after = frames(), frames("snapshot-next.json")
            row = after["threads"]["threads"][0]
            if change == "counter":
                row["rpc"]["procedures"][0]["latency_samples_total"] = "0"
            elif change == "clock":
                row["sampled_monotonic_ns"] = "1"
            else:
                row["rpc_thread_id"] = 999
            state = WatchState(); state.update(before)
            self.assertIsNone(state.update(after)["rates"].get("0", {}).get("rpc"))

    def test_partial_sample_never_means_disconnected(self):
        state = WatchState(); state.update(frames())
        partial = frames("snapshot-next.json")
        partial["connections"].update(complete=False, missing_workers=[0])
        result = state.update(partial)
        self.assertFalse(result["changes_comparable"])
        self.assertEqual(result["absent"], [])
        self.assertFalse(state.update(frames("snapshot-next.json"))["changes_comparable"])

    def test_restart_and_mixed_dashboard_reset_comparison(self):
        state = WatchState(); state.update(frames())
        new = frames("snapshot-next.json")
        for snap in new.values():
            snap["server_instance_id"] = "new-process"
        result = state.update(new)
        self.assertTrue(result["restarted"])
        self.assertEqual(result["rates"], {})
        mixed = frames(); mixed["connections"]["server_instance_id"] = "other"
        result = state.update(mixed)
        self.assertTrue(result["mixed_instances"])
        self.assertIsNone(state.previous)

    def test_filtered_dashboard_keeps_whole_worker_metrics(self):
        a = args("--input", str(ROOT / "examples/snapshot.json"), "--remote-port", "40100")
        result = fetch(None, a)
        self.assertEqual(len(result["connections"]["connections"]), 1)
        self.assertEqual(len(result["threads"]["threads"]), 8)
        self.assertEqual(result["threads"]["threads"][0]["rpc"]["procedures"][0]["latency_samples_total"], "81234")

    def test_unknown_version_duplicates_and_terminal_control_characters(self):
        row = frames()["connections"]["connections"][0]
        row["nfs_minor_versions_seen"] = []
        self.assertEqual(protocol(row), "NFSv4.?")
        bad = frames()["connections"]
        bad["connections"].append(copy.deepcopy(bad["connections"][0]))
        with self.assertRaises(ValueError):
            valid(bad, "connections")
        self.assertNotIn("\x1b", endpoint({"family": "ipv4", "address": "x\x1b[2J", "port": 2049}))

    def test_watch_json_and_stale_frames(self):
        a = args("--watch", "--iterations", "3", "--json")
        stream = io.StringIO()
        with patch("chimera_admin.inspection.fetch", side_effect=[frames(), ChimeraAdminError("temporary", 503), frames("snapshot-next.json")]), patch("chimera_admin.inspection.time.sleep"), redirect_stdout(stream):
            code = run_inspection(None, a)
        result = [json.loads(line) for line in stream.getvalue().splitlines()]
        self.assertEqual(code, 0)
        self.assertEqual(len(result), 3)
        self.assertTrue(result[1]["_cli"]["stale"])
        self.assertEqual(result[2]["_cli"]["rates"], {})
        self.assertEqual(result[2]["_cli"]["absent"], [])
        self.assertNotIn("\x1b", stream.getvalue())

    def test_cursor_restored_on_ctrl_c(self):
        class Terminal(io.StringIO):
            def isatty(self):
                return True
        stream = Terminal()
        with patch("chimera_admin.inspection.fetch", side_effect=KeyboardInterrupt), patch.dict(os.environ, {"TERM": "xterm"}), redirect_stdout(stream):
            self.assertEqual(run_inspection(None, args("--watch")), 130)
        self.assertTrue(stream.getvalue().endswith("\x1b[?25h"))

    def test_python_38_syntax(self):
        for source in (ROOT / "chimera_admin").glob("*.py"):
            ast.parse(source.read_text(), feature_version=(3, 8))


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        target = urlsplit(self.path)
        self.server.requests_seen.append((target.path, parse_qs(target.query), self.headers.get("Authorization")))
        status = self.server.test_status
        if target.path == "/version":
            body = {"version": "test-version"}
        elif target.path.endswith("threads"):
            body = fixture()["threads_snapshot"]
        else:
            body = fixture()["connections_snapshot"]
        if status != 200:
            body = {"message": "test API error"}
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Retry-After", "1")
        if status == 302:
            self.send_header("Location", "/redirected")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


class HTTPIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.server.requests_seen = []
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown(); cls.server.server_close(); cls.thread.join()

    def setUp(self):
        self.server.test_status = 200
        self.server.requests_seen.clear()

    def client(self, **kwargs):
        return ChimeraAdminClient(host="127.0.0.1", port=self.server.server_port, timeout=2, **kwargs)

    def test_real_http_filters_and_bearer_auth(self):
        with self.client(token="example-test-token") as client:
            result = client.get_connections(worker_id=3, remote_port=40103, service="nfs")
        self.assertTrue(result["complete"])
        path, query, auth = self.server.requests_seen[0]
        self.assertEqual(path, "/api/v1/connections")
        self.assertEqual(query, {"worker_id": ["3"], "remote_port": ["40103"], "service": ["nfs"]})
        self.assertEqual(auth, "Bearer example-test-token")

    def test_basic_auth_and_existing_version_command(self):
        with self.client(username="example-user", password="example-password") as client:
            self.assertEqual(client.get_version(), {"version": "test-version"})
        self.assertTrue(self.server.requests_seen[0][2].startswith("Basic "))

    def test_429_and_redirects_are_not_followed(self):
        for status in (429, 302, 404):
            self.server.test_status = status
            with self.client() as client:
                with self.assertRaises(ChimeraAdminError) as got:
                    client.get_threads()
            self.assertEqual(got.exception.status_code, status)
            self.assertEqual(got.exception.retry_after, 1)
        self.assertFalse(any(path == "/redirected" for path, _, _ in self.server.requests_seen))

    def test_cli_dashboard_network_and_partial_exit_status(self):
        stream = io.StringIO()
        env = {"CHIMERA_ADMIN_TOKEN": "", "CHIMERA_ADMIN_USER": "", "CHIMERA_ADMIN_PASSWORD": ""}
        with patch.dict(os.environ, env), redirect_stdout(stream):
            status = main(["--host", "127.0.0.1", "--port", str(self.server.server_port), "inspect", "dashboard", "--json"])
        self.assertEqual(status, 0)
        self.assertEqual([r[0] for r in self.server.requests_seen],
                         ["/api/v1/threads", "/api/v1/threads", "/api/v1/connections",
                          "/api/v1/nfs/sessions"])
        self.assertEqual(self.server.requests_seen[1][1], {"live": ["1"]})
        self.assertEqual(len(json.loads(stream.getvalue())["connections_snapshot"]["connections"]), 10)
        sample = frames(); sample["connections"].update(complete=False, truncated=True)
        with patch("chimera_admin.inspection.fetch", return_value=sample), redirect_stdout(io.StringIO()):
            self.assertEqual(run_inspection(None, args()), 4)


if __name__ == "__main__":
    unittest.main()
