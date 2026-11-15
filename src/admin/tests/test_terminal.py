# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Apache-2.0

"""Worker tree behavior and actual curses terminal restoration."""
import copy
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import patch

from chimera_admin.cli import _build_parser, main
from chimera_admin.fleet import Failure, PollGroup, Target, TargetView
from chimera_admin.terminal import TreeState, _put, run_interactive, screen_loop

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


def args(*extra):
    return _build_parser().parse_args(["inspect", "dashboard", "--interactive"] + list(extra))


def snapshot():
    raw = json.loads((EXAMPLES / "interactive-snapshot.json").read_text())
    return {"threads": raw["threads_snapshot"], "connections": raw["connections_snapshot"]}


def result(data, at=1, error=None):
    return {"kind": "result", "attempt": int(at), "finished": at, "snapshots": data, "error": error, "done": False}


def view(name="nas-a", data=None):
    item = TargetView(Target(name, "http://localhost:8080", "localhost", 8080, False, .1, {}))
    item.accept(result(data or snapshot()))
    return item


def text(model, width=240):
    return "\n".join(line.text for line in model.lines(width, 3)[0])


class TreeBehavior(unittest.TestCase):
    def test_expansion_many_connections_zero_worker_and_filters(self):
        sample = snapshot()
        sample["connections"]["connections"] = [c for c in sample["connections"]["connections"] if c["service"] == "nfs"]
        v = view(data=sample)
        model = TreeState(args("--service", "nfs", "--max-rows", "1"))
        model.refresh([v])
        self.assertEqual(len(model.order), 8)
        self.assertNotIn("CLIENT", text(model))
        model.action("expand")
        worker = model.rows[model.selected]
        self.assertGreater(len(worker.connections), 5)
        output = text(model)
        self.assertEqual(output.count("CLIENT"), 1)
        self.assertIn("MATCH", output)
        self.assertRegex(output, r"\s+8\s+8\s+16\s+81234")
        for c in worker.connections:
            self.assertIn(c["connection_id"], output)
        model.action("collapse")
        self.assertNotIn("CLIENT", text(model))
        model.action("toggle")
        self.assertIn(model.selected, model.expanded)
        model.action("expand_all")
        self.assertEqual(model.expanded, set(model.order))
        self.assertIn("chm_core_7", text(model))
        self.assertIn("TOTAL(all)", text(model))
        self.assertIn("No matching connections", text(model))
        model.action("collapse_all")
        self.assertEqual(model.expanded, set())
        # Filtering children never changes the worker's all-traffic counters.
        sample["connections"]["connections"] = sample["connections"]["connections"][:1]
        v.accept(result(sample, 2)); model.refresh([v])
        self.assertIn("MATCH", text(model))
        self.assertIn("TOTAL(all)", text(model))

    def test_selection_disappearance_and_refresh_preserve_state(self):
        v = view(); model = TreeState(args()); model.refresh([v])
        model.action("next"); model.action("expand")
        selected = model.selected
        v.accept(result(snapshot(), 2)); model.refresh([v])
        self.assertEqual(model.selected, selected)
        self.assertIn(selected, model.expanded)
        newer = snapshot()
        newer["threads"]["threads"] = [w for w in newer["threads"]["threads"] if w["worker_id"] != 1]
        newer["connections"]["connections"] = [c for c in newer["connections"]["connections"] if c["worker_id"] != 1]
        v.accept(result(newer, 3)); model.refresh([v])
        self.assertEqual(model.selected[2], 2)
        self.assertNotIn(selected, model.expanded)
        model.action("previous")
        self.assertEqual(model.selected[2], 0)
        model.action("previous")
        self.assertEqual(model.selected[2], 0)

    def test_colliding_ids_and_restart_reset_only_one_target(self):
        a, b = view("a"), view("b")
        model = TreeState(args()); model.refresh([a,b]); model.action("expand_all")
        self.assertEqual(len(model.expanded), 16)
        b_key = next(key for key in model.order if key[0] == "b")
        model.selected = b_key
        newer = snapshot()
        for item in newer.values(): item["server_instance_id"] = "new-process"
        a.accept(result(newer, 2)); model.refresh([a,b])
        self.assertEqual(model.selected, b_key)
        self.assertEqual(model.expanded, {key for key in model.order if key[0] == "b"})
        self.assertEqual(len(model.rows), 16)
        self.assertIn("a", text(model)); self.assertIn("b", text(model))

    def test_partial_missing_and_failed_refresh_are_never_zero(self):
        v = view(); model = TreeState(args()); model.refresh([v]); model.action("expand")
        first_id = model.rows[model.selected].connections[0]["connection_id"]
        partial = snapshot()
        partial["threads"].update(complete=False, missing_workers=[0])
        partial["threads"]["threads"] = partial["threads"]["threads"][1:]
        partial["connections"].update(complete=False, missing_workers=[0])
        partial["connections"]["connections"] = []
        v.accept(result(partial, 2)); model.refresh([v])
        output = text(model)
        self.assertIn("PARTIAL", output); self.assertIn("MISSING", output)
        self.assertIn(first_id, output); self.assertIn("UNCONFIRMED", output)
        self.assertRegex(output, r">=0\s+8")
        self.assertNotIn("No matching connections", output)
        self.assertNotIn("ABSENT ", output)
        self.assertNotIn("RPC/s=0", output)
        v.accept(result(None, 3, Failure("unreachable", 1))); model.refresh([v])
        self.assertIn("STALE", text(model)); self.assertIn("Error: unreachable", text(model))
        self.assertIn(first_id, text(model)); self.assertEqual(v.meta["rates"], {})
        # A cold missing worker has unknown counters, not zero counters.
        cold = TreeState(args()); cold.refresh([view(data=partial)])
        self.assertRegex(text(cold), r"--\s+--\s+--\s+--")

    def test_partial_additions_do_not_grow_retained_history(self):
        v = view(); model = TreeState(args()); model.refresh([v])
        partial = snapshot(); partial["connections"]["complete"] = False
        conn = copy.deepcopy(partial["connections"]["connections"][0]); conn["connection_id"] = "0:999"
        partial["connections"]["connections"] = [conn]
        v.accept(result(partial, 2)); model.refresh([v])
        self.assertIn("0:999", [c["connection_id"] for c in model.rows[model.selected].connections])
        partial["connections"]["connections"] = []
        v.accept(result(partial, 3)); model.refresh([v])
        self.assertNotIn("0:999", [c["connection_id"] for c in model.rows[model.selected].connections])

    def test_restart_between_gets_does_not_join_connections(self):
        data = snapshot(); data["connections"]["server_instance_id"] = "other-process"
        model = TreeState(args()); model.refresh([view(data=data)]); model.action("expand_all")
        self.assertIn("INCONSISTENT", text(model)); self.assertNotIn("CONNECTION 0:", text(model))
        self.assertIn(">=0", text(model)); self.assertNotIn("No matching connections", text(model))

    def test_paging_wraps_full_endpoints_and_does_not_select_children(self):
        v = view(); model = TreeState(args()); model.refresh([v]); model.action("expand_all")
        for width in (80, 120):
            rendered = model.lines(width, 2)[0]
            self.assertTrue(all(len(line.text) <= width for line in rendered))
            self.assertIn("[2001:db8::20]:50101", "".join(line.text.strip() for line in rendered))
        lines, positions = model.lines(60, 2)
        model.viewport(lines, positions, 8)
        selected = model.selected
        model.action("page_down", 8); model.viewport(lines, positions, 8)
        self.assertGreater(model.scroll, 0); self.assertEqual(model.selected, selected)
        # Strip continuation indentation: full data remains accessible by scrolling.
        flattened = "".join(line.text.strip() for line in lines)
        self.assertIn("10.0.0.20:40100", flattened)
        model.action("next"); self.assertEqual(model.selected[2], 1)
        self.assertEqual(len(model.order), 8)

    def test_rates_warm_up_again_after_failure_and_do_not_infer_absence(self):
        v = view(); model = TreeState(args()); model.refresh([v])
        self.assertIn("-- (warming up)", text(model))
        newer = snapshot()
        for w in newer["threads"]["threads"]:
            w["sampled_monotonic_ns"] = str(int(w["sampled_monotonic_ns"]) + 1000000000)
            p = w["rpc"]["procedures"][0]
            p["latency_samples_total"] = str(int(p["latency_samples_total"]) + 10)
        v.accept(result(newer, 2)); model.refresh([v])
        self.assertIn("10.0", text(model))
        v.accept(result(None, 3, Failure("timeout", 1))); model.refresh([v])
        self.assertIn("-- (stale)", text(model))
        newer["connections"]["connections"] = []
        v.accept(result(newer, 4)); model.refresh([v])
        self.assertIn("-- (warming up)", text(model))
        self.assertNotIn("ABSENT ", text(model))
        self.assertEqual(model.rows[model.selected].connections, [])

    def test_pending_and_failure_groups_are_visible(self):
        v = TargetView(view().target); model = TreeState(args()); model.refresh([v])
        self.assertIn("PENDING", text(model)); self.assertIn("Workers unavailable", text(model))
        v.accept(result(None, error=Failure("down", 1))); model.refresh([v])
        self.assertIn("UNREACHABLE", text(model)); self.assertIn("down", text(model))


class Polling(unittest.TestCase):
    def test_healthy_advances_and_expansion_never_fetches(self):
        release = threading.Event()
        class Client:
            def __init__(self, blocked=False): self.blocked, self.calls, self.closed = blocked, 0, False
            def get_threads(self, **kwargs):
                self.calls += 1
                if self.blocked: release.wait(4)
                return snapshot()["threads"]
            def get_connections(self, **kwargs):
                self.calls += 1
                return snapshot()["connections"]
            def close(self): self.closed = True
        clients = {"a": Client(), "b": Client(True)}
        options = args("--iterations", "2"); options.watch=True; options.interval=.05
        group = PollGroup([view("a").target, view("b").target], options, lambda t: clients[t.name])
        model = TreeState(options)
        try:
            group.start(); deadline = time.monotonic()+2
            while not group.views[0].done and time.monotonic() < deadline:
                group.ready.wait(.02); group.update()
            self.assertTrue(group.views[0].done)
            self.assertEqual(clients["a"].calls, 4)
            self.assertEqual(clients["b"].calls, 1)
            model.refresh(group.views)
            for _ in range(20):
                model.action("expand_all"); model.action("collapse_all"); model.lines(100, 2)
            self.assertEqual(clients["a"].calls, 4)
            started=time.monotonic(); group.close()
            self.assertLess(time.monotonic()-started, .5)
        finally:
            release.set(); group.close()
        self.assertTrue(clients["a"].closed)


class FakeCurses:
    error = RuntimeError
    A_REVERSE, A_BOLD = 1, 2
    KEY_UP, KEY_DOWN, KEY_LEFT, KEY_RIGHT = 101, 102, 103, 104
    KEY_PPAGE, KEY_NPAGE, KEY_ENTER, KEY_RESIZE = 105, 106, 107, 108
    def __init__(self): self.cursor=[]
    def curs_set(self, value): self.cursor.append(value); return 1


class FakeScreen:
    def __init__(self, keys): self.keys=iter(keys); self.sizes=[]; self.draws=0
    def keypad(self, value): pass
    def timeout(self, value): pass
    def getmaxyx(self): return (20, 100) if self.draws else (5, 10)
    def erase(self): self.draws += 1
    def addnstr(self, *values): self.sizes.append(values)
    def refresh(self): pass
    def getch(self):
        item=next(self.keys)
        if isinstance(item, Exception): raise item
        return item


class Lifecycle(unittest.TestCase):
    def test_screen_resize_keys_and_cursor_on_exit_and_exception(self):
        for last, expected in ((ord("q"), 0), (3,130), (ValueError("paint failure"), None)):
            curses = FakeCurses(); screen = FakeScreen([108, ord("a"), 105, 102, 104, 103, 107, 32, ord("c"), last])
            class Group:
                views=[view()]; done=False
                def start(self): pass
                def update(self): return True
            if expected is None:
                with self.assertRaises(ValueError): screen_loop(screen, curses, Group(), args())
            else:
                self.assertEqual(screen_loop(screen, curses, Group(), args()), expected)
            self.assertEqual(curses.cursor, [0,1])
            self.assertGreater(screen.draws, 2)

    def test_display_clips_by_cells_and_preserves_combining_characters(self):
        screen, curses = FakeScreen([]), FakeCurses()
        value = "e\u0301" * 8 + "XYZ"
        _put(screen, curses, 0, value, 10)
        rendered = screen.sizes[-1]
        self.assertEqual(rendered[2], "e\u0301" * 8 + "X")
        self.assertEqual(rendered[3], len(rendered[2]))

    def test_tty_and_option_errors_precede_network_and_credentials(self):
        for options, message in ((("--json",), "--json"), (("--no-clear",), "--no-clear"), ((), "requires a TTY")):
            with patch("sys.stdin.isatty", return_value=False), patch("sys.stdout.isatty", return_value=False):
                with self.assertRaisesRegex(ValueError, message):
                    run_interactive(args(*options), lambda: self.fail("credentials must not be read"))
        with patch("sys.stderr", new_callable=io.StringIO) as err:
            self.assertEqual(main(["inspect", "dashboard", "--interactive", "--json"]),2)
            self.assertIn("--interactive cannot be combined with --json",err.getvalue())

    @unittest.skipUnless(os.name == "posix", "requires POSIX PTY")
    def test_real_curses_restores_terminal_on_q_sigint_and_exception(self):
        import fcntl
        import pty
        import select
        import struct
        import termios
        for mode in ("q", "sigint", "exception"):
            with self.subTest(mode=mode):
                master, slave = pty.openpty()
                fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 30, 160, 0, 0))
                original = termios.tcgetattr(slave)
                code = "from chimera_admin.cli import main\n"
                if mode == "exception":
                    code += "from chimera_admin.terminal import TreeState\ndef fail(*a): raise RuntimeError('injected paint failure')\nTreeState.lines=fail\n"
                code += "raise SystemExit(main(['inspect','dashboard','--interactive','--input'," + repr(str(EXAMPLES / "interactive-snapshot.json")) + "]))\n"
                child = subprocess.Popen([sys.executable,"-c",code], stdin=slave, stdout=slave, stderr=slave, env=dict(os.environ,TERM="xterm"))
                output, sent = b"", False
                try:
                    deadline = time.monotonic()+5
                    while child.poll() is None and time.monotonic()<deadline:
                        if select.select([master], [], [], .1)[0]: output += os.read(master,65536)
                        if b"Chimera inspection" in output and not sent:
                            sent = True
                            if mode == "q":
                                fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 20, 90, 0, 0))
                                child.send_signal(signal.SIGWINCH)
                                os.write(master,b"aq")
                            elif mode == "sigint": child.send_signal(signal.SIGINT)
                    child.wait(timeout=2)
                    while select.select([master], [], [], 0)[0]: output += os.read(master,65536)
                    self.assertEqual(termios.tcgetattr(slave),original)
                    self.assertIn(b"\x1b[?25h",output)
                    self.assertEqual(child.returncode,0 if mode == "q" else 130 if mode == "sigint" else 1, output.decode(errors="replace"))
                finally:
                    if child.poll() is None: child.kill(); child.wait()
                    os.close(master); os.close(slave)

    @unittest.skipUnless(os.name == "posix", "requires POSIX PTY")
    def test_text_watch_q_exits_immediately_and_restores_terminal(self):
        import fcntl
        import pty
        import select
        import struct
        import termios

        master, slave = pty.openpty()
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 30, 120, 0, 0))
        original = termios.tcgetattr(slave)
        snapshot_path = EXAMPLES / "interactive-snapshot.json"
        code = (
            "import json\nfrom pathlib import Path\nfrom unittest.mock import patch\n"
            "from chimera_admin.cli import main\n"
            "raw=json.loads(Path(" + repr(str(snapshot_path)) + ").read_text())\n"
            "data={'threads':raw['threads_snapshot'],'connections':raw['connections_snapshot']}\n"
            "with patch('chimera_admin.inspection.fetch', return_value=data):\n"
            " raise SystemExit(main(['inspect','dashboard','--watch','--interval','1']))\n"
        )
        child = subprocess.Popen([sys.executable, "-c", code], stdin=slave, stdout=slave,
                                 stderr=slave, env=dict(os.environ, TERM="xterm"))
        output, sent = b"", False
        try:
            deadline = time.monotonic() + 5
            while child.poll() is None and time.monotonic() < deadline:
                if select.select([master], [], [], .1)[0]:
                    output += os.read(master, 65536)
                if b"INSPECTION DASHBOARD" in output and not sent:
                    sent = True
                    os.write(master, b"q")
            child.wait(timeout=2)
            while select.select([master], [], [], 0)[0]:
                output += os.read(master, 65536)
            self.assertTrue(sent, output.decode(errors="replace"))
            self.assertEqual(child.returncode, 0, output.decode(errors="replace"))
            self.assertEqual(termios.tcgetattr(slave), original)
            self.assertIn(b"\x1b[?25h", output)
        finally:
            if child.poll() is None:
                child.kill(); child.wait()
            os.close(master); os.close(slave)


if __name__ == "__main__":
    unittest.main()
