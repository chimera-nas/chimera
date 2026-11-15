# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Apache-2.0

"""Curses worker tree over the same bounded pollers used by text watch.

TreeState is terminal-independent. Only the UI thread touches it or curses;
network sessions, retries, deadlines and comparison baselines stay in fleet.
"""
from dataclasses import dataclass, field
import os
import sys
import time
import unicodedata

from .client import ChimeraAdminClient
from .fleet import PollGroup, Target, configured_targets, overall_code, target_client
from .inspection import _age_text, _integer, _metric_map, _rate_text, _table, complete, endpoint, protocol, safe


def validate_interactive(args):
    if args.inspection_view != "dashboard":
        raise ValueError("--interactive requires inspect dashboard")
    if args.json:
        raise ValueError("--interactive cannot be combined with --json")
    if args.no_clear:
        raise ValueError("--interactive cannot be combined with --no-clear")
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        raise ValueError("--interactive requires a TTY on both stdin and stdout; use --watch for pipes")
    if os.environ.get("TERM") == "dumb":
        raise ValueError("--interactive requires a cursor-addressable terminal (TERM is dumb)")
    if args.target and not args.targets:
        raise ValueError("--target requires --targets")


def _mapping(value):
    return value if isinstance(value, dict) else {}


def _metric_total(worker, kind):
    try:
        values = _metric_map(worker, kind)
        return sum(values.values()) if values is not None else None
    except (TypeError, ValueError):
        return None


def _rate_cell(row, kind):
    value = row.rates.get(kind)
    if value is not None:
        return _rate_text(value)
    if row.status != "LIVE":
        reason = row.status.lower()
    elif _metric_total(row.worker, kind) is None or _integer(row.worker.get("sampled_monotonic_ns")) is None:
        reason = "unavailable"
    else:
        reason = "warming up"
    return "-- ({})".format(reason)


def _total(worker):
    counts = _mapping(worker.get("connection_counts"))
    values = [_integer(value) for value in counts.values()]
    return sum(values) if values and all(value is not None for value in values) else None


def _wrap(text, width, indent=""):
    """Wrap full data, including wide Unicode characters, without ellipses."""
    width = max(1, width)
    text = safe(text)
    lines, current, cells = [], "", 0
    continuation = indent[:max(0, width - 1)]
    for char in text:
        size = 0 if unicodedata.combining(char) else 2 if unicodedata.east_asian_width(char) in ("W", "F") else 1
        if current and cells + size > width:
            lines.append(current)
            current, cells = continuation, len(continuation)
        current += char
        cells += size
    lines.append(current)
    return lines


@dataclass
class WorkerRow:
    key: tuple
    worker: dict
    connections: list
    observed: int
    uncertain: set = field(default_factory=set)
    status: str = "LIVE"
    matching_complete: bool = True
    rates: dict = field(default_factory=dict)
    appeared: set = field(default_factory=set)


@dataclass
class Line:
    text: str
    key: object = None
    kind: str = "note"
    head: bool = False


class TreeState:
    def __init__(self, args):
        self.args = args
        self.expanded = set()
        self.selected = None
        self.order = []
        self.rows = {}
        self.groups = []
        self.instances = {}
        self.worker_baselines = {}
        self.connection_baselines = {}
        self.scroll = 0
        self.focus = True

    def refresh(self, views):
        old_index = self.order.index(self.selected) if self.selected in self.order else 0
        rows, order, groups = {}, [], []
        for view in views:
            name = view.target.name
            threads = view.snapshots.get("threads", {})
            connections = view.snapshots.get("connections", {})
            instance = threads.get("server_instance_id") or connections.get("server_instance_id")
            if not instance:
                groups.append((view, [], []))
                continue
            if self.instances.get(name) != instance:
                self.expanded = {key for key in self.expanded if key[0] != name}
                self.worker_baselines.pop(name, None)
                self.connection_baselines.pop(name, None)
                self.instances[name] = instance
            consistent = connections.get("server_instance_id") == instance
            current_workers = {w["worker_id"]: w for w in threads.get("threads", [])}
            if complete(threads):
                self.worker_baselines[name] = dict(current_workers)
                self.expanded = {key for key in self.expanded if key[0] != name or key[2] in current_workers}
            workers = dict(self.worker_baselines.get(name, {}))
            workers.update(current_workers)
            for worker in threads.get("missing_workers", []):
                workers.setdefault(worker, {"worker_id": worker})
            current_connections = {c["connection_id"]: c for c in connections.get("connections", [])} if consistent else {}
            connection_complete = consistent and complete(connections)
            if connection_complete:
                self.connection_baselines[name] = dict(current_connections)
            listed = {} if not consistent else dict(self.connection_baselines.get(name, {}))
            listed.update(current_connections)
            grouped = {}
            for ident, conn in listed.items():
                worker = conn["worker_id"]
                workers.setdefault(worker, {"worker_id": worker, "worker_name": conn.get("worker_name")})
                grouped.setdefault(worker, []).append(conn)
            fresh = view.status() == "LIVE"
            keys = []
            for worker_id, worker in workers.items():
                key = (name, instance, worker_id)
                state = view.status() if worker_id in current_workers else "MISSING"
                rates = view.meta.get("rates", {}).get(str(worker_id), {}) if fresh and worker_id in current_workers else {}
                rates.update(view.meta.get("live_rates", {}).get(str(worker_id), {}) if fresh else {})
                children = grouped.get(worker_id, [])
                children.sort(key=(lambda c: (endpoint(c.get("remote")), c["connection_id"])) if self.args.sort == "client" else (lambda c: c["connection_id"]))
                rows[key] = WorkerRow(key, worker, children,
                    sum(c["worker_id"] == worker_id for c in current_connections.values()),
                    set(listed) - set(current_connections), state, connection_complete, rates,
                    set(view.meta.get("appeared", [])) if fresh else set())
                keys.append(key)
            def sort_key(key):
                row = rows[key]
                if self.args.sort in ("rpc-rate", "vfs-rate"):
                    value = row.rates.get("rpc" if self.args.sort == "rpc-rate" else "vfs")
                    return (-(value if value is not None else -1), key[2])
                if self.args.sort == "connections":
                    total = _total(row.worker)
                    return (-(total if total is not None else -1), key[2])
                return (key[2],)
            keys.sort(key=sort_key)
            notes = []
            for kind, snapshot in view.snapshots.items():
                if not complete(snapshot):
                    notes.append("{} PARTIAL: truncated={} missing_workers={}".format(kind,
                        snapshot.get("truncated", False), snapshot.get("missing_workers", [])))
            if not consistent and connections:
                notes.append("INCONSISTENT instances: connections are not joined to these workers")
            if fresh and view.meta.get("changes_comparable"):
                for conn in view.meta.get("absent", [])[:10]:
                    notes.append("ABSENT {} {} (transport observation, not lease expiry)".format(
                        conn["connection_id"], endpoint(conn.get("remote"))))
            if view.meta.get("restarted"):
                notes.append("Instance restarted; this target's tree and comparison baselines reset")
            groups.append((view, keys, notes))
            order.extend(keys)
        self.rows, self.order, self.groups = rows, order, groups
        if self.selected not in self.rows:
            self.selected = self.order[min(old_index, len(self.order) - 1)] if self.order else None
            self.focus = True

    def action(self, action, page=10):
        if action in ("next", "previous") and self.order:
            index = self.order.index(self.selected) if self.selected in self.order else 0
            index = max(0, min(len(self.order) - 1, index + (1 if action == "next" else -1)))
            self.selected = self.order[index]
        elif action == "expand_all":
            self.expanded.update(self.order)
        elif action == "collapse_all":
            self.expanded.clear()
        elif action in ("expand", "collapse", "toggle") and self.selected is not None:
            if action == "collapse" or (action == "toggle" and self.selected in self.expanded):
                self.expanded.discard(self.selected)
            else:
                self.expanded.add(self.selected)
        elif action in ("page_up", "page_down"):
            self.scroll = max(0, self.scroll + (-page if action == "page_up" else page))
            self.focus = False
            return
        self.focus = True

    def lines(self, width, now):
        result, positions = [], {}
        def add(text, key=None, kind="note", indent="    "):
            for i, part in enumerate(_wrap(text, width, indent)):
                result.append(Line(part, key, kind, i == 0))
        for view, keys, notes in self.groups:
            age = "--" if view.last_success is None else "{:.1f}s".format(max(0, now - view.last_success))
            instance = self.instances.get(view.target.name, "unavailable")
            add("[{}] {}  sample age {}  {}{}".format(view.target.name,
                view.status(), age, view.target.url, "  polling stopped" if view.done else ""), kind="target")
            if self.args.wide:
                add("  Instance ID: {}".format(instance), kind="target")
            if view.error:
                add("  Error: {}".format(view.error))
            for note in notes:
                if "PARTIAL" in note or "INCONSISTENT" in note or "restarted" in note or "ABSENT" in note:
                    if self.args.quiet and "ABSENT" in note:
                        note = note.split(" (transport observation", 1)[0]
                    add("  Warning: " + note)
                elif not self.args.quiet:
                    add("  " + note)
            if not keys:
                add("  Workers unavailable" if view.status() != "LIVE" else "  No workers returned")
            worker_rows = []
            for key in keys:
                row = self.rows[key]
                worker, counts = row.worker, _mapping(row.worker.get("connection_counts"))
                marker = "[-]" if key in self.expanded else "[+]"
                listed = str(row.observed) if row.matching_complete else ">={}".format(row.observed)
                matched_label = "MATCH" if any(getattr(self.args, field, None) is not None for field in
                    ("worker_id", "service", "transport", "local_ip", "local_port", "remote_ip", "remote_port")) else "LISTED"
                total = _total(worker)
                rpc = _mapping(worker.get("rpc"))
                rpc_samples, vfs_samples = _metric_total(worker, "rpc"), _metric_total(worker, "vfs")
                worker_rows.append([marker, safe(worker.get("worker_name", key[2])), safe(worker.get("os_tid")),
                    safe(worker.get("rpc_thread_id")), safe(counts.get("nfs")), safe(counts.get("smb")),
                    safe(counts.get("s3")), listed, safe(total), safe(rpc.get("server_inflight")),
                    safe(rpc_samples), safe(vfs_samples), safe(_rate_cell(row, "rpc")),
                    safe(_rate_cell(row, "vfs")), safe(_rate_cell(row, "rx_MB_s")),
                    safe(_rate_cell(row, "tx_MB_s")), safe(_rate_cell(row, "cpu")),
                    safe(_rate_cell(row, "busy")), row.status])
            if keys:
                headers = ["", "WORKER", "TID", "RPC ID", "NFS", "SMB", "S3", matched_label,
                    "TOTAL(all)", "RPC IN", "RPC SAMPLES", "VFS SAMPLES", "RPC samp/s", "VFS samp/s",
                    "RX MB/s", "TX MB/s", "CPU%", "BUSY%", "STATUS"]
                numeric = range(2, len(headers) - 1)
                formatted = _table(headers, worker_rows, 0, numeric=numeric).splitlines()
                add(formatted[0], kind="summary", indent="  ")
                for key, summary in zip(keys, formatted[1:]):
                    positions[key] = len(result)
                    add(summary, key, "worker", "  ")
            for key in keys:
                row = self.rows[key]
                worker = row.worker
                if key == self.selected:
                    rpc = _mapping(worker.get("rpc"))
                    add("    SELECTED WORKER DETAILS", key, "summary")
                    for detail_line in _table(["FIELD", "VALUE"], [["RPC CALLS IN-FLIGHT", rpc.get("client_inflight")]], 0).splitlines():
                        add(detail_line, key, "summary")
                auxiliary = ["{}={}".format(name, safe(counts.get(name))) for name in ("mount", "nlm", "nsm", "rpcbind")
                             if _integer(counts.get(name)) not in (0, None) or self.args.service == name]
                if auxiliary:
                    add("    Auxiliary connections: " + " ".join(auxiliary), key, "summary")
                if row.status != "LIVE":
                    add("    Worker sample: {} (unknown activity is not zero)".format(safe(worker.get("sampled_at"))), key, "summary")
                if key in self.expanded:
                    if not row.connections:
                        add("    No matching connections" if row.matching_complete and not view.error else "    Connections unavailable or incomplete; zero is not inferred")
                    for conn in row.connections:
                        ident = conn["connection_id"]
                        note = " UNCONFIRMED (last observed)" if ident in row.uncertain else " STALE" if view.error else " NEW" if ident in row.appeared else ""
                        add("    ID   CLIENT   SERVER   PROTOCOL   TRANSPORT   BACKEND   STATE", key, "connection", "      ") if conn is row.connections[0] else None
                        add("    {}{}  {}  {}  {}  {}  {}  {}".format(ident, note,
                            endpoint(conn.get("remote")), endpoint(conn.get("local")), protocol(conn),
                            safe(conn.get("transport")), safe(conn.get("backend")), safe(conn.get("state"))), key, "connection", "      ")
                        if self.args.wide:
                            add("      AGE {}  RX MSGS {}  RAW TRANSPORT {}".format(_age_text(conn.get("age_ms")),
                                safe(_mapping(conn.get("per_connection_stats")).get("messages_received")), safe(conn.get("raw_transport"))))
            add("")
        return result, positions

    def viewport(self, lines, positions, height):
        height = max(1, height)
        if self.focus and self.selected in positions:
            position = positions[self.selected]
            if position < self.scroll:
                self.scroll = position
            elif position >= self.scroll + height:
                self.scroll = position - height + 1
        self.focus = False
        self.scroll = min(max(0, self.scroll), max(0, len(lines) - height))
        return lines[self.scroll:self.scroll + height]


def _key_action(key, curses):
    return {curses.KEY_UP: "previous", curses.KEY_DOWN: "next", curses.KEY_RIGHT: "expand",
            curses.KEY_LEFT: "collapse", curses.KEY_PPAGE: "page_up", curses.KEY_NPAGE: "page_down",
            curses.KEY_ENTER: "toggle", 10: "toggle", 13: "toggle", 32: "toggle",
            ord("a"): "expand_all", ord("c"): "collapse_all"}.get(key)


def _put(screen, curses, row, text, width, attr=0):
    if width <= 1:
        return
    try:
        clipped = _wrap(text, width - 1)[0]
        screen.addnstr(row, 0, clipped, len(clipped), attr)
    except curses.error:
        # A resize can race a paint; the next iteration uses the new geometry.
        pass


def screen_loop(screen, curses, group, args):
    model = TreeState(args)
    previous_cursor = None
    try:
        try:
            previous_cursor = curses.curs_set(0)
        except curses.error:
            pass
        screen.keypad(True)
        screen.timeout(100)
        group.start()
        last_draw, size = 0.0, None
        while True:
            changed = group.update()
            if changed or not model.groups:
                model.refresh(group.views)
            height, width = screen.getmaxyx()
            now = time.monotonic()
            if changed or size != (height, width) or now - last_draw >= 0.2:
                screen.erase()
                if height < 6 or width < 20:
                    _put(screen, curses, 0, "Resize terminal; q exits", width)
                else:
                    if not args.quiet:
                        _put(screen, curses, 0, "Chimera inspection | worker rates are histogram samples/s", width)
                        synthetic = any(s.get("synthetic") for v in group.views for s in v.snapshots.values())
                        if synthetic:
                            _put(screen, curses, 1, "Synthetic example data", width)
                    lines, positions = model.lines(width - 1, now)
                    top = 0 if args.quiet else 2
                    footer_rows = 0 if args.quiet else 1
                    visible = model.viewport(lines, positions, height - top - footer_rows - 1)
                    for row, line in enumerate(visible, top):
                        attr = curses.A_REVERSE if line.key == model.selected and line.kind in ("worker", "summary") else curses.A_BOLD if line.kind == "target" else 0
                        _put(screen, curses, row, line.text, width, attr)
                    if not args.quiet:
                        _put(screen, curses, height - 1, "↑/↓ select  →/← expand  Enter toggle  a/c all  PgUp/PgDn scroll  q quit", width)
                screen.refresh()
                size, last_draw = (height, width), now
            if args.iterations and group.done:
                return overall_code(group.views)
            key = screen.getch()
            if key == ord("q"):
                return 0
            if key == 3:
                return 130
            action = _key_action(key, curses)
            if action:
                model.action(action, max(1, height - 5))
                last_draw = 0
            if key == curses.KEY_RESIZE:
                size = None
    finally:
        try:
            curses.curs_set(previous_cursor if previous_cursor is not None else 1)
        except curses.error:
            pass


def run_interactive(args, options_factory=None):
    validate_interactive(args)
    try:
        import curses
    except ImportError:
        raise ValueError("--interactive requires Python curses support on this platform")
    args.watch = True
    if args.targets:
        targets, factory = configured_targets(args), target_client
    elif args.input:
        targets = [Target("snapshot", "saved snapshot", "", 0, False, 3, {})]
        factory = lambda target: None
    else:
        options = options_factory()
        host = "[{}]".format(args.host) if ":" in args.host and not args.host.startswith("[") else args.host
        url = "{}://{}:{}".format("https" if args.https else "http", host, args.port)
        targets = [Target(args.host, url, args.host, args.port, args.https, options["timeout"], {})]
        factory = lambda target: ChimeraAdminClient(**options)
    group = PollGroup(targets, args, factory)
    try:
        return curses.wrapper(screen_loop, curses, group, args)
    except KeyboardInterrupt:
        return 130
    except curses.error as exc:
        raise ValueError("cannot initialize interactive terminal: {}".format(exc))
    finally:
        group.close()
