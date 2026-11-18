# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Apache-2.0

"""Read-only connection/worker rendering and sequential snapshot watch."""

import argparse
import copy
import ipaddress
import json
import math
import os
from pathlib import Path
import shutil
import sys
import time
from .presentation import display_width
from .watch_keys import WatchKeys


def _positive(value):
    result = float(value)
    if not math.isfinite(result) or result < 1:
        raise argparse.ArgumentTypeError("must be at least 1 second")
    return result


def _nonnegative(value):
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return number


def _port(value):
    number = int(value)
    if not 0 <= number <= 65535:
        raise argparse.ArgumentTypeError("port must be between 0 and 65535")
    return number


def _ip(value):
    try:
        return str(ipaddress.ip_address(value))
    except ValueError:
        raise argparse.ArgumentTypeError("use a numeric IPv4 or IPv6 address")


def add_inspection_commands(subparsers):
    root = subparsers.add_parser("inspect", help="Inspect connections and workers")
    commands = root.add_subparsers(dest="inspection_view", required=True)
    for view in ("connections", "threads", "dashboard"):
        p = commands.add_parser(view, help="Render the {} view".format(view),
            epilog=("Interactive keys: Up/Down select a worker; Right/Left expand/collapse; "
                    "Enter/Space toggle; a expand all; c collapse all; PageUp/PageDown scroll; "
                    "q or Ctrl-C exit. Requires a TTY; incompatible with --json and --no-clear.")
                    if view == "dashboard" else None)
        source = p.add_mutually_exclusive_group()
        source.add_argument("--input", metavar="FILE", help="Read a saved JSON snapshot instead of the API")
        source.add_argument("--targets", metavar="FILE", help="Monitor named management endpoints from a JSON targets file")
        p.add_argument("--target", action="append", help="Select a named target; repeat to select several")
        if view == "dashboard":
            p.add_argument("--interactive", action="store_true",
                           help="Interactive worker tree (TTY required); implies live polling; arrows expand/select, q exits")
        p.add_argument("--watch", action="store_true", help="Refresh until Ctrl-C or q in a TTY")
        p.add_argument("--interval", type=_positive, default=2.0, help="Refresh interval, minimum 1s (default: 2)")
        p.add_argument("--iterations", type=_nonnegative, default=0, help="Stop after N poll attempts; 0 means unlimited in watch")
        p.add_argument("--json", action="store_true", help="JSON output; newline-delimited JSON in watch")
        p.add_argument("--quiet", action="store_true", help="Hide banners and explanatory notes in human-readable output")
        p.add_argument("--no-clear", action="store_true", help="Append watch frames even on a terminal")
        p.add_argument("--max-rows", type=_nonnegative, default=20, help="Rows per text table; 0 shows all (interactive mode scrolls all returned rows)")
        p.add_argument("--wide", action="store_true", help="Include worker context IDs, full instance UUIDs, human-formatted ages and raw transport identifiers")
        p.add_argument("--worker", type=_nonnegative, dest="worker_id")
        choices = ["worker", "connections", "rpc-rate", "vfs-rate"] if view == "threads" else ["worker", "client"]
        if view == "dashboard":
            choices += ["connections", "rpc-rate", "vfs-rate"]
        p.add_argument("--sort", choices=choices, default="worker")
        if view != "threads":
            p.add_argument("--service", choices=["nfs", "smb", "s3", "mount", "nlm", "nsm", "rpcbind"])
            p.add_argument("--transport", choices=["tcp", "rdma", "inproc", "unix"])
            p.add_argument("--remote-ip", type=_ip)
            p.add_argument("--remote-port", type=_port)
            p.add_argument("--local-ip", type=_ip)
            p.add_argument("--local-port", type=_port)

    nfs = commands.add_parser("nfs", help="Inspect NFSv4.1 and later sessions")
    nfs_views = nfs.add_subparsers(dest="nfs_inspection_view", required=True)
    sessions = nfs_views.add_parser("sessions", help="Inspect NFSv4.1 and later sessions")
    sessions.set_defaults(inspection_view="nfs_sessions", target=None, targets=None, worker_id=None,
                         service=None, transport=None, remote_ip=None, remote_port=None,
                         local_ip=None, local_port=None)
    sessions_source = sessions.add_mutually_exclusive_group()
    sessions_source.add_argument("--input", metavar="FILE", help="Read a saved JSON snapshot instead of the API")
    sessions_source.add_argument("--targets", metavar="FILE", help="Monitor named management endpoints from a JSON targets file")
    sessions.add_argument("--target", action="append", help="Select a named target; repeat to select several")
    sessions.add_argument("--client", dest="client_id", type=_nonnegative, help="Filter by NFS client ID")
    sessions.add_argument("--slots", metavar="SESSION_ID", help="Show the slot table for one session")
    sessions.add_argument("--watch", action="store_true", help="Refresh until Ctrl-C")
    sessions.add_argument("--interval", type=_positive, default=2.0, help="Refresh interval, minimum 1s (default: 2)")
    sessions.add_argument("--iterations", type=_nonnegative, default=0, help="Stop after N poll attempts; 0 means unlimited in watch")
    sessions.add_argument("--json", action="store_true", help="JSON output; newline-delimited JSON in watch")
    sessions.add_argument("--quiet", action="store_true", help="Hide banners and explanatory notes")
    sessions.add_argument("--no-clear", action="store_true", help="Append watch frames on a terminal")
    sessions.add_argument("--max-rows", type=_nonnegative, default=20, help="Rows per text table; 0 shows all returned rows")
    sessions.add_argument("--wide", action="store_true", help="Include full identifiers and timestamps")
    sessions.add_argument("--sort", choices=["client", "inflight", "peak"], default="client")

    clients = nfs_views.add_parser("clients", help="Inspect NFSv4.1 and later clients")
    clients.set_defaults(inspection_view="nfs_clients", target=None, targets=None, worker_id=None,
                        service=None, transport=None, remote_ip=None, remote_port=None,
                        local_ip=None, local_port=None)
    clients_source = clients.add_mutually_exclusive_group()
    clients_source.add_argument("--input", metavar="FILE", help="Read a saved JSON snapshot instead of the API")
    clients_source.add_argument("--targets", metavar="FILE", help="Monitor named management endpoints from a JSON targets file")
    clients.add_argument("--target", action="append", help="Select a named target; repeat to select several")
    clients.add_argument("--watch", action="store_true", help="Refresh until Ctrl-C")
    clients.add_argument("--interval", type=_positive, default=2.0, help="Refresh interval, minimum 1s (default: 2)")
    clients.add_argument("--iterations", type=_nonnegative, default=0, help="Stop after N poll attempts; 0 means unlimited in watch")
    clients.add_argument("--json", action="store_true", help="JSON output; newline-delimited JSON in watch")
    clients.add_argument("--quiet", action="store_true", help="Hide banners and explanatory notes")
    clients.add_argument("--no-clear", action="store_true", help="Append watch frames on a terminal")
    clients.add_argument("--max-rows", type=_nonnegative, default=20, help="Rows per text table; 0 shows all returned rows")
    clients.add_argument("--wide", action="store_true", help="Include full identifiers and owner strings")
    clients.add_argument("--sort", choices=["client", "state", "lease", "sessions", "opens"], default="client")


def safe(value):
    """Never interpret server-supplied control sequences in the terminal."""
    s = str(value) if value is not None else "--"
    return "".join(c if c.isprintable() else "\\x{:02x}".format(ord(c)) for c in s)


def endpoint(value):
    if not isinstance(value, dict):
        return "--"
    family = value.get("family")
    address = value.get("address", value.get("name"))
    port = value.get("port")
    if address is None:
        return "--"
    if family in ("inproc", "unix"):
        return "{}:{}".format(family, safe(address))
    if family == "ipv6" and value.get("scope_id"):
        address = "{}%{}".format(address, value["scope_id"])
    if family == "ipv6" or ":" in str(address):
        address = "[{}]".format(safe(address))
    return "{}:{}".format(safe(address), safe(port))


def protocol(row):
    service = str(row.get("service", "unknown"))
    if service == "nfs":
        names = []
        for item in row.get("observed_rpc_programs", []):
            version = item.get("version")
            if item.get("program") == 100003:
                if version == 4:
                    minors = row.get("nfs_minor_versions_seen", [])
                    names += ["NFSv4.{}".format(v) for v in minors] if minors else ["NFSv4.?"]
                else:
                    names.append("NFSv{}".format(version))
            else:
                names.append("RPC:{}/v{}".format(item.get("program"), version))
        return ",".join(sorted(set(names))) if names else "NFS (unobserved)"
    if service == "smb":
        dialect = row.get("negotiated_dialect")
        if not dialect:
            return "SMB (unnegotiated)"
        return "SMB/{}".format(dialect if isinstance(dialect, str) else "0x{:04x}".format(dialect))
    if service == "s3":
        return "S3/{}".format(row.get("http_framing", "unknown"))
    return service.upper()


def _integer(value):
    if isinstance(value, bool):
        return None
    try:
        result = int(value)
        return result if 0 <= result <= (1 << 64) - 1 and str(result) == str(value) else None
    except (ValueError, TypeError, OverflowError):
        return None


def _metric_map(worker, kind):
    section = worker.get(kind)
    if not isinstance(section, dict):
        return None
    entries = section.get("procedures" if kind == "rpc" else "operations")
    if not isinstance(entries, list):
        return None
    result = {}
    for item in entries:
        if not isinstance(item, dict):
            return None
        key = tuple(item.get(k) for k in ("program", "version", "procedure", "name")) if kind == "rpc" else item.get("name")
        count = _integer(item.get("latency_samples_total"))
        if count is None or key in result:
            return None
        result[key] = count
    return result


def _rate(previous, current, kind):
    old_time = _integer(previous.get("sampled_monotonic_ns"))
    new_time = _integer(current.get("sampled_monotonic_ns"))
    before, after = _metric_map(previous, kind), _metric_map(current, kind)
    if old_time is None or new_time is None or new_time <= old_time or before is None or after is None:
        return None
    if before.keys() != after.keys() or any(after[k] < before[k] for k in before):
        return None
    return sum(after[k] - before[k] for k in before) * 1_000_000_000 / (new_time - old_time)


def valid(snapshot, key):
    if not isinstance(snapshot, dict) or not isinstance(snapshot.get("server_instance_id"), str):
        raise ValueError("snapshot must contain server_instance_id")
    if not isinstance(snapshot.get("complete"), bool) or not isinstance(snapshot.get(key), list):
        raise ValueError("snapshot must contain a complete flag and a {} array".format(key))
    seen = set()
    for row in snapshot[key]:
        if not isinstance(row, dict) or not isinstance(row.get("worker_id"), int) or isinstance(row.get("worker_id"), bool):
            raise ValueError("each {} row needs an integer worker_id".format(key))
        ident = row.get("connection_id") if key == "connections" else row["worker_id"]
        if key == "connections" and (not isinstance(ident, str) or not ident):
            raise ValueError("each connection needs a nonempty string connection_id")
        if ident in seen:
            raise ValueError("duplicate identity in {} snapshot".format(key))
        seen.add(ident)
        if key == "connections":
            if not isinstance(row.get("observed_rpc_programs", []), list) or any(not isinstance(p, dict) for p in row.get("observed_rpc_programs", [])):
                raise ValueError("observed_rpc_programs must be an array of objects")
            if not isinstance(row.get("nfs_minor_versions_seen", []), list):
                raise ValueError("nfs_minor_versions_seen must be an array")
            if any(row.get(side) is not None and not isinstance(row.get(side), dict) for side in ("local", "remote")):
                raise ValueError("connection endpoints must be objects or null")
    return snapshot


def complete(snapshot):
    return bool(snapshot and snapshot.get("complete") is True and not snapshot.get("truncated") and not snapshot.get("missing_workers"))


def _matches(row, args):
    for field in ("worker_id", "service", "transport"):
        wanted = getattr(args, field, None)
        if wanted is not None and row.get(field) != wanted:
            return False
    for side in ("remote", "local"):
        actual = row.get(side) or {}
        address, port = getattr(args, side + "_ip", None), getattr(args, side + "_port", None)
        if port is not None and actual.get("port") != port:
            return False
        if address is not None:
            try:
                if ipaddress.ip_address(str(actual.get("address"))) != ipaddress.ip_address(address):
                    return False
            except ValueError:
                return False
    return True


def fetch(client, args):
    view = args.inspection_view
    result = {}
    if view in ("nfs_sessions", "nfs_clients"):
        if args.input:
            path = Path(args.input)
            if path.stat().st_size > 8 * 1024 * 1024:
                raise ValueError("snapshot exceeds the 8 MiB input limit")
            raw = json.loads(path.read_text(encoding="utf-8"))
            key = "sessions_snapshot" if view == "nfs_sessions" else "clients_snapshot"
            result[view] = raw.get(key, raw)
            if view == "nfs_sessions":
                live = raw.get("live_threads_snapshot", raw.get("live"))
                if isinstance(live, dict):
                    result["live"] = valid(live, "threads")
        elif view == "nfs_sessions" and args.slots:
            result[view] = client.get_nfs_session_slots(args.slots)
        elif view == "nfs_sessions":
            result[view] = client.get_nfs_sessions(limit=1024, client_id=args.client_id)
            result["live"] = valid(client.get_live_threads(), "threads")
        else:
            result[view] = client.get_nfs_clients(limit=1024)
        snapshot = result[view]
        collection = "sessions" if view == "nfs_sessions" else "clients"
        if not isinstance(snapshot, dict) or not isinstance(snapshot.get(collection), list):
            raise ValueError("NFS {} snapshot must contain a {} array".format(collection, collection))
        return copy.deepcopy(result)
    if args.input:
        path = Path(args.input)
        if path.stat().st_size > 8 * 1024 * 1024:
            raise ValueError("snapshot exceeds the 8 MiB input limit")
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError) as exc:
            raise ValueError("cannot read snapshot: {}".format(exc))
        if not isinstance(raw, dict):
            raise ValueError("snapshot must be a JSON object")
        for key in ("threads", "connections"):
            if view in (key, "dashboard"):
                result[key] = valid(raw.get(key + "_snapshot", raw), key)
        if view == "dashboard" and isinstance(raw.get("nfs_sessions_snapshot"), dict):
            result["nfs_sessions"] = raw["nfs_sessions_snapshot"]
    else:
        if view in ("threads", "dashboard"):
            result["threads"] = valid(client.get_threads(worker_id=args.worker_id), "threads")
            result["live"] = valid(client.get_live_threads(), "threads")
        if view in ("connections", "dashboard"):
            params = {key: getattr(args, key, None) for key in ("worker_id", "service", "transport", "remote_ip", "remote_port", "local_ip", "local_port")}
            result["connections"] = valid(client.get_connections(**params), "connections")
        if view == "dashboard":
            result["nfs_sessions"] = client.get_nfs_sessions(limit=1024)
    result = copy.deepcopy(result)
    if "threads" in result and args.worker_id is not None:
        result["threads"]["threads"] = [w for w in result["threads"]["threads"] if w["worker_id"] == args.worker_id]
    if "connections" in result:
        result["connections"]["connections"] = [r for r in result["connections"]["connections"] if _matches(r, args)]
    return result


class WatchState:
    def __init__(self):
        self.previous = None
        self.nfs_samples = {}
        self.nfs_saturated = {}

    def reset(self):
        self.previous = None
        self.nfs_samples = {}
        self.nfs_saturated = {}

    def update(self, snapshots):
        meta = {"rates": {}, "live_rates": {}, "appeared": [], "absent": [], "changes_comparable": False, "restarted": False, "nfs_peaks": {}}
        if len({s["server_instance_id"] for s in snapshots.values()}) > 1:
            self.reset()
            meta["mixed_instances"] = True
            return meta
        previous = self.previous or {}
        old_live, current_live = previous.get("live"), snapshots.get("live")
        if current_live:
            for worker in current_live.get("threads", []):
                live = meta["live_rates"].setdefault(str(worker.get("worker_id")), {})
                for item in worker.get("vfs", {}).get("operations", []):
                    label = {"write": "wr", "read": "rd"}.get(str(item.get("name", "")).lower())
                    if label:
                        live[label + "_avg_ns"] = _integer(item.get("latency_avg_ns"))
                        live[label + "_p99_ns"] = _integer(item.get("latency_p99_ns"))
        if old_live and current_live:
            same = old_live["server_instance_id"] == current_live["server_instance_id"]
            meta["restarted"] |= not same
            old_time = _integer(old_live.get("sampled_monotonic_ns"))
            new_time = _integer(current_live.get("sampled_monotonic_ns"))
            if same and complete(old_live) and complete(current_live) and old_time is not None and new_time > old_time:
                before = {w["worker_id"]: w for w in old_live["threads"]}
                elapsed = new_time - old_time
                for worker in current_live["threads"]:
                    prior = before.get(worker["worker_id"])
                    if not prior:
                        continue
                    old_cpu, new_cpu = _integer(prior.get("cpu_ns")), _integer(worker.get("cpu_ns"))
                    old_iters, new_iters = _integer(prior.get("loop_iters")), _integer(worker.get("loop_iters"))
                    old_work = _integer(prior.get("loop_iters_with_work"))
                    new_work = _integer(worker.get("loop_iters_with_work"))
                    live = meta["live_rates"].setdefault(str(worker["worker_id"]), {})
                    if None not in (old_cpu, new_cpu) and new_cpu >= old_cpu:
                        live["cpu"] = 100.0 * (new_cpu - old_cpu) / elapsed
                    old_runq, new_runq = _integer(prior.get("runq_wait_ns")), _integer(worker.get("runq_wait_ns"))
                    if old_runq is not None and new_runq is not None and new_runq >= old_runq:
                        live["runq"] = 100.0 * (new_runq - old_runq) / elapsed
                    if None not in (old_iters, new_iters, old_work, new_work) and new_iters > old_iters and new_work >= old_work:
                        live["busy"] = 100.0 * (new_work - old_work) / (new_iters - old_iters)
                    old_rpc, new_rpc = prior.get("rpc", {}), worker.get("rpc", {})
                    if isinstance(old_rpc, dict) and isinstance(new_rpc, dict):
                        old_msgs = _integer(old_rpc.get("rx_msgs"))
                        new_msgs = _integer(new_rpc.get("rx_msgs"))
                        if old_msgs is not None and new_msgs is not None and new_msgs >= old_msgs:
                            live["rpc_s"] = (new_msgs - old_msgs) * 1_000_000_000 / elapsed
                        for direction in ("rx", "tx"):
                            old_bytes = _integer(old_rpc.get(direction + "_bytes"))
                            new_bytes = _integer(new_rpc.get(direction + "_bytes"))
                            if old_bytes is not None and new_bytes is not None and new_bytes >= old_bytes:
                                live[direction + "_MB_s"] = (
                                    (new_bytes - old_bytes) / elapsed * 1000
                                )
        old_sessions, current_sessions = previous.get("nfs_sessions"), snapshots.get("nfs_sessions")
        if current_sessions:
            same_instance = old_sessions and old_sessions.get("server_instance_id") == current_sessions.get("server_instance_id")
            if old_sessions and not same_instance:
                meta["restarted"] = True
                self.nfs_samples = {}
                self.nfs_saturated = {}
            before_sessions = {row.get("session_id"): row for row in old_sessions.get("sessions", [])} if same_instance else {}
            for row in current_sessions.get("sessions", []):
                prior = before_sessions.get(row.get("session_id"), {})
                current_inflight = _integer(row.get("slots_in_progress")) or 0
                previous_peak = _integer(prior.get("slots_peak")) or _integer(prior.get("slots_in_progress")) or 0
                row_peak = max(previous_peak, current_inflight)
                meta["nfs_peaks"][row.get("session_id")] = row_peak
                row["slots_peak"] = row_peak
                session_id = row.get("session_id")
                if complete(current_sessions):
                    self.nfs_samples[session_id] = self.nfs_samples.get(session_id, 0) + 1
                    if _integer(row.get("negotiated_slots")) and current_inflight >= _integer(row.get("negotiated_slots")):
                        self.nfs_saturated[session_id] = self.nfs_saturated.get(session_id, 0) + 1
                    meta.setdefault("nfs_saturation", {})[session_id] = {
                        "samples": self.nfs_samples[session_id],
                        "saturated": self.nfs_saturated.get(session_id, 0),
                    }
        old = previous.get("threads")
        current = snapshots.get("threads")
        if old and current:
            same = old["server_instance_id"] == current["server_instance_id"]
            meta["restarted"] |= not same
            if same and complete(old) and complete(current):
                before = {w["worker_id"]: w for w in old["threads"]}
                for worker in current["threads"]:
                    prior = before.get(worker["worker_id"])
                    if prior and prior.get("rpc_thread_id") == worker.get("rpc_thread_id"):
                        meta["rates"][str(worker["worker_id"])] = {kind: _rate(prior, worker, kind) for kind in ("rpc", "vfs")}
        old, current = previous.get("connections"), snapshots.get("connections")
        if old and current:
            same = old["server_instance_id"] == current["server_instance_id"]
            meta["restarted"] |= not same
            if same and complete(old) and complete(current):
                before = {c["connection_id"]: c for c in old["connections"]}
                after = {c["connection_id"]: c for c in current["connections"]}
                meta["appeared"] = sorted(after.keys() - before.keys())
                meta["absent"] = [before[k] for k in sorted(before.keys() - after.keys())]
                meta["changes_comparable"] = True
        self.previous = copy.deepcopy(snapshots)
        return meta


def _table(headers, rows, max_rows, numeric=()):
    shown = rows if max_rows == 0 else rows[:max_rows]
    cells = [[safe(c) for c in row] for row in shown]
    widths = [max([display_width(h)] + [display_width(row[i]) for row in cells]) for i, h in enumerate(headers)]
    def aligned(value, i, right=False):
        padding = " " * max(0, widths[i] - display_width(value))
        return padding + value if right else value + padding
    lines = ["  ".join(aligned(h, i) for i, h in enumerate(headers)).rstrip()]
    lines += ["  ".join(aligned(c, i, i in numeric) for i, c in enumerate(row)).rstrip() for row in cells]
    if not rows:
        lines.append("(no rows returned)")
    if len(shown) < len(rows):
        lines.append("Showing {} of {} rows; use --max-rows 0 for all returned rows.".format(len(shown), len(rows)))
    return "\n".join(lines)


def _rate_text(value):
    return "--" if value is None else "{:.1f}".format(value)


def _latency_text(value):
    number = _integer(value)
    if number is None:
        return "--"
    return "{:.3f} ms".format(number / 1_000_000)


def _age_text(value):
    milliseconds = _integer(value)
    if milliseconds is None:
        return "--"
    seconds = milliseconds // 1000
    if seconds < 60:
        return "{}s".format(seconds)
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return "{}m {:02d}s".format(minutes, seconds)
    hours, minutes = divmod(minutes, 60)
    return "{}h {:02d}m {:02d}s".format(hours, minutes, seconds)


def render(snapshots, meta, args, error=None, stale_age=0, width=None):
    if args.inspection_view == "nfs_sessions":
        session_snapshot = copy.copy(snapshots.get("nfs_sessions", {}))
        session_snapshot["live"] = snapshots.get("live", {})
        return _render_nfs_sessions(session_snapshot, meta, args, error)
    if args.inspection_view == "nfs_clients":
        return _render_nfs_clients(snapshots.get("nfs_clients", {}), args, error)
    states = list(snapshots.values())
    instances = {s["server_instance_id"] for s in states}
    mixed = len(instances) > 1
    partial = any(not complete(s) for s in states) or mixed
    status = "STALE ({:.1f}s since last success)".format(stale_age) if error else ("PARTIAL" if partial else "LIVE" if args.watch and not args.input else "SNAPSHOT")
    width = width or shutil.get_terminal_size((120, 24)).columns
    identity = "{}:{}".format(safe(getattr(args, "host", "server")), safe(getattr(args, "port", ""))).rstrip(":")
    sample_age = "{:.1f}s old".format(stale_age) if error else ("0.0s" if args.watch and not args.input else "--")
    lines = [] if getattr(args, "quiet", False) else ["INSPECTION {}".format(args.inspection_view.upper())]
    lines.append("{}  {}  sample age {}".format(identity, status, sample_age))
    if getattr(args, "wide", False):
        lines.append("Instance ID: {}".format(safe(", ".join(sorted(instances))) if instances else "unavailable"))
    if any(s.get("synthetic") for s in states) and not getattr(args, "quiet", False):
        lines.append("Synthetic example data")
    if error:
        lines.append("Refresh failed: {}".format(safe(error)))
    if mixed:
        lines.append("Warning: server restarted between endpoint queries; worker and connection samples are not joined.")
    for name, snapshot in snapshots.items():
        if not complete(snapshot):
            lines.append("Warning: {} PARTIAL; truncated={}, missing workers={}".format(name, snapshot.get("truncated", False), safe(snapshot.get("missing_workers", []))))
    if meta.get("restarted"):
        lines.append("Server restart observed; rate and change baselines reset.")
    if "threads" in snapshots:
        filters_applied = any(getattr(args, field, None) is not None for field in
                              ("worker_id", "service", "transport", "remote_ip", "remote_port", "local_ip", "local_port"))
        live_by_id = {w["worker_id"]: w for w in snapshots.get("live", {}).get("threads", [])}
        workers = [{**w, **live_by_id.get(w["worker_id"], {})} for w in snapshots["threads"]["threads"]]
        def worker_key(w):
            rates = meta["rates"].get(str(w["worker_id"]), {})
            if args.sort in ("rpc-rate", "vfs-rate"):
                value = rates.get("rpc" if args.sort == "rpc-rate" else "vfs")
                return (-(value if value is not None else -1), w["worker_id"])
            if args.sort == "connections":
                return (-sum(_integer(n) or 0 for n in (w.get("connection_counts") or {}).values()), w["worker_id"])
            return (w["worker_id"],)
        rows = []
        connection_counts = {}
        if "connections" in snapshots:
            for connection in snapshots["connections"]["connections"]:
                connection_counts[connection["worker_id"]] = connection_counts.get(connection["worker_id"], 0) + 1
        for w in sorted(workers, key=worker_key):
            counts, rpc = w.get("connection_counts") or {}, w.get("rpc") or {}
            rates = {} if error or mixed else meta["rates"].get(str(w["worker_id"]), {})
            live_rates = {} if error or mixed else meta["live_rates"].get(str(w["worker_id"]), {})
            samples = _metric_map(w, "rpc")
            row = [w.get("worker_name", w["worker_id"]), w.get("os_tid"), w.get("rpc_thread_id"), counts.get("nfs", "--"), counts.get("smb", "--"), counts.get("s3", "--"), rpc.get("server_inflight"), sum(samples.values()) if samples is not None else None, _rate_text(rates.get("rpc")), _rate_text(rates.get("vfs")), _rate_text(live_rates.get("rpc_s")), _rate_text(live_rates.get("rx_MB_s")), _rate_text(live_rates.get("tx_MB_s")), _rate_text(live_rates.get("cpu")), _rate_text(live_rates.get("runq")), _rate_text(live_rates.get("busy")), _latency_text(live_rates.get("wr_avg_ns")), _latency_text(live_rates.get("wr_p99_ns")), _latency_text(live_rates.get("rd_avg_ns")), _latency_text(live_rates.get("rd_p99_ns"))]
            if args.inspection_view == "dashboard":
                listed = connection_counts.get(w["worker_id"], 0)
                listed = (">=" if not complete(snapshots.get("connections", {})) else "") + str(listed)
                total_values = [_integer(value) for value in counts.values()]
                total = sum(total_values) if total_values and all(value is not None for value in total_values) else "--"
                row += [listed, total]
            rows.append(row)
        headers = ["WORKER", "NFS", "SMB", "S3", "INFL", "RPC SAMPLES", "RPC samp/s", "VFS samp/s", "RPC/s", "RX MB/s", "TX MB/s", "CPU%", "RUNQ%", "BUSY%", "Wr avg", "Wr p99", "Rd avg", "Rd p99"]
        if args.inspection_view == "dashboard":
            headers += ["MATCH" if filters_applied else "LISTED", "TOTAL(all)"]
        display_rows = [[row[0], *row[3:]] for row in rows]
        if args.wide:
            headers[1:1] = ["TID", "RPC ID"]
            display_rows = [[row[0], row[1], row[2], *row[3:]] for row in rows]
        if width < 140:
            compact_headers = ["WORKER", "NFS", "SMB", "S3"]
            compact_rows = [[row[0], row[3], row[4], row[5]] for row in rows]
            if args.inspection_view == "dashboard":
                compact_headers += ["MATCH" if filters_applied else "LISTED", "TOTAL(all)"]
                compact_rows = [[*compact_rows[i], display_rows[i][-2], display_rows[i][-1]] for i in range(len(rows))]
            if width < 90:
                metric_headers = ["WORKER", "RPC/s", "RX MB/s", "TX MB/s", "CPU%", "RUNQ%", "BUSY%"]
                metric_rows = [[row[0], row[10], row[11], row[12], row[13], row[14], row[15]] for row in rows]
            else:
                metric_headers = ["WORKER", "INFL", "RPC SAMPLES", "RPC samp/s", "VFS samp/s", "RPC/s", "RX MB/s", "TX MB/s", "CPU%", "RUNQ%", "BUSY%"]
                metric_rows = [[row[0], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15]] for row in rows]
            lines += ["", "WORKERS (all traffic)", _table(compact_headers, compact_rows, args.max_rows, numeric=range(1, len(compact_headers))),
                      "\nWORKER METRICS", _table(metric_headers, metric_rows, args.max_rows, numeric=range(1, len(metric_headers)))]
            if args.wide:
                lines.append("\nWORKER CONTEXTS")
                lines.append(_table(["WORKER", "TID", "RPC ID"], [[row[0], row[1], row[2]] for row in rows], args.max_rows, numeric={1, 2}))
        else:
            lines += ["", "WORKERS (all traffic)", _table(headers, display_rows, args.max_rows, numeric=range(1, len(headers)))]
    if "connections" in snapshots:
        rows = []
        data = snapshots["connections"]["connections"]
        key = (lambda c: (endpoint(c.get("remote")), c["connection_id"])) if args.sort == "client" else (lambda c: (c["worker_id"], c["connection_id"]))
        ordered = sorted(data, key=key)
        narrow = width < 108
        if args.inspection_view == "dashboard":
            grouped = {}
            for conn in ordered:
                grouped.setdefault(conn["worker_id"], []).append(conn)
            lines += ["", "CONNECTIONS BY WORKER (listed rows; worker counts cover all traffic)"]
            shown_workers = [w for w in snapshots.get("threads", {}).get("threads", [])]
            for worker in shown_workers:
                worker_id = worker["worker_id"]
                connections = grouped.get(worker_id, [])
                lines.append("  {}".format(safe(worker.get("worker_name", worker_id))))
                if not connections:
                    lines.append("    (no matching connections)" if complete(snapshots["connections"]) else "    Connections unavailable or incomplete")
                    continue
                if narrow:
                    for conn in connections[:args.max_rows or None]:
                        rows = [["CLIENT", endpoint(conn.get("remote"))], ["SERVER", endpoint(conn.get("local"))],
                                ["PROTOCOL", protocol(conn)], ["TRANSPORT", conn.get("transport")],
                                ["BACKEND", conn.get("backend")], ["STATE", conn.get("state")]]
                        lines.append("    CONNECTION {}".format(safe(conn["connection_id"])))
                        lines.append(_table(["FIELD", "VALUE"], rows, 0))
                        if args.wide:
                            lines.append(_table(["AGE", "RX MSGS", "RAW TRANSPORT"], [[_age_text(conn.get("age_ms")), (conn.get("per_connection_stats") or {}).get("messages_received"), conn.get("raw_transport")]], 0, numeric={1}))
                else:
                    headers = ["ID", "CLIENT", "SERVER", "PROTOCOL", "TRANSPORT", "BACKEND", "STATE"]
                    rows = [[c["connection_id"], endpoint(c.get("remote")), endpoint(c.get("local")), protocol(c), c.get("transport"), c.get("backend"), c.get("state")] for c in connections]
                    lines.append(_table(headers, rows, args.max_rows))
            if args.wide and not narrow:
                lines.append("\nEXTENDED CONNECTION DETAILS")
                lines.append(_table(["ID", "AGE", "TID", "RX MSGS", "RAW TRANSPORT"], [[c["connection_id"], _age_text(c.get("age_ms")), c.get("os_tid"), (c.get("per_connection_stats") or {}).get("messages_received"), c.get("raw_transport")] for c in ordered], args.max_rows))
        else:
            rows = []
            for c in ordered:
                row = ["+" if c["connection_id"] in meta["appeared"] and not error else " ", c["connection_id"], endpoint(c.get("remote")), endpoint(c.get("local")), protocol(c), c.get("transport"), c.get("backend"), c.get("worker_name", c["worker_id"]), c.get("state")]
                rows.append(row)
            headers = ["", "CONNECTION", "CLIENT", "SERVER", "PROTOCOL", "TRANSPORT", "BACKEND", "WORKER", "STATE"]
            if narrow:
                matching = any(getattr(args, field, None) is not None for field in
                               ("worker_id", "service", "transport", "remote_ip", "remote_port", "local_ip", "local_port"))
                lines += ["", "CONNECTIONS ({} rows)".format("matching" if matching else "listed")]
                for c in ordered[:args.max_rows or None]:
                    rows = [["CLIENT", endpoint(c.get("remote"))], ["SERVER", endpoint(c.get("local"))],
                            ["PROTOCOL", protocol(c)], ["TRANSPORT", c.get("transport")],
                            ["BACKEND", c.get("backend")], ["WORKER", c.get("worker_name", c["worker_id"])],
                            ["STATE", c.get("state")]]
                    lines.append(_table(["FIELD", "VALUE"], [["CONNECTION", c["connection_id"]], *rows], 0))
                    if args.wide:
                        lines.append(_table(["AGE", "RX MSGS", "RAW TRANSPORT", "TID"], [[_age_text(c.get("age_ms")), (c.get("per_connection_stats") or {}).get("messages_received"), c.get("raw_transport"), c.get("os_tid")]], 0, numeric={1, 3}))
            else:
                lines += ["", "CONNECTIONS (listed rows)", _table(headers, rows, args.max_rows)]
                if args.wide:
                    lines += ["", "EXTENDED CONNECTION DETAILS", _table(
                        ["CONNECTION", "AGE", "TID", "RX MSGS", "RAW TRANSPORT"],
                        [[c["connection_id"], _age_text(c.get("age_ms")), c.get("os_tid"),
                          (c.get("per_connection_stats") or {}).get("messages_received"), c.get("raw_transport")] for c in ordered],
                        args.max_rows, numeric={2, 3})]
        if meta["changes_comparable"] and not error:
            lines.append("Since previous complete sample: {} appeared, {} absent.".format(len(meta["appeared"]), len(meta["absent"])))
            for c in meta["absent"][:10]:
                lines.append("  ABSENT {} {} on {}".format(safe(c["connection_id"]), endpoint(c.get("remote")), safe(c.get("worker_name", c["worker_id"]))))
        elif args.watch and not getattr(args, "quiet", False):
            lines.append("Connection changes require consecutive complete samples from the same server instance.")
    if args.inspection_view == "dashboard" and "nfs_sessions" in snapshots:
        sessions = snapshots["nfs_sessions"].get("sessions", [])
        nfs_enabled = bool(sessions) or any(
            _integer((worker.get("connection_counts") or {}).get("nfs"))
            for worker in snapshots.get("threads", {}).get("threads", []))
        if nfs_enabled:
            rows = [[str(session.get("session_id", ""))[:8], session.get("client_id", "--"),
                     session.get("negotiated_slots", "--"), session.get("slots_in_progress", "--"),
                     meta.get("nfs_peaks", {}).get(session.get("session_id"), "--"),
                     len(session.get("connections", []))]
                    for session in sessions]
            lines += ["", "NFSv4.1+ SESSIONS", _table(
                ["SESSION", "CLIENT", "GRANT", "INFL", "PEAK", "CONNS"], rows, args.max_rows)]
    if not getattr(args, "quiet", False):
        if args.inspection_view == "dashboard":
            lines += ["", "Connection filters affect listed connections; worker counters cover all traffic."]
        if "threads" in snapshots:
            lines.append("Rates use consecutive live samples; -- means the baseline is unavailable or warming up.")
            lines.append("BUSY% is the loop-iteration ratio, not CPU usage; poll-mode workers can show high CPU% with low BUSY%.")
        if meta.get("changes_comparable"):
            lines.append("ABSENT means no longer observed as a connection; it does not imply NFS lease expiry.")
    return "\n".join(lines)


def _render_nfs_sessions(snapshot, meta, args, error=None):
    lines = [] if args.quiet else ["NFSv4.1+ SESSIONS"]
    lines.append("{}  {}".format(
        "{}:{}".format(safe(getattr(args, "host", "server")), safe(getattr(args, "port", ""))).rstrip(":"),
        "STALE" if error else "LIVE" if args.watch and not args.input else "SNAPSHOT"))
    if error:
        lines.append("Refresh failed: {}".format(safe(error)))
    if args.slots:
        rows = [[slot.get("slot", "--"), slot.get("state", "--"), slot.get("seqid", "--"),
                 _latency_text(slot.get("age_ns")), slot.get("operation", "--")]
                for slot in snapshot.get("slots", [])]
        lines += ["", _table(["SLOT", "STATE", "SEQID", "AGE", "OPERATION"], rows, args.max_rows)]
        return "\n".join(lines)
    rows = []
    for session in snapshot.get("sessions", []):
        session_id = str(session.get("session_id", ""))
        sid = session_id if args.wide else session_id[:8]
        replay = _integer(session.get("replay_hits"))
        errors = sum(_integer(session.get(name)) or 0 for name in
                     ("seq_misordered", "bad_slot", "delay_errors"))
        rows.append([sid, session.get("client_id", "--"), session.get("requested_slots", "--"),
                     session.get("negotiated_slots", "--"), session.get("slots_in_progress", "--"),
                     session.get("slots_peak", meta.get("nfs_peaks", {}).get(session_id, "--")),
                     len(session.get("connections", [])),
                     len(set(session.get("workers", []))), replay if replay is not None else "--",
                     errors if any(session.get(name) is not None for name in
                                   ("seq_misordered", "bad_slot", "delay_errors")) else "--"])
    if args.sort == "inflight":
        rows.sort(key=lambda row: -(row[4] if isinstance(row[4], int) else -1))
    elif args.sort == "peak":
        rows.sort(key=lambda row: -(row[5] if isinstance(row[5], int) else -1))
    lines += ["", _table(["SESSION", "CLIENT", "REQ", "GRANT", "INFL", "PEAK", "CONNS", "WORKERS", "REPLAY", "ERR"], rows, args.max_rows)]
    counter_rows = [[str(session.get("session_id", ""))[:8],
                     session.get("replay_hits", "--"), session.get("seq_misordered", "--"),
                     session.get("bad_slot", "--"), session.get("delay_errors", "--"),
                     session.get("slot_in_progress_retries", "--")]
                    for session in snapshot.get("sessions", [])]
    lines += ["", "SESSION EVENTS", _table(
        ["SESSION", "REPLAY", "MISORDERED", "BAD SLOT", "DELAY", "IN-PROGRESS RETRY"],
        counter_rows, args.max_rows)]
    if not args.quiet:
        saturation = meta.get("nfs_saturation", {})
        likely = next((session for session in snapshot.get("sessions", [])
                      if saturation.get(session.get("session_id"), {}).get("samples", 0) > 0
                      and saturation[session.get("session_id")]["saturated"] * 2 >
                      saturation[session.get("session_id")]["samples"]), None)
        if likely:
            sid = likely.get("session_id")
            counts = saturation[sid]
            write_samples = []
            for worker in snapshot.get("live", {}).get("threads", []):
                for operation in worker.get("vfs", {}).get("operations", []):
                    if str(operation.get("name", "")).lower() == "write":
                        avg = _integer(operation.get("latency_avg_ns"))
                        count = _integer(operation.get("latency_samples_total")) or 0
                        if avg and count:
                            write_samples.append((avg, count))
            if write_samples and likely.get("negotiated_slots", 0):
                avg_ns = sum(avg * count for avg, count in write_samples) // sum(count for _, count in write_samples)
                bound = likely["negotiated_slots"] * 1_000_000_000 / avg_ns
                lines.append("Slot-saturated: INFL == GRANT in {} of {} samples. Bound at GRANT / avg VFS Write latency = {} / {:.3f} s = {:.0f} req/s.".format(
                    counts["saturated"], counts["samples"], likely["negotiated_slots"], avg_ns / 1_000_000_000, bound))
            else:
                lines.append("Slot-saturated: INFL == GRANT in {} of {} samples; Write latency is unavailable for a throughput bound.".format(
                    counts["saturated"], counts["samples"]))
        lines.append("Peak is the maximum INFL observed across watch samples. A full slot table can also indicate a slow backend; compare with a run that negotiates more slots.")
    return "\n".join(lines)


def _render_nfs_clients(snapshot, args, error=None):
    lines = [] if args.quiet else ["NFSv4.1+ CLIENTS"]
    lines.append("{}  {}".format(
        "{}:{}".format(safe(getattr(args, "host", "server")), safe(getattr(args, "port", ""))).rstrip(":"),
        "STALE" if error else "LIVE" if args.watch and not args.input else "SNAPSHOT"))
    if error:
        lines.append("Refresh failed: {}".format(safe(error)))
    clients = list(snapshot.get("clients", []))
    if args.sort == "state":
        clients.sort(key=lambda row: (str(row.get("state", "")), str(row.get("client_id", ""))))
    elif args.sort == "lease":
        clients.sort(key=lambda row: _integer(row.get("lease_remaining_ms")) or 0)
    elif args.sort == "sessions":
        clients.sort(key=lambda row: -(_integer(row.get("sessions")) or 0))
    elif args.sort == "opens":
        clients.sort(key=lambda row: -(_integer(row.get("opens")) or 0))
    rows = []
    for client in clients:
        client_id = str(client.get("client_id", ""))
        lease = _integer(client.get("lease_remaining_ms"))
        lease_text = "--" if lease is None else "{}s".format(lease // 1000)
        addresses = ",".join(safe(address) for address in client.get("remote_addresses", [])) or "--"
        if client.get("remote_addresses_truncated"):
            addresses += ",…" if addresses != "--" else "…"
        rows.append([client_id if args.wide else client_id[:8], client.get("state", "--"),
                     lease_text, client.get("sessions", "--"), client.get("opens", "--"),
                     client.get("locks", "--"), client.get("delegations", "--"), addresses])
    lines += ["", _table(["CLIENT", "STATE", "LEASE", "SESSIONS", "OPENS", "LOCKS",
                         "DELEGATIONS", "REMOTE ADDRESSES"], rows, args.max_rows)]
    if args.wide:
        lines += ["", "CLIENT OWNERS", _table(
            ["CLIENT", "OWNER HEX", "OWNER LENGTH", "TRUNCATED", "MINOR"],
            [[str(client.get("client_id", "")), client.get("owner_hex", ""),
              client.get("owner_length", "--"), client.get("owner_truncated", False),
              client.get("minor_version", "--")] for client in clients], args.max_rows)]
    if not args.quiet and snapshot.get("truncated"):
        lines.append("Client results are truncated; use limit/offset through the REST API to page the full table.")
    return "\n".join(lines)


def _json_frame(snapshots, meta, args, error=None, stale_age=0):
    if args.inspection_view == "dashboard":
        result = {key + "_snapshot": value for key, value in snapshots.items()}
    else:
        result = copy.deepcopy(snapshots.get(args.inspection_view, {}))
        if args.inspection_view == "threads" and "live" in snapshots:
            result["live"] = copy.deepcopy(snapshots["live"])
        if args.inspection_view == "nfs_sessions" and "live" in snapshots:
            result["live"] = copy.deepcopy(snapshots["live"])
    result["_cli"] = {"view": args.inspection_view, "stale": error is not None, "stale_age_seconds": stale_age, "error": str(error) if error else None, **meta}
    return result


def run_inspection(client, args):
    if args.target:
        raise ValueError("--target requires --targets")
    if args.iterations and not args.watch:
        raise ValueError("--iterations requires --watch")
    state, last = WatchState(), {}
    last_success, failures, attempts, exit_code = None, 0, 0, 0
    terminal = args.watch and not args.json and not args.no_clear and sys.stdout.isatty() and os.environ.get("TERM") != "dumb"
    watch_keys = WatchKeys() if args.watch and not args.json and sys.stdin.isatty() and sys.stdout.isatty() else None
    try:
        if terminal:
            sys.stdout.write("\x1b[?25l")
        while True:
            started, error, retry_after = time.monotonic(), None, 0.0
            meta = {"rates": {}, "appeared": [], "absent": [], "changes_comparable": False, "restarted": False}
            try:
                current = fetch(client, args)
                meta = state.update(current)
                last, last_success, failures = current, time.monotonic(), 0
                mixed = len({s["server_instance_id"] for s in current.values()}) > 1
                exit_code = 0 if all(complete(s) for s in current.values()) and not mixed else 4
            except Exception as exc:
                # Keep the final frame and stop for validation or auth errors;
                # network/429/5xx failures may be retried in watch mode.
                if not isinstance(exc, (ValueError, OSError)) and not hasattr(exc, "status_code"):
                    raise
                error, failures = exc, failures + 1
                state.reset()
                status = getattr(exc, "status_code", None)
                exit_code = 3 if status in (401, 403) else 5 if status in (404, 405) or (status is not None and 300 <= status < 400) else 2 if isinstance(exc, ValueError) or (status is not None and 400 <= status < 500 and status != 429) else 1
                retry_after = getattr(exc, "retry_after", 0) or 0
            attempts += 1
            stale_age = max(0.0, time.monotonic() - last_success) if last_success is not None else 0.0
            if args.json:
                output = json.dumps(_json_frame(last, meta, args, error, stale_age), separators=(",", ":") if args.watch else None, indent=None if args.watch else 2)
            else:
                output = render(last, meta, args, error, stale_age)
            sys.stdout.write(("\x1b[H\x1b[2J" if terminal else "") + output + "\n")
            sys.stdout.flush()
            if not args.watch or (args.iterations and attempts >= args.iterations) or (error and exit_code in (2, 3, 5)):
                return exit_code
            delay = args.interval
            if error:
                delay = max(delay, min(30.0, 2.0 ** min(failures, 5)), min(30.0, float(retry_after)))
            remaining = max(0.0, delay - (time.monotonic() - started))
            if watch_keys and watch_keys.active:
                if watch_keys.wait(remaining):
                    return 0
            else:
                time.sleep(remaining)
    except KeyboardInterrupt:
        return 130
    finally:
        if watch_keys:
            watch_keys.close()
        if terminal:
            sys.stdout.write("\x1b[?25h")
            sys.stdout.flush()
