# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Apache-2.0

"""Independent bounded polling of named Chimera management instances."""

from dataclasses import dataclass, field
import datetime
import json
import math
import os
from pathlib import Path
import queue
import re
import sys
import threading
import time
from urllib.parse import urlsplit

from .client import ChimeraAdminClient, ChimeraAdminError
from .inspection import (WatchState, _age_text, _integer, _metric_map, _rate_text, _render_nfs_clients,
                         _render_nfs_sessions, _table, complete, endpoint, fetch, protocol, safe)
from .presentation import terminal_width
from .watch_keys import WatchKeys

MAX_TARGETS = 32


@dataclass(frozen=True)
class Target:
    name: str
    url: str
    host: str
    port: int
    https: bool
    timeout: float
    options: dict = field(repr=False)


def load_targets(path, selected=None, default_timeout=3):
    """Validate names/endpoints without reading or returning secret values."""
    source = Path(path)
    if source.stat().st_size > 256 * 1024:
        raise ValueError("targets file exceeds 256 KiB")
    try:
        root = json.loads(source.read_text(encoding="utf-8"))
    except (UnicodeError, ValueError) as exc:
        raise ValueError("invalid targets JSON: {}".format(exc))
    if not isinstance(root, dict) or set(root) != {"targets"} or not isinstance(root["targets"], list):
        raise ValueError("targets file must be an object containing only a targets array")
    if not 1 <= len(root["targets"]) <= MAX_TARGETS:
        raise ValueError("configure between 1 and {} targets".format(MAX_TARGETS))
    result, seen = [], set()
    allowed = {"name", "url", "token_file", "token_env", "username", "password_file", "password_env", "ca_file", "timeout_seconds"}
    for item in root["targets"]:
        if not isinstance(item, dict) or set(item) - allowed:
            raise ValueError("target has invalid or unsupported fields")
        name, url = item.get("name"), item.get("url")
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", name):
            raise ValueError("target name must use 1-64 letters, digits, dots, dashes or underscores")
        if name in seen:
            raise ValueError("duplicate target name: " + name)
        seen.add(name)
        if not isinstance(url, str):
            raise ValueError("target {} needs a management URL".format(name))
        if any(not c.isprintable() or c.isspace() for c in url):
            raise ValueError("target {}: URL must not contain whitespace or control characters".format(name))
        try:
            parsed = urlsplit(url)
            port = parsed.port if parsed.port is not None else (443 if parsed.scheme == "https" else 80)
            if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username is not None or parsed.password is not None or parsed.path not in ("", "/") or parsed.query or parsed.fragment or not 1 <= port <= 65535:
                raise ValueError()
        except ValueError:
            raise ValueError("target {} needs an http(s) origin URL without credentials, path or query".format(name))
        for key in allowed - {"name", "url", "timeout_seconds"}:
            if key in item and (not isinstance(item[key], str) or not item[key].strip()):
                raise ValueError("target {}: {} must be a nonempty string".format(name, key))
        token_keys = [key for key in ("token_file", "token_env") if key in item]
        password_keys = [key for key in ("password_file", "password_env") if key in item]
        if len(token_keys) > 1 or len(password_keys) > 1 or (token_keys and ("username" in item or password_keys)):
            raise ValueError("target {}: choose one authentication mechanism".format(name))
        if bool(password_keys) != ("username" in item):
            raise ValueError("target {}: Basic auth needs username and one password source".format(name))
        if "ca_file" in item and parsed.scheme != "https":
            raise ValueError("target {}: ca_file requires HTTPS".format(name))
        for key in ("token_env", "password_env"):
            if key in item and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", item[key]):
                raise ValueError("target {}: invalid environment variable name".format(name))
        try:
            raw_timeout = item.get("timeout_seconds", default_timeout)
            if isinstance(raw_timeout, bool):
                raise ValueError()
            timeout = float(raw_timeout)
            if not math.isfinite(timeout) or not 0.05 <= timeout <= 60:
                raise ValueError()
        except (ValueError, TypeError):
            raise ValueError("target {}: timeout_seconds must be between 0.05 and 60".format(name))
        options = {k: v for k, v in item.items() if k not in ("name", "url", "timeout_seconds")}
        for key in ("token_file", "password_file", "ca_file"):
            if key in options:
                options[key] = str((source.resolve().parent / options[key]).resolve())
        result.append(Target(name, url.rstrip("/"), parsed.hostname, port, parsed.scheme == "https", timeout, options))
    requested = set(selected or [])
    unknown = requested - seen
    if unknown:
        raise ValueError("unknown target name(s): " + ", ".join(sorted(unknown)))
    return [target for target in result if not requested or target.name in requested]


def _secret(options, prefix):
    if prefix + "_file" in options:
        return Path(options[prefix + "_file"]).read_text(encoding="utf-8").rstrip("\r\n")
    if prefix + "_env" in options:
        name = options[prefix + "_env"]
        if name not in os.environ:
            raise ValueError("credential environment variable {} is not set".format(name))
        return os.environ[name]
    return None


def target_client(target):
    """Called and closed on this target's polling thread only."""
    token = _secret(target.options, "token")
    password = _secret(target.options, "password")
    if token is not None and (not token or not token.isascii() or any(c.isspace() or not c.isprintable() for c in token)):
        raise ValueError("Bearer token must be nonempty printable ASCII without whitespace")
    return ChimeraAdminClient(host=target.host, port=target.port, use_https=target.https,
                             timeout=target.timeout, token=token,
                             username=target.options.get("username"), password=password,
                             ca_file=target.options.get("ca_file"))


@dataclass(frozen=True)
class Failure:
    message: str
    code: int
    retry_after: float = 0

    def __str__(self):
        return self.message


def failure(exc):
    status = getattr(exc, "status_code", None)
    if status in (401, 403):
        code = 3
    elif status in (404, 405) or (status is not None and 300 <= status < 400):
        code = 5
    elif isinstance(exc, (ValueError, OSError)) or (status is not None and 400 <= status < 500 and status != 429):
        code = 2
    else:
        code = 1
    known = isinstance(exc, (ValueError, OSError)) or hasattr(exc, "status_code")
    message = str(exc) if known else "Unexpected {} while polling target".format(type(exc).__name__)
    return Failure(message, code, max(0.0, min(30.0, float(getattr(exc, "retry_after", 0) or 0))))


def empty_meta():
    return {"rates": {}, "appeared": [], "absent": [], "changes_comparable": False, "restarted": False}


class Poller(threading.Thread):
    """One session and one non-overlapping request sequence per named target."""
    def __init__(self, target, args, ready, client_factory=target_client):
        super().__init__(name="inspect-" + target.name, daemon=True)
        self.target, self.args, self.ready = target, args, ready
        self.client_factory = client_factory
        self.mailbox = queue.Queue(maxsize=2)
        self.stop_event = threading.Event()
        self.budget = target.timeout * (2 if args.inspection_view == "dashboard" else 1) + 0.25

    def publish(self, message):
        while not self.stop_event.is_set():
            try:
                self.mailbox.put(message, timeout=0.05)
                self.ready.set()
                return True
            except queue.Full:
                pass
        return False

    def run(self):
        client, attempt, errors = None, 0, 0
        limit = self.args.iterations if self.args.watch else 1
        try:
            while not self.stop_event.is_set():
                attempt += 1
                started = time.monotonic()
                if not self.publish({"kind": "start", "attempt": attempt, "started": started}):
                    break
                snapshots, err = None, None
                try:
                    if client is None:
                        client = self.client_factory(self.target)
                    snapshots = fetch(client, self.args)
                    if time.monotonic() - started > self.budget:
                        raise ChimeraAdminError("Target snapshot exceeded its time budget")
                except Exception as exc:
                    err = failure(exc)
                    snapshots = None
                ended = time.monotonic()
                terminal = err is not None and err.code in (2, 3, 5)
                done = terminal or (limit > 0 and attempt >= limit)
                if not self.publish({"kind": "result", "attempt": attempt, "finished": ended,
                                     "snapshots": snapshots, "error": err, "done": done}):
                    break
                if done:
                    break
                errors = errors + 1 if err else 0
                delay = self.args.interval
                if err:
                    delay = max(delay, min(30.0, 2.0 ** min(errors, 5)), err.retry_after)
                if self.stop_event.wait(max(0.0, delay - (ended - started))):
                    break
        finally:
            if client is not None:
                client.close()


@dataclass
class TargetView:
    target: Target
    state: WatchState = field(default_factory=WatchState)
    snapshots: dict = field(default_factory=dict)
    meta: dict = field(default_factory=empty_meta)
    error: object = None
    last_success: object = None
    pending_since: object = None
    attempt: int = 0
    done: bool = False
    overdue: bool = False

    def accept(self, message):
        if self.done and self.overdue:
            return
        self.attempt = message["attempt"]
        if message["kind"] == "start":
            self.pending_since = message["started"]
            self.overdue = False
            return
        self.pending_since = None
        self.error, self.done = message["error"], message["done"]
        self.overdue = False
        if self.error:
            self.state.reset()
            self.meta = empty_meta()
        else:
            self.snapshots = message["snapshots"]
            self.meta = self.state.update(self.snapshots)
            self.last_success = message["finished"]

    def timed_out(self):
        self.overdue = True
        self.error = Failure("Snapshot deadline exceeded; target request is still outstanding", 1)
        self.state.reset()
        self.meta = empty_meta()

    def status(self):
        if self.error:
            if self.snapshots:
                return "STALE"
            return {1: "TIMEOUT" if self.overdue else "UNREACHABLE", 2: "INPUT ERROR", 3: "AUTH ERROR", 5: "UNSUPPORTED"}.get(self.error.code, "ERROR")
        if not self.snapshots:
            return "PENDING"
        if len({s["server_instance_id"] for s in self.snapshots.values()}) > 1:
            return "INCONSISTENT"
        if any(not complete(s) for s in self.snapshots.values()):
            return "PARTIAL"
        return "LIVE"


def overall_code(views):
    if all(view.status() == "LIVE" for view in views):
        return 0
    if any(view.snapshots for view in views):
        return 4
    for code in (3, 5, 2, 1):
        if any(view.error and view.error.code == code for view in views):
            return code
    return 1


def fleet_json(views, args, now):
    result = {"view": args.inspection_view, "complete": overall_code(views) == 0, "targets": {}}
    for view in views:
        result["targets"][view.target.name] = {
            "url": view.target.url, "status": view.status(), "attempts": view.attempt,
            "polling": view.pending_since is not None,
            "last_success_age_seconds": None if view.last_success is None else max(0, now - view.last_success),
            "error": str(view.error) if view.error else None,
            "error_exit_code": view.error.code if view.error else None,
            "snapshots": view.snapshots, "comparison": view.meta,
        }
    return result


def render_fleet(views, args, now):
    quiet = getattr(args, "quiet", False)
    width = terminal_width()
    narrow_worker = width < 108
    narrow_connection = width < 150
    lines = [] if quiet else ["INSPECTION {} | {} targets".format(args.inspection_view.upper(), len(views))]
    for view in views:
        name = safe(view.target.name)
        age = "--" if view.last_success is None else "{:.1f}s".format(max(0, now - view.last_success))
        lines.extend(["{}  {}  sample age {}  {}{}".format(name, view.status(), age, safe(view.target.url), "  polling stopped" if view.done and args.watch else "")])
        ids = sorted({s["server_instance_id"] for s in view.snapshots.values()})
        if args.wide:
            lines.append("  Instance ID: {}".format(", ".join(map(safe, ids)) or "unavailable"))
        if view.error:
            lines.append("  Error: {}".format(safe(view.error)))
        for section, snapshot in view.snapshots.items():
            if not complete(snapshot):
                lines.append("  Warning: {} PARTIAL; truncated={}, missing workers={}".format(
                    section, snapshot.get("truncated", False), safe(snapshot.get("missing_workers", []))))
        if view.meta.get("restarted"):
            lines.append("  Warning: instance restart; this target's comparison baselines reset.")
        if args.inspection_view == "nfs_sessions":
            lines.extend("  " + line for line in _render_nfs_sessions(
                view.snapshots.get("nfs_sessions", {}), view.meta, args,
                str(view.error) if view.error else None).splitlines())
            lines.append("")
            continue
        if args.inspection_view == "nfs_clients":
            lines.extend("  " + line for line in _render_nfs_clients(
                view.snapshots.get("nfs_clients", {}), args,
                str(view.error) if view.error else None).splitlines())
            lines.append("")
            continue
        threads = view.snapshots.get("threads", {})
        connections = view.snapshots.get("connections", {})
        workers = list(threads.get("threads", []))
        connection_rows = list(connections.get("connections", []))
        by_worker = {}
        for conn in connection_rows:
            by_worker.setdefault(conn["worker_id"], []).append(conn)
        if args.inspection_view in ("threads", "dashboard"):
            wrates = view.meta.get("rates", {}) if view.status() == "LIVE" else {}
            headers = ["WORKER", "TID", "RPC ID", "NFS", "SMB", "S3", "RPC IN", "RPC SAMPLES", "RPC samp/s", "VFS samp/s"]
            rows = []
            for worker in workers:
                counts, rpc = worker.get("connection_counts") or {}, worker.get("rpc") or {}
                samples = _metric_map(worker, "rpc")
                rates = wrates.get(str(worker["worker_id"]), {})
                row = [worker.get("worker_name", worker["worker_id"]), worker.get("os_tid"), worker.get("rpc_thread_id"), counts.get("nfs", "--"), counts.get("smb", "--"), counts.get("s3", "--"), rpc.get("server_inflight"), sum(samples.values()) if samples is not None else None, _rate_text(rates.get("rpc")), _rate_text(rates.get("vfs"))]
                if args.inspection_view == "dashboard":
                    listed = len(by_worker.get(worker["worker_id"], []))
                    if not complete(connections):
                        listed = ">={}".format(listed)
                    total = sum(_integer(value) for value in counts.values()) if counts and all(_integer(value) is not None for value in counts.values()) else "--"
                    row += [listed, total]
                rows.append(row)
            if args.inspection_view == "dashboard":
                headers += ["MATCH/LISTED", "TOTAL(all)"]
            if narrow_worker:
                compact_headers = ["WORKER", "NFS", "SMB", "S3"]
                compact_rows = [[row[0], row[3], row[4], row[5]] for row in rows]
                if args.inspection_view == "dashboard":
                    compact_headers += ["MATCH/LISTED", "TOTAL(all)"]
                    compact_rows = [[*compact_rows[i], rows[i][-2], rows[i][-1]] for i in range(len(rows))]
                lines += ["", "  WORKERS (all traffic)", _table(compact_headers, compact_rows, args.max_rows, numeric=range(1, len(compact_headers))),
                          "\n  WORKER METRICS", _table(["WORKER", "TID", "RPC ID", "RPC IN", "RPC SAMPLES", "RPC samp/s", "VFS samp/s"],
                          [[row[0], row[1], row[2], row[6], row[7], row[8], row[9]] for row in rows], args.max_rows, numeric=range(1, 7))]
            else:
                lines += ["", "  WORKERS (all traffic)", _table(headers, rows, args.max_rows, numeric=range(1, len(headers)))]
        if args.inspection_view in ("connections", "dashboard"):
            key = (lambda c: (endpoint(c.get("remote")), c["connection_id"])) if args.sort == "client" else (lambda c: (c["worker_id"], c["connection_id"]))
            connection_rows.sort(key=key)
            if args.inspection_view == "dashboard":
                if workers:
                    lines += ["", "  CONNECTIONS BY WORKER (listed rows)"]
                    for worker in workers:
                        children = by_worker.get(worker["worker_id"], [])
                        lines.append("    {}".format(safe(worker.get("worker_name", worker["worker_id"]))))
                        if not children:
                            lines.append("      (no matching connections)" if complete(connections) else "      Connections unavailable or incomplete")
                            continue
                        if narrow_connection:
                            for c in children[:args.max_rows or None]:
                                detail = [["CLIENT", endpoint(c.get("remote"))], ["SERVER", endpoint(c.get("local"))],
                                          ["PROTOCOL", protocol(c)], ["TRANSPORT", c.get("transport")],
                                          ["BACKEND", c.get("backend")], ["STATE", c.get("state")]]
                                lines.append(_table(["FIELD", "VALUE"], [["CONNECTION", c["connection_id"]], *detail], 0))
                            if args.wide:
                                lines.append(_table(["ID", "AGE", "TID", "RX MSGS", "RAW TRANSPORT"],
                                    [[c["connection_id"], _age_text(c.get("age_ms")), c.get("os_tid"),
                                      (c.get("per_connection_stats") or {}).get("messages_received"), c.get("raw_transport")] for c in children], args.max_rows, numeric={2, 3}))
                        else:
                            headers = ["ID", "CLIENT", "SERVER", "PROTOCOL", "TRANSPORT", "BACKEND", "STATE"]
                            rows = [[c["connection_id"], endpoint(c.get("remote")), endpoint(c.get("local")), protocol(c), c.get("transport"), c.get("backend"), c.get("state")] for c in children]
                            lines.append(_table(headers, rows, args.max_rows))
                else:
                    lines += ["", "  Connections unavailable (worker sample missing)."]
            else:
                headers = [" ", "CONNECTION", "CLIENT", "SERVER", "PROTOCOL", "TRANSPORT", "BACKEND", "WORKER", "STATE"]
                rows = [["+" if view.status() == "LIVE" and c["connection_id"] in view.meta.get("appeared", []) else " ", c["connection_id"], endpoint(c.get("remote")), endpoint(c.get("local")), protocol(c), c.get("transport"), c.get("backend"), c.get("worker_name", c["worker_id"]), c.get("state")] for c in connection_rows]
                if args.wide:
                    headers += ["TID", "AGE", "RX MSGS", "RAW TRANSPORT"]
                    for row, conn in zip(rows, connection_rows):
                        row += [conn.get("os_tid"), _age_text(conn.get("age_ms")), (conn.get("per_connection_stats") or {}).get("messages_received"), conn.get("raw_transport")]
                if narrow_connection:
                    lines += ["", "  CONNECTIONS (listed rows)"]
                    for c in connection_rows[:args.max_rows or None]:
                        lines.append(_table(["FIELD", "VALUE"], [["CONNECTION", c["connection_id"]],
                            ["CLIENT", endpoint(c.get("remote"))], ["SERVER", endpoint(c.get("local"))],
                            ["PROTOCOL", protocol(c)], ["TRANSPORT", c.get("transport")],
                            ["BACKEND", c.get("backend")], ["WORKER", c.get("worker_name", c["worker_id"])],
                            ["STATE", c.get("state")]], 0))
                    if args.wide:
                        lines.append(_table(["ID", "AGE", "TID", "RX MSGS", "RAW TRANSPORT"],
                            [[c["connection_id"], _age_text(c.get("age_ms")), c.get("os_tid"),
                              (c.get("per_connection_stats") or {}).get("messages_received"), c.get("raw_transport")] for c in connection_rows], args.max_rows, numeric={2, 3}))
                else:
                    lines += ["", "  CONNECTIONS (listed rows)", _table(headers, rows, args.max_rows)]
        if view.status() == "LIVE" and view.meta.get("changes_comparable"):
            for conn in view.meta.get("absent", [])[:10]:
                detail = "" if quiet else " (not observed; not lease expiry)"
                lines.append("  ABSENT {} {} on {}{}".format(
                    safe(conn["connection_id"]), endpoint(conn.get("remote")), safe(conn.get("worker_name", conn["worker_id"])), detail))
        lines.append("")
    if lines and lines[-1] == "":
        lines.pop()
    if not quiet:
        lines += ["", "Worker metrics cover all traffic; listed connection rows may be filtered or partial.",
                  "Rates are histogram samples/s; -- means unavailable or warming up."]
    return "\n".join(lines)


def configured_targets(args):
    """Shared endpoint validation for text and interactive fleet dashboards."""
    if args.host != "localhost" or args.port != 8080 or args.https or args.no_verify_ssl or args.token_file or args.user or args.ca_file:
        raise ValueError("put endpoints, TLS and credentials in --targets; do not combine it with global connection/auth options")
    return load_targets(args.targets, args.target, args.timeout if args.timeout is not None else 3)


class PollGroup:
    """UI-independent coordinator for the existing per-target pollers."""
    def __init__(self, targets, args, client_factory=None):
        self.args = args
        self.ready = threading.Event()
        self.pollers = [Poller(target, args, self.ready) if client_factory is None else
                        Poller(target, args, self.ready, client_factory) for target in targets]
        self.views = [TargetView(target) for target in targets]

    def start(self):
        for poller in self.pollers:
            poller.start()

    def update(self):
        changed = False
        self.ready.clear()
        for poller, view in zip(self.pollers, self.views):
            while True:
                try:
                    view.accept(poller.mailbox.get_nowait())
                    changed = True
                except queue.Empty:
                    break
        now = time.monotonic()
        for poller, view in zip(self.pollers, self.views):
            if view.pending_since is not None and not view.overdue and now - view.pending_since > poller.budget:
                view.timed_out()
                changed = True
                if not self.args.watch or self.args.iterations:
                    view.done = True
                    poller.stop_event.set()
        return changed

    @property
    def done(self):
        return all(view.done for view in self.views)

    def close(self):
        for poller in self.pollers:
            poller.stop_event.set()
        deadline = time.monotonic() + 0.25
        for poller in self.pollers:
            if poller.ident is not None:
                poller.join(timeout=max(0, deadline - time.monotonic()))


def run_fleet(args):
    if args.iterations and not args.watch:
        raise ValueError("--iterations requires --watch")
    group = PollGroup(configured_targets(args), args)
    ready, views = group.ready, group.views
    terminal = args.watch and not args.json and not args.no_clear and sys.stdout.isatty() and os.environ.get("TERM") != "dumb"
    watch_keys = WatchKeys() if args.watch and not args.json and sys.stdin.isatty() and sys.stdout.isatty() else None
    last_draw = None
    try:
        if terminal:
            sys.stdout.write("\x1b[?25l")
        group.start()
        while True:
            if watch_keys and watch_keys.active and watch_keys.poll():
                return 0
            ready.wait(0.1)
            group.update()
            now = time.monotonic()
            done = all(view.done for view in views)
            have_result = any(view.snapshots or view.error for view in views)
            draw = done or (args.watch and have_result and (last_draw is None or now - last_draw >= args.interval))
            if draw:
                output = json.dumps(fleet_json(views, args, now), separators=(",", ":") if args.watch else None, indent=None if args.watch else 2) if args.json else render_fleet(views, args, now)
                sys.stdout.write(("\x1b[H\x1b[2J" if terminal else "") + output + "\n")
                sys.stdout.flush()
                last_draw = now
            if done:
                return overall_code(views)
    except KeyboardInterrupt:
        return 130
    finally:
        # Poller threads own session closure, including late network returns.
        group.close()
        if watch_keys:
            watch_keys.close()
        if terminal:
            sys.stdout.write("\x1b[?25h")
            sys.stdout.flush()
