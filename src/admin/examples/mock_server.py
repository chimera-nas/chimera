#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Apache-2.0

"""Synthetic loopback API for exercising inspection watch without Chimera."""

import argparse
import copy
import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import time
from urllib.parse import parse_qs, urlsplit
import uuid

SEED = json.loads((Path(__file__).with_name("snapshot.json")).read_text())
START = time.monotonic_ns()
INSTANCE = str(uuid.uuid4())


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        target = urlsplit(self.path)
        if target.path not in ("/api/v1/threads", "/api/v1/connections"):
            self.send_error(404)
            return
        ns = time.monotonic_ns()
        elapsed = (ns - START) / 1_000_000_000
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        data = copy.deepcopy(SEED)
        cs, ts = data["connections_snapshot"], data["threads_snapshot"]
        for snapshot in (cs, ts):
            snapshot["server_instance_id"] = INSTANCE
        for worker in ts["threads"]:
            i = worker["worker_id"]
            worker["sampled_monotonic_ns"] = str(ns)
            worker["sampled_at"] = now
            worker["rpc"]["procedures"][0]["latency_samples_total"] = str(81234 + i * 100 + int(elapsed * (500 if i == 0 else 350 if i == 1 else 0)))
            worker["vfs"]["operations"][0]["latency_samples_total"] = str(12001 + i * 25 + int(elapsed * (750 if i == 0 else 450 if i == 1 else 0)))
        # A reconnect gets a new serial, despite reusing the same source port.
        if int(elapsed // 6) % 2:
            cs["connections"] = cs["connections"][1:]
        else:
            cs["connections"][0]["connection_id"] = "0:{}".format(10000 + int(elapsed // 12))
        for worker in ts["threads"]:
            worker["connection_counts"] = {service: sum(c["worker_id"] == worker["worker_id"] and c["service"] == service
                for c in cs["connections"]) for service in worker["connection_counts"]}
        for row in cs["connections"]:
            row["age_ms"] = str(int(elapsed * 1000))
        for sample in cs["worker_samples"]:
            sample["sampled_monotonic_ns"] = str(ns)
            sample["sampled_at"] = now
            sample["scanned"] = sample["returned"] = sum(c["worker_id"] == sample["worker_id"] for c in cs["connections"])
        query = parse_qs(target.query)
        if "worker_id" in query:
            wanted = int(query["worker_id"][0])
            ts["threads"] = [w for w in ts["threads"] if w["worker_id"] == wanted]
            cs["connections"] = [c for c in cs["connections"] if c["worker_id"] == wanted]
        for name in ("service", "transport"):
            if name in query:
                cs["connections"] = [c for c in cs["connections"] if c.get(name) == query[name][0]]
        for side in ("local", "remote"):
            for suffix, field in (("ip", "address"), ("port", "port")):
                if side + "_" + suffix in query:
                    value = query[side + "_" + suffix][0]
                    cs["connections"] = [c for c in cs["connections"] if str((c.get(side) or {}).get(field)) == value]
        payload = ts if target.path.endswith("threads") else cs
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--snapshot", type=Path, default=Path(__file__).with_name("snapshot.json"),
                        help="Synthetic seed snapshot; interactive-snapshot.json has many connections on one worker and an idle worker")
    args = parser.parse_args()
    SEED = json.loads(args.snapshot.read_text())
    with ThreadingHTTPServer(("127.0.0.1", args.port), Handler) as server:
        print("SYNTHETIC inspection API on http://127.0.0.1:{}; Ctrl-C stops it.".format(server.server_port), flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
