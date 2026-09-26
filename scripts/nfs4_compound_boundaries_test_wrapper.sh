#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
# Usage: <chimera_binary> <pynfs_dir> <test_script> [minorversion] [retry_preload]
# CHIMERA_COMPOUND_FEATURE=delegation enables protocol delegations;
# CHIMERA_COMPOUND_FEATURE=pnfs starts a separate backing data server.
set -eu

SAVED_LD_PRELOAD="${LD_PRELOAD:-}"
unset LD_PRELOAD
CHIMERA_BINARY=$1
PYNFS_DIR=$2
TEST_SCRIPT=$3
MINOR=${4:-2}
RETRY_PRELOAD=${5:-}
FEATURE=${CHIMERA_COMPOUND_FEATURE:-none}
if [ -n "$RETRY_PRELOAD" ]; then
    # An instrumented daemon requires its ASan runtime before any injected
    # library. Discover the runtime linked by this binary, rather than the
    # host compiler's possibly different version. Keep it away from helpers.
    ASAN_RUNTIME=$(ldd "$CHIMERA_BINARY" | awk '/libasan[.]so/ && $2 == "=>" { print $3; exit }')
    if [ -n "$ASAN_RUNTIME" ]; then
        SAVED_LD_PRELOAD="$ASAN_RUNTIME${SAVED_LD_PRELOAD:+:$SAVED_LD_PRELOAD}"
    fi
    SAVED_LD_PRELOAD="${SAVED_LD_PRELOAD:+$SAVED_LD_PRELOAD:}$RETRY_PRELOAD"
fi
NETNS_NAME="nfscompound_$$_$(date +%s%N)"
SESSION_DIR=$(mktemp -d "${TMPDIR:-/tmp}/chimera-compound-wire.XXXXXX")
CHIMERA_LOG="${SESSION_DIR}/chimera.log"
CHIMERA_PID=""
DS_PID=""
DS_NETNS_NAME=""
KEEP_SESSION=${CHIMERA_KEEP_TEST_SESSION:-0}
if [ "$FEATURE" = namespace ] && [ -n "$RETRY_PRELOAD" ]; then
    export CHIMERA_COMPOUND_JUNCTION_GATE="$SESSION_DIR/junction-gate"
fi

cleanup() {
    if [ -n "$CHIMERA_PID" ]; then
        kill "$CHIMERA_PID" 2>/dev/null || true
        for ((i=0; i<150; i++)); do
            kill -0 "$CHIMERA_PID" 2>/dev/null || break
            sleep 0.02
        done
        kill -9 "$CHIMERA_PID" 2>/dev/null || true
        wait "$CHIMERA_PID" 2>/dev/null || true
    fi
    if [ -n "$DS_PID" ]; then
        kill "$DS_PID" 2>/dev/null || true
        for ((i=0; i<150; i++)); do
            kill -0 "$DS_PID" 2>/dev/null || break
            sleep 0.02
        done
        kill -9 "$DS_PID" 2>/dev/null || true
        wait "$DS_PID" 2>/dev/null || true
    fi
    ip netns delete "$NETNS_NAME" 2>/dev/null || true
    if [ -n "$DS_NETNS_NAME" ]; then
        ip netns delete "$DS_NETNS_NAME" 2>/dev/null || true
    fi
    if [ "$KEEP_SESSION" = 1 ]; then
        echo "Preserved test session: $SESSION_DIR"
    else
        rm -rf "$SESSION_DIR"
    fi
}
trap cleanup EXIT

if [ ! -f "$PYNFS_DIR/nfs4.1/nfs4client.py" ]; then
    echo "pynfs 4.1 runtime not found; skipping"
    exit 77
fi

cat > "$SESSION_DIR/chimera.json" <<'EOF'
{
    "common": {"rcu_reclaim_threads": 2},
    "server": {
        "nfs_enabled": true,
        "threads": 2,
        "delegation_threads": 2,
        "external_portmap": false,
        "nfs4_delegations": false,
        "nfs4_lease_time": 90,
        "nfs4_grace_time": 0
    },
    "filesystems": {"fs0": {"module": "memfs"}},
    "mounts": {"share": {"module": "memfs", "path": "fs0"}},
    "exports": {"/share": {"path": "/share"}}
}
EOF

python3 - "$SESSION_DIR" "$FEATURE" <<'PY'
import json
import pathlib
import sys
directory = pathlib.Path(sys.argv[1])
feature = sys.argv[2]
path = directory / "chimera.json"
config = json.loads(path.read_text())
if feature == "delegation":
    config["server"]["nfs4_delegations"] = True
elif feature == "namespace":
    config["server"]["rest_http_port"] = 8080
    config["server"]["rest_auth_enabled"] = False
    config["filesystems"]["rootfs"] = {"module": "memfs"}
    config["mounts"]["rootfs"] = {"module": "memfs", "path": "rootfs"}
    config["exports"]["/"] = {"path": "/rootfs"}
elif feature in ("pnfs", "pnfs_unsupported", "proxy3", "proxy4"):
    ds = {
        "common": {"rcu_reclaim_threads": 2},
        "server": {"nfs_enabled": True, "threads": 2,
                   "nfs_port": 2049 if feature.startswith("proxy") else 2050,
                   "data_server": feature.startswith("pnfs"),
                   "external_portmap": feature.startswith("pnfs"),
                   "metrics_port": 9001, "nfs4_lease_time": 90, "nfs4_grace_time": 0},
        "filesystems": {"fs0": {"module": "memfs"}},
        "mounts": {"ds_data": {"module": "memfs", "path": "fs0"}},
        "exports": {"/ds_export": {"path": "/ds_data"}},
    }
    (directory / "ds.json").write_text(json.dumps(ds))
    if feature.startswith("pnfs"):
        config["server"]["pnfs"] = {
            "enabled": True,
            "data_servers": [{"tcp": "127.0.0.1:2050", "backing_path": "/ds0"}],
        }
    config["mounts"]["ds0"] = {"module": "nfs", "path": "127.0.0.1:/ds_export",
                               "options": "vers=4,port=2050"}
    if feature in ("pnfs", "proxy3", "proxy4"):
        config["exports"]["/share"]["path"] = "/ds0"
    if feature.startswith("proxy"):
        config["mounts"]["ds0"]["path"] = "10.231.0.2:/ds_export"
        config["mounts"]["ds0"]["options"] = f"vers={feature[-1]},port=2049"
elif feature != "none":
    raise ValueError(f"Unknown compound feature: {feature}")
path.write_text(json.dumps(config))
PY

ip netns add "$NETNS_NAME"
ip netns exec "$NETNS_NAME" ip link set lo up
if [[ "$FEATURE" == proxy* ]]; then
    # Full upstream servers need their own portmap, mountd and lock services.
    # These auxiliary ports are fixed, so isolate both servers and connect
    # their namespaces rather than suppress services needed by NFSv3 mounts.
    DS_NETNS_NAME="${NETNS_NAME}_upstream"
    ip netns add "$DS_NETNS_NAME"
    ip -n "$DS_NETNS_NAME" link set lo up
    ip -n "$NETNS_NAME" link add upstream type veth peer name downstream
    ip -n "$NETNS_NAME" link set downstream netns "$DS_NETNS_NAME"
    ip -n "$NETNS_NAME" addr add 10.231.0.1/24 dev upstream
    ip -n "$NETNS_NAME" link set upstream up
    ip -n "$DS_NETNS_NAME" addr add 10.231.0.2/24 dev downstream
    ip -n "$DS_NETNS_NAME" link set downstream up
fi
ulimit -l unlimited

wait_ready() {
    local pid=$1 log=$2 port=$3 namespace=${4:-$NETNS_NAME}
    for ((i=0; i<1500; i++)); do
        if grep -q "Server is ready." "$log" &&
           ip netns exec "$namespace" bash -c "echo > /dev/tcp/127.0.0.1/$port" 2>/dev/null; then
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            cat "$log"
            return 1
        fi
        sleep 0.02
    done
    echo "NFS server did not become ready on port $port"
    cat "$log"
    return 1
}

if [[ "$FEATURE" == pnfs* || "$FEATURE" == proxy* ]]; then
    # Fault injection belongs to the MDS compound under test. The backing
    # data server must execute its ordinary requests without injected retries.
    DS_NAMESPACE=${DS_NETNS_NAME:-$NETNS_NAME}
    DS_PORT=2050
    if [[ "$FEATURE" == proxy* ]]; then DS_PORT=2049; fi
    ip netns exec "$DS_NAMESPACE" env LD_PRELOAD="" \
        "$CHIMERA_BINARY" -d -c "$SESSION_DIR/ds.json" > "$SESSION_DIR/ds.log" 2>&1 &
    DS_PID=$!
    wait_ready "$DS_PID" "$SESSION_DIR/ds.log" "$DS_PORT" "$DS_NAMESPACE"
fi
ip netns exec "$NETNS_NAME" env LD_PRELOAD="$SAVED_LD_PRELOAD" \
    "$CHIMERA_BINARY" -d -c "$SESSION_DIR/chimera.json" > "$CHIMERA_LOG" 2>&1 &
CHIMERA_PID=$!
wait_ready "$CHIMERA_PID" "$CHIMERA_LOG" 2049

export PYNFS_DIR
export PYTHONUNBUFFERED=1
RC=0
ip netns exec "$NETNS_NAME" timeout 120 python3 "$TEST_SCRIPT" \
    --host 127.0.0.1 --port 2049 --export share --minor "$MINOR" \
    --server-log "$CHIMERA_LOG" || RC=$?
if [ "$RC" = 0 ] && [ -n "$RETRY_PRELOAD" ]; then
    # Loading the fixture must actually exercise rejected and accepted
    # attempts; a missing/preempted interposition cannot silently pass.
    python3 - "$CHIMERA_LOG" "$MINOR" "$FEATURE" <<'PY' || RC=$?
import pathlib
import re
import sys
log = pathlib.Path(sys.argv[1]).read_text()
injected = log.count("NFS4_FINISH_RETRY injected ")
accepted = log.count("NFS4_FINISH_RETRY accepted ")
assert injected >= 2, f"Expected at least two injected compounds, got {injected}"
assert accepted == injected, f"Rejected {injected} compounds but accepted {accepted} retries"
injected_ids = re.findall(r"NFS4_FINISH_RETRY injected [^\n]* id=(\d+)", log)
accepted_ids = re.findall(r"NFS4_FINISH_RETRY accepted [^\n]* id=(\d+)", log)
assert len(set(injected_ids)) == injected, "Finish fixture identities are missing or reused"
assert sorted(injected_ids) == sorted(accepted_ids), "Rejected and accepted finish identities differ"
failed = sum("NFS4_FINISH_RETRY injected " in line and
             re.search(r"execution=(?!0(?:\s|$))\d+", line) is not None
             for line in log.splitlines())
assert failed >= 1, "Expected a failed execution prefix to encounter finish EAGAIN"
if sys.argv[3] != "none":
    print(f"PASS feature finish EAGAIN injection: {injected} compounds retried and accepted")
    sys.exit(0)
if sys.argv[2] == "0":
    # The v4.0 fixture sets up fresh OPEN/CONFIRM separately, then measures
    # confirmed-owner OPEN and replay checkpoints under rejected finishes.
    assert injected >= 8, f"Expected v4.0 state/replay checkpoints to retry, got {injected}"
    targets = re.findall(r"NFS4_FINISH_RETRY injected [^\n]* id=(\d+) "
                         r"name=retry-v40-confirmed-existing named_opens=2 reads=1(?:\s|$)", log)
    assert len(targets) == 1, "Two confirmed v4.0 OPENs did not encounter exactly one rejected finish"
    assert len(re.findall(r"NFS4_FINISH_RETRY accepted [^\n]* id=" + targets[0] + r"(?:\s|$)", log)) == 1, \
        "Two confirmed v4.0 OPENs did not accept their retried finish"
    print(f"PASS finish EAGAIN injection: {injected} compounds retried and accepted")
    sys.exit(0)
opened = sum("NFS4_FINISH_RETRY injected " in line and "open=1" in line
             for line in log.splitlines())
assert opened >= 2, f"Expected both read-only OPEN cases to retry, got {opened}"
# Server logs drain asynchronously relative to the fixture's stderr, and VFS
# allocations reuse addresses. Match the unique two-OPEN test shape and the
# fixture's monotonic identity rather than a pointer or adjacent log interval.
targets = re.findall(r"NFS4_FINISH_RETRY injected [^\n]* id=(\d+) "
                     r"name=retry-coalesced-existing named_opens=2(?:\s|$)", log)
assert len(targets) == 1, "Coalesced OPEN case did not encounter exactly one rejected finish"
assert len(re.findall(r"NFS4_FINISH_RETRY accepted [^\n]* id=" + targets[0] + r"(?:\s|$)", log)) == 1, \
    "Coalesced OPEN case did not accept its retried finish"
targets = re.findall(r"NFS4_FINISH_RETRY injected [^\n]* id=(\d+) "
                     r"name=retry-downgrade-existing named_opens=1 reads=1(?:\s|$)", log)
assert len(targets) == 1, "OPEN_DOWNGRADE case did not encounter exactly one rejected finish"
assert len(re.findall(r"NFS4_FINISH_RETRY accepted [^\n]* id=" + targets[0] + r"(?:\s|$)", log)) == 1, \
    "OPEN_DOWNGRADE case did not accept its retried finish"
targets = re.findall(r"NFS4_FINISH_RETRY injected [^\n]* id=(\d+) "
                     r"name=retry-lock-existing named_opens=1 reads=2(?:\s|$)", log)
assert len(targets) == 1, "OPEN/LOCK/LOCKU case did not encounter exactly one rejected finish"
assert len(re.findall(r"NFS4_FINISH_RETRY accepted [^\n]* id=" + targets[0] + r"(?:\s|$)", log)) == 1, \
    "OPEN/LOCK/LOCKU case did not accept its retried finish"
targets = re.findall(r"NFS4_FINISH_RETRY injected [^\n]* id=(\d+) "
                     r"name=retry-lock-close-existing named_opens=1 reads=1(?:\s|$)", log)
assert len(targets) == 1, "OPEN/held-LOCK/CLOSE case did not encounter exactly one rejected finish"
assert len(re.findall(r"NFS4_FINISH_RETRY accepted [^\n]* id=" + targets[0] + r"(?:\s|$)", log)) == 1, \
    "OPEN/held-LOCK/CLOSE case did not accept its retried finish"
print(f"PASS finish EAGAIN injection: {injected} compounds retried and accepted")
PY
fi
if [ "$RC" != 0 ]; then
    echo "=== compound boundary failure: server log ==="
    tail -250 "$CHIMERA_LOG"
    if [ -f "$SESSION_DIR/ds.log" ]; then
        echo "=== compound boundary failure: data server log ==="
        tail -100 "$SESSION_DIR/ds.log"
    fi
fi
exit "$RC"
