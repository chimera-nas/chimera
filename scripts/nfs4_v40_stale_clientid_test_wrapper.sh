#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: nfs4_v40_stale_clientid_test_wrapper.sh <chimera_binary> <pynfs_dir> <test_script>
#
# Stale-clientid-across-restart regression (issue #1377): stands up a chimera
# NFS server in a network namespace, lets an NFSv4.0 client register and take a
# lock, crashes the server (SIGKILL), starts it again with the same config in
# the same namespace -- so the same derived node_id -- and then presents the
# pre-restart clientid to the new instance.
#
# memfs only and no vfs section at all (memfs is a built-in module), so the KV
# store is the default in-memory one: nothing survives the restart and the
# grace window closes at once, which is the default deployment and the one
# where the new boot has nothing to go on but its own minting.

set -u

source "$(dirname "${BASH_SOURCE[0]}")/chimera_clientid_test_env.sh"

# Save and clear LD_PRELOAD immediately to avoid ASAN interference with system
# binaries (ip, sysctl, ...) which exit non-zero under ASAN.  Restored only for
# the chimera daemon.
SAVED_LD_PRELOAD="${LD_PRELOAD:-}"
unset LD_PRELOAD

CHIMERA_BINARY=$1; shift
PYNFS_DIR=$1; shift
TEST_SCRIPT=$1; shift

NETNS_NAME="v40stale_$$_$(date +%s%N)"
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/v40stale_session_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
STATE_FILE="${SESSION_DIR}/client_a.json"
PYCOMPAT_DIR="${SESSION_DIR}/pycompat"
CHIMERA_PID=""
CHIMERA_LOG=""

TEST_TIMEOUT="${V40STALE_TIMEOUT:-120}"

stop_chimera() {
    local sig=$1
    if [ -n "$CHIMERA_PID" ]; then
        kill "-${sig}" "$CHIMERA_PID" 2>/dev/null || true
        for i in $(seq 1 150); do
            kill -0 "$CHIMERA_PID" 2>/dev/null || break
            sleep 0.02
        done
        kill -9 "$CHIMERA_PID" 2>/dev/null || true
        wait "$CHIMERA_PID" 2>/dev/null || true
        CHIMERA_PID=""
    fi
}

cleanup() {
    stop_chimera TERM
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -rf "$SESSION_DIR"
}
trap cleanup EXIT

dump_logs() {
    for log in "${SESSION_DIR}"/chimera.*.log; do
        [ -f "$log" ] || continue
        echo "=== $(basename "$log") ==="
        cat "$log"
    done
}

# Check the same file CMake gates registration on, so a half-present checkout
# skips here instead of failing inside the import.
if [ ! -f "${PYNFS_DIR}/nfs4.0/nfs4lib.py" ]; then
    echo "pynfs not found at ${PYNFS_DIR}; skipping"
    exit 77
fi

cat > "$CONFIG_FILE" << EOF
{
    "common": {
        "rcu_reclaim_threads": 4
    },
    "server": {
        "nfs_enabled": true,
        "threads": 4,
        "delegation_threads": 4,
        "nfs4_grace_time": 1,
        "external_portmap": false
    },
    "filesystems": {
        "fs0": {
            "module": "memfs"
        }
    },
    "mounts": {
        "share": {
            "module": "memfs",
            "path": "fs0"
        }
    },
    "exports": {
        "/share": {
            "path": "/share"
        }
    }
}
EOF

ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up

ulimit -l unlimited

# start_chimera <boot number>: start the daemon and wait for its NFS port.
start_chimera() {
    CHIMERA_LOG="${SESSION_DIR}/chimera.boot$1.log"
    chimera_clientid_test_env   # a fresh supervisor grant per start
    if [ -n "${SAVED_LD_PRELOAD}" ]; then
        ip netns exec "${NETNS_NAME}" env LD_PRELOAD="${SAVED_LD_PRELOAD}" \
            "$CHIMERA_BINARY" -c "$CONFIG_FILE" > "$CHIMERA_LOG" 2>&1 &
    else
        ip netns exec "${NETNS_NAME}" \
            "$CHIMERA_BINARY" -c "$CONFIG_FILE" > "$CHIMERA_LOG" 2>&1 &
    fi
    CHIMERA_PID=$!

    # 30s: an ASan daemon starting on a runner under `ctest -j 32` can take
    # well over 10s, and a readiness timeout reads as a real failure rather
    # than as contention.
    for i in $(seq 1 1500); do
        if grep -q "Server is ready." "$CHIMERA_LOG" &&
           ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/127.0.0.1/2049" 2>/dev/null; then
            # Let the grace window close: a CLAIM_NULL OPEN inside it is
            # NFS4ERR_GRACE.
            sleep 2
            return 0
        fi
        if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
            echo "chimera daemon (boot $1) exited prematurely"
            dump_logs
            exit 1
        fi
        sleep 0.02
    done
    echo "chimera NFS port never became ready (boot $1)"
    dump_logs
    exit 1
}

# Python 3.14 images ship xdrlib3, whose pack_string requires bytes; pynfs still
# hands it str in places.  Same in-process shim the pynfs wrapper installs,
# rather than modifying the external /opt/pynfs checkout.
mkdir -p "${PYCOMPAT_DIR}"
cat > "${PYCOMPAT_DIR}/sitecustomize.py" <<'EOF'
try:
    from xdrlib3 import Packer
except ImportError:
    Packer = None

if Packer is not None:
    _pack_string = Packer.pack_string

    def _chimera_pack_string(self, s):
        if isinstance(s, str):
            s = s.encode()
        return _pack_string(self, s)

    Packer.pack_string = _chimera_pack_string
EOF
export PYTHONPATH="${PYCOMPAT_DIR}:${PYNFS_DIR}:${PYTHONPATH:-}"
export PYNFS_DIR   # the test resolves its nfs4.0/ tree under this

run_phase() {
    ip netns exec "${NETNS_NAME}" \
        timeout "${TEST_TIMEOUT}" python3 "$TEST_SCRIPT" \
            --host 127.0.0.1 --port 2049 --export share \
            --phase "$1" --state "$STATE_FILE"
}

start_chimera 1
run_phase before
RC=$?
if [ "$RC" != "0" ]; then
    echo "=== 'before' phase failed (rc $RC) ==="
    dump_logs
    exit 1
fi

# Crash, not a clean shutdown: the client gets no chance to see anything.
stop_chimera KILL
grep -h "NFSv4 server node_id" "${SESSION_DIR}"/chimera.boot1.log || true

start_chimera 2
grep -h "NFSv4 server node_id" "${SESSION_DIR}"/chimera.boot2.log || true

run_phase after
RC=$?

if [ "$RC" = "124" ]; then
    echo "=== test timed out after ${TEST_TIMEOUT}s ==="
fi

if [ "$RC" != "0" ]; then
    dump_logs
fi

exit $RC
