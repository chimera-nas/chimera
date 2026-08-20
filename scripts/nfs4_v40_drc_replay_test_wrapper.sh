#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: nfs4_v40_drc_replay_test_wrapper.sh <chimera_binary> <pynfs_dir> <test_script>
#
# Cross-client replay regression: stands up a chimera NFS server with the NFSv4.0
# duplicate-request cache ENABLED (server.nfs4_drc) in a network namespace, then
# drives the replay test against it.  The cache is opt-in, so a run without the
# flag would prove nothing -- the code under test would not be installed.
#
# memfs only and no vfs section at all (memfs is a built-in module), so the
# server is the only thing in the picture: no dlopened backend, no KV store.

set -u

# Save and clear LD_PRELOAD immediately to avoid ASAN interference with system
# binaries (ip, sysctl, ...) which exit non-zero under ASAN.  Restored only for
# the chimera daemon.
SAVED_LD_PRELOAD="${LD_PRELOAD:-}"
unset LD_PRELOAD

CHIMERA_BINARY=$1; shift
PYNFS_DIR=$1; shift
TEST_SCRIPT=$1; shift

NETNS_NAME="v40drc_$$_$(date +%s%N)"
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/v40drc_session_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
CHIMERA_LOG="${SESSION_DIR}/chimera.log"
PYCOMPAT_DIR="${SESSION_DIR}/pycompat"
CHIMERA_PID=""

TEST_TIMEOUT="${V40DRC_TIMEOUT:-120}"

cleanup() {
    if [ -n "$CHIMERA_PID" ]; then
        kill "$CHIMERA_PID" 2>/dev/null || true
        for i in $(seq 1 150); do
            kill -0 "$CHIMERA_PID" 2>/dev/null || break
            sleep 0.02
        done
        kill -9 "$CHIMERA_PID" 2>/dev/null || true
        wait "$CHIMERA_PID" 2>/dev/null || true
    fi
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -rf "$SESSION_DIR"
}
trap cleanup EXIT

if [ ! -d "$PYNFS_DIR" ]; then
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
        "nfs4_drc": true,
        "external_portmap": false
    },
    "mounts": {
        "share": {
            "module": "memfs",
            "path": "/"
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

if [ -n "${SAVED_LD_PRELOAD}" ]; then
    ip netns exec "${NETNS_NAME}" env LD_PRELOAD="${SAVED_LD_PRELOAD}" \
        "$CHIMERA_BINARY" -c "$CONFIG_FILE" > "$CHIMERA_LOG" 2>&1 &
else
    ip netns exec "${NETNS_NAME}" \
        "$CHIMERA_BINARY" -c "$CONFIG_FILE" > "$CHIMERA_LOG" 2>&1 &
fi
CHIMERA_PID=$!

READY=0
for i in $(seq 1 500); do
    if grep -q "Server is ready." "$CHIMERA_LOG" &&
       ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/127.0.0.1/2049" 2>/dev/null; then
        READY=1
        break
    fi
    if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
        echo "chimera daemon exited prematurely"
        cat "$CHIMERA_LOG"
        exit 1
    fi
    sleep 0.02
done

if [ "$READY" != "1" ]; then
    echo "chimera NFS port never became ready"
    cat "$CHIMERA_LOG"
    exit 1
fi

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

ip netns exec "${NETNS_NAME}" \
    timeout "${TEST_TIMEOUT}" python3 "$TEST_SCRIPT" \
        --host 127.0.0.1 --port 2049 --export share
RC=$?

if [ "$RC" = "124" ]; then
    echo "=== test timed out after ${TEST_TIMEOUT}s ==="
fi

if [ "$RC" != "0" ]; then
    echo "=== chimera log ==="
    cat "$CHIMERA_LOG"
fi

exit $RC
