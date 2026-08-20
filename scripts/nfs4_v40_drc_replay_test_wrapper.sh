#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: nfs4_v40_drc_replay_test_wrapper.sh <chimera_binary> <pynfs_dir> <test_script>
#
# Cross-client replay regression: stands up a chimera NFS server in a network
# namespace and drives the replay test against it.  The NFSv4.0 reply cache is
# keyed per connection and always installed, so no config flag is needed --
# server.nfs4_drc gates NFSv4.1 reply-cache persistence and is irrelevant here.
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

if [ -n "${SAVED_LD_PRELOAD}" ]; then
    ip netns exec "${NETNS_NAME}" env LD_PRELOAD="${SAVED_LD_PRELOAD}" \
        "$CHIMERA_BINARY" -c "$CONFIG_FILE" > "$CHIMERA_LOG" 2>&1 &
else
    ip netns exec "${NETNS_NAME}" \
        "$CHIMERA_BINARY" -c "$CONFIG_FILE" > "$CHIMERA_LOG" 2>&1 &
fi
CHIMERA_PID=$!

READY=0
# 30s: an ASan daemon starting on a runner under `ctest -j 32` can take well
# over the 10s this used to allow, and a readiness timeout reads as a real
# failure rather than as contention.
for i in $(seq 1 1500); do
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
export PYNFS_DIR   # the test resolves nfs4.0/ and nfs4.0/lib/ under this

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
