#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: kvm_nfs_multinode_test_wrapper.sh <vmlinuz> <rootfs.qcow2> <chimera_binary>
#
# Shared-store multi-instance semantics test.  Two chimera instances with
# distinct server.nfs4_node_id values drive ONE shared cairn store (a stand-in
# for the out-of-tree shared FS+KV the product targets).  cairn is single-writer
# RocksDB, so the instances run one at a time (sequentially) -- which is exactly
# enough to prove the on-store NAMESPACING is right:
#
#   1. node_id=1 (initialize=true): mount NFSv4.1, create /mnt/from_node1 + write
#      state (=> a confirmed client => a recovery record under node 1's prefix).
#      Abandon the mount (a crash, not a graceful unmount); stop the daemon.
#   2. node_id=2 (initialize=false, SAME store): mount, confirm node 1's file is
#      readable (the FS data is genuinely shared across instances), create
#      /mnt/from_node2 (=> a recovery record under node 2's DISJOINT prefix);
#      stop the daemon.
#   3. node_id=1 again (initialize=false): mount, confirm BOTH files survive, and
#      check node 1's cold-start log reloaded >= 1 recovery record -- i.e. node
#      1's records survived node 2's run untouched, and node 1 reloads only its
#      own band (it never reconstructs node 2's live clients).
#
# Passing proves: with the keyspace namespaced by node_id, N instances over one
# backing store don't corrupt each other's persisted protocol state, the FS data
# is shared, and each instance reloads exactly its own records.

set -u

ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then
    QEMU_BIN="qemu-system-aarch64"
    QEMU_MACHINE="-machine virt"
    QEMU_CONSOLE="ttyAMA0"
else
    QEMU_BIN="qemu-system-x86_64"
    QEMU_MACHINE="-M microvm,acpi=on,rtc=on,pit=on,pcie=on"
    QEMU_CONSOLE="ttyS0"
fi

VMLINUZ=$1; shift
ROOTFS=$1; shift
CHIMERA_BINARY=$1; shift

NETNS_NAME="kvm_nfsmn_$$_$(date +%s%N)"
TAP_NAME="tapm_$$"
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/kvm_nfsmn_session_XXXXXX")
CAIRN_DIR="${SESSION_DIR}/cairn"
CONFIG_FILE="${SESSION_DIR}/chimera.json"
CHIMERA_LOG="${SESSION_DIR}/chimera.log"
CHIMERA_PID=""
mkdir -p "$CAIRN_DIR"

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

# Generate the chimera config.  $1 = initialize flag (true/false), $2 = node_id.
generate_config() {
    local init="$1"
    local node_id="$2"
    cat > "$CONFIG_FILE" << EOF
{
    "common": {
        "rcu_reclaim_threads": 4
    },
    "server": {
        "threads": 4,
        "delegation_threads": 4,
        "kv_module": "cairn",
        "nfs4_drc": true,
        "nfs4_node_id": ${node_id},
        "nfs4_grace_time": 2,
        "vfs": {
            "cairn": {
                "config": {"initialize":${init},"path":"$CAIRN_DIR"}
            }
        },
        "external_portmap": false
    },
    "mounts": {
        "share": {
            "module": "cairn",
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
}

BOOT_N=0
start_chimera() {
    BOOT_N=$((BOOT_N + 1))
    CHIMERA_LOG="${SESSION_DIR}/chimera_${BOOT_N}.log"
    ip netns exec "${NETNS_NAME}" "$CHIMERA_BINARY" -c "$CONFIG_FILE" \
        > "$CHIMERA_LOG" 2>&1 &
    CHIMERA_PID=$!
    for i in $(seq 1 200); do
        if ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/10.0.0.1/2049" 2>/dev/null; then
            return 0
        fi
        if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
            echo "chimera daemon exited prematurely"
            cat "$CHIMERA_LOG"
            return 1
        fi
        sleep 0.02
    done
    echo "chimera NFS port never came up"
    return 1
}

stop_chimera() {
    kill "$CHIMERA_PID" 2>/dev/null || true
    for i in $(seq 1 200); do
        kill -0 "$CHIMERA_PID" 2>/dev/null || break
        sleep 0.02
    done
    kill -9 "$CHIMERA_PID" 2>/dev/null || true
    wait "$CHIMERA_PID" 2>/dev/null || true
    CHIMERA_PID=""
}

# Boot a guest that runs $1 (NFSv4.1 mounted at /mnt).  $2 = log label.  Emits
# the parsed guest exit code.
run_guest() {
    local guest_cmd="$1"
    local label="$2"
    local log="${SESSION_DIR}/guest_${label}.log"
    local mount_opts="vers=4.1,tcp,nconnect=16"
    local full="mount -t nfs -o ${mount_opts} 10.0.0.1:/share /mnt && ${guest_cmd}"

    ip netns exec "${NETNS_NAME}" "$QEMU_BIN" \
        -enable-kvm -smp 4 -m 1G -cpu host \
        -kernel "$VMLINUZ" \
        $QEMU_MACHINE \
        -nodefaults \
        -drive file="$ROOTFS",if=virtio,format=qcow2,snapshot=on \
        -netdev tap,id=net0,ifname="${TAP_NAME}",script=no,downscript=no \
        -device virtio-net-pci,netdev=net0,romfile="" \
        -serial stdio \
        -nographic \
        -no-reboot \
        -append "root=/dev/vda rw console=${QEMU_CONSOLE} net.ifnames=0 biosdevname=0 quiet mitigations=off tsc=reliable panic=-1 test_cmd=\"${full}\" init=/init.sh" \
        > "$log" 2>/dev/null

    grep -oaP 'CHIMERA_KVM_EXIT_CODE=\K[0-9]+' "$log" | tail -1
}

# --- network namespace + TAP ---
ulimit -l unlimited
ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up
ip netns exec "${NETNS_NAME}" ip tuntap add dev "${TAP_NAME}" mode tap
ip netns exec "${NETNS_NAME}" ip addr add 10.0.0.1/24 dev "${TAP_NAME}"
ip netns exec "${NETNS_NAME}" ip link set "${TAP_NAME}" up

# --- boot 1: node 1 creates state on a fresh store, abandons the mount ---
generate_config "true" 1
start_chimera || exit 1
RC1=$(run_guest "mkdir -p /mnt/from_node1 && echo n1 > /mnt/from_node1/marker && sync && ls -l /mnt/from_node1" n1)
echo "=== node1 boot guest exit: ${RC1:-<none>} ==="
[ "${RC1:-1}" = "0" ] || { echo "FAIL: node1 guest did not succeed"; cat "$CHIMERA_LOG"; exit 1; }
sleep 1
stop_chimera

# --- boot 2: node 2 on the SAME store; must see node 1's data, add its own ---
generate_config "false" 2
start_chimera || exit 1
RC2=$(run_guest "cat /mnt/from_node1/marker && mkdir -p /mnt/from_node2 && echo n2 > /mnt/from_node2/marker && sync" n2)
echo "=== node2 boot guest exit: ${RC2:-<none>} ==="
[ "${RC2:-1}" = "0" ] || { echo "FAIL: node2 could not read node1 data / write its own"; cat "$CHIMERA_LOG"; exit 1; }
sleep 1
stop_chimera

# --- boot 3: node 1 again; both trees survive + node 1 reloads its own band ---
generate_config "false" 1
start_chimera || exit 1
RC3=$(run_guest "cat /mnt/from_node1/marker && cat /mnt/from_node2/marker" n1b)
echo "=== node1 reboot guest exit: ${RC3:-<none>} ==="

FAIL=0
[ "${RC3:-1}" = "0" ] || { echo "FAIL: node1 restart lost shared data"; FAIL=1; }

if grep -q "cold-start load complete:" "$CHIMERA_LOG"; then
    RELOADED=$(grep -oP 'cold-start load complete: \K[0-9]+' "$CHIMERA_LOG" | tail -1)
    echo "=== node1 restart reloaded ${RELOADED} of its OWN recovery record(s) ==="
    if [ "${RELOADED:-0}" -lt 1 ]; then
        echo "FAIL: node1 records did not survive node2's run (reloaded 0)"
        FAIL=1
    fi
else
    echo "FAIL: node1 restart logged no cold-start recovery load"
    FAIL=1
fi

if grep -qi "non-persistent" "$CHIMERA_LOG"; then
    echo "FAIL: KV backend reported non-persistent (expected cairn)"
    FAIL=1
fi

if [ "$FAIL" != "0" ]; then
    echo "=== chimera log ==="
    cat "$CHIMERA_LOG"
    exit 1
fi

echo "PASS: shared-store multi-instance namespacing (data shared, per-node records isolated + reloaded)"
exit 0
