#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: kvm_nfs_reboot_test_wrapper.sh <vmlinuz> <rootfs.qcow2> <chimera_binary> <nfsver>
#
# Cross-reboot persistence test.  Exercises the NFSv4 recovery-record + DRC
# persistence end to end across a real chimera server restart, backed by a
# persistent cairn store (initialize=true on first boot, false on restart) with
# server.kv_module=cairn and server.nfs4_drc=true:
#
#   1. Start chimera A; a guest mounts NFSv4.1 and creates a known tree under
#      /mnt/persist (writing a file => OPEN => confirmed client => a recovery
#      record is persisted to the KV store).  The guest does NOT unmount, so the
#      client state is abandoned (a crash, not a graceful teardown).
#   2. Stop chimera A and restart it (chimera B) against the same cairn store
#      with initialize=false, so the FS data and the recovery/DRC KV records
#      survive.
#   3. A second guest mounts and verifies the tree survived, then chimera B's
#      log is checked for the cold-start reload marker (>=1 client record
#      reloaded) emitted by nfs_recovery_finalize_load.
#
# Passing proves: recovery records are written to stable storage and reloaded on
# restart (grace activates), and the persistent FS data survives -- the
# stable-storage tier the in-memory server lacked.

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
NFS_VERSION=$1; shift

NETNS_NAME="kvm_nfsreboot_$$_$(date +%s%N)"
TAP_NAME="tapr_$$"
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/kvm_nfsreboot_session_XXXXXX")
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

# Generate the chimera config.  $1 = initialize flag (true/false).
generate_config() {
    local init="$1"
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
    # Fresh per-boot log so CHIMERA_LOG always reflects the current daemon
    # (the post-restart assertions grep the restarted daemon's cold-start line).
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

# Boot a guest that runs $1.  Serial goes to a per-boot log file (NOT stdout, so
# the only thing this function emits is the parsed guest exit code, which the
# caller captures).  $2 = a label for the log file.
run_guest() {
    local guest_cmd="$1"
    local label="$2"
    local log="${SESSION_DIR}/guest_${label}.log"
    local mount_opts="vers=${NFS_VERSION},tcp,nconnect=16"
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

MARKER="chimera-reboot-marker-content"

# --- boot 1: create persistent state, abandon the mount (no umount) ---
generate_config "true"
start_chimera || exit 1
RC1=$(run_guest "mkdir -p /mnt/persist && echo ${MARKER} > /mnt/persist/file && sync && ls -l /mnt/persist" boot1)
echo "=== boot 1 guest exit: ${RC1:-<none>} ==="
if [ "${RC1:-1}" != "0" ]; then
    echo "FAIL: pre-reboot guest did not succeed"
    cat "$CHIMERA_LOG"
    exit 1
fi
stop_chimera

# --- restart: same cairn store, initialize=false ---
generate_config "false"
start_chimera || exit 1

# --- boot 2: verify the tree survived the restart ---
RC2=$(run_guest "cat /mnt/persist/file" boot2)
echo "=== boot 2 guest exit: ${RC2:-<none>} ==="

# --- assertions ---
FAIL=0

if [ "${RC2:-1}" != "0" ]; then
    echo "FAIL: post-reboot guest could not read the persisted file"
    FAIL=1
fi

# The cold-start load runs on the first NFSv4 compound (i.e. when boot 2
# mounts).  Its completion log reports how many client records were reloaded.
if grep -q "cold-start load complete:" "$CHIMERA_LOG"; then
    RELOADED=$(grep -oP 'cold-start load complete: \K[0-9]+' "$CHIMERA_LOG" | tail -1)
    echo "=== chimera B reloaded ${RELOADED} recovery record(s) ==="
    if [ "${RELOADED:-0}" -lt 1 ]; then
        echo "FAIL: no recovery records were reloaded after restart"
        FAIL=1
    fi
else
    echo "FAIL: chimera B did not log a cold-start recovery load"
    FAIL=1
fi

# The persistent backend must NOT have re-initialized (which would wipe data).
if grep -qi "non-persistent" "$CHIMERA_LOG"; then
    echo "FAIL: KV backend reported non-persistent (expected cairn)"
    FAIL=1
fi

if [ "$FAIL" != "0" ]; then
    echo "=== chimera log ==="
    cat "$CHIMERA_LOG"
    exit 1
fi

echo "PASS: cross-reboot persistence (data survived + recovery records reloaded)"
exit 0
