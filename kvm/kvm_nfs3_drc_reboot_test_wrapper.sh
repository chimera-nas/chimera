#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: kvm_nfs3_drc_reboot_test_wrapper.sh <vmlinuz> <rootfs.qcow2> <chimera_binary>
#
# Cross-reboot persistence test for the NFSv3 duplicate-request cache (DRC).
# Exercises the reply-cache persistence end to end across a real chimera server
# restart, backed by a persistent cairn store (initialize=true on first boot,
# false on restart) with server.kv_module=cairn and server.nfs3_drc=true:
#
#   1. Start chimera A; a guest mounts NFSv3 and runs a batch of non-idempotent
#      ops (mkdir/create/rename/remove) under /mnt.  Each captured reply is
#      written through to the cairn KV store keyed by {client,xid,proc,cksum}.
#      The guest does NOT unmount (a crash, not a graceful teardown).
#   2. Stop chimera A and restart it (chimera B) against the same cairn store
#      with initialize=false, so the FS data and the DRC reply records survive.
#   3. chimera B warms the DRC from stable storage at thread-init; its log is
#      checked for the cold-start reload marker (>=1 reply record reloaded)
#      emitted by nfs3_drc_reload_complete.  A second guest then mounts and
#      confirms the FS is still usable.
#
# Passing proves: NFSv3 reply records are written to stable storage and
# reloaded into the in-memory cache on restart -- the cross-reboot replay tier
# the in-memory-only DRC lacks.

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

NETNS_NAME="kvm_nfs3drc_$$_$(date +%s%N)"
TAP_NAME="tap3_$$"
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/kvm_nfs3drc_session_XXXXXX")
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
        "nfs3_drc": true,
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

# Boot a guest that runs $1 (mounted NFSv3 at /mnt).  $2 = log label.  Emits the
# parsed guest exit code.
run_guest() {
    local guest_cmd="$1"
    local label="$2"
    local log="${SESSION_DIR}/guest_${label}.log"
    local mount_opts="vers=3,tcp,nconnect=16,nolock"
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

# --- boot 1: drive non-idempotent ops so reply records get persisted ---
generate_config "true"
start_chimera || exit 1
GUEST_OPS="mkdir -p /mnt/d1 /mnt/d2 && \
echo hi > /mnt/d1/f && \
mv /mnt/d1/f /mnt/d1/g && \
rmdir /mnt/d2 && \
sync && ls -l /mnt/d1"
RC1=$(run_guest "${GUEST_OPS}" boot1)
echo "=== boot 1 guest exit: ${RC1:-<none>} ==="
if [ "${RC1:-1}" != "0" ]; then
    echo "FAIL: pre-reboot guest did not succeed"
    cat "$CHIMERA_LOG"
    exit 1
fi
# Let the fire-and-forget DRC KV writes + cairn commit settle before the crash.
sleep 2
stop_chimera

# --- restart: same cairn store, initialize=false ---
generate_config "false"
start_chimera || exit 1

# chimera B warms the DRC at thread-init; give the async scan a moment, then a
# The records are keyed by client, not node, and loaded lazily: the first
# CACHEABLE op from the returning client (the mkdir below) hydrates that
# client's whole reply band from the KV store before the op is served.  A plain
# read would not trigger it, so drive a mkdir.
RC2=$(run_guest "mkdir -p /mnt/d3 && ls -l /mnt/d1 && cat /mnt/d1/g" boot2)
echo "=== boot 2 guest exit: ${RC2:-<none>} ==="

# --- assertions ---
FAIL=0

if [ "${RC2:-1}" != "0" ]; then
    echo "FAIL: post-reboot guest could not use the filesystem"
    FAIL=1
fi

if grep -q "conn DRC (type 0x05): hydrated" "$CHIMERA_LOG"; then
    RELOADED=$(grep -oP 'conn DRC \(type 0x05\): hydrated \K[0-9]+' "$CHIMERA_LOG" | tail -1)
    echo "=== chimera B hydrated ${RELOADED} NFSv3 reply record(s) for the returning client ==="
    if [ "${RELOADED:-0}" -lt 1 ]; then
        echo "FAIL: no NFSv3 reply records hydrated for the returning client"
        FAIL=1
    fi
else
    echo "FAIL: chimera B did not lazily hydrate the returning client's DRC band"
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

echo "PASS: NFSv3 DRC client-keyed persistence + lazy per-client hydrate"
exit 0
