#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: kvm_nfstest_test_wrapper.sh <vmlinuz> <rootfs.qcow2> <chimera_binary> <backend> <nfsver> <nfstest_program>
#
# Orchestrates a chimera NFS server + QEMU VM to run the Linux nfstest suite
# (https://wiki.linux-nfs.org/wiki/index.php/NFStest) over NFS.
#
# Unlike the cthon04/xfstests harness (kvm_nfs_test_wrapper.sh), nfstest mounts
# the share itself: it is handed --server/--export/--nfsversion/--mtpoint and
# drives the real kernel NFS client, capturing the traffic with tcpdump and
# verifying the server's on-the-wire behavior against its own RPC/NFS dissector.
# So this wrapper does NOT pre-mount; it builds the nfstest invocation as the
# guest test command.  The nfstest tree + its in-guest deps (python3, tcpdump, a
# default route for nfstest's source-IP probe) ship in the kvm-test-base image.

set -u

# Detect architecture for QEMU configuration
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
BACKEND=$1; shift
NFS_VERSION=$1; shift
NFSTEST_PROGRAM=$1; shift

# Boot with no initrd (virtio root mounted directly); see kvm_nfs_test_wrapper.sh.
QEMU_INITRD=""

NETNS_NAME="kvm_nfstest_$$_$(date +%s%N)"
TAP_NAME="tap_$$"
LOG_FILE=$(mktemp /tmp/kvm_nfstest_test_XXXXXX.log)
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/kvm_nfstest_session_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
CHIMERA_PID=""
TCPDUMP_PID=""
PCAP_DIR="${KVM_PCAP_DIR:-}"
if [ -n "$PCAP_DIR" ]; then
    PCAP_FILE="${PCAP_DIR}/${NFSTEST_PROGRAM}_${BACKEND}_nfs${NFS_VERSION}_$$.pcap"
else
    PCAP_FILE="${KVM_PCAP_FILE:-}"
fi

cleanup() {
    if [ -n "$TCPDUMP_PID" ]; then
        kill -INT "$TCPDUMP_PID" 2>/dev/null || true
        wait "$TCPDUMP_PID" 2>/dev/null || true
    fi
    if [ -n "$CHIMERA_PID" ]; then
        kill "$CHIMERA_PID" 2>/dev/null || true
        for i in $(seq 1 150); do
            kill -0 "$CHIMERA_PID" 2>/dev/null || break
            sleep 0.02
        done
        if kill -0 "$CHIMERA_PID" 2>/dev/null; then
            echo "=== Chimera shutdown hung, force killing ===" >&2
            kill -9 "$CHIMERA_PID" 2>/dev/null || true
        fi
        wait "$CHIMERA_PID" 2>/dev/null || true
    fi
    if [ -f "$LOG_FILE" ]; then
        echo "=== Guest serial log ==="
        cat "$LOG_FILE"
    fi
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -f "$LOG_FILE"
    rm -rf "$SESSION_DIR"
}
trap cleanup EXIT

# Generate chimera config based on backend
generate_config() {
    local mount_path="/"
    local vfs_section=""

    case "$BACKEND" in
        linux|io_uring)
            mount_path="$SESSION_DIR/data"
            mkdir -p "$SESSION_DIR/data"
            ;;
        memfs)
            mount_path="/"
            # nfstest_alloc fills the device and expects ENOSPC, so give memfs a
            # bounded capacity for it; by default memfs reports unlimited space.
            # 16 MiB: large enough for the test's working set, small enough that
            # the fill-the-device / ENOSPC assertions trigger quickly.
            if [ "$NFSTEST_PROGRAM" = "nfstest_alloc" ]; then
                vfs_section="\"vfs\": { \"memfs\": { \"config\": {\"size\": 16777216} } },"
            fi
            ;;
        diskfs_io_uring|diskfs_aio)
            local device_type="io_uring"
            if [ "$BACKEND" = "diskfs_aio" ]; then
                device_type="libaio"
            fi
            # nfstest_alloc fills the device to exercise ENOSPC, so give it a
            # small bounded backing (a local device's capacity is its file size);
            # other tools get the usual 10x1 GiB.  Keep each device >= 1 AG
            # (1 GiB metadata needs >64 MiB) -- 128 MiB works.
            local dev_count=10
            local dev_bytes="1G"
            if [ "$NFSTEST_PROGRAM" = "nfstest_alloc" ]; then
                dev_count=2
                dev_bytes="128M"
            fi
            local devices_json=""
            for i in $(seq 0 $((dev_count - 1))); do
                local device_path="${SESSION_DIR}/device-${i}.img"
                truncate -s "$dev_bytes" "$device_path"
                if [ $i -gt 0 ]; then
                    devices_json="${devices_json},"
                fi
                devices_json="${devices_json}{\"type\":\"$device_type\",\"size\":1,\"path\":\"$device_path\"}"
            done
            mount_path="/"
            BACKEND="diskfs"
            vfs_section="\"vfs\": {
                \"diskfs\": {
                    \"config\": {\"initialize\":true,\"devices\":[$devices_json],\"unsafe_async\":true}
                }
            },"
            ;;
        cairn)
            mount_path="/"
            vfs_section="\"vfs\": {
                \"cairn\": {
                    \"config\": {\"initialize\":true,\"path\":\"$SESSION_DIR\"}
                }
            },"
            ;;
    esac

    cat > "$CONFIG_FILE" << EOF
{
    "common": {
        "rcu_reclaim_threads": 4
    },
    "server": {
        "threads": 4,
        "delegation_threads": 4,
        $vfs_section
        "external_portmap": false
    },
    "mounts": {
        "share": {
            "module": "$BACKEND",
            "path": "$mount_path"
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

generate_config

ulimit -l unlimited
echo 16777216 > /proc/sys/fs/aio-max-nr

# Network namespace + TAP (server side of the link, 10.0.0.1)
ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up
ip netns exec "${NETNS_NAME}" ip tuntap add dev "${TAP_NAME}" mode tap
ip netns exec "${NETNS_NAME}" ip addr add 10.0.0.1/24 dev "${TAP_NAME}"
ip netns exec "${NETNS_NAME}" ip link set "${TAP_NAME}" up

if [ -n "$PCAP_FILE" ]; then
    ip netns exec "${NETNS_NAME}" tcpdump -U -i "${TAP_NAME}" -w "$PCAP_FILE" -s 0 &
    TCPDUMP_PID=$!
    sleep 0.5
fi

# Start chimera daemon in the netns (stderr flows to ctest)
ip netns exec "${NETNS_NAME}" "$CHIMERA_BINARY" -c "$CONFIG_FILE" &
CHIMERA_PID=$!

# Wait for the NFS port to be ready
for i in $(seq 1 150); do
    if ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/10.0.0.1/2049" 2>/dev/null; then
        break
    fi
    if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
        echo "chimera daemon exited prematurely"
        exit 1
    fi
    sleep 0.02
done

# Map the harness NFS version to nfstest's --nfsversion (it expresses NFSv4.0 as
# plain "4").  NFSv3 needs nolock: the guest runs no rpc.statd, so the kernel's
# NLM locking would hang -- the same nolock the cthon/xfstests wrapper uses.
NFSTEST_MTOPTS="hard,rsize=4096,wsize=4096"
case "$NFS_VERSION" in
    4.0) NFSTEST_VERSION="4" ;;
    3)   NFSTEST_VERSION="3"; NFSTEST_MTOPTS="${NFSTEST_MTOPTS},nolock" ;;
    *)   NFSTEST_VERSION="$NFS_VERSION" ;;
esac

# nfstest_interop drives its own mounts at several NFS versions (including v3),
# so force nolock for it regardless of the top-level version.
if [ "$NFSTEST_PROGRAM" = "nfstest_interop" ]; then
    case ",${NFSTEST_MTOPTS}," in
        *,nolock,*) ;;
        *) NFSTEST_MTOPTS="${NFSTEST_MTOPTS},nolock" ;;
    esac
fi

# Skip the perf01 group of nfstest_alloc: it is a performance benchmark
# (ALLOCATE must beat zero-init by some margin), environment-sensitive and not a
# correctness gate.
NFSTEST_EXTRA=""
if [ "$NFSTEST_PROGRAM" = "nfstest_alloc" ]; then
    NFSTEST_EXTRA=" --runtest ^perf01"
fi

# nfstest runs as root inside the guest, mounts the share itself, and finds its
# package tree via PYTHONPATH (no install step needed beyond the cloned tree).
TEST_CMD="mkdir -p /mnt/t && PYTHONPATH=/opt/nfstest /opt/nfstest/test/${NFSTEST_PROGRAM} \
--server 10.0.0.1 --export /share --nfsversion ${NFSTEST_VERSION} \
--mtpoint /mnt/t --mtopts ${NFSTEST_MTOPTS} --datadir nfstest_data${NFSTEST_EXTRA}"

# Guest RAM.  Most tools fit in 1 GiB; nfstest_alloc needs more because nfstest
# analyses the whole packet trace in memory and its fill/dealloc subtests
# generate large traces.
QEMU_MEM="1G"
if [ "$NFSTEST_PROGRAM" = "nfstest_alloc" ]; then
    QEMU_MEM="8G"
fi

# Boot QEMU inside the netns; the guest's /init.sh runs test_cmd (no pre-mount).
ip netns exec "${NETNS_NAME}" "$QEMU_BIN" \
    -enable-kvm -smp 4 -m "${QEMU_MEM}" -cpu host \
    -kernel "$VMLINUZ" \
    $QEMU_INITRD \
    $QEMU_MACHINE \
    -nodefaults \
    -drive file="$ROOTFS",if=virtio,format=qcow2,snapshot=on \
    -netdev tap,id=net0,ifname="${TAP_NAME}",script=no,downscript=no \
    -device virtio-net-pci,netdev=net0,romfile="" \
    -serial stdio \
    -nographic \
    -no-reboot \
    -append "root=/dev/vda rw console=${QEMU_CONSOLE} net.ifnames=0 biosdevname=0 quiet mitigations=off tsc=reliable panic=-1 test_cmd=\"${TEST_CMD}\" init=/init.sh" \
    2>/dev/null | tee "$LOG_FILE"

if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
    wait "$CHIMERA_PID" 2>/dev/null
    CHIMERA_EXIT=$?
    echo "=== Chimera daemon DIED during test (exit code: $CHIMERA_EXIT) ==="
    CHIMERA_PID=""
fi

EXIT_CODE=$(grep -oP 'CHIMERA_KVM_EXIT_CODE=\K[0-9]+' "$LOG_FILE" | tail -1)
exit ${EXIT_CODE:-1}
