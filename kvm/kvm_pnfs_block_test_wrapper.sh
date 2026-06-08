#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: kvm_pnfs_block_test_wrapper.sh <vmlinuz> <rootfs.qcow2> <chimera_binary> <backend> <test_cmd>
#
# Orchestrates a pNFS *block* layout (RFC 5663) setup and a QEMU VM to exercise
# it.  Unlike flex-files there is no data server: the file data lives on a block
# device the *client* can reach directly.
#
# 1. A raw "data" disk image is created and a SIMPLE-volume signature is written
#    at a fixed offset.  diskfs is told about this device as a "remote" data
#    device (size + deviceid + signature) -- the chimera daemon NEVER opens it;
#    it only tracks free space on it and hands out block extents.  The disk is
#    attached to the VM as a virtio-SCSI disk (/dev/sda).  It MUST be SCSI
#    (not virtio-blk): blkmapd identifies candidate disks via an SG_IO SCSI
#    INQUIRY, which virtio-blk does not support.
# 2. A block-mode chimera metadata server (diskfs, block_layout=true) runs on
#    10.0.0.1:2049 with a local metadata device.
# 3. The VM boots, starts blkmapd (which matches the advertised signature to
#    /dev/sda), mounts the MDS vers=4.1, and runs the workload.  On LAYOUTGET
#    the MDS allocates extents on the remote volume and returns a block layout;
#    the client reads/writes the data directly to /dev/sda.  Because the MDS has
#    no access to the data device it rejects inline READ/WRITE with EINVAL, so a
#    drop-caches re-read that still matches proves the block layout was used.

set -u

ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then
    QEMU_BIN="qemu-system-aarch64"
    QEMU_MACHINE="-machine virt"
    QEMU_CONSOLE="ttyAMA0"
else
    QEMU_BIN="qemu-system-x86_64"
    # microvm machine: skips legacy PCI/ACPI device probing for a faster boot
    # (~0.1s/test).  pcie=on keeps a PCIe bus so the existing virtio-pci and
    # virtio-scsi-pci devices attach unchanged; rtc/pit on so the guest kernel
    # uses normal timers (without them it falls back to slow calibration paths).
    QEMU_MACHINE="-M microvm,acpi=on,rtc=on,pit=on,pcie=on"
    QEMU_CONSOLE="ttyS0"
fi

VMLINUZ=$1; shift
ROOTFS=$1; shift
CHIMERA_BINARY=$1; shift
BACKEND=$1; shift
TEST_CMD_ARG="$*"

# Boot with no initrd: every kernel in the KVM image matrix builds the virtio
# block/net drivers in, so the kernel mounts the virtio root disk directly.
# Skipping the ~63MB initrd unpack saves ~0.9s/test.  (See kvm/CMakeLists.txt:
# the 22.04 generic kernel, which needs an initrd, was dropped from the matrix
# in favor of its HWE kernel for exactly this reason.)
QEMU_INITRD=""

NETNS_NAME="kvm_pnfsblk_$$_$(date +%s%N)"
TAP_NAME="tapb_$$"
LOG_FILE=$(mktemp /tmp/kvm_pnfsblk_test_XXXXXX.log)
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/kvm_pnfsblk_session_XXXXXX")
MDS_CONFIG="${SESSION_DIR}/mds.json"
MDS_LOG="${SESSION_DIR}/mds.log"
DATA_IMG="${SESSION_DIR}/data.img"
MDS_PID=""

MDS_IP="10.0.0.1"
MDS_PORT=2049

# Data volume: size, stable deviceid, and a SIMPLE-volume content signature at
# offset 0 that blkmapd matches against /dev/sda.
DATA_SIZE=2147483648
DEVICEID="00112233445566778899aabbccddeeff"
SIG_OFFSET=0
SIG_MAGIC="CHIMERAPNFSBLK01"
SIG_HEX=$(printf '%s' "$SIG_MAGIC" | od -An -v -tx1 | tr -d ' \n')

cleanup() {
    if [ -n "$MDS_PID" ]; then
        kill "$MDS_PID" 2>/dev/null || true
        for i in $(seq 1 150); do
            kill -0 "$MDS_PID" 2>/dev/null || break
            sleep 0.02
        done
        kill -9 "$MDS_PID" 2>/dev/null || true
        wait "$MDS_PID" 2>/dev/null || true
    fi
    if [ -f "$LOG_FILE" ]; then
        echo "=== Guest serial log ==="
        cat "$LOG_FILE"
    fi
    if [ -f "$MDS_LOG" ]; then
        echo "=== Metadata server log (tail) ==="
        tail -30 "$MDS_LOG"
    fi
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -f "$LOG_FILE"
    rm -rf "$SESSION_DIR"
}
trap cleanup EXIT

# The block-mode metadata server: a local metadata device (device 0) plus the
# remote data device (device 1) described purely by config.
generate_mds_config() {
    local device_type="io_uring"
    [ "$BACKEND" = "diskfs_aio" ] && device_type="libaio"
    local meta="${SESSION_DIR}/mds-meta.img"
    truncate -s 4G "$meta"

    cat > "$MDS_CONFIG" << EOF
{
    "common": {
        "rcu_reclaim_threads": 4
    },
    "server": {
        "threads": 4,
        "external_portmap": false,
        "vfs": {
            "diskfs": {
                "config": {
                    "initialize": true,
                    "unsafe_async": true,
                    "block_layout": true,
                    "devices": [
                        { "type": "${device_type}", "size": 4294967296, "path": "${meta}" },
                        { "role": "remote", "size": ${DATA_SIZE}, "deviceid": "${DEVICEID}",
                          "signature": { "offset": ${SIG_OFFSET}, "bytes": "${SIG_HEX}" } }
                    ]
                }
            }
        },
        "pnfs": { "enabled": true }
    },
    "mounts": { "share": { "module": "diskfs", "path": "/" } },
    "exports": { "/share": { "path": "/share" } }
}
EOF
}

wait_for_port() {
    local ip="$1" port="$2" pid="$3"
    for i in $(seq 1 250); do
        if ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/${ip}/${port}" 2>/dev/null; then
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "chimera daemon (pid $pid) exited prematurely" >&2
            return 1
        fi
        sleep 0.02
    done
    echo "timed out waiting for ${ip}:${port}" >&2
    return 1
}

ulimit -l unlimited
echo 16777216 > /proc/sys/fs/aio-max-nr 2>/dev/null || true

# The signed data disk the client will reach as /dev/sda (virtio-SCSI).
truncate -s "$DATA_SIZE" "$DATA_IMG"
printf '%s' "$SIG_MAGIC" | dd of="$DATA_IMG" bs=1 conv=notrunc 2>/dev/null

# Network namespace + TAP for the VM.
ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up
ip netns exec "${NETNS_NAME}" ip tuntap add dev "${TAP_NAME}" mode tap
ip netns exec "${NETNS_NAME}" ip addr add 10.0.0.1/24 dev "${TAP_NAME}"
ip netns exec "${NETNS_NAME}" ip link set "${TAP_NAME}" up

generate_mds_config
ip netns exec "${NETNS_NAME}" "$CHIMERA_BINARY" -c "$MDS_CONFIG" > "$MDS_LOG" 2>&1 &
MDS_PID=$!
wait_for_port "$MDS_IP" "$MDS_PORT" "$MDS_PID" || exit 1
echo "=== pNFS block metadata server up on ${MDS_IP}:${MDS_PORT} ==="

# Boot the VM: rootfs as vda (virtio-blk), the signed data volume as /dev/sda
# (virtio-SCSI, so blkmapd can SG_IO-identify it).  The test command
# (see kvm/tests/CMakeLists.txt PNFS_BLOCK_CMD_*) starts blkmapd, mounts the MDS
# and runs the workload -- it owns ordering because blkmapd must be up before
# the first LAYOUTGET.
TEST_CMD="${TEST_CMD_ARG}"

ip netns exec "${NETNS_NAME}" "$QEMU_BIN" \
    -enable-kvm -smp 4 -m 1G -cpu host \
    -kernel "$VMLINUZ" \
    $QEMU_INITRD \
    $QEMU_MACHINE \
    -nodefaults \
    -drive file="$ROOTFS",if=virtio,format=qcow2,snapshot=on \
    -device virtio-scsi-pci,id=scsi0 \
    -drive file="$DATA_IMG",if=none,id=dd0,format=raw,snapshot=on \
    -device scsi-hd,drive=dd0,bus=scsi0.0,serial=CHIMERADATA01 \
    -netdev tap,id=net0,ifname="${TAP_NAME}",script=no,downscript=no \
    -device virtio-net-pci,netdev=net0,romfile="" \
    -serial stdio \
    -nographic \
    -no-reboot \
    -append "root=/dev/vda rw console=${QEMU_CONSOLE} net.ifnames=0 biosdevname=0 quiet mitigations=off tsc=reliable panic=-1 test_cmd=\"${TEST_CMD}\" init=/init.sh" \
    2>/dev/null | tee "$LOG_FILE"

if ! kill -0 "$MDS_PID" 2>/dev/null; then
    wait "$MDS_PID" 2>/dev/null
    echo "=== chimera MDS (pid $MDS_PID) DIED during the test (exit $?) ==="
    MDS_PID=""
fi

EXIT_CODE=$(grep -oP 'CHIMERA_KVM_EXIT_CODE=\K[0-9]+' "$LOG_FILE" | tail -1)
exit ${EXIT_CODE:-1}
