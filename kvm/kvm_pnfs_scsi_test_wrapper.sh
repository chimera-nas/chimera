#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: kvm_pnfs_scsi_test_wrapper.sh <vmlinuz> <rootfs.qcow2> <chimera_binary> <backend> <test_cmd>
#
# Orchestrates a pNFS *SCSI* layout (RFC 8154) setup and a QEMU VM to exercise
# it.  Like the block layout there is no data server: file data lives on a block
# device the *client* reaches directly.  Unlike block (RFC 5663), the client
# matches the device strictly by its *hardware identifier* -- the SCSI VPD-0x83
# designator (here a NAA WWN) -- with NOTHING written to the data disk.
#
# 1. A raw "data" disk image is created (left entirely blank -- the whole point
#    of SCSI layout is that no content signature is needed).  It is attached to
#    the VM as a virtio-SCSI disk carrying a WWN, so the guest kernel exposes a
#    VPD-0x83 NAA designator at /dev/disk/by-id/wwn-0x...  diskfs is told about
#    this device as a "remote" data device (size + deviceid + scsi designator);
#    the chimera daemon NEVER opens it -- it only tracks free space and hands
#    out block extents.
# 2. A SCSI-mode chimera metadata server (diskfs, scsi_layout=true) runs on
#    10.0.0.1:2049 with a local metadata device.
# 3. The VM boots, loads the kernel block/SCSI layout driver, mounts the MDS
#    vers=4.1, and runs the workload.  On LAYOUTGET the MDS allocates extents on
#    the remote volume and returns a SCSI layout naming the LU by its designator;
#    the kernel matches /dev/sd? by WWN in-kernel (no blkmapd upcall) and does
#    READ/WRITE directly to it.  Because the MDS has no access to the data device
#    it rejects inline READ/WRITE with EINVAL, so a drop-caches re-read that still
#    matches proves the SCSI layout was used.
#
# IMPORTANT (bring-up finding): the Linux SCSI layout client issues a MANDATORY
# SCSI PERSISTENT RESERVE OUT (REGISTER) to the matched LU before using the
# layout.  This wrapper's data disk is an emulated virtio-SCSI scsi-hd, which
# does NOT implement persistent reservations, so the client's registration fails
# ("Invalid command operation code") and the guest kernel then Oopses in its
# failed-registration cleanup path.  Up to that point the server side works
# end-to-end: the client parses our GETDEVICEINFO, matches /dev/sd? by its NAA
# WWN ("pNFS: using block device sda"), and reaches LAYOUTGET -- i.e. the
# hardware-ID matching this layout type exists for is proven.  To run the data
# path you must back the LU with a PR-CAPABLE target (an LIO/tcmu device, or a
# real SAS/FC/iSCSI LU) and pass it through (e.g. scsi-block + qemu-pr-helper,
# which forwards PR to the host device).  Emulated scsi-hd cannot do this, and
# qemu-pr-helper has no real device to forward to, so the ctest entries are
# registered DISABLED (see kvm/tests/CMakeLists.txt); this script stays runnable
# for manual use against PR-capable storage.  The server itself issues no
# reservations in v1 (revocation is recall-only).

set -u

ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then
    QEMU_BIN="qemu-system-aarch64"
    QEMU_MACHINE="-machine virt"
    QEMU_CONSOLE="ttyAMA0"
else
    QEMU_BIN="qemu-system-x86_64"
    QEMU_MACHINE="-machine q35,usb=off"
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

NETNS_NAME="kvm_pnfsscsi_$$_$(date +%s%N)"
TAP_NAME="taps_$$"
LOG_FILE=$(mktemp /tmp/kvm_pnfsscsi_test_XXXXXX.log)
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/kvm_pnfsscsi_session_XXXXXX")
MDS_CONFIG="${SESSION_DIR}/mds.json"
MDS_LOG="${SESSION_DIR}/mds.log"
DATA_IMG="${SESSION_DIR}/data.img"
MDS_PID=""

MDS_IP="10.0.0.1"
MDS_PORT=2049

# Data volume: size, stable deviceid, and a SCSI VPD-0x83 NAA designator the
# client matches against /dev/disk/by-id/wwn-0x<WWN>.  The WWN is the disk's
# hardware identity; nothing is written to the disk to identify it.
DATA_SIZE=2147483648
DEVICEID="00112233445566778899aabbccddeeff"
WWN="5000c500deadbeef"

cleanup() {
    if [ -n "$MDS_PID" ]; then
        kill "$MDS_PID" 2>/dev/null || true
        for i in $(seq 1 30); do
            kill -0 "$MDS_PID" 2>/dev/null || break
            sleep 0.1
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

# The SCSI-mode metadata server: a local metadata device (device 0) plus the
# remote data device (device 1) described purely by config -- identified by its
# SCSI NAA designator rather than a content signature.
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
                    "scsi_layout": true,
                    "devices": [
                        { "type": "${device_type}", "size": 4294967296, "path": "${meta}" },
                        { "role": "remote", "size": ${DATA_SIZE}, "deviceid": "${DEVICEID}",
                          "scsi": { "designator_type": "naa", "code_set": "binary",
                                    "id": "${WWN}", "pr_key": 1 } }
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
    for i in $(seq 1 50); do
        if ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/${ip}/${port}" 2>/dev/null; then
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "chimera daemon (pid $pid) exited prematurely" >&2
            return 1
        fi
        sleep 0.1
    done
    echo "timed out waiting for ${ip}:${port}" >&2
    return 1
}

ulimit -l unlimited
echo 16777216 > /proc/sys/fs/aio-max-nr 2>/dev/null || true

# The (blank) data disk the client will reach as a virtio-SCSI LU, matched by WWN.
truncate -s "$DATA_SIZE" "$DATA_IMG"

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
echo "=== pNFS SCSI metadata server up on ${MDS_IP}:${MDS_PORT} ==="

# Boot the VM: rootfs as vda (virtio-blk), the blank data volume as a virtio-SCSI
# LU carrying a WWN so the guest exposes a VPD-0x83 designator.  The test command
# (see kvm/tests/CMakeLists.txt PNFS_SCSI_SETUP) loads the layout driver, mounts
# the MDS and runs the workload.
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
    -device scsi-hd,drive=dd0,bus=scsi0.0,wwn=0x${WWN} \
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
