#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: kvm_pnfs_test_wrapper.sh <vmlinuz> <rootfs.qcow2> <chimera_binary> <backend> <nfsver> <ds_version> <test_cmd>
#
# Orchestrates an NFSv4.1 flex-files pNFS setup and a QEMU VM to exercise it:
# 1. Starts a chimera *data server* (plain memfs) on 10.0.0.1:2050, exporting a
#    directory.  data_server mode skips portmap/mountd (so it can share a netns
#    with the MDS) and serves READ/WRITE statelessly by file handle.
# 2. Starts a chimera *metadata server* (pNFS enabled) on 10.0.0.1:2049, which
#    nfs-mounts the data server's export (the control path, over NFSv4 so no
#    portmap is needed) and steers each file's data to it.
# 3. Boots a QEMU VM which mounts the MDS with vers=4.1 and runs the test.
#
# <ds_version> selects the NFS version the client uses for the direct data path
# to the data server (advertised in ff_device_addr4.ffda_versions): "3" for the
# NFSv3 data path, "4.1" for the NFSv4.1 data path.  The client->MDS mount is
# always 4.1 (pNFS requires a session); ds_version is independent of it.
#
# On the first LAYOUTGET the MDS creates a backing file on the data server and
# hands the client a flex-files layout pointing at the DS's native handle; the
# client then does READ/WRITE straight to the data server over the configured
# DS version.  memfs FHs are version-independent, so the v4 control path and the
# v3/v4.1 data path address the same backing file.  A workload that reads back
# what it wrote proves the client reached the data server via the layout.

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
NFS_VERSION=$1; shift
DS_VERSION=$1; shift
TOPOLOGY=$1; shift
TEST_CMD_ARG="$*"

# pNFS requires NFSv4.1+.
case "$NFS_VERSION" in
    4.1 | 4.2) ;;
    *) echo "pNFS test requires NFS 4.1+, got ${NFS_VERSION}" >&2; exit 1 ;;
esac

# The DS data path is either NFSv3 or NFSv4.1.
case "$DS_VERSION" in
    3 | 4.1) ;;
    *) echo "pNFS DS version must be 3 or 4.1, got ${DS_VERSION}" >&2; exit 1 ;;
esac

# Topology:
#   split    - a dedicated data-server daemon, reached by the MDS over the nfs
#              proxy module (the backing handle is proxy-wrapped).
#   combined - one daemon is both MDS and DS, backed by a LOCAL mount (no nfs
#              proxy); exercises the local-backing layout path where the DS
#              handle is the backing handle as-is.  v3 data path only (a v4.1 DS
#              at the MDS's own address/identity would be coalesced by the
#              client).
case "$TOPOLOGY" in
    split) ;;
    combined)
        if [ "$DS_VERSION" != "3" ]; then
            echo "combined topology supports only ds_version=3, got ${DS_VERSION}" >&2
            exit 1
        fi
        ;;
    *) echo "topology must be split or combined, got ${TOPOLOGY}" >&2; exit 1 ;;
esac

# Boot with no initrd: every kernel in the KVM image matrix builds the virtio
# block/net drivers in, so the kernel mounts the virtio root disk directly.
# Skipping the ~63MB initrd unpack saves ~0.9s/test.  (See kvm/CMakeLists.txt:
# the 22.04 generic kernel, which needs an initrd, was dropped from the matrix
# in favor of its HWE kernel for exactly this reason.)
QEMU_INITRD=""

NETNS_NAME="kvm_pnfs_$$_$(date +%s%N)"
TAP_NAME="tap_$$"
LOG_FILE=$(mktemp /tmp/kvm_pnfs_test_XXXXXX.log)
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/kvm_pnfs_session_XXXXXX")
MDS_CONFIG="${SESSION_DIR}/mds.json"
DS_CONFIG="${SESSION_DIR}/ds.json"
DS_LOG="${SESSION_DIR}/ds.log"
MDS_LOG="${SESSION_DIR}/mds.log"
MDS_PID=""
DS_PID=""

MDS_IP="10.0.0.1"
DS_IP="10.0.0.1"
MDS_PORT=2049
DS_PORT=2050

# NFSv4.1 DS I/O is session-based, and the Linux client keys its sessions on the
# server's network address: a v4.1 DS that shares the MDS's address collides in
# the client's transport switch ("addr already in xprt switch") and the data
# path never comes up.  Give the v4.1 DS its own address (the v3 DS is stateless
# and works fine co-located with the MDS, so it keeps 10.0.0.1).
if [ "$DS_VERSION" = "4.1" ]; then
    DS_IP="10.0.0.6"
fi

cleanup() {
    for pid in "$MDS_PID" "$DS_PID"; do
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null || true
            for i in $(seq 1 150); do
                kill -0 "$pid" 2>/dev/null || break
                sleep 0.02
            done
            kill -9 "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    if [ -f "$LOG_FILE" ]; then
        echo "=== Guest serial log ==="
        cat "$LOG_FILE"
    fi
    if [ -f "$DS_LOG" ]; then
        echo "=== Data server log (tail) ==="
        tail -20 "$DS_LOG"
    fi
    if [ -f "$MDS_LOG" ]; then
        echo "=== Metadata server log (tail) ==="
        tail -20 "$MDS_LOG"
    fi
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -f "$LOG_FILE"
    rm -rf "$SESSION_DIR"
}
trap cleanup EXIT

# The data server is a plain memfs export.  data_server mode skips
# portmap/mountd (port coexistence with the MDS) and serves READ/WRITE
# statelessly by file handle, which is exactly what the client's direct pNFS
# I/O needs.
#
# nfs_server_scope is distinct from the MDS's (default 42): for a v4.1 DS the
# client keys server identity on EXCHANGE_ID scope, and an identical scope makes
# it coalesce the DS with the MDS and misroute the data path.  Harmless for v3.
generate_ds_config() {
    cat > "$DS_CONFIG" << EOF
{
    "common": {
        "rcu_reclaim_threads": 4
    },
    "server": {
        "threads": 2,
        "nfs_port": ${DS_PORT},
        "data_server": true,
        "external_portmap": true,
        "nfs_server_scope": 43,
        "metrics_port": 9001
    },
    "mounts": { "ds_data": { "module": "memfs", "path": "/" } },
    "exports": { "/ds_export": { "path": "/ds_data" } }
}
EOF
}

# The metadata server runs the namespace on the requested backend, nfs-mounts
# the data server's export at /ds0 (control path, over NFSv4 so no portmap is
# needed), and steers file data to it via server.pnfs (backing_path "/ds0").
#
# In flex-files mode the MDS holds only namespace + the opaque pNFS blob per
# file (data lives on the DS), so a diskfs MDS needs only a small metadata
# device.  memfs needs no backing device.
generate_mds_config() {
    local module="$BACKEND"
    local vfs_section=""

    case "$BACKEND" in
        memfs) ;;
        diskfs_io_uring | diskfs_aio)
            local device_type="io_uring"
            [ "$BACKEND" = "diskfs_aio" ] && device_type="libaio"
            local devices_json=""
            for i in 0 1; do
                local device_path="${SESSION_DIR}/mds-device-${i}.img"
                truncate -s 16G "$device_path"
                [ "$i" -gt 0 ] && devices_json="${devices_json},"
                devices_json="${devices_json}{\"type\":\"${device_type}\",\"size\":1,\"path\":\"${device_path}\"}"
            done
            module="diskfs"
            vfs_section="\"vfs\": { \"diskfs\": { \"config\": { \"initialize\": true, \"unsafe_async\": true, \"devices\": [${devices_json}] } } },"
            ;;
        *) echo "pNFS flex test supports memfs/diskfs backends, got ${BACKEND}" >&2; exit 1 ;;
    esac

    cat > "$MDS_CONFIG" << EOF
{
    "common": {
        "rcu_reclaim_threads": 4
    },
    "server": {
        "threads": 4,
        "external_portmap": false,
        ${vfs_section}
        "pnfs": {
            "enabled": true,
            "data_servers": [
                { "tcp": "${DS_IP}:${DS_PORT}", "version": "${DS_VERSION}", "backing_path": "/ds0" }
            ]
        }
    },
    "mounts": {
        "share": { "module": "${module}", "path": "/" },
        "ds0": { "module": "nfs", "path": "${DS_IP}:/ds_export", "options": "vers=4,port=${DS_PORT}" }
    },
    "exports": { "/share": { "path": "/share" } }
}
EOF
}

# Combined topology: one daemon is both the MDS and its own data server, backed
# by a LOCAL mount (data_servers backing_path "/share" -> the local "share"
# mount, not the nfs proxy).  The MDS creates backing files locally and serves
# the client's direct v3 DS I/O itself, so the layout must hand the client the
# backing handle as-is (no proxy wrapper to strip) -- the local-backing path.
generate_combined_config() {
    local module="$BACKEND"
    local vfs_section=""

    case "$BACKEND" in
        memfs) ;;
        diskfs_io_uring | diskfs_aio)
            local device_type="io_uring"
            [ "$BACKEND" = "diskfs_aio" ] && device_type="libaio"
            local devices_json=""
            for i in 0 1; do
                local device_path="${SESSION_DIR}/mds-device-${i}.img"
                truncate -s 16G "$device_path"
                [ "$i" -gt 0 ] && devices_json="${devices_json},"
                devices_json="${devices_json}{\"type\":\"${device_type}\",\"size\":1,\"path\":\"${device_path}\"}"
            done
            module="diskfs"
            vfs_section="\"vfs\": { \"diskfs\": { \"config\": { \"initialize\": true, \"unsafe_async\": true, \"devices\": [${devices_json}] } } },"
            ;;
        *) echo "pNFS flex test supports memfs/diskfs backends, got ${BACKEND}" >&2; exit 1 ;;
    esac

    cat > "$MDS_CONFIG" << EOF
{
    "common": {
        "rcu_reclaim_threads": 4
    },
    "server": {
        "threads": 4,
        "external_portmap": false,
        ${vfs_section}
        "pnfs": {
            "enabled": true,
            "data_servers": [
                { "tcp": "${MDS_IP}:${MDS_PORT}", "version": "3", "backing_path": "/share" }
            ]
        }
    },
    "mounts": {
        "share": { "module": "${module}", "path": "/" }
    },
    "exports": { "/share": { "path": "/share" } }
}
EOF
}

# Wait until a chimera daemon is genuinely ready: both its log says so and the
# NFS port accepts.  The port alone is not enough: the daemon logs through an
# asynchronous flusher thread, so a line emitted before the port came up (e.g.
# "backing root resolved") can land in the log file tens of milliseconds later
# under CI load -- a single grep right after the port check races and flakes.
# The log is strictly ordered, and "Server is ready." is emitted after every
# boot-time mount and the pNFS backing-root resolution complete, so once it is
# greppable every earlier line is too, making the hard confirmations below
# race-free.  (Mirrors scripts/pynfs_pnfs_test_wrapper.sh.)
wait_for_ready() {
    local ip="$1" port="$2" pid="$3" log="$4"
    for i in $(seq 1 500); do
        if grep -q "Server is ready." "$log" 2>/dev/null &&
           ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/${ip}/${port}" 2>/dev/null; then
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "chimera daemon (pid $pid) exited prematurely" >&2
            return 1
        fi
        sleep 0.02
    done
    echo "timed out waiting for ${ip}:${port} to become ready" >&2
    return 1
}

ulimit -l unlimited
echo 16777216 > /proc/sys/fs/aio-max-nr 2>/dev/null || true

# Network namespace + TAP for the VM.
ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up
ip netns exec "${NETNS_NAME}" ip tuntap add dev "${TAP_NAME}" mode tap
ip netns exec "${NETNS_NAME}" ip addr add 10.0.0.1/24 dev "${TAP_NAME}"
# A second server address: the Linux NFS client keys its client-id on the
# server address, so a guest mounting both 10.0.0.1 and 10.0.0.5 acts as two
# distinct pNFS clients -- enough to exercise inter-client layout recall.
ip netns exec "${NETNS_NAME}" ip addr add 10.0.0.5/24 dev "${TAP_NAME}"
# A dedicated address for the data server so a v4.1 (session-based) DS does not
# collide with the MDS's address in the client's transport switch.
ip netns exec "${NETNS_NAME}" ip addr add 10.0.0.6/24 dev "${TAP_NAME}"
ip netns exec "${NETNS_NAME}" ip link set "${TAP_NAME}" up

if [ "$TOPOLOGY" = "split" ]; then
    # 1. Start the data server (must be up before the MDS, which mounts it at boot).
    generate_ds_config
    ip netns exec "${NETNS_NAME}" "$CHIMERA_BINARY" -c "$DS_CONFIG" > "$DS_LOG" 2>&1 &
    DS_PID=$!
    wait_for_ready "$DS_IP" "$DS_PORT" "$DS_PID" "$DS_LOG" || exit 1
    echo "=== pNFS data server up on ${DS_IP}:${DS_PORT} ==="

    # 2. Start the metadata server; it nfs-mounts the data server at boot.
    generate_mds_config
else
    # Combined: a single daemon is both MDS and its own (local-backed) DS.
    generate_combined_config
fi
ip netns exec "${NETNS_NAME}" "$CHIMERA_BINARY" -c "$MDS_CONFIG" > "$MDS_LOG" 2>&1 &
MDS_PID=$!
wait_for_ready "$MDS_IP" "$MDS_PORT" "$MDS_PID" "$MDS_LOG" || exit 1
echo "=== pNFS metadata server up on ${MDS_IP}:${MDS_PORT} ==="
grep -q "backing root resolved" "$MDS_LOG" || {
    echo "MDS did not resolve its data-server backing root:" >&2
    grep -iE "pNFS|backing|nfs|error" "$MDS_LOG" | tail -10 >&2
    exit 1
}

# 3. Boot the VM, mount the MDS with pNFS, run the workload.
NFS_MOUNT_OPTS="vers=${NFS_VERSION},tcp"
TEST_CMD="mount -t nfs -o ${NFS_MOUNT_OPTS} ${MDS_IP}:/share /mnt && ${TEST_CMD_ARG}"

ip netns exec "${NETNS_NAME}" "$QEMU_BIN" \
    -enable-kvm -smp 4 -m 1G -cpu host \
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

for pid in "$MDS_PID" "$DS_PID"; do
    if ! kill -0 "$pid" 2>/dev/null; then
        wait "$pid" 2>/dev/null
        echo "=== A chimera daemon (pid $pid) DIED during the test (exit $?) ==="
        [ "$pid" = "$MDS_PID" ] && MDS_PID=""
        [ "$pid" = "$DS_PID" ] && DS_PID=""
    fi
done

EXIT_CODE=$(grep -oP 'CHIMERA_KVM_EXIT_CODE=\K[0-9]+' "$LOG_FILE" | tail -1)
exit ${EXIT_CODE:-1}
