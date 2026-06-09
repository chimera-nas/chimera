#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: kvm_pnfs_proxy_test_wrapper.sh <vmlinuz> <rootfs.qcow2> <chimera_binary> <backend> <test_cmd>
#
# Exercises chimera's *own* NFSv4.1 pNFS CLIENT (the nfs VFS proxy module)
# against chimera's pNFS server.  Three chimera daemons plus a guest VM:
#
#   1. A *data server* (plain memfs, data_server mode) in the backend netns.
#   2. A pNFS *metadata server* (the requested backend) in the backend netns,
#      which nfs-mounts the data server and steers file data to it.
#   3. A *proxy* in the frontend netns: a full NFS server whose backing store is
#      the nfs VFS module mounting the MDS with pNFS enabled (options=...,pnfs).
#      So the proxy IS a pNFS client -- it fetches layouts from the MDS, drives
#      READ/WRITE straight to the data server, and (this is the point) receives
#      the MDS's CB_LAYOUTRECALL over the back channel it established.
#   4. A guest VM mounts the proxy over plain NFSv4.1 and runs the workload.
#
# The proxy must be a *full* server (it advertises USE_NON_PNFS in EXCHANGE_ID,
# so a regular client can mount it); a data_server advertises USE_PNFS_DS and the
# Linux client rejects it.  A full proxy binds the fixed NFS/SMB/S3 ports, so it
# cannot share a netns with the MDS -- hence two netns joined by a veth: the MDS
# and DS in `be` (10.0.1.0/24), the proxy and the VM's TAP in `fe`.  The proxy
# reaches the MDS/DS over the veth; the VM reaches the proxy over the TAP.
#
# pNFS engaged iff the proxy opened a direct connection to the data server (it
# only does so to service a layout); the wrapper asserts that from the DS log.
# A recall workload (truncate -> MDS SETATTR -> CB_LAYOUTRECALL to the proxy) is
# asserted from the proxy log.

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

BE_NS="kvm_pnfsproxy_be_$$_$(date +%s%N)"
FE_NS="kvm_pnfsproxy_fe_$$_$(date +%s%N)"
TAP_NAME="tap_$$"
VETH_BE="vbe_$$"
VETH_FE="vfe_$$"
LOG_FILE=$(mktemp /tmp/kvm_pnfs_proxy_test_XXXXXX.log)
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/kvm_pnfs_proxy_session_XXXXXX")
DS_CONFIG="${SESSION_DIR}/ds.json"
MDS_CONFIG="${SESSION_DIR}/mds.json"
PROXY_CONFIG="${SESSION_DIR}/proxy.json"
DS_LOG="${SESSION_DIR}/ds.log"
MDS_LOG="${SESSION_DIR}/mds.log"
PROXY_LOG="${SESSION_DIR}/proxy.log"
DS_PID=""
MDS_PID=""
PROXY_PID=""

# Backend netns addresses (MDS + DS), reached by the proxy over the veth.
BE_IP="10.0.1.1"
MDS_PORT=2049
DS_PORT=2050
# Frontend netns: the proxy serves the VM here; TAP host side is PROXY_IP.
PROXY_IP="10.0.0.1"
PROXY_PORT=2049

cleanup() {
    for pid in "$PROXY_PID" "$MDS_PID" "$DS_PID"; do
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
    for lbl in "Data server:$DS_LOG" "Metadata server:$MDS_LOG" "Proxy:$PROXY_LOG"; do
        f="${lbl#*:}"
        [ -f "$f" ] && { echo "=== ${lbl%%:*} log (tail) ==="; tail -25 "$f"; }
    done
    ip netns delete "${FE_NS}" 2>/dev/null || true
    ip netns delete "${BE_NS}" 2>/dev/null || true
    ip link delete "${VETH_BE}" 2>/dev/null || true
    rm -f "$LOG_FILE"
    rm -rf "$SESSION_DIR"
}
trap cleanup EXIT

# Data server: plain memfs, data_server mode (NFS-only, stateless by handle).
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

# Metadata server: the namespace on the requested backend, nfs-mounts the DS at
# /ds0 (control path), and steers file data to it via server.pnfs (v3 data path).
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
        *) echo "pNFS proxy test supports memfs/diskfs backends, got ${BACKEND}" >&2; exit 1 ;;
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
                { "tcp": "${BE_IP}:${DS_PORT}", "rdma": "${BE_IP}:20050", "version": "3", "backing_path": "/ds0" }
            ]
        }
    },
    "mounts": {
        "share": { "module": "${module}", "path": "/" },
        "ds0": { "module": "nfs", "path": "${BE_IP}:/ds_export", "options": "vers=4,port=${DS_PORT}" }
    },
    "exports": { "/share": { "path": "/share" } }
}
EOF
}

# Proxy: a full NFS server whose backing store is the nfs module mounting the MDS
# with pNFS enabled.  It serves the VM plain NFSv4.1 and is itself the pNFS
# client under test.
generate_proxy_config() {
    cat > "$PROXY_CONFIG" << EOF
{
    "common": {
        "rcu_reclaim_threads": 4
    },
    "server": {
        "threads": 4,
        "nfs_port": ${PROXY_PORT},
        "external_portmap": false,
        "metrics_port": 9002
    },
    "mounts": {
        "pshare": { "module": "nfs", "path": "${BE_IP}:/share", "options": "vers=4,pnfs,port=${MDS_PORT}" }
    },
    "exports": { "/pshare": { "path": "/pshare" } }
}
EOF
}

wait_for_port() {
    local ns="$1" ip="$2" port="$3" pid="$4"
    for i in $(seq 1 250); do
        if ip netns exec "${ns}" bash -c "echo > /dev/tcp/${ip}/${port}" 2>/dev/null; then
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

# Two netns joined by a veth.  Backend (be) holds the MDS + DS; frontend (fe)
# holds the proxy and the VM's TAP.
ip netns add "${BE_NS}"
ip netns add "${FE_NS}"
ip link add "${VETH_BE}" type veth peer name "${VETH_FE}"
ip link set "${VETH_BE}" netns "${BE_NS}"
ip link set "${VETH_FE}" netns "${FE_NS}"

ip netns exec "${BE_NS}" ip link set lo up
ip netns exec "${BE_NS}" ip addr add "${BE_IP}/24" dev "${VETH_BE}"
ip netns exec "${BE_NS}" ip link set "${VETH_BE}" up

ip netns exec "${FE_NS}" ip link set lo up
ip netns exec "${FE_NS}" ip addr add 10.0.1.2/24 dev "${VETH_FE}"
ip netns exec "${FE_NS}" ip link set "${VETH_FE}" up
ip netns exec "${FE_NS}" ip tuntap add dev "${TAP_NAME}" mode tap
ip netns exec "${FE_NS}" ip addr add "${PROXY_IP}/24" dev "${TAP_NAME}"
ip netns exec "${FE_NS}" ip link set "${TAP_NAME}" up

# 1. Data server (must be up before the MDS, which mounts it at boot).
generate_ds_config
ip netns exec "${BE_NS}" "$CHIMERA_BINARY" -c "$DS_CONFIG" > "$DS_LOG" 2>&1 &
DS_PID=$!
wait_for_port "${BE_NS}" "$BE_IP" "$DS_PORT" "$DS_PID" || exit 1
echo "=== pNFS data server up on ${BE_IP}:${DS_PORT} (be) ==="

# 2. Metadata server (nfs-mounts the DS at boot).
generate_mds_config
ip netns exec "${BE_NS}" "$CHIMERA_BINARY" -c "$MDS_CONFIG" > "$MDS_LOG" 2>&1 &
MDS_PID=$!
wait_for_port "${BE_NS}" "$BE_IP" "$MDS_PORT" "$MDS_PID" || exit 1
echo "=== pNFS metadata server up on ${BE_IP}:${MDS_PORT} (be) ==="
grep -q "backing root resolved" "$MDS_LOG" || {
    echo "MDS did not resolve its data-server backing root:" >&2
    grep -iE "pNFS|backing|nfs|error" "$MDS_LOG" | tail -10 >&2
    exit 1
}

# 3. Proxy (the pNFS client under test): mounts the MDS with pNFS at boot.
generate_proxy_config
ip netns exec "${FE_NS}" "$CHIMERA_BINARY" -c "$PROXY_CONFIG" > "$PROXY_LOG" 2>&1 &
PROXY_PID=$!
wait_for_port "${FE_NS}" "$PROXY_IP" "$PROXY_PORT" "$PROXY_PID" || exit 1
echo "=== pNFS proxy up on ${PROXY_IP}:${PROXY_PORT} (fe) ==="
grep -q "pnfs_mds=1" "$PROXY_LOG" || {
    echo "Proxy did not negotiate pNFS-MDS with the metadata server:" >&2
    grep -iE "EXCHANGE_ID|pnfs|back-channel|error" "$PROXY_LOG" | tail -10 >&2
    exit 1
}
grep -q "back-channel session established" "$PROXY_LOG" || {
    echo "Proxy did not establish its NFSv4.1 back channel:" >&2
    grep -iE "back-channel|EXCHANGE_ID|error" "$PROXY_LOG" | tail -10 >&2
    exit 1
}
echo "=== proxy negotiated pNFS-MDS + established back channel ==="

# 4. Boot the VM, mount the proxy over plain NFSv4.1, run the workload.
NFS_MOUNT_OPTS="vers=4.1,tcp"
TEST_CMD="mount -t nfs -o ${NFS_MOUNT_OPTS} ${PROXY_IP}:/pshare /mnt && ${TEST_CMD_ARG}"

ip netns exec "${FE_NS}" "$QEMU_BIN" \
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

for pid in "$PROXY_PID" "$MDS_PID" "$DS_PID"; do
    if ! kill -0 "$pid" 2>/dev/null; then
        wait "$pid" 2>/dev/null
        echo "=== A chimera daemon (pid $pid) DIED during the test (exit $?) ==="
        [ "$pid" = "$PROXY_PID" ] && PROXY_PID=""
        [ "$pid" = "$MDS_PID" ] && MDS_PID=""
        [ "$pid" = "$DS_PID" ] && DS_PID=""
    fi
done

EXIT_CODE=$(grep -oP 'CHIMERA_KVM_EXIT_CODE=\K[0-9]+' "$LOG_FILE" | tail -1)
if [ "${EXIT_CODE:-1}" != "0" ]; then
    exit "${EXIT_CODE:-1}"
fi

# Workload succeeded in the guest; now assert the data really traversed pNFS:
# the proxy opens a connection to the data server (10.0.1.1:DS_PORT) only to
# service a layout.  Its absence means the proxy silently fell back to MDS I/O.
if ! grep -qE "Connected.* to ${BE_IP}:${DS_PORT}" "$PROXY_LOG"; then
    echo "=== pNFS NOT exercised: proxy never connected to the data server ===" >&2
    grep -iE "layoutget|getdeviceinfo|pnfs|Connected" "$PROXY_LOG" | tail -15 >&2
    exit 1
fi
echo "=== verified: proxy drove direct data-server I/O via a flex-files layout ==="

# The DS device advertises BOTH an rdma and a tcp netaddr (see the MDS config).
# The proxy mounts the MDS over TCP, so it must select the tcp netaddr for the
# DS (had it wrongly picked rdma it would have tried RDMA to a non-RDMA port).
if ! grep -q "GETDEVICEINFO ok: netid=tcp" "$PROXY_LOG"; then
    echo "=== wrong DS transport selected from a multi-netaddr device ===" >&2
    grep -iE "getdeviceinfo|registered DS" "$PROXY_LOG" | tail -10 >&2
    exit 1
fi
echo "=== verified: proxy selected the tcp netaddr from an rdma+tcp device ==="

# For recall workloads, assert the proxy actually received + handled the recall.
if echo "$TEST_CMD_ARG" | grep -q "RECALL_EXPECTED"; then
    if ! grep -q "CB_LAYOUTRECALL" "$PROXY_LOG"; then
        echo "=== recall NOT delivered: proxy logged no CB_LAYOUTRECALL ===" >&2
        grep -iE "layoutrecall|back-channel|fenced" "$PROXY_LOG" | tail -15 >&2
        exit 1
    fi
    echo "=== verified: proxy received + handled CB_LAYOUTRECALL ==="
fi

exit 0
