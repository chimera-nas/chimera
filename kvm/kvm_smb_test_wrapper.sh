#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: kvm_smb_test_wrapper.sh <vmlinuz> <rootfs.qcow2> <chimera_binary> \
#            <backend> <dialect> <secmode> <test_cmd>
#
#   <dialect>  SMB protocol version the guest mounts with and the server's
#              dialect floor: one of 2.0.2 | 2.1 | 3.0 | 3.0.2 | 3.1.1.
#   <secmode>  per-mount security mode: none | sign | seal.  "sign" forces SMB
#              signing on the mount; "seal" forces SMB3 transport encryption.
#
# Orchestrates a chimera SMB server + QEMU VM to run tests over CIFS.
# 1. Generates chimera config for the given backend (dialect floor = <dialect>)
# 2. Creates a network namespace with TAP device
# 3. Starts chimera daemon in the netns (SMB on 10.0.0.1:445)
# 4. Boots QEMU VM which mounts CIFS (vers=<dialect>, <secmode>) and runs the
#    test command
# 5. Captures exit code and cleans up

set -u

# Detect architecture for QEMU configuration
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
DIALECT=$1; shift
SECMODE=$1; shift
TEST_CMD_ARG="$*"

case "$DIALECT" in
    2.0.2|2.1|3.0|3.0.2|3.1.1) ;;
    *) echo "invalid dialect '$DIALECT' (expected 2.0.2|2.1|3.0|3.0.2|3.1.1)" >&2; exit 1 ;;
esac
case "$SECMODE" in
    none|sign|seal) ;;
    *) echo "invalid secmode '$SECMODE' (expected none|sign|seal)" >&2; exit 1 ;;
esac

# Optional server-side feature knobs, passed through the environment so the
# positional argument list stays stable.
#   SMB_FEATURES  comma-separated tokens injected into the generated config:
#                   encrypt -> smb_encryption="enabled" + share encrypt_data:true
#                   sign    -> smb_signing_required:true
#                   leases  -> smb_leases + smb_oplocks + smb_directory_leases
#   SMB_EXPECT    pass (default) | denied.  "denied" inverts the verdict: the
#                 test passes iff the mount/first access is rejected (used for
#                 the per-share encryption-enforcement negative case).
SMB_FEATURES="${SMB_FEATURES:-}"
SMB_EXPECT="${SMB_EXPECT:-pass}"
#   SMB_CACHE     cifs cache mode (loose|strict|none); default loose.  strict
#                 makes the client lean on leases/oplocks for cache coherency,
#                 which is what the leases feature test wants to exercise.
#   SMB_BRL       0 (default) mounts with nobrl, suppressing byte-range locks;
#                 1 drops nobrl so the kernel client drives real SMB2 LOCK
#                 requests (for the locking suites).
#   SMB_MULTICHANNEL  0 (default) | 1.  1 advertises two server interfaces
#                 (a second IP on the tap) and mounts multichannel,max_channels=2
#                 so the kernel client binds a second channel.
SMB_CACHE="${SMB_CACHE:-loose}"
SMB_BRL="${SMB_BRL:-0}"
SMB_MULTICHANNEL="${SMB_MULTICHANNEL:-0}"
# Second server IP advertised for multichannel (also assigned to the tap below).
SMB_MC_IP2="10.0.0.4"

SERVER_EXTRA=""
SHARE_EXTRA=""
for feat in ${SMB_FEATURES//,/ }; do
    case "$feat" in
        encrypt)
            SERVER_EXTRA="${SERVER_EXTRA}\"smb_encryption\": \"enabled\","
            SHARE_EXTRA="${SHARE_EXTRA}\"encrypt_data\": true,"
            ;;
        sign)
            SERVER_EXTRA="${SERVER_EXTRA}\"smb_signing_required\": true,"
            ;;
        leases)
            SERVER_EXTRA="${SERVER_EXTRA}\"smb_leases\": true, \"smb_oplocks\": true, \"smb_directory_leases\": true,"
            ;;
        "") ;;
        *) echo "unknown SMB_FEATURES token '$feat'" >&2; exit 1 ;;
    esac
done

# Multichannel: advertise two NICs (1 Gbit each; the speed is parsed through an
# int in the daemon, so keep it under 2^31) so the client opens a 2nd channel.
if [ "$SMB_MULTICHANNEL" = "1" ]; then
    SERVER_EXTRA="${SERVER_EXTRA}\"smb_multichannel\": [ {\"address\":\"10.0.0.1\",\"speed\":1000000000,\"rdma\":false}, {\"address\":\"${SMB_MC_IP2}\",\"speed\":1000000000,\"rdma\":false} ],"
fi

# Boot with no initrd: every kernel in the KVM image matrix builds the virtio
# block/net drivers in, so the kernel mounts the virtio root disk directly.
# Skipping the ~63MB initrd unpack saves ~0.9s/test.  (See kvm/CMakeLists.txt:
# the 22.04 generic kernel, which needs an initrd, was dropped from the matrix
# in favor of its HWE kernel for exactly this reason.)
QEMU_INITRD=""

NETNS_NAME="kvm_smb_$$_$(date +%s%N)"
TAP_NAME="tap_$$"
LOG_FILE=$(mktemp /tmp/kvm_smb_test_XXXXXX.log)
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/kvm_smb_session_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
CHIMERA_PID=""
TCPDUMP_PID=""
PCAP_FILE="${KVM_PCAP_FILE:-}"

cleanup() {
    if [ -n "$TCPDUMP_PID" ]; then
        # Use SIGINT so tcpdump flushes its capture buffer before exiting
        kill -INT "$TCPDUMP_PID" 2>/dev/null || true
        wait "$TCPDUMP_PID" 2>/dev/null || true
    fi
    if [ -n "$CHIMERA_PID" ]; then
        kill "$CHIMERA_PID" 2>/dev/null || true
        # Give chimera up to 3 seconds to shut down cleanly
        for i in $(seq 1 150); do
            kill -0 "$CHIMERA_PID" 2>/dev/null || break
            sleep 0.02
        done
        # Force kill if still alive
        if kill -0 "$CHIMERA_PID" 2>/dev/null; then
            echo "=== Chimera shutdown hung, force killing ===" >&2
            kill -9 "$CHIMERA_PID" 2>/dev/null || true
        fi
        wait "$CHIMERA_PID" 2>/dev/null || true
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
            ;;
        diskfs_io_uring|diskfs_aio)
            local device_type="io_uring"
            if [ "$BACKEND" = "diskfs_aio" ]; then
                device_type="libaio"
            fi
            local devices_json=""
            for i in $(seq 0 9); do
                local device_path="${SESSION_DIR}/device-${i}.img"
                truncate -s 1G "$device_path"
                if [ $i -gt 0 ]; then
                    devices_json="${devices_json},"
                fi
                devices_json="${devices_json}{\"type\":\"$device_type\",\"size\":1,\"path\":\"$device_path\"}"
            done
            mount_path="/"
            BACKEND="diskfs"
            vfs_section="\"vfs\": {
                \"diskfs\": {
                    \"config\": {\"initialize\":true,\"devices\":[$devices_json],\"unsafe_async\":true,\"intent_log_size\":67108864}
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
        $SERVER_EXTRA
        "smb_min_dialect": "$DIALECT",
        "external_portmap": false
    },
    "mounts": {
        "share": {
            "module": "$BACKEND",
            "path": "$mount_path"
        }
    },
    "shares": {
        "share": {
            $SHARE_EXTRA
            "path": "/share"
        }
    },
    "users": [
        {
            "username": "root",
            "smbpasswd": "secret",
            "uid": 0,
            "gid": 0
        }
    ]
}
EOF
}

generate_config

# Raise system limits for high-parallelism testing
ulimit -l unlimited
echo 16777216 > /proc/sys/fs/aio-max-nr

# Create network namespace
ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up

# Create TAP device inside the netns
ip netns exec "${NETNS_NAME}" ip tuntap add dev "${TAP_NAME}" mode tap
ip netns exec "${NETNS_NAME}" ip addr add 10.0.0.1/24 dev "${TAP_NAME}"
# Second server address for multichannel (advertised via smb_multichannel).
[ "$SMB_MULTICHANNEL" = "1" ] && ip netns exec "${NETNS_NAME}" ip addr add "${SMB_MC_IP2}/24" dev "${TAP_NAME}"
ip netns exec "${NETNS_NAME}" ip link set "${TAP_NAME}" up

# Optionally start tcpdump to capture traffic (set KVM_PCAP_FILE to enable)
if [ -n "$PCAP_FILE" ]; then
    ip netns exec "${NETNS_NAME}" tcpdump -U -i "${TAP_NAME}" -w "$PCAP_FILE" -s 0 &
    TCPDUMP_PID=$!
    sleep 0.5
fi

# Start chimera daemon in the netns
CHIMERA_LOG="${SESSION_DIR}/chimera_stderr.log"
ip netns exec "${NETNS_NAME}" env \
    ASAN_OPTIONS="detect_leaks=0:handle_abort=2:print_cmdline=1" \
    "$CHIMERA_BINARY" ${CHIMERA_DEBUG:+-d} -c "$CONFIG_FILE" \
    2>"$CHIMERA_LOG" &
CHIMERA_PID=$!

# Wait for SMB port to be ready
for i in $(seq 1 150); do
    if ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/10.0.0.1/445" 2>/dev/null; then
        break
    fi
    if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
        echo "chimera daemon exited prematurely"
        exit 1
    fi
    sleep 0.02
done

# Translate the server-side dialect string into the kernel cifs `vers=` token.
# They are NOT identical: SMB 2.0.2 mounts as vers=2.0 (there is no "2.0.2"
# token) and 3.0.2 canonically as vers=3.02.  2.1 / 3.0 / 3.1.1 match as-is.
case "$DIALECT" in
    2.0.2) VERS="2.0"   ;;
    2.1)   VERS="2.1"   ;;
    3.0)   VERS="3.0"   ;;
    3.0.2) VERS="3.02"  ;;
    3.1.1) VERS="3.1.1" ;;
esac

# Build the mount options for the CIFS mount.  vers= pins the dialect; seal/sign
# add SMB3 transport encryption / signing for the secmode variants.
# nobrl suppresses byte-range locking; drop it when SMB_BRL=1 so the kernel
# client issues real SMB2 LOCK requests.
BRL_OPT=",nobrl"
[ "$SMB_BRL" = "1" ] && BRL_OPT=""
SMB_MOUNT_OPTS="username=root,password=secret,vers=${VERS}${BRL_OPT},modefromsid,cache=${SMB_CACHE}"
case "$SECMODE" in
    seal) SMB_MOUNT_OPTS="${SMB_MOUNT_OPTS},seal" ;;
    sign) SMB_MOUNT_OPTS="${SMB_MOUNT_OPTS},sign" ;;
esac
[ "$SMB_MULTICHANNEL" = "1" ] && SMB_MOUNT_OPTS="${SMB_MOUNT_OPTS},multichannel,max_channels=2"

# Build the test command to run inside the VM.  Normally we mount and then run
# the supplied command; for SMB_EXPECT=denied we instead assert that the mount
# (or the first access through it) is rejected -- the per-share encryption
# enforcement returns STATUS_ACCESS_DENIED, which tree-connect is exempt from,
# so a mount can succeed while the first I/O is denied.  Either is a pass.
if [ "$SMB_EXPECT" = "denied" ]; then
    # Pass iff access is denied: the if/else is the last command so its status
    # becomes the test's exit code (no explicit `exit`, which could terminate
    # the guest init harness before it records the result -- as the positive
    # path also relies on $? of its last command).  If the mount succeeds, the
    # first I/O ("! ls") must fail; if the mount itself is refused, that is also
    # a pass.
    TEST_CMD="if mount -t cifs //10.0.0.1/share /mnt -o ${SMB_MOUNT_OPTS} 2>/dev/null; then ! ls /mnt >/dev/null 2>&1; else true; fi"
else
    TEST_CMD="mount -t cifs //10.0.0.1/share /mnt -o ${SMB_MOUNT_OPTS} && ${TEST_CMD_ARG}"
fi

# Boot QEMU inside the netns
# Use -serial stdio so serial output goes to stdout in real-time (captured by ctest).
# Pipe through tee to also write to LOG_FILE for exit code parsing.
# This ensures guest output is visible even when ctest kills the process on timeout.
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

# Check if chimera is still alive after QEMU exits
if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
    wait "$CHIMERA_PID" 2>/dev/null
    CHIMERA_EXIT=$?
    echo "=== Chimera daemon DIED during test (exit code: $CHIMERA_EXIT) ==="
    CHIMERA_PID=""
fi

# Show chimera debug output if present
if [ -f "$CHIMERA_LOG" ]; then
    echo "=== Chimera stderr (last 100 lines) ==="
    tail -100 "$CHIMERA_LOG"
    cp "$CHIMERA_LOG" /tmp/chimera_stderr_last.log 2>/dev/null || true
fi

EXIT_CODE=$(grep -oP 'CHIMERA_KVM_EXIT_CODE=\K[0-9]+' "$LOG_FILE" | tail -1)
exit ${EXIT_CODE:-1}
