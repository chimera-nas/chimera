#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: kvm_smb_2client_test_wrapper.sh <vmlinuz> <rootfs.qcow2> \
#            <chimera_binary> <backend> <case>
#
# Boots TWO guests so cross-client SMB coherency can be tested with the real
# kernel cifs client.  Guest A drives the test and reaches guest B over ssh (a
# baked, passwordless key in the image, >= kvm-test-base v1.6.0); both mount the
# same chimera SMB share, so an operation B performs must be observed correctly
# by A through the server's lease/oplock/lock machinery.
#
# Topology, all inside one network namespace on the host:
#
#     guest A (primary, 10.0.0.2) --\
#                                     >-- br (10.0.0.1) -- chimera (SMB server)
#     guest B (secondary, 10.0.0.3) -/
#
# Unlike the nfstest 2-client harness this needs no hub/promiscuous-capture
# tricks: the cases assert on observable file state, not on a packet trace, so a
# plain learning bridge with the server IP on the bridge itself is enough.
#
# <case> selects the scenario:
#   cache_coherency  A reads (and caches under a read lease); B overwrites; A
#                    must see the new data -- exercises the lease/oplock break.
#   brl_conflict     A holds a POSIX byte-range lock (cifs -> SMB2 LOCK); B's
#                    conflicting non-blocking lock must be refused by the server.

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
BACKEND=$1; shift
CASE=$1; shift

case "$CASE" in
    cache_coherency|brl_conflict) ;;
    *) echo "invalid case '$CASE' (expected cache_coherency|brl_conflict)" >&2; exit 1 ;;
esac

QEMU_INITRD=""

NETNS_NAME="kvm_smb2c_$$_$(date +%s%N)"
TAP_A="tapA_$$"
TAP_B="tapB_$$"
BR_NAME="br_$$"
LOG_A=$(mktemp /tmp/kvm_smb2c_A_XXXXXX.log)
LOG_B=$(mktemp /tmp/kvm_smb2c_B_XXXXXX.log)
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/kvm_smb2c_session_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
CHIMERA_PID=""
QEMU_B_PID=""

cleanup() {
    [ -n "$QEMU_B_PID" ] && kill "$QEMU_B_PID" 2>/dev/null
    if [ -n "$CHIMERA_PID" ]; then
        kill "$CHIMERA_PID" 2>/dev/null
        for i in $(seq 1 150); do
            kill -0 "$CHIMERA_PID" 2>/dev/null || break
            sleep 0.02
        done
        kill -9 "$CHIMERA_PID" 2>/dev/null || true
        wait "$CHIMERA_PID" 2>/dev/null || true
    fi
    [ -n "$QEMU_B_PID" ] && wait "$QEMU_B_PID" 2>/dev/null
    echo "=== Guest A (primary) serial log ==="
    [ -f "$LOG_A" ] && cat "$LOG_A"
    echo "=== Guest B (secondary) serial log (tail) ==="
    [ -f "$LOG_B" ] && tail -40 "$LOG_B"
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -f "$LOG_A" "$LOG_B"
    rm -rf "$SESSION_DIR"
}
trap cleanup EXIT

# ----- chimera config --------------------------------------------------------
# Leases + oplocks on so the server actually grants caching leases to A and
# breaks them on B's write (the whole point of the coherency case).
generate_config() {
    local mount_path="/"
    local vfs_section=""

    case "$BACKEND" in
        linux|io_uring)
            mount_path="$SESSION_DIR/data"
            mkdir -p "$SESSION_DIR/data"
            ;;
        memfs) mount_path="/" ;;
        diskfs_io_uring|diskfs_aio)
            local device_type="io_uring"
            [ "$BACKEND" = "diskfs_aio" ] && device_type="libaio"
            local devices_json=""
            for i in $(seq 0 9); do
                local device_path="${SESSION_DIR}/device-${i}.img"
                truncate -s 1G "$device_path"
                [ "$i" -gt 0 ] && devices_json="${devices_json},"
                devices_json="${devices_json}{\"type\":\"$device_type\",\"size\":1,\"path\":\"$device_path\"}"
            done
            mount_path="/"
            BACKEND="diskfs"
            vfs_section="\"vfs\": { \"diskfs\": { \"config\": {\"initialize\":true,\"devices\":[$devices_json],\"unsafe_async\":true} } },"
            ;;
        cairn)
            mount_path="/"
            vfs_section="\"vfs\": { \"cairn\": { \"config\": {\"initialize\":true,\"path\":\"$SESSION_DIR\"} } },"
            ;;
    esac

    cat > "$CONFIG_FILE" << EOF
{
    "common": { "rcu_reclaim_threads": 4 },
    "server": {
        "threads": 4,
        "delegation_threads": 4,
        $vfs_section
        "smb_leases": true,
        "smb_oplocks": true,
        "smb_directory_leases": true,
        "external_portmap": false
    },
    "mounts": { "share": { "module": "$BACKEND", "path": "$mount_path" } },
    "shares": { "share": { "path": "/share" } },
    "users": [ { "username": "root", "smbpasswd": "secret", "uid": 0, "gid": 0 } ]
}
EOF
}
generate_config

ulimit -l unlimited
echo 16777216 > /proc/sys/fs/aio-max-nr 2>/dev/null || true

# ----- network: a plain learning bridge with the server IP on the bridge -----
ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up
ip netns exec "${NETNS_NAME}" ip link add "${BR_NAME}" type bridge
ip netns exec "${NETNS_NAME}" ip link set "${BR_NAME}" type bridge stp_state 0 forward_delay 0
ip netns exec "${NETNS_NAME}" ip addr add 10.0.0.1/24 dev "${BR_NAME}"
ip netns exec "${NETNS_NAME}" ip link set "${BR_NAME}" up
for t in "${TAP_A}" "${TAP_B}"; do
    ip netns exec "${NETNS_NAME}" ip tuntap add dev "$t" mode tap
    ip netns exec "${NETNS_NAME}" ip link set "$t" master "${BR_NAME}"
    ip netns exec "${NETNS_NAME}" ip link set "$t" up
done

# ----- chimera daemon --------------------------------------------------------
ip netns exec "${NETNS_NAME}" "$CHIMERA_BINARY" -c "$CONFIG_FILE" &
CHIMERA_PID=$!
for i in $(seq 1 150); do
    if ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/10.0.0.1/445" 2>/dev/null; then break; fi
    if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then echo "chimera exited early"; exit 1; fi
    sleep 0.02
done

# Mount options shared by both clients.  cache=strict makes the client lean on
# leases for coherency; nobrl is omitted so byte-range locks reach the server.
MNT_OPTS="username=root,password=secret,vers=3.1.1,modefromsid,cache=strict"
MOUNT_CMD="mount -t cifs //10.0.0.1/share /mnt -o ${MNT_OPTS}"

# Re-assert ssh perms at boot (the image's Docker->qcow2 conversion can leave
# /root/.ssh with an owner/mode ssh rejects).  No double quotes -- this is
# embedded in the kernel-cmdline test_cmd="..." the guest splits on the quote.
SSHFIX='mkdir -p /run/sshd; chown root:root /run/sshd; chmod 0755 /run/sshd; chown root:root /root; chmod 700 /root; chown -R root:root /root/.ssh /etc/ssh 2>/dev/null; chmod 700 /root/.ssh 2>/dev/null; chmod 600 /root/.ssh/id_ed25519 /root/.ssh/authorized_keys /root/.ssh/config 2>/dev/null; chmod 600 /etc/ssh/ssh_host_ed25519_key /etc/ssh/ssh_host_rsa_key /etc/ssh/ssh_host_ecdsa_key 2>/dev/null; /usr/sbin/sshd'

# A byte-range-lock helper, embedded via base64 so the kernel cmdline stays free
# of quotes.  "hold" takes an exclusive lock on the first 4 KiB and idles;
# "challenge" tries the same lock non-blocking and exits 0 iff it is refused.
read -r -d '' LOCK_HELPER <<'PY'
import sys, os, fcntl, time
mode = sys.argv[1]
fd = os.open('/mnt/f', os.O_RDWR | os.O_CREAT, 0o644)
if mode == 'hold':
    fcntl.lockf(fd, fcntl.LOCK_EX, 4096, 0, os.SEEK_SET)
    open('/tmp/held', 'w').close()
    time.sleep(60)
else:
    try:
        fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB, 4096, 0, os.SEEK_SET)
    except OSError:
        print('CONFLICT')
        sys.exit(0)
    print('NO CONFLICT')
    sys.exit(1)
PY
LOCK_B64=$(printf '%s' "$LOCK_HELPER" | base64 -w0)

# ----- per-case guest-A workload (mount already done by the prefix below) -----
# All commands are quote-free except single-quoted ssh payloads (no double
# quotes anywhere -- see SSHFIX).  Each ends with the assertion as its last
# command so its status becomes the guest's exit code.
case "$CASE" in
    cache_coherency)
        # A writes + caches, B overwrites, A must read back B's content.
        CASE_CMD="echo aaaaaaaa > /mnt/f && cat /mnt/f >/dev/null && ssh -o ConnectTimeout=5 10.0.0.3 '${MOUNT_CMD} && echo bbbbbbbb > /mnt/f && sync' && sleep 1 && grep -q bbbbbbbb /mnt/f"
        ;;
    brl_conflict)
        # A holds the lock; once held, B's non-blocking lock must be refused.
        # ( exit $RC ) yields the verdict without an explicit exit that could
        # terminate the guest init harness early.
        # B's non-blocking lock must be refused *immediately*; wrap it in a
        # timeout so a server that wrongly blocks the conflicting request fails
        # the test fast instead of deadlocking against A (which holds the lock
        # until B's ssh returns).
        B_CHALLENGE="${MOUNT_CMD} && echo ${LOCK_B64} | base64 -d > /tmp/h.py && timeout 20 python3 /tmp/h.py challenge"
        # Decode the helper foreground first so $! is the python holder's own pid
        # (kill it directly -- a backgrounded "&&" pipeline would hide it).  The
        # trailing ( exit $RC ) yields B's verdict without an explicit exit that
        # could terminate the guest init harness before it records the result.
        CASE_CMD="echo ${LOCK_B64} | base64 -d > /tmp/h.py; python3 /tmp/h.py hold & HOLDER=\$!; for i in \$(seq 1 100); do [ -f /tmp/held ] && break; sleep 0.1; done; ssh -o ConnectTimeout=5 10.0.0.3 '${B_CHALLENGE}'; RC=\$?; kill \$HOLDER 2>/dev/null; ( exit \$RC )"
        ;;
esac

# ----- guest B (secondary client): idle with sshd up -------------------------
ip netns exec "${NETNS_NAME}" "$QEMU_BIN" \
    -enable-kvm -smp 2 -m 1G -cpu host \
    -kernel "$VMLINUZ" $QEMU_INITRD $QEMU_MACHINE -nodefaults \
    -drive file="$ROOTFS",if=virtio,format=qcow2,snapshot=on \
    -netdev tap,id=net0,ifname="${TAP_B}",script=no,downscript=no \
    -device virtio-net-pci,netdev=net0,mac=52:54:00:00:00:03,romfile="" \
    -serial stdio -nographic -no-reboot \
    -append "root=/dev/vda rw console=${QEMU_CONSOLE} net.ifnames=0 biosdevname=0 quiet mitigations=off tsc=reliable panic=-1 guest_ip=10.0.0.3 start_sshd=1 test_cmd=\"${SSHFIX}; sleep 1800\" init=/init.sh" \
    > "$LOG_B" 2>&1 &
QEMU_B_PID=$!

# ----- guest A (primary): wait for B's sshd, mount, run the case -------------
TEST_CMD="${SSHFIX}; mkdir -p /mnt && for i in \$(seq 1 120); do ssh -o ConnectTimeout=2 10.0.0.3 true 2>/dev/null && break; sleep 1; done && ${MOUNT_CMD} && ${CASE_CMD}"

ip netns exec "${NETNS_NAME}" "$QEMU_BIN" \
    -enable-kvm -smp 4 -m 2G -cpu host \
    -kernel "$VMLINUZ" $QEMU_INITRD $QEMU_MACHINE -nodefaults \
    -drive file="$ROOTFS",if=virtio,format=qcow2,snapshot=on \
    -netdev tap,id=net0,ifname="${TAP_A}",script=no,downscript=no \
    -device virtio-net-pci,netdev=net0,mac=52:54:00:00:00:02,romfile="" \
    -serial stdio -nographic -no-reboot \
    -append "root=/dev/vda rw console=${QEMU_CONSOLE} net.ifnames=0 biosdevname=0 quiet mitigations=off tsc=reliable panic=-1 guest_ip=10.0.0.2 test_cmd=\"${TEST_CMD}\" init=/init.sh" \
    2>/dev/null | tee "$LOG_A"

EXIT_CODE=$(grep -oP 'CHIMERA_KVM_EXIT_CODE=\K[0-9]+' "$LOG_A" | tail -1)
exit ${EXIT_CODE:-1}
