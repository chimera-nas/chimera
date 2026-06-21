#!/bin/bash
# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
#
# Usage: kvm_nfs_kerberos_test_wrapper.sh <vmlinuz> <rootfs.qcow2> <chimera_binary> \
#                                         <backend> <nfsver> <post_mount_cmd>
#
# Boots a real Linux NFS client (QEMU/KVM) and mounts a chimera export with
# sec=krb5, proving the RPCSEC_GSS path against the kernel client + rpc.gssd.
#
#   1. Network namespace + TAP (host 10.0.0.1, guest 10.0.0.2).
#   2. An ephemeral MIT KDC in the netns (realm TEST.LOCAL, KDC on 10.0.0.1).
#   3. chimera NFS server with Kerberos enabled (keytab holds nfs/<server>).
#   4. A 9p share carrying the client's krb5.conf + machine keytab + hosts; the
#      guest (image >= v1.9.0, booted with krb5=1) copies it into place and
#      starts rpc.gssd.
#   5. The guest runs: mount -o sec=krb5 <server>:/share /mnt && <post_mount_cmd>
#
# Skips (exit 77) without /dev/kvm or the krb5 tooling.

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
NFS_VERSION=$1; shift
POST_MOUNT_CMD="$*"

REALM="TEST.LOCAL"
KDC_IP="10.0.0.1"
KDC_PORT="8899"
SERVER_HOST="nfsserver.test.local"
CLIENT_HOST="nfsclient.test.local"

TEST_ID="$$_$(date +%s%N | tail -c 6)"
NETNS_NAME="kvm_krb_${TEST_ID}"
TAP_NAME="tapk_$$"
KRB_DIR="/tmp/kvmkrb_${TEST_ID}"
KRB_SHARE="${KRB_DIR}/share"
SERVER_KEYTAB="${KRB_DIR}/server.keytab"
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/kvm_krb_session_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
LOG_FILE=$(mktemp /tmp/kvm_krb_XXXXXX.log)
CHIMERA_PID=""

log() { echo "[kvm_krb] $*" >&2; }

cleanup() {
    if [ -n "$CHIMERA_PID" ]; then
        kill "$CHIMERA_PID" 2>/dev/null || true
        for _ in $(seq 1 150); do kill -0 "$CHIMERA_PID" 2>/dev/null || break; sleep 0.02; done
        kill -9 "$CHIMERA_PID" 2>/dev/null || true
        wait "$CHIMERA_PID" 2>/dev/null || true
    fi
    if [ -f "$LOG_FILE" ]; then
        echo "=== Guest serial log ==="
        cat "$LOG_FILE"
    fi
    if ip netns list 2>/dev/null | grep -q "^${NETNS_NAME}"; then
        ip netns pids "${NETNS_NAME}" 2>/dev/null | xargs -r kill -9 2>/dev/null || true
        sleep 0.1
        ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    fi
    rm -f "$LOG_FILE"
    rm -rf "$KRB_DIR" "$SESSION_DIR"
}
trap cleanup EXIT

check_requirements() {
    if [ ! -e /dev/kvm ]; then
        log "SKIP: /dev/kvm not available"
        exit 77
    fi
    local cmd
    for cmd in kdb5_util krb5kdc kadmin.local; do
        command -v "$cmd" &>/dev/null || { log "SKIP: missing $cmd"; exit 77; }
    done
}

setup_kdc() {
    mkdir -p "${KRB_DIR}"/{etc,var/lib/krb5kdc} "${KRB_SHARE}"

    cat > "${KRB_DIR}/etc/kdc.conf" <<EOF
[kdcdefaults]
    kdc_ports = ${KDC_PORT}
    kdc_tcp_ports = ${KDC_PORT}

[realms]
    ${REALM} = {
        database_name = ${KRB_DIR}/var/lib/krb5kdc/principal
        admin_keytab = ${KRB_DIR}/var/lib/krb5kdc/kadm5.keytab
        acl_file = ${KRB_DIR}/var/lib/krb5kdc/kadm5.acl
        key_stash_file = ${KRB_DIR}/var/lib/krb5kdc/.k5.${REALM}
        kdc_ports = ${KDC_PORT}
        max_life = 24h
        max_renewable_life = 7d
    }
EOF

    # Shared by both the server (KRB5_CONFIG) and, via the 9p share, the client.
    cat > "${KRB_DIR}/etc/krb5.conf" <<EOF
[libdefaults]
    default_realm = ${REALM}
    dns_lookup_realm = false
    dns_lookup_kdc = false
    dns_canonicalize_hostname = false
    rdns = false
    ticket_lifetime = 24h
    forwardable = true

[realms]
    ${REALM} = {
        kdc = ${KDC_IP}:${KDC_PORT}
        admin_server = ${KDC_IP}:${KDC_PORT}
    }

[domain_realm]
    .test.local = ${REALM}
    test.local = ${REALM}
EOF

    echo "*/admin@${REALM} *" > "${KRB_DIR}/var/lib/krb5kdc/kadm5.acl"

    export KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf"
    export KRB5_KDC_PROFILE="${KRB_DIR}/etc/kdc.conf"

    kdb5_util create -s -r "${REALM}" -P "$(head -c 32 /dev/urandom | base64)" 2>/dev/null

    # nfs/<server> -> chimera's acceptor keytab; host/<client> -> the client's
    # machine keytab that rpc.gssd uses for the mount.
    kadmin.local -q "addprinc -randkey nfs/${SERVER_HOST}@${REALM}" 2>/dev/null
    kadmin.local -q "addprinc -randkey host/${CLIENT_HOST}@${REALM}" 2>/dev/null

    kadmin.local -q "ktadd -k ${SERVER_KEYTAB} nfs/${SERVER_HOST}@${REALM}" 2>/dev/null
    kadmin.local -q "ktadd -k ${KRB_SHARE}/krb5.keytab host/${CLIENT_HOST}@${REALM}" 2>/dev/null

    # Material handed to the guest over 9p.
    cp "${KRB_DIR}/etc/krb5.conf" "${KRB_SHARE}/krb5.conf"
    cat > "${KRB_SHARE}/hosts" <<EOF
10.0.0.1 ${SERVER_HOST}
10.0.0.2 ${CLIENT_HOST}
EOF
    chmod -R a+rX "${KRB_SHARE}"
}

generate_config() {
    local mount_path="/" vfs_section=""
    case "$BACKEND" in
        linux | io_uring) mount_path="$SESSION_DIR/data"; mkdir -p "$SESSION_DIR/data" ;;
        memfs) mount_path="/" ;;
        cairn)
            vfs_section="\"vfs\": { \"cairn\": { \"config\": {\"initialize\":true,\"path\":\"$SESSION_DIR\"} } },"
            ;;
        *) log "Unsupported backend: $BACKEND"; exit 1 ;;
    esac

    cat > "$CONFIG_FILE" <<EOF
{
    "common": { "rcu_reclaim_threads": 4 },
    "server": {
        "threads": 4,
        "delegation_threads": 4,
        ${vfs_section}
        "nfs_auth": { "kerberos_enabled": true, "kerberos_keytab": "${SERVER_KEYTAB}" },
        "external_portmap": false
    },
    "mounts": { "share": { "module": "$BACKEND", "path": "$mount_path" } },
    "exports": { "/share": { "path": "/share" } }
}
EOF
}

check_requirements
setup_kdc
generate_config

ulimit -l unlimited
echo 16777216 > /proc/sys/fs/aio-max-nr 2>/dev/null || true

# netns + TAP
ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up
ip netns exec "${NETNS_NAME}" ip tuntap add dev "${TAP_NAME}" mode tap
ip netns exec "${NETNS_NAME}" ip addr add 10.0.0.1/24 dev "${TAP_NAME}"
ip netns exec "${NETNS_NAME}" ip link set "${TAP_NAME}" up

if [ -n "${KVM_KRB_PCAP:-}" ]; then
    ip netns exec "${NETNS_NAME}" tcpdump -U -i "${TAP_NAME}" -w "${KVM_KRB_PCAP}" -s 0 port 2049 &
    sleep 0.3
fi

# KDC (binds 10.0.0.1 inside the netns; the guest reaches it over the TAP)
ip netns exec "${NETNS_NAME}" env \
    KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf" KRB5_KDC_PROFILE="${KRB_DIR}/etc/kdc.conf" \
    krb5kdc -n -P "${KRB_DIR}/kdc.pid" &
for _ in $(seq 1 60); do
    [ -f "${KRB_DIR}/kdc.pid" ] && kill -0 "$(cat "${KRB_DIR}/kdc.pid" 2>/dev/null)" 2>/dev/null && break
    sleep 0.5
done

# chimera NFS server (Kerberos acceptor via SERVER_KEYTAB)
ip netns exec "${NETNS_NAME}" env \
    KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf" KRB5_KTNAME="${SERVER_KEYTAB}" \
    "$CHIMERA_BINARY" -c "$CONFIG_FILE" &
CHIMERA_PID=$!
for _ in $(seq 1 250); do
    ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/10.0.0.1/2049" 2>/dev/null && break
    kill -0 "$CHIMERA_PID" 2>/dev/null || { log "chimera exited prematurely"; exit 1; }
    sleep 0.02
done

# Guest test command: bring up the GSS client, krb5 mount, caller checks.
# rpc.gssd backs the kernel's sec=krb5 upcall over rpc_pipefs; this nfs-utils
# build looks for it at /run/rpc_pipefs.  (init.sh copies the krb5.conf/keytab/
# hosts material from the 9p share; we start gssd here so the mount path is
# explicit and correct regardless of the guest's nfs-utils pipefs default.)
# Mount security flavor: krb5 (auth), krb5i (integrity) or krb5p (privacy).
# Defaults to krb5; a test sets MOUNT_SEC to exercise the integrity/privacy
# wrapping on the data path.
NFS_MOUNT_OPTS="vers=${NFS_VERSION},sec=${MOUNT_SEC:-krb5},tcp"
# Self-contained krb5 client bring-up (does not rely on init.sh's single-shot
# attempt): set the hostname, then deliver the per-test realm material from the
# 9p share with a retry loop -- under ctest the 9p backend can race the guest's
# probe and a one-shot mount silently misses, leaving rpc.gssd without a keytab.
# Then mount rpc_pipefs (/run is this nfs-utils' default) and (re)start rpc.gssd.
KRB_PREP="hostname ${CLIENT_HOST} 2>/dev/null; modprobe 9pnet_virtio 2>/dev/null; modprobe 9p 2>/dev/null; for i in 1 2 3 4 5 6 7 8; do [ -s /etc/krb5.keytab ] && break; mkdir -p /mnt/krb; if mount -t 9p -o trans=virtio,version=9p2000.L krbshare /mnt/krb 2>/dev/null; then cp /mnt/krb/krb5.conf /etc/krb5.conf; cp /mnt/krb/krb5.keytab /etc/krb5.keytab; cat /mnt/krb/hosts >> /etc/hosts; umount /mnt/krb; else sleep 1; fi; done; mkdir -p /run/rpc_pipefs; mountpoint -q /run/rpc_pipefs || mount -t rpc_pipefs rpc_pipefs /run/rpc_pipefs; pkill -x rpc.gssd 2>/dev/null; /usr/sbin/rpc.gssd; sleep 1;"
# NFSv3's kernel mount path wants rpc.statd (NSM) for NLM locking and aborts the
# mount (rc=32) if it cannot reach it; rpc.statd is not in the test image, so we
# mount v3 with nolock (the krb5 data path -- MOUNT + portmap + NFS READ/WRITE
# under RPCSEC_GSS -- is what we are validating; NLM-under-krb5 needs statd in
# the guest image and is a separate exercise).  NFSv4 needs none of this.
NFS_NOLOCK=""
case "${NFS_VERSION}" in
    3*) NFS_NOLOCK=",nolock" ;;
esac
NFS_MOUNT_OPTS="${NFS_MOUNT_OPTS}${NFS_NOLOCK}"
if [ -n "${KVM_KRB_DEBUG:-}" ]; then
    TEST_CMD="${KRB_PREP} echo '=== keytab ==='; ls -l /etc/krb5.keytab; klist -k /etc/krb5.keytab 2>&1; echo '=== KDC reach ==='; (echo > /dev/tcp/10.0.0.1/${KDC_PORT}) 2>&1 && echo kdc-tcp-ok || echo kdc-tcp-FAIL; echo '=== kinit -k ==='; timeout 10 kinit -k -t /etc/krb5.keytab host/${CLIENT_HOST}@${REALM} 2>&1; echo kinit_rc=\$?; klist 2>&1; echo '=== krb5 mount -v ==='; timeout 25 mount -v -t nfs -o ${NFS_MOUNT_OPTS} ${SERVER_HOST}:/share /mnt 2>&1; echo krb5_rc=\$?"
else
    TEST_CMD="${KRB_PREP} mount -t nfs -o ${NFS_MOUNT_OPTS} ${SERVER_HOST}:/share /mnt && ${POST_MOUNT_CMD}"
fi

# Boot the guest with the 9p krb material + krb5=1 trigger.
ip netns exec "${NETNS_NAME}" "$QEMU_BIN" \
    -enable-kvm -smp 4 -m 1G -cpu host \
    -kernel "$VMLINUZ" \
    $QEMU_MACHINE \
    -nodefaults \
    -drive file="$ROOTFS",if=virtio,format=qcow2,snapshot=on \
    -netdev tap,id=net0,ifname="${TAP_NAME}",script=no,downscript=no \
    -device virtio-net-pci,netdev=net0,romfile="" \
    -fsdev local,id=krbfs,path="${KRB_SHARE}",security_model=none \
    -device virtio-9p-pci,fsdev=krbfs,mount_tag=krbshare \
    -serial stdio \
    -nographic \
    -no-reboot \
    -append "root=/dev/vda rw console=${QEMU_CONSOLE} net.ifnames=0 biosdevname=0 quiet mitigations=off tsc=reliable panic=-1 krb5=1 guest_host=${CLIENT_HOST} test_cmd=\"${TEST_CMD}\" init=/init.sh" \
    > "$LOG_FILE" 2>&1
# NB: capture the guest serial to a file rather than `tee`-ing it to our own
# stdout.  The guest's in-image watchdog dumps diagnostics every 5s once a test
# runs past 10s (a krb5 mount + rpc.gssd setup can), and under `ctest` that flood
# would backpressure the captured-stdout pipe, block the guest's console writes,
# and wedge the VM.  Writing to a file never blocks; we print it below.

if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
    wait "$CHIMERA_PID" 2>/dev/null
    echo "=== Chimera daemon DIED during test (exit $?) ==="
    CHIMERA_PID=""
fi

EXIT_CODE=$(grep -oP 'CHIMERA_KVM_EXIT_CODE=\K[0-9]+' "$LOG_FILE" | tail -1)
exit "${EXIT_CODE:-1}"
