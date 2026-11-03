#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: kvm_nfs3_nlm_reclaim_reboot_test_wrapper.sh <vmlinuz> <rootfs.qcow2> <chimera_binary>
#
# NFSv3 lock reclaim across a real chimera server restart, backed by a
# persistent cairn store (initialize=true on first boot, false on restart) with
# server.kv_module=cairn and server.nfs_nsm_name set to the address the guest
# mounts (10.0.0.1), so SM_NOTIFY carries the name the guest's statd monitors:
#
#   1. Start chimera A.  One guest brings up rpcbind + rpc.statd, mounts NFSv3
#      *with* locking and takes an exclusive POSIX lock on byte 0 of /mnt/rl.
#      chimera A persists a monitor record for the guest in the cairn store.
#   2. The guest stays up and keeps holding the lock while chimera A is
#      stopped and restarted (chimera B) against the same store.  chimera B
#      reloads the record, enters the NLM grace window and notifies the guest;
#      the guest's statd accepts the notify, its lockd reclaims byte 0, and
#      once every notified host answered chimera B shortens the grace to 10 s.
#   3. 25 s after the first lock a child process (a second lock owner) asks
#      for byte 0 without waiting, through a second, nosharecache mount of the
#      same export.  That NLM LOCK goes to the server, which must refuse it:
#      the conflict is the proof that the server granted the reclaim.  The
#      child then takes a NEW lock on byte 1.  Neither is a
#      reclaim, so the first one waits out the grace; from that first request
#      the byte-1 lock must be granted in under 45 s.  The full 90 s window
#      would make it wait over a minute (the Linux client retries a
#      DENIED_GRACE_PERIOD every 5 s).
#   4. chimera C runs against the same store with nfs3_nsm_persist=false: it
#      must log the switch and enter neither the NLM grace nor the NSM cold
#      start (no guest).
#
# RECLAIM_STOP_SIGNAL (TERM or KILL, default TERM) is the signal that stops
# chimera A.  TERM is the graceful shutdown, which closes every client
# connection before the process exits and must still leave the monitor record
# in the store; KILL is a crash.  Both run the same assertions.
#
# Passing proves: persisted NSM monitor records drive a real reclaim after a
# restart -- the client accepts the notify name, the server grants its
# reclaim during grace, and fresh locks are not stalled for the full grace
# window -- and nfs3_nsm_persist=false turns all of that off on a persistent
# store.

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

# Upper bound on a single guest, enforced by a process that is not this shell
# (see kvm_nfs3_drc_reboot_test_wrapper.sh): a backstop, not a test budget;
# ctest's own per-test TIMEOUT is what should normally fire first.
KVM_QEMU_DEADLINE="${KVM_QEMU_DEADLINE:-2400}"

VMLINUZ=$1; shift
ROOTFS=$1; shift
CHIMERA_BINARY=$1; shift

NETNS_NAME="kvm_nlmreclaim_$$_$(date +%s%N)"
TAP_NAME="tapl_$$"
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/kvm_nlmreclaim_session_XXXXXX")
CAIRN_DIR="${SESSION_DIR}/cairn"
# Persisted NFS state (the file-handle signing key among it) must survive the
# restart, or the reclaimed lock's file handle is stale; keep it in the
# session instead of relying on the install-prefix default.
STATE_DIR="${SESSION_DIR}/state"
CONFIG_FILE="${SESSION_DIR}/chimera.json"
CHIMERA_LOG="${SESSION_DIR}/chimera.log"
GUEST_LOG="${SESSION_DIR}/guest.log"
CHIMERA_PID=""
GUEST_PID=""
mkdir -p "$CAIRN_DIR" "$STATE_DIR"

RECLAIM_STOP_SIGNAL="${RECLAIM_STOP_SIGNAL:-TERM}"
case "$RECLAIM_STOP_SIGNAL" in
    TERM|KILL) ;;
    *)
        echo "FAIL: RECLAIM_STOP_SIGNAL must be TERM or KILL, not '${RECLAIM_STOP_SIGNAL}'"
        exit 1
        ;;
esac

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
    # Reclaim the guest.  It runs in the background across the restart, so
    # every early exit below leaves it running: walk this shell's own
    # descendants (timeout(1), then qemu under it) and signal all of them.  Scoped to this shell's tree, so a sibling test's
    # guest is never touched -- these run under ctest -j.
    #
    # Collect the whole tree before killing any of it: killing a parent first
    # reparents its children away and loses them.
    kvm_tree=""
    kvm_frontier="$(pgrep -P $$ 2>/dev/null)"
    while [ -n "${kvm_frontier}" ]; do
        kvm_tree="${kvm_tree} ${kvm_frontier}"
        kvm_next=""
        for kvm_p in ${kvm_frontier}; do
            kvm_next="${kvm_next} $(pgrep -P "${kvm_p}" 2>/dev/null)"
        done
        kvm_frontier="${kvm_next}"
    done
    for kvm_p in ${kvm_tree}; do
        kill -TERM "${kvm_p}" 2>/dev/null || true
    done
}
trap cleanup EXIT

# Generate the chimera config.  $1 = initialize flag (true/false).  $2 =
# optional extra "server" keys, each followed by a comma.
generate_config() {
    local init="$1"
    local extra="${2:-}"
    cat > "$CONFIG_FILE" << EOF
{
    "common": {
        "rcu_reclaim_threads": 4
    },
    "server": {
        "nfs_enabled": true,
        "threads": 4,
        "delegation_threads": 4,
        "kv_module": "cairn",
        "nfs_nsm_name": "10.0.0.1",
        "state_dir": "$STATE_DIR",
        ${extra}
        "vfs": {
            "cairn": {
                "config": {"initialize":${init},"path":"$CAIRN_DIR"}
            }
        },
        "external_portmap": false
    },
    "filesystems": {
        "fs0": {
            "module": "cairn"
        }
    },
    "mounts": {
        "share": {
            "module": "cairn",
            "path": "fs0"
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

# Start chimera with a fresh per-boot log, so CHIMERA_LOG always reflects the
# current daemon.  Extra arguments are passed to the daemon.
BOOT_N=0
start_chimera() {
    BOOT_N=$((BOOT_N + 1))
    CHIMERA_LOG="${SESSION_DIR}/chimera_${BOOT_N}.log"
    ip netns exec "${NETNS_NAME}" "$CHIMERA_BINARY" -c "$CONFIG_FILE" "$@" \
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
    cat "$CHIMERA_LOG"
    return 1
}

# Stop chimera with signal $1 (default TERM); one still up 4 s later is
# killed.
stop_chimera() {
    local sig="${1:-TERM}"
    kill -s "$sig" "$CHIMERA_PID" 2>/dev/null || true
    for i in $(seq 1 200); do
        kill -0 "$CHIMERA_PID" 2>/dev/null || break
        sleep 0.02
    done
    if kill -0 "$CHIMERA_PID" 2>/dev/null; then
        echo "note: chimera still running 4 s after SIG${sig}; killing it"
        kill -9 "$CHIMERA_PID" 2>/dev/null || true
    fi
    wait "$CHIMERA_PID" 2>/dev/null || true
    CHIMERA_PID=""
}

# NLM locking over NFSv3 needs the guest kernel client's NSM (rpc.statd) and a
# registered rpcbind; bring them up before mounting *with* locking (the same
# bring-up kvm_nfs_test_wrapper.sh uses for NFS_ENABLE_NLM).
NLM_PREP="mkdir -p /run/rpcbind /var/lib/nfs/sm /var/lib/nfs/sm.bak /var/lib/nfs/statd; modprobe lockd 2>/dev/null; /usr/sbin/rpcbind 2>/dev/null; sleep 0.3; /usr/sbin/rpc.statd 2>/dev/null; sleep 0.5;"

# Boot a guest in the background that runs $1 on an NFSv3 mount with locking.
# $2 = the serial log file.  Sets GUEST_PID.  The caller polls the log while
# the guest runs and reads its exit code from the log (guest_rc) once it has
# exited.
#
# Backgrounded here, not by the caller (run_guest ... &): ip netns exec execs
# timeout(1), so GUEST_PID is timeout itself, a direct child of this shell, and
# the cleanup walk reaches qemu through it.  A backgrounded function would put
# a bash subshell in between, and if that subshell died first qemu would be
# reparented out of this shell's tree and leak.
run_guest() {
    local guest_cmd="$1"
    local log="$2"
    local mount_opts="vers=3,tcp,nconnect=16"
    local full="${NLM_PREP} mount -t nfs -o ${mount_opts} 10.0.0.1:/share /mnt && ${guest_cmd}"

    ip netns exec "${NETNS_NAME}" timeout --foreground --kill-after=10s "${KVM_QEMU_DEADLINE}" "$QEMU_BIN" \
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
        > "$log" 2>/dev/null &
    GUEST_PID=$!
}

# The guest's exit code, parsed from its serial log $1.
guest_rc() {
    grep -oaP 'CHIMERA_KVM_EXIT_CODE=\K[0-9]+' "$1" | tail -1
}

dump_logs() {
    local f
    for f in "$@"; do
        echo "=== $(basename "$f") ==="
        cat "$f" 2>/dev/null
    done
}

# Guest program, base64-embedded so the kernel cmdline that carries the test
# command stays quote-free (generated with base64 -w0).  Decoded:
#
#   import os,sys,time,fcntl,signal,errno
#   P='/mnt/rl'
#   Q='/mnt2/rl'
#   h=open(P,'w')
#   fcntl.lockf(h,fcntl.LOCK_EX,1,0)
#   print('NLM_HOLDER_LOCKED',flush=True)
#   time.sleep(25)
#   pid=os.fork()
#   if pid==0:
#       signal.alarm(60)
#       try:
#           f=open(Q,'a')
#       except OSError as e:
#           print('NLM_NEW_LOCK_ERROR',e,flush=True)
#           os._exit(1)
#       t=time.monotonic()
#       try:
#           fcntl.lockf(f,fcntl.LOCK_EX|fcntl.LOCK_NB,1,0)
#           print('NLM_RECLAIM_LOST granted',flush=True)
#           os._exit(1)
#       except OSError as e:
#           if e.errno not in (errno.EAGAIN,errno.EACCES):
#               print('NLM_RECLAIM_LOST',e,flush=True)
#               os._exit(1)
#       print('NLM_RECLAIM_HELD',flush=True)
#       try:
#           fcntl.lockf(f,fcntl.LOCK_EX,1,1)
#           w=time.monotonic()-t
#       except OSError as e:
#           print('NLM_NEW_LOCK_ERROR',e,flush=True)
#           os._exit(1)
#       print('NLM_NEW_LOCK_WAIT %.1f'%w,flush=True)
#       os._exit(0 if w<45 else 1)
#   s=os.waitpid(pid,0)[1]
#   sys.exit(os.WEXITSTATUS(s) if os.WIFEXITED(s) else 1)
#
# The parent holds byte 0 across the restart (the lock its lockd reclaims).
# The child is a separate lock owner.  Its non-blocking byte-0 request
# (F_SETLK) must reach the server, which must answer DENIED because it
# granted the parent's reclaim: EAGAIN or EACCES.  The Linux client checks
# its own lock table first and would refuse the request locally if the child
# used the parent's inode, whatever the server holds; /mnt2 is a nosharecache
# mount of the same export, so the child's inode is a different one with no
# local locks, and the answer is the server's.  A server that lost the
# reclaim grants the request instead and the child fails (NLM_RECLAIM_LOST).
# Its byte-1 lock is a fresh, non-reclaim NLM LOCK.  The first of the two
# requests waits out the grace window, so the wait is timed from it.  The
# alarm turns a lock still blocked after 60 s into a SIGALRM kill, which fails
# the test.
NLM_RECLAIM_PY="aW1wb3J0IG9zLHN5cyx0aW1lLGZjbnRsLHNpZ25hbCxlcnJubwpQPScvbW50L3JsJwpRPScvbW50Mi9ybCcKaD1vcGVuKFAsJ3cnKQpmY250bC5sb2NrZihoLGZjbnRsLkxPQ0tfRVgsMSwwKQpwcmludCgnTkxNX0hPTERFUl9MT0NLRUQnLGZsdXNoPVRydWUpCnRpbWUuc2xlZXAoMjUpCnBpZD1vcy5mb3JrKCkKaWYgcGlkPT0wOgogICAgc2lnbmFsLmFsYXJtKDYwKQogICAgdHJ5OgogICAgICAgIGY9b3BlbihRLCdhJykKICAgIGV4Y2VwdCBPU0Vycm9yIGFzIGU6CiAgICAgICAgcHJpbnQoJ05MTV9ORVdfTE9DS19FUlJPUicsZSxmbHVzaD1UcnVlKQogICAgICAgIG9zLl9leGl0KDEpCiAgICB0PXRpbWUubW9ub3RvbmljKCkKICAgIHRyeToKICAgICAgICBmY250bC5sb2NrZihmLGZjbnRsLkxPQ0tfRVh8ZmNudGwuTE9DS19OQiwxLDApCiAgICAgICAgcHJpbnQoJ05MTV9SRUNMQUlNX0xPU1QgZ3JhbnRlZCcsZmx1c2g9VHJ1ZSkKICAgICAgICBvcy5fZXhpdCgxKQogICAgZXhjZXB0IE9TRXJyb3IgYXMgZToKICAgICAgICBpZiBlLmVycm5vIG5vdCBpbiAoZXJybm8uRUFHQUlOLGVycm5vLkVBQ0NFUyk6CiAgICAgICAgICAgIHByaW50KCdOTE1fUkVDTEFJTV9MT1NUJyxlLGZsdXNoPVRydWUpCiAgICAgICAgICAgIG9zLl9leGl0KDEpCiAgICBwcmludCgnTkxNX1JFQ0xBSU1fSEVMRCcsZmx1c2g9VHJ1ZSkKICAgIHRyeToKICAgICAgICBmY250bC5sb2NrZihmLGZjbnRsLkxPQ0tfRVgsMSwxKQogICAgICAgIHc9dGltZS5tb25vdG9uaWMoKS10CiAgICBleGNlcHQgT1NFcnJvciBhcyBlOgogICAgICAgIHByaW50KCdOTE1fTkVXX0xPQ0tfRVJST1InLGUsZmx1c2g9VHJ1ZSkKICAgICAgICBvcy5fZXhpdCgxKQogICAgcHJpbnQoJ05MTV9ORVdfTE9DS19XQUlUICUuMWYnJXcsZmx1c2g9VHJ1ZSkKICAgIG9zLl9leGl0KDAgaWYgdzw0NSBlbHNlIDEpCnM9b3Mud2FpdHBpZChwaWQsMClbMV0Kc3lzLmV4aXQob3MuV0VYSVRTVEFUVVMocykgaWYgb3MuV0lGRVhJVEVEKHMpIGVsc2UgMSkK"
GUEST_CMD="mkdir -p /mnt2 && mount -t nfs -o vers=3,tcp,nosharecache 10.0.0.1:/share /mnt2 && echo ${NLM_RECLAIM_PY} | base64 -d > /tmp/rl.py && python3 /tmp/rl.py"

# --- network namespace + TAP ---
ulimit -l unlimited
ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up
ip netns exec "${NETNS_NAME}" ip tuntap add dev "${TAP_NAME}" mode tap
ip netns exec "${NETNS_NAME}" ip addr add 10.0.0.1/24 dev "${TAP_NAME}"
ip netns exec "${NETNS_NAME}" ip link set "${TAP_NAME}" up

# --- boot 1: the guest takes its lock and stays up across the restart ---
generate_config "true"
start_chimera || exit 1
run_guest "${GUEST_CMD}" "${GUEST_LOG}"

# Bounded wait (90 s: guest boot, statd bring-up, mount) for the lock.  Stop
# early if the guest exits without taking it.
for i in $(seq 1 180); do
    grep -qa "NLM_HOLDER_LOCKED" "$GUEST_LOG" 2>/dev/null && break
    kill -0 "$GUEST_PID" 2>/dev/null || break
    sleep 0.5
done
if ! grep -qa "NLM_HOLDER_LOCKED" "$GUEST_LOG" 2>/dev/null; then
    if kill -0 "$GUEST_PID" 2>/dev/null; then
        echo "FAIL: guest did not report holding its lock within 90 s"
    else
        RC=$(guest_rc "$GUEST_LOG")
        echo "FAIL: guest exited (code ${RC:-<none>}) without taking its lock"
    fi
    dump_logs "$CHIMERA_LOG" "$GUEST_LOG"
    exit 1
fi
echo "=== boot 1: guest holds its lock; restarting chimera ==="

# Let the monitor record's KV put and the cairn commit land before the stop.
sleep 2
echo "=== stopping chimera with SIG${RECLAIM_STOP_SIGNAL} ==="
stop_chimera "$RECLAIM_STOP_SIGNAL"

# --- boot 2: same cairn store, initialize=false; the guest is still up ---
generate_config "false"
start_chimera || exit 1
RESTART_LOG="$CHIMERA_LOG"

# Bounded wait for the guest to finish: what is left of its 25 s sleep plus at
# most 60 s for the new lock, plus shutdown.
for i in $(seq 1 240); do
    kill -0 "$GUEST_PID" 2>/dev/null || break
    sleep 0.5
done
if kill -0 "$GUEST_PID" 2>/dev/null; then
    echo "FAIL: guest still running 120 s after the restart"
    dump_logs "$RESTART_LOG" "$GUEST_LOG"
    exit 1
fi
wait "$GUEST_PID" 2>/dev/null
GUEST_PID=""
RC=$(guest_rc "$GUEST_LOG")
WAIT_S=$(grep -oaP 'NLM_NEW_LOCK_WAIT \K[0-9.]+' "$GUEST_LOG" | tail -1)
echo "=== guest exit: ${RC:-<none>}; new lock waited ${WAIT_S:-<none>} s ==="

# --- boot 2 assertions ---
FAIL=0

# $1 = log, $2 = fixed substring it must contain, $3 = what that shows.
expect_line() {
    if ! grep -qaF -- "$2" "$1"; then
        echo "FAIL: no '$2' in $(basename "$1") ($3)"
        FAIL=1
    fi
}

if [ "${RC:-1}" != "0" ]; then
    echo "FAIL: guest exit ${RC:-<none>}: the server did not hold the reclaimed lock, or the new lock was not granted within 45 s (or the guest failed)"
    FAIL=1
fi
expect_line "$GUEST_LOG" "NLM_RECLAIM_HELD" \
    "the server did not refuse a second owner the reclaimed byte"
if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
    echo "FAIL: the restarted chimera exited while the guest reclaimed"
    FAIL=1
fi
# Also the positive control for boot 3, which must not log it.
expect_line "$RESTART_LOG" "NLM entering" \
    "the restarted daemon did not open the NLM grace window"
expect_line "$RESTART_LOG" "NSM cold-start: 1 monitored host(s)" \
    "the guest's monitor record was not reloaded"
expect_line "$RESTART_LOG" "NSM notify: told host" \
    "SM_NOTIFY did not reach the guest's statd"
# statd answers SM_NOTIFY whatever the name; only a reclaim shows it matched
# nfs_nsm_name against the host it monitors.
expect_line "$RESTART_LOG" "reclaim from" \
    "the guest did not reclaim its lock during grace"
expect_line "$RESTART_LOG" "all 1 monitored host(s) answered; NLM grace ends in" \
    "the grace window was not shortened once the guest answered"
if grep -qai "non-persistent" "$RESTART_LOG"; then
    echo "FAIL: KV backend reported non-persistent (expected cairn)"
    FAIL=1
fi

if [ "$FAIL" != "0" ]; then
    dump_logs "$RESTART_LOG" "$GUEST_LOG"
    exit 1
fi

# --- boot 3: same store, nfs3_nsm_persist=false (no guest) ---
stop_chimera
generate_config "false" '"nfs3_nsm_persist": false,'
# -d: the NSM state line that names the switch is a debug line.
start_chimera -d || exit 1
SWITCH_LOG="$CHIMERA_LOG"

# The negative checks below mean something only once the daemon finished
# starting; then give the NSM cold start, which begins on the first lease tick
# (1 s) after that, the time boot 2's cold start needed to log.
READY=0
for i in $(seq 1 100); do
    if grep -qaF "Server is ready." "$SWITCH_LOG"; then
        READY=1
        break
    fi
    kill -0 "$CHIMERA_PID" 2>/dev/null || break
    sleep 0.1
done
sleep 2

if [ "$READY" != "1" ]; then
    echo "FAIL: chimera with nfs3_nsm_persist=false never logged 'Server is ready.'"
    FAIL=1
fi
if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
    echo "FAIL: chimera with nfs3_nsm_persist=false exited"
    FAIL=1
fi
expect_line "$SWITCH_LOG" "persistence=disabled (nfs3_nsm_persist=false)" \
    "the switch did not disable NSM persistence"
for unwanted in "NLM entering" "NSM cold-start"; do
    if grep -qaF -- "$unwanted" "$SWITCH_LOG"; then
        echo "FAIL: '$unwanted' logged with nfs3_nsm_persist=false"
        FAIL=1
    fi
done
stop_chimera

if [ "$FAIL" != "0" ]; then
    dump_logs "$SWITCH_LOG"
    exit 1
fi

echo "PASS: NFSv3 lock reclaimed across a SIG${RECLAIM_STOP_SIGNAL} restart (notify accepted, reclaim held, grace shortened); nfs3_nsm_persist=false disables it"
exit 0
