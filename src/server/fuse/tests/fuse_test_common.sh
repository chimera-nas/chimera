# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
#
# Shared plumbing for FUSE end-to-end tests: start a chimera daemon serving
# one backend with a FUSE mountpoint inside the session directory, and tear
# everything down (lazy-unmounting BEFORE killing, so cleanup never hangs on
# a dead mountpoint).  Sourced by fuse_mount_test.sh and fuse_posix_test.sh.

set -u

SESSION=""
CFG=""
LOG=""
MNT=""
PID=""

fuse_test_cleanup() {
    [ -n "$MNT" ] && umount -l "$MNT" 2> /dev/null
    [ -n "$PID" ] && kill -9 "$PID" 2> /dev/null
    [ -n "$SESSION" ] && rm -rf "$SESSION"
}
trap fuse_test_cleanup EXIT

fail() {
    echo "FAIL: $1"
    echo "--- daemon log ---"
    [ -n "$LOG" ] && cat "$LOG"
    exit 1
}

# fuse_test_start <chimera-binary> <backend-module> [backend-path] [mount-options]
fuse_test_start() {
    local bin=$1
    local module=$2
    local backend_path=${3:-/}
    local options=${4:-}
    local options_json=""
    local filesystems_json=""

    [ -n "$options" ] && options_json=", \"options\": \"$options\""

    # The named-filesystem backends come up empty and must have a filesystem
    # created before anything can mount it; linux and io_uring keep host paths.
    case "$module" in
        linux | io_uring) ;;
        *)
            filesystems_json="\"filesystems\": { \"fs0\": { \"module\": \"$module\" } },"
            backend_path="fs0"
            ;;
    esac

    SESSION=$(mktemp -d)
    CFG="$SESSION/config.json"
    LOG="$SESSION/daemon.log"
    MNT="$SESSION/mnt"

    mkdir -p "$MNT"

    if [ "$module" = "linux" ]; then
        backend_path="$SESSION/backing"
        mkdir -p "$backend_path"
    fi

    cat > "$CFG" <<EOF
{
    "server": {
        "fuse_enabled": true,
        "threads": 2,
        "metrics_port": 0
    },
    $filesystems_json
    "mounts": {
        "data": { "module": "$module", "path": "$backend_path" }
    },
    "fuse_mounts": {
        "$MNT": { "path": "/data"$options_json }
    }
}
EOF

    "$bin" -c "$CFG" > "$LOG" 2>&1 &
    PID=$!

    for _ in $(seq 1 100); do
        grep -q "Server is ready" "$LOG" 2> /dev/null && break
        kill -0 "$PID" 2> /dev/null || break
        sleep 0.1
    done

    grep -q "Server is ready" "$LOG" || fail "daemon did not start"
    grep -q "fuse mount $MNT" "$LOG" || fail "daemon did not establish the FUSE mount"

    mountpoint -q "$MNT" || fail "$MNT is not a mountpoint"
}

fuse_test_stop() {
    umount "$MNT" || fail "clean unmount of $MNT failed"
    MNT=""

    kill -TERM "$PID" 2> /dev/null

    for _ in $(seq 1 100); do
        kill -0 "$PID" 2> /dev/null || break
        sleep 0.1
    done

    kill -0 "$PID" 2> /dev/null && fail "daemon did not shut down after SIGTERM"
    PID=""
}
