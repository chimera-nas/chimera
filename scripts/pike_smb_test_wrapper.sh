#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: pike_smb_test_wrapper.sh <chimera_binary> <backend> <test_list>
#
#   <test_list>  comma-separated pytest node-ids (relative to pike's test
#                directory), e.g.
#                  negotiate.py::NegotiateTest::test_negotiate_smb2_002,lease.py::LeaseTest::test_lease_break
#                Run in ONE pytest invocation against a single chimera server
#                (daemon + netns bringup dominates, so batching is far faster).
#                Empty test_list runs the WHOLE pike test suite (bootstrap mode).
#
# Runs the pike pure-Python SMB2/3 protocol test framework against a standalone
# chimera SMB server. pike is the SMB analog of pynfs: an ordinary Python test
# suite that speaks SMB2 from its own client stack, so -- like the WPTS .NET host
# -- no VM / kernel CIFS client is needed. The runner and the daemon both live in
# an isolated network namespace and talk over TCP/445.
#
# 1. Generates a chimera config exposing the "share" the tests connect to
# 2. Creates a network namespace with the SUT IP (10.0.0.1)
# 3. Starts the chimera daemon in the netns (SMB on 10.0.0.1:445)
# 4. Runs pike's tests via pytest, authenticating with pure-Python NTLM
# 5. Captures the JUnit result + pytest exit code and cleans up
#
# pike is run straight from its source tree (PYTHONPATH=$PIKE_DIR/src) exactly
# like pynfs runs from /opt/pynfs; the optional pike.kerberos C extension is NOT
# needed because we authenticate with NTLM (PIKE_CREDS).
#
# Optional environment (feature toggles -- mirror the WPTS wrapper so the same
# config-matrix machinery drives both):
#   PIKE_DIR                    pike source root (default: /opt/pike)
#   PIKE_JUNIT_FILE             write a JUnit XML report here
#   CHIMERA_SMB_PERSISTENT=1    durable/persistent handles + CA share
#   CHIMERA_SMB_ENCRYPTION=1    SMB3 transport encryption (also drives PIKE_ENCRYPT)
#   CHIMERA_SMB_MULTICHANNEL=1  advertise a second NIC for multichannel
#   PIKE_TIMEOUT                per-run wall-clock cap in seconds (default 300)

set -u

# Save and clear LD_PRELOAD immediately to avoid ASAN interference with system
# binaries (ip, etc.) which exit non-zero under ASAN. Restored only for chimera.
SAVED_LD_PRELOAD="${LD_PRELOAD:-}"
unset LD_PRELOAD

CHIMERA_BINARY=$1; shift
BACKEND=$1; shift
TEST_LIST="${1:-}"; shift || true

# Resolve the daemon to an absolute path: the runner cd's into pike's test dir
# before launching pytest, so every other path the script uses (session dir,
# logs, JUnit) must be absolute to survive that cd.
CHIMERA_BINARY=$(readlink -f "$CHIMERA_BINARY")

PIKE_DIR="${PIKE_DIR:-/opt/pike}"
PIKE_TEST_DIR="${PIKE_DIR}/src/pike/test"
PIKE_TIMEOUT="${PIKE_TIMEOUT:-300}"

if [ ! -d "$PIKE_TEST_DIR" ]; then
    echo "pike test dir not found: ${PIKE_TEST_DIR} (set PIKE_DIR)" >&2
    exit 1
fi

SUT_IP="10.0.0.1"
SUT_IP2="10.0.0.2"          # second NIC for SMB3 multichannel
SUT_IPV6="fd00::1"
NETNS_NAME="pike_smb_$$_$(date +%s%N)"
DUMMY_IF="pike0"
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/pike_smb_session_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
CHIMERA_LOG="${SESSION_DIR}/chimera_stderr.log"
JUNIT_OUT="${SESSION_DIR}/pike-junit.xml"
CHIMERA_PID=""

cleanup() {
    if [ -n "$CHIMERA_PID" ]; then
        kill "$CHIMERA_PID" 2>/dev/null || true
        for _ in $(seq 1 20); do
            kill -0 "$CHIMERA_PID" 2>/dev/null || break
            sleep 0.1
        done
        kill -9 "$CHIMERA_PID" 2>/dev/null || true
        wait "$CHIMERA_PID" 2>/dev/null || true
    fi
    for pid in $(ip netns pids "${NETNS_NAME}" 2>/dev/null); do
        kill "$pid" 2>/dev/null || true
    done
    sleep 0.1
    for pid in $(ip netns pids "${NETNS_NAME}" 2>/dev/null); do
        kill -9 "$pid" 2>/dev/null || true
    done
    timeout 2s ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -rf "$SESSION_DIR"
}
trap cleanup EXIT

generate_config() {
    # The single share the tests connect to (PIKE_SHARE=share). For the path
    # backends each share gets a real on-disk directory; memfs is mounted at "/".
    local mpath="/"
    case "$BACKEND" in
        linux|io_uring)
            mpath="${SESSION_DIR}/data/share"
            mkdir -p "$mpath"
            ;;
    esac

    # Continuous-availability is required for the durable/persistent handle tests
    # to be granted persistent handles; gate it on the persistent config.
    local share_ca=""
    if [ "${CHIMERA_SMB_PERSISTENT:-0}" = "1" ]; then
        share_ca=', "continuous_availability": true'
    fi
    # Per-share encrypt_data so the encryption tests (run with PIKE_ENCRYPT=yes)
    # exercise the encrypted transport path.
    local share_enc=""
    if [ "${CHIMERA_SMB_ENCRYPTION:-0}" = "1" ]; then
        share_enc=', "encrypt_data": true'
    fi

    local persistent_line=""
    if [ "${CHIMERA_SMB_PERSISTENT:-0}" = "1" ]; then
        persistent_line='"smb_persistent_handles": true,'
    fi
    local encryption_line=""
    if [ "${CHIMERA_SMB_ENCRYPTION:-0}" = "1" ]; then
        encryption_line='"smb_encryption": "enabled",'
    fi
    local multichannel_line=""
    if [ "${CHIMERA_SMB_MULTICHANNEL:-0}" = "1" ]; then
        multichannel_line="\"smb_multichannel\": [{\"address\": \"${SUT_IP}\", \"speed\": 10, \"rdma\": false}, {\"address\": \"${SUT_IP2}\", \"speed\": 10, \"rdma\": false}, {\"address\": \"${SUT_IPV6}\", \"speed\": 10, \"rdma\": false}],"
    fi

    cat > "$CONFIG_FILE" << EOF
{
    "common": {
        "rcu_reclaim_threads": 4
    },
    "server": {
        ${persistent_line}
        ${encryption_line}
        ${multichannel_line}
        "smb_named_streams": true,
        "smb_leases": true,
        "smb_oplocks": true,
        "threads": 4,
        "delegation_threads": 4,
        "external_portmap": false
    },
    "mounts": {
        "share": {"module": "${BACKEND}", "path": "${mpath}"}
    },
    "shares": {
        "share": {"path": "/share"${share_ca}${share_enc}}
    },
    "users": [
        {
            "username": "administrator",
            "smbpasswd": "Password01!",
            "uid": 0,
            "gid": 0
        }
    ]
}
EOF
}

generate_config

ulimit -l unlimited 2>/dev/null || true

# Network namespace with the SUT IP on a dummy interface (locally routed). The
# pike client connects to 10.0.0.1 from inside the same netns.
ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up
ip netns exec "${NETNS_NAME}" ip link add "${DUMMY_IF}" type dummy
ip netns exec "${NETNS_NAME}" ip addr add "${SUT_IP}/24" dev "${DUMMY_IF}"
if [ "${CHIMERA_SMB_MULTICHANNEL:-0}" = "1" ]; then
    ip netns exec "${NETNS_NAME}" ip addr add "${SUT_IP2}/24" dev "${DUMMY_IF}"
    ip netns exec "${NETNS_NAME}" ip -6 addr add "${SUT_IPV6}/64" dev "${DUMMY_IF}"
fi
ip netns exec "${NETNS_NAME}" ip link set "${DUMMY_IF}" up

# Start the chimera daemon inside the netns (restore LD_PRELOAD for chimera only).
ip netns exec "${NETNS_NAME}" env \
    LD_PRELOAD="${SAVED_LD_PRELOAD}" \
    ASAN_OPTIONS="detect_leaks=0:handle_abort=2:print_cmdline=1" \
    "$CHIMERA_BINARY" ${CHIMERA_DEBUG:+-d} -c "$CONFIG_FILE" \
    >"$CHIMERA_LOG" 2>&1 &
CHIMERA_PID=$!

# Wait for the SMB port to come up.
ready=0
for _ in $(seq 1 50); do
    if ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/${SUT_IP}/445" 2>/dev/null; then
        ready=1
        break
    fi
    if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
        echo "chimera daemon exited prematurely"
        echo "=== Chimera stderr ==="
        cat "$CHIMERA_LOG"
        exit 1
    fi
    sleep 0.1
done
if [ "$ready" != "1" ]; then
    echo "chimera SMB port ${SUT_IP}:445 never became ready"
    cat "$CHIMERA_LOG"
    exit 1
fi

# Build the pytest target list. An explicit comma-separated node-id list selects
# exactly those cases; an empty list runs the whole suite (bootstrap mode).
# pytest runs with cwd = pike's test dir (see below), so node-ids stay short and
# stable: "module.py::Class::method". An empty list runs the whole suite.
PYTEST_TARGETS=()
if [ -n "${TEST_LIST}" ]; then
    IFS=',' read -ra _ids <<< "${TEST_LIST}"
    for _id in "${_ids[@]}"; do
        [ -n "${_id}" ] && PYTEST_TARGETS+=("${_id}")
    done
else
    PYTEST_TARGETS=(".")
fi

# Encryption config: ask the pike client to negotiate an encrypted session so the
# transport-encryption path is actually exercised (the share also requires it).
PIKE_ENCRYPT_ENV=()
if [ "${CHIMERA_SMB_ENCRYPTION:-0}" = "1" ]; then
    PIKE_ENCRYPT_ENV=(PIKE_ENCRYPT=yes)
fi

JUNIT_ARG=()
if [ -n "${PIKE_JUNIT_FILE:-}" ]; then
    mkdir -p "$(dirname "${PIKE_JUNIT_FILE}")"
    JUNIT_ARG=(--junitxml="${JUNIT_OUT}")
fi

# Run pike from inside the netns so it can reach the SUT IP. cwd is pike's test
# dir so the node-ids in the CSV stay short and stable ("module.py::Class::method").
cd "${PIKE_TEST_DIR}"
ip netns exec "${NETNS_NAME}" env \
    PYTHONPATH="${PIKE_DIR}/src" \
    PYTHONUNBUFFERED=1 \
    PIKE_SERVER="${SUT_IP}" \
    PIKE_PORT=445 \
    PIKE_SHARE=share \
    PIKE_CREDS='administrator%Password01!' \
    "${PIKE_ENCRYPT_ENV[@]}" \
    timeout "${PIKE_TIMEOUT}" \
    python3 -m pytest "${PYTEST_TARGETS[@]}" \
        --rootdir="${PIKE_TEST_DIR}" \
        -p no:cacheprovider -o addopts="" \
        --timeout="${PIKE_CASE_TIMEOUT:-90}" --timeout-method=signal \
        -rfE --tb=short -q \
        "${JUNIT_ARG[@]}" \
    > "${SESSION_DIR}/pike_stdout.log" 2>&1
PYTEST_RC=$?

cat "${SESSION_DIR}/pike_stdout.log"
echo "=== pytest exit code: ${PYTEST_RC} ==="

# Copy out the JUnit (the netns run wrote it inside SESSION_DIR).
if [ -n "${PIKE_JUNIT_FILE:-}" ] && [ -f "${JUNIT_OUT}" ]; then
    cp -f "${JUNIT_OUT}" "${PIKE_JUNIT_FILE}" 2>/dev/null || true
fi

# A daemon that died during the run is a finding in its own right.
if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
    wait "$CHIMERA_PID" 2>/dev/null
    DAEMON_RC=$?
    echo "=== chimera daemon exited during run (rc=${DAEMON_RC}) ==="
    tail -80 "$CHIMERA_LOG" 2>/dev/null || true
    CHIMERA_PID=""
    [ "$PYTEST_RC" = "0" ] && PYTEST_RC=70
elif [ "$PYTEST_RC" != "0" ]; then
    echo "=== Chimera stderr (last 40 lines) ==="
    tail -40 "$CHIMERA_LOG" 2>/dev/null || true
fi

exit ${PYTEST_RC}
