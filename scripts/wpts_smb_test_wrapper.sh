#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: wpts_smb_test_wrapper.sh <chimera_binary> <backend> <test_filter>
#
# Runs Microsoft's Windows Protocol Test Suites (WPTS) File Server / MS-SMB2
# server test suite against a standalone chimera SMB server. Unlike the KVM
# tests, WPTS speaks SMB2 from its own managed (.NET) stack, so no VM / kernel
# CIFS client is needed -- the driver and the daemon both live in an isolated
# network namespace and talk over TCP/445.
#
# 1. Generates a chimera config exposing the share names WPTS expects
# 2. Creates a network namespace with the SUT IP (10.0.0.1)
# 3. Starts the chimera daemon in the netns (SMB on 10.0.0.1:445)
# 4. Stages our pinned .ptfconfig files into the WPTS Bin dir
# 5. Runs `dotnet vstest` with the requested TestCategory filter
# 6. Captures the TRX result + vstest exit code and cleans up
#
# Required environment:
#   WPTS_BIN_DIR  Path to the extracted WPTS FileServer "Bin" directory
#   DOTNET        Path to the dotnet executable
# Optional:
#   WPTS_PTFCONFIG_DIR  Dir holding our *.deployment.ptfconfig fixtures
#                       (defaults to <repo>/src/server/smb/tests/wpts)
#   WPTS_RESULT_DIR     Where to copy the TRX result (defaults to session dir)

set -u

CHIMERA_BINARY=$1; shift
BACKEND=$1; shift
TEST_FILTER="$1"; shift || true

: "${WPTS_BIN_DIR:?WPTS_BIN_DIR must point at the WPTS FileServer Bin dir}"
: "${DOTNET:?DOTNET must point at the dotnet executable}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WPTS_PTFCONFIG_DIR="${WPTS_PTFCONFIG_DIR:-${SCRIPT_DIR}/../src/server/smb/tests/wpts}"

SUT_IP="10.0.0.1"
NETNS_NAME="wpts_smb_$$_$(date +%s%N)"
DUMMY_IF="wpts0"
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/wpts_smb_session_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
CHIMERA_LOG="${SESSION_DIR}/chimera_stderr.log"
RESULT_DIR="${WPTS_RESULT_DIR:-${SESSION_DIR}/results}"
CHIMERA_PID=""

# WPTS-required share names. Each gets its own VFS mount so the share root
# exists and is isolated (mirrors how the KVM config mounts at /share).
SHARES=(SMBBasic SMBEncrypted FileShare ShareForceLevel2)

cleanup() {
    if [ -n "$CHIMERA_PID" ]; then
        kill "$CHIMERA_PID" 2>/dev/null || true
        wait "$CHIMERA_PID" 2>/dev/null || true
    fi
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -rf "$SESSION_DIR"
}
trap cleanup EXIT

generate_config() {
    local mounts="" shares="" sep=""
    for share in "${SHARES[@]}"; do
        local mpath="/"
        case "$BACKEND" in
            linux|io_uring)
                mpath="${SESSION_DIR}/data/${share}"
                mkdir -p "$mpath"
                ;;
        esac
        mounts="${mounts}${sep}\"${share}\": {\"module\": \"${BACKEND}\", \"path\": \"${mpath}\"}"
        shares="${shares}${sep}\"${share}\": {\"path\": \"/${share}\"}"
        sep=",
        "
    done

    cat > "$CONFIG_FILE" << EOF
{
    "server": {
        "threads": 4,
        "delegation_threads": 4,
        "external_portmap": false
    },
    "mounts": {
        ${mounts}
    },
    "shares": {
        ${shares}
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

# Network namespace with the SUT IP on a dummy interface. The driver connects
# to 10.0.0.1 from inside the same netns (locally routed).
ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up
ip netns exec "${NETNS_NAME}" ip link add "${DUMMY_IF}" type dummy
ip netns exec "${NETNS_NAME}" ip addr add "${SUT_IP}/24" dev "${DUMMY_IF}"
ip netns exec "${NETNS_NAME}" ip link set "${DUMMY_IF}" up

# Start the chimera daemon inside the netns
ip netns exec "${NETNS_NAME}" env \
    ASAN_OPTIONS="detect_leaks=0:handle_abort=2:print_cmdline=1" \
    "$CHIMERA_BINARY" ${CHIMERA_DEBUG:+-d} -c "$CONFIG_FILE" \
    >"$CHIMERA_LOG" 2>&1 &
CHIMERA_PID=$!

# Wait for SMB port to be ready
ready=0
for i in $(seq 1 50); do
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

# Stage our pinned ptfconfig fixtures into the WPTS Bin dir. WPTS auto-loads
# the *.deployment.ptfconfig files sitting next to the test DLL.
cp -f "${WPTS_PTFCONFIG_DIR}/CommonTestSuite.deployment.ptfconfig" \
      "${WPTS_PTFCONFIG_DIR}/MS-SMB2_ServerTestSuite.deployment.ptfconfig" \
      "${WPTS_BIN_DIR}/"

mkdir -p "$RESULT_DIR"

FILTER_ARGS=()
if [ -n "${TEST_FILTER}" ]; then
    FILTER_ARGS=(--TestCaseFilter:"${TEST_FILTER}")
fi

# Run the suite from inside the netns so it can reach the SUT IP.
# Suppress the .NET first-run experience (telemetry notice + dev-cert
# generation): it pollutes output and writes to HOME, which may be unset or
# read-only under ctest/CI.
ip netns exec "${NETNS_NAME}" env \
    DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    DOTNET_NOLOGO=1 \
    DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1 \
    HOME="${SESSION_DIR}" \
    "$DOTNET" vstest "${WPTS_BIN_DIR}/MS-SMB2_ServerTestSuite.dll" \
        "${FILTER_ARGS[@]}" \
        --logger:"trx;LogFileName=SMB2TestResult.trx" \
        --ResultsDirectory:"${RESULT_DIR}"
VSTEST_RC=$?

echo "=== vstest exit code: ${VSTEST_RC} ==="

# Persist the daemon log alongside the TRX so a crash/abort is recoverable
# after the session dir is cleaned up.
cp -f "$CHIMERA_LOG" "${RESULT_DIR}/chimera_stderr.log" 2>/dev/null || true

# A daemon that died during the run is a finding in its own right: fail the
# test even if vstest itself reported success.
if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
    wait "$CHIMERA_PID" 2>/dev/null
    DAEMON_RC=$?
    echo "=== chimera daemon exited during run (rc=${DAEMON_RC}) ==="
    tail -80 "$CHIMERA_LOG" 2>/dev/null || true
    CHIMERA_PID=""
    [ "$VSTEST_RC" = "0" ] && VSTEST_RC=70
elif [ "$VSTEST_RC" != "0" ]; then
    echo "=== Chimera stderr (last 80 lines) ==="
    tail -80 "$CHIMERA_LOG" 2>/dev/null || true
fi

exit ${VSTEST_RC}
