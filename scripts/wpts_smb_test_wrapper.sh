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
# Second SUT IP for SMB3 multichannel (a redundant "NIC"). Only wired up when
# CHIMERA_SMB_MULTICHANNEL=1; the WPTS MultipleChannel cases establish the
# alternative channel against it (and the main channel queries it back via
# FSCTL_QUERY_NETWORK_INTERFACE_INFO).
SUT_IP2="10.0.0.2"
# IPv6 SUT address for SMB3 multichannel. Only wired up when
# CHIMERA_SMB_MULTICHANNEL=1; the NetworkInterfaceInfo_Query_ReturnsIPv4IPv6
# case requires the server to advertise both an IPv4 and an IPv6 interface over
# FSCTL_QUERY_NETWORK_INTERFACE_INFO. ULA address (RFC 4193).
SUT_IPV6="fd00::1"
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
        for _ in $(seq 1 20); do
            if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
                break
            fi
            sleep 0.1
        done
        if kill -0 "$CHIMERA_PID" 2>/dev/null; then
            kill -9 "$CHIMERA_PID" 2>/dev/null || true
        fi
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
        # FileShare is the designated continuous-availability share (matches
        # CAShareName in the ptfconfig); mark it CA so persistent handles are
        # granted there but not on the other (non-CA) shares.
        local share_ca=""
        if [ "${CHIMERA_SMB_PERSISTENT:-0}" = "1" ] && [ "$share" = "FileShare" ]; then
            share_ca=', "continuous_availability": true'
        fi
        # SMBEncrypted is the designated per-share-encrypted share (matches
        # EncryptedFileShare in the ptfconfig); flag it so TREE_CONNECT
        # advertises SMB2_SHAREFLAG_ENCRYPT_DATA and its traffic is encrypted.
        local share_enc=""
        if [ "${CHIMERA_SMB_ENCRYPTION:-0}" = "1" ] && [ "$share" = "SMBEncrypted" ]; then
            share_enc=', "encrypt_data": true'
        fi
        shares="${shares}${sep}\"${share}\": {\"path\": \"/${share}\"${share_ca}${share_enc}}"
        sep=",
        "
    done

    # Enable SMB3 durable/persistent handles when requested (the durable/
    # persistent WPTS cases need the feature on; default off keeps the rest of
    # the suite exercising the baseline path).
    local persistent_line=""
    if [ "${CHIMERA_SMB_PERSISTENT:-0}" = "1" ]; then
        persistent_line='"smb_persistent_handles": true,'
    fi

    # Enable SMB3 transport compression when requested (the SMB2Compression
    # WPTS cases need the feature on; default off keeps the rest of the suite
    # exercising the uncompressed path).
    local compression_line=""
    if [ "${CHIMERA_SMB_COMPRESSION:-0}" = "1" ]; then
        compression_line='"smb_compression": true,'
    fi

    # Advertise two NICs for SMB3 multichannel when requested.  The daemon
    # reports these back over FSCTL_QUERY_NETWORK_INTERFACE_INFO so the WPTS
    # MultipleChannel cases can discover the alternative server address.
    local multichannel_line=""
    if [ "${CHIMERA_SMB_MULTICHANNEL:-0}" = "1" ]; then
        multichannel_line="\"smb_multichannel\": [{\"address\": \"${SUT_IP}\", \"speed\": 10, \"rdma\": false}, {\"address\": \"${SUT_IP2}\", \"speed\": 10, \"rdma\": false}, {\"address\": \"${SUT_IPV6}\", \"speed\": 10, \"rdma\": false}],"
        # The MultipleChannel_Negative_SMB2002 case establishes its main channel
        # with the original SMB 2.0.2 dialect (then verifies the bind is refused),
        # so the multichannel harness lowers the dialect floor to 2.0.2.
        multichannel_line="${multichannel_line}
        \"smb_min_dialect\": \"2.0.2\","
    fi

    # Enable SMB3 transport encryption when requested. "enabled" turns on global
    # (whole-session) encryption AND lets the per-share SMBEncrypted share opt in;
    # default off keeps the rest of the suite on the cleartext path.
    local encryption_line=""
    if [ "${CHIMERA_SMB_ENCRYPTION:-0}" = "1" ]; then
        encryption_line='"smb_encryption": "enabled",'
    fi

    cat > "$CONFIG_FILE" << EOF
{
    "server": {
        ${persistent_line}
        ${compression_line}
        ${multichannel_line}
        ${encryption_line}
        "smb_named_streams": true,
        "smb_leases": true,
        "smb_oplocks": true,
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
# Second address on the same dummy interface for multichannel: both the SUT's
# alternative NIC and the driver's second client NIC live here (loopback-routed
# within the netns, so a single device carrying both addresses is sufficient).
if [ "${CHIMERA_SMB_MULTICHANNEL:-0}" = "1" ]; then
    ip netns exec "${NETNS_NAME}" ip addr add "${SUT_IP2}/24" dev "${DUMMY_IF}"
    # IPv6 address so the daemon can advertise an AF_INET6 interface over
    # FSCTL_QUERY_NETWORK_INTERFACE_INFO (NetworkInterfaceInfo_Query_ReturnsIPv4IPv6).
    ip netns exec "${NETNS_NAME}" ip -6 addr add "${SUT_IPV6}/64" dev "${DUMMY_IF}"
fi
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

# When persistent handles are enabled, advertise the matching capability +
# CA-share to the test driver so the DurableHandleV2 / PersistentHandle cases
# become applicable (otherwise they are Inconclusive/skipped).
if [ "${CHIMERA_SMB_PERSISTENT:-0}" = "1" ]; then
    staged="${WPTS_BIN_DIR}/CommonTestSuite.deployment.ptfconfig"
    sed -i \
        -e 's#<Property name="IsPersistentHandlesSupported" value="false"/>#<Property name="IsPersistentHandlesSupported" value="true"/>#' \
        -e 's#<Property name="CAShareName" value=""/>#<Property name="CAShareName" value="FileShare"/>#' \
        -e "s#<Property name=\"CAShareServerName\" value=\"\"/>#<Property name=\"CAShareServerName\" value=\"${SUT_IP}\"/>#" \
        "$staged"
fi

# When transport compression is enabled, advertise the supported algorithms and
# a compressed share so the SMB2Compression cases become applicable. Chimera
# implements Plain LZ77 and the chained Pattern_V1 run-length payload.
if [ "${CHIMERA_SMB_COMPRESSION:-0}" = "1" ]; then
    staged="${WPTS_BIN_DIR}/CommonTestSuite.deployment.ptfconfig"
    sed -i \
        -e 's#<Property name="CompressedFileShare" value=""/>#<Property name="CompressedFileShare" value="SMBBasic"/>#' \
        -e 's#<Property name="IsChainedCompressionSupported" value="false"/>#<Property name="IsChainedCompressionSupported" value="true"/>#' \
        -e 's#<Property name="SupportedCompressionAlgorithms" value=""/>#<Property name="SupportedCompressionAlgorithms" value="LZ77;Pattern_V1"/>#' \
        "$staged"
fi

# When multichannel is enabled, advertise the capability and the second
# server/client NIC addresses so the WPTS MultipleChannel cases become
# applicable (they assert serverIps.Count > 1 and the IsMultiChannelCapable
# capability; otherwise they are Inconclusive/skipped).
if [ "${CHIMERA_SMB_MULTICHANNEL:-0}" = "1" ]; then
    staged_common="${WPTS_BIN_DIR}/CommonTestSuite.deployment.ptfconfig"
    staged_smb2="${WPTS_BIN_DIR}/MS-SMB2_ServerTestSuite.deployment.ptfconfig"
    sed -i \
        -e 's#<Property name="IsMultiChannelCapable" value="false"/>#<Property name="IsMultiChannelCapable" value="true"/>#' \
        -e "s#<Property name=\"ClientNic2IPAddress\" value=\"\"/>#<Property name=\"ClientNic2IPAddress\" value=\"${SUT_IP2}\"/>#" \
        "$staged_common"
    sed -i \
        -e "s#<Property name=\"SutAlternativeIPAddress\" value=\"\"/>#<Property name=\"SutAlternativeIPAddress\" value=\"${SUT_IP2}\"/>#" \
        "$staged_smb2"
fi

# When encryption is enabled, advertise it to the driver so the Encryption cases
# become applicable: declare support, the per-share encrypted share, and that
# global encrypt-data is on. SutSupportedEncryptionAlgorithms is set
# unconditionally in the fixture: even in baseline mode the SMB 3.1.1 NEGOTIATE
# selects and echoes a CipherId from the client's offered list (transport
# encryption is decided separately at SESSION_SETUP), so the driver must always
# know the cipher set to validate the echoed Connection.CipherId
# (BVT_Negotiate_SMB311).
if [ "${CHIMERA_SMB_ENCRYPTION:-0}" = "1" ]; then
    staged="${WPTS_BIN_DIR}/CommonTestSuite.deployment.ptfconfig"
    sed -i \
        -e 's#<Property name="IsEncryptionSupported" value="false"/>#<Property name="IsEncryptionSupported" value="true"/>#' \
        -e 's#<Property name="IsGlobalEncryptDataEnabled" value="false"/>#<Property name="IsGlobalEncryptDataEnabled" value="true"/>#' \
        -e 's#<Property name="EncryptedFileShare" value=""/>#<Property name="EncryptedFileShare" value="SMBEncrypted"/>#' \
        "$staged"
fi

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
