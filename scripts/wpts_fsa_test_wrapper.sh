#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: wpts_fsa_test_wrapper.sh <chimera_binary> <backend> [test_list]
#
# Out-of-the-box harness for the WPTS MS-FSAModel server test suite, modelled
# directly on scripts/wpts_smb_test_wrapper.sh (the working MS-SMB2 harness).
# MS-FSA(Model) exercises the [MS-FSA] File System Algorithms over an SMB3
# transport: the managed IFSAAdapter opens/queries/sets files on the share and
# checks the file-system behaviour. We therefore reuse the exact same network
# isolation + daemon bringup, only swapping the DLL + ptfconfig set.
#
#   <test_list>  optional comma-separated vstest test-name list (exact match).
#                Empty => run the whole MS-FSAModel suite.
#
# Required environment:
#   WPTS_BIN_DIR  Path to the extracted WPTS FileServer "Bin" directory
#   DOTNET        Path to the dotnet executable

set -u

CHIMERA_BINARY=$1; shift
BACKEND=$1; shift
TEST_FILTER="${1:-}"; shift || true

: "${WPTS_BIN_DIR:?WPTS_BIN_DIR must point at the WPTS FileServer Bin dir}"
: "${DOTNET:?DOTNET must point at the dotnet executable}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# chimera's overridden Common ptfconfig (correct SUT IP/creds for the netns).
WPTS_PTFCONFIG_DIR="${WPTS_PTFCONFIG_DIR:-${SCRIPT_DIR}/../src/server/smb/tests/wpts}"

SUT_IP="10.0.0.1"
NETNS_NAME="wpts_fsa_$$_$(date +%s%N)"
DUMMY_IF="wpts0"
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/wpts_fsa_session_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
CHIMERA_LOG="${SESSION_DIR}/chimera_stderr.log"
RESULT_DIR="${WPTS_RESULT_DIR:-${SESSION_DIR}/results}"
CHIMERA_PID=""

# FSA uses NTFS_ShareFolder=FileShare; keep the MS-SMB2 share set so the HVRS
# SharePath (\\SUT\SMBBasic) and the basic share both resolve.
SHARES=(SMBBasic SMBEncrypted FileShare ShareForceLevel2)

cleanup() {
    if [ -n "$CHIMERA_PID" ]; then
        kill "$CHIMERA_PID" 2>/dev/null || true
        for _ in $(seq 1 20); do
            kill -0 "$CHIMERA_PID" 2>/dev/null || break
            sleep 0.1
        done
        kill -0 "$CHIMERA_PID" 2>/dev/null && kill -9 "$CHIMERA_PID" 2>/dev/null || true
        wait "$CHIMERA_PID" 2>/dev/null || true
    fi
    for pid in $(ip netns pids "${NETNS_NAME}" 2>/dev/null); do kill "$pid" 2>/dev/null || true; done
    sleep 0.1
    for pid in $(ip netns pids "${NETNS_NAME}" 2>/dev/null); do kill -9 "$pid" 2>/dev/null || true; done
    timeout 2s ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -rf "$SESSION_DIR"
    [ -n "${WPTS_STAGE_DIR:-}" ] && rm -rf "$WPTS_STAGE_DIR"
}
trap cleanup EXIT

generate_config() {
    local mounts="" shares="" sep=""
    for share in "${SHARES[@]}"; do
        local mpath="/"
        case "$BACKEND" in
            linux|io_uring) mpath="${SESSION_DIR}/data/${share}"; mkdir -p "$mpath";;
        esac
        mounts="${mounts}${sep}\"${share}\": {\"module\": \"${BACKEND}\", \"path\": \"${mpath}\"}"
        shares="${shares}${sep}\"${share}\": {\"path\": \"/${share}\"}"
        sep=",
        "
    done

    cat > "$CONFIG_FILE" << EOF
{
    "server": {
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

ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up
ip netns exec "${NETNS_NAME}" ip link add "${DUMMY_IF}" type dummy
ip netns exec "${NETNS_NAME}" ip addr add "${SUT_IP}/24" dev "${DUMMY_IF}"
ip netns exec "${NETNS_NAME}" ip link set "${DUMMY_IF}" up

# NOTE: the suite's on-SUT fixtures (ExistingFolder, ExistingFile.txt, link.txt,
# MountPoint -- created by Create-FSAEnv.ps1 on a Windows SUT) are NOT seeded
# here.  memfs is in-memory and a symlink/mount-point reparse cannot be created
# over SMB, so seeding needs a daemon hook (future work).  The handful of cases
# that depend on those fixtures are marked xfail in wpts_fsa_cases.csv.

ip netns exec "${NETNS_NAME}" env \
    ASAN_OPTIONS="detect_leaks=0:handle_abort=2:print_cmdline=1" \
    "$CHIMERA_BINARY" ${CHIMERA_DEBUG:+-d} -c "$CONFIG_FILE" \
    >"$CHIMERA_LOG" 2>&1 &
CHIMERA_PID=$!

ready=0
for i in $(seq 1 50); do
    if ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/${SUT_IP}/445" 2>/dev/null; then ready=1; break; fi
    if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
        echo "chimera daemon exited prematurely"; cat "$CHIMERA_LOG"; exit 1
    fi
    sleep 0.1
done
[ "$ready" = "1" ] || { echo "chimera SMB port never ready"; cat "$CHIMERA_LOG"; exit 1; }

# Stage a private shadow of the WPTS Bin dir (see MS-SMB2 wrapper for rationale:
# PTF auto-loads *.deployment.ptfconfig next to the DLL by real path).
WPTS_BIN_ABS=$(cd "${WPTS_BIN_DIR}" && pwd)
_bin_dev=$(stat -c %d "${WPTS_BIN_ABS}")
_shadow_parent="${SESSION_DIR}"
for _cand in "$(dirname "${WPTS_BIN_ABS}")" "${TMPDIR:-/tmp}" /var/tmp; do
    [ -n "${_cand}" ] && [ -d "${_cand}" ] && [ -w "${_cand}" ] || continue
    if [ "$(stat -c %d "${_cand}" 2>/dev/null)" = "${_bin_dev}" ]; then _shadow_parent="${_cand}"; break; fi
done
WPTS_STAGE_DIR=$(mktemp -d "${_shadow_parent}/wpts_bin_XXXXXX")
if [ "$(stat -c %d "${WPTS_STAGE_DIR}")" = "${_bin_dev}" ]; then
    cp -al "${WPTS_BIN_ABS}/." "${WPTS_STAGE_DIR}/"
else
    cp -a "${WPTS_BIN_ABS}/." "${WPTS_STAGE_DIR}/"
fi

# Swap in chimera's Common override (correct SUT IP/creds). Keep the suite's own
# stock MS-FSA / MS-FSAModel deployment ptfconfigs (they carry the FSA group:
# FileSystem=NTFS, NTFS_ShareFolder=FileShare, the fixture names, and the FSA
# adapter binding). rm first so the hardlinked inode isn't edited in place.
rm -f "${WPTS_STAGE_DIR}/CommonTestSuite.deployment.ptfconfig" \
      "${WPTS_STAGE_DIR}/PTFApplicationLog.txt"
rm -rf "${WPTS_STAGE_DIR}/TestResults" "${WPTS_STAGE_DIR}/.\\TestLog"
cp "${WPTS_PTFCONFIG_DIR}/CommonTestSuite.deployment.ptfconfig" "${WPTS_STAGE_DIR}/"

# FSA capability profile: the suite's stock NTFS deployment ptfconfig (FileSystem,
# NTFS_ShareFolder, WhichFileSystemSupport_* flags) is used as-is -- it yields the
# stable, deterministic full-run baseline.  (A chimera-tuned profile that blanks
# unsupported-feature flags was tried and rejected: it reduced the passing set AND
# reproducibly crashed the daemon in the query_directory path.)  WPTS_FSA_PTFCONFIG
# may still point at an alternative profile for experiments.
if [ -n "${WPTS_FSA_PTFCONFIG:-}" ] && [ -f "${WPTS_FSA_PTFCONFIG}" ]; then
    rm -f "${WPTS_STAGE_DIR}/MS-FSA_ServerTestSuite.deployment.ptfconfig"
    cp "${WPTS_FSA_PTFCONFIG}" "${WPTS_STAGE_DIR}/MS-FSA_ServerTestSuite.deployment.ptfconfig"
fi

mkdir -p "$RESULT_DIR"

FILTER_ARGS=()
if [ -n "${TEST_FILTER}" ]; then
    _f=""
    IFS=',' read -ra _names <<< "${TEST_FILTER}"
    for _n in "${_names[@]}"; do [ -n "${_f}" ] && _f="${_f}|"; _f="${_f}Name=${_n}"; done
    FILTER_ARGS=(--TestCaseFilter:"${_f}")
fi

# Pre-seed NuGet migration marker (avoids the NuGet-Migrations mutex race).
NUGET_MIGRATIONS_DIR="${SESSION_DIR}/.local/share/NuGet/Migrations"
mkdir -p "${NUGET_MIGRATIONS_DIR}"; : > "${NUGET_MIGRATIONS_DIR}/1"

ip netns exec "${NETNS_NAME}" env \
    DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    DOTNET_NOLOGO=1 \
    DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1 \
    HOME="${SESSION_DIR}" \
    "$DOTNET" vstest "${WPTS_STAGE_DIR}/MS-FSAModel_ServerTestSuite.dll" \
        "${FILTER_ARGS[@]}" \
        --logger:"trx;LogFileName=FSAModelTestResult.trx" \
        --ResultsDirectory:"${RESULT_DIR}"
VSTEST_RC=$?

echo "=== vstest exit code: ${VSTEST_RC} ==="

# Per-case JUnit for the CI report: a consolidated entry is one CTest test, so
# convert the TRX into a per-case JUnit (same hook the MS-SMB2 wrapper uses).
# Non-fatal -- a conversion failure must not fail the run.
if [ -n "${WPTS_JUNIT_FILE:-}" ] && [ -n "${WPTS_JUNIT_CONVERTER:-}" ]; then
    python3 "${WPTS_JUNIT_CONVERTER}" "${RESULT_DIR}/FSAModelTestResult.trx" \
        "${WPTS_JUNIT_FILE}" 2>/dev/null \
        || echo "WPTS JUnit conversion failed (non-fatal)"
fi

cp -f "$CHIMERA_LOG" "${RESULT_DIR}/chimera_stderr.log" 2>/dev/null || true
echo "RESULT_DIR=${RESULT_DIR}"
# Keep the result dir around for inspection (don't let cleanup nuke it).
if [ -n "${WPTS_RESULT_DIR:-}" ]; then cp -f "${RESULT_DIR}"/*.trx "${WPTS_RESULT_DIR}/" 2>/dev/null || true; fi

DAEMON_DIED=0
if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
    wait "$CHIMERA_PID" 2>/dev/null; DAEMON_RC=$?
    echo "=== chimera daemon exited during run (rc=${DAEMON_RC}) ==="
    tail -80 "$CHIMERA_LOG" 2>/dev/null || true
    CHIMERA_PID=""
    DAEMON_DIED=1
fi

# Baseline-diff mode: MS-FSAModel is a model-based suite whose cases are NOT
# independent (outcomes depend on the full ordered sequence), so we cannot gate
# on a green subset -- we run the WHOLE suite and gate on regression against a
# recorded baseline (wpts_fsa_cases.csv).  vstest's own exit code is always
# non-zero here (the known-failing cases), so when WPTS_FSA_BASELINE is set the
# comparator's verdict (plus daemon liveness) decides the test outcome instead.
if [ -n "${WPTS_FSA_BASELINE:-}" ]; then
    if [ "${DAEMON_DIED}" = "1" ]; then
        echo "=== FAIL: chimera daemon died during the run ==="
        exit 1
    fi
    python3 "${SCRIPT_DIR}/../tools/wpts/fsa_baseline.py" \
        "${RESULT_DIR}/FSAModelTestResult.trx" "${WPTS_FSA_BASELINE}"
    exit $?
fi

exit ${VSTEST_RC}
