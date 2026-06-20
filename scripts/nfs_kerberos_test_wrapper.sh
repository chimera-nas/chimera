#!/bin/bash
# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
#
# RPCSEC_GSS / Kerberos NFS test wrapper.
#
# Stands up a minimal MIT KDC, a chimera NFS server with Kerberos enabled, and
# runs the pynfs 4.1 suite against it with sec=krb5 -- all inside an isolated
# network namespace.  Exercises the full RPCSEC_GSS path end to end: context
# establishment (NULL-proc INIT/CONTINUE), per-request call verifiers and the
# sequence window, and reply verifiers.
#
# Usage:
#   nfs_kerberos_test_wrapper.sh <chimera_binary> <backend> <pynfs_dir> [pynfs_flags...]
#
# Requirements: krb5-kdc, krb5-user, python3-gssapi, root (for the netns).
# Skips (exit 77) if the krb5 tooling or python gssapi module is missing.

set -u

# Clear LD_PRELOAD up front so ASAN does not perturb system binaries (ip,
# kadmin, ...).  Restored only for the chimera daemon.
SAVED_LD_PRELOAD="${LD_PRELOAD:-}"
unset LD_PRELOAD

CHIMERA_BINARY=$1; shift
BACKEND=$1; shift
PYNFS_DIR=$1; shift
FLAG_ARGS="$*"

TEST_ID="$$_$(date +%s%N | tail -c 6)"
NETNS_NAME="ns_nfskrb_${TEST_ID}"
KRB_DIR="/tmp/nfskrb_${TEST_ID}"
REALM="TEST.LOCAL"
KDC_IP="127.0.0.1"
KDC_PORT="8899"
NFS_HOST="nfshost.test.local"
KEYTAB_FILE="${KRB_DIR}/chimera.keytab"
CCACHE="FILE:${KRB_DIR}/krb5cc_testuser1"

# Optional per-export security policy: NFS_EXPORT_SEC="krb5,krb5i" restricts the
# /share export to those flavors (testing NFS4ERR_WRONGSEC enforcement).
EXPORT_SEC_JSON=""
if [ -n "${NFS_EXPORT_SEC:-}" ]; then
    EXPORT_SEC_JSON=", \"sec\": [$(echo "${NFS_EXPORT_SEC}" | sed 's/[^,]\+/"&"/g')]"
fi

BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/nfskrb_session_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
RESULTS_XML="${SESSION_DIR}/results.xml"
CHIMERA_LOG="${SESSION_DIR}/chimera.log"
CHIMERA_PID=""
HOSTS_MODIFIED=0

log() {
    echo "[nfs_krb] $*" >&2
}

cleanup() {
    if [ -n "$CHIMERA_PID" ]; then
        kill "$CHIMERA_PID" 2>/dev/null || true
        for _ in $(seq 1 150); do
            kill -0 "$CHIMERA_PID" 2>/dev/null || break
            sleep 0.02
        done
        kill -9 "$CHIMERA_PID" 2>/dev/null || true
        wait "$CHIMERA_PID" 2>/dev/null || true
    fi
    if [ -f "$CHIMERA_LOG" ]; then
        echo "=== Chimera log (last 100 lines) ==="
        tail -100 "$CHIMERA_LOG"
    fi
    if ip netns list 2>/dev/null | grep -q "^${NETNS_NAME}"; then
        ip netns pids "${NETNS_NAME}" 2>/dev/null | xargs -r kill -9 2>/dev/null || true
        sleep 0.1
        ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    fi
    if [ "${HOSTS_MODIFIED}" = "1" ]; then
        sed -i "/^127\.0\.0\.1 ${NFS_HOST}\$/d" /etc/hosts 2>/dev/null || true
    fi
    rm -rf "${KRB_DIR}" "${SESSION_DIR}" 2>/dev/null || true
}
trap cleanup EXIT

check_requirements() {
    local missing=""
    for cmd in kdb5_util krb5kdc kadmin.local kinit; do
        command -v "$cmd" &>/dev/null || missing="$missing $cmd"
    done
    if [ -n "$missing" ]; then
        log "SKIP: missing krb5 tooling:$missing"
        exit 77
    fi
    if ! python3 -c 'import gssapi' 2>/dev/null; then
        log "SKIP: python3 gssapi module not available (pynfs cannot do krb5)"
        exit 77
    fi
}

setup_namespace() {
    ip netns add "${NETNS_NAME}"
    ip netns exec "${NETNS_NAME}" ip link set lo up
    # gssapi refuses hostbased 'nfs@127.0.0.1'; map a stable hostname to lo.
    echo "127.0.0.1 ${NFS_HOST}" >> /etc/hosts
    HOSTS_MODIFIED=1
}

setup_kdc() {
    mkdir -p "${KRB_DIR}"/{etc,var/lib/krb5kdc,var/run}

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
    ${NFS_HOST} = ${REALM}
EOF

    echo "*/admin@${REALM} *" > "${KRB_DIR}/var/lib/krb5kdc/kadm5.acl"

    export KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf"
    export KRB5_KDC_PROFILE="${KRB_DIR}/etc/kdc.conf"

    kdb5_util create -s -r "${REALM}" -P "$(head -c 32 /dev/urandom | base64)" 2>/dev/null

    # NFS service principal (what the client targets as nfs@<host>) + a test user.
    kadmin.local -q "addprinc -randkey nfs/${NFS_HOST}@${REALM}" 2>/dev/null
    kadmin.local -q "addprinc -randkey host/${NFS_HOST}@${REALM}" 2>/dev/null
    kadmin.local -q "addprinc -pw Password1! testuser1@${REALM}" 2>/dev/null

    kadmin.local -q "ktadd -k ${KEYTAB_FILE} nfs/${NFS_HOST}@${REALM}" 2>/dev/null
    kadmin.local -q "ktadd -k ${KEYTAB_FILE} host/${NFS_HOST}@${REALM}" 2>/dev/null
    chmod 644 "${KEYTAB_FILE}"
}

start_kdc() {
    ip netns exec "${NETNS_NAME}" env \
        KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf" \
        KRB5_KDC_PROFILE="${KRB_DIR}/etc/kdc.conf" \
        krb5kdc -n -P "${KRB_DIR}/kdc.pid" &
    local launcher=$!
    local waited
    for waited in $(seq 1 60); do
        if [ -f "${KRB_DIR}/kdc.pid" ]; then
            local pid
            pid=$(cat "${KRB_DIR}/kdc.pid" 2>/dev/null)
            if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
                log "KDC up (pid ${pid})"
                return 0
            fi
        fi
        kill -0 "${launcher}" 2>/dev/null || { log "KDC exited early"; return 1; }
        sleep 1
    done
    log "KDC failed to start"
    return 1
}

obtain_ticket() {
    ip netns exec "${NETNS_NAME}" env \
        KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf" \
        KRB5CCNAME="${CCACHE}" \
        sh -c 'printf "%s" "Password1!" | kinit testuser1@'"${REALM}" 2>/dev/null || {
        log "kinit failed"
        return 1
    }
    # Pre-fetch the nfs service ticket so the test never blocks on the KDC.
    ip netns exec "${NETNS_NAME}" env \
        KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf" \
        KRB5CCNAME="${CCACHE}" \
        kvno "nfs/${NFS_HOST}@${REALM}" 2>/dev/null || true
}

generate_config() {
    local mount_path="/"
    local vfs_section=""

    case "$BACKEND" in
        linux | io_uring)
            mount_path="$SESSION_DIR/data"
            mkdir -p "$SESSION_DIR/data"
            ;;
        memfs)
            mount_path="/"
            ;;
        cairn)
            mount_path="/"
            vfs_section="\"vfs\": { \"cairn\": { \"config\": {\"initialize\":true,\"path\":\"$SESSION_DIR\"} } },"
            ;;
        *)
            log "Unsupported backend for krb5 test: $BACKEND"
            exit 1
            ;;
    esac

    cat > "$CONFIG_FILE" <<EOF
{
    "common": { "rcu_reclaim_threads": 4 },
    "server": {
        "threads": 4,
        "delegation_threads": 4,
        "nfs4_lease_time": 5,
        "nfs4_grace_time": 10,
        ${vfs_section}
        "nfs_auth": {
            "kerberos_enabled": true,
            "kerberos_keytab": "${KEYTAB_FILE}"
        },
        "external_portmap": false
    },
    "mounts": {
        "share": { "module": "$BACKEND", "path": "$mount_path" }
    },
    "exports": {
        "/share": { "path": "/share"${EXPORT_SEC_JSON} }
    }
}
EOF
}

start_chimera() {
    local preload_env=()
    [ -n "${SAVED_LD_PRELOAD}" ] && preload_env=(LD_PRELOAD="${SAVED_LD_PRELOAD}")

    ip netns exec "${NETNS_NAME}" env \
        KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf" \
        KRB5_KTNAME="${KEYTAB_FILE}" \
        "${preload_env[@]}" \
        "$CHIMERA_BINARY" -c "$CONFIG_FILE" >"$CHIMERA_LOG" 2>&1 &
    CHIMERA_PID=$!

    local i
    for i in $(seq 1 500); do
        if grep -q "Server is ready." "$CHIMERA_LOG" &&
           ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/127.0.0.1/2049" 2>/dev/null; then
            return 0
        fi
        kill -0 "$CHIMERA_PID" 2>/dev/null || { log "chimera exited during startup"; return 1; }
        sleep 0.02
    done
    log "chimera NFS port never became ready"
    return 1
}

run_pynfs() {
    export PYTHONPATH="${PYNFS_DIR}:${PYTHONPATH:-}"
    local testserver="${PYNFS_DIR}/nfs4.1/testserver.py"
    local timeout_s="${PYNFS_TIMEOUT:-120}"

    # pynfs 4.1 derives the GSS target as nfs@<server>, so the server must be
    # addressed by the keytab hostname, not 127.0.0.1.
    # shellcheck disable=SC2086
    timeout --foreground -k 5 "${timeout_s}" \
        ip netns exec "${NETNS_NAME}" env \
        KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf" \
        KRB5CCNAME="${CCACHE}" \
        KRB5_KTNAME="${KEYTAB_FILE}" \
        PYTHONPATH="${PYTHONPATH}" \
        python3 "${testserver}" "${NFS_HOST}:/share" \
        --minorversion=1 --security="${NFS_KRB_SEC:-krb5}" --maketree --force -v \
        --xml="${RESULTS_XML}" $FLAG_ARGS 2>&1 | tee "${SESSION_DIR}/pynfs.out"
    return "${PIPESTATUS[0]}"
}

main() {
    check_requirements
    setup_namespace
    setup_kdc
    start_kdc || exit 1
    obtain_ticket || exit 1
    generate_config
    start_chimera || exit 1

    run_pynfs
    local rc=$?

    # Negative test: a disallowed flavor must be rejected with NFS4ERR_WRONGSEC.
    # pynfs then fails to initialize, which is the expected, passing outcome.
    if [ -n "${NFS_EXPECT_WRONGSEC:-}" ]; then
        if grep -q 'NFS4ERR_WRONGSEC' "${SESSION_DIR}/pynfs.out" 2>/dev/null; then
            log "PASS (got expected NFS4ERR_WRONGSEC for disallowed flavor)"
            exit 0
        fi
        log "FAIL (expected NFS4ERR_WRONGSEC, not observed)"
        exit 1
    fi

    if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
        log "pynfs wall-clock timeout"
        exit 1
    fi
    if [ "$rc" -ne 0 ]; then
        exit "$rc"
    fi

    if [ ! -f "$RESULTS_XML" ]; then
        log "results file missing"
        exit 1
    fi
    read -r FAILURES ERRORS < <(python3 - "$RESULTS_XML" <<'PY'
import sys, xml.etree.ElementTree as ET
try:
    root = ET.parse(sys.argv[1]).getroot()
    ts = root if root.tag == "testsuite" else (root.find("testsuite") or root)
    print(ts.get("failures", "1"), ts.get("errors", "1"))
except Exception:
    print("1", "1")
PY
)
    if [ "${FAILURES:-1}" != "0" ] || [ "${ERRORS:-1}" != "0" ]; then
        log "pynfs reported ${FAILURES} failure(s), ${ERRORS} error(s)"
        exit 1
    fi
    log "PASS (sec=krb5)"
    exit 0
}

main "$@"
