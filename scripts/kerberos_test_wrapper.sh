#!/bin/bash

# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Kerberos Test Wrapper (MIT KDC only - no AD)
#
# Runs SMB Kerberos authentication tests against a minimal MIT KDC.
# Everything runs inside an isolated network namespace.
#
# Requirements:
# - krb5-kdc, krb5-admin-server packages
# - Root privileges (for network namespace)
#
# Usage:
#   ./kerberos_test_wrapper.sh <test_command> [args...]

set -e

# Use short names to fit 15-char interface limit
TEST_ID="$$_$(date +%s%N | tail -c 6)"
NETNS_NAME="ns_krb_${TEST_ID}"
KRB_DIR="/tmp/krb5_${TEST_ID}"
REALM="TEST.LOCAL"
KDC_IP="127.0.0.1"
KDC_PORT="8888"
SMB_HOST="smbhost.test.local"

log() {
    echo "[krb_test] $*" >&2
}

cleanup() {
    log "Cleaning up..."

    # Kill all processes in the namespace
    if ip netns list 2>/dev/null | grep -q "^${NETNS_NAME}"; then
        # Kill any remaining processes in the namespace
        ip netns pids "${NETNS_NAME}" 2>/dev/null | xargs -r kill -9 2>/dev/null || true
        sleep 0.1
        ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    fi

    # Remove hostname entry from /etc/hosts
    if [ "${HOSTS_MODIFIED:-0}" = "1" ]; then
        sed -i "/^127\.0\.0\.1 ${SMB_HOST}$/d" /etc/hosts 2>/dev/null || true
    fi

    # Clean up temp directory
    rm -rf "${KRB_DIR}" 2>/dev/null || true
}

trap cleanup EXIT

check_requirements() {
    local missing=""

    for cmd in kdb5_util krb5kdc kadmin.local; do
        if ! command -v "$cmd" &>/dev/null; then
            missing="$missing $cmd"
        fi
    done

    if [ -n "$missing" ]; then
        log "ERROR: Missing required commands:$missing"
        log "Install with: apt-get install krb5-kdc krb5-admin-server"
        exit 1
    fi
}

setup_namespace() {
    log "Creating network namespace..."

    ip netns add "${NETNS_NAME}"
    ip netns exec "${NETNS_NAME}" ip link set lo up

    # Add hostname for SMB server (smbclient refuses Kerberos to 'localhost')
    echo "127.0.0.1 ${SMB_HOST}" >> /etc/hosts
    HOSTS_MODIFIED=1

    log "Namespace ${NETNS_NAME} created"
}

setup_kdc() {
    log "Setting up MIT KDC..."

    mkdir -p "${KRB_DIR}"/{etc,var/lib/krb5kdc,var/run}

    # Create kdc.conf
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

    # Create krb5.conf
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
    ${SMB_HOST} = ${REALM}
EOF

    # Create ACL file
    echo "*/admin@${REALM} *" > "${KRB_DIR}/var/lib/krb5kdc/kadm5.acl"

    log "KRB5_CONFIG=${KRB_DIR}/etc/krb5.conf"
}

create_database() {
    log "Creating KDC database..."

    export KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf"
    export KRB5_KDC_PROFILE="${KRB_DIR}/etc/kdc.conf"

    # Create the database with a random master password
    local master_pass
    master_pass=$(head -c 32 /dev/urandom | base64)

    kdb5_util create -s -r "${REALM}" -P "${master_pass}" 2>/dev/null

    log "KDC database created"
}

create_principals() {
    log "Creating principals..."

    export KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf"
    export KRB5_KDC_PROFILE="${KRB_DIR}/etc/kdc.conf"

    # Create service principals for our SMB server
    # localhost: for libsmb2 tests (MIT Kerberos, no localhost restriction)
    kadmin.local -q "addprinc -randkey cifs/localhost@${REALM}" 2>/dev/null
    kadmin.local -q "addprinc -randkey host/localhost@${REALM}" 2>/dev/null
    # SMB_HOST: for smbclient tests (smbclient refuses Kerberos to 'localhost')
    kadmin.local -q "addprinc -randkey cifs/${SMB_HOST}@${REALM}" 2>/dev/null
    kadmin.local -q "addprinc -randkey host/${SMB_HOST}@${REALM}" 2>/dev/null

    # Create test users
    kadmin.local -q "addprinc -pw Password1! testuser1@${REALM}" 2>/dev/null
    kadmin.local -q "addprinc -pw Password2! testuser2@${REALM}" 2>/dev/null

    log "Principals created"
}

generate_keytab() {
    log "Generating keytab..."

    export KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf"
    export KRB5_KDC_PROFILE="${KRB_DIR}/etc/kdc.conf"

    KEYTAB_FILE="${KRB_DIR}/chimera.keytab"

    kadmin.local -q "ktadd -k ${KEYTAB_FILE} cifs/localhost@${REALM}" 2>/dev/null
    kadmin.local -q "ktadd -k ${KEYTAB_FILE} host/localhost@${REALM}" 2>/dev/null
    kadmin.local -q "ktadd -k ${KEYTAB_FILE} cifs/${SMB_HOST}@${REALM}" 2>/dev/null
    kadmin.local -q "ktadd -k ${KEYTAB_FILE} host/${SMB_HOST}@${REALM}" 2>/dev/null

    chmod 644 "${KEYTAB_FILE}"

    log "KRB5_KTNAME=${KEYTAB_FILE}"
}

start_kdc() {
    log "Starting KDC on port ${KDC_PORT} inside namespace..."

    # Start KDC inside the namespace
    ip netns exec "${NETNS_NAME}" env \
        KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf" \
        KRB5_KDC_PROFILE="${KRB_DIR}/etc/kdc.conf" \
        krb5kdc -n -P "${KRB_DIR}/kdc.pid" &

    # Wait for KDC to be ready
    sleep 1

    if [ -f "${KRB_DIR}/kdc.pid" ] && kill -0 "$(cat "${KRB_DIR}/kdc.pid")" 2>/dev/null; then
        log "KDC started (PID: $(cat "${KRB_DIR}/kdc.pid"))"
    else
        log "ERROR: KDC failed to start"
        return 1
    fi
}

obtain_ticket() {
    log "Obtaining Kerberos ticket for testuser1..."

    # Get a ticket for testuser1 inside the namespace
    # Use printf to avoid newline issues with password
    if ip netns exec "${NETNS_NAME}" env \
        KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf" \
        KRB5CCNAME="${KRB_DIR}/krb5cc_testuser1" \
        sh -c 'printf "%s" "Password1!" | kinit testuser1@'"${REALM}"' 2>/dev/null'; then
        log "Obtained TGT for testuser1@${REALM}"
        log "KRB5CCNAME=${KRB_DIR}/krb5cc_testuser1"

        # Pre-acquire cifs service ticket so smbclient doesn't need KDC access
        ip netns exec "${NETNS_NAME}" env \
            KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf" \
            KRB5CCNAME="${KRB_DIR}/krb5cc_testuser1" \
            kvno cifs/${SMB_HOST}@${REALM} 2>/dev/null || true

        ip netns exec "${NETNS_NAME}" env \
            KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf" \
            KRB5CCNAME="${KRB_DIR}/krb5cc_testuser1" \
            klist 2>/dev/null || true
        return 0
    else
        log "ERROR: Failed to obtain Kerberos ticket"
        return 1
    fi
}

run_test() {
    log "Running test inside namespace: $*"

    # Run test inside the namespace with all required environment variables
    ip netns exec "${NETNS_NAME}" env \
        KRB5_CONFIG="${KRB_DIR}/etc/krb5.conf" \
        KRB5_KDC_PROFILE="${KRB_DIR}/etc/kdc.conf" \
        KRB5_KTNAME="${KRB_DIR}/chimera.keytab" \
        KRB5CCNAME="${KRB_DIR}/krb5cc_testuser1" \
        KRB_REALM="${REALM}" \
        KRB_USER="testuser1" \
        KRB_SMB_HOST="${SMB_HOST}" \
        KRB_KDC="${KDC_IP}:${KDC_PORT}" \
        "$@"
}

main() {
    if [ $# -lt 1 ]; then
        echo "Usage: $0 <test_command> [args...]"
        echo ""
        echo "Runs a test command with a minimal MIT KDC for Kerberos testing."
        echo "Both KDC and test run inside an isolated network namespace."
        echo ""
        echo "Environment variables exported to test:"
        echo "  KRB5_CONFIG   - Path to krb5.conf"
        echo "  KRB5_KTNAME   - Path to keytab file"
        echo "  KRB_REALM     - Kerberos realm"
        echo "  KRB_KDC       - KDC address:port"
        exit 1
    fi

    check_requirements
    setup_namespace
    setup_kdc
    create_database
    create_principals
    generate_keytab
    start_kdc
    obtain_ticket
    run_test "$@"
}

main "$@"
