#!/bin/bash

# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# AD Integration Test Wrapper
#
# Runs SMB authentication tests against a real Samba AD DC in an isolated
# network namespace. This enables testing of:
# - Kerberos authentication via GSSAPI
# - NTLM authentication via winbind
# - SID to UID/GID mapping
#
# Requirements:
# - samba, samba-ad-dc, krb5-user packages
# - Root privileges (for network namespace)
#
# Usage:
#   ./ad_test_wrapper.sh <test_command> [args...]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_NAME="chimera_ad_$$"
NETNS_NAME="netns_${TEST_NAME}"
SAMBA_DIR="/tmp/samba_${TEST_NAME}"
REALM="TEST.LOCAL"
DOMAIN="TEST"
ADMIN_PASS="TestPass123!"
DC_IP="10.99.0.1"
CLIENT_IP="10.99.0.2"

log() {
    echo "[ad_test] $*" >&2
}

cleanup() {
    log "Cleaning up..."

    # Kill samba processes in namespace
    ip netns exec "${NETNS_NAME}" pkill -9 samba 2>/dev/null || true
    ip netns exec "${NETNS_NAME}" pkill -9 winbindd 2>/dev/null || true

    # Remove veth pair
    ip link delete "veth_${TEST_NAME}" 2>/dev/null || true

    # Remove namespace
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true

    # Clean up samba directory
    rm -rf "${SAMBA_DIR}" 2>/dev/null || true
}

trap cleanup EXIT

# Check requirements
check_requirements() {
    local missing=""

    for cmd in samba-tool kinit ip; do
        if ! command -v "$cmd" &>/dev/null; then
            missing="$missing $cmd"
        fi
    done

    if [ -n "$missing" ]; then
        log "ERROR: Missing required commands:$missing"
        log "Install with: apt-get install samba samba-ad-dc krb5-user iproute2"
        exit 1
    fi
}

setup_namespace() {
    log "Creating network namespace..."

    # Create namespace
    ip netns add "${NETNS_NAME}"

    # Create veth pair
    ip link add "veth_${TEST_NAME}" type veth peer name "veth_${TEST_NAME}_ns"

    # Move one end to namespace
    ip link set "veth_${TEST_NAME}_ns" netns "${NETNS_NAME}"

    # Configure host side
    ip addr add "${CLIENT_IP}/24" dev "veth_${TEST_NAME}"
    ip link set "veth_${TEST_NAME}" up

    # Configure namespace side
    ip netns exec "${NETNS_NAME}" ip addr add "${DC_IP}/24" dev "veth_${TEST_NAME}_ns"
    ip netns exec "${NETNS_NAME}" ip link set "veth_${TEST_NAME}_ns" up
    ip netns exec "${NETNS_NAME}" ip link set lo up
}

provision_samba_ad() {
    log "Provisioning Samba AD DC (this takes ~30 seconds)..."

    mkdir -p "${SAMBA_DIR}"/{etc,var/lib/samba,var/run/samba,var/log/samba}

    # Create minimal smb.conf for provisioning
    cat > "${SAMBA_DIR}/etc/smb.conf" <<EOF
[global]
    netbios name = DC
    realm = ${REALM}
    workgroup = ${DOMAIN}
    server role = active directory domain controller
    dns forwarder = 8.8.8.8

    private dir = ${SAMBA_DIR}/var/lib/samba/private
    state directory = ${SAMBA_DIR}/var/lib/samba
    cache directory = ${SAMBA_DIR}/var/lib/samba
    lock directory = ${SAMBA_DIR}/var/run/samba
    pid directory = ${SAMBA_DIR}/var/run/samba
    log file = ${SAMBA_DIR}/var/log/samba/log.%m

    # Disable unnecessary services for testing
    server services = ldap, cldap, kdc, drepl, winbindd, ntp_signd, kcc, dnsupdate, dns
    dcerpc endpoint servers = epmapper, wkssvc, samr, netlogon, lsarpc, drsuapi, dssetup, unixinfo, browser, eventlog6, backupkey, dnsserver

[sysvol]
    path = ${SAMBA_DIR}/var/lib/samba/sysvol
    read only = No

[netlogon]
    path = ${SAMBA_DIR}/var/lib/samba/sysvol/${REALM,,}/scripts
    read only = No
EOF

    # Provision the domain inside the namespace
    ip netns exec "${NETNS_NAME}" samba-tool domain provision \
        --realm="${REALM}" \
        --domain="${DOMAIN}" \
        --server-role=dc \
        --dns-backend=SAMBA_INTERNAL \
        --adminpass="${ADMIN_PASS}" \
        --configfile="${SAMBA_DIR}/etc/smb.conf" \
        --targetdir="${SAMBA_DIR}/var/lib/samba" \
        --option="private dir = ${SAMBA_DIR}/var/lib/samba/private" \
        --quiet

    log "Samba AD DC provisioned"
}

start_samba() {
    log "Starting Samba AD DC..."

    ip netns exec "${NETNS_NAME}" samba \
        --configfile="${SAMBA_DIR}/etc/smb.conf" \
        --option="private dir = ${SAMBA_DIR}/var/lib/samba/private" \
        --option="state directory = ${SAMBA_DIR}/var/lib/samba" \
        --option="interfaces = lo ${DC_IP}/24" \
        --option="bind interfaces only = yes" \
        --daemon

    # Wait for samba to be ready
    local retries=30
    while [ $retries -gt 0 ]; do
        if ip netns exec "${NETNS_NAME}" samba-tool dns query "${DC_IP}" "${REALM,,}" @ ALL \
            --configfile="${SAMBA_DIR}/etc/smb.conf" &>/dev/null; then
            log "Samba AD DC is ready"
            return 0
        fi
        sleep 1
        retries=$((retries - 1))
    done

    log "ERROR: Samba AD DC failed to start"
    return 1
}

create_test_users() {
    log "Creating test users..."

    # Create test users
    ip netns exec "${NETNS_NAME}" samba-tool user create testuser1 "Password1!" \
        --configfile="${SAMBA_DIR}/etc/smb.conf" --quiet
    ip netns exec "${NETNS_NAME}" samba-tool user create testuser2 "Password2!" \
        --configfile="${SAMBA_DIR}/etc/smb.conf" --quiet

    log "Test users created: testuser1, testuser2"
}

generate_keytab() {
    log "Generating keytab..."

    KEYTAB_FILE="${SAMBA_DIR}/chimera.keytab"

    ip netns exec "${NETNS_NAME}" samba-tool domain exportkeytab "${KEYTAB_FILE}" \
        --principal="cifs/dc.${REALM,,}@${REALM}" \
        --configfile="${SAMBA_DIR}/etc/smb.conf"

    chmod 644 "${KEYTAB_FILE}"

    log "Keytab generated: ${KEYTAB_FILE}"
    export KRB5_KTNAME="${KEYTAB_FILE}"
}

setup_krb5_conf() {
    log "Setting up krb5.conf..."

    KRB5_CONF="${SAMBA_DIR}/krb5.conf"

    cat > "${KRB5_CONF}" <<EOF
[libdefaults]
    default_realm = ${REALM}
    dns_lookup_realm = false
    dns_lookup_kdc = false

[realms]
    ${REALM} = {
        kdc = ${DC_IP}
        admin_server = ${DC_IP}
        default_domain = ${REALM,,}
    }

[domain_realm]
    .${REALM,,} = ${REALM}
    ${REALM,,} = ${REALM}
EOF

    export KRB5_CONFIG="${KRB5_CONF}"
    log "KRB5_CONFIG=${KRB5_CONF}"
}

obtain_ticket() {
    log "Obtaining Kerberos ticket for testuser1..."

    # Set up credential cache
    export KRB5CCNAME="${SAMBA_DIR}/krb5cc_testuser1"

    # Get a ticket for testuser1 (used by the SMB client for GSSAPI auth)
    if printf '%s' "Password1!" | kinit testuser1@${REALM} 2>/dev/null; then
        log "Obtained TGT for testuser1@${REALM}"
        log "KRB5CCNAME=${KRB5CCNAME}"
        klist 2>/dev/null || true
        return 0
    else
        log "WARNING: Failed to obtain Kerberos ticket (NTLM tests may still work)"
        return 0
    fi
}

run_test() {
    log "Running test: $*"

    # Export environment for test
    export AD_DC_IP="${DC_IP}"
    export AD_REALM="${REALM}"
    export AD_DOMAIN="${DOMAIN}"
    export AD_ADMIN_PASS="${ADMIN_PASS}"
    export WINBINDD_SOCKET_DIR="${SAMBA_DIR}/var/run/samba/winbindd"

    # Run the test command
    "$@"
}

main() {
    if [ $# -lt 1 ]; then
        echo "Usage: $0 <test_command> [args...]"
        echo ""
        echo "Runs a test command with a real Samba AD DC environment."
        echo "The AD DC runs in an isolated network namespace."
        echo ""
        echo "Environment variables exported to test:"
        echo "  AD_DC_IP         - IP address of the DC"
        echo "  AD_REALM         - Kerberos realm (e.g., TEST.LOCAL)"
        echo "  AD_DOMAIN        - NetBIOS domain name (e.g., TEST)"
        echo "  AD_ADMIN_PASS    - Administrator password"
        echo "  KRB5_CONFIG      - Path to krb5.conf"
        echo "  KRB5_KTNAME      - Path to keytab file"
        echo "  WINBINDD_SOCKET_DIR - Path to winbind socket"
        exit 1
    fi

    check_requirements
    setup_namespace
    provision_samba_ad
    start_samba
    create_test_users
    generate_keytab
    setup_krb5_conf
    obtain_ticket
    run_test "$@"
}

main "$@"
