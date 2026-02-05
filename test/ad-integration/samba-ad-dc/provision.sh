#!/bin/bash
# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Provision Samba AD DC for testing

set -e

REALM=${REALM:-TEST.LOCAL}
DOMAIN=${DOMAIN:-TEST}
ADMIN_PASSWORD=${ADMIN_PASSWORD:-TestPass123!}
DNS_FORWARDER=${DNS_FORWARDER:-8.8.8.8}

SAMBA_DATA=/var/lib/samba

# Check if already provisioned
if [ -f "$SAMBA_DATA/private/secrets.keytab" ]; then
    echo "Samba already provisioned, starting..."
    exec "$@"
fi

echo "Provisioning Samba AD DC..."
echo "  Realm: $REALM"
echo "  Domain: $DOMAIN"

# Clean up any existing configuration
rm -rf /etc/samba/smb.conf
rm -rf "$SAMBA_DATA"/*

# Provision the domain
samba-tool domain provision \
    --realm="$REALM" \
    --domain="$DOMAIN" \
    --adminpass="$ADMIN_PASSWORD" \
    --server-role=dc \
    --dns-backend=SAMBA_INTERNAL \
    --use-rfc2307

# Configure DNS forwarder
sed -i "s/dns forwarder.*/dns forwarder = $DNS_FORWARDER/" /etc/samba/smb.conf

# Copy Kerberos configuration
cp "$SAMBA_DATA/private/krb5.conf" /etc/krb5.conf

# Create test users
echo "Creating test users..."

samba-tool user create aduser1 'ADPassword1!' \
    --given-name="AD" \
    --surname="User1" \
    --mail-address="aduser1@$REALM"

samba-tool user create aduser2 'ADPassword2!' \
    --given-name="AD" \
    --surname="User2" \
    --mail-address="aduser2@$REALM"

# Create test groups
samba-tool group add testgroup1
samba-tool group add testgroup2

# Add users to groups
samba-tool group addmembers testgroup1 aduser1,aduser2
samba-tool group addmembers testgroup2 aduser2

# Export keytab for the test runner
samba-tool domain exportkeytab /var/lib/samba/private/test.keytab \
    --principal="cifs/$(hostname -f)"

chmod 644 /var/lib/samba/private/test.keytab

echo "Provisioning complete!"
echo "Test users:"
echo "  aduser1 / ADPassword1!"
echo "  aduser2 / ADPassword2!"

# Start Samba
exec "$@"
