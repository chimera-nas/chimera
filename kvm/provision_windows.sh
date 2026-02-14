#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: provision_windows.sh <ssh_port> <ssh_key> <password>
#
# Provisions a running Windows Server VM over SSH:
# 1. Disables Windows Firewall (belt and suspenders)
# 2. Injects SSH public key for passwordless auth
#
# The VM's default SSH shell is PowerShell (configured in autounattend.xml).
# Commands are sent via -EncodedCommand (base64 UTF-16LE) to avoid quoting
# issues between bash, SSH, and Windows OpenSSH's PowerShell wrapping.

set -euo pipefail

SSH_PORT=$1
SSH_KEY=$2
PASSWORD=$3

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

# Run a PowerShell command on the Windows VM via SSH.
# Uses -EncodedCommand to avoid quoting issues with Windows OpenSSH.
run_ps() {
    local encoded
    encoded=$(printf '%s' "$1" | iconv -f utf-8 -t utf-16le | base64 -w0)
    sshpass -p "$PASSWORD" ssh $SSH_OPTS -p "$SSH_PORT" "Administrator@127.0.0.1" \
        "powershell -NoProfile -EncodedCommand $encoded"
}

echo "=== Provisioning Windows VM ==="

# Disable Windows Firewall (redundant with autounattend, but belt and suspenders)
echo "--- Disabling Windows Firewall ---"
run_ps 'Set-NetFirewallProfile -Profile Domain,Public,Private -Enabled False'

# Inject SSH public key for passwordless authentication during tests
echo "--- Injecting SSH public key ---"
SSH_PUBKEY=$(cat "${SSH_KEY}.pub")
run_ps "New-Item -Path C:\ProgramData\ssh -ItemType Directory -Force | Out-Null"
run_ps "Set-Content -Path C:\ProgramData\ssh\administrators_authorized_keys -Value '${SSH_PUBKEY}'"
run_ps "icacls C:\ProgramData\ssh\administrators_authorized_keys /inheritance:r /grant Administrators:F /grant SYSTEM:F"

# Verify passwordless SSH works
echo "--- Verifying passwordless SSH ---"
ssh $SSH_OPTS -i "$SSH_KEY" -p "$SSH_PORT" "Administrator@127.0.0.1" "echo SSH key auth working"

echo "=== Provisioning complete ==="
