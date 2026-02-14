#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: kvm_smb_windows_test_wrapper.sh <windows.qcow2> <OVMF_VARS.fd> <OVMF_CODE.fd>
#                                         <ssh_key> <chimera_binary> <backend> <test_cmd>
#
# Orchestrates a chimera SMB server + Windows QEMU VM to run tests over SMB.
# 1. Generates chimera config for the given backend
# 2. Creates a network namespace with TAP device
# 3. Starts chimera daemon in the netns (SMB on 10.0.0.1:445)
# 4. Boots Windows QEMU VM (UEFI) with TAP + user-mode networking (SSH)
# 5. Maps SMB share via 'net use', runs PowerShell test command via SSH
# 6. Captures exit code and cleans up

set -u

DISK_IMAGE=$1; shift
OVMF_VARS_TEMPLATE=$1; shift
OVMF_CODE=$1; shift
SSH_KEY=$1; shift
CHIMERA_BINARY=$1; shift
BACKEND=$1; shift
TEST_CMD_ARG="$*"

NETNS_NAME="kvm_smbw_$$_$(date +%s%N)"
TAP_NAME="tap_$$"
BUILD_DIR=$(dirname "$(dirname "$CHIMERA_BINARY")")
SESSION_DIR=$(mktemp -d "${BUILD_DIR}/kvm_smbw_session_XXXXXX")
CONFIG_FILE="${SESSION_DIR}/chimera.json"
OVMF_VARS="${SESSION_DIR}/OVMF_VARS.fd"
CHIMERA_PID=""
QEMU_PID=""
TCPDUMP_PID=""
PCAP_FILE="${KVM_PCAP_FILE:-}"

# SSH settings — SSH is forwarded via QEMU user-mode networking on a second NIC
SSH_PORT=10022
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10"

cleanup() {
    if [ -n "$QEMU_PID" ]; then
        kill "$QEMU_PID" 2>/dev/null || true
        wait "$QEMU_PID" 2>/dev/null || true
    fi
    if [ -n "$TCPDUMP_PID" ]; then
        kill "$TCPDUMP_PID" 2>/dev/null || true
        wait "$TCPDUMP_PID" 2>/dev/null || true
    fi
    if [ -n "$CHIMERA_PID" ]; then
        kill "$CHIMERA_PID" 2>/dev/null || true
        wait "$CHIMERA_PID" 2>/dev/null || true
    fi
    ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    rm -rf "$SESSION_DIR"
}
trap cleanup EXIT

# Generate chimera config based on backend (same logic as Linux SMB wrapper)
generate_config() {
    local mount_path="/"
    local vfs_section=""

    case "$BACKEND" in
        linux|io_uring)
            mount_path="$SESSION_DIR/data"
            mkdir -p "$SESSION_DIR/data"
            ;;
        memfs)
            mount_path="/"
            ;;
        demofs_io_uring|demofs_aio)
            local device_type="io_uring"
            if [ "$BACKEND" = "demofs_aio" ]; then
                device_type="libaio"
            fi
            local devices_json=""
            for i in $(seq 0 9); do
                local device_path="${SESSION_DIR}/device-${i}.img"
                truncate -s 256G "$device_path"
                if [ $i -gt 0 ]; then
                    devices_json="${devices_json},"
                fi
                devices_json="${devices_json}{\"type\":\"$device_type\",\"size\":1,\"path\":\"$device_path\"}"
            done
            mount_path="/"
            BACKEND="demofs"
            vfs_section="\"vfs\": {
                \"demofs\": {
                    \"config\": {\"devices\":[$devices_json]}
                }
            },"
            ;;
        cairn)
            mount_path="/"
            vfs_section="\"vfs\": {
                \"cairn\": {
                    \"config\": {\"initialize\":true,\"path\":\"$SESSION_DIR\"}
                }
            },"
            ;;
    esac

    cat > "$CONFIG_FILE" << EOF
{
    "server": {
        "threads": 4,
        "delegation_threads": 4,
        $vfs_section
        "external_portmap": false
    },
    "mounts": {
        "share": {
            "module": "$BACKEND",
            "path": "$mount_path"
        }
    },
    "shares": {
        "share": {
            "path": "/share"
        }
    },
    "users": [
        {
            "username": "root",
            "smbpasswd": "secret",
            "uid": 0,
            "gid": 0
        }
    ]
}
EOF
}

generate_config

# Create a session-local copy of OVMF_VARS (each VM run needs its own writable copy)
cp "$OVMF_VARS_TEMPLATE" "$OVMF_VARS"

# Create network namespace
ip netns add "${NETNS_NAME}"
ip netns exec "${NETNS_NAME}" ip link set lo up

# Create TAP device inside the netns (for chimera <-> Windows SMB traffic)
ip netns exec "${NETNS_NAME}" ip tuntap add dev "${TAP_NAME}" mode tap
ip netns exec "${NETNS_NAME}" ip addr add 10.0.0.1/24 dev "${TAP_NAME}"
ip netns exec "${NETNS_NAME}" ip link set "${TAP_NAME}" up

# Optionally start tcpdump to capture traffic (set KVM_PCAP_FILE to enable)
if [ -n "$PCAP_FILE" ]; then
    ip netns exec "${NETNS_NAME}" tcpdump -i "${TAP_NAME}" -w "$PCAP_FILE" -s 0 &
    TCPDUMP_PID=$!
    sleep 0.5
fi

# Start chimera daemon in the netns
CHIMERA_LOG="${SESSION_DIR}/chimera_stderr.log"
ip netns exec "${NETNS_NAME}" env \
    ASAN_OPTIONS="detect_leaks=0:handle_abort=2:print_cmdline=1" \
    "$CHIMERA_BINARY" ${CHIMERA_DEBUG:+-d} -c "$CONFIG_FILE" \
    2>"$CHIMERA_LOG" &
CHIMERA_PID=$!

# Wait for SMB port to be ready
for i in $(seq 1 30); do
    if ip netns exec "${NETNS_NAME}" bash -c "echo > /dev/tcp/10.0.0.1/445" 2>/dev/null; then
        break
    fi
    if ! kill -0 "$CHIMERA_PID" 2>/dev/null; then
        echo "chimera daemon exited prematurely"
        exit 1
    fi
    sleep 0.1
done

# Boot Windows QEMU VM inside the netns
# Two NICs:
#   - virtio-net on TAP (10.0.0.x) for SMB traffic to chimera
#   - e1000 on user-mode networking with SSH port forward for control
ip netns exec "${NETNS_NAME}" qemu-system-x86_64 \
    -enable-kvm -smp 4 -m 4G -cpu host \
    -machine q35 \
    -drive if=pflash,format=raw,readonly=on,file="$OVMF_CODE" \
    -drive if=pflash,format=raw,file="$OVMF_VARS" \
    -drive file="$DISK_IMAGE",if=virtio,format=qcow2,snapshot=on \
    -netdev tap,id=net0,ifname="${TAP_NAME}",script=no,downscript=no \
    -device virtio-net-pci,netdev=net0 \
    -netdev user,id=net1,hostfwd=tcp:127.0.0.1:${SSH_PORT}-:22 \
    -device e1000,netdev=net1 \
    -display none \
    -serial null \
    -no-reboot &
QEMU_PID=$!

# Wait for SSH to become available (Windows boot takes longer)
echo "Waiting for Windows VM SSH..."
SSH_READY=0
for i in $(seq 1 120); do
    if ip netns exec "${NETNS_NAME}" ssh $SSH_OPTS \
        -i "$SSH_KEY" -p "$SSH_PORT" \
        "Administrator@127.0.0.1" "echo ready" 2>/dev/null; then
        SSH_READY=1
        echo "SSH available after ~${i} seconds"
        break
    fi
    if ! kill -0 "$QEMU_PID" 2>/dev/null; then
        echo "QEMU exited during boot"
        QEMU_PID=""
        exit 1
    fi
    sleep 1
done

if [ "$SSH_READY" -ne 1 ]; then
    echo "Timed out waiting for SSH"
    exit 1
fi

# Helper: run raw command via SSH inside the network namespace
vm_ssh() {
    ip netns exec "${NETNS_NAME}" ssh $SSH_OPTS \
        -i "$SSH_KEY" -p "$SSH_PORT" \
        "Administrator@127.0.0.1" "$@"
}

# Helper: run PowerShell command via SSH using -EncodedCommand (quoting-safe)
vm_ps() {
    local encoded
    encoded=$(printf '%s' "$1" | iconv -f utf-8 -t utf-16le | base64 -w0)
    vm_ssh "powershell -NoProfile -EncodedCommand $encoded"
}

# Configure the Windows VM's TAP NIC with a static IP
echo "Configuring Windows network..."
vm_ps '
$tapAdapter = Get-NetAdapter | Where-Object { $_.InterfaceDescription -like "*VirtIO*" } | Select-Object -First 1
New-NetIPAddress -InterfaceIndex $tapAdapter.ifIndex -IPAddress 10.0.0.2 -PrefixLength 24 -DefaultGateway 10.0.0.1 -ErrorAction SilentlyContinue
Set-DnsClientServerAddress -InterfaceIndex $tapAdapter.ifIndex -ServerAddresses @() -ErrorAction SilentlyContinue
'

# Map the SMB share and run the test in a single session
# (Windows drive mappings are per-logon session, so net use + test must share one)
echo "Mapping SMB share and running test..."
echo "Test command: $TEST_CMD_ARG"
vm_ps "net use Z: \\\\10.0.0.1\\share /user:root secret
if (\$LASTEXITCODE -ne 0) { Write-Error 'net use failed'; exit 1 }
$TEST_CMD_ARG"
EXIT_CODE=$?

echo "Test exit code: $EXIT_CODE"

# Show chimera debug output if present
if [ -f "$CHIMERA_LOG" ]; then
    echo "=== Chimera stderr (last 100 lines) ==="
    tail -100 "$CHIMERA_LOG"
fi

# Shut down the VM
vm_ps 'Stop-Computer -Force' || true
for i in $(seq 1 15); do
    if ! kill -0 "$QEMU_PID" 2>/dev/null; then
        QEMU_PID=""
        break
    fi
    sleep 1
done

exit $EXIT_CODE
