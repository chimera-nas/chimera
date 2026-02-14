#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Usage: build_windows_image.sh <output_dir> <windows_iso_path> <source_dir>
#
# Builds a provisioned Windows Server 2025 QEMU image for KVM testing.
#
# Steps:
# 1. Download virtio-win drivers ISO
# 2. Build a modified Windows ISO (Autounattend.xml + ei.cfg + noprompt boot)
# 3. Run fully unattended Windows installation via QEMU/UEFI
# 4. Provision the VM over SSH (MSYS2, build tools, test suites)
# 5. Output: windows.qcow2, OVMF_VARS.fd, ssh_key
#
# Dependencies: qemu-system-x86_64, OVMF firmware, xorriso,
#               sshpass, ssh-keygen, curl, internet access

set -euo pipefail

OUTDIR=$1
WINDOWS_ISO=$2
SOURCE_DIR=$3

OVMF_CODE="${OVMF_CODE_PATH:-/usr/share/OVMF/OVMF_CODE_4M.fd}"
DISK_IMAGE="${OUTDIR}/windows.qcow2"
OVMF_VARS="${OUTDIR}/OVMF_VARS.fd"
SSH_KEY="${OUTDIR}/ssh_key"
VIRTIO_ISO="${OUTDIR}/virtio-win.iso"
MODIFIED_ISO="${OUTDIR}/windows_unattended.iso"
SSH_PORT=2222
ADMIN_PASSWORD="P@ssw0rd!"
QEMU_PID=""

SESSION_DIR=$(mktemp -d /tmp/kvm_windows_build_XXXXXX)

cleanup() {
    if [ -n "$QEMU_PID" ]; then
        kill "$QEMU_PID" 2>/dev/null || true
        wait "$QEMU_PID" 2>/dev/null || true
    fi
    rm -rf "$SESSION_DIR"
}
trap cleanup EXIT

mkdir -p "$OUTDIR"

echo "=== Building Windows Server 2025 KVM test image ==="
echo "Output directory: $OUTDIR"
echo "Windows ISO: $WINDOWS_ISO"

# Verify Windows ISO exists
if [ ! -f "$WINDOWS_ISO" ]; then
    echo "ERROR: Windows ISO not found: $WINDOWS_ISO"
    exit 1
fi

# Verify OVMF firmware exists
if [ ! -f "$OVMF_CODE" ]; then
    echo "ERROR: OVMF firmware not found: $OVMF_CODE"
    echo "Install with: apt-get install ovmf"
    exit 1
fi

# Verify xorriso is available
if ! command -v xorriso &>/dev/null; then
    echo "ERROR: xorriso not found. Install with: apt-get install xorriso"
    exit 1
fi

# Generate SSH keypair for provisioning and test access
if [ ! -f "$SSH_KEY" ]; then
    echo "--- Generating SSH keypair ---"
    ssh-keygen -t ed25519 -f "$SSH_KEY" -N "" -C "chimera-kvm-test"
fi

# Download virtio-win drivers ISO if not cached
if [ ! -f "$VIRTIO_ISO" ]; then
    echo "--- Downloading virtio-win drivers ---"
    curl -L -o "$VIRTIO_ISO" \
        "https://fedorapeople.org/groups/virt/virtio-win/direct-downloads/stable-virtio/virtio-win.iso"
fi

# Create a fresh copy of OVMF_VARS for this image (UEFI variable store)
echo "--- Preparing OVMF variable store ---"
OVMF_VARS_TEMPLATE="${OVMF_CODE_PATH:-/usr/share/OVMF/OVMF_CODE_4M.fd}"
OVMF_VARS_TEMPLATE="${OVMF_VARS_TEMPLATE/OVMF_CODE/OVMF_VARS}"
if [ ! -f "$OVMF_VARS_TEMPLATE" ]; then
    for candidate in \
        /usr/share/OVMF/OVMF_VARS_4M.fd \
        /usr/share/OVMF/OVMF_VARS.fd \
        /usr/share/edk2/ovmf/OVMF_VARS.fd \
        /usr/share/qemu/OVMF_VARS.fd; do
        if [ -f "$candidate" ]; then
            OVMF_VARS_TEMPLATE="$candidate"
            break
        fi
    done
fi
cp "$OVMF_VARS_TEMPLATE" "$OVMF_VARS"

# Build a modified Windows ISO with Autounattend.xml embedded
# This approach:
# - Embeds the answer file directly on the install media (most reliable detection)
# - Uses efisys_noprompt.bin to skip "Press any key to boot from CD or DVD"
# - Adds ei.cfg to bypass the "Choose a licensing method" dialog
if [ ! -f "$MODIFIED_ISO" ]; then
    echo "--- Building modified Windows ISO ---"
    ISO_MOUNT="${SESSION_DIR}/iso_mount"
    ADDONS_DIR="${SESSION_DIR}/addons"
    mkdir -p "$ISO_MOUNT" "$ADDONS_DIR"

    mount -o loop,ro "$WINDOWS_ISO" "$ISO_MOUNT"

    cp "${SOURCE_DIR}/kvm/autounattend.xml" "${ADDONS_DIR}/Autounattend.xml"

    # ei.cfg bypasses the "Choose a licensing method" dialog on evaluation ISOs
    mkdir -p "${ADDONS_DIR}/sources"
    cat > "${ADDONS_DIR}/sources/ei.cfg" << 'EICFG'
[Channel]
Retail
EICFG

    # Rebuild ISO: efisys_noprompt.bin for silent UEFI boot
    xorriso -as mkisofs \
        -iso-level 4 \
        -disable-deep-relocation \
        -untranslated-filenames \
        -b boot/etfsboot.com \
        -no-emul-boot \
        -boot-load-size 8 \
        -eltorito-alt-boot \
        -eltorito-platform efi \
        -b efi/microsoft/boot/efisys_noprompt.bin \
        -no-emul-boot \
        -o "$MODIFIED_ISO" \
        "$ISO_MOUNT" "$ADDONS_DIR"

    umount "$ISO_MOUNT"
    echo "Modified ISO created: $(du -h "$MODIFIED_ISO" | cut -f1)"
fi

# Create the disk image
echo "--- Creating disk image ---"
qemu-img create -f qcow2 "$DISK_IMAGE" 40G

# VNC port for monitoring (connect with VNC viewer to localhost:VNC_PORT)
VNC_PORT="${KVM_VNC_PORT:-5900}"
VNC_DISPLAY=$((VNC_PORT - 5900))

# Single QEMU session: install + provision
# Windows installs unattended (multiple reboots), OpenSSH starts after OOBE,
# then we SSH in to provision and shut down.
echo "=== Starting Windows installation and provisioning ==="
echo "This will take 15-30 minutes..."
echo "Monitor via VNC at localhost:${VNC_PORT}"

qemu-system-x86_64 \
    -enable-kvm -smp 4 -m 4G -cpu host \
    -machine q35 \
    -global ICH9-LPC.disable_s3=1 \
    -drive if=pflash,format=raw,readonly=on,file="$OVMF_CODE" \
    -drive if=pflash,format=raw,file="$OVMF_VARS" \
    -drive file="$DISK_IMAGE",if=virtio,format=qcow2 \
    -drive file="$MODIFIED_ISO",media=cdrom,readonly=on \
    -drive file="$VIRTIO_ISO",media=cdrom,readonly=on \
    -netdev user,id=net0,hostfwd=tcp::${SSH_PORT}-:22 \
    -device virtio-net-pci,netdev=net0 \
    -vnc :${VNC_DISPLAY} &
QEMU_PID=$!

# Wait for SSH to become available (installation complete + OpenSSH running)
echo "--- Waiting for SSH (port ${SSH_PORT}) ---"
echo "Windows will reboot multiple times during installation..."
SSH_READY=0
for i in $(seq 1 600); do
    if sshpass -p "$ADMIN_PASSWORD" ssh \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR \
        -o ConnectTimeout=5 \
        -p "$SSH_PORT" \
        "Administrator@127.0.0.1" "echo SSH ready" 2>/dev/null; then
        echo "SSH available after ~${i} seconds"
        SSH_READY=1
        break
    fi
    if ! kill -0 "$QEMU_PID" 2>/dev/null; then
        echo "ERROR: QEMU exited unexpectedly"
        QEMU_PID=""
        exit 1
    fi
    sleep 1
done

if [ "$SSH_READY" -ne 1 ]; then
    echo "ERROR: SSH did not become available within 10 minutes"
    exit 1
fi

echo "=== Windows installation complete, starting provisioning ==="

# Run provisioning script
bash "${SOURCE_DIR}/kvm/provision_windows.sh" "$SSH_PORT" "$SSH_KEY" "$ADMIN_PASSWORD"

# Shut down the VM cleanly
echo "--- Shutting down VM ---"
sshpass -p "$ADMIN_PASSWORD" ssh \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    -o LogLevel=ERROR \
    -p "$SSH_PORT" \
    "Administrator@127.0.0.1" \
    'Stop-Computer -Force' || true

# Wait for QEMU to exit
for i in $(seq 1 60); do
    if ! kill -0 "$QEMU_PID" 2>/dev/null; then
        break
    fi
    sleep 1
done
kill "$QEMU_PID" 2>/dev/null || true
wait "$QEMU_PID" 2>/dev/null || true
QEMU_PID=""

echo "=== Windows image built successfully ==="
echo "  Disk image: ${DISK_IMAGE}"
echo "  OVMF vars:  ${OVMF_VARS}"
echo "  SSH key:     ${SSH_KEY}"
