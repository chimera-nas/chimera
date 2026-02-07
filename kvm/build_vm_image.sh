#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set -euo pipefail

OUTDIR=$1
DOCKERFILE=$2
SOURCE_DIR=$3
IMAGE_TAG="chimera-kvm-$(basename "$DOCKERFILE" | sed 's/Dockerfile\.//')"

echo "Building VM image: ${IMAGE_TAG}"

docker build -t "$IMAGE_TAG" -f "$DOCKERFILE" "$SOURCE_DIR"

CONTAINER_ID=$(docker create "$IMAGE_TAG")

cleanup() {
    docker rm -f "$CONTAINER_ID" 2>/dev/null || true
    rm -f "${OUTDIR}/rootfs.raw" "${OUTDIR}/rootfs.tar" 2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "$OUTDIR"

# Extract the kernel (resolve symlink inside the container)
VMLINUZ_PATH=$(docker run --rm "$IMAGE_TAG" readlink -f /boot/vmlinuz)
docker cp "${CONTAINER_ID}:${VMLINUZ_PATH}" "${OUTDIR}/vmlinuz"

# Export the filesystem to a qcow2
docker export "$CONTAINER_ID" > "${OUTDIR}/rootfs.tar"

truncate -s 2G "${OUTDIR}/rootfs.raw"
mkfs.ext4 -F "${OUTDIR}/rootfs.raw"

MOUNT_DIR=$(mktemp -d)
mount -o loop "${OUTDIR}/rootfs.raw" "$MOUNT_DIR"
tar xf "${OUTDIR}/rootfs.tar" -C "$MOUNT_DIR"
umount "$MOUNT_DIR"
rmdir "$MOUNT_DIR"
rm -f "${OUTDIR}/rootfs.tar"

qemu-img convert -f raw -O qcow2 "${OUTDIR}/rootfs.raw" "${OUTDIR}/rootfs.qcow2"
rm -f "${OUTDIR}/rootfs.raw"

echo "VM image built: ${OUTDIR}/vmlinuz ${OUTDIR}/rootfs.qcow2"
