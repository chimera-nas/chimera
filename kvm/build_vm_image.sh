#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set -euo pipefail

OUTDIR=$1
DOCKERFILE=$2
SOURCE_DIR=$3
shift 3

# Derive Docker image tag from output directory basename
IMAGE_TAG="chimera-kvm-$(basename "$OUTDIR")"

# Build extra --build-arg flags from remaining KEY=VALUE arguments
BUILD_ARGS=""
for arg in "$@"; do
    BUILD_ARGS="${BUILD_ARGS} --build-arg ${arg}"
done

echo "Building VM image: ${IMAGE_TAG}"

docker build ${DOCKER_MIRROR:+--build-arg DOCKER_MIRROR=$DOCKER_MIRROR} ${BUILD_ARGS} -t "$IMAGE_TAG" -f "$DOCKERFILE" "$SOURCE_DIR"

CONTAINER_ID=$(docker create "$IMAGE_TAG")

EXTRACT_DIR=""

cleanup() {
    docker rm -f "$CONTAINER_ID" 2>/dev/null || true
    rm -f "${OUTDIR}/rootfs.raw" 2>/dev/null || true
    if [ -n "$EXTRACT_DIR" ]; then
        rm -rf "$EXTRACT_DIR" 2>/dev/null || true
    fi
}
trap cleanup EXIT

mkdir -p "$OUTDIR"

# Extract the kernel and initrd (resolve symlinks inside the container)
VMLINUZ_PATH=$(docker run --rm "$IMAGE_TAG" readlink -f /boot/vmlinuz)
docker cp "${CONTAINER_ID}:${VMLINUZ_PATH}" "${OUTDIR}/vmlinuz"
INITRD_PATH=$(docker run --rm "$IMAGE_TAG" readlink -f /boot/initrd)
docker cp "${CONTAINER_ID}:${INITRD_PATH}" "${OUTDIR}/initrd"

# Export the container filesystem and populate an ext4 image directly.
# Uses mkfs.ext4 -d to avoid loop devices, which are a limited resource
# when multiple images build in parallel.
EXTRACT_DIR=$(mktemp -d)
docker export "$CONTAINER_ID" | tar xf - -C "$EXTRACT_DIR"

truncate -s 4G "${OUTDIR}/rootfs.raw"
mkfs.ext4 -F -d "$EXTRACT_DIR" "${OUTDIR}/rootfs.raw"
rm -rf "$EXTRACT_DIR"
EXTRACT_DIR=""

qemu-img convert -f raw -O qcow2 "${OUTDIR}/rootfs.raw" "${OUTDIR}/rootfs.qcow2"
rm -f "${OUTDIR}/rootfs.raw"

echo "VM image built: ${OUTDIR}/vmlinuz ${OUTDIR}/rootfs.qcow2"
