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

# If KVM_CACHE_REGISTRY is set, use docker buildx with a registry-backed
# cache so the (expensive) inner Dockerfile.kvm build can share layers across
# CI jobs. Without it, fall back to a plain local-cache `docker build`.
if [ -n "${KVM_CACHE_REGISTRY:-}" ]; then
    case "$(uname -m)" in
        x86_64)  CACHE_ARCH=amd64 ;;
        aarch64) CACHE_ARCH=arm64 ;;
        *)       CACHE_ARCH=$(uname -m) ;;
    esac
    CACHE_REF="${KVM_CACHE_REGISTRY}/${IMAGE_TAG}:${CACHE_ARCH}"

    # cache-to=type=registry,mode=max requires the docker-container driver;
    # the default "docker" driver only supports inline cache. Create the
    # builder lazily and reuse it across the 4 KVM image targets.
    if ! docker buildx inspect chimera-kvm-builder >/dev/null 2>&1; then
        docker buildx create --name chimera-kvm-builder --driver docker-container >/dev/null
    fi

    set -x
    docker buildx build \
        --builder chimera-kvm-builder \
        ${DOCKER_MIRROR:+--build-arg DOCKER_MIRROR=$DOCKER_MIRROR} \
        ${APT_MIRROR:+--build-arg APT_MIRROR=$APT_MIRROR} \
        ${BUILD_ARGS} \
        --cache-from "type=registry,ref=${CACHE_REF}" \
        --cache-to "type=registry,ref=${CACHE_REF},mode=max,image-manifest=true,ignore-error=true" \
        --load \
        -t "$IMAGE_TAG" \
        -f "$DOCKERFILE" \
        "$SOURCE_DIR"
    set +x
else
    set -x
    docker build \
        ${DOCKER_MIRROR:+--build-arg DOCKER_MIRROR=$DOCKER_MIRROR} \
        ${APT_MIRROR:+--build-arg APT_MIRROR=$APT_MIRROR} \
        ${BUILD_ARGS} \
        -t "$IMAGE_TAG" \
        -f "$DOCKERFILE" \
        "$SOURCE_DIR"
    set +x
fi

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
