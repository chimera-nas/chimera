#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Fetch a prebuilt KVM guest image (vmlinuz + initrd + rootfs.qcow2) published
# by chimera-nas/kvm-test-base as an OCI artifact, instead of building it.
#
# Usage: fetch_vm_image.sh <outdir> <variant>
#
# Environment:
#   KVM_IMAGE_REGISTRY  OCI registry base (default: ghcr.io/chimera-nas/kvm-test-base)
#   KVM_IMAGE_VERSION   version tag to pull (default: latest)
#
# The architecture suffix is derived from the host so the same source builds
# the right image on amd64 and arm64 runners.

set -euo pipefail

OUTDIR=$1
VARIANT=$2

REGISTRY=${KVM_IMAGE_REGISTRY:-ghcr.io/chimera-nas/kvm-test-base}
VERSION=${KVM_IMAGE_VERSION:-latest}

case "$(uname -m)" in
    x86_64)  ARCH=amd64 ;;
    aarch64) ARCH=arm64 ;;
    *)       ARCH=$(uname -m) ;;
esac

# Single 2-segment repo with the variant in the tag (registry-proxy friendly):
#   <registry>:<variant>-<version>-<arch>
REF="${REGISTRY}:${VARIANT}-${VERSION}-${ARCH}"

mkdir -p "$OUTDIR"
echo "Fetching KVM test image: ${REF}"

# oras pull restores the original file names (vmlinuz, initrd, rootfs.qcow2)
# into OUTDIR via the per-blob title annotations.
oras pull "$REF" --output "$OUTDIR"

# Sanity-check the expected payload landed.
for f in vmlinuz initrd rootfs.qcow2; do
    if [ ! -s "${OUTDIR}/${f}" ]; then
        echo "error: ${REF} did not yield ${f}" >&2
        exit 1
    fi
done
