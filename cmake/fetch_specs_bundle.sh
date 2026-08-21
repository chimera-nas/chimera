#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only
#
# Pull the prebuilt MBT trace bundle the chimera-nas/specs CI published for a
# given submodule commit and unpack it into the build tree.  Invoked by
# cmake/SpecsBundle.cmake only in the fetch path (clean submodule + a bundle
# available for its SHA); local spec development uses the in-tree build instead.
#
# Usage: fetch_specs_bundle.sh <oci-ref> <bundle-root>
#   <oci-ref>      e.g. ghcr.io/chimera-nas/specs:<sha>
#   <bundle-root>  destination dir; traces land in <bundle-root>/traces/<suite>/
# ORAS may override the oras binary (defaults to `oras` on PATH).

set -euo pipefail

REF="${1:?usage: fetch_specs_bundle.sh <oci-ref> <bundle-root>}"
ROOT="${2:?usage: fetch_specs_bundle.sh <oci-ref> <bundle-root>}"
ORAS="${ORAS:-oras}"

TRACES="$ROOT/traces"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

echo "fetch_specs_bundle: oras pull $REF"
# First attempt honors any configured registry auth (e.g. a CI `oras login`).
# The bundle is published public, so if that fails -- most often because a
# stale/absent docker credential helper (credsStore in ~/.docker/config.json)
# aborts auth resolution -- retry anonymously with an isolated empty docker
# config, which a public pull needs no credentials for.
if ! "$ORAS" pull "$REF" --output "$work"; then
    echo "fetch_specs_bundle: retrying anonymously (isolated docker config)" >&2
    anoncfg="$work/.anon-docker"
    mkdir -p "$anoncfg"
    printf '{}\n' > "$anoncfg/config.json"
    DOCKER_CONFIG="$anoncfg" "$ORAS" pull "$REF" --output "$work"
fi

tarball="$work/specs-traces.tar.gz"
if [ ! -f "$tarball" ]; then
    echo "fetch_specs_bundle: $tarball missing after oras pull" >&2
    exit 1
fi

# Replace any prior extraction so a re-fetch is a clean slate.
rm -rf "$TRACES"
mkdir -p "$TRACES"
tar xzf "$tarball" -C "$TRACES"

echo "fetch_specs_bundle: unpacked $REF into $TRACES"
