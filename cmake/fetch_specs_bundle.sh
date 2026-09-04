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
# Usage: fetch_specs_bundle.sh <oci-ref> <bundle-root> [expected-sha]
#   <oci-ref>      e.g. ghcr.io/chimera-nas/specs:<sha>
#   <bundle-root>  destination dir; traces land in <bundle-root>/traces/<suite>/
#   [expected-sha] the submodule commit this build is pinned to; when given,
#                  the bundle must say it was generated from it
# ORAS may override the oras binary (defaults to `oras` on PATH).

set -euo pipefail

REF="${1:?usage: fetch_specs_bundle.sh <oci-ref> <bundle-root> [expected-sha]}"
ROOT="${2:?usage: fetch_specs_bundle.sh <oci-ref> <bundle-root> [expected-sha]}"
EXPECT_SHA="${3:-}"
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

# Provenance, not just arrival.  The bundle's manifest records the specs commit
# its traces were generated from, and that is checked against the commit this
# build is pinned to rather than inferred from the tag it was fetched under.
#
# Today the two cannot disagree: the reference is built from the submodule SHA,
# so a tag mismatch is impossible.  The check is here for the coordinate that
# CAN disagree -- specs also publishes release-versioned bundles, and a version
# names the last tagged commit, so anything resolving one while pinned past
# that tag would get an older corpus and replay it against models it was not
# generated from.  That failure is silent, which is the only kind worth
# spending a check on; a corpus that does not match its models produces
# confident, wrong results.
if [ -n "$EXPECT_SHA" ]; then
    manifest="$TRACES/manifest.json"

    if [ ! -f "$manifest" ]; then
        # Bundles published before the manifest carried provenance have no
        # source_sha.  Say so and continue: refusing them would break every
        # older pin for a check that could not have been satisfied then.
        echo "fetch_specs_bundle: $REF has no manifest.json; skipping the" \
             "provenance check (bundle predates it)" >&2
    else
        got="$(sed -n 's/.*"source_sha"[[:space:]]*:[[:space:]]*"\([0-9a-f]*\)".*/\1/p' \
               "$manifest" | head -1)"

        if [ -z "$got" ]; then
            echo "fetch_specs_bundle: $REF records no source_sha; skipping the" \
                 "provenance check (bundle predates it)" >&2
        elif [ "$got" != "$EXPECT_SHA" ]; then
            echo "fetch_specs_bundle: $REF was generated from $got, but this" \
                 "build is pinned to $EXPECT_SHA -- refusing to replay a corpus" \
                 "against models it was not generated from" >&2
            rm -rf "$TRACES"
            exit 1
        else
            echo "fetch_specs_bundle: provenance ok (generated from $got)"
        fi
    fi
fi

echo "fetch_specs_bundle: unpacked $REF into $TRACES"
