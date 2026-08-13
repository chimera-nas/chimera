#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
#
# Enforce the VFS module SDK boundary: backend modules may consume the SDK
# (src/vfs/sdk) plus the short allowlist below -- never chimera-internal
# headers like vfs/vfs.h or vfs/vfs_internal.h.
#
# The test the allowlist has to pass is not "is this header tidy" but "would
# a third-party backend under a different license need it to interoperate?"
# If yes, it belongs in vfs/sdk/ where that module can actually get it; the
# entries that remain below are in-tree conveniences no out-of-tree module
# would reach for.  examples/vfs_module is the enforcement of the other half:
# it compiles with ONLY vfs/sdk on its include path, so anything the SDK
# needs but does not contain breaks that build.
#
# Exempt from the check:
#   - root: the pseudo-filesystem is VFS-core plumbing, not a backend.
#   - nfs:  the NFS client backend re-exports mounts and reaches into the
#           open cache / request internals; untangling it is future work.

set -u
cd "$(dirname "$0")/.."

MODULES="memfs linux io_uring cairn diskfs smb memkv sqlite"

# Allowed include prefixes/paths for module code, beyond headers in the
# module's own directory (includes with no directory component):
#   vfs/sdk/            the module SDK
#   evpl/               libevpl public API
#
# The remaining entries are deliberate in-tree conveniences.  Each one is
# here because an out-of-tree module would NOT need it to interoperate --
# anything a third-party backend requires belongs in vfs/sdk/ instead:
#
#   common/             the project's C utility layer (logging, rbtree,
#                       container_of, iovec cursors).  A module outside this
#                       tree brings its own; module-facing logging is in
#                       sdk/vfs_log.h.
#   vfs/vfs_fsid.h      st_dev -> stable fsid, specific to Linux passthrough.
#                       A backend that is not passthrough mints its own fsid.
#   vfs/vfs_clock.h     the process-wide tick clock (virtual in deterministic
#                       tests).  memfs stamps its lease-recall deadlines with
#                       it so the in-tree tests stay deterministic; an
#                       out-of-tree arbiter brings its own clock (the core
#                       never interprets a backend's deadlines).
#   server/smb/smb2.h   SMB2 wire vocabulary, needed to speak SMB to a third
#                       party -- not to interoperate with chimera.
#   ../linux/           io_uring is the linux passthrough backend with a
#                       different I/O engine, not an independent module.
ALLOWED='^(vfs/sdk/|common/|evpl/|\.\./linux/|vfs/vfs_fsid\.h$|vfs/vfs_clock\.h$|server/smb/smb2\.h$)'

fail=0

for mod in $MODULES; do
    dir="src/vfs/$mod"

    [ -d "$dir" ] || continue

    while IFS=: read -r file line inc; do
        # Strip everything but the quoted include path.
        inc=$(printf '%s' "$inc" | sed -E 's/.*#include[[:space:]]*"([^"]+)".*/\1/')

        case "$inc" in
            */*) ;;
            *) continue ;;   # module-local header
        esac

        if printf '%s' "$inc" | grep -qE "$ALLOWED"; then
            continue
        fi

        echo "VFS SDK boundary violation: $file:$line includes \"$inc\"" >&2
        fail=1
    done < <(grep -rnE '#include[[:space:]]*"' "$dir" --include='*.c' --include='*.h' \
             | grep -v "$dir/tests/")
done

if [ "$fail" -ne 0 ]; then
    echo "" >&2
    echo "Backend modules must include only vfs/sdk/, the backend helper" >&2
    echo "libraries, and common/ utilities (see scripts/check_vfs_sdk_includes.sh)." >&2
    exit 1
fi

echo "VFS SDK include boundary clean."
