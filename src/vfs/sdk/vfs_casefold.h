// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/* Case-insensitive name matching, the way Windows matches names: each code
 * point through Unicode's simple uppercase mapping (NTFS's upcase table).
 * A backend serving SMB clients (CHIMERA_VFS_AUTH_ATTR credentials) matches
 * names this way when an exact match misses; NFS and POSIX callers keep exact
 * matching.  Names are UTF-8; bytes that do not decode compare as they are. */

#include <stdint.h>

#include "vfs_sdk_export.h"

/* Longest folded name the hash covers in full (a component is at most 255
 * UTF-16 code units, at most 1020 bytes folded). */
#define CHIMERA_VFS_CASEFOLD_MAX 1024

/* A hash of the folded name: equal for names that match case-insensitively. */
CHIMERA_VFS_SDK_EXPORT uint64_t
chimera_vfs_casefold_hash(
    const char *name,
    int         len);

/* Nonzero if the names match case-insensitively. */
CHIMERA_VFS_SDK_EXPORT int
chimera_vfs_name_equal_ci(
    const char *a,
    int         alen,
    const char *b,
    int         blen);
