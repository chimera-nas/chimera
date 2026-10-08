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
#define CHIMERA_VFS_CASEFOLD_MAX     1024

/* Which case table chimera_vfs_casefold_hash() folds through.  A module that
 * persists folded-name index keys records it with each case-folding
 * filesystem, and refuses to mount one whose index was built with another
 * table: its keys would no longer find their entries.  Bump it whenever the
 * table or the folding changes (the filesystems then need their index
 * rebuilt).
 *   1  Unicode 16.0.0 simple uppercase, BMP */
#define CHIMERA_VFS_CASEFOLD_VERSION 1

/* A hash of the folded name: equal for names that match case-insensitively. */
CHIMERA_VFS_SDK_EXPORT uint64_t
chimera_vfs_casefold_hash(
    const char *name,
    int         len);

/* Decode a UTF-8 name into at most max code points in out, each folded
 * through the same case table when fold is set, and return how many.  A byte
 * that does not decode becomes 0x110000 + its value, which no code point
 * equals.  For matchers, such as SMB wildcards, that work on characters. */
CHIMERA_VFS_SDK_EXPORT int
chimera_vfs_casefold_decode(
    const char *name,
    int         len,
    uint32_t   *out,
    int         max,
    int         fold);

/* Nonzero if the names match case-insensitively. */
CHIMERA_VFS_SDK_EXPORT int
chimera_vfs_name_equal_ci(
    const char *a,
    int         alen,
    const char *b,
    int         blen);
