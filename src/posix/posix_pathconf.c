// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <unistd.h>
#include <limits.h>

#include "posix_internal.h"

/*
 * pathconf(3)/fpathconf(3) for the Chimera POSIX client.
 *
 * The VFS does not expose per-file configurable limits, so these return the
 * filesystem-wide constants the Chimera VFS enforces.  CHIMERA_VFS_PATH_MAX is
 * the wire/path buffer limit shared by every backend; NAME_MAX mirrors the
 * common POSIX value.  Callers (e.g. conformance suites) use these to size
 * maximal names/paths for ENAMETOOLONG testing.
 */

#ifndef CHIMERA_POSIX_NAME_MAX
#define CHIMERA_POSIX_NAME_MAX 255
#endif /* ifndef CHIMERA_POSIX_NAME_MAX */

#ifndef CHIMERA_POSIX_LINK_MAX
#define CHIMERA_POSIX_LINK_MAX 32767
#endif /* ifndef CHIMERA_POSIX_LINK_MAX */

static long
chimera_posix_pathconf_value(int name)
{
    switch (name) {
        case _PC_NAME_MAX:
            return CHIMERA_POSIX_NAME_MAX;
        case _PC_PATH_MAX:
            return CHIMERA_VFS_PATH_MAX;
        case _PC_LINK_MAX:
            return CHIMERA_POSIX_LINK_MAX;
#ifdef _PC_SYMLINK_MAX
        case _PC_SYMLINK_MAX:
            return CHIMERA_VFS_PATH_MAX;
#endif /* ifdef _PC_SYMLINK_MAX */
        case _PC_NO_TRUNC:
            return 1;
        case _PC_CHOWN_RESTRICTED:
            return 1;
        default:
            errno = EINVAL;
            return -1;
    } /* switch */
} /* chimera_posix_pathconf_value */

SYMBOL_EXPORT long
chimera_posix_pathconf(
    const char *path,
    int         name)
{
    (void) path;
    return chimera_posix_pathconf_value(name);
} /* chimera_posix_pathconf */

SYMBOL_EXPORT long
chimera_posix_fpathconf(
    int fd,
    int name)
{
    (void) fd;
    return chimera_posix_pathconf_value(name);
} /* chimera_posix_fpathconf */
