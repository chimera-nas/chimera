// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <string.h>

/*
 * The "user." namespace is Chimera's protocol-exported extended-attribute
 * keyspace, shared by every protocol server that surfaces user xattrs:
 *
 *   - NFSv4.2 (RFC 8276) carries names in the user namespace with no prefix on
 *     the wire; the NFS layer prepends "user." on the way in and strips it (and
 *     drops other namespaces) on the way out.
 *   - SMB Extended Attributes (MS-FSCC FILE_FULL_EA_INFORMATION) are bare OS/2
 *     names; the SMB layer maps them to "user.<name>" as well (matching Samba's
 *     unix_ea_name).
 *
 * Mapping both onto the same "user." keyspace makes the VFS xattr layer a true
 * common denominator: a user.FOO attribute is one backend object whether it was
 * set via NFS or as an SMB EA.  The VFS backends themselves are namespace-
 * agnostic -- the passthrough backends (linux, io_uring) hand the fully-
 * qualified name straight to fgetxattr/fsetxattr/flistxattr (which require a
 * namespace prefix), and the opaque-store backends (memfs, cairn, diskfs)
 * round-trip whatever they are given.
 *
 * These helpers are pure so they can be unit tested without a dbuf.
 */

#define CHIMERA_VFS_XATTR_USER_PREFIX     "user."
#define CHIMERA_VFS_XATTR_USER_PREFIX_LEN 5

/* Longest fully-qualified xattr name the Linux backends accept (XATTR_NAME_MAX). */
#define CHIMERA_VFS_XATTR_NAME_MAX        255

/*
 * True iff a fully-qualified name belongs to the user namespace and has a
 * non-empty key after the prefix.  A bare "user." (len == prefix len) is not a
 * valid user attribute and returns false.
 */
static inline int
chimera_vfs_xattr_is_user(
    const char *name,
    uint32_t    len)
{
    return len > CHIMERA_VFS_XATTR_USER_PREFIX_LEN &&
           memcmp(name, CHIMERA_VFS_XATTR_USER_PREFIX,
                  CHIMERA_VFS_XATTR_USER_PREFIX_LEN) == 0;
} /* chimera_vfs_xattr_is_user */

/*
 * Build the fully-qualified "user."+key name into dst.  Returns the total
 * length written, or -1 if the key is empty, the result would exceed
 * XATTR_NAME_MAX, or dst is too small.  dst is NOT NUL-terminated (callers pass
 * an explicit length to the VFS); allocate at least the returned length.
 */
static inline int
chimera_vfs_xattr_build_user(
    char       *dst,
    uint32_t    dstcap,
    const char *key,
    uint32_t    keylen)
{
    uint32_t total;

    /* Bound keylen before the addition so a hostile wire length cannot overflow
     * the uint32 and slip a huge memcpy past the size checks. */
    if (keylen == 0 ||
        keylen > CHIMERA_VFS_XATTR_NAME_MAX -
        CHIMERA_VFS_XATTR_USER_PREFIX_LEN) {
        return -1;
    }

    total = CHIMERA_VFS_XATTR_USER_PREFIX_LEN + keylen;

    if (total > dstcap) {
        return -1;
    }

    memcpy(dst, CHIMERA_VFS_XATTR_USER_PREFIX,
           CHIMERA_VFS_XATTR_USER_PREFIX_LEN);
    memcpy(dst + CHIMERA_VFS_XATTR_USER_PREFIX_LEN, key, keylen);

    return (int) total;
} /* chimera_vfs_xattr_build_user */

/*
 * The SMB/OS-2 "EaSize" value -- the combined-EA byte length reported in
 * FILE_EA_INFORMATION and the EaSize sub-fields of FILE_ALL_INFORMATION and the
 * directory-enumeration info levels.  This is the OS/2 FEALIST size, NOT the
 * SMB2 FILE_FULL_EA_INFORMATION reply-buffer size (which has an 8-byte header
 * and 4-byte alignment, computed separately by the SMB marshaller).
 *
 * Per MS-FSCC and Samba (get_ea_list_from_file_path / ea_list_size) the reported
 * EaSize is, summed over the file's EAs:
 *
 *     ( sum of [ 4 + EaNameLength + 1 + EaValueLength ] )  +  4  (iff any EA)
 *
 * where EaNameLength is the client-facing name length (excluding "user.").  The
 * trailing +4 is added once when at least one EA is present.  Defined here so
 * the backends that compute va_ea_size all agree; a backend loops
 * chimera_vfs_xattr_ea_entry_size() over its user.* xattrs and, if it counted
 * any, adds CHIMERA_VFS_XATTR_EA_LIST_OVERHEAD.
 */
#define CHIMERA_VFS_XATTR_EA_LIST_OVERHEAD 4

static inline uint64_t
chimera_vfs_xattr_ea_entry_size(
    uint32_t client_name_len,
    uint32_t value_len)
{
    return 4 + (uint64_t) client_name_len + 1 + value_len;
} /* chimera_vfs_xattr_ea_entry_size */
