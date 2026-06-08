// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

#include "nfs4_xdr.h"
#include "vfs/vfs_acl.h"

/*
 * Pure ACCESS-bit decision logic, factored out of the ACCESS handler so it can
 * be unit tested without a live VFS thread or a kernel NFS mount.
 *
 * Three steps: pick the meaningful ACCESS4_* bits for the object, translate the
 * requested bits to the canonical ACE mask the access engine evaluates, then
 * map the granted ACE bits back to allowed ACCESS4_* bits.
 */

/*
 * The ACCESS4_* bits that are meaningful for an object.  RFC 7530 sec 16.1.4:
 * LOOKUP/DELETE apply only to directories, EXECUTE only to non-directories.
 * RFC 8276 sec 8.4: the xattr bits (XAREAD/XAWRITE/XALIST) are meaningful only
 * when the backing filesystem implements extended attributes -- advertising
 * them in `supported` is what tells the client it may attempt SETXATTR/GETXATTR.
 */
static inline uint32_t
chimera_nfs4_access_meaningful(
    int is_dir,
    int xattr_supported)
{
    uint32_t meaningful = ACCESS4_READ | ACCESS4_MODIFY | ACCESS4_EXTEND;

    if (is_dir) {
        meaningful |= ACCESS4_LOOKUP | ACCESS4_DELETE;
    } else {
        meaningful |= ACCESS4_EXECUTE;
    }

    if (xattr_supported) {
        meaningful |= ACCESS4_XAREAD | ACCESS4_XAWRITE | ACCESS4_XALIST;
    }

    return meaningful;
} /* chimera_nfs4_access_meaningful */

/*
 * Translate ACCESS4_* request bits into the canonical ACE mask the central
 * engine understands.  LOOKUP (directory search) and EXECUTE (file execute)
 * both map to ACE_EXECUTE; they are never meaningful for the same object type,
 * so they do not collide on the way back out.
 */
static inline uint32_t
chimera_nfs4_access4_to_mask(uint32_t access)
{
    uint32_t mask = 0;

    if (access & ACCESS4_READ) {
        mask |= CHIMERA_ACE_READ_DATA;
    }
    if (access & ACCESS4_LOOKUP) {
        mask |= CHIMERA_ACE_EXECUTE;
    }
    if (access & ACCESS4_MODIFY) {
        mask |= CHIMERA_ACE_WRITE_DATA;
    }
    if (access & ACCESS4_EXTEND) {
        mask |= CHIMERA_ACE_APPEND_DATA;
    }
    if (access & ACCESS4_DELETE) {
        mask |= CHIMERA_ACE_DELETE;
    }
    if (access & ACCESS4_EXECUTE) {
        mask |= CHIMERA_ACE_EXECUTE;
    }
    /* RFC 8276 sec 8.4: reading/listing xattrs needs read-named-attrs;
     * creating/removing/replacing needs write-named-attrs. */
    if (access & (ACCESS4_XAREAD | ACCESS4_XALIST)) {
        mask |= CHIMERA_ACE_READ_NAMED_ATTRS;
    }
    if (access & ACCESS4_XAWRITE) {
        mask |= CHIMERA_ACE_WRITE_NAMED_ATTRS;
    }

    return mask;
} /* chimera_nfs4_access4_to_mask */

/*
 * Map the ACE bits the engine granted back to the allowed ACCESS4_* bits,
 * limited to the bits the client actually requested.
 */
static inline uint32_t
chimera_nfs4_access_from_granted(
    uint32_t requested,
    uint32_t granted)
{
    uint32_t access = 0;

    if ((requested & ACCESS4_READ) && (granted & CHIMERA_ACE_READ_DATA)) {
        access |= ACCESS4_READ;
    }
    if ((requested & ACCESS4_LOOKUP) && (granted & CHIMERA_ACE_EXECUTE)) {
        access |= ACCESS4_LOOKUP;
    }
    if ((requested & ACCESS4_MODIFY) && (granted & CHIMERA_ACE_WRITE_DATA)) {
        access |= ACCESS4_MODIFY;
    }
    if ((requested & ACCESS4_EXTEND) && (granted & CHIMERA_ACE_APPEND_DATA)) {
        access |= ACCESS4_EXTEND;
    }
    if ((requested & ACCESS4_DELETE) && (granted & CHIMERA_ACE_DELETE)) {
        access |= ACCESS4_DELETE;
    }
    if ((requested & ACCESS4_EXECUTE) && (granted & CHIMERA_ACE_EXECUTE)) {
        access |= ACCESS4_EXECUTE;
    }
    if ((requested & ACCESS4_XAREAD) && (granted & CHIMERA_ACE_READ_NAMED_ATTRS)) {
        access |= ACCESS4_XAREAD;
    }
    if ((requested & ACCESS4_XALIST) && (granted & CHIMERA_ACE_READ_NAMED_ATTRS)) {
        access |= ACCESS4_XALIST;
    }
    if ((requested & ACCESS4_XAWRITE) && (granted & CHIMERA_ACE_WRITE_NAMED_ATTRS)) {
        access |= ACCESS4_XAWRITE;
    }

    return access;
} /* chimera_nfs4_access_from_granted */
