// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Stable, versioned, little-endian serialization of a canonical chimera_acl,
 * used for on-disk persistence by native backends (e.g. cairn's CAIRN_KEY_ACL
 * blob).  This is distinct from the NFSv4 XDR and SMB security-descriptor wire
 * formats, which are protocol marshalling, not storage.
 *
 * Layout (all integers little-endian):
 *   u8  version (= CHIMERA_ACL_SERIAL_VERSION)
 *   u16 ctrl_flags
 *   u16 num_aces
 *   num_aces x {
 *       u16 type, u16 flags, u32 access_mask,
 *       u8 principal_type, u8 special, u32 id
 *   }
 */

#include <stddef.h>
#include "vfs_acl.h"

#define CHIMERA_ACL_SERIAL_VERSION 1
#define CHIMERA_ACL_SERIAL_HDR     5  /* version + ctrl_flags + num_aces */
#define CHIMERA_ACL_SERIAL_ACE     14 /* type+flags+mask+ptype+special+id */

/* Number of bytes chimera_acl_serialize() will write for `acl`. */
static inline size_t
chimera_acl_serialized_size(const struct chimera_acl *acl)
{
    return CHIMERA_ACL_SERIAL_HDR +
           (size_t) acl->num_aces * CHIMERA_ACL_SERIAL_ACE;
} /* chimera_acl_serialized_size */

/*
 * Serialize `acl` into `buf` (capacity `buflen`).  Returns the number of bytes
 * written, or -1 if the buffer is too small.
 */
int chimera_acl_serialize(
    const struct chimera_acl *acl,
    void                     *buf,
    size_t                    buflen);

/*
 * Deserialize from `buf` (`buflen` bytes) into `out` (capacity `max_aces`).
 * Returns the ACE count, or -1 on a malformed/oversized/unsupported blob.
 */
int chimera_acl_deserialize(
    const void         *buf,
    size_t              buflen,
    struct chimera_acl *out,
    unsigned            max_aces);
