// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <string.h>
#include "nfs_common.h"

/*
 * NFSv4 named attributes (RFC 7530/8881 sec 5.3) are projected onto the VFS
 * named-stream primitives (the same backend storage as SMB alternate data
 * streams).  The per-file "named attribute directory" returned by OPENATTR is a
 * purely server-side synthetic object: its file handle is the base file's VFS fh
 * with an 8-byte magic prefix.  The directory's *entries* (the named streams)
 * use the real backend stream fh that open_stream returns, so READ/WRITE/CLOSE
 * on a stream need no special handling here.
 *
 * The marker is a fixed 8-byte magic prefix rather than a single sentinel byte:
 * a real chimera fh begins with a 16-byte XXH128 mount-id hash (effectively
 * random), so a 1-byte sentinel would alias ~1/256 of real handles.  An 8-byte
 * magic collides with probability ~2^-64, the same order as the fh-hash
 * collisions the system already tolerates.
 */
#define CHIMERA_NFS4_ATTRDIR_MAGIC     "\xc4NMADIR"
#define CHIMERA_NFS4_ATTRDIR_MAGIC_LEN 8

/* True iff `fh` is a synthetic named-attribute-directory handle. */
static inline int
chimera_nfs4_fh_is_attrdir(
    const uint8_t *fh,
    int            fh_len)
{
    return fh_len > CHIMERA_NFS4_ATTRDIR_MAGIC_LEN &&
           memcmp(fh, CHIMERA_NFS4_ATTRDIR_MAGIC,
                  CHIMERA_NFS4_ATTRDIR_MAGIC_LEN) == 0;
} /* chimera_nfs4_fh_is_attrdir */

/*
 * Build the attr-dir handle for a base file fh into `out` (must hold at least
 * base_len + CHIMERA_NFS4_ATTRDIR_MAGIC_LEN bytes, which stays within
 * NFS4_FHSIZE for every backend fh).  Returns the encoded length.
 */
static inline int
chimera_nfs4_make_attrdir_fh(
    uint8_t       *out,
    const uint8_t *base_fh,
    int            base_len)
{
    memcpy(out, CHIMERA_NFS4_ATTRDIR_MAGIC, CHIMERA_NFS4_ATTRDIR_MAGIC_LEN);
    memcpy(out + CHIMERA_NFS4_ATTRDIR_MAGIC_LEN, base_fh, base_len);
    return base_len + CHIMERA_NFS4_ATTRDIR_MAGIC_LEN;
} /* chimera_nfs4_make_attrdir_fh */

/*
 * Strip the magic prefix, yielding the underlying base-file fh.  `*r_base` points
 * into `fh`; valid for the lifetime of `fh`.  Caller must have checked
 * chimera_nfs4_fh_is_attrdir().
 */
static inline void
chimera_nfs4_attrdir_base(
    const uint8_t  *fh,
    int             fh_len,
    const uint8_t **r_base,
    int            *r_base_len)
{
    *r_base     = fh + CHIMERA_NFS4_ATTRDIR_MAGIC_LEN;
    *r_base_len = fh_len - CHIMERA_NFS4_ATTRDIR_MAGIC_LEN;
} /* chimera_nfs4_attrdir_base */
