// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "nfs_internal.h"

/*
 * Parse readdir entry attributes from fattr4
 * This is a specialized version for readdir that handles FATTR4_FILEHANDLE
 *
 * fh_data must have room for CHIMERA_NFS_PROXY_REMOTE_FH_MAX bytes; a handle
 * larger than that is skipped, leaving *fh_len at 0.
 *
 * Lives in a header rather than nfs4_readdir.c so the bound above can be
 * exercised directly (vfs/nfs/tests/nfs_fh_bounds_test.c), the same way
 * chimera_nfs3_unmarshall_fh is reachable from nfs_common/nfs3_attr.h.  The
 * caller distinguishes "no handle for this entry" by *fh_len == 0, so the
 * zeroing at the top of this function is load-bearing, not defensive.
 */
static inline void
chimera_nfs4_readdir_parse_attrs(
    const struct fattr4      *fattr,
    struct chimera_vfs_attrs *attr,
    uint64_t                 *fileid,
    uint8_t                  *fh_data,
    int                      *fh_len)
{
    void    *data    = fattr->attr_vals.data;
    void    *dataend = data + fattr->attr_vals.len;
    uint32_t type;
    uint32_t len;

    *fileid           = 0;
    *fh_len           = 0;
    attr->va_set_mask = 0;

    if (fattr->num_attrmask < 1) {
        return;
    }

    /* Parse attributes in bitmap order */

    /* FATTR4_TYPE = 1 */
    if (fattr->attrmask[0] & (1 << FATTR4_TYPE)) {
        if (data + sizeof(uint32_t) > dataend) {
            return;
        }
        type               = chimera_nfs_ntoh32(*(uint32_t *) data);
        data              += sizeof(uint32_t);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
        switch (type) {
            case NF4REG:
                attr->va_mode = S_IFREG;
                break;
            case NF4DIR:
                attr->va_mode = S_IFDIR;
                break;
            case NF4BLK:
                attr->va_mode = S_IFBLK;
                break;
            case NF4CHR:
                attr->va_mode = S_IFCHR;
                break;
            case NF4LNK:
                attr->va_mode = S_IFLNK;
                break;
            case NF4SOCK:
                attr->va_mode = S_IFSOCK;
                break;
            case NF4FIFO:
                attr->va_mode = S_IFIFO;
                break;
            default:
                attr->va_mode = S_IFREG;
                break;
        } /* switch */
    }

    /* FATTR4_SIZE = 4 */
    if (fattr->attrmask[0] & (1 << FATTR4_SIZE)) {
        if (data + sizeof(uint64_t) > dataend) {
            return;
        }
        attr->va_size      = chimera_nfs_ntoh64(*(uint64_t *) data);
        data              += sizeof(uint64_t);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
    }

    /* FATTR4_FILEHANDLE = 19 - opaque<NFS4_FHSIZE> */
    if (fattr->attrmask[0] & (1 << FATTR4_FILEHANDLE)) {
        if (data + sizeof(uint32_t) > dataend) {
            return;
        }
        len   = chimera_nfs_ntoh32(*(uint32_t *) data);
        data += sizeof(uint32_t);

        /* A length running past the end of the attribute blob is malformed: it
         * makes the next attribute's offset unknowable, so parsing has to stop
         * here.  Note the generated decoders enforce neither this nor the
         * `<NFS4_FHSIZE>` bound nfs4.x declares, so neither is a check we get
         * for free. */
        if (data + len > dataend) {
            return;
        }

        /* A well-formed handle can still be one chimera cannot re-encode: the
         * caller prepends a mount_id and a server index, and the result has to
         * fit CHIMERA_VFS_FH_SIZE.  Leaving *fh_len at 0 omits this entry's
         * handle while keeping the rest of its attributes, which is what the v3
         * readdir does with the same case (nfs3_readdir.c) -- readdir handles
         * are an optimization, so the client just looks the entry up instead.
         *
         * This ceiling is 47, below the 128 NFS4_FHSIZE permits, so it subsumes
         * the protocol bound rather than needing a separate test for it: a
         * handle past NFS4_FHSIZE is skipped here and parsing continues, where
         * folding that case into the malformed check above used to cost the
         * entry every attribute that follows the handle. */
        if (len <= CHIMERA_NFS_PROXY_REMOTE_FH_MAX) {
            memcpy(fh_data, data, len);
            *fh_len = len;
        }

        data += len;

        /* XDR padding to 4-byte boundary */
        if (len % 4) {
            data += 4 - (len % 4);
        }
    }

    /* FATTR4_FILEID = 20 */
    if (fattr->attrmask[0] & (1 << FATTR4_FILEID)) {
        if (data + sizeof(uint64_t) > dataend) {
            return;
        }
        *fileid            = chimera_nfs_ntoh64(*(uint64_t *) data);
        attr->va_ino       = *fileid;
        data              += sizeof(uint64_t);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_INUM;
    }

    if (fattr->num_attrmask < 2) {
        return;
    }

    /* FATTR4_MODE = 33 */
    if (fattr->attrmask[1] & (1 << (FATTR4_MODE - 32))) {
        if (data + sizeof(uint32_t) > dataend) {
            return;
        }
        attr->va_mode     |= chimera_nfs_ntoh32(*(uint32_t *) data) & ~S_IFMT;
        data              += sizeof(uint32_t);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
    }

    /* FATTR4_NUMLINKS = 35 */
    if (fattr->attrmask[1] & (1 << (FATTR4_NUMLINKS - 32))) {
        if (data + sizeof(uint32_t) > dataend) {
            return;
        }
        attr->va_nlink     = chimera_nfs_ntoh32(*(uint32_t *) data);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_NLINK;
    }
} /* chimera_nfs4_readdir_parse_attrs */
