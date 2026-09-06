// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <sys/stat.h>

#include "nfs_internal.h"
#include "vfs/sdk/vfs_error.h"

static inline nfsstat4
chimera_nfs4_errno_to_nfsstat4(enum chimera_vfs_error err)
{
    switch (err) {
        case CHIMERA_VFS_OK:
            return NFS4_OK;
        case CHIMERA_VFS_EPERM:
            return NFS4ERR_PERM;
        case CHIMERA_VFS_ENOENT:
            return NFS4ERR_NOENT;
        case CHIMERA_VFS_EIO:
            return NFS4ERR_IO;
        case CHIMERA_VFS_ENXIO:
            return NFS4ERR_NXIO;
        case CHIMERA_VFS_EACCES:
            return NFS4ERR_ACCESS;
        case CHIMERA_VFS_EEXIST:
            return NFS4ERR_EXIST;
        case CHIMERA_VFS_EXDEV:
            return NFS4ERR_XDEV;
        case CHIMERA_VFS_ENOTDIR:
            return NFS4ERR_NOTDIR;
        case CHIMERA_VFS_EISDIR:
            return NFS4ERR_ISDIR;
        case CHIMERA_VFS_EINVAL:
            return NFS4ERR_INVAL;
        case CHIMERA_VFS_EFBIG:
            return NFS4ERR_FBIG;
        case CHIMERA_VFS_ENOSPC:
            return NFS4ERR_NOSPC;
        case CHIMERA_VFS_EROFS:
            return NFS4ERR_ROFS;
        case CHIMERA_VFS_EMLINK:
            return NFS4ERR_MLINK;
        case CHIMERA_VFS_ENAMETOOLONG:
            return NFS4ERR_NAMETOOLONG;
        case CHIMERA_VFS_ENOTEMPTY:
            return NFS4ERR_NOTEMPTY;
        case CHIMERA_VFS_EDQUOT:
            return NFS4ERR_DQUOT;
        case CHIMERA_VFS_ESTALE:
            return NFS4ERR_STALE;
        case CHIMERA_VFS_EBADCOOKIE:
            return NFS4ERR_NOT_SAME;
        case CHIMERA_VFS_EBADF:
            return NFS4ERR_BADHANDLE;
        case CHIMERA_VFS_ENOTSUP:
            return NFS4ERR_NOTSUPP;
        case CHIMERA_VFS_ENODATA:
            return NFS4ERR_NOXATTR;
        case CHIMERA_VFS_ERANGE:
            return NFS4ERR_XATTR2BIG;
        case CHIMERA_VFS_EOVERFLOW:
            return NFS4ERR_TOOSMALL;
        case CHIMERA_VFS_EFAULT:
            return NFS4ERR_SERVERFAULT;
        case CHIMERA_VFS_ESYMLINK:
            return NFS4ERR_SYMLINK;
        case CHIMERA_VFS_ELOOP:
            return NFS4ERR_SERVERFAULT;
        /* Engine-compound contention surfacing on a member op.  The v4 server
         * runs its VFS compound in the grouping lane (not RETRYABLE) because a
         * COMPOUND can never be replayed -- OPEN seqids advance, stateids
         * install, replay-cache slots record, WRITE iovecs are released -- so
         * a conflict is never delivered for replay; it surfaces per-op as the
         * retriable ECOMPOUND_EXHAUSTED, which maps to NFS4ERR_DELAY exactly
         * like the delegation-recall precedent (the client backs off and
         * retries the op).  ECOMPOUND_CONFLICT is mapped identically, purely
         * defensively: the core rewrites conflicts to EXHAUSTED for
         * non-retryable compounds, so it should never reach here. */
        case CHIMERA_VFS_ECOMPOUND_EXHAUSTED:
            return NFS4ERR_DELAY;
        case CHIMERA_VFS_ECOMPOUND_CONFLICT:
            return NFS4ERR_DELAY;
        default:
            chimera_nfs_error("Unknown VFS error code: %d", err);
            return NFS4ERR_SERVERFAULT;
    } /* switch */
} /* chimera_nfs4_errno_to_nfsstat4 */

/*
 * Status for a data-path op -- READ, WRITE, COMMIT, LOCKT, SETATTR(size) and
 * the RFC 7862 sparse ops -- whose current filehandle is not a regular file.
 *
 * A directory is NFS4ERR_ISDIR and a symbolic link is NFS4ERR_SYMLINK; every
 * other non-regular type is NFS4ERR_INVAL.  This is deliberately NOT
 * chimera_nfs4_open_nonreg_status(): OPEN reports NFS4ERR_WRONG_TYPE for
 * fifos, sockets and devices under 4.1+, but the data-path ops must keep
 * answering INVAL for those -- pynfs RD7b/RD7c (st_read.testBlock/testChar)
 * assert INVAL exactly.  Splitting out the symlink arm is safe because
 * RFC 7530 Table 7 lists both SYMLINK and INVAL for a symlink cfh and pynfs
 * RD7a accepts either.
 */
static inline nfsstat4
chimera_nfs4_data_nonreg_status(mode_t mode)
{
    if (S_ISDIR(mode)) {
        return NFS4ERR_ISDIR;
    }
    if (S_ISLNK(mode)) {
        return NFS4ERR_SYMLINK;
    }
    return NFS4ERR_INVAL;
} /* chimera_nfs4_data_nonreg_status */
