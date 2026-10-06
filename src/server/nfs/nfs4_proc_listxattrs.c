// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/sdk/vfs_xattr_name.h"

/*
 * Marshal a LISTXATTRS4 result from a name list the backend has produced.
 * `names` must stay valid until the reply is sent: the result entries point
 * straight into the accepted reply's staging buffer without copying each name.
 */
nfsstat4
chimera_nfs4_listxattrs_fill(
    struct nfs_request    *req,
    struct LISTXATTRS4res *res,
    const char            *names,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie)
{
    const char *p = names;
    uint32_t    i, emitted;
    int         rc;

    /* The backend returns fully-qualified names from every namespace; RFC 8276
     * scopes NFS xattrs to the user namespace.  Count the user.* names so the
     * result array is sized to what we will actually emit. */
    emitted = 0;
    for (i = 0; i < count; i++) {
        uint32_t namelen = strlen(p);

        if (chimera_vfs_xattr_is_user(p, namelen)) {
            emitted++;
        }
        p += namelen + 1;
    }

    rc = xdr_dbuf_alloc_array(&res->lxr_value, lxr_names, emitted,
                              req->encoding->dbuf);
    if (rc) {
        return NFS4ERR_RESOURCE;
    }

    /* names is a sequence of NUL-terminated names. Strip the "user." prefix and
     * drop non-user namespaces, pointing each result entry directly at the
     * staging buffer. */
    p       = names;
    emitted = 0;
    for (i = 0; i < count; i++) {
        uint32_t namelen = strlen(p);

        if (chimera_vfs_xattr_is_user(p, namelen)) {
            res->lxr_value.lxr_names[emitted].xn_name.data =
                (void *) (p + CHIMERA_VFS_XATTR_USER_PREFIX_LEN);
            res->lxr_value.lxr_names[emitted].xn_name.len =
                namelen - CHIMERA_VFS_XATTR_USER_PREFIX_LEN;
            emitted++;
        }
        p += namelen + 1;
    }

    res->lxr_value.lxr_cookie = cookie;
    res->lxr_value.lxr_eof    = eof;

    return NFS4_OK;
} /* chimera_nfs4_listxattrs_fill */

void
chimera_nfs4_listxattrs(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LISTXATTRS4res *res = &resop->oplistxattrs;

    if (req->fhlen == 0) {
        res->lxr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->lxr_status);
        return;
    }

    chimera_nfs4_compound_single(thread, req);
} /* chimera_nfs4_listxattrs */
