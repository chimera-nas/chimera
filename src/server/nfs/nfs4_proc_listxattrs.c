// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/sdk/vfs_xattr_name.h"
#include "vfs/vfs_compound.h"

/*
 * Marshal a LISTXATTRS4 result from a name list the backend has produced.
 * Shared by the per-op path below and by the VFS-compound path.
 *
 * `names` must stay valid until the reply is sent: the result entries point
 * straight into it rather than copying each name.  The per-op path passes the
 * response dbuf's staging buffer; the VFS-compound path passes the sequence's,
 * which outlives the reply for the same reason.
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

/* PUTFH, OPEN_CURRENT, LISTXATTRS: the enumeration is op 2 of the run. */
#define NFS4_LISTXATTRS_OP_LISTXATTRS 2

static void
chimera_nfs4_listxattrs_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    struct LISTXATTRS4res                *res = &req->res_compound.resarray[req->index].oplistxattrs;
    const struct chimera_vfs_compound_op *xop;
    enum chimera_vfs_error                error_code;
    char                                 *names;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_free(compound);
        /* A too-small maxcount maps to TOOSMALL rather than XATTR2BIG. */
        res->lxr_status = (error_code == CHIMERA_VFS_ERANGE) ?
            NFS4ERR_TOOSMALL : chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->lxr_status);
        return;
    }

    xop = chimera_vfs_compound_op(compound, NFS4_LISTXATTRS_OP_LISTXATTRS);

    /* The result entries point into the name buffer instead of copying each
     * name out of it, so it has to outlive the reply -- and the sequence's
     * buffer goes with the free below.  Restage the names where the per-op path
     * kept them, in the reply buffer, before pointing anything at them. */
    names = xdr_dbuf_alloc_space(xop->buffer_len ? xop->buffer_len : 1,
                                 req->encoding->dbuf);

    if (!names) {
        chimera_vfs_compound_free(compound);
        res->lxr_status = NFS4ERR_RESOURCE;
        chimera_nfs4_compound_complete(req, res->lxr_status);
        return;
    }

    memcpy(names, xop->buffer, xop->buffer_len);

    res->lxr_status = chimera_nfs4_listxattrs_fill(req, res, names,
                                                   xop->buffer_count,
                                                   xop->eof, xop->r_cookie);

    chimera_vfs_compound_free(compound);

    chimera_nfs4_compound_complete(req, res->lxr_status);
} /* chimera_nfs4_listxattrs_complete */

void
chimera_nfs4_listxattrs(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LISTXATTRS4args      *args = &argop->oplistxattrs;
    struct LISTXATTRS4res       *res  = &resop->oplistxattrs;
    struct chimera_vfs_compound *compound;

    if (req->fhlen == 0) {
        res->lxr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->lxr_status);
        return;
    }

    req->handle = NULL;

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED, 0);
    chimera_vfs_compound_add_listxattrs(compound, args->lxa_cookie,
                                        chimera_nfs4_xattr_stage_max(
                                            req, args->lxa_maxcount));

    chimera_vfs_compound_submit(compound, chimera_nfs4_listxattrs_complete,
                                req);
} /* chimera_nfs4_listxattrs */
