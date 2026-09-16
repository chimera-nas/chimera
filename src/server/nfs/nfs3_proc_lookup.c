// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_internal.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"
static void
chimera_nfs3_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct LOOKUP3res                 res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {

        uint8_t wire[CHIMERA_NFS_FH_MAX];
        int     wirelen;

        chimera_nfs_abort_if(!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH),
                             "NFS3 lookup: no file handle was returned");

        /* The looked-up child lives in the same export as the directory; wrap
         * its handle with the request's export id (and sign it) for the wire. */
        chimera_nfs_fh_encode(req, attr->va_fh, attr->va_fh_len, wire, &wirelen);

        rc = xdr_dbuf_opaque_copy(&res.resok.object.data,
                                  wire,
                                  wirelen,
                                  req->encoding->dbuf);
        chimera_nfs_abort_if(rc, "Failed to copy opaque");

        chimera_nfs3_set_post_op_attr(&res.resok.obj_attributes, attr);
        chimera_nfs3_set_post_op_attr(&res.resok.dir_attributes, dir_attr);
    } else {
        chimera_nfs3_set_post_op_attr(&res.resfail.dir_attributes, dir_attr);
    }

    /* The open belonged to the sequence and went with it. */

    rc = shared->nfs_v3.send_reply_NFSPROC3_LOOKUP(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");


    nfs_request_free(thread, req);
} /* chimera_nfs3_lookup_complete */

/*
 * PUTFH, OPEN, LOOKUP.  The directory attributes come back from the LOOKUP
 * itself rather than from a GETATTR behind it -- LOOKUP3res carries them in
 * both arms, and on the resfail arm there is no successful op to hang one on.
 */
static void
chimera_nfs3_lookup_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              attr, dir_attr;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    memset(&attr, 0, sizeof(attr));
    memset(&dir_attr, 0, sizeof(dir_attr));

    /* Everything out of the sequence before it is freed and recycled. */
    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    dir_attr = op->dir_post_attr;

    if (status == CHIMERA_VFS_OK) {
        attr = op->attr;
    }

    chimera_vfs_compound_free(compound);

    chimera_nfs3_lookup_complete(status, &attr, &dir_attr, req);
} /* chimera_nfs3_lookup_sequence_complete */


void
chimera_nfs3_lookup(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct LOOKUP3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct chimera_vfs_compound      *compound;
    struct LOOKUP3res                 res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_lookup(req, args);
    nfs3_trace_lookup(req, args);

    req->args_lookup = args;

    res.status = chimera_nfs3_decode_fh(req, args->what.dir.data.data, args->what.dir.data.len);
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_LOOKUP(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY, 0);
    chimera_vfs_compound_add_lookup(compound,
                                    args->what.name.str,
                                    args->what.name.len,
                                    CHIMERA_VFS_ATTR_FH | CHIMERA_NFS3_ATTR_MASK,
                                    CHIMERA_NFS3_ATTR_MASK);

    chimera_vfs_compound_submit(compound,
                                chimera_nfs3_lookup_sequence_complete, req);

} /* chimera_nfs3_lookup */
