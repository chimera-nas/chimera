// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"

struct chimera_nfs4_remove_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_client_server *server;
    uint32_t                          remove_index;
    uint32_t                          type_mask;
    uint32_t                          directory_type;
};

static void
chimera_nfs4_remove_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request     *request = private_data;
    struct nfs_resop4              *remove_res;
    struct chimera_nfs4_remove_ctx *ctx = request->plugin_data;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    /* Preserve the parent's pre-op snapshot even when type validation or
     * REMOVE terminates the remote compound. */
    chimera_nfs4_unmarshall_dir_attr(res, 2, &request->remove_at.r_dir_pre_attr);
    if (ctx->remove_index == 6 && res->num_resarray == 5 &&
        ((res->resarray[4].resop == OP_VERIFY &&
          res->resarray[4].opverify.status == NFS4ERR_NOT_SAME) ||
         (res->resarray[4].resop == OP_NVERIFY &&
          res->resarray[4].opnverify.status == NFS4ERR_SAME))) {
        request->status = (request->remove_at.flags & CHIMERA_VFS_REMOVE_ISDIR) ?
            CHIMERA_VFS_ENOTDIR : CHIMERA_VFS_EISDIR;
        request->complete(request);
        return;
    }
    if (res->status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(res->status);
        request->complete(request);
        return;
    }

    /* Check SEQUENCE result */
    if (res->num_resarray < 1 || res->resarray[0].opsequence.sr_status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check PUTFH result */
    if (res->num_resarray < 2 || res->resarray[1].opputfh.status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check REMOVE result */
    if (res->num_resarray <= ctx->remove_index) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }
    remove_res = &res->resarray[ctx->remove_index];
    if (remove_res->opremove.status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(remove_res->opremove.status);
        request->complete(request);
        return;
    }

    chimera_nfs4_unmarshall_cinfo(&remove_res->opremove.resok4.cinfo,
                                  &request->remove_at.r_dir_pre_attr, &request->remove_at.r_dir_post_attr);
    chimera_nfs4_unmarshall_dir_attr(res, ctx->remove_index + 2, &request->remove_at.r_dir_post_attr);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_remove_callback */

void
chimera_vfs_nfs4_remove_at(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs_client_server        *server;
    struct chimera_nfs4_client_session      *session;
    struct chimera_nfs4_remove_ctx          *ctx;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[9];
    uint32_t                                 attr_request[2];
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

    ctx = request->plugin_data;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    server  = server_thread->server;
    session = server->nfs4_session;

    if (!session) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    ctx->thread = thread;
    ctx->server = server;

    chimera_nfs4_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    /* Build compound: SEQUENCE + PUTFH + GETATTR(dir pre) + REMOVE + PUTFH +
     * GETATTR(dir post).  The two directory GETATTRs are what the protocol
     * server turns into change_info4; see chimera_nfs4_dir_getattr_op. */
    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 0;

    /* Op 0: SEQUENCE */
    argarray[0].argop = OP_SEQUENCE;

    /* Op 1: PUTFH - set current file handle to parent directory */
    chimera_nfs4_putfh_op(&argarray[1], fh, fhlen);

    /* Op 2: GETATTR - the parent's change attribute before the mutation */
    chimera_nfs4_dir_getattr_op(&argarray[2], attr_request);

    ctx->remove_index = 3;
    if (request->remove_at.flags & (CHIMERA_VFS_REMOVE_ISDIR | CHIMERA_VFS_REMOVE_ISNOTDIR)) {
        /* NFSv4 REMOVE accepts either type. Validate the named object without
         * following a symlink, inside the same remote compound, before REMOVE.
         * This is not atomic against a concurrent remote rename: NFSv4 has no
         * typed/matched REMOVE, and this backend does not advertise that
         * stronger identity capability. */
        memset(&argarray[3], 0, 2 * sizeof(argarray[0]));
        argarray[3].argop                 = OP_LOOKUP;
        argarray[3].oplookup.objname.data = (uint8_t *) request->remove_at.name;
        argarray[3].oplookup.objname.len  = request->remove_at.namelen;
        struct fattr4 *attrs;
        if (request->remove_at.flags & CHIMERA_VFS_REMOVE_ISDIR) {
            argarray[4].argop = OP_VERIFY;
            attrs             = &argarray[4].opverify.obj_attributes;
        } else {
            argarray[4].argop = OP_NVERIFY;
            attrs             = &argarray[4].opnverify.obj_attributes;
        }
        ctx->type_mask        = 1u << FATTR4_TYPE;
        ctx->directory_type   = chimera_nfs_hton32(NF4DIR);
        attrs->num_attrmask   = 1;
        attrs->attrmask       = &ctx->type_mask;
        attrs->attr_vals.data = (void *) &ctx->directory_type;
        attrs->attr_vals.len  = sizeof(ctx->directory_type);
        chimera_nfs4_putfh_op(&argarray[5], fh, fhlen);
        ctx->remove_index = 6;
    }
    uint32_t remove = ctx->remove_index;
    argarray[remove].argop                = OP_REMOVE;
    argarray[remove].opremove.target.data = (uint8_t *) request->remove_at.name;
    argarray[remove].opremove.target.len  = request->remove_at.namelen;
    chimera_nfs4_putfh_op(&argarray[remove + 1], fh, fhlen);
    chimera_nfs4_dir_getattr_op(&argarray[remove + 2], attr_request);
    args.num_argarray = remove + 3;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    chimera_nfs4_compound_call(
        thread,
        shared,
        server_thread,
        request,
        &args,
        &rpc2_cred,
        0, 0, NULL, 0, 0,
        chimera_nfs4_remove_callback,
        request,
        chimera_vfs_nfs4_dispatch, private_data);
} /* chimera_vfs_nfs4_remove_at */
