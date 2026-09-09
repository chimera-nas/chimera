// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"

struct chimera_nfs4_rename_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_client_server *server;
};

static void
chimera_nfs4_rename_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct nfs_resop4          *rename_res;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(res->status);
        request->complete(request);
        return;
    }

    /* Check SEQUENCE result (index 0) */
    if (res->num_resarray < 1 || res->resarray[0].opsequence.sr_status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check PUTFH result (index 1) - source directory */
    if (res->num_resarray < 2 || res->resarray[1].opputfh.status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check SAVEFH result (index 3) */
    if (res->num_resarray < 4 || res->resarray[3].opsavefh.status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check PUTFH result (index 4) - target directory */
    if (res->num_resarray < 5 || res->resarray[4].opputfh.status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* The two pre-mutation snapshots (ops 2 and 5).  Taken before the RENAME
     * status check so they are recorded even when the rename itself failed. */
    chimera_nfs4_unmarshall_dir_attr(res, 2, &request->rename_at.r_fromdir_pre_attr);
    chimera_nfs4_unmarshall_dir_attr(res, 5, &request->rename_at.r_todir_pre_attr);

    /* Check RENAME result (index 6) */
    if (res->num_resarray < 7) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }
    rename_res = &res->resarray[6];
    if (rename_res->oprename.status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(rename_res->oprename.status);
        request->complete(request);
        return;
    }

    /* The two post-mutation snapshots (ops 8 and 10). */
    chimera_nfs4_unmarshall_dir_attr(res, 8, &request->rename_at.r_fromdir_post_attr);
    chimera_nfs4_unmarshall_dir_attr(res, 10, &request->rename_at.r_todir_post_attr);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_rename_callback */

void
chimera_nfs4_rename_at(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs_client_server        *server;
    struct chimera_nfs4_client_session      *session;
    struct chimera_nfs4_rename_ctx          *ctx;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[11];
    uint32_t                                 attr_request[2];
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *src_fh;
    int                                      src_fhlen;
    uint8_t                                 *dst_fh;
    int                                      dst_fhlen;

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

    /* Map source directory FH (request->fh) */
    chimera_nfs4_map_fh(request->fh, request->fh_len, &src_fh, &src_fhlen);

    /* Map target directory FH */
    chimera_nfs4_map_fh(request->rename_at.new_fh, request->rename_at.new_fhlen, &dst_fh, &dst_fhlen);

    /* Build compound: SEQUENCE + PUTFH(src) + GETATTR(src pre) + SAVEFH +
     * PUTFH(dst) + GETATTR(dst pre) + RENAME + PUTFH(src) + GETATTR(src post) +
     * PUTFH(dst) + GETATTR(dst post).  RENAME reports change_info4 for BOTH
     * directories, so both need bracketing; see chimera_nfs4_dir_getattr_op. */
    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 11;

    /* Op 0: SEQUENCE */
    argarray[0].argop = OP_SEQUENCE;

    /* Op 1: PUTFH - set current FH to source directory */
    chimera_nfs4_putfh_op(&argarray[1], src_fh, src_fhlen);

    /* Op 2: GETATTR - source directory's change attribute before the rename */
    chimera_nfs4_dir_getattr_op(&argarray[2], attr_request);

    /* Op 3: SAVEFH - save source directory FH (GETATTR left it in place) */
    argarray[3].argop = OP_SAVEFH;

    /* Op 4: PUTFH - set current FH to target directory */
    chimera_nfs4_putfh_op(&argarray[4], dst_fh, dst_fhlen);

    /* Op 5: GETATTR - target directory's change attribute before the rename */
    chimera_nfs4_dir_getattr_op(&argarray[5], attr_request);

    /* Op 6: RENAME - rename from saved FH (source dir) to current FH (target dir) */
    argarray[6].argop                 = OP_RENAME;
    argarray[6].oprename.oldname.data = (uint8_t *) request->rename_at.name;
    argarray[6].oprename.oldname.len  = request->rename_at.namelen;
    argarray[6].oprename.newname.data = (uint8_t *) request->rename_at.new_name;
    argarray[6].oprename.newname.len  = request->rename_at.new_namelen;

    /* Ops 7-10: re-name each directory and snapshot it again. */
    chimera_nfs4_putfh_op(&argarray[7], src_fh, src_fhlen);
    chimera_nfs4_dir_getattr_op(&argarray[8], attr_request);
    chimera_nfs4_putfh_op(&argarray[9], dst_fh, dst_fhlen);
    chimera_nfs4_dir_getattr_op(&argarray[10], attr_request);

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
        chimera_nfs4_rename_callback,
        request,
        chimera_nfs4_dispatch, private_data);
} /* chimera_nfs4_rename_at */
