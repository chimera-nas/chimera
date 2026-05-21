// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <xxhash.h>

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "server/server.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_state.h"

static void
chimera_nfs4_remove_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct REMOVE4res                *res    = &req->res_compound.resarray[req->index].opremove;

    if (error_code == CHIMERA_VFS_OK) {
        res->status = NFS4_OK;
    } else {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_remove_complete */

static void
chimera_nfs4_remove_do(struct nfs_request *req)
{
    struct REMOVE4args *args = &req->args_compound->argarray[req->index].opremove;

    chimera_vfs_remove_at(req->thread->vfs_thread, &req->cred,
                          req->handle,
                          args->target.data,
                          args->target.len,
                          NULL,
                          0,
                          0,
                          0,
                          chimera_nfs4_remove_complete,
                          req);
} /* chimera_nfs4_remove_do */

/*
 * RFC 7530 §10.4.5: a REMOVE of a delegated file must recall the delegation.
 * We looked the target up to learn its FH; if a caching lease (delegation) is
 * held on it, kick the recall and tell the client to retry (NFS4ERR_DELAY).
 * Once the holder returns the delegation the retry finds no lease and the
 * remove proceeds.
 */
static void
chimera_nfs4_remove_recall_lookup(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;

    if (error_code == CHIMERA_VFS_OK &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        uint64_t fh_hash = XXH3_64bits(attr->va_fh, attr->va_fh_len) & INT64_MAX;

        if (chimera_vfs_state_break_caching(req->thread->vfs->vfs_state,
                                            attr->va_fh, attr->va_fh_len,
                                            fh_hash)) {
            struct REMOVE4res *res = &req->res_compound.resarray[req->index].opremove;
            chimera_vfs_release(req->thread->vfs_thread, req->handle);
            res->status = NFS4ERR_DELAY;
            chimera_nfs4_compound_complete(req, NFS4ERR_DELAY);
            return;
        }
    }

    chimera_nfs4_remove_do(req);
} /* chimera_nfs4_remove_recall_lookup */

static void
chimera_nfs4_remove_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *parent_handle,
    void                           *private_data)
{
    struct nfs_request *req  = private_data;
    struct REMOVE4args *args = &req->args_compound->argarray[req->index].opremove;

    req->handle = parent_handle;

    if (error_code != CHIMERA_VFS_OK) {
        struct REMOVE4res *res = &req->res_compound.resarray[req->index].opremove;
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* When delegations are enabled, resolve the victim's FH first so any
     * delegation on it can be recalled before the remove. */
    if (chimera_server_config_get_nfs4_delegations(req->thread->shared->config)) {
        chimera_vfs_lookup(req->thread->vfs_thread, &req->cred,
                           req->fh, req->fhlen,
                           args->target.data, args->target.len,
                           CHIMERA_VFS_ATTR_FH, 0,
                           chimera_nfs4_remove_recall_lookup, req);
        return;
    }

    chimera_nfs4_remove_do(req);
} /* chimera_nfs4_remove_open_callback */


void
chimera_nfs4_remove(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct REMOVE4args *args = &argop->opremove;
    struct REMOVE4res  *res  = &resop->opremove;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    res->status = chimera_nfs4_validate_name(&args->target);

    if (res->status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs4_remove_open_callback,
                        req);

} /* chimera_nfs4_remove */
