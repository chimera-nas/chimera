// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_acl.h"
#include "vfs/vfs_access.h"
#include "nfs3_dump.h"

/* Map the meaningful ACCESS3_* request bits to the canonical ACE mask. */
static uint32_t
chimera_nfs3_access3_to_mask(uint32_t access)
{
    uint32_t mask = 0;

    if (access & ACCESS3_READ) {
        mask |= CHIMERA_ACE_READ_DATA;
    }
    if (access & ACCESS3_LOOKUP) {
        mask |= CHIMERA_ACE_EXECUTE;
    }
    if (access & ACCESS3_MODIFY) {
        mask |= CHIMERA_ACE_WRITE_DATA;
    }
    if (access & ACCESS3_EXTEND) {
        mask |= CHIMERA_ACE_APPEND_DATA;
    }
    if (access & ACCESS3_DELETE) {
        mask |= CHIMERA_ACE_DELETE;
    }
    if (access & ACCESS3_EXECUTE) {
        mask |= CHIMERA_ACE_EXECUTE;
    }

    return mask;
} /* chimera_nfs3_access3_to_mask */

static void
chimera_nfs3_access_reply(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    if (req->txn_op_status != CHIMERA_VFS_OK) {
        req->res_access.status = chimera_vfs_error_to_nfsstat3(req->txn_op_status);
    }

    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_ACCESS(thread->evpl, NULL,
                                                   &req->res_access, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_access_reply */

static void
chimera_nfs3_access_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req  = private_data;
    struct ACCESS3args *args = req->args_access;
    struct ACCESS3res  *res  = &req->res_access;

    if (error_code == CHIMERA_VFS_OK) {
        res->status = NFS3_OK;
        chimera_nfs3_set_post_op_attr(&res->resok.obj_attributes, attr);

        /* Evaluate the canonical ACL (or mode fallback) once via the shared
         * gate -- this honours the caller's full credential rather than the
         * legacy owner-bits-only check. */
        uint32_t granted = chimera_vfs_access_check(
            attr, &req->cred,
            chimera_nfs3_access3_to_mask(args->access));

        res->resok.access = 0;

        if ((args->access & ACCESS3_READ) && (granted & CHIMERA_ACE_READ_DATA)) {
            res->resok.access |= ACCESS3_READ;
        }
        if ((args->access & ACCESS3_LOOKUP) && (granted & CHIMERA_ACE_EXECUTE)) {
            res->resok.access |= ACCESS3_LOOKUP;
        }
        if ((args->access & ACCESS3_MODIFY) && (granted & CHIMERA_ACE_WRITE_DATA)) {
            res->resok.access |= ACCESS3_MODIFY;
        }
        if ((args->access & ACCESS3_EXTEND) && (granted & CHIMERA_ACE_APPEND_DATA)) {
            res->resok.access |= ACCESS3_EXTEND;
        }
        if ((args->access & ACCESS3_DELETE) && (granted & CHIMERA_ACE_DELETE)) {
            res->resok.access |= ACCESS3_DELETE;
        }
        if ((args->access & ACCESS3_EXECUTE) && (granted & CHIMERA_ACE_EXECUTE)) {
            res->resok.access |= ACCESS3_EXECUTE;
        }
    }

    chimera_nfs3_txn_finish(req, error_code);
} /* chimera_nfs3_access_complete */

static void
chimera_nfs3_access_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs3_txn_finish(req, error_code);
        return;
    }

    req->handle = handle;

    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred, req->txn,
                        handle,
                        CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_ACL,
                        chimera_nfs3_access_complete,
                        req);
} /* chimera_nfs3_access_open_callback */

static void
chimera_nfs3_access_start(struct nfs_request *req)
{
    struct ACCESS3args *args = req->args_access;

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred, req->txn,
                        args->object.data.data,
                        args->object.data.len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_nfs3_access_open_callback,
                        req);
} /* chimera_nfs3_access_start */

void
chimera_nfs3_access(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct ACCESS3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_access(req, args);

    req->args_access = args;

    chimera_nfs3_txn_run(req, args->object.data.data, args->object.data.len,
                         CHIMERA_VFS_TXN_READ,
                         chimera_nfs3_access_start, chimera_nfs3_access_reply);
} /* chimera_nfs3_access */
