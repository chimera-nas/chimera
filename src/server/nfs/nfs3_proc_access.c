// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/sdk/vfs_acl.h"
#include "vfs/sdk/vfs_access.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"

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
    /* RFC 1813 §3.3.4: ACCESS3_DELETE is "delete an existing directory entry",
     * a property of the directory being queried -- not of deleting that
     * directory itself.  POSIX governs it with write+execute on the directory,
     * which the engine exposes as DELETE_CHILD (CHIMERA_ACE_DELETE would ask
     * whether the caller may unlink the directory from *its* parent, which an
     * ACCESS on the directory cannot answer).  Matches knfsd's NFSD_MAY_REMOVE
     * mapping in its nfs3_diraccess table. */
    if (access & ACCESS3_DELETE) {
        mask |= CHIMERA_ACE_DELETE_CHILD;
    }
    if (access & ACCESS3_EXECUTE) {
        mask |= CHIMERA_ACE_EXECUTE;
    }

    return mask;
} /* chimera_nfs3_access3_to_mask */

static void
chimera_nfs3_access_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct ACCESS3args               *args   = req->args_access;
    struct ACCESS3res                 res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_post_op_attr(&res.resok.obj_attributes, attr);

        /* RFC 1813 3.3.4: ACCESS3_LOOKUP and ACCESS3_DELETE are meaningful only
         * for a directory, ACCESS3_EXECUTE only for a non-directory.  Mask the
         * request to the type-applicable bits before evaluating, so a
         * privileged caller -- whose DAC override grants every requested bit --
         * cannot report, e.g., EXECUTE on a directory or LOOKUP on a file.  The
         * NFSv4 ACCESS path already does this via chimera_nfs4_access_meaningful;
         * this brings NFSv3 in line with it, the Linux server, NFS-Ganesha and
         * the model. */
        uint32_t meaningful = ACCESS3_READ | ACCESS3_MODIFY | ACCESS3_EXTEND;

        if (S_ISDIR(attr->va_mode)) {
            meaningful |= ACCESS3_LOOKUP | ACCESS3_DELETE;
        } else {
            meaningful |= ACCESS3_EXECUTE;
        }

        uint32_t access = args->access & meaningful;

        /* Evaluate the canonical ACL (or mode fallback) once via the shared
         * gate -- this honours the caller's full credential rather than the
         * legacy owner-bits-only check. */
        uint32_t granted = chimera_vfs_access_check(
            attr, &req->cred,
            chimera_nfs3_access3_to_mask(access));

        res.resok.access = 0;

        if ((access & ACCESS3_READ) && (granted & CHIMERA_ACE_READ_DATA)) {
            res.resok.access |= ACCESS3_READ;
        }
        if ((access & ACCESS3_LOOKUP) && (granted & CHIMERA_ACE_EXECUTE)) {
            res.resok.access |= ACCESS3_LOOKUP;
        }
        if ((access & ACCESS3_MODIFY) && (granted & CHIMERA_ACE_WRITE_DATA)) {
            res.resok.access |= ACCESS3_MODIFY;
        }
        if ((access & ACCESS3_EXTEND) && (granted & CHIMERA_ACE_APPEND_DATA)) {
            res.resok.access |= ACCESS3_EXTEND;
        }
        if ((access & ACCESS3_DELETE) && (granted & CHIMERA_ACE_DELETE_CHILD)) {
            res.resok.access |= ACCESS3_DELETE;
        }
        if ((access & ACCESS3_EXECUTE) && (granted & CHIMERA_ACE_EXECUTE)) {
            res.resok.access |= ACCESS3_EXECUTE;
        }

        /* A read-only export never grants write-class access, regardless of
         * what the ACL/mode would allow. */
        if (chimera_nfs_export_id_is_ro(shared, req->export_id)) {
            res.resok.access &= ~(ACCESS3_MODIFY | ACCESS3_EXTEND | ACCESS3_DELETE);
        }
    } else {
        chimera_nfs3_set_post_op_attr(&res.resfail.obj_attributes, attr);
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

    rc = shared->nfs_v3.send_reply_NFSPROC3_ACCESS(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_access_complete */

static void
chimera_nfs3_access_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct ACCESS3res                 res;
    int                               rc;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        chimera_vfs_getattr(thread->vfs_thread, &req->cred,
                            handle,
                            CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_ACL,
                            chimera_nfs3_access_complete,
                            req);
    } else {
        res.status = chimera_vfs_error_to_nfsstat3(error_code);
        rc         = shared->nfs_v3.send_reply_NFSPROC3_ACCESS(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_access_open_callback */

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
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct ACCESS3res                 res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_access(req, args);
    nfs3_trace_access(req, args);

    req->args_access = args;

    res.status = chimera_nfs3_decode_fh(req, args->object.data.data, args->object.data.len);
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_ACCESS(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_nfs3_access_open_callback,
                        req);
} /* chimera_nfs3_access */
