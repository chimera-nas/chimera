// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <utlist.h>

#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_mount.h"
#include "vfs/vfs_procs.h"

void
chimera_nfs_mount_null(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    rc = shared->mount_v3.send_reply_MOUNTPROC3_NULL(evpl, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_nfs_mount_null */

static void
chimera_nfs_mount_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct evpl                      *evpl   = thread->evpl;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int32_t                           auth_flavors[2];
    struct mountres3                  res;
    int                               rc;

    if (error_code == CHIMERA_VFS_OK) {
        res.fhs_status                 = MNT3_OK;
        res.mountinfo.num_auth_flavors = 2;
        res.mountinfo.auth_flavors     = auth_flavors;
        auth_flavors[0]                = AUTH_NONE;
        auth_flavors[1]                = AUTH_SYS;

        chimera_nfs_abort_if(!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH),
                             "NFS mount: no file handle was returned");

        rc = xdr_dbuf_alloc_opaque(&res.mountinfo.fhandle,
                                   attr->va_fh_len,
                                   msg->dbuf);
        chimera_nfs_abort_if(rc, "Failed to allocate opaque");
        memcpy(res.mountinfo.fhandle.data,
               attr->va_fh,
               attr->va_fh_len);
    } else {
        res.fhs_status = MNT3ERR_NOENT;
    }

    rc = shared->mount_v3.send_reply_MOUNTPROC3_MNT(evpl, &res, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs_mount_lookup_complete */

void
chimera_nfs_mount_mnt(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct mountarg3      *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;
    uint8_t                          *fh_magic;

    fh_magic = xdr_dbuf_alloc_space(sizeof(*fh_magic), msg->dbuf);
    chimera_nfs_abort_if(fh_magic == NULL, "Failed to allocate space");

    *fh_magic = CHIMERA_VFS_FH_MAGIC_ROOT;

    req = nfs_request_alloc(thread, conn, msg);

    chimera_vfs_lookup_path(thread->vfs_thread,
                            fh_magic,
                            sizeof(*fh_magic),
                            args->path.str,
                            args->path.len,
                            CHIMERA_VFS_ATTR_FH,
                            CHIMERA_VFS_LOOKUP_FOLLOW,
                            chimera_nfs_mount_lookup_complete,
                            req);

} /* chimera_nfs_mount_mnt */

void
chimera_nfs_mount_dump(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
} /* chimera_nfs_mount_dump */

void
chimera_nfs_mount_umnt(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct mountarg3      *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    rc = shared->mount_v3.send_reply_MOUNTPROC3_UMNT(evpl, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_nfs_mount_umnt */

void
chimera_nfs_mount_umntall(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
} /* chimera_nfs_mount_umntall */

void
chimera_nfs_mount_export(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct exportres                  export;
    int                               rc;

    export.exports = NULL;

    rc = shared->mount_v3.send_reply_MOUNTPROC3_EXPORT(evpl, &export, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
} /* chimera_nfs_mount_export */
