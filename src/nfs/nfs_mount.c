#include <stdio.h>
#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_mount.h"
#include "vfs/vfs_procs.h"
#include "uthash/utlist.h"

void
chimera_nfs_mount_null(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;

    shared->mount_v3.send_reply_MOUNTPROC3_NULL(evpl, msg);
} /* chimera_nfs_mount_null */

static void
chimera_nfs_mount_lookup_complete(
    enum chimera_vfs_error error_code,
    const void            *fh,
    int                    fhlen,
    void                  *private_data)
{
    struct nfs_request               *req    = private_data;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct evpl                      *evpl   = thread->evpl;
    struct chimera_server_nfs_shared *shared = thread->shared;

    struct mountres3                  res;

    xdr_dbuf_alloc_opaque(&res.mountinfo.fhandle, fhlen, msg->dbuf);
    memcpy(res.mountinfo.fhandle.data, fh, fhlen);

    chimera_nfs_debug("mount lookup complete error %u fhlen %u",
                      error_code, fhlen);

    if (error_code == CHIMERA_VFS_OK) {
        res.fhs_status = MNT3_OK;
        xdr_dbuf_alloc_opaque(&res.mountinfo.fhandle, fhlen, msg->dbuf);
        memcpy(res.mountinfo.fhandle.data, fh, fhlen);
    } else {
        res.fhs_status = MNT3ERR_NOENT;
    }

    shared->mount_v3.send_reply_MOUNTPROC3_MNT(evpl, &res, msg);

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

    req = nfs_request_alloc(thread);

    req->msg = msg;

    chimera_vfs_lookup_path(thread->vfs,
                            args->path.str,
                            args->path.len,
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

    shared->mount_v3.send_reply_MOUNTPROC3_UMNT(evpl, msg);
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
    struct exportnode                 export;

    shared->mount_v3.send_reply_MOUNTPROC3_EXPORT(evpl, &export, msg);
} /* chimera_nfs_mount_export */
