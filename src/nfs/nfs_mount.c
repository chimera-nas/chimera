#include <stdio.h>
#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_mount.h"

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

void
chimera_nfs_mount_mnt(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct mountarg3      *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct mountres3                  res;

    chimera_nfs_debug("mount request for '%.*s'", args->path.len, args->path.str
                      );

    res.fhs_status = MNT3_OK;

    xdr_dbuf_alloc_opaque(&res.mountinfo.fhandle, 1, msg->dbuf);
    memcpy(res.mountinfo.fhandle.data, "1", 1);

    res.mountinfo.num_auth_flavors = 0;

    shared->mount_v3.send_reply_MOUNTPROC3_MNT(evpl, &res, msg);
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
