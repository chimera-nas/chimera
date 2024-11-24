#include "nfs3_procs.h"
#include "nfs_internal.h"

void
chimera_nfs3_null(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;

    shared->nfs_v3.send_reply_NFSPROC3_NULL(evpl, msg);
} /* nfs3_null */

void
chimera_nfs3_getattr(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    GETATTR3args          *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_GETATTR
} /* chimera_nfs3_getattr */

void
chimera_nfs3_setattr(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    SETATTR3args          *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_SETATTR
} /* chimera_nfs3_setattr */

void
chimera_nfs3_lookup(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    LOOKUP3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_LOOKUP
} /* chimera_nfs3_lookup */

void
chimera_nfs3_access(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    ACCESS3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_ACCESS
} /* chimera_nfs3_access */

void
chimera_nfs3_readlink(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    READLINK3args         *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_READLINK
} /* chimera_nfs3_readlink */

void
chimera_nfs3_read(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    READ3args             *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_READ
} /* chimera_nfs3_read */

void
chimera_nfs3_write(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    WRITE3args            *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_WRITE
} /* chimera_nfs3_write */

void
chimera_nfs3_create(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    CREATE3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_CREATE
} /* chimera_nfs3_create */

void
chimera_nfs3_mkdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    MKDIR3args            *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_MKDIR
} /* chimera_nfs3_mkdir */

void
chimera_nfs3_symlink(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    SYMLINK3args          *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_SYMLINK
} /* chimera_nfs3_symlink */

void
chimera_nfs3_mknod(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    MKNOD3args            *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_MKNOD
} /* chimera_nfs3_mknod */

void
chimera_nfs3_remove(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    REMOVE3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_REMOVE
} /* chimera_nfs3_remove */

void
chimera_nfs3_rmdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    RMDIR3args            *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_RMDIR
} /* chimera_nfs3_rmdir */

void
chimera_nfs3_rename(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    RENAME3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_RENAME
} /* chimera_nfs3_rename */

void
chimera_nfs3_link(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    LINK3args             *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_LINK
} /* chimera_nfs3_link */

void
chimera_nfs3_readdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    READDIR3args          *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_READDIR
} /* chimera_nfs3_readdir */

void
chimera_nfs3_readdirplus(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    READDIRPLUS3args      *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_READDIRPLUS
} /* chimera_nfs3_readdirplus */

void
chimera_nfs3_fsstat(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    FSSTAT3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_FSSTAT
} /* chimera_nfs3_fsstat */

void
chimera_nfs3_fsinfo(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    FSINFO3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_FSINFO
} /* chimera_nfs3_fsinfo */

void
chimera_nfs3_pathconf(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    PATHCONF3args         *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_PATHCONF
} /* chimera_nfs3_pathconf */

void
chimera_nfs3_commit(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    COMMIT3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    // TODO: Implement NFSPROC3_COMMIT
} /* chimera_nfs3_commit */