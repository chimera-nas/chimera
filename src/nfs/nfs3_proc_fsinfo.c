#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "vfs/vfs_procs.h"

void
chimera_nfs3_fsinfo(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct FSINFO3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct FSINFO3res                 res;

    res.status = NFS3_OK;

    res.resok.obj_attributes.attributes_follow = 0;

    res.resok.maxfilesize         = UINT64_MAX;
    res.resok.time_delta.seconds  = 0;
    res.resok.time_delta.nseconds = 1;
    res.resok.rtmax               = 128 * 1024;
    res.resok.rtpref              = 128 * 1024;
    res.resok.rtmult              = 4096;
    res.resok.wtmax               = 128 * 1024;
    res.resok.wtpref              = 128 * 1024;
    res.resok.wtmult              = 4096;
    res.resok.dtpref              = 64 * 1024;
    res.resok.properties          = FSF3_LINK | FSF3_SYMLINK |
        FSF3_HOMOGENEOUS | FSF3_CANSETTIME;

    shared->nfs_v3.send_reply_NFSPROC3_FSINFO(evpl, &res, msg);



} /* chimera_nfs3_fsinfo */
