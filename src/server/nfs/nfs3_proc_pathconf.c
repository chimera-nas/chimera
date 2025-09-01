// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include "nfs3_procs.h"
#include "nfs3_dump.h"

void
chimera_nfs3_pathconf(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct PATHCONF3args  *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct PATHCONF3res               res;

    nfs3_dump_pathconf(NULL, args);

    res.status = NFS3_OK;

    res.resok.obj_attributes.attributes_follow = 0;

    res.resok.case_insensitive = 0;
    res.resok.case_preserving  = 1;
    res.resok.no_trunc         = 1;
    res.resok.linkmax          = UINT32_MAX;
    res.resok.name_max         = 255;

    shared->nfs_v3.send_reply_NFSPROC3_PATHCONF(evpl, &res, msg);
} /* chimera_nfs3_pathconf */
