#pragma once

#include "nfs_common.h"

void
chimera_nfs4_null(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void
chimera_nfs4_compound(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct COMPOUND4args  *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);