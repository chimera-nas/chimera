#pragma once

#include "rpc2/rpc2.h"
#include "portmap_xdr.h"

void chimera_portmap_null(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_portmap_getport(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct mapping        *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);
