// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "evpl/evpl_rpc2.h"
#include "portmap_xdr.h"

void chimera_portmap_null_v2(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_getport_v2(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct mapping            *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_set_v2(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct mapping            *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_unset_v2(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct mapping            *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_dump_v2(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_callit_v2(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct call_args          *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_set_v3(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct rpcb               *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_unset_v3(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct rpcb               *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_getaddr_v3(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct rpcb               *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_dump_v3(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_callit_v3(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct rpcb_rmtcallargs   *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_gettime_v3(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_uaddr2taddr_v3(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    xdr_string                *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_taddr2uaddr_v3(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct netbuf             *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_set_v4(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct rpcb               *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_unset_v4(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct rpcb               *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_getaddr_v4(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct rpcb               *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_dump_v4(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_callit_v4(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct rpcb_rmtcallargs   *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_gettime_v4(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_uaddr2taddr_v4(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    xdr_string                *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_taddr2uaddr_v4(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct netbuf             *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_getversaddr_v4(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct rpcb               *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_indirect_v4(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct rpcb_rmtcallargs   *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_getaddrlist_v4(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct rpcb               *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_portmap_getstat_v4(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);
