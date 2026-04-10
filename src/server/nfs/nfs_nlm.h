// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "nlm4_xdr.h"

void chimera_nfs_nlm4_null(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_test(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_testargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_lock(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_lockargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_cancel(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_cancargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_unlock(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_unlockargs    *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_granted(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_testargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_test_msg(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_testargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_lock_msg(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_lockargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_cancel_msg(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_cancargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_unlock_msg(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_unlockargs    *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_granted_msg(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_testargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_test_res(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_testres       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_lock_res(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_res           *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_cancel_res(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_res           *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_unlock_res(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_res           *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_granted_res(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_res           *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_reserved_16(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_reserved_17(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_reserved_18(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_reserved_19(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_share(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_shareargs     *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_unshare(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_shareargs     *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_nm_lock(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_lockargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_nlm4_free_all(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_notify        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);
