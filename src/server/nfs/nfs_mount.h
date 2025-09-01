// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "nfs_mount_xdr.h"

void chimera_nfs_mount_null(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs_mount_mnt(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct mountarg3      *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs_mount_dump(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs_mount_umnt(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct mountarg3      *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs_mount_umntall(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs_mount_export(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);
