// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "nfs_common.h"
#include "nfs_internal.h"

void
chimera_nfs4_access(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_getfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_putrootfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_putfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_savefh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_link(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_rename(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_getattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_setattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_lookup(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_read(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_write(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_commit(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_open(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_create(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_readdir(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_readlink(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_remove(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_close(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_setclientid(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_setclientid_confirm(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_exchange_id(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_create_session(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_destroy_session(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_destroy_clientid(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_sequence(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_reclaim_complete(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_test_stateid(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_secinfo_no_name(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_compound_process(
    struct nfs_request *req,
    nfsstat4            status);

static inline void
chimera_nfs4_compound_complete(
    struct nfs_request *req,
    nfsstat4            status)
{
    struct chimera_server_nfs_thread *thread = req->thread;

    if (status != NFS4_OK) {
        req->res_compound.status = status;

        /* Set the status for the failed operation */
        req->res_compound.resarray[req->index].opillegal.status = status;

        req->index++;

        while (req->index < req->res_compound.num_resarray) {
            req->res_compound.resarray[req->index].opillegal.status = NFS4ERR_NOTSUPP;
            req->index++;
        }
    }

    if (thread->active) {
        thread->again = 1;
    } else {
        req->index++;
        chimera_nfs4_compound_process(req, status);
    }

} /* chimera_nfs4_compound_complete */

void
chimera_nfs4_null(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void
chimera_nfs4_compound(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct COMPOUND4args      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);