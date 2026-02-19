// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stddef.h>
#include "nfs_common.h"
#include "nfs_internal.h"

/* Static root file handle for the nfs4_root pseudo-filesystem */
static const uint8_t *nfs4_root_fh     = (const uint8_t *) "CHIMERA NFS4 ROOT FH";
static uint32_t       nfs4_root_fh_len = 21; /* strlen("CHIMERA NFS4 ROOT FH") */

/**
 * Check if a file handle corresponds to the NFSv4 root pseudo-filesystem.
 *
 * @param fh      Pointer to the file handle buffer.
 * @param fh_len  Length of the file handle buffer.
 * @return        1 if the file handle matches the NFSv4 root, 0 otherwise.
 */
static inline int
fh_is_nfs4_root(
    const uint8_t *fh,
    uint32_t       fh_len)
{
    return (fh_len == nfs4_root_fh_len) && (memcmp(fh, nfs4_root_fh, nfs4_root_fh_len) == 0);
} /* fh_is_nfs4_root */


/**
 * Retrieve the NFSv4 pseudo-root file handle.
 *
 * @param fh      Output buffer to receive the root file handle.
 * @param fh_len  Output pointer to receive the length of the file handle.
 */
static inline void
nfs4_root_get_fh(
    uint8_t  *fh,
    uint32_t *fh_len)
{
    memcpy(fh, nfs4_root_fh, nfs4_root_fh_len);
    *fh_len = nfs4_root_fh_len;
} /* nfs4_root_get_fh */

/**
 * Populate attributes for the NFSv4 pseudo-root directory.
 *
 * @param thread     Pointer to the NFS server thread context.
 * @param attr       Output pointer to the attribute structure to populate.
 * @param attr_mask  Attribute mask specifying which attributes to retrieve.
 */
void
nfs4_root_getattr(
    struct chimera_server_nfs_thread *thread,
    struct chimera_vfs_attrs         *attr,
    uint64_t                          attr_mask);

void
nfs4_root_lookup(
    struct chimera_server_nfs_thread *nfs_thread,
    struct nfs_request               *req);

/**
 * Populate directory entries for the NFSv4 pseudo-root directory.
 *
 * @param thread Pointer to the NFS server thread context.
 * @param req    Pointer to the NFS request structure.
 */
void
nfs4_root_readdir(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req);


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
chimera_nfs4_restorefh(
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
chimera_nfs4_allocate(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_deallocate(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_seek(
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

        /* Per RFC 7530 section 15.2, the response must only include
         * operations up to and including the one that failed. Truncate
         * num_resarray so that the XDR encoder does not attempt to
         * serialize subsequent entries whose resop discriminant is
         * uninitialized, which produces a malformed response. */
        req->res_compound.num_resarray = req->index + 1;
        req->index                     = req->res_compound.num_resarray;
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