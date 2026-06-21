// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "nfs_common.h"

/*
 * Decode a wire NFSv3 filehandle into req->fh and translate a decode failure to
 * the appropriate NFSv3 status.  A security-policy rejection -- the request's
 * RPC auth flavor is not permitted by the target export's sec= list -- maps to
 * NFS3ERR_ACCES; any other malformed/unrecognized handle stays NFS3ERR_BADHANDLE.
 * (NFSv3 has no dedicated wrong-security error, unlike NFSv4's NFS4ERR_WRONGSEC,
 * so a permission error is the closest honest answer.)  Returns NFS3_OK on
 * success.
 */
static inline nfsstat3
chimera_nfs3_decode_fh(
    struct nfs_request *req,
    const void         *fhdata,
    uint32_t            fhlen)
{
    switch (chimera_nfs_fh_decode(req, fhdata, fhlen, req->fh, &req->fhlen)) {
        case CHIMERA_NFS_FH_OK:
            return NFS3_OK;
        case CHIMERA_NFS_FH_WRONGSEC:
            return NFS3ERR_ACCES;
        default:
            return NFS3ERR_BADHANDLE;
    } /* switch */
} /* chimera_nfs3_decode_fh */

void chimera_nfs3_null(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_getattr(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct GETATTR3args       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_setattr(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct SETATTR3args       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_lookup(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct LOOKUP3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_access(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct ACCESS3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_readlink(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct READLINK3args      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_read(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct READ3args          *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_write(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct WRITE3args         *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_create(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct CREATE3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_mkdir(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct MKDIR3args         *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_symlink(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct SYMLINK3args       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_mknod(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct MKNOD3args         *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_remove(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct REMOVE3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_rmdir(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct RMDIR3args         *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_rename(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct RENAME3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_link(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct LINK3args          *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_readdir(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct READDIR3args       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_readdirplus(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct READDIRPLUS3args   *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_fsstat(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct FSSTAT3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_fsinfo(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct FSINFO3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_pathconf(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct PATHCONF3args      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_commit(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct COMMIT3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_secinfo_no_name(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);
