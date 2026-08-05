// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
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

/*
 * Per-export read-only gate for mutating NFSv3 procedures.  Call after a
 * successful chimera_nfs3_decode_fh with the export id the mutation targets.
 * RENAME and LINK mutate through two handles and use chimera_nfs3_check_rofs2
 * below instead.
 */
static inline nfsstat3
chimera_nfs3_check_rofs(
    struct nfs_request *req,
    uint16_t            export_id)
{
    return chimera_nfs_export_id_is_ro(req->thread->shared, export_id) ?
           NFS3ERR_ROFS : NFS3_OK;
} /* chimera_nfs3_check_rofs */

/*
 * Per-export read-only gate for the two-directory mutating procedures, RENAME
 * and LINK.  Both handles' exports must be writable: two exports may share one
 * backing VFS mount, so a cross-export operation would otherwise succeed at
 * the VFS layer.  The first side is the request's own export (stamped by
 * chimera_nfs3_decode_fh on the first handle) and is read from the request
 * here rather than passed in, so a call site cannot accidentally check the
 * same export twice; dir2_export_id is the second handle's export as recovered
 * by chimera_nfs_fh_unwrap.  The cross-export combinations are unreachable
 * through a POSIX client (cross-mount RENAME/LINK fails client-side with
 * EXDEV), so tests/test_nfs3_rofs.c drives them directly.
 */
static inline nfsstat3
chimera_nfs3_check_rofs2(
    struct nfs_request *req,
    uint16_t            dir2_export_id)
{
    nfsstat3 status = chimera_nfs3_check_rofs(req, req->export_id);

    if (status == NFS3_OK) {
        status = chimera_nfs3_check_rofs(req, dir2_export_id);
    }
    return status;
} /* chimera_nfs3_check_rofs2 */

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
