// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "nfs_common.h"

/*
 * Largest payload chimera will move in a single NFSv3 READ or WRITE.  FSINFO
 * advertises this as rtmax/rtpref and wtmax/wtpref, and READ and WRITE clamp
 * the wire count to it, so the advertised limit and the enforced limit cannot
 * drift apart.  RFC 1813 3.3.6/3.3.7 permit the resulting short read or write.
 */
#define CHIMERA_NFS3_MAX_XFER (1024 * 1024)

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
 * Decode the second (destination) wire handle of RENAME / LINK into
 * req->saved_fh and report its export id.
 *
 * The first handle went through chimera_nfs3_decode_fh, which stamped
 * req->export_id and squashed req->cred under that export.  Two exports may
 * name sub-paths of one VFS share, so the destination directory can belong to
 * a different export whose policy the VFS-level cross-mount check will not
 * catch: its sec= list must permit the request's auth flavor, and its squash
 * must apply to the credential the new entry is created under.  Unwrapping
 * alone enforces neither.
 *
 * Squashing only ever maps toward the anonymous identity, so layering the
 * destination export's policy on top of the source's yields the more
 * restrictive of the two and is a no-op when both handles name the same
 * export.  A sec= rejection maps to NFS3ERR_ACCES for the same reason as in
 * chimera_nfs3_decode_fh.
 */
static inline nfsstat3
chimera_nfs3_decode_fh2(
    struct nfs_request *req,
    const void         *fhdata,
    uint32_t            fhlen,
    uint16_t           *export_id)
{
    struct chimera_server_nfs_shared *shared = req->thread->shared;

    if (chimera_nfs_fh_unwrap(fhdata, fhlen, export_id,
                              req->saved_fh, &req->saved_fhlen,
                              shared->fh_key, shared->fh_sign) !=
        CHIMERA_NFS_FH_OK) {
        return NFS3ERR_BADHANDLE;
    }

    const struct chimera_nfs_export *export =
        chimera_nfs_get_export_by_id(shared, *export_id);

    if (!chimera_nfs_export_sec_ok(export, req->sec_bit)) {
        return NFS3ERR_ACCES;
    }

    chimera_nfs_squash_cred(&req->cred, export);

    return NFS3_OK;
} /* chimera_nfs3_decode_fh2 */

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
