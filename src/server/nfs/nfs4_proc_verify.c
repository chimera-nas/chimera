// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "server/server.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

/* RFC 7530 §16.31 (VERIFY) and §16.14 (NVERIFY).
 *
 * Compare the client-supplied attrmask+attr_vals against the server's
 * current attributes for the current filehandle.
 *
 * VERIFY  returns NFS4_OK if all match, NFS4ERR_NOT_SAME otherwise.
 * NVERIFY returns NFS4_OK if any differ, NFS4ERR_SAME otherwise.
 *
 * Validation (both share the same rule set):
 *   - no current FH                        → NFS4ERR_NOFILEHANDLE
 *   - unsupported attrs in mask            → NFS4ERR_ATTRNOTSUPP
 *   - write-only attrs in mask             → NFS4ERR_INVAL
 *
 * The comparison marshals the server's current attrs into the same
 * fattr4 byte stream the client sent, then memcmp. This relies on the
 * marshaller producing canonical output (it does — see
 * chimera_nfs4_marshall_attrs).
 */

static struct fattr4 *
verify_args_fattr4(struct nfs_request *req)
{
    struct nfs_argop4 *argop = &req->args_compound->argarray[req->index];

    /* VERIFY4args and NVERIFY4args are layout-identical (a single fattr4).
     * The discriminant we pivot on is argop->argop. */
    if (argop->argop == OP_NVERIFY) {
        return &argop->opnverify.obj_attributes;
    }
    return &argop->opverify.obj_attributes;
} /* verify_args_fattr4 */

static bool
verify_is_nverify(struct nfs_request *req)
{
    return req->args_compound->argarray[req->index].argop == OP_NVERIFY;
} /* verify_is_nverify */

static void
chimera_nfs4_verify_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request      *req = private_data;
    /* VERIFY4res and NVERIFY4res are layout-identical (status only). */
    struct VERIFY4res       *res        = &req->res_compound.resarray[req->index].opverify;
    struct fattr4           *args       = verify_args_fattr4(req);
    bool                     is_nverify = verify_is_nverify(req);
    struct chimera_vfs_attrs marshall_attr;
    nfsstat4                 status;

    chimera_vfs_release(req->thread->vfs_thread, req->handle);

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* Marshal current attrs into the same on-wire format the client sent
     * us, restricted to the bits in the request mask. */
    uint32_t out_mask[3] = { 0, 0, 0 };
    uint32_t num_out_mask;
    uint8_t  out_buf[4096];
    uint32_t out_len = 0;

    marshall_attr = *attr;
    chimera_nfs4_attrs_fill_filehandle(&marshall_attr,
                                       args->num_attrmask,
                                       args->attrmask,
                                       req->fh,
                                       req->fhlen);

    chimera_nfs4_marshall_attrs(&marshall_attr,
                                args->num_attrmask,
                                args->attrmask,
                                &num_out_mask,
                                out_mask,
                                3,
                                out_buf,
                                &out_len,
                                sizeof(out_buf),
                                req->minorversion,
                                chimera_nfs4_pnfs_layout_type(req->thread->vfs_thread,
                                                              req->thread->shared->vfs,
                                                              req->fh, req->fhlen),
                                chimera_nfs4_xattr_supported(req->thread->vfs_thread,
                                                             req->fh, req->fhlen),
                                chimera_server_config_get_nfs4_delegations(
                                    req->thread->shared->config),
                                req->thread->shared->nfs_lease_time_s);

    bool match = (num_out_mask == args->num_attrmask) &&
        (memcmp(out_mask, args->attrmask,
                num_out_mask * sizeof(uint32_t)) == 0) &&
        (out_len == args->attr_vals.len) &&
        (memcmp(out_buf, args->attr_vals.data, out_len) == 0);

    if (is_nverify) {
        status = match ? NFS4ERR_SAME : NFS4_OK;
    } else {
        status = match ? NFS4_OK : NFS4ERR_NOT_SAME;
    }

    res->status = status;
    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_verify_complete */

static void
chimera_nfs4_verify_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req  = private_data;
    struct VERIFY4res  *res  = &req->res_compound.resarray[req->index].opverify;
    struct fattr4      *args = verify_args_fattr4(req);

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->handle = handle;

    uint64_t attr_mask = chimera_nfs4_attr2mask(args->attrmask,
                                                args->num_attrmask);

    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred, NULL,
                        handle,
                        attr_mask,
                        chimera_nfs4_verify_complete,
                        req);
} /* chimera_nfs4_verify_open_callback */

static nfsstat4
verify_validate_mask(
    uint32_t        num,
    const uint32_t *mask)
{
    static const uint32_t supported_word0 =
        (1 << FATTR4_SUPPORTED_ATTRS) | (1 << FATTR4_TYPE) |
        (1 << FATTR4_FH_EXPIRE_TYPE)  | (1 << FATTR4_CHANGE) |
        (1 << FATTR4_SIZE)            | (1 << FATTR4_LINK_SUPPORT) |
        (1 << FATTR4_SYMLINK_SUPPORT) | (1 << FATTR4_NAMED_ATTR) |
        (1 << FATTR4_FSID)            | (1 << FATTR4_UNIQUE_HANDLES) |
        (1 << FATTR4_LEASE_TIME)      | (1 << FATTR4_RDATTR_ERROR) |
        (1 << FATTR4_ACLSUPPORT)      | (1 << FATTR4_ARCHIVE) |
        (1 << FATTR4_CANSETTIME)      | (1 << FATTR4_CASE_INSENSITIVE) |
        (1 << FATTR4_CASE_PRESERVING) | (1 << FATTR4_CHOWN_RESTRICTED) |
        (1 << FATTR4_FILEHANDLE)      | (1 << FATTR4_FILEID) |
        (1 << FATTR4_FILES_AVAIL)     | (1 << FATTR4_FILES_FREE) |
        (1 << FATTR4_FILES_TOTAL)     | (1 << FATTR4_MAXNAME) |
        (1 << FATTR4_MAXREAD)         | (1U << FATTR4_MAXWRITE);
    static const uint32_t supported_word1 =
        (1U << (FATTR4_MODE - 32))         | (1U << (FATTR4_NUMLINKS - 32)) |
        (1U << (FATTR4_OWNER - 32))        | (1U << (FATTR4_OWNER_GROUP - 32)) |
        (1U << (FATTR4_SPACE_AVAIL - 32))  | (1U << (FATTR4_SPACE_FREE - 32)) |
        (1U << (FATTR4_SPACE_TOTAL - 32))  | (1U << (FATTR4_SPACE_USED - 32)) |
        (1U << (FATTR4_TIME_ACCESS - 32))  | (1U << (FATTR4_TIME_METADATA - 32)) |
        (1U << (FATTR4_TIME_MODIFY - 32));
    static const uint32_t writeonly_word1 =
        (1U << (FATTR4_TIME_ACCESS_SET - 32)) |
        (1U << (FATTR4_TIME_MODIFY_SET - 32));

    /* FATTR4_RDATTR_ERROR is only meaningful inside READDIR's per-entry
     * attrs; requesting it via GETATTR/VERIFY is INVAL per RFC 7530 §5.6. */
    static const uint32_t writeonly_word0 = (1U << FATTR4_RDATTR_ERROR);

    if (num >= 1 && (mask[0] & writeonly_word0)) {
        return NFS4ERR_INVAL;
    }
    if (num >= 1 && (mask[0] & ~supported_word0)) {
        return NFS4ERR_ATTRNOTSUPP;
    }
    if (num >= 2 && (mask[1] & writeonly_word1)) {
        return NFS4ERR_INVAL;
    }
    if (num >= 2 && (mask[1] & ~(supported_word1 | writeonly_word1))) {
        return NFS4ERR_ATTRNOTSUPP;
    }
    if (num >= 3 && mask[2] != 0) {
        return NFS4ERR_ATTRNOTSUPP;
    }
    return NFS4_OK;
} /* verify_validate_mask */

static void
chimera_nfs4_verify_dispatch(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    struct VERIFY4res *res  = &req->res_compound.resarray[req->index].opverify;
    struct fattr4     *args = verify_args_fattr4(req);

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    res->status = verify_validate_mask(args->num_attrmask, args->attrmask);
    if (res->status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred, NULL,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_nfs4_verify_open_callback,
                        req);
} /* chimera_nfs4_verify_dispatch */

void
chimera_nfs4_verify(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    chimera_nfs4_verify_dispatch(thread, req);
} /* chimera_nfs4_verify */

void
chimera_nfs4_nverify(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    chimera_nfs4_verify_dispatch(thread, req);
} /* chimera_nfs4_nverify */
