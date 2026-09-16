// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <fcntl.h>
#include <string.h>

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"

static void
chimera_nfs3_create_reply(
    struct nfs_request             *req,
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct CREATE3res                 res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        if (attr->va_set_mask & CHIMERA_VFS_ATTR_FH) {
            uint8_t wire[CHIMERA_NFS_FH_MAX];
            int     wirelen;

            chimera_nfs_fh_encode(req, handle->fh, handle->fh_len, wire, &wirelen);
            res.resok.obj.handle_follows = 1;
            rc                           = xdr_dbuf_opaque_copy(&res.resok.obj.handle.data,
                                                                wire,
                                                                wirelen,
                                                                req->encoding->dbuf);
            chimera_nfs_abort_if(rc, "Failed to copy opaque");
        } else {
            res.resok.obj.handle_follows = 0;
        }
        chimera_nfs3_set_post_op_attr(&res.resok.obj_attributes, attr);
        chimera_nfs3_set_wcc_data(&res.resok.dir_wcc, dir_pre_attr, dir_post_attr);
        chimera_vfs_release(thread->vfs_thread, handle);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, dir_pre_attr, dir_post_attr);
    }

    /* The parent open belonged to the sequence and went with it. */

    rc = shared->nfs_v3.send_reply_NFSPROC3_CREATE(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
    nfs_request_free(thread, req);
} /* chimera_nfs3_create_reply */

/*
 * PUTFH, OPEN(parent), OPEN(name).
 *
 * An EXCLUSIVE create asks for EXCLUSIVE_RETRY, so a name that is already taken
 * is re-opened rather than failed, and the op reports `existed`.  That is the
 * whole of RFC 1813's exclusive-create idempotency: the verifier the client
 * stamped into atime/mtime is compared against what is actually there, and a
 * match means this is the same client's own earlier CREATE arriving twice.
 * Without the retry the caller would see EEXIST and have to re-open by hand,
 * which is what this used to do.
 */
static void
chimera_nfs3_create_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req  = private_data;
    struct CREATE3args                   *args = req->args_create;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_open_handle       *handle = NULL;
    struct chimera_vfs_attrs              attr, pre_attr, post_attr;
    enum chimera_vfs_error                status;
    uint32_t                              idx, verf_atime, verf_mtime;

    status = chimera_vfs_compound_status(compound);

    memset(&attr, 0, sizeof(attr));

    idx = chimera_vfs_compound_num_ops(compound) - 1;
    op  = chimera_vfs_compound_op(compound, idx);

    /* RFC 1813 3.3.8 requires dir_wcc even when the CREATE failed. */
    pre_attr  = op->dir_pre_attr;
    post_attr = op->dir_post_attr;

    if (status == CHIMERA_VFS_OK) {
        attr   = op->attr;
        handle = chimera_vfs_compound_take_handle(compound, idx);

        if (args->how.mode == EXCLUSIVE && op->existed) {
            memcpy(&verf_atime, args->how.verf, sizeof(verf_atime));
            memcpy(&verf_mtime, args->how.verf + sizeof(verf_atime),
                   sizeof(verf_mtime));

            if (attr.va_atime.tv_sec != verf_atime ||
                attr.va_mtime.tv_sec != verf_mtime) {
                /* Somebody else's file sits at the name. */
                status = CHIMERA_VFS_EEXIST;
            }
        }
    }

    chimera_vfs_compound_free(compound);

    if (status != CHIMERA_VFS_OK && handle) {
        chimera_vfs_release(req->thread->vfs_thread, handle);
        handle = NULL;
    }

    chimera_nfs3_create_reply(req, status, handle,
                              status == CHIMERA_VFS_OK ? &attr : NULL,
                              &pre_attr, &post_attr);
} /* chimera_nfs3_create_sequence_complete */

void
chimera_nfs3_create(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct CREATE3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct chimera_vfs_compound      *compound;
    struct chimera_vfs_attrs         *attr;
    unsigned int                      flags, opts;
    struct CREATE3res                 res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_create(req, args);
    nfs3_trace_create(req, args);

    req->args_create = args;

    res.status = chimera_nfs3_decode_fh(req, args->where.dir.data.data, args->where.dir.data.len);
    if (res.status == NFS3_OK) {
        res.status = chimera_nfs3_check_rofs(req, req->export_id);
    }
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_CREATE(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    attr = xdr_dbuf_alloc_space(sizeof(*attr), req->encoding->dbuf);
    chimera_nfs_abort_if(attr == NULL, "Failed to allocate space");

    attr->va_req_mask = 0;
    attr->va_set_mask = 0;
    flags             = CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_INFERRED;
    opts              = 0;

    switch (args->how.mode) {
        case UNCHECKED:
            chimera_nfs3_sattr3_to_va(attr, &args->how.obj_attributes);
            /* CREATE materializes a regular file: an existing directory at the
             * name gives NFS3ERR_ISDIR and any other non-regular object gives
             * NFS3ERR_EXIST.  (GUARDED/EXCLUSIVE below already collide on any
             * existing object via OPEN_EXCLUSIVE.) */
            flags |= CHIMERA_VFS_OPEN_CREATE_REGULAR;
            break;
        case GUARDED:
            chimera_nfs3_sattr3_to_va(attr, &args->how.obj_attributes);
            flags |= CHIMERA_VFS_OPEN_EXCLUSIVE;
            break;
        case EXCLUSIVE:
            flags |= CHIMERA_VFS_OPEN_EXCLUSIVE;
            /* A collision is not an answer yet -- the verifier decides. */
            opts |= CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY;
            /* EXCLUSIVE create carries the verifier in the sattr slot, not a
             * mode, so the file's permissions are undefined until the client's
             * follow-up SETATTR (RFC 1813 3.3.8).  Create it owner-only (0600),
             * matching Linux nfsd, NFS-Ganesha and the quint model. */
            attr->va_set_mask = CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME |
                CHIMERA_VFS_ATTR_MODE;
            attr->va_mode = 0600;
            /* Zero-extend the two 32-bit verifier halves into the 64-bit tv_sec
             * via uint32 temporaries; a bare 4-byte memcpy leaves the high bytes
             * as stale dbuf data, which breaks EXCLUSIVE-create retransmit
             * idempotency against the uint32 read back at verify time. */
            uint32_t verf_lo, verf_hi;
            memcpy(&verf_lo, args->how.verf, 4);
            memcpy(&verf_hi, args->how.verf + 4, 4);
            attr->va_atime.tv_sec  = verf_lo;
            attr->va_atime.tv_nsec = 0;
            attr->va_mtime.tv_sec  = verf_hi;
            attr->va_mtime.tv_nsec = 0;
            break;
    } /* switch */

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY, 0);
    chimera_vfs_compound_add_open(compound,
                                  args->where.name.str,
                                  args->where.name.len,
                                  flags, opts, attr,
                                  CHIMERA_NFS3_ATTR_MASK |
                                  CHIMERA_VFS_ATTR_FH,
                                  CHIMERA_NFS3_ATTR_WCC_MASK,
                                  CHIMERA_NFS3_ATTR_MASK);

    chimera_vfs_compound_submit(compound,
                                chimera_nfs3_create_sequence_complete, req);
} /* chimera_nfs3_create */
