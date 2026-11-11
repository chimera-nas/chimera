// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_state.h"
#include "nfs4_session.h"
#include "nfs4_stateid.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

/*
 * CLONE (RFC 7862 15.13) reflinks a byte range from the SAVED_FH (source) into
 * the CURRENT_FH (destination): the ranges share physical storage copy-on-write.
 * Chimera projects this onto the backend's native reflink (chimera_vfs_clone_range,
 * CAP_CLONE_RANGE).  A cross-module clone, or a backend without reflink support,
 * yields NFS4ERR_NOTSUPP.  cl_count == 0 means "to the end of the source file".
 *
 * cl_src_stateid must be READ-valid and cl_dst_stateid WRITE-valid, following
 * the general stateid rules of RFC 5661 8.2.5 / 18.22.3 / 18.32.3 (RFC 7862
 * 15.13.3) -- the same rules COPY, READ and WRITE follow.  That includes the
 * special (anonymous / READ-bypass) stateids, which name no state-table entry
 * and are judged by the share-reservation deny modes held by other clients,
 * exactly as for COPY/ALLOCATE/DEALLOCATE; a wrong-filehandle or
 * wrong-client stateid is NFS4ERR_BAD_STATEID; an OPEN/LOCK stateid lacking
 * the required access bit is NFS4ERR_OPENMODE; a delegation is judged by
 * nfs_delegation_check_io_for_fh; and a LAYOUT (or any other) stateid grants
 * no I/O here and falls out as NFS4ERR_BAD_STATEID via
 * nfs_state_check_access_for_fh.
 */

struct nfs4_clone_state_refs {
    void                           *src_state;
    uint8_t                         src_type;
    void                           *dst_state;
    uint8_t                         dst_type;
    /* Handles CLONE opens itself for a special or delegation stateid, which
     * names no open (or no open handle); released with the refs. */
    struct chimera_vfs_open_handle *src_own;
    struct chimera_vfs_open_handle *dst_own;
    uint8_t                         src_special;
    uint8_t                         dst_special;
    uint8_t                         src_deleg;
    uint8_t                         dst_deleg;
    struct nfs_request             *req;
};

static void
chimera_nfs4_clone_finish(
    struct nfs_request *req,
    nfsstat4            status)
{
    struct CLONE4res             *res   = &req->res_compound.resarray[req->index].opclone;
    struct nfs4_clone_state_refs *refs  = req->nfs_state_ref;
    struct nfs_state_table       *table = &req->thread->shared->nfs4_state_table;

    req->nfs_state_ref = NULL;

    if (refs) {
        if (refs->src_state) {
            nfs_state_table_release(table, refs->src_state, refs->src_type,
                                    req->thread->vfs_thread);
        }
        if (refs->dst_state) {
            nfs_state_table_release(table, refs->dst_state, refs->dst_type,
                                    req->thread->vfs_thread);
        }
        if (refs->src_own) {
            chimera_vfs_release(req->thread->vfs_thread, refs->src_own);
        }
        if (refs->dst_own) {
            chimera_vfs_release(req->thread->vfs_thread, refs->dst_own);
        }
        free(refs);
    }

    res->cl_status = status;
    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_clone_finish */

static void
chimera_nfs4_clone_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;

    chimera_nfs4_clone_finish(req, error_code == CHIMERA_VFS_OK ?
                              NFS4_OK : chimera_nfs4_errno_to_nfsstat4(error_code));
} /* chimera_nfs4_clone_complete */

/* Select the handle to use for an endpoint: the on-the-fly handle CLONE
 * opened itself for a delegation stateid, or the handle retained by an
 * open/lock state.  NULL if neither applies (e.g. a layout stateid, which
 * authorizes no I/O here). */
static inline struct chimera_vfs_open_handle *
chimera_nfs4_clone_handle(
    struct nfs4_clone_state_refs *refs,
    int                           is_src)
{
    struct chimera_vfs_open_handle *own    = is_src ? refs->src_own : refs->dst_own;
    void                           *state  = is_src ? refs->src_state : refs->dst_state;
    uint8_t                         type   = is_src ? refs->src_type : refs->dst_type;
    uint32_t                        access = is_src ? OPEN4_SHARE_ACCESS_READ : OPEN4_SHARE_ACCESS_WRITE;

    if (own) {
        return own;
    }

    return nfs_state_io_handle(state, type, access);
} /* chimera_nfs4_clone_handle */

static void
chimera_nfs4_clone_issue(
    struct nfs_request *req,
    uint64_t            length)
{
    struct CLONE4args              *args = &req->args_compound->argarray[req->index].opclone;
    struct nfs4_clone_state_refs   *refs = req->nfs_state_ref;
    struct chimera_vfs_open_handle *src_handle;
    struct chimera_vfs_open_handle *dst_handle;

    src_handle = chimera_nfs4_clone_handle(refs, 1);
    dst_handle = chimera_nfs4_clone_handle(refs, 0);

    if (!src_handle || !dst_handle) {
        chimera_nfs4_clone_finish(req, NFS4ERR_BAD_STATEID);
        return;
    }

    /* Reflink requires both files be served by the same module and that module
     * support clone_range; otherwise it is not a supported operation. */
    if (src_handle->vfs_module != dst_handle->vfs_module ||
        !(dst_handle->vfs_module->capabilities & CHIMERA_VFS_CAP_CLONE_RANGE)) {
        chimera_nfs4_clone_finish(req, NFS4ERR_NOTSUPP);
        return;
    }

    chimera_vfs_clone_range(req->thread->vfs_thread, &req->cred,
                            src_handle, args->cl_src_offset,
                            dst_handle, args->cl_dst_offset,
                            length,
                            0, 0,
                            chimera_nfs4_clone_complete,
                            req);
} /* chimera_nfs4_clone_issue */

static void
chimera_nfs4_clone_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req  = private_data;
    struct CLONE4args  *args = &req->args_compound->argarray[req->index].opclone;
    uint64_t            src_size;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_clone_finish(req, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    src_size = (attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) ? attr->va_size : 0;

    /* cl_count == 0: clone from cl_src_offset to the end of the source.
     * RFC 7862 15.13.3 derives CLONE's range rules from COPY's (15.2.3),
     * which requires NFS4ERR_INVAL when the source offset exceeds the
     * source's size -- with no exception for cl_count == 0. An offset
     * exactly at EOF is not "beyond" it, so that case is a zero-length
     * no-op success rather than an error. */
    if (args->cl_src_offset > src_size) {
        chimera_nfs4_clone_finish(req, NFS4ERR_INVAL);
        return;
    }

    if (src_size == args->cl_src_offset) {
        chimera_nfs4_clone_finish(req, NFS4_OK);
        return;
    }

    chimera_nfs4_clone_issue(req, src_size - args->cl_src_offset);
} /* chimera_nfs4_clone_getattr_complete */

/* Start the clone once both endpoints have a usable handle (either an
 * open/lock state's retained handle, or one CLONE opened on the fly for a
 * delegation stateid). */
static void
chimera_nfs4_clone_begin(struct nfs_request *req)
{
    struct CLONE4args              *args = &req->args_compound->argarray[req->index].opclone;
    struct nfs4_clone_state_refs   *refs = req->nfs_state_ref;
    struct chimera_vfs_open_handle *src_handle;

    if (args->cl_count != 0) {
        chimera_nfs4_clone_issue(req, args->cl_count);
        return;
    }

    /* Resolve the source size to bound a clone-to-EOF. */
    src_handle = chimera_nfs4_clone_handle(refs, 1);
    if (!src_handle) {
        chimera_nfs4_clone_finish(req, NFS4ERR_BAD_STATEID);
        return;
    }

    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred,
                        src_handle,
                        CHIMERA_VFS_ATTR_SIZE,
                        chimera_nfs4_clone_getattr_complete,
                        req);
} /* chimera_nfs4_clone_begin */

static void
chimera_nfs4_clone_dst_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request           *req  = private_data;
    struct nfs4_clone_state_refs *refs = req->nfs_state_ref;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_clone_finish(req, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    refs->dst_own = handle;
    chimera_nfs4_clone_begin(req);
} /* chimera_nfs4_clone_dst_open_complete */

static void
chimera_nfs4_clone_open_dst(struct nfs_request *req)
{
    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs4_clone_dst_open_complete,
                        req);
} /* chimera_nfs4_clone_open_dst */

static void
chimera_nfs4_clone_src_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request           *req  = private_data;
    struct nfs4_clone_state_refs *refs = req->nfs_state_ref;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_clone_finish(req, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    refs->src_own = handle;

    if (refs->dst_special || refs->dst_deleg) {
        chimera_nfs4_clone_open_dst(req);
    } else {
        chimera_nfs4_clone_begin(req);
    }
} /* chimera_nfs4_clone_src_open_complete */

static void
chimera_nfs4_clone_fail(
    struct nfs_request           *req,
    struct nfs4_clone_state_refs *refs,
    struct nfs_state_table       *table,
    nfsstat4                      status)
{
    struct CLONE4res *res = &req->res_compound.resarray[req->index].opclone;

    if (refs->src_state) {
        nfs_state_table_release(table, refs->src_state, refs->src_type,
                                req->thread->vfs_thread);
    }
    if (refs->dst_state) {
        nfs_state_table_release(table, refs->dst_state, refs->dst_type,
                                req->thread->vfs_thread);
    }
    free(refs);
    res->cl_status = status;
    chimera_nfs4_compound_complete(req, res->cl_status);
} /* chimera_nfs4_clone_fail */

void
chimera_nfs4_clone(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct CLONE4args            *args  = &argop->opclone;
    struct CLONE4res             *res   = &resop->opclone;
    struct nfs_state_table       *table = &thread->shared->nfs4_state_table;
    struct nfs4_clone_state_refs *refs;
    nfsstat4                      status;

    req->nfs_state_ref = NULL;

    if (req->saved_fhlen == 0 || req->fhlen == 0) {
        res->cl_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->cl_status);
        return;
    }

    /* RFC 7862 15.13.3: a clone of a file onto itself whose source and
     * target ranges overlap is NFS4ERR_INVAL.  cl_count == 0 reaches to the
     * source's EOF, which is never known to be disjoint from a destination
     * range starting anywhere but strictly past where the source range could
     * end, so treat it conservatively as "may overlap" rather than deferring
     * to a later size lookup. */
    if (req->saved_fhlen == req->fhlen &&
        memcmp(req->saved_fh, req->fh, req->fhlen) == 0) {
        bool overlap;

        if (args->cl_count == 0) {
            overlap = true;
        } else {
            uint64_t src_end = args->cl_src_offset + args->cl_count;
            uint64_t dst_end = args->cl_dst_offset + args->cl_count;
            overlap = args->cl_src_offset < dst_end && args->cl_dst_offset < src_end;
        }

        if (overlap) {
            res->cl_status = NFS4ERR_INVAL;
            chimera_nfs4_compound_complete(req, res->cl_status);
            return;
        }
    }

    chimera_nfs4_resolve_current_stateid(req, &args->cl_src_stateid);
    chimera_nfs4_resolve_current_stateid(req, &args->cl_dst_stateid);

    refs = calloc(1, sizeof(*refs));
    chimera_nfs_abort_if(refs == NULL, "clone state refs OOM");
    refs->req         = req;
    refs->src_special = nfs4_stateid_is_special(&args->cl_src_stateid);
    refs->dst_special = nfs4_stateid_is_special(&args->cl_dst_stateid);

    /* RFC 7862 15.13.3 requires cl_src_stateid to be READ-valid and
    * cl_dst_stateid WRITE-valid under the general stateid rules (RFC 5661
    * 8.2.5 / 18.22.3 / 18.32.3), which make the special stateids valid for
    * either.  A special stateid names no state-table entry, so it is judged
    * by the share-reservation deny modes any other client holds, exactly as
    * READ/WRITE/COPY/ALLOCATE/DEALLOCATE judge it, and the source is opened
    * on the fly below instead of resolving a state-table handle. */
    if (!refs->src_special) {
        status = nfs_state_table_acquire(table, &args->cl_src_stateid, 0,
                                         &refs->src_state, &refs->src_type);
        status = nfs4_stateid_acquire_status(status, req->minorversion);
        if (status != NFS4_OK) {
            free(refs);
            res->cl_status = status;
            chimera_nfs4_compound_complete(req, res->cl_status);
            return;
        }

        status = nfs_state_check_client(
            refs->src_state, refs->src_type,
            req->session ? req->session->client_unified : NULL);
        if (status != NFS4_OK) {
            chimera_nfs4_clone_fail(req, refs, table, status);
            return;
        }

        /* A delegation stateid authorizes the clone's source read but
         * carries no open handle (only open/lock states do); judge it as
         * READ judges it, then open the saved FH on the fly.  Any other
         * state type (OPEN/LOCK) must name a read-capable open of the saved
         * filehandle; a LAYOUT or otherwise unrecognized type is rejected as
         * NFS4ERR_BAD_STATEID by nfs_state_check_access_for_fh. */
        if (refs->src_type == NFS4_SLOT_TYPE_DELEG) {
            status = nfs_delegation_check_io_for_fh(refs->src_state,
                                                    req->saved_fh,
                                                    req->saved_fhlen,
                                                    OPEN4_SHARE_ACCESS_READ);
            refs->src_deleg = 1;
        } else {
            status = nfs_state_check_read_for_fh(refs->src_state,
                                                 refs->src_type,
                                                 req->saved_fh,
                                                 req->saved_fhlen);
        }
        if (status != NFS4_OK) {
            chimera_nfs4_clone_fail(req, refs, table, status);
            return;
        }
    } else {
        status = nfs4_clients_check_io_denied(
            &thread->shared->nfs4_shared_clients,
            req->saved_fh, req->saved_fhlen, OPEN4_SHARE_ACCESS_READ);
        if (status != NFS4_OK) {
            free(refs);
            res->cl_status = status;
            chimera_nfs4_compound_complete(req, res->cl_status);
            return;
        }
    }

    if (!refs->dst_special) {
        status = nfs_state_table_acquire(table, &args->cl_dst_stateid, 0,
                                         &refs->dst_state, &refs->dst_type);
        status = nfs4_stateid_acquire_status(status, req->minorversion);
        if (status != NFS4_OK) {
            chimera_nfs4_clone_fail(req, refs, table, status);
            return;
        }

        status = nfs_state_check_client(
            refs->dst_state, refs->dst_type,
            req->session ? req->session->client_unified : NULL);
        if (status != NFS4_OK) {
            chimera_nfs4_clone_fail(req, refs, table, status);
            return;
        }

        /* As the source above: a delegation stateid for the destination
         * carries no open handle, and a write delegation of the current
         * filehandle is what authorizes it (RFC 8881 9.1.2's write-mode
         * rule, judged as COPY's destination is).  An OPEN/LOCK stateid must
         * name a write-capable open of the current filehandle. */
        if (refs->dst_type == NFS4_SLOT_TYPE_DELEG) {
            status = nfs_delegation_check_io_for_fh(refs->dst_state,
                                                    req->fh, req->fhlen,
                                                    OPEN4_SHARE_ACCESS_WRITE);
            refs->dst_deleg = 1;
        } else {
            status = nfs_state_check_write_for_fh(refs->dst_state,
                                                  refs->dst_type,
                                                  req->fh, req->fhlen);
        }
        if (status != NFS4_OK) {
            chimera_nfs4_clone_fail(req, refs, table, status);
            return;
        }
    } else {
        status = nfs4_clients_check_io_denied(
            &thread->shared->nfs4_shared_clients,
            req->fh, req->fhlen, OPEN4_SHARE_ACCESS_WRITE);
        if (status != NFS4_OK) {
            chimera_nfs4_clone_fail(req, refs, table, status);
            return;
        }
    }

    req->nfs_state_ref = refs;

    if (refs->src_special || refs->src_deleg) {
        chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                            req->saved_fh,
                            req->saved_fhlen,
                            CHIMERA_VFS_OPEN_INFERRED |
                            CHIMERA_VFS_OPEN_READ_ONLY,
                            chimera_nfs4_clone_src_open_complete,
                            req);
    } else if (refs->dst_special || refs->dst_deleg) {
        chimera_nfs4_clone_open_dst(req);
    } else {
        chimera_nfs4_clone_begin(req);
    }
} /* chimera_nfs4_clone */
