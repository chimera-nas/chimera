// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "server/server.h"
#include "nfs4_status.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "evpl/evpl.h"
#include "common/evpl_iovec_cursor.h"
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */

/*
 * READ_PLUS (RFC 7862 15.10) is a sparse-aware READ: the server may report the
 * file as a sequence of DATA and HOLE segments so a client reading a sparse
 * file transfers no bytes for the holes.  Chimera projects the work into the
 * backend (chimera_vfs_read_plus), which classifies the leading byte-run at the
 * requested offset as a single DATA or HOLE segment.  Returning one segment per
 * call is RFC-legal (rpr_contents is an array; the client re-issues from the
 * last byte returned).  Backends without CAP_READ_PLUS surface NFS4ERR_NOTSUPP,
 * and the client falls back to plain READ.
 */

/* How many iovecs one data run may arrive in; the plain READ reserves the
 * same number. */
#define NFS4_READ_PLUS_MAX_IOV 256


/* Release the acquired state ref or on-the-fly handle, then complete. */
static void
chimera_nfs4_read_plus_finish(
    struct nfs_request *req,
    nfsstat4            status)
{
    struct READ_PLUS4res *res = &req->res_compound.resarray[req->index].opread_plus;

    res->rp_status = status;

    if (req->nfs_state_ref) {
        nfs_state_table_release(&req->thread->shared->nfs4_state_table,
                                req->nfs_state_ref, req->nfs_state_type,
                                req->thread->vfs_thread);
        req->nfs_state_ref = NULL;
        req->handle        = NULL; /* borrowed from the state; not ours to free */
    } else if (req->handle) {
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
    }

    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_read_plus_finish */

/*
 * READ_PLUS is TWO ops: the classification, and -- only when it says DATA -- the
 * read of the bytes it described.  The gate is what makes that "only": it reads
 * the classification as it finishes, skips the READ behind it on a hole or an
 * empty run, and otherwise writes the run's length into the READ's count.  That
 * is an ordinary gate edit under the ordinary rule -- assigned from the
 * caller's own offset plus what the finished op reported, never accumulated --
 * so a second execution computes the same read.
 *
 * The run's shape depends on how the object was reached: a stateid carries its
 * own handle and the run is the two ops alone; a stateid that carries none
 * opens the current object, behind the type gate a data open of a FIFO makes
 * necessary.  The indices are therefore carried rather than fixed.
 */
struct nfs4_read_plus_ctx {
    struct nfs_request *req;
    int                 type_op;
    int                 plus_op;
    int                 read_op;
};

static void
chimera_nfs4_read_plus_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_read_plus_ctx            *ctx  = private_data;
    struct nfs_request                   *req  = ctx->req;
    struct READ_PLUS4args                *args = &req->args_compound->argarray[req->index].opread_plus;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_compound_op       *edit;

    if (*status != CHIMERA_VFS_OK) {
        return;
    }

    if ((int) index == ctx->type_op) {
        op = chimera_vfs_compound_op(compound, index);

        if ((op->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
            !S_ISREG(op->attr.va_mode)) {
            *status = CHIMERA_VFS_EINVAL;
        }

        return;
    }

    if ((int) index != ctx->plus_op) {
        return;
    }

    op   = chimera_vfs_compound_op(compound, index);
    edit = chimera_vfs_compound_op_edit(compound, (uint32_t) ctx->read_op);

    if (!edit) {
        return;
    }

    if (op->is_data && op->read_len > 0) {
        edit->offset = args->rpa_offset;
        edit->count  = op->read_len;
    } else {
        /* A hole, or nothing left to describe: there are no bytes to fetch.
         * The gate's skip -- not the caller's, which a gate cannot clear -- so
         * the decision is remade on every execution. */
        edit->skip = 1;
    }
} /* chimera_nfs4_read_plus_gate */

static void
chimera_nfs4_read_plus_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs4_read_plus_ctx            *ctx  = private_data;
    struct nfs_request                   *req  = ctx->req;
    struct READ_PLUS4args                *args = &req->args_compound->argarray[req->index].opread_plus;
    struct READ_PLUS4res                 *res  = &req->res_compound.resarray[req->index].opread_plus;
    const struct chimera_vfs_compound_op *pop, *rop, *top;
    struct read_plus_content             *content;
    struct evpl_iovec                    *iov  = NULL;
    int                                   niov = 0;
    enum chimera_vfs_error                error_code;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code != CHIMERA_VFS_OK) {
        nfsstat4 status;

        if (ctx->type_op >= 0 &&
            (top = chimera_vfs_compound_op(compound, (uint32_t) ctx->type_op)) &&
            top->status != CHIMERA_VFS_OK &&
            (top->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
            !S_ISREG(top->attr.va_mode)) {
            status = S_ISDIR(top->attr.va_mode) ? NFS4ERR_ISDIR : NFS4ERR_INVAL;
        } else {
            status = chimera_nfs4_errno_to_nfsstat4(error_code);
        }

        chimera_vfs_compound_free(compound);
        chimera_nfs4_read_plus_finish(req, status);
        return;
    }

    pop = chimera_vfs_compound_op(compound, (uint32_t) ctx->plus_op);
    rop = chimera_vfs_compound_op(compound, (uint32_t) ctx->read_op);

    res->rp_resok4.num_rpr_contents = 0;
    res->rp_resok4.rpr_contents     = NULL;

    if (rop->status == CHIMERA_VFS_OK) {
        /* The READ ran, so the segment is DATA and its bytes are the run's.
         * eof is the READ's, as the per-op path took it. */
        res->rp_resok4.rpr_eof = rop->eof_read;

        if (rop->read_len) {
            content = xdr_dbuf_alloc_space(sizeof(*content),
                                           req->encoding->dbuf);
            chimera_nfs_abort_if(content == NULL, "Failed to allocate space");

            content->rpc_content       = NFS4_CONTENT_DATA;
            content->rpc_data.d_offset = args->rpa_offset;

            /* data4.d_data is a plain (copying) opaque, so flatten the read
             * iovecs into a contiguous dbuf-backed buffer for the
             * marshaller. */
            chimera_nfs_abort_if(
                xdr_dbuf_alloc_opaque(&content->rpc_data.d_data, rop->read_len,
                                      req->encoding->dbuf) != 0,
                "Failed to allocate space");
            content->rpc_data.d_data.len = rop->read_len;

            chimera_vfs_compound_take_iov(compound, (uint32_t) ctx->read_op,
                                          &iov, &niov);

            struct evpl_iovec_cursor cursor;
            evpl_iovec_cursor_init(&cursor, iov, niov);
            evpl_iovec_cursor_copy(&cursor, content->rpc_data.d_data.data,
                                   rop->read_len);

            evpl_iovecs_release(req->thread->evpl, iov, niov);

            res->rp_resok4.num_rpr_contents = 1;
            res->rp_resok4.rpr_contents     = content;
        }
    } else {
        res->rp_resok4.rpr_eof = pop->eof_read;

        if (!pop->is_data && pop->read_len > 0) {
            content = xdr_dbuf_alloc_space(sizeof(*content),
                                           req->encoding->dbuf);
            chimera_nfs_abort_if(content == NULL, "Failed to allocate space");

            content->rpc_content        = NFS4_CONTENT_HOLE;
            content->rpc_hole.di_offset = args->rpa_offset;
            content->rpc_hole.di_length = pop->read_len;

            res->rp_resok4.num_rpr_contents = 1;
            res->rp_resok4.rpr_contents     = content;
        }
        /* Otherwise at/past EOF or an empty range: no content segments. */
    }

    chimera_vfs_compound_free(compound);

    chimera_nfs4_read_plus_finish(req, NFS4_OK);
} /* chimera_nfs4_read_plus_complete */

/*
 * `handle` is the stateid's, lent for the run; NULL means the object is the
 * current file handle and the run opens it -- behind the type gate, because a
 * data open of a FIFO blocks and RFC 7862 owes ISDIR/INVAL for a non-regular
 * target.
 */
static void
chimera_nfs4_read_plus_issue(
    struct nfs_request             *req,
    struct chimera_vfs_open_handle *handle)
{
    struct READ_PLUS4args       *args = &req->args_compound->argarray[req->index].opread_plus;
    struct nfs4_read_plus_ctx   *ctx;
    struct chimera_vfs_compound *compound;
    struct evpl_iovec           *iov;

    ctx = xdr_dbuf_alloc_space(sizeof(*ctx), req->encoding->dbuf);
    chimera_nfs_abort_if(ctx == NULL, "Failed to allocate space");

    iov = xdr_dbuf_alloc_space(sizeof(*iov) * NFS4_READ_PLUS_MAX_IOV,
                               req->encoding->dbuf);
    chimera_nfs_abort_if(iov == NULL, "Failed to allocate space");

    ctx->req     = req;
    ctx->type_op = -1;

    /* Borrowed from the state, which releases it; the finish knows not to. */
    req->handle = handle;

    compound = chimera_vfs_compound_alloc(req->thread->vfs_thread, &req->cred);

    if (!handle) {
        chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
        chimera_vfs_compound_add_open_current(compound,
                                              CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_PATH |
                                              CHIMERA_VFS_OPEN_NOFOLLOW, 0);
        ctx->type_op = chimera_vfs_compound_add_getattr(
            compound, CHIMERA_VFS_ATTR_MODE);
        chimera_vfs_compound_add_open_current(compound,
                                              CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_READ_ONLY, 0);
    }

    ctx->plus_op = chimera_vfs_compound_add_read_plus(compound, handle,
                                                      args->rpa_offset,
                                                      args->rpa_count);
    ctx->read_op = chimera_vfs_compound_add_read(compound, handle,
                                                 args->rpa_offset,
                                                 args->rpa_count,
                                                 iov, NFS4_READ_PLUS_MAX_IOV,
                                                 0, NULL, NULL, 0);

    chimera_vfs_compound_set_gate(compound, chimera_nfs4_read_plus_gate, ctx);

    chimera_vfs_compound_submit(compound, chimera_nfs4_read_plus_complete, ctx);
} /* chimera_nfs4_read_plus_issue */

void
chimera_nfs4_read_plus(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct READ_PLUS4args          *args  = &argop->opread_plus;
    struct READ_PLUS4res           *res   = &resop->opread_plus;
    struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
    void                           *state_void;
    uint8_t                         state_type;
    struct chimera_vfs_open_handle *state_handle;
    struct nfs_open_state          *open_state;
    nfsstat4                        status;

    req->nfs_state_ref = NULL;
    req->handle        = NULL;

    /* NFS4.1 current-stateid substitution (RFC 8881 16.2.3.1.2). */
    chimera_nfs4_resolve_current_stateid(req, &args->rpa_stateid);

    /* Special/anonymous stateids (and a pNFS data server) carry no state-table
     * entry: open the current FH on the fly, mirroring READ. */
    if (nfs4_stateid_is_special(&args->rpa_stateid) ||
        chimera_server_config_get_nfs_data_server(thread->shared->config)) {
        if (req->fhlen == 0) {
            res->rp_status = NFS4ERR_NOFILEHANDLE;
            chimera_nfs4_compound_complete(req, res->rp_status);
            return;
        }

        /* RFC 7530 9.1.4.3 / RFC 8881 9.7: a special-stateid read against a
        * deny-READ share reservation held by ANY owner of ANY client is
        * NFS4ERR_LOCKED.  READ_PLUS is a READ, so it owes the same answer --
        * check the shared client table, not just this client's own opens,
        * and do it whether or not the request carries a session (4.0 has
        * none).  A pNFS data server is authorized by the layout instead. */
        if (!chimera_server_config_get_nfs_data_server(
                thread->shared->config)) {
            status = nfs4_clients_check_io_denied(
                &thread->shared->nfs4_shared_clients,
                req->fh,
                req->fhlen,
                OPEN4_SHARE_ACCESS_READ);
            if (status != NFS4_OK) {
                res->rp_status = status;
                chimera_nfs4_compound_complete(req, res->rp_status);
                return;
            }
        }

        chimera_nfs4_read_plus_issue(req, NULL);
        return;
    }

    status = nfs_state_table_acquire(table, &args->rpa_stateid, 0,
                                     &state_void, &state_type);
    if (status != NFS4_OK) {
        res->rp_status = status;
        chimera_nfs4_compound_complete(req, res->rp_status);
        return;
    }

    status = nfs_state_check_client(
        state_void, state_type,
        req->session ? req->session->client_unified : NULL);
    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->rp_status = status;
        chimera_nfs4_compound_complete(req, res->rp_status);
        return;
    }

    /* A delegation stateid authorizes the READ but carries no open handle;
     * drop the ref and open the current FH on the fly, as for READ. */
    if (state_type == NFS4_SLOT_TYPE_DELEG) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        if (req->fhlen == 0) {
            res->rp_status = NFS4ERR_NOFILEHANDLE;
            chimera_nfs4_compound_complete(req, res->rp_status);
            return;
        }
        chimera_nfs4_read_plus_issue(req, NULL);
        return;
    }

    if (state_type == NFS4_SLOT_TYPE_OPEN) {
        open_state = state_void;
    } else {
        open_state = ((struct nfs_lock_state *) state_void)->open_state;
    }
    state_handle = nfs_state_io_handle(state_void, state_type, OPEN4_SHARE_ACCESS_READ);

    status = nfs_open_state_check_io_denied(open_state,
                                            OPEN4_SHARE_ACCESS_READ);
    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->rp_status = status;
        chimera_nfs4_compound_complete(req, res->rp_status);
        return;
    }

    if (!nfs_open_state_check_principal(open_state,
                                        req->principal_flavor,
                                        req->principal_machinename,
                                        req->principal_machinename_len)) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->rp_status = NFS4ERR_ACCESS;
        chimera_nfs4_compound_complete(req, res->rp_status);
        return;
    }

    req->nfs_state_ref  = state_void;
    req->nfs_state_type = state_type;

    chimera_nfs4_read_plus_issue(req, state_handle);
} /* chimera_nfs4_read_plus */
