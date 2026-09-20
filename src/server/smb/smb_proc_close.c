// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_sharemode.h"
#include "smb_notify.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

/* The postquery GETATTR's place in the close run, which is built only when the
 * client asked for it -- so the gate is registered only then too, and this
 * index never names the CLOSE. */
#define CHIMERA_SMB_CLOSE_OP_GETATTR 1

void
chimera_smb_break_caching_for_namespace(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file)
{
    struct chimera_vfs_thread      *vfs_thread =
        request->compound->thread->vfs_thread;
    struct chimera_vfs_state       *vfs_state = vfs_thread->vfs->vfs_state;
    struct chimera_vfs_open_handle *oh        = open_file->handle;
    struct chimera_claim_actor      actor     = {
        .owner          = {
            .proto      = CHIMERA_CLAIM_PROTO_SMB2,
            .client_key = request->session_handle->session->client_key,
            .owner_lo   = open_file->file_id.pid,
            .owner_hi   = open_file->file_id.vid,
        },
        .op_handle      = open_file->handle,
    };

    if (!oh) {
        return;
    }

    /* An RqLs open is owned by its lease key, not its file_id: identify the
    * operating open by the key so its OWN lease is spared by the recall. */
    if (open_file->oplock_level == SMB2_OPLOCK_LEVEL_LEASE) {
        memcpy(actor.owner.key, open_file->lease_key, 16);
        memcpy(&actor.owner.owner_lo, open_file->lease_key, 8);
        memcpy(&actor.owner.owner_hi, open_file->lease_key + 8, 8);
    }

    chimera_vfs_claim_invalidate(
        vfs_state, oh->fh, oh->fh_len, oh->fh_hash,
        CHIMERA_TRIGGER_NS_UNLINK, &actor,
        CHIMERA_CLAIM_CR /* strip beyond-R holders to a read cache */);
} /* chimera_smb_break_caching_for_namespace */

/* Fire-and-forget completion for the persistent-handle record delete. */
static void
chimera_smb_close_durable_delete_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    (void) error_code;
    (void) private_data;
} /* chimera_smb_close_durable_delete_callback */

/* Map a delete-on-close remove_at failure to the SMB CLOSE response status.
 * MS-FSA close semantics: the close itself succeeds, but the delete failure
 * (ENOTEMPTY on a non-empty directory most commonly, also EACCES) must be
 * reported to the client so it knows the object survived.  Silently turning
 * every failure into SUCCESS makes smb2_deltree believe its recursive teardown
 * worked, leaving a stale BASEDIR / leftover entries that wreck the next
 * subtest's setup. */
static inline uint32_t
chimera_smb_close_doc_status(enum chimera_vfs_error error_code)
{
    switch (error_code) {
        case CHIMERA_VFS_OK:        return SMB2_STATUS_SUCCESS;
        case CHIMERA_VFS_ENOTEMPTY: return SMB2_STATUS_DIRECTORY_NOT_EMPTY;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:     return SMB2_STATUS_ACCESS_DENIED;
        case CHIMERA_VFS_ENOENT:
        /* A handle whose object is gone reports ESTALE now that the backends
         * distinguish it from a name that was never there; SMB has one status
         * for both. */
        case CHIMERA_VFS_ESTALE:    return SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
        default:                    return SMB2_STATUS_INTERNAL_ERROR;
    } /* switch */
} /* chimera_smb_close_doc_status */

/*
 * Request-less delete-on-close unlink for the abrupt-teardown path
 * (connection disconnect / session logoff / tree disconnect).
 *
 * The clean-CLOSE DOC path above threads parent_handle, doc_info and the
 * VFS-module close state through the live chimera_smb_request.  Teardown frees
 * open files with no request in hand, so this self-contained context carries a
 * heap-owned copy of the doc_info returned by chimera_vfs_release_doc and runs
 * the same open-parent -> remove_at -> close-backend sequence as a
 * fire-and-forget operation on the SMB thread's VFS thread.
 *
 * Only invoked for opens teardown is genuinely DISCARDING: a preserved
 * (courtesy-held / parked) durable handle never reaches release_doc in the
 * teardown path, so its file is never deleted on disconnect.
 */
struct chimera_smb_teardown_doc_ctx {
    struct chimera_server_smb_thread *thread;
    struct chimera_vfs_doc_info       doc_info;
};

static void
chimera_smb_teardown_doc_close_backend(struct chimera_smb_teardown_doc_ctx *ctx)
{
    chimera_vfs_close_ref_dispatch(ctx->thread->vfs_thread,
                                   &ctx->doc_info.close_ref, NULL, NULL);
    free(ctx);
} /* chimera_smb_teardown_doc_close_backend */

/* PUTFH(parent), REMOVE(name). */
static void
chimera_smb_teardown_doc_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_teardown_doc_ctx *ctx = private_data;
    enum chimera_vfs_error               status;

    status = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);

    if (status != CHIMERA_VFS_OK) {
        chimera_smb_debug("teardown delete-on-close: remove failed for "
                          "'%.*s' (error %d)",
                          ctx->doc_info.name_len,
                          ctx->doc_info.name,
                          status);
    }

    chimera_smb_teardown_doc_close_backend(ctx);
} /* chimera_smb_teardown_doc_sequence_complete */

void
chimera_smb_teardown_doc_unlink(
    struct chimera_server_smb_thread  *thread,
    const struct chimera_vfs_doc_info *doc_info)
{
    struct chimera_smb_teardown_doc_ctx *ctx;
    struct chimera_vfs_compound         *compound;

    if (doc_info->parent_fh_len == 0) {
        /* No parent fh recorded — cannot unlink, just close the backend
         * handle that release_doc detached from the cache. */
        chimera_vfs_close_ref_dispatch(thread->vfs_thread, &doc_info->close_ref,
                                       NULL, NULL);
        return;
    }

    ctx           = calloc(1, sizeof(*ctx));
    ctx->thread   = thread;
    ctx->doc_info = *doc_info;

    /* Under the credential the flag was armed with, which the ctx owns for the
     * life of the run -- the compound borrows it, and there is no request here
     * to hang it off. */
    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &ctx->doc_info.cred);

    chimera_vfs_compound_add_putfh(compound, ctx->doc_info.parent_fh,
                                   ctx->doc_info.parent_fh_len);

    /* No lease self-exemption here: teardown is dropping every handle this
     * connection/session/tree owned, so all directory leases break.  The child
     * FH is intentionally not supplied (no cross-client file-lease recall): the
     * delete-on-close lease-recall semantics are owned by the parallel
     * smb-lease-crossclient-rh work, and the clean-CLOSE DOC path likewise does
     * not recall here. */
    chimera_vfs_compound_add_remove(compound, ctx->doc_info.name,
                                    ctx->doc_info.name_len, 0, 0, 0);

    chimera_vfs_compound_submit(compound,
                                chimera_smb_teardown_doc_sequence_complete,
                                ctx);
} /* chimera_smb_teardown_doc_unlink */

/* A named stream flagged delete-on-close removes only the stream, never the
 * base file -- so its close is the bare release (no CHIMERA_VFS_COMPOUND_CLOSE_
 * DOC) and the fork is removed by the consecutive run below.  The VFS doc
 * mechanism may nonetheless be ARMED on such a handle, because a stream CREATE
 * with FILE_DELETE_ON_CLOSE arms it with the BASE file's name; honouring it
 * here would unlink the base. */
static inline int
chimera_smb_close_is_stream_delete(const struct chimera_smb_open_file *open_file)
{
    return (open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM) &&
           (open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE) &&
           open_file->base_fh_len > 0;
} /* chimera_smb_close_is_stream_delete */

/*
 * The stream tails, each its own sequence.
 *
 * A CLOSE(CLOSE_DOC) empties the current open; the object these two go on to
 * act on -- the base file, or the base's parent -- is not on that run's
 * cursors, so neither can be an op behind the close.  They are consecutive
 * sequences within the one request, which is what the compound contract says
 * this shape is.  Both answer SUCCESS whatever the removal did: the close
 * itself succeeded, and the client is not waiting on a fork's disposal.
 */
static void
chimera_smb_close_stream_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request *request = private_data;
    enum chimera_vfs_error      status;

    status = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (status != CHIMERA_VFS_OK) {
        chimera_smb_debug("stream delete-on-close: remove failed (error %d)",
                          status);
    }

    chimera_smb_open_file_release(request, request->close.open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_close_stream_sequence_complete */

/* PUTFH(base), REMOVE_STREAM: a named stream flagged delete-on-close removes
 * only the stream, never the base file, and never armed the VFS doc
 * mechanism. */
static void
chimera_smb_close_remove_stream(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open_file = request->close.open_file;

    request->vfs_compound = chimera_vfs_compound_alloc(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred);

    chimera_vfs_compound_add_putfh(request->vfs_compound,
                                   open_file->base_fh,
                                   open_file->base_fh_len);

    chimera_vfs_compound_add_remove_stream(request->vfs_compound,
                                           open_file->stream_name,
                                           open_file->stream_name_len);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_close_stream_sequence_complete,
                                request);
} /* chimera_smb_close_remove_stream */

/* PUTFH(parent), REMOVE(base name, matching the base's fh): the deferred base
 * removal owed when the last named stream keeping a delete-pending base file
 * alive is closed (smb2.streams.delete).  Matched on the fh for the reason
 * every late unlink is: the name may belong to something else by now. */
static void
chimera_smb_close_remove_deferred_base(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open_file = request->close.open_file;
    int                           index;

    request->vfs_compound = chimera_vfs_compound_alloc(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred);

    chimera_vfs_compound_add_putfh(request->vfs_compound,
                                   open_file->parent_fh,
                                   open_file->parent_fh_len);

    index = chimera_vfs_compound_add_remove(request->vfs_compound,
                                            open_file->name,
                                            open_file->name_len,
                                            0, 0, 0);

    chimera_vfs_compound_op_set_remove_match(request->vfs_compound, index,
                                             open_file->base_fh,
                                             open_file->base_fh_len,
                                             open_file->base_fh_len > 0,
                                             NULL);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_close_stream_sequence_complete,
                                request);
} /* chimera_smb_close_remove_deferred_base */

/*
 * PUTHANDLE, [GETATTR(STAT | BTIME)], CLOSE(CLOSE_DOC).
 *
 * The postquery GETATTR must not be able to stop the run: the CLOSE behind it
 * is what ends the client's open, and a handle left open because an attribute
 * read failed is a leak the client can never undo.  So the gate answers OK for
 * it whatever it reported and records the refusal for the marshal, which is
 * what the per-op chain did by calling close_release from both arms of its
 * getattr callback.
 */
static void
chimera_smb_close_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_smb_request *request = private_data;

    (void) compound;

    if (index != CHIMERA_SMB_CLOSE_OP_GETATTR) {
        return;
    }

    request->close.attr_failed = (*status != CHIMERA_VFS_OK);
    *status                    = CHIMERA_VFS_OK;
} /* chimera_smb_close_gate */

static void
chimera_smb_close_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request   = private_data;
    struct chimera_smb_open_file         *open_file = request->close.open_file;
    const struct chimera_vfs_compound_op *op;
    uint32_t                              status = SMB2_STATUS_SUCCESS;

    if ((request->close.flags & SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB) &&
        !request->close.attr_failed) {
        op = chimera_vfs_compound_op(compound, CHIMERA_SMB_CLOSE_OP_GETATTR);
        chimera_smb_marshal_attrs(&op->attr, &request->close.r_attrs);
    } else {
        memset(&request->close.r_attrs, 0, sizeof(request->close.r_attrs));
    }

    /* The close performed the delete-on-close unlink; MS-FSA wants what it did
     * reported, not swallowed -- see chimera_smb_close_doc_status.  Nothing is
     * owed for the executor's doc_base_deferred: it already marked the base
     * delete-pending and closed the detached backend handle, and the removal is
     * the STREAM's last close below. */
    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    if (op->doc_fired) {
        status = chimera_smb_close_doc_status(op->doc_status);
    }

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    /* The sequence ended the handle whatever its provenance, so the open must
     * stop naming it (the compound contract's CLOSE rule: the caller does not
     * release it afterwards). */
    open_file->handle = NULL;

    if (chimera_smb_close_is_stream_delete(open_file)) {
        chimera_smb_close_remove_stream(request);
        return;
    }

    /* Excludes this open's own base reservation when checking for other
     * holders, so the deferred removal fires only on the final stream close. */
    if ((open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM) &&
        open_file->base_share_file_state &&
        chimera_vfs_state_is_delete_pending(open_file->base_share_file_state) &&
        !chimera_vfs_state_has_other_share_holder(open_file->base_share_file_state,
                                                  &open_file->base_share_lease) &&
        open_file->parent_fh_len > 0) {
        chimera_smb_close_remove_deferred_base(request);
        return;
    }

    chimera_smb_open_file_release(request, open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_close_sequence_complete */

void
chimera_smb_close(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;

    request->close.open_file = chimera_smb_open_file_close(request, &request->close.file_id);

    if (unlikely(!request->close.open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    /* A blocking byte-range LOCK still parked on this handle is aborted by the
     * close and completed with RANGE_NOT_LOCKED (MS-SMB2; smb2.lock.cancel
     * "cancel by close").  The open is already unhashed + marked CLOSED above, so
     * the parked request's deferred grant can no longer land; finishing it here
     * drops the open_file reference it held. */
    {
        struct chimera_smb_request *parked =
            chimera_smb_lock_abort_parked(thread, request->close.open_file);

        if (parked) {
            chimera_smb_lock_park_finish(parked, parked->lock.resume_status);
        }
    }

    /* Clean up any notify watches on this open file */
    if (request->close.open_file->notify_state) {
        chimera_smb_notify_close(thread->shared->vfs->vfs_notify,
                                 request->close.open_file->notify_state);
        request->close.open_file->notify_state = NULL;
    }

    /* Deferred directory-lease content break for a modified file: a write does
     * not break the parent dir lease at write time (the file's directory-visible
     * size/mtime settle only at close), so emit the break now.  Self-exempt the
     * directory lease named by this handle's ParentLeaseKey -- a writer that
     * holds the parent's lease keeps its cached view coherent (MS-SMB2;
     * dirlease.v2_request "only the close on the modified file break[s] the
     * directory lease", and the valid-parent-key write closes without a break). */
    if ((request->close.open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_MODIFIED) &&
        request->close.open_file->parent_fh_len > 0) {
        uint64_t skip_lo, skip_hi;
        bool     has_skip = chimera_smb_parent_lease_skip(
            request->close.open_file->parent_lease_key, &skip_lo, &skip_hi);

        chimera_vfs_notify_emit_lease(thread->shared->vfs->vfs_notify,
                                      request->close.open_file->parent_fh,
                                      request->close.open_file->parent_fh_len,
                                      CHIMERA_VFS_NOTIFY_FILE_MODIFIED,
                                      request->close.open_file->name,
                                      request->close.open_file->name_len,
                                      NULL, 0,
                                      skip_lo, skip_hi, has_skip);
    }

    /* Release share mode entry for regular file opens */
    if (request->close.open_file->type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE &&
        request->tree->share) {
        chimera_smb_sharemode_release(&request->tree->share->sharemode,
                                      request->close.open_file);
    }

    /* Persistent handle: delete its backend record so a server restart does
    * not resurrect a handle the client explicitly closed.  Best-effort and
    * fire-and-forget; routed to the share's backend via the file handle. */
    if ((request->close.open_file->flags & CHIMERA_SMB_OPEN_FILE_PERSISTED) &&
        request->close.open_file->handle) {
        uint8_t  dkey[CHIMERA_SMB_DURABLE_KEY_LEN];
        uint32_t dkey_len = chimera_smb_durable_key(dkey, request->close.open_file->file_id.pid);

        chimera_vfs_delete_key_at(thread->vfs_thread,
                                  &request->session_handle->session->cred,
                                  request->close.open_file->handle->fh,
                                  request->close.open_file->handle->fh_len,
                                  dkey, dkey_len,
                                  chimera_smb_close_durable_delete_callback, NULL);
    }

    /* Named-pipe FIDs carry handle == NULL: there is nothing to close and
     * nothing to query, so answer the zero-attrs reply rather than lending a
     * NULL handle to the sequence (MS-SMB2 3.3.5.10). */
    if (!request->close.open_file->handle) {
        memset(&request->close.r_attrs, 0, sizeof(request->close.r_attrs));
        chimera_smb_open_file_release(request, request->close.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    request->close.attr_failed = 0;

    request->vfs_compound = chimera_vfs_compound_alloc(
        thread->vfs_thread, &request->session_handle->session->cred);

    chimera_vfs_compound_add_puthandle(request->vfs_compound,
                                       request->close.open_file->handle,
                                       request->close.open_file->open_flags);

    if (request->close.flags & SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB) {
        /* The POSTQUERY response is FILE_NETWORK_OPEN_INFORMATION whose first
         * field is CreationTime (BTIME).  MASK_STAT deliberately omits BTIME, so
         * request it explicitly or CreationTime is emitted as 0 (issue #1117). */
        chimera_vfs_compound_add_getattr(request->vfs_compound,
                                         CHIMERA_VFS_ATTR_MASK_STAT |
                                         CHIMERA_VFS_ATTR_BTIME);

        chimera_vfs_compound_set_gate(request->vfs_compound,
                                      chimera_smb_close_gate, request);
    }

    /* Self-exempt a directory lease ONLY when the handle being closed here is
     * the one that carried delete-on-close: its ParentLeaseKey names the
     * directory lease whose cached view is coherent with the removal it caused,
     * so spare it (dirlease.unlink_same_*).  When the last handle to close is
     * NOT the one that set delete-on-close (a different open triggers the actual
     * removal), the set and closing parent keys differ, so no lease is spared
     * and ALL directory leases break (MS-SMB2; dirlease.unlink_different_*). */
    chimera_vfs_compound_add_close(
        request->vfs_compound,
        chimera_smb_close_is_stream_delete(request->close.open_file) ?
        0 : CHIMERA_VFS_COMPOUND_CLOSE_DOC,
        (request->close.open_file->flags &
         CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE) ?
        request->close.open_file->parent_lease_key : NULL);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_close_sequence_complete,
                                request);

} /* chimera_smb_close */

void
chimera_smb_close_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_CLOSE_REPLY_SIZE);
    /* Emit only the defined POSTQUERY_ATTRIB bit; do not reflect reserved client
     * Flags bits (MS-SMB2 3.3.5.10 / 2.2.16). */
    evpl_iovec_cursor_append_uint16(reply_cursor,
                                    request->close.flags & SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB);

    if (request->close.flags & SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB) {
        chimera_smb_append_network_open_info(reply_cursor, &request->close.r_attrs);
    } else {
        chimera_smb_append_null_network_open_info_null(reply_cursor);
    }

} /* chimera_smb_close_reply */

int
chimera_smb_parse_close(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{

    if (unlikely(request->request_struct_size != SMB2_CLOSE_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 CLOSE request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_CLOSE_REQUEST_SIZE);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    int prc = 0;
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &request->close.flags);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->close.file_id.pid);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->close.file_id.vid);

    if (unlikely(prc)) {
        chimera_smb_error("Received SMB2 CLOSE request truncated in fixed body");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

    return 0;
} /* chimera_smb_parse_close */
