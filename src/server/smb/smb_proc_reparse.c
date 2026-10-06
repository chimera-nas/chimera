// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */
#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_string.h"
#include "smb_common/smb2.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "common/compound_retry.h"

/* ------------------------------------------------------------------ */
/* SET_REPARSE_POINT guarded identity replacement                     */
/* ------------------------------------------------------------------ */

/* Replace a regular placeholder in one standalone compound. The source
 * identity is pinned and REMOVE compares its FH atomically. The accepted
 * suffix transfers handle, ACCESS owner, file state and namespace membership
 * together. Opens with additional state cannot use this migration yet and
 * must be rejected before any filesystem mutation. */
struct smb_set_reparse_ctx {
    struct chimera_smb_request                *request;
    struct chimera_vfs_open_handle            *source;
    bool                                       gated, unsupported, created;
    struct chimera_vfs_cred                    cred;
    struct chimera_smb_namespace_replace_token replacement;
    struct chimera_vfs_claim_access_fence      old_admission, new_admission;
    struct chimera_smb_doc_fence               new_doc;
    struct chimera_vfs_claim                   template;
    struct chimera_vfs_file_state             *new_file;
    int                                        remove_op, create_op, handle_op, reserve_op, ready_op;
};

/* Observe existing pins and exclude future pins under the same bucket lock.
 * A refcount check by itself is not a lifetime barrier. Teardown may still
 * cut off this open; the retained resolver reference anchors the gate token. */
static bool
smb_set_reparse_gate(struct smb_set_reparse_ctx *ctx)
{
    struct chimera_smb_open_file *open   = ctx->request->ioctl.rp_open_file;
    unsigned                      bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    evpl_mutex_lock(&open->tree->open_files_lock[bucket]);
    bool                          available = !(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED) &&
        !open->identity_rebind && open->refcnt == 2;
    if (available) {
        open->identity_rebind = ctx; ctx->gated = true;
    }
    evpl_mutex_unlock(&open->tree->open_files_lock[bucket]);
    return available;
} /* smb_set_reparse_gate */

static void
smb_set_reparse_target_release(struct smb_set_reparse_ctx *ctx)
{
    struct chimera_vfs_state *state = ctx->request->compound->thread->shared->vfs->vfs_state;

    chimera_vfs_claim_access_fence_release(&ctx->new_admission);
    chimera_smb_doc_fence_release(&ctx->new_doc);
    if (ctx->new_file) {
        chimera_vfs_state_put(state, ctx->new_file); ctx->new_file = NULL;
    }
} /* smb_set_reparse_target_release */


static void
smb_set_reparse_done(
    struct smb_set_reparse_ctx *ctx,
    uint32_t                    status)
{
    struct chimera_smb_request *request = ctx->request;
    struct chimera_vfs_thread  *thread  = request->compound->thread->vfs_thread;

    smb_set_reparse_target_release(ctx);
    chimera_vfs_claim_access_fence_release(&ctx->old_admission);
    chimera_smb_namespace_replace_end(&ctx->replacement);
    if (ctx->gated) {
        struct chimera_smb_open_file *open   = request->ioctl.rp_open_file;
        unsigned                      bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
        evpl_mutex_lock(&open->tree->open_files_lock[bucket]);
        chimera_smb_abort_if(open->identity_rebind != ctx, "lost SET_REPARSE identity gate");
        open->identity_rebind = NULL;
        evpl_mutex_unlock(&open->tree->open_files_lock[bucket]);
    }
    if (ctx->source) {
        chimera_vfs_release(thread, ctx->source);
    }
    chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
    request->ioctl.rp_open_file = NULL;
    free(ctx);
    chimera_smb_complete_request(request, status);
} /* smb_set_reparse_done */

/* Each excluded owner needs its own identity migration journal. */
static bool
smb_set_reparse_native_eligible(struct chimera_smb_open_file *open)
{
    return open->type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE && open->access_owner &&
           open->share_file_state && open->namespace_registered &&
           !open->base_namespace_participant && !open->base_access_owner &&
           !(open->flags & (CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY |
                            CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE |
                            CHIMERA_SMB_OPEN_FILE_FLAG_STREAM |
                            CHIMERA_SMB_OPEN_FILE_PERSISTED | CHIMERA_SMB_OPEN_FILE_PARKED)) &&
           !open->doc_from_create && !open->doc_posix && !open->durable_flags &&
           !open->resilient && !open->grant && !open->caching_file_state && !open->lease_key_binding &&
           !open->range_owner && !open->parked_lock_req &&
           !open->notify_state && !open->doc_close_started;
} /* smb_set_reparse_native_eligible */

static void
smb_set_reparse_peer(
    void                                        *context,
    const struct chimera_smb_namespace_snapshot *snapshot,
    void                                        *private_data)
{
    unsigned *count = private_data;

    (void) context; (void) snapshot;
    ++*count;
} /* smb_set_reparse_peer */

static void
smb_set_reparse_reset(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct smb_set_reparse_ctx *ctx = private_data;

    (void) compound;
    smb_set_reparse_target_release(ctx);
    ctx->unsupported = ctx->created = false;
    memset(&ctx->template, 0, sizeof(ctx->template));
} /* smb_set_reparse_reset */

static void
smb_set_reparse_regular(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_set_reparse_ctx     *ctx  = private_data;
    const struct chimera_vfs_attrs *attr = &chimera_vfs_compound_op(compound, index)->attr;

    if (*status == CHIMERA_VFS_OK && (!(attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) ||
                                      !S_ISREG(attr->va_mode))) {
        ctx->unsupported = true;
        *status          = CHIMERA_VFS_ENOTSUP;
    }
} /* smb_set_reparse_regular */

static void
smb_set_reparse_removed(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    (void) private_data;
    if (*status == CHIMERA_VFS_OK && chimera_vfs_compound_op(compound, index)->remove_unmatched) {
        *status = CHIMERA_VFS_ENOENT;
    }
} /* smb_set_reparse_removed */

static void
smb_set_reparse_created(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_set_reparse_ctx     *ctx = private_data;

    ctx->created = *status == CHIMERA_VFS_OK;
    const struct chimera_vfs_attrs *attr = &chimera_vfs_compound_op(compound, index)->attr;
    if (*status == CHIMERA_VFS_OK && (!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH) ||
                                      !attr->va_fh_len || attr->va_fh_len > CHIMERA_VFS_FH_SIZE)) {
        *status = CHIMERA_VFS_EIO;
    }
} /* smb_set_reparse_created */

static void
smb_set_reparse_admit(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint64_t                     token,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    void                        *private_data)
{
    struct smb_set_reparse_ctx            *ctx      = private_data;
    struct chimera_server_smb_thread      *thread   = ctx->request->compound->thread;
    struct chimera_smb_namespace_registry *registry = &thread->shared->namespace_registry;

    (void) index;
    ctx->new_file = chimera_vfs_state_get(thread->shared->vfs->vfs_state,
                                          fh, fh_len, chimera_vfs_hash(fh, fh_len), true);
    if (!ctx->new_file || !chimera_smb_doc_fence_acquire(&ctx->new_doc, registry, fh, fh_len, ctx->request) ||
        !chimera_vfs_claim_access_fence_acquire(&ctx->new_admission, ctx->new_file, ctx)) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_EAGAIN);
        return;
    }
    chimera_smb_namespace_lock(registry);
    evpl_mutex_lock(&ctx->new_file->lock);
    bool busy = chimera_smb_namespace_identity_present_locked(registry, fh, fh_len) ||
        ctx->new_file->delete_pending || ctx->new_file->smb_pending_delete ||
        ctx->new_file->smb_delete_started || ctx->new_file->stream_holders ||
        ctx->new_file->claims[CHIMERA_CLAIM_CLASS_CACHE] ||
        ctx->new_file->claims[CHIMERA_CLAIM_CLASS_RANGE];
    for (struct chimera_vfs_claim *claim = ctx->new_file->claims[CHIMERA_CLAIM_CLASS_ACCESS];
         claim; claim = claim->next) {
        if (claim != &ctx->new_file->implicit_claim) {
            busy = true;
        }
    }
    evpl_mutex_unlock(&ctx->new_file->lock);
    chimera_smb_namespace_unlock(registry);
    chimera_vfs_compound_coordinate_done(compound, token, busy ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
} /* smb_set_reparse_admit */

static void
smb_set_reparse_reserve_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_set_reparse_ctx   *ctx  = private_data;
    struct chimera_smb_open_file *open = ctx->request->ioctl.rp_open_file;
    struct chimera_vfs_claim     *old  = chimera_smb_share_claim(open);

    (void) index; (void) status;
    chimera_vfs_claim_init_smb_open(&ctx->template, old->used, old->denied, &old->owner);
    ctx->template.op_handle  = chimera_vfs_compound_op(compound, ctx->handle_op)->out_handle;
    ctx->template.policy_tag = old->policy_tag;
    /* A tentative replacement cannot be found through ACCESS backreferences. */
    ctx->template.cb_private = NULL;
} /* smb_set_reparse_reserve_prepare */

static void
smb_set_reparse_native_done(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct smb_set_reparse_ctx            *ctx        = private_data;
    struct chimera_smb_request            *request    = ctx->request;
    struct chimera_server_smb_thread      *thread     = request->compound->thread;
    struct chimera_smb_open_file          *open       = request->ioctl.rp_open_file;
    struct chimera_smb_namespace_registry *registry   = &thread->shared->namespace_registry;
    struct chimera_vfs_open_handle        *old_handle = NULL;
    struct chimera_vfs_claim_access_owner *old_owner  = NULL;
    struct chimera_vfs_file_state         *old_file   = NULL;
    enum chimera_vfs_error                 status     = chimera_vfs_compound_status(compound);
    bool                                   accepted   = chimera_vfs_compound_finish_status(compound) == CHIMERA_VFS_OK;
    bool                                   canceled   = chimera_vfs_compound_is_canceled(compound) || request->
        standalone_cancel_requested;
    bool                                   ready = accepted && chimera_vfs_compound_op(compound, ctx->ready_op)->
        completed &&
        chimera_vfs_compound_op(compound, ctx->ready_op)->status == CHIMERA_VFS_OK;
    uint32_t                               reply = status == CHIMERA_VFS_OK ? SMB2_STATUS_SUCCESS :
        status == CHIMERA_VFS_ENOENT ? SMB2_STATUS_OBJECT_NAME_NOT_FOUND :
        status == CHIMERA_VFS_EINTR ? SMB2_STATUS_CANCELLED : SMB2_STATUS_INTERNAL_ERROR;

    chimera_smb_standalone_untrack(request);
    if (ready) {
        /* The irreversible suffix completed despite a late cancellation. */
        reply = SMB2_STATUS_SUCCESS;
        unsigned bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
        chimera_smb_namespace_lock(registry);
        evpl_mutex_lock(&open->tree->open_files_lock[bucket]);
        if (!(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED)) {
            old_file = open->share_file_state;
            evpl_mutex_lock(&old_file->lock);
            evpl_mutex_lock(&ctx->new_file->lock);
            struct chimera_vfs_open_handle        *handle = chimera_vfs_compound_take_handle(compound, ctx->handle_op)
            ;
            struct chimera_vfs_file_state         *new_file = NULL;
            struct chimera_vfs_claim_access_owner *owner    =
                chimera_vfs_compound_take_access_owner(compound, ctx->reserve_op, &new_file);
            chimera_smb_abort_if(!handle || !owner || new_file != ctx->new_file,
                                 "SET_REPARSE accepted identity missing");
            bool                                   rebound = chimera_smb_namespace_rebind_locked(open->
                                                                                                 namespace_participant,
                                                                                                 ctx->source->fh, ctx->
                                                                                                 source->fh_len, handle
                                                                                                 ->fh, handle->fh_len);
            chimera_smb_abort_if(!rebound, "SET_REPARSE fenced namespace identity changed");
            old_handle                                = open->handle;
            old_owner                                 = open->access_owner;
            open->handle                              = handle;
            open->open_flags                          = chimera_smb_open_handle_flags(handle, 0);
            open->access_owner                        = owner;
            open->share_file_state                    = new_file;
            open->share_lease_inserted                = true;
            chimera_smb_share_claim(open)->cb_private = open;
            evpl_mutex_unlock(&ctx->new_file->lock);
            evpl_mutex_unlock(&old_file->lock);
        } else {
            reply = SMB2_STATUS_FILE_CLOSED;
        }
        evpl_mutex_unlock(&open->tree->open_files_lock[bucket]);
        chimera_smb_namespace_unlock(registry);
    }
    if (accepted) {
        const struct chimera_vfs_compound_op *removed = chimera_vfs_compound_op(compound, ctx->remove_op);
        const struct chimera_vfs_compound_op *created = chimera_vfs_compound_op(compound, ctx->create_op);
        if (removed->completed && removed->status == CHIMERA_VFS_OK && !removed->remove_unmatched) {
            chimera_vfs_notify_emit(thread->shared->vfs->vfs_notify,
                                    open->parent_fh, open->parent_fh_len, CHIMERA_VFS_NOTIFY_FILE_REMOVED,
                                    open->name, open->name_len, NULL, 0);
            chimera_vfs_notify_emit_delete(thread->shared->vfs->vfs_notify, ctx->source->fh, ctx->source->fh_len);
        }
        /* CREATE can succeed physically but return unusable identity. Its
         * callback records that prefix independently of validation below. */
        if (created->completed && ctx->created) {
            chimera_vfs_notify_emit(thread->shared->vfs->vfs_notify,
                                    open->parent_fh, open->parent_fh_len, CHIMERA_VFS_NOTIFY_FILE_ADDED,
                                    open->name, open->name_len, NULL, 0);
        }
    }
    chimera_vfs_compound_free(compound);
    /* Retired owner journal pins drain at compound_free, before final put. */
    if (old_owner) {
        chimera_vfs_claim_access_owner_put(old_owner);
    }
    if (old_file) {
        chimera_vfs_state_put(thread->shared->vfs->vfs_state, old_file);
    }
    if (old_handle) {
        chimera_vfs_release(thread->vfs_thread, old_handle);
    }
    if (accepted && ctx->unsupported) {
        reply = canceled ? SMB2_STATUS_CANCELLED : SMB2_STATUS_NOT_SUPPORTED;
    }
    smb_set_reparse_done(ctx, reply);
} /* smb_set_reparse_native_done */

/* Return false only before submitting or changing any filesystem state. */
static bool
smb_set_reparse_native_start(struct smb_set_reparse_ctx *ctx)
{
    struct chimera_smb_request            *request  = ctx->request;
    struct chimera_server_smb_thread      *thread   = request->compound->thread;
    struct chimera_smb_open_file          *open     = request->ioctl.rp_open_file;
    struct chimera_smb_namespace_registry *registry = &thread->shared->namespace_registry;

    if (!smb_set_reparse_native_eligible(open)) {
        return false;
    }
    if (!chimera_smb_namespace_replace_begin(&ctx->replacement, registry, request->compound) ||
        !chimera_vfs_claim_access_fence_acquire(&ctx->old_admission, open->share_file_state, ctx)) {
        smb_set_reparse_done(ctx, SMB2_STATUS_FILE_NOT_AVAILABLE);
        return true;
    }
    unsigned                       peers = 0;
    chimera_smb_namespace_lock(registry);
    chimera_smb_namespace_foreach_locked(registry, ctx->source->fh, ctx->source->fh_len,
                                         smb_set_reparse_peer, &peers);
    evpl_mutex_lock(&open->share_file_state->lock);
    struct chimera_vfs_file_state *file     = open->share_file_state;
    struct chimera_vfs_claim      *claim    = chimera_smb_share_claim(open);
    bool                           occupied = peers != 1 || file->delete_pending || file->smb_pending_delete ||
        file->smb_delete_started || file->stream_holders || file->pending_head ||
        file->claims[CHIMERA_CLAIM_CLASS_CACHE] || file->claims[CHIMERA_CLAIM_CLASS_RANGE];
    for (struct chimera_vfs_claim *peer = file->claims[CHIMERA_CLAIM_CLASS_ACCESS]; peer; peer = peer->next) {
        /* Cached VFS I/O is not another published protocol identity. */
        if (peer != claim && peer != &file->implicit_claim) {
            occupied = true;
        }
    }
    evpl_mutex_unlock(&file->lock);
    chimera_smb_namespace_unlock(registry);
    if (occupied) {
        chimera_vfs_claim_access_fence_release(&ctx->old_admission);
        chimera_smb_namespace_replace_end(&ctx->replacement);
        return false;
    }
    struct chimera_vfs_compound *compound = chimera_vfs_compound_alloc(thread->vfs_thread, &ctx->cred);
    if (!compound) {
        smb_set_reparse_done(ctx, SMB2_STATUS_INSUFFICIENT_RESOURCES); return true;
    }
    chimera_vfs_compound_set_admission_cookie(compound, ctx);
    chimera_vfs_compound_set_attempt_reset(compound, smb_set_reparse_reset, ctx);
    int                          op = chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_MODE);
    if (op < 0) {
        goto build_error;
    }
    chimera_vfs_compound_op_set_handle(compound, op, ctx->source);
    chimera_vfs_compound_set_op_callbacks(compound, op, NULL, smb_set_reparse_regular, ctx);
    if (chimera_vfs_compound_add_putfh(compound, open->parent_fh, open->parent_fh_len) < 0) {
        goto build_error;
    }
    ctx->remove_op = chimera_vfs_compound_add_remove(compound, open->name, open->name_len, CHIMERA_VFS_REMOVE_ISNOTDIR |
                                                     CHIMERA_VFS_REMOVE_NO_NOTIFY, 0, 0);
    if (ctx->remove_op < 0) {
        goto build_error;
    }
    struct chimera_vfs_compound_op *remove = chimera_vfs_compound_op_args(compound, ctx->remove_op);
    remove->remove_match_child_fh = true;
    memcpy(remove->arg_fh, ctx->source->fh, ctx->source->fh_len);
    remove->arg_fh_len = ctx->source->fh_len;
    chimera_vfs_compound_set_op_callbacks(compound, ctx->remove_op, NULL, smb_set_reparse_removed, ctx);
    struct chimera_vfs_attrs        attr = { 0 };
    uint8_t                         type = CHIMERA_VFS_COMPOUND_CREATE_SYMLINK;
    if (request->ioctl.rp_nfs_type != SMB2_NFS_SPECFILE_LNK) {
        type         = CHIMERA_VFS_COMPOUND_CREATE_NODE;
        attr.va_mode = 0666 | (request->ioctl.rp_nfs_type == SMB2_NFS_SPECFILE_CHR ? S_IFCHR :
                               request->ioctl.rp_nfs_type == SMB2_NFS_SPECFILE_BLK ? S_IFBLK :
                               request->ioctl.rp_nfs_type == SMB2_NFS_SPECFILE_FIFO ? S_IFIFO : S_IFSOCK);
        attr.va_req_mask = attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        if (S_ISCHR(attr.va_mode) || S_ISBLK(attr.va_mode)) {
            attr.va_rdev      = ((uint64_t) request->ioctl.rp_device_major << 32) | request->ioctl.rp_device_minor;
            attr.va_req_mask |= CHIMERA_VFS_ATTR_RDEV;
            attr.va_set_mask |= CHIMERA_VFS_ATTR_RDEV;
        }
    }
    ctx->create_op = chimera_vfs_compound_add_create(compound, type, open->name, open->name_len, request->ioctl.
                                                     rp_target, request->ioctl.rp_target_len, &attr, CHIMERA_VFS_ATTR_FH
                                                     , 0, 0);
    if (ctx->create_op < 0) {
        goto build_error;
    }
    chimera_vfs_compound_op_args(compound, ctx->create_op)->namespace_flags =
        type == CHIMERA_VFS_COMPOUND_CREATE_SYMLINK ? CHIMERA_VFS_SYMLINK_NO_NOTIFY : CHIMERA_VFS_MKNOD_NO_NOTIFY;
    chimera_vfs_compound_set_op_callbacks(compound, ctx->create_op, NULL, smb_set_reparse_created, ctx);
    if (chimera_vfs_compound_add_open_current(compound, 0, 0) < 0) {
        goto build_error;
    }
    ctx->handle_op = chimera_vfs_compound_add_gethandle(compound);
    if (ctx->handle_op < 0) {
        goto build_error;
    }
    op = chimera_vfs_compound_add_coordinate(compound, smb_set_reparse_admit, ctx);
    if (op < 0) {
        goto build_error;
    }
    chimera_vfs_compound_op_args(compound, op)->coordinate_each_attempt = 1;
    ctx->reserve_op                                                     = chimera_vfs_compound_add_reserve_access(
        compound, ctx->handle_op, &ctx->template);
    if (ctx->reserve_op < 0) {
        goto build_error;
    }
    chimera_vfs_compound_set_op_prepare(compound, ctx->reserve_op, smb_set_reparse_reserve_prepare, ctx);
    if (chimera_vfs_compound_add_retire_access(compound, open->access_owner) < 0) {
        goto build_error;
    }
    ctx->ready_op = chimera_vfs_compound_add_checkpoint(compound);
    if (ctx->ready_op < 0) {
        goto build_error;
    }
    struct chimera_vfs_compound_group_config group = { .first_op = 0,
                                                       .num_ops  = chimera_vfs_compound_num_ops(compound), .dependency =
                                                           -1 };
    /* This standalone group owns the new reservation until accepted transfer.
     * A failed suffix leaves it untransferred for compound_free to retire.
     * Do not use reserve_access_until here: the group records EINTR for a late
     * cancel even when the mandatory suffix reached readiness, and that API
     * would withdraw the new owner after the old retirement was staged. */
    bool                                     valid = chimera_vfs_compound_add_group(compound, &group) >= 0 &&
        chimera_vfs_compound_set_cancel_scope(compound, ctx->remove_op, ctx->ready_op);
    if (!valid) {
        goto build_error;
    }
    if (!chimera_smb_standalone_track(request, compound)) {
        chimera_vfs_compound_free(compound);
        smb_set_reparse_done(ctx, SMB2_STATUS_FILE_CLOSED);
        return true;
    }
    chimera_frontend_compound_submit(compound, smb_set_reparse_native_done, ctx);
    return true;
 build_error:
    chimera_vfs_compound_free(compound);
    smb_set_reparse_done(ctx, SMB2_STATUS_INSUFFICIENT_RESOURCES);
    return true;
} /* smb_set_reparse_native_start */

/* ------------------------------------------------------------------ */
/* Reparse points of any other tag                                    */
/* ------------------------------------------------------------------ */

/* A reparse point whose tag chimera does not turn into a special file is kept
 * verbatim: the whole REPARSE_[GUID_]DATA_BUFFER is the file's chimera.reparse
 * xattr (outside the user. namespace, so it is neither an EA nor an NFS user
 * xattr), and FILE_ATTRIBUTE_REPARSE_POINT is set in the file's persisted DOS
 * attributes so every query reports it without reading the buffer.  A plain
 * open of such a file is refused with STATUS_IO_REPARSE_TAG_NOT_HANDLED (see
 * chimera_smb_create_open_at_callback), as on a Windows server, which has no
 * filter to handle the tag either. */

static void
chimera_smb_reparse_finish(
    struct chimera_smb_request *request,
    uint32_t                    status)
{
    free(request->ioctl.rp_existing);
    request->ioctl.rp_existing = NULL;
    chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_reparse_finish */

static uint32_t
chimera_smb_reparse_vfs_status(enum chimera_vfs_error error_code)
{
    switch (error_code) {
        case CHIMERA_VFS_OK:
            return SMB2_STATUS_SUCCESS;
        case CHIMERA_VFS_ENOTSUP:
            return SMB2_STATUS_INVALID_DEVICE_REQUEST;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:
            return SMB2_STATUS_ACCESS_DENIED;
        default:
            return SMB2_STATUS_INTERNAL_ERROR;
    } /* switch */
} /* chimera_smb_reparse_vfs_status */

/* Compare the reparse point already on the file (rp_existing, len bytes) with
 * the tag and GUID of this request: SMB2_STATUS_SUCCESS if they agree. */
static uint32_t
chimera_smb_reparse_match_existing(
    struct chimera_smb_request *request,
    uint32_t                    len)
{
    const uint8_t *cur = request->ioctl.rp_existing;
    uint32_t       tag = request->ioctl.rp_reparse_tag;
    uint32_t       cur_tag;

    if (len < SMB2_REPARSE_DATA_HEADER_SIZE) {
        return SMB2_STATUS_SUCCESS;
    }
    memcpy(&cur_tag, cur, 4);
    if (cur_tag != tag) {
        return SMB2_STATUS_IO_REPARSE_TAG_MISMATCH;
    }
    if (!SMB2_IO_REPARSE_TAG_IS_MICROSOFT(tag) &&
        len >= SMB2_REPARSE_GUID_DATA_HEADER_SIZE &&
        memcmp(cur + 8, request->ioctl.rp_guid, 16) != 0) {
        return SMB2_STATUS_REPARSE_ATTRIBUTE_CONFLICT;
    }
    return SMB2_STATUS_SUCCESS;
} /* chimera_smb_reparse_match_existing */

static void
chimera_smb_reparse_setattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    (void) pre_attr;
    (void) set_attr;
    (void) post_attr;

    chimera_smb_reparse_finish(private_data, chimera_smb_reparse_vfs_status(error_code));
} /* chimera_smb_reparse_setattr_cb */

/* Set or clear FILE_ATTRIBUTE_REPARSE_POINT in the persisted DOS attributes; a
 * data file is also marked ARCHIVE (MS-FSA 2.1.5.10.37 / 2.1.5.10.3). */
static void
chimera_smb_reparse_set_dos(
    struct chimera_smb_request *request,
    int                         reparse)
{
    struct chimera_vfs_attrs *attr = &request->ioctl.rp_set_attr;
    uint32_t                  dos  = request->ioctl.rp_dos & ~SMB2_FILE_ATTRIBUTE_NORMAL;

    if (reparse) {
        dos |= SMB2_FILE_ATTRIBUTE_REPARSE_POINT;
    } else {
        dos &= ~SMB2_FILE_ATTRIBUTE_REPARSE_POINT;
    }
    if (!request->ioctl.rp_isdir) {
        dos |= SMB2_FILE_ATTRIBUTE_ARCHIVE;
    }

    attr->va_req_mask       = CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
    attr->va_set_mask       = CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
    attr->va_dos_attributes = dos;

    chimera_vfs_setattr(request->compound->thread->vfs_thread,
                        &request->session_handle->session->cred,
                        request->ioctl.rp_open_file->handle,
                        attr,
                        0,
                        0,
                        chimera_smb_reparse_setattr_cb,
                        request);
} /* chimera_smb_reparse_set_dos */

static void
chimera_smb_reparse_set_xattr_cb(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct chimera_smb_request *request = private_data;

    (void) pre_attr;
    (void) post_attr;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_reparse_finish(request, chimera_smb_reparse_vfs_status(error_code));
        return;
    }
    chimera_smb_reparse_set_dos(request, 1);
} /* chimera_smb_reparse_set_xattr_cb */

static void
chimera_smb_reparse_set_store(struct chimera_smb_request *request)
{
    chimera_vfs_set_xattr(request->compound->thread->vfs_thread,
                          &request->session_handle->session->cred,
                          request->ioctl.rp_open_file->handle,
                          0,
                          CHIMERA_SMB_REPARSE_XATTR,
                          CHIMERA_SMB_REPARSE_XATTR_LEN,
                          request->ioctl.rp_response,
                          request->ioctl.rp_generic_len,
                          chimera_smb_reparse_set_xattr_cb,
                          request);
} /* chimera_smb_reparse_set_store */

/* A reparse point is already on the file: it may only be replaced by one of
 * the same tag (and, for a non-Microsoft tag, the same GUID). */
static void
chimera_smb_reparse_set_existing_cb(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data)
{
    struct chimera_smb_request *request = private_data;
    uint32_t                    status;

    if (error_code == CHIMERA_VFS_OK) {
        status = chimera_smb_reparse_match_existing(request, value_len);
        if (status != SMB2_STATUS_SUCCESS) {
            chimera_smb_reparse_finish(request, status);
            return;
        }
    } else if (error_code != CHIMERA_VFS_ENODATA) {
        chimera_smb_reparse_finish(request, chimera_smb_reparse_vfs_status(error_code));
        return;
    }
    chimera_smb_reparse_set_store(request);
} /* chimera_smb_reparse_set_existing_cb */

static void
chimera_smb_reparse_set_check_existing(struct chimera_smb_request *request)
{
    if (!(request->ioctl.rp_dos & SMB2_FILE_ATTRIBUTE_REPARSE_POINT)) {
        chimera_smb_reparse_set_store(request);
        return;
    }

    request->ioctl.rp_existing = malloc(CHIMERA_SMB_REPARSE_READ_MAX);
    chimera_vfs_get_xattr(request->compound->thread->vfs_thread,
                          &request->session_handle->session->cred,
                          request->ioctl.rp_open_file->handle,
                          CHIMERA_SMB_REPARSE_XATTR,
                          CHIMERA_SMB_REPARSE_XATTR_LEN,
                          request->ioctl.rp_existing,
                          CHIMERA_SMB_REPARSE_READ_MAX,
                          chimera_smb_reparse_set_existing_cb,
                          request);
} /* chimera_smb_reparse_set_check_existing */

/* Directory emptiness: a reparse point may only be set on an empty one. */
static int
chimera_smb_reparse_dir_entry_cb(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct chimera_smb_request *request = arg;

    (void) inum;
    (void) cookie;
    (void) attrs;

    if ((namelen == 1 && name[0] == '.') ||
        (namelen == 2 && name[0] == '.' && name[1] == '.')) {
        return 0;
    }
    request->ioctl.rp_dir_nonempty = 1;
    return 1;
} /* chimera_smb_reparse_dir_entry_cb */

static void
chimera_smb_reparse_dir_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_smb_request *request = private_data;

    (void) handle;
    (void) cookie;
    (void) verifier;
    (void) eof;
    (void) attr;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_reparse_finish(request, chimera_smb_reparse_vfs_status(error_code));
        return;
    }
    if (request->ioctl.rp_dir_nonempty) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_DIRECTORY_NOT_EMPTY);
        return;
    }
    chimera_smb_reparse_set_check_existing(request);
} /* chimera_smb_reparse_dir_complete */

static void
chimera_smb_reparse_set_getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_reparse_finish(request, chimera_smb_reparse_vfs_status(error_code));
        return;
    }

    request->ioctl.rp_isdir = S_ISDIR(attr->va_mode);
    request->ioctl.rp_dos   = (attr->va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) ?
        attr->va_dos_attributes : 0;

    if (request->ioctl.rp_reparse_tag == SMB2_IO_REPARSE_TAG_MOUNT_POINT &&
        !request->ioctl.rp_isdir) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_NOT_A_DIRECTORY);
        return;
    }

    /* A file with EAs cannot also become a reparse point. */
    if (!(request->ioctl.rp_dos & SMB2_FILE_ATTRIBUTE_REPARSE_POINT) &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_EA_SIZE) && attr->va_ea_size) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_EAS_NOT_SUPPORTED);
        return;
    }

    if (request->ioctl.rp_isdir) {
        request->ioctl.rp_dir_nonempty = 0;
        chimera_vfs_readdir(request->compound->thread->vfs_thread,
                            &request->session_handle->session->cred,
                            request->ioctl.rp_open_file->handle,
                            0, 0, 0, 0, 0, NULL, 0,
                            chimera_smb_reparse_dir_entry_cb,
                            chimera_smb_reparse_dir_complete,
                            request);
        return;
    }

    chimera_smb_reparse_set_check_existing(request);
} /* chimera_smb_reparse_set_getattr_cb */

/* MS-FSA 2.1.5.10.37 for a reparse point kept verbatim. */
static void
chimera_smb_reparse_generic_set(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open_file = request->ioctl.rp_open_file;

    if (!(open_file->granted_access & (SMB2_FILE_WRITE_DATA | SMB2_FILE_WRITE_ATTRIBUTES))) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }
    if (!(open_file->handle->vfs_module->capabilities & CHIMERA_VFS_CAP_XATTR)) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_INVALID_DEVICE_REQUEST);
        return;
    }

    if (!SMB2_IO_REPARSE_TAG_IS_MICROSOFT(request->ioctl.rp_reparse_tag)) {
        memcpy(request->ioctl.rp_guid, request->ioctl.rp_response + 8, 16);
    }

    chimera_vfs_getattr(request->compound->thread->vfs_thread,
                        &request->session_handle->session->cred,
                        open_file->handle,
                        CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_DOS_ATTRIBUTES |
                        CHIMERA_VFS_ATTR_EA_SIZE,
                        chimera_smb_reparse_set_getattr_cb,
                        request);
} /* chimera_smb_reparse_generic_set */

/* ---- DELETE_REPARSE_POINT (MS-FSA 2.1.5.10.3) ---- */

static void
chimera_smb_reparse_delete_removed_cb(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct chimera_smb_request *request = private_data;

    (void) pre_attr;
    (void) post_attr;

    if (error_code != CHIMERA_VFS_OK && error_code != CHIMERA_VFS_ENODATA) {
        chimera_smb_reparse_finish(request, chimera_smb_reparse_vfs_status(error_code));
        return;
    }
    chimera_smb_reparse_set_dos(request, 0);
} /* chimera_smb_reparse_delete_removed_cb */

static void
chimera_smb_reparse_delete_existing_cb(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data)
{
    struct chimera_smb_request *request = private_data;
    uint32_t                    status;

    if (error_code == CHIMERA_VFS_ENODATA) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_NOT_A_REPARSE_POINT);
        return;
    }
    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_reparse_finish(request, chimera_smb_reparse_vfs_status(error_code));
        return;
    }

    status = chimera_smb_reparse_match_existing(request, value_len);
    if (status != SMB2_STATUS_SUCCESS) {
        chimera_smb_reparse_finish(request, status);
        return;
    }

    chimera_vfs_remove_xattr(request->compound->thread->vfs_thread,
                             &request->session_handle->session->cred,
                             request->ioctl.rp_open_file->handle,
                             CHIMERA_SMB_REPARSE_XATTR,
                             CHIMERA_SMB_REPARSE_XATTR_LEN,
                             chimera_smb_reparse_delete_removed_cb,
                             request);
} /* chimera_smb_reparse_delete_existing_cb */

static void
chimera_smb_reparse_delete_getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_reparse_finish(request, chimera_smb_reparse_vfs_status(error_code));
        return;
    }

    request->ioctl.rp_isdir = S_ISDIR(attr->va_mode);
    request->ioctl.rp_dos   = (attr->va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) ?
        attr->va_dos_attributes : 0;

    if (!(request->ioctl.rp_dos & SMB2_FILE_ATTRIBUTE_REPARSE_POINT)) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_NOT_A_REPARSE_POINT);
        return;
    }

    request->ioctl.rp_existing = malloc(CHIMERA_SMB_REPARSE_READ_MAX);
    chimera_vfs_get_xattr(request->compound->thread->vfs_thread,
                          &request->session_handle->session->cred,
                          request->ioctl.rp_open_file->handle,
                          CHIMERA_SMB_REPARSE_XATTR,
                          CHIMERA_SMB_REPARSE_XATTR_LEN,
                          request->ioctl.rp_existing,
                          CHIMERA_SMB_REPARSE_READ_MAX,
                          chimera_smb_reparse_delete_existing_cb,
                          request);
} /* chimera_smb_reparse_delete_getattr_cb */

void
chimera_smb_ioctl_delete_reparse(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open_file;
    uint32_t                      tag = request->ioctl.rp_reparse_tag;
    uint32_t                      header;

    open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);
    if (!open_file) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }
    request->ioctl.rp_open_file = open_file;
    request->ioctl.rp_existing  = NULL;

    if (!(open_file->granted_access & (SMB2_FILE_WRITE_DATA | SMB2_FILE_WRITE_ATTRIBUTES))) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }
    if (tag == 0 || tag == 1) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_IO_REPARSE_TAG_INVALID);
        return;
    }
    /* The request names the reparse point and carries no data: exactly the
     * header for its kind of tag. */
    header = SMB2_IO_REPARSE_TAG_IS_MICROSOFT(tag) ?
        SMB2_REPARSE_DATA_HEADER_SIZE : SMB2_REPARSE_GUID_DATA_HEADER_SIZE;
    if (request->ioctl.rp_data_len != 0 || request->ioctl.input_count != header) {
        chimera_smb_info("DELETE_REPARSE_POINT: malformed request tag 0x%08x data_len %u "
                         "input %u max_output %u",
                         tag, request->ioctl.rp_data_len, request->ioctl.input_count,
                         request->ioctl.max_output_response);
        chimera_smb_reparse_finish(request, SMB2_STATUS_IO_REPARSE_DATA_INVALID);
        return;
    }
    if (!(open_file->handle->vfs_module->capabilities & CHIMERA_VFS_CAP_XATTR)) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_NOT_A_REPARSE_POINT);
        return;
    }

    chimera_vfs_getattr(request->compound->thread->vfs_thread,
                        &request->session_handle->session->cred,
                        open_file->handle,
                        CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_DOS_ATTRIBUTES,
                        chimera_smb_reparse_delete_getattr_cb,
                        request);
} /* chimera_smb_ioctl_delete_reparse */

/* ---- GET_REPARSE_POINT of a reparse point kept verbatim ---- */

/* Fit a built GET response to the caller's buffer (MS-FSA 2.1.5.10.14): not
 * even the header -> BUFFER_TOO_SMALL; part of the data -> BUFFER_OVERFLOW
 * with what fits, as NTFS answers. */
static void
chimera_smb_get_reparse_finish(struct chimera_smb_request *request)
{
    uint32_t tag, header, max = request->ioctl.max_output_response;
    uint32_t status = SMB2_STATUS_SUCCESS;

    memcpy(&tag, request->ioctl.rp_response, 4);
    header = SMB2_IO_REPARSE_TAG_IS_MICROSOFT(tag) ?
        SMB2_REPARSE_DATA_HEADER_SIZE : SMB2_REPARSE_GUID_DATA_HEADER_SIZE;

    if (max < header) {
        request->ioctl.rp_response_len = 0;
        status                         = SMB2_STATUS_BUFFER_TOO_SMALL;
    } else if ((uint32_t) request->ioctl.rp_response_len > max) {
        request->ioctl.rp_response_len = max;
        status                         = SMB2_STATUS_BUFFER_OVERFLOW;
    }

    chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_get_reparse_finish */

static void
chimera_smb_get_reparse_xattr_cb(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK || value_len < SMB2_REPARSE_DATA_HEADER_SIZE) {
        chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
        chimera_smb_complete_request(request,
                                     (error_code == CHIMERA_VFS_OK ||
                                      error_code == CHIMERA_VFS_ENODATA) ?
                                     SMB2_STATUS_NOT_A_REPARSE_POINT :
                                     chimera_smb_reparse_vfs_status(error_code));
        return;
    }
    request->ioctl.rp_response_len = value_len;
    chimera_smb_get_reparse_finish(request);
} /* chimera_smb_get_reparse_xattr_cb */

void
chimera_smb_ioctl_set_reparse(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file;

    /* If the tag was unsupported (cleared to 0 by parser), accept and ignore */
    if (request->ioctl.rp_reparse_tag == 0) {
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    /* A resolved open reference protects the open slot, not its tree's bucket
     * storage. Pin the request context before any resolver or failure release. */
    if (!chimera_smb_request_pin_context(request)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }
    open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);

    if (!open_file) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    switch (request->ioctl.rp_nfs_type) {
        case SMB2_NFS_SPECFILE_LNK:
        case SMB2_NFS_SPECFILE_CHR:
        case SMB2_NFS_SPECFILE_BLK:
        case SMB2_NFS_SPECFILE_FIFO:
        case SMB2_NFS_SPECFILE_SOCK:
            break;
        default:
            /* The parser preserves unknown NFS types. Reject BEFORE removing
             * the original object, rather than in remove's completion. */
            chimera_smb_open_file_release(request, open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_NOT_IMPLEMENTED);
            return;
    } /* switch */

    request->ioctl.rp_open_file = open_file;
    request->ioctl.rp_existing  = NULL;

    if (request->ioctl.rp_generic) {
        chimera_smb_reparse_generic_set(request);
        return;
    }

    /* VFS mknod requires a privileged caller for device nodes. SET replaces
     * an existing placeholder: enforce this known failure before unlinking it,
     * before constructing the replacement. FIFO/socket creation has no such
     * privilege requirement. */
    if ((request->ioctl.rp_nfs_type == SMB2_NFS_SPECFILE_CHR ||
         request->ioctl.rp_nfs_type == SMB2_NFS_SPECFILE_BLK) &&
        request->session_handle->session->cred.uid != 0) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    struct smb_set_reparse_ctx *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INSUFFICIENT_RESOURCES);
        return;
    }
    ctx->request                = request;
    ctx->cred                   = request->session_handle->session->cred;
    request->ioctl.rp_open_file = open_file;
    if (!smb_set_reparse_gate(ctx)) {
        smb_set_reparse_done(ctx, SMB2_STATUS_FILE_NOT_AVAILABLE);
        return;
    }
    uint32_t namespace_status = chimera_smb_namespace_legacy_begin(request, open_file);
    if (namespace_status != SMB2_STATUS_SUCCESS) {
        smb_set_reparse_done(ctx, namespace_status);
        return;
    }
    ctx->source = chimera_smb_disposition_pin_handle(vfs_thread, open_file);
    if (!ctx->source) {
        smb_set_reparse_done(ctx, SMB2_STATUS_FILE_CLOSED);
        return;
    }
    if (!(ctx->source->vfs_module->capabilities & CHIMERA_VFS_CAP_REMOVE_MATCH_FH)) {
        /* Never substitute a lookup followed by unconditional unlink. */
        smb_set_reparse_done(ctx, SMB2_STATUS_NOT_SUPPORTED);
        return;
    }

    if (smb_set_reparse_native_start(ctx)) {
        return;
    }
    /* No handle-only fallback: it would leave claims and namespace entries
     * attached to the removed inode after installing the replacement handle. */
    smb_set_reparse_done(ctx, SMB2_STATUS_NOT_SUPPORTED);
} /* chimera_smb_ioctl_set_reparse */

/* ------------------------------------------------------------------ */
/* GET_REPARSE_POINT                                                  */
/* ------------------------------------------------------------------ */

static uint32_t
smb_get_reparse_symlink(
    struct chimera_smb_request *request,
    int                         target_length)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    uint8_t                          *buf    = request->ioctl.rp_response;
    int                               utf16_len;
    uint16_t                          reparse_data_length;
    uint32_t                          flags;

    /* A relative target carries no leading separator (test before the slash
     * conversion below). */
    flags = (target_length > 0 && request->ioctl.rp_target[0] != '/') ?
        SMB2_SYMLINK_FLAG_RELATIVE : SMB2_SYMLINK_FLAG_ABSOLUTE;

    /* Convert Unix forward slashes to Windows backslashes */
    for (int i = 0; i < target_length; i++) {
        if (request->ioctl.rp_target[i] == '/') {
            request->ioctl.rp_target[i] = '\\';
        }
    }

    /* A symlink is reported as a SYMBOLIC_LINK_REPARSE_BUFFER under
     * IO_REPARSE_TAG_SYMLINK (MS-FSCC 2.1.2.4): the Windows representation that
     * Windows-style clients (e.g. pike's get_symlink) decode.  Convert the
     * target to UTF-16LE for the Substitute name; the Print name is an identical
     * copy that immediately follows it. */
    utf16_len = chimera_smb_utf8_to_utf16le(
        &thread->iconv_ctx,
        request->ioctl.rp_target,
        target_length,
        (uint16_t *) (buf + 20),
        (CHIMERA_VFS_PATH_MAX - 1) * 2);

    if (utf16_len < 0) {
        return SMB2_STATUS_INTERNAL_ERROR;
    }

    memcpy(buf + 20 + utf16_len, buf + 20, utf16_len); /* Print name copy */

    reparse_data_length = 12 + 2 * utf16_len;

    /* ReparseTag = IO_REPARSE_TAG_SYMLINK */
    buf[0] = (SMB2_IO_REPARSE_TAG_SYMLINK >>  0) & 0xff;
    buf[1] = (SMB2_IO_REPARSE_TAG_SYMLINK >>  8) & 0xff;
    buf[2] = (SMB2_IO_REPARSE_TAG_SYMLINK >> 16) & 0xff;
    buf[3] = (SMB2_IO_REPARSE_TAG_SYMLINK >> 24) & 0xff;
    /* ReparseDataLength */
    buf[4] = (reparse_data_length >> 0) & 0xff;
    buf[5] = (reparse_data_length >> 8) & 0xff;
    /* Reserved */
    buf[6] = 0;
    buf[7] = 0;
    /* SubstituteNameOffset = 0 */
    buf[8] = 0;
    buf[9] = 0;
    /* SubstituteNameLength */
    buf[10] = (utf16_len >> 0) & 0xff;
    buf[11] = (utf16_len >> 8) & 0xff;
    /* PrintNameOffset (immediately after the Substitute name) */
    buf[12] = (utf16_len >> 0) & 0xff;
    buf[13] = (utf16_len >> 8) & 0xff;
    /* PrintNameLength */
    buf[14] = (utf16_len >> 0) & 0xff;
    buf[15] = (utf16_len >> 8) & 0xff;
    /* Flags */
    buf[16] = (flags >>  0) & 0xff;
    buf[17] = (flags >>  8) & 0xff;
    buf[18] = (flags >> 16) & 0xff;
    buf[19] = (flags >> 24) & 0xff;
    /* PathBuffer (Substitute name + Print name) already at buf+20 */

    request->ioctl.rp_response_len = 20 + 2 * utf16_len;

    return SMB2_STATUS_SUCCESS;
} /* smb_get_reparse_symlink */

static void
chimera_smb_get_reparse_readlink_cb(
    enum chimera_vfs_error error_code,
    int                    target_length,
    void                  *private_data)
{
    struct chimera_smb_request       *request = private_data;
    struct chimera_server_smb_thread *thread  = request->compound->thread;
    uint8_t                          *buf     = request->ioctl.rp_response;
    int                               utf16_len;
    uint16_t                          reparse_data_length;
    uint32_t                          flags;

    chimera_smb_open_file_release(request, request->ioctl.rp_open_file);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    /* A relative target carries no leading separator (test before the slash
     * conversion below). */
    flags = (target_length > 0 && request->ioctl.rp_target[0] != '/') ?
        SMB2_SYMLINK_FLAG_RELATIVE : SMB2_SYMLINK_FLAG_ABSOLUTE;

    /* Convert Unix forward slashes to Windows backslashes */
    for (int i = 0; i < target_length; i++) {
        if (request->ioctl.rp_target[i] == '/') {
            request->ioctl.rp_target[i] = '\\';
        }
    }

    /* A symlink is reported as a SYMBOLIC_LINK_REPARSE_BUFFER under
     * IO_REPARSE_TAG_SYMLINK (MS-FSCC 2.1.2.4): the Windows representation that
     * Windows-style clients (e.g. pike's get_symlink) decode.  Convert the
     * target to UTF-16LE for the Substitute name; the Print name is an identical
     * copy that immediately follows it. */
    utf16_len = chimera_smb_utf8_to_utf16le(
        &thread->iconv_ctx,
        request->ioctl.rp_target,
        target_length,
        (uint16_t *) (buf + 20),
        (CHIMERA_VFS_PATH_MAX - 1) * 2);

    if (utf16_len < 0) {
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    memcpy(buf + 20 + utf16_len, buf + 20, utf16_len); /* Print name copy */

    reparse_data_length = 12 + 2 * utf16_len;

    /* ReparseTag = IO_REPARSE_TAG_SYMLINK */
    buf[0] = (SMB2_IO_REPARSE_TAG_SYMLINK >>  0) & 0xff;
    buf[1] = (SMB2_IO_REPARSE_TAG_SYMLINK >>  8) & 0xff;
    buf[2] = (SMB2_IO_REPARSE_TAG_SYMLINK >> 16) & 0xff;
    buf[3] = (SMB2_IO_REPARSE_TAG_SYMLINK >> 24) & 0xff;
    /* ReparseDataLength */
    buf[4] = (reparse_data_length >> 0) & 0xff;
    buf[5] = (reparse_data_length >> 8) & 0xff;
    /* Reserved */
    buf[6] = 0;
    buf[7] = 0;
    /* SubstituteNameOffset = 0 */
    buf[8] = 0;
    buf[9] = 0;
    /* SubstituteNameLength */
    buf[10] = (utf16_len >> 0) & 0xff;
    buf[11] = (utf16_len >> 8) & 0xff;
    /* PrintNameOffset (immediately after the Substitute name) */
    buf[12] = (utf16_len >> 0) & 0xff;
    buf[13] = (utf16_len >> 8) & 0xff;
    /* PrintNameLength */
    buf[14] = (utf16_len >> 0) & 0xff;
    buf[15] = (utf16_len >> 8) & 0xff;
    /* Flags */
    buf[16] = (flags >>  0) & 0xff;
    buf[17] = (flags >>  8) & 0xff;
    buf[18] = (flags >> 16) & 0xff;
    buf[19] = (flags >> 24) & 0xff;
    /* PathBuffer (Substitute name + Print name) already at buf+20 */

    request->ioctl.rp_response_len = 20 + 2 * utf16_len;

    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_get_reparse_readlink_cb */

/*
 * PUTHANDLE, READLINK -- the second sequence, run only when the first one's
 * mode says symlink.  The target belongs to the compound, so it is copied into
 * the request before the free.
 */
static void
chimera_smb_get_reparse_readlink_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    int                                   target_len = 0;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);

        target_len = (int) op->target_len;

        if (target_len > CHIMERA_VFS_PATH_MAX - 1) {
            target_len = CHIMERA_VFS_PATH_MAX - 1;
        }

        if (target_len > 0) {
            memcpy(request->ioctl.rp_target, op->target, target_len);
        }
        request->ioctl.rp_target[target_len] = '\0';
    }

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    request->ioctl.rp_target_len = target_len;

    chimera_smb_get_reparse_readlink_cb(status, target_len, request);
} /* chimera_smb_get_reparse_readlink_complete */

static inline void
chimera_smb_get_reparse_build_simple(
    struct chimera_smb_request *request,
    uint64_t                    nfs_type)
{
    uint8_t *buf      = request->ioctl.rp_response;
    int      data_len = 8; /* InodeType only */

    /* ReparseTag */
    buf[0] = (SMB2_IO_REPARSE_TAG_NFS >>  0) & 0xff;
    buf[1] = (SMB2_IO_REPARSE_TAG_NFS >>  8) & 0xff;
    buf[2] = (SMB2_IO_REPARSE_TAG_NFS >> 16) & 0xff;
    buf[3] = (SMB2_IO_REPARSE_TAG_NFS >> 24) & 0xff;
    /* ReparseDataLength */
    buf[4] = (data_len >>  0) & 0xff;
    buf[5] = (data_len >>  8) & 0xff;
    /* Reserved */
    buf[6] = 0;
    buf[7] = 0;
    /* InodeType */
    buf[8]  = (nfs_type >>  0) & 0xff;
    buf[9]  = (nfs_type >>  8) & 0xff;
    buf[10] = (nfs_type >> 16) & 0xff;
    buf[11] = (nfs_type >> 24) & 0xff;
    buf[12] = (nfs_type >> 32) & 0xff;
    buf[13] = (nfs_type >> 40) & 0xff;
    buf[14] = (nfs_type >> 48) & 0xff;
    buf[15] = (nfs_type >> 56) & 0xff;

    request->ioctl.rp_response_len = 8 + data_len; /* header(8) + data */
} /* chimera_smb_get_reparse_build_simple */

static inline void
chimera_smb_get_reparse_build_device(
    struct chimera_smb_request *request,
    uint64_t                    nfs_type,
    uint32_t                    major,
    uint32_t                    minor)
{
    uint8_t *buf      = request->ioctl.rp_response;
    int      data_len = 8 + 8; /* InodeType(8) + major(4) + minor(4) */

    /* ReparseTag */
    buf[0] = (SMB2_IO_REPARSE_TAG_NFS >>  0) & 0xff;
    buf[1] = (SMB2_IO_REPARSE_TAG_NFS >>  8) & 0xff;
    buf[2] = (SMB2_IO_REPARSE_TAG_NFS >> 16) & 0xff;
    buf[3] = (SMB2_IO_REPARSE_TAG_NFS >> 24) & 0xff;
    /* ReparseDataLength */
    buf[4] = (data_len >>  0) & 0xff;
    buf[5] = (data_len >>  8) & 0xff;
    /* Reserved */
    buf[6] = 0;
    buf[7] = 0;
    /* InodeType */
    buf[8]  = (nfs_type >>  0) & 0xff;
    buf[9]  = (nfs_type >>  8) & 0xff;
    buf[10] = (nfs_type >> 16) & 0xff;
    buf[11] = (nfs_type >> 24) & 0xff;
    buf[12] = (nfs_type >> 32) & 0xff;
    buf[13] = (nfs_type >> 40) & 0xff;
    buf[14] = (nfs_type >> 48) & 0xff;
    buf[15] = (nfs_type >> 56) & 0xff;
    /* Major */
    buf[16] = (major >>  0) & 0xff;
    buf[17] = (major >>  8) & 0xff;
    buf[18] = (major >> 16) & 0xff;
    buf[19] = (major >> 24) & 0xff;
    /* Minor */
    buf[20] = (minor >>  0) & 0xff;
    buf[21] = (minor >>  8) & 0xff;
    buf[22] = (minor >> 16) & 0xff;
    buf[23] = (minor >> 24) & 0xff;

    request->ioctl.rp_response_len = 8 + data_len; /* header(8) + data */
} /* chimera_smb_get_reparse_build_device */

/*
 * PUTHANDLE, GETATTR(MODE | RDEV).
 *
 * The symlink arm runs as a SECOND sequence rather than a READLINK behind this
 * GETATTR, because whether to read a link at all depends on the mode this op
 * answers with -- and an op cannot be skipped on an earlier op's result: the
 * gate can only refuse, and a refusal here would fail a GET_REPARSE of a device
 * node that is perfectly well answered from the attributes alone.
 */
static void
chimera_smb_get_reparse_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request   = private_data;
    struct chimera_smb_open_file         *open_file = request->ioctl.rp_open_file;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              attr;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    memset(&attr, 0, sizeof(attr));

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        attr = op->attr;
    }

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (status != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    switch (attr.va_mode & S_IFMT) {
        case S_IFLNK:
            request->vfs_compound = chimera_vfs_compound_alloc(
                request->compound->thread->vfs_thread,
                &request->session_handle->session->cred);

            chimera_vfs_compound_add_puthandle(request->vfs_compound,
                                               open_file->handle,
                                               open_file->open_flags);

            chimera_vfs_compound_add_readlink(request->vfs_compound);

            chimera_frontend_compound_submit(
                request->vfs_compound,
                chimera_smb_get_reparse_readlink_complete, request);
            return;
        case S_IFCHR:
            chimera_smb_get_reparse_build_device(
                request,
                SMB2_NFS_SPECFILE_CHR,
                (uint32_t) (attr.va_rdev >> 32),
                (uint32_t) (attr.va_rdev & 0xFFFFFFFF));
            break;
        case S_IFBLK:
            chimera_smb_get_reparse_build_device(
                request,
                SMB2_NFS_SPECFILE_BLK,
                (uint32_t) (attr.va_rdev >> 32),
                (uint32_t) (attr.va_rdev & 0xFFFFFFFF));
            break;
        case S_IFIFO:
            chimera_smb_get_reparse_build_simple(request, SMB2_NFS_SPECFILE_FIFO);
            break;
        case S_IFSOCK:
            chimera_smb_get_reparse_build_simple(request, SMB2_NFS_SPECFILE_SOCK);
            break;
        default:
            if ((attr.va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) &&
                (attr.va_dos_attributes & SMB2_FILE_ATTRIBUTE_REPARSE_POINT)) {
                chimera_vfs_get_xattr(request->compound->thread->vfs_thread,
                                      &request->session_handle->session->cred,
                                      request->ioctl.rp_open_file->handle,
                                      CHIMERA_SMB_REPARSE_XATTR,
                                      CHIMERA_SMB_REPARSE_XATTR_LEN,
                                      request->ioctl.rp_response,
                                      sizeof(request->ioctl.rp_response),
                                      chimera_smb_get_reparse_xattr_cb,
                                      request);
                return;
            }
            chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_NOT_A_REPARSE_POINT);
            return;
    } /* switch */

    chimera_smb_get_reparse_finish(request);
} /* chimera_smb_get_reparse_sequence_complete */

void
chimera_smb_ioctl_get_reparse(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file;

    open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);

    if (!open_file) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    request->ioctl.rp_open_file = open_file;

    request->vfs_compound = chimera_vfs_compound_alloc(
        vfs_thread, &request->session_handle->session->cred);

    chimera_vfs_compound_add_puthandle(request->vfs_compound,
                                       open_file->handle,
                                       open_file->open_flags);

    chimera_vfs_compound_add_getattr(request->vfs_compound,
                                     CHIMERA_VFS_ATTR_MODE |
                                     CHIMERA_VFS_ATTR_RDEV | CHIMERA_VFS_ATTR_DOS_ATTRIBUTES);

    chimera_frontend_compound_submit(request->vfs_compound,
                                     chimera_smb_get_reparse_sequence_complete,
                                     request);
} /* chimera_smb_ioctl_get_reparse */

/* GET_REPARSE uses attempt-owned attributes and link data; only wire scratch
 * is modified while execution can still be retried. */
static void
smb_get_reparse_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command               *command = private_data;
    struct chimera_smb_request           *request = command->request;
    const struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op(compound, index);

    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    if (op->type == CHIMERA_VFS_COMPOUND_OP_READLINK) {
        if (op->target_len > sizeof(request->ioctl.rp_target)) {
            *status = CHIMERA_VFS_EIO;
            return;
        }
        memcpy(request->ioctl.rp_target, op->target, op->target_len);
        command->status = smb_get_reparse_symlink(request, op->target_len);
        return;
    }
    switch (op->attr.va_mode & S_IFMT) {
        case S_IFLNK: {
            int next = chimera_vfs_compound_add_readlink(compound);
            if (next >= 0) {
                chimera_vfs_compound_op_set_handle(compound, next, command->handle);
                chimera_vfs_compound_set_op_callbacks(compound, next, NULL, smb_get_reparse_complete, command);
            }
            break;
        }
        case S_IFCHR:
        case S_IFBLK:
            chimera_smb_get_reparse_build_device(request,
                                                 S_ISCHR(op->attr.va_mode) ? SMB2_NFS_SPECFILE_CHR :
                                                 SMB2_NFS_SPECFILE_BLK,
                                                 op->attr.va_rdev >> 32, op->attr.va_rdev & UINT32_MAX);
            break;
        case S_IFIFO: chimera_smb_get_reparse_build_simple(request, SMB2_NFS_SPECFILE_FIFO); break;
        case S_IFSOCK: chimera_smb_get_reparse_build_simple(request, SMB2_NFS_SPECFILE_SOCK); break;
        default: command->status = SMB2_STATUS_NOT_A_REPARSE_POINT; break;
    } /* switch */
} /* smb_get_reparse_complete */

static int
smb_get_reparse_build(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    int op = chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV);

    chimera_vfs_compound_op_set_handle(compound, op, command->handle);
    return op;
} /* smb_get_reparse_build */

static void
smb_get_reparse_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command *command = private_data;

    (void) compound; (void) index; (void) status;
    command->request->ioctl.rp_response_len = 0;
} /* smb_get_reparse_prepare */

static int
smb_get_reparse_eligible(struct chimera_smb_request *request)
{
    (void) request;
    return 1;
} /* smb_get_reparse_eligible */

static struct chimera_smb_file_id
smb_get_reparse_file_id(struct chimera_smb_request *request)
{
    return request->ioctl.file_id;
} /* smb_get_reparse_file_id */

const struct smb_vfs_command_ops chimera_smb_get_reparse_compound_ops = {
    .file_id  = smb_get_reparse_file_id,
    .eligible = smb_get_reparse_eligible,
    .build    = smb_get_reparse_build,
    .prepare  = smb_get_reparse_prepare,
    .complete = smb_get_reparse_complete,
};
