// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>
#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_string.h"
#include "smb_common/smb2.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "common/compound_retry.h"

/* ------------------------------------------------------------------ */
/* SET_REPARSE_POINT guarded identity replacement                     */
/* ------------------------------------------------------------------ */

/* The retained source identity must survive asynchronous parent lookup and
 * must be compared atomically by REMOVE. A stale FileId must never replace a
 * different object subsequently installed at the same name. This legacy chain
 * remains a boundary until open claims and namespace identity can migrate as
 * one accepted compound publication. */
struct smb_set_reparse_ctx {
    struct chimera_smb_request *request;
    struct chimera_vfs_open_handle *source;
    uint8_t unmatched;
    bool gated, fallback, created;
    struct chimera_vfs_cred cred;
    struct chimera_smb_namespace_replace_token replacement;
    struct chimera_vfs_claim_access_fence old_admission, new_admission;
    struct chimera_smb_doc_fence new_doc;
    struct chimera_vfs_claim template;
    struct chimera_vfs_file_state *new_file;
    int remove_op, create_op, handle_op, reserve_op, ready_op;
};

/* Observe existing pins and exclude future pins under the same bucket lock.
 * A refcount check by itself is not a lifetime barrier. Teardown may still
 * cut off this open; the retained resolver reference anchors the gate token. */
static bool
smb_set_reparse_gate(struct smb_set_reparse_ctx *ctx)
{
    struct chimera_smb_open_file *open = ctx->request->ioctl.rp_open_file;
    unsigned bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    pthread_mutex_lock(&open->tree->open_files_lock[bucket]);
    bool available = !(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED) &&
        !open->identity_rebind && open->refcnt == 2;
    if (available) { open->identity_rebind = ctx; ctx->gated = true; }
    pthread_mutex_unlock(&open->tree->open_files_lock[bucket]);
    return available;
}

static void
smb_set_reparse_target_release(struct smb_set_reparse_ctx *ctx)
{
    struct chimera_vfs_state *state = ctx->request->compound->thread->shared->vfs->vfs_state;
    chimera_vfs_claim_access_fence_release(&ctx->new_admission);
    chimera_smb_doc_fence_release(&ctx->new_doc);
    if (ctx->new_file) { chimera_vfs_state_put(state, ctx->new_file); ctx->new_file = NULL; }
}


static void
smb_set_reparse_done(struct smb_set_reparse_ctx *ctx, uint32_t status)
{
    struct chimera_smb_request *request = ctx->request;
    struct chimera_vfs_thread *thread = request->compound->thread->vfs_thread;
    if (request->ioctl.rp_parent_handle) {
        chimera_vfs_release(thread, request->ioctl.rp_parent_handle);
        request->ioctl.rp_parent_handle = NULL;
    }
    smb_set_reparse_target_release(ctx);
    chimera_vfs_claim_access_fence_release(&ctx->old_admission);
    chimera_smb_namespace_replace_end(&ctx->replacement);
    if (ctx->gated) {
        struct chimera_smb_open_file *open = request->ioctl.rp_open_file;
        unsigned bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
        pthread_mutex_lock(&open->tree->open_files_lock[bucket]);
        chimera_smb_abort_if(open->identity_rebind != ctx, "lost SET_REPARSE identity gate");
        open->identity_rebind = NULL;
        pthread_mutex_unlock(&open->tree->open_files_lock[bucket]);
    }
    if (ctx->source) { chimera_vfs_release(thread, ctx->source); }
    chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
    request->ioctl.rp_open_file = NULL;
    free(ctx);
    chimera_smb_complete_request(request, status);
}

static void
chimera_smb_set_reparse_rebind_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static void
chimera_smb_set_reparse_create_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct smb_set_reparse_ctx *ctx       = private_data;
    struct chimera_smb_request *request    = ctx->request;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    (void) set_attr;
    (void) dir_pre_attr;
    (void) dir_post_attr;

    if (error_code != CHIMERA_VFS_OK) {
        smb_set_reparse_done(ctx, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    /* Re-bind the open handle to the new special-file inode (exactly as the
     * symlink path does) so the client's follow-up owner/mode SET_SECURITY
     * lands on the node rather than the removed placeholder.  Without this the
     * device/FIFO/socket keeps the server's default owner (root) and mode
     * (0666) that mknod_at laid down. */
    if (attr && (attr->va_set_mask & CHIMERA_VFS_ATTR_FH) && attr->va_fh_len &&
        attr->va_fh_len <= sizeof(attr->va_fh)) {
        memcpy(request->ioctl.rp_new_fh, attr->va_fh, attr->va_fh_len);
        request->ioctl.rp_new_fh_len = attr->va_fh_len;

        chimera_vfs_open_fh(
            vfs_thread,
            &ctx->cred,
            request->ioctl.rp_new_fh,
            request->ioctl.rp_new_fh_len,
            0,
            chimera_smb_set_reparse_rebind_cb,
            ctx);
        return;
    }

    /* A successful mutation without its new identity cannot complete SET. */
    smb_set_reparse_done(ctx, SMB2_STATUS_INTERNAL_ERROR);
} /* chimera_smb_set_reparse_create_cb */

/* The SET created a new symlink inode, replacing the original object that the
 * client's open still references.  Re-bind the open's VFS handle (open_file->
 * handle) to the new inode so a following GET_REPARSE_POINT -- or any handle op
 * -- resolves the link rather than the now-orphaned original
 * (pike reparse test_set_get_reparse_point). */
static void
chimera_smb_set_reparse_rebind_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct smb_set_reparse_ctx   *ctx        = private_data;
    struct chimera_smb_request   *request    = ctx->request;
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file  = request->ioctl.rp_open_file;

    if (error_code != CHIMERA_VFS_OK || !oh) {
        if (oh) { chimera_vfs_release(vfs_thread, oh); }
        smb_set_reparse_done(ctx, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    struct chimera_smb_tree *tree = open_file->tree;
    unsigned int bucket = open_file->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    struct chimera_vfs_file_state *file = open_file->share_file_state;
    struct chimera_vfs_open_handle *old = NULL;
    pthread_mutex_lock(&tree->open_files_lock[bucket]);
    if (file) { pthread_mutex_lock(&file->lock); }
    if (!(open_file->flags & CHIMERA_SMB_OPEN_FILE_CLOSED) && open_file->handle) {
        /* Arm only the handle we are actually installing. Bucket/file locks
         * protect it against concurrent CLOSE; cache locking nests inside
         * the file lock, as it does in the ordinary DOC paths. */
        if (open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE) {
            chimera_vfs_set_delete_on_close(vfs_thread, oh,
                open_file->parent_fh, open_file->parent_fh_len,
                open_file->name, open_file->name_len,
                &ctx->cred);
        }
        old = open_file->handle;
        open_file->handle = oh;
    }
    if (file) { pthread_mutex_unlock(&file->lock); }
    pthread_mutex_unlock(&tree->open_files_lock[bucket]);
    if (!old) {
        /* Teardown can win while the replacement is being opened. Never
         * resurrect its handle or report a usable FileId after that cutoff. */
        chimera_vfs_release(vfs_thread, oh);
        smb_set_reparse_done(ctx, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    chimera_vfs_release(vfs_thread, old);
    smb_set_reparse_done(ctx, SMB2_STATUS_SUCCESS);
} /* chimera_smb_set_reparse_rebind_cb */

static void
chimera_smb_set_reparse_symlink_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct smb_set_reparse_ctx *ctx       = private_data;
    struct chimera_smb_request *request    = ctx->request;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_error("SET_REPARSE: symlink failed error=%d target='%s' target_len=%d",
                          error_code,
                          request->ioctl.rp_target,
                          request->ioctl.rp_target_len);
        smb_set_reparse_done(ctx, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    /* Re-bind the open handle to the new symlink inode (its file handle was
     * returned in attr->va_fh).  rp_parent_handle and rp_open_file are released
     * in the rebind callback. */
    if (attr && (attr->va_set_mask & CHIMERA_VFS_ATTR_FH) && attr->va_fh_len &&
        attr->va_fh_len <= sizeof(attr->va_fh)) {
        memcpy(request->ioctl.rp_new_fh, attr->va_fh, attr->va_fh_len);
        request->ioctl.rp_new_fh_len = attr->va_fh_len;

        /* Open a real (backend) handle on the new inode -- not an INFERRED/PATH
         * handle -- so the open keeps a valid backend handle for delete-on-close
         * (the close path closes it via chimera_vfs_close). */
        chimera_vfs_open_fh(
            vfs_thread,
            &ctx->cred,
            request->ioctl.rp_new_fh,
            request->ioctl.rp_new_fh_len,
            0,
            chimera_smb_set_reparse_rebind_cb,
            ctx);
        return;
    }

    /* Missing result identity cannot be reported as a usable replacement. */
    smb_set_reparse_done(ctx, SMB2_STATUS_INTERNAL_ERROR);
} /* chimera_smb_set_reparse_symlink_cb */

static void
chimera_smb_set_reparse_remove_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct smb_set_reparse_ctx   *ctx        = private_data;
    struct chimera_smb_request   *request    = ctx->request;
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file  = request->ioctl.rp_open_file;
    struct chimera_vfs_attrs     *set_attr   = &request->ioctl.rp_set_attr;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_error("SET_REPARSE: remove failed error=%d name='%.*s'",
                          error_code, open_file->name_len, open_file->name);
        smb_set_reparse_done(ctx, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    if (ctx->unmatched) {
        smb_set_reparse_done(ctx, SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
        return;
    }

    memset(set_attr, 0, sizeof(*set_attr));

    switch (request->ioctl.rp_nfs_type) {
        case SMB2_NFS_SPECFILE_LNK:
            chimera_vfs_symlink_at(
                vfs_thread,
                &ctx->cred,
                request->ioctl.rp_parent_handle,
                open_file->name,
                open_file->name_len,
                request->ioctl.rp_target,
                request->ioctl.rp_target_len,
                set_attr,
                CHIMERA_VFS_ATTR_FH,
                0,
                0,
                chimera_smb_set_reparse_symlink_cb,
                ctx);
            break;
        case SMB2_NFS_SPECFILE_CHR:
            set_attr->va_mode = S_IFCHR | 0666;
            set_attr->va_rdev = ((uint64_t) request->ioctl.rp_device_major << 32) |
                request->ioctl.rp_device_minor;
            set_attr->va_req_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV;
            set_attr->va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV;
            chimera_vfs_mknod_at(
                vfs_thread,
                &ctx->cred,
                request->ioctl.rp_parent_handle,
                open_file->name,
                open_file->name_len,
                set_attr,
                CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV | CHIMERA_VFS_ATTR_FH,
                0,
                0,
                chimera_smb_set_reparse_create_cb,
                ctx);
            break;
        case SMB2_NFS_SPECFILE_BLK:
            set_attr->va_mode = S_IFBLK | 0666;
            set_attr->va_rdev = ((uint64_t) request->ioctl.rp_device_major << 32) |
                request->ioctl.rp_device_minor;
            set_attr->va_req_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV;
            set_attr->va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV;
            chimera_vfs_mknod_at(
                vfs_thread,
                &ctx->cred,
                request->ioctl.rp_parent_handle,
                open_file->name,
                open_file->name_len,
                set_attr,
                CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV | CHIMERA_VFS_ATTR_FH,
                0,
                0,
                chimera_smb_set_reparse_create_cb,
                ctx);
            break;
        case SMB2_NFS_SPECFILE_FIFO:
            set_attr->va_mode     = S_IFIFO | 0666;
            set_attr->va_req_mask = CHIMERA_VFS_ATTR_MODE;
            set_attr->va_set_mask = CHIMERA_VFS_ATTR_MODE;
            chimera_vfs_mknod_at(
                vfs_thread,
                &ctx->cred,
                request->ioctl.rp_parent_handle,
                open_file->name,
                open_file->name_len,
                set_attr,
                CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_FH,
                0,
                0,
                chimera_smb_set_reparse_create_cb,
                ctx);
            break;
        case SMB2_NFS_SPECFILE_SOCK:
            set_attr->va_mode     = S_IFSOCK | 0666;
            set_attr->va_req_mask = CHIMERA_VFS_ATTR_MODE;
            set_attr->va_set_mask = CHIMERA_VFS_ATTR_MODE;
            chimera_vfs_mknod_at(
                vfs_thread,
                &ctx->cred,
                request->ioctl.rp_parent_handle,
                open_file->name,
                open_file->name_len,
                set_attr,
                CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_FH,
                0,
                0,
                chimera_smb_set_reparse_create_cb,
                ctx);
            break;
        default:
            smb_set_reparse_done(ctx, SMB2_STATUS_NOT_IMPLEMENTED);
            break;
    } /* switch */
} /* chimera_smb_set_reparse_remove_cb */

static void
chimera_smb_set_reparse_open_parent_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct smb_set_reparse_ctx   *ctx        = private_data;
    struct chimera_smb_request   *request    = ctx->request;
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file  = request->ioctl.rp_open_file;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_error("SET_REPARSE: open_parent failed error=%d", error_code);
        if (oh) { chimera_vfs_release(vfs_thread, oh); }
        smb_set_reparse_done(ctx, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    request->ioctl.rp_parent_handle = oh;

    chimera_vfs_remove_at_match_fh_flags(
        vfs_thread, &ctx->cred, oh,
        open_file->name, open_file->name_len,
        ctx->source->fh, ctx->source->fh_len, 0, 0, 0, NULL, NULL,
        &ctx->unmatched, chimera_smb_set_reparse_remove_cb, ctx);
} /* chimera_smb_set_reparse_open_parent_cb */

/* The first native slice deliberately excludes every owner whose state needs
 * its own migration journal. Excluded opens keep the guarded legacy boundary. */
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
}

static void
smb_set_reparse_peer(void *context,
    const struct chimera_smb_namespace_snapshot *snapshot, void *private_data)
{
    unsigned *count = private_data;
    (void) context; (void) snapshot;
    ++*count;
}

static void
smb_set_reparse_reset(struct chimera_vfs_compound *compound, void *private_data)
{
    struct smb_set_reparse_ctx *ctx = private_data;
    (void) compound;
    smb_set_reparse_target_release(ctx);
    ctx->fallback = ctx->created = false;
    memset(&ctx->template, 0, sizeof(ctx->template));
}

static void
smb_set_reparse_regular(struct chimera_vfs_compound *compound, uint32_t index,
    enum chimera_vfs_error *status, void *private_data)
{
    struct smb_set_reparse_ctx *ctx = private_data;
    const struct chimera_vfs_attrs *attr = &chimera_vfs_compound_op(compound, index)->attr;
    if (*status == CHIMERA_VFS_OK && (!(attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) ||
        !S_ISREG(attr->va_mode))) {
        ctx->fallback = true;
        *status = CHIMERA_VFS_ENOTSUP;
    }
}

static void
smb_set_reparse_removed(struct chimera_vfs_compound *compound, uint32_t index,
    enum chimera_vfs_error *status, void *private_data)
{
    (void) private_data;
    if (*status == CHIMERA_VFS_OK && chimera_vfs_compound_op(compound, index)->remove_unmatched)
        *status = CHIMERA_VFS_ENOENT;
}

static void
smb_set_reparse_created(struct chimera_vfs_compound *compound, uint32_t index,
    enum chimera_vfs_error *status, void *private_data)
{
    struct smb_set_reparse_ctx *ctx = private_data;
    ctx->created = *status == CHIMERA_VFS_OK;
    const struct chimera_vfs_attrs *attr = &chimera_vfs_compound_op(compound, index)->attr;
    if (*status == CHIMERA_VFS_OK && (!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH) ||
        !attr->va_fh_len || attr->va_fh_len > CHIMERA_VFS_FH_SIZE))
        *status = CHIMERA_VFS_EIO;
}

static void
smb_set_reparse_admit(struct chimera_vfs_compound *compound, uint32_t index,
    uint64_t token, const uint8_t *fh, uint32_t fh_len, void *private_data)
{
    struct smb_set_reparse_ctx *ctx = private_data;
    struct chimera_server_smb_thread *thread = ctx->request->compound->thread;
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
    pthread_mutex_lock(&ctx->new_file->lock);
    bool busy = chimera_smb_namespace_identity_present_locked(registry, fh, fh_len) ||
        ctx->new_file->delete_pending || ctx->new_file->smb_pending_delete ||
        ctx->new_file->smb_delete_started || ctx->new_file->stream_holders ||
        ctx->new_file->claims[CHIMERA_CLAIM_CLASS_CACHE] ||
        ctx->new_file->claims[CHIMERA_CLAIM_CLASS_RANGE];
    for (struct chimera_vfs_claim *claim = ctx->new_file->claims[CHIMERA_CLAIM_CLASS_ACCESS];
         claim; claim = claim->next) {
        if (claim != &ctx->new_file->implicit_claim) busy = true;
    }
    pthread_mutex_unlock(&ctx->new_file->lock);
    chimera_smb_namespace_unlock(registry);
    chimera_vfs_compound_coordinate_done(compound, token, busy ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
}

static void
smb_set_reparse_reserve_prepare(struct chimera_vfs_compound *compound, uint32_t index,
    enum chimera_vfs_error *status, void *private_data)
{
    struct smb_set_reparse_ctx *ctx = private_data;
    struct chimera_smb_open_file *open = ctx->request->ioctl.rp_open_file;
    struct chimera_vfs_claim *old = chimera_smb_share_claim(open);
    (void) index; (void) status;
    chimera_vfs_claim_init_smb_open(&ctx->template, old->used, old->denied, &old->owner);
    ctx->template.op_handle = chimera_vfs_compound_op(compound, ctx->handle_op)->out_handle;
    ctx->template.policy_tag = old->policy_tag;
    /* A tentative replacement cannot be found through ACCESS backreferences. */
    ctx->template.cb_private = NULL;
}

static void
smb_set_reparse_native_done(struct chimera_vfs_compound *compound, void *private_data)
{
    struct smb_set_reparse_ctx *ctx = private_data;
    struct chimera_smb_request *request = ctx->request;
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_smb_open_file *open = request->ioctl.rp_open_file;
    struct chimera_smb_namespace_registry *registry = &thread->shared->namespace_registry;
    struct chimera_vfs_open_handle *old_handle = NULL;
    struct chimera_vfs_claim_access_owner *old_owner = NULL;
    struct chimera_vfs_file_state *old_file = NULL;
    enum chimera_vfs_error status = chimera_vfs_compound_status(compound);
    bool accepted = chimera_vfs_compound_finish_status(compound) == CHIMERA_VFS_OK;
    bool canceled = chimera_vfs_compound_is_canceled(compound) || request->standalone_cancel_requested;
    bool ready = accepted && chimera_vfs_compound_op(compound, ctx->ready_op)->completed &&
        chimera_vfs_compound_op(compound, ctx->ready_op)->status == CHIMERA_VFS_OK;
    uint32_t reply = status == CHIMERA_VFS_OK ? SMB2_STATUS_SUCCESS :
        status == CHIMERA_VFS_ENOENT ? SMB2_STATUS_OBJECT_NAME_NOT_FOUND :
        status == CHIMERA_VFS_EINTR ? SMB2_STATUS_CANCELLED : SMB2_STATUS_INTERNAL_ERROR;
    chimera_smb_standalone_untrack(request);
    if (ready) {
        /* The irreversible suffix completed despite a late cancellation. */
        reply = SMB2_STATUS_SUCCESS;
        unsigned bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
        chimera_smb_namespace_lock(registry);
        pthread_mutex_lock(&open->tree->open_files_lock[bucket]);
        if (!(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED)) {
            old_file = open->share_file_state;
            pthread_mutex_lock(&old_file->lock);
            pthread_mutex_lock(&ctx->new_file->lock);
            struct chimera_vfs_open_handle *handle = chimera_vfs_compound_take_handle(compound, ctx->handle_op);
            struct chimera_vfs_file_state *new_file = NULL;
            struct chimera_vfs_claim_access_owner *owner =
                chimera_vfs_compound_take_access_owner(compound, ctx->reserve_op, &new_file);
            chimera_smb_abort_if(!handle || !owner || new_file != ctx->new_file,
                "SET_REPARSE accepted identity missing");
            bool rebound = chimera_smb_namespace_rebind_locked(open->namespace_participant,
                ctx->source->fh, ctx->source->fh_len, handle->fh, handle->fh_len);
            chimera_smb_abort_if(!rebound, "SET_REPARSE fenced namespace identity changed");
            old_handle = open->handle;
            old_owner = open->access_owner;
            open->handle = handle;
            open->access_owner = owner;
            open->share_file_state = new_file;
            open->share_lease_inserted = true;
            chimera_smb_share_claim(open)->cb_private = open;
            pthread_mutex_unlock(&ctx->new_file->lock);
            pthread_mutex_unlock(&old_file->lock);
        } else { reply = SMB2_STATUS_FILE_CLOSED; }
        pthread_mutex_unlock(&open->tree->open_files_lock[bucket]);
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
    if (old_owner) chimera_vfs_claim_access_owner_put(old_owner);
    if (old_file) chimera_vfs_state_put(thread->shared->vfs->vfs_state, old_file);
    if (old_handle) chimera_vfs_release(thread->vfs_thread, old_handle);
    if (accepted && ctx->fallback && !canceled) {
        unsigned bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
        pthread_mutex_lock(&open->tree->open_files_lock[bucket]);
        bool closed = !!(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED);
        pthread_mutex_unlock(&open->tree->open_files_lock[bucket]);
        if (closed) { smb_set_reparse_done(ctx, SMB2_STATUS_FILE_CLOSED); return; }
        smb_set_reparse_target_release(ctx);
        chimera_vfs_claim_access_fence_release(&ctx->old_admission);
        chimera_smb_namespace_replace_end(&ctx->replacement);
        chimera_vfs_open_fh(thread->vfs_thread, &ctx->cred, open->parent_fh, open->parent_fh_len,
            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH, chimera_smb_set_reparse_open_parent_cb, ctx);
        return;
    }
    if (ctx->fallback && canceled) reply = SMB2_STATUS_CANCELLED;
    smb_set_reparse_done(ctx, reply);
}

/* Return false only before submitting or changing any filesystem state. */
static bool
smb_set_reparse_native_start(struct smb_set_reparse_ctx *ctx)
{
    struct chimera_smb_request *request = ctx->request;
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_smb_open_file *open = request->ioctl.rp_open_file;
    struct chimera_smb_namespace_registry *registry = &thread->shared->namespace_registry;
    if (!smb_set_reparse_native_eligible(open)) return false;
    if (!chimera_smb_namespace_replace_begin(&ctx->replacement, registry, request->compound) ||
        !chimera_vfs_claim_access_fence_acquire(&ctx->old_admission, open->share_file_state, ctx)) {
        smb_set_reparse_done(ctx, SMB2_STATUS_FILE_NOT_AVAILABLE);
        return true;
    }
    unsigned peers = 0;
    chimera_smb_namespace_lock(registry);
    chimera_smb_namespace_foreach_locked(registry, ctx->source->fh, ctx->source->fh_len,
        smb_set_reparse_peer, &peers);
    pthread_mutex_lock(&open->share_file_state->lock);
    struct chimera_vfs_file_state *file = open->share_file_state;
    struct chimera_vfs_claim *claim = chimera_smb_share_claim(open);
    bool occupied = peers != 1 || file->delete_pending || file->smb_pending_delete ||
        file->smb_delete_started || file->stream_holders || file->pending_head ||
        file->claims[CHIMERA_CLAIM_CLASS_CACHE] || file->claims[CHIMERA_CLAIM_CLASS_RANGE];
    for (struct chimera_vfs_claim *peer = file->claims[CHIMERA_CLAIM_CLASS_ACCESS]; peer; peer = peer->next) {
        /* Cached VFS I/O is not another published protocol identity. */
        if (peer != claim && peer != &file->implicit_claim) occupied = true;
    }
    pthread_mutex_unlock(&file->lock);
    chimera_smb_namespace_unlock(registry);
    if (occupied) {
        chimera_vfs_claim_access_fence_release(&ctx->old_admission);
        chimera_smb_namespace_replace_end(&ctx->replacement);
        return false;
    }
    struct chimera_vfs_compound *compound = chimera_vfs_compound_alloc(thread->vfs_thread, &ctx->cred);
    if (!compound) { smb_set_reparse_done(ctx, SMB2_STATUS_INSUFFICIENT_RESOURCES); return true; }
    chimera_vfs_compound_set_admission_cookie(compound, ctx);
    chimera_vfs_compound_set_attempt_reset(compound, smb_set_reparse_reset, ctx);
    int op = chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_MODE);
    if (op < 0) goto build_error;
    chimera_vfs_compound_op_set_handle(compound, op, ctx->source);
    chimera_vfs_compound_set_op_callbacks(compound, op, NULL, smb_set_reparse_regular, ctx);
    if (chimera_vfs_compound_add_putfh(compound, open->parent_fh, open->parent_fh_len) < 0) goto build_error;
    ctx->remove_op = chimera_vfs_compound_add_remove(compound, open->name, open->name_len,
        CHIMERA_VFS_REMOVE_ISNOTDIR | CHIMERA_VFS_REMOVE_NO_NOTIFY);
    if (ctx->remove_op < 0) goto build_error;
    struct chimera_vfs_compound_op *remove = chimera_vfs_compound_op_args(compound, ctx->remove_op);
    remove->remove_match_child_fh = true;
    memcpy(remove->arg_fh, ctx->source->fh, ctx->source->fh_len);
    remove->arg_fh_len = ctx->source->fh_len;
    chimera_vfs_compound_set_op_callbacks(compound, ctx->remove_op, NULL, smb_set_reparse_removed, ctx);
    struct chimera_vfs_attrs attr = { 0 };
    uint8_t type = CHIMERA_VFS_COMPOUND_CREATE_SYMLINK;
    if (request->ioctl.rp_nfs_type != SMB2_NFS_SPECFILE_LNK) {
        type = CHIMERA_VFS_COMPOUND_CREATE_NODE;
        attr.va_mode = 0666 | (request->ioctl.rp_nfs_type == SMB2_NFS_SPECFILE_CHR ? S_IFCHR :
            request->ioctl.rp_nfs_type == SMB2_NFS_SPECFILE_BLK ? S_IFBLK :
            request->ioctl.rp_nfs_type == SMB2_NFS_SPECFILE_FIFO ? S_IFIFO : S_IFSOCK);
        attr.va_req_mask = attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        if (S_ISCHR(attr.va_mode) || S_ISBLK(attr.va_mode)) {
            attr.va_rdev = ((uint64_t) request->ioctl.rp_device_major << 32) | request->ioctl.rp_device_minor;
            attr.va_req_mask |= CHIMERA_VFS_ATTR_RDEV;
            attr.va_set_mask |= CHIMERA_VFS_ATTR_RDEV;
        }
    }
    ctx->create_op = chimera_vfs_compound_add_create(compound, type, open->name, open->name_len,
        request->ioctl.rp_target, request->ioctl.rp_target_len, &attr, CHIMERA_VFS_ATTR_FH);
    if (ctx->create_op < 0) goto build_error;
    chimera_vfs_compound_op_args(compound, ctx->create_op)->namespace_flags =
        type == CHIMERA_VFS_COMPOUND_CREATE_SYMLINK ? CHIMERA_VFS_SYMLINK_NO_NOTIFY : CHIMERA_VFS_MKNOD_NO_NOTIFY;
    chimera_vfs_compound_set_op_callbacks(compound, ctx->create_op, NULL, smb_set_reparse_created, ctx);
    if (chimera_vfs_compound_add_open_current(compound, 0, 0) < 0) goto build_error;
    ctx->handle_op = chimera_vfs_compound_add_gethandle(compound);
    if (ctx->handle_op < 0) goto build_error;
    op = chimera_vfs_compound_add_coordinate(compound, smb_set_reparse_admit, ctx);
    if (op < 0) goto build_error;
    chimera_vfs_compound_op_args(compound, op)->coordinate_each_attempt = 1;
    ctx->reserve_op = chimera_vfs_compound_add_reserve_access(compound, ctx->handle_op, &ctx->template);
    if (ctx->reserve_op < 0) goto build_error;
    chimera_vfs_compound_set_op_prepare(compound, ctx->reserve_op, smb_set_reparse_reserve_prepare, ctx);
    if (chimera_vfs_compound_add_retire_access(compound, open->access_owner) < 0) goto build_error;
    ctx->ready_op = chimera_vfs_compound_add_checkpoint(compound);
    if (ctx->ready_op < 0) goto build_error;
    struct chimera_vfs_compound_group_config group = { .first_op = 0,
        .num_ops = chimera_vfs_compound_num_ops(compound), .dependency = -1 };
    /* This standalone group owns the new reservation until accepted transfer.
     * A failed suffix leaves it untransferred for compound_free to retire.
     * Do not use reserve_access_until here: the group records EINTR for a late
     * cancel even when the mandatory suffix reached readiness, and that API
     * would withdraw the new owner after the old retirement was staged. */
    bool valid = chimera_vfs_compound_add_group(compound, &group) >= 0 &&
        chimera_vfs_compound_set_cancel_scope(compound, ctx->remove_op, ctx->ready_op);
    if (!valid) goto build_error;
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
}

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
    }

    /* VFS mknod requires a privileged caller for device nodes. SET replaces
     * an existing placeholder: enforce this known failure before unlinking it,
     * on both the typed and legacy routes. FIFO/socket creation has no such
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
    ctx->request = request;
    ctx->cred = request->session_handle->session->cred;
    request->ioctl.rp_open_file = open_file;
    request->ioctl.rp_parent_handle = NULL;
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

    if (smb_set_reparse_native_start(ctx)) return;
    chimera_vfs_open_fh(
        vfs_thread,
        &ctx->cred,
        open_file->parent_fh,
        open_file->parent_fh_len,
        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
        chimera_smb_set_reparse_open_parent_cb,
        ctx);
} /* chimera_smb_ioctl_set_reparse */

/* ------------------------------------------------------------------ */
/* GET_REPARSE_POINT async chain                                      */
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
    enum chimera_vfs_error    error_code,
    int                       target_length,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    (void) attr;
    uint32_t                    status = error_code == CHIMERA_VFS_OK ?
        smb_get_reparse_symlink(request, target_length) : SMB2_STATUS_INTERNAL_ERROR;
    chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_get_reparse_readlink_cb */

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

static void
chimera_smb_get_reparse_getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    switch (attr->va_mode & S_IFMT) {
        case S_IFLNK:
            chimera_vfs_readlink(
                vfs_thread,
                &request->session_handle->session->cred,
                request->ioctl.rp_open_file->handle,
                request->ioctl.rp_target,
                CHIMERA_VFS_PATH_MAX,
                0,
                chimera_smb_get_reparse_readlink_cb,
                request);
            return;
        case S_IFCHR:
            chimera_smb_get_reparse_build_device(
                request,
                SMB2_NFS_SPECFILE_CHR,
                (uint32_t) (attr->va_rdev >> 32),
                (uint32_t) (attr->va_rdev & 0xFFFFFFFF));
            break;
        case S_IFBLK:
            chimera_smb_get_reparse_build_device(
                request,
                SMB2_NFS_SPECFILE_BLK,
                (uint32_t) (attr->va_rdev >> 32),
                (uint32_t) (attr->va_rdev & 0xFFFFFFFF));
            break;
        case S_IFIFO:
            chimera_smb_get_reparse_build_simple(request, SMB2_NFS_SPECFILE_FIFO);
            break;
        case S_IFSOCK:
            chimera_smb_get_reparse_build_simple(request, SMB2_NFS_SPECFILE_SOCK);
            break;
        default:
            chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_NOT_A_REPARSE_POINT);
            return;
    } /* switch */

    chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_get_reparse_getattr_cb */

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

    chimera_vfs_getattr(
        vfs_thread,
        &request->session_handle->session->cred,
        open_file->handle,
        CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV,
        chimera_smb_get_reparse_getattr_cb,
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
