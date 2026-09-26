// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>

#include "smb_internal.h"
#include "vfs/vfs_internal_procs.h"
#include "smb_procs.h"
#include "smb_doc_stream.h"
#include "common/misc.h"
#include "common/compound_retry.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_claim.h"
#include "vfs/vfs_claim_access.h"

/*
 * SET_INFO FileRenameInformation, as the consecutive sequences it is.
 *
 * A rename is not one sequence because three of its four steps ask a question
 * whose ANSWER decides whether the next step exists at all, and two of those
 * fan out by a count nothing knows before the first answer:
 *
 *   1. resolve the destination parent and probe the destination name
 *      (PUTFH, [LOOKUP_PATH], LOOKUP)
 *   2. for a DIRECTORY rename, enumerate the source's children that hold a
 *      live share reservation (PUTFH, READDIR page) and RECALL each one's
 *      caching lease (RECALL per child), then re-scan for one opened during
 *      the break wave
 *   3. break the destination parent's directory lease with a deny probe
 *      (PUTFH, OPEN_CURRENT, CLAIM)
 *   4. the rename itself (PUTFH src parent, SAVEFH, PUTFH dst parent, RENAME)
 *
 * The per-child recall count is the directory's, so step 2 cannot be folded
 * into one run; the deny probe has to be RELEASED whichever way it resolves,
 * which is out of band and so cannot sit inside the run that renames.  The
 * post-rename repath of every open of the file, and the sharemode re-key, stay
 * out of band for the ordinary reason: they mutate nothing the run holds.
 */

/* Legacy callbacks share the compound namespace publication lock and update
 * only the namespace link that moved, preserving other hardlink aliases. */
struct chimera_smb_rename_path {
    struct chimera_smb_namespace_path before, after;
    struct chimera_vfs_thread        *thread;
    struct chimera_vfs_file_state    *locked_file;
};

static bool
chimera_smb_set_info_rename_same_link(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file   *open_file = request->set_info.open_file;
    struct chimera_smb_rename_info *info      = &request->set_info.rename_info;
    struct chimera_vfs_open_handle *parent    = info->new_parent_handle ?
        info->new_parent_handle : request->set_info.parent_handle;

    return parent && parent->fh_len == open_file->parent_fh_len &&
           memcmp(parent->fh, open_file->parent_fh, parent->fh_len) == 0 &&
           info->new_name_len == open_file->name_len &&
           memcmp(info->new_name, open_file->name, open_file->name_len) == 0;
} /* chimera_smb_set_info_rename_same_link */

static void
chimera_smb_rename_repath(
    void *private_open,
    void *private_data)
{
    struct chimera_smb_open_file            *open   = private_open;
    const struct chimera_smb_rename_path    *change = private_data;
    const struct chimera_smb_namespace_path *before = &change->before, *after = &change->after;

    struct chimera_vfs_file_state           *peer_file = open->share_file_state;
    bool                                     lock_peer = peer_file && peer_file != change->locked_file;

    if (lock_peer) {
        evpl_mutex_lock(&peer_file->lock);
    }
    if (open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM) {
        chimera_smb_stream_doc_repath_locked(peer_file, change->locked_file, before, after);
    }
    if (open->parent_fh_len != before->parent_fh_len || open->name_len != before->name_len ||
        memcmp(open->parent_fh, before->parent_fh, before->parent_fh_len) ||
        memcmp(open->name, before->name, before->name_len)) {
        if (lock_peer) {
            evpl_mutex_unlock(&peer_file->lock);
        }
        return;
    }
    struct chimera_smb_namespace_path path = { 0 };
    path.view_fh_len = open->tree->fh_len;
    memcpy(path.view_fh, open->tree->fh, path.view_fh_len);
    path.parent_fh_len = after->parent_fh_len;
    memcpy(path.parent_fh, after->parent_fh, path.parent_fh_len);
    path.name_len = after->name_len;
    memcpy(path.name, after->name, path.name_len);
    bool                              same_parent = before->parent_fh_len == after->parent_fh_len &&
        !memcmp(before->parent_fh, after->parent_fh, before->parent_fh_len);
    bool                              same_view = path.view_fh_len == after->view_fh_len &&
        !memcmp(path.view_fh, after->view_fh, path.view_fh_len);
    if (!same_parent && same_view) {
        path.full_path_len = after->full_path_len;
        memcpy(path.full_path, after->full_path, path.full_path_len);
    } else {
        /* Same-parent changes preserve each share's independent path prefix.
         * Cross-parent, cross-share full-path translation remains a legacy
         * boundary; retain its prefix while keeping DOC's parent/name exact. */
        uint32_t prefix = open->full_path_len >= before->name_len ?
            open->full_path_len - before->name_len : 0;
        if (prefix + after->name_len < sizeof(path.full_path)) {
            memcpy(path.full_path, open->full_path, prefix);
            memcpy(path.full_path + prefix, after->name, after->name_len);
            path.full_path_len = prefix + after->name_len;
        } else {
            path.full_path_len = open->full_path_len;
            memcpy(path.full_path, open->full_path, path.full_path_len);
        }
    }
    open->parent_fh_len = path.parent_fh_len;
    memcpy(open->parent_fh, path.parent_fh, path.parent_fh_len);
    open->name_len = path.name_len;
    memcpy(open->name, path.name, path.name_len);
    open->full_path_len = path.full_path_len;
    memcpy(open->full_path, path.full_path, path.full_path_len + 1);
    chimera_smb_namespace_set_path_locked(open->namespace_participant, &path);
    chimera_smb_namespace_set_path_locked(open->base_namespace_participant, &path);
    struct chimera_vfs_open_handle *handle = open->handle;
    struct vfs_open_cache          *cache  = handle ? chimera_vfs_get_cache_for_handle(change->thread, handle) : NULL;
    if (cache) {
        struct vfs_open_cache_shard *shard = &cache->shards[handle->fh_hash & cache->shard_mask];
        evpl_mutex_lock(&shard->lock);
        if (handle->doc_delete_on_close && handle->doc_parent_fh_len == before->parent_fh_len &&
            handle->doc_name_len == before->name_len &&
            !memcmp(handle->doc_parent_fh, before->parent_fh, before->parent_fh_len) &&
            !memcmp(handle->doc_name, before->name, before->name_len)) {
            handle->doc_parent_fh_len = after->parent_fh_len;
            memcpy(handle->doc_parent_fh, after->parent_fh, after->parent_fh_len);
            handle->doc_name_len = after->name_len;
            memcpy(handle->doc_name, after->name, after->name_len);
        }
        evpl_mutex_unlock(&shard->lock);
    }
    if (lock_peer) {
        evpl_mutex_unlock(&peer_file->lock);
    }
} /* chimera_smb_rename_repath */

static void
chimera_smb_rename_repath_participant(
    void                                        *open,
    const struct chimera_smb_namespace_snapshot *snapshot,
    void                                        *private_data)
{
    (void) snapshot;
    chimera_smb_rename_repath(open, private_data);
} /* chimera_smb_rename_repath_participant */


/* Release the transient destination-parent dir-lease conflict probe (if it was
 * inserted) and drop the file-state reference taken for it. */
static void
chimera_smb_set_info_rename_dp_release(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_vfs_state  *vfs_state  = vfs_thread->vfs->vfs_state;

    if (request->set_info.dp_probe_active) {
        chimera_vfs_claim_release(vfs_state, request->set_info.dp_file_state,
                                  &request->set_info.dp_probe);
        request->set_info.dp_probe_active = 0;
    }
    if (request->set_info.dp_file_state) {
        chimera_vfs_state_put(vfs_state, request->set_info.dp_file_state);
        request->set_info.dp_file_state = NULL;
    }
} /* chimera_smb_set_info_rename_dp_release */


/* PUTFH(src parent), SAVEFH, PUTFH(dst parent), RENAME.
 *
 * Issued once the destination parent's directory lease (if any) has yielded its
 * HANDLE caching.  RENAME reads the SAVED file handle for the source directory
 * and the CURRENT one for the target, which is what rename_at takes, so neither
 * directory is opened. */
struct chimera_vfs_attrs;
static void
chimera_smb_set_info_rename_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data);

static void
smb_rename_mutation_done(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;
    const struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op(compound, 3);

    request->set_info.rename_info.outcome = op->rename_outcome;
    chimera_smb_set_info_rename_callback(chimera_vfs_compound_status(compound),
                                         NULL, NULL, NULL, NULL, request);
    chimera_vfs_compound_free(compound);
} /* smb_rename_mutation_done */

static void
chimera_smb_set_info_rename_emit(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file   *open     = request->set_info.open_file;
    struct chimera_smb_rename_info *rename   = &request->set_info.rename_info;
    struct chimera_vfs_compound    *compound = chimera_vfs_compound_alloc(
        request->compound->thread->vfs_thread, &request->session_handle->session->cred);

    chimera_vfs_compound_add_putfh(compound, open->parent_fh, open->parent_fh_len);
    chimera_vfs_compound_add_savefh(compound);
    chimera_vfs_compound_add_putfh(compound, request->set_info.dst_parent_fh,
                                   request->set_info.dst_parent_fh_len);
    int                             index = chimera_vfs_compound_add_rename(compound, open->name, open->name_len,
                                                                            rename->new_name, rename->new_name_len,
                                                                            (open->flags &
                                                                             CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) ?
                                                                            CHIMERA_VFS_RENAME_SRC_IS_DIR : 0, 0, 0);
    chimera_vfs_compound_op_set_rename_opts(compound, index, NULL, 0,
                                            open->handle, open->parent_lease_key, 0);
    if (index >= 0) {
        struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(compound, index);
        op->io_owner.owner     = chimera_smb_open_actor_owner(open);
        op->io_owner.op_handle = open->handle;
        op->have_io_owner      = true;
    }
    chimera_vfs_compound_submit(compound, smb_rename_mutation_done, request);
} /* chimera_smb_set_info_rename_emit */

/*
 * PUTFH(dst parent), OPEN_CURRENT, CLAIM(deny probe, WAIT).
 *
 * Its own run, not an op ahead of the rename: the probe has to be RELEASED
 * however it resolves -- it was only ever a way to fire the break and read the
 * answer -- and a release is out of band by the rule, so it cannot sit behind
 * an op in the same sequence.
 *
 * GRANTED: no conflicting handle-leased opener remains (none, or it closed in
 * response to the RH->R break) -> proceed with the rename.  DENIED: a holder
 * kept its handle open -> SHARING_VIOLATION (MS-SMB2 dirlease.rename_dst_parent).
 */
static void
chimera_smb_set_info_rename_dp_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    enum chimera_vfs_claim_result         result;

    status = chimera_vfs_compound_status(compound);
    op     = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
    result = op->claim_result;

    /* The claim is the caller's once the run finished OK, and what releasing it
     * needs is the file state the op resolved. */
    request->set_info.dp_file_state =
        chimera_vfs_compound_take_file_state(
            compound, chimera_vfs_compound_num_ops(compound) - 1);

    request->set_info.dp_probe_active =
        (request->set_info.dp_file_state != NULL);

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (status == CHIMERA_VFS_OK && result == CHIMERA_CLAIM_GRANTED) {
        /* The acquire inserted the probe; drop it (its only purpose was to
         * break the dir lease / detect the conflict) and rename. */
        chimera_smb_set_info_rename_dp_release(request);
        chimera_smb_set_info_rename_emit(request);
        return;
    }

    chimera_smb_set_info_rename_dp_release(request);

    chimera_smb_open_file_release(request, request->set_info.open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SHARING_VIOLATION);
} /* chimera_smb_set_info_rename_dp_complete */

static void
chimera_smb_set_info_rename_do_rename(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file     *open_file  = request->set_info.open_file;
    struct chimera_server_smb_thread *thread     = request->compound->thread;
    struct chimera_vfs_thread        *vfs_thread = thread->vfs_thread;
    struct chimera_vfs_state         *vfs_state  = vfs_thread->vfs->vfs_state;
    struct chimera_vfs_file_state    *fs;
    struct chimera_claim_owner        dp_owner;

    request->set_info.dp_probe_active = 0;
    request->set_info.dp_file_state   = NULL;
    if (chimera_smb_set_info_rename_same_link(request)) {
        request->set_info.rename_info.outcome = CHIMERA_VFS_RENAME_OUTCOME_NOOP;
        chimera_smb_set_info_rename_callback(CHIMERA_VFS_OK, NULL, NULL, NULL, NULL, request);
        return;
    }


    /* A rename INTO a directory must break that directory's lease HANDLE caching
    * (RH->R): a conflicting handle-leased opener (one holding the dst parent
    * open with DELETE access) may close in response and free the rename, else
    * the rename fails SHARING_VIOLATION (MS-SMB2; dirlease.rename_dst_parent).
    * Model it as a transient deny-only probe (deny=D) on the dst parent: it
    * conflicts ONLY with a DELETE-access holder, so it is inert for ordinary
    * renames into a leased directory (dirlease.rename holders take no DELETE
    * access).  No dst-parent state => no lease => rename directly.
    *
    * The look-up is a synchronous question answered from memory, and it is
    * asked here rather than by the run so that a destination parent nothing
    * has a state for costs neither a sequence nor a state allocation -- which
    * an unconditional CLAIM would make (it resolves the state, creating one). */
    fs = chimera_vfs_state_get(vfs_state,
                               request->set_info.dst_parent_fh,
                               request->set_info.dst_parent_fh_len,
                               chimera_vfs_hash(request->set_info.dst_parent_fh,
                                                request->set_info.dst_parent_fh_len),
                               false);

    if (!fs) {
        chimera_smb_set_info_rename_emit(request);
        return;
    }

    /* The run resolves the state for itself; this one was only the question. */
    chimera_vfs_state_put(vfs_state, fs);

    /* Self-exempt the directory lease named by the operating open's
     * ParentLeaseKey: a rename issued under the dst parent's own lease must not
     * break (and then deny against) that lease.  The 16-byte key rides in
     * owner.key (the KEY circle replaces the old break-skip fields; an all-zero
     * key means no exemption -- every dir lease breaks). */
    dp_owner = chimera_smb_open_actor_owner(open_file);
    memcpy(dp_owner.key, open_file->parent_lease_key, 16);

    chimera_vfs_claim_init_deny_probe(&request->set_info.dp_probe,
                                      CHIMERA_CLAIM_D, &dp_owner);
    request->set_info.dp_probe.policy_tag = open_file->file_id.pid;

    request->vfs_compound = chimera_vfs_compound_alloc(
        vfs_thread, &request->session_handle->session->cred);

    chimera_vfs_compound_add_putfh(request->vfs_compound,
                                   request->set_info.dst_parent_fh,
                                   request->set_info.dst_parent_fh_len);

    /* A CLAIM takes its claim against the current OPEN, and wants a PATH one:
     * the probe uses only the file it refers to. */
    chimera_vfs_compound_add_open_current(request->vfs_compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY, 0);

    chimera_vfs_compound_add_claim(request->vfs_compound,
                                   &request->set_info.dp_probe,
                                   &request->set_info.dp_ticket,
                                   CHIMERA_VFS_COMPOUND_CLAIM_WAIT |
                                   CHIMERA_VFS_COMPOUND_CLAIM_OPTIONAL,
                                   0, 0, 0, 0);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_set_info_rename_dp_complete,
                                request);

    /* If the probe parked on a dir-lease break, that break targets the dst
     * parent's holder on THIS connection (the rename's own conn is mid-compound,
     * so the break was deferred for reply-before-break ordering).  The rename
     * will not reply until the break resolves, so flush the deferred break now or
     * the holder never sees it and the rename deadlocks (it never gets the chance
     * to close / ack).  Harmless if the probe resolved synchronously. */
    chimera_smb_lease_break_flush(thread);
} /* chimera_smb_set_info_rename_do_rename */

/* ---- Directory-rename contained-open recall (smb2.lease.rename_dir_openfile) ----
 *
 * Renaming a directory breaks the HANDLE lease of every file open inside it.
 * Each contained holder gets an RH->R break; a well-behaved holder closes (and
 * frees the rename), a holder that keeps the file open makes the rename fail
 * ACCESS_DENIED.  Only a directory rename enters this path -- file renames are
 * untouched. */

static void chimera_smb_set_info_rename_recall_next(
    struct chimera_smb_request *request);

static void chimera_smb_set_info_rename_recall_scan(
    struct chimera_smb_request *request);

static void
chimera_smb_set_info_rename_recall_free(struct chimera_smb_request *request)
{
    if (request->set_info.recall_children) {
        free(request->set_info.recall_children);
        request->set_info.recall_children = NULL;
    }
    request->set_info.recall_child_count = 0;
    request->set_info.recall_child_cap   = 0;
    request->set_info.recall_child_idx   = 0;
} /* chimera_smb_set_info_rename_recall_free */

/* Abandon the rename with ACCESS_DENIED: a contained holder kept its file open
 * across the handle-lease break (or a new open raced the rename). */
static void
chimera_smb_set_info_rename_recall_fail(
    struct chimera_smb_request *request,
    uint32_t                    status)
{
    struct chimera_vfs_thread *vfs_thread = request->compound->thread->vfs_thread;

    chimera_smb_set_info_rename_recall_free(request);

    if (request->set_info.rename_info.new_parent_handle) {
        chimera_vfs_release(vfs_thread,
                            request->set_info.rename_info.new_parent_handle);
        request->set_info.rename_info.new_parent_handle = NULL;
    }
    if (request->set_info.parent_handle) {
        chimera_vfs_release(vfs_thread, request->set_info.parent_handle);
        request->set_info.parent_handle = NULL;
    }
    chimera_smb_open_file_release(request, request->set_info.open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_set_info_rename_recall_fail */

static void
chimera_smb_set_info_rename_recall_deny(struct chimera_smb_request *request)
{
    chimera_smb_set_info_rename_recall_fail(request, SMB2_STATUS_ACCESS_DENIED);
} /* chimera_smb_set_info_rename_recall_deny */

/* Per-child RECALL completion.  A holder that kept the file open
 * (recall_still_open) -- because it acked the handle-lease break without
 * closing, or never held a lease to break -- denies the rename IMMEDIATELY:
 * matching Windows, the scan stops at the first non-releasing open and does not
 * break any further contained holders.  Otherwise advance to the next child. */
static void
chimera_smb_set_info_rename_recall_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    int                                   still_open;

    status = chimera_vfs_compound_status(compound);
    op     = chimera_vfs_compound_op(compound, 0);

    still_open = op->recall_still_open;

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (status != CHIMERA_VFS_OK || still_open) {
        chimera_smb_set_info_rename_recall_deny(request);
        return;
    }

    request->set_info.recall_child_idx++;
    chimera_smb_set_info_rename_recall_next(request);
} /* chimera_smb_set_info_rename_recall_complete */

/*
 * RECALL(child fh, retain CR|CW) -- one sequence per child, because the number
 * of children is the directory's and nothing knows it before the scan.
 *
 * The op carries the child's file handle rather than addressing a cursor, which
 * is the shape that spares nothing and breaks every holder: a directory rename
 * invalidates a contained open's cached handle, not its data, so the retain
 * floor is R|W (a single RWH->RW break, not a cascade that would strip the
 * write cache first).  The op PARKS until the recall drains.
 */
static void
chimera_smb_set_info_rename_recall_next(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_smb_rename_recall *rc;

    if (request->set_info.recall_child_idx >= request->set_info.recall_child_count) {
        if (!request->set_info.recall_final) {
            /* First wave drained with every holder releasing; re-scan for a
             * child that was opened while the breaks were in flight. */
            request->set_info.recall_final = 1;
            chimera_smb_set_info_rename_recall_scan(request);
            return;
        }
        chimera_smb_set_info_rename_recall_free(request);
        chimera_smb_set_info_rename_do_rename(request);
        return;
    }

    rc = &request->set_info.recall_children[request->set_info.recall_child_idx];

    request->vfs_compound = chimera_vfs_compound_alloc(
        thread->vfs_thread, &request->session_handle->session->cred);

    chimera_vfs_compound_add_recall(request->vfs_compound, rc->fh, rc->fh_len,
                                    CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW, 0);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_set_info_rename_recall_complete,
                                request);

    /* The contained holder is on another connection; if its break was deferred
     * for reply-before-break ordering, flush it now so it actually fires while
     * the rename is parked on the recall.  (Harmless if the recall already
     * completed inline -- it operates on the thread, not the request.) */
    chimera_smb_lease_break_flush(thread);
} /* chimera_smb_set_info_rename_recall_next */

/* The streaming READDIR's reset: wind the collection back to exactly what it
 * held when this page was built, so re-executing the page is a first run from
 * the caller's side.  It is what makes the append below reversible, which is
 * the contract on a streaming enumeration. */

/* Each accepted directory page has a private checkpoint. A rejected finish
 * rewinds only that page; earlier accepted pages remain available to the recall
 * driver. No entry/prepare callback sends breaks, replies or namespace writes. */
struct smb_rename_scan {
    struct chimera_smb_request     *request;
    struct chimera_vfs_open_handle *handle;
    uint32_t                        start_count;
    int                             start_deny;
    int                             readdir;
};

static void
smb_rename_scan_reset(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *arg)
{
    struct smb_rename_scan *scan = arg;

    (void) compound; (void) index;
    scan->request->set_info.recall_child_count = scan->start_count;
    scan->request->set_info.recall_deny        = scan->start_deny;
} /* smb_rename_scan_reset */

/* readdir callback: collect each child that currently has a live share holder
 * (a real protocol open) so its caching lease can be recalled before the
 * directory rename. */

/*
 * PUTHANDLE(source directory), READDIR(streaming) -- one sequence per page.
 *
 * A streaming READDIR reads ONE page and reports where it stopped, so a
 * directory larger than a page is walked by consecutive sequences, as it was by
 * consecutive readdirs before.  It stages nothing of the compound's: the
 * collection is the request's array, and `reset` winds it back before every
 * execution.
 */
static void smb_rename_scan_page(
    struct chimera_smb_request *request,
    uint64_t                    cookie,
    uint64_t                    verifier);

static void
smb_rename_scan_done(
    struct chimera_vfs_compound *compound,
    void                        *arg)
{
    struct smb_rename_scan               *scan    = arg;
    struct chimera_smb_request           *request = scan->request;
    enum chimera_vfs_error                finish  = chimera_vfs_compound_finish_status(compound);
    enum chimera_vfs_error                error   = chimera_vfs_compound_status(compound);
    const struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op(compound, scan->readdir);
    uint64_t                              cookie = op->r_cookie, verifier = op->r_verifier;
    bool                                  eof  = op->eof;
    bool                                  deny = request->set_info.recall_deny;

    chimera_vfs_compound_free(compound);
    chimera_vfs_release(request->compound->thread->vfs_thread, scan->handle);
    free(scan);

    /* Rejected/canceled enumeration must never start recalls or rename. The
     * adapter already exhausted any safe retry before reaching this callback. */
    if (finish != CHIMERA_VFS_OK || error == CHIMERA_VFS_EINTR) {
        chimera_smb_set_info_rename_recall_fail(request,
                                                error == CHIMERA_VFS_EINTR || finish == CHIMERA_VFS_EINTR ?
                                                SMB2_STATUS_CANCELLED : SMB2_STATUS_INTERNAL_ERROR);
        return;
    }
    if (!request->set_info.open_file || !request->set_info.open_file->handle ||
        deny || error == CHIMERA_VFS_ENOSPC) {
        chimera_smb_set_info_rename_recall_deny(request);
        return;
    }
    if (error != CHIMERA_VFS_OK) {
        /* Preserve the legacy permission/backend enumeration fallback, after
         * accepted finish only. A private collection error never uses it. */
        chimera_smb_set_info_rename_recall_free(request);
        chimera_smb_set_info_rename_do_rename(request);
        return;
    }
    if (!eof) {
        request->set_info.recall_readdir_cookie = cookie;
        smb_rename_scan_page(request, cookie, verifier);
        return;
    }

    if (request->set_info.recall_final) {
        /* Second pass: any child still (or newly) open denies the rename;
        * otherwise the directory is quiescent and the rename proceeds. */
        if (request->set_info.recall_deny ||
            request->set_info.recall_child_count > 0) {
            chimera_smb_set_info_rename_recall_deny(request);
        } else {
            chimera_smb_set_info_rename_recall_free(request);
            chimera_smb_set_info_rename_do_rename(request);
        }
        return;
    }

    request->set_info.recall_child_idx = 0;
    chimera_smb_set_info_rename_recall_next(request);
} /* smb_rename_scan_done */

struct chimera_vfs_attrs;
struct chimera_vfs_compound;
static int
chimera_smb_set_info_rename_recall_readdir_cb(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg);

static void
smb_rename_scan_page(
    struct chimera_smb_request *request,
    uint64_t                    cookie,
    uint64_t                    verifier)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;

    if (!request->set_info.open_file || !request->set_info.open_file->handle) {
        chimera_smb_set_info_rename_recall_deny(request);
        return;
    }
    struct smb_rename_scan           *scan     = calloc(1, sizeof(*scan));
    struct chimera_vfs_compound      *compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                                            &request->session_handle->session->cred);
    if (!scan || !compound) {
        free(scan);
        if (compound) {
            chimera_vfs_compound_free(compound);
        }
        chimera_smb_set_info_rename_recall_fail(request, SMB2_STATUS_INSUFFICIENT_RESOURCES);
        return;
    }
    scan->request     = request;
    scan->start_count = request->set_info.recall_child_count;
    scan->start_deny  = request->set_info.recall_deny;
    scan->handle      = chimera_smb_retain_vfs_handle(thread->vfs_thread, request->set_info.open_file->handle);
    chimera_vfs_compound_add_puthandle(compound, scan->handle, scan->handle->flags);
    scan->readdir = chimera_vfs_compound_add_readdir_stream(compound, cookie, verifier, CHIMERA_VFS_ATTR_FH, 0, 0, NULL,
                                                            0, smb_rename_scan_reset,
                                                            chimera_smb_set_info_rename_recall_readdir_cb, scan);
    if (scan->readdir < 0) {
        chimera_vfs_compound_free(compound);
        chimera_vfs_release(thread->vfs_thread, scan->handle);
        free(scan);
        chimera_smb_set_info_rename_recall_fail(request, SMB2_STATUS_INSUFFICIENT_RESOURCES);
        return;
    }
    chimera_frontend_compound_submit(compound, smb_rename_scan_done, scan);
} /* smb_rename_scan_page */

/* The first and final scans use the same read-only paging machinery. */
static void
chimera_smb_set_info_rename_recall_scan(struct chimera_smb_request *request)
{
    chimera_smb_set_info_rename_recall_free(request);
    request->set_info.recall_deny = 0;
    smb_rename_scan_page(request, 0, 0);
} /* chimera_smb_set_info_rename_recall_scan */

/* Entry point: for a directory rename, enumerate the source directory's
 * children and recall the caching leases of any that are open before issuing
 * the rename.  Non-directory renames go straight to do_rename. */
static void
chimera_smb_set_info_rename_recall_children(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file   *open_file   = request->set_info.open_file;
    struct chimera_smb_rename_info *rename_info = &request->set_info.rename_info;

    request->set_info.recall_children       = NULL;
    request->set_info.recall_child_count    = 0;
    request->set_info.recall_child_cap      = 0;
    request->set_info.recall_child_idx      = 0;
    request->set_info.recall_readdir_cookie = 0;
    request->set_info.recall_deny           = 0;
    request->set_info.recall_final          = 0;

    /* POSIX rename semantics (the POSIX-over-SMB loopback): a rename never fails
     * because of open handles -- on the source, on a file inside the renamed
     * directory, or on the destination parent.  Skip the SMB contained-open
     * recall and the destination-parent dir-lease probe entirely and go straight
     * to rename_at, which enforces the POSIX rename(2) error rules. */
    if (request->compound->thread->shared->config.posix_rename) {
        chimera_smb_set_info_rename_emit(request);
        return;
    }

    if (!open_file->handle ||
        !(open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY)) {
        chimera_smb_set_info_rename_do_rename(request);
        return;
    }

    /* Renaming a directory into itself (the new parent IS the source directory,
     * i.e. "mv c c/d") is POSIX EINVAL -- a structural invalidity that precedes
     * every SMB lease/recall gate.  The contained-open recall would enumerate
     * the source's children (including the just-probed destination name) and
     * deny ACCESS_DENIED, and the destination-parent dir-lease probe would
     * conflict with the source's own DELETE handle and deny SHARING_VIOLATION.
     * Go straight to rename_at, which returns EINVAL. */
    if (rename_info->new_parent_len &&
        request->set_info.dst_parent_fh_len == open_file->handle->fh_len &&
        memcmp(request->set_info.dst_parent_fh, open_file->handle->fh,
               open_file->handle->fh_len) == 0) {
        chimera_smb_set_info_rename_emit(request);
        return;
    }

    chimera_smb_set_info_rename_recall_scan(request);
} /* chimera_smb_set_info_rename_recall_children */


/*
 * PUTFH(tree root), [LOOKUP_PATH(destination parent)], LOOKUP(destination name).
 *
 * Resolving the destination parent and asking whether the destination name is
 * already taken are one sequence: the second resolves in what the first found,
 * which is the chaining the cursor was made for.  Nothing is OPENED -- the
 * rename, and the deny probe that may precede it, take file handles -- so the
 * per-op chain's open of the destination parent goes with it.
 *
 * The final LOOKUP is ALLOWED to fail: "the destination does not exist" is the
 * common answer and the one the rename wants.  It is the last op, so its
 * failure is simply where the run stopped, and the ops before it kept their
 * results -- which is how the destination parent's file handle survives a
 * destination that is not there.
 */

struct chimera_vfs_attrs;
struct chimera_vfs_open_handle;
static void chimera_smb_set_info_rename_check_dest_callback(
    enum chimera_vfs_error,
    struct chimera_vfs_attrs *,
    struct chimera_vfs_attrs *,
    void *);

static void
smb_rename_lookup_done(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;
    uint32_t                              last    = chimera_vfs_compound_num_ops(compound) - 1;
    const struct chimera_vfs_compound_op *lookup  = chimera_vfs_compound_op(compound, last);
    struct chimera_vfs_open_handle       *parent  = chimera_vfs_compound_take_handle(compound, last - 1);

    if (!parent || lookup->status == CHIMERA_VFS_UNSET) {
        if (parent) {
            chimera_vfs_release(request->compound->thread->vfs_thread, parent);
        }
        chimera_vfs_compound_free(compound);
        chimera_smb_open_file_release(request, request->set_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_PATH_NOT_FOUND);
        return;
    }
    request->set_info.parent_handle = parent;
    memcpy(request->set_info.dst_parent_fh, parent->fh, parent->fh_len);
    request->set_info.dst_parent_fh_len = parent->fh_len;
    struct chimera_vfs_attrs attr = lookup->attr, dir = lookup->dir_post_attr;
    chimera_smb_set_info_rename_check_dest_callback(lookup->status, &attr, &dir, request);
    chimera_vfs_compound_free(compound);
} /* smb_rename_lookup_done */

void
chimera_smb_set_info_rename_process(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread      *vfs_thread  = request->compound->thread->vfs_thread;
    struct chimera_smb_tree        *tree        = request->tree;
    struct chimera_smb_rename_info *rename_info = &request->set_info.rename_info;
    struct chimera_smb_open_file   *open_file   = request->set_info.open_file;

    /* MS-FSA 2.1.5.14.11.1 SetInfo(FileRenameInformation): if the handle was
     * not opened with DELETE access, the rename MUST fail with ACCESS_DENIED
     * (the rename removes the old name from its parent and adds a new one, so
     * the source handle's GrantedAccess must include DELETE).  Gate here, ahead
     * of the destination-existence probe — otherwise a target collision would
     * surface as OBJECT_NAME_COLLISION and mask the real authorization error. */
    if (!(open_file->granted_access & SMB2_DELETE)) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    uint32_t namespace_status = chimera_smb_namespace_legacy_begin(request, open_file);
    if (namespace_status != SMB2_STATUS_SUCCESS) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, namespace_status);
        return;
    }

    /* A marked source link cannot be renamed, including an exact-name no-op.
     * This also keeps any deferred last-close deletion path tied to its link. */
    if (chimera_vfs_state_is_delete_pending(open_file->share_file_state)) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    request->set_info.parent_handle = NULL;
    rename_info->new_parent_handle  = NULL;
    struct chimera_vfs_compound *compound = chimera_vfs_compound_alloc(
        vfs_thread, &request->session_handle->session->cred);
    chimera_vfs_compound_add_putfh(compound, tree->fh, tree->fh_len);
    if (rename_info->new_parent_len) {
        chimera_vfs_compound_add_lookup_path(compound, rename_info->new_parent,
                                             rename_info->new_parent_len, CHIMERA_VFS_ATTR_FH, 0);
    }
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY
                                          , 0);
    chimera_vfs_compound_add_gethandle(compound);
    chimera_vfs_compound_add_lookup(compound, rename_info->new_name,
                                    rename_info->new_name_len, CHIMERA_VFS_ATTR_MODE, 0);
    chimera_vfs_compound_submit(compound, smb_rename_lookup_done, request);
} /* chimera_smb_set_info_rename_process */



/* Parse functions for SET_INFO SMB2_FILE_RENAME_INFO
 * request structures
 * Structure:
 *  Offset  Size  Field
 *  0       1     ReplaceIfExists (BOOLEAN)
 *  1       7     Reserved (ignored)
 *  8       8     RootDirectory (handle) -> MUST be 0 for network ops
 *  16      4     FileNameLength (bytes)
 *  20      N     FileName (UTF-16LE, NOT null-terminated)
 *  20+N    P     Padding (optional; ignored). Total size >= 24 bytes.
 */

int
chimera_smb_parse_rename_info(
    struct evpl_iovec_cursor   *cursor,
    struct chimera_smb_request *request)
{
    struct chimera_smb_rename_info *rename_info = &request->set_info.rename_info;
    uint64_t                        root_dir;
    uint16_t                        name16[SMB_FILENAME_MAX];  /* UTF-16LE bytes */
    uint32_t                        name_len;

    int                             prc = 0;

    prc |= evpl_iovec_cursor_try_get_uint8(cursor, &rename_info->replace_if_exist);
    prc |= evpl_iovec_cursor_try_skip(cursor, 7); /* Reserved */
    prc |= evpl_iovec_cursor_try_get_uint64(cursor, &root_dir);
    prc |= evpl_iovec_cursor_try_get_uint32(cursor, &name_len);

    if (unlikely(prc)) {
        chimera_smb_error("SET_INFO RENAME_INFO request truncated in fixed body");
        request->status = SMB2_STATUS_INFO_LENGTH_MISMATCH;
        return -1;
    }

    if (root_dir != 0) {
        // Non-zero root directory not supported
        chimera_smb_error("SET_INFO RENAME_INFO with non-zero root directory not supported");
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }
    if (name_len > sizeof(name16)) {
        chimera_smb_error("SET_INFO RENAME_INFO request: UTF-16 name too long (%u bytes)",
                          name_len);
        request->status = SMB2_STATUS_INFO_LENGTH_MISMATCH;
        return -1;
    }

    if (unlikely(evpl_iovec_cursor_try_copy(cursor, (uint8_t *) name16, name_len) != 0)) {
        chimera_smb_error("SET_INFO RENAME_INFO name runs past the input buffer");
        request->status = SMB2_STATUS_INFO_LENGTH_MISMATCH;
        return -1;
    }
    /* Convert UTF-16LE name to UTF-8 */
    rename_info->new_parent_len = chimera_smb_utf16le_to_utf8(&request->compound->thread->iconv_ctx,
                                                              name16,
                                                              name_len,
                                                              rename_info->new_parent,
                                                              sizeof(rename_info->new_parent));

    if (rename_info->new_parent_len < 0) {
        chimera_smb_error("SET_INFO RENAME_INFO failed to convert new name to UTF-8");
        request->status = SMB2_STATUS_OBJECT_NAME_INVALID;
        return -1;
    }

    /* Split into parent path and name, similar to chimera_smb_parse_create */
    char *slash = strrchr(rename_info->new_parent, '\\');

    if (slash) {
        *slash                      = '\0';
        rename_info->new_name       = slash + 1;
        rename_info->new_name_len   = rename_info->new_parent_len - (slash - rename_info->new_parent) - 1;
        rename_info->new_parent_len = slash - rename_info->new_parent;

        chimera_smb_slash_back_to_forward(rename_info->new_parent, rename_info->new_parent_len);
    } else {
        rename_info->new_name       = rename_info->new_parent;
        rename_info->new_name_len   = rename_info->new_parent_len;
        rename_info->new_parent_len = 0;
    }

    return 0;
} /* chimera_smb_parse_rename_info */

/* Existing regular-file moves use typed parent resolution and private path
 * publication. ReplaceIfExists first uses atomic NOREPLACE. Same-inode aliases
 * then use matched source+destination identities and an atomic no-op outcome;
 * distinct occupied destinations without holders use constructor/mutator, DOC and
 * ACCESS admission before atomic matched replacement. Live targets defer.
 * Named-stream peers follow their base link. Directory moves support a bounded
 * root-child same-parent slice under constructor/mutator exclusion and conservative
 * participant isolation; occupied targets, descendant holders, large scans,
 * stream renames and unsupported cross-view moves remain boundaries. */
#include "smb_compound.h"
#include "smb_doc_compound.h"

struct smb_rename_compound {
    struct chimera_smb_namespace_path          before, after;
    struct chimera_vfs_claim                   probe;
    struct chimera_vfs_pending_acquire         ticket;
    struct chimera_vfs_file_state             *parent;
    struct chimera_vfs_compound               *compound;
    struct smb_vfs_command                    *command;
    uint64_t                                   token;
    bool                                       noop, renamed, replace_needed, first_moved, directory;
    uint8_t                                    target_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                                   target_fh_len;
    uint32_t                                   target_mode;
    struct chimera_smb_namespace_replace_token replacement;
    struct chimera_smb_doc_fence               target_doc;
    struct chimera_vfs_claim_access_fence      target_admission;
    struct chimera_vfs_file_state             *target;

};

static void
smb_rename_target_release(struct smb_rename_compound *ctx)
{
    chimera_vfs_claim_access_fence_release(&ctx->target_admission);
    chimera_smb_doc_fence_release(&ctx->target_doc);
    if (ctx->target) {
        chimera_vfs_state_put(ctx->command->request->compound->thread->vfs_thread->vfs->vfs_state, ctx->target);
        ctx->target = NULL;
    }
    chimera_smb_namespace_replace_end(&ctx->replacement);
} /* smb_rename_target_release */

static int
smb_rename_eligible(struct chimera_smb_request *request)
{
    return request->set_info.info_type == SMB2_INFO_FILE &&
           request->set_info.info_class == SMB2_FILE_RENAME_INFO;
} /* smb_rename_eligible */

static int
smb_rename_bound(struct smb_vfs_command *command)
{
    struct chimera_smb_open_file *open = command->open;

    if (!open || !command->state) {
        return 0;
    }
    /* A producer's type, FH and backend capability are execution inputs. */
    if (command->state->producer) {
        return 1;
    }
    uint64_t caps = command->handle ? command->handle->vfs_module->capabilities : 0;
    if (!open->share_file_state ||
        (open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM) ||
        !(caps & CHIMERA_VFS_CAP_RENAME_NOREPLACE) || !(caps & CHIMERA_VFS_CAP_RENAME_MATCH_FH) ||
        open->share_file_state->fh_len != command->handle->fh_len ||
        memcmp(open->share_file_state->fh, command->handle->fh, command->handle->fh_len)) {
        return 0;
    }
    return 1;
} /* smb_rename_bound */

static struct chimera_smb_file_id
smb_rename_file_id(struct chimera_smb_request *request)
{
    return request->set_info.file_id;
} /* smb_rename_file_id */

/* Directory admission permits same-view root-child siblings while the global
* writer excludes every constructor and RENAME/LINK mutator. Public snapshots
* and the private batch journal must both establish the bounded root paths;
* otherwise an earlier sibling move could be hidden until accepted finish. */
static void
smb_rename_directory_coordinate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint64_t                     token,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    void                        *private_data)
{
    struct smb_vfs_command                *command = private_data;
    struct smb_rename_compound            *ctx     = command->private_data;

    (void) index; (void) fh; (void) fh_len;
    if (!ctx->directory) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_OK); return;
    }
    struct chimera_smb_namespace_registry *registry =
        &command->request->compound->thread->shared->namespace_registry;
    if (!chimera_smb_namespace_replace_begin(&ctx->replacement, registry,
                                             command->request->compound)) {
        goto defer;
    }
    chimera_smb_namespace_lock(registry);
    bool                                   isolated = chimera_smb_namespace_directory_isolated_locked(registry,
                                                                                                      command->handle->
                                                                                                      fh, command->
                                                                                                      handle->fh_len, &
                                                                                                      ctx->before);
    chimera_smb_namespace_unlock(registry);
    if (!isolated || !chimera_smb_compound_directory_isolated(command)) {
        goto defer;
    }
    chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_OK);
    return;
 defer:
    smb_rename_target_release(ctx);
    chimera_smb_compound_defer(command);
    chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_EINTR);
} /* smb_rename_directory_coordinate */

static void
smb_rename_directory_scan_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_rename_compound *ctx = ((struct smb_vfs_command *) private_data)->private_data;

    (void) status;
    if (!ctx->directory) {
        chimera_vfs_compound_op_skip(compound, index);
    }
} /* smb_rename_directory_scan_prepare */

static void
smb_rename_directory_scan_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command               *command = private_data;
    struct smb_rename_compound           *ctx     = command->private_data;

    if (!ctx->directory || *status == CHIMERA_VFS_EINTR) {
        return;
    }
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);
    if (*status != CHIMERA_VFS_OK || !op->eof) {
        goto defer;
    }
    for (uint32_t i = 0; i < op->num_entries; i++) {
        const struct chimera_vfs_compound_dirent *entry = &op->entries[i];
        if ((entry->name_len == 1 && entry->name[0] == '.') ||
            (entry->name_len == 2 && !memcmp(entry->name, "..", 2))) {
            continue;
        }
        const struct chimera_vfs_attrs           *attr = &entry->attr;
        if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH) || !attr->va_fh_len ||
            attr->va_fh_len > CHIMERA_VFS_FH_SIZE ||
            chimera_vfs_fh_has_share_holder(command->request->compound->thread->vfs_thread,
                                            attr->va_fh, attr->va_fh_len)) {
            goto defer;
        }
    }
    return;
 defer:
    /* Only read-only discovery has executed for this command. Legacy retains
     * paging/contained-open recalls and its backend-error fallback semantics. */
    chimera_smb_compound_defer(command);
    *status = CHIMERA_VFS_EINTR;
} /* smb_rename_directory_scan_complete */

static void
smb_rename_probe_done(
    enum chimera_vfs_claim_result            result,
    struct chimera_vfs_claim                *claim,
    const struct chimera_vfs_claim_conflict *conflict,
    void                                    *private_data)
{
    struct smb_rename_compound *ctx   = private_data;
    struct chimera_vfs_state   *state = ctx->command->request->compound->thread->vfs_thread->vfs->vfs_state;

    (void) claim; (void) conflict;
    if (result == CHIMERA_CLAIM_GRANTED) {
        chimera_vfs_claim_release(state, ctx->parent, &ctx->probe);
    }
    chimera_vfs_state_put(state, ctx->parent);
    ctx->parent = NULL;
    if (result != CHIMERA_CLAIM_GRANTED) {
        ctx->command->status = SMB2_STATUS_SHARING_VIOLATION;
    }
    chimera_vfs_compound_coordinate_done(ctx->compound, ctx->token,
                                         result == CHIMERA_CLAIM_GRANTED ? CHIMERA_VFS_OK : CHIMERA_VFS_EAGAIN);
} /* smb_rename_probe_done */

static void
smb_rename_parent_probe(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint64_t                     token,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    void                        *private_data)
{
    struct smb_vfs_command           *command = private_data;
    struct smb_rename_compound       *ctx     = command->private_data;
    struct chimera_server_smb_thread *thread  = command->request->compound->thread;
    struct chimera_vfs_state         *state   = thread->vfs_thread->vfs->vfs_state;

    (void) index;
    struct chimera_smb_rename_info   *info = &command->request->set_info.rename_info;
    ctx->after               = ctx->before;
    ctx->after.parent_fh_len = fh_len;
    memcpy(ctx->after.parent_fh, fh, fh_len);
    ctx->after.name_len = info->new_name_len;
    memcpy(ctx->after.name, info->new_name, info->new_name_len);
    ctx->after.name[info->new_name_len] = 0;
    ctx->after.full_path_len            = info->new_parent_len;
    memcpy(ctx->after.full_path, info->new_parent, info->new_parent_len);
    chimera_smb_slash_forward_to_back(ctx->after.full_path, ctx->after.full_path_len);
    if (ctx->after.full_path_len) {
        ctx->after.full_path[ctx->after.full_path_len++] = '\\';
    }
    memcpy(ctx->after.full_path + ctx->after.full_path_len, info->new_name, info->new_name_len);
    ctx->after.full_path_len                      += info->new_name_len;
    ctx->after.full_path[ctx->after.full_path_len] = 0;
    ctx->noop                                      = fh_len == ctx->before.parent_fh_len && !memcmp(fh, ctx->before.
                                                                                                    parent_fh, fh_len)
        &&
        ctx->before.name_len == (uint32_t) info->new_name_len &&
        !memcmp(ctx->before.name, info->new_name, info->new_name_len);
    if (!smb_doc_path_prepare_move(command, &ctx->after)) {
        /* A foreign share view needs a path translation not available to
         * this slice. No mutation has run; preserve ordinary legacy behavior. */
        chimera_smb_compound_defer(command);
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_EINTR); return;
    }
    if (ctx->noop) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_OK); return;
    }
    ctx->parent = chimera_vfs_state_get(state, fh, fh_len, chimera_vfs_hash(fh, fh_len), false);
    if (!ctx->parent) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_OK); return;
    }
    struct chimera_claim_owner owner = command->actor.owner;
    memcpy(owner.key, command->open->parent_lease_key, 16);
    chimera_vfs_claim_init_deny_probe(&ctx->probe, CHIMERA_CLAIM_D, &owner);
    ctx->probe.policy_tag = command->open->file_id.pid;
    ctx->compound         = compound;
    ctx->token            = token;
    chimera_vfs_claim_acquire(thread->vfs_thread, state, ctx->parent, &ctx->probe,
                              &ctx->ticket, true, false, smb_rename_probe_done, NULL, ctx);
    chimera_smb_lease_break_flush(thread);
} /* smb_rename_parent_probe */

static void
smb_rename_probe_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command *command = private_data;

    (void) compound; (void) index;
    if (*status == CHIMERA_VFS_EAGAIN) {
        command->status = SMB2_STATUS_SHARING_VIOLATION;
    }
} /* smb_rename_probe_complete */

static void
smb_rename_result_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command         *command = private_data;
    struct smb_rename_compound     *ctx     = command->private_data;
    struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op_args(compound, index);

    (void) status;
    if (ctx->noop) {
        chimera_vfs_compound_op_skip(compound, index); return;
    }
    if (ctx->directory) {
        op->remove_flags |= CHIMERA_VFS_RENAME_SRC_IS_DIR;
    }
    op->name_len = ctx->before.name_len;
    memcpy(op->name, ctx->before.name, op->name_len);
    op->arg_fh_len = command->handle->fh_len;
    memcpy(op->arg_fh, command->handle->fh, op->arg_fh_len);
    op->io_owner                         = command->actor;
    op->have_io_owner                    = 1;
    op->namespace_parent_lease_key_valid = 1;
    memcpy(op->namespace_parent_lease_key, command->open->parent_lease_key, 16);
} /* smb_rename_result_prepare */

static void
smb_rename_source_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command         *command = private_data;
    struct smb_rename_compound     *ctx     = command->private_data;
    struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op_args(compound, index);

    (void) status;
    op->arg_fh_len = ctx->before.parent_fh_len;
    memcpy(op->arg_fh, ctx->before.parent_fh, op->arg_fh_len);
} /* smb_rename_source_prepare */

static void
smb_rename_root_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command         *command = private_data;
    struct smb_rename_compound     *ctx     = command->private_data;
    struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op_args(compound, index);

    (void) status;
    op->arg_fh_len = ctx->before.view_fh_len;
    memcpy(op->arg_fh, ctx->before.view_fh, op->arg_fh_len);
} /* smb_rename_root_prepare */

static void
smb_rename_parent_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command *command = private_data;

    if (*status == CHIMERA_VFS_OK && !S_ISDIR(chimera_vfs_compound_op(compound, index)->attr.va_mode)) {
        *status = CHIMERA_VFS_ENOTDIR;
    }
    if (*status != CHIMERA_VFS_OK) {
        command->status = SMB2_STATUS_OBJECT_PATH_NOT_FOUND;
    }
} /* smb_rename_parent_complete */

static void
smb_rename_first_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command     *command = private_data;
    struct smb_rename_compound *ctx     = command->private_data;

    (void) compound; (void) index;
    ctx->first_moved = *status == CHIMERA_VFS_OK && !ctx->noop;
    if (ctx->first_moved) {
        /* The optional replacement suffix can be canceled after this real
         * move. Preserve its accepted filesystem prefix and path publication
         * even when the command's final operation never executes. */
        ctx->renamed = true;
        smb_doc_path_complete(command, true);
    }
    if (*status == CHIMERA_VFS_EEXIST && ctx->directory) {
        /* Directory replacement has distinct legacy rules even when the wire
         * ReplaceIfExists bit is clear. NOREPLACE guarantees no side effects. */
        chimera_smb_compound_defer(command);
        *status = CHIMERA_VFS_EINTR;
    } else if (*status == CHIMERA_VFS_EEXIST && command->request->set_info.rename_info.replace_if_exist) {
        ctx->replace_needed = true;
        *status             = CHIMERA_VFS_OK;
    }
} /* smb_rename_first_complete */

static void
smb_rename_tail_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_rename_compound *ctx = ((struct smb_vfs_command *) private_data)->private_data;

    (void) status;
    if (!ctx->replace_needed) {
        chimera_vfs_compound_op_skip(compound, index);
    }
} /* smb_rename_tail_prepare */

static void
smb_rename_target_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command     *command = private_data;
    struct smb_rename_compound *ctx     = command->private_data;

    if (!ctx->replace_needed) {
        return;
    }
    ctx->target_fh_len = 0;
    if (*status == CHIMERA_VFS_ENOENT) {
        *status = CHIMERA_VFS_OK; return;
    }
    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    const struct chimera_vfs_attrs *attr = &chimera_vfs_compound_op(compound, index)->attr;
    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH) || !attr->va_fh_len ||
        attr->va_fh_len > sizeof(ctx->target_fh) || !(attr->va_set_mask & CHIMERA_VFS_ATTR_MODE)) {
        *status = CHIMERA_VFS_EIO; return;
    }
    ctx->target_fh_len = attr->va_fh_len;
    memcpy(ctx->target_fh, attr->va_fh, attr->va_fh_len);
    ctx->target_mode = attr->va_mode;
} /* smb_rename_target_complete */

static void
smb_rename_target_parent_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_rename_compound     *ctx = ((struct smb_vfs_command *) private_data)->private_data;

    (void) status;
    if (!ctx->replace_needed) {
        chimera_vfs_compound_op_skip(compound, index); return;
    }
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(compound, index);
    op->arg_fh_len = ctx->after.parent_fh_len;
    memcpy(op->arg_fh, ctx->after.parent_fh, op->arg_fh_len);
} /* smb_rename_target_parent_prepare */

static void
smb_rename_target_coordinate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint64_t                     token,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    void                        *private_data)
{
    struct smb_vfs_command     *command = private_data;
    struct smb_rename_compound *ctx     = command->private_data;

    (void) index; (void) fh; (void) fh_len;
    if (!ctx->replace_needed || !ctx->target_fh_len) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_OK); return;
    }
    uint64_t                    required = CHIMERA_VFS_CAP_RENAME_MATCH_DEST_FH | CHIMERA_VFS_CAP_RENAME_OUTCOME;
    if (!S_ISREG(ctx->target_mode) ||
        (command->handle->vfs_module->capabilities & required) != required) {
        goto defer;
    }
    /* A hardlink alias needs no destination teardown: the atomic backend
     * result verifies that both links still name this same source inode. */
    if (ctx->target_fh_len == command->handle->fh_len &&
        !memcmp(ctx->target_fh, command->handle->fh, ctx->target_fh_len)) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_OK); return;
    }
    /* Reader admission covers native/legacy construction and all RENAME/LINK
     * mutation before the backend can acquire or relocate the old target. The
     * destination additionally needs no
     * live, closing or pending namespace participants or protocol claims. Hold
     * all three barriers through accepted path publication. No waiting while
     * the source command already owns its DOC/ACCESS fences. */
    struct chimera_server_smb_thread      *thread   = command->request->compound->thread;
    struct chimera_smb_namespace_registry *registry = &thread->shared->namespace_registry;
    if (!chimera_smb_namespace_replace_begin(&ctx->replacement, registry, command->request->compound)) {
        goto defer;
    }
    ctx->target = chimera_vfs_state_get(thread->vfs_thread->vfs->vfs_state, ctx->target_fh,
                                        ctx->target_fh_len, chimera_vfs_hash(ctx->target_fh, ctx->target_fh_len), true);
    if (!ctx->target || !chimera_smb_doc_fence_acquire(&ctx->target_doc, registry,
                                                       ctx->target_fh, ctx->target_fh_len, command->batch) ||
        !chimera_vfs_claim_access_fence_acquire(&ctx->target_admission, ctx->target,
                                                chimera_smb_compound_doc_batch(command))) {
        goto defer;
    }
    chimera_smb_namespace_lock(registry);
    evpl_mutex_lock(&ctx->target->lock);
    bool occupied = chimera_smb_namespace_identity_present_locked(registry,
                                                                  ctx->target_fh, ctx->target_fh_len) || ctx->target->
        delete_pending ||
        ctx->target->smb_delete_started || ctx->target->smb_pending_delete ||
        ctx->target->stream_holders || ctx->target->pending_head || ctx->target->break_waiters ||
        ctx->target->claims[CHIMERA_CLAIM_CLASS_CACHE] ||
        ctx->target->claims[CHIMERA_CLAIM_CLASS_RANGE];
    for (struct chimera_vfs_claim *claim = ctx->target->claims[CHIMERA_CLAIM_CLASS_ACCESS];
         claim; claim = claim->next) {
        /* The VFS's cached implicit I/O row is not a protocol opener and
         * carries no pathname publication or delayed SMB admission. */
        if (claim != &ctx->target->implicit_claim) {
            occupied = true;
        }
    }
    evpl_mutex_unlock(&ctx->target->lock);
    chimera_smb_namespace_unlock(registry);
    if (occupied) {
        goto defer;
    }
    chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_OK);
    return;
 defer:
    smb_rename_target_release(ctx);
    chimera_smb_compound_defer(command);
    chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_EINTR);
} /* smb_rename_target_coordinate */

static void
smb_rename_replace_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command         *command = private_data;
    struct smb_rename_compound     *ctx     = command->private_data;

    if (!ctx->replace_needed) {
        chimera_vfs_compound_op_skip(compound, index); return;
    }
    smb_rename_result_prepare(compound, index, status, private_data);
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(compound, index);
    op->remove_flags = CHIMERA_VFS_RENAME_NO_NOTIFY | CHIMERA_VFS_RENAME_MATCH_SOURCE_FH |
        (ctx->target_fh_len ? CHIMERA_VFS_RENAME_MATCH_DEST_FH : CHIMERA_VFS_RENAME_NOREPLACE);
    op->rename_target_fh_len = ctx->target_fh_len;
    memcpy(op->rename_target_fh, ctx->target_fh, ctx->target_fh_len);
} /* smb_rename_replace_prepare */

static int
smb_rename_build(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    struct smb_rename_compound *ctx = calloc(1, sizeof(*ctx));

    command->private_data = ctx;
    if (!ctx || !smb_doc_path_reserve(command)) {
        command->input_status = SMB2_STATUS_INSUFFICIENT_RESOURCES; return -1;
    }
    ctx->command = command;
    int                         directory_coord = chimera_vfs_compound_add_coordinate(compound,
                                                                                      smb_rename_directory_coordinate,
                                                                                      command);
    if (directory_coord < 0) {
        return -1;
    }
    chimera_vfs_compound_op_args(compound, directory_coord)->coordinate_each_attempt = 1;
    if (smb_doc_close_add_coordinate(compound, command) < 0) {
        return -1;
    }
    int directory_scan = chimera_vfs_compound_add_readdir(compound, 0, 0, 0, 0, 128,
                                                          CHIMERA_VFS_ATTR_FH, 0);
    if (directory_scan < 0) {
        return -1;
    }
    chimera_vfs_compound_set_op_callbacks(compound, directory_scan,
                                          smb_rename_directory_scan_prepare, smb_rename_directory_scan_complete, command
                                          );
    /* Both identities may be absent at construction for a cold-tree CREATE.
     * The real root is a valid placeholder, replaced before either op runs. */
    uint8_t                         root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        root_fh_len;
    chimera_vfs_get_root_fh(root_fh, &root_fh_len);
    int                             source = chimera_vfs_compound_add_putfh(compound, root_fh, root_fh_len);
    chimera_vfs_compound_set_op_prepare(compound, source, smb_rename_source_prepare, command);
    chimera_vfs_compound_add_savefh(compound);
    int                             root = chimera_vfs_compound_add_putfh(compound, root_fh, root_fh_len);
    chimera_vfs_compound_set_op_prepare(compound, root, smb_rename_root_prepare, command);
    struct chimera_smb_rename_info *info = &command->request->set_info.rename_info;
    if (info->new_parent_len) {
        int lookup = chimera_vfs_compound_add_lookup_path(compound, info->new_parent,
                                                          info->new_parent_len, CHIMERA_VFS_ATTR_FH |
                                                          CHIMERA_VFS_ATTR_MODE, 0);
        chimera_vfs_compound_set_op_callbacks(compound, lookup, NULL, smb_rename_parent_complete, command);
    }
    int                             probe = chimera_vfs_compound_add_coordinate(compound, smb_rename_parent_probe,
                                                                                command);
    chimera_vfs_compound_set_op_callbacks(compound, probe, NULL, smb_rename_probe_complete, command);
    if (probe >= 0) {
        chimera_vfs_compound_op_args(compound, probe)->coordinate_each_attempt = 1;
    }
    int                             rename = chimera_vfs_compound_add_rename(compound, "source", 6, command->request->
                                                                             set_info.rename_info.
                                                                             new_name, command->request->set_info.
                                                                             rename_info.
                                                                             new_name_len, CHIMERA_VFS_RENAME_NOREPLACE
                                                                             |
                                                                             CHIMERA_VFS_RENAME_NO_NOTIFY |
                                                                             CHIMERA_VFS_RENAME_MATCH_SOURCE_FH, 0, 0);
    chimera_vfs_compound_set_op_callbacks(compound, rename, smb_rename_result_prepare,
                                          smb_rename_first_complete, command);
    int lookup = chimera_vfs_compound_add_lookup(compound, info->new_name, info->
                                                 new_name_len, CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE, 0);
    chimera_vfs_compound_set_op_callbacks(compound, lookup, smb_rename_tail_prepare,
                                          smb_rename_target_complete, command);
    int target_parent = chimera_vfs_compound_add_putfh(compound, root_fh, root_fh_len);
    chimera_vfs_compound_set_op_prepare(compound, target_parent, smb_rename_target_parent_prepare, command);
    int target_coord = chimera_vfs_compound_add_coordinate(compound,
                                                           smb_rename_target_coordinate, command);
    chimera_vfs_compound_set_op_prepare(compound, target_coord, smb_rename_tail_prepare, command);
    if (target_coord < 0) {
        command->input_status = SMB2_STATUS_INSUFFICIENT_RESOURCES; return -1;
    }
    chimera_vfs_compound_op_args(compound, target_coord)->coordinate_each_attempt = 1;
    int replace = chimera_vfs_compound_add_rename(compound, "source", 6, info->new_name,
                                                  info->new_name_len, CHIMERA_VFS_RENAME_NOREPLACE |
                                                  CHIMERA_VFS_RENAME_NO_NOTIFY |
                                                  CHIMERA_VFS_RENAME_MATCH_SOURCE_FH, 0, 0);
    chimera_vfs_compound_set_op_prepare(compound, replace, smb_rename_replace_prepare, command);
    return replace;
} /* smb_rename_build */

static void
smb_rename_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command     *command = private_data;
    struct smb_rename_compound *ctx     = command->private_data;

    (void) compound; (void) index; (void) status;
    if (!(command->open->granted_access & SMB2_DELETE)) {
        command->status = SMB2_STATUS_ACCESS_DENIED; return;
    }
    uint64_t                    caps = command->handle ? command->handle->vfs_module->capabilities : 0;
    if ((command->state->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM) ||
        !(caps & CHIMERA_VFS_CAP_RENAME_NOREPLACE) || !(caps & CHIMERA_VFS_CAP_RENAME_MATCH_FH) ||
        (!command->state->producer && !smb_rename_bound(command))) {
        chimera_smb_compound_defer(command);
        *status = CHIMERA_VFS_EINTR; return;
    }
    smb_doc_command_path(command, &ctx->before);
    ctx->directory = !!(command->state->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY);
    if (ctx->directory &&
        (command->request->set_info.rename_info.new_parent_len ||
         ctx->before.parent_fh_len != ctx->before.view_fh_len ||
         memcmp(ctx->before.parent_fh, ctx->before.view_fh, ctx->before.view_fh_len) ||
         ctx->before.full_path_len != ctx->before.name_len || !ctx->before.name_len ||
         (ctx->before.name_len == 1 && ctx->before.name[0] == '.') ||
         (ctx->before.name_len == 2 && !memcmp(ctx->before.name, "..", 2)) ||
         (command->handle->fh_len == ctx->before.view_fh_len &&
          !memcmp(command->handle->fh, ctx->before.view_fh, ctx->before.view_fh_len)))) {
        chimera_smb_compound_defer(command);
        *status = CHIMERA_VFS_EINTR; return;
    }
    if (command->state->channel_sequence_valid &&
        (uint16_t) (command->request->channel_sequence - command->state->channel_sequence) >= 0x8000) {
        command->status = SMB2_STATUS_FILE_NOT_AVAILABLE; return;
    }
    command->state->channel_sequence       = command->request->channel_sequence;
    command->state->channel_sequence_valid = command->state->sequence_dirty = 1;
} /* smb_rename_prepare */

static void
smb_rename_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command     *command = private_data;
    struct smb_rename_compound *ctx     = command->private_data;

    (void) compound; (void) index;
    ctx->renamed = ctx->first_moved || (*status == CHIMERA_VFS_OK && ctx->replace_needed &&
                                        chimera_vfs_compound_op(compound, index)->rename_outcome !=
                                        CHIMERA_VFS_RENAME_OUTCOME_NOOP);
    smb_doc_path_complete(command, ctx->renamed);
    if (*status == CHIMERA_VFS_EEXIST && command->request->set_info.rename_info.replace_if_exist) {
        /* NOREPLACE checks and insertion are atomic in the backend. EEXIST
         * therefore guarantees this command has not changed either link;
         * finish the earlier prefix before entering the target's replacement
         * lifecycle. Do not infer safety from a racy destination lookup, and
         * do not turn source-identity or sharing failures into a retry. */
        chimera_smb_compound_defer(command);
        *status = CHIMERA_VFS_EINTR;
    }
} /* smb_rename_complete */

static unsigned int
smb_rename_map_error(enum chimera_vfs_error status)
{
    switch (status) {
        case CHIMERA_VFS_OK: return SMB2_STATUS_SUCCESS;
        case CHIMERA_VFS_EEXIST: return SMB2_STATUS_OBJECT_NAME_COLLISION;
        case CHIMERA_VFS_ENOENT:
        case CHIMERA_VFS_ESTALE: return SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM: return SMB2_STATUS_ACCESS_DENIED;
        case CHIMERA_VFS_ENOTEMPTY: return SMB2_STATUS_DIRECTORY_NOT_EMPTY;
        case CHIMERA_VFS_EISDIR: return SMB2_STATUS_FILE_IS_A_DIRECTORY;
        case CHIMERA_VFS_ENOTDIR: return SMB2_STATUS_NOT_A_DIRECTORY;
        case CHIMERA_VFS_EINVAL: return SMB2_STATUS_INVALID_PARAMETER;
        case CHIMERA_VFS_EINTR: return SMB2_STATUS_CANCELLED;
        default: return SMB2_STATUS_INTERNAL_ERROR;
    } /* switch */
} /* smb_rename_map_error */

static void
smb_rename_publish(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    struct smb_rename_compound *ctx = command->private_data;

    (void) compound;
    if (!ctx->renamed) {
        return;
    }
    smb_doc_path_publish(command);
    struct chimera_claim_actor  actor = { .owner = chimera_smb_open_actor_owner(command->open) };
    memcpy(actor.owner.key, command->open->parent_lease_key, 16);
    uint32_t                    action = ctx->directory ? CHIMERA_VFS_NOTIFY_RENAMED_DIR :
        CHIMERA_VFS_NOTIFY_RENAMED;
    bool                        same_parent = ctx->before.parent_fh_len == ctx->after.parent_fh_len &&
        !memcmp(ctx->before.parent_fh, ctx->after.parent_fh, ctx->before.parent_fh_len);
    chimera_vfs_notify_emit_actor(command->request->compound->thread->shared->vfs->vfs_notify,
                                  ctx->before.parent_fh, ctx->before.parent_fh_len, action,
                                  same_parent ? ctx->after.name : NULL, same_parent ? ctx->after.name_len : 0,
                                  ctx->before.name, ctx->before.name_len,
                                  chimera_claim_owner_has_key(&actor.owner) ? &actor : NULL);
    if (!same_parent) {
        chimera_vfs_notify_emit_actor(command->request->compound->thread->shared->vfs->vfs_notify,
                                      ctx->after.parent_fh, ctx->after.parent_fh_len, action,
                                      ctx->after.name, ctx->after.name_len, NULL, 0,
                                      chimera_claim_owner_has_key(&actor.owner) ? &actor : NULL);
    }
} /* smb_rename_publish */

static void
smb_rename_reset(struct smb_vfs_command *command)
{
    struct smb_rename_compound *ctx = command->private_data;

    smb_rename_target_release(ctx);
    ctx->renamed       = ctx->noop = ctx->first_moved = ctx->replace_needed = false;
    ctx->target_fh_len = 0;
} /* smb_rename_reset */
static void
smb_rename_release(struct smb_vfs_command *command)
{
    if (command->private_data) {
        smb_rename_target_release(command->private_data);
    }
    free(command->private_data);
    command->private_data = NULL;
} /* smb_rename_release */

const struct smb_vfs_command_ops chimera_smb_rename_compound_ops = {
    .eligible       = smb_rename_eligible,
    .bound_eligible = smb_rename_bound,
    .file_id        = smb_rename_file_id,
    .map_error      = smb_rename_map_error,
    .build          = smb_rename_build,
    .prepare        = smb_rename_prepare,
    .complete       = smb_rename_complete,
    .publish        = smb_rename_publish,
    .reset          = smb_rename_reset,
    .release        = smb_rename_release,
};

struct chimera_vfs_open_handle;
static int
chimera_smb_set_info_rename_recall_readdir_cb(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct smb_rename_scan           *scan       = arg;
    struct chimera_smb_request       *request    = scan->request;
    struct chimera_vfs_thread        *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_rename_recall *rc;

    (void) compound;
    (void) index;
    (void) inum;
    (void) cookie;

    if ((namelen == 1 && name[0] == '.') ||
        (namelen == 2 && name[0] == '.' && name[1] == '.')) {
        return 0;
    }

    if (!(attrs->va_set_mask & CHIMERA_VFS_ATTR_FH) || attrs->va_fh_len == 0 ||
        attrs->va_fh_len > CHIMERA_VFS_FH_SIZE) {
        request->set_info.recall_deny = 1;
        return -1;
    }

    if (!chimera_vfs_fh_has_share_holder(vfs_thread, attrs->va_fh,
                                         attrs->va_fh_len)) {
        return 0;
    }

    if (request->set_info.recall_child_count == request->set_info.recall_child_cap) {
        uint64_t                          capacity = request->set_info.recall_child_cap ?
            (uint64_t) request->set_info.recall_child_cap * 2 : 8;
        if (capacity > UINT32_MAX || capacity > SIZE_MAX / sizeof(*rc)) {
            request->set_info.recall_deny = 1;
            return -1;
        }
        uint32_t                          newcap = capacity;
        struct chimera_smb_rename_recall *grown  = realloc(
            request->set_info.recall_children, newcap * sizeof(*grown));

        if (!grown) {
            /* Out of memory: deny conservatively rather than rename past an
             * un-recalled open. */
            request->set_info.recall_deny = 1;
            return -1;
        }
        request->set_info.recall_children  = grown;
        request->set_info.recall_child_cap = newcap;
    }

    rc = &request->set_info.recall_children[request->set_info.recall_child_count++];
    memcpy(rc->fh, attrs->va_fh, attrs->va_fh_len);
    rc->fh_len = attrs->va_fh_len;

    return 0;
} /* chimera_smb_set_info_rename_recall_readdir_cb */

struct chimera_vfs_attrs;
static void
chimera_smb_set_info_rename_check_dest_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data);


static void
chimera_smb_set_info_rename_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data)
{
    struct chimera_smb_request     *request     = private_data;
    struct chimera_smb_open_file   *open_file   = request->set_info.open_file;
    struct chimera_smb_rename_info *rename_info = &request->set_info.rename_info;

    if (!error_code && rename_info->outcome != CHIMERA_VFS_RENAME_OUTCOME_NOOP) {
        /* Release the sharemode entry keyed by the old name before updating
         * the path, then re-acquire under the new name below.  Without this,
         * close would hash the new name and fail to find the entry registered
         * under the old one, leaking it.
         *
         * Only the OPERATING handle is re-keyed, unlike the cached paths
         * below: nothing calls chimera_smb_sharemode_acquire for a peer any
         * more -- share arbitration moved to the VFS claim layer -- so a
         * sibling has no reservation here to move. */
        chimera_smb_sharemode_release(&request->tree->share->sharemode,
                                      open_file);

        struct chimera_vfs_open_handle *new_parent = rename_info->new_parent_handle
            ? rename_info->new_parent_handle : request->set_info.parent_handle;
        struct chimera_smb_rename_path  change = { .thread = request->compound->thread->vfs_thread };
        change.before.view_fh_len = request->tree->fh_len;
        memcpy(change.before.view_fh, request->tree->fh, request->tree->fh_len);
        change.before.parent_fh_len = open_file->parent_fh_len;
        memcpy(change.before.parent_fh, open_file->parent_fh, open_file->parent_fh_len);
        change.before.name_len = open_file->name_len;
        memcpy(change.before.name, open_file->name, open_file->name_len);
        change.before.full_path_len = open_file->full_path_len;
        memcpy(change.before.full_path, open_file->full_path, open_file->full_path_len);
        change.after               = change.before;
        change.after.parent_fh_len = new_parent->fh_len;
        memcpy(change.after.parent_fh, new_parent->fh, new_parent->fh_len);
        change.after.name_len = rename_info->new_name_len;
        memcpy(change.after.name, rename_info->new_name, rename_info->new_name_len);
        change.after.name[change.after.name_len] = 0;
        change.after.full_path_len               = rename_info->new_parent_len;
        memcpy(change.after.full_path, rename_info->new_parent, rename_info->new_parent_len);
        chimera_smb_slash_forward_to_back(change.after.full_path, change.after.full_path_len);
        if (change.after.full_path_len) {
            change.after.full_path[change.after.full_path_len++] = '\\';
        }
        memcpy(change.after.full_path + change.after.full_path_len,
               rename_info->new_name, rename_info->new_name_len);
        change.after.full_path_len                        += rename_info->new_name_len;
        change.after.full_path[change.after.full_path_len] = 0;
        struct chimera_vfs_file_state         *file     = open_file->share_file_state;
        struct chimera_smb_namespace_registry *registry = &request->compound->thread->shared->namespace_registry;
        change.locked_file = file;
        chimera_smb_namespace_lock(registry);
        if (file) {
            evpl_mutex_lock(&file->lock);
        }
        chimera_smb_rename_repath(open_file, &change);
        if (file) {
            chimera_smb_namespace_foreach_locked(registry, file->fh, file->fh_len,
                                                 chimera_smb_rename_repath_participant, &change);
            for (struct chimera_vfs_claim *claim = file->claims[CHIMERA_CLAIM_CLASS_ACCESS];
                 claim; claim = claim->next) {
                if ((claim->construct == CHIMERA_CONSTRUCT_SMB_OPEN ||
                     claim->construct == CHIMERA_CONSTRUCT_SMB_OPEN_INERT) && claim->cb_private) {
                    chimera_smb_rename_repath(claim->cb_private, &change);
                }
            }
            chimera_smb_namespace_pending_move_locked(registry, file->fh, file->fh_len,
                                                      &change.before, &change.after);
            evpl_mutex_unlock(&file->lock);
        }
        chimera_smb_namespace_unlock(registry);

        /* Re-acquire sharemode entry under the new name */
        if (open_file->type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE &&
            request->tree->share &&
            (open_file->desired_access & SMB2_SHAREMODE_ACCESS_MASK)) {
            chimera_smb_sharemode_acquire(
                &request->tree->share->sharemode,
                open_file->parent_fh, open_file->parent_fh_len,
                open_file->name, open_file->name_len,
                open_file->desired_access, open_file->share_access,
                open_file);
        }


    }

    if (request->set_info.parent_handle) {
        chimera_vfs_release(request->compound->thread->vfs_thread, request->set_info.parent_handle);
        request->set_info.parent_handle = NULL;
    }

    if (rename_info->new_parent_handle) {
        chimera_vfs_release(request->compound->thread->vfs_thread, rename_info->new_parent_handle);
        rename_info->new_parent_handle = NULL;
    }

    chimera_smb_open_file_release(request, open_file);

    /* Map the rename_at failure (if any) back to the SMB status the client
    * needs.  EACCES/EPERM commonly surface from the engine's DELETE_CHILD /
    * sharing-violation gates and must be reported as ACCESS_DENIED, not
    * INTERNAL_ERROR — a STATUS_INTERNAL_ERROR makes smbtorture treat the
    * server as broken instead of asserting on the actual returned code. */
    uint32_t status;
    switch (error_code) {
        case CHIMERA_VFS_OK:        status = SMB2_STATUS_SUCCESS; break;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:     status = SMB2_STATUS_ACCESS_DENIED; break;
        case CHIMERA_VFS_EEXIST:    status = SMB2_STATUS_OBJECT_NAME_COLLISION; break;
        case CHIMERA_VFS_ENOENT:
        case CHIMERA_VFS_ESTALE:    status = SMB2_STATUS_OBJECT_NAME_NOT_FOUND; break;
        case CHIMERA_VFS_ENOTEMPTY: status = SMB2_STATUS_DIRECTORY_NOT_EMPTY; break;
        case CHIMERA_VFS_EISDIR:    status = SMB2_STATUS_FILE_IS_A_DIRECTORY; break;
        case CHIMERA_VFS_ENOTDIR:   status = SMB2_STATUS_NOT_A_DIRECTORY; break;
        case CHIMERA_VFS_EINVAL:    status = SMB2_STATUS_INVALID_PARAMETER; break;
        default:                    status = SMB2_STATUS_INTERNAL_ERROR; break;
    } /* switch */

    chimera_smb_complete_request(request, status);
} /* chimera_smb_set_info_rename_callback */


static void
chimera_smb_set_info_rename_check_dest_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_smb_request     *request     = private_data;
    struct chimera_smb_rename_info *rename_info = &request->set_info.rename_info;

    if (error_code == CHIMERA_VFS_OK) {
        /* Destination exists.  A non-directory destination is overwritten only
         * when ReplaceIfExists is set (POSIX rename always sets it, so this
         * COLLISION only fires for an SMB caller that cleared it).  A directory
         * destination is left to rename_at, which enforces POSIX rename(2)
         * semantics: replace an empty directory, DIRECTORY_NOT_EMPTY otherwise,
         * and FILE_IS_A_DIRECTORY when the source is not itself a directory.
         * (The SMB "rename a file INTO a directory" shell behaviour is not
         * rename(2) and is deliberately not applied here.) */
        if (!S_ISDIR(attr->va_mode) && !rename_info->replace_if_exist &&
            !chimera_smb_set_info_rename_same_link(request)) {
            if (rename_info->new_parent_handle) {
                chimera_vfs_release(request->compound->thread->vfs_thread,
                                    rename_info->new_parent_handle);
            }
            if (request->set_info.parent_handle) {
                chimera_vfs_release(request->compound->thread->vfs_thread,
                                    request->set_info.parent_handle);
            }
            chimera_smb_open_file_release(request, request->set_info.open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_COLLISION);
            return;
        }
        /* Fall through: rename_at replaces the destination per POSIX. */
    }
    /* Destination doesn't exist or we're overwriting - proceed with rename.
     * For a directory rename, first break the handle leases of any files open
     * inside the source directory (smb2.lease.rename_dir_openfile). */
    chimera_smb_set_info_rename_recall_children(request);
} /* chimera_smb_set_info_rename_check_dest_callback */
