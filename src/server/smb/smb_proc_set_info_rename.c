// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>

#include "smb_internal.h"
#include "smb_procs.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_claim.h"

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

/* The new path a rename gives every open of the file. */
struct chimera_smb_rename_path {
    uint32_t parent_fh_len;
    uint8_t  parent_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t name_len;
    char     name[SMB_FILENAME_MAX];
};

static void
chimera_smb_rename_repath(
    struct chimera_smb_open_file         *open_file,
    const struct chimera_smb_rename_path *p)
{
    if (p->parent_fh_len > 0) {
        memcpy(open_file->parent_fh, p->parent_fh, p->parent_fh_len);
    }
    open_file->parent_fh_len = p->parent_fh_len;
    memcpy(open_file->name, p->name, p->name_len);
    open_file->name[p->name_len] = '\0';
    open_file->name_len          = p->name_len;
} /* chimera_smb_rename_repath */

/* Runs under the file state's lock (chimera_vfs_claim_foreach_smb_open), so it
 * writes fields and nothing else. */
static void
chimera_smb_rename_repath_cb(
    void *smb_open_file,
    void *private_data)
{
    chimera_smb_rename_repath(smb_open_file, private_data);
} /* chimera_smb_rename_repath_cb */

static void
chimera_smb_set_info_rename_finish(
    struct chimera_smb_request *request,
    enum chimera_vfs_error      error_code)
{
    struct chimera_smb_open_file   *open_file   = request->set_info.open_file;
    struct chimera_smb_rename_info *rename_info = &request->set_info.rename_info;

    if (!error_code) {
        /* A rename renames the FILE, not the handle that asked for it.  Every
         * open handle carries the file's path as its own (parent_fh, name) --
         * that pair is the ONLY path the SMB layer keeps, and every later path
         * operation reads it: the source of the next rename, the remove a
         * delete-on-close issues, the name in a CHANGE_NOTIFY record.  A
         * sibling left holding the old name does not merely fail (a rename
         * through it answers STATUS_OBJECT_NAME_NOT_FOUND for a file that is
         * plainly there); if another file later takes the old name, that
         * sibling's delete-on-close unlinks IT.
         *
         * So update every open of this file, not just this one.  The claim
         * layer is where that set lives -- each SMB open registers an ACCESS
         * claim on the file and points it back at itself -- and it spans
         * sessions and trees, which the SMB layer's own per-tree open-file
         * hashes cannot. */
        struct chimera_smb_rename_path newpath;

        newpath.parent_fh_len = open_file->parent_fh_len;
        memcpy(newpath.parent_fh, open_file->parent_fh,
               open_file->parent_fh_len);

        if (request->set_info.dst_parent_fh_len) {
            newpath.parent_fh_len = request->set_info.dst_parent_fh_len;
            memcpy(newpath.parent_fh, request->set_info.dst_parent_fh,
                   request->set_info.dst_parent_fh_len);
        }

        newpath.name_len = rename_info->new_name_len;
        memcpy(newpath.name, rename_info->new_name, rename_info->new_name_len);
        newpath.name[rename_info->new_name_len] = '\0';

        /* The operating handle first and unconditionally: it is the one case
         * that must be right even if the claim registration is not there to be
         * walked (a stream open, or a state torn down under us). */
        chimera_smb_rename_repath(open_file, &newpath);

        chimera_vfs_claim_foreach_smb_open(open_file->share_file_state,
                                           chimera_smb_rename_repath_cb,
                                           &newpath);

        /* Sharing reservations are VFS claims keyed by the file handle, so
         * they survive a rename without re-keying. Do not touch the legacy
         * sharemode table: TREE_DISCONNECT may have released tree->share
         * while the backend rename was in flight. The request holds the open
         * alive until this callback releases it below. */

        /* Update VFS handle DOC path so close deletes the new name */
        if ((open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE) &&
            open_file->handle) {
            chimera_vfs_set_delete_on_close(
                request->compound->thread->vfs_thread,
                open_file->handle,
                open_file->parent_fh,
                open_file->parent_fh_len,
                open_file->name,
                open_file->name_len,
                &request->session_handle->session->cred);
        }

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
} /* chimera_smb_set_info_rename_finish */

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

static void
chimera_smb_set_info_rename_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request *request = private_data;
    enum chimera_vfs_error      status;

    status = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    chimera_smb_set_info_rename_finish(request, status);
} /* chimera_smb_set_info_rename_sequence_complete */

/* PUTFH(src parent), SAVEFH, PUTFH(dst parent), RENAME.
 *
 * Issued once the destination parent's directory lease (if any) has yielded its
 * HANDLE caching.  RENAME reads the SAVED file handle for the source directory
 * and the CURRENT one for the target, which is what rename_at takes, so neither
 * directory is opened. */
static void
chimera_smb_set_info_rename_emit(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open_file = request->set_info.open_file;
    int                           index;

    request->vfs_compound = chimera_vfs_compound_alloc(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred);

    chimera_vfs_compound_add_putfh(request->vfs_compound,
                                   open_file->parent_fh,
                                   open_file->parent_fh_len);
    chimera_vfs_compound_add_savefh(request->vfs_compound);

    chimera_vfs_compound_add_putfh(request->vfs_compound,
                                   request->set_info.dst_parent_fh,
                                   request->set_info.dst_parent_fh_len);

    index = chimera_vfs_compound_add_rename(
        request->vfs_compound,
        open_file->name, open_file->name_len,
        request->set_info.rename_info.new_name,
        request->set_info.rename_info.new_name_len,
        0, 0, 0);

    chimera_vfs_compound_op_set_rename_opts(
        request->vfs_compound, index,
        NULL, 0,
        /* Self-exempt the renamer's own file lease from the source recall
         * (renaming a file it holds a lease on must not break it). */
        open_file->handle,
        /* ...and the directory lease named by the operating open's
         * ParentLeaseKey (dirlease.rename correct-parent-leaskey case). */
        open_file->parent_lease_key,
        /* The open carries the object's type, and it is the only layer that
         * does -- so it is the only one that can tell a file rename from a
         * directory one for the name filters (MS-FSCC 2.7.1). */
        (open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY)
        ? CHIMERA_VFS_RENAME_SRC_IS_DIR : 0);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_set_info_rename_sequence_complete,
                                request);
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
    memset(&dp_owner, 0, sizeof(dp_owner));
    dp_owner.proto      = CHIMERA_CLAIM_PROTO_SMB2;
    dp_owner.client_key = request->session_handle->session->client_key;
    dp_owner.owner_lo   = open_file->file_id.pid;
    dp_owner.owner_hi   = open_file->file_id.vid;
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
chimera_smb_set_info_rename_recall_deny(struct chimera_smb_request *request)
{
    chimera_smb_set_info_rename_recall_free(request);

    chimera_smb_open_file_release(request, request->set_info.open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
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
static void
chimera_smb_set_info_rename_recall_reset(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data)
{
    struct chimera_smb_request *request = private_data;

    (void) compound;
    (void) index;

    request->set_info.recall_child_count = request->set_info.recall_child_mark;
    request->set_info.recall_deny        = request->set_info.recall_deny_mark;
} /* chimera_smb_set_info_rename_recall_reset */

/* readdir append: collect each child that currently has a live share holder
 * (a real protocol open) so its caching lease can be recalled before the
 * directory rename. */
static int
chimera_smb_set_info_rename_recall_append(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct chimera_smb_request       *request    = arg;
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

    if (!(attrs->va_set_mask & CHIMERA_VFS_ATTR_FH) || attrs->va_fh_len == 0) {
        return 0;
    }

    if (!chimera_vfs_fh_has_share_holder(vfs_thread, attrs->va_fh,
                                         attrs->va_fh_len)) {
        return 0;
    }

    if (request->set_info.recall_child_count == request->set_info.recall_child_cap) {
        uint32_t                          newcap = request->set_info.recall_child_cap
            ? request->set_info.recall_child_cap * 2 : 8;
        struct chimera_smb_rename_recall *grown = realloc(
            request->set_info.recall_children, newcap * sizeof(*grown));

        if (!grown) {
            /* Out of memory: deny conservatively rather than rename past an
             * un-recalled open. */
            request->set_info.recall_deny = 1;
            return 0;
        }
        request->set_info.recall_children  = grown;
        request->set_info.recall_child_cap = newcap;
    }

    rc = &request->set_info.recall_children[request->set_info.recall_child_count++];
    memcpy(rc->fh, attrs->va_fh, attrs->va_fh_len);
    rc->fh_len = attrs->va_fh_len;

    return 0;
} /* chimera_smb_set_info_rename_recall_append */

static void chimera_smb_set_info_rename_recall_page(
    struct chimera_smb_request *request,
    uint64_t                    cookie,
    uint64_t                    verifier);

static void
chimera_smb_set_info_rename_recall_page_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    uint64_t                              cookie, verifier;
    uint32_t                              eof;

    status = chimera_vfs_compound_status(compound);
    op     = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);

    cookie   = op->r_cookie;
    verifier = op->r_verifier;
    eof      = op->eof;

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (status != CHIMERA_VFS_OK) {
        /* Could not enumerate (e.g. the dir handle lacks list access): fall back
         * to the plain rename rather than failing one that would have worked. */
        chimera_smb_set_info_rename_recall_free(request);
        chimera_smb_set_info_rename_do_rename(request);
        return;
    }

    if (!eof) {
        request->set_info.recall_readdir_cookie = cookie;
        chimera_smb_set_info_rename_recall_page(request, cookie, verifier);
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
} /* chimera_smb_set_info_rename_recall_page_complete */

/*
 * PUTHANDLE(source directory), READDIR(streaming) -- one sequence per page.
 *
 * A streaming READDIR reads ONE page and reports where it stopped, so a
 * directory larger than a page is walked by consecutive sequences, as it was by
 * consecutive readdirs before.  It stages nothing of the compound's: the
 * collection is the request's array, and `reset` winds it back before every
 * execution.
 */
static void
chimera_smb_set_info_rename_recall_page(
    struct chimera_smb_request *request,
    uint64_t                    cookie,
    uint64_t                    verifier)
{
    struct chimera_smb_open_file *open_file = request->set_info.open_file;

    /* The operating connection can disconnect while a child recall is parked
     * (the peer timed out); its teardown releases open_file->handle.  Abort the
     * scan cleanly rather than lending a NULL directory handle to the sequence
     * (recall_deny releases the open file and replies). */
    if (!open_file || !open_file->handle) {
        chimera_smb_set_info_rename_recall_deny(request);
        return;
    }

    /* Where the collection stands as this page is built: what the reset winds
     * back to. */
    request->set_info.recall_child_mark = request->set_info.recall_child_count;
    request->set_info.recall_deny_mark  = request->set_info.recall_deny;

    request->vfs_compound = chimera_vfs_compound_alloc(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred);

    chimera_vfs_compound_add_puthandle(request->vfs_compound,
                                       open_file->handle,
                                       open_file->open_flags);

    chimera_vfs_compound_add_readdir_stream(
        request->vfs_compound,
        cookie,
        verifier,
        CHIMERA_VFS_ATTR_FH, /* per-entry attr: need the child FH */
        0,                   /* dir_attr_mask                     */
        0,                   /* flags: no EMIT_DOT                */
        NULL, 0,             /* match everything                  */
        chimera_smb_set_info_rename_recall_reset,
        chimera_smb_set_info_rename_recall_append,
        request);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_set_info_rename_recall_page_complete,
                                request);
} /* chimera_smb_set_info_rename_recall_page */

/* Enumerate the source directory, collecting children that currently have a
 * live share holder.  Used for both the initial collect-and-recall pass and the
 * final race-detection re-scan (distinguished by recall_final). */
static void
chimera_smb_set_info_rename_recall_scan(struct chimera_smb_request *request)
{
    chimera_smb_set_info_rename_recall_free(request);

    chimera_smb_set_info_rename_recall_page(request, 0, 0);
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
static void
chimera_smb_set_info_rename_resolve_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request     = private_data;
    struct chimera_smb_rename_info       *rename_info = &request->set_info.rename_info;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    uint32_t                              nops, parent_index;
    uint32_t                              dest_mode = 0;
    int                                   dest_exists;

    status = chimera_vfs_compound_status(compound);
    nops   = chimera_vfs_compound_num_ops(compound);

    /* The op that established the destination parent: the path lookup when the
     * client named one, else the PUTFH of the tree root. */
    parent_index = nops - 2;

    if (status != CHIMERA_VFS_OK &&
        chimera_vfs_compound_num_completed(compound) < nops) {
        /* The run stopped BEFORE the destination lookup, so what failed is the
         * destination parent's path.  num_completed COUNTS the failing op, so
         * "it stopped early" is < nops and not <= the parent's index: the
         * lookup's own failure is the common "the name is not taken" answer and
         * leaves the count at nops. */
        chimera_vfs_compound_free(compound);
        request->vfs_compound = NULL;
        chimera_smb_open_file_release(request, request->set_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_PATH_NOT_FOUND);
        return;
    }

    op = chimera_vfs_compound_op(compound, parent_index);

    request->set_info.dst_parent_fh_len = op->fh_len;
    memcpy(request->set_info.dst_parent_fh, op->fh, op->fh_len);

    dest_exists = (status == CHIMERA_VFS_OK);

    if (dest_exists) {
        op        = chimera_vfs_compound_op(compound, nops - 1);
        dest_mode = op->attr.va_mode;
    }

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (dest_exists) {
        /* Destination exists.  A non-directory destination is overwritten only
         * when ReplaceIfExists is set (POSIX rename always sets it, so this
         * COLLISION only fires for an SMB caller that cleared it).  A directory
         * destination is left to rename_at, which enforces POSIX rename(2)
         * semantics: replace an empty directory, DIRECTORY_NOT_EMPTY otherwise,
         * and FILE_IS_A_DIRECTORY when the source is not itself a directory.
         * (The SMB "rename a file INTO a directory" shell behaviour is not
         * rename(2) and is deliberately not applied here.) */
        if (!S_ISDIR(dest_mode) && !rename_info->replace_if_exist) {
            chimera_smb_open_file_release(request, request->set_info.open_file);
            chimera_smb_complete_request(request,
                                         SMB2_STATUS_OBJECT_NAME_COLLISION);
            return;
        }
        /* Fall through: rename_at replaces the destination per POSIX. */
    }

    /* Destination doesn't exist or we're overwriting - proceed with rename.
     * For a directory rename, first break the handle leases of any files open
     * inside the source directory (smb2.lease.rename_dir_openfile). */
    chimera_smb_set_info_rename_recall_children(request);
} /* chimera_smb_set_info_rename_resolve_complete */

void
chimera_smb_set_info_rename_process(struct chimera_smb_request *request)
{
    struct chimera_smb_tree        *tree        = request->tree;
    struct chimera_smb_rename_info *rename_info = &request->set_info.rename_info;
    struct chimera_smb_open_file   *open_file   = request->set_info.open_file;

    /* MS-FSA 2.1.5.14.11.1 SetInfo(FileRenameInformation): if the handle was
     * not opened with DELETE access, the rename MUST fail with ACCESS_DENIED
     * (the rename removes the old name from its parent and adds a new one, so
     * the source handle's GrantedAccess must include DELETE).  Gate here, ahead
     * of the destination-existence probe -- otherwise a target collision would
     * surface as OBJECT_NAME_COLLISION and mask the real authorization error. */
    if (!(open_file->granted_access & SMB2_DELETE)) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    request->set_info.dst_parent_fh_len = 0;

    request->vfs_compound = chimera_vfs_compound_alloc(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred);

    chimera_vfs_compound_add_putfh(request->vfs_compound,
                                   tree->fh, tree->fh_len);

    if (rename_info->new_parent_len) {
        /* Moving to a different directory - resolve the new parent path */
        chimera_vfs_compound_add_lookup_path(request->vfs_compound,
                                             rename_info->new_parent,
                                             rename_info->new_parent_len,
                                             CHIMERA_VFS_ATTR_FH, 0);
    }

    chimera_vfs_compound_add_lookup(request->vfs_compound,
                                    rename_info->new_name,
                                    rename_info->new_name_len,
                                    CHIMERA_VFS_ATTR_MODE, 0);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_set_info_rename_resolve_complete,
                                request);
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
