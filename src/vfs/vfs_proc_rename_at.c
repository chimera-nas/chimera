// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include "vfs_procs.h"
#include "vfs_state.h"
#include "vfs_internal.h"
#include "vfs_name_cache.h"
#include "vfs_attr_cache.h"
#include "vfs_notify.h"
#include "vfs_access.h"
#include "vfs_acl.h"
#include "common/macros.h"

static void
chimera_vfs_rename_at_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread       *thread     = request->thread;
    struct chimera_vfs_name_cache   *name_cache = thread->vfs->vfs_name_cache;
    struct chimera_vfs_attr_cache   *attr_cache = thread->vfs->vfs_attr_cache;
    chimera_vfs_rename_at_callback_t callback   = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        int cross_dir = (request->fh_len != request->rename_at.new_fhlen) ||
            memcmp(request->fh, request->rename_at.new_fh,
                   request->fh_len) != 0;

        /* SMB3 directory-lease self-exemption: spare the directory lease named
         * by the operating open's ParentLeaseKey (set by the SMB rename path)
         * from the RENAMED break.  For a same-dir rename this spares the one
         * dir; for a cross-dir rename it spares whichever parent's lease the key
         * matches (the source, since the open lived there), naturally breaking
         * the other.  NULL caller (NFS/S3) breaks every directory lease. */
        uint64_t skip_lo = 0, skip_hi = 0;
        bool     has_skip = request->rename_at.parent_lease_skip_valid;

        if (has_skip) {
            memcpy(&skip_lo, request->rename_at.parent_lease_skip, 8);
            memcpy(&skip_hi, request->rename_at.parent_lease_skip + 8, 8);
        }

        if (!cross_dir) {
            /* Intra-directory rename: a single RENAMED event on the
             * directory carrying both old and new names. */
            chimera_vfs_notify_emit_lease(thread->vfs->vfs_notify,
                                          request->fh,
                                          request->fh_len,
                                          CHIMERA_VFS_NOTIFY_RENAMED,
                                          request->rename_at.new_name,
                                          request->rename_at.new_namelen,
                                          request->rename_at.name,
                                          request->rename_at.namelen,
                                          skip_lo, skip_hi, has_skip);
        } else {
            /* Cross-directory rename: source dir sees the OLD name only,
             * destination sees the NEW name only. */
            chimera_vfs_notify_emit_lease(thread->vfs->vfs_notify,
                                          request->fh,
                                          request->fh_len,
                                          CHIMERA_VFS_NOTIFY_RENAMED,
                                          NULL, 0,
                                          request->rename_at.name,
                                          request->rename_at.namelen,
                                          skip_lo, skip_hi, has_skip);
            chimera_vfs_notify_emit_lease(thread->vfs->vfs_notify,
                                          request->rename_at.new_fh,
                                          request->rename_at.new_fhlen,
                                          CHIMERA_VFS_NOTIFY_RENAMED,
                                          request->rename_at.new_name,
                                          request->rename_at.new_namelen,
                                          NULL, 0,
                                          skip_lo, skip_hi, has_skip);
        }

        /* Remove cache entries for both old and new paths.
         * We don't insert a negative entry for the old path because
         * if the source and destination are hard links to the same inode,
         * the backend may treat the rename as a no-op and leave both
         * paths valid. Inserting a negative entry would incorrectly
         * mark the old path as deleted. */

        chimera_vfs_name_cache_remove(name_cache,
                                      request->fh_hash,
                                      request->fh,
                                      request->fh_len,
                                      request->rename_at.name_hash,
                                      request->rename_at.name,
                                      request->rename_at.namelen);

        chimera_vfs_name_cache_remove(name_cache,
                                      request->rename_at.new_fh_hash,
                                      request->rename_at.new_fh,
                                      request->rename_at.new_fhlen,
                                      request->rename_at.new_name_hash,
                                      request->rename_at.new_name,
                                      request->rename_at.new_namelen);

        /* A rename mutates both parent directories' attributes (mtime/ctime,
         * and on a cross-directory directory move their link counts).  Refresh
         * the attr cache with the post-rename attributes the backend reported,
         * or a subsequent getattr/stat would serve a stale nlink/timestamp.
         * For a same-directory rename both FHs are identical and the second
         * insert simply re-inserts the same fresh entry. */
        chimera_vfs_attr_cache_insert(thread, attr_cache,
                                      request->fh_hash,
                                      request->fh,
                                      request->fh_len,
                                      &request->rename_at.r_fromdir_post_attr);

        chimera_vfs_attr_cache_insert(thread, attr_cache,
                                      request->rename_at.new_fh_hash,
                                      request->rename_at.new_fh,
                                      request->rename_at.new_fhlen,
                                      &request->rename_at.r_todir_post_attr);

        /* A cross-directory move of a directory re-homes its ".." entry to the
         * new parent.  The name cache keys ".." under the moved directory's own
         * FH (unchanged by the rename), so a ".." lookup cached before the move
         * would still resolve to the old parent.  Drop that entry; the backend
         * resolves ".." authoritatively from the moved directory on the next
         * lookup.  source_fh is the moved object's FH resolved during the
         * delegation-recall pre-step. */
        if (cross_dir && request->rename_at.source_fh_len > 0) {
            static const char dotdot[2] = { '.', '.' };

            chimera_vfs_name_cache_remove(name_cache,
                                          chimera_vfs_hash(request->rename_at.source_fh,
                                                           request->rename_at.source_fh_len),
                                          request->rename_at.source_fh,
                                          request->rename_at.source_fh_len,
                                          chimera_vfs_hash(dotdot, 2),
                                          dotdot,
                                          2);
        }
    }

    chimera_vfs_complete(request);

    callback(request->status,
             &request->rename_at.r_fromdir_pre_attr,
             &request->rename_at.r_fromdir_post_attr,
             &request->rename_at.r_todir_pre_attr,
             &request->rename_at.r_todir_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_rename_at_complete */

/*
 * Recall any delegation/oplock on the doomed destination (the file being
 * overwritten, if any) before it is replaced, then perform the rename.
 */
static void
chimera_vfs_rename_at_recall_target(struct chimera_vfs_request *request)
{
    chimera_vfs_io_recall(request,
                          request->rename_at.target_fh,
                          request->rename_at.target_fh_len,
                          request->rename_at.target_fh_len ?
                          chimera_vfs_hash(request->rename_at.target_fh,
                                           request->rename_at.target_fh_len) : 0,
                          0 /* namespace recall: revoke fully */,
                          chimera_vfs_dispatch);
} /* chimera_vfs_rename_at_recall_target */

/*
 * The source file is being moved: renaming it changes its ctime and directory
 * linkage, which invalidates a delegation holder's cached state, so recall any
 * delegation on it first (matching the Linux VFS, which breaks the source
 * lease on rename).  Then fall through to the destination recall.
 */
static void
chimera_vfs_rename_at_source_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (error_code == CHIMERA_VFS_OK && attr->va_fh_len) {
        memcpy(request->rename_at.source_fh, attr->va_fh, attr->va_fh_len);
        request->rename_at.source_fh_len = attr->va_fh_len;

        /* How hard to recall the source's caching holders depends on the caller:
         *
         *  - SMB (op_handle != NULL): the renamed file SURVIVES with its data
         *    intact -- only the name/handle binding is invalidated -- so an SMB
         *    v2 lease is broken with a SINGLE handle recall (RH->R), preserving
         *    the read cache.  A full revoke would cascade RH->R->NONE, sending
         *    two break notifications where the client expects one
         *    (smb2.lease.v2_rename).
         *
         *  - NFS / S3 (op_handle == NULL): an NFSv4 delegation on the source must
         *    be RECALLED outright on rename (RFC 7530 namespace recall) -- there
         *    is no "keep the read cache" downgrade for a delegation, and the
         *    explicit NFSv4-rename recall does not cover every backend/path -- so
         *    fall back to a full revoke (retain NONE).  Single-stepping a
         *    delegation to R here would leave it un-recalled and the client
         *    would never see CB_RECALL (pynfs DELEG6/17/18).
         *
         * The doomed DESTINATION (recall_target) is always destroyed and so is
         * always fully revoked. */
        if (request->io_handle) {
            chimera_vfs_io_recall_single(request,
                                         request->rename_at.source_fh,
                                         request->rename_at.source_fh_len,
                                         chimera_vfs_hash(request->rename_at.source_fh,
                                                          request->rename_at.source_fh_len),
                                         CHIMERA_VFS_LEASE_MODE_R,
                                         chimera_vfs_rename_at_recall_target);
        } else {
            chimera_vfs_io_recall(request,
                                  request->rename_at.source_fh,
                                  request->rename_at.source_fh_len,
                                  chimera_vfs_hash(request->rename_at.source_fh,
                                                   request->rename_at.source_fh_len),
                                  0 /* namespace recall: revoke fully */,
                                  chimera_vfs_rename_at_recall_target);
        }
    } else {
        /* Source not resolvable (e.g. ENOENT); the backend rename will return
         * the appropriate error.  Just recall the destination and proceed. */
        request->rename_at.source_fh_len = 0;
        chimera_vfs_rename_at_recall_target(request);
    }
} /* chimera_vfs_rename_at_source_lookup_complete */

static void
chimera_vfs_rename_at_dispatch(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_transaction  *txn,
    const void                      *fh,
    int                              fhlen,
    const char                      *name,
    int                              namelen,
    const void                      *new_fh,
    int                              new_fhlen,
    const char                      *new_name,
    int                              new_namelen,
    const uint8_t                   *target_fh,
    int                              target_fh_len,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    const uint8_t                   *parent_lease_skip,
    struct chimera_vfs_open_handle  *op_handle,
    chimera_vfs_rename_at_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, NULL, NULL, private_data);
        return;
    }

    request->transaction = txn;

    request->opcode                                    = CHIMERA_VFS_OP_RENAME_AT;
    request->complete                                  = chimera_vfs_rename_at_complete;
    request->rename_at.name                            = name;
    request->rename_at.namelen                         = namelen;
    request->rename_at.name_hash                       = chimera_vfs_hash(name, namelen);
    request->rename_at.new_fh                          = new_fh;
    request->rename_at.new_fhlen                       = new_fhlen;
    request->rename_at.new_fh_hash                     = chimera_vfs_hash(new_fh, new_fhlen);
    request->rename_at.new_name                        = new_name;
    request->rename_at.new_namelen                     = new_namelen;
    request->rename_at.new_name_hash                   = chimera_vfs_hash(new_name, new_namelen);
    request->rename_at.target_fh                       = target_fh;
    request->rename_at.target_fh_len                   = target_fh_len;
    request->rename_at.r_fromdir_pre_attr.va_req_mask  = pre_attr_mask;
    request->rename_at.r_fromdir_pre_attr.va_set_mask  = 0;
    request->rename_at.r_fromdir_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->rename_at.r_fromdir_post_attr.va_set_mask = 0;
    request->rename_at.r_todir_pre_attr.va_req_mask    = pre_attr_mask;
    request->rename_at.r_todir_pre_attr.va_set_mask    = 0;
    request->rename_at.r_todir_post_attr.va_req_mask   = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->rename_at.r_todir_post_attr.va_set_mask   = 0;
    request->proto_callback                            = callback;
    request->proto_private_data                        = private_data;
    request->rename_at.source_fh_len                   = 0;
    if (parent_lease_skip) {
        memcpy(request->rename_at.parent_lease_skip, parent_lease_skip, 16);
        request->rename_at.parent_lease_skip_valid = 1;
    } else {
        request->rename_at.parent_lease_skip_valid = 0;
    }
    /* Self-exempt the operating handle's own caching lease from the source-file
     * recall below: the renamer is coherent with its own rename, so renaming a
     * file it holds a lease on must not break that lease (the inode is unchanged;
     * MS-SMB2 / dirlease.rename).  A NULL caller (NFS/S3) recalls every holder,
     * preserving the RFC 7530 namespace-recall behaviour. */
    request->io_handle = op_handle;

    /* Recall delegations before the directory change: first on the source file
     * being moved (its ctime/linkage changes invalidate cached state), then on
     * any file overwritten at the destination.  Resolve the source FH only when
     * the lease subsystem is active; otherwise go straight to the destination
     * recall (which fast-paths to dispatch when there is nothing to break). */
    if (thread->vfs->vfs_state) {
        chimera_vfs_lookup(thread, cred, NULL, fh, fhlen, name, namelen,
                           CHIMERA_VFS_ATTR_FH, 0,
                           chimera_vfs_rename_at_source_lookup_complete,
                           request);
    } else {
        chimera_vfs_rename_at_recall_target(request);
    }
} /* chimera_vfs_rename_at_dispatch */

/*
 * Enforcement pre-step context for rename.  Chained checks on
 * engine-authoritative backends:
 *   1. delete_allowed on the source name (DELETE_CHILD on the source directory
 *      or DELETE on the source object, plus the POSIX sticky-bit owner rule).
 *      The source object's FH is resolved up front with a LOOKUP so the sticky
 *      owner check can run -- a sticky source directory (e.g. /tmp) only lets a
 *      non-owner rename an entry it owns.
 *   2. ADD_FILE/WRITE_DATA on the destination directory (create the new name).
 *   3. if an existing name is being replaced, delete_allowed on it (which also
 *      applies the sticky-bit rule to the destination directory).
 *
 * Note: renaming a subdirectory is authorized via WRITE_DATA rather than
 * distinguishing APPEND_DATA on the destination.
 */
struct chimera_vfs_rename_at_gate {
    struct chimera_vfs_thread       *thread;
    const struct chimera_vfs_cred   *cred;
    struct chimera_vfs_transaction  *txn;
    const void                      *fh;
    int                              fhlen;
    const char                      *name;
    int                              namelen;
    const void                      *new_fh;
    int                              new_fhlen;
    const char                      *new_name;
    int                              new_namelen;
    const uint8_t                   *target_fh;
    int                              target_fh_len;
    uint8_t                          src_child_fh[CHIMERA_VFS_FH_SIZE];
    int                              src_child_fh_len;
    uint64_t                         pre_attr_mask;
    uint64_t                         post_attr_mask;
    uint8_t                          parent_lease_skip[16];
    uint8_t                          parent_lease_skip_valid;
    struct chimera_vfs_open_handle  *op_handle;
    chimera_vfs_rename_at_callback_t callback;
    void                            *private_data;
};

static void
chimera_vfs_rename_at_gate_fail(
    struct chimera_vfs_rename_at_gate *gate,
    enum chimera_vfs_error             status)
{
    gate->callback(status, NULL, NULL, NULL, NULL, gate->private_data);
    free(gate);
} /* chimera_vfs_rename_at_gate_fail */

static void
chimera_vfs_rename_at_gate_dispatch(struct chimera_vfs_rename_at_gate *gate)
{
    chimera_vfs_rename_at_dispatch(gate->thread, gate->cred, gate->txn, gate->fh,
                                   gate->fhlen, gate->name, gate->namelen,
                                   gate->new_fh, gate->new_fhlen,
                                   gate->new_name, gate->new_namelen,
                                   gate->target_fh, gate->target_fh_len,
                                   gate->pre_attr_mask, gate->post_attr_mask,
                                   gate->parent_lease_skip_valid ?
                                   gate->parent_lease_skip : NULL,
                                   gate->op_handle,
                                   gate->callback, gate->private_data);
    free(gate);
} /* chimera_vfs_rename_at_gate_dispatch */

/* Step 3 complete: replaced-target delete authorized -> dispatch. */
static void
chimera_vfs_rename_at_gate_target(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_vfs_rename_at_gate *gate = private_data;

    if (status != CHIMERA_VFS_OK) {
        chimera_vfs_rename_at_gate_fail(gate, status);
        return;
    }
    chimera_vfs_rename_at_gate_dispatch(gate);
} /* chimera_vfs_rename_at_gate_target */

/* Step 2 complete: destination ADD authorized -> check replaced target. */
static void
chimera_vfs_rename_at_gate_dst(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_vfs_rename_at_gate *gate = private_data;

    if (status != CHIMERA_VFS_OK) {
        chimera_vfs_rename_at_gate_fail(gate, status);
        return;
    }

    if (gate->target_fh && gate->target_fh_len > 0) {
        chimera_vfs_gate_delete(gate->thread, gate->cred,
                                gate->new_fh, gate->new_fhlen,
                                gate->target_fh, gate->target_fh_len,
                                chimera_vfs_rename_at_gate_target, gate);
        return;
    }

    chimera_vfs_rename_at_gate_dispatch(gate);
} /* chimera_vfs_rename_at_gate_dst */

/* Step 1 complete: source DELETE_CHILD authorized -> check destination ADD. */
static void
chimera_vfs_rename_at_gate_src(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_vfs_rename_at_gate *gate = private_data;

    if (status != CHIMERA_VFS_OK) {
        chimera_vfs_rename_at_gate_fail(gate, status);
        return;
    }

    chimera_vfs_gate_fh(gate->thread, gate->cred, gate->new_fh, gate->new_fhlen,
                        CHIMERA_ACE_WRITE_DATA,
                        chimera_vfs_rename_at_gate_dst, gate);
} /* chimera_vfs_rename_at_gate_src */

/* Source name resolved -> authorize its removal (delete_allowed: DELETE_CHILD
 * on the source dir or DELETE on the object, plus the sticky-bit owner rule). */
static void
chimera_vfs_rename_at_gate_lookup(
    enum chimera_vfs_error    status,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_rename_at_gate *gate = private_data;

    if (status != CHIMERA_VFS_OK) {
        chimera_vfs_rename_at_gate_fail(gate, status);
        return;
    }

    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_FH) &&
        attr->va_fh_len > 0 && attr->va_fh_len <= CHIMERA_VFS_FH_SIZE) {
        memcpy(gate->src_child_fh, attr->va_fh, attr->va_fh_len);
        gate->src_child_fh_len = attr->va_fh_len;

        chimera_vfs_gate_delete(gate->thread, gate->cred,
                                gate->fh, gate->fhlen,
                                gate->src_child_fh, gate->src_child_fh_len,
                                chimera_vfs_rename_at_gate_src, gate);
        return;
    }

    /* No FH resolved: fall back to DELETE_CHILD on the source directory alone
     * (sticky owner check needs the object's attrs). */
    chimera_vfs_gate_fh(gate->thread, gate->cred, gate->fh, gate->fhlen,
                        CHIMERA_ACE_DELETE_CHILD,
                        chimera_vfs_rename_at_gate_src, gate);
} /* chimera_vfs_rename_at_gate_lookup */

SYMBOL_EXPORT void
chimera_vfs_rename_at(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_transaction  *txn,
    const void                      *fh,
    int                              fhlen,
    const char                      *name,
    int                              namelen,
    const void                      *new_fh,
    int                              new_fhlen,
    const char                      *new_name,
    int                              new_namelen,
    const uint8_t                   *target_fh,
    int                              target_fh_len,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    const uint8_t                   *parent_lease_skip,
    struct chimera_vfs_open_handle  *op_handle,
    chimera_vfs_rename_at_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_module         *module;
    struct chimera_vfs_rename_at_gate *gate;

    /* POSIX: renaming to/from "." or ".." is invalid (EINVAL); a final
     * component longer than {NAME_MAX} is ENAMETOOLONG. */
    if ((namelen == 1 && name[0] == '.') ||
        (namelen == 2 && name[0] == '.' && name[1] == '.') ||
        (new_namelen == 1 && new_name[0] == '.') ||
        (new_namelen == 2 && new_name[0] == '.' && new_name[1] == '.')) {
        callback(CHIMERA_VFS_EINVAL, NULL, NULL, NULL, NULL, private_data);
        return;
    }

    if (namelen >= CHIMERA_VFS_NAME_MAX || new_namelen >= CHIMERA_VFS_NAME_MAX) {
        callback(CHIMERA_VFS_ENAMETOOLONG, NULL, NULL, NULL, NULL, private_data);
        return;
    }

    /* POSIX rename(2): the source and destination must be on the same file
     * system.  A file handle's first CHIMERA_VFS_MOUNT_ID_SIZE bytes are its
     * mount id; differing source- and destination-directory mount ids mean the
     * rename would span mounts -> EXDEV (pjd rename/15). */
    if (fhlen >= CHIMERA_VFS_MOUNT_ID_SIZE && new_fhlen >= CHIMERA_VFS_MOUNT_ID_SIZE &&
        memcmp(fh, new_fh, CHIMERA_VFS_MOUNT_ID_SIZE) != 0) {
        callback(CHIMERA_VFS_EXDEV, NULL, NULL, NULL, NULL, private_data);
        return;
    }

    module = chimera_vfs_get_module(thread, fh, fhlen);

    if (module && chimera_vfs_gate_needed(module->capabilities, cred)) {
        gate                 = malloc(sizeof(*gate));
        gate->thread         = thread;
        gate->cred           = cred;
        gate->txn            = txn;
        gate->fh             = fh;
        gate->fhlen          = fhlen;
        gate->name           = name;
        gate->namelen        = namelen;
        gate->new_fh         = new_fh;
        gate->new_fhlen      = new_fhlen;
        gate->new_name       = new_name;
        gate->new_namelen    = new_namelen;
        gate->target_fh      = target_fh;
        gate->target_fh_len  = target_fh_len;
        gate->pre_attr_mask  = pre_attr_mask;
        gate->post_attr_mask = post_attr_mask;
        if (parent_lease_skip) {
            memcpy(gate->parent_lease_skip, parent_lease_skip, 16);
            gate->parent_lease_skip_valid = 1;
        } else {
            gate->parent_lease_skip_valid = 0;
        }
        gate->op_handle        = op_handle;
        gate->callback         = callback;
        gate->private_data     = private_data;
        gate->src_child_fh_len = 0;

        /* Resolve the source object's FH first so the sticky-bit owner check on
         * the source directory can be evaluated (no-follow: rename operates on
         * the name itself). */
        chimera_vfs_lookup(thread, cred, NULL, fh, fhlen, name, namelen,
                           CHIMERA_VFS_ATTR_FH, 0,
                           chimera_vfs_rename_at_gate_lookup, gate);
        return;
    }

    chimera_vfs_rename_at_dispatch(thread, cred, txn, fh, fhlen, name, namelen,
                                   new_fh, new_fhlen, new_name, new_namelen,
                                   target_fh, target_fh_len, pre_attr_mask,
                                   post_attr_mask, parent_lease_skip,
                                   op_handle, callback, private_data);
} /* chimera_vfs_rename_at */