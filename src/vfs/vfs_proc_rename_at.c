// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include "vfs_procs.h"
#include "vfs_state.h"
#include "vfs_internal.h"
#include "vfs_name_cache.h"
#include "vfs_notify.h"
#include "vfs_access.h"
#include "vfs_acl.h"
#include "common/macros.h"

static void
chimera_vfs_rename_at_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread       *thread     = request->thread;
    struct chimera_vfs_name_cache   *name_cache = thread->vfs->vfs_name_cache;
    chimera_vfs_rename_at_callback_t callback   = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        int cross_dir = (request->fh_len != request->rename_at.new_fhlen) ||
            memcmp(request->fh, request->rename_at.new_fh,
                   request->fh_len) != 0;

        if (!cross_dir) {
            /* Intra-directory rename: a single RENAMED event on the
             * directory carrying both old and new names.  The SMB
             * serializer expands this to FILE_ACTION_RENAMED_OLD_NAME
             * followed by FILE_ACTION_RENAMED_NEW_NAME per MS-FSCC. */
            chimera_vfs_notify_emit(thread->vfs->vfs_notify,
                                    request->fh,
                                    request->fh_len,
                                    CHIMERA_VFS_NOTIFY_RENAMED,
                                    request->rename_at.new_name,
                                    request->rename_at.new_namelen,
                                    request->rename_at.name,
                                    request->rename_at.namelen);
        } else {
            /* Cross-directory rename: source dir sees the OLD name
             * only (RENAMED_OLD_NAME record), destination sees the
             * NEW name only (RENAMED_NEW_NAME record).  Matches
             * Windows behavior and avoids reporting a name on the
             * source dir that exists in a different directory. */
            chimera_vfs_notify_emit(thread->vfs->vfs_notify,
                                    request->fh,
                                    request->fh_len,
                                    CHIMERA_VFS_NOTIFY_RENAMED,
                                    NULL, 0,
                                    request->rename_at.name,
                                    request->rename_at.namelen);
            chimera_vfs_notify_emit(thread->vfs->vfs_notify,
                                    request->rename_at.new_fh,
                                    request->rename_at.new_fhlen,
                                    CHIMERA_VFS_NOTIFY_RENAMED,
                                    request->rename_at.new_name,
                                    request->rename_at.new_namelen,
                                    NULL, 0);
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

static void
chimera_vfs_rename_at_dispatch(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
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
    chimera_vfs_rename_at_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, NULL, NULL, private_data);
        return;
    }

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
    request->rename_at.r_fromdir_post_attr.va_req_mask = post_attr_mask;
    request->rename_at.r_fromdir_post_attr.va_set_mask = 0;
    request->rename_at.r_todir_pre_attr.va_req_mask    = pre_attr_mask;
    request->rename_at.r_todir_pre_attr.va_set_mask    = 0;
    request->rename_at.r_todir_post_attr.va_req_mask   = post_attr_mask;
    request->rename_at.r_todir_post_attr.va_set_mask   = 0;
    request->proto_callback                            = callback;
    request->proto_private_data                        = private_data;

    /* If the rename overwrites an existing destination, recall any
     * delegation/oplock on that doomed file before it is replaced. */
    chimera_vfs_io_recall(request, target_fh, target_fh_len,
                          target_fh_len ? chimera_vfs_hash(target_fh, target_fh_len) : 0,
                          chimera_vfs_dispatch);
} /* chimera_vfs_rename_at_dispatch */

/*
 * Enforcement pre-step context for rename.  Three chained checks on
 * engine-authoritative backends:
 *   1. DELETE_CHILD on the source directory (remove the old name).
 *   2. ADD_FILE on the destination directory (create the new name).
 *   3. if an existing name is being replaced, delete_allowed on it.
 *
 * Limitation: the VFS rename signature does not carry the source object's FH,
 * so the POSIX sticky-bit owner check on the *source* directory cannot be
 * evaluated here (we authorize source removal by DELETE_CHILD alone).  This
 * only under-enforces world-writable sticky source directories; tracked as a
 * follow-up.  Likewise renaming a subdirectory is authorized via WRITE_DATA
 * rather than distinguishing APPEND_DATA on the destination.
 */
struct chimera_vfs_rename_at_gate {
    struct chimera_vfs_thread       *thread;
    const struct chimera_vfs_cred   *cred;
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
    uint64_t                         pre_attr_mask;
    uint64_t                         post_attr_mask;
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
    chimera_vfs_rename_at_dispatch(gate->thread, gate->cred, gate->fh,
                                   gate->fhlen, gate->name, gate->namelen,
                                   gate->new_fh, gate->new_fhlen,
                                   gate->new_name, gate->new_namelen,
                                   gate->target_fh, gate->target_fh_len,
                                   gate->pre_attr_mask, gate->post_attr_mask,
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

SYMBOL_EXPORT void
chimera_vfs_rename_at(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
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
    chimera_vfs_rename_at_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_module         *module;
    struct chimera_vfs_rename_at_gate *gate;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    if (module && chimera_vfs_gate_needed(module->capabilities, cred)) {
        gate                 = malloc(sizeof(*gate));
        gate->thread         = thread;
        gate->cred           = cred;
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
        gate->callback       = callback;
        gate->private_data   = private_data;

        chimera_vfs_gate_fh(thread, cred, fh, fhlen, CHIMERA_ACE_DELETE_CHILD,
                            chimera_vfs_rename_at_gate_src, gate);
        return;
    }

    chimera_vfs_rename_at_dispatch(thread, cred, fh, fhlen, name, namelen,
                                   new_fh, new_fhlen, new_name, new_namelen,
                                   target_fh, target_fh_len, pre_attr_mask,
                                   post_attr_mask, callback, private_data);
} /* chimera_vfs_rename_at */