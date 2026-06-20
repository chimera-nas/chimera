// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Attributes and metadata operations: getattr/setattr (including truncate),
 * serialized NFSv4/Windows ACL storage, inheritance and access checks,
 * extended attributes, the fh-routed KV store (SMB durable-handle state)
 * and pNFS block layouts.
 */

#include "diskfs_internal.h"

/* Forward declarations (definitions below, in call-graph order) */

static void
diskfs_getattr_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_setattr_trunc_done(
    struct chimera_vfs_request *request);

static void
diskfs_setattr_trunc_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_setattr_trunc_advance(
    struct chimera_vfs_request *request);

static void
diskfs_setattr_trunc_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_setattr_trunc_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_setattr_trunc_process(
    struct chimera_vfs_request *request);

static void
diskfs_setattr_trunc_first_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_setattr_finish(
    struct chimera_vfs_request *request);

static void
diskfs_setattr_pnfs_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_setattr_pnfs_insert(
    struct chimera_vfs_request *request);

static void
diskfs_setattr_pnfs_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_setattr_acl_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_setattr_acl_insert(
    struct chimera_vfs_request *request);

static void
diskfs_setattr_acl_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_setattr_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_mount_walk_dirent_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_mount_walk_acquired_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static int
diskfs_kv_key_in_range(
    const void *key,
    uint32_t    key_len,
    const void *start_key,
    uint32_t    start_key_len,
    const void *end_key,
    uint32_t    end_key_len,
    uint32_t    flags);

static inline int
diskfs_xattr_rec_matches(
    const struct diskfs_xattr_rec *rec,
    uint32_t                       rec_len,
    const char                    *name,
    uint32_t                       name_len);

static void
diskfs_get_xattr_lookup_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_get_xattr_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static inline struct diskfs_bt_key
diskfs_set_xattr_key(
    struct chimera_vfs_request *request);

static void
diskfs_set_xattr_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_set_xattr_insert(
    struct chimera_vfs_request *request);

static void
diskfs_set_xattr_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_set_xattr_lookup_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_set_xattr_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static int
diskfs_list_xattrs_consume(
    struct chimera_vfs_request *request,
    struct diskfs_bt_op        *op,
    int                         result);

static void
diskfs_list_xattrs_lookup_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_list_xattrs_step(
    struct chimera_vfs_request *request);

static void
diskfs_list_xattrs_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static inline struct diskfs_bt_key
diskfs_remove_xattr_key(
    struct chimera_vfs_request *request);

static void
diskfs_remove_xattr_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_remove_xattr_lookup_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_remove_xattr_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_layout_add_device(
    struct chimera_vfs_request *request,
    struct diskfs_shared       *shared,
    uint32_t                    device_id);

static void
diskfs_layout_emit(
    struct chimera_vfs_request *request,
    struct diskfs_shared       *shared,
    uint64_t                    file_off,
    uint64_t                    len,
    uint32_t                    device_id,
    uint64_t                    vol_off,
    uint32_t                    state);

static void
diskfs_get_layout_finish(
    struct chimera_vfs_request *request);

static void
diskfs_get_layout_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_get_layout_advance(
    struct chimera_vfs_request *request);

static void
diskfs_get_layout_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_get_layout_do_alloc(
    struct diskfs_thread *thread,
    void                 *arg);

static void
diskfs_get_layout_process(
    struct chimera_vfs_request *request);

static void
diskfs_get_layout_first_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_get_layout_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);


/* ------------------------------------------------------------------ */
/* ACLs: one serialized NFSv4/Windows ACL per inode, stored as the single
 * DISKFS_REC_ACL record in the inode's b+tree (mirrors the memfs/cairn
 * native-ACL stores).  The record is mirrored into inode->acl_serial when the
 * inode is faulted in (and kept coherent by the write paths under the inode
 * write lock), so reads -- attr mapping, inheritance, chmod regeneration --
 * never touch the tree; only the writes do, through the async b+tree ops. */

/*
 * Decode a serialized ACL record (serial[0..len), len<0 == no record) into a
 * per-thread scratch chimera_acl and point attr->va_acl at it.  When no ACL is
 * stored the ACL is synthesised from the mode, so a caller always sees an ACL
 * for an inode (identical to memfs/cairn).  The scratch is valid through the
 * synchronous completion that consumes it.
 */
void
diskfs_acl_decode_into(
    struct chimera_vfs_attrs *attr,
    const uint8_t            *serial,
    int                       len,
    uint32_t                  mode)
{
    static __thread uint8_t scratch[sizeof(struct chimera_acl) +
                                    CHIMERA_ACL_MAX_ACES * sizeof(struct chimera_ace)];
    struct chimera_acl     *dst = (struct chimera_acl *) scratch;

    if (len < 0 ||
        chimera_acl_deserialize((const char *) serial, len, dst,
                                CHIMERA_ACL_MAX_ACES) < 0) {
        chimera_acl_from_mode(mode, dst, CHIMERA_ACL_MAX_ACES);
    }

    attr->va_acl       = dst;
    attr->va_set_mask |= CHIMERA_VFS_ATTR_ACL;
} /* diskfs_acl_decode_into */


/* Mode/ACL-aware access check against a loaded diskfs inode (mirror of
 * memfs/cairn): build the canonical attrs (the stored ACL, or one synthesised
 * from the mode) and consult the shared access engine.  Reads the in-memory
 * ACL mirror (inode->acl_serial), so it never touches the b+tree. */
int
diskfs_inode_access(
    struct diskfs_thread          *thread,
    struct diskfs_inode           *inode,
    const struct chimera_vfs_cred *cred,
    uint32_t                       requested)
{
    struct chimera_vfs_attrs attr;

    attr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    attr.va_mode = inode->mode;
    attr.va_uid  = inode->uid;
    attr.va_gid  = inode->gid;

    (void) thread;
    diskfs_acl_decode_into(&attr, inode->acl_serial,
                           inode->acl_serial ? (int) inode->acl_serial_len : -1,
                           inode->mode);

    return chimera_vfs_access_allowed(&attr, cred, requested);
} /* diskfs_inode_access */


/* Replace inode->acl_serial (the in-memory mirror of the DISKFS_REC_ACL
 * record) with a copy of serial[0..len), or clear it when len < 0.  Caller
 * holds the inode write lock. */
void
diskfs_acl_serial_install(
    struct diskfs_inode *inode,
    const uint8_t       *serial,
    int                  len)
{
    free(inode->acl_serial);
    inode->acl_serial     = NULL;
    inode->acl_serial_len = 0;
    if (len >= 0) {
        inode->acl_serial = malloc(len);
        memcpy(inode->acl_serial, serial, len);
        inode->acl_serial_len = (uint32_t) len;
    }
} /* diskfs_acl_serial_install */


/*
 * Seed a freshly-created child's ACL (mirrors memfs/cairn).  Precedence:
 *   1. an explicit ACL supplied at create (e.g. an SMB SD via SecD) -> store
 *      it and re-derive the child mode (the caller snapshots new_acl from
 *      set_attr BEFORE diskfs_apply_attrs() rewrites va_set_mask);
 *   2. parent has ACEs inheritable for the child's type -> store the inherited
 *      ACL and re-derive the child mode from it (Windows inheritance);
 *   3. otherwise, for an SMB create (windows_default) -> store a Windows-style
 *      owner-full-control default DACL, leaving the POSIX mode intact;
 *   4. otherwise (NFS/POSIX create) -> no record, child stays mode-derived.
 * The parent's ACL is read from its in-memory mirror; only the child-record
 * insert touches the b+tree.  Returns 1 if it completed inline, 0 if the
 * insert suspended (cb fires with the result later), matching the b+tree op
 * convention.
 */
int
diskfs_inherit_acl_async(
    struct diskfs_bt_op      *op,
    struct diskfs_thread     *thread,
    struct diskfs_txn        *txn,
    struct diskfs_inode      *child,
    struct diskfs_inode      *parent,
    const struct chimera_acl *new_acl,
    int                       windows_default,
    diskfs_bt_cb_t            cb,
    void                     *private_data)
{
    int                       is_dir = S_ISDIR(child->mode);
    uint16_t                  want   = CHIMERA_ACE_FLAG_FILE_INHERIT |
        (is_dir ? CHIMERA_ACE_FLAG_DIR_INHERIT : 0);
    uint8_t                   abuf[sizeof(struct chimera_acl) +
                                   DISKFS_ACL_REC_MAX_ACES * sizeof(struct chimera_ace)];
    const struct chimera_acl *store       = NULL;
    int                       derive_mode = 0;

    if (new_acl && new_acl->num_aces) {
        store       = new_acl;
        derive_mode = 1;
    } else {
        uint8_t             pbuf[sizeof(struct chimera_acl) +
                                 DISKFS_ACL_REC_MAX_ACES * sizeof(struct chimera_ace)];
        struct chimera_acl *parent_acl = (struct chimera_acl *) pbuf;
        int                 has_inh    = 0;

        if (!parent->acl_serial ||
            chimera_acl_deserialize((const char *) parent->acl_serial,
                                    parent->acl_serial_len, parent_acl,
                                    DISKFS_ACL_REC_MAX_ACES) < 0) {
            parent_acl = NULL;
        }

        if (parent_acl) {
            for (unsigned i = 0; i < parent_acl->num_aces; i++) {
                if (parent_acl->aces[i].flags & want) {
                    has_inh = 1;
                    break;
                }
            }
        }

        if (has_inh) {
            struct chimera_acl *tmp = (struct chimera_acl *) abuf;
            int                 n   = chimera_acl_inherit(parent_acl, is_dir,
                                                          child->mode & 07777, tmp,
                                                          DISKFS_ACL_REC_MAX_ACES);

            if (n > 0) {
                store       = tmp;
                derive_mode = 1;
            }
            /* Nothing actually inherited: fall through to the default. */
        }

        if (!store && windows_default) {
            struct chimera_acl *def = (struct chimera_acl *) abuf;

            if (chimera_acl_default_acl(child->mode & 07777, def, 4) > 0) {
                store = def;
            }
        }
    }

    if (store && store->num_aces && store->num_aces <= DISKFS_ACL_REC_MAX_ACES) {
        uint8_t sbuf[DISKFS_ACL_REC_MAX];
        int     len = chimera_acl_serialize(store, sbuf, sizeof(sbuf));

        if (len >= 0) {
            if (derive_mode) {
                child->mode = (child->mode & S_IFMT) | chimera_acl_to_mode(store);
            }
            diskfs_acl_serial_install(child, sbuf, len);
            /* The child is brand new, so there is no existing record to
             * replace: a single insert suffices.  (It can still suspend --
             * the pre-descent split reservation may journal onto a cold
             * AG-log block.) */
            return diskfs_bt_insert_async(op, thread, txn, child,
                                          &diskfs_acl_key, sbuf, len,
                                          cb, private_data);
        }
    }

    /* Nothing to store: complete inline without touching the tree. */
    op->result = 0;
    return 1;
} /* diskfs_inherit_acl_async */


static void
diskfs_getattr_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    diskfs_map_attrs(p->thread, &request->getattr.r_attr, inode);

    diskfs_op_ok(request, p->txn);
} /* diskfs_getattr_inode_cb */


void
diskfs_getattr(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_READ);

    diskfs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              diskfs_getattr_inode_cb, request);
} /* diskfs_getattr */


static void
diskfs_setattr_trunc_done(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_inode           *inode  = p->inode_stash[0];

    /* POSIX: a successful (f)truncate marks the last data modification time.
     * Bump mtime unless the caller supplied an explicit mtime, or is an
     * AUTH_ATTR (SMB/Windows) caller managing the write time itself. */
    int                            bump_mtime = !(request->setattr.set_attr->va_set_mask &
                                                  CHIMERA_VFS_ATTR_MTIME) &&
        request->cred->flavor != CHIMERA_VFS_AUTH_ATTR;

    diskfs_apply_attrs(inode, request->setattr.set_attr);

    if (bump_mtime) {
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        inode->mtime_sec  = now.tv_sec;
        inode->mtime_nsec = now.tv_nsec;
    }

    diskfs_map_attrs(thread, &request->setattr.r_post_attr, inode);
    diskfs_op_ok(request, p->txn);
} /* diskfs_setattr_trunc_done */


static void
diskfs_setattr_trunc_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    p->loop_have = diskfs_ext_from_op(op, result, &p->ext_iter);
    diskfs_bt_op_free(p->thread, op);
    diskfs_setattr_trunc_process(request);
} /* diskfs_setattr_trunc_walk_cb */


static void
diskfs_setattr_trunc_advance(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    if (diskfs_ext_next_async(op, thread, p->inode_stash[0], p->ext_iter.file_offset,
                              p->rec_scratch, sizeof(p->rec_scratch),
                              diskfs_setattr_trunc_walk_cb, request)) {
        diskfs_setattr_trunc_walk_cb(op, op->result, request);
    }
} /* diskfs_setattr_trunc_advance */


static void
diskfs_setattr_trunc_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request *request = private_data;

    (void) result;
    diskfs_bt_op_free(((struct diskfs_request_private *) request->plugin_data)->thread, op);
    diskfs_setattr_trunc_advance(request);
} /* diskfs_setattr_trunc_inserted_cb */


/* A trimmed or removed extent's slot is gone; re-insert the trimmed head
 * (trim case) then advance, or just advance (full-remove case). */
static void
diskfs_setattr_trunc_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request  = private_data;
    struct diskfs_request_private *p        = request->plugin_data;
    struct diskfs_thread          *thread   = p->thread;
    uint64_t                       new_size = p->loop_off;

    (void) result;
    diskfs_bt_op_free(thread, op);

    if (p->ext_iter.file_offset + p->ext_iter.length > new_size &&
        p->ext_iter.file_offset < new_size) {
        /* Trim case: reinsert the surviving head [start, new_size). */
        uint64_t new_logical = new_size - p->ext_iter.file_offset;

        op = diskfs_bt_op_alloc(thread);
        {
            struct diskfs_extent_rec rec = {
                .length        = new_logical,
                .device_id     = p->ext_iter.device_id,
                .flags         = p->ext_iter.flags,
                .device_offset = p->ext_iter.device_offset,
            };
            struct diskfs_bt_key     key = diskfs_extent_key(p->ext_iter.file_offset);

            if (diskfs_bt_insert_async(op, thread, p->txn, p->inode_stash[0], &key,
                                       &rec, sizeof(rec),
                                       diskfs_setattr_trunc_inserted_cb, request)) {
                diskfs_setattr_trunc_inserted_cb(op, op->result, request);
            }
        }
        return;
    }

    diskfs_setattr_trunc_advance(request);
} /* diskfs_setattr_trunc_removed_cb */


static void
diskfs_setattr_trunc_process(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p        = request->plugin_data;
    struct diskfs_thread          *thread   = p->thread;
    uint64_t                       new_size = p->loop_off;
    uint64_t                       extent_start, extent_end;
    struct diskfs_bt_op           *op;

    if (!p->loop_have) {
        diskfs_setattr_trunc_done(request);
        return;
    }

    extent_start = p->ext_iter.file_offset;
    extent_end   = extent_start + p->ext_iter.length;

    if (extent_start >= new_size) {
        diskfs_thread_free_space(thread, p->txn, p->ext_iter.device_id,
                                 p->ext_iter.device_offset,
                                 SM_ALIGN_UP(p->ext_iter.length));
    } else if (extent_end > new_size) {
        uint64_t old_aligned = SM_ALIGN_UP(p->ext_iter.length);
        uint64_t new_logical = new_size - extent_start;
        uint64_t new_aligned = SM_ALIGN_UP(new_logical);

        if (old_aligned > new_aligned) {
            diskfs_thread_free_space(thread, p->txn, p->ext_iter.device_id,
                                     p->ext_iter.device_offset + new_aligned,
                                     old_aligned - new_aligned);
        }
    } else {
        /* Extent entirely within new size: nothing to do, just advance. */
        diskfs_setattr_trunc_advance(request);
        return;
    }

    /* Both the full-remove and trim cases start by removing the slot. */
    op = diskfs_bt_op_alloc(thread);
    {
        struct diskfs_bt_key key = diskfs_extent_key(extent_start);

        if (diskfs_bt_remove_async(op, thread, p->txn, p->inode_stash[0], &key,
                                   diskfs_setattr_trunc_removed_cb, request)) {
            diskfs_setattr_trunc_removed_cb(op, op->result, request);
        }
    }
} /* diskfs_setattr_trunc_process */


/* First-extent selection for truncation: floor(new_size), else first extent. */
static void
diskfs_setattr_trunc_first_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    int                            have    = diskfs_ext_from_op(op, result, &p->ext_iter);

    diskfs_bt_op_free(thread, op);

    if (!have) {
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_ceil_async(op, thread, p->inode_stash[0], 0, p->rec_scratch,
                                  sizeof(p->rec_scratch), diskfs_setattr_trunc_walk_cb,
                                  request)) {
            diskfs_setattr_trunc_walk_cb(op, op->result, request);
        }
        return;
    }

    p->loop_have = 1;
    diskfs_setattr_trunc_process(request);
} /* diskfs_setattr_trunc_first_cb */


/* Common setattr tail: map the post attrs and commit. */
static void
diskfs_setattr_finish(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p = request->plugin_data;

    diskfs_map_attrs(p->thread, &request->setattr.r_post_attr,
                     p->inode_stash[0]);
    diskfs_op_ok(request, p->txn);
} /* diskfs_setattr_finish */


/*
 * Completion for a setattr that also persisted a pNFS layout blob: the
 * DISKFS_REC_PNFS record is now in the b+tree, so map the post attrs and
 * commit.
 */
static void
diskfs_setattr_pnfs_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    diskfs_bt_op_free(p->thread, op);

    if (unlikely(result < 0)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EIO);
        return;
    }

    diskfs_setattr_finish(request);
} /* diskfs_setattr_pnfs_cb */


/* Insert the new layout record (its bytes already installed as the inode's
 * in-memory mirror by the dispatch below). */
static void
diskfs_setattr_pnfs_insert(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p     = request->plugin_data;
    struct diskfs_inode           *inode = p->inode_stash[0];
    struct diskfs_bt_op           *op    = diskfs_bt_op_alloc(p->thread);

    if (diskfs_bt_insert_async(op, p->thread, p->txn, inode, &diskfs_pnfs_key,
                               inode->pnfs_blob, inode->pnfs_blob_len,
                               diskfs_setattr_pnfs_cb, request)) {
        diskfs_setattr_pnfs_cb(op, op->result, request);
    }
} /* diskfs_setattr_pnfs_insert */


/* A replaced layout's old record is removed; insert the new one. */
static void
diskfs_setattr_pnfs_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_setattr_pnfs_insert(request);
} /* diskfs_setattr_pnfs_removed_cb */


/*
 * ACL-coherence chain steps (see the dispatch in diskfs_setattr_inode_cb):
 * the inode's in-memory ACL mirror already holds the chain's end state; the
 * steps replay it into the b+tree (remove the old record if there was one,
 * insert the new one if there is one).
 */
static void
diskfs_setattr_acl_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_setattr_finish(request);
} /* diskfs_setattr_acl_inserted_cb */


static void
diskfs_setattr_acl_insert(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p     = request->plugin_data;
    struct diskfs_inode           *inode = p->inode_stash[0];
    struct diskfs_bt_op           *op;

    if (!inode->acl_serial) {
        /* Remove-only chain (revert to mode-derived). */
        diskfs_setattr_finish(request);
        return;
    }

    op = diskfs_bt_op_alloc(p->thread);
    if (diskfs_bt_insert_async(op, p->thread, p->txn, inode, &diskfs_acl_key,
                               inode->acl_serial, inode->acl_serial_len,
                               diskfs_setattr_acl_inserted_cb, request)) {
        diskfs_setattr_acl_inserted_cb(op, op->result, request);
    }
} /* diskfs_setattr_acl_insert */


static void
diskfs_setattr_acl_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_setattr_acl_insert(request);
} /* diskfs_setattr_acl_removed_cb */


static void
diskfs_setattr_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_bt_op           *op;
    /* diskfs_apply_attrs() rewrites set_attr->va_set_mask (resets to ATOMIC,
     * re-adds only the scalar bits), dropping the ACL bit -- capture the
     * caller's original mask first. */
    uint64_t                       orig_mask = request->setattr.set_attr->va_set_mask;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if ((request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
        !S_ISREG(inode->mode)) {
        diskfs_op_fail(request, p->txn,
                       S_ISDIR(inode->mode) ?
                       CHIMERA_VFS_EISDIR : CHIMERA_VFS_EINVAL);
        return;
    }

    diskfs_map_attrs(thread, &request->setattr.r_pre_attr, inode);

    /* Handle truncation: remove/trim extents past new EOF. */
    if ((request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
        S_ISREG(inode->mode) &&
        request->setattr.set_attr->va_size < inode->size) {

        p->inode_stash[0] = inode;
        p->loop_off       = request->setattr.set_attr->va_size;

        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_floor_async(op, thread, inode, p->loop_off, p->rec_scratch,
                                   sizeof(p->rec_scratch), diskfs_setattr_trunc_first_cb,
                                   request)) {
            diskfs_setattr_trunc_first_cb(op, op->result, request);
        }
        return;
    }

    diskfs_apply_attrs(inode, request->setattr.set_attr);

    p->inode_stash[0] = inode;

    /* Persist the opaque pNFS layout blob as this inode's single PNFS record,
     * replacing any previous one (the insert aborts on a duplicate key).  The
     * in-memory mirror is installed up front; the chain replays it into the
     * tree. */
    if (request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_PNFS_LAYOUT) {
        uint32_t blob_len = request->setattr.set_attr->va_pnfs_len;
        int      had_old  = inode->pnfs_blob != NULL;

        if (blob_len > CHIMERA_VFS_PNFS_LAYOUT_MAX) {
            blob_len = CHIMERA_VFS_PNFS_LAYOUT_MAX;
        }

        free(inode->pnfs_blob);
        inode->pnfs_blob = malloc(blob_len);
        memcpy(inode->pnfs_blob, request->setattr.set_attr->va_pnfs, blob_len);
        inode->pnfs_blob_len = blob_len;

        if (had_old) {
            op = diskfs_bt_op_alloc(thread);
            if (diskfs_bt_remove_async(op, thread, p->txn, inode,
                                       &diskfs_pnfs_key,
                                       diskfs_setattr_pnfs_removed_cb,
                                       request)) {
                diskfs_setattr_pnfs_removed_cb(op, op->result, request);
            }
        } else {
            diskfs_setattr_pnfs_insert(request);
        }
        return;
    }

    /*
     * ACL coherence (mirrors memfs/cairn): an explicit ACL set is persisted
     * and the mode re-derived; a bare chmod regenerates the special-who ACEs
     * of any stored ACL while preserving named entries.  The new state is
     * computed from the in-memory mirrors here and installed immediately;
     * the async chain (remove the old record, insert the new one) replays it
     * into the b+tree before the post attrs are mapped.
     */
    if (orig_mask & CHIMERA_VFS_ATTR_ACL) {
        const struct chimera_acl *acl     = request->setattr.set_attr->va_acl;
        int                       had_old = inode->acl_serial != NULL;
        uint8_t                   sbuf[DISKFS_ACL_REC_MAX];
        int                       slen = -1;

        if (acl && acl->num_aces) {
            inode->mode = (inode->mode & S_IFMT) | chimera_acl_to_mode(acl);
        }
        if (acl && acl->num_aces && acl->num_aces <= DISKFS_ACL_REC_MAX_ACES) {
            slen = chimera_acl_serialize(acl, sbuf, sizeof(sbuf));
        }
        diskfs_acl_serial_install(inode, sbuf, slen);

        if (!had_old && slen < 0) {
            /* No record before, none now: nothing to replay. */
            diskfs_setattr_finish(request);
            return;
        }
        if (had_old) {
            op = diskfs_bt_op_alloc(thread);
            if (diskfs_bt_remove_async(op, thread, p->txn, inode,
                                       &diskfs_acl_key,
                                       diskfs_setattr_acl_removed_cb,
                                       request)) {
                diskfs_setattr_acl_removed_cb(op, op->result, request);
            }
        } else {
            diskfs_setattr_acl_insert(request);
        }
        return;
    } else if ((orig_mask & CHIMERA_VFS_ATTR_MODE) && inode->acl_serial) {
        uint8_t             obuf[sizeof(struct chimera_acl) +
                                 DISKFS_ACL_REC_MAX_ACES * sizeof(struct chimera_ace)];
        uint8_t             nbuf[sizeof(struct chimera_acl) +
                                 DISKFS_ACL_REC_MAX_ACES * sizeof(struct chimera_ace)];
        struct chimera_acl *old_acl = (struct chimera_acl *) obuf;
        struct chimera_acl *new_acl = (struct chimera_acl *) nbuf;
        uint8_t             sbuf[DISKFS_ACL_REC_MAX];
        int                 slen;

        if (chimera_acl_deserialize((const char *) inode->acl_serial,
                                    inode->acl_serial_len, old_acl,
                                    DISKFS_ACL_REC_MAX_ACES) >= 0 &&
            chimera_acl_chmod(old_acl, inode->mode, new_acl,
                              DISKFS_ACL_REC_MAX_ACES) >= 0 &&
            (slen = chimera_acl_serialize(new_acl, sbuf, sizeof(sbuf))) >= 0) {

            diskfs_acl_serial_install(inode, sbuf, slen);

            op = diskfs_bt_op_alloc(thread);
            if (diskfs_bt_remove_async(op, thread, p->txn, inode,
                                       &diskfs_acl_key,
                                       diskfs_setattr_acl_removed_cb,
                                       request)) {
                diskfs_setattr_acl_removed_cb(op, op->result, request);
            }
            return;
        }
        /* Regeneration failed: leave the stored ACL untouched. */
    }

    diskfs_setattr_finish(request);
} /* diskfs_setattr_inode_cb */


void
diskfs_setattr(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    diskfs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              diskfs_setattr_inode_cb, request);
} /* diskfs_setattr */


static void
diskfs_mount_walk_dirent_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_dirent_rec      *rec     = (struct diskfs_dirent_rec *) p->rec_scratch;
    uint64_t                       child_inum;
    uint32_t                       child_gen;

    diskfs_bt_op_free(p->thread, op);

    if (result < 0) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }
    child_inum = rec->inum;
    child_gen  = rec->gen;

    /* Done descending the parent; release its read lock so a deep walk reuses
     * the slot, then fetch the child. */
    diskfs_txn_unlock_inode(p->txn, p->inode_stash[0]);

    diskfs_inode_get_inum_async(p->thread, p->txn, child_inum, child_gen,
                                diskfs_mount_walk_acquired_cb, request);
} /* diskfs_mount_walk_dirent_cb */


static void
diskfs_mount_walk_acquired_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    const char                    *path    = request->mount.path;
    int                            pathlen = request->mount.pathlen;
    const char                    *pathc, *name, *slash;
    int                            namelen;
    uint64_t                       hash;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    pathc = path + p->op_scratch;
    while (pathc < path + pathlen && *pathc == '/') {
        pathc++;
    }

    if (pathc >= path + pathlen) {
        /* Fully resolved. */
        diskfs_map_attrs(thread, &request->mount.r_attr, inode);
        diskfs_op_ok(request, p->txn);
        return;
    }

    if (unlikely(!S_ISDIR(inode->mode))) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    slash         = memchr(pathc, '/', (size_t) (path + pathlen - pathc));
    name          = pathc;
    namelen       = slash ? (int) (slash - pathc) : (int) (path + pathlen - pathc);
    p->op_scratch = (uint32_t) ((pathc + namelen) - path);

    hash              = chimera_vfs_hash(name, namelen);
    p->inode_stash[0] = inode;     /* parent for the dirent lookup */

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(op, thread, inode, hash, p->rec_scratch,
                                sizeof(p->rec_scratch),
                                diskfs_mount_walk_dirent_cb, request)) {
        diskfs_mount_walk_dirent_cb(op, op->result, request);
    }
} /* diskfs_mount_walk_acquired_cb */


void
diskfs_mount(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;
    uint64_t                       inum;
    uint32_t                       gen;

    (void) private_data;

    /* Resume any inode drains left pending by a crash during mount, before
     * the export becomes usable.  Fresh bootstrap creates an empty orphan list
     * and marks it scanned. */
    if (unlikely(!shared->orphans_scanned)) {
        diskfs_orphan_scan(thread);
    }
    p->thread     = thread;
    p->txn        = diskfs_txn_begin(thread, DISKFS_TXN_READ);
    p->op_scratch = 0;

    /* Resolve the mount path asynchronously starting from the root inode. */
    diskfs_fh_to_inum(&inum, &gen, shared->root_fh, shared->root_fhlen);
    diskfs_inode_get_inum_async(thread, p->txn, inum, gen,
                                diskfs_mount_walk_acquired_cb, request);
} /* diskfs_mount */


void
diskfs_umount(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_READ);
    diskfs_op_ok(request, p->txn);
} /* diskfs_umount */


/* inode_stash[0] = parent dir (locked across child fetch) */

void
diskfs_lookup_at_child_cb(
    struct diskfs_inode *child,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    diskfs_map_attrs(p->thread, &request->lookup_at.r_attr, child);

    diskfs_op_ok(request, p->txn);
} /* diskfs_lookup_at_child_cb */



void
diskfs_put_key(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;
    uint64_t                       hash;
    int                            shard_idx;
    struct diskfs_kv_shard        *shard;
    struct diskfs_kv_entry        *entry, *existing;

    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    hash      = chimera_vfs_hash(request->put_key.key, request->put_key.key_len);
    shard_idx = hash % shared->num_kv_shards;
    shard     = &shared->kv_shards[shard_idx];

    pthread_mutex_lock(&shard->lock);

    rb_tree_query_exact(&shard->entries, hash, hash, existing);

    if (existing) {
        slab_allocator_free(thread->allocator, existing->value, existing->value_len);
        existing->value_len = request->put_key.value_len;
        existing->value     = slab_allocator_alloc(thread->allocator, request->put_key.value_len);
        memcpy(existing->value, request->put_key.value, request->put_key.value_len);
    } else {
        entry = diskfs_kv_entry_alloc(thread, hash,
                                      request->put_key.key,
                                      request->put_key.key_len,
                                      request->put_key.value,
                                      request->put_key.value_len);
        rb_tree_insert(&shard->entries, hash, entry);
    }

    pthread_mutex_unlock(&shard->lock);

    diskfs_op_ok(request, p->txn);
} /* diskfs_put_key */


void
diskfs_get_key(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;
    uint64_t                       hash;
    int                            shard_idx;
    struct diskfs_kv_shard        *shard;
    struct diskfs_kv_entry        *entry;

    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_READ);

    hash      = chimera_vfs_hash(request->get_key.key, request->get_key.key_len);
    shard_idx = hash % shared->num_kv_shards;
    shard     = &shared->kv_shards[shard_idx];

    pthread_mutex_lock(&shard->lock);

    rb_tree_query_exact(&shard->entries, hash, hash, entry);

    if (!entry) {
        pthread_mutex_unlock(&shard->lock);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    request->get_key.r_value     = entry->value;
    request->get_key.r_value_len = entry->value_len;

    pthread_mutex_unlock(&shard->lock);

    diskfs_op_ok(request, p->txn);
} /* diskfs_get_key */


void
diskfs_delete_key(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;
    uint64_t                       hash;
    int                            shard_idx;
    struct diskfs_kv_shard        *shard;
    struct diskfs_kv_entry        *entry;

    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    hash      = chimera_vfs_hash(request->delete_key.key, request->delete_key.key_len);
    shard_idx = hash % shared->num_kv_shards;
    shard     = &shared->kv_shards[shard_idx];

    pthread_mutex_lock(&shard->lock);

    rb_tree_query_exact(&shard->entries, hash, hash, entry);

    if (!entry) {
        pthread_mutex_unlock(&shard->lock);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    rb_tree_remove(&shard->entries, &entry->node);
    pthread_mutex_unlock(&shard->lock);

    diskfs_kv_entry_free(thread, entry);

    diskfs_op_ok(request, p->txn);
} /* diskfs_delete_key */


static int
diskfs_kv_key_in_range(
    const void *key,
    uint32_t    key_len,
    const void *start_key,
    uint32_t    start_key_len,
    const void *end_key,
    uint32_t    end_key_len,
    uint32_t    flags)
{
    int cmp;

    /* Compare key to start_key */
    if (start_key_len > 0) {
        cmp = memcmp(key, start_key,
                     key_len < start_key_len ? key_len : start_key_len);
        if (cmp < 0 || (cmp == 0 && key_len < start_key_len)) {
            return 0; /* key < start_key */
        }
    }

    /* Compare key to end_key */
    if (end_key_len > 0) {
        cmp = memcmp(key, end_key,
                     key_len < end_key_len ? key_len : end_key_len);
        if (cmp > 0 || (cmp == 0 && key_len > end_key_len)) {
            return 0; /* key > end_key */
        }
        if ((flags & CHIMERA_VFS_SEARCH_KEYS_END_EXCLUSIVE) &&
            cmp == 0 && key_len == end_key_len) {
            return 0; /* key == end_key, end is exclusive */
        }
    }

    return 1; /* key is in range */
} /* diskfs_kv_key_in_range */


/* One matched key/value, copied out from under the shard lock so the search
 * can sort across shards and invoke the callback without holding any lock. */
struct diskfs_search_item {
    void    *key;
    uint32_t key_len;
    void    *value;
    uint32_t value_len;
};

static int
diskfs_search_item_cmp(
    const void *a,
    const void *b)
{
    const struct diskfs_search_item *ia  = a;
    const struct diskfs_search_item *ib  = b;
    uint32_t                         min = ia->key_len < ib->key_len ? ia->key_len : ib->key_len;
    int                              cmp = memcmp(ia->key, ib->key, min);

    if (cmp) {
        return cmp;
    }
    /* Shorter key (a byte-prefix of the other) sorts first. */
    return (ia->key_len > ib->key_len) - (ia->key_len < ib->key_len);
} /* diskfs_search_item_cmp */

void
diskfs_search_keys(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private     *p = request->plugin_data;
    int                                i;
    size_t                             n = 0, cap = 0, k;
    struct diskfs_kv_shard            *shard;
    struct diskfs_kv_entry            *entry;
    struct diskfs_search_item         *items = NULL, *grown;
    chimera_vfs_search_keys_callback_t callback = request->search_keys.callback;

    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_READ);

    /* Entries are stored hash-ordered (sharded by hash), so collect every
     * in-range match across all shards, then sort by key to return results in
     * the backend's natural (lexicographic) key order.  Keys and values are
     * copied so the callback runs without any shard lock held. */
    for (i = 0; i < shared->num_kv_shards; i++) {
        shard = &shared->kv_shards[i];

        pthread_mutex_lock(&shard->lock);

        rb_tree_first(&shard->entries, entry);

        while (entry) {
            if (diskfs_kv_key_in_range(entry->key,
                                       entry->key_len,
                                       request->search_keys.start_key,
                                       request->search_keys.start_key_len,
                                       request->search_keys.end_key,
                                       request->search_keys.end_key_len,
                                       request->search_keys.flags)) {
                struct diskfs_search_item *item;

                if (n == cap) {
                    cap   = cap ? cap * 2 : 16;
                    grown = realloc(items, cap * sizeof(*items));
                    if (!grown) {
                        pthread_mutex_unlock(&shard->lock);
                        goto enomem;
                    }
                    items = grown;
                }

                item            = &items[n];
                item->key_len   = entry->key_len;
                item->value_len = entry->value_len;
                item->key       = malloc(entry->key_len);
                item->value     = entry->value_len ? malloc(entry->value_len) : NULL;

                if (!item->key || (entry->value_len && !item->value)) {
                    free(item->key);
                    free(item->value);
                    pthread_mutex_unlock(&shard->lock);
                    goto enomem;
                }

                memcpy(item->key, entry->key, entry->key_len);
                if (entry->value_len) {
                    memcpy(item->value, entry->value, entry->value_len);
                }
                n++;
            }

            entry = rb_tree_next(&shard->entries, entry);
        }

        pthread_mutex_unlock(&shard->lock);
    }

    if (n > 1) {
        qsort(items, n, sizeof(*items), diskfs_search_item_cmp);
    }

    for (k = 0; k < n; k++) {
        if (callback(items[k].key, items[k].key_len,
                     items[k].value, items[k].value_len,
                     request->proto_private_data)) {
            break; /* caller aborted the search */
        }
    }

    for (k = 0; k < n; k++) {
        free(items[k].key);
        free(items[k].value);
    }
    free(items);

    diskfs_op_ok(request, p->txn);
    return;

 enomem:
    for (k = 0; k < n; k++) {
        free(items[k].key);
        free(items[k].value);
    }
    free(items);

    diskfs_op_fail(request, p->txn, CHIMERA_VFS_EIO);
} /* diskfs_search_keys */


static inline int
diskfs_xattr_rec_matches(
    const struct diskfs_xattr_rec *rec,
    uint32_t                       rec_len,
    const char                    *name,
    uint32_t                       name_len)
{
    return rec_len >= sizeof(*rec) &&
           rec->name_len == name_len &&
           rec_len >= sizeof(*rec) + rec->name_len + rec->value_len &&
           memcmp(rec->data, name, name_len) == 0;
} /* diskfs_xattr_rec_matches */


static void
diskfs_get_xattr_lookup_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_xattr_rec       *rec     = p->xattr_rec;

    diskfs_bt_op_free(p->thread, op);

    if (result < 0 ||
        !diskfs_xattr_rec_matches(rec, result, request->get_xattr.name,
                                  request->get_xattr.namelen)) {
        free(p->xattr_rec);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENODATA);
        return;
    }

    if (rec->value_len > request->get_xattr.value_maxlen) {
        free(p->xattr_rec);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ERANGE);
        return;
    }

    memcpy(request->get_xattr.value, rec->data + rec->name_len,
           rec->value_len);
    request->get_xattr.r_value_len = rec->value_len;
    free(p->xattr_rec);
    diskfs_op_ok(request, p->txn);
} /* diskfs_get_xattr_lookup_cb */


static void
diskfs_get_xattr_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_bt_key           key;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    key.type   = DISKFS_REC_XATTR;
    key.subkey = chimera_vfs_hash(request->get_xattr.name,
                                  request->get_xattr.namelen);

    p->xattr_rec = malloc(DISKFS_BT_NODE_CAP);
    op           = diskfs_bt_op_alloc(p->thread);
    if (diskfs_bt_lookup_async(op, p->thread, inode,
                               DISKFS_BT_OP_LOOKUP_EXACT, &key, NULL,
                               p->xattr_rec, DISKFS_BT_NODE_CAP,
                               diskfs_get_xattr_lookup_cb, request)) {
        diskfs_get_xattr_lookup_cb(op, op->result, request);
    }
} /* diskfs_get_xattr_inode_cb */


void
diskfs_get_xattr(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_READ);

    diskfs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              diskfs_get_xattr_inode_cb, request);
} /* diskfs_get_xattr */


static inline struct diskfs_bt_key
diskfs_set_xattr_key(struct chimera_vfs_request *request)
{
    struct diskfs_bt_key key = {
        .type   = DISKFS_REC_XATTR,
        .subkey = chimera_vfs_hash(request->set_xattr.name,
                                   request->set_xattr.namelen),
    };

    return key;
} /* diskfs_set_xattr_key */


static void
diskfs_set_xattr_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *inode   = p->inode_stash[0];
    struct timespec                now;

    (void) result;
    diskfs_bt_op_free(p->thread, op);

    clock_gettime(CLOCK_REALTIME, &now);
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;
    inode->change++;

    diskfs_map_attrs(p->thread, &request->set_xattr.r_post_attr, inode);
    diskfs_op_ok(request, p->txn);
} /* diskfs_set_xattr_inserted_cb */


static void
diskfs_set_xattr_insert(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p   = request->plugin_data;
    struct diskfs_bt_key           key = diskfs_set_xattr_key(request);
    struct diskfs_xattr_rec       *new_rec;
    uint32_t                       rec_len;
    struct diskfs_bt_op           *op;
    int                            r;

    rec_len = sizeof(*new_rec) + request->set_xattr.namelen +
        request->set_xattr.value_len;
    new_rec            = malloc(rec_len);
    new_rec->name_len  = request->set_xattr.namelen;
    new_rec->value_len = request->set_xattr.value_len;
    memcpy(new_rec->data, request->set_xattr.name, request->set_xattr.namelen);
    memcpy(new_rec->data + request->set_xattr.namelen,
           request->set_xattr.value, request->set_xattr.value_len);

    /* The insert stages the record into op-owned storage up front, so the
     * staging buffer can be freed as soon as the call returns -- even if the
     * op suspended. */
    op = diskfs_bt_op_alloc(p->thread);
    r  = diskfs_bt_insert_async(op, p->thread, p->txn, p->inode_stash[0],
                                &key, new_rec, rec_len,
                                diskfs_set_xattr_inserted_cb, request);
    free(new_rec);
    if (r) {
        diskfs_set_xattr_inserted_cb(op, op->result, request);
    }
} /* diskfs_set_xattr_insert */


static void
diskfs_set_xattr_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_set_xattr_insert(request);
} /* diskfs_set_xattr_removed_cb */


static void
diskfs_set_xattr_lookup_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_xattr_rec       *old_rec = p->xattr_rec;

    diskfs_bt_op_free(p->thread, op);

    if (result >= 0 &&
        !diskfs_xattr_rec_matches(old_rec, result, request->set_xattr.name,
                                  request->set_xattr.namelen)) {
        free(p->xattr_rec);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
        return;
    }

    if (result >= 0) {
        if (request->set_xattr.option == CHIMERA_VFS_XATTR_CREATE) {
            free(p->xattr_rec);
            diskfs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
            return;
        }
        free(p->xattr_rec);
        {
            struct diskfs_bt_key key = diskfs_set_xattr_key(request);

            op = diskfs_bt_op_alloc(p->thread);
            if (diskfs_bt_remove_async(op, p->thread, p->txn,
                                       p->inode_stash[0], &key,
                                       diskfs_set_xattr_removed_cb, request)) {
                diskfs_set_xattr_removed_cb(op, op->result, request);
            }
        }
        return;
    }

    free(p->xattr_rec);
    if (request->set_xattr.option == CHIMERA_VFS_XATTR_REPLACE) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENODATA);
        return;
    }

    diskfs_set_xattr_insert(request);
} /* diskfs_set_xattr_lookup_cb */


static void
diskfs_set_xattr_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_bt_key           key;
    uint32_t                       rec_len;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    rec_len = sizeof(struct diskfs_xattr_rec) + request->set_xattr.namelen +
        request->set_xattr.value_len;
    if (rec_len > DISKFS_XATTR_REC_MAX) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EFBIG);
        return;
    }

    diskfs_map_attrs(p->thread, &request->set_xattr.r_pre_attr, inode);

    p->inode_stash[0] = inode;

    key          = diskfs_set_xattr_key(request);
    p->xattr_rec = malloc(DISKFS_BT_NODE_CAP);
    op           = diskfs_bt_op_alloc(p->thread);
    if (diskfs_bt_lookup_async(op, p->thread, inode,
                               DISKFS_BT_OP_LOOKUP_EXACT, &key, NULL,
                               p->xattr_rec, DISKFS_BT_NODE_CAP,
                               diskfs_set_xattr_lookup_cb, request)) {
        diskfs_set_xattr_lookup_cb(op, op->result, request);
    }
} /* diskfs_set_xattr_inode_cb */


void
diskfs_set_xattr(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    diskfs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              diskfs_set_xattr_inode_cb, request);
} /* diskfs_set_xattr */


/* Consume one completed lookup; returns 1 if the request was finalized. */
static int
diskfs_list_xattrs_consume(
    struct chimera_vfs_request *request,
    struct diskfs_bt_op        *op,
    int                         result)
{
    struct diskfs_request_private *p     = request->plugin_data;
    struct diskfs_xattr_rec       *rec   = p->xattr_rec;
    uint8_t                       *buf   = request->list_xattrs.buffer;
    struct diskfs_bt_key           found = op->found_key;

    diskfs_bt_op_free(p->thread, op);

    if (result < 0 || found.type != DISKFS_REC_XATTR) {
        request->list_xattrs.r_len    = (uint32_t) p->loop_pos;
        request->list_xattrs.r_count  = (uint32_t) p->loop_left;
        request->list_xattrs.r_eof    = 1;
        request->list_xattrs.r_cookie = 0;
        free(p->xattr_rec);
        diskfs_op_ok(request, p->txn);
        return 1;
    }

    if (result < (int) sizeof(*rec) ||
        result < (int) (sizeof(*rec) + rec->name_len + rec->value_len)) {
        free(p->xattr_rec);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EIO);
        return 1;
    }
    if (p->loop_pos + rec->name_len + 1 > request->list_xattrs.max_bytes) {
        free(p->xattr_rec);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ERANGE);
        return 1;
    }

    memcpy(buf + p->loop_pos, rec->data, rec->name_len);
    p->loop_pos       += rec->name_len;
    buf[p->loop_pos++] = '\0';
    p->loop_left++;

    if (found.subkey == UINT64_MAX) {
        request->list_xattrs.r_len    = (uint32_t) p->loop_pos;
        request->list_xattrs.r_count  = (uint32_t) p->loop_left;
        request->list_xattrs.r_eof    = 1;
        request->list_xattrs.r_cookie = 0;
        free(p->xattr_rec);
        diskfs_op_ok(request, p->txn);
        return 1;
    }

    p->loop_off = found.subkey + 1;
    return 0;
} /* diskfs_list_xattrs_consume */


static void
diskfs_list_xattrs_lookup_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (diskfs_list_xattrs_consume(request, op, result)) {
        return;
    }
    diskfs_list_xattrs_step(request);
} /* diskfs_list_xattrs_lookup_cb */


static void
diskfs_list_xattrs_step(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p = request->plugin_data;

    for (;; ) {
        struct diskfs_bt_key key = {
            .type = DISKFS_REC_XATTR, .subkey = p->loop_off
        };
        struct diskfs_bt_op *op = diskfs_bt_op_alloc(p->thread);

        if (!diskfs_bt_lookup_async(op, p->thread, p->inode_stash[0],
                                    DISKFS_BT_OP_LOOKUP_GE, &key,
                                    &op->found_key, p->xattr_rec,
                                    DISKFS_BT_NODE_CAP,
                                    diskfs_list_xattrs_lookup_cb, request)) {
            return;     /* suspended; the callback resumes the walk */
        }
        if (diskfs_list_xattrs_consume(request, op, op->result)) {
            return;
        }
    }
} /* diskfs_list_xattrs_step */


static void
diskfs_list_xattrs_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    p->inode_stash[0] = inode;
    p->xattr_rec      = malloc(DISKFS_BT_NODE_CAP);
    p->loop_off       = 0;
    p->loop_pos       = 0;
    p->loop_left      = 0;

    diskfs_list_xattrs_step(request);
} /* diskfs_list_xattrs_inode_cb */


void
diskfs_list_xattrs(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_READ);

    diskfs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              diskfs_list_xattrs_inode_cb, request);
} /* diskfs_list_xattrs */


static inline struct diskfs_bt_key
diskfs_remove_xattr_key(struct chimera_vfs_request *request)
{
    struct diskfs_bt_key key = {
        .type   = DISKFS_REC_XATTR,
        .subkey = chimera_vfs_hash(request->remove_xattr.name,
                                   request->remove_xattr.namelen),
    };

    return key;
} /* diskfs_remove_xattr_key */


static void
diskfs_remove_xattr_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *inode   = p->inode_stash[0];
    struct timespec                now;

    (void) result;
    diskfs_bt_op_free(p->thread, op);

    clock_gettime(CLOCK_REALTIME, &now);
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;
    inode->change++;

    diskfs_map_attrs(p->thread, &request->remove_xattr.r_post_attr, inode);
    diskfs_op_ok(request, p->txn);
} /* diskfs_remove_xattr_removed_cb */


static void
diskfs_remove_xattr_lookup_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_xattr_rec       *rec     = p->xattr_rec;
    struct diskfs_bt_key           key     = diskfs_remove_xattr_key(request);

    diskfs_bt_op_free(p->thread, op);

    if (result < 0 ||
        !diskfs_xattr_rec_matches(rec, result, request->remove_xattr.name,
                                  request->remove_xattr.namelen)) {
        free(p->xattr_rec);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENODATA);
        return;
    }
    free(p->xattr_rec);

    op = diskfs_bt_op_alloc(p->thread);
    if (diskfs_bt_remove_async(op, p->thread, p->txn, p->inode_stash[0],
                               &key, diskfs_remove_xattr_removed_cb,
                               request)) {
        diskfs_remove_xattr_removed_cb(op, op->result, request);
    }
} /* diskfs_remove_xattr_lookup_cb */


static void
diskfs_remove_xattr_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_bt_key           key     = diskfs_remove_xattr_key(request);
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    diskfs_map_attrs(p->thread, &request->remove_xattr.r_pre_attr, inode);

    p->inode_stash[0] = inode;

    p->xattr_rec = malloc(DISKFS_BT_NODE_CAP);
    op           = diskfs_bt_op_alloc(p->thread);
    if (diskfs_bt_lookup_async(op, p->thread, inode,
                               DISKFS_BT_OP_LOOKUP_EXACT, &key, NULL,
                               p->xattr_rec, DISKFS_BT_NODE_CAP,
                               diskfs_remove_xattr_lookup_cb, request)) {
        diskfs_remove_xattr_lookup_cb(op, op->result, request);
    }
} /* diskfs_remove_xattr_inode_cb */


void
diskfs_remove_xattr(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    diskfs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              diskfs_remove_xattr_inode_cb, request);
} /* diskfs_remove_xattr */


static void
diskfs_layout_add_device(
    struct chimera_vfs_request *request,
    struct diskfs_shared       *shared,
    uint32_t                    device_id)
{
    struct diskfs_device             *dev = &shared->devices[device_id];
    struct chimera_vfs_layout_device *ld;
    uint32_t                          i;

    for (i = 0; i < request->get_layout.r_num_devices; i++) {
        if (memcmp(request->get_layout.r_devices[i].deviceid, dev->deviceid,
                   SM_DEVICEID_SIZE) == 0) {
            return;
        }
    }
    if (request->get_layout.r_num_devices >= CHIMERA_VFS_LAYOUT_MAX_DEVICES) {
        return;
    }
    ld = &request->get_layout.r_devices[request->get_layout.r_num_devices++];
    memset(ld, 0, sizeof(*ld));
    memcpy(ld->deviceid, dev->deviceid, SM_DEVICEID_SIZE);

    if (shared->scsi_layout) {
        /* SCSI layout (RFC 8154): the client matches the LU by its VPD-0x83
         * hardware designator; nothing is written to the disk. */
        ld->layout_class    = CHIMERA_VFS_LAYOUT_CLASS_SCSI;
        ld->blk_vol_size    = dev->size;
        ld->scsi_code_set   = dev->scsi_code_set;
        ld->scsi_desig_type = dev->scsi_desig_type;
        ld->scsi_desig_len  = dev->scsi_desig_len;
        memcpy(ld->scsi_desig, dev->scsi_desig, dev->scsi_desig_len);
        ld->scsi_pr_key = dev->scsi_pr_key;
    } else {
        /* Block layout (RFC 5663): the client matches the disk by a content
         * signature read at a fixed offset. */
        ld->layout_class   = CHIMERA_VFS_LAYOUT_CLASS_BLOCK;
        ld->blk_vol_size   = dev->size;
        ld->blk_sig_offset = dev->sig_offset;
        ld->blk_sig_len    = dev->sig_len;
        memcpy(ld->blk_sig, dev->sig, dev->sig_len);
    }
} /* diskfs_layout_add_device */


/* Append one block segment [file_off, file_off+len) -> (device_id, vol_off). */
static void
diskfs_layout_emit(
    struct chimera_vfs_request *request,
    struct diskfs_shared       *shared,
    uint64_t                    file_off,
    uint64_t                    len,
    uint32_t                    device_id,
    uint64_t                    vol_off,
    uint32_t                    state)
{
    struct chimera_vfs_layout_segment *seg =
        &request->get_layout.r_segments[request->get_layout.r_num_segments++];

    memset(seg, 0, sizeof(*seg));
    seg->offset = file_off;
    seg->length = len;
    seg->iomode = request->get_layout.iomode;
    memcpy(seg->deviceid, shared->devices[device_id].deviceid, SM_DEVICEID_SIZE);
    seg->blk_vol_offset = vol_off;
    seg->blk_state      = state;
    diskfs_layout_add_device(request, shared, device_id);
} /* diskfs_layout_emit */


static void
diskfs_get_layout_finish(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p = request->plugin_data;

    request->get_layout.r_layout_class = p->thread->shared->scsi_layout
        ? CHIMERA_VFS_LAYOUT_CLASS_SCSI : CHIMERA_VFS_LAYOUT_CLASS_BLOCK;
    /* Commits the txn (durably persisting any new extent records) before the
     * layout is handed back; a pure-READ layout has nothing journaled. */
    diskfs_op_ok(request, p->txn);
} /* diskfs_get_layout_finish */


/* Advance the walk to the next extent after the current one, then re-process. */
static void
diskfs_get_layout_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    p->loop_have = diskfs_ext_from_op(op, result, &p->ext_iter);
    diskfs_bt_op_free(p->thread, op);
    diskfs_get_layout_process(request);
} /* diskfs_get_layout_walk_cb */


static void
diskfs_get_layout_advance(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    if (diskfs_ext_next_async(op, thread, p->inode_stash[0], p->ext_iter.file_offset,
                              p->rec_scratch, sizeof(p->rec_scratch),
                              diskfs_get_layout_walk_cb, request)) {
        diskfs_get_layout_walk_cb(op, op->result, request);
    }
} /* diskfs_get_layout_advance */


/* The freshly-allocated gap extent is now in the b+tree (durable on commit);
* emit it as INVALID_DATA and continue (the held ext_iter is still valid). */
static void
diskfs_get_layout_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *inode   = p->inode_stash[0];

    diskfs_bt_op_free(thread, op);

    if (unlikely(result < 0)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EIO);
        return;
    }

    inode->space_used += p->rmw_aligned_length;

    diskfs_layout_emit(request, thread->shared, p->rmw_aligned_start,
                       p->rmw_aligned_length, (uint32_t) p->rmw_device_id,
                       p->rmw_device_offset, CHIMERA_VFS_BLOCK_INVALID_DATA);

    p->loop_off += p->rmw_aligned_length;
    diskfs_get_layout_process(request);
} /* diskfs_get_layout_inserted_cb */


/* Allocate one gap extent at p->loop_off (length p->rmw_aligned_length) and
 * insert it.  Re-driven by the allocator on SM_AGAIN. */
static void
diskfs_get_layout_do_alloc(
    struct diskfs_thread *thread,
    void                 *arg)
{
    struct chimera_vfs_request    *request = arg;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_bt_op           *op;
    uint64_t                       dev_id, dev_off;
    int                            rc;

    rc = diskfs_inode_alloc_space(thread, p->txn, p->inode_stash[0],
                                  (int64_t) p->rmw_aligned_length,
                                  0 /* exact, no retained tail */,
                                  &dev_id, &dev_off,
                                  diskfs_get_layout_do_alloc, request);
    if (rc == SM_AGAIN) {
        return;     /* parked; re-driven into this function */
    }
    if (rc != 0) {
        diskfs_op_fail(request, p->txn, rc);
        return;
    }

    p->rmw_device_id     = dev_id;
    p->rmw_device_offset = dev_off;
    p->rmw_aligned_start = p->loop_off;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0],
                                p->loop_off, p->rmw_aligned_length,
                                (uint32_t) dev_id, dev_off, 0,
                                diskfs_get_layout_inserted_cb, request)) {
        diskfs_get_layout_inserted_cb(op, op->result, request);
    }
} /* diskfs_get_layout_do_alloc */


static void
diskfs_get_layout_process(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_shared          *shared = thread->shared;
    struct diskfs_inode           *inode  = p->inode_stash[0];
    struct diskfs_extent          *ext    = &p->ext_iter;
    int                            rw     = request->get_layout.iomode == DISKFS_LAYOUTIOMODE_RW;
    uint64_t                       size   = inode->size;
    uint64_t                       end    = p->loop_pos;

    /* Done: range covered, or no more segment slots (client re-LAYOUTGETs). */
    if (p->loop_off >= end ||
        request->get_layout.r_num_segments >= request->get_layout.max_segments) {
        diskfs_get_layout_finish(request);
        return;
    }

    if (p->loop_have && ext->file_offset <= p->loop_off &&
        ext->file_offset + ext->length > p->loop_off) {
        /* Backed run at the cursor. */
        uint64_t ext_end = ext->file_offset + ext->length;
        uint64_t seg_end = ext_end < end ? ext_end : end;
        uint64_t vol_off = ext->device_offset + (p->loop_off - ext->file_offset);

        if (rw && p->loop_off < size && seg_end > size) {
            /* Straddles committed size: written part then unwritten part. */
            uint64_t split = size;
            diskfs_layout_emit(request, shared, p->loop_off, split - p->loop_off,
                               ext->device_id, vol_off, CHIMERA_VFS_BLOCK_READ_WRITE_DATA);
            diskfs_layout_emit(request, shared, split, seg_end - split,
                               ext->device_id, vol_off + (split - p->loop_off),
                               CHIMERA_VFS_BLOCK_INVALID_DATA);
        } else {
            uint32_t state = rw ?
                (p->loop_off < size ? CHIMERA_VFS_BLOCK_READ_WRITE_DATA :
                 CHIMERA_VFS_BLOCK_INVALID_DATA) : CHIMERA_VFS_BLOCK_READ_DATA;
            diskfs_layout_emit(request, shared, p->loop_off, seg_end - p->loop_off,
                               ext->device_id, vol_off, state);
        }
        p->loop_off = seg_end;
        diskfs_get_layout_advance(request);
        return;
    }

    /* Gap from the cursor to the next extent (or the end of the range). */
    {
        uint64_t gap_end = (p->loop_have && ext->file_offset < end) ?
            ext->file_offset : end;

        if (!rw) {
            /* Read of a hole: skip it (the client reads zeros). */
            p->loop_off = gap_end;
            diskfs_get_layout_process(request);
            return;
        }

        uint64_t gap_len = gap_end - p->loop_off;
        if (gap_len > DISKFS_LAYOUT_GAP_MAX) {
            gap_len = DISKFS_LAYOUT_GAP_MAX;
        }
        p->rmw_aligned_length = gap_len;
        diskfs_get_layout_do_alloc(thread, request);
    }
} /* diskfs_get_layout_process */


static void
diskfs_get_layout_first_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    p->loop_have = diskfs_ext_from_op(op, result, &p->ext_iter);
    diskfs_bt_op_free(thread, op);

    /* A floor extent entirely before the cursor doesn't overlap: step to the
     * next one so the cursor sees the first extent at or after loop_off. */
    if (p->loop_have &&
        p->ext_iter.file_offset + p->ext_iter.length <= p->loop_off) {
        diskfs_get_layout_advance(request);
        return;
    }
    diskfs_get_layout_process(request);
} /* diskfs_get_layout_first_cb */


static void
diskfs_get_layout_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_shared          *shared  = thread->shared;
    uint64_t                       off, end;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }
    if (unlikely(!S_ISREG(inode->mode))) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EINVAL);
        return;
    }

    off = request->get_layout.offset & ~(uint64_t) (DISKFS_BLOCK_SIZE - 1);
    end = (request->get_layout.offset + request->get_layout.length +
           DISKFS_BLOCK_SIZE - 1) & ~(uint64_t) (DISKFS_BLOCK_SIZE - 1);

    /* A READ layout is never returned past the committed size (the client reads
     * zeros for a hole / beyond EOF). */
    if (request->get_layout.iomode != DISKFS_LAYOUTIOMODE_RW) {
        uint64_t size_end = (inode->size + DISKFS_BLOCK_SIZE - 1) &
            ~(uint64_t) (DISKFS_BLOCK_SIZE - 1);
        if (end > size_end) {
            end = size_end;
        }
    }

    p->inode_stash[0] = inode;
    p->loop_off       = off;
    p->loop_pos       = end;

    request->get_layout.r_layout_class = shared->scsi_layout
        ? CHIMERA_VFS_LAYOUT_CLASS_SCSI : CHIMERA_VFS_LAYOUT_CLASS_BLOCK;
    request->get_layout.r_num_segments = 0;
    request->get_layout.r_num_devices  = 0;

    if (off >= end) {
        diskfs_get_layout_finish(request);
        return;
    }

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_floor_async(op, thread, inode, off, p->rec_scratch,
                               sizeof(p->rec_scratch), diskfs_get_layout_first_cb,
                               request)) {
        diskfs_get_layout_first_cb(op, op->result, request);
    }
} /* diskfs_get_layout_inode_cb */


void
diskfs_get_layout(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;
    int                            rw;

    (void) private_data;

    if (unlikely(!shared->block_layout && !shared->scsi_layout)) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    rw        = request->get_layout.iomode == DISKFS_LAYOUTIOMODE_RW;
    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, rw ? DISKFS_TXN_WRITE : DISKFS_TXN_READ);

    diskfs_inode_get_fh_async(thread, p->txn, request->fh, request->fh_len,
                              diskfs_get_layout_inode_cb, request);
} /* diskfs_get_layout */
