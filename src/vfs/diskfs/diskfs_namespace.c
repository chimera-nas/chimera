// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Namespace operations over directory-entry b+tree records: lookup, create,
 * mkdir, mknod, open (by handle and by name), close, remove, rename, link,
 * symlink/readlink and readdir.
 */

#include "diskfs_internal.h"

/* Forward declarations (definitions below, in call-graph order) */

static inline struct diskfs_bt_key
diskfs_dirent_key(
    uint64_t hash);

static int
diskfs_dir_next_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_inode  *dir,
    uint64_t              from_hash,
    struct diskfs_bt_key *r_key,
    void                 *rec_out,
    uint32_t              rec_cap,
    diskfs_bt_cb_t        cb,
    void                 *private_data);

static int
diskfs_dir_insert_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *dir,
    uint64_t              hash,
    const char           *name,
    int                   namelen,
    uint64_t              child_inum,
    uint32_t              child_gen,
    diskfs_bt_cb_t        cb,
    void                 *private_data);

static int
diskfs_dir_remove_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *dir,
    uint64_t              hash,
    diskfs_bt_cb_t        cb,
    void                 *private_data);

static int
diskfs_symlink_set_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    const void           *target,
    int                   len,
    diskfs_bt_cb_t        cb,
    void                 *private_data);

static void
diskfs_lookup_at_dirent_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_lookup_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data);

static void
diskfs_mkdir_at_existing_cb(
    struct diskfs_inode *existing_inode,
    int                  status,
    void                *private_data);

static void
diskfs_mkdir_at_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_mkdir_at_acl_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_mkdir_at_alloc_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_mkdir_at_check_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_mkdir_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data);

static void
diskfs_mknod_at_existing_cb(
    struct diskfs_inode *existing_inode,
    int                  status,
    void                *private_data);

static void
diskfs_mknod_at_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_mknod_at_alloc_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_mknod_at_check_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_mknod_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data);

static void
diskfs_remove_at_finish(
    struct chimera_vfs_request *request);

static void
diskfs_remove_orphan_done(
    void *priv);

static void
diskfs_remove_at_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_remove_at_empty_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_remove_at_child_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_remove_at_lookup_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_remove_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data);

static void
diskfs_readdir_finish(
    struct chimera_vfs_request *request);

static void
diskfs_readdir_complete(
    struct chimera_vfs_request *request);

static void
diskfs_readdir_iter_inode_cb(
    struct diskfs_inode *dirent_inode,
    int                  status,
    void                *private_data);

static void
diskfs_readdir_next_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_readdir_iter_step(
    struct chimera_vfs_request *request);

static void
diskfs_readdir_start_iter(
    struct chimera_vfs_request *request);

static void
diskfs_readdir_emit_dotdot(
    struct chimera_vfs_request *request,
    struct chimera_vfs_attrs   *attr);

static void
diskfs_readdir_dotdot_cb(
    struct diskfs_inode *parent_inode,
    int                  status,
    void                *private_data);

static void
diskfs_readdir_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_open_at_finish(
    struct chimera_vfs_request *request,
    struct diskfs_inode        *parent,
    struct diskfs_inode        *inode);

static void
diskfs_open_at_existing_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_open_at_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_open_at_acl_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_open_at_alloc_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_open_at_check_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_open_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data);

static void
diskfs_create_unlinked_orphaned_cb(
    void *priv);

static void
diskfs_create_unlinked_alloc_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_close_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_symlink_at_dirent_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_symlink_at_target_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_symlink_at_alloc_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_symlink_at_check_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_symlink_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data);

static void
diskfs_readlink_done_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_readlink_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static inline int
diskfs_fh_compare(
    const void *fha,
    int         fha_len,
    const void *fhb,
    int         fhb_len);

static void
diskfs_rename_at_unlock_parents(
    struct chimera_vfs_request *request);

static void
diskfs_rename_at_replaced_empty_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_rename_at_existing_cb(
    struct diskfs_inode *existing_inode,
    int                  status,
    void                *private_data);

static void
diskfs_rename_at_perform_final_cb(
    struct diskfs_bt_op *bop,
    int                  result,
    void                *private_data);

static void
diskfs_rename_at_perform_inserted_cb(
    struct diskfs_bt_op *bop,
    int                  result,
    void                *private_data);

static void
diskfs_rename_at_perform_insert(
    struct chimera_vfs_request *request);

static void
diskfs_rename_at_replaced_orphaned_cb(
    void *priv);

static void
diskfs_rename_at_perform_removed_cb(
    struct diskfs_bt_op *bop,
    int                  result,
    void                *private_data);

static void
diskfs_rename_at_perform(
    struct chimera_vfs_request *request);

static void
diskfs_rename_at_dest_cb(
    struct diskfs_bt_op *bop,
    int                  result,
    void                *private_data);

static void
diskfs_rename_at_child_cb(
    struct diskfs_inode *child,
    int                  status,
    void                *private_data);

static void
diskfs_rename_at_check_descendant_step(
    struct chimera_vfs_request *request);

static void
diskfs_rename_at_descendant_cb(
    struct diskfs_inode *anc,
    int                  status,
    void                *private_data);

static void
diskfs_rename_at_dest_lookup(
    struct chimera_vfs_request *request);

static void
diskfs_rename_at_source_cb(
    struct diskfs_bt_op *bop,
    int                  result,
    void                *private_data);

static void
diskfs_rename_at_have_parents(
    struct chimera_vfs_request *request);

static void
diskfs_rename_at_second_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_rename_at_first_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_link_at_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_link_at_unorphaned_cb(
    void *priv);

static void
diskfs_link_at_finish(
    struct chimera_vfs_request *request);

static void
diskfs_link_at_replaced_orphaned_cb(
    void *priv);

static void
diskfs_link_at_existing_cb(
    struct diskfs_inode *existing_inode,
    int                  status,
    void                *private_data);

static void
diskfs_link_at_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_link_at_check_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_link_at_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_link_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data);


/* ------------------------------------------------------------------ */
/* Directory / symlink records over the inode b+tree                   */
/* ------------------------------------------------------------------ */

static inline struct diskfs_bt_key
diskfs_dirent_key(uint64_t hash)
{
    struct diskfs_bt_key k = { .type = DISKFS_REC_DIRENT, .subkey = hash };

    return k;
} /* diskfs_dirent_key */


/*
 * Async directory-record helpers (thin wrappers over the b+tree op driver).
 * Each returns 1 if it completed synchronously (result in op->result; the
 * looked-up record, if any, written into rec_out), or 0 if it suspended (cb
 * fires with the result later).  Callers parse the dirent record themselves.
 */
int
diskfs_dir_lookup_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_inode  *dir,
    uint64_t              hash,
    void                 *rec_out,
    uint32_t              rec_cap,
    diskfs_bt_cb_t        cb,
    void                 *private_data)
{
    struct diskfs_bt_key key = diskfs_dirent_key(hash);

    return diskfs_bt_lookup_async(op, thread, dir, DISKFS_BT_OP_LOOKUP_EXACT,
                                  &key, NULL, rec_out, rec_cap, cb, private_data);
} /* diskfs_dir_lookup_async */


static int
diskfs_dir_next_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_inode  *dir,
    uint64_t              from_hash,
    struct diskfs_bt_key *r_key,
    void                 *rec_out,
    uint32_t              rec_cap,
    diskfs_bt_cb_t        cb,
    void                 *private_data)
{
    struct diskfs_bt_key key = diskfs_dirent_key(from_hash);

    return diskfs_bt_lookup_async(op, thread, dir, DISKFS_BT_OP_LOOKUP_GE,
                                  &key, r_key, rec_out, rec_cap, cb, private_data);
} /* diskfs_dir_next_async */


static int
diskfs_dir_insert_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *dir,
    uint64_t              hash,
    const char           *name,
    int                   namelen,
    uint64_t              child_inum,
    uint32_t              child_gen,
    diskfs_bt_cb_t        cb,
    void                 *private_data)
{
    char                      buf[DISKFS_DIRENT_REC_MAX];
    struct diskfs_dirent_rec *r   = (struct diskfs_dirent_rec *) buf;
    struct diskfs_bt_key      key = diskfs_dirent_key(hash);

    r->inum     = child_inum;
    r->gen      = child_gen;
    r->name_len = (uint16_t) namelen;
    memcpy(r->name, name, namelen);

    return diskfs_bt_insert_async(op, thread, txn, dir, &key, buf,
                                  sizeof(*r) + namelen, cb, private_data);
} /* diskfs_dir_insert_async */


static int
diskfs_dir_remove_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *dir,
    uint64_t              hash,
    diskfs_bt_cb_t        cb,
    void                 *private_data)
{
    struct diskfs_bt_key key = diskfs_dirent_key(hash);

    return diskfs_bt_remove_async(op, thread, txn, dir, &key, cb, private_data);
} /* diskfs_dir_remove_async */


static int
diskfs_symlink_set_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    const void           *target,
    int                   len,
    diskfs_bt_cb_t        cb,
    void                 *private_data)
{
    struct diskfs_bt_key key = { .type = DISKFS_REC_SYMLINK, .subkey = 0 };

    return diskfs_bt_insert_async(op, thread, txn, inode, &key, target, len,
                                  cb, private_data);
} /* diskfs_symlink_set_async */


/* b+tree lookup completion: parse the dirent and fetch the child inode. */
static void
diskfs_lookup_at_dirent_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_dirent_rec      *rec     = (struct diskfs_dirent_rec *) p->rec_scratch;

    diskfs_bt_op_free(p->thread, op);

    if (result < 0) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    diskfs_inode_get_inum_async(p->thread, p->txn, rec->inum, rec->gen,
                                diskfs_lookup_at_child_cb, request);
} /* diskfs_lookup_at_dirent_cb */


static void
diskfs_lookup_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    const char                    *name    = request->lookup_at.component;
    uint32_t                       namelen = request->lookup_at.component_len;
    uint64_t                       hash    = request->lookup_at.component_hash;
    struct diskfs_bt_key           key;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (unlikely(!S_ISDIR(parent->mode))) {
        enum chimera_vfs_error err = S_ISLNK(parent->mode) ?
            CHIMERA_VFS_ESYMLINK : CHIMERA_VFS_ENOTDIR;
        diskfs_op_fail(request, p->txn, err);
        return;
    }

    diskfs_map_attrs(thread, &request->lookup_at.r_dir_attr, parent);

    if (namelen == 1 && name[0] == '.') {
        diskfs_map_attrs(thread, &request->lookup_at.r_attr, parent);
        diskfs_op_ok(request, p->txn);
        return;
    }

    p->inode_stash[0] = parent;

    if (namelen == 2 && name[0] == '.' && name[1] == '.') {
        diskfs_inode_get_inum_async(thread, p->txn,
                                    parent->parent_inum,
                                    parent->parent_gen,
                                    diskfs_lookup_at_child_cb, request);
        return;
    }

    key = diskfs_dirent_key(hash);
    op  = diskfs_bt_op_alloc(thread);
    if (diskfs_bt_lookup_async(op, thread, parent, DISKFS_BT_OP_LOOKUP_EXACT,
                               &key, NULL, p->rec_scratch, sizeof(p->rec_scratch),
                               diskfs_lookup_at_dirent_cb, request)) {
        diskfs_lookup_at_dirent_cb(op, op->result, request);
    }
} /* diskfs_lookup_at_parent_cb */


void
diskfs_lookup_at(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    uint64_t                       inum;
    uint32_t                       gen;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_READ);

    diskfs_fh_to_inum(&inum, &gen, request->fh, request->fh_len);
    request->wait_reason   = "diskfs_open_fh_inode_lock";
    request->wait_since_ns = diskfs_diag_now_ns();
    request->wait_arg0     = inum;
    request->wait_arg1     = gen;
    request->wait_arg2     = 0;

    diskfs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              diskfs_lookup_at_parent_cb, request);
} /* diskfs_lookup_at */


/* inode_stash[0] = parent (locked across alloc / existing-inode fetch) */

static void
diskfs_mkdir_at_existing_cb(
    struct diskfs_inode *existing_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *parent  = p->inode_stash[0];

    if (likely(status == CHIMERA_VFS_OK)) {
        diskfs_map_attrs(p->thread, &request->mkdir_at.r_attr, existing_inode);
    }
    diskfs_map_attrs(p->thread, &request->mkdir_at.r_dir_post_attr, parent);

    diskfs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
} /* diskfs_mkdir_at_existing_cb */


static void
diskfs_mkdir_at_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_op_ok(request, p->txn);
} /* diskfs_mkdir_at_inserted_cb */


/* The new directory's ACL record (if any) is stored: map the attrs (r_attr
 * now reflects any inherited mode/ACL) and link the dirent into the parent. */
static void
diskfs_mkdir_at_acl_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *parent  = p->inode_stash[0];
    struct diskfs_inode           *inode   = p->inode_stash[1];

    (void) result;
    diskfs_bt_op_free(thread, op);

    diskfs_map_attrs(thread, &request->mkdir_at.r_attr, inode);
    diskfs_map_attrs(thread, &request->mkdir_at.r_dir_post_attr, parent);

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_insert_async(op, thread, p->txn, parent,
                                request->mkdir_at.name_hash, request->mkdir_at.name,
                                request->mkdir_at.name_len, inode->inum, inode->gen,
                                diskfs_mkdir_at_inserted_cb, request)) {
        diskfs_mkdir_at_inserted_cb(op, op->result, request);
    }
} /* diskfs_mkdir_at_acl_cb */


static void
diskfs_mkdir_at_alloc_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *parent  = p->inode_stash[0];
    struct diskfs_bt_op           *op;
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    inode->size       = 4096;
    inode->space_used = 4096;
    inode->uid        = request->cred->uid;
    inode->gid        = request->cred->gid;
    inode->nlink      = 2;
    inode->mode       = S_IFDIR | 0755;
    inode->atime_sec  = now.tv_sec;
    inode->atime_nsec = now.tv_nsec;
    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;
    inode->change++;
    inode->btime_sec      = now.tv_sec;
    inode->btime_nsec     = now.tv_nsec;
    inode->dos_attributes = 0;

    inode->parent_inum = parent->inum;
    inode->parent_gen  = parent->gen;

    /* Snapshot any explicit ACL pointer BEFORE diskfs_apply_attrs() rewrites
     * va_set_mask and drops the ATTR_ACL bit. */
    const struct chimera_acl *new_acl_mkdir =
        (request->mkdir_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_ACL)
        ? request->mkdir_at.set_attr->va_acl : NULL;

    diskfs_apply_attrs(inode, request->mkdir_at.set_attr);

    parent->nlink++;
    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;
    parent->change++;

    p->inode_stash[1] = inode;

    /* Seed the new directory's ACL (inherited, or a Windows default DACL for
     * SMB creates) before mapping attrs so r_attr reflects any inherited mode
     * and carries the freshly-stored ACL.  An explicit ACL in set_attr (e.g.
     * an SMB SD via SecD) takes precedence. */
    op = diskfs_bt_op_alloc(thread);
    if (diskfs_inherit_acl_async(op, thread, p->txn, inode, parent,
                                 new_acl_mkdir,
                                 request->cred->flavor == CHIMERA_VFS_AUTH_ATTR,
                                 diskfs_mkdir_at_acl_cb, request)) {
        diskfs_mkdir_at_acl_cb(op, op->result, request);
    }
} /* diskfs_mkdir_at_alloc_cb */


static void
diskfs_mkdir_at_check_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    diskfs_bt_op_free(thread, op);

    if (result >= 0) {
        struct diskfs_dirent_rec *rec = (struct diskfs_dirent_rec *) p->rec_scratch;

        diskfs_inode_get_inum_async(thread, p->txn, rec->inum, rec->gen,
                                    diskfs_mkdir_at_existing_cb, request);
        return;
    }

    diskfs_inode_alloc_async(thread, p->txn, diskfs_mkdir_at_alloc_cb, request);
} /* diskfs_mkdir_at_check_cb */


static void
diskfs_mkdir_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->mkdir_at.name_hash;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(parent->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    diskfs_map_attrs(thread, &request->mkdir_at.r_dir_pre_attr, parent);

    p->inode_stash[0] = parent;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(op, thread, parent, hash, p->rec_scratch,
                                sizeof(p->rec_scratch), diskfs_mkdir_at_check_cb,
                                request)) {
        diskfs_mkdir_at_check_cb(op, op->result, request);
    }
} /* diskfs_mkdir_at_parent_cb */


void
diskfs_mkdir_at(
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
                              diskfs_mkdir_at_parent_cb, request);
} /* diskfs_mkdir_at */


/* inode_stash[0] = parent (locked across alloc / existing fetch) */

static void
diskfs_mknod_at_existing_cb(
    struct diskfs_inode *existing_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *parent  = p->inode_stash[0];

    if (likely(status == CHIMERA_VFS_OK)) {
        diskfs_map_attrs(p->thread, &request->mknod_at.r_attr, existing_inode);
    }
    diskfs_map_attrs(p->thread, &request->mknod_at.r_dir_post_attr, parent);

    diskfs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
} /* diskfs_mknod_at_existing_cb */


static void
diskfs_mknod_at_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_op_ok(request, p->txn);
} /* diskfs_mknod_at_inserted_cb */


static void
diskfs_mknod_at_alloc_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *parent  = p->inode_stash[0];
    struct diskfs_bt_op           *op;
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    inode->size       = 0;
    inode->space_used = 0;
    inode->uid        = request->cred->uid;
    inode->gid        = request->cred->gid;
    inode->nlink      = 1;
    inode->rdev       = 0;
    inode->atime_sec  = now.tv_sec;
    inode->atime_nsec = now.tv_nsec;
    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;
    inode->change++;
    inode->btime_sec      = now.tv_sec;
    inode->btime_nsec     = now.tv_nsec;
    inode->dos_attributes = 0;

    if (request->mknod_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        inode->mode = request->mknod_at.set_attr->va_mode;
    } else {
        inode->mode = S_IFREG | 0644;
    }
    if (request->mknod_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_RDEV) {
        inode->rdev = request->mknod_at.set_attr->va_rdev;
    }

    diskfs_apply_attrs(inode, request->mknod_at.set_attr);
    diskfs_map_attrs(thread, &request->mknod_at.r_attr, inode);

    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;
    parent->change++;

    diskfs_map_attrs(thread, &request->mknod_at.r_dir_post_attr, parent);

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_insert_async(op, thread, p->txn, parent,
                                request->mknod_at.name_hash, request->mknod_at.name,
                                request->mknod_at.name_len, inode->inum, inode->gen,
                                diskfs_mknod_at_inserted_cb, request)) {
        diskfs_mknod_at_inserted_cb(op, op->result, request);
    }
} /* diskfs_mknod_at_alloc_cb */


static void
diskfs_mknod_at_check_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    diskfs_bt_op_free(thread, op);

    if (result >= 0) {
        struct diskfs_dirent_rec *rec = (struct diskfs_dirent_rec *) p->rec_scratch;

        diskfs_inode_get_inum_async(thread, p->txn, rec->inum, rec->gen,
                                    diskfs_mknod_at_existing_cb, request);
        return;
    }

    diskfs_inode_alloc_async(thread, p->txn, diskfs_mknod_at_alloc_cb, request);
} /* diskfs_mknod_at_check_cb */


static void
diskfs_mknod_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->mknod_at.name_hash;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(parent->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    diskfs_map_attrs(thread, &request->mknod_at.r_dir_pre_attr, parent);

    p->inode_stash[0] = parent;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(op, thread, parent, hash, p->rec_scratch,
                                sizeof(p->rec_scratch), diskfs_mknod_at_check_cb,
                                request)) {
        diskfs_mknod_at_check_cb(op, op->result, request);
    }
} /* diskfs_mknod_at_parent_cb */


void
diskfs_mknod_at(
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
                              diskfs_mknod_at_parent_cb, request);
} /* diskfs_mknod_at */


/* inode_stash[0] = parent (locked across child fetch) */

/* Finish a remove: map the parent's post-attrs and commit. */
static void
diskfs_remove_at_finish(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_inode           *parent = p->inode_stash[0];

    diskfs_map_attrs(p->thread, &request->remove_at.r_dir_post_attr, parent);
    diskfs_op_ok(request, p->txn);
} /* diskfs_remove_at_finish */


/* The deleted inode is recorded on the durable orphan list (and queued for
 * reclaim when unreferenced): finish the request. */
static void
diskfs_remove_orphan_done(void *priv)
{
    diskfs_remove_at_finish(priv);
} /* diskfs_remove_orphan_done */


static void
diskfs_remove_at_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *parent  = p->inode_stash[0];
    struct diskfs_inode           *inode   = p->inode_stash[1];
    struct timespec                now;

    diskfs_bt_op_free(thread, op);

    /* The dirent was located before the child fetch and the parent has been
     * write-locked throughout, so it must still be present. */
    if (unlikely(result != 1)) {
        chimera_diskfs_error("remove_at lost dirent after lookup name=%.*s hash=%lu parent=%lu",
                             request->remove_at.namelen,
                             request->remove_at.name,
                             request->remove_at.name_hash,
                             parent->inum);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    if (S_ISDIR(inode->mode)) {
        parent->nlink--;
    }
    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;
    parent->change++;

    if (S_ISDIR(inode->mode)) {
        inode->nlink = 0;
    } else {
        inode->nlink--;
        /* Removing one of several hard links changes the surviving inode's
         * link count, which is a status change: bump its ctime. */
        if (inode->nlink > 0) {
            inode->ctime_sec  = now.tv_sec;
            inode->ctime_nsec = now.tv_nsec;
            inode->change++;
        }
    }

    if (inode->nlink == 0) {
        request->remove_at.r_removed_attr.va_req_mask = CHIMERA_VFS_ATTR_FH;
    }

    diskfs_map_attrs(thread, &request->remove_at.r_removed_attr, inode);

    if (inode->nlink == 0) {
        /* Record the deleted inode on the durable orphan list -- atomic with
         * this unlink txn (the orphan shard is acquired last, a leaf in the
         * lock order, so no deadlock) -- and hand it to the reclaim workers
         * once nothing references it.  All space reclaim (extents + b+tree
         * burn-down) happens there, off this hot path; a crash before this
         * txn commits leaves neither the unlink nor the orphan record, and
         * after it the mount scan resumes the drain. */
        diskfs_inode_orphaned(thread, p->txn, inode,
                              diskfs_remove_orphan_done, request);
        return;
    }

    diskfs_remove_at_finish(request);
} /* diskfs_remove_at_removed_cb */


/* Probe result for a removed directory's first dirent: any hit means the
 * directory is not empty. */
static void
diskfs_remove_at_empty_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->remove_at.name_hash;
    int                            nonempty;

    nonempty = (result >= 0 && op->found_key.type == DISKFS_REC_DIRENT);
    diskfs_bt_op_free(thread, op);

    if (nonempty) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTEMPTY);
        return;
    }

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_remove_async(op, thread, p->txn, p->inode_stash[0], hash,
                                diskfs_remove_at_removed_cb, request)) {
        diskfs_remove_at_removed_cb(op, op->result, request);
    }
} /* diskfs_remove_at_empty_cb */


static void
diskfs_remove_at_child_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->remove_at.name_hash;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    p->inode_stash[1] = inode;

    /* rmdir of a non-empty directory: probe for any dirent record
     * (asynchronously -- the child's tree may not be cached).  "." and ".."
     * are synthesised, so one record means non-empty. */
    if (S_ISDIR(inode->mode)) {
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_dir_next_async(op, thread, inode, 0, &op->found_key,
                                  p->rec_scratch, sizeof(p->rec_scratch),
                                  diskfs_remove_at_empty_cb, request)) {
            diskfs_remove_at_empty_cb(op, op->result, request);
        }
        return;
    }

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_remove_async(op, thread, p->txn, p->inode_stash[0], hash,
                                diskfs_remove_at_removed_cb, request)) {
        diskfs_remove_at_removed_cb(op, op->result, request);
    }
} /* diskfs_remove_at_child_cb */


static void
diskfs_remove_at_lookup_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_dirent_rec      *rec     = (struct diskfs_dirent_rec *) p->rec_scratch;

    diskfs_bt_op_free(thread, op);

    if (result < 0) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    diskfs_inode_get_inum_async(thread, p->txn, rec->inum, rec->gen,
                                diskfs_remove_at_child_cb, request);
} /* diskfs_remove_at_lookup_cb */


static void
diskfs_remove_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->remove_at.name_hash;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    diskfs_map_attrs(thread, &request->remove_at.r_dir_pre_attr, parent);

    if (!S_ISDIR(parent->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    p->inode_stash[0] = parent;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(op, thread, parent, hash, p->rec_scratch,
                                sizeof(p->rec_scratch), diskfs_remove_at_lookup_cb,
                                request)) {
        diskfs_remove_at_lookup_cb(op, op->result, request);
    }
} /* diskfs_remove_at_parent_cb */


void
diskfs_remove_at(
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
                              diskfs_remove_at_parent_cb, request);
} /* diskfs_remove_at */


static void
diskfs_readdir_finish(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p     = request->plugin_data;
    struct diskfs_inode           *inode = p->inode_stash[0];

    diskfs_map_attrs(p->thread, &request->readdir.r_dir_attr, inode);
    diskfs_op_ok(request, p->txn);
} /* diskfs_readdir_finish */


static void
diskfs_readdir_complete(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p = request->plugin_data;

    /* If a synchronous iteration loop is driving the walk (see
     * diskfs_readdir_iter_step), only flag completion: the loop calls
     * diskfs_readdir_finish() once it unwinds.  Finishing inline here would
     * commit the txn and free the request out from under the active loop. */
    if (p->rd_looping) {
        p->rd_done = 1;
        return;
    }

    diskfs_readdir_finish(request);
} /* diskfs_readdir_complete */


static void
diskfs_readdir_iter_inode_cb(
    struct diskfs_inode *dirent_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct chimera_vfs_attrs       attr;
    int                            rc;

    if (status != CHIMERA_VFS_OK) {
        /* Stale dirent — skip to the next. */
        p->rd_from_hash = p->rd_hash + 1;
        diskfs_readdir_iter_step(request);
        return;
    }

    attr.va_req_mask = request->readdir.attr_mask;
    diskfs_map_attrs(thread, &attr, dirent_inode);

    /* Done with this child; release its slot so the next iteration reuses
     * it (only the directory itself stays held across the walk). */
    diskfs_txn_unlock_inode(p->txn, dirent_inode);

    rc = request->readdir.callback(
        p->rd_inum,
        p->rd_hash + DISKFS_COOKIE_FIRST,
        p->rd_name, p->rd_namelen,
        &attr, request->proto_private_data);

    request->readdir.r_cookie = p->rd_hash + DISKFS_COOKIE_FIRST;

    if (rc) {
        request->readdir.r_eof = 0;
        diskfs_readdir_complete(request);
        return;
    }

    p->rd_from_hash = p->rd_hash + 1;
    diskfs_readdir_iter_step(request);
} /* diskfs_readdir_iter_inode_cb */


static void
diskfs_readdir_next_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_dirent_rec      *rec     = (struct diskfs_dirent_rec *) p->rec_scratch;

    if (result < 0 || op->found_key.type != DISKFS_REC_DIRENT) {
        diskfs_bt_op_free(thread, op);
        request->readdir.r_eof = 1;
        diskfs_readdir_complete(request);
        return;
    }

    p->rd_hash    = op->found_key.subkey;
    p->rd_inum    = rec->inum;
    p->rd_gen     = rec->gen;
    p->rd_namelen = rec->name_len;
    memcpy(p->rd_name, rec->name, rec->name_len);

    diskfs_bt_op_free(thread, op);

    diskfs_inode_get_inum_async(thread, p->txn, p->rd_inum, p->rd_gen,
                                diskfs_readdir_iter_inode_cb, request);
} /* diskfs_readdir_next_cb */


static void
diskfs_readdir_iter_step(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_inode           *inode  = p->inode_stash[0];
    struct diskfs_bt_op           *op;

    /* A re-entrant call from a step that completed synchronously: don't
    * recurse, just ask the active loop to advance to the next entry. */
    if (p->rd_looping) {
        p->rd_advance = 1;
        return;
    }

    p->rd_looping = 1;
    do {
        p->rd_advance = 0;
        op            = diskfs_bt_op_alloc(thread);
        if (diskfs_dir_next_async(op, thread, inode, p->rd_from_hash, &op->found_key,
                                  p->rec_scratch, sizeof(p->rec_scratch),
                                  diskfs_readdir_next_cb, request)) {
            diskfs_readdir_next_cb(op, op->result, request);
        }
        /* rd_advance: the step finished inline; loop for the next entry.
         * rd_done:    the walk completed (terminal dirent or full buffer).
         * neither:    the step suspended on block I/O; its completion will
         *             re-enter this function with rd_looping clear. */
    } while (p->rd_advance && !p->rd_done);

    p->rd_looping = 0;

    if (p->rd_done) {
        diskfs_readdir_finish(request);
    }
} /* diskfs_readdir_iter_step */


static void
diskfs_readdir_start_iter(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    uint64_t                       cookie = request->readdir.r_cookie;

    if (cookie < DISKFS_COOKIE_FIRST) {
        p->rd_from_hash = 0;
    } else {
        p->rd_from_hash = (cookie - DISKFS_COOKIE_FIRST) + 1;
    }

    diskfs_readdir_iter_step(request);
} /* diskfs_readdir_start_iter */


static void
diskfs_readdir_emit_dotdot(
    struct chimera_vfs_request *request,
    struct chimera_vfs_attrs   *attr)
{
    struct diskfs_request_private *p     = request->plugin_data;
    struct diskfs_inode           *inode = p->inode_stash[0];
    int                            rc;

    rc = request->readdir.callback(
        inode->parent_inum,
        DISKFS_COOKIE_DOTDOT,
        "..", 2,
        attr, request->proto_private_data);

    if (rc) {
        request->readdir.r_cookie = DISKFS_COOKIE_DOTDOT;
        request->readdir.r_eof    = 0;
        diskfs_readdir_complete(request);
        return;
    }
    request->readdir.r_cookie = DISKFS_COOKIE_DOTDOT;
    diskfs_readdir_start_iter(request);
} /* diskfs_readdir_emit_dotdot */


static void
diskfs_readdir_dotdot_cb(
    struct diskfs_inode *parent_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *inode   = p->inode_stash[0];
    struct chimera_vfs_attrs       attr;

    attr.va_req_mask = request->readdir.attr_mask;

    if (status == CHIMERA_VFS_OK) {
        diskfs_map_attrs(p->thread, &attr, parent_inode);
        /* Release the parent (".." target); it's distinct from the dir
        * being read (the self-parent root case never reaches here). */
        diskfs_txn_unlock_inode(p->txn, parent_inode);
    } else {
        diskfs_map_attrs(p->thread, &attr, inode);
    }

    diskfs_readdir_emit_dotdot(request, &attr);
} /* diskfs_readdir_dotdot_cb */


static void
diskfs_readdir_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       cookie  = request->readdir.cookie;
    struct chimera_vfs_attrs       attr;
    int                            rc;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(inode->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    p->inode_stash[0]         = inode;
    request->readdir.r_cookie = cookie;
    request->readdir.r_eof    = 1;

    attr.va_req_mask = request->readdir.attr_mask;

    if ((request->readdir.flags & CHIMERA_VFS_READDIR_EMIT_DOT) &&
        cookie < DISKFS_COOKIE_DOT) {
        diskfs_map_attrs(thread, &attr, inode);
        rc = request->readdir.callback(
            inode->inum, DISKFS_COOKIE_DOT, ".", 1,
            &attr, request->proto_private_data);
        if (rc) {
            request->readdir.r_cookie = DISKFS_COOKIE_DOT;
            request->readdir.r_eof    = 0;
            diskfs_readdir_complete(request);
            return;
        }
        cookie                    = DISKFS_COOKIE_DOT;
        request->readdir.r_cookie = cookie;
    }

    if ((request->readdir.flags & CHIMERA_VFS_READDIR_EMIT_DOT) &&
        cookie < DISKFS_COOKIE_DOTDOT) {
        if (inode->parent_inum == inode->inum &&
            inode->parent_gen == inode->gen) {
            diskfs_map_attrs(thread, &attr, inode);
            diskfs_readdir_emit_dotdot(request, &attr);
            return;
        }
        diskfs_inode_get_inum_async(thread, p->txn,
                                    inode->parent_inum,
                                    inode->parent_gen,
                                    diskfs_readdir_dotdot_cb, request);
        return;
    }

    if (!(request->readdir.flags & CHIMERA_VFS_READDIR_EMIT_DOT) &&
        cookie < DISKFS_COOKIE_DOTDOT) {
        request->readdir.r_cookie = DISKFS_COOKIE_DOTDOT;
    }

    diskfs_readdir_start_iter(request);
} /* diskfs_readdir_inode_cb */


void
diskfs_readdir(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread     = thread;
    p->txn        = diskfs_txn_begin(thread, DISKFS_TXN_READ);
    p->rd_looping = 0;
    p->rd_advance = 0;
    p->rd_done    = 0;

    diskfs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              diskfs_readdir_inode_cb, request);
} /* diskfs_readdir */


void
diskfs_open_fh_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    request->wait_reason = NULL;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if ((request->open_fh.flags & CHIMERA_VFS_OPEN_DIRECTORY) &&
        !S_ISDIR(inode->mode)) {
        /* A directory open of a symlink is NFS4ERR_SYMLINK, not NOTDIR (e.g.
         * LOOKUP/LOOKUPP through a symlink); other non-dirs are NOTDIR. */
        diskfs_op_fail(request, p->txn,
                       S_ISLNK(inode->mode) ? CHIMERA_VFS_ESYMLINK : CHIMERA_VFS_ENOTDIR);
        return;
    }

    /* A deleted, unreferenced inode is dead (queued for or under background
     * reclaim); a stale handle must not resurrect it mid-burn-down.  One that
     * still has open handles (deleted-while-open / anonymous) stays openable. */
    if (unlikely(inode->nlink == 0 && inode->refcnt == 0)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    diskfs_inode_ref_get(p->thread, inode);

    request->open_fh.r_vfs_private = (uint64_t) inode;
    diskfs_op_ok(request, p->txn);
} /* diskfs_open_fh_inode_cb */


void
diskfs_open_fh(
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
                              diskfs_open_fh_inode_cb, request);
} /* diskfs_open_fh */


/* inode_stash[0] = parent (locked across alloc / existing-inode fetch) */

static void
diskfs_open_at_finish(
    struct chimera_vfs_request *request,
    struct diskfs_inode        *parent,
    struct diskfs_inode        *inode)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;

    /* diskfs is CAP_OPEN_FILE_REQUIRED: every open (inferred or not) yields a
     * cached handle matched by a diskfs_close, so always pin the inode and stash
     * the real pointer in vfs_private.  read/write reuse it (and close releases
     * the pin); there is no throwaway/synthetic open_at for this backend. */
    diskfs_inode_ref_get(thread, inode);
    request->open_at.r_vfs_private = (uint64_t) inode;

    diskfs_map_attrs(thread, &request->open_at.r_dir_post_attr, parent);

    diskfs_map_attrs(thread, &request->open_at.r_attr, inode);

    diskfs_op_ok(request, p->txn);
} /* diskfs_open_at_finish */


static void
diskfs_open_at_existing_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *parent  = p->inode_stash[0];

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    /* A symlink as the final component under O_NOFOLLOW: a *data* open
     * (POSIX open(O_NOFOLLOW)) must fail with ELOOP, but an O_PATH-style open
     * (SMB FILE_OPEN_REPARSE_POINT, i.e. O_PATH|O_NOFOLLOW) wants a handle to
     * the link itself so the caller can read its attributes / security
     * descriptor / reparse data -- so fall through and open the symlink inode
     * in that case (mirrors memfs and the linux backend). */
    if (S_ISLNK(inode->mode) &&
        (request->open_at.flags & CHIMERA_VFS_OPEN_NOFOLLOW) &&
        !(request->open_at.flags & CHIMERA_VFS_OPEN_PATH)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ELOOP);
        return;
    }

    if ((request->open_at.flags & CHIMERA_VFS_OPEN_DIRECTORY) &&
        !S_ISDIR(inode->mode)) {
        diskfs_op_fail(request, p->txn,
                       S_ISLNK(inode->mode) ? CHIMERA_VFS_ESYMLINK : CHIMERA_VFS_ENOTDIR);
        return;
    }

    /* Access is enforced at the VFS layer (the credential-keyed gate in
     * chimera_vfs_read/write and the protocol's own create-time check), which
     * is ACL-aware and honors each protocol's access semantics; diskfs does not
     * re-check here -- a coarse mode-based read/write test would mis-handle SMB
     * opens that carry only control rights (e.g. WRITE_DAC) and not data access,
     * and would ignore a stored ACL that grants more (or less) than the mode.
     * (Mirrors the memfs and cairn backends.) */

    /* Overwrite/supersede disposition: replace the existing file's contents
     * (truncate to zero) and apply the new attributes (including DOS
     * attributes), mirroring memfs/cairn.  The SMB layer conveys the truncate
     * via the OPEN_TRUNCATE flag rather than a SIZE=0 set_attr, so honor both.
     * As with the pre-existing SIZE=0 path, this resets EOF without reclaiming
     * the data extents here (the open flow finishes synchronously); a non-empty
     * file's extents are reclaimed lazily / on the async setattr-truncate path. */
    if (S_ISREG(inode->mode) &&
        ((request->open_at.flags & CHIMERA_VFS_OPEN_TRUNCATE) ||
         ((request->open_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
          request->open_at.set_attr->va_size == 0))) {
        inode->size       = 0;
        inode->space_used = 0;
        diskfs_apply_attrs(inode, request->open_at.set_attr);
    }

    diskfs_open_at_finish(request, parent, inode);
} /* diskfs_open_at_existing_cb */


static void
diskfs_open_at_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    /* Signal to the protocol layer that this open created the file (vs.
     * opened an existing one) so the SMB CREATE reply reports the correct
     * Create Action (FILE_CREATED vs FILE_OPENED) for OPEN_IF / SUPERSEDE /
     * OVERWRITE_IF dispositions.  Matches memfs/cairn. */
    request->open_at.r_created = 1;
    diskfs_open_at_finish(request, p->inode_stash[0], p->inode_stash[1]);
} /* diskfs_open_at_inserted_cb */


/* The new file's ACL record (if any) is stored: link the dirent into the
 * parent. */
static void
diskfs_open_at_acl_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *parent  = p->inode_stash[0];
    struct diskfs_inode           *inode   = p->inode_stash[1];

    (void) result;
    diskfs_bt_op_free(thread, op);

    request->wait_reason = "openat:dir_insert";   /* diag: park-point localization */
    op                   = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_insert_async(op, thread, p->txn, parent,
                                request->open_at.name_hash, request->open_at.name,
                                request->open_at.namelen, inode->inum, inode->gen,
                                diskfs_open_at_inserted_cb, request)) {
        diskfs_open_at_inserted_cb(op, op->result, request);
    }
} /* diskfs_open_at_acl_cb */


static void
diskfs_open_at_alloc_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *parent  = p->inode_stash[0];
    struct diskfs_bt_op           *op;
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    inode->size       = 0;
    inode->space_used = 0;
    inode->uid        = request->cred->uid;
    inode->gid        = request->cred->gid;
    inode->nlink      = 1;
    inode->mode       = S_IFREG | 0644;
    inode->atime_sec  = now.tv_sec;
    inode->atime_nsec = now.tv_nsec;
    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;
    inode->change++;
    inode->btime_sec      = now.tv_sec;
    inode->btime_nsec     = now.tv_nsec;
    inode->dos_attributes = 0;

    /* Snapshot any explicit ACL pointer BEFORE diskfs_apply_attrs() rewrites
     * va_set_mask and drops the ATTR_ACL bit. */
    const struct chimera_acl *new_acl_open =
        (request->open_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_ACL)
        ? request->open_at.set_attr->va_acl : NULL;

    diskfs_apply_attrs(inode, request->open_at.set_attr);

    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;
    parent->change++;

    p->inode_stash[1] = inode;

    /* Seed the new file's ACL (inherited from the parent, or a Windows default
     * DACL for SMB creates) as the child's ACL record.  An explicit ACL in
     * set_attr (e.g. an SMB SD via SecD) takes precedence. */
    request->wait_reason = "openat:inherit_acl";   /* diag: park-point localization */
    op                   = diskfs_bt_op_alloc(thread);
    if (diskfs_inherit_acl_async(op, thread, p->txn, inode, parent,
                                 new_acl_open,
                                 request->cred->flavor == CHIMERA_VFS_AUTH_ATTR,
                                 diskfs_open_at_acl_cb, request)) {
        diskfs_open_at_acl_cb(op, op->result, request);
    }
} /* diskfs_open_at_alloc_cb */


static void
diskfs_open_at_check_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    unsigned int                   flags   = request->open_at.flags;

    diskfs_bt_op_free(thread, op);

    if (result < 0) {
        if (!(flags & CHIMERA_VFS_OPEN_CREATE)) {
            diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
            return;
        }

        /* Creating a new file requires add-file (WRITE_DATA) + search (EXECUTE)
         * permission on the parent directory.  On the NFSv4/Windows ACL model
         * WRITE_DATA == ADD_FILE and APPEND_DATA == ADD_SUBDIRECTORY, so a plain
         * file create is gated by WRITE_DATA (mkdir is gated by APPEND_DATA in
         * the VFS-core mkdir_at path).  Enforce POSIX semantics for AUTH_UNIX
         * callers (root is exempt); SMB/ACL (AUTH_ATTR) callers are authorized
         * by the engine. */
        if (request->cred->flavor == CHIMERA_VFS_AUTH_UNIX &&
            request->cred->uid != 0 &&
            !diskfs_inode_access(thread, p->inode_stash[0], request->cred,
                                 CHIMERA_ACE_WRITE_DATA | CHIMERA_ACE_EXECUTE)) {
            diskfs_op_fail(request, p->txn, CHIMERA_VFS_EACCES);
            return;
        }

        request->wait_reason = "openat:inode_alloc";   /* diag: park-point localization */
        diskfs_inode_alloc_async(thread, p->txn, diskfs_open_at_alloc_cb, request);
        return;
    }

    if (flags & CHIMERA_VFS_OPEN_EXCLUSIVE) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
        return;
    }

    {
        struct diskfs_dirent_rec *rec = (struct diskfs_dirent_rec *) p->rec_scratch;

        diskfs_inode_get_inum_async(thread, p->txn, rec->inum, rec->gen,
                                    diskfs_open_at_existing_cb, request);
    }
} /* diskfs_open_at_check_cb */


static void
diskfs_open_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->open_at.name_hash;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(parent->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    diskfs_map_attrs(thread, &request->open_at.r_dir_pre_attr, parent);

    p->inode_stash[0] = parent;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(op, thread, parent, hash, p->rec_scratch,
                                sizeof(p->rec_scratch), diskfs_open_at_check_cb,
                                request)) {
        diskfs_open_at_check_cb(op, op->result, request);
    }
} /* diskfs_open_at_parent_cb */


void
diskfs_open_at(
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
                              diskfs_open_at_parent_cb, request);
} /* diskfs_open_at */



static void
diskfs_create_unlinked_orphaned_cb(void *priv)
{
    struct chimera_vfs_request    *request = priv;
    struct diskfs_request_private *p       = request->plugin_data;

    diskfs_op_ok(request, p->txn);
} /* diskfs_create_unlinked_orphaned_cb */


static void
diskfs_create_unlinked_alloc_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    inode->size       = 0;
    inode->space_used = 0;
    inode->uid        = request->cred->uid;
    inode->gid        = request->cred->gid;
    inode->nlink      = 0;         /* anonymous; orphan-recorded below */
    inode->mode       = S_IFREG | 0644;
    inode->atime_sec  = now.tv_sec;
    inode->atime_nsec = now.tv_nsec;
    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;
    inode->change++;
    inode->btime_sec      = now.tv_sec;
    inode->btime_nsec     = now.tv_nsec;
    inode->dos_attributes = 0;

    diskfs_apply_attrs(inode, request->create_unlinked.set_attr);

    inode->refcnt++;     /* the open handle */
    request->create_unlinked.r_vfs_private = (uint64_t) inode;

    diskfs_map_attrs(thread, &request->create_unlinked.r_attr, inode);

    /* Anonymous from birth (nlink==0 => no namespace base reference): record
     * it on the durable orphan list in the creating txn, so a crash while it
     * is open reclaims it at the next mount, and the final handle close hands
     * it to the reclaim workers.  A later link_at removes the record. */
    diskfs_inode_orphaned(thread, p->txn, inode,
                          diskfs_create_unlinked_orphaned_cb, request);
} /* diskfs_create_unlinked_alloc_cb */


void
diskfs_create_unlinked(
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

    diskfs_inode_alloc_async(thread, p->txn,
                             diskfs_create_unlinked_alloc_cb, request);
} /* diskfs_create_unlinked */


static void
diskfs_close_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        /* The pinned inode could not be locked (only ENOENT, on a gen mismatch
         * after a free/reuse race -- the reservation went with the old inode).
         * Drop our handle ref and finish; there is nothing to return. */
        diskfs_txn_abort(p->txn);
        diskfs_inode_ref_drop(p->thread, inode);
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* Return this file's unused data-space reservation tail on close.  Done
     * unconditionally (not just at refcnt 0, which is the delete-on-close case):
     * a normal close drops the inode back to its idle cache reference, where it
     * would otherwise strand the reservation until eviction.  A file held open
     * by another handle simply re-reserves on its next write (the inode write
     * lock, held here, serializes that against this close).
     *
     * The write lock is essential: the reservation tail (inode->space_resv) is
     * a lock-free cache mutated only under the inode write lock by the data
     * path.  Returning it here without the lock raced a concurrent write that
     * was actively allocating from it (e.g. an in-flight WRITE on another
     * nconnect connection), double-freeing a block the write had just handed to
     * a file extent -- a fatal space-map double-free (#733). */
    diskfs_inode_return_reservation(p->thread, p->txn, inode);

    /* Drop the handle's reference under the lock, then release it (no durable
     * record: the reservation discard is purely an in-memory allocator
     * update).  The last ref drop on a deleted inode hands it to the reclaim
     * workers, whose drain re-acquires the write lock this txn is about to
     * release. */
    diskfs_inode_ref_drop(p->thread, inode);

    diskfs_txn_abort(p->txn);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* diskfs_close_inode_cb */


void
diskfs_close(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_inode           *inode = (struct diskfs_inode *) request->close.vfs_private;
    struct diskfs_request_private *p     = request->plugin_data;

    (void) shared;
    (void) private_data;

    /* Backend close is not a durable operation: open counts are in-memory, and
     * nlink==0 crash safety is recorded by the unlink/create-unlinked txn that
     * put the inode on the orphan list.  File-data reservation tails are
     * volatile, so dropping them only updates the live allocator view -- but
     * that drop must still happen under the inode write lock to serialize
     * against concurrent writes mutating the same reservation (see the cb).
     * The handle pinned the inode (refcnt), so it is resident; acquire its
     * lock by the pinned pointer (no fh/cache lookup) and finish in the cb.
     *
     * Use WRITE_NOPIN: close holds the inode exclusively only to drop the
     * volatile reservation and never reads or writes the on-disk dinode/b+tree.
     * Plain WRITE would fault the inode's home block (finish_write_pin) just to
     * abort it -- the dominant source of metadata read I/O, since close targets
     * idle inodes whose home block has usually been recycled out of cache. */
    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    diskfs_inode_acquire_pinned(thread, p->txn, inode, DISKFS_INODE_LOCK_WRITE_NOPIN,
                                diskfs_close_inode_cb, request);
} /* diskfs_close */


/* inode_stash[0] = parent (locked across alloc), inode_stash[1] = new symlink */

static void
diskfs_symlink_at_dirent_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_op_ok(request, p->txn);
} /* diskfs_symlink_at_dirent_cb */


static void
diskfs_symlink_at_target_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *inode   = p->inode_stash[1];

    (void) result;
    diskfs_bt_op_free(thread, op);

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_insert_async(op, thread, p->txn, p->inode_stash[0],
                                request->symlink_at.name_hash, request->symlink_at.name,
                                request->symlink_at.namelen, inode->inum, inode->gen,
                                diskfs_symlink_at_dirent_cb, request)) {
        diskfs_symlink_at_dirent_cb(op, op->result, request);
    }
} /* diskfs_symlink_at_target_cb */


static void
diskfs_symlink_at_alloc_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *parent  = p->inode_stash[0];
    struct diskfs_bt_op           *op;
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    inode->size       = request->symlink_at.targetlen;
    inode->space_used = request->symlink_at.targetlen;
    inode->uid        = request->cred->uid;
    inode->gid        = request->cred->gid;
    inode->nlink      = 1;
    inode->mode       = S_IFLNK | 0755;
    inode->atime_sec  = now.tv_sec;
    inode->atime_nsec = now.tv_nsec;
    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;
    inode->change++;
    inode->btime_sec      = now.tv_sec;
    inode->btime_nsec     = now.tv_nsec;
    inode->dos_attributes = 0;

    diskfs_map_attrs(thread, &request->symlink_at.r_attr, inode);

    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;
    parent->change++;

    diskfs_map_attrs(thread, &request->symlink_at.r_dir_post_attr, parent);

    /* Chain: insert the symlink target into the new inode's tree, then the
     * dirent into the parent. */
    p->inode_stash[1] = inode;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_symlink_set_async(op, thread, p->txn, inode,
                                 request->symlink_at.target,
                                 request->symlink_at.targetlen,
                                 diskfs_symlink_at_target_cb, request)) {
        diskfs_symlink_at_target_cb(op, op->result, request);
    }
} /* diskfs_symlink_at_alloc_cb */


static void
diskfs_symlink_at_check_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    diskfs_bt_op_free(thread, op);

    if (result >= 0) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
        return;
    }

    diskfs_map_attrs(thread, &request->symlink_at.r_dir_pre_attr, p->inode_stash[0]);
    diskfs_inode_alloc_async(thread, p->txn, diskfs_symlink_at_alloc_cb, request);
} /* diskfs_symlink_at_check_cb */


static void
diskfs_symlink_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->symlink_at.name_hash;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(parent->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    p->inode_stash[0] = parent;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(op, thread, parent, hash, p->rec_scratch,
                                sizeof(p->rec_scratch), diskfs_symlink_at_check_cb,
                                request)) {
        diskfs_symlink_at_check_cb(op, op->result, request);
    }
} /* diskfs_symlink_at_parent_cb */


void
diskfs_symlink_at(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;

    /* The target is the new inode's single b+tree record and must fit one
     * node; reject anything longer rather than aborting deeper in the insert. */
    if (request->symlink_at.targetlen > DISKFS_SYMLINK_TARGET_MAX) {
        request->status = CHIMERA_VFS_ENAMETOOLONG;
        request->complete(request);
        return;
    }

    p->txn = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    diskfs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              diskfs_symlink_at_parent_cb, request);
} /* diskfs_symlink_at */


static void
diskfs_readlink_done_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *inode   = p->inode_stash[0];

    diskfs_bt_op_free(p->thread, op);

    chimera_diskfs_abort_if(result < 0, "symlink record missing (inum %lu)", inode->inum);
    request->readlink.r_target_length = result;

    diskfs_map_attrs(p->thread, &request->readlink.r_attr, inode);
    diskfs_op_ok(request, p->txn);
} /* diskfs_readlink_done_cb */


static void
diskfs_readlink_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_bt_key           key     = { .type = DISKFS_REC_SYMLINK, .subkey = 0 };
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (unlikely(!S_ISLNK(inode->mode))) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EINVAL);
        return;
    }

    p->inode_stash[0] = inode;

    op = diskfs_bt_op_alloc(p->thread);
    if (diskfs_bt_lookup_async(op, p->thread, inode, DISKFS_BT_OP_LOOKUP_EXACT,
                               &key, NULL, request->readlink.r_target,
                               request->readlink.target_maxlength,
                               diskfs_readlink_done_cb, request)) {
        diskfs_readlink_done_cb(op, op->result, request);
    }
} /* diskfs_readlink_inode_cb */


void
diskfs_readlink(
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
                              diskfs_readlink_inode_cb, request);
} /* diskfs_readlink */


static inline int
diskfs_fh_compare(
    const void *fha,
    int         fha_len,
    const void *fhb,
    int         fhb_len)
{
    int minlen = fha_len < fhb_len ? fha_len : fhb_len;

    return memcmp(fha, fhb, minlen);
} /* diskfs_fh_compare */


static void
diskfs_rename_at_unlock_parents(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p  = request->plugin_data;
    struct diskfs_inode           *op = p->inode_stash[0];
    struct diskfs_inode           *np = p->inode_stash[1];

    if (op) {
    }
    if (np && np != op) {
    }
} /* diskfs_rename_at_unlock_parents */


/* Probe result for a replaced directory's first dirent: any hit means the
 * target is not empty and the rename must fail. */
static void
diskfs_rename_at_replaced_empty_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    int                            nonempty;

    nonempty = (result >= 0 && op->found_key.type == DISKFS_REC_DIRENT);
    diskfs_bt_op_free(p->thread, op);

    if (nonempty) {
        diskfs_rename_at_unlock_parents(request);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTEMPTY);
        return;
    }

    diskfs_rename_at_perform(request);
} /* diskfs_rename_at_replaced_empty_cb */


static void
diskfs_rename_at_existing_cb(
    struct diskfs_inode *existing_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *child   = p->inode_stash[2];

    if (status != CHIMERA_VFS_OK) {
        /* Existing dirent referenced a stale inum — proceed without delete. */
        p->inode_stash[3] = NULL;
        diskfs_rename_at_perform(request);
        return;
    }

    if (S_ISDIR(child->mode) != S_ISDIR(existing_inode->mode)) {
        int err = S_ISDIR(existing_inode->mode) ? CHIMERA_VFS_EISDIR : CHIMERA_VFS_ENOTDIR;
        diskfs_rename_at_unlock_parents(request);
        diskfs_op_fail(request, p->txn, err);
        return;
    }
    p->inode_stash[3] = existing_inode;

    /* Replacing a directory requires it empty: probe for any dirent record
     * (asynchronously -- the target's tree may not be cached). */
    if (S_ISDIR(existing_inode->mode)) {
        struct diskfs_bt_op *op = diskfs_bt_op_alloc(p->thread);

        if (diskfs_dir_next_async(op, p->thread, existing_inode, 0,
                                  &op->found_key, p->rec_scratch,
                                  sizeof(p->rec_scratch),
                                  diskfs_rename_at_replaced_empty_cb,
                                  request)) {
            diskfs_rename_at_replaced_empty_cb(op, op->result, request);
        }
        return;
    }

    diskfs_rename_at_perform(request);
} /* diskfs_rename_at_existing_cb */


static void
diskfs_rename_at_perform_final_cb(
    struct diskfs_bt_op *bop,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *op      = p->inode_stash[0];
    struct diskfs_inode           *np      = p->inode_stash[1];
    struct diskfs_inode           *child   = p->inode_stash[2];
    struct timespec                now;

    (void) result;
    diskfs_bt_op_free(thread, bop);

    clock_gettime(CLOCK_REALTIME, &now);

    if (S_ISDIR(child->mode) && np != op) {
        /* Cross-directory move of a directory: shift the subdirectory backlink
         * from the source parent to the destination parent, and re-home the
         * moved directory's ".." (diskfs derives ".." from parent_inum). */
        op->nlink--;
        np->nlink++;
        child->parent_inum = np->inum;
        child->parent_gen  = np->gen;
    }

    op->mtime_sec  = now.tv_sec;
    op->mtime_nsec = now.tv_nsec;
    op->ctime_sec  = now.tv_sec;
    op->ctime_nsec = now.tv_nsec;
    op->change++;
    if (np != op) {
        np->mtime_sec  = now.tv_sec;
        np->mtime_nsec = now.tv_nsec;
        np->ctime_sec  = now.tv_sec;
        np->ctime_nsec = now.tv_nsec;
        np->change++;
    }

    /* POSIX: a successful rename marks the renamed file's status-change time. */
    child->ctime_sec  = now.tv_sec;
    child->ctime_nsec = now.tv_nsec;
    child->change++;

    diskfs_map_attrs(thread, &request->rename_at.r_fromdir_post_attr, op);
    diskfs_map_attrs(thread, &request->rename_at.r_todir_post_attr, np);

    diskfs_rename_at_unlock_parents(request);
    diskfs_op_ok(request, p->txn);
} /* diskfs_rename_at_perform_final_cb */


static void
diskfs_rename_at_perform_inserted_cb(
    struct diskfs_bt_op *bop,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    (void) result;
    diskfs_bt_op_free(thread, bop);

    bop = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_remove_async(bop, thread, p->txn, p->inode_stash[0],
                                request->rename_at.name_hash,
                                diskfs_rename_at_perform_final_cb, request)) {
        diskfs_rename_at_perform_final_cb(bop, bop->result, request);
    }
} /* diskfs_rename_at_perform_inserted_cb */


static void
diskfs_rename_at_perform_insert(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *bop    = diskfs_bt_op_alloc(thread);

    if (diskfs_dir_insert_async(bop, thread, p->txn, p->inode_stash[1],
                                request->rename_at.new_name_hash,
                                request->rename_at.new_name,
                                request->rename_at.new_namelen,
                                p->rd_inum, p->rd_gen,
                                diskfs_rename_at_perform_inserted_cb, request)) {
        diskfs_rename_at_perform_inserted_cb(bop, bop->result, request);
    }
} /* diskfs_rename_at_perform_insert */


/* The replaced target's orphan record is in place: continue with the new
 * dirent insert. */
static void
diskfs_rename_at_replaced_orphaned_cb(void *priv)
{
    diskfs_rename_at_perform_insert(priv);
} /* diskfs_rename_at_replaced_orphaned_cb */


static void
diskfs_rename_at_perform_removed_cb(
    struct diskfs_bt_op *bop,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request        = private_data;
    struct diskfs_request_private *p              = request->plugin_data;
    struct diskfs_inode           *np             = p->inode_stash[1];
    struct diskfs_inode           *existing_inode = p->inode_stash[3];

    diskfs_bt_op_free(p->thread, bop);

    if (result == 1) {
        if (S_ISDIR(existing_inode->mode)) {
            /* A replaced directory must be empty (validated above); like
            * remove_at, the delete zeroes its self+parent link count. */
            existing_inode->nlink = 0;
            np->nlink--;
        } else {
            existing_inode->nlink--;
        }
        if (existing_inode->nlink == 0) {
            diskfs_inode_orphaned(p->thread, p->txn, existing_inode,
                                  diskfs_rename_at_replaced_orphaned_cb,
                                  request);
            return;
        }
    }

    diskfs_rename_at_perform_insert(request);
} /* diskfs_rename_at_perform_removed_cb */


static void
diskfs_rename_at_perform(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p              = request->plugin_data;
    struct diskfs_thread          *thread         = p->thread;
    struct diskfs_inode           *existing_inode = p->inode_stash[3];
    struct diskfs_bt_op           *bop;

    if (existing_inode) {
        bop = diskfs_bt_op_alloc(thread);
        if (diskfs_dir_remove_async(bop, thread, p->txn, p->inode_stash[1],
                                    request->rename_at.new_name_hash,
                                    diskfs_rename_at_perform_removed_cb, request)) {
            diskfs_rename_at_perform_removed_cb(bop, bop->result, request);
        }
        return;
    }

    diskfs_rename_at_perform_insert(request);
} /* diskfs_rename_at_perform */


/* The dest-name lookup completed; decide replace vs hardlink-shortcut. */
static void
diskfs_rename_at_dest_cb(
    struct diskfs_bt_op *bop,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *op      = p->inode_stash[0];
    struct diskfs_inode           *np      = p->inode_stash[1];
    struct diskfs_dirent_rec      *rec     = (struct diskfs_dirent_rec *) p->rec_scratch;
    uint64_t                       existing_inum;
    uint32_t                       existing_gen;

    diskfs_bt_op_free(thread, bop);

    if (result < 0) {
        p->inode_stash[3] = NULL;
        diskfs_rename_at_perform(request);
        return;
    }

    existing_inum = rec->inum;
    existing_gen  = rec->gen;

    /* Hardlink shortcut: source and dest already refer to the same inode. */
    if (existing_inum == p->rd_inum && existing_gen == p->rd_gen) {
        diskfs_map_attrs(thread, &request->rename_at.r_fromdir_post_attr, op);
        diskfs_map_attrs(thread, &request->rename_at.r_todir_post_attr, np);
        diskfs_rename_at_unlock_parents(request);
        diskfs_op_ok(request, p->txn);
        return;
    }

    diskfs_inode_get_inum_async(thread, p->txn, existing_inum, existing_gen,
                                diskfs_rename_at_existing_cb, request);
} /* diskfs_rename_at_dest_cb */


static void
diskfs_rename_at_child_cb(
    struct diskfs_inode *child,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *np      = p->inode_stash[1];

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_rename_at_unlock_parents(request);
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    p->inode_stash[2] = child;

    /* POSIX: a directory may not be renamed into itself or one of its own
     * descendants (EINVAL).  This must be detected before mutating the
     * namespace; otherwise the directory would be spliced under itself and the
     * subtree orphaned.  Only a directory source can be an ancestor of the
     * destination, so the walk is skipped for non-directories.  Walk the
     * destination parent's ancestry up to the root, comparing each ancestor
     * against the source child. */
    if (S_ISDIR(child->mode)) {
        p->anc_inum  = np->inum;
        p->anc_gen   = np->gen;
        p->anc_depth = 0;
        diskfs_rename_at_check_descendant_step(request);
        return;
    }

    diskfs_rename_at_dest_lookup(request);
} /* diskfs_rename_at_child_cb */


/* Look up the destination name in the (already-locked) destination parent and
 * decide replace vs hardlink-shortcut.  Reached once the descendant check (if
 * any) has cleared. */
static void
diskfs_rename_at_dest_lookup(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_inode           *np     = p->inode_stash[1];
    struct diskfs_bt_op           *bop;

    bop = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(bop, thread, np, request->rename_at.new_name_hash,
                                p->rec_scratch, sizeof(p->rec_scratch),
                                diskfs_rename_at_dest_cb, request)) {
        diskfs_rename_at_dest_cb(bop, bop->result, request);
    }
} /* diskfs_rename_at_dest_lookup */


/*
 * One step of the destination-parent ancestry walk for the rename(2)
 * descendant check.  p->anc_inum/anc_gen is the ancestor currently under
 * examination.  If it is the source directory, the rename would make the
 * directory its own descendant -> EINVAL.  Otherwise acquire it, read its
 * parent link, release it (unless it is one of the two already-held parents),
 * and recurse on the parent until the root (a self-parent) is reached.
 *
 * Acquiring with READ (not the txn's WRITE mode) and holding at most one
 * transient ancestor at a time keeps the walk within the txn's inode-slot
 * budget and avoids escalating locks on the path.  The two held parents are
 * returned by the txn fast-path without re-locking, so this never
 * self-deadlocks on them.
 */
static void
diskfs_rename_at_check_descendant_step(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_inode           *child  = p->inode_stash[2];

    /* The destination parent (or an ancestor of it) is the source directory
     * itself: renaming into it would splice the subtree under itself. */
    if (p->anc_inum == child->inum && p->anc_gen == child->gen) {
        diskfs_rename_at_unlock_parents(request);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EINVAL);
        return;
    }

    if (unlikely(p->anc_depth++ >= CHIMERA_VFS_PATH_MAX)) {
        /* Defensive: a corrupt parent cycle would otherwise loop forever. */
        diskfs_rename_at_unlock_parents(request);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ELOOP);
        return;
    }

    diskfs_inode_acquire(thread, p->txn, p->anc_inum, p->anc_gen,
                         DISKFS_INODE_LOCK_READ,
                         diskfs_rename_at_descendant_cb, request);
} /* diskfs_rename_at_check_descendant_step */


static void
diskfs_rename_at_descendant_cb(
    struct diskfs_inode *anc,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *op      = p->inode_stash[0];
    struct diskfs_inode           *np      = p->inode_stash[1];
    uint64_t                       par_inum;
    uint32_t                       par_gen;
    int                            is_root;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        /* The ancestor vanished mid-walk (concurrent removal): treat as a clean
         * path -- the rename's own dirent lookups will surface any real error. */
        diskfs_rename_at_dest_lookup(request);
        return;
    }

    par_inum = anc->parent_inum;
    par_gen  = anc->parent_gen;
    is_root  = (anc->parent_inum == anc->inum && anc->parent_gen == anc->gen);

    /* Release the ancestor unless it is one of the two parents the rename holds
     * for the duration (those were returned by the txn fast-path, not relocked,
     * so they must not be dropped here). */
    if (anc != op && anc != np) {
        diskfs_txn_unlock_inode(p->txn, anc);
    }

    if (is_root) {
        /* Reached the filesystem root without crossing the source: safe. */
        diskfs_rename_at_dest_lookup(request);
        return;
    }

    p->anc_inum = par_inum;
    p->anc_gen  = par_gen;
    diskfs_rename_at_check_descendant_step(request);
} /* diskfs_rename_at_descendant_cb */


/* The source-name lookup completed; capture old inum/gen and fetch the
 * child inode. */
static void
diskfs_rename_at_source_cb(
    struct diskfs_bt_op *bop,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_dirent_rec      *rec     = (struct diskfs_dirent_rec *) p->rec_scratch;

    diskfs_bt_op_free(thread, bop);

    if (result < 0) {
        diskfs_rename_at_unlock_parents(request);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    p->rd_inum = rec->inum;
    p->rd_gen  = rec->gen;

    diskfs_inode_get_inum_async(thread, p->txn, rec->inum, rec->gen,
                                diskfs_rename_at_child_cb, request);
} /* diskfs_rename_at_source_cb */


static void
diskfs_rename_at_have_parents(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_inode           *op     = p->inode_stash[0];
    struct diskfs_inode           *np     = p->inode_stash[1];
    struct diskfs_bt_op           *bop;

    diskfs_map_attrs(thread, &request->rename_at.r_fromdir_pre_attr, op);
    diskfs_map_attrs(thread, &request->rename_at.r_todir_pre_attr, np);

    bop = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(bop, thread, op, request->rename_at.name_hash,
                                p->rec_scratch, sizeof(p->rec_scratch),
                                diskfs_rename_at_source_cb, request)) {
        diskfs_rename_at_source_cb(bop, bop->result, request);
    }
} /* diskfs_rename_at_have_parents */


static void
diskfs_rename_at_second_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    int                            cmp;

    cmp = diskfs_fh_compare(request->fh, request->fh_len,
                            request->rename_at.new_fh,
                            request->rename_at.new_fhlen);

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_rename_at_unlock_parents(request);
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(inode->mode)) {
        diskfs_rename_at_unlock_parents(request);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    if (cmp < 0) {
        p->inode_stash[1] = inode;
    } else {
        p->inode_stash[0] = inode;
    }
    diskfs_rename_at_have_parents(request);
} /* diskfs_rename_at_second_cb */


static void
diskfs_rename_at_first_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    int                            cmp;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(inode->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    cmp = diskfs_fh_compare(request->fh, request->fh_len,
                            request->rename_at.new_fh,
                            request->rename_at.new_fhlen);

    if (cmp == 0) {
        p->inode_stash[0] = inode;
        p->inode_stash[1] = inode;
        diskfs_rename_at_have_parents(request);
        return;
    }

    if (cmp < 0) {
        p->inode_stash[0] = inode;
        diskfs_inode_get_fh_async(thread, p->txn,
                                  request->rename_at.new_fh,
                                  request->rename_at.new_fhlen,
                                  diskfs_rename_at_second_cb, request);
    } else {
        p->inode_stash[1] = inode;
        diskfs_inode_get_fh_async(thread, p->txn,
                                  request->fh, request->fh_len,
                                  diskfs_rename_at_second_cb, request);
    }
} /* diskfs_rename_at_first_cb */


void
diskfs_rename_at(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;
    int                            cmp;

    (void) shared;
    (void) private_data;

    p->thread         = thread;
    p->txn            = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);
    p->inode_stash[0] = NULL;
    p->inode_stash[1] = NULL;
    p->inode_stash[2] = NULL;
    p->inode_stash[3] = NULL;

    cmp = diskfs_fh_compare(request->fh, request->fh_len,
                            request->rename_at.new_fh,
                            request->rename_at.new_fhlen);

    if (cmp <= 0) {
        diskfs_inode_get_fh_async(thread, p->txn,
                                  request->fh, request->fh_len,
                                  diskfs_rename_at_first_cb, request);
    } else {
        diskfs_inode_get_fh_async(thread, p->txn,
                                  request->rename_at.new_fh,
                                  request->rename_at.new_fhlen,
                                  diskfs_rename_at_first_cb, request);
    }
} /* diskfs_rename_at */


/* inode_stash[0] = parent dir; inode_stash[1] = link target inode (both locked) */

static void
diskfs_link_at_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_op_ok(request, p->txn);
} /* diskfs_link_at_inserted_cb */


/* Linking an anonymous (created-unlinked) inode into the namespace: its
 * orphan record is removed in the same txn; continue with the link. */
static void
diskfs_link_at_unorphaned_cb(void *priv)
{
    diskfs_link_at_finish(priv);
} /* diskfs_link_at_unorphaned_cb */


static void
diskfs_link_at_finish(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_inode           *parent = p->inode_stash[0];
    struct diskfs_inode           *inode  = p->inode_stash[1];
    uint64_t                       hash   = request->link_at.name_hash;
    struct diskfs_bt_op           *op;
    struct timespec                now;

    /* An nlink==0 source is an anonymous inode (create_unlinked) entering the
     * namespace: it re-takes the namespace's base reference and leaves the
     * durable orphan list before the link proceeds.  (A deleted-and-
     * unreferenced source was already rejected at fetch time.) */
    if (inode->nlink == 0 && !p->op_scratch) {
        p->op_scratch = 1;     /* unorphan once; this re-enters */
        inode->refcnt++;
        diskfs_orphan_op_start(thread, p->txn, inode->inum, inode->gen,
                               1 /* remove */, diskfs_link_at_unorphaned_cb,
                               request);
        return;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    inode->nlink++;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;
    inode->change++;
    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;
    parent->change++;

    diskfs_map_attrs(thread, &request->link_at.r_attr, inode);
    diskfs_map_attrs(thread, &request->link_at.r_dir_post_attr, parent);

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_insert_async(op, thread, p->txn, parent, hash,
                                request->link_at.name, request->link_at.namelen,
                                inode->inum, inode->gen, diskfs_link_at_inserted_cb,
                                request)) {
        diskfs_link_at_inserted_cb(op, op->result, request);
    }
} /* diskfs_link_at_finish */


/* The replaced target's orphan record is in place: continue with the link. */
static void
diskfs_link_at_replaced_orphaned_cb(void *priv)
{
    diskfs_link_at_finish(priv);
} /* diskfs_link_at_replaced_orphaned_cb */


static void
diskfs_link_at_existing_cb(
    struct diskfs_inode *existing_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    /* The old dirent was already removed; if its inode is still resident,
     * drop its link count for the replace. */
    if (status == CHIMERA_VFS_OK) {
        existing_inode->nlink--;
        diskfs_map_attrs(p->thread, &request->link_at.r_replaced_attr,
                         existing_inode);
        if (existing_inode->nlink == 0) {
            diskfs_inode_orphaned(p->thread, p->txn, existing_inode,
                                  diskfs_link_at_replaced_orphaned_cb,
                                  request);
            return;
        }
    }

    diskfs_link_at_finish(request);
} /* diskfs_link_at_existing_cb */


/* dir_remove of the replaced dirent completed; fetch the old inode to fix up
 * its link count. */
static void
diskfs_link_at_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_inode_get_inum_async(p->thread, p->txn, p->rd_inum, p->rd_gen,
                                diskfs_link_at_existing_cb, request);
} /* diskfs_link_at_removed_cb */


static void
diskfs_link_at_check_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_dirent_rec      *rec     = (struct diskfs_dirent_rec *) p->rec_scratch;
    uint64_t                       hash    = request->link_at.name_hash;

    diskfs_bt_op_free(thread, op);

    if (result >= 0) {
        if (request->link_at.replace) {
            p->rd_inum = rec->inum;
            p->rd_gen  = rec->gen;

            op = diskfs_bt_op_alloc(thread);
            if (diskfs_dir_remove_async(op, thread, p->txn, p->inode_stash[0], hash,
                                        diskfs_link_at_removed_cb, request)) {
                diskfs_link_at_removed_cb(op, op->result, request);
            }
            return;
        }
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
        return;
    }

    diskfs_link_at_finish(request);
} /* diskfs_link_at_check_cb */


static void
diskfs_link_at_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *parent  = p->inode_stash[0];
    uint64_t                       hash    = request->link_at.name_hash;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (unlikely(S_ISDIR(inode->mode))) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EISDIR);
        return;
    }

    /* A deleted, unreferenced inode is dead -- it is queued for (or already
     * under) background reclaim, so it must not re-enter the namespace.  An
     * nlink==0 inode with open handles is a live anonymous file
     * (create_unlinked) and may be linked. */
    if (unlikely(inode->nlink == 0 && inode->refcnt == 0)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    p->inode_stash[1] = inode;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(op, thread, parent, hash, p->rec_scratch,
                                sizeof(p->rec_scratch), diskfs_link_at_check_cb,
                                request)) {
        diskfs_link_at_check_cb(op, op->result, request);
    }
} /* diskfs_link_at_inode_cb */


static void
diskfs_link_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    diskfs_map_attrs(thread, &request->link_at.r_dir_pre_attr, parent);

    if (!S_ISDIR(parent->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    p->inode_stash[0] = parent;
    diskfs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              diskfs_link_at_inode_cb, request);
} /* diskfs_link_at_parent_cb */


void
diskfs_link_at(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread     = thread;
    p->txn        = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);
    p->op_scratch = 0;     /* anonymous-source unorphan latch (link_at_finish) */

    diskfs_inode_get_fh_async(thread, p->txn,
                              request->link_at.dir_fh,
                              request->link_at.dir_fhlen,
                              diskfs_link_at_parent_cb, request);
} /* diskfs_link_at */
