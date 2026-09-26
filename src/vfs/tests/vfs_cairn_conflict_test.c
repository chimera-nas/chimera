// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/test_host.h"
#include <rocksdb/c.h>
#undef NDEBUG
#include <assert.h>

static void interleaved_put(
    rocksdb_transaction_t *txn,
    const char            *key,
    size_t                 key_len,
    const char            *value,
    size_t                 value_len,
    char                 **err);

/* Compile the real backend with one scheduling seam: before CLOSE writes its
 * inode, commit a RENAME on another worker. This exercises the production
 * read path and deferred retry without depending on OS thread timing or
 * exporting backend internals. Rename the module symbol to keep this private
 * copy distinct from the normal backend linked through chimera_vfs. */
#define rocksdb_transaction_put interleaved_put
#define vfs_cairn               conflict_test_cairn
#include "../cairn/cairn.c"
#undef vfs_cairn
#undef rocksdb_transaction_put

static struct cairn_thread       *closing_thread;
static void                       (*before_put)(
    void);
static unsigned int               close_writes;
static struct cairn_thread       *renaming_thread;
static struct chimera_vfs_request rename_request;
static unsigned int               rename_completions;

static void
interleaved_put(
    rocksdb_transaction_t *txn,
    const char            *key,
    size_t                 key_len,
    const char            *value,
    size_t                 value_len,
    char                 **err)
{
    if (closing_thread && txn == closing_thread->meta_txn) {
        close_writes++;
    }
    if (before_put) {
        void (*interleave)(
            void) = before_put;

        before_put = NULL;
        interleave();
    }
    rocksdb_transaction_put(txn, key, key_len, value, value_len, err);
} /* interleaved_put */

static void
completed(struct chimera_vfs_request *request)
{
    unsigned int *count = request->proto_private_data;

    assert(request->status == CHIMERA_VFS_OK);
    ++*count;
} /* completed */

static void
rename_over_victim(void)
{
    cairn_dispatch(&rename_request, renaming_thread);
    assert(rename_completions == 0);
    cairn_thread_commit(NULL, renaming_thread);
    assert(rename_completions == 1);
} /* rename_over_victim */

static void
seed_inode(
    struct cairn_thread *thread,
    uint64_t             inum,
    unsigned int         mode,
    unsigned int         nlink,
    unsigned int         refcnt)
{
    struct cairn_inode inode = { 0 };

    inode.inum        = inum;
    inode.parent_inum = 1;
    inode.gen         = 1;
    inode.mode        = mode;
    inode.nlink       = nlink;
    inode.refcnt      = refcnt;
    cairn_put_inode(thread, &inode);
} /* seed_inode */

static void
seed_name(
    struct cairn_thread *thread,
    const char          *name,
    uint64_t             inum)
{
    struct cairn_dirent_key   key = {
        .keytype = CAIRN_KEY_DIRENT,
        .inum    = 1,
        .hash    = chimera_vfs_hash(name, strlen(name))
    };
    struct cairn_dirent_value value = { 0 };

    value.inum     = inum;
    value.name_len = strlen(name);
    memcpy(value.name, name, value.name_len);
    cairn_put_dirent(thread, &key, &value);
} /* seed_name */

static void
check_name(
    struct cairn_thread *thread,
    const char          *name,
    uint64_t             inum)
{
    struct cairn_dirent_key    key = {
        .keytype = CAIRN_KEY_DIRENT,
        .inum    = 1,
        .hash    = chimera_vfs_hash(name, strlen(name))
    };
    struct cairn_dirent_handle dh;
    int                        rc = cairn_dirent_get(thread, &key, &dh);

    if (inum) {
        assert(rc == 0);
        assert(dh.dirent->inum == inum);
        cairn_dirent_handle_release(&dh);
    } else {
        assert(rc != 0);
    }
} /* check_name */

int
main(void)
{
    char                       path[] = "cairn-conflict-XXXXXX";
    char                       config[256];
    struct cairn_shared       *shared;
    struct cairn_thread        closer = { 0 }, renamer = { 0 };
    struct cairn_fs            fs            = { .fsid = 1, .root_inum = 1, .root_gen = 1 };
    struct chimera_vfs_request close_request = { 0 };
    struct cairn_inode_handle  ih;
    unsigned int               close_completions                   = 0;
    uint8_t                    mount_id[CHIMERA_VFS_MOUNT_ID_SIZE] = { 1 };

    assert(mkdtemp(path));
    snprintf(config, sizeof(config), "{\"path\":\"%s\",\"initialize\":true}", path);
    chimera_log_init();
    shared        = cairn_init(config, NULL);
    closer.shared = renamer.shared = shared;
    /* Drive the workers' deferred commits explicitly. There is no event loop
     * in this deterministic schedule, so suppress evpl_defer on each batch. */
    closer.commit_scheduled = renamer.commit_scheduled = 1;
    fs.root_fhlen           = chimera_vfs_encode_fh_inum_parent(mount_id, 1, 1, fs.root_fh);

    seed_inode(&renamer, 1, S_IFDIR | 0777, 2, 1);
    seed_inode(&renamer, 2, S_IFREG | 0666, 2, 2);
    seed_inode(&renamer, 3, S_IFREG | 0666, 1, 1);
    seed_name(&renamer, "a", 2);
    seed_name(&renamer, "c", 2);
    seed_name(&renamer, "d", 3);
    cairn_thread_commit(NULL, &renamer);
    renamer.commit_scheduled = 1;

    rename_request.opcode = CHIMERA_VFS_OP_RENAME_AT;
    memcpy(rename_request.fh, fs.root_fh, fs.root_fhlen);
    rename_request.fh_len                  = fs.root_fhlen;
    rename_request.mount_private           = &fs;
    rename_request.complete                = completed;
    rename_request.proto_private_data      = &rename_completions;
    rename_request.rename_at.name          = "d";
    rename_request.rename_at.namelen       = 1;
    rename_request.rename_at.name_hash     = chimera_vfs_hash("d", 1);
    rename_request.rename_at.new_fh        = fs.root_fh;
    rename_request.rename_at.new_fhlen     = fs.root_fhlen;
    rename_request.rename_at.new_name      = "a";
    rename_request.rename_at.new_namelen   = 1;
    rename_request.rename_at.new_name_hash = chimera_vfs_hash("a", 1);

    close_request.opcode             = CHIMERA_VFS_OP_CLOSE;
    close_request.mount_private      = &fs;
    close_request.close.vfs_private  = 2;
    close_request.complete           = completed;
    close_request.proto_private_data = &close_completions;
    closing_thread                   = &closer;
    renaming_thread                  = &renamer;
    before_put                       = rename_over_victim;
    cairn_dispatch(&close_request, &closer);
    assert(!before_put && close_completions == 0);
    cairn_thread_commit(NULL, &closer);
    assert(close_completions == 1 && rename_completions == 1);

    cairn_read_begin(&closer, 0);
    assert(cairn_inode_get_inum(&closer, 2, &ih) == 0);
    fprintf(stderr, "after rename-over and close: nlink=%u refcnt=%u close writes=%u\n",
            ih.inode->nlink, ih.inode->refcnt, close_writes);
    assert(ih.inode->nlink == 1);
    assert(ih.inode->refcnt == 1);
    assert(close_writes == 2); /* The stale close was retried, not acknowledged. */
    cairn_inode_handle_release(&ih);
    check_name(&closer, "a", 3);
    check_name(&closer, "c", 2);
    check_name(&closer, "d", 0);
    cairn_read_end(&closer);

    assert(!closer.meta_txn && !renamer.meta_txn);
    closing_thread = NULL;
    cairn_destroy(shared);
    assert(chimera_test_remove_tree(path) == 0);
    return 0;
} /* main */
