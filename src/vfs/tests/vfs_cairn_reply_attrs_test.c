// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/test_host.h"
#undef NDEBUG
#include <assert.h>
#include "vfs/vfs_internal.h"

/* Execute the real backend, but leave its posted completions unconsumed until
 * another request has reused the worker's ACL/SID scratch. This is the exact
 * delegation schedule that used to turn an allowed SMB rename into EACCES. */
#define vfs_cairn reply_attrs_test_cairn
#include "../cairn/cairn.c"
#undef vfs_cairn

static unsigned int completions;

static void
posted(struct chimera_vfs_request *request)
{
    assert(request->status == CHIMERA_VFS_OK);
    completions++;
} /* posted */

static void
get_attrs(
    struct cairn_thread        *thread,
    struct cairn_fs            *fs,
    struct chimera_vfs_request *request,
    const uint8_t              *mount_id,
    uint64_t                    inum)
{
    request->opcode                     = CHIMERA_VFS_OP_GETATTR;
    request->fh_len                     = chimera_vfs_encode_fh_inum_parent(mount_id, inum, 1, request->fh);
    request->mount_private              = fs;
    request->complete                   = posted;
    request->getattr.r_attr.va_req_mask = CHIMERA_VFS_ATTR_MASK_STAT |
        CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_OWNER_SID | CHIMERA_VFS_ATTR_GROUP_SID;
    cairn_dispatch(request, thread);
} /* get_attrs */

int
main(void)
{
    char                       path[] = "cairn-reply-attrs-XXXXXX", config[256];
    uint8_t                    mount_id[CHIMERA_VFS_MOUNT_ID_SIZE] = { 1 };
    struct cairn_shared       *shared;
    struct cairn_thread        worker = { 0 };
    struct cairn_fs            fs     = { .fsid = 1, .root_inum = 1, .root_gen = 1 };
    struct cairn_inode         inode  = {
        .inum        = 1,
        .parent_inum = 1,
        .gen         = 1,
        .mode        = S_IFDIR | 0777,
        .nlink       = 2,
        .refcnt      = 1
    };
    struct chimera_vfs_request first = { 0 }, second = { 0 };
    struct chimera_sid         owner, group, other_owner, other_group;
    /* Larger than plugin_data: preserving an ACL must not depend on fitting
     * into the backend's fixed-size request scratch. */
    size_t                     acl_size = chimera_acl_size(128);
    struct chimera_acl        *acl      = calloc(1, acl_size);

    assert(acl);
    acl->num_aces = 128;
    for (unsigned int i = 0; i < acl->num_aces; i++) {
        acl->aces[i].type        = CHIMERA_ACE_ALLOWED;
        acl->aces[i].access_mask = CHIMERA_ACE_MASK_ALL;
        acl->aces[i].who.type    = CHIMERA_PRINCIPAL_SPECIAL;
        acl->aces[i].who.special = CHIMERA_WHO_EVERYONE;
    }
    assert(chimera_sid_from_str(&owner, "S-1-5-21-11-22-33-1001") == 0);
    assert(chimera_sid_from_str(&group, "S-1-5-21-11-22-33-1002") == 0);
    assert(chimera_sid_from_str(&other_owner, "S-1-5-21-44-55-66-2001") == 0);
    assert(chimera_sid_from_str(&other_group, "S-1-5-21-44-55-66-2002") == 0);
    assert(mkdtemp(path));
    snprintf(config, sizeof(config), "{\"path\":\"%s\",\"initialize\":true}", path);
    chimera_log_init();
    shared                  = cairn_init(config, NULL);
    worker.shared           = shared;
    worker.commit_scheduled = 1;
    fs.root_fhlen           = chimera_vfs_encode_fh_inum_parent(mount_id, 1, 1, fs.root_fh);
    cairn_put_inode(&worker, &inode);
    cairn_put_acl(&worker, 1, acl);
    cairn_put_sids(&worker, 1, &owner, &group);
    inode.inum = 2;
    inode.mode = S_IFREG;
    cairn_put_inode(&worker, &inode);
    cairn_put_sids(&worker, 2, &other_owner, &other_group);
    cairn_thread_commit(NULL, &worker);

    get_attrs(&worker, &fs, &first, mount_id, 1);
    get_attrs(&worker, &fs, &second, mount_id, 2);
    assert(completions == 2);
    assert(first.getattr.r_attr.va_acl->num_aces == 128);
    assert(memcmp(first.getattr.r_attr.va_acl, acl, acl_size) == 0);
    assert(chimera_sid_equal(first.getattr.r_attr.va_owner_sid, &owner));
    assert(chimera_sid_equal(first.getattr.r_attr.va_group_sid, &group));
    assert(chimera_sid_equal(second.getattr.r_attr.va_owner_sid, &other_owner));
    assert(chimera_sid_equal(second.getattr.r_attr.va_group_sid, &other_group));
    /* The core performs this cleanup after the consumer callback, before
     * returning a request to its pool. Exercise reuse without retaining data. */
    chimera_vfs_request_free_memory(&first);
    chimera_vfs_request_free_memory(&second);
    assert(!first.memory && !second.memory);
    first.getattr.r_attr.va_set_mask = 0;
    get_attrs(&worker, &fs, &first, mount_id, 2);
    assert(chimera_sid_equal(first.getattr.r_attr.va_owner_sid, &other_owner));
    chimera_vfs_request_free_memory(&first);
    /* A mkdir reply waits for its write batch to commit, before it even
     * reaches the delegation completion queue. Keep that ACL stable too. */
    struct chimera_vfs_request mkdir_req = { 0 };
    struct chimera_vfs_cred    cred      = { 0 };
    struct chimera_vfs_attrs   set_attr  = {
        .va_set_mask = CHIMERA_VFS_ATTR_ACL,
        .va_acl      = acl
    };
    unsigned int               before = completions;

    worker.next_inum        = 3;
    worker.commit_scheduled = 1;
    mkdir_req.opcode        = CHIMERA_VFS_OP_MKDIR_AT;
    mkdir_req.cred          = &cred;
    mkdir_req.mount_private = &fs;
    mkdir_req.complete      = posted;
    memcpy(mkdir_req.fh, fs.root_fh, fs.root_fhlen);
    mkdir_req.fh_len                      = fs.root_fhlen;
    mkdir_req.mkdir_at.name               = "queued";
    mkdir_req.mkdir_at.name_len           = 6;
    mkdir_req.mkdir_at.name_hash          = chimera_vfs_hash("queued", 6);
    mkdir_req.mkdir_at.set_attr           = &set_attr;
    mkdir_req.mkdir_at.r_attr.va_req_mask = CHIMERA_VFS_ATTR_ACL;
    cairn_dispatch(&mkdir_req, &worker);
    assert(completions == before);
    second.getattr.r_attr.va_set_mask = 0;
    get_attrs(&worker, &fs, &second, mount_id, 2);
    cairn_thread_commit(NULL, &worker);
    assert(completions == before + 2);
    assert(mkdir_req.mkdir_at.r_attr.va_acl->num_aces == 128);
    assert(memcmp(mkdir_req.mkdir_at.r_attr.va_acl, acl, acl_size) == 0);
    chimera_vfs_request_free_memory(&mkdir_req);
    chimera_vfs_request_free_memory(&second);
    free(acl);
    cairn_destroy(shared);
    assert(chimera_test_remove_tree(path) == 0);
    return 0;
} /* main */
