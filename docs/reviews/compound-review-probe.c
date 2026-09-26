// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include "vfs/vfs_compound.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
static int releases;
static void review_release(struct chimera_vfs_thread *t, struct chimera_vfs_open_handle *h) { releases++; }
#define chimera_vfs_release review_release
#include "src/vfs/vfs_compound.c"
static void complete(struct chimera_vfs_compound *cp, void *arg) {}
int main(void) {
    struct chimera_vfs_thread thread = {0};
    struct chimera_vfs_cred cred = {0};
    struct chimera_vfs_open_handle handle = {0};
    handle.fh_len = 1;
    handle.fh[0] = 1;
    struct chimera_vfs_compound *cp = chimera_vfs_compound_alloc(&thread, &cred);
    chimera_vfs_compound_add_puthandle(cp, &handle, CHIMERA_VFS_OPEN_INFERRED);
    chimera_vfs_compound_add_gethandle(cp);
    chimera_vfs_compound_submit(cp, complete, NULL);
    chimera_vfs_compound_free(cp);
    printf("borrowed PUTHANDLE/GETHANDLE/free: release calls=%d (expected 0)\n", releases);
    releases = 0;
    cp = chimera_vfs_compound_alloc(&thread, &cred);
    chimera_vfs_compound_add_puthandle(cp, &handle, CHIMERA_VFS_OPEN_INFERRED);
    chimera_vfs_compound_add_gethandle(cp);
    chimera_vfs_compound_add_close(cp);
    chimera_vfs_compound_submit(cp, complete, NULL);
    chimera_vfs_compound_free(cp);
    printf("PUTHANDLE/GETHANDLE/CLOSE/free: release calls=%d (expected 1)\n", releases);
    releases = 0;
    cp = chimera_vfs_compound_alloc(&thread, &cred);
    chimera_vfs_compound_add_puthandle(cp, &handle, CHIMERA_VFS_OPEN_INFERRED);
    chimera_vfs_compound_add_gethandle(cp);
    chimera_vfs_compound_add_gethandle(cp);
    chimera_vfs_compound_submit(cp, complete, NULL);
    chimera_vfs_compound_free(cp);
    printf("PUTHANDLE/GETHANDLE/GETHANDLE/free: release calls=%d (expected 0)\n", releases);
    printf("sizeof compound=%zu, retained 64/thread=%zu bytes\n", sizeof(*cp), 64*sizeof(*cp));
    cp = chimera_vfs_compound_alloc(&thread, &cred);
    chimera_vfs_compound_add_write_same(cp, &handle, 0, 4096, 1048576, "x", 1, 0, 0, 0, 0);
    cp->callback = complete;
    chimera_vfs_compound_write_same_callback(CHIMERA_VFS_OK, 1ULL << 32, 0, NULL, NULL, cp);
    printf("WRITE_SAME completion 4294967296 bytes => result %u (expected 4294967296)\n", cp->ops[0].written);
    chimera_vfs_compound_free(cp);
    struct chimera_acl *acl = calloc(1, chimera_acl_size(2));
    acl->num_aces = 2;
    acl->aces[0].type = CHIMERA_ACE_DENIED;
    acl->aces[0].access_mask = CHIMERA_ACE_READ_DATA;
    acl->aces[0].who.type = CHIMERA_PRINCIPAL_USER;
    acl->aces[0].who.id = 1234;
    acl->aces[1].type = CHIMERA_ACE_ALLOWED;
    acl->aces[1].access_mask = CHIMERA_ACE_READ_DATA;
    acl->aces[1].who.type = CHIMERA_PRINCIPAL_SPECIAL;
    acl->aces[1].who.special = CHIMERA_WHO_EVERYONE;
    struct chimera_vfs_attrs attr = {0};
    attr.va_set_mask = CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
    attr.va_acl = acl;
    attr.va_mode = S_IFREG | 0644;
    cred.flavor = CHIMERA_VFS_AUTH_UNIX;
    cred.uid = 1234;
    cred.gid = 1234;
    struct chimera_vfs_compound_op stored = {0};
    chimera_vfs_compound_store_attr(&stored, &attr);
    printf("ACCESS READ for explicitly denied UID: original=%u, compound stored attrs=%u\n",
        chimera_vfs_access_check(&attr, &cred, CHIMERA_ACE_READ_DATA),
        chimera_vfs_access_check(&stored.attr, &cred, CHIMERA_ACE_READ_DATA));
    free(acl);
    chimera_vfs_compound_thread_destroy(&thread);
}
