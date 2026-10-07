// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#include <stdio.h>
#include <stdlib.h>
#include <sched.h>
#include <stdatomic.h>
#include "dfs_mock.h"

/* Exercise the actual dispatch, lifecycle and helper code with DFS doubles. */
#include "../vfs_daos_ops.c"
#include "../vfs_daos.c"

static void
setup_mount(
    struct vfs_daos_state    *s,
    struct chimera_vfs_attrs *root)
{
    struct chimera_vfs_request r = { 0 };

    memset(s, 0, sizeof(*s));
    s->initialized                 = true;
    s->config.registry_max_entries = 64;
    s->config.open_max_handles     = 128;
    s->config.batch_entries        = 64;
    s->generation                  = 19;
    s->next_cookie                 = 3;
    s->config.uuid[0]              = 1;
    CHECK(mount_fs(s, &r) == 0);
    vfs_daos_unreserve(s);
    *root = r.mount.r_attr;
} /* setup_mount */

static void
finish_mount(struct vfs_daos_state *s)
{
    vfs_daos_unreserve(s);
    CHECK(!s->objects);
    CHECK(vfs_daos_disconnect(s) == 0);
    CHECK(!s->registry && !s->owned && !s->cursors);
} /* finish_mount */

static void
test_names_and_inferred_handles(void)
{
    char       name[CHIMERA_VFS_NAME_MAX], input[257];

    CHECK(name_copy("dir/file", 3, name) == 0 && !strcmp(name, "dir"));
    const char exact[] = { '3', '0' };
    CHECK(name_copy(exact, sizeof(exact), name) == 0 && !strcmp(name, "30"));
    CHECK(name_copy("a\0b", 3, name) == EINVAL);
    CHECK(name_copy("a/b", 3, name) == EINVAL);
    CHECK(name_copy(NULL, 0, name) == EINVAL);
    memset(input, 'a', sizeof(input));
    CHECK(name_copy(input, 255, name) == 0 && name[255] == 0);
    CHECK(name_copy(input, 256, name) == ENAMETOOLONG);
    CHECK(mutation_name(".", 1, name) == EINVAL);
    CHECK(mutation_name("..", 2, name) == EINVAL);

    struct vfs_daos_state          s;
    struct chimera_vfs_attrs       root;
    struct chimera_vfs_open_handle inferred = { 0 };
    struct chimera_vfs_request     r        = { 0 };
    setup_mount(&s, &root);
    memcpy(inferred.fh, root.va_fh, 33);
    inferred.fh_len                = 33;
    r.mount_private                = &s;
    r.opcode                       = CHIMERA_VFS_OP_LOOKUP_AT;
    r.lookup_at.handle             = &inferred;
    r.lookup_at.component          = exact;
    r.lookup_at.component_len      = sizeof(exact);
    r.lookup_at.r_attr.va_req_mask = CHIMERA_VFS_ATTR_MASK_STAT;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_OK);
    CHECK(r.lookup_at.r_attr.va_fh_len == 33);
    CHECK(r.lookup_at.r_attr.va_ino == (17 ^ 30) && r.lookup_at.r_attr.va_nlink == 1);
    unsigned int before = lookups, before_stats = stats;
    r.lookup_at.component     = input;
    r.lookup_at.component_len = 256;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_ENAMETOOLONG);
    CHECK(lookups == before && stats == before_stats);
    finish_mount(&s);
} /* test_names_and_inferred_handles */

static void
test_setattr_masks(void)
{
    struct vfs_daos_state    s;
    struct chimera_vfs_attrs root, a = { 0 };
    dfs_obj_t               *obj;

    setup_mount(&s, &root);
    CHECK(lookup(&s, s.root, "1", &obj) == 0);
    uint64_t                 ignored = CHIMERA_VFS_ATTR_DOS_ATTRIBUTES | CHIMERA_VFS_ATTR_ACL |
        CHIMERA_VFS_ATTR_ALLOC_SIZE | CHIMERA_VFS_ATTR_OWNER_SID |
        CHIMERA_VFS_ATTR_GROUP_SID;
    a.va_set_mask = ignored;
    unsigned int             before = setattrs, before_stats = stats;
    CHECK(set_attrs(&s, obj, &a, NULL, false) == 0);
    CHECK(setattrs == before && stats == before_stats);
    a.va_set_mask     |= SETTABLE;
    a.va_mode          = 06750;
    a.va_uid           = 123;
    a.va_gid           = 456;
    a.va_size          = 0;
    a.va_atime.tv_nsec = CHIMERA_VFS_TIME_NOW;
    a.va_mtime.tv_nsec = CHIMERA_VFS_TIME_OMIT;
    CHECK(set_attrs(&s, obj, &a, NULL, false) == 0);
    CHECK(setattrs == before + 1);
    CHECK(setattr_flags == (DFS_SET_ATTR_MODE | DFS_SET_ATTR_UID | DFS_SET_ATTR_GID |
                            DFS_SET_ATTR_SIZE | DFS_SET_ATTR_ATIME));
    CHECK(setattr_value.st_uid == 123 && setattr_value.st_gid == 456);
    CHECK(setattr_value.st_atim.tv_sec > 0 && (setattr_value.st_mode & 07777) == 06750);
    a.va_set_mask      = CHIMERA_VFS_ATTR_CTIME | CHIMERA_VFS_ATTR_BTIME | ignored;
    a.va_ctime.tv_nsec = a.va_btime.tv_nsec = CHIMERA_VFS_TIME_OMIT;
    CHECK(set_attrs(&s, obj, &a, NULL, false) == 0);
    a.va_ctime.tv_nsec = 0;
    CHECK(set_attrs(&s, obj, &a, NULL, false) == ENOTSUP);
    a.va_ctime.tv_nsec = CHIMERA_VFS_TIME_OMIT;
    a.va_btime.tv_nsec = CHIMERA_VFS_TIME_NOW;
    CHECK(set_attrs(&s, obj, &a, NULL, false) == ENOTSUP);
    CHECK(setattrs == before + 1);
    a.va_req_mask = UINT64_MAX;
    CHECK(get_attrs(&s, obj, &a) == 0);
    CHECK(!(a.va_set_mask & (ignored | CHIMERA_VFS_ATTR_BTIME | CHIMERA_VFS_ATTR_CHANGE |
                             CHIMERA_VFS_ATTR_EA_SIZE)));
    CHECK(vfs_daos_release(&s, obj) == 0);
    finish_mount(&s);
} /* test_setattr_masks */

struct collected {
    unsigned int count;
    unsigned int stop;
    uint64_t     cookies[8];
    char         names[8][16];
};

static int
collect(
    uint64_t                        ino,
    uint64_t                        cookie,
    const char                     *name,
    int                             length,
    const struct chimera_vfs_attrs *attrs,
    void                           *private_data)
{
    struct collected *c = private_data;

    (void) ino;
    (void) attrs;
    CHECK(c->count < 8 && length < 16);
    c->cookies[c->count] = cookie;
    memcpy(c->names[c->count], name, length);
    c->names[c->count][length] = 0;
    c->count++;
    return c->stop && c->count >= c->stop;
} /* collect */

static void
test_setgid_and_readdir_access(void)
{
    struct vfs_daos_state      s;
    struct chimera_vfs_attrs   root, a = { 0 };
    struct chimera_vfs_request r     = { 0 };
    struct chimera_vfs_cred    owner = {
        .flavor = CHIMERA_VFS_AUTH_UNIX, .uid = 1000, .gid = 1000
    };
    struct collected           c = { 0 };
    dfs_obj_t                 *obj;

    setup_mount(&s, &root);
    directory_entries = 0;
    CHECK(lookup(&s, s.root, "1", &obj) == 0);
    a.va_set_mask = CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID | CHIMERA_VFS_ATTR_MODE;
    a.va_uid      = 1000;
    a.va_gid      = 50;
    a.va_mode     = 0644;
    CHECK(set_attrs(&s, obj, &a, NULL, false) == 0);
    a.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    a.va_mode     = 02750;
    CHECK(set_attrs(&s, obj, &a, &owner, false) == 0);
    CHECK((setattr_value.st_mode & 07777) == 0750);
    owner.gid = 50;
    a.va_mode = 02750;
    CHECK(set_attrs(&s, obj, &a, &owner, false) == 0);
    CHECK((setattr_value.st_mode & 07777) == 02750);
    CHECK(vfs_daos_release(&s, obj) == 0);

    r.cred               = &owner;
    r.readdir.callback   = collect;
    r.proto_private_data = &c;
    CHECK(read_directory(&s, &r, s.root) == EACCES);
    a.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    a.va_mode     = 0755;
    CHECK(set_attrs(&s, s.root, &a, NULL, false) == 0);
    CHECK(read_directory(&s, &r, s.root) == 0);
    CHECK(c.count == 0 && r.readdir.r_eof);
    finish_mount(&s);
} /* test_setgid_and_readdir_access */

static void
test_readdir_resume(void)
{
    struct vfs_daos_state      s;
    struct chimera_vfs_attrs   root;
    struct chimera_vfs_request r = { 0 };
    struct collected           c = { 0 };

    setup_mount(&s, &root);
    directory_entries    = 4;
    r.readdir.callback   = collect;
    r.proto_private_data = &c;
    CHECK(read_directory(&s, &r, s.root) == 0);
    CHECK(c.count == 4 && s.cursor_count == 1 && r.readdir.r_eof);
    uint64_t                   first = c.cookies[0], verifier = r.readdir.r_verifier;
    CHECK(!strcmp(c.names[0], "30") && !strcmp(c.names[3], "33"));
    memset(&c, 0, sizeof(c));
    c.stop             = 2;
    r.readdir.cookie   = first;
    r.readdir.verifier = 0; /* SMB omits the verifier. */
    CHECK(read_directory(&s, &r, s.root) == 0);
    CHECK(c.count == 2 && !strcmp(c.names[0], "31") && !strcmp(c.names[1], "32"));
    CHECK(!r.readdir.r_eof && s.cursor_count == 1);
    uint64_t stopped_cookie = c.cookies[1];
    memset(&c, 0, sizeof(c));
    r.readdir.cookie = stopped_cookie;
    CHECK(read_directory(&s, &r, s.root) == 0);
    CHECK(c.count == 1 && !strcmp(c.names[0], "33"));
    r.readdir.verifier = verifier + 1;
    CHECK(read_directory(&s, &r, s.root) == BAD_COOKIE);
    r.readdir.cookie   = UINT64_MAX;
    r.readdir.verifier = 0;
    CHECK(read_directory(&s, &r, s.root) == BAD_COOKIE);
    daos_obj_id_t              oid;
    CHECK(dfs_obj2id(s.root, &oid) == 0);
    for (unsigned int i = 1; i < 1024; i++) {
        CHECK(cursor_start(&s, oid, 0));
    }
    struct vfs_daos_checkpoint position;
    CHECK(cursor_find(&s, first, oid, 0, &position));
    CHECK(cursor_start(&s, oid, 0));
    CHECK(s.cursor_count == 1024 && s.cursor_evictions == 1);
    CHECK(cursor_find(&s, first, oid, 0, &position));
    for (unsigned int i = 0; i < 1024; i++) {
        CHECK(cursor_start(&s, oid, 0));
    }
    CHECK(!cursor_find(&s, first, oid, 0, &position));
    directory_entries = 0;
    memset(&c, 0, sizeof(c));
    r.readdir.cookie = 0;
    r.readdir.flags  = CHIMERA_VFS_READDIR_EMIT_DOT;
    CHECK(read_directory(&s, &r, s.root) == 0);
    CHECK(c.count == 2 && !strcmp(c.names[0], ".") && !strcmp(c.names[1], ".."));
    finish_mount(&s);
} /* test_readdir_resume */

static void
test_create_and_parent(void)
{
    struct vfs_daos_state      s;
    struct chimera_vfs_attrs   root, dir_attr = { 0 }, source = { 0 }, victim = { 0 };
    struct chimera_vfs_request r = { 0 };
    dfs_obj_t                 *dir, *obj, *parent;

    setup_mount(&s, &root);
    unsigned int               before = creates;
    s.config.registry_max_entries = 1;
    CHECK(create_entry(&s, &r, s.root, "10", NULL, S_IFREG, NULL, &obj) == EMFILE);
    CHECK(!obj && creates == before);
    vfs_daos_unreserve(&s);
    s.config.registry_max_entries = 64;
    s.root->mode                 |= S_ISGID;
    CHECK(create_entry(&s, &r, s.root, "11", NULL, S_IFDIR, NULL, &dir) == 0);
    CHECK(S_ISDIR(dir->mode) && (dir->mode & S_ISGID));
    CHECK(encode_object(&s, dir, &dir_attr) == 0);
    vfs_daos_unreserve(&s);
    CHECK(create_entry(&s, &r, s.root, "12", "missing/target", S_IFLNK, NULL, &obj) == 0);
    CHECK(encode_object(&s, obj, &source) == 0 && source.va_fh[32] == 3);
    CHECK(vfs_daos_release(&s, obj) == 0);
    vfs_daos_unreserve(&s);
    CHECK(lookup(&s, s.root, "20", &obj) == 0);
    CHECK(encode_object(&s, obj, &source) == 0);
    CHECK(vfs_daos_release(&s, obj) == 0);
    CHECK(lookup(&s, dir, "21", &obj) == 0);
    CHECK(encode_object(&s, obj, &victim) == 0);
    CHECK(vfs_daos_release(&s, obj) == 0);
    memcpy(r.fh, root.va_fh, 33);
    r.fh_len                = 33;
    r.rename_at.name        = "20";
    r.rename_at.namelen     = 2;
    r.rename_at.new_name    = "21";
    r.rename_at.new_namelen = 2;
    r.rename_at.new_fh      = dir_attr.va_fh;
    r.rename_at.new_fhlen   = 33;
    CHECK(rename_at(&s, &r) == 0);
    vfs_daos_unreserve(&s);
    CHECK(vfs_daos_resolve(&s, victim.va_fh, 33, &obj) == ESTALE);
    CHECK(lookup(&s, dir, "21", &obj) == 0);
    CHECK(vfs_daos_parent(&s, obj, &parent) == 0 && parent == dir);
    CHECK(vfs_daos_release(&s, parent) == 0);
    CHECK(vfs_daos_release(&s, obj) == 0);
    CHECK(lookup(&s, s.root, "21", &obj) == EIO && !obj);
    memset(&r, 0, sizeof(r));
    r.remove_at.name    = "21";
    r.remove_at.namelen = 2;
    uint8_t            unmatched[33];
    uint32_t           length;
    struct vfs_daos_fh absent = { 99, 99, 1 };
    CHECK(vfs_daos_fh_encode(s.mount_id, &absent, unmatched, &length) == 0);
    r.remove_at.match_child_fh = 1;
    r.remove_at.child_fh       = unmatched;
    r.remove_at.child_fh_len   = length;
    unsigned int       before_remove = removes;
    CHECK(remove_at(&s, &r, dir) == 0 && r.remove_at.r_unmatched);
    CHECK(removes == before_remove);
    r.remove_at.match_child_fh = 0;
    CHECK(remove_at(&s, &r, dir) == 0);
    CHECK(vfs_daos_resolve(&s, source.va_fh, 33, &obj) == ESTALE);
    CHECK(lookup(&s, dir, "21", &obj) == EIO && !obj);
    CHECK(vfs_daos_release(&s, dir) == 0);
    finish_mount(&s);
} /* test_create_and_parent */

static void
test_open_io_and_unsupported(void)
{
    struct vfs_daos_state          s;
    struct chimera_vfs_attrs       root;
    struct chimera_vfs_request     r = { 0 };
    struct chimera_vfs_open_handle h = { 0 };
    dfs_obj_t                     *obj;

    setup_mount(&s, &root);
    CHECK(lookup(&s, s.root, "3", &obj) == 0);
    CHECK(open_type(&s, obj, CHIMERA_VFS_OPEN_STOP_SYMLINK | CHIMERA_VFS_OPEN_PATH) == ELOOP);
    CHECK(open_type(&s, obj, CHIMERA_VFS_OPEN_NOFOLLOW) == ELOOP);
    CHECK(open_type(&s, obj, CHIMERA_VFS_OPEN_NOFOLLOW | CHIMERA_VFS_OPEN_PATH) == 0);
    CHECK(open_type(&s, obj, CHIMERA_VFS_OPEN_REGULAR_ONLY) == CHIMERA_VFS_ESYMLINK);
    CHECK(open_type(&s, obj, CHIMERA_VFS_OPEN_CREATE_REGULAR) == EEXIST);
    CHECK(vfs_daos_release(&s, obj) == 0);
    CHECK(lookup(&s, s.root, "1", &obj) == 0);
    CHECK(bind_open(&s, obj, 0, NULL, false, &h.vfs_private) == 0);
    CHECK(vfs_daos_release(&s, obj) == 0);
    char              data[16] = { 0 };
    struct evpl_iovec iov      = { .data = data, .length = sizeof(data) };
    r.mount_private = &s;
    r.opcode        = CHIMERA_VFS_OP_WRITE;
    r.write.handle  = &h;
    r.write.iov     = &iov;
    r.write.niov    = 1;
    r.write.length  = 7;
    r.write.sync    = CHIMERA_VFS_WRITE_FILESYNC;
    unsigned int      before = syncs;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_OK);
    CHECK(r.write.r_length == 7 && submitted_length == 7 && !r.write.r_sync && syncs == before);
    memset(&r, 0, sizeof(r));
    r.mount_private         = &s;
    r.opcode                = CHIMERA_VFS_OP_READ;
    r.read.handle           = &h;
    r.read.iov              = &iov;
    r.read.buffers_provided = 1;
    r.read.aligned_prefix   = 2;
    r.read.length           = 7;
    read_length             = 3;
    file_size               = 3;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_OK);
    CHECK(r.read.r_length == 3 && submitted_length == 7 && r.read.r_eof);
    memset(&r, 0, sizeof(r));
    r.mount_private = &s;
    r.opcode        = CHIMERA_VFS_OP_COMMIT;
    r.commit.handle = &h;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_OK && syncs == before + 1);
    sync_error = EIO;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_EIO && syncs == before + 2);
    sync_error = 0;
    CHECK(close_open(&s, h.vfs_private) == 0);
    unsigned int             unsupported[] = { CHIMERA_VFS_OP_GETPARENT, CHIMERA_VFS_OP_SEEK,
                                               CHIMERA_VFS_OP_LINK_AT,               CHIMERA_VFS_OP_CREATE_UNLINKED,
                                               CHIMERA_VFS_OP_GET_LAYOUT,            CHIMERA_VFS_OP_READ_PLUS,
                                               CHIMERA_VFS_OP_WRITE_SAME,            27,
                                               999 };
    before = lookups;
    for (size_t i = 0; i < sizeof(unsupported) / sizeof(unsupported[0]); i++) {
        r.opcode = unsupported[i];
        CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_ENOTSUP);
    }
    CHECK(lookups == before);
    unsigned int             before_stats = stats;
    r.opcode         = CHIMERA_VFS_OP_ALLOCATE;
    r.allocate.flags = 0;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_ENOTSUP);
    struct chimera_vfs_attrs special = { .va_mode = S_IFCHR };
    r.opcode            = CHIMERA_VFS_OP_MKNOD_AT;
    r.mknod_at.set_attr = &special;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_ENOTSUP);
    CHECK(stats == before_stats && lookups == before);
    CHECK(dfs_errno_to_vfs(ENOMEM) == CHIMERA_VFS_EIO);
    CHECK(dfs_errno_to_vfs(-1) == CHIMERA_VFS_EIO);
    finish_mount(&s);
} /* test_open_io_and_unsupported */

static void
test_xattr(void)
{
    struct vfs_daos_state          s;
    struct chimera_vfs_attrs       root;
    struct chimera_vfs_request     r    = { 0 };
    struct chimera_vfs_open_handle h    = { 0 };
    struct chimera_vfs_cred        cred = { .uid = 1000, .gid = 1000 };
    dfs_obj_t                     *obj;
    char                           value[16];
    char                           list[64];
    char                           long_name[DFS_MAX_XATTR_NAME + 2];

    setup_mount(&s, &root);
    CHECK(lookup(&s, s.root, "1", &obj) == 0);
    CHECK(bind_open(&s, obj, 0, NULL, false, &h.vfs_private) == 0);
    CHECK(vfs_daos_release(&s, obj) == 0);

    r.mount_private                     = &s;
    r.opcode                            = CHIMERA_VFS_OP_SET_XATTR;
    r.set_xattr.handle                  = &h;
    r.set_xattr.name                    = "user.test";
    r.set_xattr.namelen                 = 9;
    r.set_xattr.value                   = "abc";
    r.set_xattr.value_len               = 3;
    r.set_xattr.option                  = CHIMERA_VFS_XATTR_CREATE;
    r.set_xattr.r_pre_attr.va_req_mask  = CHIMERA_VFS_ATTR_MASK_STAT;
    r.set_xattr.r_post_attr.va_req_mask = CHIMERA_VFS_ATTR_MASK_STAT;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_OK);
    CHECK(r.set_xattr.r_pre_attr.va_set_mask && r.set_xattr.r_post_attr.va_set_mask);

    r.set_xattr.option = CHIMERA_VFS_XATTR_CREATE;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_EEXIST);
    r.set_xattr.name    = "user.missing";
    r.set_xattr.namelen = 12;
    r.set_xattr.option  = CHIMERA_VFS_XATTR_REPLACE;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_ENODATA);

    memset(&r, 0, sizeof(r));
    r.mount_private          = &s;
    r.opcode                 = CHIMERA_VFS_OP_GET_XATTR;
    r.get_xattr.handle       = &h;
    r.get_xattr.name         = "user.test";
    r.get_xattr.namelen      = 9;
    r.get_xattr.value        = value;
    r.get_xattr.value_maxlen = 2;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_ERANGE);
    r.get_xattr.value_maxlen = sizeof(value);
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_OK);
    CHECK(r.get_xattr.r_value_len == 3 && !memcmp(value, "abc", 3));

    memset(&r, 0, sizeof(r));
    r.mount_private         = &s;
    r.opcode                = CHIMERA_VFS_OP_LIST_XATTRS;
    r.list_xattrs.handle    = &h;
    r.list_xattrs.buffer    = list;
    r.list_xattrs.max_bytes = 4;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_ERANGE);
    r.list_xattrs.max_bytes = sizeof(list);
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_OK);
    CHECK(r.list_xattrs.r_count == 1 && r.list_xattrs.r_eof &&
          r.list_xattrs.r_len == 10 && !strcmp(list, "user.test"));
    r.list_xattrs.cookie = 1;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_OK);
    CHECK(!r.list_xattrs.r_count && r.list_xattrs.r_eof && !r.list_xattrs.r_len);

    memset(&r, 0, sizeof(r));
    r.mount_private       = &s;
    r.cred                = &cred;
    r.opcode              = CHIMERA_VFS_OP_SET_XATTR;
    r.set_xattr.handle    = &h;
    r.set_xattr.name      = "user.other";
    r.set_xattr.namelen   = 10;
    r.set_xattr.value     = "no";
    r.set_xattr.value_len = 2;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_EACCES);
    r.cred             = NULL;
    s.config.read_only = true;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_EROFS);
    s.config.read_only = false;
    memset(long_name, 'a', sizeof(long_name));
    r.set_xattr.name    = long_name;
    r.set_xattr.namelen = DFS_MAX_XATTR_NAME + 1;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_EINVAL);

    memset(&r, 0, sizeof(r));
    r.mount_private        = &s;
    r.opcode               = CHIMERA_VFS_OP_REMOVE_XATTR;
    r.remove_xattr.handle  = &h;
    r.remove_xattr.name    = "missing";
    r.remove_xattr.namelen = 7;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_ENODATA);
    r.remove_xattr.name    = "user.test";
    r.remove_xattr.namelen = 9;
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_OK);

    memset(&r, 0, sizeof(r));
    r.mount_private          = &s;
    r.opcode                 = CHIMERA_VFS_OP_GET_XATTR;
    r.get_xattr.handle       = &h;
    r.get_xattr.name         = "user.test";
    r.get_xattr.namelen      = 9;
    r.get_xattr.value        = value;
    r.get_xattr.value_maxlen = sizeof(value);
    CHECK(vfs_daos_operation(&s, &r) == CHIMERA_VFS_ENODATA);
    CHECK(close_open(&s, h.vfs_private) == 0);
    finish_mount(&s);
} /* test_xattr */

static void
test_mount_lifecycle(void)
{
    const char                *json = "{\"pool\":\"p\",\"container\":\"c\","
        "\"mount_uuid\":\"6c0f6325-07f3-46fd-97d8-7d6dfdfeac5b\"}";
    unsigned int               before = connects, before_fini = finis;
    struct vfs_daos_state     *s = vfs_daos_init(json, NULL);

    CHECK(s && s->initialized && !s->dfs && connects == before);
    CHECK(vfs_daos_thread_init(NULL, s) == s);
    vfs_daos_thread_destroy(s);
    struct chimera_vfs_request r = { 0 };
    r.mount.path    = "/";
    r.mount.pathlen = 1;
    CHECK(mount_fs(s, &r) == EINVAL && connects == before);
    r.mount.path    = "";
    r.mount.pathlen = 0;
    connect_error   = ENOENT;
    CHECK(mount_fs(s, &r) == ENOENT && !r.mount.r_attr.va_set_mask);
    CHECK(!s->dfs && !s->active);
    connect_error = 0;
    CHECK(mount_fs(s, &r) == 0 && connect_flags == O_RDWR);
    vfs_daos_unreserve(s);
    CHECK(r.mount.r_attr.va_fh_len == 33 && r.mount.r_attr.va_fh[32] == 2);
    CHECK(r.mount.r_attr.va_nlink == 2);
    before = connects;
    CHECK(mount_fs(s, &r) == EBUSY && connects == before);
    uint8_t                  old_fh[33];
    dfs_obj_t               *obj;
    struct chimera_vfs_attrs a = { 0 };
    CHECK(lookup(s, s->root, "1", &obj) == 0);
    CHECK(encode_object(s, obj, &a) == 0);
    memcpy(old_fh, a.va_fh, 33);
    CHECK(vfs_daos_release(s, obj) == 0);
    CHECK(vfs_daos_disconnect(s) == 0);
    s->config.read_only = true;
    CHECK(mount_fs(s, &r) == 0 && connect_flags == O_RDONLY);
    vfs_daos_unreserve(s);
    before = lookups;
    CHECK(vfs_daos_resolve(s, old_fh, 33, &obj) == ESTALE && lookups == before);
    memset(&r, 0, sizeof(r));
    r.opcode        = CHIMERA_VFS_OP_OPEN_FH;
    r.mount_private = s;
    memcpy(r.fh, old_fh, 33);
    r.fh_len = 33;
    CHECK(vfs_daos_operation(s, &r) == CHIMERA_VFS_ESTALE);
    vfs_daos_destroy(s);
    CHECK(finis == before_fini + 1);
} /* test_mount_lifecycle */

struct completion_check {
    struct vfs_daos_state *state;
    atomic_uint            count;
};

static void
completed(struct chimera_vfs_request *r)
{
    struct completion_check *c = r->proto_private_data;

    /* This would deadlock if dispatch called complete while holding the mutex. */
    CHECK(pthread_mutex_lock(&c->state->mutex) == 0);
    CHECK(pthread_mutex_unlock(&c->state->mutex) == 0);
    atomic_fetch_add(&c->count, 1);
} /* completed */

static void *
unmount_thread(void *arg)
{
    struct chimera_vfs_request *r = arg;
    struct completion_check    *c = r->proto_private_data;

    vfs_daos_dispatch(r, c->state);
    return NULL;
} /* unmount_thread */

static void
test_unmount_drain(void)
{
    const char                *json = "{\"pool\":\"p\",\"container\":\"c\","
        "\"mount_uuid\":\"6c0f6325-07f3-46fd-97d8-7d6dfdfeac5b\"}";
    struct vfs_daos_state     *s     = vfs_daos_init(json, NULL);
    struct chimera_vfs_request mount = { .opcode = CHIMERA_VFS_OP_MOUNT };

    CHECK(s && vfs_daos_operation(s, &mount) == CHIMERA_VFS_OK);
    uint64_t                   wrapper;
    CHECK(bind_open(s, s->root, CHIMERA_VFS_OPEN_PATH, NULL, false, &wrapper) == 0);
    struct completion_check    c       = { .state = s };
    struct chimera_vfs_request unmount = { .opcode   = CHIMERA_VFS_OP_UMOUNT,
                                           .complete = completed,            .proto_private_data = &c };
    unmount.umount.mount_private = s;
    pthread_t                  thread;
    CHECK(pthread_create(&thread, NULL, unmount_thread, &unmount) == 0);
    uint64_t                   deadline = monotonic_ns() + 5000000000ULL;
    for (;;) {
        CHECK(pthread_mutex_lock(&s->mutex) == 0);
        bool draining = s->draining;
        CHECK(pthread_mutex_unlock(&s->mutex) == 0);
        if (draining) {
            break;
        }
        CHECK(monotonic_ns() < deadline);
        sched_yield();
    }
    CHECK(atomic_load(&c.count) == 0);
    struct chimera_vfs_request close = { .opcode   = CHIMERA_VFS_OP_CLOSE,
                                         .complete = completed,           .proto_private_data = &c };
    close.close.vfs_private = wrapper;
    vfs_daos_dispatch(&close, s);
    CHECK(pthread_join(thread, NULL) == 0);
    CHECK(close.status == CHIMERA_VFS_OK && unmount.status == CHIMERA_VFS_OK);
    CHECK(atomic_load(&c.count) == 2 && !s->dfs && !s->objects && !s->registry);
    vfs_daos_destroy(s);
} /* test_unmount_drain */

int
main(void)
{
    CHECK(vfs_daos.sdk_version == 3);
    CHECK(vfs_daos.fh_magic == CHIMERA_VFS_FH_MAGIC_DAOS);
    CHECK(vfs_daos.capabilities == (CHIMERA_VFS_CAP_FS | CHIMERA_VFS_CAP_FS_RELATIVE_OP |
                                    CHIMERA_VFS_CAP_BLOCKING | CHIMERA_VFS_CAP_CREATE_GID_ENGINE |
                                    CHIMERA_VFS_CAP_DELEGATES_DAC | CHIMERA_VFS_CAP_XATTR));
    test_names_and_inferred_handles();
    test_setattr_masks();
    test_setgid_and_readdir_access();
    test_readdir_resume();
    test_create_and_parent();
    test_open_io_and_unsupported();
    test_xattr();
    test_mount_lifecycle();
    test_unmount_drain();
    CHECK(allocations == releases);
    puts("SDK 3 lifecycle, names, masks, scan cookies, parent tracking and I/O passed");
    return 0;
} /* main */
