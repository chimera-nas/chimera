// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/xattr.h>
#include "dfs_mock.h"

/* A local test double, not an assertion about the opaque installed DFS ABI. */


unsigned int             allocations, releases, lookups, stats, creates, removes, moves;
static mode_t            created_mode[100];
static uint64_t          renamed_oid[100];
int                      stat_error, release_error, allocation_error, repair_error;
unsigned int             connects, disconnects, inits, finis, syncs, writes, reads, setattrs;
unsigned int             directory_entries;
int                      connect_error, sync_error, io_error, setattr_flags, connect_flags;
struct stat              setattr_value;
const char              *missing_name;
daos_size_t              read_length, file_size, submitted_length;
int                      access_denied;
struct mock_xattr {
    int         used;
    uint64_t    lo;
    char        name[DFS_MAX_XATTR_NAME + 1];
    char        value[64];
    daos_size_t len;
};
static struct mock_xattr xattrs[8];

static struct mock_xattr *
xattr_find(
    uint64_t    lo,
    const char *name)
{
    for (int i = 0; i < 8; i++) {
        if (xattrs[i].used && xattrs[i].lo == lo && !strcmp(xattrs[i].name, name)) {
            return &xattrs[i];
        }
    }
    return NULL;
} /* xattr_find */


int
dfs_lookup_rel(
    dfs_t       *dfs,
    dfs_obj_t   *parent,
    const char  *name,
    int          flags,
    dfs_obj_t  **obj,
    mode_t      *mode,
    struct stat *st)
{
    (void) dfs;
    (void) parent;
    (void) flags;
    lookups++;
    if (missing_name && !strcmp(name, missing_name)) {
        return ENOENT;
    }
    if (allocation_error) {
        return allocation_error;
    }
    *obj = calloc(1, sizeof(**obj));
    CHECK(*obj);
    (*obj)->oid.hi = 17;
    (*obj)->oid.lo = (uint64_t) strtoull(name, NULL, 10);
    CHECK((*obj)->oid.lo < 100);
    if (renamed_oid[(*obj)->oid.lo]) {
        (*obj)->oid.lo = renamed_oid[(*obj)->oid.lo];
    }
    (*obj)->mode = created_mode[(*obj)->oid.lo] ? created_mode[(*obj)->oid.lo] :
        (*obj)->oid.lo == 0 ? S_IFDIR : (*obj)->oid.lo == 3 ? S_IFLNK : S_IFREG;
    if (mode) {
        *mode = (*obj)->mode;
    }
    if (st) {
        memset(st, 0, sizeof(*st));
        st->st_mode = (*obj)->mode;
    }
    allocations++;
    return 0;
} /* dfs_lookup_rel */

int
dfs_lookup(
    dfs_t       *dfs,
    const char  *path,
    int          flags,
    dfs_obj_t  **obj,
    mode_t      *mode,
    struct stat *st)
{
    CHECK(!strcmp(path, "/"));
    return dfs_lookup_rel(dfs, NULL, "0", flags, obj, mode, st);
} /* dfs_lookup */

int
dfs_open(
    dfs_t           *dfs,
    dfs_obj_t       *parent,
    const char      *name,
    mode_t           mode,
    int              flags,
    daos_oclass_id_t oclass,
    daos_size_t      chunk,
    const char      *value,
    dfs_obj_t      **obj)
{
    (void) oclass;
    (void) chunk;
    (void) value;
    creates++;
    if (access_denied) {
        return access_denied;
    }
    missing_name = NULL;
    int rc = dfs_lookup_rel(dfs, parent, name, flags, obj, NULL, NULL);
    if (!rc) {
        (*obj)->mode                 = mode;
        created_mode[(*obj)->oid.lo] = mode;
    }
    return rc;
} /* dfs_open */

int
dfs_dup(
    dfs_t      *dfs,
    dfs_obj_t  *obj,
    int         flags,
    dfs_obj_t **copy)
{
    (void) dfs;
    (void) flags;
    if (allocation_error) {
        return allocation_error;
    }
    *copy = malloc(sizeof(**copy));
    CHECK(*copy);
    **copy = *obj;
    allocations++;
    return 0;
} /* dfs_dup */

int
dfs_release(dfs_obj_t *obj)
{
    releases++;
    if (release_error) {
        return release_error;
    }
    free(obj);
    return 0;
} /* dfs_release */

int
dfs_obj2id(
    dfs_obj_t     *obj,
    daos_obj_id_t *oid)
{
    *oid = obj->oid;
    return 0;
} /* dfs_obj2id */

int
dfs_ostat(
    dfs_t       *dfs,
    dfs_obj_t   *obj,
    struct stat *st)
{
    (void) dfs;
    stats++;
    memset(st, 0, sizeof(*st));
    st->st_mode = obj->mode;
    return stat_error;
} /* dfs_ostat */

int
dfs_update_parent(
    dfs_obj_t  *obj,
    dfs_obj_t  *src,
    const char *name)
{
    (void) src;
    (void) name;
    obj->repairs++;
    return repair_error;
} /* dfs_update_parent */


int
dfs_mkdir(
    dfs_t           *dfs,
    dfs_obj_t       *parent,
    const char      *name,
    mode_t           mode,
    daos_oclass_id_t oclass)
{
    (void) dfs;
    (void) parent;
    (void) oclass;
    unsigned int index = (unsigned int) strtoul(name, NULL, 10);
    CHECK(index < 100);
    created_mode[index] = S_IFDIR | mode;
    creates++;
    return 0;
} /* dfs_mkdir */

int
dfs_remove(
    dfs_t         *dfs,
    dfs_obj_t     *parent,
    const char    *name,
    bool           force,
    daos_obj_id_t *oid)
{
    (void) dfs;
    (void) parent;
    CHECK(!force);
    CHECK(oid);
    if (oid) {
        oid->hi = 17;
        oid->lo = strtoull(name, NULL, 10);
        if (renamed_oid[oid->lo]) {
            oid->lo = renamed_oid[oid->lo];
        }
    }
    removes++;
    return 0;
} /* dfs_remove */

int
dfs_move(
    dfs_t         *dfs,
    dfs_obj_t     *parent,
    const char    *name,
    dfs_obj_t     *dest,
    const char    *new_name,
    daos_obj_id_t *oid)
{
    (void) dfs;
    (void) parent;
    (void) dest;
    unsigned int index = (unsigned int) strtoul(new_name, NULL, 10);
    CHECK(index < 100);
    oid->hi            = 17;
    oid->lo            = index;
    renamed_oid[index] = strtoull(name, NULL, 10);
    moves++;
    return 0;
} /* dfs_move */

int
dfs_osetattr(
    dfs_t       *dfs,
    dfs_obj_t   *obj,
    struct stat *st,
    int          flags)
{
    (void) dfs;
    setattrs++;
    setattr_flags = flags;
    setattr_value = *st;
    if (flags & DFS_SET_ATTR_MODE) {
        obj->mode = st->st_mode;
    }
    return 0;
} /* dfs_osetattr */

int
dfs_readdir(
    dfs_t         *dfs,
    dfs_obj_t     *obj,
    daos_anchor_t *anchor,
    uint32_t      *nr,
    struct dirent *entries)
{
    (void) dfs;
    (void) obj;
    unsigned int index;
    memcpy(&index, anchor->da_buf, sizeof(index));
    if (index < directory_entries) {
        snprintf(entries->d_name, sizeof(entries->d_name), "%u", 30 + index++);
        memcpy(anchor->da_buf, &index, sizeof(index));
        *nr = 1;
    } else {
        *nr = 0;
    }
    if (index == directory_entries) {
        anchor->da_type = DAOS_ANCHOR_TYPE_EOF;
    }
    return 0;
} /* dfs_readdir */

int
dfs_listxattr(
    dfs_t       *dfs,
    dfs_obj_t   *obj,
    char        *list,
    daos_size_t *size)
{
    daos_size_t need = 0;
    char       *out  = list;

    (void) dfs;
    if (!obj || !size) {
        return EINVAL;
    }
    for (unsigned int i = 0; i < 8; i++) {
        if (xattrs[i].used && xattrs[i].lo == obj->oid.lo) {
            need += strlen(xattrs[i].name) + 1;
        }
    }
    if (!list) {
        *size = need;
        return 0;
    }
    if (*size < need) {
        *size = need;
        return ERANGE;
    }
    for (unsigned int i = 0; i < 8; i++) {
        size_t n;
        if (!xattrs[i].used || xattrs[i].lo != obj->oid.lo) {
            continue;
        }
        n = strlen(xattrs[i].name) + 1;
        memcpy(out, xattrs[i].name, n);
        out += n;
    }
    *size = need;
    return 0;
} /* dfs_listxattr */

int
dfs_getxattr(
    dfs_t       *dfs,
    dfs_obj_t   *obj,
    const char  *name,
    void        *value,
    daos_size_t *size)
{
    struct mock_xattr *slot;

    (void) dfs;
    if (!obj || !name || !size) {
        return EINVAL;
    }
    slot = xattr_find(obj->oid.lo, name);
    if (!slot) {
        return ENODATA;
    }
    if (!value || !*size) {
        *size = slot->len;
        return 0;
    }
    if (*size < slot->len) {
        *size = slot->len;
        return ERANGE;
    }
    if (slot->len) {
        memcpy(value, slot->value, slot->len);
    }
    *size = slot->len;
    return 0;
} /* dfs_getxattr */

int
dfs_connect(
    const char *pool,
    const char *system,
    const char *container,
    int         flags,
    dfs_attr_t *attr,
    dfs_t     **dfs)
{
    (void) pool;
    (void) system;
    (void) container;
    connects++;
    connect_flags = flags;
    CHECK(!(flags & O_CREAT));
    CHECK(!attr);
    if (connect_error) {
        return connect_error;
    }
    *dfs = (dfs_t *) (uintptr_t) 1;
    return 0;
} /* dfs_connect */

int
dfs_query(
    dfs_t      *dfs,
    dfs_attr_t *attr)
{
    (void) dfs;
    attr->da_mode = DFS_BALANCED;
    return 0;
} /* dfs_query */

int
dfs_disconnect(dfs_t *dfs)
{
    CHECK(dfs == (dfs_t *) (uintptr_t) 1);
    disconnects++;
    return 0;
} /* dfs_disconnect */

/* Lifecycle and I/O doubles record calls; they make no durability claim. */
int
dfs_init(void)
{
    inits++;
    return 0;
} /* dfs_init */

int
dfs_fini(void)
{
    finis++;
    return 0;
} /* dfs_fini */

int
dfs_sync(dfs_t *dfs)
{
    (void) dfs;
    syncs++;
    return sync_error;
} /* dfs_sync */

int
dfs_read(
    dfs_t        *dfs,
    dfs_obj_t    *obj,
    d_sg_list_t  *sgl,
    daos_off_t    off,
    daos_size_t  *size,
    daos_event_t *event)
{
    (void) dfs;
    (void) obj;
    (void) off;
    CHECK(!event);
    reads++;
    submitted_length = 0;
    for (unsigned int i = 0; i < sgl->sg_nr; i++) {
        submitted_length += sgl->sg_iovs[i].iov_len;
    }
    *size = read_length;
    return io_error;
} /* dfs_read */

int
dfs_write(
    dfs_t        *dfs,
    dfs_obj_t    *obj,
    d_sg_list_t  *sgl,
    daos_off_t    off,
    daos_event_t *event)
{
    (void) dfs;
    (void) obj;
    (void) off;
    CHECK(!event);
    writes++;
    submitted_length = 0;
    for (unsigned int i = 0; i < sgl->sg_nr; i++) {
        submitted_length += sgl->sg_iovs[i].iov_len;
    }
    return io_error;
} /* dfs_write */

int
dfs_get_size(
    dfs_t       *dfs,
    dfs_obj_t   *obj,
    daos_size_t *size)
{
    (void) dfs;
    (void) obj;
    *size = file_size;
    return 0;
} /* dfs_get_size */

int
dfs_get_symlink_value(
    dfs_obj_t   *obj,
    char        *buffer,
    daos_size_t *size)
{
    CHECK(S_ISLNK(obj->mode));
    static const char target[] = "missing/target";
    if (buffer) {
        CHECK(*size >= sizeof(target));
        memcpy(buffer, target, sizeof(target));
    }
    *size = sizeof(target);
    return 0;
} /* dfs_get_symlink_value */

int
dfs_punch(
    dfs_t      *dfs,
    dfs_obj_t  *obj,
    daos_off_t  off,
    daos_size_t length)
{
    (void) dfs;
    (void) obj;
    (void) off;
    (void) length;
    return ENOTSUP;
} /* dfs_punch */

int
dfs_setxattr(
    dfs_t      *dfs,
    dfs_obj_t  *obj,
    const char *name,
    const void *value,
    daos_size_t size,
    int         flags)
{
    struct mock_xattr *slot;
    int                free_slot = -1;

    (void) dfs;
    if (!obj || !name || (size && !value) || size > sizeof(xattrs[0].value)) {
        return EINVAL;
    }
    for (int i = 0; i < 8; i++) {
        if (!xattrs[i].used) {
            if (free_slot < 0) {
                free_slot = i;
            }
            continue;
        }
        if (xattrs[i].lo == obj->oid.lo && !strcmp(xattrs[i].name, name)) {
            slot = &xattrs[i];
            if (flags == XATTR_CREATE) {
                return EEXIST;
            }
            goto store;
        }
    }
    if (flags == XATTR_REPLACE) {
        return ENOENT;
    }
    if (free_slot < 0) {
        return ENOSPC;
    }
    slot = &xattrs[free_slot];
    memset(slot, 0, sizeof(*slot));
    slot->used = 1;
    slot->lo   = obj->oid.lo;
    memcpy(slot->name, name, strlen(name) + 1);
 store:
    slot->len = size;
    if (size) {
        memcpy(slot->value, value, size);
    }
    return 0;
} /* dfs_setxattr */

int
dfs_removexattr(
    dfs_t      *dfs,
    dfs_obj_t  *obj,
    const char *name)
{
    struct mock_xattr *slot;

    (void) dfs;
    if (!obj || !name) {
        return EINVAL;
    }
    slot = xattr_find(obj->oid.lo, name);
    if (!slot) {
        return ENOENT;
    }
    slot->used = 0;
    return 0;
} /* dfs_removexattr */

int
daos_oclass_id2name(
    daos_oclass_id_t id,
    char            *name)
{
    (void) id;
    (void) name;
    return EINVAL;
} /* daos_oclass_id2name */
