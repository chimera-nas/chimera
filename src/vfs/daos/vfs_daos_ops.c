// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/xattr.h>
#include "vfs_daos.h"

#define SETTABLE       (CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID | \
                        CHIMERA_VFS_ATTR_GID | CHIMERA_VFS_ATTR_SIZE | \
                        CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME)
#define BAD_COOKIE     200
#define SYMLINK_RESULT 120

static bool
oid_equal(
    daos_obj_id_t a,
    daos_obj_id_t b)
{
    return a.hi == b.hi && a.lo == b.lo;
} /* oid_equal */

static void
release_object(
    struct vfs_daos_state *s,
    dfs_obj_t             *obj)
{
    int rc;

    if (obj) {
        rc = vfs_daos_release(s, obj);
        if (rc) {
            s->unavailable = true;
            fprintf(stderr, "vfs_daos: uncertain release, mount unavailable: %d\n", rc);
        }
    }
} /* release_object */

static int
name_copy(
    const char *input,
    size_t      length,
    char        out[CHIMERA_VFS_NAME_MAX])
{
    if (!input || !length) {
        return EINVAL;
    }
    if (length > DFS_MAX_NAME || length >= CHIMERA_VFS_NAME_MAX) {
        return ENAMETOOLONG;
    }
    if (memchr(input, 0, length) || memchr(input, '/', length)) {
        return EINVAL;
    }
    memcpy(out, input, length);
    out[length] = 0;
    return 0;
} /* name_copy */

static int
mutation_name(
    const char *input,
    size_t      length,
    char        out[CHIMERA_VFS_NAME_MAX])
{
    int rc = name_copy(input, length, out);

    if (!rc && (!strcmp(out, ".") || !strcmp(out, ".."))) {
        rc = EINVAL;
    }
    return rc;
} /* mutation_name */

static struct vfs_daos_object *
find_object(
    struct vfs_daos_state *s,
    uint64_t               value)
{
    struct vfs_daos_object *o;

    for (o = s->objects; o; o = o->next) {
        if ((uintptr_t) o == value && o->obj) {
            return o;
        }
    }
    return NULL;
} /* find_object */

static dfs_obj_t *
handle_object(
    struct vfs_daos_state          *s,
    struct chimera_vfs_open_handle *h)
{
    struct vfs_daos_object *o = h ? find_object(s, h->vfs_private) : NULL;

    return o ? o->obj : NULL;
} /* handle_object */

static int
encode_object(
    struct vfs_daos_state    *s,
    dfs_obj_t                *obj,
    struct chimera_vfs_attrs *a)
{
    struct vfs_daos_entry *entry;
    struct vfs_daos_fh     key;
    int                    rc = vfs_daos_registry_add(s, obj, &entry);

    if (rc) {
        return rc;
    }
    key.hi   = entry->oid.hi;
    key.lo   = entry->oid.lo;
    key.type = entry->type;
    rc       = vfs_daos_fh_encode(s->mount_id, &key, a->va_fh, &a->va_fh_len);
    if (!rc) {
        a->va_fh_hash   = chimera_vfs_hash(a->va_fh, a->va_fh_len);
        a->va_set_mask |= CHIMERA_VFS_ATTR_FH;
    }
    return rc;
} /* encode_object */

static int
object_ino(
    struct vfs_daos_state *s,
    dfs_obj_t             *obj,
    uint64_t              *ino)
{
    daos_obj_id_t oid;
    int           rc = dfs_obj2id(obj, &oid);

    if (rc) {
        return rc;
    }
    for (struct vfs_daos_entry *e = s->registry; e; e = e->next) {
        uint64_t existing = e->oid.hi ^ e->oid.lo;
        if (!e->removed && existing == (oid.hi ^ oid.lo) &&
            (e->oid.hi != oid.hi || e->oid.lo != oid.lo)) {
            return EIO;
        }
    }
    /* The low word repeats for one object class, so it is not an inode. */
    *ino = oid.hi ^ oid.lo;
    return 0;
} /* object_ino */

static int
get_attrs(
    struct vfs_daos_state    *s,
    dfs_obj_t                *obj,
    struct chimera_vfs_attrs *a)
{
    struct stat st        = { 0 };
    uint64_t    mask      = a->va_req_mask;
    uint64_t    supported = SETTABLE | CHIMERA_VFS_ATTR_CTIME |
        CHIMERA_VFS_ATTR_NLINK | CHIMERA_VFS_ATTR_INUM | CHIMERA_VFS_ATTR_SPACE_USED |
        CHIMERA_VFS_ATTR_DEV | CHIMERA_VFS_ATTR_RDEV | CHIMERA_VFS_ATTR_FSID;
    int         rc;

    a->va_set_mask = 0;
    a->va_acl      = NULL;
    if (!obj) {
        return EBADF;
    }
    rc = dfs_ostat(s->dfs, obj, &st);
    if (rc) {
        return rc;
    }
    if (st.st_size < 0 || st.st_blocks < 0 || (uint64_t) st.st_blocks > UINT64_MAX / 512) {
        return EOVERFLOW;
    }
    a->va_dev  = XXH3_64bits_withSeed(s->mount_id, 16, 1);
    a->va_fsid = XXH3_64bits_withSeed(s->mount_id, 16, 2);
    rc         = object_ino(s, obj, &a->va_ino);
    if (rc) {
        return rc;
    }
    a->va_mode       = st.st_mode;
    a->va_uid        = st.st_uid;
    a->va_gid        = st.st_gid;
    a->va_rdev       = 0;
    a->va_size       = st.st_size;
    a->va_space_used = (uint64_t) st.st_blocks * 512;
    a->va_atime      = st.st_atim;
    a->va_mtime      = st.st_mtim;
    a->va_ctime      = st.st_ctim;
    a->va_nlink      = S_ISDIR(st.st_mode) ? 2 : 1;
    a->va_set_mask   = mask & supported;
    if (mask & CHIMERA_VFS_ATTR_FH) {
        rc = encode_object(s, obj, a);
        if (rc) {
            return rc;
        }
    }
    return 0;
} /* get_attrs */

static void
post_attrs(
    struct vfs_daos_state    *s,
    dfs_obj_t                *obj,
    struct chimera_vfs_attrs *a)
{
    if (get_attrs(s, obj, a)) {
        a->va_set_mask = 0;
    }
} /* post_attrs */

static int
cred_in_group(
    const struct chimera_vfs_cred *cred,
    uint64_t                       gid)
{
    if ((uint64_t) cred->gid == gid) {
        return 1;
    }
    for (uint32_t i = 0; i < cred->ngids && i < CHIMERA_VFS_CRED_MAX_GIDS; i++) {
        if ((uint64_t) cred->gids[i] == gid) {
            return 1;
        }
    }
    return 0;
} /* cred_in_group */

static int
check_access(
    struct vfs_daos_state         *s,
    dfs_obj_t                     *obj,
    const struct chimera_vfs_cred *cred,
    uint32_t                       access)
{
    struct chimera_vfs_attrs a = { .va_req_mask = CHIMERA_VFS_ATTR_MODE |
                                       CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID };
    int                      rc = get_attrs(s, obj, &a);

    if (rc) {
        return rc;
    }
    return chimera_vfs_access_allowed(&a, cred, access) ? 0 : EACCES;
} /* check_access */

static int
check_delete(
    struct vfs_daos_state         *s,
    dfs_obj_t                     *parent,
    dfs_obj_t                     *child,
    const struct chimera_vfs_cred *cred)
{
    struct chimera_vfs_attrs a = { .va_req_mask = CHIMERA_VFS_ATTR_MODE |
                                       CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID };
    struct chimera_vfs_attrs b  = { .va_req_mask = a.va_req_mask };
    int                      rc = get_attrs(s, parent, &a);

    if (!rc) {
        rc = get_attrs(s, child, &b);
    }
    if (!rc && !chimera_vfs_delete_allowed(&a, &b, cred)) {
        rc = EACCES;
    }
    return rc;
} /* check_delete */

static int
validate_setattr(const struct chimera_vfs_attrs *a)
{
    if (!a) {
        return 0;
    }
    uint64_t unsupported = a->va_set_mask &
        (CHIMERA_VFS_ATTR_CTIME | CHIMERA_VFS_ATTR_BTIME);
    if (a->va_ctime.tv_nsec == CHIMERA_VFS_TIME_OMIT) {
        unsupported &= ~CHIMERA_VFS_ATTR_CTIME;
    }
    if (a->va_btime.tv_nsec == CHIMERA_VFS_TIME_OMIT) {
        unsupported &= ~CHIMERA_VFS_ATTR_BTIME;
    }
    if (unsupported) {
        return ENOTSUP;
    }
    if (((a->va_set_mask & CHIMERA_VFS_ATTR_UID) && a->va_uid > UINT32_MAX) ||
        ((a->va_set_mask & CHIMERA_VFS_ATTR_GID) && a->va_gid > UINT32_MAX) ||
        ((a->va_set_mask & CHIMERA_VFS_ATTR_SIZE) && a->va_size > INT64_MAX)) {
        return EOVERFLOW;
    }
    const struct timespec *times[] = { &a->va_atime, &a->va_mtime };
    const uint64_t         masks[] = { CHIMERA_VFS_ATTR_ATIME, CHIMERA_VFS_ATTR_MTIME };
    for (unsigned int i = 0; i < 2; i++) {
        long ns = times[i]->tv_nsec;
        if ((a->va_set_mask & masks[i]) && ns != CHIMERA_VFS_TIME_NOW &&
            ns != CHIMERA_VFS_TIME_OMIT && (ns < 0 || ns >= 1000000000)) {
            return EINVAL;
        }
    }
    return 0;
} /* validate_setattr */

static int
set_attrs(
    struct vfs_daos_state          *s,
    dfs_obj_t                      *obj,
    const struct chimera_vfs_attrs *a,
    const struct chimera_vfs_cred  *cred,
    bool                            creating)
{
    struct stat     st = { 0 };
    struct timespec now;
    uint64_t        mask  = a ? a->va_set_mask & SETTABLE : 0;
    int             flags = 0;
    int             rc    = validate_setattr(a);

    if (rc || !mask) {
        return rc;
    }
    if (s->config.read_only) {
        return EROFS;
    }
    rc = dfs_ostat(s->dfs, obj, &st);
    if (rc) {
        return rc;
    }
    if (!creating && cred && cred->flavor != CHIMERA_VFS_AUTH_NONE && cred->uid) {
        if ((mask & (CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
                     CHIMERA_VFS_ATTR_GID)) && cred->uid != st.st_uid) {
            return EPERM;
        }
        const struct timespec *times[]      = { &a->va_atime, &a->va_mtime };
        const uint64_t         time_masks[] = { CHIMERA_VFS_ATTR_ATIME, CHIMERA_VFS_ATTR_MTIME };
        for (unsigned int i = 0; i < 2; i++) {
            if (!(mask & time_masks[i]) || times[i]->tv_nsec == CHIMERA_VFS_TIME_OMIT ||
                cred->uid == st.st_uid) {
                continue;
            }
            if (times[i]->tv_nsec != CHIMERA_VFS_TIME_NOW) {
                return EPERM;
            }
            rc = check_access(s, obj, cred, CHIMERA_ACE_WRITE_DATA);
            if (rc) {
                return rc;
            }
        }
        if ((mask & CHIMERA_VFS_ATTR_UID) && a->va_uid != st.st_uid) {
            return EPERM;
        }
        if (mask & CHIMERA_VFS_ATTR_GID) {
            bool member = a->va_gid == cred->gid;
            for (uint32_t i = 0; i < cred->ngids && i < CHIMERA_VFS_CRED_MAX_GIDS; i++) {
                member |= a->va_gid == cred->gids[i];
            }
            if (a->va_gid != st.st_gid && !member) {
                return EPERM;
            }
        }
    }
    if (mask & CHIMERA_VFS_ATTR_MODE) {
        mode_t mode = (st.st_mode & S_IFMT) | (a->va_mode & 07777);

        /* A non-privileged chmod of a non-directory clears set-group-ID
         * when the caller is not in the file's group. */
        if (!creating && cred && cred->flavor != CHIMERA_VFS_AUTH_NONE &&
            cred->uid && !S_ISDIR(st.st_mode) && (mode & S_ISGID) &&
            !cred_in_group(cred, st.st_gid)) {
            mode &= ~(mode_t) S_ISGID;
        }
        st.st_mode = mode;
        flags     |= DFS_SET_ATTR_MODE;
    }
    if (mask & CHIMERA_VFS_ATTR_UID) {
        st.st_uid = a->va_uid;
        flags    |= DFS_SET_ATTR_UID;
    }
    if (mask & CHIMERA_VFS_ATTR_GID) {
        st.st_gid = a->va_gid;
        flags    |= DFS_SET_ATTR_GID;
    }
    if (mask & CHIMERA_VFS_ATTR_SIZE) {
        if (!S_ISREG(st.st_mode)) {
            return EISDIR;
        }
        st.st_size = a->va_size;
        st.st_mode = chimera_vfs_killpriv_mode(cred, st.st_mode);
        flags     |= DFS_SET_ATTR_SIZE | DFS_SET_ATTR_MODE;
    }
    clock_gettime(CLOCK_REALTIME, &now);
    if ((mask & CHIMERA_VFS_ATTR_ATIME) &&
        chimera_vfs_resolve_set_time(&a->va_atime, &now, &st.st_atim)) {
        flags |= DFS_SET_ATTR_ATIME;
    }
    if ((mask & CHIMERA_VFS_ATTR_MTIME) &&
        chimera_vfs_resolve_set_time(&a->va_mtime, &now, &st.st_mtim)) {
        flags |= DFS_SET_ATTR_MTIME;
    }
    return flags ? dfs_osetattr(s->dfs, obj, &st, flags) : 0;
} /* set_attrs */

static int
lookup(
    struct vfs_daos_state *s,
    dfs_obj_t             *parent,
    const char            *name,
    dfs_obj_t            **obj)
{
    int flags = (s->config.read_only ? O_RDONLY : O_RDWR) | O_NOFOLLOW;

    if (!strcmp(name, ".")) {
        return vfs_daos_dup(s, parent, flags, obj);
    }
    if (!strcmp(name, "..")) {
        return vfs_daos_parent(s, parent, obj);
    }
    int rc = vfs_daos_lookup_rel(s, parent, name, flags, obj, NULL, NULL);
    if (!rc) {
        rc = vfs_daos_admit(s, obj, parent, false);
        if (rc) {
            release_object(s, *obj);
            *obj = NULL;
        }
    }
    return rc;
} /* lookup */

static void
cursor_free(struct vfs_daos_cursor *c)
{
    while (c->checkpoints) {
        struct vfs_daos_checkpoint *p = c->checkpoints;
        c->checkpoints = p->next;
        free(p);
    }
    free(c);
} /* cursor_free */

void
vfs_daos_cursors_clear(struct vfs_daos_state *s)
{
    while (s->cursors) {
        struct vfs_daos_cursor *c = s->cursors;
        s->cursors = c->next;
        cursor_free(c);
    }
    s->cursor_count = 0;
    s->generation++;
} /* vfs_daos_cursors_clear */

static struct vfs_daos_cursor *
cursor_start(
    struct vfs_daos_state *s,
    daos_obj_id_t          oid,
    uint32_t               flags)
{
    struct vfs_daos_cursor *c = calloc(1, sizeof(*c));

    if (!c) {
        return NULL;
    }
    if (s->cursor_count == 1024) {
        struct vfs_daos_cursor **oldest = &s->cursors;
        for (struct vfs_daos_cursor **p = &s->cursors; *p; p = &(*p)->next) {
            if ((*p)->touched < (*oldest)->touched) {
                oldest = p;
            }
        }
        struct vfs_daos_cursor  *old = *oldest;
        *oldest                      = old->next;
        cursor_free(old);
        s->cursor_count--;
        s->cursor_evictions++;
    }
    c->oid        = oid;
    c->flags      = flags;
    c->generation = s->generation;
    c->touched    = ++s->cursor_clock;
    c->next       = s->cursors;
    s->cursors    = c;
    s->cursor_count++;
    return c;
} /* cursor_start */

static struct vfs_daos_cursor *
cursor_find(
    struct vfs_daos_state      *s,
    uint64_t                    token,
    daos_obj_id_t               oid,
    uint32_t                    flags,
    struct vfs_daos_checkpoint *position)
{
    for (struct vfs_daos_cursor *c = s->cursors; c; c = c->next) {
        if (c->generation != s->generation || !oid_equal(c->oid, oid) ||
            c->flags != flags) {
            continue;
        }
        for (struct vfs_daos_checkpoint *p = c->checkpoints; p; p = p->next) {
            if (p->token == token) {
                *position = *p;
                c->touched = ++s->cursor_clock;
                return c;
            }
        }
    }
    return NULL;
} /* cursor_find */

static int
cursor_store(
    struct vfs_daos_state            *s,
    struct vfs_daos_cursor           *c,
    const struct vfs_daos_checkpoint *position,
    uint64_t                         *token)
{
    if (!s->next_cookie || s->next_cookie == UINT64_MAX) {
        return EOVERFLOW;
    }
    struct vfs_daos_checkpoint *p = malloc(sizeof(*p));
    if (!p) {
        return ENOMEM;
    }
    *p             = *position;
    p->token       = s->next_cookie++;
    p->next        = c->checkpoints;
    c->checkpoints = p;
    c->touched     = ++s->cursor_clock;
    *token         = p->token;
    return 0;
} /* cursor_store */

static int
read_directory(
    struct vfs_daos_state      *s,
    struct chimera_vfs_request *r,
    dfs_obj_t                  *dir)
{
    struct vfs_daos_checkpoint position = { .anchor = DAOS_ANCHOR_INIT };
    struct vfs_daos_cursor    *scan;
    daos_obj_id_t              oid;
    int                        rc = dfs_obj2id(dir, &oid);

    r->readdir.r_cookie               = r->readdir.cookie;
    r->readdir.r_eof                  = 0;
    r->readdir.r_verifier             = 0;
    r->readdir.r_dir_attr.va_set_mask = 0;
    if (rc) {
        return rc;
    }
    if (r->cred) {
        rc = check_access(s, dir, r->cred, CHIMERA_ACE_READ_DATA);
        if (rc) {
            return rc;
        }
    }
    if (r->readdir.flags & ~CHIMERA_VFS_READDIR_EMIT_DOT) {
        return EINVAL;
    }
    if (r->readdir.verifier && r->readdir.verifier != s->generation) {
        return BAD_COOKIE;
    }
    if (r->readdir.cookie) {
        scan = cursor_find(s, r->readdir.cookie, oid, r->readdir.flags, &position);
        if (!scan) {
            return BAD_COOKIE;
        }
    } else {
        scan = cursor_start(s, oid, r->readdir.flags);
        if (!scan) {
            return ENOMEM;
        }
        position.dot = r->readdir.flags & CHIMERA_VFS_READDIR_EMIT_DOT ? 0 : 2;
    }
    r->readdir.r_verifier = s->generation;
    for (uint32_t count = 0; count < s->config.batch_entries; count++) {
        struct dirent            entry = { 0 };
        struct chimera_vfs_attrs a     = { .va_req_mask = r->readdir.attr_mask |
                                               CHIMERA_VFS_ATTR_INUM };
        dfs_obj_t               *child = NULL;
        uint64_t                 cookie;
        if (position.dot < 2) {
            strcpy(entry.d_name, position.dot++ ? ".." : ".");
        } else {
            uint32_t nr = 1;
            if (daos_anchor_is_eof(&position.anchor)) {
                r->readdir.r_eof = 1;
                break;
            }
            rc = dfs_readdir(s->dfs, dir, &position.anchor, &nr, &entry);
            if (rc) {
                return rc;
            }
            if (!nr || !strcmp(entry.d_name, ".") || !strcmp(entry.d_name, "..")) {
                continue;
            }
        }
        rc = lookup(s, dir, entry.d_name, &child);
        if (rc == ENOENT) {
            continue;
        }
        if (!rc) {
            rc = get_attrs(s, child, &a);
        }
        release_object(s, child);
        if (rc) {
            return rc;
        }
        rc = cursor_store(s, scan, &position, &cookie);
        if (rc) {
            return rc;
        }
        if (r->readdir.callback(a.va_ino, cookie, entry.d_name, strlen(entry.d_name),
                                &a, r->proto_private_data)) {
            post_attrs(s, dir, &r->readdir.r_dir_attr);
            return 0;
        }
        r->readdir.r_cookie = cookie;
    }
    if (position.dot == 2 && daos_anchor_is_eof(&position.anchor)) {
        r->readdir.r_eof = 1;
    }
    post_attrs(s, dir, &r->readdir.r_dir_attr);
    return 0;
} /* read_directory */

static int
open_flags(uint32_t flags)
{
    if (flags & ~((1U << 13) - 1)) {
        return EINVAL;
    }
    if ((flags & CHIMERA_VFS_OPEN_READ_ONLY) && (flags & CHIMERA_VFS_OPEN_WRITE_ONLY)) {
        return EINVAL;
    }
    if ((flags & CHIMERA_VFS_OPEN_EXCLUSIVE) && !(flags & CHIMERA_VFS_OPEN_CREATE)) {
        return EINVAL;
    }
    if ((flags & CHIMERA_VFS_OPEN_TRUNCATE) &&
        (flags & (CHIMERA_VFS_OPEN_READ_ONLY | CHIMERA_VFS_OPEN_PATH))) {
        return EINVAL;
    }
    return 0;
} /* open_flags */

static int
open_type(
    struct vfs_daos_state *s,
    dfs_obj_t             *obj,
    uint32_t               flags)
{
    struct stat st;
    int         rc = dfs_ostat(s->dfs, obj, &st);

    if (rc) {
        return rc;
    }
    if (S_ISLNK(st.st_mode) && (flags & CHIMERA_VFS_OPEN_STOP_SYMLINK)) {
        return ELOOP;
    }
    if (S_ISLNK(st.st_mode) && (flags & CHIMERA_VFS_OPEN_NOFOLLOW) &&
        !(flags & CHIMERA_VFS_OPEN_PATH)) {
        return ELOOP;
    }
    if ((flags & CHIMERA_VFS_OPEN_REGULAR_ONLY) && !S_ISREG(st.st_mode)) {
        return chimera_vfs_nonreg_error(st.st_mode);
    }
    if ((flags & CHIMERA_VFS_OPEN_CREATE_REGULAR) && !S_ISREG(st.st_mode)) {
        return S_ISDIR(st.st_mode) ? EISDIR : EEXIST;
    }
    if ((flags & CHIMERA_VFS_OPEN_DIRECTORY) && !S_ISDIR(st.st_mode)) {
        return ENOTDIR;
    }
    if (S_ISLNK(st.st_mode) && !(flags & CHIMERA_VFS_OPEN_PATH)) {
        return (flags & CHIMERA_VFS_OPEN_NOFOLLOW) ? ELOOP : SYMLINK_RESULT;
    }
    return 0;
} /* open_type */

static int
bind_open(
    struct vfs_daos_state         *s,
    dfs_obj_t                     *obj,
    uint32_t                       flags,
    const struct chimera_vfs_cred *cred,
    bool                           created,
    uint64_t                      *value)
{
    struct vfs_daos_object *o;
    uint32_t                access = 0;
    int                     rc     = open_type(s, obj, flags);

    if (rc) {
        return rc;
    }
    if (!(flags & CHIMERA_VFS_OPEN_PATH)) {
        if (!(flags & CHIMERA_VFS_OPEN_WRITE_ONLY)) {
            access |= CHIMERA_ACE_READ_DATA;
        }
        if (!(flags & CHIMERA_VFS_OPEN_READ_ONLY)) {
            access |= CHIMERA_ACE_WRITE_DATA;
            if (s->config.read_only) {
                return EROFS;
            }
        }
        rc = created ? 0 : check_access(s, obj, cred, access);
        if (rc) {
            return rc;
        }
    }
    struct vfs_daos_entry *entry;
    rc = vfs_daos_registry_add(s, obj, &entry);
    if (rc) {
        return rc;
    }
    o                   = s->reserved_wrapper;
    s->reserved_wrapper = NULL;
    if (!o) {
        o = calloc(1, sizeof(*o));
    }
    if (!o) {
        return ENOMEM;
    }
    rc = dfs_obj2id(obj, &o->oid);
    if (!rc) {
        rc = vfs_daos_dup(s, obj,
                          (flags & (CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_READ_ONLY)) ?
                          O_RDONLY : O_RDWR, &o->obj);
    }
    if (rc) {
        free(o);
        return rc;
    }
    o->flags   = flags;
    o->next    = s->objects;
    s->objects = o;
    *value     = (uintptr_t) o;
    return 0;
} /* bind_open */

static int
close_open(
    struct vfs_daos_state *s,
    uint64_t               value)
{
    struct vfs_daos_object **p = &s->objects;

    while (*p) {
        struct vfs_daos_object *o = *p;
        if ((uintptr_t) o == value) {
            int rc = o->obj ? vfs_daos_release(s, o->obj) : EBADF;
            o->obj = NULL;
            if (rc) {
                s->unavailable = true;
                /* Retain the record; never retry an uncertain release. */
                return EIO;
            }
            *p = o->next;
            free(o);
            return 0;
        }
        p = &o->next;
    }
    return EBADF;
} /* close_open */

static int
killpriv(
    struct vfs_daos_state         *s,
    dfs_obj_t                     *obj,
    const struct chimera_vfs_cred *cred)
{
    struct stat st;
    uint32_t    mode;
    int         rc = dfs_ostat(s->dfs, obj, &st);

    if (rc) {
        return rc;
    }
    mode = chimera_vfs_killpriv_mode(cred, st.st_mode);
    if (mode != st.st_mode) {
        st.st_mode = mode;
        return dfs_osetattr(s->dfs, obj, &st, DFS_SET_ATTR_MODE);
    }
    return 0;
} /* killpriv */

static int
sync_container(struct vfs_daos_state *s)
{
    struct timespec start, end;

    clock_gettime(CLOCK_MONOTONIC, &start);
    int             rc = dfs_sync(s->dfs);
    clock_gettime(CLOCK_MONOTONIC, &end);
    s->sync_ns += (uint64_t) (end.tv_sec - start.tv_sec) * 1000000000 +
        end.tv_nsec - start.tv_nsec;
    return rc;
} /* sync_container */

static int
io_data(
    struct vfs_daos_state      *s,
    struct chimera_vfs_request *r,
    bool                        write)
{
    struct chimera_vfs_open_handle *h      = write ? r->write.handle : r->read.handle;
    struct vfs_daos_object         *o      = h ? find_object(s, h->vfs_private) : NULL;
    struct evpl_iovec              *iov    = write ? r->write.iov : r->read.iov;
    int                             niov   = write ? r->write.niov : r->read.buffers_provided;
    uint64_t                        offset = write ? r->write.offset : r->read.offset;
    size_t                          left   = write ? r->write.length : r->read.length;
    size_t                          wanted = left;
    uint32_t                        prefix = write ? 0 : r->read.aligned_prefix;
    d_sg_list_t                     sgl    = { 0 };
    daos_size_t                     actual = 0, size;
    int                             rc = 0;

    if (write) {
        r->write.r_length                = 0;
        r->write.r_sync                  = CHIMERA_VFS_WRITE_UNSTABLE;
        r->write.r_pre_attr.va_set_mask  = 0;
        r->write.r_post_attr.va_set_mask = 0;
    } else {
        r->read.r_length           = 0;
        r->read.r_eof              = 0;
        r->read.r_niov             = 0;
        r->read.r_attr.va_set_mask = 0;
    }
    if (!o || (o->flags & CHIMERA_VFS_OPEN_PATH) ||
        (o->flags & (write ? CHIMERA_VFS_OPEN_READ_ONLY : CHIMERA_VFS_OPEN_WRITE_ONLY))) {
        return EBADF;
    }
    if (offset > INT64_MAX || left > INT64_MAX - offset) {
        return EFBIG;
    }
    if (write && s->config.read_only) {
        return EROFS;
    }
    if (write && r->write.sync > CHIMERA_VFS_WRITE_FILESYNC) {
        return EINVAL;
    }
    if (left && (!iov || niov <= 0 || niov > 65536)) {
        return EINVAL;
    }
    if (left) {
        sgl.sg_iovs = calloc((size_t) niov, sizeof(*sgl.sg_iovs));
        if (!sgl.sg_iovs) {
            return ENOMEM;
        }
        for (int i = 0; i < niov && left; i++) {
            size_t skip = i ? 0 : prefix;
            size_t n    = iov[i].length;
            if (skip > n || !iov[i].data) {
                rc = EINVAL;
                goto out;
            }
            n -= skip;
            if (n > left) {
                n = left;
            }
            if (n) {
                d_iov_set(&sgl.sg_iovs[sgl.sg_nr++], (char *) iov[i].data + skip, n);
                left -= n;
            }
        }
        if (left) {
            rc = EINVAL;
            goto out;
        }
    }
    if (write) {
        post_attrs(s, o->obj, &r->write.r_pre_attr);
        if (wanted) {
            rc = o->removed ? 0 : killpriv(s, o->obj, r->cred);
            if (!rc) {
                rc = dfs_write(s->dfs, o->obj, &sgl, offset, NULL);
            }
        }
        if (!rc) {
            r->write.r_length = wanted;
            post_attrs(s, o->obj, &r->write.r_post_attr);
        }
        if (rc) {
            fprintf(stderr, "vfs_daos: write/sync may have changed data: %d\n", rc);
        }
    } else {
        if (wanted) {
            rc = dfs_read(s->dfs, o->obj, &sgl, offset, &actual, NULL);
        }
        if (!rc && actual > wanted) {
            rc = EIO;
        }
        if (!rc) {
            r->read.r_length = actual;
            r->read.r_niov   = sgl.sg_nr;
            if (!dfs_get_size(s->dfs, o->obj, &size)) {
                r->read.r_eof = offset + actual >= size;
            }
            post_attrs(s, o->obj, &r->read.r_attr);
        }
    }
 out:
    free(sgl.sg_iovs);
    return rc;
} /* io_data */

static void
rollback_create(
    struct vfs_daos_state *s,
    dfs_obj_t             *parent,
    const char            *name,
    dfs_obj_t             *obj)
{
    dfs_obj_t    *current = NULL;
    daos_obj_id_t expected, actual;
    int           rc = dfs_obj2id(obj, &expected);

    if (!rc) {
        rc = lookup(s, parent, name, &current);
    }
    if (!rc) {
        rc = dfs_obj2id(current, &actual);
    }
    if (!rc && oid_equal(expected, actual)) {
        rc = dfs_remove(s->dfs, parent, name, false, &actual);
        if (!rc) {
            vfs_daos_invalidate(s, expected);
        }
    }
    release_object(s, current);
    if (rc) {
        fprintf(stderr, "vfs_daos: create rollback incomplete: %d\n", rc);
    }
} /* rollback_create */

static int
create_entry(
    struct vfs_daos_state          *s,
    struct chimera_vfs_request     *r,
    dfs_obj_t                      *parent,
    const char                     *name,
    const char                     *target,
    mode_t                          type,
    const struct chimera_vfs_attrs *set,
    dfs_obj_t                     **obj)
{
    struct chimera_vfs_attrs intended = { 0 };
    struct stat              pst;
    int                      rc = validate_setattr(set);

    *obj = NULL;
    if (rc) {
        return rc;
    }
    if (s->config.read_only) {
        return EROFS;
    }
    rc = check_access(s, parent, r->cred, CHIMERA_ACE_WRITE_DATA | CHIMERA_ACE_EXECUTE);
    if (!rc) {
        rc = dfs_ostat(s->dfs, parent, &pst);
    }
    if (rc) {
        return rc;
    }
    if (set) {
        intended = *set;
    }
    intended.va_mode = type | ((set && (set->va_set_mask & CHIMERA_VFS_ATTR_MODE)) ?
                               set->va_mode & 07777 : (type == S_IFDIR ? 0777 : 0666));
    if (!(intended.va_set_mask & CHIMERA_VFS_ATTR_UID)) {
        intended.va_uid = r->cred ? r->cred->uid : 0;
    }
    if (!(intended.va_set_mask & CHIMERA_VFS_ATTR_GID)) {
        intended.va_gid = (pst.st_mode & S_ISGID) ? pst.st_gid : (r->cred ? r->cred->gid : 0);
    }
    if (type == S_IFDIR && (pst.st_mode & S_ISGID)) {
        intended.va_mode |= S_ISGID;
    }
    intended.va_set_mask |= CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
    if (r->cred && r->cred->flavor == CHIMERA_VFS_AUTH_UNIX && r->cred->uid) {
        bool member = intended.va_gid == r->cred->gid;
        for (uint32_t i = 0; i < r->cred->ngids && i < CHIMERA_VFS_CRED_MAX_GIDS; i++) {
            member |= intended.va_gid == r->cred->gids[i];
        }
        if (intended.va_uid != r->cred->uid ||
            (!member && !((pst.st_mode & S_ISGID) && intended.va_gid == pst.st_gid))) {
            return EPERM;
        }
    }
    if (type != S_IFREG && (intended.va_set_mask & CHIMERA_VFS_ATTR_SIZE)) {
        return EINVAL;
    }
    /* Reserve handles and a registry slot before the name is created. */
    rc = vfs_daos_reserve(s, 3, true);
    if (rc) {
        return rc;
    }
    if (s->unavailable) {
        return EIO;
    }
    rc = vfs_daos_open(s, parent, name, type | 0600, O_CREAT | O_EXCL | O_RDWR,
                       s->config.oclass, type == S_IFREG ? s->config.chunk_size : 0,
                       target, obj);
    if (rc) {
        *obj = NULL;
        return rc;
    }
    uint64_t ino;
    rc = vfs_daos_admit(s, obj, parent, true);
    if (!rc) {
        rc = object_ino(s, *obj, &ino);
    }
    if (!rc) {
        rc = set_attrs(s, *obj, &intended, r->cred, true);
    }
    if (rc) {
        rollback_create(s, parent, name, *obj);
        release_object(s, *obj);
        *obj = NULL;
    }
    return rc;
} /* create_entry */

static int
open_at(
    struct vfs_daos_state      *s,
    struct chimera_vfs_request *r,
    dfs_obj_t                  *parent)
{
    char       name[CHIMERA_VFS_NAME_MAX];
    dfs_obj_t *obj     = NULL;
    uint32_t   flags   = r->open_at.flags;
    bool       created = false;
    int        rc;

    r->open_at.r_created                   = 0;
    r->open_at.r_vfs_private               = 0;
    r->open_at.r_attr.va_set_mask          = 0;
    r->open_at.r_dir_pre_attr.va_set_mask  = 0;
    r->open_at.r_dir_post_attr.va_set_mask = 0;
    rc                                     = open_flags(flags);
    if (!rc) {
        rc = mutation_name(r->open_at.name, r->open_at.namelen, name);
    }
    if (!rc && (flags & (CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_TRUNCATE))) {
        rc = validate_setattr(r->open_at.set_attr);
    }
    if (rc) {
        return rc;
    }
    post_attrs(s, parent, &r->open_at.r_dir_pre_attr);
    rc = lookup(s, parent, name, &obj);
    if (!rc) {
        rc = open_type(s, obj, flags);
        if (!rc && (flags & CHIMERA_VFS_OPEN_EXCLUSIVE)) {
            rc = EEXIST;
        }
    } else if (rc == ENOENT && (flags & CHIMERA_VFS_OPEN_CREATE)) {
        rc = create_entry(s, r, parent, name, NULL,
                          flags & CHIMERA_VFS_OPEN_DIRECTORY ? S_IFDIR : S_IFREG,
                          r->open_at.set_attr, &obj);
        created = !rc;
    }
    if (rc) {
        goto out;
    }
    /* Obtain the identity before an existing object's truncate has effects. */
    r->open_at.r_attr.va_req_mask |= CHIMERA_VFS_ATTR_FH;
    rc                             = get_attrs(s, obj, &r->open_at.r_attr);
    if (!rc) {
        rc = bind_open(s, obj, flags, r->cred, created, &r->open_at.r_vfs_private);
    }
    if (!rc && (flags & CHIMERA_VFS_OPEN_TRUNCATE) && !created) {
        struct chimera_vfs_attrs a = { 0 };
        if (r->open_at.set_attr) {
            a = *r->open_at.set_attr;
        }
        a.va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
        a.va_size      = 0;
        struct vfs_daos_object  *opened = find_object(s, r->open_at.r_vfs_private);
        rc = set_attrs(s, opened->obj, &a, r->cred, false);
        if (!rc) {
            /* Preserve a successful mutation even if optional stat fails. */
            struct chimera_vfs_attrs after = r->open_at.r_attr;
            if (!get_attrs(s, opened->obj, &after)) {
                r->open_at.r_attr = after;
            } else {
                r->open_at.r_attr.va_set_mask = CHIMERA_VFS_ATTR_FH;
            }
        }
    }
    if (rc && r->open_at.r_vfs_private) {
        close_open(s, r->open_at.r_vfs_private);
        r->open_at.r_vfs_private = 0;
    }
    if (rc && created) {
        rollback_create(s, parent, name, obj);
    }
    if (!rc) {
        r->open_at.r_created = created;
    }
    post_attrs(s, parent, &r->open_at.r_dir_post_attr);
 out:
    release_object(s, obj);
    return rc;
} /* open_at */

static int
remove_at(
    struct vfs_daos_state      *s,
    struct chimera_vfs_request *r,
    dfs_obj_t                  *parent)
{
    char          name[CHIMERA_VFS_NAME_MAX];
    dfs_obj_t    *obj = NULL;
    daos_obj_id_t actual;
    struct stat   st;
    int           rc = mutation_name(r->remove_at.name, r->remove_at.namelen, name);

    r->remove_at.r_unmatched                 = 0;
    r->remove_at.r_removed_attr.va_set_mask  = 0;
    r->remove_at.r_dir_pre_attr.va_set_mask  = 0;
    r->remove_at.r_dir_post_attr.va_set_mask = 0;
    if (rc || s->config.read_only) {
        return rc ? rc : EROFS;
    }
    post_attrs(s, parent, &r->remove_at.r_dir_pre_attr);
    rc = lookup(s, parent, name, &obj);
    if (!rc && r->remove_at.match_child_fh) {
        struct vfs_daos_fh expected;
        daos_obj_id_t      oid;
        rc = vfs_daos_fh_decode(s->mount_id, r->remove_at.child_fh,
                                r->remove_at.child_fh_len, &expected);
        if (!rc) {
            rc = dfs_obj2id(obj, &oid);
        }
        if (!rc && (oid.hi != expected.hi || oid.lo != expected.lo)) {
            r->remove_at.r_unmatched = 1;
            goto out;
        }
    }
    if (!rc) {
        rc = dfs_ostat(s->dfs, obj, &st);
    }
    if (!rc && (r->remove_at.flags & CHIMERA_VFS_REMOVE_ISDIR) && !S_ISDIR(st.st_mode)) {
        rc = ENOTDIR;
    }
    if (!rc && (r->remove_at.flags & CHIMERA_VFS_REMOVE_ISNOTDIR) && S_ISDIR(st.st_mode)) {
        rc = EISDIR;
    }
    if (!rc) {
        rc = check_delete(s, parent, obj, r->cred);
    }
    if (!rc) {
        post_attrs(s, obj, &r->remove_at.r_removed_attr);
        rc = dfs_remove(s->dfs, parent, name, false, &actual);
        if (!rc) {
            vfs_daos_invalidate(s, actual);
            post_attrs(s, parent, &r->remove_at.r_dir_post_attr);
        }
    }
 out:
    release_object(s, obj);
    return rc;
} /* remove_at */

static int
rename_at(
    struct vfs_daos_state      *s,
    struct chimera_vfs_request *r)
{
    char          name[CHIMERA_VFS_NAME_MAX], new_name[CHIMERA_VFS_NAME_MAX];
    dfs_obj_t    *parent = NULL, *dest = NULL, *child = NULL, *victim = NULL, *moved = NULL;
    daos_obj_id_t oid, replaced = { 0 };
    int           rc = mutation_name(r->rename_at.name, r->rename_at.namelen, name);

    r->rename_at.r_fromdir_pre_attr.va_set_mask  = 0;
    r->rename_at.r_fromdir_post_attr.va_set_mask = 0;
    r->rename_at.r_todir_pre_attr.va_set_mask    = 0;
    r->rename_at.r_todir_post_attr.va_set_mask   = 0;
    if (!rc) {
        rc = mutation_name(r->rename_at.new_name, r->rename_at.new_namelen, new_name);
    }
    if (rc || s->config.read_only) {
        return rc ? rc : EROFS;
    }
    if (!r->rename_at.new_fh || r->rename_at.new_fhlen < 16) {
        s->fh_decode_failures++;
        return ESTALE;
    }
    if (memcmp(r->rename_at.new_fh, s->mount_id, 16)) {
        return EXDEV;
    }
    rc = vfs_daos_resolve(s, r->fh, r->fh_len, &parent);
    if (!rc) {
        rc = vfs_daos_resolve(s, r->rename_at.new_fh, r->rename_at.new_fhlen, &dest);
    }
    if (!rc) {
        rc = lookup(s, parent, name, &child);
    }
    if (!rc) {
        rc = dfs_obj2id(child, &oid);
    }
    if (!rc) {
        rc = check_delete(s, parent, child, r->cred);
    }
    if (!rc) {
        rc = check_access(s, dest, r->cred, CHIMERA_ACE_WRITE_DATA | CHIMERA_ACE_EXECUTE);
    }
    if (!rc) {
        rc = lookup(s, dest, new_name, &victim);
        if (rc == ENOENT) {
            rc = 0;
        } else if (!rc) {
            rc = check_delete(s, dest, victim, r->cred);
        }
    }
    if (!rc) {
        post_attrs(s, parent, &r->rename_at.r_fromdir_pre_attr);
        post_attrs(s, dest, &r->rename_at.r_todir_pre_attr);
        rc = vfs_daos_reserve(s, 1, false);
        if (rc) {
            goto out;
        }
        rc = dfs_move(s->dfs, parent, name, dest, new_name, &replaced);
        if (!rc) {
            int update_rc = vfs_daos_lookup_rel(s, dest, new_name,
                                                O_RDWR | O_NOFOLLOW,
                                                &moved, NULL, NULL);
            if (!oid_equal(oid, replaced)) {
                vfs_daos_invalidate(s, replaced);
            }
            vfs_daos_repair(s, oid, moved, dest, new_name, update_rc);
            post_attrs(s, parent, &r->rename_at.r_fromdir_post_attr);
            post_attrs(s, dest, &r->rename_at.r_todir_post_attr);
        }
    }
 out:
    release_object(s, moved);
    release_object(s, victim);
    release_object(s, child);
    release_object(s, dest);
    release_object(s, parent);
    return rc;
} /* rename_at */

static int
mount_fs(
    struct vfs_daos_state      *s,
    struct chimera_vfs_request *r)
{
    struct chimera_vfs_attrs *a = &r->mount.r_attr;
    int                       rc;

    a->va_set_mask           = 0;
    r->mount.r_mount_private = NULL;
    if (r->mount.pathlen != 0 || (r->mount.path && r->mount.path[0])) {
        return EINVAL;
    }
    if (s->active) {
        return EBUSY;
    }
    if (!s->initialized) {
        return s->error ? s->error : EIO;
    }
    if (s->unavailable) {
        return EIO;
    }
    rc = vfs_daos_connect(s);
    if (!rc) {
        rc = vfs_daos_reserve_root(s);
    }
    if (!rc) {
        rc = vfs_daos_lookup_root(s, &s->root);
    }
    if (!rc) {
        struct stat st;
        rc = dfs_ostat(s->dfs, s->root, &st);
        if (!rc && !S_ISDIR(st.st_mode)) {
            rc = ESTALE;
        }
    }
    if (!rc) {
        struct vfs_daos_entry *entry;
        rc = vfs_daos_registry_add(s, s->root, &entry);
        if (!rc) {
            entry->parent_oid   = entry->oid;
            entry->parent_valid = true;
            struct vfs_daos_fh key = { entry->oid.hi, entry->oid.lo, entry->type };
            rc = vfs_daos_fh_mount(s->config.uuid, &key, a->va_fh, &a->va_fh_len);
            if (rc) {
                vfs_daos_disconnect(s);
                return rc;
            }
            memcpy(s->mount_id, a->va_fh, 16);
            a->va_req_mask |= CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_FH |
                CHIMERA_VFS_ATTR_FSID;
            rc = get_attrs(s, s->root, a);
        }
    }
    if (rc) {
        a->va_set_mask = 0;
        vfs_daos_disconnect(s);
        return rc;
    }
    s->active                = true;
    s->error                 = 0;
    r->mount.r_mount_private = s;
    return 0;
} /* mount_fs */

static int
validate_request(const struct chimera_vfs_request *r)
{
    char name[CHIMERA_VFS_NAME_MAX];
    int  rc;

    switch (r->opcode) {
        case CHIMERA_VFS_OP_LOOKUP_AT:
            return name_copy(r->lookup_at.component, r->lookup_at.component_len, name);
        case CHIMERA_VFS_OP_OPEN_AT:
            rc = open_flags(r->open_at.flags);
            if (!rc) {
                rc = mutation_name(r->open_at.name, r->open_at.namelen, name);
            }
            if (!rc && (r->open_at.flags & (CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_TRUNCATE))) {
                rc = validate_setattr(r->open_at.set_attr);
            }
            return rc;
        case CHIMERA_VFS_OP_REMOVE_AT:
            return mutation_name(r->remove_at.name, r->remove_at.namelen, name);
        case CHIMERA_VFS_OP_RENAME_AT:
            rc = mutation_name(r->rename_at.name, r->rename_at.namelen, name);
            return rc ? rc : mutation_name(r->rename_at.new_name, r->rename_at.new_namelen, name);
        case CHIMERA_VFS_OP_MKDIR_AT:
            rc = mutation_name(r->mkdir_at.name, r->mkdir_at.name_len, name);
            return rc ? rc : validate_setattr(r->mkdir_at.set_attr);
        case CHIMERA_VFS_OP_MKNOD_AT:
            if (!r->mknod_at.set_attr || (r->mknod_at.set_attr->va_mode & S_IFMT) != S_IFREG) {
                return ENOTSUP;
            }
            rc = mutation_name(r->mknod_at.name, r->mknod_at.name_len, name);
            return rc ? rc : validate_setattr(r->mknod_at.set_attr);
        case CHIMERA_VFS_OP_SYMLINK_AT:
            rc = mutation_name(r->symlink_at.name, r->symlink_at.namelen, name);
            return rc ? rc : validate_setattr(r->symlink_at.set_attr);
        case CHIMERA_VFS_OP_SETATTR:
            return validate_setattr(r->setattr.set_attr);
        case CHIMERA_VFS_OP_ALLOCATE:
            return r->allocate.flags == CHIMERA_VFS_ALLOCATE_DEALLOCATE ? 0 : ENOTSUP;
        default:
            return 0;
    } /* switch */
} /* validate_request */

static int
xattr_name(
    const char *in,
    uint32_t    len,
    char       *out)
{
    if (!in || !len || memchr(in, 0, len) || len > DFS_MAX_XATTR_NAME) {
        return EINVAL;
    }
    memcpy(out, in, len);
    out[len] = 0;
    return 0;
} /* xattr_name */

static int
xattr_flags(uint32_t option)
{
    if (option == CHIMERA_VFS_XATTR_EITHER) {
        return 0;
    }
    if (option == CHIMERA_VFS_XATTR_CREATE) {
        return XATTR_CREATE;
    }
    if (option == CHIMERA_VFS_XATTR_REPLACE) {
        return XATTR_REPLACE;
    }
    return -1;
} /* xattr_flags */

/* libdfs reports the full size and can write a short list without an error. */
static int
xattr_size(
    daos_size_t need,
    daos_size_t room)
{
    return need > room ? ERANGE : 0;
} /* xattr_size */

static int
xattr_get(
    struct vfs_daos_state      *s,
    dfs_obj_t                  *obj,
    struct chimera_vfs_request *r)
{
    char        name[DFS_MAX_XATTR_NAME + 1];
    daos_size_t need = 0;
    int         rc   = xattr_name(r->get_xattr.name, r->get_xattr.namelen, name);

    if (!rc) {
        rc = check_access(s, obj, r->cred, CHIMERA_ACE_READ_DATA);
    }
    if (!rc) {
        rc = dfs_getxattr(s->dfs, obj, name, NULL, &need);
    }
    if (!rc) {
        rc = xattr_size(need, r->get_xattr.value_maxlen);
    }
    if (!rc && need) {
        daos_size_t got = r->get_xattr.value_maxlen;
        rc = dfs_getxattr(s->dfs, obj, name, (void *) r->get_xattr.value, &got);
        if (!rc && got != need) {
            rc = EIO;
        }
    }
    if (!rc) {
        r->get_xattr.r_value_len = need;
    }
    return rc;
} /* xattr_get */

static int
xattr_set(
    struct vfs_daos_state      *s,
    dfs_obj_t                  *obj,
    struct chimera_vfs_request *r)
{
    char name[DFS_MAX_XATTR_NAME + 1];
    int  flags = xattr_flags(r->set_xattr.option);
    int  rc;

    r->set_xattr.r_pre_attr.va_set_mask  = 0;
    r->set_xattr.r_post_attr.va_set_mask = 0;
    if (flags < 0 || r->set_xattr.value_len > DFS_MAX_XATTR_LEN) {
        return EINVAL;
    }
    rc = xattr_name(r->set_xattr.name, r->set_xattr.namelen, name);
    if (!rc && s->config.read_only) {
        rc = EROFS;
    }
    if (!rc) {
        rc = check_access(s, obj, r->cred, CHIMERA_ACE_WRITE_DATA);
    }
    if (!rc) {
        post_attrs(s, obj, &r->set_xattr.r_pre_attr);
        rc = dfs_setxattr(s->dfs, obj, name, r->set_xattr.value,
                          r->set_xattr.value_len, flags);
        if (rc == ENOENT && flags == XATTR_REPLACE) {
            rc = ENODATA;
        }
    }
    if (!rc) {
        post_attrs(s, obj, &r->set_xattr.r_post_attr);
    }
    return rc;
} /* xattr_set */

static int
xattr_list(
    struct vfs_daos_state      *s,
    dfs_obj_t                  *obj,
    struct chimera_vfs_request *r)
{
    daos_size_t need = 0;
    int         rc   = check_access(s, obj, r->cred, CHIMERA_ACE_READ_DATA);

    r->list_xattrs.r_len    = 0;
    r->list_xattrs.r_count  = 0;
    r->list_xattrs.r_eof    = 0;
    r->list_xattrs.r_cookie = 0;
    if (rc) {
        return rc;
    }
    if (r->list_xattrs.cookie) {
        r->list_xattrs.r_eof = 1;
        return 0;
    }
    rc = dfs_listxattr(s->dfs, obj, NULL, &need);
    if (!rc) {
        rc = xattr_size(need, r->list_xattrs.max_bytes);
    }
    if (!rc && need) {
        daos_size_t got = r->list_xattrs.max_bytes;
        rc = dfs_listxattr(s->dfs, obj, r->list_xattrs.buffer, &got);
        if (!rc && got != need) {
            rc = EIO;
        }
    }
    if (!rc) {
        const char *p   = r->list_xattrs.buffer;
        const char *end = p + need;
        r->list_xattrs.r_len = need;
        r->list_xattrs.r_eof = 1;
        while (p < end) {
            const char *nul = memchr(p, 0, end - p);
            if (!nul) {
                return EIO;
            }
            r->list_xattrs.r_count++;
            p = nul + 1;
        }
    }
    return rc;
} /* xattr_list */

static int
xattr_remove(
    struct vfs_daos_state      *s,
    dfs_obj_t                  *obj,
    struct chimera_vfs_request *r)
{
    char name[DFS_MAX_XATTR_NAME + 1];
    int  rc = xattr_name(r->remove_xattr.name, r->remove_xattr.namelen, name);

    r->remove_xattr.r_pre_attr.va_set_mask  = 0;
    r->remove_xattr.r_post_attr.va_set_mask = 0;
    if (!rc && s->config.read_only) {
        rc = EROFS;
    }
    if (!rc) {
        rc = check_access(s, obj, r->cred, CHIMERA_ACE_WRITE_DATA);
    }
    if (!rc) {
        post_attrs(s, obj, &r->remove_xattr.r_pre_attr);
        rc = dfs_removexattr(s->dfs, obj, name);
        if (rc == ENOENT) {
            rc = ENODATA;
        }
    }
    if (!rc) {
        post_attrs(s, obj, &r->remove_xattr.r_post_attr);
    }
    return rc;
} /* xattr_remove */

static int
operation(
    struct vfs_daos_state      *s,
    struct chimera_vfs_request *r)
{
    dfs_obj_t                      *obj = NULL, *child = NULL;
    struct chimera_vfs_open_handle *handle = NULL;
    char                            name[CHIMERA_VFS_NAME_MAX];
    int                             rc = 0;

    /* Excluded opcodes are answered even on a failed connection. */
    switch (r->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            return mount_fs(s, r);
        case CHIMERA_VFS_OP_CLOSE:
            return close_open(s, r->close.vfs_private);
        case CHIMERA_VFS_OP_UMOUNT:
            if (r->umount.mount_private != s) {
                return ESTALE;
            }
            return vfs_daos_disconnect(s);
        case CHIMERA_VFS_OP_LOOKUP_AT:
            handle = r->lookup_at.handle;
            break;
        case CHIMERA_VFS_OP_GETATTR:
            handle = r->getattr.handle;
            break;
        case CHIMERA_VFS_OP_SETATTR:
            handle = r->setattr.handle;
            break;
        case CHIMERA_VFS_OP_READDIR:
            handle = r->readdir.handle;
            break;
        case CHIMERA_VFS_OP_READLINK:
            handle = r->readlink.handle;
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            handle = r->open_at.handle;
            break;
        case CHIMERA_VFS_OP_READ:
            handle = r->read.handle;
            break;
        case CHIMERA_VFS_OP_WRITE:
            handle = r->write.handle;
            break;
        case CHIMERA_VFS_OP_COMMIT:
            handle = r->commit.handle;
            break;
        case CHIMERA_VFS_OP_REMOVE_AT:
            handle = r->remove_at.handle;
            break;
        case CHIMERA_VFS_OP_MKDIR_AT:
            handle = r->mkdir_at.handle;
            break;
        case CHIMERA_VFS_OP_MKNOD_AT:
            handle = r->mknod_at.handle;
            break;
        case CHIMERA_VFS_OP_SYMLINK_AT:
            handle = r->symlink_at.handle;
            break;
        case CHIMERA_VFS_OP_ALLOCATE:
            handle = r->allocate.handle;
            break;
        case CHIMERA_VFS_OP_GET_XATTR:
            handle = r->get_xattr.handle;
            break;
        case CHIMERA_VFS_OP_SET_XATTR:
            handle = r->set_xattr.handle;
            break;
        case CHIMERA_VFS_OP_LIST_XATTRS:
            handle = r->list_xattrs.handle;
            break;
        case CHIMERA_VFS_OP_REMOVE_XATTR:
            handle = r->remove_xattr.handle;
            break;
        case CHIMERA_VFS_OP_OPEN_FH:
        case CHIMERA_VFS_OP_RENAME_AT:
            break;
        default:
            return ENOTSUP;
    } /* switch */
    if (!s->active || !s->dfs || s->unavailable || s->draining) {
        return EIO;
    }
    if (r->mount_private != s) {
        return ESTALE;
    }
    if (handle && handle->vfs_private) {
        obj = handle_object(s, handle);
        if (!obj) {
            return EBADF;
        }
        rc = vfs_daos_validate(s, obj);
        if (rc) {
            return rc;
        }
    } else if (r->opcode != CHIMERA_VFS_OP_OPEN_FH &&
               r->opcode != CHIMERA_VFS_OP_RENAME_AT) {
        rc = vfs_daos_resolve(s, handle ? handle->fh : r->fh,
                              handle ? handle->fh_len : r->fh_len, &obj);
        if (rc) {
            return rc;
        }
        /* The registry retains this object for the entire locked dispatch. */
        release_object(s, obj);
    }
    switch (r->opcode) {
        case CHIMERA_VFS_OP_LOOKUP_AT:
            r->lookup_at.r_attr.va_set_mask     = 0;
            r->lookup_at.r_dir_attr.va_set_mask = 0;
            rc                                  = name_copy(r->lookup_at.component, r->lookup_at.component_len, name);
            if (!rc) {
                rc = check_access(s, obj, r->cred, CHIMERA_ACE_EXECUTE);
            }
            if (!rc) {
                rc = lookup(s, obj, name, &child);
            }
            if (!rc) {
                r->lookup_at.r_attr.va_req_mask |= CHIMERA_VFS_ATTR_FH;
                rc                               = get_attrs(s, child, &r->lookup_at.r_attr);
                post_attrs(s, obj, &r->lookup_at.r_dir_attr);
            }
            release_object(s, child);
            return rc;
        case CHIMERA_VFS_OP_GETATTR:
            return get_attrs(s, obj, &r->getattr.r_attr);
        case CHIMERA_VFS_OP_SETATTR:
            r->setattr.r_post_attr.va_set_mask = 0;
            post_attrs(s, obj, &r->setattr.r_pre_attr);
            if (handle && handle->vfs_private && r->setattr.set_attr &&
                (r->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE)) {
                struct vfs_daos_object *o = find_object(s, handle->vfs_private);
                if (!(o->flags & CHIMERA_VFS_OPEN_PATH) &&
                    (o->flags & CHIMERA_VFS_OPEN_READ_ONLY)) {
                    return EBADF;
                }
            }
            if (s->config.read_only) {
                return EROFS;
            }
            rc = validate_setattr(r->setattr.set_attr);
            if (rc || !r->setattr.set_attr || !(r->setattr.set_attr->va_set_mask & SETTABLE)) {
                if (!rc) {
                    post_attrs(s, obj, &r->setattr.r_post_attr);
                }
                return rc;
            }
            rc = vfs_daos_dup(s, obj, O_RDWR, &child);
            if (!rc) {
                rc = set_attrs(s, child, r->setattr.set_attr, r->cred, false);
            }
            release_object(s, child);
            if (!rc) {
                post_attrs(s, obj, &r->setattr.r_post_attr);
            }
            return rc;
        case CHIMERA_VFS_OP_OPEN_FH:
            r->open_fh.r_vfs_private = 0;
            r->open_fh.r_stream      = 0;
            rc                       = open_flags(r->open_fh.flags);
            if (!rc && (r->open_fh.flags & CHIMERA_VFS_OPEN_CREATE)) {
                rc = EINVAL;
            }
            if (!rc) {
                rc = vfs_daos_resolve(s, r->fh, r->fh_len, &child);
            }
            if (!rc) {
                rc = bind_open(s, child, r->open_fh.flags, r->cred, false, &r->open_fh.r_vfs_private);
            }
            if (!rc && (r->open_fh.flags & CHIMERA_VFS_OPEN_TRUNCATE)) {
                struct chimera_vfs_attrs a      = { .va_set_mask = CHIMERA_VFS_ATTR_SIZE, .va_size = 0 };
                struct vfs_daos_object  *opened = find_object(s, r->open_fh.r_vfs_private);
                rc = set_attrs(s, opened->obj, &a, r->cred, false);
                if (rc) {
                    close_open(s, r->open_fh.r_vfs_private);
                    r->open_fh.r_vfs_private = 0;
                }
            }
            release_object(s, child);
            return rc;
        case CHIMERA_VFS_OP_OPEN_AT:
            return open_at(s, r, obj);
        case CHIMERA_VFS_OP_READ:
            return io_data(s, r, false);
        case CHIMERA_VFS_OP_WRITE:
            return io_data(s, r, true);
        case CHIMERA_VFS_OP_READDIR:
            return read_directory(s, r, obj);
        case CHIMERA_VFS_OP_REMOVE_AT:
            return remove_at(s, r, obj);
        case CHIMERA_VFS_OP_RENAME_AT:
            return rename_at(s, r);
        case CHIMERA_VFS_OP_COMMIT:
            r->commit.r_post_attr.va_set_mask = 0;
            post_attrs(s, obj, &r->commit.r_pre_attr);
            rc = sync_container(s);
            if (!rc) {
                post_attrs(s, obj, &r->commit.r_post_attr);
            }
            return rc;
        case CHIMERA_VFS_OP_READLINK: {
            daos_size_t size = 0;
            char       *target;
            r->readlink.r_target_length    = 0;
            r->readlink.r_attr.va_set_mask = 0;
            struct stat st;
            rc = dfs_ostat(s->dfs, obj, &st);
            if (rc || !S_ISLNK(st.st_mode)) {
                return rc ? rc : EINVAL;
            }
            rc = dfs_get_symlink_value(obj, NULL, &size);
            if (rc) {
                return rc;
            }
            if (!size || size > CHIMERA_VFS_PATH_MAX + 1) {
                return EOVERFLOW;
            }
            target = malloc(size);
            if (!target) {
                return ENOMEM;
            }
            daos_size_t capacity = size;
            rc = dfs_get_symlink_value(obj, target, &size);
            if (!rc && (!size || size > capacity || target[size - 1] != 0)) {
                rc = EIO;
            }
            if (!rc && size - 1 > r->readlink.target_maxlength) {
                rc = ENAMETOOLONG;
            }
            if (!rc && size > 1 && !r->readlink.r_target) {
                rc = EFAULT;
            }
            if (!rc) {
                if (size > 1) {
                    memcpy(r->readlink.r_target, target, size - 1);
                }
                r->readlink.r_target_length = size - 1;
                post_attrs(s, obj, &r->readlink.r_attr);
            }
            free(target);
            return rc;
        }
        case CHIMERA_VFS_OP_MKDIR_AT:
        case CHIMERA_VFS_OP_MKNOD_AT:
        case CHIMERA_VFS_OP_SYMLINK_AT: {
            const struct chimera_vfs_attrs *set;
            struct chimera_vfs_attrs       *attr, *pre, *post;
            mode_t                          type;
            char                            target[CHIMERA_VFS_PATH_MAX + 1];
            const char                     *value = NULL;
            if (r->opcode == CHIMERA_VFS_OP_MKDIR_AT) {
                set  = r->mkdir_at.set_attr;
                attr = &r->mkdir_at.r_attr;
                pre  = &r->mkdir_at.r_dir_pre_attr;
                post = &r->mkdir_at.r_dir_post_attr;
                type = S_IFDIR;
                rc   = mutation_name(r->mkdir_at.name, r->mkdir_at.name_len, name);
            } else if (r->opcode == CHIMERA_VFS_OP_MKNOD_AT) {
                set  = r->mknod_at.set_attr;
                attr = &r->mknod_at.r_attr;
                pre  = &r->mknod_at.r_dir_pre_attr;
                post = &r->mknod_at.r_dir_post_attr;
                type = set ? set->va_mode & S_IFMT : 0;
                rc   = type == S_IFREG ? mutation_name(r->mknod_at.name, r->mknod_at.name_len, name) : ENOTSUP;
            } else {
                set  = r->symlink_at.set_attr;
                attr = &r->symlink_at.r_attr;
                pre  = &r->symlink_at.r_dir_pre_attr;
                post = &r->symlink_at.r_dir_post_attr;
                type = S_IFLNK;
                rc   = mutation_name(r->symlink_at.name, r->symlink_at.namelen, name);
                if (!rc && (!r->symlink_at.target || r->symlink_at.targetlen <= 0 ||
                            r->symlink_at.targetlen > CHIMERA_VFS_PATH_MAX ||
                            memchr(r->symlink_at.target, 0, r->symlink_at.targetlen))) {
                    rc = EINVAL;
                }
                if (!rc) {
                    memcpy(target, r->symlink_at.target, r->symlink_at.targetlen);
                    target[r->symlink_at.targetlen] = 0;
                    value                           = target;
                }
            }
            attr->va_set_mask = pre->va_set_mask = post->va_set_mask = 0;
            if (rc) {
                return rc;
            }
            post_attrs(s, obj, pre);
            rc = create_entry(s, r, obj, name, value, type, set, &child);
            if (!rc) {
                attr->va_req_mask |= CHIMERA_VFS_ATTR_FH;
                rc                 = get_attrs(s, child, attr);
                if (rc) {
                    rollback_create(s, obj, name, child);
                }
                post_attrs(s, obj, post);
            }
            release_object(s, child);
            return rc;
        }
        case CHIMERA_VFS_OP_ALLOCATE: {
            daos_size_t size;
            uint64_t    length = r->allocate.length;
            r->allocate.r_pre_attr.va_set_mask  = 0;
            r->allocate.r_post_attr.va_set_mask = 0;
            if (r->allocate.flags != CHIMERA_VFS_ALLOCATE_DEALLOCATE) {
                return ENOTSUP;
            }
            if (s->config.read_only) {
                return EROFS;
            }
            struct vfs_daos_object *o = find_object(s, handle ? handle->vfs_private : 0);
            if (!o || (o->flags & (CHIMERA_VFS_OPEN_READ_ONLY | CHIMERA_VFS_OPEN_PATH))) {
                return EBADF;
            }
            post_attrs(s, obj, &r->allocate.r_pre_attr);
            rc = dfs_get_size(s->dfs, obj, &size);
            if (!rc && r->allocate.offset < size) {
                if (length > size - r->allocate.offset) {
                    length = size - r->allocate.offset;
                }
                if (length) {
                    rc = o->removed ? 0 : killpriv(s, obj, r->cred);
                    if (!rc) {
                        rc = dfs_punch(s->dfs, obj, r->allocate.offset, length);
                    }
                }
            }
            if (!rc) {
                post_attrs(s, obj, &r->allocate.r_post_attr);
            }
            return rc;
        }
        case CHIMERA_VFS_OP_GET_XATTR:
            return xattr_get(s, obj, r);
        case CHIMERA_VFS_OP_SET_XATTR:
            return xattr_set(s, obj, r);
        case CHIMERA_VFS_OP_LIST_XATTRS:
            return xattr_list(s, obj, r);
        case CHIMERA_VFS_OP_REMOVE_XATTR:
            return xattr_remove(s, obj, r);
        default:
            return ENOTSUP;
    } /* switch */
} /* operation */

enum chimera_vfs_error
vfs_daos_operation(
    struct vfs_daos_state      *s,
    struct chimera_vfs_request *r)
{
    int rc = validate_request(r);

    if (!rc) {
        rc = operation(s, r);
    }
    if (!rc && s->unavailable) {
        rc = EIO;
    }

    vfs_daos_unreserve(s);

    if (rc == BAD_COOKIE) {
        return CHIMERA_VFS_EBADCOOKIE;
    }
    if (rc == SYMLINK_RESULT) {
        return CHIMERA_VFS_ESYMLINK;
    }
    return dfs_errno_to_vfs(rc);
} /* vfs_daos_operation */
