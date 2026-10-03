// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * overlay: a union of two filesystems mounted elsewhere in the VFS, in the
 * manner of Linux overlayfs.
 *
 *   mount options   lowerdir=<namespace path>[,upperdir=<namespace path>]
 *
 * The lower layer is never modified.  Names resolve in the upper layer first
 * and fall through to the lower; a directory present in both is merged.
 * Modifying something that lives only in the lower layer first copies it up
 * (with its parent directories) into the upper layer.  Removing a name that
 * the lower layer provides leaves a whiteout in the upper layer, and a
 * directory created over a whiteout is opaque: it hides the lower directory
 * of the same name.  Without an upperdir the overlay is read-only.
 *
 * Whiteouts and opaque markers are ordinary empty files in the upper layer --
 * ".wh.<name>" and ".wh..wh..opq", the aufs convention -- rather than Linux's
 * 0/0 character devices and xattrs, so any backend that stores files can be
 * the upper layer.  Names beginning ".wh." are reserved: the overlay never
 * shows them and refuses to create them.
 *
 * The layers are reached only through the SDK's stacking calls (vfs_stack.h),
 * with the credential the overlay was mounted with, as Linux overlayfs acts
 * with the mounter's; the VFS core enforces access on the overlay's own objects from the
 * attributes this module reports.
 *
 * Handles are an overlay inode number under the mount's id.  The inode table
 * lives in memory, so an object keeps its handle across copy-up, but handles
 * do not survive a remount.  Renaming a directory that exists in the lower
 * layer is not supported (EXDEV, as Linux overlayfs without redirect_dir).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <uthash.h>
#include <xxhash.h>

#include "vfs/sdk/chimera_vfs_sdk.h"
#include "evpl/evpl.h"
#include "evpl/evpl_platform.h"
#include "common/macros.h"
#include "overlay.h"

#define OVL_ROOT_INO   1
#define OVL_WH         ".wh."
#define OVL_WH_LEN     4
#define OVL_OPQ        ".wh..wh..opq"
#define OVL_OPQ_LEN    12
#define OVL_IOV_MAX    64
#define OVL_COPY_CHUNK (256 * 1024)
#define OVL_MAX_DEPTH  128

#define OVL_STAT_MASK  (CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT)
#define OVL_DIR_FLAGS  (CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | \
                        CHIMERA_VFS_OPEN_DIRECTORY)
#define OVL_PATH_FLAGS (CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED)

/* ------------------------------------------------------------------------ */
/* Inode table                                                               */
/* ------------------------------------------------------------------------ */

/*
 * An object of the overlay.  Its names are the ovl_dent entries that point at
 * it -- more than one for a hard link -- and it is found from its upper-layer
 * handle too, so every name of an upper object, however it was reached, is
 * the same overlay inode.  parent/name are its first name, which copy-up and
 * getparent use.
 */
struct ovl_node {
    uint64_t       ino;
    uint64_t       parent;
    uint32_t       mode;        /* S_IFMT of the object */
    int            opaque;      /* upper directory hides the lower one */
    int            nlinks;      /* names pointing at it */
    int            dead;        /* its last name is gone (a tombstone) */
    int            upper_len;
    int            lower_len;
    uint8_t        upper[CHIMERA_VFS_FH_SIZE];
    uint8_t        lower[CHIMERA_VFS_FH_SIZE];
    int            namelen;
    char           name[CHIMERA_VFS_NAME_MAX];
    UT_hash_handle hh_ino;
    UT_hash_handle hh_upper;
};

/* A name: (directory inode, name) -> object. */
struct ovl_dent {
    int              keylen;
    uint8_t          key[sizeof(uint64_t) + CHIMERA_VFS_NAME_MAX];
    struct ovl_node *node;
    UT_hash_handle   hh;
};

/* A node as of some moment, copied out under the lock for use across the
 * asynchronous steps of an operation (the node itself may be renamed or
 * removed meanwhile). */
struct ovl_info {
    uint64_t ino;
    uint64_t parent;
    uint32_t mode;
    int      opaque;
    int      upper_len;
    int      lower_len;
    uint8_t  upper[CHIMERA_VFS_FH_SIZE];
    uint8_t  lower[CHIMERA_VFS_FH_SIZE];
    int      namelen;
    char     name[CHIMERA_VFS_NAME_MAX];
};

struct ovl_fs {
    evpl_mutex_t            lock;
    struct ovl_node        *by_ino;
    struct ovl_node        *by_upper;
    struct ovl_dent        *by_key;
    uint64_t                next_ino;
    uint8_t                 root_fh[CHIMERA_VFS_FH_SIZE];
    int                     root_fh_len;
    uint64_t                fsid;
    int                     writable;
    /* The mounter's identity, which the overlay works on its layers with,
     * as Linux overlayfs does. */
    struct chimera_vfs_cred cred;
};

struct ovl_thread {
    struct evpl *evpl;
};

/* The vfs_private of an open overlay handle: the layer handle it reads and
 * writes through. */
struct ovl_open {
    evpl_mutex_t                    lock;
    struct chimera_vfs_open_handle *backing;
    /* The lower handle a copy-up replaced.  Another request may still be
     * using it, so it is kept until the overlay handle closes. */
    struct chimera_vfs_open_handle *retired;
    uint64_t                        ino;
    int                             upper;
    unsigned int                    flags;
};

static void
ovl_key(
    uint8_t    *key,
    int        *keylen,
    uint64_t    parent,
    const char *name,
    int         namelen)
{
    memcpy(key, &parent, sizeof(parent));
    memcpy(key + sizeof(parent), name, namelen);
    *keylen = sizeof(parent) + namelen;
} /* ovl_key */

/* Caller holds fs->lock for all of the table helpers. */
static struct ovl_node *
ovl_node_get(
    struct ovl_fs *fs,
    uint64_t       ino)
{
    struct ovl_node *node;

    HASH_FIND(hh_ino, fs->by_ino, &ino, sizeof(ino), node);
    return node;
} /* ovl_node_get */

static struct ovl_dent *
ovl_dent_find(
    struct ovl_fs *fs,
    uint64_t       parent,
    const char    *name,
    int            namelen)
{
    struct ovl_dent *dent;
    uint8_t          key[sizeof(uint64_t) + CHIMERA_VFS_NAME_MAX];
    int              keylen;

    ovl_key(key, &keylen, parent, name, namelen);
    HASH_FIND(hh, fs->by_key, key, keylen, dent);
    return dent;
} /* ovl_dent_find */

static struct ovl_node *
ovl_node_child(
    struct ovl_fs *fs,
    uint64_t       parent,
    const char    *name,
    int            namelen)
{
    struct ovl_dent *dent = ovl_dent_find(fs, parent, name, namelen);

    return dent ? dent->node : NULL;
} /* ovl_node_child */

static void
ovl_index_upper(
    struct ovl_fs   *fs,
    struct ovl_node *node,
    const uint8_t   *fh,
    int              fhlen)
{
    if (node->upper_len) {
        HASH_DELETE(hh_upper, fs->by_upper, node);
    }
    memcpy(node->upper, fh, fhlen);
    node->upper_len = fhlen;
    HASH_ADD(hh_upper, fs->by_upper, upper, node->upper_len, node);
} /* ovl_index_upper */

static struct ovl_node *
ovl_node_by_upper(
    struct ovl_fs *fs,
    const uint8_t *fh,
    int            fhlen)
{
    struct ovl_node *node;

    HASH_FIND(hh_upper, fs->by_upper, fh, fhlen, node);
    return node;
} /* ovl_node_by_upper */

/* Give `node` the name (parent, name). */
static void
ovl_dent_add(
    struct ovl_fs   *fs,
    struct ovl_node *node,
    uint64_t         parent,
    const char      *name,
    int              namelen)
{
    struct ovl_dent *dent = calloc(1, sizeof(*dent));

    ovl_key(dent->key, &dent->keylen, parent, name, namelen);
    dent->node = node;
    HASH_ADD(hh, fs->by_key, key, dent->keylen, dent);

    if (node->nlinks++ == 0) {
        node->parent  = parent;
        node->namelen = namelen;
        memcpy(node->name, name, namelen);
    }
} /* ovl_dent_add */

static struct ovl_node *
ovl_node_add(
    struct ovl_fs *fs,
    uint64_t       parent,
    const char    *name,
    int            namelen)
{
    struct ovl_node *node = calloc(1, sizeof(*node));

    node->ino = fs->next_ino++;
    HASH_ADD(hh_ino, fs->by_ino, ino, sizeof(node->ino), node);
    ovl_dent_add(fs, node, parent, name, namelen);
    return node;
} /* ovl_node_add */

static void
ovl_node_free(
    struct ovl_fs   *fs,
    struct ovl_node *node)
{
    if (node->upper_len) {
        HASH_DELETE(hh_upper, fs->by_upper, node);
    }
    HASH_DELETE(hh_ino, fs->by_ino, node);
    free(node);
} /* ovl_node_free */

/* Remove a name; the object goes with its last name. */
static void
ovl_dent_remove(
    struct ovl_fs   *fs,
    struct ovl_dent *dent)
{
    struct ovl_node *node = dent->node;
    struct ovl_dent *other, *tmp;

    HASH_DELETE(hh, fs->by_key, dent);
    free(dent);

    if (--node->nlinks == 0) {
        /* Keep the type as a tombstone: an operation through a handle that
         * outlived the object answers as the backends would for a removed
         * file (ENOTDIR as a directory) or directory (ENOENT). */
        if (node->upper_len) {
            HASH_DELETE(hh_upper, fs->by_upper, node);
            node->upper_len = 0;
        }
        node->dead = 1;
        return;
    }

    /* The first name went; any remaining one will do. */
    HASH_ITER(hh, fs->by_key, other, tmp)
    {
        if (other->node == node) {
            memcpy(&node->parent, other->key, sizeof(node->parent));
            node->namelen = other->keylen - (int) sizeof(uint64_t);
            memcpy(node->name, other->key + sizeof(uint64_t), node->namelen);
            break;
        }
    }
} /* ovl_dent_remove */

static void
ovl_info_of(
    const struct ovl_node *node,
    struct ovl_info       *info)
{
    info->ino       = node->ino;
    info->parent    = node->parent;
    info->mode      = node->mode;
    info->opaque    = node->opaque;
    info->upper_len = node->upper_len;
    info->lower_len = node->lower_len;
    memcpy(info->upper, node->upper, node->upper_len);
    memcpy(info->lower, node->lower, node->lower_len);
    info->namelen = node->namelen;
    memcpy(info->name, node->name, node->namelen);
    info->name[info->namelen] = '\0';
} /* ovl_info_of */

/* Snapshot the node `ino`; returns 0, or -1 if it no longer exists. */
static int
ovl_snapshot(
    struct ovl_fs   *fs,
    uint64_t         ino,
    struct ovl_info *info)
{
    struct ovl_node *node;

    evpl_mutex_lock(&fs->lock);
    node = ovl_node_get(fs, ino);
    if (node && node->dead) {
        node = NULL;
    }
    if (node) {
        ovl_info_of(node, info);
    }
    evpl_mutex_unlock(&fs->lock);

    return node ? 0 : -1;
} /* ovl_snapshot */

/* What an operation on the removed object `ino` answers. */
static enum chimera_vfs_error
ovl_gone(
    struct ovl_fs *fs,
    uint64_t       ino)
{
    struct ovl_node       *node;
    enum chimera_vfs_error err = CHIMERA_VFS_ESTALE;

    evpl_mutex_lock(&fs->lock);
    node = ovl_node_get(fs, ino);
    if (node && node->dead) {
        err = S_ISDIR(node->mode) ? CHIMERA_VFS_ENOENT : CHIMERA_VFS_ENOTDIR;
    }
    evpl_mutex_unlock(&fs->lock);

    return err;
} /* ovl_gone */

/* Record what a lookup found for (parent, name), creating the node (or, for
 * a new name of a known upper object, the name) on first sight, and snapshot
 * it. */
static void
ovl_record(
    struct ovl_fs   *fs,
    uint64_t         parent,
    const char      *name,
    int              namelen,
    uint32_t         mode,
    const uint8_t   *upper,
    int              upper_len,
    const uint8_t   *lower,
    int              lower_len,
    int              opaque,
    struct ovl_info *info)
{
    struct ovl_dent *dent;
    struct ovl_node *node = NULL, *alias;

    evpl_mutex_lock(&fs->lock);

    dent = ovl_dent_find(fs, parent, name, namelen);

    if (dent) {
        node = dent->node;
        /* The name now denotes a different object. */
        if ((mode & S_IFMT) && ((node->mode ^ mode) & S_IFMT)) {
            ovl_dent_remove(fs, dent);
            node = NULL;
        } else if (upper_len && node->upper_len &&
                   (node->upper_len != upper_len ||
                    memcmp(node->upper, upper, upper_len) != 0)) {
            ovl_dent_remove(fs, dent);
            node = NULL;
        }
    }

    if (!node) {
        alias = upper_len ? ovl_node_by_upper(fs, upper, upper_len) : NULL;
        if (alias) {
            node = alias;
            ovl_dent_add(fs, node, parent, name, namelen);
        } else {
            node = ovl_node_add(fs, parent, name, namelen);
        }
    }

    if (mode & S_IFMT) {
        node->mode = mode & S_IFMT;
    }
    node->opaque = opaque;
    if (upper_len && (node->upper_len != upper_len ||
                      memcmp(node->upper, upper, upper_len) != 0)) {
        ovl_index_upper(fs, node, upper, upper_len);
    }
    node->lower_len = lower_len;
    if (lower_len) {
        memcpy(node->lower, lower, lower_len);
    }

    ovl_info_of(node, info);

    evpl_mutex_unlock(&fs->lock);
} /* ovl_record */

static void
ovl_forget(
    struct ovl_fs *fs,
    uint64_t       parent,
    const char    *name,
    int            namelen)
{
    struct ovl_dent *dent;

    evpl_mutex_lock(&fs->lock);
    dent = ovl_dent_find(fs, parent, name, namelen);
    if (dent) {
        ovl_dent_remove(fs, dent);
    }
    evpl_mutex_unlock(&fs->lock);
} /* ovl_forget */

/* Move the name (from_parent, from) to (to_parent, to), replacing whatever
 * that name held. */
static void
ovl_move(
    struct ovl_fs *fs,
    uint64_t       from_parent,
    const char    *from,
    int            fromlen,
    uint64_t       to_parent,
    const char    *to,
    int            tolen)
{
    struct ovl_dent *dent, *target;
    struct ovl_node *node;

    evpl_mutex_lock(&fs->lock);

    dent   = ovl_dent_find(fs, from_parent, from, fromlen);
    target = ovl_dent_find(fs, to_parent, to, tolen);

    if (target && target != dent) {
        ovl_dent_remove(fs, target);
    }

    if (dent) {
        node = dent->node;
        HASH_DELETE(hh, fs->by_key, dent);
        ovl_key(dent->key, &dent->keylen, to_parent, to, tolen);
        HASH_ADD(hh, fs->by_key, key, dent->keylen, dent);

        if (node->parent == from_parent && node->namelen == fromlen &&
            memcmp(node->name, from, fromlen) == 0) {
            node->parent  = to_parent;
            node->namelen = tolen;
            memcpy(node->name, to, tolen);
        }
    }

    evpl_mutex_unlock(&fs->lock);
} /* ovl_move */

static void
ovl_set_upper(
    struct ovl_fs *fs,
    uint64_t       ino,
    const uint8_t *fh,
    int            fhlen)
{
    struct ovl_node *node;

    evpl_mutex_lock(&fs->lock);
    node = ovl_node_get(fs, ino);
    if (node) {
        ovl_index_upper(fs, node, fh, fhlen);
    }
    evpl_mutex_unlock(&fs->lock);
} /* ovl_set_upper */

/* ------------------------------------------------------------------------ */
/* Handles and attributes                                                    */
/* ------------------------------------------------------------------------ */

static int
ovl_fh(
    struct ovl_fs *fs,
    uint64_t       ino,
    uint8_t       *out)
{
    return chimera_vfs_encode_fh_inum_parent(fs->root_fh, ino, 0, out);
} /* ovl_fh */

static int
ovl_ino(
    struct ovl_fs *fs,
    const uint8_t *fh,
    int            fhlen,
    uint64_t      *ino)
{
    uint32_t gen;

    if (fhlen <= CHIMERA_VFS_MOUNT_ID_SIZE ||
        fhlen > CHIMERA_VFS_MOUNT_ID_SIZE + CHIMERA_VARINT_UINT64_MAX_BYTES +
        CHIMERA_VARINT_UINT32_MAX_BYTES ||
        memcmp(fh, fs->root_fh, CHIMERA_VFS_MOUNT_ID_SIZE) != 0) {
        return -1;
    }

    chimera_vfs_decode_fh_inum(fh, fhlen, ino, &gen);
    return 0;
} /* ovl_ino */

/* Present a layer's attributes as the overlay object `ino`'s. */
static void
ovl_map(
    struct ovl_fs                  *fs,
    uint64_t                        ino,
    struct chimera_vfs_attrs       *out,
    const struct chimera_vfs_attrs *in)
{
    uint64_t req = out->va_req_mask;

    *out              = *in;
    out->va_req_mask  = req;
    out->va_set_mask &= ~(CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_OWNER_SID |
                          CHIMERA_VFS_ATTR_GROUP_SID);
    out->va_acl       = NULL;
    out->va_owner_sid = NULL;
    out->va_group_sid = NULL;

    out->va_fh_len    = ovl_fh(fs, ino, out->va_fh);
    out->va_set_mask |= CHIMERA_VFS_ATTR_FH;

    if (out->va_set_mask & CHIMERA_VFS_ATTR_INUM) {
        out->va_ino = ino;
    }
    if (out->va_set_mask & CHIMERA_VFS_ATTR_FSID) {
        out->va_fsid = fs->fsid;
    }
} /* ovl_map */

static void
ovl_attr_copy(
    struct chimera_vfs_attrs       *out,
    const struct chimera_vfs_attrs *in)
{
    *out              = *in;
    out->va_set_mask &= ~(CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_OWNER_SID |
                          CHIMERA_VFS_ATTR_GROUP_SID);
    out->va_acl       = NULL;
    out->va_owner_sid = NULL;
    out->va_group_sid = NULL;
} /* ovl_attr_copy */

static int
ovl_reserved(
    const char *name,
    int         namelen)
{
    return namelen >= OVL_WH_LEN && memcmp(name, OVL_WH, OVL_WH_LEN) == 0;
} /* ovl_reserved */


/* ------------------------------------------------------------------------ */
/* Operation context                                                         */
/* ------------------------------------------------------------------------ */

struct ovl_ctx;

typedef void (*ovl_k_t)(
    struct ovl_ctx *ctx);

struct ovl_ent {
    char                     name[CHIMERA_VFS_NAME_MAX];
    int                      namelen;
    int                      upper;          /* found in the upper layer */
    uint8_t                  lower[CHIMERA_VFS_FH_SIZE];
    int                      lower_len;      /* lower dir of a merged dir */
    struct chimera_vfs_attrs attr;
};

struct ovl_list {
    /* "." and ".." as the first layer to list the directory reported them */
    struct chimera_vfs_attrs dot[2];
    int                      have_dot[2];
    struct ovl_ent          *ents;
    int                      num;
    int                      max;
    char          (*wh)[CHIMERA_VFS_NAME_MAX];
    int                      num_wh;
    int                      max_wh;
    int                      opaque;
};

/*
 * Per-request state, allocated when the request arrives (it is larger than
 * the request's plugin page) and freed as it completes.  The operation is a
 * chain of layer calls; `k` is the step that the pending call continues
 * with.  The shared sub-steps (resolve a name, copy up, list a directory,
 * make a whiteout) return through `ret`.
 */
struct ovl_ctx {
    struct chimera_vfs_request     *req;
    struct ovl_fs                  *fs;
    struct ovl_thread              *thr;
    ovl_k_t                         k;
    ovl_k_t                         ret[4];
    int                             nret;
    enum chimera_vfs_error          err;

    /* Results of the last layer call */
    struct chimera_vfs_open_handle *oh;
    struct chimera_vfs_attrs        a;
    uint32_t                        count;
    uint32_t                        eof;
    int                             niov;
    uint32_t                        sync;

    /* Layer handles held across steps */
    struct chimera_vfs_open_handle *h1;
    struct chimera_vfs_open_handle *h2;

    /* Subjects */
    struct ovl_info                 dir;      /* directory operated in */
    struct ovl_info                 node;     /* object operated on */
    const char                     *name;
    int                             namelen;
    char                            whname[CHIMERA_VFS_NAME_MAX + OVL_WH_LEN];
    int                             whnamelen;

    /* resolve() */
    int                             r_found;
    int                             r_need_lower;  /* also ask the lower layer */
    int                             r_lower_any;   /* lower layer has the name */
    int                             s_lower_any;   /* ... for the source */
    int                             r_whiteout;
    int                             r_opaque;
    uint32_t                        r_mode;
    uint8_t                         r_upper[CHIMERA_VFS_FH_SIZE];
    int                             r_upper_len;
    uint8_t                         r_lower[CHIMERA_VFS_FH_SIZE];
    int                             r_lower_len;
    struct chimera_vfs_attrs        r_attr;

    /* copyup() */
    uint64_t                        c_chain[OVL_MAX_DEPTH];
    int                             c_depth;
    struct ovl_info                 c_node;
    struct ovl_info                 c_parent;
    struct chimera_vfs_attrs        c_attr;
    struct chimera_vfs_open_handle *c_hl;     /* lower object */
    struct chimera_vfs_open_handle *c_hp;     /* upper parent */
    struct chimera_vfs_open_handle *c_hu;     /* upper copy (regular file) */
    uint8_t                         c_fh[CHIMERA_VFS_FH_SIZE];
    int                             c_fhlen;
    uint64_t                        c_offset;
    struct evpl_iovec               c_iov[OVL_IOV_MAX];
    char                           *c_target;

    /* list() */
    struct ovl_list                 list;
    uint64_t                        l_cookie;
    int                             l_pass;

    /* Per-operation scratch */
    struct chimera_vfs_attrs        set_attr;
    struct ovl_info                 dir1;     /* source directory */
    struct ovl_info                 dir2;     /* destination directory */
    char                            tmpname[CHIMERA_VFS_NAME_MAX + OVL_WH_LEN];
    int                             t_whiteout;
    struct ovl_info                 src;      /* rename/link source */
    struct ovl_open                *ov;
    int                             t_found;
    int                             t_lower;
    uint32_t                        t_mode;
    int                             i;
    int                             flag;
};

static const struct chimera_vfs_cred *
ovl_cred(const struct ovl_ctx *ctx)
{
    return &ctx->fs->cred;
} /* ovl_cred */

/* The credential an operation the caller asked for is made with in the upper
 * layer: creating, linking, renaming or removing a name there takes the
 * caller's identity, so ownership and the backend's rules (setgid
 * directories, sticky directories) come out as they would without the
 * overlay.  Copy-up and the overlay's own bookkeeping use ovl_cred(ctx). */
static const struct chimera_vfs_cred *
ovl_ccred(const struct ovl_ctx *ctx)
{
    return ctx->req->cred ? ctx->req->cred : ovl_cred(ctx);
} /* ovl_ccred */


static void
ovl_call(
    struct ovl_ctx *ctx,
    ovl_k_t         sub,
    ovl_k_t         ret)
{
    ctx->ret[ctx->nret++] = ret;
    sub(ctx);
} /* ovl_call */

static void
ovl_return(struct ovl_ctx *ctx)
{
    ovl_k_t ret = ctx->ret[--ctx->nret];

    ret(ctx);
} /* ovl_return */

static void
ovl_release(
    struct ovl_ctx                  *ctx,
    struct chimera_vfs_open_handle **h)
{
    if (*h) {
        chimera_vfs_release_handle(ctx->req->thread, *h);
        *h = NULL;
    }
} /* ovl_release */

static void
ovl_list_free(struct ovl_list *list)
{
    free(list->ents);
    free(list->wh);
    memset(list, 0, sizeof(*list));
} /* ovl_list_free */

/* Finish the overlay request with `status`, dropping anything still held. */
static void
ovl_finish(
    struct ovl_ctx        *ctx,
    enum chimera_vfs_error status)
{
    struct chimera_vfs_request *req = ctx->req;

    ovl_release(ctx, &ctx->h1);
    ovl_release(ctx, &ctx->h2);
    ovl_release(ctx, &ctx->c_hl);
    ovl_release(ctx, &ctx->c_hp);
    ovl_release(ctx, &ctx->c_hu);
    ovl_list_free(&ctx->list);
    free(ctx->c_target);
    free(ctx);

    req->status = status;
    req->complete(req);
} /* ovl_finish */

/* ---- layer call callbacks: record the result and continue ---------------- */

static void
ovl_cb_open(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct ovl_ctx *ctx = private_data;

    ctx->err = error_code;
    ctx->oh  = error_code == CHIMERA_VFS_OK ? oh : NULL;
    ctx->k(ctx);
} /* ovl_cb_open */

static void
ovl_cb_lookup_at(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct ovl_ctx *ctx = private_data;

    (void) dir_attr;
    ctx->err = error_code;
    if (error_code == CHIMERA_VFS_OK) {
        ovl_attr_copy(&ctx->a, attr);
    }
    ctx->k(ctx);
} /* ovl_cb_lookup_at */

static void
ovl_cb_lookup(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    ovl_cb_lookup_at(error_code, attr, NULL, private_data);
} /* ovl_cb_lookup */

static void
ovl_cb_getattr(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    ovl_cb_lookup_at(error_code, attr, NULL, private_data);
} /* ovl_cb_getattr */

static void
ovl_cb_setattr(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    (void) pre_attr;
    (void) set_attr;
    ovl_cb_lookup_at(error_code, post_attr, NULL, private_data);
} /* ovl_cb_setattr */

static void
ovl_cb_setattr_status(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct ovl_ctx *ctx = private_data;

    (void) pre_attr;
    (void) set_attr;
    (void) post_attr;
    ctx->err = error_code;
    ctx->k(ctx);
} /* ovl_cb_setattr_status */

/* allocate's callback shape (pre, post), keeping the post attributes. */
static void
ovl_cb_setattr_like(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct ovl_ctx *ctx = private_data;

    (void) pre_attr;
    ctx->err = error_code;
    if (error_code == CHIMERA_VFS_OK && post_attr) {
        ovl_attr_copy(&ctx->a, post_attr);
    } else {
        ctx->a.va_set_mask = 0;
    }
    ctx->k(ctx);
} /* ovl_cb_setattr_like */

static void
ovl_cb_create(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    (void) set_attr;
    (void) dir_pre_attr;
    (void) dir_post_attr;
    ovl_cb_lookup_at(error_code, attr, NULL, private_data);
} /* ovl_cb_create */

static void
ovl_cb_symlink(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    (void) dir_pre_attr;
    (void) dir_post_attr;
    ovl_cb_lookup_at(error_code, attr, NULL, private_data);
} /* ovl_cb_symlink */

static void
ovl_cb_open_at(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct ovl_ctx *ctx = private_data;

    (void) set_attr;
    (void) dir_pre_attr;
    (void) dir_post_attr;
    ctx->err = error_code;
    ctx->oh  = error_code == CHIMERA_VFS_OK ? oh : NULL;
    if (error_code == CHIMERA_VFS_OK) {
        ovl_attr_copy(&ctx->a, attr);
    }
    ctx->k(ctx);
} /* ovl_cb_open_at */

static void
ovl_cb_status(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct ovl_ctx *ctx = private_data;

    (void) pre_attr;
    (void) post_attr;
    ctx->err = error_code;
    ctx->k(ctx);
} /* ovl_cb_status */

static void
ovl_cb_rename(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data)
{
    (void) fromdir_pre_attr;
    (void) fromdir_post_attr;
    (void) todir_pre_attr;
    (void) todir_post_attr;
    ovl_cb_status(error_code, NULL, NULL, private_data);
} /* ovl_cb_rename */

static void
ovl_cb_link(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    (void) dir_pre_attr;
    (void) dir_post_attr;
    ovl_cb_lookup_at(error_code, attr, NULL, private_data);
} /* ovl_cb_link */

static void
ovl_cb_read(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct ovl_ctx *ctx = private_data;

    (void) iov;
    ctx->err = error_code;
    if (error_code == CHIMERA_VFS_OK) {
        ctx->count = count;
        ctx->eof   = eof;
        ctx->niov  = niov;
        if (attr) {
            ovl_attr_copy(&ctx->a, attr);
        } else {
            ctx->a.va_set_mask = 0;
        }
    }
    ctx->k(ctx);
} /* ovl_cb_read */

static void
ovl_cb_write(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct ovl_ctx *ctx = private_data;

    (void) pre_attr;
    ctx->err = error_code;
    if (error_code == CHIMERA_VFS_OK) {
        ctx->count = length;
        ctx->sync  = sync;
        if (post_attr) {
            ovl_attr_copy(&ctx->a, post_attr);
        } else {
            ctx->a.va_set_mask = 0;
        }
    }
    ctx->k(ctx);
} /* ovl_cb_write */

static void
ovl_cb_readlink(
    enum chimera_vfs_error    error_code,
    int                       targetlen,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct ovl_ctx *ctx = private_data;

    (void) attr;
    ctx->err   = error_code;
    ctx->count = error_code == CHIMERA_VFS_OK ? targetlen : 0;
    ctx->k(ctx);
} /* ovl_cb_readlink */

/* ---- small layer steps ---------------------------------------------------- */

static void
ovl_open_fh(
    struct ovl_ctx *ctx,
    const uint8_t  *fh,
    int             fhlen,
    unsigned int    flags,
    ovl_k_t         k)
{
    ctx->k = k;
    chimera_vfs_open_fh(ctx->req->thread, ovl_cred(ctx), fh, fhlen, flags,
                        ovl_cb_open, ctx);
} /* ovl_open_fh */

static void
ovl_lookup_at(
    struct ovl_ctx                 *ctx,
    struct chimera_vfs_open_handle *dir,
    const char                     *name,
    int                             namelen,
    ovl_k_t                         k)
{
    ctx->k = k;
    chimera_vfs_lookup_at(ctx->req->thread, ovl_cred(ctx), dir, name, namelen,
                          OVL_STAT_MASK, 0, ovl_cb_lookup_at, ctx);
} /* ovl_lookup_at */

/* ------------------------------------------------------------------------ */
/* resolve(): what (ctx->dir, ctx->name) denotes                             */
/* ------------------------------------------------------------------------ */

static void ovl_resolve_lower(
    struct ovl_ctx *ctx);

static void
ovl_resolve_done(struct ovl_ctx *ctx)
{
    ovl_release(ctx, &ctx->h1);
    ovl_return(ctx);
} /* ovl_resolve_done */

static void
ovl_resolve_lower_found(struct ovl_ctx *ctx)
{
    if (ctx->err == CHIMERA_VFS_OK) {
        ctx->r_lower_any = 1;
        if (!ctx->r_found) {
            ctx->r_found = 1;
            ctx->r_mode  = ctx->a.va_mode;
            ctx->r_attr  = ctx->a;
            memcpy(ctx->r_lower, ctx->a.va_fh, ctx->a.va_fh_len);
            ctx->r_lower_len = ctx->a.va_fh_len;
        } else if (S_ISDIR(ctx->a.va_mode) && S_ISDIR(ctx->r_mode) && !ctx->r_opaque) {
            /* The lower directory merged under the upper one. */
            memcpy(ctx->r_lower, ctx->a.va_fh, ctx->a.va_fh_len);
            ctx->r_lower_len = ctx->a.va_fh_len;
        }
    } else if (ctx->err != CHIMERA_VFS_ENOENT) {
        ovl_release(ctx, &ctx->h1);
        ctx->r_found = -1;
        ovl_return(ctx);
        return;
    }

    ovl_resolve_done(ctx);
} /* ovl_resolve_lower_found */

static void
ovl_resolve_lower_opened(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ctx->r_found = -1;
        ovl_return(ctx);
        return;
    }

    ctx->h1 = ctx->oh;
    ovl_lookup_at(ctx, ctx->h1, ctx->name, ctx->namelen, ovl_resolve_lower_found);
} /* ovl_resolve_lower_opened */

static void
ovl_resolve_lower(struct ovl_ctx *ctx)
{
    ovl_release(ctx, &ctx->h1);

    /* The lower layer shows through unless the directory is opaque or has no
     * lower part, or the name is already an upper non-directory or opaque
     * directory -- though removing the name must still learn whether the
     * lower layer has it, to hide it with a whiteout. */
    if (!ctx->dir.lower_len || ctx->dir.opaque ||
        (ctx->r_found && (!S_ISDIR(ctx->r_mode) || ctx->r_opaque) &&
         !ctx->r_need_lower)) {
        ovl_resolve_done(ctx);
        return;
    }

    ovl_open_fh(ctx, ctx->dir.lower, ctx->dir.lower_len, OVL_DIR_FLAGS,
                ovl_resolve_lower_opened);
} /* ovl_resolve_lower */

static void
ovl_resolve_opaque_checked(struct ovl_ctx *ctx)
{
    ctx->r_opaque = (ctx->err == CHIMERA_VFS_OK);
    ovl_resolve_lower(ctx);
} /* ovl_resolve_opaque_checked */

static void
ovl_resolve_child_opened(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ctx->r_found = -1;
        ovl_return(ctx);
        return;
    }

    ctx->h1 = ctx->oh;
    ovl_lookup_at(ctx, ctx->h1, OVL_OPQ, OVL_OPQ_LEN, ovl_resolve_opaque_checked);
} /* ovl_resolve_child_opened */

static void
ovl_resolve_whiteout_checked(struct ovl_ctx *ctx)
{
    if (ctx->err == CHIMERA_VFS_OK) {
        ctx->r_whiteout = 1;
        ovl_resolve_done(ctx);
        return;
    }

    ovl_resolve_lower(ctx);
} /* ovl_resolve_whiteout_checked */

static void
ovl_resolve_upper_found(struct ovl_ctx *ctx)
{
    if (ctx->err == CHIMERA_VFS_OK) {
        ctx->r_found = 1;
        ctx->r_mode  = ctx->a.va_mode;
        ctx->r_attr  = ctx->a;
        memcpy(ctx->r_upper, ctx->a.va_fh, ctx->a.va_fh_len);
        ctx->r_upper_len = ctx->a.va_fh_len;

        if (!S_ISDIR(ctx->r_mode)) {
            ovl_resolve_lower(ctx);
            return;
        }

        /* An upper directory hides its lower namesake when opaque. */
        ovl_release(ctx, &ctx->h1);
        ovl_open_fh(ctx, ctx->r_upper, ctx->r_upper_len, OVL_DIR_FLAGS,
                    ovl_resolve_child_opened);
        return;
    }

    if (ctx->err != CHIMERA_VFS_ENOENT) {
        ovl_release(ctx, &ctx->h1);
        ctx->r_found = -1;
        ovl_return(ctx);
        return;
    }

    ovl_lookup_at(ctx, ctx->h1, ctx->whname, ctx->whnamelen,
                  ovl_resolve_whiteout_checked);
} /* ovl_resolve_upper_found */

static void
ovl_resolve_upper_opened(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ctx->r_found = -1;
        ovl_return(ctx);
        return;
    }

    ctx->h1 = ctx->oh;
    ovl_lookup_at(ctx, ctx->h1, ctx->name, ctx->namelen, ovl_resolve_upper_found);
} /* ovl_resolve_upper_opened */

/*
 * Resolve ctx->name in ctx->dir.  On return r_found is 1 (and the r_ fields
 * and ctx->node describe the object), 0 (no such name; r_whiteout says a
 * whiteout hides a lower one), or -1 (a layer failed; ctx->err).
 */
static void
ovl_resolve(struct ovl_ctx *ctx)
{
    ctx->r_found     = 0;
    ctx->r_lower_any = 0;
    ctx->r_whiteout  = 0;
    ctx->r_opaque    = 0;
    ctx->r_upper_len = 0;
    ctx->r_lower_len = 0;
    ctx->whnamelen   = snprintf(ctx->whname, sizeof(ctx->whname), "%s%.*s",
                                OVL_WH, ctx->namelen, ctx->name);

    if (ctx->dir.upper_len) {
        ovl_open_fh(ctx, ctx->dir.upper, ctx->dir.upper_len, OVL_DIR_FLAGS,
                    ovl_resolve_upper_opened);
    } else {
        ovl_resolve_lower(ctx);
    }
} /* ovl_resolve */

/* After resolve(): enter what it found into the inode table as ctx->node. */
static void
ovl_resolve_record(struct ovl_ctx *ctx)
{
    ovl_record(ctx->fs, ctx->dir.ino, ctx->name, ctx->namelen, ctx->r_mode,
               ctx->r_upper, ctx->r_upper_len, ctx->r_lower, ctx->r_lower_len,
               ctx->r_upper_len ? (ctx->r_opaque || !ctx->r_lower_len) : 0,
               &ctx->node);
} /* ovl_resolve_record */

/* ------------------------------------------------------------------------ */
/* whiteout(): create ".wh.<name>" in ctx->dir's upper directory             */
/* ------------------------------------------------------------------------ */

static void
ovl_whiteout_created(struct ovl_ctx *ctx)
{
    if (ctx->oh) {
        chimera_vfs_release_handle(ctx->req->thread, ctx->oh);
        ctx->oh = NULL;
    }
    ovl_release(ctx, &ctx->h1);
    if (ctx->err == CHIMERA_VFS_EEXIST) {
        ctx->err = CHIMERA_VFS_OK;
    }
    ovl_return(ctx);
} /* ovl_whiteout_created */

static void
ovl_whiteout_dir_opened(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_return(ctx);
        return;
    }

    ctx->h1                   = ctx->oh;
    ctx->set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    ctx->set_attr.va_mode = S_IFREG;
    ctx->set_attr.va_uid  = 0;
    ctx->set_attr.va_gid  = 0;
    ctx->k                = ovl_whiteout_created;
    chimera_vfs_open_at(ctx->req->thread, ovl_cred(ctx), ctx->h1, ctx->whname,
                        ctx->whnamelen, CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_INFERRED,
                        &ctx->set_attr, OVL_STAT_MASK, 0, 0, ovl_cb_open_at, ctx);
} /* ovl_whiteout_dir_opened */

/* Needs ctx->dir to have an upper directory and ctx->whname set. */
static void
ovl_whiteout(struct ovl_ctx *ctx)
{
    ovl_open_fh(ctx, ctx->dir.upper, ctx->dir.upper_len, OVL_DIR_FLAGS,
                ovl_whiteout_dir_opened);
} /* ovl_whiteout */

/* unwhiteout(): remove ".wh.<name>" from ctx->dir's upper directory, if any. */

static void
ovl_unwhiteout_removed(struct ovl_ctx *ctx)
{
    ovl_release(ctx, &ctx->h1);
    ctx->err = (ctx->err == CHIMERA_VFS_ENOENT) ? CHIMERA_VFS_OK : ctx->err;
    ovl_return(ctx);
} /* ovl_unwhiteout_removed */

static void
ovl_unwhiteout_dir_opened(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_return(ctx);
        return;
    }

    ctx->h1 = ctx->oh;
    ctx->k  = ovl_unwhiteout_removed;
    chimera_vfs_remove_at(ctx->req->thread, ovl_cred(ctx), ctx->h1, ctx->whname,
                          ctx->whnamelen, NULL, 0, 0, 0, 0, NULL, ovl_cb_status, ctx);
} /* ovl_unwhiteout_dir_opened */

static void
ovl_unwhiteout(struct ovl_ctx *ctx)
{
    ovl_open_fh(ctx, ctx->dir.upper, ctx->dir.upper_len, OVL_DIR_FLAGS,
                ovl_unwhiteout_dir_opened);
} /* ovl_unwhiteout */

/* ------------------------------------------------------------------------ */
/* copyup(): give ctx->node (and its ancestors) an upper-layer object        */
/* ------------------------------------------------------------------------ */

static void ovl_copyup_next(
    struct ovl_ctx *ctx);

static void
ovl_copyup_drop(struct ovl_ctx *ctx)
{
    if (ctx->niov) {
        evpl_iovecs_release(ctx->thr->evpl, ctx->c_iov, ctx->niov);
        ctx->niov = 0;
    }
    ovl_release(ctx, &ctx->c_hl);
    ovl_release(ctx, &ctx->c_hp);
    ovl_release(ctx, &ctx->c_hu);
    free(ctx->c_target);
    ctx->c_target = NULL;
} /* ovl_copyup_drop */

static void
ovl_copyup_fail(
    struct ovl_ctx        *ctx,
    enum chimera_vfs_error err)
{
    ovl_copyup_drop(ctx);
    ctx->err = err;
    ovl_return(ctx);
} /* ovl_copyup_fail */

/* The copy of c_node now exists in the upper layer as c_fh. */
static void
ovl_copyup_made(struct ovl_ctx *ctx)
{
    ovl_copyup_drop(ctx);
    ovl_set_upper(ctx->fs, ctx->c_node.ino, ctx->c_fh, ctx->c_fhlen);
    ctx->c_depth--;
    ovl_copyup_next(ctx);
} /* ovl_copyup_made */

static void
ovl_copyup_exists_found(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_copyup_fail(ctx, ctx->err);
        return;
    }
    memcpy(ctx->c_fh, ctx->a.va_fh, ctx->a.va_fh_len);
    ctx->c_fhlen = ctx->a.va_fh_len;
    ovl_copyup_made(ctx);
} /* ovl_copyup_exists_found */

/* Another request copied it up first (the create said EEXIST): use theirs. */
static void
ovl_copyup_exists(struct ovl_ctx *ctx)
{
    ovl_lookup_at(ctx, ctx->c_hp, ctx->c_node.name, ctx->c_node.namelen,
                  ovl_copyup_exists_found);
} /* ovl_copyup_exists */

/* Record the created object's handle, or divert to an existing copy. */
static int
ovl_copyup_created(struct ovl_ctx *ctx)
{
    if (ctx->err == CHIMERA_VFS_EEXIST) {
        ovl_copyup_exists(ctx);
        return -1;
    }
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_copyup_fail(ctx, ctx->err);
        return -1;
    }
    memcpy(ctx->c_fh, ctx->a.va_fh, ctx->a.va_fh_len);
    ctx->c_fhlen = ctx->a.va_fh_len;
    return 0;
} /* ovl_copyup_created */

static void
ovl_copyup_simple_done(struct ovl_ctx *ctx)
{
    if (ovl_copyup_created(ctx) == 0) {
        ovl_copyup_made(ctx);
    }
} /* ovl_copyup_simple_done */

/* ---- regular files ---- */

static void
ovl_copyup_file_meta_set(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_copyup_fail(ctx, ctx->err);
        return;
    }
    ovl_copyup_made(ctx);
} /* ovl_copyup_file_meta_set */

/* The data is in; the lower file's times and final mode go on last. */
static void
ovl_copyup_file_meta(struct ovl_ctx *ctx)
{
    struct chimera_vfs_attrs *set = &ctx->set_attr;

    memset(set, 0, sizeof(*set));
    set->va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_ATIME |
        CHIMERA_VFS_ATTR_MTIME;
    set->va_mode  = ctx->c_attr.va_mode & ~S_IFMT;
    set->va_atime = ctx->c_attr.va_atime;
    set->va_mtime = ctx->c_attr.va_mtime;

    ctx->k = ovl_copyup_file_meta_set;
    chimera_vfs_setattr(ctx->req->thread, ovl_cred(ctx), ctx->c_hu, set,
                        0, 0, ovl_cb_setattr, ctx);
} /* ovl_copyup_file_meta */

static void ovl_copyup_file_read(
    struct ovl_ctx *ctx);

static void
ovl_copyup_file_written(struct ovl_ctx *ctx)
{
    evpl_iovecs_release(ctx->thr->evpl, ctx->c_iov, ctx->niov);
    ctx->niov = 0;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_copyup_fail(ctx, ctx->err);
        return;
    }
    ovl_copyup_file_read(ctx);
} /* ovl_copyup_file_written */

static void
ovl_copyup_file_got(struct ovl_ctx *ctx)
{
    uint64_t offset = ctx->c_offset;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_copyup_fail(ctx, ctx->err);
        return;
    }

    if (ctx->count == 0) {
        evpl_iovecs_release(ctx->thr->evpl, ctx->c_iov, ctx->niov);
        ctx->niov = 0;
        ovl_copyup_file_meta(ctx);
        return;
    }

    ctx->c_offset += ctx->count;
    ctx->k         = ovl_copyup_file_written;
    chimera_vfs_write(ctx->req->thread, ovl_cred(ctx), ctx->c_hu, offset, ctx->count,
                      0, 0, 0, ctx->c_iov, ctx->niov, ovl_cb_write, ctx);
} /* ovl_copyup_file_got */

static void
ovl_copyup_file_read(struct ovl_ctx *ctx)
{
    if (ctx->c_offset >= ctx->c_attr.va_size) {
        ovl_copyup_file_meta(ctx);
        return;
    }

    ctx->niov = 0;
    ctx->k    = ovl_copyup_file_got;
    chimera_vfs_read(ctx->req->thread, ovl_cred(ctx), ctx->c_hl, ctx->c_offset,
                     OVL_COPY_CHUNK, ctx->c_iov, OVL_IOV_MAX, 0, ovl_cb_read, ctx);
} /* ovl_copyup_file_read */

static void
ovl_copyup_file_created(struct ovl_ctx *ctx)
{
    struct chimera_vfs_open_handle *oh = ctx->oh;

    if (ovl_copyup_created(ctx) != 0) {
        return;
    }

    ctx->c_hu     = oh;
    ctx->c_offset = 0;
    ovl_copyup_file_read(ctx);
} /* ovl_copyup_file_created */

/* ---- symbolic links ---- */

static void
ovl_copyup_link_read(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_copyup_fail(ctx, ctx->err);
        return;
    }

    ctx->set_attr.va_set_mask = CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
    ctx->set_attr.va_uid      = ctx->c_attr.va_uid;
    ctx->set_attr.va_gid      = ctx->c_attr.va_gid;
    ctx->k                    = ovl_copyup_simple_done;
    chimera_vfs_symlink_at(ctx->req->thread, ovl_cred(ctx), ctx->c_hp,
                           ctx->c_node.name, ctx->c_node.namelen,
                           ctx->c_target, ctx->count, &ctx->set_attr,
                           OVL_STAT_MASK, 0, 0, ovl_cb_symlink, ctx);
} /* ovl_copyup_link_read */

/* ---- dispatch on type, once the upper parent is open ---- */

static void
ovl_copyup_parent_opened(struct ovl_ctx *ctx)
{
    struct chimera_vfs_attrs *set  = &ctx->set_attr;
    uint32_t                  type = ctx->c_attr.va_mode & S_IFMT;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_copyup_fail(ctx, ctx->err);
        return;
    }

    ctx->c_hp = ctx->oh;

    memset(set, 0, sizeof(*set));
    set->va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    set->va_mode = ctx->c_attr.va_mode & ~S_IFMT;
    set->va_uid  = ctx->c_attr.va_uid;
    set->va_gid  = ctx->c_attr.va_gid;

    switch (type) {
        case S_IFDIR:
            ctx->k = ovl_copyup_simple_done;
            chimera_vfs_mkdir_at(ctx->req->thread, ovl_cred(ctx), ctx->c_hp,
                                 ctx->c_node.name, ctx->c_node.namelen, set,
                                 OVL_STAT_MASK, 0, 0, ovl_cb_create, ctx);
            break;
        case S_IFLNK:
            ctx->c_target = malloc(CHIMERA_VFS_PATH_MAX);
            ctx->k        = ovl_copyup_link_read;
            chimera_vfs_readlink(ctx->req->thread, ovl_cred(ctx), ctx->c_hl,
                                 ctx->c_target, CHIMERA_VFS_PATH_MAX, 0,
                                 ovl_cb_readlink, ctx);
            break;
        case S_IFREG:
            /* Owner-writable while the data goes in; the final mode follows. */
            set->va_mode |= S_IWUSR | S_IRUSR;
            ctx->k        = ovl_copyup_file_created;
            chimera_vfs_open_at(ctx->req->thread, ovl_cred(ctx), ctx->c_hp,
                                ctx->c_node.name, ctx->c_node.namelen,
                                CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE,
                                set, OVL_STAT_MASK, 0, 0, ovl_cb_open_at, ctx);
            break;
        default:
            set->va_mode      = ctx->c_attr.va_mode;
            set->va_rdev      = ctx->c_attr.va_rdev;
            set->va_set_mask |= CHIMERA_VFS_ATTR_RDEV;
            ctx->k            = ovl_copyup_simple_done;
            chimera_vfs_mknod_at(ctx->req->thread, ovl_cred(ctx), ctx->c_hp,
                                 ctx->c_node.name, ctx->c_node.namelen, set,
                                 OVL_STAT_MASK, 0, 0, ovl_cb_create, ctx);
            break;
    } /* switch */
} /* ovl_copyup_parent_opened */

static void
ovl_copyup_lower_stat(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_copyup_fail(ctx, ctx->err);
        return;
    }

    ctx->c_attr = ctx->a;
    ovl_open_fh(ctx, ctx->c_parent.upper, ctx->c_parent.upper_len, OVL_DIR_FLAGS,
                ovl_copyup_parent_opened);
} /* ovl_copyup_lower_stat */

static void
ovl_copyup_lower_opened(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_copyup_fail(ctx, ctx->err);
        return;
    }

    ctx->c_hl = ctx->oh;
    ctx->k    = ovl_copyup_lower_stat;
    chimera_vfs_getattr(ctx->req->thread, ovl_cred(ctx), ctx->c_hl, OVL_STAT_MASK,
                        ovl_cb_getattr, ctx);
} /* ovl_copyup_lower_opened */

/* Copy up the next node of the chain, outermost first. */
static void
ovl_copyup_next(struct ovl_ctx *ctx)
{
    if (ctx->c_depth == 0) {
        /* Refresh the caller's view of the node it asked about. */
        if (ovl_snapshot(ctx->fs, ctx->node.ino, &ctx->node) != 0) {
            ctx->err = CHIMERA_VFS_ESTALE;
        } else {
            ctx->err = CHIMERA_VFS_OK;
        }
        ovl_return(ctx);
        return;
    }

    if (ovl_snapshot(ctx->fs, ctx->c_chain[ctx->c_depth - 1], &ctx->c_node) != 0 ||
        ovl_snapshot(ctx->fs, ctx->c_node.parent, &ctx->c_parent) != 0) {
        ovl_copyup_fail(ctx, CHIMERA_VFS_ESTALE);
        return;
    }

    if (ctx->c_node.upper_len) {
        /* Copied up meanwhile. */
        ctx->c_depth--;
        ovl_copyup_next(ctx);
        return;
    }

    if (!ctx->c_parent.upper_len || !ctx->c_node.lower_len) {
        ovl_copyup_fail(ctx, CHIMERA_VFS_EIO);
        return;
    }

    ovl_open_fh(ctx, ctx->c_node.lower, ctx->c_node.lower_len,
                S_ISREG(ctx->c_node.mode) ?
                (CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_READ_ONLY) :
                OVL_PATH_FLAGS,
                ovl_copyup_lower_opened);
} /* ovl_copyup_next */

/*
 * Make sure ctx->node has an upper-layer object, copying it and any ancestor
 * that lacks one up from the lower layer.  Returns with ctx->err, and
 * ctx->node refreshed.
 */
static void
ovl_copyup(struct ovl_ctx *ctx)
{
    struct ovl_node *node;
    uint64_t         ino = ctx->node.ino;

    if (!ctx->fs->writable) {
        ctx->err = CHIMERA_VFS_EROFS;
        ovl_return(ctx);
        return;
    }

    if (ctx->node.upper_len) {
        ctx->err = CHIMERA_VFS_OK;
        ovl_return(ctx);
        return;
    }

    ctx->c_depth = 0;

    evpl_mutex_lock(&ctx->fs->lock);
    for (node = ovl_node_get(ctx->fs, ino); node && !node->dead && !node->upper_len;
         node = ovl_node_get(ctx->fs, node->parent)) {
        if (ctx->c_depth == OVL_MAX_DEPTH || node->ino == OVL_ROOT_INO) {
            node = NULL;
            break;
        }
        ctx->c_chain[ctx->c_depth++] = node->ino;
    }
    evpl_mutex_unlock(&ctx->fs->lock);

    if (!node) {
        ctx->err = ctx->c_depth == OVL_MAX_DEPTH ? CHIMERA_VFS_ENAMETOOLONG :
            CHIMERA_VFS_ESTALE;
        ovl_return(ctx);
        return;
    }

    ovl_copyup_next(ctx);
} /* ovl_copyup */

/* ------------------------------------------------------------------------ */
/* list(): the merged entries of ctx->dir                                    */
/* ------------------------------------------------------------------------ */

static int
ovl_list_find(
    struct ovl_list *list,
    const char      *name,
    int              namelen)
{
    for (int i = 0; i < list->num; i++) {
        if (list->ents[i].namelen == namelen &&
            memcmp(list->ents[i].name, name, namelen) == 0) {
            return i;
        }
    }
    return -1;
} /* ovl_list_find */

static int
ovl_list_whited(
    struct ovl_list *list,
    const char      *name,
    int              namelen)
{
    for (int i = 0; i < list->num_wh; i++) {
        if ((int) strlen(list->wh[i]) == namelen &&
            memcmp(list->wh[i], name, namelen) == 0) {
            return 1;
        }
    }
    return 0;
} /* ovl_list_whited */

static int
ovl_list_entry(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct ovl_ctx  *ctx  = arg;
    struct ovl_list *list = &ctx->list;
    struct ovl_ent  *ent;
    int              i;

    (void) inum;
    ctx->l_cookie = cookie;

    if ((namelen == 1 && name[0] == '.') ||
        (namelen == 2 && name[0] == '.' && name[1] == '.')) {
        if (!list->have_dot[namelen - 1]) {
            ovl_attr_copy(&list->dot[namelen - 1], attrs);
            list->have_dot[namelen - 1] = 1;
        }
        return 0;
    }

    if (ctx->l_pass == 0) {
        /* Upper layer: markers are bookkeeping, not entries. */
        if (namelen == OVL_OPQ_LEN && memcmp(name, OVL_OPQ, OVL_OPQ_LEN) == 0) {
            list->opaque = 1;
            return 0;
        }
        if (ovl_reserved(name, namelen)) {
            if (list->num_wh == list->max_wh) {
                list->max_wh = list->max_wh ? list->max_wh * 2 : 16;
                list->wh     = realloc(list->wh, list->max_wh * sizeof(*list->wh));
            }
            snprintf(list->wh[list->num_wh++], CHIMERA_VFS_NAME_MAX, "%.*s",
                     namelen - OVL_WH_LEN, name + OVL_WH_LEN);
            return 0;
        }
    } else {
        if (ovl_reserved(name, namelen) || ovl_list_whited(list, name, namelen)) {
            return 0;
        }
        i = ovl_list_find(list, name, namelen);
        if (i >= 0) {
            /* A directory in both layers is one merged directory. */
            if (S_ISDIR(list->ents[i].attr.va_mode) && S_ISDIR(attrs->va_mode)) {
                memcpy(list->ents[i].lower, attrs->va_fh, attrs->va_fh_len);
                list->ents[i].lower_len = attrs->va_fh_len;
            }
            return 0;
        }
    }

    if (list->num == list->max) {
        list->max  = list->max ? list->max * 2 : 64;
        list->ents = realloc(list->ents, list->max * sizeof(*list->ents));
    }

    ent = &list->ents[list->num++];
    memcpy(ent->name, name, namelen);
    ent->namelen   = namelen;
    ent->upper     = (ctx->l_pass == 0);
    ent->lower_len = 0;
    ovl_attr_copy(&ent->attr, attrs);
    return 0;
} /* ovl_list_entry */

static void ovl_list_pass(
    struct ovl_ctx *ctx);

static void ovl_list_opaque_next(
    struct ovl_ctx *ctx);

/* A merged subdirectory whose upper half is opaque is not merged after all. */
static void
ovl_list_opaque_checked(struct ovl_ctx *ctx)
{
    ovl_release(ctx, &ctx->h1);
    if (ctx->err == CHIMERA_VFS_OK) {
        ctx->list.ents[ctx->i].lower_len = 0;
    }
    ctx->i++;
    ovl_list_opaque_next(ctx);
} /* ovl_list_opaque_checked */

static void
ovl_list_opaque_opened(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_return(ctx);
        return;
    }
    ctx->h1 = ctx->oh;
    ovl_lookup_at(ctx, ctx->h1, OVL_OPQ, OVL_OPQ_LEN, ovl_list_opaque_checked);
} /* ovl_list_opaque_opened */

static void
ovl_list_opaque_next(struct ovl_ctx *ctx)
{
    struct ovl_ent *ent;

    for (; ctx->i < ctx->list.num; ctx->i++) {
        ent = &ctx->list.ents[ctx->i];
        if (ent->upper && ent->lower_len) {
            ovl_open_fh(ctx, ent->attr.va_fh, ent->attr.va_fh_len, OVL_DIR_FLAGS,
                        ovl_list_opaque_opened);
            return;
        }
    }

    ctx->err = CHIMERA_VFS_OK;
    ovl_return(ctx);
} /* ovl_list_opaque_next */

static void
ovl_list_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct ovl_ctx *ctx = private_data;

    (void) handle;
    (void) verifier;
    (void) attr;

    if (error_code != CHIMERA_VFS_OK) {
        ovl_release(ctx, &ctx->h1);
        ctx->err = error_code;
        ovl_return(ctx);
        return;
    }

    if (!eof) {
        ctx->l_cookie = cookie;
        chimera_vfs_readdir(ctx->req->thread, ovl_cred(ctx), ctx->h1, OVL_STAT_MASK, 0,
                            ctx->l_cookie, 0, CHIMERA_VFS_READDIR_EMIT_DOT, NULL, 0,
                            ovl_list_entry, ovl_list_complete, ctx);
        return;
    }

    ovl_release(ctx, &ctx->h1);
    ctx->l_pass++;
    ovl_list_pass(ctx);
} /* ovl_list_complete */

static void
ovl_list_opened(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_return(ctx);
        return;
    }

    ctx->h1       = ctx->oh;
    ctx->l_cookie = 0;
    chimera_vfs_readdir(ctx->req->thread, ovl_cred(ctx), ctx->h1, OVL_STAT_MASK, 0,
                        0, 0, CHIMERA_VFS_READDIR_EMIT_DOT, NULL, 0,
                        ovl_list_entry, ovl_list_complete, ctx);
} /* ovl_list_opened */

static void
ovl_list_pass(struct ovl_ctx *ctx)
{
    if (ctx->l_pass == 0) {
        if (ctx->dir.upper_len) {
            ovl_open_fh(ctx, ctx->dir.upper, ctx->dir.upper_len, OVL_DIR_FLAGS,
                        ovl_list_opened);
            return;
        }
        ctx->l_pass = 1;
    }

    if (ctx->l_pass == 1 && ctx->dir.lower_len && !ctx->dir.opaque &&
        !ctx->list.opaque) {
        ovl_open_fh(ctx, ctx->dir.lower, ctx->dir.lower_len, OVL_DIR_FLAGS,
                    ovl_list_opened);
        return;
    }

    ctx->i = 0;
    ovl_list_opaque_next(ctx);
} /* ovl_list_pass */

/* Collect ctx->dir's merged entries into ctx->list. */
static void
ovl_list(struct ovl_ctx *ctx)
{
    ovl_list_free(&ctx->list);
    ctx->l_pass = 0;
    ovl_list_pass(ctx);
} /* ovl_list */

/* ------------------------------------------------------------------------ */
/* Operations                                                                */
/* ------------------------------------------------------------------------ */

static struct ovl_ctx *
ovl_begin(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx *ctx = calloc(1, sizeof(*ctx));

    ctx->req = request;
    ctx->thr = private_data;
    ctx->fs  = request->mount_private;
    return ctx;
} /* ovl_begin */

/* Snapshot the node the request's handle names into ctx->node.  On failure
 * ctx->err says how to answer (see ovl_gone). */
static int
ovl_subject(
    struct ovl_ctx  *ctx,
    struct ovl_info *info)
{
    uint64_t ino;

    if (!ctx->fs || ovl_ino(ctx->fs, ctx->req->fh, ctx->req->fh_len, &ino) != 0) {
        ctx->err = CHIMERA_VFS_ESTALE;
        return -1;
    }
    if (ovl_snapshot(ctx->fs, ino, info) != 0) {
        ctx->err = ovl_gone(ctx->fs, ino);
        return -1;
    }
    return 0;
} /* ovl_subject */

/* The layer handle a node is read through: upper if it has one. */
static const uint8_t *
ovl_primary(
    const struct ovl_info *info,
    int                   *fhlen)
{
    if (info->upper_len) {
        *fhlen = info->upper_len;
        return info->upper;
    }
    *fhlen = info->lower_len;
    return info->lower;
} /* ovl_primary */

static void
ovl_fail_or(
    struct ovl_ctx *ctx,
    ovl_k_t         k)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    k(ctx);
} /* ovl_fail_or */

/*
 * Bring an open overlay handle's backing up to date: once the object has
 * been copied up, reads and writes go to the upper copy.
 */
static void ovl_backing_opened(
    struct ovl_ctx *ctx);

static void
ovl_backing(
    struct ovl_ctx *ctx,
    ovl_k_t         k)
{
    struct ovl_open *ov = ctx->ov;

    ctx->ret[ctx->nret++] = k;

    if (ovl_snapshot(ctx->fs, ov->ino, &ctx->node) != 0 ||
        ov->upper || !ctx->node.upper_len) {
        ctx->err = CHIMERA_VFS_OK;
        ovl_return(ctx);
        return;
    }

    ovl_open_fh(ctx, ctx->node.upper, ctx->node.upper_len, ov->flags,
                ovl_backing_opened);
} /* ovl_backing */

static void
ovl_backing_opened(struct ovl_ctx *ctx)
{
    struct ovl_open                *ov = ctx->ov;
    struct chimera_vfs_open_handle *old;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_return(ctx);
        return;
    }

    evpl_mutex_lock(&ov->lock);
    if (ov->upper) {
        old = ctx->oh;      /* someone else swapped first */
    } else {
        old         = NULL;
        ov->retired = ov->backing;
        ov->backing = ctx->oh;
        ov->upper   = 1;
    }
    evpl_mutex_unlock(&ov->lock);

    if (old) {
        chimera_vfs_release_handle(ctx->req->thread, old);
    }
    ovl_return(ctx);
} /* ovl_backing_opened */

static struct chimera_vfs_open_handle *
ovl_backing_handle(struct ovl_open *ov)
{
    struct chimera_vfs_open_handle *h;

    evpl_mutex_lock(&ov->lock);
    h = ov->backing;
    evpl_mutex_unlock(&ov->lock);
    return h;
} /* ovl_backing_handle */

/*
 * The layer handle an operation on an overlay handle goes through.  A data
 * open carries an ovl_open (kept current by ovl_backing); an inferred path
 * open never reaches the module -- the overlay handle alone names the object
 * -- so the layer object is opened for this operation in h2 instead.
 */
static struct chimera_vfs_open_handle *
ovl_bh(struct ovl_ctx *ctx)
{
    return ctx->ov ? ovl_backing_handle(ctx->ov) : ctx->h2;
} /* ovl_bh */

static uint64_t
ovl_cur_ino(struct ovl_ctx *ctx)
{
    return ctx->ov ? ctx->ov->ino : ctx->node.ino;
} /* ovl_cur_ino */

static void
ovl_handle_opened(struct ovl_ctx *ctx)
{
    if (ctx->err == CHIMERA_VFS_OK) {
        ctx->h2 = ctx->oh;
    }
    ovl_return(ctx);
} /* ovl_handle_opened */

/* Establish ovl_bh() for `handle`, then continue with k (ctx->err set).
 * want_upper: the operation modifies the object, which has been copied up. */
static void
ovl_handle(
    struct ovl_ctx                 *ctx,
    struct chimera_vfs_open_handle *handle,
    int                             want_upper,
    ovl_k_t                         k)
{
    const uint8_t *fh;
    int            fhlen;

    ctx->ov = handle ? (struct ovl_open *) handle->vfs_private : NULL;

    if (ctx->ov) {
        ovl_backing(ctx, k);
        return;
    }

    if (ovl_subject(ctx, &ctx->node) != 0) {
        ctx->err = CHIMERA_VFS_ESTALE;
        k(ctx);
        return;
    }

    if (want_upper && !ctx->node.upper_len) {
        ctx->err = CHIMERA_VFS_EIO;
        k(ctx);
        return;
    }

    fh                    = ovl_primary(&ctx->node, &fhlen);
    ctx->ret[ctx->nret++] = k;
    ovl_open_fh(ctx, fh, fhlen, OVL_PATH_FLAGS, ovl_handle_opened);
} /* ovl_handle */

/*
 * Snapshot the node an open overlay handle is on into ctx->node.  A file
 * removed while open has no node any more but lives on through the handle,
 * as on POSIX; if its backing is already in the upper layer, stand in a node
 * with no name that operations can still use.  Returns -1 when there is
 * nothing to operate on.
 */
static int
ovl_open_node(
    struct ovl_ctx  *ctx,
    struct ovl_open *ov)
{
    if (ovl_snapshot(ctx->fs, ov->ino, &ctx->node) == 0) {
        return 0;
    }

    if (!ov->upper) {
        return -1;
    }

    memset(&ctx->node, 0, sizeof(ctx->node));
    ctx->node.ino       = ov->ino;
    ctx->node.parent    = OVL_ROOT_INO;
    ctx->node.mode      = S_IFREG;
    ctx->node.upper_len = 1;   /* marks it present; the handle is the object */
    return 0;
} /* ovl_open_node */

/*
 * The layers are written with the mounter's identity -- the core has already
 * authorized the caller against the overlay's own object, with POSIX's rules
 * for open descriptors -- so the one effect of the writer's identity a layer
 * would apply is applied here: a write, truncate or allocation by a caller
 * without privilege clears set-user-ID (and group-executable set-group-ID),
 * exactly as chimera_vfs_killpriv_mode says.  ctx->a holds the post-op
 * attributes; k continues with them.
 */
static void
ovl_killpriv_set(struct ovl_ctx *ctx)
{
    ovl_k_t k = ctx->ret[--ctx->nret];

    if (ctx->err == CHIMERA_VFS_OK) {
        ctx->a.va_mode = (ctx->a.va_mode & S_IFMT) | ctx->set_attr.va_mode;
    }
    k(ctx);
} /* ovl_killpriv_set */

static void
ovl_killpriv(
    struct ovl_ctx *ctx,
    ovl_k_t         k)
{
    uint32_t mode;

    if (ctx->err != CHIMERA_VFS_OK || !(ctx->a.va_set_mask & CHIMERA_VFS_ATTR_MODE) ||
        !S_ISREG(ctx->a.va_mode)) {
        k(ctx);
        return;
    }

    mode = chimera_vfs_killpriv_mode(ctx->req->cred, ctx->a.va_mode);
    if (mode == ctx->a.va_mode) {
        k(ctx);
        return;
    }

    memset(&ctx->set_attr, 0, sizeof(ctx->set_attr));
    ctx->set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    ctx->set_attr.va_mode     = mode & ~S_IFMT;
    ctx->ret[ctx->nret++]     = k;
    ctx->k                    = ovl_killpriv_set;
    chimera_vfs_setattr(ctx->req->thread, ovl_cred(ctx), ovl_bh(ctx), &ctx->set_attr,
                        0, 0, ovl_cb_setattr_status, ctx);
} /* ovl_killpriv */

/* ---- getattr ---- */

static void
ovl_getattr_done(struct ovl_ctx *ctx)
{
    if (ctx->err == CHIMERA_VFS_OK) {
        ovl_map(ctx->fs, ovl_cur_ino(ctx), &ctx->req->getattr.r_attr, &ctx->a);
    }
    ovl_finish(ctx, ctx->err);
} /* ovl_getattr_done */

static void
ovl_getattr_current(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->k = ovl_getattr_done;
    chimera_vfs_getattr(req->thread, ovl_cred(ctx), ovl_bh(ctx),
                        req->getattr.r_attr.va_req_mask | CHIMERA_VFS_ATTR_MASK_STAT,
                        ovl_cb_getattr, ctx);
} /* ovl_getattr_current */

static void
ovl_getattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx *ctx = ovl_begin(request, private_data);

    ovl_handle(ctx, request->getattr.handle, 0, ovl_getattr_current);
} /* ovl_getattr */

/* ---- open_fh / close ---- */

static int
ovl_wants_write(unsigned int flags)
{
    return !(flags & (CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_READ_ONLY));
} /* ovl_wants_write */

static void
ovl_open_fh_done(struct ovl_ctx *ctx)
{
    struct ovl_open *ov;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ov          = calloc(1, sizeof(*ov));
    ov->backing = ctx->oh;
    ov->ino     = ctx->node.ino;
    ov->upper   = ctx->node.upper_len != 0;
    ov->flags   = ctx->req->open_fh.flags;
    evpl_mutex_init(&ov->lock, NULL);

    ctx->req->open_fh.r_vfs_private = (uint64_t) ov;
    ovl_finish(ctx, CHIMERA_VFS_OK);
} /* ovl_open_fh_done */

static void
ovl_open_fh_ready(struct ovl_ctx *ctx)
{
    const uint8_t *fh;
    int            fhlen;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    fh = ovl_primary(&ctx->node, &fhlen);
    ovl_open_fh(ctx, fh, fhlen, ctx->req->open_fh.flags, ovl_open_fh_done);
} /* ovl_open_fh_ready */

static void
ovl_open_fh_op(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx *ctx = ovl_begin(request, private_data);

    if (ovl_subject(ctx, &ctx->node) != 0) {
        ovl_finish(ctx, CHIMERA_VFS_ESTALE);
        return;
    }

    /* Opening a lower file for writing copies it up first, as on Linux. */
    if (ovl_wants_write(request->open_fh.flags) && !ctx->node.upper_len &&
        S_ISREG(ctx->node.mode)) {
        ovl_call(ctx, ovl_copyup, ovl_open_fh_ready);
        return;
    }

    ctx->err = CHIMERA_VFS_OK;
    ovl_open_fh_ready(ctx);
} /* ovl_open_fh_op */

static void
ovl_close(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_open *ov = (struct ovl_open *) request->close.vfs_private;

    (void) private_data;

    if (ov) {
        chimera_vfs_release_handle(request->thread, ov->backing);
        if (ov->retired) {
            chimera_vfs_release_handle(request->thread, ov->retired);
        }
        evpl_mutex_destroy(&ov->lock);
        free(ov);
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* ovl_close */

/* ---- lookup_at ---- */

static void
ovl_lookup_self_stat(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    ovl_release(ctx, &ctx->h1);
    if (ctx->err == CHIMERA_VFS_OK) {
        ovl_map(ctx->fs, ctx->node.ino, &req->lookup_at.r_attr, &ctx->a);
        req->lookup_at.r_dir_attr.va_set_mask = 0;
    }
    ovl_finish(ctx, ctx->err);
} /* ovl_lookup_self_stat */

static void
ovl_lookup_self_opened(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    ctx->h1 = ctx->oh;
    ctx->k  = ovl_lookup_self_stat;
    chimera_vfs_getattr(ctx->req->thread, ovl_cred(ctx), ctx->h1,
                        ctx->req->lookup_at.r_attr.va_req_mask | OVL_STAT_MASK,
                        ovl_cb_getattr, ctx);
} /* ovl_lookup_self_opened */

static void
ovl_lookup_resolved(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->r_found < 0) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    if (!ctx->r_found) {
        ovl_finish(ctx, CHIMERA_VFS_ENOENT);
        return;
    }

    ovl_resolve_record(ctx);
    ovl_map(ctx->fs, ctx->node.ino, &req->lookup_at.r_attr, &ctx->r_attr);
    req->lookup_at.r_dir_attr.va_set_mask = 0;
    ovl_finish(ctx, CHIMERA_VFS_OK);
} /* ovl_lookup_resolved */

static void
ovl_lookup(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx *ctx     = ovl_begin(request, private_data);
    const char     *name    = request->lookup_at.component;
    int             namelen = request->lookup_at.component_len;
    const uint8_t  *fh;
    int             fhlen;

    if (ovl_subject(ctx, &ctx->dir) != 0) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    if ((namelen == 1 && name[0] == '.') ||
        (namelen == 2 && name[0] == '.' && name[1] == '.')) {
        if (namelen == 1 || ctx->dir.ino == OVL_ROOT_INO) {
            ctx->node = ctx->dir;
        } else if (ovl_snapshot(ctx->fs, ctx->dir.parent, &ctx->node) != 0) {
            ovl_finish(ctx, CHIMERA_VFS_ESTALE);
            return;
        }
        fh = ovl_primary(&ctx->node, &fhlen);
        ovl_open_fh(ctx, fh, fhlen, OVL_PATH_FLAGS, ovl_lookup_self_opened);
        return;
    }

    if (ovl_reserved(name, namelen)) {
        ovl_finish(ctx, CHIMERA_VFS_ENOENT);
        return;
    }

    ctx->name    = name;
    ctx->namelen = namelen;
    ovl_call(ctx, ovl_resolve, ovl_lookup_resolved);
} /* ovl_lookup */

/* ---- readdir ---- */

static void
ovl_readdir_listed(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req    = ctx->req;
    uint64_t                    cookie = req->readdir.cookie;
    struct chimera_vfs_attrs    attr;
    struct ovl_info             child;
    struct ovl_ent             *ent;
    int                         eof = 1;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    req->readdir.r_cookie = cookie;

    /* "." and ".." (cookies 1 and 2) when asked for, as the overlay's own. */
    if (req->readdir.flags & CHIMERA_VFS_READDIR_EMIT_DOT) {
        for (uint64_t d = cookie; d < 2; d++) {
            uint64_t dino = d == 0 ? ctx->dir.ino : ctx->dir.parent;

            if (!ctx->list.have_dot[d]) {
                continue;
            }
            attr.va_req_mask = req->readdir.attr_mask;
            ovl_map(ctx->fs, dino, &attr, &ctx->list.dot[d]);
            /* A non-zero return stops the listing after this entry, which
             * the caller has taken. */
            req->readdir.r_cookie = d + 1;
            if (req->readdir.callback(dino, d + 1, d == 0 ? "." : "..", d + 1, &attr,
                                      req->proto_private_data)) {
                eof = 0;
                goto out;
            }
        }
    }

    for (int i = cookie > 2 ? (int) (cookie - 2) : 0; i < ctx->list.num; i++) {
        uint32_t upper_len = 0, lower_len = 0;
        uint8_t *upper     = NULL, *lower = NULL;

        ent = &ctx->list.ents[i];

        if (ent->upper) {
            upper     = ent->attr.va_fh;
            upper_len = ent->attr.va_fh_len;
            lower     = ent->lower;
            lower_len = ent->lower_len;
        } else {
            lower     = ent->attr.va_fh;
            lower_len = ent->attr.va_fh_len;
        }

        ovl_record(ctx->fs, ctx->dir.ino, ent->name, ent->namelen, ent->attr.va_mode,
                   upper, upper_len, lower, lower_len,
                   ent->upper && !ent->lower_len, &child);

        attr.va_req_mask = req->readdir.attr_mask;
        ovl_map(ctx->fs, child.ino, &attr, &ent->attr);

        req->readdir.r_cookie = i + 3;
        if (req->readdir.callback(child.ino, i + 3, ent->name, ent->namelen, &attr,
                                  req->proto_private_data)) {
            eof = 0;
            break;
        }
    }

 out:
    req->readdir.r_eof                  = eof;
    req->readdir.r_verifier             = 0;
    req->readdir.r_dir_attr.va_set_mask = 0;
    ovl_finish(ctx, CHIMERA_VFS_OK);
} /* ovl_readdir_listed */

static void
ovl_readdir(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx *ctx = ovl_begin(request, private_data);

    if (ovl_subject(ctx, &ctx->dir) != 0) {
        if (ctx->err == CHIMERA_VFS_ENOENT) {
            /* A removed directory lists as empty. */
            request->readdir.r_cookie               = request->readdir.cookie;
            request->readdir.r_eof                  = 1;
            request->readdir.r_verifier             = 0;
            request->readdir.r_dir_attr.va_set_mask = 0;
            ctx->err                                = CHIMERA_VFS_OK;
        }
        ovl_finish(ctx, ctx->err);
        return;
    }

    if (!S_ISDIR(ctx->dir.mode) && ctx->dir.ino != OVL_ROOT_INO) {
        ovl_finish(ctx, CHIMERA_VFS_ENOTDIR);
        return;
    }

    ovl_call(ctx, ovl_list, ovl_readdir_listed);
} /* ovl_readdir */

/* ---- read / write / commit ---- */

static void
ovl_read_done(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err == CHIMERA_VFS_OK) {
        req->read.r_niov   = ctx->niov;
        req->read.r_length = ctx->count;
        req->read.r_eof    = ctx->eof;
        if (ctx->a.va_set_mask) {
            ovl_map(ctx->fs, ctx->ov->ino, &req->read.r_attr, &ctx->a);
        } else {
            req->read.r_attr.va_set_mask = 0;
        }
    }
    ovl_finish(ctx, ctx->err);
} /* ovl_read_done */

static void
ovl_read_current(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->k = ovl_read_done;
    chimera_vfs_read(req->thread, ovl_cred(ctx), ovl_backing_handle(ctx->ov),
                     req->read.offset, req->read.length, req->read.iov, req->read.niov,
                     req->read.attrmask, ovl_cb_read, ctx);
} /* ovl_read_current */

static void
ovl_read(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx *ctx = ovl_begin(request, private_data);

    ctx->ov = (struct ovl_open *) request->read.handle->vfs_private;
    ovl_backing(ctx, ovl_read_current);
} /* ovl_read */

static void
ovl_write_done(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err == CHIMERA_VFS_OK) {
        req->write.r_length               = ctx->count;
        req->write.r_sync                 = ctx->sync;
        req->write.r_pre_attr.va_set_mask = 0;
        if (ctx->a.va_set_mask) {
            ovl_map(ctx->fs, ctx->ov->ino, &req->write.r_post_attr, &ctx->a);
        } else {
            req->write.r_post_attr.va_set_mask = 0;
        }
    }
    ovl_finish(ctx, ctx->err);
} /* ovl_write_done */

static void
ovl_write_killpriv(struct ovl_ctx *ctx)
{
    ovl_killpriv(ctx, ovl_write_done);
} /* ovl_write_killpriv */

static void
ovl_write_current(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    if (!ctx->ov->upper) {
        ovl_finish(ctx, CHIMERA_VFS_EIO);
        return;
    }

    ctx->k = ovl_write_killpriv;
    chimera_vfs_write(req->thread, ovl_cred(ctx), ovl_backing_handle(ctx->ov),
                      req->write.offset, req->write.length, req->write.sync,
                      0, req->write.r_post_attr.va_req_mask | OVL_STAT_MASK,
                      req->write.iov, req->write.niov, ovl_cb_write, ctx);
} /* ovl_write_current */

static void
ovl_write_copied(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    ovl_backing(ctx, ovl_write_current);
} /* ovl_write_copied */

static void
ovl_write(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx *ctx = ovl_begin(request, private_data);

    ctx->ov = (struct ovl_open *) request->write.handle->vfs_private;

    if (ovl_open_node(ctx, ctx->ov) != 0) {
        ovl_finish(ctx, CHIMERA_VFS_ESTALE);
        return;
    }

    ovl_call(ctx, ovl_copyup, ovl_write_copied);
} /* ovl_write */

static void
ovl_commit_done(struct ovl_ctx *ctx)
{
    ctx->req->commit.r_pre_attr.va_set_mask  = 0;
    ctx->req->commit.r_post_attr.va_set_mask = 0;
    ovl_finish(ctx, ctx->err);
} /* ovl_commit_done */

static void
ovl_commit(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx *ctx = ovl_begin(request, private_data);

    ctx->ov = (struct ovl_open *) request->commit.handle->vfs_private;
    ctx->k  = ovl_commit_done;
    chimera_vfs_commit(request->thread, ovl_cred(ctx), ovl_backing_handle(ctx->ov),
                       request->commit.offset, request->commit.length, 0, 0,
                       ovl_cb_status, ctx);
} /* ovl_commit */

/* ---- allocate / seek ---- */

static void
ovl_allocate_done(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err == CHIMERA_VFS_OK) {
        req->allocate.r_pre_attr.va_set_mask  = 0;
        req->allocate.r_post_attr.va_set_mask = 0;
    }
    ovl_finish(ctx, ctx->err);
} /* ovl_allocate_done */

static void
ovl_allocate_killpriv(struct ovl_ctx *ctx)
{
    ovl_killpriv(ctx, ovl_allocate_done);
} /* ovl_allocate_killpriv */

static void
ovl_allocate_current(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->k = ovl_allocate_killpriv;
    chimera_vfs_allocate(req->thread, ovl_cred(ctx), ovl_bh(ctx), req->allocate.offset,
                         req->allocate.length, req->allocate.flags, 0, OVL_STAT_MASK,
                         ovl_cb_setattr_like, ctx);
} /* ovl_allocate_current */

static void
ovl_allocate_copied(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    ovl_handle(ctx, ctx->req->allocate.handle, 1, ovl_allocate_current);
} /* ovl_allocate_copied */

static void
ovl_allocate(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx  *ctx = ovl_begin(request, private_data);
    struct ovl_open *ov  = (struct ovl_open *) request->allocate.handle->vfs_private;

    if (ov ? ovl_open_node(ctx, ov) != 0 : ovl_subject(ctx, &ctx->node) != 0) {
        ovl_finish(ctx, CHIMERA_VFS_ESTALE);
        return;
    }

    ovl_call(ctx, ovl_copyup, ovl_allocate_copied);
} /* ovl_allocate */

static void
ovl_cb_seek(
    enum chimera_vfs_error error_code,
    int                    sr_eof,
    uint64_t               sr_offset,
    void                  *private_data)
{
    struct ovl_ctx *ctx = private_data;

    ctx->err = error_code;
    if (error_code == CHIMERA_VFS_OK) {
        ctx->req->seek.r_eof    = sr_eof;
        ctx->req->seek.r_offset = sr_offset;
    }
    ovl_finish(ctx, error_code);
} /* ovl_cb_seek */

static void
ovl_seek_current(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    chimera_vfs_seek(req->thread, ovl_cred(ctx), ovl_bh(ctx), req->seek.offset,
                     req->seek.what, ovl_cb_seek, ctx);
} /* ovl_seek_current */

static void
ovl_seek(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx *ctx = ovl_begin(request, private_data);

    ovl_handle(ctx, request->seek.handle, 0, ovl_seek_current);
} /* ovl_seek */

/* ---- clone_range ---- */

static void
ovl_clone_done(struct ovl_ctx *ctx)
{
    ctx->req->clone_range.r_pre_attr.va_set_mask  = 0;
    ctx->req->clone_range.r_post_attr.va_set_mask = 0;
    ovl_finish(ctx, ctx->err);
} /* ovl_clone_done */

static void
ovl_clone_dst_current(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;
    struct ovl_open            *src = (struct ovl_open *) req->clone_range.src_handle->vfs_private;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->k = ovl_clone_done;
    chimera_vfs_clone_range(req->thread, ovl_ccred(ctx), ovl_backing_handle(src),
                            req->clone_range.src_offset, ovl_backing_handle(ctx->ov),
                            req->clone_range.dst_offset, req->clone_range.length,
                            0, 0, ovl_cb_status, ctx);
} /* ovl_clone_dst_current */

static void
ovl_clone_dst_copied(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    ctx->ov = (struct ovl_open *) ctx->req->clone_range.dst_handle->vfs_private;
    ovl_backing(ctx, ovl_clone_dst_current);
} /* ovl_clone_dst_copied */

static void
ovl_clone_src_current(struct ovl_ctx *ctx)
{
    struct ovl_open *dst = (struct ovl_open *) ctx->req->clone_range.dst_handle->vfs_private;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    if (ovl_open_node(ctx, dst) != 0) {
        ovl_finish(ctx, CHIMERA_VFS_ESTALE);
        return;
    }
    ovl_call(ctx, ovl_copyup, ovl_clone_dst_copied);
} /* ovl_clone_src_current */

static void
ovl_clone_src_copied(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    ovl_backing(ctx, ovl_clone_src_current);
} /* ovl_clone_src_copied */

/*
 * Both ends must be in the upper layer to be cloned there: a clone cannot
 * span two filesystems, so a lower source is copied up first.
 */
static void
ovl_clone(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx *ctx = ovl_begin(request, private_data);

    ctx->ov = (struct ovl_open *) request->clone_range.src_handle->vfs_private;

    if (!ctx->ov || !request->clone_range.dst_handle->vfs_private ||
        ovl_open_node(ctx, ctx->ov) != 0) {
        ovl_finish(ctx, CHIMERA_VFS_ESTALE);
        return;
    }

    ovl_call(ctx, ovl_copyup, ovl_clone_src_copied);
} /* ovl_clone */

/* ---- setattr ---- */

static void
ovl_setattr_done(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err == CHIMERA_VFS_OK) {
        req->setattr.r_pre_attr.va_set_mask = 0;
        ovl_map(ctx->fs, ovl_cur_ino(ctx), &req->setattr.r_post_attr, &ctx->a);
    }
    ovl_finish(ctx, ctx->err);
} /* ovl_setattr_done */

static void
ovl_setattr_killpriv(struct ovl_ctx *ctx)
{
    ovl_killpriv(ctx, ovl_setattr_done);
} /* ovl_setattr_killpriv */

static void
ovl_setattr_current(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    /* A truncation clears the set-id bits, unless this same setattr names
     * the mode, which then wins (as in the backends). */
    ctx->k = ((req->setattr.set_attr->va_set_mask &
               (CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_MODE)) == CHIMERA_VFS_ATTR_SIZE) ?
        ovl_setattr_killpriv : ovl_setattr_done;
    chimera_vfs_setattr(req->thread, ovl_cred(ctx), ovl_bh(ctx),
                        req->setattr.set_attr, 0,
                        req->setattr.r_post_attr.va_req_mask | OVL_STAT_MASK,
                        ovl_cb_setattr, ctx);
} /* ovl_setattr_current */

static void
ovl_setattr_copied(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    ovl_handle(ctx, ctx->req->setattr.handle, 1, ovl_setattr_current);
} /* ovl_setattr_copied */

static void
ovl_setattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx  *ctx = ovl_begin(request, private_data);
    struct ovl_open *ov  = (struct ovl_open *) request->setattr.handle->vfs_private;

    if (ov ? ovl_open_node(ctx, ov) != 0 : ovl_subject(ctx, &ctx->node) != 0) {
        ovl_finish(ctx, CHIMERA_VFS_ESTALE);
        return;
    }

    ovl_call(ctx, ovl_copyup, ovl_setattr_copied);
} /* ovl_setattr */

/* ---- readlink ---- */

static void
ovl_readlink_done(struct ovl_ctx *ctx)
{
    if (ctx->err == CHIMERA_VFS_OK) {
        ctx->req->readlink.r_target_length    = ctx->count;
        ctx->req->readlink.r_attr.va_set_mask = 0;
    }
    ovl_finish(ctx, ctx->err);
} /* ovl_readlink_done */

static void
ovl_readlink_current(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->k = ovl_readlink_done;
    chimera_vfs_readlink(req->thread, ovl_cred(ctx), ovl_bh(ctx),
                         req->readlink.r_target, req->readlink.target_maxlength,
                         0, ovl_cb_readlink, ctx);
} /* ovl_readlink_current */

static void
ovl_readlink(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx *ctx = ovl_begin(request, private_data);

    ovl_handle(ctx, request->readlink.handle, 0, ovl_readlink_current);
} /* ovl_readlink */

/* ---- getparent ---- */

static void
ovl_getparent(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx *ctx = ovl_begin(request, private_data);

    if (ovl_subject(ctx, &ctx->node) != 0) {
        ovl_finish(ctx, CHIMERA_VFS_ESTALE);
        return;
    }

    request->getparent.r_parent_fh_len = ovl_fh(ctx->fs, ctx->node.parent,
                                                request->getparent.r_parent_fh);
    memcpy(request->getparent.r_name, ctx->node.name, ctx->node.namelen);
    request->getparent.r_name_len = ctx->node.namelen;
    ovl_finish(ctx, CHIMERA_VFS_OK);
} /* ovl_getparent */

/* ---- creating a name: shared preparation ---- */

static void
ovl_prepare_unwhited(struct ovl_ctx *ctx)
{
    ovl_return(ctx);
} /* ovl_prepare_unwhited */

static void
ovl_prepare_dir_up(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_return(ctx);
        return;
    }

    ctx->dir = ctx->node;

    if (ctx->r_whiteout) {
        ovl_call(ctx, ovl_unwhiteout, ovl_prepare_unwhited);
        return;
    }

    ovl_return(ctx);
} /* ovl_prepare_dir_up */

static void
ovl_prepare_resolved(struct ovl_ctx *ctx)
{
    if (ctx->r_found < 0) {
        ovl_return(ctx);
        return;
    }

    ctx->t_found = ctx->r_found;
    if (ctx->t_found) {
        ctx->err = CHIMERA_VFS_OK;
        ovl_return(ctx);
        return;
    }

    ctx->node = ctx->dir;
    ovl_call(ctx, ovl_copyup, ovl_prepare_dir_up);
} /* ovl_prepare_resolved */

/*
 * Before creating ctx->name in ctx->dir: resolve it (t_found says it already
 * exists, with the r_ fields describing it); if it does not, make sure the
 * directory exists in the upper layer (ctx->dir refreshed) and clear any
 * whiteout the name left behind.
 */
static void
ovl_prepare_create(struct ovl_ctx *ctx)
{
    if (ovl_reserved(ctx->name, ctx->namelen)) {
        ctx->err = CHIMERA_VFS_EINVAL;
        ovl_return(ctx);
        return;
    }

    if (!ctx->fs->writable) {
        ctx->err = CHIMERA_VFS_EROFS;
        ovl_return(ctx);
        return;
    }

    ovl_call(ctx, ovl_resolve, ovl_prepare_resolved);
} /* ovl_prepare_create */

/* Open ctx->dir's upper directory into h1, then continue. */
static void
ovl_open_upper_dir(
    struct ovl_ctx *ctx,
    ovl_k_t         k)
{
    ovl_open_fh(ctx, ctx->dir.upper, ctx->dir.upper_len, OVL_DIR_FLAGS, k);
} /* ovl_open_upper_dir */

/* ---- mkdir / mknod / symlink ---- */

static void
ovl_made_done(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;
    struct chimera_vfs_attrs   *r_attr;

    ovl_release(ctx, &ctx->h2);

    switch (req->opcode) {
        case CHIMERA_VFS_OP_MKDIR_AT:
            r_attr                                    = &req->mkdir_at.r_attr;
            req->mkdir_at.r_dir_pre_attr.va_set_mask  = 0;
            req->mkdir_at.r_dir_post_attr.va_set_mask = 0;
            break;
        case CHIMERA_VFS_OP_MKNOD_AT:
            r_attr                                    = &req->mknod_at.r_attr;
            req->mknod_at.r_dir_pre_attr.va_set_mask  = 0;
            req->mknod_at.r_dir_post_attr.va_set_mask = 0;
            break;
        default:
            r_attr                                      = &req->symlink_at.r_attr;
            req->symlink_at.r_dir_pre_attr.va_set_mask  = 0;
            req->symlink_at.r_dir_post_attr.va_set_mask = 0;
            break;
    } /* switch */

    ovl_map(ctx->fs, ctx->node.ino, r_attr, &ctx->r_attr);
    ovl_finish(ctx, CHIMERA_VFS_OK);
} /* ovl_made_done */

static void
ovl_made_opaque_marked(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    chimera_vfs_release_handle(ctx->req->thread, ctx->oh);
    ctx->oh = NULL;
    ovl_made_done(ctx);
} /* ovl_made_opaque_marked */

/* A directory made over a whiteout hides the lower directory it replaces. */
static void
ovl_made_opaque_dir_opened(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->h2                   = ctx->oh;
    ctx->oh                   = NULL;
    ctx->set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    ctx->set_attr.va_mode     = 0;
    ctx->k                    = ovl_made_opaque_marked;
    chimera_vfs_open_at(ctx->req->thread, ovl_cred(ctx), ctx->h2, OVL_OPQ, OVL_OPQ_LEN,
                        CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_INFERRED,
                        &ctx->set_attr, OVL_STAT_MASK, 0, 0, ovl_cb_open_at, ctx);
} /* ovl_made_opaque_dir_opened */

static void
ovl_made(struct ovl_ctx *ctx)
{
    ovl_release(ctx, &ctx->h1);

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->r_attr = ctx->a;
    ovl_record(ctx->fs, ctx->dir.ino, ctx->name, ctx->namelen, ctx->a.va_mode,
               ctx->a.va_fh, ctx->a.va_fh_len, NULL, 0, 1, &ctx->node);

    if (ctx->req->opcode == CHIMERA_VFS_OP_MKDIR_AT && ctx->r_whiteout) {
        ovl_open_fh(ctx, ctx->a.va_fh, ctx->a.va_fh_len, OVL_DIR_FLAGS,
                    ovl_made_opaque_dir_opened);
        return;
    }

    ovl_made_done(ctx);
} /* ovl_made */

static void
ovl_make_dir_opened(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->h1 = ctx->oh;
    ctx->k  = ovl_made;

    switch (req->opcode) {
        case CHIMERA_VFS_OP_MKDIR_AT:
            chimera_vfs_mkdir_at(req->thread, ovl_ccred(ctx), ctx->h1, ctx->name,
                                 ctx->namelen, &ctx->set_attr, OVL_STAT_MASK, 0, 0,
                                 ovl_cb_create, ctx);
            break;
        case CHIMERA_VFS_OP_MKNOD_AT:
            chimera_vfs_mknod_at(req->thread, ovl_ccred(ctx), ctx->h1, ctx->name,
                                 ctx->namelen, &ctx->set_attr, OVL_STAT_MASK, 0, 0,
                                 ovl_cb_create, ctx);
            break;
        default:
            chimera_vfs_symlink_at(req->thread, ovl_ccred(ctx), ctx->h1, ctx->name,
                                   ctx->namelen, req->symlink_at.target,
                                   req->symlink_at.targetlen, &ctx->set_attr,
                                   OVL_STAT_MASK, 0, 0, ovl_cb_symlink, ctx);
            break;
    } /* switch */
} /* ovl_make_dir_opened */

static void
ovl_make_prepared(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    if (ctx->t_found) {
        ovl_finish(ctx, CHIMERA_VFS_EEXIST);
        return;
    }
    ovl_open_upper_dir(ctx, ovl_make_dir_opened);
} /* ovl_make_prepared */

static void
ovl_make(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx           *ctx = ovl_begin(request, private_data);
    struct chimera_vfs_attrs *set_attr;

    if (ovl_subject(ctx, &ctx->dir) != 0) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    switch (request->opcode) {
        case CHIMERA_VFS_OP_MKDIR_AT:
            ctx->name    = request->mkdir_at.name;
            ctx->namelen = request->mkdir_at.name_len;
            set_attr     = request->mkdir_at.set_attr;
            break;
        case CHIMERA_VFS_OP_MKNOD_AT:
            ctx->name    = request->mknod_at.name;
            ctx->namelen = request->mknod_at.name_len;
            set_attr     = request->mknod_at.set_attr;
            break;
        default:
            ctx->name    = request->symlink_at.name;
            ctx->namelen = request->symlink_at.namelen;
            set_attr     = request->symlink_at.set_attr;
            break;
    } /* switch */

    if (set_attr) {
        ovl_attr_copy(&ctx->set_attr, set_attr);
    }

    ovl_call(ctx, ovl_prepare_create, ovl_make_prepared);
} /* ovl_make */

/* ---- open_at ---- */

static void
ovl_open_at_reply(
    struct ovl_ctx *ctx,
    int             created)
{
    struct chimera_vfs_request *req = ctx->req;
    struct ovl_open            *ov;

    ov          = calloc(1, sizeof(*ov));
    ov->backing = ctx->h2;
    ov->ino     = ctx->node.ino;
    ov->upper   = ctx->node.upper_len != 0;
    ov->flags   = req->open_at.flags & ~(CHIMERA_VFS_OPEN_CREATE |
                                         CHIMERA_VFS_OPEN_EXCLUSIVE |
                                         CHIMERA_VFS_OPEN_TRUNCATE);
    evpl_mutex_init(&ov->lock, NULL);
    ctx->h2 = NULL;

    req->open_at.r_vfs_private = (uint64_t) ov;
    req->open_at.r_created     = created;
    ovl_map(ctx->fs, ctx->node.ino, &req->open_at.r_attr, &ctx->r_attr);
    req->open_at.r_dir_pre_attr.va_set_mask  = 0;
    req->open_at.r_dir_post_attr.va_set_mask = 0;
    ovl_finish(ctx, CHIMERA_VFS_OK);
} /* ovl_open_at_reply */

static void
ovl_open_at_existing_stat(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    ctx->r_attr = ctx->a;
    ovl_open_at_reply(ctx, 0);
} /* ovl_open_at_existing_stat */

static void
ovl_open_at_existing_truncated(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->k = ovl_open_at_existing_stat;
    chimera_vfs_getattr(ctx->req->thread, ovl_cred(ctx), ctx->h2,
                        ctx->req->open_at.r_attr.va_req_mask | OVL_STAT_MASK,
                        ovl_cb_getattr, ctx);
} /* ovl_open_at_existing_truncated */

static void
ovl_open_at_existing_opened(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->h2 = ctx->oh;

    if ((ctx->req->open_at.flags & CHIMERA_VFS_OPEN_TRUNCATE) &&
        S_ISREG(ctx->node.mode)) {
        ctx->set_attr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
        ctx->set_attr.va_size     = 0;
        ctx->k                    = ovl_open_at_existing_truncated;
        chimera_vfs_setattr(ctx->req->thread, ovl_cred(ctx), ctx->h2, &ctx->set_attr,
                            0, 0, ovl_cb_setattr, ctx);
        return;
    }

    ctx->err = CHIMERA_VFS_OK;
    ovl_open_at_existing_truncated(ctx);
} /* ovl_open_at_existing_opened */

static void
ovl_open_at_existing_ready(struct ovl_ctx *ctx)
{
    unsigned int   flags = ctx->req->open_at.flags;
    const uint8_t *fh;
    int            fhlen;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    fh = ovl_primary(&ctx->node, &fhlen);
    ovl_open_fh(ctx, fh, fhlen,
                flags & ~(CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE |
                          CHIMERA_VFS_OPEN_TRUNCATE),
                ovl_open_at_existing_opened);
} /* ovl_open_at_existing_ready */

/* The name exists (resolved into the r_ fields): open it. */
static void
ovl_open_at_existing(struct ovl_ctx *ctx)
{
    unsigned int flags = ctx->req->open_at.flags;

    ovl_resolve_record(ctx);

    if (S_ISREG(ctx->node.mode) && !ctx->node.upper_len &&
        (ovl_wants_write(flags) || (flags & CHIMERA_VFS_OPEN_TRUNCATE))) {
        ovl_call(ctx, ovl_copyup, ovl_open_at_existing_ready);
        return;
    }

    ctx->err = CHIMERA_VFS_OK;
    ovl_open_at_existing_ready(ctx);
} /* ovl_open_at_existing */

static void
ovl_open_at_created(struct ovl_ctx *ctx)
{
    ovl_release(ctx, &ctx->h1);

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->h2     = ctx->oh;
    ctx->r_attr = ctx->a;
    ovl_record(ctx->fs, ctx->dir.ino, ctx->name, ctx->namelen, ctx->a.va_mode,
               ctx->a.va_fh, ctx->a.va_fh_len, NULL, 0, 1, &ctx->node);
    ovl_open_at_reply(ctx, 1);
} /* ovl_open_at_created */

static void
ovl_open_at_dir_opened(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->h1 = ctx->oh;
    ctx->k  = ovl_open_at_created;
    chimera_vfs_open_at(req->thread, ovl_ccred(ctx), ctx->h1, ctx->name, ctx->namelen,
                        req->open_at.flags, &ctx->set_attr,
                        req->open_at.r_attr.va_req_mask | OVL_STAT_MASK, 0, 0,
                        ovl_cb_open_at, ctx);
} /* ovl_open_at_dir_opened */

static void
ovl_open_at_prepared(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    if (ctx->t_found) {
        if (ctx->req->open_at.flags & CHIMERA_VFS_OPEN_EXCLUSIVE) {
            ovl_finish(ctx, CHIMERA_VFS_EEXIST);
            return;
        }
        ovl_open_at_existing(ctx);
        return;
    }

    ovl_open_upper_dir(ctx, ovl_open_at_dir_opened);
} /* ovl_open_at_prepared */

static void
ovl_open_at_resolved(struct ovl_ctx *ctx)
{
    if (ctx->r_found < 0) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    if (!ctx->r_found) {
        ovl_finish(ctx, CHIMERA_VFS_ENOENT);
        return;
    }
    ovl_open_at_existing(ctx);
} /* ovl_open_at_resolved */

static void
ovl_open_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx *ctx = ovl_begin(request, private_data);

    if (ovl_subject(ctx, &ctx->dir) != 0) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->name    = request->open_at.name;
    ctx->namelen = request->open_at.namelen;

    if (!(request->open_at.flags & CHIMERA_VFS_OPEN_CREATE)) {
        if (ovl_reserved(ctx->name, ctx->namelen)) {
            ovl_finish(ctx, CHIMERA_VFS_ENOENT);
            return;
        }
        ovl_call(ctx, ovl_resolve, ovl_open_at_resolved);
        return;
    }

    if (request->open_at.set_attr) {
        ovl_attr_copy(&ctx->set_attr, request->open_at.set_attr);
    }

    ovl_call(ctx, ovl_prepare_create, ovl_open_at_prepared);
} /* ovl_open_at */

/* ---- remove_at ---- */

static void
ovl_remove_done(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ovl_forget(ctx->fs, ctx->dir.ino, ctx->name, ctx->namelen);
    ctx->req->remove_at.r_dir_pre_attr.va_set_mask  = 0;
    ctx->req->remove_at.r_dir_post_attr.va_set_mask = 0;
    ctx->req->remove_at.r_removed_attr.va_set_mask  = 0;
    ovl_finish(ctx, CHIMERA_VFS_OK);
} /* ovl_remove_done */

static void
ovl_remove_parent_up(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    ctx->dir = ctx->node;
    ovl_call(ctx, ovl_whiteout, ovl_remove_done);
} /* ovl_remove_parent_up */

/* The upper object (if any) is gone; hide the lower one (if any). */
static void
ovl_remove_whiteout(struct ovl_ctx *ctx)
{
    if (!ctx->s_lower_any) {
        ctx->err = CHIMERA_VFS_OK;
        ovl_remove_done(ctx);
        return;
    }

    ctx->node = ctx->dir;
    ovl_call(ctx, ovl_copyup, ovl_remove_parent_up);
} /* ovl_remove_whiteout */

static void
ovl_remove_upper_removed(struct ovl_ctx *ctx)
{
    ovl_release(ctx, &ctx->h1);

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    ovl_remove_whiteout(ctx);
} /* ovl_remove_upper_removed */

static void
ovl_remove_upper_dir_opened(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->h1 = ctx->oh;
    ctx->k  = ovl_remove_upper_removed;
    chimera_vfs_remove_at(ctx->req->thread, ovl_ccred(ctx), ctx->h1, ctx->name,
                          ctx->namelen, NULL, 0, 0, 0, 0, NULL, ovl_cb_status, ctx);
} /* ovl_remove_upper_dir_opened */

static void ovl_remove_purge_next(
    struct ovl_ctx *ctx);

static void
ovl_remove_purged_one(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK && ctx->err != CHIMERA_VFS_ENOENT) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    ctx->i++;
    ovl_remove_purge_next(ctx);
} /* ovl_remove_purged_one */

/* An upper directory holds only markers by now; clear them so it can go. */
static void
ovl_remove_purge_next(struct ovl_ctx *ctx)
{
    int namelen;

    if (ctx->i < ctx->list.num_wh) {
        namelen = snprintf(ctx->tmpname, sizeof(ctx->tmpname), "%s%s", OVL_WH,
                           ctx->list.wh[ctx->i]);
    } else if (ctx->i == ctx->list.num_wh && ctx->list.opaque) {
        namelen = snprintf(ctx->tmpname, sizeof(ctx->tmpname), "%s", OVL_OPQ);
    } else {
        ovl_release(ctx, &ctx->h2);
        ovl_open_upper_dir(ctx, ovl_remove_upper_dir_opened);
        return;
    }

    ctx->k = ovl_remove_purged_one;
    chimera_vfs_remove_at(ctx->req->thread, ovl_cred(ctx), ctx->h2, ctx->tmpname,
                          namelen, NULL, 0, 0, 0, 0, NULL, ovl_cb_status, ctx);
} /* ovl_remove_purge_next */

static void
ovl_remove_purge_opened(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    ctx->h2 = ctx->oh;
    ctx->i  = 0;
    ovl_remove_purge_next(ctx);
} /* ovl_remove_purge_opened */

static void
ovl_remove_go(struct ovl_ctx *ctx)
{
    if (!ctx->src.upper_len) {
        ovl_remove_whiteout(ctx);
        return;
    }

    if (S_ISDIR(ctx->src.mode) && (ctx->list.num_wh || ctx->list.opaque)) {
        ovl_open_fh(ctx, ctx->src.upper, ctx->src.upper_len, OVL_DIR_FLAGS,
                    ovl_remove_purge_opened);
        return;
    }

    ovl_open_upper_dir(ctx, ovl_remove_upper_dir_opened);
} /* ovl_remove_go */

static void
ovl_remove_listed(struct ovl_ctx *ctx)
{
    ctx->dir = ctx->dir1;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    if (ctx->list.num) {
        ovl_finish(ctx, CHIMERA_VFS_ENOTEMPTY);
        return;
    }
    ovl_remove_go(ctx);
} /* ovl_remove_listed */

static void
ovl_remove_resolved(struct ovl_ctx *ctx)
{
    unsigned int flags = ctx->req->remove_at.flags;

    if (ctx->r_found < 0) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    if (!ctx->r_found) {
        ovl_finish(ctx, CHIMERA_VFS_ENOENT);
        return;
    }

    ovl_resolve_record(ctx);
    ctx->src          = ctx->node;
    ctx->s_lower_any  = ctx->r_lower_any;
    ctx->r_need_lower = 0;

    if ((flags & CHIMERA_VFS_REMOVE_ISDIR) && !S_ISDIR(ctx->src.mode)) {
        ovl_finish(ctx, CHIMERA_VFS_ENOTDIR);
        return;
    }
    if ((flags & CHIMERA_VFS_REMOVE_ISNOTDIR) && S_ISDIR(ctx->src.mode)) {
        ovl_finish(ctx, CHIMERA_VFS_EISDIR);
        return;
    }

    if (S_ISDIR(ctx->src.mode)) {
        /* Only an empty merged directory can go. */
        ctx->dir1 = ctx->dir;
        ctx->dir  = ctx->src;
        ovl_call(ctx, ovl_list, ovl_remove_listed);
        return;
    }

    ovl_remove_go(ctx);
} /* ovl_remove_resolved */

static void
ovl_remove(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx *ctx = ovl_begin(request, private_data);

    if (ovl_subject(ctx, &ctx->dir) != 0) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->name    = request->remove_at.name;
    ctx->namelen = request->remove_at.namelen;

    if (ovl_reserved(ctx->name, ctx->namelen)) {
        ovl_finish(ctx, CHIMERA_VFS_ENOENT);
        return;
    }
    if (!ctx->fs->writable) {
        ovl_finish(ctx, CHIMERA_VFS_EROFS);
        return;
    }

    ctx->r_need_lower = 1;
    ovl_call(ctx, ovl_resolve, ovl_remove_resolved);
} /* ovl_remove */

/* ---- rename_at ---- */

static void
ovl_rename_done(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ovl_move(ctx->fs, ctx->dir1.ino, req->rename_at.name, req->rename_at.namelen,
             ctx->dir2.ino, req->rename_at.new_name, req->rename_at.new_namelen);

    req->rename_at.r_fromdir_pre_attr.va_set_mask  = 0;
    req->rename_at.r_fromdir_post_attr.va_set_mask = 0;
    req->rename_at.r_todir_pre_attr.va_set_mask    = 0;
    req->rename_at.r_todir_post_attr.va_set_mask   = 0;
    ovl_finish(ctx, CHIMERA_VFS_OK);
} /* ovl_rename_done */

/* The source had a lower object, which must not show through its old name. */
static void
ovl_rename_renamed(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err != CHIMERA_VFS_OK || !ctx->s_lower_any) {
        ovl_rename_done(ctx);
        return;
    }

    ctx->dir       = ctx->dir1;
    ctx->whnamelen = snprintf(ctx->whname, sizeof(ctx->whname), "%s%.*s", OVL_WH,
                              req->rename_at.namelen, req->rename_at.name);
    ovl_call(ctx, ovl_whiteout, ovl_rename_done);
} /* ovl_rename_renamed */

static void
ovl_rename_go(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    if (ovl_snapshot(ctx->fs, ctx->dir1.ino, &ctx->dir1) != 0 ||
        ovl_snapshot(ctx->fs, ctx->src.ino, &ctx->src) != 0 ||
        !ctx->dir1.upper_len || !ctx->src.upper_len) {
        ovl_finish(ctx, CHIMERA_VFS_ESTALE);
        return;
    }

    ctx->k = ovl_rename_renamed;
    chimera_vfs_rename_at(req->thread, ovl_ccred(ctx), ctx->dir1.upper, ctx->dir1.upper_len,
                          req->rename_at.name, req->rename_at.namelen,
                          ctx->dir2.upper, ctx->dir2.upper_len,
                          req->rename_at.new_name, req->rename_at.new_namelen,
                          NULL, 0, 0, 0, 0, NULL, NULL, ovl_cb_rename, ctx);
} /* ovl_rename_go */

static void
ovl_rename_dst_up(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->dir2 = ctx->node;

    /* A whiteout at the destination gives way to the renamed object. */
    if (ctx->t_whiteout) {
        ctx->dir       = ctx->dir2;
        ctx->whnamelen = snprintf(ctx->whname, sizeof(ctx->whname), "%s%.*s", OVL_WH,
                                  req->rename_at.new_namelen, req->rename_at.new_name);
        ovl_call(ctx, ovl_unwhiteout, ovl_rename_go);
        return;
    }

    ovl_rename_go(ctx);
} /* ovl_rename_dst_up */

static void
ovl_rename_src_up(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->src = ctx->node;
    if (ovl_snapshot(ctx->fs, ctx->dir2.ino, &ctx->node) != 0) {
        ovl_finish(ctx, CHIMERA_VFS_ESTALE);
        return;
    }
    ovl_call(ctx, ovl_copyup, ovl_rename_dst_up);
} /* ovl_rename_src_up */

static void
ovl_rename_dst_resolved(struct ovl_ctx *ctx)
{
    if (ctx->r_found < 0) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->t_found    = ctx->r_found;
    ctx->t_whiteout = ctx->r_whiteout;
    ctx->t_mode     = ctx->r_mode;
    ctx->t_lower    = ctx->r_lower_len;

    /* A target in the upper layer is the upper rename's to judge, in the
     * order the backend checks things; one only the lower layer has is not
     * seen there, so judge it here. */
    if (ctx->t_found && !ctx->r_upper_len) {
        if (S_ISDIR(ctx->t_mode) && !S_ISDIR(ctx->src.mode)) {
            ovl_finish(ctx, CHIMERA_VFS_EISDIR);
            return;
        }
        if (!S_ISDIR(ctx->t_mode) && S_ISDIR(ctx->src.mode)) {
            ovl_finish(ctx, CHIMERA_VFS_ENOTDIR);
            return;
        }
        /* Replacing a directory the lower layer provides would need the
         * replaced directory's contents whited out one by one. */
        if (S_ISDIR(ctx->t_mode) && ctx->t_lower) {
            ovl_finish(ctx, CHIMERA_VFS_EXDEV);
            return;
        }
    }

    ctx->node = ctx->src;
    ovl_call(ctx, ovl_copyup, ovl_rename_src_up);
} /* ovl_rename_dst_resolved */

static void
ovl_rename_src_resolved(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->r_found < 0) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    if (!ctx->r_found) {
        ovl_finish(ctx, CHIMERA_VFS_ENOENT);
        return;
    }

    ovl_resolve_record(ctx);
    ctx->src          = ctx->node;
    ctx->s_lower_any  = ctx->r_lower_any;
    ctx->r_need_lower = 0;

    /* A directory the lower layer provides cannot move (Linux overlayfs
     * without redirect_dir); callers fall back to copying it. */
    if (S_ISDIR(ctx->src.mode) && ctx->src.lower_len) {
        ovl_finish(ctx, CHIMERA_VFS_EXDEV);
        return;
    }

    ctx->dir     = ctx->dir2;
    ctx->name    = req->rename_at.new_name;
    ctx->namelen = req->rename_at.new_namelen;
    ovl_call(ctx, ovl_resolve, ovl_rename_dst_resolved);
} /* ovl_rename_src_resolved */

static void
ovl_rename(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx *ctx = ovl_begin(request, private_data);
    uint64_t        ino;

    if (ovl_subject(ctx, &ctx->dir1) != 0) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    if (ovl_ino(ctx->fs, request->rename_at.new_fh, request->rename_at.new_fhlen,
                &ino) != 0) {
        ovl_finish(ctx, CHIMERA_VFS_ESTALE);
        return;
    }
    if (ovl_snapshot(ctx->fs, ino, &ctx->dir2) != 0) {
        ovl_finish(ctx, ovl_gone(ctx->fs, ino));
        return;
    }

    if (ovl_reserved(request->rename_at.name, request->rename_at.namelen) ||
        ovl_reserved(request->rename_at.new_name, request->rename_at.new_namelen)) {
        ovl_finish(ctx, CHIMERA_VFS_EINVAL);
        return;
    }
    if (!ctx->fs->writable) {
        ovl_finish(ctx, CHIMERA_VFS_EROFS);
        return;
    }

    ctx->dir          = ctx->dir1;
    ctx->name         = request->rename_at.name;
    ctx->namelen      = request->rename_at.namelen;
    ctx->r_need_lower = 1;
    ovl_call(ctx, ovl_resolve, ovl_rename_src_resolved);
} /* ovl_rename */

/* ---- link_at ---- */

static void
ovl_link_made(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ovl_record(ctx->fs, ctx->dir.ino, ctx->name, ctx->namelen, ctx->src.mode,
               ctx->src.upper, ctx->src.upper_len, NULL, 0, 1, &ctx->node);
    ovl_map(ctx->fs, ctx->node.ino, &req->link_at.r_attr, &ctx->a);
    req->link_at.r_replaced_attr.va_set_mask = 0;
    req->link_at.r_dir_pre_attr.va_set_mask  = 0;
    req->link_at.r_dir_post_attr.va_set_mask = 0;
    ovl_finish(ctx, CHIMERA_VFS_OK);
} /* ovl_link_made */

static void
ovl_link_src_up(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }

    ctx->src = ctx->node;
    ctx->k   = ovl_link_made;
    chimera_vfs_link_at(req->thread, ovl_ccred(ctx), ctx->src.upper, ctx->src.upper_len,
                        ctx->dir.upper, ctx->dir.upper_len, ctx->name, ctx->namelen,
                        0, OVL_STAT_MASK, 0, 0, NULL, NULL, ovl_cb_link, ctx);
} /* ovl_link_src_up */

static void
ovl_link_prepared(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_finish(ctx, ctx->err);
        return;
    }
    if (ctx->t_found) {
        ovl_finish(ctx, CHIMERA_VFS_EEXIST);
        return;
    }

    ctx->node = ctx->src;
    ovl_call(ctx, ovl_copyup, ovl_link_src_up);
} /* ovl_link_prepared */

static void
ovl_link(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx *ctx = ovl_begin(request, private_data);
    uint64_t        ino;

    if (ovl_subject(ctx, &ctx->src) != 0) {
        /* Linking a file whose last name is gone. */
        ovl_finish(ctx, ctx->err == CHIMERA_VFS_ENOTDIR ? CHIMERA_VFS_ENOENT : ctx->err);
        return;
    }
    if (ovl_ino(ctx->fs, request->link_at.dir_fh, request->link_at.dir_fhlen, &ino) != 0) {
        ovl_finish(ctx, CHIMERA_VFS_ESTALE);
        return;
    }
    if (ovl_snapshot(ctx->fs, ino, &ctx->dir) != 0) {
        ovl_finish(ctx, ovl_gone(ctx->fs, ino));
        return;
    }

    if (S_ISDIR(ctx->src.mode)) {
        ovl_finish(ctx, CHIMERA_VFS_EPERM);
        return;
    }
    if (request->link_at.replace) {
        ovl_finish(ctx, CHIMERA_VFS_ENOTSUP);
        return;
    }

    ctx->name    = request->link_at.name;
    ctx->namelen = request->link_at.namelen;
    ovl_call(ctx, ovl_prepare_create, ovl_link_prepared);
} /* ovl_link */

/* ---- mount / umount ---- */

static void
ovl_fs_free(struct ovl_fs *fs)
{
    struct ovl_node *node, *tmp;
    struct ovl_dent *dent, *dtmp;

#ifndef __clang_analyzer__ /* HASH_ITER trips the analyzer's alias checker */
    HASH_ITER(hh, fs->by_key, dent, dtmp)
    {
        HASH_DELETE(hh, fs->by_key, dent);
        free(dent);
    }
    HASH_CLEAR(hh_upper, fs->by_upper);
    HASH_ITER(hh_ino, fs->by_ino, node, tmp)
    {
        HASH_DELETE(hh_ino, fs->by_ino, node);
        free(node);
    }
#endif /* ifndef __clang_analyzer__ */
    evpl_mutex_destroy(&fs->lock);
    free(fs);
} /* ovl_fs_free */

static void
ovl_mount_fail(
    struct ovl_ctx        *ctx,
    enum chimera_vfs_error err)
{
    ovl_fs_free(ctx->fs);
    ctx->fs = NULL;
    ovl_finish(ctx, err);
} /* ovl_mount_fail */

static void
ovl_mount_stat(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;

    ovl_release(ctx, &ctx->h1);

    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_mount_fail(ctx, ctx->err);
        return;
    }

    ovl_map(ctx->fs, OVL_ROOT_INO, &req->mount.r_attr, &ctx->a);
    req->mount.r_mount_private = ctx->fs;
    ovl_finish(ctx, CHIMERA_VFS_OK);
} /* ovl_mount_stat */

static void
ovl_mount_root_opened(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        ovl_mount_fail(ctx, ctx->err);
        return;
    }

    ctx->h1 = ctx->oh;
    ctx->k  = ovl_mount_stat;
    chimera_vfs_getattr(ctx->req->thread, ovl_cred(ctx), ctx->h1,
                        ctx->req->mount.r_attr.va_req_mask | OVL_STAT_MASK,
                        ovl_cb_getattr, ctx);
} /* ovl_mount_root_opened */

static void
ovl_mount_ready(struct ovl_ctx *ctx)
{
    struct ovl_node *root = calloc(1, sizeof(*root));
    const uint8_t   *fh;
    int              fhlen;

    root->ino    = OVL_ROOT_INO;
    root->parent = OVL_ROOT_INO;
    root->mode   = S_IFDIR;
    root->nlinks = 1;
    memcpy(root->lower, ctx->r_lower, ctx->r_lower_len);
    root->lower_len = ctx->r_lower_len;
    HASH_ADD(hh_ino, ctx->fs->by_ino, ino, sizeof(root->ino), root);
    if (ctx->r_upper_len) {
        ovl_index_upper(ctx->fs, root, ctx->r_upper, ctx->r_upper_len);
    }

    ovl_info_of(root, &ctx->node);
    fh = ovl_primary(&ctx->node, &fhlen);
    ovl_open_fh(ctx, fh, fhlen, OVL_PATH_FLAGS, ovl_mount_root_opened);
} /* ovl_mount_ready */

/* A layer path resolved: record it, then resolve the next. */
static void ovl_mount_resolve(
    struct ovl_ctx *ctx);

static void
ovl_mount_resolved(struct ovl_ctx *ctx)
{
    if (ctx->err != CHIMERA_VFS_OK) {
        chimera_vfs_error("overlay: cannot resolve %sdir %s: %d",
                          ctx->i == 0 ? "lower" : "upper",
                          ctx->i == 0 ? ctx->req->mount.options.options[ctx->t_mode].value :
                          ctx->req->mount.options.options[ctx->t_lower].value,
                          ctx->err);
        ovl_mount_fail(ctx, ctx->err);
        return;
    }
    if (!(ctx->a.va_set_mask & CHIMERA_VFS_ATTR_MODE) || !S_ISDIR(ctx->a.va_mode)) {
        ovl_mount_fail(ctx, CHIMERA_VFS_ENOTDIR);
        return;
    }

    if (ctx->i == 0) {
        memcpy(ctx->r_lower, ctx->a.va_fh, ctx->a.va_fh_len);
        ctx->r_lower_len = ctx->a.va_fh_len;
    } else {
        memcpy(ctx->r_upper, ctx->a.va_fh, ctx->a.va_fh_len);
        ctx->r_upper_len = ctx->a.va_fh_len;
    }

    ctx->i++;
    ovl_mount_resolve(ctx);
} /* ovl_mount_resolved */

static void
ovl_mount_resolve(struct ovl_ctx *ctx)
{
    struct chimera_vfs_request *req = ctx->req;
    const char                 *path;
    uint8_t                     root[CHIMERA_VFS_FH_SIZE];
    uint32_t                    root_len;

    if (ctx->i == 0) {
        path = req->mount.options.options[ctx->t_mode].value;
    } else if (ctx->i == 1 && ctx->fs->writable) {
        path = req->mount.options.options[ctx->t_lower].value;
    } else {
        ovl_mount_ready(ctx);
        return;
    }

    chimera_vfs_stack_root_fh(req->thread, root, &root_len);
    ctx->k = ovl_mount_resolved;
    chimera_vfs_lookup(req->thread, ovl_cred(ctx), root, root_len, path, strlen(path),
                       OVL_STAT_MASK, CHIMERA_VFS_LOOKUP_FOLLOW, ovl_cb_lookup, ctx);
} /* ovl_mount_resolve */

static void
ovl_mount(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct ovl_ctx                   *ctx  = ovl_begin(request, private_data);
    struct chimera_vfs_mount_options *opts = &request->mount.options;
    int                               lower = -1, upper = -1;
    XXH128_hash_t                     hash;
    XXH3_state_t                     *state;
    uint8_t                           fsid[CHIMERA_VFS_FSID_SIZE];

    for (int i = 0; i < opts->num_options; i++) {
        if (!strcmp(opts->options[i].key, "lowerdir") && opts->options[i].value) {
            lower = i;
        } else if (!strcmp(opts->options[i].key, "upperdir") && opts->options[i].value) {
            upper = i;
        }
    }

    if (lower < 0) {
        chimera_vfs_error("overlay: mount needs a lowerdir= option");
        ovl_finish(ctx, CHIMERA_VFS_EINVAL);
        return;
    }

    ctx->fs              = calloc(1, sizeof(*ctx->fs));
    ctx->fs->next_ino    = OVL_ROOT_INO + 1;
    ctx->fs->writable    = upper >= 0;
    ctx->fs->cred        = request->cred ? *request->cred : *chimera_vfs_get_server_cred();
    ctx->fs->cred.origin = NULL;
    evpl_mutex_init(&ctx->fs->lock, NULL);

    /* The overlay's identity comes from its layers. */
    state = XXH3_createState();
    XXH3_128bits_reset(state);
    XXH3_128bits_update(state, "overlay", 8);
    XXH3_128bits_update(state, opts->options[lower].value,
                        strlen(opts->options[lower].value) + 1);
    if (upper >= 0) {
        XXH3_128bits_update(state, opts->options[upper].value,
                            strlen(opts->options[upper].value) + 1);
    }
    hash = XXH3_128bits_digest(state);
    XXH3_freeState(state);
    memcpy(fsid, &hash, sizeof(fsid));
    memcpy(&ctx->fs->fsid, fsid, sizeof(ctx->fs->fsid));

    ctx->fs->root_fh_len = chimera_vfs_encode_fh_inum_mount(fsid, OVL_ROOT_INO, 0,
                                                            ctx->fs->root_fh);

    ctx->t_mode  = lower;   /* option indexes, for the resolve steps */
    ctx->t_lower = upper;
    ctx->i       = 0;
    ovl_mount_resolve(ctx);
} /* ovl_mount */

static void
ovl_umount(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    (void) private_data;

    if (request->umount.mount_private) {
        ovl_fs_free(request->umount.mount_private);
    }
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* ovl_umount */

/* ------------------------------------------------------------------------ */
/* Module                                                                    */
/* ------------------------------------------------------------------------ */

static void *
ovl_init(
    const char                *cfgdata,
    struct prometheus_metrics *metrics)
{
    (void) cfgdata;
    (void) metrics;
    return calloc(1, 1);
} /* ovl_init */

static void
ovl_destroy(void *private_data)
{
    free(private_data);
} /* ovl_destroy */

static void *
ovl_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct ovl_thread *thread = calloc(1, sizeof(*thread));

    (void) private_data;
    thread->evpl = evpl;
    return thread;
} /* ovl_thread_init */

static void
ovl_thread_destroy(void *private_data)
{
    free(private_data);
} /* ovl_thread_destroy */

static void
ovl_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            ovl_mount(request, private_data);
            break;
        case CHIMERA_VFS_OP_UMOUNT:
            ovl_umount(request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            ovl_getattr(request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            ovl_setattr(request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP_AT:
            ovl_lookup(request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            ovl_readdir(request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_FH:
            ovl_open_fh_op(request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            ovl_open_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            ovl_close(request, private_data);
            break;
        case CHIMERA_VFS_OP_READ:
            ovl_read(request, private_data);
            break;
        case CHIMERA_VFS_OP_WRITE:
            ovl_write(request, private_data);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            ovl_commit(request, private_data);
            break;
        case CHIMERA_VFS_OP_ALLOCATE:
            ovl_allocate(request, private_data);
            break;
        case CHIMERA_VFS_OP_SEEK:
            ovl_seek(request, private_data);
            break;
        case CHIMERA_VFS_OP_CLONE_RANGE:
            ovl_clone(request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR_AT:
        case CHIMERA_VFS_OP_MKNOD_AT:
        case CHIMERA_VFS_OP_SYMLINK_AT:
            ovl_make(request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            ovl_readlink(request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE_AT:
            ovl_remove(request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME_AT:
            ovl_rename(request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK_AT:
            ovl_link(request, private_data);
            break;
        case CHIMERA_VFS_OP_GETPARENT:
            ovl_getparent(request, private_data);
            break;
        default:
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* ovl_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_overlay = {
    .sdk_version  = CHIMERA_VFS_SDK_VERSION,
    .fh_magic     = CHIMERA_VFS_FH_MAGIC_OVERLAY,
    .name         = "overlay",
    .capabilities = CHIMERA_VFS_CAP_FS | CHIMERA_VFS_CAP_FS_RELATIVE_OP |
        CHIMERA_VFS_CAP_READ_PROVIDES_BUFFERS | CHIMERA_VFS_CAP_CLONE_RANGE,
    .init           = ovl_init,
    .destroy        = ovl_destroy,
    .thread_init    = ovl_thread_init,
    .thread_destroy = ovl_thread_destroy,
    .dispatch       = ovl_dispatch,
};
