// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */

#include "vfs/vfs.h"
#include "vfs/vfs_internal.h"
#include "vfs/vfs_mount_table.h"
#include "vfs/vfs_name_cache.h"
#include "vfs/vfs_rootfs.h"
#include "vfs/sdk/vfs_fh.h"
#include "vfs/sdk/vfs_varint.h"
#include "common/macros.h"

/*
 * See vfs_rootfs.h.  The lock orders directory creation and removal against
 * the readers; every access to a node happens under it.
 */

#define ROOTFS_INO_ROOT      2
#define ROOTFS_COOKIE_DOT    1
#define ROOTFS_COOKIE_DOTDOT 2
#define ROOTFS_COOKIE_FIRST  3

struct rootfs_node {
    uint64_t             ino;
    struct rootfs_node  *parent;
    char                 name[CHIMERA_VFS_NAME_MAX];
    int                  namelen;
    struct rootfs_node **children;
    int                  num_children;
    int                  max_children;
    uint8_t              fh[CHIMERA_VFS_FH_SIZE];
    int                  fh_len;
    struct timespec      ctime;
};

struct rootfs {
    evpl_mutex_t         lock;
    struct rootfs_node **nodes;     /* indexed by ino - ROOTFS_INO_ROOT */
    int                  num_nodes;
    int                  max_nodes;
};

static uint8_t  rootfs_root_fh[CHIMERA_VFS_FH_SIZE];
static uint32_t rootfs_root_fh_len;

static void
rootfs_compute_root_fh(void)
{
    uint8_t fsid[CHIMERA_VFS_FSID_SIZE] = { 0 };

    rootfs_root_fh_len = chimera_vfs_encode_fh_mount(fsid, NULL, 0, rootfs_root_fh);
} /* rootfs_compute_root_fh */

void
chimera_vfs_rootfs_root_fh(
    uint8_t  *fh,
    uint32_t *fh_len)
{
    if (!rootfs_root_fh_len) {
        rootfs_compute_root_fh();
    }

    memcpy(fh, rootfs_root_fh, rootfs_root_fh_len);
    *fh_len = rootfs_root_fh_len;
} /* chimera_vfs_rootfs_root_fh */

int
chimera_vfs_rootfs_owns(
    const void *fh,
    int         fhlen)
{
    if (!rootfs_root_fh_len) {
        rootfs_compute_root_fh();
    }

    return fhlen >= CHIMERA_VFS_MOUNT_ID_SIZE &&
           memcmp(fh, rootfs_root_fh, CHIMERA_VFS_MOUNT_ID_SIZE) == 0;
} /* chimera_vfs_rootfs_owns */

static struct rootfs_node *
rootfs_node_alloc(
    struct rootfs      *fs,
    struct rootfs_node *parent,
    const char         *name,
    int                 namelen)
{
    struct rootfs_node *node = calloc(1, sizeof(*node));
    uint8_t             fragment[CHIMERA_VARINT_UINT64_MAX_BYTES];
    int                 fraglen;

    if (fs->num_nodes == fs->max_nodes) {
        fs->max_nodes = fs->max_nodes ? fs->max_nodes * 2 : 16;
        fs->nodes     = realloc(fs->nodes, fs->max_nodes * sizeof(*fs->nodes));
    }

    node->ino    = ROOTFS_INO_ROOT + fs->num_nodes;
    node->parent = parent ? parent : node;
    memcpy(node->name, name, namelen);
    node->namelen = namelen;
    chimera_vfs_realtime(&node->ctime);

    if (!parent) {
        memcpy(node->fh, rootfs_root_fh, rootfs_root_fh_len);
        node->fh_len = rootfs_root_fh_len;
    } else {
        fraglen      = chimera_encode_uint64(node->ino, fragment);
        node->fh_len = chimera_vfs_encode_fh_parent(rootfs_root_fh, fragment,
                                                    fraglen, node->fh);
    }

    fs->nodes[fs->num_nodes++] = node;

    if (parent) {
        if (parent->num_children == parent->max_children) {
            parent->max_children = parent->max_children ? parent->max_children * 2 : 8;
            parent->children     = realloc(parent->children,
                                           parent->max_children * sizeof(*parent->children));
        }
        parent->children[parent->num_children++] = node;
    }

    return node;
} /* rootfs_node_alloc */

/* Caller holds fs->lock. */
static struct rootfs_node *
rootfs_node_by_fh(
    struct rootfs *fs,
    const uint8_t *fh,
    int            fhlen)
{
    uint64_t ino;

    if (!chimera_vfs_rootfs_owns(fh, fhlen)) {
        return NULL;
    }

    if (fhlen == CHIMERA_VFS_MOUNT_ID_SIZE) {
        ino = ROOTFS_INO_ROOT;
    } else {
        if (fhlen > CHIMERA_VFS_MOUNT_ID_SIZE + CHIMERA_VARINT_UINT64_MAX_BYTES ||
            chimera_decode_uint64(fh + CHIMERA_VFS_MOUNT_ID_SIZE, &ino) !=
            fhlen - CHIMERA_VFS_MOUNT_ID_SIZE) {
            return NULL;
        }
    }

    if (ino < ROOTFS_INO_ROOT || ino - ROOTFS_INO_ROOT >= (uint64_t) fs->num_nodes) {
        return NULL;
    }

    /* NULL once removed: a stale handle to it is ESTALE.  Inode numbers are
     * never reused, so a handle cannot name a later directory. */
    return fs->nodes[ino - ROOTFS_INO_ROOT];
} /* rootfs_node_by_fh */

/* Caller holds fs->lock. */
static struct rootfs_node *
rootfs_child(
    struct rootfs_node *dir,
    const char         *name,
    int                 namelen)
{
    for (int i = 0; i < dir->num_children; i++) {
        if (dir->children[i]->namelen == namelen &&
            memcmp(dir->children[i]->name, name, namelen) == 0) {
            return dir->children[i];
        }
    }

    return NULL;
} /* rootfs_child */

/* Caller holds fs->lock. */
static void
rootfs_map_attrs(
    struct chimera_vfs_attrs *attr,
    const struct rootfs_node *node)
{
    uint64_t req = attr->va_req_mask;

    attr->va_set_mask = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT;
    memcpy(attr->va_fh, node->fh, node->fh_len);
    attr->va_fh_len = node->fh_len;

    attr->va_mode       = S_IFDIR | 0755;
    attr->va_nlink      = 2 + node->num_children;
    attr->va_uid        = 0;
    attr->va_gid        = 0;
    attr->va_size       = 4096;
    attr->va_space_used = 4096;
    attr->va_atime      = node->ctime;
    attr->va_mtime      = node->ctime;
    attr->va_ctime      = node->ctime;
    attr->va_ino        = node->ino;
    attr->va_dev        = 0;
    attr->va_rdev       = 0;

    if (req & CHIMERA_VFS_ATTR_FSID) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_FSID;
        attr->va_fsid      = 0;
    }

    if (req & CHIMERA_VFS_ATTR_MASK_STATFS_VALUES) {
        attr->va_set_mask      |= CHIMERA_VFS_ATTR_MASK_STATFS;
        attr->va_fs_space_total = 0;
        attr->va_fs_space_free  = 0;
        attr->va_fs_space_avail = 0;
        attr->va_fs_space_used  = 0;
        attr->va_fs_files_total = 0;
        attr->va_fs_files_free  = 0;
        attr->va_fs_files_avail = 0;
        attr->va_fsid           = 0;
    }
} /* rootfs_map_attrs */

static void *
rootfs_init(
    const char                *cfgdata,
    struct prometheus_metrics *metrics)
{
    struct rootfs *fs = calloc(1, sizeof(*fs));

    (void) cfgdata;
    (void) metrics;

    if (!rootfs_root_fh_len) {
        rootfs_compute_root_fh();
    }

    evpl_mutex_init(&fs->lock, NULL);
    rootfs_node_alloc(fs, NULL, "", 0);

    return fs;
} /* rootfs_init */

static void
rootfs_destroy(void *private_data)
{
    struct rootfs *fs = private_data;

    for (int i = 0; i < fs->num_nodes; i++) {
        if (fs->nodes[i]) {
            free(fs->nodes[i]->children);
            free(fs->nodes[i]);
        }
    }

    free(fs->nodes);
    evpl_mutex_destroy(&fs->lock);
    free(fs);
} /* rootfs_destroy */

static void *
rootfs_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    (void) evpl;
    return private_data;
} /* rootfs_thread_init */

static void
rootfs_thread_destroy(void *private_data)
{
    (void) private_data;
} /* rootfs_thread_destroy */

static void
rootfs_getattr(
    struct rootfs              *fs,
    struct chimera_vfs_request *request)
{
    struct rootfs_node *node;

    evpl_mutex_lock(&fs->lock);

    node = rootfs_node_by_fh(fs, request->fh, request->fh_len);

    if (node) {
        rootfs_map_attrs(&request->getattr.r_attr, node);
        request->status = CHIMERA_VFS_OK;
    } else {
        request->status = CHIMERA_VFS_ESTALE;
    }

    evpl_mutex_unlock(&fs->lock);

    request->complete(request);
} /* rootfs_getattr */

static void
rootfs_lookup_at(
    struct rootfs              *fs,
    struct chimera_vfs_request *request)
{
    struct rootfs_node *dir, *child = NULL;
    const char         *name    = request->lookup_at.component;
    int                 namelen = request->lookup_at.component_len;

    evpl_mutex_lock(&fs->lock);

    dir = rootfs_node_by_fh(fs, request->fh, request->fh_len);

    if (!dir) {
        request->status = CHIMERA_VFS_ESTALE;
    } else {
        if (namelen == 1 && name[0] == '.') {
            child = dir;
        } else if (namelen == 2 && name[0] == '.' && name[1] == '.') {
            child = dir->parent;
        } else {
            child = rootfs_child(dir, name, namelen);
        }

        if (child) {
            rootfs_map_attrs(&request->lookup_at.r_attr, child);
            rootfs_map_attrs(&request->lookup_at.r_dir_attr, dir);
            request->status = CHIMERA_VFS_OK;
        } else {
            request->status = CHIMERA_VFS_ENOENT;
        }
    }

    evpl_mutex_unlock(&fs->lock);

    request->complete(request);
} /* rootfs_lookup_at */

static void
rootfs_readdir(
    struct rootfs              *fs,
    struct chimera_vfs_request *request)
{
    struct rootfs_node      *dir;
    struct chimera_vfs_attrs attr;
    uint64_t                 cookie = request->readdir.cookie;
    uint64_t                 next   = cookie;
    int                      eof    = 1;

    evpl_mutex_lock(&fs->lock);

    dir = rootfs_node_by_fh(fs, request->fh, request->fh_len);

    if (!dir) {
        evpl_mutex_unlock(&fs->lock);
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    if ((request->readdir.flags & CHIMERA_VFS_READDIR_EMIT_DOT) &&
        cookie < ROOTFS_COOKIE_DOTDOT) {
        struct rootfs_node *dots[2]  = { dir, dir->parent };
        const char         *names[2] = { ".", ".." };

        for (int i = (int) cookie; i < 2; i++) {
            attr.va_req_mask = request->readdir.attr_mask;
            rootfs_map_attrs(&attr, dots[i]);
            /* A non-zero return stops after this entry, which the caller
             * has taken. */
            next = i + 1;
            if (request->readdir.callback(dots[i]->ino, i + 1, names[i], i + 1,
                                          &attr, request->proto_private_data)) {
                eof = 0;
                goto out;
            }
        }
    }

    for (int i = cookie > ROOTFS_COOKIE_DOTDOT ? (int) (cookie - ROOTFS_COOKIE_DOTDOT) : 0;
         i < dir->num_children; i++) {
        struct rootfs_node *child = dir->children[i];

        attr.va_req_mask = request->readdir.attr_mask;
        rootfs_map_attrs(&attr, child);

        next = ROOTFS_COOKIE_FIRST + i;
        if (request->readdir.callback(child->ino, ROOTFS_COOKIE_FIRST + i,
                                      child->name, child->namelen,
                                      &attr, request->proto_private_data)) {
            eof = 0;
            break;
        }
    }

 out:
    rootfs_map_attrs(&request->readdir.r_dir_attr, dir);

    evpl_mutex_unlock(&fs->lock);

    request->readdir.r_cookie = next;
    request->readdir.r_eof    = eof;
    request->status           = CHIMERA_VFS_OK;
    request->complete(request);
} /* rootfs_readdir */

static void
rootfs_getparent(
    struct rootfs              *fs,
    struct chimera_vfs_request *request)
{
    struct rootfs_node *node;

    evpl_mutex_lock(&fs->lock);

    node = rootfs_node_by_fh(fs, request->fh, request->fh_len);

    if (!node) {
        request->status = CHIMERA_VFS_ESTALE;
    } else {
        memcpy(request->getparent.r_parent_fh, node->parent->fh, node->parent->fh_len);
        request->getparent.r_parent_fh_len = node->parent->fh_len;
        memcpy(request->getparent.r_name, node->name, node->namelen);
        request->getparent.r_name_len = node->namelen;
        request->status               = CHIMERA_VFS_OK;
    }

    evpl_mutex_unlock(&fs->lock);

    request->complete(request);
} /* rootfs_getparent */

static void
rootfs_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct rootfs *fs = private_data;

    switch (request->opcode) {
        case CHIMERA_VFS_OP_GETATTR:
            rootfs_getattr(fs, request);
            break;
        case CHIMERA_VFS_OP_LOOKUP_AT:
            rootfs_lookup_at(fs, request);
            break;
        case CHIMERA_VFS_OP_READDIR:
            rootfs_readdir(fs, request);
            break;
        case CHIMERA_VFS_OP_GETPARENT:
            rootfs_getparent(fs, request);
            break;
        case CHIMERA_VFS_OP_OPEN_FH:
            evpl_mutex_lock(&fs->lock);
            request->status = rootfs_node_by_fh(fs, request->fh, request->fh_len) ?
                CHIMERA_VFS_OK : CHIMERA_VFS_ESTALE;
            evpl_mutex_unlock(&fs->lock);
            request->open_fh.r_vfs_private = 0;
            request->complete(request);
            break;
        case CHIMERA_VFS_OP_CLOSE:
        case CHIMERA_VFS_OP_UMOUNT:
            request->status = CHIMERA_VFS_OK;
            request->complete(request);
            break;
        case CHIMERA_VFS_OP_MOUNT:
        case CHIMERA_VFS_OP_OPEN_AT:
        case CHIMERA_VFS_OP_MKDIR_AT:
        case CHIMERA_VFS_OP_MKNOD_AT:
        case CHIMERA_VFS_OP_SYMLINK_AT:
        case CHIMERA_VFS_OP_LINK_AT:
        case CHIMERA_VFS_OP_REMOVE_AT:
        case CHIMERA_VFS_OP_RENAME_AT:
        case CHIMERA_VFS_OP_SETATTR:
            request->status = CHIMERA_VFS_EROFS;
            request->complete(request);
            break;
        default:
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* rootfs_dispatch */

struct chimera_vfs_module chimera_vfs_rootfs_module = {
    .sdk_version    = CHIMERA_VFS_SDK_VERSION,
    .fh_magic       = CHIMERA_VFS_FH_MAGIC_ROOT,
    .name           = "rootfs",
    .capabilities   = CHIMERA_VFS_CAP_FS | CHIMERA_VFS_CAP_FS_RELATIVE_OP,
    .init           = rootfs_init,
    .destroy        = rootfs_destroy,
    .thread_init    = rootfs_thread_init,
    .thread_destroy = rootfs_thread_destroy,
    .dispatch       = rootfs_dispatch,
};

void
chimera_vfs_rootfs_mount(struct chimera_vfs *vfs)
{
    struct chimera_vfs_mount *mount = calloc(1, sizeof(*mount));
    struct rootfs            *fs    = vfs->module_private[CHIMERA_VFS_FH_MAGIC_ROOT];

    mount->module       = &chimera_vfs_rootfs_module;
    mount->builtin      = 1;
    mount->path         = strdup("");
    mount->module_path  = strdup("");
    mount->pathlen      = 0;
    mount->attrs.flags |= CHIMERA_VFS_MOUNT_ATTR_READONLY;

    memcpy(mount->root_fh, rootfs_root_fh, rootfs_root_fh_len);
    mount->root_fh_len = rootfs_root_fh_len;

    evpl_mutex_lock(&fs->lock);
    mount->root_attrs.va_req_mask = CHIMERA_VFS_ATTR_MASK_STAT;
    rootfs_map_attrs(&mount->root_attrs, fs->nodes[0]);
    evpl_mutex_unlock(&fs->lock);

    chimera_vfs_mount_table_insert(vfs->mount_table, mount);
} /* chimera_vfs_rootfs_mount */

int
chimera_vfs_rootfs_mkdir(
    struct chimera_vfs *vfs,
    const void         *parent_fh,
    int                 parent_fh_len,
    const char         *name,
    int                 namelen,
    uint8_t            *r_fh,
    int                *r_fh_len)
{
    struct rootfs      *fs = vfs->module_private[CHIMERA_VFS_FH_MAGIC_ROOT];
    struct rootfs_node *dir, *child;
    int                 rc = CHIMERA_VFS_OK;

    if (namelen <= 0 || namelen >= CHIMERA_VFS_NAME_MAX) {
        return CHIMERA_VFS_ENAMETOOLONG;
    }

    evpl_mutex_lock(&fs->lock);

    dir = rootfs_node_by_fh(fs, parent_fh, parent_fh_len);

    if (!dir) {
        rc = CHIMERA_VFS_ESTALE;
    } else {
        child = rootfs_child(dir, name, namelen);
        if (!child) {
            child = rootfs_node_alloc(fs, dir, name, namelen);
        }
        memcpy(r_fh, child->fh, child->fh_len);
        *r_fh_len = child->fh_len;
    }

    evpl_mutex_unlock(&fs->lock);

    return rc;
} /* chimera_vfs_rootfs_mkdir */

int
chimera_vfs_rootfs_rmdir(
    struct chimera_vfs *vfs,
    const void         *fh,
    int                 fhlen,
    uint8_t            *r_parent_fh,
    int                *r_parent_fh_len,
    char               *r_name,
    int                *r_namelen)
{
    struct rootfs      *fs = vfs->module_private[CHIMERA_VFS_FH_MAGIC_ROOT];
    struct rootfs_node *node, *parent;
    int                 rc = CHIMERA_VFS_OK;

    evpl_mutex_lock(&fs->lock);

    node = rootfs_node_by_fh(fs, fh, fhlen);

    if (!node) {
        rc = CHIMERA_VFS_ESTALE;
    } else if (node->parent == node) {
        rc = CHIMERA_VFS_EBUSY;
    } else if (node->num_children) {
        rc = CHIMERA_VFS_ENOTEMPTY;
    } else {
        parent = node->parent;

        for (int i = 0; i < parent->num_children; i++) {
            if (parent->children[i] == node) {
                memmove(&parent->children[i], &parent->children[i + 1],
                        (parent->num_children - i - 1) * sizeof(*parent->children));
                parent->num_children--;
                break;
            }
        }

        memcpy(r_parent_fh, parent->fh, parent->fh_len);
        *r_parent_fh_len = parent->fh_len;
        memcpy(r_name, node->name, node->namelen);
        *r_namelen = node->namelen;

        fs->nodes[node->ino - ROOTFS_INO_ROOT] = NULL;
        free(node->children);
        free(node);
    }

    evpl_mutex_unlock(&fs->lock);

    return rc;
} /* chimera_vfs_rootfs_rmdir */

void
chimera_vfs_rootfs_prune(
    struct chimera_vfs *vfs,
    const void         *fh,
    int                 fhlen)
{
    uint8_t dir_fh[CHIMERA_VFS_FH_SIZE], parent_fh[CHIMERA_VFS_FH_SIZE];
    char    name[CHIMERA_VFS_NAME_MAX];
    int     dir_fh_len = fhlen, parent_fh_len, namelen;

    if (!chimera_vfs_rootfs_owns(fh, fhlen) || fhlen > CHIMERA_VFS_FH_SIZE) {
        return;
    }

    memcpy(dir_fh, fh, fhlen);

    /* Directories are removed only once empty, and a directory with a mount
     * on it is never reached: the walk that made it would have crossed into
     * the mount instead. */
    while (chimera_vfs_rootfs_rmdir(vfs, dir_fh, dir_fh_len,
                                    parent_fh, &parent_fh_len,
                                    name, &namelen) == CHIMERA_VFS_OK) {
        chimera_vfs_name_cache_remove(vfs->vfs_name_cache,
                                      chimera_vfs_hash(parent_fh, parent_fh_len),
                                      parent_fh, parent_fh_len,
                                      chimera_vfs_hash(name, namelen),
                                      name, namelen);
        memcpy(dir_fh, parent_fh, parent_fh_len);
        dir_fh_len = parent_fh_len;
    }
} /* chimera_vfs_rootfs_prune */
