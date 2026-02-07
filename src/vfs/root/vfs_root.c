// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>

#include "vfs_root.h"
#include "common/logging.h"
#include "common/macros.h"
#include "vfs/vfs.h"
#include "vfs/vfs_fh.h"
#include "vfs/vfs_internal.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_mount_table.h"

#define chimera_vfs_root_debug(...) chimera_debug("vfs_root", \
                                                  __FILE__, \
                                                  __LINE__, \
                                                  __VA_ARGS__)
#define chimera_vfs_root_info(...)  chimera_info("vfs_root", \
                                                 __FILE__, \
                                                 __LINE__, \
                                                 __VA_ARGS__)
#define chimera_vfs_root_error(...) chimera_error("vfs_root", \
                                                  __FILE__, \
                                                  __LINE__, \
                                                  __VA_ARGS__)
#define chimera_vfs_root_fatal(...) chimera_fatal("vfs_root", \
                                                  __FILE__, \
                                                  __LINE__, \
                                                  __VA_ARGS__)
#define chimera_vfs_root_abort(...) chimera_abort("vfs_root", \
                                                  __FILE__, \
                                                  __LINE__, \
                                                  __VA_ARGS__)

#define chimera_vfs_root_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "vfs_root", \
                         __FILE__, \
                         __LINE__, \
                         __VA_ARGS__)

struct chimera_vfs_root_mount_ctx {
    struct chimera_vfs_mount *mount;
};

/* Static root file handle for the vfs_root pseudo-filesystem */
static uint8_t  vfs_root_fh[CHIMERA_VFS_MOUNT_ID_SIZE];
static uint32_t vfs_root_fh_len;

static void *
chimera_vfs_root_init(const char *cfgfile)
{
    /* Create the root FH using a fixed FSID of all zeros */
    uint8_t fsid[CHIMERA_VFS_FSID_SIZE] = { 0 };

    /* The root has no fh_fragment, just the mount_id derived from fsid alone */
    vfs_root_fh_len = chimera_vfs_encode_fh_mount(fsid, NULL, 0, vfs_root_fh);

    /* We don't need any private data, but we're expected to return something */
    return (void *) 42;
} /* vfs_root_init */

SYMBOL_EXPORT void
chimera_vfs_root_get_fh(
    uint8_t  *fh,
    uint32_t *fh_len)
{
    memcpy(fh, vfs_root_fh, vfs_root_fh_len);
    *fh_len = vfs_root_fh_len;
} /* chimera_vfs_root_get_fh */

SYMBOL_EXPORT void
chimera_vfs_root_register_mount(struct chimera_vfs *vfs)
{
    struct chimera_vfs_mount *mount;

    /* Create a pseudo-mount entry for the root filesystem */
    mount = calloc(1, sizeof(*mount));

    mount->module  = &vfs_root;
    mount->path    = strdup("");
    mount->pathlen = 0;

    /* Store the root FH (first 16 bytes is the mount_id) */
    memcpy(mount->root_fh, vfs_root_fh, vfs_root_fh_len);
    mount->root_fh_len = vfs_root_fh_len;

    chimera_vfs_mount_table_insert(vfs->mount_table, mount);
} /* chimera_vfs_root_register_mount */

static void
chimera_vfs_root_destroy(void *private_data)
{

} /* vfs_root_destroy */

static void *
chimera_vfs_root_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    /* We don't need any private data, but we're expected to return something */
    return (void *) 42;
} /* vfs_root_thread_init */

static void
chimera_vfs_root_thread_destroy(void *private_data)
{

} /* vfs_root_thread_destroy */

static void
chimera_vfs_root_getattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_vfs_attrs *attr;
    int                       num_mounts;

    num_mounts = chimera_vfs_mount_table_count(request->thread->vfs->mount_table);

    attr = &request->getattr.r_attr;

    memset(attr, 0, sizeof(*attr));

    attr->va_set_mask = CHIMERA_VFS_ATTR_MASK_STAT;

    /* Synthetic root directory attribute */
    attr->va_mode  = S_IFDIR | 0755;
    attr->va_nlink = 2 + num_mounts;
    attr->va_uid   = 0;
    attr->va_gid   = 0;
    attr->va_size  = 4096;
    clock_gettime(CLOCK_REALTIME, &attr->va_atime);
    attr->va_mtime = attr->va_atime;
    attr->va_ctime = attr->va_atime;
    attr->va_ino   = 2;
    attr->va_dev   = 0;
    attr->va_rdev  = 0;

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_MASK_STATFS) {
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

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_vfs_getattr_root */

struct chimera_vfs_root_lookup_ctx {
    struct chimera_vfs_open_handle *oh;
    uint8_t                         mount_id[CHIMERA_VFS_FH_SIZE + 16];
    int                             mount_id_len;
};

static void
chimera_vfs_root_lookup_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request         *lookup_request = private_data;
    struct chimera_vfs                 *vfs            = lookup_request->thread->vfs;
    chimera_vfs_lookup_callback_t       callback       = lookup_request->proto_callback;
    struct chimera_vfs_attrs           *dir_attr;
    int                                 num_mounts;
    struct chimera_vfs_root_lookup_ctx *ctx = lookup_request->plugin_data;

    if (error_code) {
        callback(error_code, NULL, NULL, lookup_request->proto_private_data);
    } else {

        attr->va_set_mask |= CHIMERA_VFS_ATTR_FH;
        memcpy(attr->va_fh, ctx->mount_id, ctx->mount_id_len);
        attr->va_fh_len = ctx->mount_id_len;

        dir_attr = &lookup_request->lookup.r_dir_attr;


        num_mounts = chimera_vfs_mount_table_count(vfs->mount_table);

        dir_attr->va_set_mask = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT;
        memcpy(dir_attr->va_fh, vfs_root_fh, vfs_root_fh_len);
        dir_attr->va_fh_len = vfs_root_fh_len;

        /* Synthetic root directory attribute */
        dir_attr->va_mode  = S_IFDIR | 0755;
        dir_attr->va_nlink = 2 + num_mounts;
        dir_attr->va_uid   = 0;
        dir_attr->va_gid   = 0;
        dir_attr->va_size  = 4096;
        clock_gettime(CLOCK_REALTIME, &dir_attr->va_atime);
        dir_attr->va_mtime = dir_attr->va_atime;
        dir_attr->va_ctime = dir_attr->va_atime;
        dir_attr->va_ino   = 2;
        dir_attr->va_dev   = 0;
        dir_attr->va_rdev  = 0;

        callback(CHIMERA_VFS_OK, attr, dir_attr, lookup_request->proto_private_data);
    }

    chimera_vfs_release(lookup_request->thread, ctx->oh);


    chimera_vfs_request_free(lookup_request->thread, lookup_request);
} /* chimera_vfs_root_lookup_getattr_callback */

static void
chimera_vfs_root_lookup_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_request         *lookup_request = private_data;
    chimera_vfs_lookup_callback_t       callback       = lookup_request->proto_callback;
    struct chimera_vfs_root_lookup_ctx *ctx;

    if (error_code) {
        callback(error_code, NULL, NULL, lookup_request->proto_private_data);
        chimera_vfs_request_free(lookup_request->thread, lookup_request);
        return;
    }

    ctx     = lookup_request->plugin_data;
    ctx->oh = oh;

    chimera_vfs_getattr(
        lookup_request->thread,
        lookup_request->cred,
        oh,
        lookup_request->lookup.r_attr.va_req_mask,
        chimera_vfs_root_lookup_getattr_callback,
        lookup_request);


} /* chimera_vfs_root_lookup_open_callback */

static void
chimera_vfs_root_lookup(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_vfs_thread          *thread  = request->thread;
    struct chimera_vfs                 *vfs     = thread->vfs;
    const char                         *name    = request->lookup.component;
    int                                 namelen = request->lookup.component_len;
    struct chimera_vfs_root_lookup_ctx *ctx     = request->plugin_data;
    int                                 rc;

    rc = chimera_vfs_mount_table_lookup_root_fh_by_name(vfs->mount_table, name, namelen, ctx->mount_id, &ctx->
                                                        mount_id_len);

    if (rc != 0) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    chimera_vfs_open(
        thread,
        request->cred,
        ctx->mount_id,
        ctx->mount_id_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED,
        chimera_vfs_root_lookup_open_callback,
        request);

} /* chimera_vfs_root_lookup */

static void
chimera_vfs_root_mount(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_vfs_attrs  *attr   = &request->mount.r_attr;
    struct chimera_vfs_thread *thread = request->thread;
    struct chimera_vfs        *vfs    = thread->vfs;
    int                        num_mounts;

    if (strcmp(request->mount.path, "/") != 0) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    num_mounts = chimera_vfs_mount_table_count(vfs->mount_table);

    attr->va_set_mask = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT;
    memcpy(attr->va_fh, vfs_root_fh, vfs_root_fh_len);
    attr->va_fh_len = vfs_root_fh_len;

    /* Synthetic root directory attribute */
    attr->va_mode  = S_IFDIR | 0755;
    attr->va_nlink = 2 + num_mounts;
    attr->va_uid   = 0;
    attr->va_gid   = 0;
    attr->va_size  = 4096;
    clock_gettime(CLOCK_REALTIME, &attr->va_atime);
    attr->va_mtime = attr->va_atime;
    attr->va_ctime = attr->va_atime;
    attr->va_ino   = 2;
    attr->va_dev   = 0;
    attr->va_rdev  = 0;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_vfs_root_lookup_path */


static void
chimera_vfs_root_umount(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    /* No action required */
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_vfs_root_umount */

/* Max mount path length for readdir - short since mount names are typically simple */
#define CHIMERA_VFS_ROOT_MOUNT_PATH_MAX 128

struct chimera_vfs_root_readdir_entry {
    uint64_t                        cookie;
    char                            path[CHIMERA_VFS_ROOT_MOUNT_PATH_MAX];
    uint8_t                         mount_id[CHIMERA_VFS_FH_SIZE + 16];
    int                             mount_id_len;
    struct chimera_vfs_open_handle *oh;
    struct chimera_vfs_attrs        attr;
    struct chimera_vfs_request     *request;
};

#define CHIMERA_VFS_ROOT_MAX_READDIR 16

struct chimera_vfs_root_readdir_ctx {
    uint32_t                              pending;
    uint32_t                              complete;
    uint32_t                              num_entries;
    struct chimera_vfs_root_readdir_entry entries[CHIMERA_VFS_ROOT_MAX_READDIR];
};


static void
chimera_vfs_root_readdir_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_root_readdir_ctx   *ctx = request->plugin_data;
    struct chimera_vfs_root_readdir_entry *entry;
    int                                    i, rc, eof = 1;
    uint64_t                               cookie = 0;


    for (i = 0; i < ctx->num_entries; i++) {
        entry = &ctx->entries[i];

        rc = request->readdir.callback(
            entry->cookie,
            entry->attr.va_ino,
            entry->path,
            strlen(entry->path),
            &entry->attr,
            request->proto_private_data);

        if (rc) {
            eof = 0;
            break;
        }

        cookie = entry->cookie;
    }

    request->status           = CHIMERA_VFS_OK;
    request->readdir.r_cookie = cookie;
    request->readdir.r_eof    = eof;
    request->complete(request);
} /* chimera_vfs_root_readdir_complete */

static void
chimera_vfs_root_readdir_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_root_readdir_entry *entry   = private_data;
    struct chimera_vfs_request            *request = entry->request;
    struct chimera_vfs_root_readdir_ctx   *ctx     = request->plugin_data;

    if (error_code != CHIMERA_VFS_OK) {
        abort();
    }

    entry->attr = *attr;

    ctx->pending--;

    chimera_vfs_release(request->thread, entry->oh);

    if (ctx->complete && ctx->pending == 0) {
        chimera_vfs_root_readdir_complete(request);
    }

} /* chimera_vfs_root_readdir_lookup_path_complete */

static void
chimera_vfs_root_readdir_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_root_readdir_entry *entry = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        abort();
    }

    entry->oh = oh;

    chimera_vfs_getattr(
        entry->request->thread,
        entry->request->cred,
        oh,
        entry->attr.va_req_mask,
        chimera_vfs_root_readdir_getattr_callback,
        entry);

} /* chimera_vfs_root_readdir_open_callback */

struct chimera_vfs_root_readdir_iter_ctx {
    struct chimera_vfs_root_readdir_ctx *ctx;
    struct chimera_vfs_request          *request;
    uint64_t                             cookie;
    int                                  index;
};

static int
chimera_vfs_root_readdir_iter_cb(
    struct chimera_vfs_mount *mount,
    void                     *private_data)
{
    struct chimera_vfs_root_readdir_iter_ctx *iter = private_data;
    struct chimera_vfs_root_readdir_ctx      *ctx  = iter->ctx;
    struct chimera_vfs_root_readdir_entry    *entry;

    if (iter->index < iter->cookie) {
        iter->index++;
        return 0;
    }

    if (ctx->num_entries >= CHIMERA_VFS_ROOT_MAX_READDIR) {
        return 1; /* Stop iteration - buffer full */
    }

    entry = &ctx->entries[ctx->num_entries++];

    entry->cookie           = iter->index;
    entry->attr.va_req_mask = iter->request->readdir.attr_mask;
    entry->attr.va_set_mask = 0;
    entry->request          = iter->request;

    /* Copy mount data by value for safe access after RCU unlock */
    strncpy(entry->path, mount->path, CHIMERA_VFS_ROOT_MOUNT_PATH_MAX - 1);
    entry->path[CHIMERA_VFS_ROOT_MOUNT_PATH_MAX - 1] = '\0';
    memcpy(entry->mount_id, mount->root_fh, mount->root_fh_len);
    entry->mount_id_len = mount->root_fh_len;

    ctx->pending++;
    iter->index++;

    return 0;
} /* chimera_vfs_root_readdir_iter_cb */

static void
chimera_vfs_root_readdir(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_vfs_thread               *thread = request->thread;
    struct chimera_vfs                      *vfs    = thread->vfs;
    struct chimera_vfs_root_readdir_ctx     *ctx    = request->plugin_data;
    struct chimera_vfs_root_readdir_iter_ctx iter;
    struct chimera_vfs_root_readdir_entry   *entry;
    int                                      i;

    ctx->pending     = 0;
    ctx->complete    = 0;
    ctx->num_entries = 0;

    iter.ctx     = ctx;
    iter.request = request;
    iter.cookie  = request->readdir.cookie;
    iter.index   = 0;

    chimera_vfs_mount_table_foreach(vfs->mount_table, chimera_vfs_root_readdir_iter_cb, &iter);

    /* Now issue async opens for all collected entries */
    for (i = 0; i < ctx->num_entries; i++) {
        entry = &ctx->entries[i];

        chimera_vfs_open(
            thread,
            request->cred,
            entry->mount_id,
            entry->mount_id_len,
            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED,
            chimera_vfs_root_readdir_open_callback,
            entry);
    }

    ctx->complete = 1;

    if (ctx->pending == 0) {
        chimera_vfs_root_readdir_complete(request);
    }

} /* chimera_vfs_root_readdir */

static void
chimera_vfs_root_open(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    request->open.r_vfs_private = 0;
    request->status             = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_vfs_root_open */

static void
chimera_vfs_root_close(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_vfs_root_close */

static void
chimera_vfs_root_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            chimera_vfs_root_mount(request, private_data);
            break;
        case CHIMERA_VFS_OP_UMOUNT:
            chimera_vfs_root_umount(request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP:
            chimera_vfs_root_lookup(request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN:
            chimera_vfs_root_open(request, private_data);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            chimera_vfs_root_close(request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            chimera_vfs_root_getattr(request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            chimera_vfs_root_readdir(request, private_data);
            break;
        default:
            chimera_vfs_root_error(
                "chimera_vfs_root_dispatch: unknown operation %d",
                request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* vfs_root_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_root = {
    .fh_magic       = CHIMERA_VFS_FH_MAGIC_ROOT,
    .name           = "root",
    .capabilities   = CHIMERA_VFS_CAP_FS,
    .init           = chimera_vfs_root_init,
    .destroy        = chimera_vfs_root_destroy,
    .thread_init    = chimera_vfs_root_thread_init,
    .thread_destroy = chimera_vfs_root_thread_destroy,
    .dispatch       = chimera_vfs_root_dispatch,
};
