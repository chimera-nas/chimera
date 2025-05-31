#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>

#include "vfs_root.h"
#include "common/logging.h"
#include "common/macros.h"
#include "vfs/vfs_internal.h"
#include "vfs/vfs_procs.h"

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

static void *
chimera_vfs_root_init(const char *cfgfile)
{
    /* We don't need any private data, but we're expected to return something */
    return (void *) 42;
} /* vfs_root_init */

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
    struct chimera_vfs_mount *mount;
    int                       num_mounts;

    pthread_rwlock_rdlock(&request->thread->vfs->mounts_lock);
    DL_COUNT(request->thread->vfs->mounts, mount, num_mounts);
    pthread_rwlock_unlock(&request->thread->vfs->mounts_lock);

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

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_vfs_getattr_root */


static void
chimera_vfs_root_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request   = private_data;
    struct chimera_vfs_thread  *thread    = request->thread;
    struct chimera_vfs         *vfs       = thread->vfs;
    struct chimera_vfs_attrs   *root_attr = &request->lookup.r_dir_attr;
    struct chimera_vfs_mount   *mount;
    int                         num_mounts;

    pthread_rwlock_rdlock(&vfs->mounts_lock);
    DL_COUNT(vfs->mounts, mount, num_mounts);
    pthread_rwlock_unlock(&vfs->mounts_lock);

    request->status        = error_code;
    request->lookup.r_attr = *attr;


    root_attr->va_set_mask = CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_FH;

    /* Synthetic root directory attribute */
    root_attr->va_mode  = S_IFDIR | 0755;
    root_attr->va_nlink = 2 + num_mounts;
    root_attr->va_uid   = 0;
    root_attr->va_gid   = 0;
    root_attr->va_size  = 4096;
    clock_gettime(CLOCK_REALTIME, &root_attr->va_atime);
    root_attr->va_mtime = root_attr->va_atime;
    root_attr->va_ctime = root_attr->va_atime;
    root_attr->va_ino   = 2;
    root_attr->va_dev   = 0;
    root_attr->va_rdev  = 0;

    root_attr->va_fh[0]  = CHIMERA_VFS_FH_MAGIC_ROOT;
    root_attr->va_fh_len = 1;

    request->complete(request);

} /* chimera_vfs_root_lookup_complete */

static void
chimera_vfs_root_lookup_module_root_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request        *request = private_data;
    struct chimera_vfs_root_mount_ctx *ctx     = request->plugin_data;
    struct chimera_vfs_mount          *mount   = ctx->mount;

    if (error_code != CHIMERA_VFS_OK) {
        request->status = error_code;
        request->complete(request);
        return;
    }

    chimera_vfs_lookup_path(
        request->thread,
        attr->va_fh,
        attr->va_fh_len,
        mount->path,
        mount->pathlen,
        request->lookup.r_attr.va_req_mask,
        chimera_vfs_root_lookup_complete,
        request);

} /* chimera_vfs_root_lookup_module_root_complete */

static void
chimera_vfs_root_lookup(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_vfs_thread         *thread = request->thread;
    struct chimera_vfs                *vfs    = thread->vfs;
    struct chimera_vfs_mount          *mount;
    struct chimera_vfs_root_mount_ctx *ctx;
    const char                        *path    = request->lookup.component;
    int                                pathlen = request->lookup.component_len;

    pthread_rwlock_rdlock(&vfs->mounts_lock);

    DL_FOREACH(vfs->mounts, mount)
    {
        if (strncmp(mount->name, path, pathlen) == 0) {
            break;
        }
    } /* DL_FOREACH */

    if (!mount) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        pthread_rwlock_unlock(&vfs->mounts_lock);
        return;
    }

    if ((mount->module->capabilities & CHIMERA_VFS_CAP_HANDLE_ALL) &&
        (mount->pathlen != 1 || mount->path[0] != '/')) {

        ctx        = request->plugin_data;
        ctx->mount = mount;

        chimera_vfs_getrootfh(
            thread,
            mount->module,
            NULL,
            0,
            request->lookup.r_attr.va_req_mask,
            chimera_vfs_root_lookup_module_root_complete,
            request);
    } else {
        chimera_vfs_getrootfh(
            thread,
            mount->module,
            mount->path,
            mount->pathlen,
            request->lookup.r_attr.va_req_mask,
            chimera_vfs_root_lookup_complete,
            request);
    }

    pthread_rwlock_unlock(&vfs->mounts_lock);

} /* chimera_vfs_root_lookup */

static void
chimera_vfs_root_getrootfh(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_vfs_attrs  *attr   = &request->getrootfh.r_attr;
    struct chimera_vfs_thread *thread = request->thread;
    struct chimera_vfs        *vfs    = thread->vfs;
    struct chimera_vfs_mount  *mount;
    int                        num_mounts;

    pthread_rwlock_rdlock(&vfs->mounts_lock);
    DL_COUNT(vfs->mounts, mount, num_mounts);
    pthread_rwlock_unlock(&vfs->mounts_lock);


    attr->va_set_mask = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT;
    attr->va_fh[0]    = CHIMERA_VFS_FH_MAGIC_ROOT;
    attr->va_fh_len   = 1;

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

struct chimera_vfs_root_readdir_entry {
    uint64_t                    cookie;
    const char                 *name;
    struct chimera_vfs_attrs    attr;
    struct chimera_vfs_request *request;
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
            entry->name,
            strlen(entry->name),
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
chimera_vfs_root_readdir_lookup_path_complete(
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

    if (ctx->complete && ctx->pending == 0) {
        chimera_vfs_root_readdir_complete(request);
    }

} /* chimera_vfs_root_readdir_lookup_path_complete */

static void
chimera_vfs_root_readdir(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_vfs_thread             *thread = request->thread;
    struct chimera_vfs                    *vfs    = thread->vfs;
    struct chimera_vfs_mount              *mount;
    int                                    i      = 0;
    uint64_t                               cookie = request->readdir.cookie;
    struct chimera_vfs_root_readdir_ctx   *ctx    = request->plugin_data;
    struct chimera_vfs_root_readdir_entry *entry;
    int                                    fh_all;

    ctx->pending     = 0;
    ctx->complete    = 0;
    ctx->num_entries = 0;

    pthread_rwlock_rdlock(&vfs->mounts_lock);

    DL_FOREACH(vfs->mounts, mount)
    {

        if (i < cookie) {
            continue;
        }

        entry = &ctx->entries[ctx->num_entries++];

        entry->cookie           = i;
        entry->name             = mount->name;
        entry->attr.va_req_mask = request->readdir.attr_mask;
        entry->attr.va_set_mask = 0;
        entry->request          = request;
        ctx->pending++;

        fh_all = (mount->module->capabilities & CHIMERA_VFS_CAP_HANDLE_ALL);


        chimera_vfs_getrootfh(
            thread,
            mount->module,
            fh_all ? NULL : mount->path,
            fh_all ? 0 : mount->pathlen,
            request->readdir.attr_mask,
            chimera_vfs_root_readdir_lookup_path_complete,
            entry);

        i++;

    } /* DL_FOREACH */

    pthread_rwlock_unlock(&vfs->mounts_lock);

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
        case CHIMERA_VFS_OP_GETROOTFH:
            chimera_vfs_root_getrootfh(request, private_data);
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
    .capabilities   = CHIMERA_VFS_CAP_HANDLE_ALL,
    .init           = chimera_vfs_root_init,
    .destroy        = chimera_vfs_root_destroy,
    .thread_init    = chimera_vfs_root_thread_init,
    .thread_destroy = chimera_vfs_root_thread_destroy,
    .dispatch       = chimera_vfs_root_dispatch,
};
