#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>

#include "vfs_root.h"
#include "common/logging.h"
#include "vfs/vfs_internal.h"

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

static void *
chimera_vfs_root_init(void)
{
    return 0;
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
    return 0;
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
    struct chimera_vfs_share *share;
    int                       num_shares;

    DL_COUNT(request->thread->vfs->shares, share, num_shares);

    attr = &request->getattr.r_attr;

    memset(attr, 0, sizeof(*attr));

    attr->va_mask = request->getattr.attr_mask;

    /* Synthetic root directory attribute */
    attr->va_mode  = S_IFDIR | 0755;
    attr->va_nlink = 2 + num_shares;
    attr->va_uid   = 0;
    attr->va_gid   = 0;
    attr->va_size  = 4096;
    clock_gettime(CLOCK_REALTIME, &attr->va_atime);
    attr->va_mtime = attr->va_atime;
    attr->va_ctime = attr->va_atime;
    attr->va_ino   = 2;
    attr->va_dev   = 0;
    attr->va_rdev  = 0;

    request->complete(request);
} /* chimera_vfs_getattr_root */


static void
chimera_vfs_root_lookup_complete(struct chimera_vfs_request *subrequest)
{
    struct chimera_vfs_thread  *thread  = subrequest->thread;
    struct chimera_vfs_request *request = subrequest->proto_private_data;

    request->status = subrequest->status;

    request->lookup.r_attr     = subrequest->lookup_path.r_attr;
    request->lookup.r_dir_attr = subrequest->lookup_path.r_dir_attr;

    request->complete(request);

    chimera_vfs_request_free(thread, subrequest);
} /* chimera_vfs_root_lookup_complete */

static void
chimera_vfs_root_lookup(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_vfs_thread  *thread = request->thread;
    struct chimera_vfs         *vfs    = thread->vfs;
    struct chimera_vfs_module  *module;
    struct chimera_vfs_share   *share;
    struct chimera_vfs_request *subrequest;
    const char                 *path    = request->lookup.component;
    int                         pathlen = request->lookup.component_len;

    DL_FOREACH(vfs->shares, share)
    {
        if (strncmp(share->name, path, pathlen) == 0) {
            break;
        }
    } /* DL_FOREACH */

    if (!share) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    module = share->module;

    subrequest = chimera_vfs_request_alloc(thread, &module->fh_magic, sizeof(module->fh_magic));

    subrequest->opcode               = CHIMERA_VFS_OP_LOOKUP_PATH;
    subrequest->complete             = chimera_vfs_root_lookup_complete;
    subrequest->lookup_path.path     = share->path;
    subrequest->lookup_path.pathlen  = strlen(share->path);
    subrequest->lookup_path.attrmask = 0;
    subrequest->proto_callback       = NULL;
    subrequest->proto_private_data   = request;

    chimera_vfs_dispatch(subrequest);

} /* chimera_vfs_root_lookup */


static void
chimera_vfs_root_lookup_path_complete(struct chimera_vfs_request *subrequest)
{
    struct chimera_vfs_thread  *thread  = subrequest->thread;
    struct chimera_vfs_request *request = subrequest->proto_private_data;

    request->status = subrequest->status;

    request->lookup_path.r_attr     = subrequest->lookup_path.r_attr;
    request->lookup_path.r_dir_attr = subrequest->lookup_path.r_dir_attr;

    request->complete(request);

    chimera_vfs_request_free(thread, subrequest);
} /* chimera_vfs_root_lookup_path_complete */

static void
chimera_vfs_root_lookup_path(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_vfs_thread  *thread = request->thread;
    struct chimera_vfs         *vfs    = thread->vfs;
    struct chimera_vfs_module  *module;
    struct chimera_vfs_share   *share;
    struct chimera_vfs_request *subrequest;
    const char                 *path = request->lookup_path.path;
    const char                 *slash;
    char                       *sharepath;
    int                         pathlen = request->lookup_path.pathlen;
    int                         complen, sharepathlen;

    while (*path == '/') {
        path++;
        pathlen--;
    }

    slash = strchr(path, '/');

    if (slash) {
        complen = slash - path;
    } else {
        complen = pathlen;
    }

    DL_FOREACH(vfs->shares, share)
    {
        if (strncmp(share->name, path, complen) == 0) {
            break;
        }
    } /* DL_FOREACH */

    if (!share) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }
    if (slash) {
        sharepath    = alloca(pathlen);
        sharepathlen = snprintf(sharepath, pathlen, "%s/%s", share->path, slash
                                + 1);
    } else {
        sharepath    = share->path;
        sharepathlen = strlen(share->path);
    }

    module = share->module;

    subrequest = chimera_vfs_request_alloc(thread, &module->fh_magic, sizeof(module->fh_magic));

    subrequest->opcode               = CHIMERA_VFS_OP_LOOKUP_PATH;
    subrequest->complete             = chimera_vfs_root_lookup_path_complete;
    subrequest->lookup_path.path     = sharepath;
    subrequest->lookup_path.pathlen  = sharepathlen;
    subrequest->lookup_path.attrmask = 0;
    subrequest->proto_callback       = NULL;
    subrequest->proto_private_data   = request;

    chimera_vfs_dispatch(subrequest);

} /* chimera_vfs_root_lookup_path */

static void
chimera_vfs_root_readdir(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_vfs_thread *thread = request->thread;
    struct chimera_vfs        *vfs    = thread->vfs;
    struct chimera_vfs_share  *share;
    struct chimera_vfs_attrs   attr;
    int                        i      = 0;
    uint64_t                   cookie = request->readdir.cookie;

    attr.va_mask = 0;

    DL_FOREACH(vfs->shares, share)
    {

        if (i < cookie) {
            continue;
        }

        /* XXX We are not going to get the attributes of the share path,
         * we could but it would be a bit fussy.  Consequently the
         * inum we are reeturning for the dirent will not match the actual
         * share root inode number.
         */

        request->readdir.callback(
            i,
            i,
            share->name,
            strlen(share->name),
            &attr,
            request->proto_private_data);

        i++;

        request->readdir.r_cookie = i;
    } /* DL_FOREACH */

    request->status        = CHIMERA_VFS_OK;
    request->readdir.r_eof = 1;
    request->complete(request);
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
        case CHIMERA_VFS_OP_LOOKUP_PATH:
            chimera_vfs_root_lookup_path(request, private_data);
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

struct chimera_vfs_module vfs_root = {
    .fh_magic           = CHIMERA_VFS_FH_MAGIC_ROOT,
    .name               = "root",
    .blocking           = 0,
    .path_open_required = 0,
    .file_open_required = 0,
    .init               = chimera_vfs_root_init,
    .destroy            = chimera_vfs_root_destroy,
    .thread_init        = chimera_vfs_root_thread_init,
    .thread_destroy     = chimera_vfs_root_thread_destroy,
    .dispatch           = chimera_vfs_root_dispatch,
};
