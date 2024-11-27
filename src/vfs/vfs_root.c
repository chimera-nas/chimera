#include <string.h>
#include <time.h>
#include <sys/stat.h>

#include "vfs/vfs_root.h"
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

    attr = &request->getattr.r_attr;

    memset(attr, 0, sizeof(*attr));

    attr->va_mask = request->getattr.attr_mask;

    /* Set dummy values for a directory */
    attr->va_mode          = S_IFDIR | 0755; /* directory with rwxr-xr-x permissions */
    attr->va_nlink         = 2;      /* . and .. minimum for directory */
    attr->va_uid           = 0;      /* root user */
    attr->va_gid           = 0;      /* root group */
    attr->va_size          = 4096;   /* typical directory size */
    attr->va_atime.tv_sec  = time(NULL);
    attr->va_atime.tv_nsec = 0;
    attr->va_mtime         = attr->va_atime; /* same as access time */
    attr->va_ctime         = attr->va_atime; /* same as access time */
    attr->va_ino           = 2;      /* root directory inode */
    attr->va_dev           = 0;      /* device ID */
    attr->va_rdev          = 0;      /* not a device file */

    request->complete(request);
} /* chimera_vfs_getattr_root */

static void
chimera_vfs_root_lookup_path_complete(struct chimera_vfs_request *subrequest)
{
    struct chimera_vfs_thread  *thread  = subrequest->thread;
    struct chimera_vfs_request *request = subrequest->proto_private_data;

    request->status = subrequest->status;

    memcpy(
        request->lookup.r_fh,
        subrequest->lookup_path.r_fh,
        subrequest->lookup_path.r_fh_len);

    request->lookup.r_fh_len = subrequest->lookup_path.r_fh_len;

    request->complete(request);

    chimera_vfs_request_free(thread, subrequest);
} /* chimera_vfs_root_lookup_path_complete */

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

    chimera_vfs_root_debug(
        "chimera_vfs_root_lookup: name=%.*s",
        request->lookup.component_len, request->lookup.component);

    DL_FOREACH(vfs->shares, share)
    {
        if (strncmp(share->name,
                    request->lookup.component,
                    request->lookup.component_len) == 0) {
            break;
        }
    } /* DL_FOREACH */


    if (!share) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    module = share->module;

    subrequest = chimera_vfs_request_alloc(thread);

    subrequest->opcode               = CHIMERA_VFS_OP_LOOKUP_PATH;
    subrequest->complete             = chimera_vfs_root_lookup_path_complete;
    subrequest->lookup_path.path     = share->path;
    subrequest->lookup_path.pathlen  = strlen(share->path);
    subrequest->lookup_path.r_fh_len = 0;
    subrequest->proto_callback       = NULL;
    subrequest->proto_private_data   = request;

    module->dispatch(subrequest, thread->module_private[module->fh_magic]);

} /* chimera_vfs_root_lookup */

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

    DL_FOREACH(vfs->shares, share)
    {

        if (i < cookie) {
            continue;
        }

        /* Set dummy values for a directory */
        memset(&attr, 0, sizeof(attr));
        attr.va_mode          = S_IFDIR | 0755; /* directory with rwxr-xr-x permissions */
        attr.va_nlink         = 2;      /* . and .. minimum for directory */
        attr.va_uid           = 0;      /* root user */
        attr.va_gid           = 0;      /* root group */
        attr.va_size          = 4096;   /* typical directory size */
        attr.va_atime.tv_sec  = time(NULL);
        attr.va_atime.tv_nsec = 0;
        attr.va_mtime         = attr.va_atime; /* same as access time */
        attr.va_ctime         = attr.va_atime; /* same as access time */
        attr.va_ino           = 2;      /* root directory inode */
        attr.va_dev           = 0;      /* device ID */
        attr.va_rdev          = 0;      /* not a device file */

        request->readdir.callback(
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
chimera_vfs_root_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    chimera_vfs_root_debug("chimera_vfs_root_dispatch: request=%p", request);

    switch (request->opcode) {
        case CHIMERA_VFS_OP_LOOKUP:
            chimera_vfs_root_lookup(request, private_data);
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
    .fh_magic       = CHIMERA_VFS_FH_MAGIC_ROOT,
    .name           = "root",
    .blocking       = 0,
    .init           = chimera_vfs_root_init,
    .destroy        = chimera_vfs_root_destroy,
    .thread_init    = chimera_vfs_root_thread_init,
    .thread_destroy = chimera_vfs_root_thread_destroy,
    .dispatch       = chimera_vfs_root_dispatch,
};
