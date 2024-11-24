#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

#include "memfs.h"
#include "common/logging.h"

#define chimera_memfs_debug(...) chimera_debug("memfs", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_memfs_info(...)  chimera_info("memfs", \
                                              __FILE__, \
                                              __LINE__, \
                                              __VA_ARGS__)
#define chimera_memfs_error(...) chimera_error("memfs", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_memfs_fatal(...) chimera_fatal("memfs", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_memfs_abort(...) chimera_abort("memfs", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)

#define chimera_memfs_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "memfs", __FILE__, __LINE__, __VA_ARGS__)

static void *
memfs_init(void)
{
    return 0;
} /* memfs_init */

static void
memfs_destroy(void *private_data)
{

} /* memfs_destroy */

static void *
memfs_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    return 0;
} /* memfs_thread_init */

static void
memfs_thread_destroy(void *private_data)
{

} /* memfs_thread_destroy */

static void
memfs_getattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    uint32_t                  attr_mask = request->getattr.attr_mask;
    struct chimera_vfs_attrs *attr      = &request->getattr.r_attr;

    request->status = CHIMERA_VFS_OK;

    request->getattr.r_attr_mask = attr_mask;

    memset(attr, 0, sizeof(*attr));

    /* Set dummy values for a directory */
    attr->va_mode          = S_IFDIR | 0755; /* directory with rwxr-xr-x permissions */
    attr->va_nlink         = 2;              /* . and .. minimum for directory */
    attr->va_uid           = 0;              /* root user */
    attr->va_gid           = 0;              /* root group */
    attr->va_size          = 4096;           /* typical directory size */
    attr->va_atime.tv_sec  = time(NULL);
    attr->va_atime.tv_nsec = 0;
    attr->va_mtime         = attr->va_atime;  /* same as access time */
    attr->va_ctime         = attr->va_atime;  /* same as access time */
    attr->va_ino           = 2;              /* root directory inode */
    attr->va_dev           = 0;              /* device ID */
    attr->va_rdev          = 0;              /* not a device file */

    request->complete(request);
} /* memfs_getattr */

static void
memfs_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    chimera_memfs_debug("memfs_dispatch: request=%p", request);

    switch (request->opcode) {
        case CHIMERA_VFS_OP_LOOKUP_PATH:
            request->status               = CHIMERA_VFS_OK;
            request->lookup_path.r_fh[0]  = 42;
            request->lookup_path.r_fh_len = 1;
            request->complete(request);
            break;
        case CHIMERA_VFS_OP_LOOKUP:
            request->status          = CHIMERA_VFS_OK;
            request->lookup.r_fh[0]  = 42;
            request->lookup.r_fh_len = 1;
            request->complete(request);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            memfs_getattr(request, private_data);
            break;
        default:
            chimera_memfs_error("memfs_dispatch: unknown operation %d",
                                request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* memfs_dispatch */

struct chimera_vfs_module vfs_memvfs = {
    .name           = "memfs",
    .fh_magic       = CHIMERA_VFS_FH_MAGIC_MEMFS,
    .blocking       = 0,
    .init           = memfs_init,
    .destroy        = memfs_destroy,
    .thread_init    = memfs_thread_init,
    .thread_destroy = memfs_thread_destroy,
    .dispatch       = memfs_dispatch,
};
