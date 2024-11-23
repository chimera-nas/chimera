#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "vfs.h"
#include "vfs_internal.h"
#include "common/format.h"
#include "common/misc.h"
#include "uthash/utlist.h"

#if 0
static inline void
chimera_vfs_dispatch(
    struct chimera_vfs         *vfs,
    struct chimera_vfs_request *request)
{
    chimera_vfs_dump_request(request);
    vfs->dispatch_cb(vfs, request);
} /* chimera_vfs_dispatch */

static inline void
chimera_vfs_complete(
    struct chimera_vfs_request *request,
    int                         status)
{
    request->status = status;
    chimera_vfs_dump_reply(request);
    request->complete_cb();
} /* chimera_vfs_complete */

struct chimera_vfs *
chimera_get_vfs_by_fh(
    const void *fh,
    uint32_t    fh_len);

struct chimera_share *
chimera_get_share_by_name(
    const char *name);

#endif /* if 0 */

static inline struct chimera_vfs_request *
chimera_vfs_request_alloc(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_request *request;

    if (thread->free_requests) {
        request = thread->free_requests;
        DL_DELETE(thread->free_requests, request);
    } else {
        request         = calloc(1, sizeof(struct chimera_vfs_request));
        request->thread = thread;
        request->status = CHIMERA_VFS_UNSET;
    }

    return request;
} /* chimera_vfs_request_alloc */

static inline void
chimera_vfs_request_free(
    struct chimera_vfs_thread  *thread,
    struct chimera_vfs_request *request)
{
    DL_PREPEND(thread->free_requests, request);
} /* chimera_vfs_request_free */

struct chimera_vfs *
chimera_vfs_init(void)
{
    struct chimera_vfs *vfs;

    vfs = calloc(1, sizeof(*vfs));

    return vfs;
} /* chimera_vfs_init */

void
chimera_vfs_destroy(struct chimera_vfs *vfs)
{
    struct chimera_vfs_module *module;
    struct chimera_vfs_share  *share;
    int                        i;

    while (vfs->shares) {
        share = vfs->shares;
        DL_DELETE(vfs->shares, share);
        free(share->name);
        free(share->path);
        free(share);
    }

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        module->destroy(vfs->module_private[i]);
    }

    free(vfs);
} /* chimera_vfs_destroy */

struct chimera_vfs_thread *
chimera_vfs_thread_init(
    struct evpl        *evpl,
    struct chimera_vfs *vfs)
{
    struct chimera_vfs_thread *thread;
    struct chimera_vfs_module *module;
    int                        i;

    thread      = calloc(1, sizeof(*thread));
    thread->vfs = vfs;

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        thread->module_private[i] = module->thread_init(
            evpl, vfs->module_private[i]);
    }

    return thread;
} /* chimera_vfs_thread_init */

void
chimera_vfs_thread_destroy(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;
    int                         i;

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = thread->vfs->modules[i];

        if (!module) {
            continue;
        }

        module->thread_destroy(thread->module_private[i]);
    }

    while (thread->free_requests) {
        request = thread->free_requests;
        DL_DELETE(thread->free_requests, request);
        free(request);
    }

    free(thread);
} /* chimera_vfs_thread_destroy */

void
chimera_vfs_register(
    struct chimera_vfs        *vfs,
    struct chimera_vfs_module *module)
{
    vfs->modules[module->fh_magic] = module;

    vfs->module_private[module->fh_magic] = module->init();
} /* chimera_vfs_register */

int
chimera_vfs_create_share(
    struct chimera_vfs *vfs,
    const char         *module_name,
    const char         *share_path,
    const char         *module_path)
{
    struct chimera_vfs_share  *share;
    struct chimera_vfs_module *module;
    int                        i;

    share = calloc(1, sizeof(*share));

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        if (strcmp(module->name, module_name) == 0) {
            share->module = module;
            break;
        }
    }

    if (!share->module) {
        chimera_vfs_error("chimera_vfs_create_share: module %s not found",
                          module_name);
        return -1;
    }

    share->name = strdup(share_path);
    share->path = strdup(module_path);

    DL_APPEND(vfs->shares, share);
    return 0;
} /* chimera_vfs_create_share */

void
chimera_vfs_getrootfh(
    struct chimera_vfs_thread *thread,
    void                      *fh,
    int                       *fh_len)
{
    uint8_t *fh8 = fh;

    fh8[0]  = 0;
    *fh_len = 1;

} /* chimera_vfs_getrootfh */

static void
chimera_vfs_lookup_path_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread    *thread   = request->thread;
    chimera_vfs_lookup_callback_t callback = request->proto_callback;

    callback(request->status,
             request->lookup_path.r_fh,
             request->lookup_path.r_fh_len,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);

} /* chimera_vfs_lookup_share_complete */

static void
chimera_vfs_lookup_share(
    struct chimera_vfs_thread    *thread,
    const char                   *name,
    uint32_t                      namelen,
    chimera_vfs_lookup_callback_t callback,
    void                         *private_data)
{
    struct chimera_vfs_share   *share;
    struct chimera_vfs_request *request;
    struct chimera_vfs_module  *module;

    chimera_vfs_debug(
        "chimera_vfs_lookup_share: name=%.*s",
        namelen, name);

    DL_FOREACH(thread->vfs->shares, share)

    if (strncmp(share->name, name, namelen) == 0) {
        break;
    }

    if (!share) {
        chimera_vfs_error("chimera_vfs_lookup_share: share %.*s not found",
                          namelen, name);

        callback(CHIMERA_VFS_ENOENT, NULL, 0, private_data);
        return;
    }

    module = share->module;

    request = chimera_vfs_request_alloc(thread);

    request->opcode               = CHIMERA_VFS_OP_LOOKUP_PATH;
    request->complete             = chimera_vfs_lookup_path_complete;
    request->lookup_path.path     = share->path;
    request->lookup_path.pathlen  = strlen(share->path);
    request->lookup_path.r_fh_len = 0;
    request->proto_callback       = callback;
    request->proto_private_data   = private_data;

    module->dispatch(request, thread->module_private[module->fh_magic]);
} /* chimera_vfs_lookup_share */

static void
chimera_vfs_lookup_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread    *thread   = request->thread;
    chimera_vfs_lookup_callback_t callback = request->proto_callback;

    callback(request->status,
             request->lookup.r_fh,
             request->lookup.r_fh_len,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);

} /* chimera_vfs_lookup_complete */

void
chimera_vfs_lookup(
    struct chimera_vfs_thread    *thread,
    const void                   *fh,
    int                           fhlen,
    const char                   *name,
    uint32_t                      namelen,
    chimera_vfs_lookup_callback_t callback,
    void                         *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;
    uint8_t                     fh_magic;
    char                        fhstr[80];

    format_hex(fhstr, sizeof(fhstr), fh, fhlen);
    chimera_vfs_debug("chimera_vfs_lookup: fh=%s name=%s",
                      fhstr, name);

    if (unlikely(fhlen < 1)) {
        callback(CHIMERA_VFS_ENOENT, NULL, 0, private_data);
        return;
    }

    fh_magic = *(uint8_t *) fh;

    if (fh_magic == CHIMERA_VFS_FH_MAGIC_ROOT) {
        chimera_vfs_lookup_share(thread, name, namelen, callback, private_data);
        return;
    }

    module = thread->vfs->modules[fh_magic];

    request                       = chimera_vfs_request_alloc(thread);
    request->opcode               = CHIMERA_VFS_OP_LOOKUP;
    request->complete             = chimera_vfs_lookup_complete;
    request->lookup.fh            = fh;
    request->lookup.fh_len        = fhlen;
    request->lookup.component     = name;
    request->lookup.component_len = namelen;
    request->proto_callback       = callback;
    request->proto_private_data   = private_data;

    module->dispatch(request, thread->module_private[fh_magic]);
} /* chimera_vfs_lookup */

static void
chimera_vfs_getattr_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread   = request->thread;
    chimera_vfs_getattr_callback_t callback = request->proto_callback;

    callback(request->status,
             request->getattr.r_attr_mask,
             &request->getattr.r_attr,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_getattr_complete */

static void
chimera_vfs_getattr_root(
    struct chimera_vfs_thread     *thread,
    uint64_t                       attr_mask,
    chimera_vfs_getattr_callback_t callback,
    void                          *private_data)
{
    struct chimera_vfs_attrs attr;

    memset(&attr, 0, sizeof(attr));

    /* Set dummy values for a directory */
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

    callback(CHIMERA_VFS_OK, attr_mask, &attr, private_data);
} /* chimera_vfs_getattr_root */

void
chimera_vfs_getattr(
    struct chimera_vfs_thread     *thread,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       req_attr_mask,
    chimera_vfs_getattr_callback_t callback,
    void                          *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;
    uint8_t                     fh_magic;

    if (unlikely(fhlen < 1)) {
        callback(CHIMERA_VFS_ENOENT, 0, NULL, private_data);
        return;
    }

    fh_magic = *(uint8_t *) fh;

    if (fh_magic == CHIMERA_VFS_FH_MAGIC_ROOT) {
        chimera_vfs_getattr_root(thread, req_attr_mask, callback, private_data);
        return;
    }

    module = thread->vfs->modules[fh_magic];

    request = chimera_vfs_request_alloc(thread);

    request->opcode             = CHIMERA_VFS_OP_GETATTR;
    request->complete           = chimera_vfs_getattr_complete;
    request->getattr.fh         = fh;
    request->getattr.fh_len     = fhlen;
    request->getattr.attr_mask  = req_attr_mask;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    module->dispatch(request, thread->module_private[fh_magic]);

} /* chimera_vfs_getattr */
