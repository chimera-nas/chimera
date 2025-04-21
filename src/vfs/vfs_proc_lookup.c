#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_name_cache.h"
#include "vfs_attr_cache.h"
#include "common/format.h"
#include "common/misc.h"
#include "common/macros.h"

static void
chimera_vfs_lookup_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread     = request->thread;
    struct chimera_vfs_name_cache *name_cache = thread->vfs->vfs_name_cache;
    struct chimera_vfs_attr_cache *attr_cache = thread->vfs->vfs_attr_cache;
    chimera_vfs_lookup_callback_t  callback   = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_name_cache_insert(name_cache,
                                      request->lookup.handle->fh_hash,
                                      request->lookup.handle->fh,
                                      request->lookup.handle->fh_len,
                                      request->lookup.component_hash,
                                      request->lookup.component,
                                      request->lookup.component_len,
                                      request->lookup.r_attr.va_fh,
                                      request->lookup.r_attr.va_fh_len);

        chimera_vfs_attr_cache_insert(attr_cache,
                                      chimera_vfs_hash(request->lookup.r_attr.va_fh, request->lookup.r_attr.
                                                       va_fh_len),
                                      request->lookup.r_attr.va_fh,
                                      request->lookup.r_attr.va_fh_len,
                                      &request->lookup.r_attr);

        chimera_vfs_attr_cache_insert(attr_cache,
                                      request->lookup.handle->fh_hash,
                                      request->lookup.handle->fh,
                                      request->lookup.handle->fh_len,
                                      &request->lookup.r_dir_attr);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             &request->lookup.r_attr,
             &request->lookup.r_dir_attr,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);

} /* chimera_vfs_lookup_complete */

SYMBOL_EXPORT void
chimera_vfs_lookup(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    uint32_t                        namelen,
    uint64_t                        attr_mask,
    uint64_t                        dir_attr_mask,
    chimera_vfs_lookup_callback_t   callback,
    void                           *private_data)
{
    struct chimera_vfs_name_cache *name_cache = thread->vfs->vfs_name_cache;
    struct chimera_vfs_attr_cache *attr_cache = thread->vfs->vfs_attr_cache;
    struct chimera_vfs_request    *request;
    uint64_t                       name_hash;
    int                            rc;
    struct chimera_vfs_attrs       cached_attr, cached_dir_attr;

    name_hash = chimera_vfs_hash(name, namelen);

    if (!(attr_mask & ~(CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE)) &&
        !(dir_attr_mask & ~(CHIMERA_VFS_ATTR_MASK_CACHEABLE))) {

        cached_attr.va_req_mask = 0;
        cached_attr.va_set_mask = 0;

        rc = chimera_vfs_name_cache_lookup(
            name_cache,
            handle->fh_hash,
            handle->fh,
            handle->fh_len,
            name_hash,
            name,
            namelen,
            cached_attr.va_fh,
            &cached_attr.va_fh_len);

        if (rc == 0) {

            if (cached_attr.va_fh_len == 0) {
                callback(CHIMERA_VFS_ENOENT,
                         &cached_attr,
                         &cached_dir_attr,
                         private_data);
                return;
            }

            rc = chimera_vfs_attr_cache_lookup(
                attr_cache,
                handle->fh_hash,
                handle->fh,
                handle->fh_len,
                &cached_dir_attr);

            if (rc == 0) {

                rc = chimera_vfs_attr_cache_lookup(
                    attr_cache,
                    chimera_vfs_hash(cached_attr.va_fh, cached_attr.va_fh_len),
                    cached_attr.va_fh,
                    cached_attr.va_fh_len,
                    &cached_attr);

                if (rc == 0) {
                    callback(CHIMERA_VFS_OK,
                             &cached_attr,
                             &cached_dir_attr,
                             private_data);
                    return;
                }
            }
        }
    }

    request = chimera_vfs_request_alloc_by_handle(thread, handle);


    request->opcode                        = CHIMERA_VFS_OP_LOOKUP;
    request->complete                      = chimera_vfs_lookup_complete;
    request->lookup.handle                 = handle;
    request->lookup.component              = name;
    request->lookup.component_len          = namelen;
    request->lookup.component_hash         = name_hash;
    request->lookup.r_attr.va_req_mask     = attr_mask | CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->lookup.r_attr.va_set_mask     = 0;
    request->lookup.r_dir_attr.va_req_mask = dir_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->lookup.r_dir_attr.va_set_mask = 0;
    request->proto_callback                = callback;
    request->proto_private_data            = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_lookup */
