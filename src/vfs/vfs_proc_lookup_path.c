#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "common/misc.h"
#include "common/macros.h"

static void
chimera_vfs_lookup_path_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data);

static inline void
chimera_vfs_lookup_path_open_dispatch(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_request *lp_request = private_data;
    struct chimera_vfs_thread  *thread     = lp_request->thread;
    const char                 *component;
    int                         componentlen;
    int                         final;

    if (error_code != CHIMERA_VFS_OK) {
        lp_request->lookup_path.callback(error_code,
                                         NULL,
                                         lp_request->lookup_path.private_data);
        chimera_vfs_request_free(thread, lp_request);
        return;
    }

    lp_request->lookup_path.handle = oh;

    component = lp_request->lookup_path.pathc;

    while (*lp_request->lookup_path.pathc != '/' && *lp_request->lookup_path.pathc != '\0') {
        lp_request->lookup_path.pathc++;
    }

    componentlen = lp_request->lookup_path.pathc - component;

    while (*lp_request->lookup_path.pathc == '/' && *lp_request->lookup_path.pathc != '\0') {
        lp_request->lookup_path.pathc++;
    }

    final = (*lp_request->lookup_path.pathc == '\0');

    chimera_vfs_lookup(
        thread,
        oh,
        component,
        componentlen,
        final ? lp_request->lookup_path.attr_mask : CHIMERA_VFS_ATTR_FH,
        0,
        chimera_vfs_lookup_path_complete,
        lp_request);

} /* chimera_vfs_lookup_path_open_dispatch */

static void
chimera_vfs_lookup_path_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_vfs_request *lp_request = private_data;
    struct chimera_vfs_thread  *thread     = lp_request->thread;
    int                         final      = (*lp_request->lookup_path.pathc == '\0');
    unsigned int                flags;

    chimera_vfs_release(thread, lp_request->lookup_path.handle);

    if (error_code != CHIMERA_VFS_OK) {
        lp_request->lookup_path.callback(error_code,
                                         NULL,
                                         lp_request->lookup_path.private_data);

        chimera_vfs_request_free(thread, lp_request);
        return;
    }

    if (final) {
        lp_request->lookup_path.callback(CHIMERA_VFS_OK,
                                         attr,
                                         lp_request->lookup_path.private_data);

        chimera_vfs_request_free(thread, lp_request);

    } else {

        flags = CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED;

        if (!final) {
            flags |= CHIMERA_VFS_OPEN_DIRECTORY;
        }

        memcpy(lp_request->lookup_path.next_fh, attr->va_fh, attr->va_fh_len);
        chimera_vfs_open(thread,
                         lp_request->lookup_path.next_fh,
                         attr->va_fh_len,
                         flags,
                         chimera_vfs_lookup_path_open_dispatch,
                         lp_request);
    }
} /* chimera_vfs_lookup_path_complete */

SYMBOL_EXPORT void
chimera_vfs_lookup_path(
    struct chimera_vfs_thread         *thread,
    const void                        *fh,
    int                                fhlen,
    const char                        *path,
    int                                pathlen,
    uint64_t                           attr_mask,
    chimera_vfs_lookup_path_callback_t callback,
    void                              *private_data)
{
    struct chimera_vfs_request *lp_request;

    while (*path == '/') {
        path++;
        pathlen--;
    }

    if (pathlen == 0) {
        struct chimera_vfs_attrs attr;

        attr.va_req_mask = attr_mask;
        attr.va_set_mask = CHIMERA_VFS_ATTR_FH;
        memcpy(attr.va_fh, fh, fhlen);
        attr.va_fh_len = fhlen;
        callback(CHIMERA_VFS_OK,
                 &attr,
                 private_data);
        return;
    }

    lp_request = chimera_vfs_request_alloc(thread, fh, fhlen);

    lp_request->lookup_path.path         = lp_request->plugin_data;
    lp_request->lookup_path.pathlen      = pathlen;
    lp_request->lookup_path.pathc        = lp_request->lookup_path.path;
    lp_request->lookup_path.handle       = NULL;
    lp_request->lookup_path.attr_mask    = attr_mask;
    lp_request->lookup_path.private_data = private_data;
    lp_request->lookup_path.callback     = callback;

    memcpy(lp_request->lookup_path.path, path, pathlen);

    lp_request->lookup_path.path[pathlen] = '\0';

    chimera_vfs_open(thread,
                     fh,
                     fhlen,
                     CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_vfs_lookup_path_open_dispatch,
                     lp_request);

} /* chimera_vfs_lookup_path */
