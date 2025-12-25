// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_stat.h"

#ifndef AT_FDCWD
#define AT_FDCWD -100
#endif

#ifndef AT_EACCESS
#define AT_EACCESS 0x200
#endif

#ifndef AT_SYMLINK_NOFOLLOW
#define AT_SYMLINK_NOFOLLOW 0x100
#endif

static void
chimera_posix_faccessat_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    const struct chimera_stat    *st,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
}

static void
chimera_posix_faccessat_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_stat(thread, request);
}

SYMBOL_EXPORT int
chimera_posix_faccessat(
    int         dirfd,
    const char *pathname,
    int         mode,
    int         flags)
{
    struct chimera_posix_client     *posix  = chimera_posix_get_global();
    struct chimera_posix_worker     *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request    req;
    struct chimera_posix_completion  comp;
    int                              path_len;

    // Note: We only check for file existence, not actual access permissions
    // Full implementation would require checking uid/gid against file mode
    // Also AT_EACCESS and AT_SYMLINK_NOFOLLOW are not implemented
    (void) mode;
    (void) flags;

    // For now, only support AT_FDCWD
    if (dirfd != AT_FDCWD) {
        errno = ENOSYS;
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    // Build path
    if (pathname[0] == '/') {
        path_len = strlen(pathname);
        memcpy(req.stat.path, pathname, path_len);
    } else {
        req.stat.path[0] = '/';
        path_len         = strlen(pathname);
        memcpy(req.stat.path + 1, pathname, path_len);
        path_len++;
    }

    req.opcode            = CHIMERA_CLIENT_OP_STAT;
    req.stat.callback     = chimera_posix_faccessat_callback;
    req.stat.private_data = &comp;
    req.stat.path_len     = path_len;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_faccessat_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    // If we got here, file exists and is accessible
    // Note: We don't actually check R_OK/W_OK/X_OK permissions
    return 0;
} /* chimera_posix_faccessat */
