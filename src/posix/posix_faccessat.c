// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_stat.h"

#ifndef AT_FDCWD
#define AT_FDCWD            -100
#endif /* ifndef AT_FDCWD */

#ifndef AT_EACCESS
#define AT_EACCESS          0x200
#endif /* ifndef AT_EACCESS */

#ifndef AT_SYMLINK_NOFOLLOW
#define AT_SYMLINK_NOFOLLOW 0x100
#endif /* ifndef AT_SYMLINK_NOFOLLOW */

struct chimera_posix_faccessat_state {
    struct chimera_posix_completion comp;
    int                             access_mode;
};

static void
chimera_posix_faccessat_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    const struct chimera_stat    *st,
    void                         *private_data)
{
    struct chimera_posix_faccessat_state *state = private_data;
    int                                   r, w, x;
    uid_t                                 proc_uid;
    gid_t                                 proc_gid;
    mode_t                                mode;

    if (status != CHIMERA_VFS_OK) {
        chimera_posix_complete(&state->comp, status);
        return;
    }

    if (state->access_mode == F_OK) {
        chimera_posix_complete(&state->comp, CHIMERA_VFS_OK);
        return;
    }

    proc_uid = getuid();
    proc_gid = getgid();
    mode     = st->st_mode;

    if (proc_uid == 0) {
        /* Root can read/write anything; execute requires at least one x bit */
        r = 1;
        w = 1;
        x = !!(mode & (S_IXUSR | S_IXGRP | S_IXOTH));
    } else if ((uint64_t) proc_uid == st->st_uid) {
        r = !!(mode & S_IRUSR);
        w = !!(mode & S_IWUSR);
        x = !!(mode & S_IXUSR);
    } else if ((uint64_t) proc_gid == st->st_gid) {
        r = !!(mode & S_IRGRP);
        w = !!(mode & S_IWGRP);
        x = !!(mode & S_IXGRP);
    } else {
        r = !!(mode & S_IROTH);
        w = !!(mode & S_IWOTH);
        x = !!(mode & S_IXOTH);
    }

    if ((state->access_mode & R_OK) && !r) {
        chimera_posix_complete(&state->comp, CHIMERA_VFS_EACCES);
        return;
    }

    if ((state->access_mode & W_OK) && !w) {
        chimera_posix_complete(&state->comp, CHIMERA_VFS_EACCES);
        return;
    }

    if ((state->access_mode & X_OK) && !x) {
        chimera_posix_complete(&state->comp, CHIMERA_VFS_EACCES);
        return;
    }

    chimera_posix_complete(&state->comp, CHIMERA_VFS_OK);
} /* chimera_posix_faccessat_callback */

static void
chimera_posix_faccessat_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_stat(thread, request);
} /* chimera_posix_faccessat_exec */

SYMBOL_EXPORT int
chimera_posix_faccessat(
    int         dirfd,
    const char *pathname,
    int         mode,
    int         flags)
{
    struct chimera_posix_client         *posix  = chimera_posix_get_global();
    struct chimera_posix_worker         *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request        req;
    struct chimera_posix_faccessat_state state;
    int                                  path_len;

    // AT_EACCESS and AT_SYMLINK_NOFOLLOW are not implemented
    (void) flags;

    // For now, only support AT_FDCWD
    if (dirfd != AT_FDCWD) {
        errno = ENOSYS;
        return -1;
    }

    chimera_posix_completion_init(&state.comp, &req);
    state.access_mode = mode;

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
    req.stat.private_data = &state;
    req.stat.path_len     = path_len;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_faccessat_exec);

    int err = chimera_posix_wait(&state.comp);

    chimera_posix_completion_destroy(&state.comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_faccessat */
