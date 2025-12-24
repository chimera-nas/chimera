// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_rename.h"

static void
chimera_posix_rename_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
}

static void
chimera_posix_rename_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_rename(thread, request);
}

SYMBOL_EXPORT int
chimera_posix_rename(
    const char *oldpath,
    const char *newpath)
{
    struct chimera_posix_client     *posix  = chimera_posix_get_global();
    struct chimera_posix_worker     *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request    req;
    struct chimera_posix_completion  comp;
    const char                      *source_slash;
    const char                      *dest_slash;
    int                              source_path_len;
    int                              dest_path_len;

    chimera_posix_completion_init(&comp, &req);

    source_path_len = strlen(oldpath);
    dest_path_len   = strlen(newpath);
    source_slash    = rindex(oldpath, '/');
    dest_slash      = rindex(newpath, '/');

    req.opcode                    = CHIMERA_CLIENT_OP_RENAME;
    req.rename.callback           = chimera_posix_rename_callback;
    req.rename.private_data       = &comp;
    req.rename.source_path_len    = source_path_len;
    req.rename.source_parent_len  = source_slash ? source_slash - oldpath : source_path_len;
    req.rename.dest_path_len      = dest_path_len;
    req.rename.dest_parent_len    = dest_slash ? dest_slash - newpath : dest_path_len;

    while (source_slash && *source_slash == '/') {
        source_slash++;
    }

    while (dest_slash && *dest_slash == '/') {
        dest_slash++;
    }

    req.rename.source_name_offset = source_slash ? source_slash - oldpath : -1;
    req.rename.dest_name_offset   = dest_slash ? dest_slash - newpath : -1;

    memcpy(req.rename.source_path, oldpath, source_path_len);
    memcpy(req.rename.dest_path, newpath, dest_path_len);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_rename_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
}
