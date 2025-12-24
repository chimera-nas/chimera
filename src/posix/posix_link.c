// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"

static void
chimera_posix_link_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
}

static void
chimera_posix_link_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_link(thread, request);
}

int
chimera_posix_link(
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

    req.opcode                  = CHIMERA_CLIENT_OP_LINK;
    req.link.callback           = chimera_posix_link_callback;
    req.link.private_data       = &comp;
    req.link.source_path_len    = source_path_len;
    req.link.source_parent_len  = source_slash ? source_slash - oldpath : source_path_len;
    req.link.dest_path_len      = dest_path_len;
    req.link.dest_parent_len    = dest_slash ? dest_slash - newpath : dest_path_len;

    while (dest_slash && *dest_slash == '/') {
        dest_slash++;
    }

    req.link.dest_name_offset = dest_slash ? dest_slash - newpath : -1;

    memcpy(req.link.source_path, oldpath, source_path_len);
    memcpy(req.link.dest_path, newpath, dest_path_len);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_link_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
}
