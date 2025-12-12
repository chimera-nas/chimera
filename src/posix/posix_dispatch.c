// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "posix_internal.h"

void
chimera_posix_dispatch_request(struct chimera_posix_worker *worker, struct chimera_posix_request *request)
{
    switch (request->type) {
        case CHIMERA_POSIX_REQ_OPEN:
            chimera_posix_exec_open(worker, request);
            break;
        case CHIMERA_POSIX_REQ_CLOSE:
            chimera_posix_exec_close(worker, request);
            break;
        case CHIMERA_POSIX_REQ_READ:
            chimera_posix_exec_read(worker, request);
            break;
        case CHIMERA_POSIX_REQ_WRITE:
            chimera_posix_exec_write(worker, request);
            break;
        case CHIMERA_POSIX_REQ_MKDIR:
            chimera_posix_exec_mkdir(worker, request);
            break;
        case CHIMERA_POSIX_REQ_SYMLINK:
            chimera_posix_exec_symlink(worker, request);
            break;
        case CHIMERA_POSIX_REQ_LINK:
            chimera_posix_exec_link(worker, request);
            break;
        case CHIMERA_POSIX_REQ_REMOVE:
            chimera_posix_exec_remove(worker, request);
            break;
        case CHIMERA_POSIX_REQ_RENAME:
            chimera_posix_exec_rename(worker, request);
            break;
        case CHIMERA_POSIX_REQ_READLINK:
            chimera_posix_exec_readlink(worker, request);
            break;
        case CHIMERA_POSIX_REQ_STAT:
            chimera_posix_exec_stat(worker, request);
            break;
        case CHIMERA_POSIX_REQ_MOUNT:
            chimera_posix_exec_mount(worker, request);
            break;
        case CHIMERA_POSIX_REQ_UMOUNT:
            chimera_posix_exec_umount(worker, request);
            break;
    }
}

