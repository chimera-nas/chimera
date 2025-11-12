// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "common/logging.h"

void
chimera_nfs4_dispatch(
    struct chimera_nfs_thread          *thread,
    struct chimera_nfs_shared          *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            chimera_nfs4_mount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP:
            chimera_nfs4_lookup(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            chimera_nfs4_getattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            chimera_nfs4_setattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR:
            chimera_nfs4_mkdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE:
            chimera_nfs4_remove(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            chimera_nfs4_readdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            chimera_nfs4_open_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN:
            chimera_nfs4_open(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            chimera_nfs4_close(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READ:
            chimera_nfs4_read(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_WRITE:
            chimera_nfs4_write(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            chimera_nfs4_commit(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SYMLINK:
            chimera_nfs4_symlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            chimera_nfs4_readlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME:
            chimera_nfs4_rename(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK:
            chimera_nfs4_link(thread, shared, request, private_data);
            break;
        default:
            chimera_error("nfs4", __FILE__, __LINE__,
                          "chimera_nfs4_dispatch: unknown operation %d",
                          request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* chimera_nfs4_dispatch */
