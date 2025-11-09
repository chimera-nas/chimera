// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "common/logging.h"

void
nfs4_dispatch(
    struct nfs_thread          *thread,
    struct nfs_shared          *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            nfs4_mount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP:
            nfs4_lookup(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            nfs4_getattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            nfs4_setattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR:
            nfs4_mkdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE:
            nfs4_remove(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            nfs4_readdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            nfs4_open_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN:
            nfs4_open(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_CREATE_UNLINKED:
            nfs4_create_unlinked(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            nfs4_close(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READ:
            nfs4_read(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_WRITE:
            nfs4_write(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            nfs4_commit(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SYMLINK:
            nfs4_symlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            nfs4_readlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME:
            nfs4_rename(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK:
            nfs4_link(thread, shared, request, private_data);
            break;
        default:
            chimera_error("nfs4", __FILE__, __LINE__,
                          "nfs4_dispatch: unknown operation %d",
                          request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* nfs4_dispatch */
