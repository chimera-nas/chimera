// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "common/logging.h"

void
nfs3_dispatch(
    struct nfs_thread          *thread,
    struct nfs_shared          *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            nfs3_mount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_UMOUNT:
            nfs3_umount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP:
            nfs3_lookup(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            nfs3_getattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            nfs3_setattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR:
            nfs3_mkdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE:
            nfs3_remove(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            nfs3_readdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            nfs3_open_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN:
            nfs3_open(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_CREATE_UNLINKED:
            nfs3_create_unlinked(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            nfs3_close(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READ:
            nfs3_read(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_WRITE:
            nfs3_write(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            nfs3_commit(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SYMLINK:
            nfs3_symlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            nfs3_readlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME:
            nfs3_rename(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK:
            nfs3_link(thread, shared, request, private_data);
            break;
        default:
            chimera_error("nfs3", __FILE__, __LINE__,
                          "nfs3_dispatch: unknown operation %d",
                          request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* nfs3_dispatch */
