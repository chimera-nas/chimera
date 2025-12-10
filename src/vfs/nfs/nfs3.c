// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "common/logging.h"

void
chimera_nfs3_dispatch(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            chimera_nfs3_mount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_UMOUNT:
            chimera_nfs3_umount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP:
            chimera_nfs3_lookup(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            chimera_nfs3_getattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            chimera_nfs3_setattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR:
            chimera_nfs3_mkdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE:
            chimera_nfs3_remove(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            chimera_nfs3_readdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            chimera_nfs3_open_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN:
            chimera_nfs3_open(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            chimera_nfs3_close(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READ:
            chimera_nfs3_read(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_WRITE:
            chimera_nfs3_write(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            chimera_nfs3_commit(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SYMLINK:
            chimera_nfs3_symlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            chimera_nfs3_readlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME:
            chimera_nfs3_rename(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK:
            chimera_nfs3_link(thread, shared, request, private_data);
            break;
        default:
            chimera_error("nfs3", __FILE__, __LINE__,
                          "chimera_nfs3_dispatch: unknown operation %d",
                          request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* chimera_nfs3_dispatch */
