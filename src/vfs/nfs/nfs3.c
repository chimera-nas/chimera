// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
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
        case CHIMERA_VFS_OP_LOOKUP_AT:
            chimera_nfs3_lookup_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            chimera_nfs3_getattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            chimera_nfs3_setattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR_AT:
            chimera_nfs3_mkdir_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKNOD_AT:
            chimera_nfs3_mknod_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE_AT:
            chimera_nfs3_remove_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            chimera_nfs3_readdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            chimera_nfs3_open_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_FH:
            chimera_nfs3_open_fh(thread, shared, request, private_data);
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
        case CHIMERA_VFS_OP_SYMLINK_AT:
            chimera_nfs3_symlink_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            chimera_nfs3_readlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME_AT:
            chimera_nfs3_rename_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK_AT:
            chimera_nfs3_link_at(thread, shared, request, private_data);
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
