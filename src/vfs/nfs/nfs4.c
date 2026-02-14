// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "common/logging.h"

void
chimera_nfs4_dispatch(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            chimera_nfs4_mount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP_AT:
            chimera_nfs4_lookup_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            chimera_nfs4_getattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            chimera_nfs4_setattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR_AT:
            chimera_nfs4_mkdir_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKNOD_AT:
            chimera_nfs4_mknod_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE_AT:
            chimera_nfs4_remove_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            chimera_nfs4_readdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            chimera_nfs4_open_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_FH:
            chimera_nfs4_open_fh(thread, shared, request, private_data);
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
        case CHIMERA_VFS_OP_SYMLINK_AT:
            chimera_nfs4_symlink_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            chimera_nfs4_readlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME_AT:
            chimera_nfs4_rename_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK_AT:
            chimera_nfs4_link_at(thread, shared, request, private_data);
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
