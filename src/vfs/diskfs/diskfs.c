// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * diskfs VFS module entry: operation dispatch and the module definition.
 * The implementation lives in the diskfs_*.c subsystem files; shared types,
 * macros and inline helpers are in diskfs_internal.h.
 */

#include "diskfs_internal.h"

/* Forward declarations (definitions below, in call-graph order) */

static void
diskfs_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data);


static void
diskfs_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_thread *thread = private_data;
    struct diskfs_shared *shared = thread->shared;

    if (unlikely(shared->root_fhlen == 0)) {
        diskfs_bootstrap(thread);
    }

    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            diskfs_mount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_UMOUNT:
            diskfs_umount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP_AT:
            diskfs_lookup_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            diskfs_getattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            diskfs_setattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR_AT:
            diskfs_mkdir_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKNOD_AT:
            diskfs_mknod_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE_AT:
            diskfs_remove_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            diskfs_readdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            diskfs_open_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_FH:
            diskfs_open_fh(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_CREATE_UNLINKED:
            diskfs_create_unlinked(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            diskfs_close(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READ:
            diskfs_read(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_WRITE:
            diskfs_write(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            diskfs_commit(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_ALLOCATE:
            diskfs_allocate(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SEEK:
            diskfs_seek(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SYMLINK_AT:
            diskfs_symlink_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            diskfs_readlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME_AT:
            diskfs_rename_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK_AT:
            diskfs_link_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_PUT_KEY:
            diskfs_put_key(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GET_KEY:
            diskfs_get_key(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_DELETE_KEY:
            diskfs_delete_key(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SEARCH_KEYS:
            diskfs_search_keys(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GET_XATTR:
            diskfs_get_xattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SET_XATTR:
            diskfs_set_xattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LIST_XATTRS:
            diskfs_list_xattrs(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE_XATTR:
            diskfs_remove_xattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GET_LAYOUT:
            diskfs_get_layout(thread, shared, request, private_data);
            break;
        default:
            chimera_diskfs_error("diskfs_dispatch: unknown operation %d",
                                 request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* diskfs_dispatch */


SYMBOL_EXPORT struct chimera_vfs_module vfs_diskfs = {
    .name         = "diskfs",
    .fh_magic     = CHIMERA_VFS_FH_MAGIC_DISKFS,
    .capabilities = CHIMERA_VFS_CAP_CREATE_UNLINKED | CHIMERA_VFS_CAP_FS | CHIMERA_VFS_CAP_KV |
        CHIMERA_VFS_CAP_FS_RELATIVE_OP | CHIMERA_VFS_CAP_XATTR | CHIMERA_VFS_CAP_LAYOUT |
        /* Require a real open so every file op carries a pinned inode in
         * handle->vfs_private (diskfs_open_fh_inode_cb), which read/write reuse
         * to skip per-I/O inode resolution. */
        CHIMERA_VFS_CAP_OPEN_FILE_REQUIRED | CHIMERA_VFS_CAP_FS_LOCK |
        CHIMERA_VFS_CAP_CHANGE,
    .init           = diskfs_init,
    .destroy        = diskfs_destroy,
    .thread_init    = diskfs_thread_init,
    .thread_destroy = diskfs_thread_destroy,
    .dispatch       = diskfs_dispatch,
};
