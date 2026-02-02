// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "common/macros.h"

static int
chimera_vfs_readdir_bounce_result_callback(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct chimera_vfs_request       *request = arg;
    struct chimera_vfs_readdir_entry *entry;
    int                               entry_size;
    char                             *entry_data;

    entry_size = (sizeof(*entry) + namelen + 7) & ~7;

    /* Check if we have enough space in the bounce buffer */
    if (request->readdir.bounce_offset + entry_size > request->readdir.bounce_iov.length) {
        return -1;
    }

    /* Pack the entry into the bounce buffer */
    entry_data = (char *) request->readdir.bounce_iov.data + request->readdir.bounce_offset;
    entry      = (struct chimera_vfs_readdir_entry *) entry_data;

    entry->inum    = inum;
    entry->cookie  = cookie;
    entry->namelen = namelen;
    entry->attrs   = *attrs;
    memcpy(entry_data + sizeof(*entry), name, namelen);

    request->readdir.bounce_offset += entry_size;

    return 0;
} /* chimera_vfs_readdir_bounce_result_callback */

static void
chimera_vfs_readdir_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_readdir_complete_t complete = request->proto_callback;

    chimera_vfs_complete(request);

    complete(request->status,
             request->readdir.handle,
             request->readdir.r_cookie,
             request->readdir.r_verifier,
             request->readdir.r_eof,
             &request->readdir.r_dir_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_readdir_complete */ /* chimera_vfs_readdir_complete */



static void
chimera_vfs_bounce_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_readdir_entry *entry;
    char                             *data_ptr;
    char                             *data_end;
    int                               rc = 0;

    request->proto_private_data = request->readdir.orig_private_data;

    data_ptr = request->readdir.bounce_iov.data;
    data_end = data_ptr + request->readdir.bounce_offset;

    while (data_ptr < data_end && rc == 0) {
        entry = (struct chimera_vfs_readdir_entry *) data_ptr;

        rc = request->readdir.orig_callback(
            entry->inum,
            entry->cookie,
            data_ptr + sizeof(*entry),
            entry->namelen,
            &entry->attrs,
            request->proto_private_data);

        if (rc != 0) {
            /* Application aborted the scan */
            request->readdir.r_eof    = 0;
            request->readdir.r_cookie = entry->cookie;
            break;
        }

        data_ptr += (sizeof(*entry) + entry->namelen + 7) & ~7;
    }

    evpl_iovec_release(request->thread->evpl, &request->readdir.bounce_iov);

    chimera_vfs_readdir_complete(request);
} /* chimera_vfs_bounce_complete */


SYMBOL_EXPORT void
chimera_vfs_readdir(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        attr_mask,
    uint64_t                        dir_attr_mask,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        flags,
    chimera_vfs_readdir_callback_t  callback,
    chimera_vfs_readdir_complete_t  complete,
    void                           *private_data)
{
    struct chimera_vfs_request *request;
    struct chimera_vfs_module  *module;

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);
    module  = request->module;

    request->opcode                         = CHIMERA_VFS_OP_READDIR;
    request->readdir.handle                 = handle;
    request->readdir.attr_mask              = attr_mask;
    request->readdir.cookie                 = cookie;
    request->readdir.verifier               = verifier;
    request->readdir.flags                  = flags;
    request->readdir.callback               = callback;
    request->readdir.r_dir_attr.va_req_mask = dir_attr_mask;
    request->readdir.r_dir_attr.va_set_mask = 0;
    request->readdir.r_verifier             = 0;
    request->proto_callback                 = complete;
    request->proto_private_data             = private_data;

    request->readdir.bounce_offset = 0;
    request->readdir.orig_callback = NULL;

    /* If this module is blocking then we need to bounce the results into the original thread
     * before making the caller provided result callback
     */

    if (module->capabilities & CHIMERA_VFS_CAP_BLOCKING) {

        evpl_iovec_alloc(thread->evpl, 64 * 1024, 8, 1, 0, &request->readdir.bounce_iov);

        request->readdir.orig_callback     = callback;
        request->readdir.orig_private_data = private_data;

        request->readdir.callback   = chimera_vfs_readdir_bounce_result_callback;
        request->proto_private_data = request;

        request->complete = chimera_vfs_bounce_complete;

    } else {
        request->complete = chimera_vfs_readdir_complete;
    }

    chimera_vfs_dispatch(request);
} /* chimera_vfs_readdir */ /* chimera_vfs_readdir */
