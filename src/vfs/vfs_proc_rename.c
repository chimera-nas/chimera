// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_name_cache.h"
#include "vfs_attr_cache.h"
#include "common/misc.h"
#include "common/macros.h"

static void
chimera_vfs_rename_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread     = request->thread;
    struct chimera_vfs_name_cache *name_cache = thread->vfs->vfs_name_cache;
    chimera_vfs_rename_callback_t  callback   = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {

        /* XXX for now just remove any pertinent cache entries
         * ideally we would get the VFS to give us back enough info
         * about the moved and potentially replaced inodes to
         * populate the new info now
         */

        chimera_vfs_name_cache_insert(name_cache,
                                      request->fh_hash,
                                      request->fh,
                                      request->fh_len,
                                      request->rename.name_hash,
                                      request->rename.name,
                                      request->rename.namelen,
                                      NULL,
                                      0);

        chimera_vfs_name_cache_remove(name_cache,
                                      request->rename.new_fh_hash,
                                      request->rename.new_fh,
                                      request->rename.new_fhlen,
                                      request->rename.new_name_hash,
                                      request->rename.new_name,
                                      request->rename.new_namelen);
    }

    chimera_vfs_complete(request);

    callback(request->status, request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_rename_complete */

SYMBOL_EXPORT void
chimera_vfs_rename(
    struct chimera_vfs_thread    *thread,
    const void                   *fh,
    int                           fhlen,
    const char                   *name,
    int                           namelen,
    const void                   *new_fh,
    int                           new_fhlen,
    const char                   *new_name,
    int                           new_namelen,
    chimera_vfs_rename_callback_t callback,
    void                         *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, fh, fhlen);

    request->opcode               = CHIMERA_VFS_OP_RENAME;
    request->complete             = chimera_vfs_rename_complete;
    request->rename.name          = name;
    request->rename.namelen       = namelen;
    request->rename.name_hash     = chimera_vfs_hash(name, namelen);
    request->rename.new_fh        = new_fh;
    request->rename.new_fhlen     = new_fhlen;
    request->rename.new_fh_hash   = chimera_vfs_hash(new_fh, new_fhlen);
    request->rename.new_name      = new_name;
    request->rename.new_namelen   = new_namelen;
    request->rename.new_name_hash = chimera_vfs_hash(new_name, new_namelen);
    request->proto_callback       = callback;
    request->proto_private_data   = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_rename */