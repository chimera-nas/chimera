// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

/*
 * Before every execution of the READDIR, and so before any retry of it.  The
 * SDK exposes no reset hook of its own: what the caller's entry callback
 * keeps has to be replaceable on the caller's side (the contract on
 * chimera_readdir_callback_t), so there is nothing here to take back.
 */
static void
chimera_readdir_reset(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data)
{
    (void) compound;
    (void) index;
    (void) private_data;
} /* chimera_readdir_reset */

/*
 * One entry as the backend produces it, handed to the caller's callback.  A
 * non-zero return from the callback stops the enumeration at this entry: the
 * page then ends with eof clear and the cookie of THIS entry, so a caller
 * that resumes from the cookie it was completed with sees what follows the
 * entry it took -- the readdir(3) shape src/posix builds on this.
 */
static int
chimera_readdir_append(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *private_data)
{
    struct chimera_client_request *request       = private_data;
    struct chimera_client_thread  *client_thread = request->thread;
    chimera_readdir_callback_t     callback      = request->readdir.callback;
    void                          *callback_arg  = request->readdir.private_data;
    struct chimera_dirent          dirent;

    (void) compound;
    (void) index;
    (void) attrs;

    dirent.ino     = inum;
    dirent.cookie  = cookie;
    dirent.namelen = namelen < 255 ? namelen : 255;
    memcpy(dirent.name, name, dirent.namelen);
    dirent.name[dirent.namelen] = '\0';

    return callback(client_thread, &dirent, callback_arg) ? -1 : 0;
} /* chimera_readdir_append */

static void
chimera_readdir_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request        *request       = private_data;
    struct chimera_client_thread         *client_thread = request->thread;
    chimera_readdir_complete_t            complete      = request->readdir.complete;
    void                                 *callback_arg  = request->readdir.private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    uint64_t                              cookie = 0;
    int                                   eof    = 0;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        /* Where the enumeration stopped, as the backend reported it: the
         * refused entry's cookie when the callback stopped it, the end
         * otherwise. */
        cookie = op->r_cookie;
        eof    = (int) op->eof;
    }

    /* Read out before the free: a freed sequence is recycled and reset. */
    chimera_vfs_compound_free(compound);

    chimera_client_request_free(client_thread, request);

    complete(client_thread, status, cookie, eof, callback_arg);
} /* chimera_readdir_sequence_complete */

static inline void
chimera_dispatch_readdir(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));

    /* The directory is the caller's open handle, lent with what it was really
     * opened with -- see open_flags on the request.  A READDIR would open the
     * directory as PATH|DIRECTORY for itself; an opendir(3) descriptor is a
     * data open with DIRECTORY, which serves it (the entry points guarantee
     * the DIRECTORY bit -- see chimera_readdir). */
    chimera_vfs_compound_add_puthandle(request->compound,
                                       request->readdir.handle,
                                       request->readdir.open_flags);

    /* Streaming, not staged: the caller's callback is driven per entry as the
     * per-op path drove it, and nothing is copied into the sequence.  No
     * attributes are asked for -- chimera_dirent carries none -- and EMIT_DOT
     * keeps "." and ".." in the listing, as readdir(3) expects. */
    chimera_vfs_compound_add_readdir_stream(request->compound,
                                            request->readdir.cookie,
                                            0, /* verifier */
                                            0, /* attr_mask */
                                            0, /* dir_attr_mask */
                                            CHIMERA_VFS_READDIR_EMIT_DOT,
                                            NULL, 0, /* no pattern */
                                            chimera_readdir_reset,
                                            chimera_readdir_append,
                                            request);

    chimera_vfs_compound_submit(request->compound,
                                chimera_readdir_sequence_complete, request);
} /* chimera_dispatch_readdir */
