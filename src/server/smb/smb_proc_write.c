// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "common/macros.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_release.h"

static void
chimera_smb_write_callback(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    chimera_smb_complete_request(private_data, error_code ? SMB2_STATUS_INTERNAL_ERROR : SMB2_STATUS_SUCCESS);
} /* chimera_smb_write_callback */


void
chimera_smb_write(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_smb_open_file     *open_file;

    open_file = chimera_smb_open_file_lookup(request, &request->write.file_id);

    chimera_vfs_write(
        thread->vfs_thread,
        open_file->handle,
        request->write.offset,
        request->write.length,
        !!(request->write.flags & SMB2_WRITEFLAG_WRITE_THROUGH),
        0,
        0,
        request->write.iov,
        request->write.niov,
        chimera_smb_write_callback,
        request);
} /* chimera_smb_write */


int
chimera_smb_parse_write(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    uint16_t data_offset, blob_offset, blob_length;

    evpl_iovec_cursor_get_uint16(request_cursor, &data_offset);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->write.length);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->write.offset);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->write.file_id.pid);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->write.file_id.vid);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->write.channel);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->write.remaining);
    evpl_iovec_cursor_get_uint16(request_cursor, &blob_offset);
    evpl_iovec_cursor_get_uint16(request_cursor, &blob_length);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->write.flags);

    request->write.niov = evpl_iovec_cursor_move(request_cursor, request->write.iov, 64, request->write.length, 0);

    return 0;
} /* chimera_smb_parse_write */


void
chimera_smb_write_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_WRITE_REPLY_SIZE);
    evpl_iovec_cursor_append_uint32(reply_cursor, request->write.length);
    evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* remaining */
    evpl_iovec_cursor_append_uint16(reply_cursor, 0); /* write channel offset */
    evpl_iovec_cursor_append_uint16(reply_cursor, 0); /* write channel length */

} /* chimera_smb_write_reply */
