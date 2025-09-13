// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "smb_string.h"

static void
chimera_smb_flush_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    chimera_smb_complete_request(request, error_code ? SMB2_STATUS_INTERNAL_ERROR : SMB2_STATUS_SUCCESS);
} /* chimera_smb_flush_callback */

void
chimera_smb_flush(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_smb_open_file     *open_file;

    open_file = chimera_smb_open_file_lookup(request, &request->flush.file_id);

    chimera_vfs_commit(
        thread->vfs_thread,
        open_file->handle,
        0,
        0xffffffffffffffffULL,
        0,
        0,
        chimera_smb_flush_callback,
        request);
} /* chimera_smb_ioctl */

void
chimera_smb_flush_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_FLUSH_REPLY_SIZE);
    evpl_iovec_cursor_append_uint16(reply_cursor, 0);
} /* chimera_smb_flush_reply */

int
chimera_smb_parse_flush(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    if (unlikely(request->request_struct_size != SMB2_FLUSH_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 FLUSH request with invalid struct size (%u expected %u)",
                          request->request_struct_size,
                          SMB2_FLUSH_REQUEST_SIZE);
        return -1;
    }

    evpl_iovec_cursor_get_uint64(request_cursor, &request->flush.file_id.pid);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->flush.file_id.vid);

    return 0;
} /* chimera_smb_parse_ioctl */