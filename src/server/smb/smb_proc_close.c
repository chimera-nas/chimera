// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "common/misc.h"
#include "vfs/vfs.h"

static void
chimera_smb_close_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    chimera_smb_open_file_release(request, request->close.open_file);

    chimera_smb_marshal_attrs(
        attr,
        &request->close.r_attrs);

    if (unlikely(error_code)) {
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
    } else {
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
    }
} /* chimera_smb_close_getattr_callback */


void
chimera_smb_close(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;


    request->close.open_file = chimera_smb_open_file_close(request, &request->close.file_id);

    if (unlikely(!request->close.open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    if (request->close.flags & SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB) {

        chimera_vfs_getattr(thread->vfs_thread,
                            &request->session_handle->session->cred,
                            request->close.open_file->handle,
                            CHIMERA_VFS_ATTR_MASK_STAT,
                            chimera_smb_close_getattr_callback,
                            request);

    } else {
        chimera_smb_open_file_release(request, request->close.open_file);

        memset(&request->close.r_attrs, 0, sizeof(request->close.r_attrs));

        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
    }

} /* chimera_smb_close */

void
chimera_smb_close_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_CLOSE_REPLY_SIZE);
    evpl_iovec_cursor_append_uint16(reply_cursor, request->close.flags);

    if (request->close.flags & SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB) {
        chimera_smb_append_network_open_info(reply_cursor, &request->close.r_attrs);
    } else {
        chimera_smb_append_null_network_open_info_null(reply_cursor);
    }

} /* chimera_smb_close_reply */

int
chimera_smb_parse_close(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{

    if (unlikely(request->request_struct_size != SMB2_CLOSE_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 CLOSE request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_CLOSE_REQUEST_SIZE);
        return -1;
    }

    evpl_iovec_cursor_get_uint16(request_cursor, &request->close.flags);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->close.file_id.pid);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->close.file_id.vid);


    return 0;
} /* chimera_smb_parse_close */
