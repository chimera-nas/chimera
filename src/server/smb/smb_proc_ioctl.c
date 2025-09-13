// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "smb_string.h"

void
chimera_smb_ioctl(struct chimera_smb_request *request)
{

    if (request->ioctl.ctl_code == SMB2_FSCTL_DFS_GET_REFERRALS) {
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    chimera_smb_complete_request(request, SMB2_STATUS_NOT_IMPLEMENTED);
} /* chimera_smb_ioctl */

void
chimera_smb_ioctl_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_IOCTL_REPLY_SIZE);
    evpl_iovec_cursor_append_uint32(reply_cursor, request->ioctl.ctl_code);
    evpl_iovec_cursor_append_uint64(reply_cursor, 0xffffffffffffffffULL);
    evpl_iovec_cursor_append_uint64(reply_cursor, 0xffffffffffffffffULL);
    evpl_iovec_cursor_append_uint32(reply_cursor, 0);
    evpl_iovec_cursor_append_uint32(reply_cursor, 0);
    evpl_iovec_cursor_append_uint32(reply_cursor, 0);
    evpl_iovec_cursor_append_uint32(reply_cursor, 0);
    evpl_iovec_cursor_append_uint32(reply_cursor, 0);
    evpl_iovec_cursor_append_uint32(reply_cursor, 0);

} /* chimera_smb_ioctl_reply */

int
chimera_smb_parse_ioctl(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    if (unlikely(request->request_struct_size != SMB2_IOCTL_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 IOCTL request with invalid struct size (%u expected %u)",
                          request->request_struct_size,
                          SMB2_IOCTL_REQUEST_SIZE);
        return -1;
    }


    evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.ctl_code);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.file_id.pid);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.file_id.vid);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.input_offset);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.input_count);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.max_input_response);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.output_offset);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.output_count);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.max_output_response);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.flags);

    return 0;
} /* chimera_smb_parse_ioctl */