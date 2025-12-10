// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"

int
chimera_smb_parse_echo(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    if (request->request_struct_size != SMB2_ECHO_REQUEST_SIZE) {
        chimera_smb_error("Received SMB2 ECHO request with invalid struct size (%u expected %u)",
                          request->request_struct_size,
                          SMB2_ECHO_REQUEST_SIZE);
        return -1;
    }

    // Echo request has only struct size (2 bytes) and reserved (2 bytes)
    // Skip the reserved field
    evpl_iovec_cursor_skip(request_cursor, 2);

    return 0;
} /* chimera_smb_parse_echo */

void
chimera_smb_echo_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    // Echo reply: struct size (2 bytes) + reserved (2 bytes)
    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_ECHO_REPLY_SIZE);
    evpl_iovec_cursor_append_uint16(reply_cursor, 0); // Reserved
} /* chimera_smb_echo_reply */

void
chimera_smb_echo(struct chimera_smb_request *request)
{
    // Echo request is a simple keepalive message
    // Just mark it as successful
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_echo */