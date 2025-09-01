// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"

void
chimera_smb_negotiate(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_server_smb_shared *shared = thread->shared;
    struct timespec                   now, up, boot;

    clock_gettime(
        CLOCK_REALTIME,
        &now);

    clock_gettime(
        CLOCK_BOOTTIME,
        &up);

    boot.tv_sec = now.tv_sec  - up.tv_sec;

    if (now.tv_nsec >= up.tv_nsec) {
        boot.tv_nsec = now.tv_nsec - up.tv_nsec;
    } else {
        boot.tv_nsec = up.tv_nsec - now.tv_nsec;
        boot.tv_sec--;
    }

    request->negotiate.r_dialect           = 0x0210;
    request->negotiate.r_security_mode     = 0;
    request->negotiate.r_capabilities      = 0;
    request->negotiate.r_max_transact_size = 1048576;
    request->negotiate.r_max_read_size     = 1048576;
    request->negotiate.r_max_write_size    = 1048576;
    request->negotiate.r_system_time       = chimera_nt_time(&now);
    request->negotiate.r_server_start_time = chimera_nt_time(&boot);

    memcpy(request->negotiate.r_server_guid, shared->guid, SMB2_GUID_SIZE);

    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* smb_procs_negotiate */

void
chimera_smb_negotiate_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_NEGOTIATE_REPLY_SIZE);
    evpl_iovec_cursor_append_uint8(reply_cursor, request->negotiate.r_security_mode);
    evpl_iovec_cursor_append_uint16(reply_cursor, request->negotiate.r_dialect);
    evpl_iovec_cursor_append_blob(reply_cursor, request->negotiate.r_server_guid, SMB2_GUID_SIZE);
    evpl_iovec_cursor_append_uint32(reply_cursor, request->negotiate.r_capabilities);
    evpl_iovec_cursor_append_uint32(reply_cursor, request->negotiate.r_max_transact_size);
    evpl_iovec_cursor_append_uint32(reply_cursor, request->negotiate.r_max_read_size);
    evpl_iovec_cursor_append_uint32(reply_cursor, request->negotiate.r_max_write_size);
    evpl_iovec_cursor_append_uint64(reply_cursor, request->negotiate.r_system_time);
    evpl_iovec_cursor_append_uint64(reply_cursor, request->negotiate.r_server_start_time);
    /* Security Buffer Offset */
    evpl_iovec_cursor_append_uint16(reply_cursor, 0);

    /* Security Buffer Length */
    evpl_iovec_cursor_append_uint16(reply_cursor, 0);

} /* chimera_smb_negotiate_reply */

int
chimera_smb_parse_negotiate(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    int i;

    if (request->request_struct_size != SMB2_NEGOTIATE_REQUEST_SIZE) {
        chimera_smb_error("Received SMB2 NEGOTIATE request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_NEGOTIATE_REQUEST_SIZE);
        return -1;
    }
    evpl_iovec_cursor_get_uint16(request_cursor, &request->negotiate.dialect_count);
    evpl_iovec_cursor_get_uint8(request_cursor, &request->negotiate.security_mode);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->negotiate.capabilities);
    evpl_iovec_cursor_copy(request_cursor, request->negotiate.client_guid, 16);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->negotiate.negotiate_context_offset);
    evpl_iovec_cursor_get_uint16(request_cursor, &request->negotiate.negotiate_context_count);

    if (request->negotiate.dialect_count > SMB2_MAX_DIALECTS) {
        chimera_smb_error("Received SMB2 NEGOTIATE request with invalid dialect count (%u max %u)",
                          request->negotiate.dialect_count,
                          SMB2_MAX_DIALECTS);
        return -1;
    }
    for (i = 0; i < request->negotiate.dialect_count; i++) {
        evpl_iovec_cursor_get_uint16(request_cursor, &request->negotiate.dialects[i]);
    }

    evpl_iovec_cursor_skip(request_cursor,
                           request->negotiate.negotiate_context_offset -
                           evpl_iovec_cursor_consumed(request_cursor));

    for (i = 0; i < request->negotiate.negotiate_context_count; i++) {
        evpl_iovec_cursor_get_uint16(request_cursor, &request->negotiate.negotiate_context[i].type);
        evpl_iovec_cursor_get_uint16(request_cursor, &request->negotiate.negotiate_context[i].length);
        evpl_iovec_cursor_skip(request_cursor, 4);
        evpl_iovec_cursor_skip(request_cursor, request->negotiate.negotiate_context[i].length);
        evpl_iovec_cursor_align64(request_cursor);
    }

    return 0;
} /* chimera_smb_parse_negotiate */
