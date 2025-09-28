// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb2.h"
#include "common/misc.h"

void
chimera_smb_ioctl(struct chimera_smb_request *request)
{
    struct chimera_smb_conn          *conn   = request->compound->conn;
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_server_smb_shared *shared = thread->shared;
    struct chimera_smb_open_file     *open_file;
    int                               status;

    switch (request->ioctl.ctl_code) {
        case SMB2_FSCTL_DFS_GET_REFERRALS:
            chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
            break;

        case SMB2_FSCTL_VALIDATE_NEGOTIATE_INFO:
            request->ioctl.r_capabilities = conn->capabilities;
            memcpy(request->ioctl.r_guid, shared->guid, 16);
            request->ioctl.r_security_mode = 0;
            request->ioctl.r_dialect       = conn->dialect;

            chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
            break;

        case SMB2_FSCTL_TRANSCEIVE_PIPE:

            open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);

            evpl_iovec_alloc(
                thread->evpl,
                65535,
                8,
                1,
                &request->ioctl.output_iov);

            status = open_file->pipe_transceive(request,
                                                request->ioctl.input_iov,
                                                request->ioctl.input_niov,
                                                &request->ioctl.output_iov);

            chimera_smb_open_file_release(request, open_file);

            if (status != 0) {
                evpl_iovec_release(&request->ioctl.output_iov);
                chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
                return;
            }

            chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
            break;

        case SMB2_FSCTL_QUERY_NETWORK_INTERFACE_INFO:
            chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
            break;
        default:
            chimera_smb_complete_request(request, SMB2_STATUS_NOT_IMPLEMENTED);
            break;
    } /* switch */
} /* chimera_smb_ioctl */

void
chimera_smb_ioctl_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    struct chimera_server_smb_shared *shared = request->compound->thread->shared;
    struct chimera_smb_nic_info      *nic_info;
    uint32_t                          output_offset = 64 + SMB2_IOCTL_REPLY_SIZE + 7;
    uint32_t                          output_length = 0;

    /* Calculate length based on IOCTL type */
    switch (request->ioctl.ctl_code) {
        case SMB2_FSCTL_VALIDATE_NEGOTIATE_INFO:
            output_length = 24; /* Capabilities(4) + GUID(16) + SecurityMode(1) + Reserved(1) + Dialect(2) */
            break;
        case SMB2_FSCTL_TRANSCEIVE_PIPE:
            output_length = request->ioctl.output_iov.length;
            break;
        case SMB2_FSCTL_QUERY_NETWORK_INTERFACE_INFO:
            output_length = 152 * shared->config.num_nic_info;
            break;
        default:
            output_length = 0;
            break;
    } /* switch */

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_IOCTL_REPLY_SIZE);
    evpl_iovec_cursor_append_uint32(reply_cursor, request->ioctl.ctl_code);
    evpl_iovec_cursor_append_uint64(reply_cursor, 0xffffffffffffffffULL); /* file_id.pid */
    evpl_iovec_cursor_append_uint64(reply_cursor, 0xffffffffffffffffULL); /* file_id.vid */
    evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* input offset */
    evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* input count */
    evpl_iovec_cursor_append_uint32(reply_cursor, output_offset); /* output_offset */
    evpl_iovec_cursor_append_uint32(reply_cursor, output_length); /* output_count */
    evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* flags */
    evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* reserved */

    /* Pad to 54 bytes for 8-byte aligned buffer offset */
    evpl_iovec_cursor_append_uint64(reply_cursor, 0);

    switch (request->ioctl.ctl_code) {
        case SMB2_FSCTL_VALIDATE_NEGOTIATE_INFO:
            evpl_iovec_cursor_append_uint32(reply_cursor, request->ioctl.r_capabilities);
            evpl_iovec_cursor_append_blob(reply_cursor, request->ioctl.r_guid, 16);
            evpl_iovec_cursor_append_uint8(reply_cursor, request->ioctl.r_security_mode);
            evpl_iovec_cursor_append_uint8(reply_cursor, 0); /* Reserved */
            evpl_iovec_cursor_append_uint16(reply_cursor, request->ioctl.r_dialect);
            break;
        case SMB2_FSCTL_TRANSCEIVE_PIPE:
            evpl_iovec_cursor_append_blob(reply_cursor, request->ioctl.output_iov.data, request->ioctl.output_iov.length
                                          );
            break;
        case SMB2_FSCTL_QUERY_NETWORK_INTERFACE_INFO:

            for (int i = 0; i < shared->config.num_nic_info; i++) {
                nic_info = &shared->config.nic_info[i];
                evpl_iovec_cursor_append_uint32(reply_cursor,  i == shared->config.num_nic_info - 1 ? 0 : 152); /* next */
                evpl_iovec_cursor_append_uint32(reply_cursor, i + 1); /* ifindex */
                evpl_iovec_cursor_append_uint32(reply_cursor, 0x00000001); /* capabilities (RSS) */
                evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* reserved */
                evpl_iovec_cursor_append_uint64(reply_cursor, nic_info->speed); /* speed */
                evpl_iovec_cursor_append_uint16(reply_cursor, nic_info->addr.ss_family == AF_INET ? 2 :
                                                10);                                                                    /* AF_INET */
                evpl_iovec_cursor_append_uint16(reply_cursor, 0); /* port */
                if (nic_info->addr.ss_family == AF_INET) {
                    evpl_iovec_cursor_append_blob(reply_cursor,
                                                  &((struct sockaddr_in *) &nic_info->addr)->sin_addr.s_addr,
                                                  4);                                                                   /* address: 10.67.25.209 */
                } else {
                    evpl_iovec_cursor_append_blob(reply_cursor,
                                                  &((struct sockaddr_in6 *) &nic_info->addr)->sin6_addr.
                                                  s6_addr,
                                                  16);                                                                  /* address: 10.67.25.209 */
                }
                evpl_iovec_cursor_zero(reply_cursor, 120); /* ifname length */
            }
            break;
        default:
            break;
    } /* switch */

} /* chimera_smb_ioctl_reply */

int
chimera_smb_parse_ioctl(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    int i;

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

    /* Parse IOCTL-specific input data if present */
    if (request->ioctl.input_count > 0) {
        /* Skip to the input data offset */
        evpl_iovec_cursor_skip(request_cursor,
                               request->ioctl.input_offset - evpl_iovec_cursor_consumed(request_cursor));

        switch (request->ioctl.ctl_code) {
            case SMB2_FSCTL_VALIDATE_NEGOTIATE_INFO:
                if (request->ioctl.input_count < 24) {
                    chimera_smb_error("VALIDATE_NEGOTIATE_INFO input too small (%u < 24)",
                                      request->ioctl.input_count);
                    return -1;
                }

                /* Parse validate negotiate info request */
                evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.vni_capabilities);
                evpl_iovec_cursor_copy(request_cursor, request->ioctl.vni_guid, 16);
                evpl_iovec_cursor_get_uint8(request_cursor, &request->ioctl.vni_security_mode);
                evpl_iovec_cursor_skip(request_cursor, 1); /* Reserved */
                evpl_iovec_cursor_get_uint16(request_cursor, &request->ioctl.vni_dialect_count);

                if (request->ioctl.vni_dialect_count > SMB2_MAX_DIALECTS) {
                    chimera_smb_error("VALIDATE_NEGOTIATE_INFO dialect count too large (%u > %u)",
                                      request->ioctl.vni_dialect_count, SMB2_MAX_DIALECTS);
                    return -1;
                }

                if (request->ioctl.input_count < 24 + (request->ioctl.vni_dialect_count * 2)) {
                    chimera_smb_error("VALIDATE_NEGOTIATE_INFO input too small for dialect count");
                    return -1;
                }

                for (i = 0; i < request->ioctl.vni_dialect_count; i++) {
                    evpl_iovec_cursor_get_uint16(request_cursor, &request->ioctl.vni_dialects[i]);
                }
                break;
            case SMB2_FSCTL_TRANSCEIVE_PIPE:
                request->ioctl.input_niov = evpl_iovec_cursor_move(request_cursor,
                                                                   request->ioctl.input_iov, 64,
                                                                   request->ioctl.
                                                                   input_count,
                                                                   0);
                break;
            default:
                /* Other IOCTLs don't need input parsing yet */
                break;
        } /* switch */
    }

    return 0;
} /* chimera_smb_parse_ioctl */