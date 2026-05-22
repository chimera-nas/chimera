// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_string.h"
#include "smb2.h"
#include "common/misc.h"

/* MS-SMB2 3.3.5.15.12: validate an FSCTL_VALIDATE_NEGOTIATE_INFO request
 * against the connection's negotiated state.  Returns true if it matches and
 * a response should be sent; false if the connection MUST be terminated. */
static bool
chimera_smb_validate_negotiate_info_ok(
    const struct chimera_smb_conn          *conn,
    const struct chimera_server_smb_shared *shared,
    const struct chimera_smb_request       *request)
{
    uint16_t gcd = 0;
    int      i, j;

    /* The response is 24 bytes; an output buffer too small to hold it is a
     * protocol violation. */
    if (request->ioctl.max_output_response < 24) {
        return false;
    }

    /* VALIDATE_NEGOTIATE_INFO is not used on SMB 3.1.1 (preauth integrity
     * supersedes it); receiving it there terminates the connection. */
    if (conn->dialect == SMB2_DIALECT_3_1_1) {
        return false;
    }

    if (memcmp(request->ioctl.vni_guid, conn->client_guid, 16) != 0) {
        return false;
    }
    if (request->ioctl.vni_security_mode != conn->client_security_mode) {
        return false;
    }
    if (request->ioctl.vni_capabilities != conn->client_capabilities) {
        return false;
    }

    /* The greatest dialect common to the request's list and the server's
     * supported set must equal the dialect negotiated on this connection. */
    for (i = 0; i < request->ioctl.vni_dialect_count; i++) {
        uint16_t cand = request->ioctl.vni_dialects[i];
        for (j = 0; j < shared->config.num_dialects; j++) {
            if (shared->config.dialects[j] == cand && cand > gcd) {
                gcd = cand;
            }
        }
    }
    if (gcd != conn->dialect) {
        return false;
    }

    return true;
} /* chimera_smb_validate_negotiate_info_ok */

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
            /* No DFS support; tell the client to stop asking. */
            chimera_smb_complete_request(request, SMB2_STATUS_FS_DRIVER_REQUIRED);
            break;

        case SMB2_FSCTL_VALIDATE_NEGOTIATE_INFO:
            if (!chimera_smb_validate_negotiate_info_ok(conn, shared, request)) {
                /* MS-SMB2 3.3.5.15.12: a mismatch (or a 3.1.1 connection)
                 * MUST terminate the transport connection with no reply.
                 * Mirror the signing-failure teardown: drop the compound and
                 * close the bind. */
                struct chimera_smb_compound *compound = request->compound;

                chimera_smb_compound_free(thread, compound);
                evpl_close(thread->evpl, conn->bind);
                return;
            }

            request->ioctl.r_capabilities = conn->capabilities;
            memcpy(request->ioctl.r_guid, shared->guid, 16);
            /* Must echo the SecurityMode advertised in NEGOTIATE; a mismatch
             * looks like a downgrade attack and the client drops the
             * connection (MS-SMB2 3.3.5.15.12). */
            request->ioctl.r_security_mode = SMB2_SIGNING_ENABLED;
            if (shared->config.signing_required) {
                request->ioctl.r_security_mode |= SMB2_SIGNING_REQUIRED;
            }
            request->ioctl.r_dialect = conn->dialect;

            chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
            break;

        case SMB2_FSCTL_TRANSCEIVE_PIPE:

            open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);

            if (unlikely(!open_file)) {
                evpl_iovecs_release(thread->evpl, request->ioctl.input_iov, request->ioctl.input_niov);
                chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
                return;
            }

            evpl_iovec_alloc(
                thread->evpl,
                65535,
                8,
                1,
                0, &request->ioctl.output_iov);

            status = open_file->pipe_transceive(request,
                                                request->ioctl.input_iov,
                                                request->ioctl.input_niov,
                                                &request->ioctl.output_iov);

            chimera_smb_open_file_release(request, open_file);

            evpl_iovecs_release(thread->evpl, request->ioctl.input_iov, request->ioctl.input_niov);

            if (status != 0) {
                evpl_iovec_release(thread->evpl, &request->ioctl.output_iov);
                chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
                return;
            }

            chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
            break;

        case SMB2_FSCTL_QUERY_NETWORK_INTERFACE_INFO:
            chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
            break;

        case SMB2_FSCTL_SET_REPARSE_POINT:
            chimera_smb_ioctl_set_reparse(request);
            break;

        case SMB2_FSCTL_GET_REPARSE_POINT:
            chimera_smb_ioctl_get_reparse(request);
            break;

        case SMB2_FSCTL_SET_SPARSE:
            chimera_smb_ioctl_set_sparse(request);
            break;

        case SMB2_FSCTL_SET_ZERO_DATA:
            chimera_smb_ioctl_set_zero_data(request);
            break;

        case SMB2_FSCTL_QUERY_ALLOCATED_RANGES:
            chimera_smb_ioctl_query_allocated_ranges(request);
            break;

        case SMB2_FSCTL_SRV_REQUEST_RESUME_KEY:
            chimera_smb_ioctl_request_resume_key(request);
            break;

        case SMB2_FSCTL_SRV_COPYCHUNK:
        case SMB2_FSCTL_SRV_COPYCHUNK_WRITE:
            chimera_smb_ioctl_copychunk(request);
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
    uint32_t                          input_offset  = 0x70;
    uint32_t                          input_length  = 0;
    uint32_t                          output_offset = input_offset + input_length;
    uint32_t                          output_length = 0;
    uint32_t                          caps          = 0;

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
        case SMB2_FSCTL_GET_REPARSE_POINT:
            output_length = request->ioctl.rp_response_len;
            break;
        case SMB2_FSCTL_QUERY_ALLOCATED_RANGES:
            output_length = request->ioctl.sp_qar_count * 16;
            break;
        case SMB2_FSCTL_SRV_REQUEST_RESUME_KEY:
            output_length = 32; /* ResumeKey(24) + ContextLength(4) + Context(pad 4) */
            break;
        case SMB2_FSCTL_SRV_COPYCHUNK:
        case SMB2_FSCTL_SRV_COPYCHUNK_WRITE:
            output_length = 12; /* ChunksWritten + ChunkBytesWritten + TotalBytesWritten */
            break;
    } /* switch */

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_IOCTL_REPLY_SIZE);
    evpl_iovec_cursor_append_uint32(reply_cursor, request->ioctl.ctl_code);
    evpl_iovec_cursor_append_uint64(reply_cursor, 0xffffffffffffffffULL); /* file_id.pid */
    evpl_iovec_cursor_append_uint64(reply_cursor, 0xffffffffffffffffULL); /* file_id.vid */
    evpl_iovec_cursor_append_uint32(reply_cursor, input_offset); /* input offset */
    evpl_iovec_cursor_append_uint32(reply_cursor, input_length); /* input count */
    evpl_iovec_cursor_append_uint32(reply_cursor, output_offset); /* output_offset */
    evpl_iovec_cursor_append_uint32(reply_cursor, output_length); /* output_count */
    evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* flags */
    evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* reserved */

    /* Pad to 54 bytes for 8-byte aligned buffer offset */
    //evpl_iovec_cursor_append_uint64(reply_cursor, 0);

    switch (request->ioctl.ctl_code) {
        case SMB2_FSCTL_VALIDATE_NEGOTIATE_INFO:
            evpl_iovec_cursor_append_uint32(reply_cursor, request->ioctl.r_capabilities);
            evpl_iovec_cursor_append_blob(reply_cursor, request->ioctl.r_guid, 16);
            evpl_iovec_cursor_append_uint8(reply_cursor, request->ioctl.r_security_mode);
            evpl_iovec_cursor_append_uint8(reply_cursor, 0); /* Reserved */
            evpl_iovec_cursor_append_uint16(reply_cursor, request->ioctl.r_dialect);
            break;
        case SMB2_FSCTL_TRANSCEIVE_PIPE:
            evpl_iovec_cursor_inject_unaligned(reply_cursor,
                                               &request->ioctl.output_iov,
                                               1,
                                               request->ioctl.output_iov.length);
            break;
        case SMB2_FSCTL_QUERY_NETWORK_INTERFACE_INFO:

            for (int i = 0; i < shared->config.num_nic_info; i++) {
                nic_info = &shared->config.nic_info[i];

                caps = 0x1; /* RSS */

                if (nic_info->rdma) {
                    caps |= 0x2; /* RDMA */
                }

                evpl_iovec_cursor_append_uint32(reply_cursor,  i == shared->config.num_nic_info - 1 ? 0 : 152); /* next */
                evpl_iovec_cursor_append_uint32(reply_cursor, i + 1); /* ifindex */
                evpl_iovec_cursor_append_uint32(reply_cursor, caps); /* capabilities (RSS) */
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
        case SMB2_FSCTL_GET_REPARSE_POINT:
            evpl_iovec_cursor_append_blob(reply_cursor,
                                          request->ioctl.rp_response,
                                          request->ioctl.rp_response_len);
            break;
        case SMB2_FSCTL_QUERY_ALLOCATED_RANGES:
            for (uint32_t qi = 0; qi < request->ioctl.sp_qar_count; qi++) {
                evpl_iovec_cursor_append_uint64(reply_cursor, request->ioctl.sp_qar_ranges[qi].offset);
                evpl_iovec_cursor_append_uint64(reply_cursor, request->ioctl.sp_qar_ranges[qi].length);
            }
            break;
        case SMB2_FSCTL_SRV_REQUEST_RESUME_KEY:
            /* ResumeKey (24): encode the source open's FileId; the client
             * treats it opaquely and echoes it back in COPYCHUNK. */
            evpl_iovec_cursor_append_uint64(reply_cursor, request->ioctl.file_id.pid);
            evpl_iovec_cursor_append_uint64(reply_cursor, request->ioctl.file_id.vid);
            evpl_iovec_cursor_append_uint64(reply_cursor, 0);
            evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* ContextLength */
            evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* Context (pad) */
            break;
        case SMB2_FSCTL_SRV_COPYCHUNK:
        case SMB2_FSCTL_SRV_COPYCHUNK_WRITE:
            evpl_iovec_cursor_append_uint32(reply_cursor, request->ioctl.cc_chunks_written);
            evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* ChunkBytesWritten */
            evpl_iovec_cursor_append_uint32(reply_cursor, (uint32_t) request->ioctl.cc_total_written);
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
        request->status = SMB2_STATUS_INVALID_PARAMETER;
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

    /* SET_SPARSE with no input buffer means SetSparse = TRUE. */
    request->ioctl.sp_set_sparse = 1;

    /* Parse IOCTL-specific input data if present */
    if (request->ioctl.input_count > 0) {
        /* Skip to the input data offset */
        evpl_iovec_cursor_skip(request_cursor,
                               request->ioctl.input_offset - evpl_iovec_cursor_consumed(request_cursor));

        switch (request->ioctl.ctl_code) {
            case SMB2_FSCTL_DFS_GET_REFERRALS:
                /* Handled in the dispatcher; no input parsing needed. */
                break;
            case SMB2_FSCTL_VALIDATE_NEGOTIATE_INFO:
                if (request->ioctl.input_count < 24) {
                    chimera_smb_error("VALIDATE_NEGOTIATE_INFO input too small (%u < 24)",
                                      request->ioctl.input_count);
                    request->status = SMB2_STATUS_INVALID_PARAMETER;
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
                    request->status = SMB2_STATUS_INVALID_PARAMETER;
                    return -1;
                }

                if (request->ioctl.input_count < 24 + (request->ioctl.vni_dialect_count * 2)) {
                    chimera_smb_error("VALIDATE_NEGOTIATE_INFO input too small for dialect count");
                    request->status = SMB2_STATUS_INVALID_PARAMETER;
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
                                                                   1);
                break;
            case SMB2_FSCTL_SET_REPARSE_POINT:
            {
                uint16_t reparse_data_len;
                uint16_t reserved;

                if (request->ioctl.input_count < 8) {
                    chimera_smb_error("SET_REPARSE_POINT input too small (%u < 8)",
                                      request->ioctl.input_count);
                    request->status = SMB2_STATUS_INVALID_PARAMETER;
                    return -1;
                }

                evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.rp_reparse_tag);
                evpl_iovec_cursor_get_uint16(request_cursor, &reparse_data_len);
                evpl_iovec_cursor_get_uint16(request_cursor, &reserved);

                if (request->ioctl.rp_reparse_tag == SMB2_IO_REPARSE_TAG_NFS) {

                    if (request->ioctl.input_count < 16) {
                        chimera_smb_error("SET_REPARSE_POINT NFS input too small (%u < 16)",
                                          request->ioctl.input_count);
                        request->status = SMB2_STATUS_INVALID_PARAMETER;
                        return -1;
                    }

                    evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.rp_nfs_type);

                    switch (request->ioctl.rp_nfs_type) {
                        case SMB2_NFS_SPECFILE_LNK:
                        {
                            int utf16_data_len = reparse_data_len - 8;
                            int utf8_len;

                            if (utf16_data_len <= 0 || utf16_data_len > (CHIMERA_VFS_PATH_MAX - 1) * 2) {
                                chimera_smb_error("SET_REPARSE_POINT LNK: invalid target len %d",
                                                  utf16_data_len);
                                request->status = SMB2_STATUS_INVALID_PARAMETER;
                                return -1;
                            }

                            {
                                uint16_t                          utf16_buf[CHIMERA_VFS_PATH_MAX];
                                struct chimera_server_smb_thread *thread = request->compound->thread;

                                evpl_iovec_cursor_copy(request_cursor, utf16_buf, utf16_data_len);

                                utf8_len = chimera_smb_utf16le_to_utf8(
                                    &thread->iconv_ctx,
                                    utf16_buf,
                                    utf16_data_len,
                                    request->ioctl.rp_target,
                                    CHIMERA_VFS_PATH_MAX - 1);

                                if (utf8_len < 0) {
                                    chimera_smb_error("SET_REPARSE_POINT LNK: UTF-16LE conversion failed");
                                    request->status = SMB2_STATUS_INVALID_PARAMETER;
                                    return -1;
                                }

                                request->ioctl.rp_target[utf8_len] = '\0';
                                request->ioctl.rp_target_len       = utf8_len;

                                /* Convert Windows backslashes to Unix forward slashes */
                                for (i = 0; i < utf8_len; i++) {
                                    if (request->ioctl.rp_target[i] == '\\') {
                                        request->ioctl.rp_target[i] = '/';
                                    }
                                }
                            }
                            break;
                        }
                        case SMB2_NFS_SPECFILE_CHR:
                        case SMB2_NFS_SPECFILE_BLK:
                            if (reparse_data_len < 16) {
                                chimera_smb_error("SET_REPARSE_POINT CHR/BLK: data too small (%u < 16)",
                                                  reparse_data_len);
                                request->status = SMB2_STATUS_INVALID_PARAMETER;
                                return -1;
                            }
                            evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.rp_device_major);
                            evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.rp_device_minor);
                            break;
                        case SMB2_NFS_SPECFILE_FIFO:
                        case SMB2_NFS_SPECFILE_SOCK:
                            /* No additional data */
                            break;
                        default:
                            chimera_smb_info("SET_REPARSE_POINT: unsupported NFS type 0x%llx",
                                             (unsigned long long) request->ioctl.rp_nfs_type);
                            break;
                    } /* switch nfs_type */

                } else if (request->ioctl.rp_reparse_tag == SMB2_IO_REPARSE_TAG_SYMLINK) {

                    uint16_t sub_name_offset, sub_name_length;
                    uint16_t print_name_offset, print_name_length;
                    uint32_t symlink_flags;
                    int      utf8_len;

                    if (reparse_data_len < 12) {
                        chimera_smb_error("SET_REPARSE_POINT SYMLINK: data too small (%u < 12)",
                                          reparse_data_len);
                        request->status = SMB2_STATUS_INVALID_PARAMETER;
                        return -1;
                    }

                    evpl_iovec_cursor_get_uint16(request_cursor, &sub_name_offset);
                    evpl_iovec_cursor_get_uint16(request_cursor, &sub_name_length);
                    evpl_iovec_cursor_get_uint16(request_cursor, &print_name_offset);
                    evpl_iovec_cursor_get_uint16(request_cursor, &print_name_length);
                    evpl_iovec_cursor_get_uint32(request_cursor, &symlink_flags);

                    if (sub_name_length == 0 ||
                        sub_name_length > (CHIMERA_VFS_PATH_MAX - 1) * 2) {
                        chimera_smb_error("SET_REPARSE_POINT SYMLINK: invalid sub_name_length %u",
                                          sub_name_length);
                        request->status = SMB2_STATUS_INVALID_PARAMETER;
                        return -1;
                    }

                    /* Skip to SubstituteName within PathBuffer */
                    if (sub_name_offset > 0) {
                        evpl_iovec_cursor_skip(request_cursor, sub_name_offset);
                    }

                    {
                        uint16_t                          utf16_buf[CHIMERA_VFS_PATH_MAX];
                        struct chimera_server_smb_thread *thread = request->compound->thread;

                        evpl_iovec_cursor_copy(request_cursor, utf16_buf, sub_name_length);

                        utf8_len = chimera_smb_utf16le_to_utf8(
                            &thread->iconv_ctx,
                            utf16_buf,
                            sub_name_length,
                            request->ioctl.rp_target,
                            CHIMERA_VFS_PATH_MAX - 1);

                        if (utf8_len < 0) {
                            chimera_smb_error("SET_REPARSE_POINT SYMLINK: UTF-16LE conversion failed");
                            request->status = SMB2_STATUS_INVALID_PARAMETER;
                            return -1;
                        }

                        request->ioctl.rp_target[utf8_len] = '\0';
                        request->ioctl.rp_target_len       = utf8_len;

                        /* Convert Windows backslashes to Unix forward slashes */
                        for (i = 0; i < utf8_len; i++) {
                            if (request->ioctl.rp_target[i] == '\\') {
                                request->ioctl.rp_target[i] = '/';
                            }
                        }
                    }

                    /* Treat as NFS LNK for the handler */
                    request->ioctl.rp_nfs_type = SMB2_NFS_SPECFILE_LNK;

                } else {
                    chimera_smb_info("SET_REPARSE_POINT: unsupported tag 0x%08x",
                                     request->ioctl.rp_reparse_tag);
                    /* Mark tag as 0 so handler knows to skip */
                    request->ioctl.rp_reparse_tag = 0;
                }
                break;
            }
            case SMB2_FSCTL_SET_SPARSE:
                /* FILE_SET_SPARSE_BUFFER: optional single SetSparse byte. */
                evpl_iovec_cursor_get_uint8(request_cursor, &request->ioctl.sp_set_sparse);
                break;
            case SMB2_FSCTL_SET_ZERO_DATA:
                /* FILE_ZERO_DATA_INFORMATION: int64 FileOffset, int64 BeyondFinalZero. */
                if (request->ioctl.input_count < 16) {
                    chimera_smb_error("SET_ZERO_DATA input too small (%u < 16)",
                                      request->ioctl.input_count);
                    request->status = SMB2_STATUS_INVALID_PARAMETER;
                    return -1;
                }
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.sp_zero_offset);
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.sp_zero_beyond);
                break;
            case SMB2_FSCTL_QUERY_ALLOCATED_RANGES:
                /* FILE_ALLOCATED_RANGE_BUFFER: int64 FileOffset, int64 Length. */
                if (request->ioctl.input_count < 16) {
                    chimera_smb_error("QUERY_ALLOCATED_RANGES input too small (%u < 16)",
                                      request->ioctl.input_count);
                    request->status = SMB2_STATUS_INVALID_PARAMETER;
                    return -1;
                }
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.sp_qar_offset);
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.sp_qar_length);
                break;
            case SMB2_FSCTL_SRV_COPYCHUNK:
            case SMB2_FSCTL_SRV_COPYCHUNK_WRITE: {
                /* SRV_COPYCHUNK_COPY: SourceKey(24) + ChunkCount(4) +
                 * Reserved(4) + ChunkCount * SRV_COPYCHUNK{ SourceOffset(8),
                 * TargetOffset(8), Length(4), Reserved(4) }. */
                uint32_t reserved, cc, i;

                if (request->ioctl.input_count < 32) {
                    request->status = SMB2_STATUS_INVALID_PARAMETER;
                    return -1;
                }
                /* The 24-byte SourceKey encodes the source open's FileId. */
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.cc_src_file_id.pid);
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.cc_src_file_id.vid);
                evpl_iovec_cursor_skip(request_cursor, 8); /* remainder of resume key */
                evpl_iovec_cursor_get_uint32(request_cursor, &cc);
                evpl_iovec_cursor_get_uint32(request_cursor, &reserved);

                if (cc > CHIMERA_SMB_COPYCHUNK_MAX ||
                    request->ioctl.input_count < 32 + (uint64_t) cc * 24) {
                    request->status = SMB2_STATUS_INVALID_PARAMETER;
                    return -1;
                }

                request->ioctl.cc_chunk_count = cc;
                for (i = 0; i < cc; i++) {
                    evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.cc_chunks[i].src_offset);
                    evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.cc_chunks[i].dst_offset);
                    evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.cc_chunks[i].length);
                    evpl_iovec_cursor_get_uint32(request_cursor, &reserved);
                }
                break;
            }
            default:
                /* Other IOCTLs don't need input parsing yet */
                chimera_smb_info("Received IOCTL request with unhandled ctl_code 0x%08x, skipping input parsing",
                                 request->ioctl.ctl_code);
                break;
        } /* switch */
    }

    return 0;
} /* chimera_smb_parse_ioctl */