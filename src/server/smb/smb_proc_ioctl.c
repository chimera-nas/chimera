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

    /* MS-SMB2 §3.3.5.2.10: the state-modifying FSCTLs belong to the same
     * channel-sequence-checked set as WRITE/SET_INFO.  A stale, post-failover
     * replay of one of these must be rejected with FILE_NOT_AVAILABLE so it
     * cannot re-apply a write the client has since superseded (e.g. re-zeroing a
     * range a newer write refilled).  Previously ONLY the read-only
     * CREATE_OR_GET_OBJECT_ID was guarded while the genuinely mutating FSCTLs
     * were not (issues #1123/#1256).  Resolve the IOCTL's target handle, run the
     * check, and release -- the per-FSCTL handlers re-resolve as needed. */
    switch (request->ioctl.ctl_code) {
        case SMB2_FSCTL_SET_REPARSE_POINT:
        case SMB2_FSCTL_SET_SPARSE:
        case SMB2_FSCTL_SET_ZERO_DATA:
        case SMB2_FSCTL_SRV_COPYCHUNK:
        case SMB2_FSCTL_SRV_COPYCHUNK_WRITE:
        case SMB2_FSCTL_DUPLICATE_EXTENTS_TO_FILE:
        case SMB2_FSCTL_OFFLOAD_WRITE:
        case SMB2_FSCTL_SET_INTEGRITY_INFORMATION:
            open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);
            if (open_file) {
                bool stale = chimera_smb_channel_sequence_stale(
                    open_file, request->channel_sequence, 1);

                chimera_smb_open_file_release(request, open_file);

                if (stale) {
                    chimera_smb_complete_request(request, SMB2_STATUS_FILE_NOT_AVAILABLE);
                    return;
                }
            }
            break;
        default:
            break;
    } /* state-modifying FSCTL channel-sequence guard */

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

        case SMB2_FSCTL_DUPLICATE_EXTENTS_TO_FILE:
            chimera_smb_ioctl_duplicate_extents(request);
            break;

        case SMB2_FSCTL_OFFLOAD_READ:
            chimera_smb_ioctl_offload_read(request);
            break;

        case SMB2_FSCTL_OFFLOAD_WRITE:
            chimera_smb_ioctl_offload_write(request);
            break;

        case SMB2_FSCTL_FILE_LEVEL_TRIM:
            /* MS-FSCC 2.3.75: a best-effort hint to free storage for the given
             * ranges.  The Key field MUST be 0 (else STATUS_INVALID_PARAMETER);
             * otherwise we acknowledge the ranges (NumRangesProcessed emitted in
             * the reply) without altering data -- trim is advisory. */
            open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);
            if (unlikely(!open_file)) {
                chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
                return;
            }
            chimera_smb_open_file_release(request, open_file);

            if (request->ioctl.tr_key != 0) {
                chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
                return;
            }
            if (request->ioctl.max_output_response < 4) {
                chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
                return;
            }
            chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
            break;

        case SMB2_FSCTL_GET_INTEGRITY_INFORMATION:
            /* MS-FSCC 2.3.54: report the data-integrity (ReFS checksum)
             * attributes remembered on this open. */
            open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);
            if (unlikely(!open_file)) {
                chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
                return;
            }
            request->ioctl.ii_algo  = open_file->integrity_algo;
            request->ioctl.ii_flags = open_file->integrity_flags;
            chimera_smb_open_file_release(request, open_file);

            if (request->ioctl.max_output_response < 16) {
                chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
                return;
            }
            chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
            break;

        case SMB2_FSCTL_SET_INTEGRITY_INFORMATION:
            /* MS-FSCC 2.3.55: remember the checksum algorithm + flags on the
             * open so a subsequent GET_INTEGRITY_INFORMATION reflects them. */
            open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);
            if (unlikely(!open_file)) {
                chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
                return;
            }
            open_file->integrity_algo  = request->ioctl.ii_algo;
            open_file->integrity_flags = request->ioctl.ii_flags;
            chimera_smb_open_file_release(request, open_file);

            chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
            break;

        case SMB2_FSCTL_CREATE_OR_GET_OBJECT_ID:
            /* MS-FSCC 2.3.7: returns FILE_OBJECTID_BUFFER (64 bytes).  We
             * don't persist object IDs, but the value is documented as
             * stable per file for the volume's lifetime — so synthesize it
             * deterministically from the open handle's file-handle bytes
             * (fh_hash) and the server-instance GUID.  That is enough for
             * clients that just want a stable, opaque per-file token. */
            open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);

            if (unlikely(!open_file)) {
                chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
                return;
            }

            /* MS-SMB2 §3.3.5.2.10: IOCTL is in the channel-sequence-checked set;
             * a stale sequence is rejected with FILE_NOT_AVAILABLE. */
            if (chimera_smb_channel_sequence_stale(open_file,
                                                   request->channel_sequence, 1)) {
                chimera_smb_open_file_release(request, open_file);
                chimera_smb_complete_request(request, SMB2_STATUS_FILE_NOT_AVAILABLE);
                return;
            }

            if (request->ioctl.max_output_response < 64) {
                chimera_smb_open_file_release(request, open_file);
                chimera_smb_complete_request(request, SMB2_STATUS_BUFFER_TOO_SMALL);
                return;
            }

            memset(request->ioctl.oid_buffer, 0, 64);

            if (open_file->handle) {
                /* ObjectId: derive from the VFS file-handle hash so each open
                 * of the same file yields the same ID across the session. */
                memcpy(request->ioctl.oid_buffer, &open_file->handle->fh_hash, 8);
                /* Mirror the hash into the upper half so a 16-byte compare
                 * is meaningful even if the lower 8 collide. */
                memcpy(request->ioctl.oid_buffer + 8, &open_file->handle->fh_hash, 8);
                request->ioctl.oid_buffer[8] ^= 0xa5;
                /* BirthVolumeId: server-instance GUID stands in for a volume
                 * GUID. */
                memcpy(request->ioctl.oid_buffer + 16, shared->guid, 16);
                /* BirthObjectId: same as ObjectId since we don't track
                 * creation-time renames. */
                memcpy(request->ioctl.oid_buffer + 32,
                       request->ioctl.oid_buffer, 16);
                /* DomainId: zeroed (MS-FSCC: zero indicates no domain). */
            }

            chimera_smb_open_file_release(request, open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
            break;

        case SMB2_FSCTL_LMR_REQUEST_RESILIENCY:
            /* MS-SMB2 3.3.5.15.9: grant handle resiliency on the open.  The
             * requested Timeout (ms) is capped at a server maximum; a Timeout of
             * 0 selects the server default.  Resiliency only applies to a
             * disk-file open — a request against a pipe is invalid. */
            open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);

            if (unlikely(!open_file)) {
                chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
                return;
            }

            if (open_file->type != CHIMERA_SMB_OPEN_FILE_TYPE_FILE) {
                chimera_smb_open_file_release(request, open_file);
                chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
                return;
            }

            {
                uint64_t timeout_ms = request->ioctl.rr_timeout_ms;

                if (timeout_ms == 0) {
                    timeout_ms = CHIMERA_SMB_RESILIENCY_DEFAULT_TIMEOUT_MS;
                } else if (timeout_ms > CHIMERA_SMB_RESILIENCY_MAX_TIMEOUT_MS) {
                    /* MS-SMB2 3.3.5.15.9: a Timeout greater than the maximum MUST
                     * fail with STATUS_INVALID_PARAMETER, not be silently clamped. */
                    chimera_smb_open_file_release(request, open_file);
                    chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
                    return;
                }

                /* Register the open in the durable registry (as a non-persistent
                 * entry) on the first resiliency grant so a disconnect parks it
                 * and a reconnect can reclaim it -- a resilient handle survives a
                 * network drop for its timeout (MS-SMB2 3.3.5.15.9) using the same
                 * machinery durable handles use.  A durable open is already
                 * registered; an already-resilient open must not register twice. */
                if (!open_file->durable_flags && !open_file->resilient) {
                    chimera_smb_durable_register(
                        request->compound->thread->shared, open_file,
                        request->session_handle->session->session_id,
                        request->session_handle->session->cred.uid,
                        request->compound->conn->client_guid,
                        open_file->name, open_file->name_len, false);
                }

                open_file->resilient            = true;
                open_file->resilient_timeout_ms = timeout_ms;
            }

            chimera_smb_open_file_release(request, open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
            break;

        case SMB2_FSCTL_SRV_ENUMERATE_SNAPSHOTS:
            /* MS-SMB2 3.3.5.15.1 / MS-FSCC 2.3.20: enumerate the previous
             * versions (VSS snapshots) of the open.  We don't expose
             * snapshots, so return an empty SRV_SNAPSHOT_ARRAY (all counts
             * zero) with SUCCESS rather than NOT_SUPPORTED — that is what a
             * server with no snapshots reports, and clients expect it. */
            open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);

            if (unlikely(!open_file)) {
                chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
                return;
            }

            chimera_smb_open_file_release(request, open_file);

            /* The SRV_SNAPSHOT_ARRAY response is a 12-byte header; the client
             * must offer at least that much output space. */
            if (request->ioctl.max_output_response < 12) {
                chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
                return;
            }

            chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
            break;

        default:
            /* MS-SMB2 3.3.5.15: an FSCTL the server does not implement is
             * rejected with STATUS_NOT_SUPPORTED. */
            chimera_smb_complete_request(request, SMB2_STATUS_NOT_SUPPORTED);
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
        case SMB2_FSCTL_OFFLOAD_READ:
            output_length = SMB2_FSCTL_OFFLOAD_READ_OUTPUT_SIZE; /* Size+Flags+TransferLength+Token */
            break;
        case SMB2_FSCTL_OFFLOAD_WRITE:
            output_length = SMB2_FSCTL_OFFLOAD_WRITE_OUTPUT_SIZE; /* Size+Flags+LengthWritten */
            break;
        case SMB2_FSCTL_CREATE_OR_GET_OBJECT_ID:
            output_length = 64; /* FILE_OBJECTID_BUFFER (type 1) */
            break;
        case SMB2_FSCTL_SRV_ENUMERATE_SNAPSHOTS:
            output_length = 12; /* SRV_SNAPSHOT_ARRAY header, empty list */
            break;
        case SMB2_FSCTL_FILE_LEVEL_TRIM:
            output_length = 4; /* NumRangesProcessed */
            break;
        case SMB2_FSCTL_GET_INTEGRITY_INFORMATION:
            output_length = 16; /* Algorithm(2)+Reserved(2)+Flags(4)+ChunkSize(4)+ClusterSize(4) */
            break;
    } /* switch */

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_IOCTL_REPLY_SIZE);
    evpl_iovec_cursor_append_uint16(reply_cursor, 0); /* Reserved (MS-SMB2 2.2.32) */
    evpl_iovec_cursor_append_uint32(reply_cursor, request->ioctl.ctl_code);
    /* FileId (MS-SMB2 2.2.32): echo the request's FileId.  A file-targeted FSCTL
     * (e.g. FSCTL_LMR_REQUEST_RESILIENCY -- 3.3.5.15.9 requires the response
     * FileId.Persistent/Volatile to be the open's DurableFileId/FileId) carries
     * the open's id, which the client validates against the handle it operated
     * on; a file-less FSCTL (FSCTL_VALIDATE_NEGOTIATE_INFO,
     * FSCTL_QUERY_NETWORK_INTERFACE_INFO, ...) carries the { 0xFFFF...FFFF,
     * 0xFFFF...FFFF } sentinel on the wire and so is echoed back unchanged. */
    evpl_iovec_cursor_append_uint64(reply_cursor, request->ioctl.file_id.pid);
    evpl_iovec_cursor_append_uint64(reply_cursor, request->ioctl.file_id.vid);
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
            /* inject() (not inject_unaligned()) so the spliced output is added
             * to cursor->consumed and thus counted in the reply length -- the
             * raw variant leaves the bytes out of the transmitted message.  At
             * this point consumed is 64 (SMB2 header) + 48 (IOCTL reply) = 0x70,
             * already 8-aligned, so no padding is inserted before the output. */
            evpl_iovec_cursor_inject(reply_cursor,
                                     &request->ioctl.output_iov,
                                     1,
                                     request->ioctl.output_iov.length);
            break;
        case SMB2_FSCTL_QUERY_NETWORK_INTERFACE_INFO:

            for (int i = 0; i < shared->config.num_nic_info; i++) {
                nic_info = &shared->config.nic_info[i];

                /* MS-SMB2 2.2.32 RSS_CAPABLE (0x1) must mirror the real NIC
                 * capability; chimera has no RSS backing and no config knob, so
                 * report 0 (no RSS) rather than asserting it unconditionally
                 * (issue #1289).  RDMA_CAPABLE (0x2) is reported only when the
                 * interface is actually configured for RDMA. */
                caps = 0;

                if (nic_info->rdma) {
                    caps |= 0x2; /* RDMA */
                }

                evpl_iovec_cursor_append_uint32(reply_cursor,  i == shared->config.num_nic_info - 1 ? 0 : 152); /* next */
                evpl_iovec_cursor_append_uint32(reply_cursor, i + 1); /* ifindex */
                evpl_iovec_cursor_append_uint32(reply_cursor, caps); /* capabilities (RSS/RDMA) */
                evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* reserved */
                evpl_iovec_cursor_append_uint64(reply_cursor, nic_info->speed); /* speed */
                /* SOCKADDR_STORAGE.ss_family is the Windows AF_* value on the
                 * wire (MS-DTYP / MS-SMB2 3.2.4.20.10): 2 == AF_INET,
                 * 23 (0x17) == AF_INET6.  Linux's AF_INET6 (10) differs, so map
                 * it explicitly. */
                evpl_iovec_cursor_append_uint16(reply_cursor, nic_info->addr.ss_family == AF_INET ? 2 :
                                                23);                                                                    /* Windows AF_INET / AF_INET6 */
                evpl_iovec_cursor_append_uint16(reply_cursor, 0); /* port */
                /* The SockAddr field is a fixed 128-byte SOCKADDR_STORAGE
                 * (MS-SMB2 2.2.32 / MS-DTYP).  family(2)+port(2) are already
                 * written; emit the family-specific body and zero-fill the
                 * remainder so EVERY record is exactly Next(4)+IfIndex(4)+
                 * Capability(4)+Reserved(4)+LinkSpeed(8)+SockAddr(128) = 152
                 * bytes -- matching the hardcoded Next and the declared
                 * output_length.  The IPv6 form is family(2)+port(2)+
                 * flowinfo(4)+addr(16)+scope_id(4), NOT the IPv4 layout. */
                if (nic_info->addr.ss_family == AF_INET) {
                    evpl_iovec_cursor_append_blob(reply_cursor,
                                                  &((struct sockaddr_in *) &nic_info->addr)->sin_addr.s_addr,
                                                  4);                                                                   /* sin_addr */
                    evpl_iovec_cursor_zero(reply_cursor, 120); /* SOCKADDR_STORAGE remainder: 128 - (2+2+4) */
                } else {
                    evpl_iovec_cursor_append_uint32(reply_cursor,
                                                    ((struct sockaddr_in6 *) &nic_info->addr)->sin6_flowinfo); /* sin6_flowinfo */
                    evpl_iovec_cursor_append_blob(reply_cursor,
                                                  &((struct sockaddr_in6 *) &nic_info->addr)->sin6_addr.
                                                  s6_addr,
                                                  16);                                                                  /* sin6_addr */
                    evpl_iovec_cursor_append_uint32(reply_cursor,
                                                    ((struct sockaddr_in6 *) &nic_info->addr)->sin6_scope_id); /* sin6_scope_id */
                    evpl_iovec_cursor_zero(reply_cursor, 100); /* SOCKADDR_STORAGE remainder: 128 - (2+2+4+16+4) */
                }
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
            evpl_iovec_cursor_append_uint32(reply_cursor, request->ioctl.cc_chunk_bytes); /* ChunkBytesWritten */
            evpl_iovec_cursor_append_uint32(reply_cursor, (uint32_t) request->ioctl.cc_total_written);
            break;
        case SMB2_FSCTL_OFFLOAD_READ:
            /* FSCTL_OFFLOAD_READ_OUTPUT (MS-FSCC 2.3.80): the self-describing
             * STORAGE_OFFLOAD_TOKEN was minted into od_token by the handler. */
            evpl_iovec_cursor_append_uint32(reply_cursor, SMB2_FSCTL_OFFLOAD_READ_OUTPUT_SIZE); /* Size */
            evpl_iovec_cursor_append_uint32(reply_cursor, 0);                                   /* Flags */
            evpl_iovec_cursor_append_uint64(reply_cursor, request->ioctl.od_transfer_length);
            evpl_iovec_cursor_append_blob(reply_cursor, request->ioctl.od_token, SMB2_OFFLOAD_TOKEN_SIZE);
            break;
        case SMB2_FSCTL_OFFLOAD_WRITE:
            /* FSCTL_OFFLOAD_WRITE_OUTPUT (MS-FSCC 2.3.82). */
            evpl_iovec_cursor_append_uint32(reply_cursor, SMB2_FSCTL_OFFLOAD_WRITE_OUTPUT_SIZE); /* Size */
            evpl_iovec_cursor_append_uint32(reply_cursor, 0);                                   /* Flags */
            evpl_iovec_cursor_append_uint64(reply_cursor, request->ioctl.od_copy_length);       /* LengthWritten */
            break;
        case SMB2_FSCTL_CREATE_OR_GET_OBJECT_ID:
            evpl_iovec_cursor_append_blob(reply_cursor, request->ioctl.oid_buffer, 64);
            break;
        case SMB2_FSCTL_SRV_ENUMERATE_SNAPSHOTS:
            /* Empty SRV_SNAPSHOT_ARRAY (MS-FSCC 2.3.20): no snapshots, none
             * returned, zero array bytes.  With NumberOfSnapShotsReturned == 0
             * the SnapShots[] array is absent. */
            evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* NumberOfSnapShots */
            evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* NumberOfSnapShotsReturned */
            evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* SnapShotArraySize */
            break;
        case SMB2_FSCTL_FILE_LEVEL_TRIM:
            /* FSCTL_FILE_LEVEL_TRIM_OUTPUT (MS-FSCC 2.3.76): NumRangesProcessed. */
            evpl_iovec_cursor_append_uint32(reply_cursor, request->ioctl.tr_num_ranges);
            break;
        case SMB2_FSCTL_GET_INTEGRITY_INFORMATION:
            /* GET_INTEGRITY_INFORMATION_BUFFER (MS-FSCC 2.3.54). */
            evpl_iovec_cursor_append_uint16(reply_cursor, request->ioctl.ii_algo);  /* ChecksumAlgorithm */
            evpl_iovec_cursor_append_uint16(reply_cursor, 0);                        /* Reserved */
            evpl_iovec_cursor_append_uint32(reply_cursor, request->ioctl.ii_flags);  /* Flags */
            evpl_iovec_cursor_append_uint32(reply_cursor, 65536);                    /* ChecksumChunkSizeInBytes */
            evpl_iovec_cursor_append_uint32(reply_cursor, 4096);                     /* ClusterSizeInBytes */
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
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }


    int prc = 0;
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->ioctl.ctl_code);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->ioctl.file_id.pid);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->ioctl.file_id.vid);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->ioctl.input_offset);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->ioctl.input_count);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->ioctl.max_input_response);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->ioctl.output_offset);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->ioctl.output_count);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->ioctl.max_output_response);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->ioctl.flags);

    if (unlikely(prc)) {
        chimera_smb_error("Received SMB2 IOCTL request truncated in fixed body");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

    /* SET_SPARSE with no input buffer means SetSparse = TRUE. */
    request->ioctl.sp_set_sparse = 1;

    /* Default: no COPYCHUNK limit body on the error reply (set only when an
     * over-limit COPYCHUNK is rejected). */
    request->ioctl.cc_limit_response = 0;

    /* Parse IOCTL-specific input data if present */
    if (request->ioctl.input_count > 0) {
        /* Seek to the client-declared input offset and fence the cursor to
         * exactly input_count bytes.  Every reader below is then bounded by the
         * declared input buffer: a field-length field that claims more than the
         * buffer holds (a classic IOCTL abuse) rejects cleanly via the checked
         * readers instead of reading out of bounds or aborting. */
        if (unlikely(smb_cursor_seek_to(request_cursor, request->ioctl.input_offset) != 0 ||
                     request->ioctl.input_count > (uint32_t) evpl_iovec_cursor_remaining(request_cursor))) {
            chimera_smb_error("Received SMB2 IOCTL with input buffer out of range");
            return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
        }
        evpl_iovec_cursor_set_limit(request_cursor, request->ioctl.input_count);

        switch (request->ioctl.ctl_code) {
            case SMB2_FSCTL_DFS_GET_REFERRALS:
                /* Handled in the dispatcher; no input parsing needed. */
                break;
            case SMB2_FSCTL_VALIDATE_NEGOTIATE_INFO:
                if (request->ioctl.input_count < 24) {
                    chimera_smb_error("VALIDATE_NEGOTIATE_INFO input too small (%u < 24)",
                                      request->ioctl.input_count);
                    return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
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
                    return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                }

                if (request->ioctl.input_count < 24 + (request->ioctl.vni_dialect_count * 2)) {
                    chimera_smb_error("VALIDATE_NEGOTIATE_INFO input too small for dialect count");
                    return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
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
                    return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                }

                evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.rp_reparse_tag);
                evpl_iovec_cursor_get_uint16(request_cursor, &reparse_data_len);
                evpl_iovec_cursor_get_uint16(request_cursor, &reserved);

                if (request->ioctl.rp_reparse_tag == SMB2_IO_REPARSE_TAG_NFS) {

                    if (request->ioctl.input_count < 16) {
                        chimera_smb_error("SET_REPARSE_POINT NFS input too small (%u < 16)",
                                          request->ioctl.input_count);
                        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
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
                                return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                            }

                            {
                                uint16_t                          utf16_buf[CHIMERA_VFS_PATH_MAX];
                                struct chimera_server_smb_thread *thread = request->compound->thread;

                                if (unlikely(evpl_iovec_cursor_try_copy(request_cursor, utf16_buf,
                                                                        utf16_data_len) != 0)) {
                                    chimera_smb_error("SET_REPARSE_POINT LNK: target runs past input buffer");
                                    return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                                }

                                utf8_len = chimera_smb_utf16le_to_utf8(
                                    &thread->iconv_ctx,
                                    utf16_buf,
                                    utf16_data_len,
                                    request->ioctl.rp_target,
                                    CHIMERA_VFS_PATH_MAX - 1);

                                if (utf8_len < 0) {
                                    chimera_smb_error("SET_REPARSE_POINT LNK: UTF-16LE conversion failed");
                                    return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
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
                                return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                            }
                            if (unlikely(evpl_iovec_cursor_try_get_uint32(request_cursor,
                                                                          &request->ioctl.rp_device_major) ||
                                         evpl_iovec_cursor_try_get_uint32(request_cursor,
                                                                          &request->ioctl.rp_device_minor))) {
                                chimera_smb_error("SET_REPARSE_POINT CHR/BLK: device numbers past input buffer");
                                return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                            }
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
                        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                    }

                    int      src = 0;
                    src |= evpl_iovec_cursor_try_get_uint16(request_cursor, &sub_name_offset);
                    src |= evpl_iovec_cursor_try_get_uint16(request_cursor, &sub_name_length);
                    src |= evpl_iovec_cursor_try_get_uint16(request_cursor, &print_name_offset);
                    src |= evpl_iovec_cursor_try_get_uint16(request_cursor, &print_name_length);
                    src |= evpl_iovec_cursor_try_get_uint32(request_cursor, &symlink_flags);

                    if (unlikely(src)) {
                        chimera_smb_error("SET_REPARSE_POINT SYMLINK: header past input buffer");
                        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                    }

                    if (sub_name_length == 0 ||
                        sub_name_length > (CHIMERA_VFS_PATH_MAX - 1) * 2) {
                        chimera_smb_error("SET_REPARSE_POINT SYMLINK: invalid sub_name_length %u",
                                          sub_name_length);
                        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                    }

                    /* Skip to SubstituteName within PathBuffer */
                    if (sub_name_offset > 0) {
                        if (unlikely(evpl_iovec_cursor_try_skip(request_cursor, sub_name_offset) != 0)) {
                            chimera_smb_error("SET_REPARSE_POINT SYMLINK: name offset past input buffer");
                            return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                        }
                    }

                    {
                        uint16_t                          utf16_buf[CHIMERA_VFS_PATH_MAX];
                        struct chimera_server_smb_thread *thread = request->compound->thread;

                        if (unlikely(evpl_iovec_cursor_try_copy(request_cursor, utf16_buf,
                                                                sub_name_length) != 0)) {
                            chimera_smb_error("SET_REPARSE_POINT SYMLINK: name runs past input buffer");
                            return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                        }

                        utf8_len = chimera_smb_utf16le_to_utf8(
                            &thread->iconv_ctx,
                            utf16_buf,
                            sub_name_length,
                            request->ioctl.rp_target,
                            CHIMERA_VFS_PATH_MAX - 1);

                        if (utf8_len < 0) {
                            chimera_smb_error("SET_REPARSE_POINT SYMLINK: UTF-16LE conversion failed");
                            return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
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
                    return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                }
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.sp_zero_offset);
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.sp_zero_beyond);
                break;
            case SMB2_FSCTL_QUERY_ALLOCATED_RANGES:
                /* FILE_ALLOCATED_RANGE_BUFFER: int64 FileOffset, int64 Length. */
                if (request->ioctl.input_count < 16) {
                    chimera_smb_error("QUERY_ALLOCATED_RANGES input too small (%u < 16)",
                                      request->ioctl.input_count);
                    return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
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
                    return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                }
                /* The 24-byte SourceKey encodes the source open's FileId. */
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.cc_src_file_id.pid);
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.cc_src_file_id.vid);
                evpl_iovec_cursor_skip(request_cursor, 8); /* remainder of resume key */
                evpl_iovec_cursor_get_uint32(request_cursor, &cc);
                evpl_iovec_cursor_get_uint32(request_cursor, &reserved);

                request->ioctl.cc_chunk_count = cc;

                /* An over-limit ChunkCount is NOT a parse failure: the handler
                 * answers STATUS_INVALID_PARAMETER with the SRV_COPYCHUNK_RESPONSE
                 * limit body so the client can resubmit (MS-SMB2 3.3.5.15.6).
                 * Rejecting it here would leave the client hanging, and parsing
                 * cc entries would overrun cc_chunks[], so skip the chunk array
                 * entirely and let the handler reject by count. */
                if (cc > CHIMERA_SMB_COPYCHUNK_MAX) {
                    break;
                }

                if (request->ioctl.input_count < 32 + (uint64_t) cc * 24) {
                    return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                }

                for (i = 0; i < cc; i++) {
                    evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.cc_chunks[i].src_offset);
                    evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.cc_chunks[i].dst_offset);
                    evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.cc_chunks[i].length);
                    evpl_iovec_cursor_get_uint32(request_cursor, &reserved);
                }
                break;
            }
            case SMB2_FSCTL_DUPLICATE_EXTENTS_TO_FILE:
                /* DUPLICATE_EXTENTS_DATA (MS-FSCC 2.3.8 / MS-SMB2 2.2.31.1.1):
                 * SourceFileID is a 16-byte SMB2 FileId (pid + vid), then
                 * SourceFileOffset(8), TargetFileOffset(8), ByteCount(8). */
                if (request->ioctl.input_count < 40) {
                    return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                }
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.de_src_file_id.pid);
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.de_src_file_id.vid);
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.de_src_offset);
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.de_dst_offset);
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.de_length);
                break;
            case SMB2_FSCTL_OFFLOAD_READ:
                /* FSCTL_OFFLOAD_READ_INPUT (MS-FSCC 2.3.79): Size(4) + Flags(4)
                 * + TokenTimeToLive(4) + Reserved(4) + FileOffset(8) +
                 * CopyLength(8). */
                if (request->ioctl.input_count < 32) {
                    return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                }
                evpl_iovec_cursor_skip(request_cursor, 16); /* Size, Flags, TTL, Reserved */
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.od_file_offset);
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.od_copy_length);
                break;
            case SMB2_FSCTL_OFFLOAD_WRITE:
                /* FSCTL_OFFLOAD_WRITE_INPUT (MS-FSCC 2.3.81): Size(4) + Flags(4)
                 * + FileOffset(8) + CopyLength(8) + TransferOffset(8) +
                 * Token(512). */
                if (request->ioctl.input_count < 32 + SMB2_OFFLOAD_TOKEN_SIZE) {
                    return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                }
                evpl_iovec_cursor_skip(request_cursor, 8); /* Size, Flags */
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.od_file_offset);
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.od_copy_length);
                evpl_iovec_cursor_get_uint64(request_cursor, &request->ioctl.od_transfer_offset);
                evpl_iovec_cursor_copy(request_cursor, request->ioctl.od_token, SMB2_OFFLOAD_TOKEN_SIZE);
                break;
            case SMB2_FSCTL_FILE_LEVEL_TRIM:
                /* FSCTL_FILE_LEVEL_TRIM_INPUT (MS-FSCC 2.3.75): Key(4) +
                 * NumRanges(4) + NumRanges * { Offset(8), Length(8) }.  We only
                 * need Key (must be 0) and NumRanges (to report processed). */
                if (request->ioctl.input_count < 8) {
                    return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                }
                evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.tr_key);
                evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.tr_num_ranges);
                break;
            case SMB2_FSCTL_SET_INTEGRITY_INFORMATION:
                /* SET_INTEGRITY_INFORMATION_BUFFER (MS-FSCC 2.3.55):
                 * ChecksumAlgorithm(2) + Reserved(2) + Flags(4). */
                if (request->ioctl.input_count < 8) {
                    return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                }
                evpl_iovec_cursor_get_uint16(request_cursor, &request->ioctl.ii_algo);
                evpl_iovec_cursor_skip(request_cursor, 2); /* Reserved */
                evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.ii_flags);
                break;
            case SMB2_FSCTL_LMR_REQUEST_RESILIENCY:
                /* NETWORK_RESILIENCY_REQUEST (MS-SMB2 2.2.31.3): Timeout(4) +
                 * Reserved(4). */
                if (request->ioctl.input_count < 8) {
                    chimera_smb_error("LMR_REQUEST_RESILIENCY input too small (%u < 8)",
                                      request->ioctl.input_count);
                    return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
                }
                evpl_iovec_cursor_get_uint32(request_cursor, &request->ioctl.rr_timeout_ms);
                evpl_iovec_cursor_skip(request_cursor, 4); /* Reserved */
                break;
            default:
                /* Other IOCTLs don't need input parsing yet */
                chimera_smb_info("Received IOCTL request with unhandled ctl_code 0x%08x, skipping input parsing",
                                 request->ioctl.ctl_code);
                break;
        } /* switch */
    }

    return 0;
} /* chimera_smb_parse_ioctl */