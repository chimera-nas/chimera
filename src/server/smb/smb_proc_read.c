// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/evpl_iovec_cursor.h"
#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_session.h"
#include "vfs/vfs.h"
#include "vfs/vfs_state.h"

static void
chimera_smb_read_callback(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request       *request = private_data;
    struct chimera_server_smb_thread *thread  = request->compound->thread;
    struct evpl                      *evpl    = thread->evpl;
    struct evpl_iovec_cursor          cursor;
    struct evpl_iovec                *chunk_iov = request->read.chunk_iov;
    int                               chunk_niov;
    int                               i;

    if (!error_code) {
        request->read.open_file->position = request->read.offset + count;
    }

    chimera_smb_open_file_release(request, request->read.open_file);

    request->read.niov     = niov;
    request->read.r_length = count;

    if (error_code) {
        chimera_smb_complete_request(private_data, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    if (count == 0 && request->read.length > 0) {
        chimera_smb_complete_request(private_data, SMB2_STATUS_END_OF_FILE);
        return;
    }

    /* MS-SMB2 §3.3.5.12: if MinimumCount is set and the actual bytes read
     * is less than MinimumCount, return STATUS_END_OF_FILE. */
    if (request->read.minimum > 0 && count < request->read.minimum) {
        chimera_smb_complete_request(private_data, SMB2_STATUS_END_OF_FILE);
        return;
    }

    if (request->read.channel == SMB2_CHANNEL_RDMA_V1) {

        request->read.pending_rdma_writes = request->read.num_rdma_elements;
        request->read.r_rdma_status       = 0;

        evpl_iovec_cursor_init(&cursor, iov, niov);

        for (i = 0; i < request->read.num_rdma_elements; i++) {

            chunk_niov = evpl_iovec_cursor_move(&cursor, chunk_iov, 64, request->read.rdma_elements[0].length, 0);

            evpl_rdma_write(
                evpl,
                request->compound->conn->bind,
                request->read.rdma_elements[0].token,
                request->read.rdma_elements[0].offset,
                chunk_iov,
                chunk_niov,
                EVPL_RDMA_FLAG_TAKE_REF,
                NULL,
                NULL);

            chunk_iov += chunk_niov;
        }
    }

    chimera_smb_complete_request(private_data, SMB2_STATUS_SUCCESS);
} /* chimera_smb_read_callback */

void
chimera_smb_read(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;

    request->read.open_file = chimera_smb_open_file_resolve(request, &request->read.file_id);

    if (unlikely(!request->read.open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    if (request->read.open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) {
        chimera_smb_open_file_release(request, request->read.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_DEVICE_REQUEST);
        return;
    }

    /* Reject only when the open carries no access mask at all that could
     * authorize a read.  Real clients commonly hold opens via the generic
     * GENERIC_READ / GENERIC_EXECUTE / GENERIC_ALL bits or MAXIMUM_ALLOWED
     * without explicit FILE_READ_DATA; only an open lacking all of these
     * (which is what smbtorture's read.access test deliberately constructs)
     * should be denied here. */
    if (!(request->read.open_file->desired_access &
          (SMB2_FILE_READ_DATA | SMB2_FILE_EXECUTE |
           SMB2_GENERIC_READ | SMB2_GENERIC_EXECUTE |
           SMB2_GENERIC_ALL | SMB2_MAXIMUM_ALLOWED))) {
        chimera_smb_open_file_release(request, request->read.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    /* MS-SMB2: FileOffset > 0x7FFFFFFFFFFFFFFF, offset+length overflows,
     * length exceeds MaxReadSize, or MinimumCount > Length must fail with
     * INVALID_PARAMETER. */
    if (request->read.offset > 0x7FFFFFFFFFFFFFFFULL ||
        request->read.offset + request->read.length < request->read.offset ||
        request->read.length > (8 * 1024 * 1024) ||
        request->read.minimum > request->read.length) {
        chimera_smb_open_file_release(request, request->read.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    struct chimera_vfs_lease_owner io_owner = {
        .protocol   = CHIMERA_VFS_LEASE_PROTO_SMB2,
        .client_key = request->session_handle->session->session_id,
        .owner_lo   = request->read.open_file->file_id.pid,
        .owner_hi   = request->read.open_file->file_id.vid,
    };

    /* Mandatory byte-range lock enforcement: an exclusive lock held by a
     * different open denies reads of the locked range. */
    if (chimera_vfs_state_range_io_conflict(
            thread->vfs_thread->vfs->vfs_state,
            request->read.open_file->handle->fh,
            request->read.open_file->handle->fh_len,
            request->read.open_file->handle->fh_hash,
            request->read.offset, request->read.length,
            false, &io_owner)) {
        chimera_smb_open_file_release(request, request->read.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_LOCK_CONFLICT);
        return;
    }

    /* Attribute the read to this open's owner so it is mediated against
     * other holders without recalling the client's own oplock/lease. */
    chimera_vfs_read_owned(
        thread->vfs_thread,
        &request->session_handle->session->cred,
        request->read.open_file->handle,
        request->read.offset,
        request->read.length,
        request->read.iov,
        request->read.niov,
        0,
        &io_owner,
        chimera_smb_read_callback,
        request);
} /* chimera_smb_read */


int
chimera_smb_parse_read(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    uint16_t blob_offset, blob_length;
    int      i;

    evpl_iovec_cursor_get_uint8(request_cursor, &request->read.flags);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->read.length);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->read.offset);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->read.file_id.pid);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->read.file_id.vid);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->read.minimum);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->read.channel);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->read.remaining);
    evpl_iovec_cursor_get_uint16(request_cursor, &blob_offset);
    evpl_iovec_cursor_get_uint16(request_cursor, &blob_length);

    if (request->read.channel == SMB2_CHANNEL_RDMA_V1) {
        evpl_iovec_cursor_skip(request_cursor, blob_offset - evpl_iovec_cursor_consumed(request_cursor));

        request->read.num_rdma_elements = blob_length >> 4;

        if (unlikely(request->read.num_rdma_elements > 8)) {
            chimera_smb_error("Received SMB2 message with too many RDMA elements");
            request->status = SMB2_STATUS_INVALID_PARAMETER;
            return -1;
        }

        for (i = 0; i < request->read.num_rdma_elements; i++) {
            evpl_iovec_cursor_get_uint64(request_cursor, &request->read.rdma_elements[i].offset);
            evpl_iovec_cursor_get_uint32(request_cursor, &request->read.rdma_elements[i].token);
            evpl_iovec_cursor_get_uint32(request_cursor, &request->read.rdma_elements[i].length);
        }
    }

    request->read.niov = 256;

    return 0;
} /* chimera_smb_parse_write */


void
chimera_smb_read_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_READ_REPLY_SIZE);

    if (request->read.channel == SMB2_CHANNEL_RDMA_V1) {
        evpl_iovec_cursor_append_uint16(reply_cursor, 0); /* data offset */
        evpl_iovec_cursor_append_uint32(reply_cursor, 0);
        evpl_iovec_cursor_append_uint32(reply_cursor, request->read.r_length); /* remaining */
    } else {
        evpl_iovec_cursor_append_uint16(reply_cursor, 64 + 16); /* data offset */
        evpl_iovec_cursor_append_uint32(reply_cursor, request->read.r_length);
        evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* remaining */

        evpl_iovec_cursor_inject(reply_cursor, request->read.iov, request->read.niov, request->read.r_length);
    }
} /* chimera_smb_write_reply */
