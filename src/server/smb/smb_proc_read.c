// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/evpl_iovec_cursor.h"
#include "smb_internal.h"
#include "smb_procs.h"
#include "vfs/vfs.h"

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

    chimera_smb_open_file_release(request, request->read.open_file);

    request->read.niov     = niov;
    request->read.r_length = count;

    if (error_code) {
        chimera_smb_complete_request(private_data, SMB2_STATUS_INTERNAL_ERROR);
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

    chimera_vfs_read(
        thread->vfs_thread,
        &request->session_handle->session->cred,
        request->read.open_file->handle,
        request->read.offset,
        request->read.length,
        request->read.iov,
        request->read.niov,
        0,
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

    request->read.niov = 64;

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
