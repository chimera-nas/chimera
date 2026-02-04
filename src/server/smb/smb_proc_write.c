// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "vfs/vfs.h"

static void
chimera_smb_write_callback(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request       *request = private_data;
    struct chimera_server_smb_thread *thread  = request->compound->thread;

    /* Release write iovecs here on the server thread, not in VFS backend.
     * The iovecs were allocated on this thread and must be released here
     * to avoid cross-thread access to non-atomic refcounts.
     */
    evpl_iovecs_release(thread->evpl, request->write.iov, request->write.niov);

    chimera_smb_open_file_release(private_data, request->write.open_file);
    chimera_smb_complete_request(private_data, error_code ? SMB2_STATUS_INTERNAL_ERROR : SMB2_STATUS_SUCCESS);
} /* chimera_smb_write_callback */

static void
chimera_smb_rdma_read_callback(
    int   status,
    void *private_data)
{
    struct chimera_smb_request       *request = private_data;
    struct chimera_server_smb_thread *thread  = request->compound->thread;
    struct evpl                      *evpl    = thread->evpl;
    int                               i;

    chimera_smb_abort_if(request->write.pending_rdma_reads == 0, "Pending RDMA reads is 0");

    if (status) {
        request->write.r_rdma_status = status;
    }

    request->write.pending_rdma_reads--;

    if (request->write.pending_rdma_reads == 0) {

        /* Release all chunk_iovs that were cloned for RDMA reads.
         * Each clone added a reference to the underlying buffer.
         */
        for (i = 0; i < request->write.num_rdma_elements; i++) {
            evpl_iovec_release(evpl, &request->write.chunk_iov[i]);
        }

        if (request->write.r_rdma_status) {
            /* Error path: release the allocated iovec since VFS won't */
            evpl_iovec_release(evpl, &request->write.iov[0]);
            chimera_smb_complete_request(private_data, SMB2_STATUS_INTERNAL_ERROR);
            return;
        }

        chimera_vfs_write(
            thread->vfs_thread,
            &request->session_handle->session->cred,
            request->write.open_file->handle,
            request->write.offset,
            request->write.length,
            !!(request->write.flags & SMB2_WRITEFLAG_WRITE_THROUGH),
            0,
            0,
            request->write.iov,
            request->write.niov,
            chimera_smb_write_callback,
            request);
    }

} /* chimera_smb_rdma_read_callback */



void
chimera_smb_write(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread    = request->compound->thread;
    struct evpl                      *evpl      = thread->evpl;
    struct evpl_iovec                *chunk_iov = request->write.chunk_iov;
    int                               i, offset = 0;

    request->write.open_file = chimera_smb_open_file_resolve(request, &request->write.file_id);

    if (request->write.channel == SMB2_CHANNEL_RDMA_V1) {
        /* We need to read in the data we're supposed to be writing first */

        request->write.pending_rdma_reads = request->write.num_rdma_elements;
        request->write.r_rdma_status      = 0;

        for (i = 0; i < request->write.num_rdma_elements; i++) {

            evpl_iovec_clone_segment(chunk_iov, &request->write.iov[0], offset, request->write.rdma_elements[i].length);

            evpl_rdma_read(
                evpl,
                request->compound->conn->bind,
                request->write.rdma_elements[i].token,
                request->write.rdma_elements[i].offset,
                chunk_iov,
                1,
                chimera_smb_rdma_read_callback,
                request);

            offset += request->write.rdma_elements[i].length;
            chunk_iov++;
        }
    } else {
        chimera_vfs_write(
            thread->vfs_thread,
            &request->session_handle->session->cred,
            request->write.open_file->handle,
            request->write.offset,
            request->write.length,
            !!(request->write.flags & SMB2_WRITEFLAG_WRITE_THROUGH),
            0,
            0,
            request->write.iov,
            request->write.niov,
            chimera_smb_write_callback,
            request);
    }
} /* chimera_smb_write */


int
chimera_smb_parse_write(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    struct evpl *evpl = request->compound->thread->evpl;
    uint16_t     data_offset, blob_offset, blob_length;
    uint32_t     total_length;
    int          i;

    evpl_iovec_cursor_get_uint16(request_cursor, &data_offset);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->write.length);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->write.offset);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->write.file_id.pid);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->write.file_id.vid);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->write.channel);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->write.remaining);
    evpl_iovec_cursor_get_uint16(request_cursor, &blob_offset);
    evpl_iovec_cursor_get_uint16(request_cursor, &blob_length);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->write.flags);

    if (request->write.channel == SMB2_CHANNEL_RDMA_V1) {

        evpl_iovec_cursor_skip(request_cursor, blob_offset - evpl_iovec_cursor_consumed(request_cursor));

        request->write.num_rdma_elements = blob_length >> 4;

        if (unlikely(request->write.num_rdma_elements > 8)) {
            chimera_smb_error("Received SMB2 message with too many RDMA elements");
            return -1;
        }

        total_length = 0;

        for (i = 0; i < request->write.num_rdma_elements; i++) {
            evpl_iovec_cursor_get_uint64(request_cursor, &request->write.rdma_elements[i].offset);
            evpl_iovec_cursor_get_uint32(request_cursor, &request->write.rdma_elements[i].token);
            evpl_iovec_cursor_get_uint32(request_cursor, &request->write.rdma_elements[i].length);
            total_length += request->write.rdma_elements[i].length;
        }

        if (unlikely(total_length != request->write.remaining)) {
            chimera_smb_error("Received SMB2 message with total length (%u) that does not match remaining (%u)",
                              total_length, request->write.remaining);
            return -1;
        }
        request->write.length = request->write.remaining;

        request->write.niov = evpl_iovec_alloc(evpl, request->write.length, 4096, 1, 0, request->write.iov);

    } else {
        request->write.niov = evpl_iovec_cursor_move(request_cursor, request->write.iov, 64, request->write.length, 1);
    }

    return 0;
} /* chimera_smb_parse_write */


void
chimera_smb_write_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_WRITE_REPLY_SIZE);
    evpl_iovec_cursor_append_uint32(reply_cursor, request->write.length);
    evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* remaining */
    evpl_iovec_cursor_append_uint16(reply_cursor, 0); /* write channel offset */
    evpl_iovec_cursor_append_uint16(reply_cursor, 0); /* write channel length */

} /* chimera_smb_write_reply */
