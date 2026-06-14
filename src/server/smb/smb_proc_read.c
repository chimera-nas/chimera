// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/evpl_iovec_cursor.h"
#include "smb_internal.h"
#include "smb_async_interim.h"
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

    /* The data iovecs the VFS filled into request->read.iov are injected into
     * the reply (and so released) only on the SUCCESS path below.  Any path
     * that completes with an error status emits a bare SMB2 error response
     * instead, so those iovecs must be released here to avoid a leak. */
    if (error_code) {
        evpl_iovecs_release(evpl, request->read.iov, niov);
        chimera_smb_complete_request(private_data, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    if (count == 0 && request->read.length > 0) {
        evpl_iovecs_release(evpl, request->read.iov, niov);
        chimera_smb_complete_request(private_data, SMB2_STATUS_END_OF_FILE);
        return;
    }

    /* MS-SMB2 §3.3.5.12: if MinimumCount is set and the actual bytes read
     * is less than MinimumCount, return STATUS_END_OF_FILE. */
    if (request->read.minimum > 0 && count < request->read.minimum) {
        evpl_iovecs_release(evpl, request->read.iov, niov);
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

/*
 * DCE/RPC over a named pipe (ncacn_np), SMB2 READ leg: drain the response PDU a
 * prior SMB2 WRITE stashed via chimera_smb_pipe_write.  Serves up to the
 * requested length, advancing an offset so a client may read the response in
 * several chunks; the stash is freed once fully drained.
 *
 * A read with no buffered response is a blocking message-mode pipe read: it does
 * not complete now.  Such reads are made async (STATUS_PENDING) up to a
 * per-connection ceiling; past it they fail with INSUFFICIENT_RESOURCES.  This
 * is the contract smb2.credits.*_ipc_max_async_credits asserts (and a parked
 * read stays parked until the client cancels it or the connection tears down).
 */
static void
chimera_smb_pipe_read(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread    = request->compound->thread;
    struct evpl                      *evpl      = thread->evpl;
    struct chimera_smb_open_file     *open_file = request->read.open_file;
    uint32_t                          avail, n;

    avail = open_file->rpc_resp_len - open_file->rpc_resp_off;
    n     = request->read.length < avail ? request->read.length : avail;

    if (n == 0) {
        struct chimera_smb_conn          *conn   = request->compound->conn;
        struct chimera_server_smb_shared *shared = thread->shared;

        /* The fid stays open; once parked the request needs nothing from the
         * open until it is cancelled, so drop our handle reference now. */
        chimera_smb_open_file_release(request, open_file);

        if (conn->async_outstanding >=
            (uint32_t) shared->config.smb2_max_async_credits - 1) {
            chimera_smb_complete_request(request,
                                         SMB2_STATUS_INSUFFICIENT_RESOURCES);
            return;
        }

        conn->async_outstanding++;
        request->async.pipe_read = 1;
        chimera_smb_async_interim_begin(request);
        return;
    }

    request->read.niov = evpl_iovec_alloc(evpl, n, 8, 1, 0, request->read.iov);
    memcpy(request->read.iov[0].data,
           open_file->rpc_resp + open_file->rpc_resp_off, n);
    request->read.r_length   = n;
    open_file->rpc_resp_off += n;

    if (open_file->rpc_resp_off >= open_file->rpc_resp_len) {
        free(open_file->rpc_resp);
        open_file->rpc_resp     = NULL;
        open_file->rpc_resp_len = 0;
        open_file->rpc_resp_off = 0;
    }

    chimera_smb_open_file_release(request, open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_pipe_read */

void
chimera_smb_read(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;

    request->read.open_file = chimera_smb_open_file_resolve(request, &request->read.file_id);

    if (unlikely(!request->read.open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    /* Named-pipe FIDs carry no VFS handle; serve RPC reads from the stashed
     * ncacn_np response before any handle-backed file logic. */
    if (request->read.open_file->type == CHIMERA_SMB_OPEN_FILE_TYPE_PIPE) {
        chimera_smb_pipe_read(request);
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

    /* MS-SMB2 3.3.5.12: fail with INVALID_PARAMETER when the starting offset
     * is beyond the maximum file size (INT64_MAX), when the last byte to read
     * would extend past it, or when Length exceeds MaxReadSize.  A
     * MinimumCount larger than Length (or than the bytes actually available)
     * is NOT a parameter error: the read proceeds and the callback returns
     * END_OF_FILE when fewer than MinimumCount bytes are read.  Because the
     * first clause bounds offset to INT64_MAX, the offset+length sum cannot
     * overflow uint64. */
    if (request->read.offset > 0x7FFFFFFFFFFFFFFFULL ||
        request->read.offset + request->read.length > 0x7FFFFFFFFFFFFFFFULL ||
        request->read.length > (8 * 1024 * 1024)) {
        chimera_smb_open_file_release(request, request->read.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* MS-SMB2 §3.3.5.2.10: a READ is never rejected for a stale ChannelSequence,
     * but it still advances the Open's tracked sequence (the third argument 0
     * marks this as a non-mutating op). */
    chimera_smb_channel_sequence_stale(request->read.open_file,
                                       request->channel_sequence, 0);

    struct chimera_vfs_lease_owner io_owner = {
        .protocol   = CHIMERA_VFS_LEASE_PROTO_SMB2,
        .client_key = request->session_handle->session->client_key,
        .owner_lo   = request->read.open_file->file_id.pid,
        .owner_hi   = request->read.open_file->file_id.vid,
    };

    /* Mandatory byte-range lock enforcement: an exclusive lock held by a
     * different open denies reads of the locked range.  A zero-length read
     * touches no bytes, so it can conflict with no lock and is exempt
     * (MS-SMB2 zerobyteread); it also avoids the length==0 => to-EOF
     * convention in the range-overlap test wrongly matching every lock. */
    if (request->read.length != 0 &&
        chimera_vfs_state_range_io_conflict(
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
    uint8_t  padding;
    int      i;

    /* MS-SMB2 §2.2.19: StructureSize(2, already consumed) is followed by
     * Padding(1) then Flags(1).  Read both explicitly — the Length uint32 read
     * below realigns to a 4-byte boundary, so a single byte read here would
     * capture Padding and silently skip the Flags field (which carries
     * SMB2_READFLAG_REQUEST_COMPRESSED). */
    int      prc = 0;

    prc |= evpl_iovec_cursor_try_get_uint8(request_cursor, &padding);
    prc |= evpl_iovec_cursor_try_get_uint8(request_cursor, &request->read.flags);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->read.length);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->read.offset);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->read.file_id.pid);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->read.file_id.vid);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->read.minimum);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->read.channel);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->read.remaining);
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &blob_offset);
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &blob_length);

    if (unlikely(prc)) {
        chimera_smb_error("Received SMB2 READ request truncated in fixed body");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

    if (request->read.channel == SMB2_CHANNEL_RDMA_V1) {
        if (unlikely(smb_cursor_seek_to(request_cursor, blob_offset) != 0)) {
            chimera_smb_error("Received SMB2 READ with RDMA channel offset out of range");
            return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
        }

        request->read.num_rdma_elements = blob_length >> 4;

        if (unlikely(request->read.num_rdma_elements > 8)) {
            chimera_smb_error("Received SMB2 message with too many RDMA elements");
            request->status = SMB2_STATUS_INVALID_PARAMETER;
            return -1;
        }

        for (i = 0; i < request->read.num_rdma_elements; i++) {
            prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->read.rdma_elements[i].offset);
            prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->read.rdma_elements[i].token);
            prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->read.rdma_elements[i].length);
        }

        if (unlikely(prc)) {
            chimera_smb_error("Received SMB2 READ with RDMA descriptor list past message");
            return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
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
