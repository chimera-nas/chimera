// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_session.h"
#include "vfs/vfs.h"
#include "vfs/vfs_notify.h"
#include "vfs/vfs_claim.h"

/* A write-time-sticky handle needs the pre-write mtime back from the VFS so the
 * write callback can restore it; otherwise no pre-attrs are requested. */
static inline uint64_t
chimera_smb_write_pre_attr_mask(const struct chimera_smb_open_file *open_file)
{
    return (open_file->flags & CHIMERA_SMB_OPEN_FILE_WRITE_TIME_STICKY)
           ? CHIMERA_VFS_ATTR_MTIME : 0;
} /* chimera_smb_write_pre_attr_mask */

/* Completion for the mtime-restore setattr issued after a write through a
 * write-time-sticky handle.  The write itself already succeeded; a failed
 * restore leaves a slightly-advanced write time but is not worth failing the
 * write over, so we always report success. */
static void
chimera_smb_write_sticky_restore_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    chimera_smb_open_file_release(request, request->write.open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_write_sticky_restore_callback */

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

    if (!error_code && request->write.open_file->parent_fh_len > 0) {
        /* A write changes the file's data and length.  Beyond the generic
         * FILE_MODIFIED class this also touches the (default or named) data
         * stream's contents and size, so set the STREAM_WRITE/STREAM_SIZE
         * classes too — a watcher requesting only
         * FILE_NOTIFY_CHANGE_STREAM_{WRITE,SIZE} must still be notified
         * (WPTS BVT_SMB2Basic_ChangeNotify_ChangeStream{Write,Size}).
         *
         * Deliver the change-notify event WITHOUT breaking the parent directory
         * lease: a write's effect on the file's directory-visible metadata
         * (size/mtime) is not observable until the handle closes, so the
         * dir-lease content break is deferred to close (the open is flagged
         * MODIFIED for chimera_smb_close to emit it).  MS-SMB2;
         * dirlease.v2_request. */
        chimera_vfs_notify_emit_nobreak(thread->shared->vfs->vfs_notify,
                                        request->write.open_file->parent_fh,
                                        request->write.open_file->parent_fh_len,
                                        CHIMERA_VFS_NOTIFY_FILE_MODIFIED |
                                        CHIMERA_VFS_NOTIFY_STREAM_WRITE |
                                        CHIMERA_VFS_NOTIFY_STREAM_SIZE,
                                        request->write.open_file->name,
                                        request->write.open_file->name_len,
                                        NULL, 0);
        request->write.open_file->flags |= CHIMERA_SMB_OPEN_FILE_FLAG_MODIFIED;
    }

    /* A handle that explicitly set its write time has "taken control" of it:
     * the backend bumped mtime as a side effect of this write, so restore it to
     * the pre-write value (reported in pre_attr) to keep it frozen. */
    if (!error_code &&
        (request->write.open_file->flags & CHIMERA_SMB_OPEN_FILE_WRITE_TIME_STICKY) &&
        (pre_attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME)) {

        request->write.restore_attrs.va_req_mask = 0;
        request->write.restore_attrs.va_set_mask = CHIMERA_VFS_ATTR_MTIME;
        request->write.restore_attrs.va_mtime    = pre_attr->va_mtime;

        chimera_vfs_setattr(thread->vfs_thread,
                            &request->session_handle->session->cred,
                            request->write.open_file->handle,
                            &request->write.restore_attrs,
                            0,
                            0,
                            chimera_smb_write_sticky_restore_callback,
                            request);
        return;
    }

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

        struct chimera_claim_actor io_owner = {
            .owner = chimera_smb_open_actor_owner(request->write.open_file),
            .op_handle = request->write.open_file->handle,
        };

        chimera_vfs_write_owned(
            thread->vfs_thread,
            &request->session_handle->session->cred,
            request->write.open_file->handle,
            request->write.offset,
            request->write.length,
            !!(request->write.flags & SMB2_WRITEFLAG_WRITE_THROUGH),
            chimera_smb_write_pre_attr_mask(request->write.open_file),
            0,
            request->write.iov,
            request->write.niov,
            &io_owner,
            chimera_smb_write_callback,
            request);
    }

} /* chimera_smb_rdma_read_callback */



/*
 * DCE/RPC over a named pipe (ncacn_np), SMB2 WRITE leg: feed the written request
 * PDU through the pipe's transceive handler and stash the response for the
 * client's following SMB2 READ(s) to drain.  This is the WRITE/READ transport
 * Windows and smbtorture use, distinct from the single-shot
 * FSCTL_PIPE_TRANSCEIVE IOCTL transport.  A request PDU is assumed to arrive in
 * one WRITE (true for the small LSARPC/SRVSVC calls in scope).
 */
static void
chimera_smb_pipe_write(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread    = request->compound->thread;
    struct evpl                      *evpl      = thread->evpl;
    struct chimera_smb_open_file     *open_file = request->write.open_file;
    struct evpl_iovec                 output_iov;
    int                               status;

    evpl_iovec_alloc(evpl, 65535, 8, 1, 0, &output_iov);

    status = open_file->pipe_transceive(request, request->write.iov,
                                        request->write.niov, &output_iov);

    evpl_iovecs_release(evpl, request->write.iov, request->write.niov);

    if (unlikely(status != 0)) {
        evpl_iovec_release(evpl, &output_iov);
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    /* Replace any still-undrained prior response, then copy the new PDU into a
     * stable heap buffer (the evpl iovec is thread-local and released now). */
    if (open_file->rpc_resp) {
        free(open_file->rpc_resp);
    }
    open_file->rpc_resp_len = output_iov.length;
    open_file->rpc_resp_off = 0;
    open_file->rpc_resp     = malloc(output_iov.length ? output_iov.length : 1);
    memcpy(open_file->rpc_resp, output_iov.data, output_iov.length);

    evpl_iovec_release(evpl, &output_iov);
    chimera_smb_open_file_release(request, open_file);

    /* chimera_smb_write_reply reports request->write.length bytes accepted. */
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_pipe_write */

void
chimera_smb_write(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread    = request->compound->thread;
    struct evpl                      *evpl      = thread->evpl;
    struct evpl_iovec                *chunk_iov = request->write.chunk_iov;
    int                               i, offset = 0;

    if (request->compound_input_gathered &&
        request->compound_input_status != SMB2_STATUS_SUCCESS) {
        evpl_iovecs_release(evpl, request->write.iov, request->write.niov);
        chimera_smb_complete_request(request, request->compound_input_status);
        return;
    }

    request->write.open_file = chimera_smb_open_file_resolve(request, &request->write.file_id);

    if (unlikely(!request->write.open_file)) {
        /* The file id does not resolve to a live open under this tree (e.g. a
         * write issued against a foreign/wrong TID).  Release the write payload
         * iovecs that parse cloned, since the normal completion path in
         * chimera_smb_write_callback is being skipped. */
        evpl_iovecs_release(evpl, request->write.iov, request->write.niov);
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    /* Named-pipe FIDs carry no VFS handle; route RPC writes to the ncacn_np
     * transport before any handle-backed file logic. */
    if (request->write.open_file->type == CHIMERA_SMB_OPEN_FILE_TYPE_PIPE) {
        chimera_smb_pipe_write(request);
        return;
    }

    if (request->write.open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) {
        evpl_iovecs_release(evpl, request->write.iov, request->write.niov);
        chimera_smb_open_file_release(request, request->write.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_DEVICE_REQUEST);
        return;
    }

    /* MS-SMB2 3.3.5.13: a WRITE request must be rejected with ACCESS_DENIED
     * when the open does not carry write access.  The handle's desired_access
     * may still be in NT generic form (GENERIC_WRITE / GENERIC_ALL /
     * MAXIMUM_ALLOWED), so accept any access bit that resolves to write data;
     * only an open lacking all of them (as smbtorture's deny test deliberately
     * constructs: SEC_FILE_READ_DATA only, no write bit) should be denied
     * here.  Without this gate, native backends -- which run the write through
     * the VFS regardless of the per-handle access mask -- silently accept
     * writes on read-only handles, while the passthrough backends are caught
     * by the underlying kernel open. */
    if (!(request->write.open_file->desired_access &
          (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA |
           SMB2_GENERIC_WRITE | SMB2_GENERIC_ALL | SMB2_MAXIMUM_ALLOWED))) {
        evpl_iovecs_release(evpl, request->write.iov, request->write.niov);
        chimera_smb_open_file_release(request, request->write.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    /* MS-SMB2 3.3.5.13: reject writes whose offset is beyond the maximum file
    * size (INT64_MAX) and writes whose last byte would extend past the
    * server's maximum supported file size.  A zero-length write at any
    * in-range offset is permitted (it changes nothing).  The first clause
    * bounds offset to INT64_MAX, so the offset+length sum cannot overflow. */
    if (request->write.offset > 0x7FFFFFFFFFFFFFFFULL ||
        (request->write.length > 0 &&
         request->write.offset + request->write.length > CHIMERA_SMB_MAX_FILE_SIZE)) {
        evpl_iovecs_release(evpl, request->write.iov, request->write.niov);
        chimera_smb_open_file_release(request, request->write.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* MS-SMB2 3.3.5.13: Length greater than MaxWriteSize is INVALID_PARAMETER.
     * This was missing entirely -- the clause above bounds where the write
     * lands, not how much it carries -- so a write of any size the transport
     * would deliver was accepted, whatever this connection advertised.  The
     * bound is the negotiated value: without SMB2_GLOBAL_CAP_LARGE_MTU that is
     * 64 KiB, not the 8 MiB a LARGE_MTU connection gets. */
    if (request->write.length > chimera_smb_max_rw_size(request->compound->conn)) {
        evpl_iovecs_release(evpl, request->write.iov, request->write.niov);
        chimera_smb_open_file_release(request, request->write.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* MS-SMB2 §3.3.5.13: an RDMA channel is only valid over an RDMA transport.
    * The Channel field is client-controlled; honoring RDMA_V1 on a plain-TCP
    * connection would dispatch evpl_rdma_read on a non-RDMA bind.  Reject with
    * INVALID_PARAMETER (the spec-required status) before touching the data. */
    if ((request->write.channel == SMB2_CHANNEL_RDMA_V1 ||
         request->write.channel == SMB2_CHANNEL_RDMA_V1_INVALIDATE) &&
        !evpl_bind_is_rdma(request->compound->conn->bind)) {
        evpl_iovecs_release(evpl, request->write.iov, request->write.niov);
        chimera_smb_open_file_release(request, request->write.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* MS-SMB2 §3.3.5.2.10: a WRITE carrying a stale ChannelSequence (the client
     * has since failed over to a higher sequence) must be rejected with
     * FILE_NOT_AVAILABLE so a delayed retry cannot clobber newer data. */
    if (chimera_smb_channel_sequence_stale(request->write.open_file,
                                           request->channel_sequence, 1)) {
        evpl_iovecs_release(evpl, request->write.iov, request->write.niov);
        chimera_smb_open_file_release(request, request->write.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_NOT_AVAILABLE);
        return;
    }

    /* A zero-length write accesses no byte range: per MS-FSA 2.1.5.3 it
     * completes immediately with Count 0.  It must take NO byte-range-lock
     * conflict (a 0-byte write inside another owner's exclusive lock is allowed
     * -- smb2.lock.allow_zero_byte_write) and break NO oplock/lease (it changes
     * no data, so cached readers stay valid -- smb2 readwrite write_none_*).
     * Short-circuit here, before the lock-conflict check and the VFS write that
     * would otherwise drive a read-cache break. */
    if (request->write.length == 0) {
        evpl_iovecs_release(evpl, request->write.iov, request->write.niov);
        chimera_smb_open_file_release(request, request->write.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    /* Reconnect preserves the open's canonical ACCESS/RANGE owner even when
     * a nonlease durable handle is reclaimed by another ClientGuid. Keep that
     * identity, adding its cache key for lease self-exemption. */
    struct chimera_claim_actor io_owner = {
        .owner = chimera_smb_open_actor_owner(request->write.open_file),
        .op_handle = request->write.open_file->handle,
    };

    /* Mandatory byte-range lock enforcement: a shared lock denies writes from
     * everyone, an exclusive lock denies writes from other opens.  (The
     * read-cache invalidation that used to follow here is now driven by the
     * VFS write path via chimera_vfs_write_owned().) */
    if (chimera_vfs_claim_io_denied(
            thread->vfs_thread->vfs->vfs_state,
            request->write.open_file->handle->fh,
            request->write.open_file->handle->fh_len,
            request->write.open_file->handle->fh_hash,
            request->write.offset, request->write.length,
            true, &io_owner)) {
        /* Release the write payload iovecs here: the normal path frees
        * them in chimera_smb_write_callback, which we are skipping. */
        evpl_iovecs_release(evpl, request->write.iov, request->write.niov);
        chimera_smb_open_file_release(request, request->write.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_LOCK_CONFLICT);
        return;
    }

    if (request->write.channel == SMB2_CHANNEL_RDMA_V1 &&
        !request->compound_input_gathered) {
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
        chimera_vfs_write_owned(
            thread->vfs_thread,
            &request->session_handle->session->cred,
            request->write.open_file->handle,
            request->write.offset,
            request->write.length,
            !!(request->write.flags & SMB2_WRITEFLAG_WRITE_THROUGH),
            chimera_smb_write_pre_attr_mask(request->write.open_file),
            0,
            request->write.iov,
            request->write.niov,
            &io_owner,
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

    int          prc = 0;

    /* MS-SMB2 3.3.5.13 / 2.2.21: a WRITE StructureSize that is not exactly 49
     * is a per-request STATUS_INVALID_PARAMETER. */
    if (unlikely(request->request_struct_size != SMB2_WRITE_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 WRITE request with invalid struct size (%u expected %u)",
                          request->request_struct_size, SMB2_WRITE_REQUEST_SIZE);
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &data_offset);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->write.length);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->write.offset);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->write.file_id.pid);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->write.file_id.vid);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->write.channel);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->write.remaining);
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &blob_offset);
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &blob_length);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->write.flags);

    if (unlikely(prc)) {
        chimera_smb_error("Received SMB2 WRITE request truncated in fixed body");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

    if (request->write.channel == SMB2_CHANNEL_RDMA_V1) {

        if (unlikely(smb_cursor_seek_to(request_cursor, blob_offset) != 0)) {
            chimera_smb_error("Received SMB2 WRITE with RDMA channel offset out of range");
            return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
        }

        request->write.num_rdma_elements = blob_length >> 4;

        if (unlikely(request->write.num_rdma_elements > 8)) {
            chimera_smb_error("Received SMB2 message with too many RDMA elements");
            request->status = SMB2_STATUS_INVALID_PARAMETER;
            return -1;
        }

        total_length = 0;

        for (i = 0; i < request->write.num_rdma_elements; i++) {
            prc          |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->write.rdma_elements[i].offset);
            prc          |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->write.rdma_elements[i].token);
            prc          |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->write.rdma_elements[i].length);
            total_length += request->write.rdma_elements[i].length;
        }

        if (unlikely(prc)) {
            chimera_smb_error("Received SMB2 WRITE with RDMA descriptor list past message");
            return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
        }

        if (unlikely(total_length != request->write.remaining)) {
            chimera_smb_error("Received SMB2 message with total length (%u) that does not match remaining (%u)",
                              total_length, request->write.remaining);
            request->status = SMB2_STATUS_INVALID_PARAMETER;
            return -1;
        }
        request->write.length = request->write.remaining;

        request->write.niov = evpl_iovec_alloc(evpl, request->write.length, 4096, 1, 0, request->write.iov);

    } else {
        /* The payload sits at the client-declared DataOffset and must fit inside
         * this request's window; otherwise the move would pull bytes from the
         * next compound element or off the end of the received data. */
        if (request->write.length > 0) {
            if (unlikely(smb_cursor_seek_to(request_cursor, data_offset) != 0 ||
                         request->write.length > (uint32_t) evpl_iovec_cursor_remaining(request_cursor))) {
                chimera_smb_error("Received SMB2 WRITE with data offset/length out of range");
                return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
            }
        }
        request->write.niov = evpl_iovec_cursor_move(request_cursor, request->write.iov, 256, request->write.length, 1);
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

/* The parsed payload belongs to this command until terminal release, including
 * rejected finishes. No callback consumes it while the attempt is replayable. */
struct smb_write_input {
    void (*done)(struct smb_vfs_command *, unsigned int);
    unsigned int pending;
    unsigned int chunks;
    int failed;
};

static int
smb_write_compound_eligible(struct chimera_smb_request *request)
{
    return request->write.channel == 0 || request->write.channel == SMB2_CHANNEL_RDMA_V1;
}

static void
smb_write_input_done(int status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_write_input *input = command->private_data;
    struct chimera_smb_request *request = command->request;
    if (status) {
        input->failed = 1;
    }
    if (--input->pending == 0) {
        for (unsigned int i = 0; i < input->chunks; i++) {
            evpl_iovec_release(request->compound->thread->evpl, &request->write.chunk_iov[i]);
        }
        input->chunks = 0;
        input->done(command, input->failed ? SMB2_STATUS_INTERNAL_ERROR : SMB2_STATUS_SUCCESS);
    }
}

static void
smb_write_compound_gather(struct smb_vfs_command *command,
                          void (*done)(struct smb_vfs_command *, unsigned int))
{
    struct chimera_smb_request *request = command->request;
    struct smb_write_input *input;
    uint64_t total = 0;
    uint32_t offset = 0;
    if (request->write.channel != SMB2_CHANNEL_RDMA_V1) {
        done(command, SMB2_STATUS_SUCCESS);
        return;
    }
    for (uint32_t i = 0; i < request->write.num_rdma_elements; i++) {
        total += request->write.rdma_elements[i].length;
    }
    if (!evpl_bind_is_rdma(request->compound->conn->bind) ||
        total != request->write.length || request->write.length > command->max_read) {
        done(command, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }
    input = calloc(1, sizeof(*input));
    if (!input) {
        done(command, SMB2_STATUS_INSUFFICIENT_RESOURCES);
        return;
    }
    command->private_data = input;
    input->done = done;
    input->pending = 1;
    for (uint32_t i = 0; i < request->write.num_rdma_elements; i++) {
        struct chimera_smb_rdma_element *element = &request->write.rdma_elements[i];
        struct evpl_iovec *chunk;
        if (!element->length) {
            continue;
        }
        chunk = &request->write.chunk_iov[input->chunks++];
        evpl_iovec_clone_segment(chunk, &request->write.iov[0], offset, element->length);
        input->pending++;
        evpl_rdma_read(request->compound->thread->evpl, request->compound->conn->bind,
            element->token, element->offset, chunk, 1, smb_write_input_done, command);
        offset += element->length;
    }
    smb_write_input_done(0, command);
}

static struct chimera_smb_file_id
smb_write_compound_file_id(struct chimera_smb_request *request)
{
    return request->write.file_id;
}

static void
smb_write_sticky_prepare(struct chimera_vfs_compound *compound, uint32_t index,
                         enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    const struct chimera_vfs_compound_op *write = chimera_vfs_compound_op(compound, command->result);
    struct chimera_vfs_compound_op *restore = chimera_vfs_compound_op_args(compound, index);
    (void) status;

    if (!(command->state->flags & CHIMERA_SMB_OPEN_FILE_WRITE_TIME_STICKY) ||
        !(write->dir_pre_attr.va_set_mask & CHIMERA_VFS_ATTR_MTIME)) {
        chimera_vfs_compound_op_skip(compound, index);
        return;
    }
    restore->set_attr.va_set_mask = CHIMERA_VFS_ATTR_MTIME;
    restore->set_attr.va_mtime = write->dir_pre_attr.va_mtime;
}

static void
smb_write_sticky_complete(struct chimera_vfs_compound *compound, uint32_t index,
                          enum chimera_vfs_error *status, void *private_data)
{
    (void) compound;
    (void) index;
    (void) private_data;
    /* Preserve the existing best-effort timestamp-restore policy. */
    *status = CHIMERA_VFS_OK;
}

static int
smb_write_compound_build(struct chimera_vfs_compound *compound,
                         struct smb_vfs_command *command)
{
    struct chimera_smb_request *request = command->request;
    struct chimera_vfs_attrs restore = { .va_set_mask = CHIMERA_VFS_ATTR_MTIME };
    int write, sticky;

    if (!request->write.length) {
        /* Zero-byte writes neither touch byte-range locks nor recall leases. */
        return chimera_vfs_compound_add_checkpoint(compound);
    }
    command->actor.owner.proto = CHIMERA_CLAIM_PROTO_SMB2;
    command->actor.owner.client_key = request->session_handle->session->client_key;
    if (command->open->grant) {
        command->actor.owner = command->open->grant->claim.owner;
    } else {
        command->actor.owner.owner_lo = command->open->file_id.pid;
        command->actor.owner.owner_hi = command->open->file_id.vid;
    }
    write = chimera_vfs_compound_add_write(compound, command->handle,
        request->write.offset, request->write.length,
        !!(request->write.flags & SMB2_WRITEFLAG_WRITE_THROUGH),
        request->write.iov, request->write.niov, &command->actor);
    /* An earlier SET_INFO in this compound can turn sticky time on. */
    chimera_vfs_compound_set_result_masks(compound, write, 0, CHIMERA_VFS_ATTR_MTIME, 0);
    sticky = chimera_vfs_compound_add_setattr(compound, command->handle, &restore, 0);
    chimera_vfs_compound_set_op_callbacks(compound, sticky, smb_write_sticky_prepare,
                                         smb_write_sticky_complete, command);
    return write;
}

static void
smb_write_compound_prepare(struct chimera_vfs_compound *compound, uint32_t index,
                           enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct chimera_smb_request *request = command->request;
    struct smb_vfs_open_state *state = command->state;
    unsigned int result = SMB2_STATUS_SUCCESS;
    (void) compound;
    (void) index;
    (void) status;

    if (state->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) {
        result = SMB2_STATUS_INVALID_DEVICE_REQUEST;
    } else if (!(command->open->desired_access &
                 (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA |
                  SMB2_GENERIC_WRITE | SMB2_GENERIC_ALL | SMB2_MAXIMUM_ALLOWED))) {
        result = SMB2_STATUS_ACCESS_DENIED;
    } else if (request->write.offset > INT64_MAX ||
               (request->write.length && request->write.offset + request->write.length > CHIMERA_SMB_MAX_FILE_SIZE) ||
               request->write.length > command->max_read) {
        result = SMB2_STATUS_INVALID_PARAMETER;
    } else if (state->channel_sequence_valid &&
               (uint16_t) (request->channel_sequence - state->channel_sequence) >= 0x8000) {
        result = SMB2_STATUS_FILE_NOT_AVAILABLE;
    } else {
        state->channel_sequence = request->channel_sequence;
        state->channel_sequence_valid = 1;
        state->sequence_dirty = 1;
        if (request->write.length && chimera_vfs_compound_io_denied(compound, command->handle,
                request->write.offset, request->write.length, true, &command->actor)) {
            result = SMB2_STATUS_FILE_LOCK_CONFLICT;
        }
    }
    command->status = result;
}

static void
smb_write_compound_complete(struct chimera_vfs_compound *compound, uint32_t index,
                            enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    (void) compound;
    (void) index;
    if (*status == CHIMERA_VFS_OK && command->request->write.length && command->open->parent_fh_len) {
        command->state->flags |= CHIMERA_SMB_OPEN_FILE_FLAG_MODIFIED;
        command->state->flags_dirty |= CHIMERA_SMB_OPEN_FILE_FLAG_MODIFIED;
    }
}

static void
smb_write_compound_publish(struct chimera_vfs_compound *compound,
                           struct smb_vfs_command *command)
{
    struct chimera_smb_request *request = command->request;
    const struct chimera_vfs_compound_op *write = chimera_vfs_compound_op(compound, command->result);
    struct chimera_smb_open_file *open = command->open;

    if (command->status != SMB2_STATUS_SUCCESS) {
        return;
    }
    if (request->write.length && open->parent_fh_len) {
        chimera_vfs_notify_emit_nobreak(request->compound->thread->shared->vfs->vfs_notify,
            open->parent_fh, open->parent_fh_len,
            CHIMERA_VFS_NOTIFY_FILE_MODIFIED | CHIMERA_VFS_NOTIFY_STREAM_WRITE | CHIMERA_VFS_NOTIFY_STREAM_SIZE,
            open->name, open->name_len, NULL, 0);
    }
    /* Report the bytes actually accepted, including a backend short write. */
    request->write.length = write->written;
}

static void
smb_write_compound_release(struct smb_vfs_command *command)
{
    struct chimera_smb_request *request = command->request;
    if (!command->deferred) {
        evpl_iovecs_release(request->compound->thread->evpl, request->write.iov, request->write.niov);
        request->write.niov = 0;
    }
    free(command->private_data);
    command->private_data = NULL;
}

const struct smb_vfs_command_ops chimera_smb_write_compound_ops = {
    .eligible = smb_write_compound_eligible,
    .file_id = smb_write_compound_file_id,
    .build = smb_write_compound_build,
    .prepare = smb_write_compound_prepare,
    .complete = smb_write_compound_complete,
    .publish = smb_write_compound_publish,
    .gather_inputs = smb_write_compound_gather,
    .release = smb_write_compound_release,
};
