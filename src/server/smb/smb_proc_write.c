// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_session.h"
#include "vfs/vfs.h"
#include "vfs/vfs_notify.h"
#include "vfs/vfs_state.h"

/* Maximum supported file size, matching the Windows/Samba value
 * (0xFFFFFFF0000 == 2^44 - 2^16).  A write whose last byte would extend past
 * this is rejected with INVALID_PARAMETER. */
#define CHIMERA_SMB_MAX_FILE_SIZE 0xfffffff0000ULL

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
         * (WPTS BVT_SMB2Basic_ChangeNotify_ChangeStream{Write,Size}). */
        chimera_vfs_notify_emit(thread->shared->vfs->vfs_notify,
                                request->write.open_file->parent_fh,
                                request->write.open_file->parent_fh_len,
                                CHIMERA_VFS_NOTIFY_FILE_MODIFIED |
                                CHIMERA_VFS_NOTIFY_STREAM_WRITE |
                                CHIMERA_VFS_NOTIFY_STREAM_SIZE,
                                request->write.open_file->name,
                                request->write.open_file->name_len,
                                NULL, 0);
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

        struct chimera_vfs_lease_owner io_owner = {
            .protocol   = CHIMERA_VFS_LEASE_PROTO_SMB2,
            .client_key = request->session_handle->session->session_id,
            .owner_lo   = request->write.open_file->file_id.pid,
            .owner_hi   = request->write.open_file->file_id.vid,
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



void
chimera_smb_write(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread    = request->compound->thread;
    struct evpl                      *evpl      = thread->evpl;
    struct evpl_iovec                *chunk_iov = request->write.chunk_iov;
    int                               i, offset = 0;

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

    /* When the open holds a caching lease, use the lease's owner identity for
     * the write so chimera_vfs_break_reads_for_write self-exempts (mode.granted
     * & MODE_W AND owner_equal): the holder is writing through its own granted
     * write cache and must NOT break itself.  For RqLs leases the owner is the
     * lease_key; for legacy oplocks it is the open's file_id.  Without this the
     * lease is broken on every self-write, which races a server-initiated
     * OPLOCK_BREAK notification with the WRITE response and surfaces as
     * INVALID_NETWORK_RESPONSE on the client. */
    struct chimera_vfs_lease_owner io_owner;
    io_owner.protocol   = CHIMERA_VFS_LEASE_PROTO_SMB2;
    io_owner.client_key = request->session_handle->session->session_id;
    if (request->write.open_file->caching_lease_inserted) {
        io_owner.owner_lo = request->write.open_file->caching_lease.owner.owner_lo;
        io_owner.owner_hi = request->write.open_file->caching_lease.owner.owner_hi;
    } else {
        io_owner.owner_lo = request->write.open_file->file_id.pid;
        io_owner.owner_hi = request->write.open_file->file_id.vid;
    }

    /* Mandatory byte-range lock enforcement: a shared lock denies writes from
     * everyone, an exclusive lock denies writes from other opens.  (The
     * read-cache invalidation that used to follow here is now driven by the
     * VFS write path via chimera_vfs_write_owned().) */
    if (chimera_vfs_state_range_io_conflict(
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
            request->status = SMB2_STATUS_INVALID_PARAMETER;
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
            request->status = SMB2_STATUS_INVALID_PARAMETER;
            return -1;
        }
        request->write.length = request->write.remaining;

        request->write.niov = evpl_iovec_alloc(evpl, request->write.length, 4096, 1, 0, request->write.iov);

    } else {
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
