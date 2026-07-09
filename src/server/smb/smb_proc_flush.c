// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

/* Map a VFS commit error to the SMB2 status a client expects at FLUSH time.
 * FLUSH is where a write-back backend surfaces a deferred-write failure, so the
 * real status (disk-full / quota / write-protected / IO) MUST be reported rather
 * than collapsing everything to INTERNAL_ERROR (issue #1250, MS-SMB2 3.3.5.11 /
 * MS-FSA flush). */
static inline uint32_t
chimera_smb_flush_error_status(enum chimera_vfs_error error_code)
{
    switch (error_code) {
        case CHIMERA_VFS_OK:     return SMB2_STATUS_SUCCESS;
        case CHIMERA_VFS_ENOSPC:
        case CHIMERA_VFS_EDQUOT: return SMB2_STATUS_DISK_FULL;
        case CHIMERA_VFS_EROFS:  return SMB2_STATUS_MEDIA_WRITE_PROTECTED;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:  return SMB2_STATUS_ACCESS_DENIED;
        case CHIMERA_VFS_EIO:    return SMB2_STATUS_IO_DEVICE_ERROR;
        default:                 return SMB2_STATUS_INTERNAL_ERROR;
    } /* switch */
} /* chimera_smb_flush_error_status */

static void
chimera_smb_flush_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    chimera_smb_open_file_release(request, request->flush.open_file);

    chimera_smb_complete_request(request, chimera_smb_flush_error_status(error_code));
} /* chimera_smb_flush_callback */

void
chimera_smb_flush(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;

    request->flush.open_file = chimera_smb_open_file_resolve(request, &request->flush.file_id);

    if (unlikely(!request->flush.open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    if (!request->flush.open_file->handle) {
        /* Named-pipe FIDs carry no VFS handle; FLUSH on a pipe completes
         * immediately with success (MS-SMB2 3.3.5.15) -- do not deref NULL. */
        chimera_smb_open_file_release(request, request->flush.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    chimera_vfs_commit(
        thread->vfs_thread,
        &request->session_handle->session->cred, NULL,
        request->flush.open_file->handle,
        0,
        0xffffffffffffffffULL,
        0,
        0,
        chimera_smb_flush_callback,
        request);
} /* chimera_smb_ioctl */

void
chimera_smb_flush_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_FLUSH_REPLY_SIZE);
    evpl_iovec_cursor_append_uint16(reply_cursor, 0);
} /* chimera_smb_flush_reply */

int
chimera_smb_parse_flush(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    if (unlikely(request->request_struct_size != SMB2_FLUSH_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 FLUSH request with invalid struct size (%u expected %u)",
                          request->request_struct_size,
                          SMB2_FLUSH_REQUEST_SIZE);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    if (unlikely(evpl_iovec_cursor_try_get_uint64(request_cursor, &request->flush.file_id.pid) ||
                 evpl_iovec_cursor_try_get_uint64(request_cursor, &request->flush.file_id.vid))) {
        chimera_smb_error("Received SMB2 FLUSH request truncated in fixed body");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

    return 0;
} /* chimera_smb_parse_ioctl */