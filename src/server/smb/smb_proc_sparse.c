// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb2.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

/* FSCTL_SET_SPARSE, FSCTL_SET_ZERO_DATA and FSCTL_QUERY_ALLOCATED_RANGES are
 * implemented entirely on top of existing VFS primitives:
 *   - SET_SPARSE          -> setattr of the persisted DOS attribute bit
 *   - SET_ZERO_DATA       -> allocate(DEALLOCATE) (punch hole; reads as zeros)
 *   - QUERY_ALLOCATED_RANGES -> iterated seek(SEEK_DATA/SEEK_HOLE)
 */

#define CHIMERA_SMB_SEEK_DATA 0
#define CHIMERA_SMB_SEEK_HOLE 1

static uint32_t
chimera_smb_sparse_status(enum chimera_vfs_error error_code)
{
    switch (error_code) {
        case CHIMERA_VFS_OK:      return SMB2_STATUS_SUCCESS;
        case CHIMERA_VFS_ENOTSUP: return SMB2_STATUS_NOT_SUPPORTED;
        default:                  return SMB2_STATUS_INTERNAL_ERROR;
    } /* switch */
} /* chimera_smb_sparse_status */

/* ------------------------------------------------------------------ */
/* FSCTL_SET_SPARSE                                                    */
/* ------------------------------------------------------------------ */

static void
chimera_smb_set_sparse_setattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    chimera_smb_open_file_release(request, request->ioctl.sp_open_file);
    chimera_smb_complete_request(request, chimera_smb_sparse_status(error_code));
} /* chimera_smb_set_sparse_setattr_cb */

static void
chimera_smb_set_sparse_getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;
    uint32_t                    dos;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, request->ioctl.sp_open_file);
        chimera_smb_complete_request(request, chimera_smb_sparse_status(error_code));
        return;
    }

    /* Read-modify-write so the SPARSE toggle preserves the settable DOS bits
     * (READONLY/HIDDEN/SYSTEM/ARCHIVE). */
    dos = attr->va_dos_attributes & ~SMB2_FILE_ATTRIBUTE_SPARSE_FILE;
    if (request->ioctl.sp_set_sparse) {
        dos |= SMB2_FILE_ATTRIBUTE_SPARSE_FILE;
    }

    memset(&request->ioctl.sp_set_attr, 0, sizeof(request->ioctl.sp_set_attr));
    request->ioctl.sp_set_attr.va_req_mask       = CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
    request->ioctl.sp_set_attr.va_set_mask       = CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
    request->ioctl.sp_set_attr.va_dos_attributes = dos;

    chimera_vfs_setattr(
        vfs_thread,
        &request->session_handle->session->cred, NULL,
        request->ioctl.sp_open_file->handle,
        &request->ioctl.sp_set_attr,
        0,
        0,
        chimera_smb_set_sparse_setattr_cb,
        request);
} /* chimera_smb_set_sparse_getattr_cb */

void
chimera_smb_ioctl_set_sparse(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file;

    open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);

    if (unlikely(!open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    request->ioctl.sp_open_file = open_file;

    chimera_vfs_getattr(
        vfs_thread,
        &request->session_handle->session->cred, NULL,
        open_file->handle,
        CHIMERA_VFS_ATTR_MASK_STAT,
        chimera_smb_set_sparse_getattr_cb,
        request);
} /* chimera_smb_ioctl_set_sparse */

/* ------------------------------------------------------------------ */
/* FSCTL_SET_ZERO_DATA                                                 */
/* ------------------------------------------------------------------ */

static void
chimera_smb_set_zero_data_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    chimera_smb_open_file_release(request, request->ioctl.sp_open_file);
    chimera_smb_complete_request(request, chimera_smb_sparse_status(error_code));
} /* chimera_smb_set_zero_data_cb */

void
chimera_smb_ioctl_set_zero_data(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file;
    uint64_t                      length;

    if (request->ioctl.sp_zero_beyond < request->ioctl.sp_zero_offset) {
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);

    if (unlikely(!open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    request->ioctl.sp_open_file = open_file;

    length = request->ioctl.sp_zero_beyond - request->ioctl.sp_zero_offset;

    if (length == 0) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    /* Punch a hole over the range; on memfs/linux/io_uring the bytes read
     * back as zeros and EOF is preserved (FALLOC_FL_PUNCH_HOLE|KEEP_SIZE). */
    chimera_vfs_allocate(
        vfs_thread,
        &request->session_handle->session->cred,
        open_file->handle,
        request->ioctl.sp_zero_offset,
        length,
        CHIMERA_VFS_ALLOCATE_DEALLOCATE,
        0,
        0,
        chimera_smb_set_zero_data_cb,
        request);
} /* chimera_smb_ioctl_set_zero_data */

/* ------------------------------------------------------------------ */
/* FSCTL_QUERY_ALLOCATED_RANGES                                        */
/* ------------------------------------------------------------------ */

static void chimera_smb_qar_seek_data(
    struct chimera_smb_request *request);

static void
chimera_smb_qar_done(
    struct chimera_smb_request *request,
    uint32_t                    status)
{
    chimera_smb_open_file_release(request, request->ioctl.sp_open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_qar_done */

static void
chimera_smb_qar_seek_hole_cb(
    enum chimera_vfs_error error_code,
    int                    sr_eof,
    uint64_t               sr_offset,
    void                  *private_data)
{
    struct chimera_smb_request *request = private_data;
    uint64_t                    hole, extent_end;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_qar_done(request, chimera_smb_sparse_status(error_code));
        return;
    }

    hole = sr_offset;

    /* Guard against a non-advancing hole (e.g. EOF report); terminate the
     * scan by treating the remainder of the queried range as a hole. */
    if (sr_eof || hole <= request->ioctl.sp_qar_data_start) {
        hole = request->ioctl.sp_qar_end;
    }

    extent_end = (hole < request->ioctl.sp_qar_end) ? hole : request->ioctl.sp_qar_end;

    if (extent_end > request->ioctl.sp_qar_data_start &&
        request->ioctl.sp_qar_count < CHIMERA_SMB_QAR_MAX) {
        request->ioctl.sp_qar_ranges[request->ioctl.sp_qar_count].offset = request->ioctl.sp_qar_data_start;
        request->ioctl.sp_qar_ranges[request->ioctl.sp_qar_count].length = extent_end - request->ioctl.sp_qar_data_start
        ;
        request->ioctl.sp_qar_count++;
    }

    request->ioctl.sp_qar_cursor = hole;

    if (request->ioctl.sp_qar_count >= CHIMERA_SMB_QAR_MAX ||
        request->ioctl.sp_qar_cursor >= request->ioctl.sp_qar_end) {
        chimera_smb_qar_done(request, SMB2_STATUS_SUCCESS);
        return;
    }

    chimera_smb_qar_seek_data(request);
} /* chimera_smb_qar_seek_hole_cb */

static void
chimera_smb_qar_seek_data_cb(
    enum chimera_vfs_error error_code,
    int                    sr_eof,
    uint64_t               sr_offset,
    void                  *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_qar_done(request, chimera_smb_sparse_status(error_code));
        return;
    }

    /* No more data before EOF, or data starts beyond the queried range. */
    if (sr_eof || sr_offset >= request->ioctl.sp_qar_end) {
        chimera_smb_qar_done(request, SMB2_STATUS_SUCCESS);
        return;
    }

    request->ioctl.sp_qar_data_start = sr_offset;

    chimera_vfs_seek(
        vfs_thread,
        &request->session_handle->session->cred,
        request->ioctl.sp_open_file->handle,
        sr_offset,
        CHIMERA_SMB_SEEK_HOLE,
        chimera_smb_qar_seek_hole_cb,
        request);
} /* chimera_smb_qar_seek_data_cb */

static void
chimera_smb_qar_seek_data(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread *vfs_thread = request->compound->thread->vfs_thread;

    chimera_vfs_seek(
        vfs_thread,
        &request->session_handle->session->cred,
        request->ioctl.sp_open_file->handle,
        request->ioctl.sp_qar_cursor,
        CHIMERA_SMB_SEEK_DATA,
        chimera_smb_qar_seek_data_cb,
        request);
} /* chimera_smb_qar_seek_data */

void
chimera_smb_ioctl_query_allocated_ranges(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open_file;
    uint64_t                      end;

    open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);

    if (unlikely(!open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    request->ioctl.sp_open_file = open_file;
    request->ioctl.sp_qar_count = 0;

    /* Compute the (overflow-safe) exclusive end of the queried range. */
    if (request->ioctl.sp_qar_length > UINT64_MAX - request->ioctl.sp_qar_offset) {
        end = UINT64_MAX;
    } else {
        end = request->ioctl.sp_qar_offset + request->ioctl.sp_qar_length;
    }

    request->ioctl.sp_qar_end    = end;
    request->ioctl.sp_qar_cursor = request->ioctl.sp_qar_offset;

    if (request->ioctl.sp_qar_offset >= end) {
        chimera_smb_qar_done(request, SMB2_STATUS_SUCCESS);
        return;
    }

    chimera_smb_qar_seek_data(request);
} /* chimera_smb_ioctl_query_allocated_ranges */
