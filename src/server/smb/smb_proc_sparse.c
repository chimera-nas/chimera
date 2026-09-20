// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_common/smb2.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_compound.h"

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

    /* The sparse attribute applies only to data streams: FSCTL_SET_SPARSE on a
     * directory is STATUS_INVALID_PARAMETER (smb2.ioctl.sparse_dir_flag). */
    if (S_ISDIR(attr->va_mode)) {
        chimera_smb_open_file_release(request, request->ioctl.sp_open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
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
        &request->session_handle->session->cred,
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

    /* FSCTL_SET_SPARSE modifies a file attribute, so the handle must hold write
     * access (FILE_WRITE_DATA or FILE_WRITE_ATTRIBUTES); a handle opened with
     * only FILE_WRITE_EA is denied (smb2.ioctl.sparse_perms). */
    if (!(open_file->granted_access &
          (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA |
           SMB2_FILE_WRITE_ATTRIBUTES))) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    request->ioctl.sp_open_file = open_file;

    chimera_vfs_getattr(
        vfs_thread,
        &request->session_handle->session->cred,
        open_file->handle,
        CHIMERA_VFS_ATTR_MASK_STAT,
        chimera_smb_set_sparse_getattr_cb,
        request);
} /* chimera_smb_ioctl_set_sparse */

/* ------------------------------------------------------------------ */
/* FSCTL_SET_ZERO_DATA                                                 */
/* ------------------------------------------------------------------ */

/* PUTHANDLE, ALLOCATE(DEALLOCATE). */
static void
chimera_smb_set_zero_data_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request *request = private_data;
    enum chimera_vfs_error      status;

    status = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    chimera_smb_open_file_release(request, request->ioctl.sp_open_file);
    chimera_smb_complete_request(request, chimera_smb_sparse_status(status));
} /* chimera_smb_set_zero_data_sequence_complete */

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

    /* Zeroing data writes the file, so the handle must hold write access
     * (smb2.ioctl.sparse_perms). */
    if (!(open_file->granted_access &
          (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA))) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
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
    request->vfs_compound = chimera_vfs_compound_alloc(
        vfs_thread, &request->session_handle->session->cred);

    chimera_vfs_compound_add_puthandle(request->vfs_compound,
                                       open_file->handle,
                                       open_file->open_flags);

    chimera_vfs_compound_add_allocate(request->vfs_compound,
                                      NULL,
                                      request->ioctl.sp_zero_offset,
                                      length,
                                      CHIMERA_VFS_ALLOCATE_DEALLOCATE,
                                      0, 0);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_set_zero_data_sequence_complete,
                                request);
} /* chimera_smb_ioctl_set_zero_data */

/* ------------------------------------------------------------------ */
/* FSCTL_QUERY_ALLOCATED_RANGES                                        */
/* ------------------------------------------------------------------ */

/*
 * PUTHANDLE, SEEK -- one sequence per seek, walking the file's extent map.
 *
 * The scan alternates SEEK_DATA and SEEK_HOLE, and the hole seek's offset IS
 * the data seek's answer.  An op cannot take an argument from an earlier op's
 * result -- the gate may refuse an op, not supply it with an offset -- so the
 * pair cannot be batched into one sequence, and the number of pairs is the
 * file's extent count, which nothing knows before the scan starts.  So this is
 * the honest shape: consecutive sequences within the one request, as the
 * compound header's corollary describes for a fan-out whose size the first
 * answer decides.
 */
static void
chimera_smb_qar_submit_seek(
    struct chimera_smb_request     *request,
    uint64_t                        offset,
    uint32_t                        what,
    chimera_vfs_compound_callback_t callback)
{
    struct chimera_vfs_thread *vfs_thread = request->compound->thread->vfs_thread;

    request->vfs_compound = chimera_vfs_compound_alloc(
        vfs_thread, &request->session_handle->session->cred);

    chimera_vfs_compound_add_puthandle(request->vfs_compound,
                                       request->ioctl.sp_handle,
                                       request->ioctl.sp_open_file->open_flags);

    chimera_vfs_compound_add_seek(request->vfs_compound, NULL, offset, what);

    chimera_vfs_compound_submit(request->vfs_compound, callback, request);
} /* chimera_smb_qar_submit_seek */

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

/* Finalize a completed allocated-range scan against the caller's output buffer
 * (MS-FSCC 2.3.20.2): with allocated ranges present but no room for even one
 * 16-byte FILE_ALLOCATED_RANGE_BUFFER -> STATUS_BUFFER_TOO_SMALL; with room for
 * some but not all -> emit those that fit and STATUS_BUFFER_OVERFLOW; otherwise
 * STATUS_SUCCESS.  An empty result is always SUCCESS even with a zero buffer. */
static void
chimera_smb_qar_finalize(struct chimera_smb_request *request)
{
    uint32_t maxout    = request->ioctl.max_output_response;
    uint32_t max_range = maxout / 16;

    if (request->ioctl.sp_qar_count == 0) {
        chimera_smb_qar_done(request, SMB2_STATUS_SUCCESS);
        return;
    }

    if (max_range == 0) {
        chimera_smb_qar_done(request, SMB2_STATUS_BUFFER_TOO_SMALL);
        return;
    }

    if (request->ioctl.sp_qar_count > max_range) {
        request->ioctl.sp_qar_count = max_range;
        chimera_smb_qar_done(request, SMB2_STATUS_BUFFER_OVERFLOW);
        return;
    }

    chimera_smb_qar_done(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_qar_finalize */

static void
chimera_smb_qar_seek_hole_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    uint64_t                              hole = 0, extent_end;
    uint32_t                              sr_eof = 0;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        hole   = op->seek_offset;
        sr_eof = op->seek_eof;
    }

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (status != CHIMERA_VFS_OK) {
        chimera_smb_qar_done(request, chimera_smb_sparse_status(status));
        return;
    }

    /* The hole offset bounds the data extent.  On EOF it is the file's data
     * end (memfs/the backend clamps SEEK_HOLE to the logical size), so the
     * extent is reported up to the actual size, not block-rounded or extended
     * to the queried end (smb2.ioctl.sparse_qar expects the byte-accurate len).
     * Guard only against a genuinely non-advancing hole (would loop forever). */
    if (hole <= request->ioctl.sp_qar_data_start) {
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

    /* Terminate on EOF (no data beyond), a full range buffer, or having
     * covered the queried range. */
    if (sr_eof ||
        request->ioctl.sp_qar_count >= CHIMERA_SMB_QAR_MAX ||
        request->ioctl.sp_qar_cursor >= request->ioctl.sp_qar_end) {
        chimera_smb_qar_finalize(request);
        return;
    }

    chimera_smb_qar_seek_data(request);
} /* chimera_smb_qar_seek_hole_complete */

static void
chimera_smb_qar_seek_data_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    uint64_t                              sr_offset = 0;
    uint32_t                              sr_eof    = 0;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        sr_offset = op->seek_offset;
        sr_eof    = op->seek_eof;
    }

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    /* SEEK_DATA reports ENXIO when there is no data at or beyond the cursor
     * (POSIX lseek): the remainder of the file is an implicit hole, so the
     * allocated-range scan is complete with whatever ranges were collected so
     * far.  This is the common case for a zero-length or fully-sparse file
     * (smb2.ioctl.sparse_qar). */
    if (status == CHIMERA_VFS_ENXIO) {
        chimera_smb_qar_finalize(request);
        return;
    }

    if (status != CHIMERA_VFS_OK) {
        chimera_smb_qar_done(request, chimera_smb_sparse_status(status));
        return;
    }

    /* No more data before EOF, or data starts beyond the queried range. */
    if (sr_eof || sr_offset >= request->ioctl.sp_qar_end) {
        chimera_smb_qar_finalize(request);
        return;
    }

    request->ioctl.sp_qar_data_start = sr_offset;

    chimera_smb_qar_submit_seek(request, sr_offset, CHIMERA_SMB_SEEK_HOLE,
                                chimera_smb_qar_seek_hole_complete);
} /* chimera_smb_qar_seek_data_complete */

static void
chimera_smb_qar_seek_data(struct chimera_smb_request *request)
{
    chimera_smb_qar_submit_seek(request, request->ioctl.sp_qar_cursor,
                                CHIMERA_SMB_SEEK_DATA,
                                chimera_smb_qar_seek_data_complete);
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

    /* Querying allocated ranges reads file layout, so the handle must hold read
     * access (FILE_READ_DATA); a write-only handle is denied
     * (smb2.ioctl.sparse_perms). */
    if (!(open_file->granted_access & (SMB2_FILE_READ_DATA | SMB2_FILE_EXECUTE))) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    request->ioctl.sp_open_file = open_file;
    request->ioctl.sp_qar_count = 0;

    /* The scan is a run of sequences built one after another; a pipelined
     * CLOSE on this FileId NULLs open_file->handle between them, so the handle
     * every one of them borrows is the one captured here. */
    request->ioctl.sp_handle = open_file->handle;

    /* FileOffset + Length must not overflow (MS-FSCC 2.3.20.1): a wrapping
     * range is STATUS_INVALID_PARAMETER (smb2.ioctl.sparse_qar_overflow). */
    if (request->ioctl.sp_qar_length > UINT64_MAX - request->ioctl.sp_qar_offset) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }
    end = request->ioctl.sp_qar_offset + request->ioctl.sp_qar_length;

    request->ioctl.sp_qar_end    = end;
    request->ioctl.sp_qar_cursor = request->ioctl.sp_qar_offset;

    if (request->ioctl.sp_qar_offset >= end) {
        chimera_smb_qar_finalize(request);
        return;
    }

    chimera_smb_qar_seek_data(request);
} /* chimera_smb_ioctl_query_allocated_ranges */
