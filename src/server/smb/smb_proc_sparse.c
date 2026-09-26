// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_common/smb2.h"
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

    /* SEEK_DATA reports ENXIO when there is no data at or beyond the cursor
     * (POSIX lseek): the remainder of the file is an implicit hole, so the
     * allocated-range scan is complete with whatever ranges were collected so
     * far.  This is the common case for a zero-length or fully-sparse file
     * (smb2.ioctl.sparse_qar). */
    if (error_code == CHIMERA_VFS_ENXIO) {
        chimera_smb_qar_finalize(request);
        return;
    }

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_qar_done(request, chimera_smb_sparse_status(error_code));
        return;
    }

    /* No more data before EOF, or data starts beyond the queried range. */
    if (sr_eof || sr_offset >= request->ioctl.sp_qar_end) {
        chimera_smb_qar_finalize(request);
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

/* Coalesced sparse commands keep scans and their wire results private to one
 * attempt. Dynamic SEEKs are inserted before the next SMB command group. */
static void
smb_sparse_scan_done(struct smb_vfs_command *command)
{
    struct chimera_smb_request *request = command->request;
    uint32_t                    max     = request->ioctl.max_output_response / 16;

    if (request->ioctl.sp_qar_count && !max) {
        command->status = SMB2_STATUS_BUFFER_TOO_SMALL;
    } else if (request->ioctl.sp_qar_count > max) {
        request->ioctl.sp_qar_count = max;
        command->status             = SMB2_STATUS_BUFFER_OVERFLOW;
    }
} /* smb_sparse_scan_done */

static void
smb_sparse_scan_next(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command               *command = private_data;
    struct chimera_smb_request           *request = command->request;
    const struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op(compound, index);
    int                                   hole    = op->seek_what == CHIMERA_SMB_SEEK_HOLE;

    if (!hole && *status == CHIMERA_VFS_ENXIO) {
        *status = CHIMERA_VFS_OK;
        smb_sparse_scan_done(command);
        return;
    }
    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    if (!request->ioctl.sp_qar_length) {
        return;
    }
    if (!hole) {
        if (op->seek_eof || op->seek_offset >= request->ioctl.sp_qar_end) {
            smb_sparse_scan_done(command);
            return;
        }
        request->ioctl.sp_qar_data_start = op->seek_offset;
    } else {
        uint64_t end = op->seek_offset;
        if (end <= request->ioctl.sp_qar_data_start) {
            end = request->ioctl.sp_qar_end;
        }
        uint64_t clipped = end < request->ioctl.sp_qar_end ? end : request->ioctl.sp_qar_end;
        if (clipped > request->ioctl.sp_qar_data_start && request->ioctl.sp_qar_count < CHIMERA_SMB_QAR_MAX) {
            unsigned int at = request->ioctl.sp_qar_count++;
            request->ioctl.sp_qar_ranges[at].offset = request->ioctl.sp_qar_data_start;
            request->ioctl.sp_qar_ranges[at].length = clipped - request->ioctl.sp_qar_data_start;
        }
        request->ioctl.sp_qar_cursor = end;
        if (op->seek_eof || request->ioctl.sp_qar_count >= CHIMERA_SMB_QAR_MAX || end >= request->ioctl.sp_qar_end) {
            smb_sparse_scan_done(command);
            return;
        }
    }
    int next = chimera_vfs_compound_add_seek(compound, command->handle,
                                             hole ? request->ioctl.sp_qar_cursor : request->ioctl.sp_qar_data_start,
                                             hole ? CHIMERA_SMB_SEEK_DATA : CHIMERA_SMB_SEEK_HOLE);
    if (next >= 0) {
        chimera_vfs_compound_set_op_callbacks(compound, next, NULL, smb_sparse_scan_next, command);
    }
} /* smb_sparse_scan_next */

static void
smb_sparse_set_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command         *command = private_data;
    struct chimera_smb_request     *request = command->request;
    const struct chimera_vfs_attrs *attrs   = &chimera_vfs_compound_op(compound, index - 1)->attr;
    struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op_args(compound, index);

    if (S_ISDIR(attrs->va_mode)) {
        command->status = SMB2_STATUS_INVALID_PARAMETER;
        *status         = CHIMERA_VFS_EINVAL;
        return;
    }
    memset(&op->set_attr, 0, sizeof(op->set_attr));
    op->set_attr.va_req_mask       = op->set_attr.va_set_mask = CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
    op->set_attr.va_dos_attributes = attrs->va_dos_attributes & ~SMB2_FILE_ATTRIBUTE_SPARSE_FILE;
    if (request->ioctl.sp_set_sparse) {
        op->set_attr.va_dos_attributes |= SMB2_FILE_ATTRIBUTE_SPARSE_FILE;
    }
} /* smb_sparse_set_prepare */

static int
smb_sparse_build(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    struct chimera_smb_request *request = command->request;
    int                         op;

    switch (request->ioctl.ctl_code) {
        case SMB2_FSCTL_SET_SPARSE:
            op = chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_MASK_STAT);
            chimera_vfs_compound_op_set_handle(compound, op, command->handle);
            op = chimera_vfs_compound_add_setattr(compound, command->handle, NULL, 0);
            chimera_vfs_compound_set_op_prepare(compound, op, smb_sparse_set_prepare, command);
            return op;
        case SMB2_FSCTL_SET_ZERO_DATA:
            if (request->ioctl.sp_zero_beyond == request->ioctl.sp_zero_offset) {
                return chimera_vfs_compound_add_checkpoint(compound);
            }
            op = chimera_vfs_compound_add_allocate(compound, command->handle,
                                                   request->ioctl.sp_zero_offset, request->ioctl.sp_zero_beyond -
                                                   request->ioctl.sp_zero_offset,
                                                   CHIMERA_VFS_ALLOCATE_DEALLOCATE, 0, 0);
            if (op >= 0) {
                chimera_vfs_compound_op_args(compound, op)->have_io_owner = 1;
            }
            return op;
        default:
            if (!request->ioctl.sp_qar_length) {
                return chimera_vfs_compound_add_checkpoint(compound);
            }
            return chimera_vfs_compound_add_seek(compound, command->handle,
                                                 request->ioctl.sp_qar_offset, CHIMERA_SMB_SEEK_DATA);
    } /* switch */
} /* smb_sparse_build */

static void
smb_sparse_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command     *command = private_data;
    struct chimera_smb_request *request = command->request;
    uint32_t                    required;

    (void) compound; (void) index; (void) status;
    if (request->ioctl.ctl_code != SMB2_FSCTL_QUERY_ALLOCATED_RANGES) {
        if (command->state->channel_sequence_valid &&
            (uint16_t) (request->channel_sequence - command->state->channel_sequence) >= 0x8000) {
            command->status = SMB2_STATUS_FILE_NOT_AVAILABLE;
            return;
        }
        command->state->channel_sequence       = request->channel_sequence;
        command->state->channel_sequence_valid = command->state->sequence_dirty = 1;
    }
    switch (request->ioctl.ctl_code) {
        case SMB2_FSCTL_SET_SPARSE:
            required = SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA | SMB2_FILE_WRITE_ATTRIBUTES;
            break;
        case SMB2_FSCTL_SET_ZERO_DATA:
            if (request->ioctl.sp_zero_beyond < request->ioctl.sp_zero_offset) {
                command->status = SMB2_STATUS_INVALID_PARAMETER;
                return;
            }
            required = SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA;
            break;
        default:
            required                    = SMB2_FILE_READ_DATA | SMB2_FILE_EXECUTE;
            request->ioctl.sp_qar_count = 0;
            if (request->ioctl.sp_qar_length > UINT64_MAX - request->ioctl.sp_qar_offset) {
                command->status = SMB2_STATUS_INVALID_PARAMETER;
                return;
            }
            request->ioctl.sp_qar_end    = request->ioctl.sp_qar_offset + request->ioctl.sp_qar_length;
            request->ioctl.sp_qar_cursor = request->ioctl.sp_qar_offset;
            break;
    } /* switch */
    if (!(command->open->granted_access & required)) {
        command->status = SMB2_STATUS_ACCESS_DENIED;
        return;
    }
    if (request->ioctl.ctl_code == SMB2_FSCTL_SET_ZERO_DATA) {
        command->actor.owner.proto      = CHIMERA_CLAIM_PROTO_SMB2;
        command->actor.owner.client_key = request->session_handle->session->client_key;
        command->actor.owner.owner_lo   = command->open->file_id.pid;
        command->actor.owner.owner_hi   = command->open->file_id.vid;
        if (command->open->grant) {
            command->actor.owner = command->open->grant->claim.owner;
        }
        uint64_t length = request->ioctl.sp_zero_beyond - request->ioctl.sp_zero_offset;
        if (length && chimera_vfs_compound_io_denied(compound, command->handle,
                                                     request->ioctl.sp_zero_offset, length, true, &command->actor)) {
            command->status = SMB2_STATUS_FILE_LOCK_CONFLICT;
        }
    }
} /* smb_sparse_prepare */

static void
smb_sparse_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command *command = private_data;

    if (command->request->ioctl.ctl_code == SMB2_FSCTL_QUERY_ALLOCATED_RANGES) {
        smb_sparse_scan_next(compound, index, status, command);
    }
} /* smb_sparse_complete */

static struct chimera_smb_file_id
smb_sparse_file_id(struct chimera_smb_request *request)
{
    return request->ioctl.file_id;
} /* smb_sparse_file_id */

static int
smb_sparse_eligible(struct chimera_smb_request *request)
{
    (void) request;
    return 1;
} /* smb_sparse_eligible */

const struct smb_vfs_command_ops chimera_smb_sparse_compound_ops = {
    .file_id   = smb_sparse_file_id,
    .eligible  = smb_sparse_eligible,
    .map_error = chimera_smb_sparse_status,
    .build     = smb_sparse_build,
    .prepare   = smb_sparse_prepare,
    .complete  = smb_sparse_complete,
};
