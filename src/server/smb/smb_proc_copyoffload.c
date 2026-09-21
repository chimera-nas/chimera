// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_common/smb2.h"
#include "smb_session.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"

/*
 * Block-level server-side copy beyond COPYCHUNK (smb_proc_copychunk.c):
 *
 *   FSCTL_DUPLICATE_EXTENTS_TO_FILE - reflink a source range into a target
 *     (MS-FSCC 2.3.8 / MS-SMB2 2.2.31.1.1).  The source open is named by a
 *     16-byte SMB2 FileId inside the request; the FSCTL itself is issued on the
 *     target handle.  We clone the range via chimera_vfs_clone_range, falling
 *     back to a byte copy (chimera_vfs_copy_range) on a backend that lacks
 *     reflink support.
 *
 *   FSCTL_OFFLOAD_READ / FSCTL_OFFLOAD_WRITE - the ODX token-copy pair
 *     (MS-FSCC 2.3.79-82).  OFFLOAD_READ (issued on the source) returns a
 *     512-byte STORAGE_OFFLOAD_TOKEN representing a source byte range;
 *     OFFLOAD_WRITE (issued on the target) consumes the token to copy that data
 *     server-side.  Rather than maintain a server token table, the token is
 *     self-describing: it carries the source open's FileId plus the read base
 *     offset and valid length, so OFFLOAD_WRITE resolves the source exactly the
 *     way COPYCHUNK resolves a resume key.  The source open must still be open
 *     when OFFLOAD_WRITE arrives (the real lifetime bound), which the ODX flow
 *     guarantees.
 */

/* ---- self-describing ODX token (inside the 512-byte STORAGE_OFFLOAD_TOKEN) --
 * TokenType(4) Reserved(2) TokenIdLength(2) then the TokenId payload:
 *   FileId.pid(8) FileId.vid(8) base_offset(8) length(8).
 * The token round-trips through us only (opaque to the client), so a plain
 * host-order encoding is self-consistent. */
#define SMB_ODX_TOKEN_ID_LEN 32

static void
chimera_smb_offload_token_build(
    uint8_t                          *token,
    const struct chimera_smb_file_id *fid,
    uint64_t                          base_offset,
    uint64_t                          length)
{
    uint32_t type  = SMB2_OFFLOAD_TOKEN_TYPE_CHIMERA;
    uint16_t resv  = 0;
    uint16_t idlen = SMB_ODX_TOKEN_ID_LEN;

    memset(token, 0, SMB2_OFFLOAD_TOKEN_SIZE);
    memcpy(token + 0, &type, 4);
    memcpy(token + 4, &resv, 2);
    memcpy(token + 6, &idlen, 2);
    memcpy(token + 8, &fid->pid, 8);
    memcpy(token + 16, &fid->vid, 8);
    memcpy(token + 24, &base_offset, 8);
    memcpy(token + 32, &length, 8);
} /* chimera_smb_offload_token_build */

static int
chimera_smb_offload_token_parse(
    const uint8_t              *token,
    struct chimera_smb_file_id *fid,
    uint64_t                   *base_offset,
    uint64_t                   *length)
{
    uint32_t type;

    memcpy(&type, token + 0, 4);
    if (type != SMB2_OFFLOAD_TOKEN_TYPE_CHIMERA) {
        return -1;
    }
    memcpy(&fid->pid, token + 8, 8);
    memcpy(&fid->vid, token + 16, 8);
    memcpy(base_offset, token + 24, 8);
    memcpy(length, token + 32, 8);
    return 0;
} /* chimera_smb_offload_token_parse */

/* ----------------------------- DUPLICATE_EXTENTS ------------------------- */

static void
chimera_smb_duplicate_extents_done(
    struct chimera_smb_request *request,
    uint32_t                    status)
{
    if (request->ioctl.de_src_open_file) {
        chimera_smb_open_file_release(request, request->ioctl.de_src_open_file);
        request->ioctl.de_src_open_file = NULL;
    }
    if (request->ioctl.de_dst_open_file) {
        chimera_smb_open_file_release(request, request->ioctl.de_dst_open_file);
        request->ioctl.de_dst_open_file = NULL;
    }
    chimera_smb_complete_request(request, status);
} /* chimera_smb_duplicate_extents_done */

static uint32_t
chimera_smb_copy_error_status(enum chimera_vfs_error error_code)
{
    return (error_code == CHIMERA_VFS_ENOTSUP)
           ? SMB2_STATUS_NOT_SUPPORTED
           : SMB2_STATUS_INVALID_PARAMETER;
} /* chimera_smb_copy_error_status */

/* COPY_RANGE -- the fallback sequence, run when the clone was refused. */
static void
chimera_smb_duplicate_extents_copy_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request *request = private_data;
    enum chimera_vfs_error      status;

    status = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    chimera_smb_duplicate_extents_done(
        request,
        (status == CHIMERA_VFS_OK)
        ? SMB2_STATUS_SUCCESS
        : chimera_smb_copy_error_status(status));
} /* chimera_smb_duplicate_extents_copy_complete */

/*
 * The range ops take BOTH objects from the caller -- neither addresses the
 * current one -- so a clone or a copy is a sequence of exactly one op, with no
 * cursor to establish first.
 */
static void
chimera_smb_duplicate_extents_submit_copy(struct chimera_smb_request *request)
{
    request->vfs_compound = chimera_vfs_compound_alloc(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred);

    chimera_vfs_compound_add_copy_range(
        request->vfs_compound,
        request->ioctl.de_src_open_file->handle,
        request->ioctl.de_src_offset,
        request->ioctl.de_dst_open_file->handle,
        request->ioctl.de_dst_offset,
        request->ioctl.de_length,
        0,
        0, 0);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_duplicate_extents_copy_complete,
                                request);
} /* chimera_smb_duplicate_extents_submit_copy */

/* CLONE_RANGE. */
static void
chimera_smb_duplicate_extents_clone_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request *request = private_data;
    enum chimera_vfs_error      status;

    status = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (status == CHIMERA_VFS_OK) {
        chimera_smb_duplicate_extents_done(request, SMB2_STATUS_SUCCESS);
        return;
    }

    /* A backend without reflink (or one rejecting this alignment) still copies
     * the bytes via the generic copy_range path so the data lands correctly.
     * It is a SECOND sequence rather than a second op behind the clone: the
     * fallback is what the clone's refusal means, and a sequence stops at its
     * first failure -- there is no "run this instead" op. */
    if (!request->ioctl.de_copy_fallback &&
        (status == CHIMERA_VFS_ENOTSUP || status == CHIMERA_VFS_EINVAL)) {
        request->ioctl.de_copy_fallback = 1;
        chimera_smb_duplicate_extents_submit_copy(request);
        return;
    }

    chimera_smb_duplicate_extents_done(request,
                                       chimera_smb_copy_error_status(status));
} /* chimera_smb_duplicate_extents_clone_complete */

static void
chimera_smb_duplicate_extents_submit_clone(struct chimera_smb_request *request)
{
    request->vfs_compound = chimera_vfs_compound_alloc(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred);

    chimera_vfs_compound_add_clone_range(
        request->vfs_compound,
        request->ioctl.de_src_open_file->handle,
        request->ioctl.de_src_offset,
        request->ioctl.de_dst_open_file->handle,
        request->ioctl.de_dst_offset,
        request->ioctl.de_length,
        0, 0);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_duplicate_extents_clone_complete,
                                request);
} /* chimera_smb_duplicate_extents_submit_clone */

/*
 * PUTHANDLE(dst), GETATTR, PUTHANDLE(src), GETATTR.
 *
 * Both objects are measured in one sequence: the destination range must already
 * lie within the destination (a dup must not extend it), the sparse-ness of the
 * two must match, and the source range must lie within the source.  The clone
 * is a sequence of its own because it must not run when any of those refuse,
 * and a caller's gate can only fail an op -- it cannot skip one -- so a refusal
 * expressed there would be indistinguishable, in the completion, from a clone
 * the backend turned down and that the copy fallback should answer.
 */
static void
chimera_smb_duplicate_extents_getattr_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    uint64_t                              dst_size = 0, src_size = 0;
    int                                   dst_sparse = 0, src_sparse = 0;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        /* ops: [0] PUTHANDLE(dst) [1] GETATTR(dst) [2] PUTHANDLE(src)
         *      [3] GETATTR(src) */
        op       = chimera_vfs_compound_op(compound, 1);
        dst_size = (op->attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE) ?
            op->attr.va_size : 0;
        /* Valid only when the reply carries DOS attributes: an attribute struct
         * that was never asked for them holds whatever that field last carried,
         * and a stale SPARSE from an earlier request turned this into an
         * intermittent NOT_SUPPORTED on the linux backend
         * (smb2.ioctl.dup_extents_dest_lock). */
        dst_sparse = ((op->attr.va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) &&
                      (op->attr.va_dos_attributes & SMB2_FILE_ATTRIBUTE_SPARSE_FILE));

        op       = chimera_vfs_compound_op(compound, 3);
        src_size = (op->attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE) ?
            op->attr.va_size : 0;
        src_sparse = ((op->attr.va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) &&
                      (op->attr.va_dos_attributes & SMB2_FILE_ATTRIBUTE_SPARSE_FILE));
    }

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (status != CHIMERA_VFS_OK) {
        chimera_smb_duplicate_extents_done(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    request->ioctl.de_dst_size   = dst_size;
    request->ioctl.de_dst_sparse = dst_sparse ? 1 : 0;

    /* A dup must not extend the destination (dup_extents_len_beyond_dest). */
    if (request->ioctl.de_dst_offset + request->ioctl.de_length > dst_size) {
        chimera_smb_duplicate_extents_done(request, SMB2_STATUS_NOT_SUPPORTED);
        return;
    }

    /* Block-cloning a sparse source into a non-sparse destination is not
     * supported (MS-FSCC 2.3.8): the sparse-ness must match.  A sparse source
     * with a sparse destination (or two dense files) clones fine
     * (smb2.ioctl.dup_extents_sparse_src vs sparse_dest/sparse_both). */
    if (src_sparse && !request->ioctl.de_dst_sparse) {
        chimera_smb_duplicate_extents_done(request, SMB2_STATUS_NOT_SUPPORTED);
        return;
    }

    /* The source range must lie within the source file (MS-FSCC 2.3.8): a range
     * past EOF cannot be duplicated. */
    if (request->ioctl.de_src_offset + request->ioctl.de_length > src_size) {
        chimera_smb_duplicate_extents_done(request, SMB2_STATUS_NOT_SUPPORTED);
        return;
    }

    chimera_smb_duplicate_extents_submit_clone(request);
} /* chimera_smb_duplicate_extents_getattr_complete */

void
chimera_smb_ioctl_duplicate_extents(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *src_open_file, *dst_open_file;

    request->ioctl.de_src_open_file = NULL;
    request->ioctl.de_dst_open_file = NULL;
    request->ioctl.de_copy_fallback = 0;

    /* The FSCTL is issued on the target (destination) handle. */
    dst_open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);
    if (unlikely(!dst_open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    /* The source is named by the SMB2 FileId carried in the request.  A FileId
     * that resolves to no open is an invalid handle (smb2.ioctl.
     * dup_extents_bad_handle expects STATUS_INVALID_HANDLE, not a name error). */
    src_open_file = chimera_smb_open_file_resolve(request, &request->ioctl.de_src_file_id);
    if (unlikely(!src_open_file)) {
        chimera_smb_open_file_release(request, dst_open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_HANDLE);
        return;
    }

    /* Source needs read access, destination needs write access. */
    if (!(src_open_file->granted_access & (SMB2_FILE_READ_DATA | SMB2_FILE_EXECUTE)) ||
        !(dst_open_file->granted_access & (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA))) {
        chimera_smb_open_file_release(request, src_open_file);
        chimera_smb_open_file_release(request, dst_open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    request->ioctl.de_src_open_file = src_open_file;
    request->ioctl.de_dst_open_file = dst_open_file;

    if (request->ioctl.de_length == 0) {
        chimera_smb_duplicate_extents_done(request, SMB2_STATUS_SUCCESS);
        return;
    }

    /* Overlapping source and destination ranges in the SAME file are not
     * supported (MS-FSCC 2.3.8; smb2.ioctl.dup_extents_src_is_dest_overlap). */
    if (chimera_memequal(src_open_file->handle->fh, src_open_file->handle->fh_len,
                         dst_open_file->handle->fh, dst_open_file->handle->fh_len) &&
        request->ioctl.de_src_offset < request->ioctl.de_dst_offset + request->ioctl.de_length &&
        request->ioctl.de_dst_offset < request->ioctl.de_src_offset + request->ioctl.de_length) {
        chimera_smb_duplicate_extents_done(request, SMB2_STATUS_NOT_SUPPORTED);
        return;
    }

    /* Measure both files, in that order: the destination's EOF bounds the dup
     * and the source's bounds what may be read. */
    request->vfs_compound = chimera_vfs_compound_alloc(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred);

    chimera_vfs_compound_add_puthandle(request->vfs_compound,
                                       dst_open_file->handle,
                                       dst_open_file->open_flags);
    chimera_vfs_compound_add_getattr(request->vfs_compound,
                                     CHIMERA_VFS_ATTR_MASK_STAT |
                                     CHIMERA_VFS_ATTR_DOS_ATTRIBUTES);

    chimera_vfs_compound_add_puthandle(request->vfs_compound,
                                       src_open_file->handle,
                                       src_open_file->open_flags);
    chimera_vfs_compound_add_getattr(request->vfs_compound,
                                     CHIMERA_VFS_ATTR_MASK_STAT |
                                     CHIMERA_VFS_ATTR_DOS_ATTRIBUTES);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_duplicate_extents_getattr_complete,
                                request);
} /* chimera_smb_ioctl_duplicate_extents */

/* ------------------------------- OFFLOAD_READ ---------------------------- */

/* PUTHANDLE, GETATTR. */
static void
chimera_smb_offload_read_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;
    struct chimera_smb_open_file         *src     = request->ioctl.od_src_open_file;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    uint64_t                              src_size = 0, avail, xfer;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        src_size = (op->attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE) ?
            op->attr.va_size : 0;
    }

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    request->ioctl.od_src_open_file = NULL;

    if (status != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, src);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    if (request->ioctl.od_file_offset >= src_size) {
        chimera_smb_open_file_release(request, src);
        chimera_smb_complete_request(request, SMB2_STATUS_END_OF_FILE);
        return;
    }

    /* TransferLength is the copy length clamped to what remains in the source
     * (MS-FSCC 2.3.80). */
    avail = src_size - request->ioctl.od_file_offset;
    xfer  = (request->ioctl.od_copy_length < avail)
            ? request->ioctl.od_copy_length : avail;

    request->ioctl.od_transfer_length = xfer;

    chimera_smb_offload_token_build(request->ioctl.od_token,
                                    &src->file_id,
                                    request->ioctl.od_file_offset,
                                    xfer);

    chimera_smb_open_file_release(request, src);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_offload_read_sequence_complete */

void
chimera_smb_ioctl_offload_read(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *src_open_file;

    request->ioctl.od_src_open_file = NULL;

    /* OFFLOAD_READ is issued on the source handle. */
    src_open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);
    if (unlikely(!src_open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    if (!(src_open_file->granted_access & (SMB2_FILE_READ_DATA | SMB2_FILE_EXECUTE))) {
        chimera_smb_open_file_release(request, src_open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    /* The client must offer room for the 528-byte OFFLOAD_READ_OUTPUT. */
    if (request->ioctl.max_output_response < SMB2_FSCTL_OFFLOAD_READ_OUTPUT_SIZE) {
        chimera_smb_open_file_release(request, src_open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    request->ioctl.od_src_open_file = src_open_file;

    request->vfs_compound = chimera_vfs_compound_alloc(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred);

    chimera_vfs_compound_add_puthandle(request->vfs_compound,
                                       src_open_file->handle,
                                       src_open_file->open_flags);

    chimera_vfs_compound_add_getattr(request->vfs_compound,
                                     CHIMERA_VFS_ATTR_MASK_STAT);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_offload_read_sequence_complete,
                                request);
} /* chimera_smb_ioctl_offload_read */

/* ------------------------------ OFFLOAD_WRITE ---------------------------- */

static void
chimera_smb_offload_write_done(
    struct chimera_smb_request *request,
    uint32_t                    status)
{
    if (request->ioctl.od_src_open_file) {
        chimera_smb_open_file_release(request, request->ioctl.od_src_open_file);
        request->ioctl.od_src_open_file = NULL;
    }
    if (request->ioctl.od_dst_open_file) {
        chimera_smb_open_file_release(request, request->ioctl.od_dst_open_file);
        request->ioctl.od_dst_open_file = NULL;
    }
    chimera_smb_complete_request(request, status);
} /* chimera_smb_offload_write_done */

/* COPY_RANGE -- the fallback sequence. */
static void
chimera_smb_offload_write_copy_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request *request = private_data;
    enum chimera_vfs_error      status;

    status = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    chimera_smb_offload_write_done(
        request,
        (status == CHIMERA_VFS_OK)
        ? SMB2_STATUS_SUCCESS
        : chimera_smb_copy_error_status(status));
} /* chimera_smb_offload_write_copy_complete */

/* CLONE_RANGE. */
static void
chimera_smb_offload_write_clone_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request *request = private_data;
    enum chimera_vfs_error      status;

    status = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (status == CHIMERA_VFS_OK) {
        chimera_smb_offload_write_done(request, SMB2_STATUS_SUCCESS);
        return;
    }

    if (!request->ioctl.od_copy_fallback &&
        (status == CHIMERA_VFS_ENOTSUP || status == CHIMERA_VFS_EINVAL)) {
        request->ioctl.od_copy_fallback = 1;

        request->vfs_compound = chimera_vfs_compound_alloc(
            request->compound->thread->vfs_thread,
            &request->session_handle->session->cred);

        chimera_vfs_compound_add_copy_range(
            request->vfs_compound,
            request->ioctl.od_src_open_file->handle,
            request->ioctl.od_transfer_offset,
            request->ioctl.od_dst_open_file->handle,
            request->ioctl.od_file_offset,
            request->ioctl.od_copy_length,
            0,
            0, 0);

        chimera_vfs_compound_submit(request->vfs_compound,
                                    chimera_smb_offload_write_copy_complete,
                                    request);
        return;
    }

    chimera_smb_offload_write_done(request,
                                   chimera_smb_copy_error_status(status));
} /* chimera_smb_offload_write_clone_complete */

void
chimera_smb_ioctl_offload_write(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *dst_open_file, *src_open_file;
    struct chimera_smb_file_id    src_file_id;
    uint64_t                      base_offset, token_length;

    request->ioctl.od_src_open_file = NULL;
    request->ioctl.od_dst_open_file = NULL;
    request->ioctl.od_copy_fallback = 0;

    /* OFFLOAD_WRITE is issued on the destination handle. */
    dst_open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);
    if (unlikely(!dst_open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    if (!(dst_open_file->granted_access & (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA))) {
        chimera_smb_open_file_release(request, dst_open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    /* The client must offer room for the 16-byte OFFLOAD_WRITE_OUTPUT. */
    if (request->ioctl.max_output_response < SMB2_FSCTL_OFFLOAD_WRITE_OUTPUT_SIZE) {
        chimera_smb_open_file_release(request, dst_open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* Decode the self-describing token minted by OFFLOAD_READ. */
    if (chimera_smb_offload_token_parse(request->ioctl.od_token, &src_file_id,
                                        &base_offset, &token_length) != 0) {
        chimera_smb_open_file_release(request, dst_open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
        return;
    }

    /* The requested slice must lie within the range the token represents. */
    if (request->ioctl.od_transfer_offset + request->ioctl.od_copy_length > token_length) {
        chimera_smb_open_file_release(request, dst_open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    request->ioctl.od_src_file_id = src_file_id;

    /* Resolve the source open on this tree, exactly like a COPYCHUNK resume
     * key.  An unknown/closed source maps to OBJECT_NAME_NOT_FOUND. */
    src_open_file = chimera_smb_open_file_resolve(request, &request->ioctl.od_src_file_id);
    if (unlikely(!src_open_file)) {
        chimera_smb_open_file_release(request, dst_open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
        return;
    }

    if (!(src_open_file->granted_access & (SMB2_FILE_READ_DATA | SMB2_FILE_EXECUTE))) {
        chimera_smb_open_file_release(request, src_open_file);
        chimera_smb_open_file_release(request, dst_open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    request->ioctl.od_dst_open_file = dst_open_file;
    request->ioctl.od_src_open_file = src_open_file;

    if (request->ioctl.od_copy_length == 0) {
        chimera_smb_offload_write_done(request, SMB2_STATUS_SUCCESS);
        return;
    }

    /* Source read position = the token's base offset + the requested transfer
     * offset within the token range. */
    request->ioctl.od_transfer_offset += base_offset;

    request->vfs_compound = chimera_vfs_compound_alloc(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred);

    chimera_vfs_compound_add_clone_range(
        request->vfs_compound,
        src_open_file->handle,
        request->ioctl.od_transfer_offset,
        dst_open_file->handle,
        request->ioctl.od_file_offset,
        request->ioctl.od_copy_length,
        0, 0);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_offload_write_clone_complete,
                                request);
} /* chimera_smb_ioctl_offload_write */
