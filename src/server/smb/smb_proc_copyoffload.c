// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_common/smb2.h"
#include "smb_session.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

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

/* Preserve the authorized endpoint identities through native copy or its
 * read/write fallback, including exemption from each open's own share deny. */
static void
chimera_smb_copyoffload_io_owner(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file,
    struct chimera_claim_actor   *actor)
{
    memset(actor, 0, sizeof(*actor));
    actor->owner.proto      = CHIMERA_CLAIM_PROTO_SMB2;
    actor->owner.client_key = request->session_handle->session->client_key;
    if (open_file->grant) {
        actor->owner = open_file->grant->claim.owner;
    } else {
        actor->owner.owner_lo = open_file->file_id.pid;
        actor->owner.owner_hi = open_file->file_id.vid;
    }
    actor->op_handle = open_file->handle;
} /* chimera_smb_copyoffload_io_owner */

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

static void
chimera_smb_duplicate_extents_copy_cb(
    enum chimera_vfs_error    error_code,
    uint64_t                  length,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    chimera_smb_duplicate_extents_done(
        request,
        (error_code == CHIMERA_VFS_OK)
        ? SMB2_STATUS_SUCCESS
        : chimera_smb_copy_error_status(error_code));
} /* chimera_smb_duplicate_extents_copy_cb */

static void
chimera_smb_duplicate_extents_clone_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (error_code == CHIMERA_VFS_OK) {
        chimera_smb_duplicate_extents_done(request, SMB2_STATUS_SUCCESS);
        return;
    }

    /* A backend without reflink (or one rejecting this alignment) still copies
    * the bytes via the generic copy_range path so the data lands correctly. */
    if (!request->ioctl.de_copy_fallback &&
        (error_code == CHIMERA_VFS_ENOTSUP || error_code == CHIMERA_VFS_EINVAL)) {
        struct chimera_claim_actor src_owner, dst_owner;
        chimera_smb_copyoffload_io_owner(request, request->ioctl.de_src_open_file, &src_owner);
        chimera_smb_copyoffload_io_owner(request, request->ioctl.de_dst_open_file, &dst_owner);
        request->ioctl.de_copy_fallback = 1;
        chimera_vfs_copy_range_owned(
            request->compound->thread->vfs_thread,
            &request->session_handle->session->cred,
            request->ioctl.de_src_open_file->handle,
            request->ioctl.de_src_offset,
            request->ioctl.de_dst_open_file->handle,
            request->ioctl.de_dst_offset,
            request->ioctl.de_length,
            0,
            0, 0,
            &src_owner, &dst_owner,
            chimera_smb_duplicate_extents_copy_cb,
            request);
        return;
    }

    chimera_smb_duplicate_extents_done(request,
                                       chimera_smb_copy_error_status(error_code));
} /* chimera_smb_duplicate_extents_clone_cb */

static void
chimera_smb_duplicate_extents_getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;
    uint64_t                    src_size;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_duplicate_extents_done(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    src_size = (attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) ? attr->va_size : 0;

    /* Block-cloning a sparse source into a non-sparse destination is not
     * supported (MS-FSCC 2.3.8): the sparse-ness must match.  A sparse source
     * with a sparse destination (or two dense files) clones fine
     * (smb2.ioctl.dup_extents_sparse_src vs sparse_dest/sparse_both).
     *
     * The sparse bit lives in the DOS attributes, which a backend reports
     * only when asked and which the passthrough backends never report.  Read
     * it only when the reply says it is there: an attribute struct that was
     * never asked for it holds whatever that field last carried, and a stale
     * SPARSE from an earlier request turned this into an intermittent
     * NOT_SUPPORTED on the linux backend (smb2.ioctl.dup_extents_dest_lock). */
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) &&
        (attr->va_dos_attributes & SMB2_FILE_ATTRIBUTE_SPARSE_FILE) &&
        !request->ioctl.de_dst_sparse) {
        chimera_smb_duplicate_extents_done(request, SMB2_STATUS_NOT_SUPPORTED);
        return;
    }

    /* The source range must lie within the source file (MS-FSCC 2.3.8): a range
     * past EOF cannot be duplicated. */
    if (request->ioctl.de_src_offset + request->ioctl.de_length > src_size) {
        chimera_smb_duplicate_extents_done(request, SMB2_STATUS_NOT_SUPPORTED);
        return;
    }

    chimera_vfs_clone_range(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred,
        request->ioctl.de_src_open_file->handle,
        request->ioctl.de_src_offset,
        request->ioctl.de_dst_open_file->handle,
        request->ioctl.de_dst_offset,
        request->ioctl.de_length,
        0, 0,
        chimera_smb_duplicate_extents_clone_cb,
        request);
} /* chimera_smb_duplicate_extents_getattr_cb */

/* Destination attributes fetched first: the destination range must already lie
 * within the destination file (a dup must not extend it).  On success, fetch
 * the source attributes and proceed to the clone. */
static void
chimera_smb_duplicate_extents_dst_getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_duplicate_extents_done(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    request->ioctl.de_dst_size = (attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) ? attr->va_size : 0;
    /* Valid only when the reply carries DOS attributes; see the sparse check
     * in chimera_smb_duplicate_extents_getattr_cb. */
    request->ioctl.de_dst_sparse =
        ((attr->va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) &&
         (attr->va_dos_attributes & SMB2_FILE_ATTRIBUTE_SPARSE_FILE)) ? 1 : 0;

    if (request->ioctl.de_dst_offset + request->ioctl.de_length > request->ioctl.de_dst_size) {
        chimera_smb_duplicate_extents_done(request, SMB2_STATUS_NOT_SUPPORTED);
        return;
    }

    chimera_vfs_getattr(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred,
        request->ioctl.de_src_open_file->handle,
        CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_DOS_ATTRIBUTES,
        chimera_smb_duplicate_extents_getattr_cb,
        request);
} /* chimera_smb_duplicate_extents_dst_getattr_cb */

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

    /* Validate the destination range against the destination's EOF first (a
     * dup must not extend the destination -- dup_extents_len_beyond_dest), then
     * the source range. */
    chimera_vfs_getattr(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred,
        dst_open_file->handle,
        CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_DOS_ATTRIBUTES,
        chimera_smb_duplicate_extents_dst_getattr_cb,
        request);
} /* chimera_smb_ioctl_duplicate_extents */

/* ------------------------------- OFFLOAD_READ ---------------------------- */

static void
chimera_smb_offload_read_getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request   *request = private_data;
    struct chimera_smb_open_file *src     = request->ioctl.od_src_open_file;
    uint64_t                      src_size, avail, xfer;

    request->ioctl.od_src_open_file = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, src);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    src_size = (attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) ? attr->va_size : 0;

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
} /* chimera_smb_offload_read_getattr_cb */

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

    chimera_vfs_getattr(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred,
        src_open_file->handle,
        CHIMERA_VFS_ATTR_MASK_STAT,
        chimera_smb_offload_read_getattr_cb,
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

static void
chimera_smb_offload_write_copy_cb(
    enum chimera_vfs_error    error_code,
    uint64_t                  length,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    chimera_smb_offload_write_done(
        request,
        (error_code == CHIMERA_VFS_OK)
        ? SMB2_STATUS_SUCCESS
        : chimera_smb_copy_error_status(error_code));
} /* chimera_smb_offload_write_copy_cb */

static void
chimera_smb_offload_write_clone_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (error_code == CHIMERA_VFS_OK) {
        chimera_smb_offload_write_done(request, SMB2_STATUS_SUCCESS);
        return;
    }

    if (!request->ioctl.od_copy_fallback &&
        (error_code == CHIMERA_VFS_ENOTSUP || error_code == CHIMERA_VFS_EINVAL)) {
        struct chimera_claim_actor src_owner, dst_owner;
        chimera_smb_copyoffload_io_owner(request, request->ioctl.od_src_open_file, &src_owner);
        chimera_smb_copyoffload_io_owner(request, request->ioctl.od_dst_open_file, &dst_owner);
        request->ioctl.od_copy_fallback = 1;
        chimera_vfs_copy_range_owned(
            request->compound->thread->vfs_thread,
            &request->session_handle->session->cred,
            request->ioctl.od_src_open_file->handle,
            request->ioctl.od_transfer_offset,
            request->ioctl.od_dst_open_file->handle,
            request->ioctl.od_file_offset,
            request->ioctl.od_copy_length,
            0,
            0, 0,
            &src_owner, &dst_owner,
            chimera_smb_offload_write_copy_cb,
            request);
        return;
    }

    chimera_smb_offload_write_done(request,
                                   chimera_smb_copy_error_status(error_code));
} /* chimera_smb_offload_write_clone_cb */

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

    chimera_vfs_clone_range(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred,
        src_open_file->handle,
        request->ioctl.od_transfer_offset,
        dst_open_file->handle,
        request->ioctl.od_file_offset,
        request->ioctl.od_copy_length,
        0, 0,
        chimera_smb_offload_write_clone_cb,
        request);
} /* chimera_smb_ioctl_offload_write */

static void
smb_offload_read_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command     *command = private_data;
    struct chimera_smb_request *request = command->request;

    (void) compound; (void) index; (void) status;
    request->ioctl.od_transfer_length = 0;
    memset(request->ioctl.od_token, 0, sizeof(request->ioctl.od_token));
    if (!(command->open->granted_access & (SMB2_FILE_READ_DATA | SMB2_FILE_EXECUTE))) {
        command->status = SMB2_STATUS_ACCESS_DENIED;
    } else if (request->ioctl.max_output_response < SMB2_FSCTL_OFFLOAD_READ_OUTPUT_SIZE) {
        command->status = SMB2_STATUS_INVALID_PARAMETER;
    }
} /* smb_offload_read_prepare */

static void
smb_offload_read_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command         *command = private_data;
    struct chimera_smb_request     *request = command->request;
    const struct chimera_vfs_attrs *attr    = &chimera_vfs_compound_op(compound, index)->attr;

    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    uint64_t                        size = (attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) ? attr->va_size : 0;
    if (request->ioctl.od_file_offset >= size) {
        command->status = SMB2_STATUS_END_OF_FILE;
        return;
    }
    uint64_t                        available = size - request->ioctl.od_file_offset;
    request->ioctl.od_transfer_length = request->ioctl.od_copy_length < available ? request->ioctl.od_copy_length :
        available;
    chimera_smb_offload_token_build(request->ioctl.od_token, &command->open->file_id,
                                    request->ioctl.od_file_offset, request->ioctl.od_transfer_length);
} /* smb_offload_read_complete */

static int
smb_offload_read_build(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    int get = chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_MASK_STAT);

    chimera_vfs_compound_op_set_handle(compound, get, command->handle);
    return get;
} /* smb_offload_read_build */

static int
smb_offload_eligible(struct chimera_smb_request *request)
{
    (void) request;
    return 1;
} /* smb_offload_eligible */

static struct chimera_smb_file_id
smb_offload_file_id(struct chimera_smb_request *request)
{
    return request->ioctl.file_id;
} /* smb_offload_file_id */

const struct smb_vfs_command_ops chimera_smb_offload_read_compound_ops = {
    .file_id   = smb_offload_file_id,
    .eligible  = smb_offload_eligible,
    .map_error = chimera_smb_copy_error_status,
    .build     = smb_offload_read_build,
    .prepare   = smb_offload_read_prepare,
    .complete  = smb_offload_read_complete,
};

struct smb_copyoffload_compound {
    struct smb_vfs_file *source;
    uint64_t             src_offset, dst_offset, length, written;
    uint64_t             token_length;
    int                  token_valid;
    int                  source_get;
    int                  destination_sparse;
};

static void
smb_copyoffload_data_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command          *command = private_data;
    struct smb_copyoffload_compound *ctx     = command->private_data;
    struct chimera_vfs_compound_op  *op      = chimera_vfs_compound_op_args(compound, index);

    if (chimera_smb_compound_file_prepare(compound, ctx->source) != SMB2_STATUS_SUCCESS) {
        command->status = command->request->ioctl.ctl_code == SMB2_FSCTL_DUPLICATE_EXTENTS_TO_FILE ?
            SMB2_STATUS_INVALID_HANDLE : SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
        *status = CHIMERA_VFS_ESTALE;
        return;
    }
    op->src_handle        = ctx->source->handle;
    op->in_handle         = command->handle;
    op->src_io_owner      = ctx->source->actor;
    op->io_owner          = command->actor;
    op->have_src_io_owner = op->have_io_owner = 1;
    if (chimera_vfs_compound_io_denied(compound, op->src_handle, ctx->src_offset, ctx->length, false, &op->src_io_owner)
        ||
        chimera_vfs_compound_io_denied(compound, command->handle, ctx->dst_offset, ctx->length, true, &command->actor))
    {
        command->status = SMB2_STATUS_FILE_LOCK_CONFLICT;
        *status         = CHIMERA_VFS_EACCES;
    }
} /* smb_copyoffload_data_prepare */

static void smb_copyoffload_next(
    struct chimera_vfs_compound *,
    uint32_t,
    enum chimera_vfs_error *,
    void *);

static void
smb_copyoffload_append_data(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command,
    bool                         clone)
{
    struct smb_copyoffload_compound *ctx = command->private_data;
    int                              op  = clone ? chimera_vfs_compound_add_clone_range(compound,
                                                                                        ctx->source->handle, ctx->
                                                                                        src_offset, command->handle, ctx
                                                                                        ->dst_offset, ctx->length, 0, 0)
    :
        chimera_vfs_compound_add_copy_range(compound,
                                            ctx->source->handle, ctx->src_offset, command->handle, ctx->dst_offset, ctx
                                            ->length, 0, 0, 0);

    if (op >= 0) {
        chimera_vfs_compound_set_op_callbacks(compound, op, smb_copyoffload_data_prepare, smb_copyoffload_next, command)
        ;
    }
} /* smb_copyoffload_append_data */

static void
smb_copyoffload_next(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command               *command   = private_data;
    struct smb_copyoffload_compound      *ctx       = command->private_data;
    struct chimera_smb_request           *request   = command->request;
    const struct chimera_vfs_compound_op *op        = chimera_vfs_compound_op(compound, index);
    bool                                  duplicate = request->ioctl.ctl_code == SMB2_FSCTL_DUPLICATE_EXTENTS_TO_FILE;

    if (*status != CHIMERA_VFS_OK) {
        if (command->status != SMB2_STATUS_SUCCESS) {
            return;
        }
        if (op->type == CHIMERA_VFS_COMPOUND_OP_CLONE_RANGE &&
            (*status == CHIMERA_VFS_ENOTSUP || *status == CHIMERA_VFS_EINVAL)) {
            *status = CHIMERA_VFS_OK;
            smb_copyoffload_append_data(compound, command, false);
        } else {
            command->status = chimera_smb_copy_error_status(*status);
        }
        return;
    }
    if (!ctx->length) {
        return;
    }
    if (op->type == CHIMERA_VFS_COMPOUND_OP_CHECKPOINT) {
        if (!duplicate) {
            smb_copyoffload_append_data(compound, command, true); return;
        }
        int get = chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_MASK_STAT |
                                                   CHIMERA_VFS_ATTR_DOS_ATTRIBUTES);
        chimera_vfs_compound_op_set_handle(compound, get, command->handle);
        chimera_vfs_compound_set_op_callbacks(compound, get, NULL, smb_copyoffload_next, command);
    } else if (op->type == CHIMERA_VFS_COMPOUND_OP_GETATTR) {
        uint64_t size = (op->attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE) ? op->attr.va_size : 0;
        if (!ctx->source_get) {
            if (ctx->dst_offset > size || ctx->length > size - ctx->dst_offset) {
                command->status = SMB2_STATUS_NOT_SUPPORTED;
                return;
            }
            ctx->destination_sparse = (op->attr.va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) &&
                (op->attr.va_dos_attributes & SMB2_FILE_ATTRIBUTE_SPARSE_FILE);
            ctx->source_get = 1;
            int get = chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_MASK_STAT |
                                                       CHIMERA_VFS_ATTR_DOS_ATTRIBUTES);
            chimera_vfs_compound_op_set_handle(compound, get, ctx->source->handle);
            chimera_vfs_compound_set_op_callbacks(compound, get, NULL, smb_copyoffload_next, command);
        } else {
            if (ctx->src_offset > size || ctx->length > size - ctx->src_offset ||
                ((op->attr.va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) &&
                 (op->attr.va_dos_attributes & SMB2_FILE_ATTRIBUTE_SPARSE_FILE) && !ctx->destination_sparse)) {
                command->status = SMB2_STATUS_NOT_SUPPORTED;
                return;
            }
            smb_copyoffload_append_data(compound, command, true);
        }
    } else {
        ctx->written = op->type == CHIMERA_VFS_COMPOUND_OP_COPY_RANGE ? op->written : ctx->length;
    }
} /* smb_copyoffload_next */

static int
smb_copyoffload_build(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    struct chimera_smb_request      *request = command->request;
    struct smb_copyoffload_compound *ctx     = calloc(1, sizeof(*ctx));
    struct chimera_smb_file_id       source_id;

    command->private_data = ctx;
    if (ctx) {
        if (request->ioctl.ctl_code == SMB2_FSCTL_DUPLICATE_EXTENTS_TO_FILE) {
            source_id        = request->ioctl.de_src_file_id;
            ctx->src_offset  = request->ioctl.de_src_offset;
            ctx->dst_offset  = request->ioctl.de_dst_offset;
            ctx->length      = request->ioctl.de_length;
            ctx->token_valid = 1;
        } else {
            uint64_t base;
            ctx->token_valid = !chimera_smb_offload_token_parse(request->ioctl.od_token,
                                                                &source_id, &base, &ctx->token_length);
            ctx->length     = request->ioctl.od_copy_length;
            ctx->dst_offset = request->ioctl.od_file_offset;
            if (ctx->token_valid && request->ioctl.od_transfer_offset <= UINT64_MAX - base) {
                ctx->src_offset = base + request->ioctl.od_transfer_offset;
            } else {
                ctx->token_valid = 0;
            }
        }
        if (ctx->token_valid) {
            ctx->source = chimera_smb_compound_pin_file(command, source_id);
        }
    }
    return chimera_vfs_compound_add_checkpoint(compound);
} /* smb_copyoffload_build */

static void
smb_copyoffload_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command          *command   = private_data;
    struct smb_copyoffload_compound *ctx       = command->private_data;
    struct chimera_smb_request      *request   = command->request;
    bool                             duplicate = request->ioctl.ctl_code == SMB2_FSCTL_DUPLICATE_EXTENTS_TO_FILE;

    (void) index; (void) status;
    if (!ctx) {
        command->status = SMB2_STATUS_INSUFFICIENT_RESOURCES; return;
    }
    ctx->written            = 0;
    ctx->source_get         = 0;
    ctx->destination_sparse = 0;
    if (command->state->channel_sequence_valid &&
        (uint16_t) (request->channel_sequence - command->state->channel_sequence) >= 0x8000) {
        command->status = SMB2_STATUS_FILE_NOT_AVAILABLE;
        return;
    }
    command->state->channel_sequence       = request->channel_sequence;
    command->state->channel_sequence_valid = command->state->sequence_dirty = 1;
    if (!duplicate && !(command->open->granted_access & (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA))) {
        command->status = SMB2_STATUS_ACCESS_DENIED;
        return;
    }
    if (!duplicate && request->ioctl.max_output_response < SMB2_FSCTL_OFFLOAD_WRITE_OUTPUT_SIZE) {
        command->status = SMB2_STATUS_INVALID_PARAMETER;
        return;
    }
    if (!ctx->token_valid) {
        command->status = SMB2_STATUS_OBJECT_NAME_NOT_FOUND; return;
    }
    if (ctx->length > UINT64_MAX - ctx->src_offset || ctx->length > UINT64_MAX - ctx->dst_offset ||
        (!duplicate && (request->ioctl.od_transfer_offset > ctx->token_length ||
                        ctx->length > ctx->token_length - request->ioctl.od_transfer_offset))) {
        command->status = SMB2_STATUS_INVALID_PARAMETER;
        return;
    }
    if (!ctx->source || chimera_smb_compound_file_prepare(compound, ctx->source) != SMB2_STATUS_SUCCESS) {
        command->status = duplicate ? SMB2_STATUS_INVALID_HANDLE : SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
        return;
    }
    if (!(ctx->source->open->granted_access & (SMB2_FILE_READ_DATA | SMB2_FILE_EXECUTE)) ||
        !(command->open->granted_access & (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA))) {
        command->status = SMB2_STATUS_ACCESS_DENIED;
        return;
    }
    if (duplicate && ctx->length &&
        chimera_memequal(ctx->source->handle->fh, ctx->source->handle->fh_len, command->handle->fh, command->handle->
                         fh_len) &&
        ctx->src_offset < ctx->dst_offset + ctx->length && ctx->dst_offset < ctx->src_offset + ctx->length) {
        command->status = SMB2_STATUS_NOT_SUPPORTED;
        return;
    }
    struct chimera_vfs_open_handle *identity = command->actor.op_handle;
    chimera_smb_copyoffload_io_owner(request, command->open, &command->actor);
    command->actor.op_handle = identity;
} /* smb_copyoffload_prepare */

static void
smb_copyoffload_publish(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    struct smb_copyoffload_compound *ctx = command->private_data;

    (void) compound;
    if (command->status == SMB2_STATUS_SUCCESS && command->request->ioctl.ctl_code == SMB2_FSCTL_OFFLOAD_WRITE) {
        /* The input length stays immutable during retries; only the accepted
         * response length replaces it for the existing wire encoder. */
        command->request->ioctl.od_copy_length = ctx->written;
    }
} /* smb_copyoffload_publish */

static void
smb_copyoffload_release(struct smb_vfs_command *command)
{
    free(command->private_data);
    command->private_data = NULL;
} /* smb_copyoffload_release */

const struct smb_vfs_command_ops chimera_smb_copyoffload_compound_ops = {
    .file_id   = smb_offload_file_id,
    .eligible  = smb_offload_eligible,
    .map_error = chimera_smb_copy_error_status,
    .build     = smb_copyoffload_build,
    .prepare   = smb_copyoffload_prepare,
    .complete  = smb_copyoffload_next,
    .publish   = smb_copyoffload_publish,
    .release   = smb_copyoffload_release,
};
