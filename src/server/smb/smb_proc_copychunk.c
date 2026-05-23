// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb2.h"
#include "smb_session.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

/*
 * Server-side copy: FSCTL_SRV_REQUEST_RESUME_KEY + FSCTL_SRV_COPYCHUNK.
 *
 * The client opens the source file and asks for a 24-byte "resume key"
 * identifying that open (MS-SMB2 2.2.32.2/2.2.32.3).  It then opens the
 * destination and issues COPYCHUNK on the destination handle, passing the
 * source's resume key plus a list of {source offset, target offset,
 * length} chunks.  We map the resume key back to the source open and copy
 * each chunk server-side via chimera_vfs_copy_range, so the data never
 * round-trips through the client (and never through the client's oplock
 * cache, which is what makes copy_file_range coherent here).
 *
 * The resume key simply encodes the source open's FileId (persistent +
 * volatile); the client treats it opaquely.  COPYCHUNK on the destination
 * resolves that FileId within the same session to find the source handle.
 */

/* MS-SMB2 server-side copy limits (2.2.32.1). */
#define CHIMERA_SMB_CC_MAX_CHUNKS    16
#define CHIMERA_SMB_CC_MAX_CHUNK_LEN (1024 * 1024)
#define CHIMERA_SMB_CC_MAX_TOTAL_LEN (16 * 1024 * 1024)

static void chimera_smb_copychunk_next(
    struct chimera_smb_request *request);

/* Resolve the source open from the resume key, then start copying. */
void
chimera_smb_ioctl_request_resume_key(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open_file;

    open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);

    if (unlikely(!open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    /* The resume key is built from the open's FileId at reply time; nothing
     * else to do here. */
    chimera_smb_open_file_release(request, open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_ioctl_request_resume_key */

static void
chimera_smb_copychunk_done(
    struct chimera_smb_request *request,
    uint32_t                    status)
{
    if (request->ioctl.cc_src_open_file) {
        chimera_smb_open_file_release(request, request->ioctl.cc_src_open_file);
        request->ioctl.cc_src_open_file = NULL;
    }
    if (request->ioctl.cc_dst_open_file) {
        chimera_smb_open_file_release(request, request->ioctl.cc_dst_open_file);
        request->ioctl.cc_dst_open_file = NULL;
    }
    chimera_smb_complete_request(request, status);
} /* chimera_smb_copychunk_done */

static void
chimera_smb_copychunk_cb(
    enum chimera_vfs_error    error_code,
    uint64_t                  length,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        /* A backend without server-side copy support must answer
         * NOT_SUPPORTED so the client falls back to a normal read/write
         * copy rather than treating it as a hard error. */
        uint32_t status = (error_code == CHIMERA_VFS_ENOTSUP)
                          ? SMB2_STATUS_NOT_SUPPORTED
                          : SMB2_STATUS_INVALID_PARAMETER;

        chimera_smb_copychunk_done(request, status);
        return;
    }

    request->ioctl.cc_total_written += length;
    request->ioctl.cc_chunks_written++;
    request->ioctl.cc_chunk_idx++;

    chimera_smb_copychunk_next(request);
} /* chimera_smb_copychunk_cb */

/* Issue the next pending chunk, or finish if all are done. */
static void
chimera_smb_copychunk_next(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread *vfs_thread = request->compound->thread->vfs_thread;
    uint32_t                   i          = request->ioctl.cc_chunk_idx;

    if (i >= request->ioctl.cc_chunk_count) {
        chimera_smb_copychunk_done(request, SMB2_STATUS_SUCCESS);
        return;
    }

    chimera_vfs_copy_range(
        vfs_thread,
        &request->session_handle->session->cred,
        request->ioctl.cc_src_open_file->handle,
        request->ioctl.cc_chunks[i].src_offset,
        request->ioctl.cc_dst_open_file->handle,
        request->ioctl.cc_chunks[i].dst_offset,
        request->ioctl.cc_chunks[i].length,
        0,
        0,
        chimera_smb_copychunk_cb,
        request);
} /* chimera_smb_copychunk_next */

void
chimera_smb_ioctl_copychunk(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *dst_open_file, *src_open_file;
    uint64_t                      total = 0;
    uint32_t                      i;

    request->ioctl.cc_src_open_file  = NULL;
    request->ioctl.cc_dst_open_file  = NULL;
    request->ioctl.cc_chunk_idx      = 0;
    request->ioctl.cc_chunks_written = 0;
    request->ioctl.cc_total_written  = 0;

    if (request->ioctl.cc_chunk_count == 0 ||
        request->ioctl.cc_chunk_count > CHIMERA_SMB_CC_MAX_CHUNKS) {
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    for (i = 0; i < request->ioctl.cc_chunk_count; i++) {
        if (request->ioctl.cc_chunks[i].length == 0 ||
            request->ioctl.cc_chunks[i].length > CHIMERA_SMB_CC_MAX_CHUNK_LEN) {
            chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
            return;
        }
        total += request->ioctl.cc_chunks[i].length;
    }

    if (total > CHIMERA_SMB_CC_MAX_TOTAL_LEN) {
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* The FSCTL is sent on the destination handle. */
    dst_open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);
    if (unlikely(!dst_open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    /* The source is identified by the resume key (its FileId). */
    src_open_file = chimera_smb_open_file_resolve(request, &request->ioctl.cc_src_file_id);
    if (unlikely(!src_open_file)) {
        chimera_smb_open_file_release(request, dst_open_file);
        /* An unknown/expired resume key -> OBJECT_NAME_NOT_FOUND per
         * MS-SMB2 3.3.5.15.6. */
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
        return;
    }

    request->ioctl.cc_dst_open_file = dst_open_file;
    request->ioctl.cc_src_open_file = src_open_file;

    chimera_smb_copychunk_next(request);
} /* chimera_smb_ioctl_copychunk */
