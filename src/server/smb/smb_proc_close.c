// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_sharemode.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void chimera_smb_close_release(
    struct chimera_smb_request *request);

static void
chimera_smb_close_doc_remove_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    if (error_code) {
        chimera_smb_debug("delete-on-close: remove_at failed for '%.*s' (error %d)",
                          request->close.doc_info.name_len,
                          request->close.doc_info.name,
                          error_code);
    }

    chimera_vfs_release(vfs_thread, request->close.parent_handle);

    /* Close the backend VFS module handle that was detached from the cache */
    chimera_vfs_close(vfs_thread,
                      request->close.doc_info.close_module,
                      request->close.doc_info.close_private,
                      request->close.doc_info.close_hash,
                      NULL,
                      NULL);

    chimera_smb_open_file_release(request, request->close.open_file);

    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_close_doc_remove_callback */

static void
chimera_smb_close_doc_open_parent_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    if (error_code != CHIMERA_VFS_OK) {
        /* Cannot open parent directory — skip deletion, but still close backend */
        chimera_smb_debug("delete-on-close: failed to open parent dir for '%.*s' (error %d)",
                          request->close.doc_info.name_len,
                          request->close.doc_info.name,
                          error_code);

        chimera_vfs_close(vfs_thread,
                          request->close.doc_info.close_module,
                          request->close.doc_info.close_private,
                          request->close.doc_info.close_hash,
                          NULL,
                          NULL);

        chimera_smb_open_file_release(request, request->close.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    request->close.parent_handle = oh;

    chimera_vfs_remove_at(
        vfs_thread,
        &request->close.doc_info.cred,
        oh,
        request->close.doc_info.name,
        request->close.doc_info.name_len,
        NULL,
        0,
        0,
        0,
        chimera_smb_close_doc_remove_callback,
        request);
} /* chimera_smb_close_doc_open_parent_callback */

/*
 * Release the VFS handle and check for delete-on-close.
 *
 * If this was the last reference and DOC was set on the VFS handle,
 * perform the unlink synchronously before completing the close request.
 * Otherwise just release and complete.
 */
static void
chimera_smb_close_release(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open_file  = request->close.open_file;
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    int                           need_doc;

    if (!open_file->handle) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    need_doc = chimera_vfs_release_doc(vfs_thread,
                                       open_file->handle,
                                       &request->close.doc_info);

    /* Detach VFS handle — it has been released (or consumed by DOC) */
    open_file->handle = NULL;

    if (need_doc && request->close.doc_info.parent_fh_len > 0) {
        chimera_vfs_open_fh(
            vfs_thread,
            &request->close.doc_info.cred,
            request->close.doc_info.parent_fh,
            request->close.doc_info.parent_fh_len,
            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
            chimera_smb_close_doc_open_parent_callback,
            request);
    } else {
        if (need_doc) {
            /* DOC set but no parent fh — close backend handle directly */
            chimera_vfs_close(vfs_thread,
                              request->close.doc_info.close_module,
                              request->close.doc_info.close_private,
                              request->close.doc_info.close_hash,
                              NULL,
                              NULL);
        }
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
    }
} /* chimera_smb_close_release */

static void
chimera_smb_close_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (unlikely(error_code)) {
        memset(&request->close.r_attrs, 0, sizeof(request->close.r_attrs));
    } else {
        chimera_smb_marshal_attrs(attr, &request->close.r_attrs);
    }

    /* Always go through close_release so DOC fires even if getattr failed */
    chimera_smb_close_release(request);
} /* chimera_smb_close_getattr_callback */


void
chimera_smb_close(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;

    request->close.open_file = chimera_smb_open_file_close(request, &request->close.file_id);

    if (unlikely(!request->close.open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    /* Release share mode entry for regular file opens */
    if (request->close.open_file->type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE &&
        request->tree->share) {
        chimera_smb_sharemode_release(&request->tree->share->sharemode,
                                      request->close.open_file);
    }

    if (request->close.flags & SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB) {

        chimera_vfs_getattr(thread->vfs_thread,
                            &request->session_handle->session->cred,
                            request->close.open_file->handle,
                            CHIMERA_VFS_ATTR_MASK_STAT,
                            chimera_smb_close_getattr_callback,
                            request);

    } else {
        memset(&request->close.r_attrs, 0, sizeof(request->close.r_attrs));
        chimera_smb_close_release(request);
    }

} /* chimera_smb_close */

void
chimera_smb_close_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_CLOSE_REPLY_SIZE);
    evpl_iovec_cursor_append_uint16(reply_cursor, request->close.flags);

    if (request->close.flags & SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB) {
        chimera_smb_append_network_open_info(reply_cursor, &request->close.r_attrs);
    } else {
        chimera_smb_append_null_network_open_info_null(reply_cursor);
    }

} /* chimera_smb_close_reply */

int
chimera_smb_parse_close(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{

    if (unlikely(request->request_struct_size != SMB2_CLOSE_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 CLOSE request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_CLOSE_REQUEST_SIZE);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    evpl_iovec_cursor_get_uint16(request_cursor, &request->close.flags);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->close.file_id.pid);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->close.file_id.vid);


    return 0;
} /* chimera_smb_parse_close */
