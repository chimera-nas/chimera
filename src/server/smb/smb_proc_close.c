// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_sharemode.h"
#include "smb_notify.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void chimera_smb_close_release(
    struct chimera_smb_request *request);

/* Fire-and-forget completion for the persistent-handle record delete. */
static void
chimera_smb_close_durable_delete_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    (void) error_code;
    (void) private_data;
} /* chimera_smb_close_durable_delete_callback */

/* Map a delete-on-close remove_at failure to the SMB CLOSE response status.
 * MS-FSA close semantics: the close itself succeeds, but the delete failure
 * (ENOTEMPTY on a non-empty directory most commonly, also EACCES) must be
 * reported to the client so it knows the object survived.  Silently turning
 * every failure into SUCCESS makes smb2_deltree believe its recursive teardown
 * worked, leaving a stale BASEDIR / leftover entries that wreck the next
 * subtest's setup. */
static inline uint32_t
chimera_smb_close_doc_status(enum chimera_vfs_error error_code)
{
    switch (error_code) {
        case CHIMERA_VFS_OK:        return SMB2_STATUS_SUCCESS;
        case CHIMERA_VFS_ENOTEMPTY: return SMB2_STATUS_DIRECTORY_NOT_EMPTY;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:     return SMB2_STATUS_ACCESS_DENIED;
        case CHIMERA_VFS_ENOENT:    return SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
        default:                    return SMB2_STATUS_INTERNAL_ERROR;
    } /* switch */
} /* chimera_smb_close_doc_status */

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

    chimera_smb_complete_request(request, chimera_smb_close_doc_status(error_code));
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

/* Completion of a stream delete-on-close remove_stream. */
static void
chimera_smb_close_stream_remove_callback(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    if (error_code) {
        chimera_smb_debug("stream delete-on-close: remove_stream failed (error %d)",
                          error_code);
    }

    chimera_vfs_release(vfs_thread, request->close.parent_handle);
    chimera_smb_open_file_release(request, request->close.open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_close_stream_remove_callback */

/* The base file is open; remove the named stream that was flagged
 * delete-on-close. */
static void
chimera_smb_close_stream_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request   *request    = private_data;
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file  = request->close.open_file;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    request->close.parent_handle = oh;

    chimera_vfs_remove_stream(
        vfs_thread,
        &request->session_handle->session->cred,
        oh,
        open_file->stream_name,
        open_file->stream_name_len,
        chimera_smb_close_stream_remove_callback,
        request);
} /* chimera_smb_close_stream_open_callback */

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
    int                           stream_delete;

    if (!open_file->handle) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    /* A named stream flagged delete-on-close removes only the stream (never
     * the base file) and never armed the VFS doc mechanism. */
    stream_delete = (open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM) &&
        (open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE) &&
        open_file->base_fh_len > 0;

    need_doc = chimera_vfs_release_doc(vfs_thread,
                                       open_file->handle,
                                       &request->close.doc_info);

    /* Detach VFS handle — it has been released (or consumed by DOC) */
    open_file->handle = NULL;

    if (stream_delete) {
        /* The stream handle is released above; open the base file and remove
         * the stream.  open_file (with stream_name/base_fh) stays valid until
         * the remove completes. */
        chimera_vfs_open_fh(
            vfs_thread,
            &request->session_handle->session->cred,
            open_file->base_fh,
            open_file->base_fh_len,
            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
            chimera_smb_close_stream_open_callback,
            request);
        return;
    }

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

    /* Clean up any notify watches on this open file */
    if (request->close.open_file->notify_state) {
        chimera_smb_notify_close(thread->shared->vfs->vfs_notify,
                                 request->close.open_file->notify_state);
        request->close.open_file->notify_state = NULL;
    }

    /* Release share mode entry for regular file opens */
    if (request->close.open_file->type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE &&
        request->tree->share) {
        chimera_smb_sharemode_release(&request->tree->share->sharemode,
                                      request->close.open_file);
    }

    /* Persistent handle: delete its backend record so a server restart does
    * not resurrect a handle the client explicitly closed.  Best-effort and
    * fire-and-forget; routed to the share's backend via the file handle. */
    if ((request->close.open_file->flags & CHIMERA_SMB_OPEN_FILE_PERSISTED) &&
        request->close.open_file->handle) {
        uint8_t  dkey[CHIMERA_SMB_DURABLE_KEY_LEN];
        uint32_t dkey_len = chimera_smb_durable_key(dkey, request->close.open_file->file_id.pid);

        chimera_vfs_delete_key_at(thread->vfs_thread,
                                  &request->session_handle->session->cred,
                                  request->close.open_file->handle->fh,
                                  request->close.open_file->handle->fh_len,
                                  dkey, dkey_len,
                                  chimera_smb_close_durable_delete_callback, NULL);
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

    int prc = 0;
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &request->close.flags);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->close.file_id.pid);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->close.file_id.vid);

    if (unlikely(prc)) {
        chimera_smb_error("Received SMB2 CLOSE request truncated in fixed body");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

    return 0;
} /* chimera_smb_parse_close */
