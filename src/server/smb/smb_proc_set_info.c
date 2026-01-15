// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "common/misc.h"
#include "vfs/vfs.h"

static void
chimera_smb_set_info_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    chimera_smb_open_file_release(request, request->set_info.open_file);

    chimera_smb_complete_request(request, error_code ? SMB2_STATUS_INTERNAL_ERROR : SMB2_STATUS_SUCCESS);
} /* chimera_smb_set_info_callback */

static void
chimera_smb_set_info_remove_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    chimera_smb_open_file_release(request, request->set_info.open_file);

    chimera_smb_complete_request(request, error_code ? SMB2_STATUS_INTERNAL_ERROR : SMB2_STATUS_SUCCESS);
} /* chimera_smb_set_info_remove_callback */

static void
chimera_smb_set_info_open_unlink_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request   *request   = private_data;
    struct chimera_smb_open_file *open_file = request->set_info.open_file;

    request->set_info.parent_handle = oh;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, request->set_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    chimera_vfs_remove(
        request->compound->thread->vfs_thread,
        oh,
        open_file->name,
        open_file->name_len,
        NULL,
        0,
        0,
        0,
        chimera_smb_set_info_remove_callback,
        request);

} /* chimera_smb_set_info_open_unlink_callback */

void
chimera_smb_set_info(struct chimera_smb_request *request)
{
    request->set_info.open_file = chimera_smb_open_file_resolve(request, &request->set_info.file_id);

    switch (request->set_info.info_type) {
        case SMB2_INFO_FILE:
            switch (request->set_info.info_class) {
                case SMB2_FILE_BASIC_INFO:

                    chimera_smb_unmarshal_basic_info(&request->set_info.attrs, &request->set_info.vfs_attrs);

                    chimera_vfs_setattr(
                        request->compound->thread->vfs_thread,
                        request->set_info.open_file->handle,
                        &request->set_info.vfs_attrs,
                        0,
                        0,
                        chimera_smb_set_info_callback,
                        request);
                    break;
                case SMB2_FILE_ENDOFFILE_INFO:
                    chimera_smb_unmarshal_end_of_file_info(&request->set_info.attrs, &request->set_info.vfs_attrs);

                    chimera_vfs_setattr(
                        request->compound->thread->vfs_thread,
                        request->set_info.open_file->handle,
                        &request->set_info.vfs_attrs,
                        0,
                        0,
                        chimera_smb_set_info_callback,
                        request);
                    break;
                case SMB2_FILE_DISPOSITION_INFO:
                    if (request->set_info.open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE) {
                        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
                    } else {
                        chimera_vfs_open(
                            request->compound->thread->vfs_thread,
                            request->set_info.open_file->parent_fh,
                            request->set_info.open_file->parent_fh_len,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                            chimera_smb_set_info_open_unlink_callback,
                            request);

                    }
                    break;
                default:
                    chimera_smb_open_file_release(request, request->set_info.open_file);
                    chimera_smb_complete_request(request, SMB2_STATUS_NOT_IMPLEMENTED);
            } /* switch */
            break;
        default:
            chimera_smb_open_file_release(request, request->set_info.open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_NOT_IMPLEMENTED);
    } /* switch */
} /* chimera_smb_set_info */

void
chimera_smb_set_info_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_SET_INFO_REPLY_SIZE);
} /* chimera_smb_set_info_reply */

int
chimera_smb_parse_set_info(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    if (unlikely(request->request_struct_size != SMB2_SET_INFO_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 SET_INFO request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_SET_INFO_REQUEST_SIZE);
        return -1;
    }

    evpl_iovec_cursor_get_uint8(request_cursor, &request->set_info.info_type);
    evpl_iovec_cursor_get_uint8(request_cursor, &request->set_info.info_class);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->set_info.buffer_length);
    evpl_iovec_cursor_get_uint16(request_cursor, &request->set_info.buffer_offset);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->set_info.addl_info);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->set_info.file_id.pid);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->set_info.file_id.vid);

    evpl_iovec_cursor_skip(request_cursor,
                           request->set_info.buffer_offset - evpl_iovec_cursor_consumed(request_cursor));

    request->set_info.attrs.smb_attr_mask = 0;

    switch (request->set_info.info_type) {
        case SMB2_INFO_FILE:
            switch (request->set_info.info_class) {
                case SMB2_FILE_BASIC_INFO:
                    chimera_smb_parse_basic_info(request_cursor, &request->set_info.attrs);
                    break;
                case SMB2_FILE_DISPOSITION_INFO:
                    chimera_smb_parse_disposition_info(request_cursor, &request->set_info.attrs);
                    break;
                case SMB2_FILE_ENDOFFILE_INFO:
                    chimera_smb_parse_end_of_file_info(
                        request_cursor,
                        &request->set_info.attrs);
                    break;
            } /* switch */
    } /* switch */
    return 0;
} /* chimera_smb_parse_set_info */