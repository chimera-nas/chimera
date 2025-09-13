// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_string.h"
#include "common/macros.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_release.h"

static void
chimera_smb_query_info_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    switch (request->query_info.info_type) {
        case SMB2_INFO_FILE:
            /* Marshal attributes based on the requested info class */
            switch (request->query_info.info_class) {
                case SMB2_INFO_FILE:
                    chimera_smb_marshal_basic_info(attr, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_STANDARD_INFO:
                    chimera_smb_marshal_standard_info(attr, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_INTERNAL_INFO:
                    chimera_smb_marshal_internal_info(attr, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_EA_INFO:
                    chimera_smb_marshal_ea_info(attr, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_COMPRESSION_INFO:
                    chimera_smb_marshal_compression_info(attr, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_NETWORK_OPEN_INFO:
                    chimera_smb_marshal_network_open_info(attr, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_ATTRIBUTE_TAG_INFO:
                    chimera_smb_marshal_attribute_tag_info(attr, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_ALL_INFO:
                    /* For FileAllInformation, we need all attributes */
                    chimera_smb_marshal_attrs(attr, &request->query_info.r_attrs);
                    break;
                default:
                    chimera_smb_abort("Unsupported info class %d",
                                      request->query_info.info_type);
                    break;
            } /* switch */
            break;
        case SMB2_INFO_FILESYSTEM:
            switch (request->query_info.info_class) {
                case SMB2_FILE_FS_FULL_SIZE_INFO:
                    chimera_smb_marshal_fs_full_size_info(attr, &request->query_info.r_fs_attrs);
                    break;
            } /* switch */
    } /* switch */

    if (unlikely(error_code)) {
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
    } else {
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
    }
} /* chimera_smb_query_info_getattr_callback */

void
chimera_smb_query_info(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread       = request->compound->thread;
    uint32_t                          getattr_mask = 0;
    uint32_t                          status       = SMB2_STATUS_SUCCESS;


    request->query_info.open_file = chimera_smb_open_file_lookup(request, &request->query_info.file_id);

    if (unlikely(!request->query_info.open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    switch (request->query_info.info_type) {
        case SMB2_INFO_FILE:
            /* Calculate the output buffer length based on the info class */
            switch (request->query_info.info_class) {
                case SMB2_FILE_BASIC_INFO:
                    request->query_info.output_length = SMB2_FILE_BASIC_INFO_SIZE;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STAT;
                    break;
                case SMB2_FILE_STANDARD_INFO:
                    request->query_info.output_length = SMB2_FILE_STANDARD_INFO_SIZE;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STAT;
                    break;
                case SMB2_FILE_INTERNAL_INFO:
                    request->query_info.output_length = SMB2_FILE_INTERNAL_INFO_SIZE;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STAT;
                    break;
                case SMB2_FILE_EA_INFO:
                    request->query_info.output_length = SMB2_FILE_EA_INFO_SIZE;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STAT;
                    break;
                case SMB2_FILE_COMPRESSION_INFO:
                    request->query_info.output_length = SMB2_FILE_COMPRESSION_INFO_SIZE;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STAT;
                    break;
                case SMB2_FILE_NETWORK_OPEN_INFO:
                    request->query_info.output_length = SMB2_FILE_NETWORK_OPEN_INFO_SIZE;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STAT;
                    break;
                case SMB2_FILE_ATTRIBUTE_TAG_INFO:
                    request->query_info.output_length = SMB2_FILE_ATTRIBUTE_TAG_INFO_SIZE;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STAT;
                    break;
                case SMB2_FILE_ALL_INFO:
                    request->query_info.output_length = SMB2_FILE_ALL_INFO_FIXED_SIZE + request->query_info.open_file->
                        name_len + 4;
                    getattr_mask = CHIMERA_VFS_ATTR_MASK_STAT;
                    break;
                case SMB2_FILE_FULL_EA_INFO:
                    request->query_info.output_length = 8;
                    break;
                default:

                    chimera_smb_error("Unsupported info class %d in reply, defaulting to ALL_INFO size",
                                      request->query_info.info_class);
                    status = SMB2_STATUS_INVALID_PARAMETER;
                    return;
            } /* switch */
            break;
        case SMB2_INFO_FILESYSTEM:
            switch (request->query_info.info_class) {
                case SMB2_FILE_FS_DEVICE_INFO:
                    request->query_info.output_length = 8;
                    break;
                case SMB2_FILE_FS_ATTRIBUTE_INFO:
                    request->query_info.output_length = 16;
                    break;
                case SMB2_FILE_FS_FULL_SIZE_INFO:
                    request->query_info.output_length = 32;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STATFS;
                    break;
            } /* switch */
            break;
        default:
            chimera_smb_error("Unsupported information type: %d", request->query_info.info_type);
            status = SMB2_STATUS_INVALID_PARAMETER;
            break;
    } /* switch */

    if (getattr_mask) {
        /* Get the file attributes */
        chimera_vfs_getattr(thread->vfs_thread,
                            request->query_info.open_file->handle,
                            getattr_mask,
                            chimera_smb_query_info_getattr_callback,
                            request);
    } else {
        chimera_smb_complete_request(request, status);
    }

} /* chimera_smb_query_info */

void
chimera_smb_query_info_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    uint16_t namebuf[8];

    /* Append the query info reply header */
    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_QUERY_INFO_REPLY_SIZE);
    evpl_iovec_cursor_append_uint16(reply_cursor, 64 + 8);  /* Fixed offset from SMB protocol */
    evpl_iovec_cursor_append_uint32(reply_cursor, request->query_info.output_length);

    switch (request->query_info.info_type) {
        case SMB2_INFO_FILE:
            /* Append the attributes based on the info class */
            switch (request->query_info.info_class) {
                case SMB2_INFO_FILE:
                    chimera_smb_append_basic_info(reply_cursor, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_STANDARD_INFO:
                    chimera_smb_append_standard_info(reply_cursor, request->query_info.open_file, &request->query_info.
                                                     r_attrs);
                    break;
                case SMB2_FILE_INTERNAL_INFO:
                    chimera_smb_append_internal_info(reply_cursor, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_EA_INFO:
                    chimera_smb_append_ea_info(reply_cursor, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_COMPRESSION_INFO:
                    chimera_smb_append_compression_info(reply_cursor, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_NETWORK_OPEN_INFO:
                    chimera_smb_append_network_open_info(reply_cursor, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_ATTRIBUTE_TAG_INFO:
                    chimera_smb_append_attribute_tag_info(reply_cursor, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_ALL_INFO:
                    chimera_smb_append_all_info(reply_cursor, request->query_info.open_file, &request->query_info.
                                                r_attrs);
                case SMB2_FILE_FULL_EA_INFO:
                    evpl_iovec_cursor_append_uint32(reply_cursor, 0);
                    evpl_iovec_cursor_append_uint32(reply_cursor, 0);
                    break;
            } /* switch */
            break;
        case SMB2_INFO_FILESYSTEM:
            switch (request->query_info.info_class) {
                case SMB2_FILE_FS_DEVICE_INFO:
                    evpl_iovec_cursor_append_uint32(reply_cursor, 0x14); /* Network File System */
                    evpl_iovec_cursor_append_uint32(reply_cursor, 0x20); /* Remote Device */
                    break;
                case SMB2_FILE_FS_ATTRIBUTE_INFO:
                    evpl_iovec_cursor_append_uint32(reply_cursor, 0);
                    evpl_iovec_cursor_append_uint32(reply_cursor, 255);
                    evpl_iovec_cursor_append_uint32(reply_cursor, 4);

                    chimera_smb_utf8_to_utf16le(
                        &request->compound->thread->iconv_ctx,
                        "fs",
                        2,
                        namebuf,
                        8);
                    evpl_iovec_cursor_append_blob(reply_cursor, namebuf, 4);
                    break;
                case SMB2_FILE_FS_FULL_SIZE_INFO:
                    evpl_iovec_cursor_append_uint64(reply_cursor, request->query_info.r_fs_attrs.
                                                    smb_total_allocation_units);
                    evpl_iovec_cursor_append_uint64(reply_cursor, request->query_info.r_fs_attrs.
                                                    smb_caller_available_allocation_units);
                    evpl_iovec_cursor_append_uint64(reply_cursor, request->query_info.r_fs_attrs.
                                                    smb_actual_available_allocation_units);
                    evpl_iovec_cursor_append_uint32(reply_cursor, request->query_info.r_fs_attrs.
                                                    smb_sectors_per_allocation_unit);
                    evpl_iovec_cursor_append_uint32(reply_cursor, request->query_info.r_fs_attrs.smb_bytes_per_sector);
                    break;
            } /* switch */
            break;
        default:
            chimera_smb_abort("Unsupported information type: %d", request->query_info.info_type);
            break;
    } /* switch */

} /* chimera_smb_query_info_reply */

int
chimera_smb_parse_query_info(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    uint32_t max_response_size, input_size;
    uint16_t input_offset;

    if (unlikely(request->request_struct_size != SMB2_QUERY_INFO_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 QUERY_INFO request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_QUERY_INFO_REQUEST_SIZE);
        return -1;
    }

    evpl_iovec_cursor_get_uint8(request_cursor, &request->query_info.info_type);
    evpl_iovec_cursor_get_uint8(request_cursor, &request->query_info.info_class);
    evpl_iovec_cursor_get_uint32(request_cursor, &max_response_size);
    evpl_iovec_cursor_get_uint16(request_cursor, &input_offset);
    evpl_iovec_cursor_get_uint32(request_cursor, &input_size);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->query_info.addl_info);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->query_info.flags);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->query_info.file_id.pid);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->query_info.file_id.vid);

    return 0;
} /* chimera_smb_parse_query_info */
