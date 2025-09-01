// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "common/macros.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_release.h"

/* SMB2 information classes for FileInformation */
#define SMB2_FILE_BASIC_INFO              0x04
#define SMB2_FILE_STANDARD_INFO           0x05
#define SMB2_FILE_INTERNAL_INFO           0x06
#define SMB2_FILE_EA_INFO                 0x07
#define SMB2_FILE_ACCESS_INFO             0x08
#define SMB2_FILE_RENAME_INFO             0x0A
#define SMB2_FILE_DISPOSITION_INFO        0x0D
#define SMB2_FILE_POSITION_INFO           0x0E
#define SMB2_FILE_FULL_EA_INFO            0x0F
#define SMB2_FILE_MODE_INFO               0x10
#define SMB2_FILE_ALIGNMENT_INFO          0x11
#define SMB2_FILE_ALL_INFO                0x12
#define SMB2_FILE_ALLOCATION_INFO         0x13
#define SMB2_FILE_ENDOFFILE_INFO          0x14
#define SMB2_FILE_ALTERNATE_NAME_INFO     0x15
#define SMB2_FILE_STREAM_INFO             0x16
#define SMB2_FILE_PIPE_INFO               0x17
#define SMB2_FILE_COMPRESSION_INFO        0x0C
#define SMB2_FILE_NETWORK_OPEN_INFO       0x22
#define SMB2_FILE_ATTRIBUTE_TAG_INFO      0x23

/* SMB2 information types */
#define SMB2_INFO_FILE                    0x01
#define SMB2_INFO_FILESYSTEM              0x02
#define SMB2_INFO_SECURITY                0x03
#define SMB2_INFO_QUOTA                   0x04

/* Fixed sizes for various information classes (per SMB spec) */
#define SMB2_FILE_BASIC_INFO_SIZE         40
#define SMB2_FILE_STANDARD_INFO_SIZE      24
#define SMB2_FILE_INTERNAL_INFO_SIZE      8
#define SMB2_FILE_EA_INFO_SIZE            4
#define SMB2_FILE_ACCESS_INFO_SIZE        4
#define SMB2_FILE_POSITION_INFO_SIZE      8
#define SMB2_FILE_MODE_INFO_SIZE          4
#define SMB2_FILE_ALIGNMENT_INFO_SIZE     4
#define SMB2_FILE_COMPRESSION_INFO_SIZE   16
#define SMB2_FILE_NETWORK_OPEN_INFO_SIZE  56
#define SMB2_FILE_ATTRIBUTE_TAG_INFO_SIZE 8

/*
 * FileAllInformation includes all the following structures:
 * - Basic (40)
 * - Standard (24)
 * - Internal (8)
 * - EA (4)
 * - Access (4)
 * - Position (8)
 * - Mode (4)
 * - Alignment (4)
 * - FileNameInformation (4 bytes for FileNameLength)
 * The actual FileName string is variable length and follows this fixed portion.
 * The fixed portion is 100 bytes.
 */
#define SMB2_FILE_ALL_INFO_FIXED_SIZE     100

static void
chimera_smb_query_info_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request       *request = private_data;
    struct chimera_server_smb_thread *thread  = request->compound->thread;

    chimera_vfs_release(thread->vfs_thread, request->query_info.open_file->handle);

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

    if (unlikely(error_code)) {
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
    } else {
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
    }
} /* chimera_smb_query_info_getattr_callback */

void
chimera_smb_query_info(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;

    request->query_info.open_file = chimera_smb_open_file_lookup(request, &request->query_info.file_id);

    if (unlikely(!request->query_info.open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* Calculate the output buffer length based on the info class */
    switch (request->query_info.info_class) {
        case SMB2_FILE_BASIC_INFO:
            request->query_info.output_length = SMB2_FILE_BASIC_INFO_SIZE;
            break;
        case SMB2_FILE_STANDARD_INFO:
            request->query_info.output_length = SMB2_FILE_STANDARD_INFO_SIZE;
            break;
        case SMB2_FILE_INTERNAL_INFO:
            request->query_info.output_length = SMB2_FILE_INTERNAL_INFO_SIZE;
            break;
        case SMB2_FILE_EA_INFO:
            request->query_info.output_length = SMB2_FILE_EA_INFO_SIZE;
            break;
        case SMB2_FILE_COMPRESSION_INFO:
            request->query_info.output_length = SMB2_FILE_COMPRESSION_INFO_SIZE;
            break;
        case SMB2_FILE_NETWORK_OPEN_INFO:
            request->query_info.output_length = SMB2_FILE_NETWORK_OPEN_INFO_SIZE;
            break;
        case SMB2_FILE_ATTRIBUTE_TAG_INFO:
            request->query_info.output_length = SMB2_FILE_ATTRIBUTE_TAG_INFO_SIZE;
            break;
        case SMB2_FILE_ALL_INFO:
            request->query_info.output_length = SMB2_FILE_ALL_INFO_FIXED_SIZE + request->query_info.open_file->name_len;
            break;
        default:
            chimera_smb_error("Unsupported info class %d in reply, defaulting to ALL_INFO size",
                              request->query_info.info_class);
            chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
            return;
    } /* switch */

    /* Only support FILE information type for now */
    if (request->query_info.info_type != SMB2_INFO_FILE) {
        chimera_smb_error("Unsupported information type: %d", request->query_info.info_type);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* Get the file attributes */
    chimera_vfs_getattr(thread->vfs_thread,
                        request->query_info.open_file->handle,
                        CHIMERA_VFS_ATTR_MASK_STAT,
                        chimera_smb_query_info_getattr_callback,
                        request);
} /* chimera_smb_query_info */

void
chimera_smb_query_info_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    /* Append the query info reply header */
    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_QUERY_INFO_REPLY_SIZE);
    evpl_iovec_cursor_append_uint16(reply_cursor, 64 + 8);  /* Fixed offset from SMB protocol */
    evpl_iovec_cursor_append_uint32(reply_cursor, request->query_info.output_length);

    /* Append the attributes based on the info class */
    switch (request->query_info.info_class) {
        case SMB2_INFO_FILE:
            chimera_smb_append_basic_info(reply_cursor, &request->query_info.r_attrs);
            break;
        case SMB2_FILE_STANDARD_INFO:
            chimera_smb_append_standard_info(reply_cursor, request->query_info.open_file, &request->query_info.r_attrs);
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
            chimera_smb_append_all_info(reply_cursor, request->query_info.open_file, &request->query_info.r_attrs);
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
