// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_string.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "xxhash.h"

void
chimera_smb_query_directory_readdir_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (request->query_directory.last_file_offset) {
        *request->query_directory.last_file_offset = 0;
    }

    chimera_smb_open_file_release(request, request->query_directory.open_file);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
    }

    if (request->query_directory.output_length) {
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
    } else {
        evpl_iovec_release(request->compound->thread->evpl, &request->query_directory.iov);
        chimera_smb_complete_request(request, SMB2_STATUS_NO_MORE_FILES);
    }

} /* chimera_smb_query_directory_readdir_complete */ /* chimera_smb_query_directory_readdir_complete */

int
chimera_smb_query_directory_readdir_callback(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct chimera_smb_request       *request = arg;
    struct chimera_server_smb_thread *thread  = request->compound->thread;
    uint16_t                         *namebuf;
    uint16_t                          namelen_padded;
    uint32_t                          file_index, expected_length;
    struct evpl_iovec_cursor          entry_cursor;
    struct chimera_smb_attrs          smb_attrs;
    int                               match;

    if (request->query_directory.pattern_length == 1 &&
        request->query_directory.pattern[0] == '*') {
        match = 1;
    } else {
        /* XXX this is assuming pattern is not a glob */
        if (namelen == request->query_directory.pattern_length) {
            match = strncmp(name, request->query_directory.pattern, namelen) == 0;
        } else {
            match = 0;
        }
    }

    if (!match) {
        return 0;
    }
    smb_attrs.smb_attr_mask = 0;

    file_index = (uint32_t) (XXH3_64bits(name, namelen) & 0xffffffff);

    namelen_padded = namelen ? namelen * 2 : 2;

    namelen_padded += 8 - (namelen_padded & 7);

    if ((request->query_directory.flags & SMB2_INDEX_SPECIFIED) &&
        (file_index != request->query_directory.file_index)) {
        return -1;
    }

    request->query_directory.flags &= ~SMB2_INDEX_SPECIFIED;

    switch (request->query_directory.info_class) {
        case SMB2_FILE_DIRECTORY_INFORMATION:
            expected_length = 4 + namelen_padded;
            break;
        case SMB2_FILE_BOTH_DIRECTORY_INFORMATION:
            expected_length = 94 + namelen_padded;
            break;
        case SMB2_FILE_NAMES_INFORMATION:
            expected_length = 64 + namelen_padded;
            break;
        case SMB2_FILE_FULL_DIRECTORY_INFORMATION:
            expected_length = 68 + namelen_padded;
            break;
        case SMB2_FILE_ID_BOTH_DIRECTORY_INFORMATION:
            expected_length = 102 + namelen_padded;
            break;
        case SMB2_FILE_ID_FULL_DIRECTORY_INFORMATION:
            expected_length = 74 + namelen_padded;
            break;
        default:
            chimera_smb_abort("Unsupported info class %d", request->query_directory.info_class);

    } /* switch */

    expected_length += (8 - (expected_length & 7)) & 7;

    if (request->query_directory.output_length + expected_length >
        request->query_directory.max_output_length) {
        return -1;
    }

    if (request->query_directory.output_length &&
        (request->query_directory.flags & SMB2_RETURN_SINGLE_ENTRY)) {
        return -1;
    }

    request->query_directory.last_file_offset = evpl_iovec_data(&request->query_directory.iov) +
        request->query_directory.output_length;

    evpl_iovec_cursor_init(&entry_cursor, &request->query_directory.iov, 1);

    evpl_iovec_cursor_skip(&entry_cursor, request->query_directory.output_length);

    evpl_iovec_cursor_append_uint32(&entry_cursor, expected_length);

    chimera_smb_marshal_attrs(attrs, &smb_attrs);

    switch (request->query_directory.info_class) {
        case SMB2_FILE_DIRECTORY_INFORMATION:
            evpl_iovec_cursor_append_uint32(&entry_cursor, file_index);
            evpl_iovec_cursor_append_uint64(&entry_cursor, 0);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_atime);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_mtime);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_ctime);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_size);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_alloc_size);
            evpl_iovec_cursor_append_uint32(&entry_cursor, smb_attrs.smb_attributes);
            evpl_iovec_cursor_append_uint32(&entry_cursor, namelen * 2);

            namebuf = evpl_iovec_cursor_data(&entry_cursor);
            chimera_smb_utf8_to_utf16le(&thread->iconv_ctx,
                                        name, namelen,
                                        namebuf, SMB_FILENAME_MAX);

            evpl_iovec_cursor_skip(&entry_cursor, namelen_padded);

            break;
        case SMB2_FILE_BOTH_DIRECTORY_INFORMATION:
            evpl_iovec_cursor_append_uint32(&entry_cursor, file_index);
            evpl_iovec_cursor_append_uint64(&entry_cursor, 0);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_atime);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_mtime);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_ctime);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_size);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_alloc_size);
            evpl_iovec_cursor_append_uint32(&entry_cursor, smb_attrs.smb_attributes);
            evpl_iovec_cursor_append_uint32(&entry_cursor, namelen * 2);
            evpl_iovec_cursor_append_uint32(&entry_cursor, 0);
            evpl_iovec_cursor_zero(&entry_cursor, 26); /* short name */


            namebuf = evpl_iovec_cursor_data(&entry_cursor);
            chimera_smb_utf8_to_utf16le(&thread->iconv_ctx,
                                        name, namelen,
                                        namebuf, SMB_FILENAME_MAX);

            evpl_iovec_cursor_skip(&entry_cursor, namelen_padded);

            break;
        case SMB2_FILE_NAMES_INFORMATION:
            evpl_iovec_cursor_append_uint32(&entry_cursor, namelen * 2);

            namebuf = evpl_iovec_cursor_data(&entry_cursor);
            chimera_smb_utf8_to_utf16le(&thread->iconv_ctx,
                                        name, namelen,
                                        namebuf, SMB_FILENAME_MAX);

            evpl_iovec_cursor_skip(&entry_cursor, namelen_padded);
            break;
        case SMB2_FILE_FULL_DIRECTORY_INFORMATION:
            evpl_iovec_cursor_append_uint32(&entry_cursor, file_index);
            evpl_iovec_cursor_append_uint64(&entry_cursor, 0);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_atime);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_mtime);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_ctime);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_size);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_alloc_size);
            evpl_iovec_cursor_append_uint32(&entry_cursor, smb_attrs.smb_attributes);
            evpl_iovec_cursor_append_uint32(&entry_cursor, namelen * 2);

            evpl_iovec_cursor_append_uint32(&entry_cursor, 0);
            namebuf = evpl_iovec_cursor_data(&entry_cursor);
            chimera_smb_utf8_to_utf16le(&thread->iconv_ctx,
                                        name, namelen,
                                        namebuf, SMB_FILENAME_MAX);

            evpl_iovec_cursor_skip(&entry_cursor, namelen_padded);

            break;

        case SMB2_FILE_ID_BOTH_DIRECTORY_INFORMATION:
            evpl_iovec_cursor_append_uint32(&entry_cursor, file_index);
            evpl_iovec_cursor_append_uint64(&entry_cursor, 0);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_atime);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_mtime);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_ctime);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_size);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_alloc_size);
            evpl_iovec_cursor_append_uint32(&entry_cursor, smb_attrs.smb_attributes);
            evpl_iovec_cursor_append_uint32(&entry_cursor, namelen * 2);
            evpl_iovec_cursor_append_uint32(&entry_cursor, 0);
            evpl_iovec_cursor_zero(&entry_cursor, 28); /* short name */
            evpl_iovec_cursor_append_uint64(&entry_cursor, attrs->va_ino);

            namebuf = evpl_iovec_cursor_data(&entry_cursor);
            chimera_smb_utf8_to_utf16le(&thread->iconv_ctx,
                                        name, namelen,
                                        namebuf, SMB_FILENAME_MAX);

            evpl_iovec_cursor_skip(&entry_cursor, namelen_padded);

            break;
        case SMB2_FILE_ID_FULL_DIRECTORY_INFORMATION:
            evpl_iovec_cursor_append_uint32(&entry_cursor, file_index);
            evpl_iovec_cursor_append_uint64(&entry_cursor, 0);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_atime);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_mtime);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_ctime);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_size);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_alloc_size);
            evpl_iovec_cursor_append_uint32(&entry_cursor, smb_attrs.smb_attributes);
            evpl_iovec_cursor_append_uint32(&entry_cursor, namelen * 2);
            evpl_iovec_cursor_append_uint32(&entry_cursor, 0);
            evpl_iovec_cursor_append_uint64(&entry_cursor, attrs->va_ino);

            namebuf = evpl_iovec_cursor_data(&entry_cursor);

            chimera_smb_utf8_to_utf16le(&thread->iconv_ctx,
                                        name, namelen,
                                        namebuf, SMB_FILENAME_MAX);

            evpl_iovec_cursor_skip(&entry_cursor, namelen_padded);

            break;
    } /* switch */

    request->query_directory.output_length += expected_length;

    request->query_directory.open_file->position = cookie;

    return 0;
} /* chimera_smb_query_directory_readdir_callback */

void
chimera_smb_query_directory(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct evpl                      *evpl   = thread->evpl;

    request->query_directory.open_file = chimera_smb_open_file_resolve(request, &request->query_directory.file_id);

    if (unlikely(!request->query_directory.open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    if (request->query_directory.flags & SMB2_RESTART_SCANS) {
        request->query_directory.open_file->position = 0;
    }

    if (request->query_directory.flags & SMB2_REOPEN) {
        request->query_directory.open_file->position = 0;
    }

    evpl_iovec_alloc(evpl,
                     request->query_directory.max_output_length,
                     4096,
                     1,
                     0, &request->query_directory.iov);

    chimera_vfs_readdir(
        thread->vfs_thread,
        &request->session_handle->session->cred,
        request->query_directory.open_file->handle,
        CHIMERA_VFS_ATTR_MASK_STAT,
        0, /* dir_attr_mask */
        request->query_directory.open_file->position,
        0, /* verifier */
        CHIMERA_VFS_READDIR_EMIT_DOT,
        chimera_smb_query_directory_readdir_callback,
        chimera_smb_query_directory_readdir_complete,
        request
        );
} /* chimera_smb_query_directory */

void
chimera_smb_query_directory_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_QUERY_DIRECTORY_REPLY_SIZE);
    evpl_iovec_cursor_append_uint16(reply_cursor, 64 + 8);
    evpl_iovec_cursor_append_uint32(reply_cursor, request->query_directory.output_length);

    if (request->query_directory.output_length) {

        evpl_iovec_set_length(&request->query_directory.iov, request->query_directory.output_length);

        evpl_iovec_cursor_inject(reply_cursor,
                                 &request->query_directory.iov,
                                 1,
                                 request->query_directory.output_length);
    }
} /* chimera_smb_query_directory_reply */

int
chimera_smb_parse_query_directory(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    uint16_t name_offset;
    uint16_t pattern16[SMB_FILENAME_MAX];

    if (unlikely(request->request_struct_size != SMB2_QUERY_DIRECTORY_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 QUERY_DIRECTORY request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_QUERY_DIRECTORY_REQUEST_SIZE);
        return -1;
    }

    evpl_iovec_cursor_get_uint8(request_cursor, &request->query_directory.info_class);
    evpl_iovec_cursor_get_uint8(request_cursor, &request->query_directory.flags);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->query_directory.file_index);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->query_directory.file_id.pid);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->query_directory.file_id.vid);
    evpl_iovec_cursor_get_uint16(request_cursor, &name_offset);
    evpl_iovec_cursor_get_uint16(request_cursor, &request->query_directory.pattern_length);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->query_directory.max_output_length);

    request->query_directory.output_length    = 0;
    request->query_directory.eof              = 1;
    request->query_directory.last_file_offset = NULL;

    if (request->query_directory.pattern_length > SMB_FILENAME_MAX) {
        chimera_smb_error("Received SMB2 QUERY_DIRECTORY request with invalid name length (%u > %u)",
                          request->query_directory.pattern_length, SMB_FILENAME_MAX);
        return -1;
    }

    evpl_iovec_cursor_copy(request_cursor, pattern16, request->query_directory.pattern_length);
    request->query_directory.pattern_length = chimera_smb_utf16le_to_utf8(&request->compound->thread->iconv_ctx,
                                                                          pattern16,
                                                                          request->query_directory.pattern_length,
                                                                          request->query_directory.pattern,
                                                                          sizeof(request->query_directory.pattern));

    return 0;
} /* chimera_smb_parse_query_directory */