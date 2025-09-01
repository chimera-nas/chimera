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
#include "xxhash.h"

void
chimera_smb_query_directory_readdir_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
    }

    if (request->query_directory.output_length) {
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
    } else {
        evpl_iovec_release(&request->query_directory.iov);
        chimera_smb_complete_request(request, SMB2_STATUS_NO_MORE_FILES);
    }

} /* chimera_smb_query_directory_readdir_complete */

int
chimera_smb_query_directory_readdir_callback(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct chimera_smb_request             *request = arg;
    struct chimera_server_smb_thread       *thread  = request->compound->thread;
    struct smb2_file_directory_information *info;
    uint16_t                               *namebuf;
    int                                     namelen16;
    int                                     pad;
    uint32_t                                file_index;

    file_index = (uint32_t) (XXH3_64bits(name, namelen) & 0xffffffff);

    if ((request->query_directory.flags & SMB2_INDEX_SPECIFIED) &&
        (file_index != request->query_directory.file_index)) {
        return -1;
    }

    request->query_directory.flags &= ~SMB2_INDEX_SPECIFIED;

    if (request->query_directory.output_length + sizeof(*info) + namelen * 3 >
        request->query_directory.max_output_length) {
        return -1;
    }

    if (request->query_directory.output_length &&
        (request->query_directory.flags & SMB2_RETURN_SINGLE_ENTRY)) {
        return -1;
    }

    info    = evpl_iovec_data(&request->query_directory.iov) + request->query_directory.output_length;
    namebuf = (uint16_t *) (info + 1);

    namelen16 = chimera_smb_utf8_to_utf16le(&thread->iconv_ctx,
                                            name, namelen,
                                            namebuf, SMB_FILENAME_MAX);

    if (request->query_directory.last_file_info) {
        request->query_directory.last_file_info->next_entry_offset = (void *) info -
            (void *) request->query_directory.last_file_info;
    }

    info->next_entry_offset = 0;
    info->file_index        = file_index;
    info->crttime           = 0;
    info->atime             = chimera_nt_time(&attrs->va_atime);
    info->mtime             = chimera_nt_time(&attrs->va_mtime);
    info->ctime             = chimera_nt_time(&attrs->va_ctime);
    info->size              = attrs->va_size;
    info->alloc_size        = attrs->va_space_used;
    info->attributes        = 0;
    info->name_length       = namelen16;
    info->ea_size           = 0;
    info->reserved          = 0;
    info->file_id           = 0;

    request->query_directory.output_length += sizeof(struct smb2_file_directory_information) + namelen16;

    pad = (8 - (request->query_directory.output_length & 7)) & 7;

    memset(namebuf + namelen16, 0, pad);

    request->query_directory.output_length += pad;

    request->query_directory.last_file_info = info;

    request->query_directory.open_file->position = cookie + 1;

    return 0;
} /* chimera_smb_query_directory_readdir_callback */

void
chimera_smb_query_directory(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct evpl                      *evpl   = thread->evpl;

    request->query_directory.open_file = chimera_smb_open_file_lookup(request, &request->query_directory.file_id);

    if (unlikely(!request->query_directory.open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    if (request->query_directory.flags & SMB2_RESTART_SCANS) {
        request->query_directory.open_file->position = 0;
    } else {
        memcpy(request->query_directory.pattern, request->query_directory.open_file->pattern, request->query_directory.
               pattern_length);
        request->query_directory.pattern_len = request->query_directory.pattern_length;
    }

    if (request->query_directory.flags & SMB2_REOPEN) {
        request->query_directory.open_file->position = 0;
    }

    evpl_iovec_alloc(evpl,
                     request->query_directory.max_output_length,
                     4096,
                     1,
                     &request->query_directory.iov);

    chimera_vfs_readdir(
        thread->vfs_thread,
        request->query_directory.open_file->handle,
        CHIMERA_VFS_ATTR_MASK_STAT,
        0, /* dir_attr_mask */
        request->query_directory.open_file->position,
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

    request->query_directory.output_length  = 0;
    request->query_directory.eof            = 1;
    request->query_directory.last_file_info = NULL;

    if (request->query_directory.pattern_length > SMB_FILENAME_MAX) {
        chimera_smb_error("Received SMB2 QUERY_DIRECTORY request with invalid name length (%u > %u)",
                          request->query_directory.pattern_length, SMB_FILENAME_MAX);
        return -1;
    }

    evpl_iovec_cursor_copy(request_cursor, request->query_directory.pattern, request->query_directory.pattern_length);

    request->query_directory.pattern_length >>= 1;

    return 0;
} /* chimera_smb_parse_query_directory */