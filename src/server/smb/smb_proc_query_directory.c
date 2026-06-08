// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_string.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "xxhash.h"

static unsigned int
chimera_smb_query_directory_status(enum chimera_vfs_error error_code)
{
    switch (error_code) {
        case CHIMERA_VFS_OK:      return SMB2_STATUS_SUCCESS;
        /* QUERY_DIRECTORY against a non-directory open: per MS-SMB2 3.3.5.18
         * the server fails the request with STATUS_INVALID_PARAMETER. */
        case CHIMERA_VFS_ENOTDIR: return SMB2_STATUS_INVALID_PARAMETER;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:   return SMB2_STATUS_ACCESS_DENIED;
        default:                  return SMB2_STATUS_INTERNAL_ERROR;
    } /* switch */
} /* chimera_smb_query_directory_status */

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

    /* Drop the extra handle reference taken in chimera_smb_query_directory.  Do
     * this before releasing the open_file: once the readdir is done with the
     * handle it is safe to let a racing CLOSE finish tearing it down. */
    if (handle && handle->cache_id != CHIMERA_VFS_OPEN_ID_SYNTHETIC) {
        chimera_vfs_release(request->compound->thread->vfs_thread, handle);
    }

    if (request->query_directory.last_file_offset) {
        *request->query_directory.last_file_offset = 0;
    }

    chimera_smb_open_file_release(request, request->query_directory.open_file);

    if (error_code != CHIMERA_VFS_OK) {
        /* Must return here: falling through would complete the request a
         * second time, overrunning the compound's completion counter and
         * tripping the compound_advance abort (smb.c). */
        evpl_iovec_release(request->compound->thread->evpl, &request->query_directory.iov);
        chimera_smb_complete_request(request, chimera_smb_query_directory_status(error_code));
        return;
    }

    if (request->query_directory.output_length) {
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
    } else {
        evpl_iovec_release(request->compound->thread->evpl, &request->query_directory.iov);
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


    /* Access-based directory enumeration: hide an entry the caller cannot read.
     * Windows requires the DACL itself to grant the read rights (DATA + EA +
     * ATTRIBUTES), so evaluate the raw ACL -- not the engine's implicit grants.
     * "." and ".." (the directory the caller already opened) are never hidden. */
    if (request->tree && request->tree->share &&
        request->tree->share->access_based_enum &&
        !(namelen == 1 && name[0] == '.') &&
        !(namelen == 2 && name[0] == '.' && name[1] == '.')) {
        uint32_t want = CHIMERA_ACE_READ_DATA | CHIMERA_ACE_READ_NAMED_ATTRS |
            CHIMERA_ACE_READ_ATTRIBUTES;
        uint32_t got = chimera_acl_access_raw(
            (attrs->va_set_mask & CHIMERA_VFS_ATTR_ACL) ? attrs->va_acl : NULL,
            attrs->va_uid, attrs->va_gid,
            &request->session_handle->session->cred, want);

        if ((got & want) != want) {
            return 0;
        }
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
            /* NextEntryOffset(4) + FileIndex(4) + 6x time/size(48) +
             * FileAttributes(4) + FileNameLength(4) = 64, then the name. */
            expected_length = 64 + namelen_padded;
            break;
        case SMB2_FILE_BOTH_DIRECTORY_INFORMATION:
            expected_length = 94 + namelen_padded;
            break;
        case SMB2_FILE_NAMES_INFORMATION:
            /* MS-FSCC 2.4.27 FileNamesInformation fixed header:
             * NextEntryOffset(4) + FileIndex(4) + FileNameLength(4) = 12. */
            expected_length = 12 + namelen_padded;
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
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_crttime);
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
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_crttime);
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
            /* FileIndex (0 = unspecified per MS-FSCC) + FileNameLength + name.
             * NextEntryOffset was already written at the top of the entry. */
            evpl_iovec_cursor_append_uint32(&entry_cursor, file_index);
            evpl_iovec_cursor_append_uint32(&entry_cursor, namelen * 2);

            namebuf = evpl_iovec_cursor_data(&entry_cursor);
            chimera_smb_utf8_to_utf16le(&thread->iconv_ctx,
                                        name, namelen,
                                        namebuf, SMB_FILENAME_MAX);

            evpl_iovec_cursor_skip(&entry_cursor, namelen_padded);
            break;
        case SMB2_FILE_FULL_DIRECTORY_INFORMATION:
            evpl_iovec_cursor_append_uint32(&entry_cursor, file_index);
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_crttime);
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
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_crttime);
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
            evpl_iovec_cursor_append_uint64(&entry_cursor, smb_attrs.smb_crttime);
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
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    if (request->query_directory.flags & SMB2_RESTART_SCANS) {
        request->query_directory.open_file->position = 0;
    }

    if (request->query_directory.flags & SMB2_REOPEN) {
        request->query_directory.open_file->position = 0;
    }

    /* The reply buffer is allocated below as a single contiguous iovec
     * (max_iovecs == 1).  A client may advertise an OutputBufferLength far
     * larger than the negotiated MaxTransactSize (smbtorture's
     * compound_find_close uses 8 MiB), which cannot be satisfied by one
     * libevpl buffer and would otherwise spin evpl_iovec_alloc() forever.
     * Cap the buffer at MaxTransactSize; the readdir callback honours the
     * same bound, and a client that wants more simply re-issues the FIND
     * for the next batch (MS-SMB2 §3.3.5.18 allows returning fewer bytes
     * than OutputBufferLength). */
    if (request->query_directory.max_output_length > CHIMERA_SMB_MAX_TRANSACT_SIZE) {
        request->query_directory.max_output_length = CHIMERA_SMB_MAX_TRANSACT_SIZE;
    }

    evpl_iovec_alloc(evpl,
                     request->query_directory.max_output_length,
                     4096,
                     1,
                     0, &request->query_directory.iov);

    /* Hold an extra reference on the directory's VFS handle for the duration of
     * the readdir.  The readdir is async (e.g. diskfs walks the directory's
     * b+tree across block I/O), so a separate, pipelined CLOSE for the same
     * file id can run before it completes.  Without this reference that CLOSE
     * drops the handle's last open count and -- if the handle was detached (a
     * second open of the same fh, which the file-creation path does to the
     * parent directory) -- closes the backend handle immediately, tearing down
     * the directory inode while the readdir is still iterating it.  The matching
     * release is in chimera_smb_query_directory_readdir_complete. */
    if (request->query_directory.open_file->handle->cache_id != CHIMERA_VFS_OPEN_ID_SYNTHETIC) {
        chimera_vfs_dup_handle(thread->vfs_thread, request->query_directory.open_file->handle);
    }

    chimera_vfs_readdir(
        thread->vfs_thread,
        &request->session_handle->session->cred,
        request->query_directory.open_file->handle,
        /* Access-based enumeration needs each entry's ACL to decide
         * visibility. */
        (request->tree && request->tree->share &&
         request->tree->share->access_based_enum) ?
        (CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME | CHIMERA_VFS_ATTR_ACL) :
        (CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME),
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
    int      name_size;

    if (unlikely(request->request_struct_size != SMB2_QUERY_DIRECTORY_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 QUERY_DIRECTORY request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_QUERY_DIRECTORY_REQUEST_SIZE);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    int prc = 0;
    prc |= evpl_iovec_cursor_try_get_uint8(request_cursor, &request->query_directory.info_class);
    prc |= evpl_iovec_cursor_try_get_uint8(request_cursor, &request->query_directory.flags);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->query_directory.file_index);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->query_directory.file_id.pid);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->query_directory.file_id.vid);
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &name_offset);
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &request->query_directory.pattern_length);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->query_directory.max_output_length);

    if (unlikely(prc)) {
        chimera_smb_error("Received SMB2 QUERY_DIRECTORY request truncated in fixed body");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

    request->query_directory.output_length    = 0;
    request->query_directory.eof              = 1;
    request->query_directory.last_file_offset = NULL;

    if (request->query_directory.pattern_length > SMB_FILENAME_MAX) {
        chimera_smb_error("Received SMB2 QUERY_DIRECTORY request with invalid name length (%u > %u)",
                          request->query_directory.pattern_length, SMB_FILENAME_MAX);
        request->status = SMB2_STATUS_NAME_TOO_LONG;
        return -1;
    }

    /* Honor the client-declared FileName offset (previously read but ignored)
     * and pull the pattern with the bounds-checked reader. */
    if (request->query_directory.pattern_length > 0) {
        if (unlikely(smb_cursor_seek_to(request_cursor, name_offset) != 0 ||
                     evpl_iovec_cursor_try_copy(request_cursor, pattern16,
                                                request->query_directory.pattern_length) != 0)) {
            chimera_smb_error("Received SMB2 QUERY_DIRECTORY with search pattern out of range");
            return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
        }
    }
    name_size = chimera_smb_utf16le_to_utf8(&request->compound->thread->iconv_ctx,
                                            pattern16,
                                            request->query_directory.pattern_length,
                                            request->query_directory.pattern,
                                            sizeof(request->query_directory.pattern));
    if (name_size < 0) {
        chimera_smb_error("Failed to convert QUERY_DIRECTORY pattern from UTF-16LE to UTF-8");
        request->status = SMB2_STATUS_OBJECT_NAME_INVALID;
        return -1;
    }
    request->query_directory.pattern_length = name_size;

    return 0;
} /* chimera_smb_parse_query_directory */