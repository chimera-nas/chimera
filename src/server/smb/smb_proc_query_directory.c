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
    uint32_t retriable = chimera_smb_vfs_retriable_status(error_code);

    if (retriable) {
        return retriable;
    }

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
    uint32_t                         *fname_len_field;
    int                               name_utf16_len;
    struct evpl_iovec_cursor          entry_cursor;
    struct chimera_smb_attrs          smb_attrs;

    /* The search-pattern wildcard match is applied by the VFS core
     * (chimera_vfs_readdir match_pattern), so every entry that reaches here has
     * already matched -- no per-entry filtering needed. */

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

    namelen_padded += (8 - (namelen_padded & 7)) & 7;

    if (request->query_directory.flags & SMB2_INDEX_SPECIFIED) {
        if (file_index != request->query_directory.file_index) {
            /* Not the target entry -- skip it but keep iterating.
             * Return 0 because any non-zero return value means "stop" to the VFS
             * readdir layer, which would halt the scan prematurely. */
            return 0;
        }
        /* Found the matching entry.  Clear the flag so subsequent entries
         * are accepted normally, but skip this entry itself -- it was
         * already returned in the previous response. */
        request->query_directory.flags &= ~SMB2_INDEX_SPECIFIED;
        return 0;
    }

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
            fname_len_field = (uint32_t *) evpl_iovec_cursor_data(&entry_cursor);
            evpl_iovec_cursor_append_uint32(&entry_cursor, 0);

            namebuf = evpl_iovec_cursor_data(&entry_cursor);
            memset(namebuf, 0, namelen_padded);
            name_utf16_len = chimera_smb_utf8_to_utf16le(&thread->iconv_ctx,
                                                         name, namelen,
                                                         namebuf, namelen_padded);
            if (name_utf16_len < 0) {
                return 0;
            }
            *fname_len_field = (uint32_t) name_utf16_len;

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
            fname_len_field = (uint32_t *) evpl_iovec_cursor_data(&entry_cursor);
            evpl_iovec_cursor_append_uint32(&entry_cursor, 0);
            evpl_iovec_cursor_append_uint32(&entry_cursor, smb_attrs.smb_ea_size);
            evpl_iovec_cursor_zero(&entry_cursor, 26); /* short name */

            namebuf = evpl_iovec_cursor_data(&entry_cursor);
            memset(namebuf, 0, namelen_padded);
            name_utf16_len = chimera_smb_utf8_to_utf16le(&thread->iconv_ctx,
                                                         name, namelen,
                                                         namebuf, namelen_padded);
            if (name_utf16_len < 0) {
                return 0;
            }
            *fname_len_field = (uint32_t) name_utf16_len;

            evpl_iovec_cursor_skip(&entry_cursor, namelen_padded);

            break;
        case SMB2_FILE_NAMES_INFORMATION:
            /* FileIndex (0 = unspecified per MS-FSCC) + FileNameLength + name.
             * NextEntryOffset was already written at the top of the entry. */
            evpl_iovec_cursor_append_uint32(&entry_cursor, file_index);
            fname_len_field = (uint32_t *) evpl_iovec_cursor_data(&entry_cursor);
            evpl_iovec_cursor_append_uint32(&entry_cursor, 0);

            namebuf = evpl_iovec_cursor_data(&entry_cursor);
            memset(namebuf, 0, namelen_padded);
            name_utf16_len = chimera_smb_utf8_to_utf16le(&thread->iconv_ctx,
                                                         name, namelen,
                                                         namebuf, namelen_padded);
            if (name_utf16_len < 0) {
                return 0;
            }
            *fname_len_field = (uint32_t) name_utf16_len;

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
            fname_len_field = (uint32_t *) evpl_iovec_cursor_data(&entry_cursor);
            evpl_iovec_cursor_append_uint32(&entry_cursor, 0);

            evpl_iovec_cursor_append_uint32(&entry_cursor, smb_attrs.smb_ea_size);
            namebuf = evpl_iovec_cursor_data(&entry_cursor);
            memset(namebuf, 0, namelen_padded);
            name_utf16_len = chimera_smb_utf8_to_utf16le(&thread->iconv_ctx,
                                                         name, namelen,
                                                         namebuf, namelen_padded);
            if (name_utf16_len < 0) {
                return 0;
            }
            *fname_len_field = (uint32_t) name_utf16_len;

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
            fname_len_field = (uint32_t *) evpl_iovec_cursor_data(&entry_cursor);
            evpl_iovec_cursor_append_uint32(&entry_cursor, 0);
            evpl_iovec_cursor_append_uint32(&entry_cursor, smb_attrs.smb_ea_size);
            evpl_iovec_cursor_zero(&entry_cursor, 28); /* short name */
            evpl_iovec_cursor_append_uint64(&entry_cursor, attrs->va_ino);

            namebuf = evpl_iovec_cursor_data(&entry_cursor);
            memset(namebuf, 0, namelen_padded);
            name_utf16_len = chimera_smb_utf8_to_utf16le(&thread->iconv_ctx,
                                                         name, namelen,
                                                         namebuf, namelen_padded);
            if (name_utf16_len < 0) {
                return 0;
            }
            *fname_len_field = (uint32_t) name_utf16_len;

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
            fname_len_field = (uint32_t *) evpl_iovec_cursor_data(&entry_cursor);
            evpl_iovec_cursor_append_uint32(&entry_cursor, 0);
            evpl_iovec_cursor_append_uint32(&entry_cursor, smb_attrs.smb_ea_size);
            evpl_iovec_cursor_append_uint64(&entry_cursor, attrs->va_ino);

            namebuf = evpl_iovec_cursor_data(&entry_cursor);
            memset(namebuf, 0, namelen_padded);
            name_utf16_len = chimera_smb_utf8_to_utf16le(&thread->iconv_ctx,
                                                         name, namelen,
                                                         namebuf, namelen_padded);
            if (name_utf16_len < 0) {
                return 0;
            }
            *fname_len_field = (uint32_t) name_utf16_len;

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

    /* Validate the client-supplied information class up front.  The readdir
     * entry marshaller only understands the classes below; anything else
     * (e.g. smbtorture smb2.scan.find probing all 255 values) must be
     * answered with STATUS_INVALID_INFO_CLASS, not crash the server when the
     * first entry is emitted. */
    switch (request->query_directory.info_class) {
        case SMB2_FILE_DIRECTORY_INFORMATION:
        case SMB2_FILE_FULL_DIRECTORY_INFORMATION:
        case SMB2_FILE_BOTH_DIRECTORY_INFORMATION:
        case SMB2_FILE_NAMES_INFORMATION:
        case SMB2_FILE_ID_BOTH_DIRECTORY_INFORMATION:
        case SMB2_FILE_ID_FULL_DIRECTORY_INFORMATION:
            break;
        default:
            chimera_smb_complete_request(request, SMB2_STATUS_INVALID_INFO_CLASS);
            return;
    } /* switch */

    request->query_directory.open_file = chimera_smb_open_file_resolve(request, &request->query_directory.file_id);

    if (unlikely(!request->query_directory.open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    /* Named-pipe FIDs on IPC$ (srvsvc / lsarpc / samr / wkssvc) carry
     * open_file->handle == NULL -- see chimera_smb_create_gen_open_file_pipe.
     * The dup_handle below dereferences that handle unconditionally and would
     * crash the server on a QUERY_DIRECTORY against a pipe FID, the same crash
     * class already closed on QUERY_INFO and SET_INFO.
     *
     * A pipe is not a DirectoryFile, so MS-FSA 2.1.5.5.3 fails the enumeration
     * with STATUS_INVALID_PARAMETER.  Check before the position resets below so
     * a rejected query leaves the open's enumeration state untouched. */
    if (unlikely(request->query_directory.open_file->type == CHIMERA_SMB_OPEN_FILE_TYPE_PIPE)) {
        chimera_smb_open_file_release(request, request->query_directory.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    if (request->query_directory.flags & SMB2_RESTART_SCANS) {
        request->query_directory.open_file->position = 0;
    }

    if (request->query_directory.flags & SMB2_REOPEN) {
        request->query_directory.open_file->position = 0;
    }

    /* MS-SMB2 §3.3.5.18: when SMB2_INDEX_SPECIFIED is set, the client
     * supplies a FileIndex from a previous response and expects the server
     * to resume from the entry matching that index.  Reset position to the
     * beginning so the readdir callback can scan for the matching entry;
     * the callback itself (INDEX_SPECIFIED check) skips entries until the
     * file_index matches and then clears the flag. */
    if (request->query_directory.flags & SMB2_INDEX_SPECIFIED) {
        request->query_directory.open_file->position = 0;
    }

    /* UINT64_MAX marks a fully-enumerated directory.  SMB2 clients always issue
     * one trailing QUERY_DIRECTORY after the last real entry to receive
     * STATUS_NO_MORE_FILES; short-circuiting here avoids a redundant backend
     * round-trip.  MS-SMB2 §3.3.5.18 permits this early response. */
    if (request->query_directory.open_file->position == UINT64_MAX) {
        /* Release the resolve ref -- this early return never enters the
         * async readdir, so leaking it would pin the open_file's share
         * reservation and cause later deletes to fail with
         * SHARING_VIOLATION. */
        chimera_smb_open_file_release(request, request->query_directory.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_NO_MORE_FILES);
        return;
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

    /* MS-FSA 2.1.5.5.3: if OutputBufferLength is too small to hold even the
     * fixed header of one entry of the requested FileInformationClass, the
     * query is failed with STATUS_INFO_LENGTH_MISMATCH -- regardless of whether
     * the directory has matching entries (WPTS MS-FSAModel QueryDirectory
     * tiny-buffer cases).  The header sizes match the per-class expected_length
     * base in the readdir callback below. */
    uint32_t qd_header_len;
    switch (request->query_directory.info_class) {
        case SMB2_FILE_DIRECTORY_INFORMATION:         qd_header_len = 64;  break;
        case SMB2_FILE_FULL_DIRECTORY_INFORMATION:    qd_header_len = 68;  break;
        case SMB2_FILE_BOTH_DIRECTORY_INFORMATION:    qd_header_len = 94;  break;
        case SMB2_FILE_NAMES_INFORMATION:             qd_header_len = 12;  break;
        case SMB2_FILE_ID_BOTH_DIRECTORY_INFORMATION: qd_header_len = 102; break;
        case SMB2_FILE_ID_FULL_DIRECTORY_INFORMATION: qd_header_len = 74;  break;
        default:                                      qd_header_len = 0;   break;
    } /* switch */

    if (request->query_directory.max_output_length < qd_header_len) {
        chimera_smb_open_file_release(request, request->query_directory.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INFO_LENGTH_MISMATCH);
        return;
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

    uint64_t readdir_mask = CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME;

    /* Only the info classes that carry an EaSize field pay for the per-entry EA
     * enumeration; FILE_DIRECTORY/NAMES information do not. */
    switch (request->query_directory.info_class) {
        case SMB2_FILE_BOTH_DIRECTORY_INFORMATION:
        case SMB2_FILE_FULL_DIRECTORY_INFORMATION:
        case SMB2_FILE_ID_BOTH_DIRECTORY_INFORMATION:
        case SMB2_FILE_ID_FULL_DIRECTORY_INFORMATION:
            readdir_mask |= CHIMERA_VFS_ATTR_EA_SIZE;
            break;
        default:
            break;
    } /* switch */

    /* Access-based enumeration needs each entry's ACL to decide visibility. */
    if (request->tree && request->tree->share &&
        request->tree->share->access_based_enum) {
        readdir_mask |= CHIMERA_VFS_ATTR_ACL;
    }

    chimera_vfs_readdir(
        thread->vfs_thread,
        &request->session_handle->session->cred,
        chimera_smb_vfs_compound(request->compound),
        request->query_directory.open_file->handle,
        readdir_mask,
        0, /* dir_attr_mask */
        request->query_directory.open_file->position,
        0, /* verifier */
        CHIMERA_VFS_READDIR_EMIT_DOT,
        /* SMB search pattern: the VFS core applies the MS-FSA wildcard match so
         * the backend stays oblivious; the callback below no longer filters. */
        request->query_directory.pattern,
        request->query_directory.pattern_length,
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

    if (request->query_directory.pattern_length > SMB_FILENAME_MAX * 2) {
        chimera_smb_error("Received SMB2 QUERY_DIRECTORY request with invalid name length (%u > %u)",
                          request->query_directory.pattern_length, SMB_FILENAME_MAX * 2);
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