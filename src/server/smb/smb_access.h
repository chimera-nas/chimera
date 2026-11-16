// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdbool.h>
#include <stdint.h>

#include "smb_common/smb2.h"
#include "vfs/sdk/vfs_error.h"

/* The DesiredAccess bits that let an open write file data.  Only these need a
 * writable backend handle: the metadata-write rights (attributes, EAs, the
 * security descriptor, delete) are carried out by operations that do not need
 * a descriptor open for writing.  MS-FSA requires FILE_WRITE_DATA for the
 * SET_INFO classes that change data (2.1.5.15.1 "FileAllocationInformation",
 * 2.1.5.15.5 "FileEndOfFileInformation"), but SET_INFO does not check the
 * handle's granted access yet, so a size change can still arrive through a
 * read-only handle -- including a MAXIMUM_ALLOWED handle whose grant
 * chimera_smb_max_allowed_trim trimmed.  It takes effect there: linux and
 * io_uring fall back from ftruncate to a truncate by path
 * (/proc/self/fd/N), and the other backends do not look at the handle's
 * access mode.  MAXIMUM_ALLOWED is included because its grant can hold
 * FILE_WRITE_DATA. */
#define SMB2_DATA_WRITE_ACCESS (SMB2_FILE_WRITE_DATA | \
                                SMB2_FILE_APPEND_DATA | \
                                SMB2_GENERIC_WRITE | \
                                SMB2_GENERIC_ALL | \
                                SMB2_MAXIMUM_ALLOWED)

/* True when an open asking for `desired_access` can write file data, i.e. must
* be opened read-write on the backend; every other open is opened read-only. */
static inline bool
chimera_smb_access_writes_data(uint32_t desired_access)
{
    return (desired_access & SMB2_DATA_WRITE_ACCESS) != 0;
} /* chimera_smb_access_writes_data */

/* True when CREATE opens the backend read-only: the open can neither write
 * data nor replaces content (`replaces_content`: an overwriting disposition,
 * whose content replacement chimera_smb_create_issue_truncate makes through
 * this handle), or it is an open the backend refused read-write and CREATE
 * re-issued (`ro_retried`, see chimera_smb_may_reopen_read_only). */
static inline bool
chimera_smb_open_read_only(
    uint32_t desired_access,
    bool     replaces_content,
    bool     ro_retried)
{
    return ro_retried ||
           !(chimera_smb_access_writes_data(desired_access) || replaces_content);
} /* chimera_smb_open_read_only */

/* Trim a MAXIMUM_ALLOWED grant to what the open can actually do.
 *
 * A file marked FILE_ATTRIBUTE_READONLY is not granted FILE_WRITE_DATA,
 * FILE_APPEND_DATA, FILE_ADD_SUBDIRECTORY or FILE_DELETE_CHILD (MS-FSA
 * 2.1.5.1.2.1 "Algorithm to Check Access to an Existing File");
 * `readonly_attr` is set by the caller only where that check applies.  A file
 * whose backend handle is read-only (a MAXIMUM_ALLOWED open the backend refused
 * read-write, re-issued read-only) cannot write data, so it is not granted
 * FILE_WRITE_DATA or FILE_APPEND_DATA.
 *
 * A directory keeps its grant in both cases.  There FILE_WRITE_DATA and
 * FILE_APPEND_DATA are FILE_ADD_FILE and FILE_ADD_SUBDIRECTORY, which act
 * through the parent rather than through this handle.  For READONLY this
 * deviates from MS-FSA on purpose: the algorithm's trim is not limited to data
 * files, but Windows ignores READONLY on a directory (smbtorture
 * smb2.maximum_allowed.read_only_dir, in Samba 4.23.9, 4.24.2 and later;
 * earlier smbtorture has only smb2.maximum_allowed.read_only, which checks
 * only that the open succeeds). */
static inline uint32_t
chimera_smb_max_allowed_trim(
    uint32_t granted,
    bool     is_directory,
    bool     read_only_handle,
    bool     readonly_attr)
{
    if (is_directory) {
        return granted;
    }

    if (read_only_handle) {
        granted &= ~(SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA);
    }

    if (readonly_attr) {
        granted &= ~(SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA |
                     SMB2_FILE_ADD_SUBDIRECTORY | SMB2_FILE_DELETE_CHILD);
    }

    return granted;
} /* chimera_smb_max_allowed_trim */

/* True for the dispositions that replace an existing file's contents
 * (OVERWRITE / OVERWRITE_IF / SUPERSEDE). */
static inline bool
chimera_smb_disposition_overwrites(uint32_t disposition)
{
    return disposition == SMB2_FILE_OVERWRITE ||
           disposition == SMB2_FILE_OVERWRITE_IF ||
           disposition == SMB2_FILE_SUPERSEDE;
} /* chimera_smb_disposition_overwrites */

/* True for a CREATE that goes down the mkdir path (FILE_DIRECTORY_FILE with
 * FILE_CREATE or FILE_OPEN_IF) instead of an open_at.  Its open of an
 * already-existing directory (OPEN_IF) is its own read-only open, not one
 * chimera_smb_create_issue_open built. */
static inline bool
chimera_smb_create_is_mkdir(
    uint32_t create_options,
    uint32_t disposition)
{
    return (create_options & SMB2_FILE_DIRECTORY_FILE) &&
           (disposition == SMB2_FILE_CREATE ||
            disposition == SMB2_FILE_OPEN_IF);
} /* chimera_smb_create_is_mkdir */

/* An overwriting open, with neither FILE_DIRECTORY_FILE nor
 * FILE_NON_DIRECTORY_FILE, that the backend refused EISDIR met an existing
 * directory: linux and io_uring refuse the read-write open of a directory
 * that an explicit data-write right asks for, which is not re-issued.  An
 * existing directory cannot be overwritten or superseded, so CREATE answers
 * OBJECT_NAME_COLLISION, as it does once such an open succeeds (MS-FSA
 * 2.1.5.1.2).  FILE_NON_DIRECTORY_FILE keeps FILE_IS_A_DIRECTORY, and
 * FILE_DIRECTORY_FILE with an overwriting disposition never reaches the
 * backend. */
static inline bool
chimera_smb_overwrite_met_directory(
    uint32_t               create_options,
    uint32_t               disposition,
    bool                   has_stream,
    enum chimera_vfs_error error_code)
{
    return error_code == CHIMERA_VFS_EISDIR && !has_stream &&
           chimera_smb_disposition_overwrites(disposition) &&
           !(create_options & (SMB2_FILE_DIRECTORY_FILE |
                               SMB2_FILE_NON_DIRECTORY_FILE));
} /* chimera_smb_overwrite_met_directory */

/* Whether CREATE re-issues a refused open read-only.
 *
 * A MAXIMUM_ALLOWED open is opened read-write (its grant can hold
 * FILE_WRITE_DATA), but MAXIMUM_ALLOWED never fails for want of a right: it is
 * granted what the object allows.  So when the backend refuses the read-write
 * open, CREATE re-issues it once read-only, and a file's grant loses the
 * data-write rights (chimera_smb_max_allowed_trim; a directory keeps its
 * grant).  The refusals that a read-only open can avoid: access denied, a
 * read-only file system, and EISDIR -- linux and io_uring open a read-write
 * open of a directory O_RDWR unless FILE_DIRECTORY_FILE was given, and the
 * kernel refuses that.  So on those backends a MAXIMUM_ALLOWED open of a
 * directory without FILE_DIRECTORY_FILE costs two backend opens, the refused
 * read-write one and the read-only one; the open does not know the object's
 * type before it is made.
 *
 * An overwriting disposition is opened read-write for its content replacement
 * (chimera_smb_open_read_only).  When that is the only reason, EISDIR is
 * re-issued the same way, so that CREATE sees the directory and refuses the
 * overwrite STATUS_OBJECT_NAME_COLLISION, as on the other backends.
 *
 * Not re-issued: an open that also asks for an explicit data-write right
 * (it needs one); FILE_CREATE, which is exclusive, so a refusal comes from the
 * parent; an overwriting disposition refused for any reason but EISDIR, since
 * the caller was refused write access to the content it asked to replace; and
 * the mkdir path's open of an existing directory (chimera_smb_create_is_mkdir),
 * which is already read-only and shares the open completion without being an
 * open chimera_smb_create_issue_open built.  The base-file open of a
 * named-stream CREATE is re-issued whatever the disposition: the disposition
 * applies to the stream, and the base open never collides or truncates.  A
 * refused stream open is not re-issued (chimera_smb_create_open_stream_callback
 * completes with the error). */
static inline bool
chimera_smb_may_reopen_read_only(
    uint32_t               desired_access,
    uint32_t               create_options,
    uint32_t               disposition,
    bool                   has_stream,
    bool                   already_retried,
    enum chimera_vfs_error error_code)
{
    bool max_allowed = (desired_access & SMB2_MAXIMUM_ALLOWED) != 0;
    bool replaces    = !has_stream &&
        chimera_smb_disposition_overwrites(disposition);

    /* Only an open that went to the backend read-write for MAXIMUM_ALLOWED or
     * for its content replacement, with no explicit data-write right, and only
     * once. */
    if (already_retried ||
        chimera_smb_create_is_mkdir(create_options, disposition) ||
        (desired_access & SMB2_DATA_WRITE_ACCESS & ~SMB2_MAXIMUM_ALLOWED) ||
        !(max_allowed || replaces)) {
        return false;
    }

    switch (error_code) {
        case CHIMERA_VFS_EISDIR:
            /* A directory: any non-exclusive open, which CREATE then
             * refuses if it overwrites (OBJECT_NAME_COLLISION). */
            return has_stream || disposition != SMB2_FILE_CREATE;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:
        case CHIMERA_VFS_EROFS:
            return max_allowed &&
                   (has_stream ||
                    (disposition != SMB2_FILE_CREATE &&
                     !chimera_smb_disposition_overwrites(disposition)));
        default:
            return false;
    } /* switch */
} /* chimera_smb_may_reopen_read_only */
