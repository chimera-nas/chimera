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
 * security descriptor, delete) are carried out through operations that do not
 * write through the handle, and the SET_INFO classes that change data
 * (FileEndOfFileInformation, FileAllocationInformation) require
 * FILE_WRITE_DATA (MS-FSA 2.1.5.15.1 "FileAllocationInformation", 2.1.5.15.5
 * "FileEndOfFileInformation").  MAXIMUM_ALLOWED is included because its grant
 * can hold FILE_WRITE_DATA. */
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

/* Trim a MAXIMUM_ALLOWED grant to what the open can actually do.
 *
 * A file whose backend handle is read-only (a MAXIMUM_ALLOWED open the backend
 * refused read-write, re-issued read-only) cannot write data, so it is not
 * granted FILE_WRITE_DATA or FILE_APPEND_DATA.  A directory keeps its grant:
 * there those bits are FILE_ADD_FILE and FILE_ADD_SUBDIRECTORY, which act
 * through the parent rather than through this handle, and Windows grants
 * them. */
static inline uint32_t
chimera_smb_max_allowed_trim(
    uint32_t granted,
    bool     is_directory,
    bool     read_only_handle)
{
    if (is_directory) {
        return granted;
    }

    if (read_only_handle) {
        granted &= ~(SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA);
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

/* Whether CREATE re-issues a refused open read-only.
 *
 * A MAXIMUM_ALLOWED open is opened read-write (its grant can hold
 * FILE_WRITE_DATA), but MAXIMUM_ALLOWED never fails for want of a right: it is
 * granted what the object allows.  So when the backend refuses the read-write
 * open, CREATE re-issues it once read-only, and the grant loses the data-write
 * rights (chimera_smb_max_allowed_trim).  The refusals that a read-only open
 * can avoid: access denied, a read-only file system, and EISDIR -- linux and
 * io_uring open a read-write open of a directory O_RDWR unless
 * FILE_DIRECTORY_FILE was given, and the kernel refuses that.
 *
 * Not re-issued: an open that also asks for an explicit data-write right
 * (it needs one); FILE_CREATE, which is exclusive, so a refusal comes from the
 * parent; an overwriting disposition refused for any reason but EISDIR, since
 * its content replacement needs a writable handle (a directory is never
 * truncated); and the mkdir path's open of an existing directory
 * (chimera_smb_create_is_mkdir), which is already read-only and shares the
 * open completion without being an open chimera_smb_create_issue_open built.
 * A named-stream open is re-issued whatever its disposition: the disposition
 * applies to the stream, and the base open never collides or truncates. */
static inline bool
chimera_smb_may_reopen_read_only(
    uint32_t               desired_access,
    uint32_t               create_options,
    uint32_t               disposition,
    bool                   has_stream,
    bool                   already_retried,
    enum chimera_vfs_error error_code)
{
    if (already_retried ||
        chimera_smb_create_is_mkdir(create_options, disposition) ||
        !(desired_access & SMB2_MAXIMUM_ALLOWED) ||
        (desired_access & (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA |
                           SMB2_GENERIC_WRITE | SMB2_GENERIC_ALL))) {
        return false;
    }

    switch (error_code) {
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:
        case CHIMERA_VFS_EROFS:
        case CHIMERA_VFS_EISDIR:
            break;
        default:
            return false;
    } /* switch */

    if (has_stream) {
        return true;
    }

    if (disposition == SMB2_FILE_CREATE) {
        return false;
    }

    return !chimera_smb_disposition_overwrites(disposition) ||
           error_code == CHIMERA_VFS_EISDIR;
} /* chimera_smb_may_reopen_read_only */
