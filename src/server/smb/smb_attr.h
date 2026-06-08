// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/misc.h"
#include "vfs/vfs.h"
#include <sys/stat.h>
#include "vfs/vfs_internal.h"
#include "common/evpl_iovec_cursor.h"

#include "smb_session.h"

/* Bitmask for tracking which attributes are populated */
#define SMB_ATTR_SIZE               (1ULL << 0)
#define SMB_ATTR_ALLOC_SIZE         (1ULL << 1)
#define SMB_ATTR_ATIME              (1ULL << 2)
#define SMB_ATTR_MTIME              (1ULL << 3)
#define SMB_ATTR_CTIME              (1ULL << 4)
#define SMB_ATTR_CRTTIME            (1ULL << 5)
#define SMB_ATTR_ATTRIBUTES         (1ULL << 6)
#define SMB_ATTR_INODE              (1ULL << 7)
#define SMB_ATTR_EA_SIZE            (1ULL << 8)
#define SMB_ATTR_LINK_COUNT         (1ULL << 9)
#define SMB_ATTR_COMPRESSION        (1ULL << 10)
#define SMB_ATTR_ACCESS_FLAGS       (1ULL << 11)
#define SMB_ATTR_REPARSE_TAG        (1ULL << 12)
#define SMB_ATTR_DISPOSITION        (1ULL << 13)
#define SMB_ATTR_POSITION           (1ULL << 14)

/* DOS attribute bits a client may set/clear via SET_INFO FileBasicInformation
 * and that the VFS persists.  DIRECTORY/REPARSE/etc. are derived from the
 * inode type, not stored, so they are excluded here. */
#define SMB_DOS_ATTR_SETTABLE       (SMB2_FILE_ATTRIBUTE_READONLY | \
                                     SMB2_FILE_ATTRIBUTE_HIDDEN |   \
                                     SMB2_FILE_ATTRIBUTE_SYSTEM |   \
                                     SMB2_FILE_ATTRIBUTE_ARCHIVE)

/* Persisted DOS bits reported back to clients.  SPARSE is reportable but is
 * excluded from SMB_DOS_ATTR_SETTABLE because it is managed via
 * FSCTL_SET_SPARSE, not FileBasicInformation. */
#define SMB_DOS_ATTR_REPORTABLE     (SMB_DOS_ATTR_SETTABLE | \
                                     SMB2_FILE_ATTRIBUTE_SPARSE_FILE)

/* Masks for each information class */
#define SMB_ATTR_MASK_BASIC         ( \
            SMB_ATTR_CRTTIME | SMB_ATTR_ATIME | SMB_ATTR_MTIME | \
            SMB_ATTR_CTIME | SMB_ATTR_ATTRIBUTES)

#define SMB_ATTR_MASK_STANDARD      ( \
            SMB_ATTR_ALLOC_SIZE | SMB_ATTR_SIZE | SMB_ATTR_LINK_COUNT)

#define SMB_ATTR_MASK_INTERNAL      (SMB_ATTR_INODE)

#define SMB_ATTR_MASK_EA            (SMB_ATTR_EA_SIZE)

#define SMB_ATTR_MASK_COMPRESSION   (SMB_ATTR_SIZE | SMB_ATTR_COMPRESSION)

#define SMB_ATTR_MASK_ATTRIBUTE_TAG (SMB_ATTR_ATTRIBUTES | SMB_ATTR_REPARSE_TAG)

#define SMB_ATTR_MASK_ACCESS        (SMB_ATTR_ACCESS_FLAGS)

#define SMB_ATTR_MASK_NETWORK_OPEN  (SMB_ATTR_MASK_BASIC | SMB_ATTR_ALLOC_SIZE | SMB_ATTR_SIZE)

struct chimera_smb_attrs {
    /* FileBasicInformation fields */
    uint64_t smb_crttime;      /* Creation time */
    uint64_t smb_atime;        /* Last access time */
    uint64_t smb_mtime;        /* Last write time */
    uint64_t smb_ctime;        /* Last change time */
    uint32_t smb_attributes;   /* File attributes */
    uint32_t smb_reparse_tag;  /* Reparse point tag */

    /* FileStandardInformation fields */
    uint64_t smb_alloc_size;   /* Allocation size */
    uint64_t smb_size;         /* End of file */
    uint32_t smb_link_count;   /* Number of links */

    /* FileInternalInformation fields */
    uint64_t smb_ino;      /* Inode ID */

    /* FileEaInformation fields */
    uint32_t smb_ea_size;      /* Extended attributes size */

    /* FileAccessInformation fields */
    uint32_t smb_access_flags; /* Access rights */

    /* FileCompressionInformation fields */
    uint16_t smb_compression_format; /* Compression format */
    uint32_t smb_compression_unit_size; /* Compression unit size */

    uint8_t  smb_disposition; /* Disposition */

    /* FilePositionInformation fields */
    uint64_t smb_position;     /* Current byte offset */

    /* Bitmap of populated attributes */
    uint64_t smb_attr_mask;
};

struct chimera_smb_fs_attrs {
    uint64_t smb_total_allocation_units;
    uint64_t smb_caller_available_allocation_units;
    uint64_t smb_actual_available_allocation_units;
    uint32_t smb_sectors_per_allocation_unit;
    uint32_t smb_bytes_per_sector;
};

/* Helper functions for common attribute marshaling operations */
static inline void
chimera_smb_marshal_basic_attrs(
    const struct chimera_vfs_attrs *attr,
    struct chimera_smb_attrs       *smb_attr)
{
    /* Time attributes.  Creation/birth time is optional in the VFS (no POSIX
     * equivalent); use it when the backend tracked it, else report 0. */
    smb_attr->smb_crttime = (attr->va_set_mask & CHIMERA_VFS_ATTR_BTIME)
                               ? chimera_nt_time(&attr->va_btime) : 0;
    smb_attr->smb_attr_mask |= SMB_ATTR_CRTTIME;

    smb_attr->smb_atime      = chimera_nt_time(&attr->va_atime);
    smb_attr->smb_attr_mask |= SMB_ATTR_ATIME;

    smb_attr->smb_mtime      = chimera_nt_time(&attr->va_mtime);
    smb_attr->smb_attr_mask |= SMB_ATTR_MTIME;

    smb_attr->smb_ctime      = chimera_nt_time(&attr->va_ctime);
    smb_attr->smb_attr_mask |= SMB_ATTR_CTIME;

    /* File attributes */
    smb_attr->smb_attributes = 0;

    /* DOS attribute bits persisted by the VFS (READONLY, HIDDEN, SYSTEM,
     * ARCHIVE, SPARSE).  Only honored when the backend reports it actually
     * stores them; otherwise we fall back to synthesizing from the mode. */
    if (attr->va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) {
        smb_attr->smb_attributes |= attr->va_dos_attributes & SMB_DOS_ATTR_REPORTABLE;
    }

    if ((attr->va_mode & S_IFMT) == S_IFDIR) {
        smb_attr->smb_attributes |= 0x10; /* FILE_ATTRIBUTE_DIRECTORY */
    }

    switch (attr->va_mode & S_IFMT) {
        case S_IFLNK:
        case S_IFCHR:
        case S_IFBLK:
        case S_IFIFO:
        case S_IFSOCK:
            smb_attr->smb_attributes |= SMB2_FILE_ATTRIBUTE_REPARSE_POINT;
            smb_attr->smb_reparse_tag = SMB2_IO_REPARSE_TAG_NFS;
            smb_attr->smb_attr_mask  |= SMB_ATTR_REPARSE_TAG;
            break;
    } // switch

    /* SMB never reports an all-zero attribute word for a regular file.  How the
     * empty set is filled depends on whether the backend persists DOS bits:
     *
     *  - Backend persists them (DOS_ATTRIBUTES in set_mask): report exactly what
     *    is stored.  An empty set is FILE_ATTRIBUTE_NORMAL (0x80) -- a file
     *    explicitly cleared to NORMAL must read back NORMAL, not a synthesized
     *    ARCHIVE (smb2.winattr).  ARCHIVE is stamped and persisted at
     *    create/modify time instead (see chimera_smb_create_issue_open).
     *
     *  - Backend does not persist them (passthrough linux/io_uring): synthesize
     *    ARCHIVE for a plain file, matching Windows' default for a new file,
     *    since there is no stored value to honor. */
    if ((attr->va_mode & S_IFMT) == S_IFREG) {
        if (attr->va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) {
            if (smb_attr->smb_attributes == 0) {
                smb_attr->smb_attributes |= SMB2_FILE_ATTRIBUTE_NORMAL;
            }
        } else if ((smb_attr->smb_attributes & SMB_DOS_ATTR_SETTABLE) == 0) {
            smb_attr->smb_attributes |= SMB2_FILE_ATTRIBUTE_ARCHIVE;
        }
    }
    smb_attr->smb_attr_mask |= SMB_ATTR_ATTRIBUTES;
} /* chimera_smb_marshal_basic_attrs */

static inline void
chimera_smb_marshal_standard_attrs(
    const struct chimera_vfs_attrs *attr,
    struct chimera_smb_attrs       *smb_attr)
{
    /* File size */
    smb_attr->smb_alloc_size = attr->va_space_used;
    smb_attr->smb_attr_mask |= SMB_ATTR_ALLOC_SIZE;

    smb_attr->smb_size       = attr->va_size;
    smb_attr->smb_attr_mask |= SMB_ATTR_SIZE;

    /* Number of links */
    smb_attr->smb_link_count = attr->va_nlink;
    smb_attr->smb_attr_mask |= SMB_ATTR_LINK_COUNT;

} /* chimera_smb_marshal_standard_attrs */

static inline void
chimera_smb_marshal_internal_attrs(
    const struct chimera_vfs_attrs *attr,
    struct chimera_smb_attrs       *smb_attr)
{
    /* File ID */
    smb_attr->smb_ino        = attr->va_ino;
    smb_attr->smb_attr_mask |= SMB_ATTR_INODE;
} /* chimera_smb_marshal_internal_attrs */

static inline void
chimera_smb_marshal_ea_attrs(
    const struct chimera_vfs_attrs *attr,
    struct chimera_smb_attrs       *smb_attr)
{
    /* EA size (not tracked in VFS) */
    smb_attr->smb_ea_size    = 0;
    smb_attr->smb_attr_mask |= SMB_ATTR_EA_SIZE;
} /* chimera_smb_marshal_ea_attrs */

static inline void
chimera_smb_marshal_compression_attrs(
    const struct chimera_vfs_attrs *attr,
    struct chimera_smb_attrs       *smb_attr)
{
    /* File size for the compression info */
    smb_attr->smb_size       = attr->va_size;
    smb_attr->smb_attr_mask |= SMB_ATTR_SIZE;

    /* Compression (not tracked in VFS) */
    smb_attr->smb_compression_format    = 0; /* COMPRESSION_FORMAT_NONE */
    smb_attr->smb_compression_unit_size = 0;
    smb_attr->smb_attr_mask            |= SMB_ATTR_COMPRESSION;
} /* chimera_smb_marshal_compression_attrs */

static inline void
chimera_smb_marshal_access_attrs(
    const struct chimera_vfs_attrs *attr,
    struct chimera_smb_attrs       *smb_attr)
{
    /* Access flags (not tracked in VFS) */
    smb_attr->smb_access_flags = 0;
    smb_attr->smb_attr_mask   |= SMB_ATTR_ACCESS_FLAGS;
} /* chimera_smb_marshal_access_attrs */

/* Main marshal functions for each information class */

/* Marshal for FileAllInformation (0x12) - the complete set */
static inline void
chimera_smb_marshal_attrs(
    const struct chimera_vfs_attrs *attr,
    struct chimera_smb_attrs       *smb_attr)
{
    /* Reset the attribute mask */
    smb_attr->smb_attr_mask = 0;

    /* Marshal all attributes by calling each helper function */
    chimera_smb_marshal_basic_attrs(attr, smb_attr);
    chimera_smb_marshal_standard_attrs(attr, smb_attr);
    chimera_smb_marshal_internal_attrs(attr, smb_attr);
    chimera_smb_marshal_ea_attrs(attr, smb_attr);
    chimera_smb_marshal_compression_attrs(attr, smb_attr);
    chimera_smb_marshal_access_attrs(attr, smb_attr);
} /* chimera_smb_marshal_attrs */

/* Marshal for FileBasicInformation (0x04) */
static inline void
chimera_smb_marshal_basic_info(
    const struct chimera_vfs_attrs *attr,
    struct chimera_smb_attrs       *smb_attr)
{
    /* Reset the attribute mask */
    smb_attr->smb_attr_mask = 0;

    /* Only include basic attributes */
    chimera_smb_marshal_basic_attrs(attr, smb_attr);
} /* chimera_smb_marshal_basic_info */

/* Marshal for FileStandardInformation (0x05) */
static inline void
chimera_smb_marshal_standard_info(
    const struct chimera_vfs_attrs *attr,
    struct chimera_smb_attrs       *smb_attr)
{
    /* Reset the attribute mask */
    smb_attr->smb_attr_mask = 0;

    /* Only include standard attributes */
    chimera_smb_marshal_standard_attrs(attr, smb_attr);
} /* chimera_smb_marshal_standard_info */

/* Marshal for FileInternalInformation (0x06) */
static inline void
chimera_smb_marshal_internal_info(
    const struct chimera_vfs_attrs *attr,
    struct chimera_smb_attrs       *smb_attr)
{
    /* Reset the attribute mask */
    smb_attr->smb_attr_mask = 0;

    /* Only include internal attributes */
    chimera_smb_marshal_internal_attrs(attr, smb_attr);
} /* chimera_smb_marshal_internal_info */

/* Marshal for FileEaInformation (0x07) */
static inline void
chimera_smb_marshal_ea_info(
    const struct chimera_vfs_attrs *attr,
    struct chimera_smb_attrs       *smb_attr)
{
    /* Reset the attribute mask */
    smb_attr->smb_attr_mask = 0;

    /* Only include EA attributes */
    chimera_smb_marshal_ea_attrs(attr, smb_attr);
} /* chimera_smb_marshal_ea_info */

/* Marshal for FileCompressionInformation (0x0C) */
static inline void
chimera_smb_marshal_compression_info(
    const struct chimera_vfs_attrs *attr,
    struct chimera_smb_attrs       *smb_attr)
{
    /* Reset the attribute mask */
    smb_attr->smb_attr_mask = 0;

    /* Only include compression attributes */
    chimera_smb_marshal_compression_attrs(attr, smb_attr);
} /* chimera_smb_marshal_compression_info */

/* Marshal for FileAttributeTagInformation (0x23) */
static inline void
chimera_smb_marshal_attribute_tag_info(
    const struct chimera_vfs_attrs *attr,
    struct chimera_smb_attrs       *smb_attr)
{
    /* Reset the attribute mask */
    smb_attr->smb_attr_mask = 0;

    chimera_smb_marshal_basic_attrs(attr, smb_attr);

    /* Clear the timestamp masks since we're not including them */
    smb_attr->smb_attr_mask &= ~(SMB_ATTR_CRTTIME | SMB_ATTR_ATIME |
                                 SMB_ATTR_MTIME | SMB_ATTR_CTIME);
} /* chimera_smb_marshal_attribute_tag_info */

/* Marshal for FileNetworkOpenInformation (0x22) */
static inline void
chimera_smb_marshal_network_open_info(
    const struct chimera_vfs_attrs *attr,
    struct chimera_smb_attrs       *smb_attr)
{
    /* Reset the attribute mask */
    smb_attr->smb_attr_mask = 0;

    chimera_smb_marshal_basic_attrs(attr, smb_attr);

    smb_attr->smb_alloc_size = attr->va_space_used;
    smb_attr->smb_attr_mask |= SMB_ATTR_ALLOC_SIZE;

    smb_attr->smb_size       = attr->va_size;
    smb_attr->smb_attr_mask |= SMB_ATTR_SIZE;
} /* chimera_smb_marshal_network_open_info */

static inline void
chimera_smb_marshal_fs_full_size_info(
    const struct chimera_vfs_attrs *attr,
    struct chimera_smb_fs_attrs    *smb_attr)
{

    smb_attr->smb_total_allocation_units            = attr->va_fs_space_total >> 12;
    smb_attr->smb_caller_available_allocation_units = attr->va_fs_space_avail >> 12;
    smb_attr->smb_actual_available_allocation_units = attr->va_fs_space_free >> 12;
    smb_attr->smb_sectors_per_allocation_unit       = 8;
    smb_attr->smb_bytes_per_sector                  = 512;

} /* chimera_smb_marshal_fs_full_size_info */

/* A timestamp field in a SET FileBasicInformation request carries one of
 * three sentinel values that all mean "do not change this field":
 *   0                 (NTTIME_OMIT)   - leave unchanged
 *   0xFFFFFFFFFFFFFFFF (NTTIME_FREEZE) - leave unchanged, stop auto-updates
 *   0xFFFFFFFFFFFFFFFE (NTTIME_THAW)   - leave unchanged, resume auto-updates
 * Any other value is a real timestamp to store.  (MS-FSCC 2.4.7; the
 * freeze/thaw values are exercised by smbtorture's smb2.timestamps.) */
static inline int
chimera_smb_time_is_omit(uint64_t nttime)
{
    return nttime == 0 ||
           nttime == UINT64_MAX ||
           nttime == UINT64_MAX - 1;
} /* chimera_smb_time_is_omit */

static inline void
chimera_smb_unmarshal_basic_info(
    const struct chimera_smb_attrs *smb_attrs,
    struct chimera_vfs_attrs       *attr)
{
    attr->va_req_mask = 0;
    attr->va_set_mask = 0;

    /* MS-FSCC 2.4.7 FileBasicInformation: a non-zero timestamp replaces the
     * stored value; a zero (or freeze/thaw sentinel) means "don't change".
     * Carry every time field through to the backend even when it is omitted,
     * using the TIME_OMIT sentinel — that way the implicit ctime bump (which
     * apply_attrs would otherwise stamp because the co-present DOS-attribute
     * change is a metadata write) is suppressed for the fields the client
     * asked to leave alone.  apply_attrs treats TIME_OMIT as "preserve". */
    if (!chimera_smb_time_is_omit(smb_attrs->smb_atime)) {
        chimera_nt_to_epoch(smb_attrs->smb_atime, &attr->va_atime);
    } else {
        attr->va_atime.tv_sec  = 0;
        attr->va_atime.tv_nsec = CHIMERA_VFS_TIME_OMIT;
    }
    attr->va_req_mask |= CHIMERA_VFS_ATTR_ATIME;
    attr->va_set_mask |= CHIMERA_VFS_ATTR_ATIME;

    if (!chimera_smb_time_is_omit(smb_attrs->smb_mtime)) {
        chimera_nt_to_epoch(smb_attrs->smb_mtime, &attr->va_mtime);
    } else {
        attr->va_mtime.tv_sec  = 0;
        attr->va_mtime.tv_nsec = CHIMERA_VFS_TIME_OMIT;
    }
    attr->va_req_mask |= CHIMERA_VFS_ATTR_MTIME;
    attr->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;

    if (!chimera_smb_time_is_omit(smb_attrs->smb_ctime)) {
        chimera_nt_to_epoch(smb_attrs->smb_ctime, &attr->va_ctime);
    } else {
        attr->va_ctime.tv_sec  = 0;
        attr->va_ctime.tv_nsec = CHIMERA_VFS_TIME_OMIT;
    }
    attr->va_req_mask |= CHIMERA_VFS_ATTR_CTIME;
    attr->va_set_mask |= CHIMERA_VFS_ATTR_CTIME;

    if (!chimera_smb_time_is_omit(smb_attrs->smb_crttime)) {
        chimera_nt_to_epoch(smb_attrs->smb_crttime, &attr->va_btime);
    } else {
        attr->va_btime.tv_sec  = 0;
        attr->va_btime.tv_nsec = CHIMERA_VFS_TIME_OMIT;
    }
    attr->va_req_mask |= CHIMERA_VFS_ATTR_BTIME;
    attr->va_set_mask |= CHIMERA_VFS_ATTR_BTIME;

    /* A FileAttributes value of 0 means "no change" (MS-FSCC 2.4.7); any
     * non-zero value replaces the persisted DOS attribute set. */
    if (smb_attrs->smb_attributes != 0) {
        attr->va_dos_attributes = smb_attrs->smb_attributes & SMB_DOS_ATTR_SETTABLE;
        attr->va_req_mask      |= CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
        attr->va_set_mask      |= CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
    }
} // chimera_smb_unmarshal_basic_info

static inline void
chimera_smb_unmarshal_end_of_file_info(
    const struct chimera_smb_attrs *smb_attrs,
    struct chimera_vfs_attrs       *attr)
{
    attr->va_req_mask = 0;
    attr->va_set_mask = 0;

    attr->va_size      = smb_attrs->smb_size;
    attr->va_req_mask |= CHIMERA_VFS_ATTR_SIZE;
    attr->va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
} // chimera_smb_unmarshal_end_of_file_info

/* The parse_*_info helpers below read a client-supplied SET_INFO buffer.  They
 * use the bounds-checked cursor readers and return -1 (without aborting) when
 * the buffer is shorter than the information class requires, so the caller can
 * answer STATUS_INFO_LENGTH_MISMATCH instead of crashing the server. */
static inline int
chimera_smb_parse_basic_info(
    struct evpl_iovec_cursor *cursor,
    struct chimera_smb_attrs *attrs)
{
    int rc = 0;

    rc |= evpl_iovec_cursor_try_get_uint64(cursor, &attrs->smb_crttime);
    rc |= evpl_iovec_cursor_try_get_uint64(cursor, &attrs->smb_atime);
    rc |= evpl_iovec_cursor_try_get_uint64(cursor, &attrs->smb_mtime);
    rc |= evpl_iovec_cursor_try_get_uint64(cursor, &attrs->smb_ctime);
    rc |= evpl_iovec_cursor_try_get_uint32(cursor, &attrs->smb_attributes);

    if (rc) {
        return -1;
    }

    attrs->smb_attr_mask |= SMB_ATTR_CRTTIME | SMB_ATTR_ATIME | SMB_ATTR_MTIME | SMB_ATTR_CTIME | SMB_ATTR_ATTRIBUTES;
    return 0;
} /* chimera_smb_parse_basic_info */

static inline int
chimera_smb_parse_disposition_info(
    struct evpl_iovec_cursor *cursor,
    struct chimera_smb_attrs *attrs)
{
    if (evpl_iovec_cursor_try_get_uint8(cursor, &attrs->smb_disposition)) {
        return -1;
    }
    attrs->smb_attr_mask |= SMB_ATTR_DISPOSITION;
    return 0;
} /* chimera_smb_parse_disposition_info */

static inline int
chimera_smb_parse_disposition_info_ex(
    struct evpl_iovec_cursor *cursor,
    struct chimera_smb_attrs *attrs)
{
    uint32_t flags;

    if (evpl_iovec_cursor_try_get_uint32(cursor, &flags)) {
        return -1;
    }
    attrs->smb_disposition = (flags & 0x01) ? 1 : 0;
    attrs->smb_attr_mask  |= SMB_ATTR_DISPOSITION;
    return 0;
} /* chimera_smb_parse_disposition_info_ex */

static inline int
chimera_smb_parse_position_info(
    struct evpl_iovec_cursor *cursor,
    struct chimera_smb_attrs *attrs)
{
    if (evpl_iovec_cursor_try_get_uint64(cursor, &attrs->smb_position)) {
        return -1;
    }
    attrs->smb_attr_mask |= SMB_ATTR_POSITION;
    return 0;
} /* chimera_smb_parse_position_info */

static inline int
chimera_smb_parse_end_of_file_info(
    struct evpl_iovec_cursor *cursor,
    struct chimera_smb_attrs *attrs)
{
    if (evpl_iovec_cursor_try_get_uint64(cursor, &attrs->smb_size)) {
        return -1;
    }
    attrs->smb_attr_mask |= SMB_ATTR_SIZE;
    return 0;
} /* chimera_smb_parse_end_of_file_info */


/* Append functions for serializing attributes - these enforce required fields */

/* Helper functions to append specific information classes */
static inline void
chimera_smb_append_basic_info(
    struct evpl_iovec_cursor       *cursor,
    const struct chimera_smb_attrs *attrs)
{
    chimera_vfs_abort_if((attrs->smb_attr_mask & SMB_ATTR_MASK_BASIC) != SMB_ATTR_MASK_BASIC,
                         "Missing required basic attributes: mask=%llx, required=%llx",
                         (unsigned long long) attrs->smb_attr_mask,
                         (unsigned long long) SMB_ATTR_MASK_BASIC);

    evpl_iovec_cursor_append_uint64(cursor, attrs->smb_crttime);
    evpl_iovec_cursor_append_uint64(cursor, attrs->smb_atime);
    evpl_iovec_cursor_append_uint64(cursor, attrs->smb_mtime);
    evpl_iovec_cursor_append_uint64(cursor, attrs->smb_ctime);
    evpl_iovec_cursor_append_uint32(cursor, attrs->smb_attributes);
    evpl_iovec_cursor_append_uint32(cursor, 0); /* Reserved */
} /* chimera_smb_append_basic_info */

static inline void
chimera_smb_append_standard_info(
    struct evpl_iovec_cursor       *cursor,
    struct chimera_smb_open_file   *open_file,
    const struct chimera_smb_attrs *attrs)
{
    chimera_vfs_abort_if((attrs->smb_attr_mask & SMB_ATTR_MASK_STANDARD) != SMB_ATTR_MASK_STANDARD,
                         "Missing required standard attributes: mask=%llx, required=%llx",
                         (unsigned long long) attrs->smb_attr_mask,
                         (unsigned long long) SMB_ATTR_MASK_STANDARD);

    evpl_iovec_cursor_append_uint64(cursor, attrs->smb_alloc_size);
    evpl_iovec_cursor_append_uint64(cursor, attrs->smb_size);
    evpl_iovec_cursor_append_uint32(cursor, attrs->smb_link_count);
    evpl_iovec_cursor_append_uint8(cursor, !!(open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE));
    evpl_iovec_cursor_append_uint8(cursor, attrs->smb_attributes & SMB2_FILE_ATTRIBUTE_DIRECTORY);
    evpl_iovec_cursor_append_uint16(cursor, 0); /* Reserved */
} /* chimera_smb_append_standard_info */

static inline void
chimera_smb_append_internal_info(
    struct evpl_iovec_cursor       *cursor,
    const struct chimera_smb_attrs *attrs)
{
    chimera_vfs_abort_if((attrs->smb_attr_mask & SMB_ATTR_MASK_INTERNAL) != SMB_ATTR_MASK_INTERNAL,
                         "Missing required internal attributes: mask=%llx, required=%llx",
                         (unsigned long long) attrs->smb_attr_mask,
                         (unsigned long long) SMB_ATTR_MASK_INTERNAL);

    evpl_iovec_cursor_append_uint64(cursor, attrs->smb_ino);
} /* chimera_smb_append_internal_info */

static inline void
chimera_smb_append_ea_info(
    struct evpl_iovec_cursor       *cursor,
    const struct chimera_smb_attrs *attrs)
{
    chimera_vfs_abort_if((attrs->smb_attr_mask & SMB_ATTR_MASK_EA) != SMB_ATTR_MASK_EA,
                         "Missing required EA attributes: mask=%llx, required=%llx",
                         (unsigned long long) attrs->smb_attr_mask,
                         (unsigned long long) SMB_ATTR_MASK_EA);

    evpl_iovec_cursor_append_uint32(cursor, attrs->smb_ea_size);
} /* chimera_smb_append_ea_info */

static inline void
chimera_smb_append_compression_info(
    struct evpl_iovec_cursor       *cursor,
    const struct chimera_smb_attrs *attrs)
{
    chimera_vfs_abort_if((attrs->smb_attr_mask & SMB_ATTR_MASK_COMPRESSION) != SMB_ATTR_MASK_COMPRESSION,
                         "Missing required compression attributes: mask=%llx, required=%llx",
                         (unsigned long long) attrs->smb_attr_mask,
                         (unsigned long long) SMB_ATTR_MASK_COMPRESSION);

    evpl_iovec_cursor_append_uint64(cursor, attrs->smb_size);
    evpl_iovec_cursor_append_uint16(cursor, attrs->smb_compression_format);
    evpl_iovec_cursor_append_uint8(cursor, 0); /* CompressionUnitShift */
    evpl_iovec_cursor_append_uint8(cursor, 0); /* ChunkShift */
    evpl_iovec_cursor_append_uint8(cursor, 0); /* ClusterShift */
    evpl_iovec_cursor_append_uint8(cursor, 0); /* Reserved1 */
    evpl_iovec_cursor_append_uint16(cursor, 0); /* Reserved2 */
} /* chimera_smb_append_compression_info */

static inline void
chimera_smb_append_attribute_tag_info(
    struct evpl_iovec_cursor       *cursor,
    const struct chimera_smb_attrs *attrs)
{
    chimera_vfs_abort_if((attrs->smb_attr_mask & SMB_ATTR_MASK_ATTRIBUTE_TAG) != SMB_ATTR_MASK_ATTRIBUTE_TAG,
                         "Missing required attribute tag attributes: mask=%llx, required=%llx",
                         (unsigned long long) attrs->smb_attr_mask,
                         (unsigned long long) SMB_ATTR_MASK_ATTRIBUTE_TAG);

    evpl_iovec_cursor_append_uint32(cursor, attrs->smb_attributes);
    evpl_iovec_cursor_append_uint32(cursor, attrs->smb_reparse_tag);
} /* chimera_smb_append_attribute_tag_info */

static inline void
chimera_smb_append_network_open_info(
    struct evpl_iovec_cursor       *cursor,
    const struct chimera_smb_attrs *attrs)
{
    chimera_vfs_abort_if((attrs->smb_attr_mask & SMB_ATTR_MASK_NETWORK_OPEN) != SMB_ATTR_MASK_NETWORK_OPEN,
                         "Missing required network open attributes: mask=%llx, required=%llx",
                         (unsigned long long) attrs->smb_attr_mask,
                         (unsigned long long) SMB_ATTR_MASK_NETWORK_OPEN);

    evpl_iovec_cursor_append_uint64(cursor, attrs->smb_crttime);
    evpl_iovec_cursor_append_uint64(cursor, attrs->smb_atime);
    evpl_iovec_cursor_append_uint64(cursor, attrs->smb_mtime);
    evpl_iovec_cursor_append_uint64(cursor, attrs->smb_ctime);
    evpl_iovec_cursor_append_uint64(cursor, attrs->smb_alloc_size);
    evpl_iovec_cursor_append_uint64(cursor, attrs->smb_size);
    evpl_iovec_cursor_append_uint32(cursor, attrs->smb_attributes);
    evpl_iovec_cursor_append_uint32(cursor, 0); /* Reserved */
} /* chimera_smb_append_network_open_info */

static inline void
chimera_smb_append_null_network_open_info_null(struct evpl_iovec_cursor *cursor)
{
    evpl_iovec_cursor_append_uint64(cursor, 0);
    evpl_iovec_cursor_append_uint64(cursor, 0);
    evpl_iovec_cursor_append_uint64(cursor, 0);
    evpl_iovec_cursor_append_uint64(cursor, 0);
    evpl_iovec_cursor_append_uint64(cursor, 0);
    evpl_iovec_cursor_append_uint64(cursor, 0);
    evpl_iovec_cursor_append_uint32(cursor, 0);
    evpl_iovec_cursor_append_uint32(cursor, 0);     /* Reserved */
} // chimera_smb_append_null_network_open_info_null

/* Append for FileAllInformation using the other append functions */
static inline void
chimera_smb_append_all_info(
    struct evpl_iovec_cursor       *cursor,
    struct chimera_smb_open_file   *open_file,
    const struct chimera_smb_attrs *attrs)
{
    chimera_smb_append_basic_info(cursor, attrs);
    chimera_smb_append_standard_info(cursor, open_file, attrs);
    chimera_smb_append_internal_info(cursor, attrs);
    chimera_smb_append_ea_info(cursor, attrs);

    evpl_iovec_cursor_append_uint32(cursor, attrs->smb_access_flags);
    evpl_iovec_cursor_append_uint64(cursor, open_file->position);

    /* Mode */
    evpl_iovec_cursor_append_uint32(cursor, 0);

    /* Alignemnt */
    evpl_iovec_cursor_append_uint32(cursor, 4095);

    /* Name Info */
    evpl_iovec_cursor_append_uint32(cursor, open_file->name_len);

    evpl_iovec_cursor_append_blob(cursor, open_file->name, open_file->name_len);

    evpl_iovec_cursor_append_uint32(cursor, 0); /* padding */

} /* chimera_smb_append_all_info */