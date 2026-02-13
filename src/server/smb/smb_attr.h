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
    /* Time attributes */
    smb_attr->smb_crttime    = 0; /* Creation time not tracked in VFS */
    smb_attr->smb_attr_mask |= SMB_ATTR_CRTTIME;

    smb_attr->smb_atime      = chimera_nt_time(&attr->va_atime);
    smb_attr->smb_attr_mask |= SMB_ATTR_ATIME;

    smb_attr->smb_mtime      = chimera_nt_time(&attr->va_mtime);
    smb_attr->smb_attr_mask |= SMB_ATTR_MTIME;

    smb_attr->smb_ctime      = chimera_nt_time(&attr->va_ctime);
    smb_attr->smb_attr_mask |= SMB_ATTR_CTIME;

    /* File attributes */
    smb_attr->smb_attributes = 0;
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

    /* Set default for normal file if no attributes are set */
    if (smb_attr->smb_attributes == 0 && (attr->va_mode & S_IFMT) == S_IFREG) {
        smb_attr->smb_attributes = SMB2_FILE_ATTRIBUTE_ARCHIVE;
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

static inline void
chimera_smb_unmarshal_basic_info(
    const struct chimera_smb_attrs *smb_attrs,
    struct chimera_vfs_attrs       *attr)
{
    attr->va_req_mask = 0;
    attr->va_set_mask = 0;

    chimera_nt_to_epoch(smb_attrs->smb_atime, &attr->va_atime);
    chimera_nt_to_epoch(smb_attrs->smb_mtime, &attr->va_mtime);
    chimera_nt_to_epoch(smb_attrs->smb_ctime, &attr->va_ctime);

    attr->va_req_mask |= CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME | CHIMERA_VFS_ATTR_CTIME;
    attr->va_set_mask |= CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME | CHIMERA_VFS_ATTR_CTIME;
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

static inline void
chimera_smb_parse_basic_info(
    struct evpl_iovec_cursor *cursor,
    struct chimera_smb_attrs *attrs)
{
    evpl_iovec_cursor_get_uint64(cursor, &attrs->smb_crttime);
    evpl_iovec_cursor_get_uint64(cursor, &attrs->smb_atime);
    evpl_iovec_cursor_get_uint64(cursor, &attrs->smb_mtime);
    evpl_iovec_cursor_get_uint64(cursor, &attrs->smb_ctime);
    evpl_iovec_cursor_get_uint32(cursor, &attrs->smb_attributes);

    attrs->smb_attr_mask |= SMB_ATTR_CRTTIME | SMB_ATTR_ATIME | SMB_ATTR_MTIME | SMB_ATTR_CTIME | SMB_ATTR_ATTRIBUTES;
} /* chimera_smb_parse_basic_info */

static inline void
chimera_smb_parse_disposition_info(
    struct evpl_iovec_cursor *cursor,
    struct chimera_smb_attrs *attrs)
{
    evpl_iovec_cursor_get_uint8(cursor, &attrs->smb_disposition);
    attrs->smb_attr_mask |= SMB_ATTR_DISPOSITION;
} /* chimera_smb_parse_disposition_info */

static inline void
chimera_smb_parse_end_of_file_info(
    struct evpl_iovec_cursor *cursor,
    struct chimera_smb_attrs *attrs)
{
    evpl_iovec_cursor_get_uint64(cursor, &attrs->smb_size);
    attrs->smb_attr_mask |= SMB_ATTR_SIZE;
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