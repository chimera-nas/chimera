// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/misc.h"

/* XXX */
#include <sys/stat.h>

#include "vfs/vfs.h"

static inline uint64_t
chimera_nfs4_attr2mask(
    uint32_t *words,
    int       num_words)
{
    uint64_t attr_mask = 0;

    for (int i = 0; i < num_words; i++) {
        uint32_t word = words[i];
        for (int bit = 0; bit < 32; bit++) {
            if (word & (1 << bit)) {
                switch (i * 32 + bit) {
                    case FATTR4_SUPPORTED_ATTRS:
                        attr_mask |= CHIMERA_VFS_ATTR_MASK_STAT;
                        break;
                    case FATTR4_TYPE:
                        attr_mask |= CHIMERA_VFS_ATTR_MODE;
                        break;
                    case FATTR4_FH_EXPIRE_TYPE:
                        attr_mask |= CHIMERA_VFS_ATTR_FH;
                        break;
                    case FATTR4_CHANGE:
                        attr_mask |= CHIMERA_VFS_ATTR_CTIME;
                        break;
                    case FATTR4_SIZE:
                        attr_mask |= CHIMERA_VFS_ATTR_SIZE;
                        break;
                    case FATTR4_LINK_SUPPORT:
                        attr_mask |= CHIMERA_VFS_ATTR_NLINK;
                        break;
                    case FATTR4_SYMLINK_SUPPORT:
                        attr_mask |= CHIMERA_VFS_ATTR_MODE;
                        break;
                    case FATTR4_NAMED_ATTR:
                        attr_mask |= CHIMERA_VFS_ATTR_MODE;
                        break;
                    case FATTR4_FSID:
                        attr_mask |= CHIMERA_VFS_ATTR_FSID;
                        break;
                    case FATTR4_FILES_AVAIL:
                        attr_mask |= CHIMERA_VFS_ATTR_SPACE_AVAIL;
                        break;
                    case FATTR4_FILES_FREE:
                        attr_mask |= CHIMERA_VFS_ATTR_SPACE_FREE;
                        break;
                    case FATTR4_FILES_TOTAL:
                        attr_mask |= CHIMERA_VFS_ATTR_SPACE_TOTAL;
                        break;
                    case FATTR4_UNIQUE_HANDLES:
                        attr_mask |= CHIMERA_VFS_ATTR_INUM;
                        break;
                    case FATTR4_LEASE_TIME:
                        attr_mask |= CHIMERA_VFS_ATTR_ATIME;
                        break;
                    case FATTR4_RDATTR_ERROR:
                        attr_mask |= CHIMERA_VFS_ATTR_MODE;
                        break;
                    case FATTR4_FILEHANDLE:
                        attr_mask |= CHIMERA_VFS_ATTR_FH;
                        break;
                    case FATTR4_OWNER:
                        attr_mask |= CHIMERA_VFS_ATTR_UID;
                        break;
                    case FATTR4_OWNER_GROUP:
                        attr_mask |= CHIMERA_VFS_ATTR_GID;
                        break;
                    case FATTR4_SPACE_AVAIL:
                        attr_mask |= CHIMERA_VFS_ATTR_SPACE_AVAIL;
                        break;
                    case FATTR4_SPACE_FREE:
                        attr_mask |= CHIMERA_VFS_ATTR_SPACE_FREE;
                        break;
                    case FATTR4_SPACE_TOTAL:
                        attr_mask |= CHIMERA_VFS_ATTR_SPACE_TOTAL;
                        break;
                    case FATTR4_SPACE_USED:
                        attr_mask |= CHIMERA_VFS_ATTR_SPACE_USED;
                        break;
                    default:
                        break;
                } /* switch */
            }
        }
    }

    return attr_mask;
} /* chimera_nfs4_getattr2mask */

static inline int
chimera_nfs4_mask2attr(
    struct chimera_vfs_attrs *attr,
    uint32_t                  num_req_mask,
    uint32_t                 *req_mask,
    uint32_t                 *rsp_mask)
{
    int max_word_used = 0;

    memset(rsp_mask, 0, sizeof(uint32_t) * num_req_mask);

    if (num_req_mask >= 1 &&
        (req_mask[1] & (1 << (FATTR4_MODE - 32))) &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE)) {
        rsp_mask[1]  |= (1 << (FATTR4_MODE - 32));
        max_word_used = 2;
    }

    if (num_req_mask >= 1 &&
        (req_mask[0] & (1 << FATTR4_SIZE)) &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE)) {
        rsp_mask[0]  |= (1 << FATTR4_SIZE);
        max_word_used = 1;
    }

    if (num_req_mask >= 2 &&
        (req_mask[1] & (1 << (FATTR4_TIME_ACCESS_SET - 32))) &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_ATIME)) {
        rsp_mask[1]  |= (1 << (FATTR4_TIME_ACCESS_SET - 32));
        max_word_used = 2;
    }

    if (num_req_mask >= 2 &&
        (req_mask[1] & (1 << (FATTR4_TIME_MODIFY_SET - 32))) &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME)) {
        rsp_mask[1]  |= (1 << (FATTR4_TIME_MODIFY_SET - 32));
        max_word_used = 2;
    }

    return max_word_used;
} /* chimera_nfs4_mask2attr */

static inline void
chimera_nfs4_attr_append_uint32(
    void   **attrs,
    uint32_t value)
{
    *(uint32_t *) *attrs = chimera_nfs_hton32(value);
    *attrs              += sizeof(uint32_t);
} /* chimera_nfs4_attr_append_uint32 */

static void
chimera_nfs4_attr_append_uint64(
    void   **attrs,
    uint64_t value)
{
    *(uint64_t *) *attrs = chimera_nfs_hton64(value);
    *attrs              += sizeof(uint64_t);
} /* chimera_nfs4_attr_append_uint64 */

static void
chimera_nfs4_attr_append_utf8str(
    void      **attrs,
    const char *value,
    int         len)
{
    int pad;

    chimera_nfs4_attr_append_uint32(attrs, len);
    memcpy(*attrs, value, len);

    pad = (4 - (len % 4)) % 4;

    if (pad ) {
        memset(*attrs + len, 0, pad);
    }
    *attrs += len + pad;
} /* chimera_nfs4_attr_append_utf8str */

static void
chimera_nfs4_attr_append_utf8str_from_uint64(
    void   **attrs,
    uint64_t value)
{
    char str[21];
    int  len;

    len = snprintf(str, sizeof(str), "%lu", value);

    chimera_nfs4_attr_append_utf8str(attrs, str, len);

} /* chimera_nfs4_attr_append_utf8str_from_uintt64 */

static int
chimera_nfs4_marshall_attrs(
    const struct chimera_vfs_attrs *attr,
    uint32_t                        num_req_mask,
    uint32_t                       *req_mask,
    uint32_t                       *num_rsp_mask,
    uint32_t                       *rsp_mask,
    uint32_t                        max_rsp_mask,
    void                           *attrs,
    uint32_t                       *attrvals_len)
{
    void    *attrbase = attrs;
    uint64_t v64;

    memset(rsp_mask, 0, sizeof(uint32_t) * max_rsp_mask);
    *num_rsp_mask = 0;

    if (num_req_mask >= 1) {

        if (req_mask[0] & (1 << FATTR4_SUPPORTED_ATTRS)) {
            rsp_mask[0]  |= (1 << FATTR4_SUPPORTED_ATTRS);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 2);
            chimera_nfs4_attr_append_uint32(&attrs,
                                            (1 << FATTR4_SUPPORTED_ATTRS) |
                                            (1 << FATTR4_TYPE) |
                                            (1 << FATTR4_FH_EXPIRE_TYPE) |
                                            (1 << FATTR4_CHANGE) |
                                            (1 << FATTR4_SIZE) |
                                            (1 << FATTR4_LINK_SUPPORT) |
                                            (1 << FATTR4_SYMLINK_SUPPORT) |
                                            (1 << FATTR4_NAMED_ATTR) |
                                            (1 << FATTR4_FSID) |
                                            (1 << FATTR4_UNIQUE_HANDLES) |
                                            (1 << FATTR4_LEASE_TIME) |
                                            (1 << FATTR4_RDATTR_ERROR) |
                                            /* acl */
                                            (1 << FATTR4_ACLSUPPORT) |
                                            (1 << FATTR4_ARCHIVE) |
                                            (1 << FATTR4_CANSETTIME) |
                                            (1 << FATTR4_CASE_INSENSITIVE) |
                                            (1 << FATTR4_CASE_PRESERVING) |
                                            (1 << FATTR4_CHOWN_RESTRICTED) |
                                            (1 << FATTR4_FILEHANDLE) |
                                            (1 << FATTR4_FILEID) |
                                            (1 << FATTR4_FILES_AVAIL) |
                                            (1 << FATTR4_FILES_FREE) |
                                            (1 << FATTR4_FILES_TOTAL) |
                                            /* fs_locations */
                                            /* hidden */
                                            /* homogeneous */
                                            /* maxfilesize */
                                            /* maxlink */
                                            (1 << FATTR4_MAXNAME) |
                                            (1 << FATTR4_MAXREAD) |
                                            (1 << FATTR4_MAXWRITE));

            chimera_nfs4_attr_append_uint32(&attrs,
                                            (1UL << (FATTR4_MODE - 32)) |
                                            (1UL << (FATTR4_NUMLINKS - 32)) |
                                            (1UL << (FATTR4_OWNER - 32)) |
                                            (1UL << (FATTR4_OWNER_GROUP - 32)) |
                                            (1UL << (FATTR4_SPACE_USED - 32)) |
                                            (1UL << (FATTR4_TIME_ACCESS - 32)) |
                                            (1UL << (FATTR4_TIME_ACCESS_SET - 32)) |
                                            (1UL << (FATTR4_TIME_MODIFY - 32)) |
                                            (1UL << (FATTR4_TIME_MODIFY_SET - 32)) |
                                            (1UL << (FATTR4_TIME_METADATA - 32)) |
                                            (1UL << (FATTR4_SPACE_AVAIL - 32)) |
                                            (1UL << (FATTR4_SPACE_FREE - 32)) |
                                            (1UL << (FATTR4_SPACE_TOTAL - 32)));
        }

        if (req_mask[0] & (1 << FATTR4_TYPE) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE)) {
            rsp_mask[0]  |= (1 << FATTR4_TYPE);
            *num_rsp_mask = 1;

            if (S_ISREG(attr->va_mode)) {
                chimera_nfs4_attr_append_uint32(&attrs, NF4REG);
            } else if (S_ISDIR(attr->va_mode)) {
                chimera_nfs4_attr_append_uint32(&attrs, NF4DIR);
            } else if (S_ISCHR(attr->va_mode)) {
                chimera_nfs4_attr_append_uint32(&attrs, NF4CHR);
            } else if (S_ISBLK(attr->va_mode)) {
                chimera_nfs4_attr_append_uint32(&attrs, NF4BLK);
            } else if (S_ISFIFO(attr->va_mode)) {
                chimera_nfs4_attr_append_uint32(&attrs, NF4FIFO);
            } else if (S_ISSOCK(attr->va_mode)) {
                chimera_nfs4_attr_append_uint32(&attrs, NF4SOCK);
            } else if (S_ISLNK(attr->va_mode)) {
                chimera_nfs4_attr_append_uint32(&attrs, NF4LNK);
            } else {
                chimera_nfs4_attr_append_uint32(&attrs, NF4REG);
            }
        }

        if (req_mask[0] & (1 << FATTR4_FH_EXPIRE_TYPE)) {
            rsp_mask[0]  |= (1 << FATTR4_FH_EXPIRE_TYPE);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, FH4_PERSISTENT);
        }

        if (req_mask[0] & (1 << FATTR4_CHANGE)) {
            rsp_mask[0]  |= (1 << FATTR4_CHANGE);
            *num_rsp_mask = 1;

            v64 = attr->va_ctime.tv_sec * 1000000000 + attr->va_ctime.tv_nsec;
            chimera_nfs4_attr_append_uint64(&attrs, v64);
        }

        if ((req_mask[0] & (1 << FATTR4_SIZE)) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE)) {
            rsp_mask[0]  |= (1 << FATTR4_SIZE);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_size);
        }

        if (req_mask[0] & (1 << FATTR4_LINK_SUPPORT)) {
            rsp_mask[0]  |= (1 << FATTR4_LINK_SUPPORT);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 1);
        }

        if (req_mask[0] & (1 << FATTR4_SYMLINK_SUPPORT)) {
            rsp_mask[0]  |= (1 << FATTR4_SYMLINK_SUPPORT);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 1);
        }

        if (req_mask[0] & (1 << FATTR4_NAMED_ATTR)) {
            rsp_mask[0]  |= (1 << FATTR4_NAMED_ATTR);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 0);
        }

        if ((req_mask[0] & (1 << FATTR4_FSID)) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_FSID)) {
            rsp_mask[0]  |= (1 << FATTR4_FSID);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_fsid);
            chimera_nfs4_attr_append_uint64(&attrs, 0);

        }

        if (req_mask[0] & (1 << FATTR4_UNIQUE_HANDLES)) {
            rsp_mask[0]  |= (1 << FATTR4_UNIQUE_HANDLES);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 1);
        }

        if (req_mask[0] & (1 << FATTR4_LEASE_TIME)) {
            rsp_mask[0]  |= (1 << FATTR4_LEASE_TIME);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 600);
        }

        if (req_mask[0] & (1 << FATTR4_ACLSUPPORT)) {
            rsp_mask[0]  |= (1 << FATTR4_ACLSUPPORT);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 0);
        }

        if (req_mask[0] & (1 << FATTR4_ARCHIVE)) {
            rsp_mask[0]  |= (1 << FATTR4_ARCHIVE);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 0);
        }

        if (req_mask[0] & (1 << FATTR4_CANSETTIME)) {
            rsp_mask[0]  |= (1 << FATTR4_CANSETTIME);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 1);
        }

        if (req_mask[0] & (1 << FATTR4_CASE_INSENSITIVE)) {
            rsp_mask[0]  |= (1 << FATTR4_CASE_INSENSITIVE);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 0);
        }

        if (req_mask[0] & (1 << FATTR4_CASE_PRESERVING)) {
            rsp_mask[0]  |= (1 << FATTR4_CASE_PRESERVING);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 1);
        }

        if (req_mask[0] & (1 << FATTR4_CHOWN_RESTRICTED)) {
            rsp_mask[0]  |= (1 << FATTR4_CHOWN_RESTRICTED);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 0);
        }

        if (req_mask[0] & (1 << FATTR4_FILEHANDLE) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
            rsp_mask[0]  |= (1 << FATTR4_FILEHANDLE);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, attr->va_fh_len);
            memcpy(attrs, attr->va_fh, attr->va_fh_len);
            attrs += attr->va_fh_len;

            if (attr->va_fh_len & 0x7) {
                uint32_t pad = 8 - (attr->va_fh_len & 0x7);
                memset(attrs, 0, pad);
                attrs += pad;
            }
        }

        if (req_mask[0] & (1 << FATTR4_FILEID) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_INUM)) {
            rsp_mask[0]  |= (1 << FATTR4_FILEID);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_ino);
        }

        if (req_mask[0] & (1 << FATTR4_FILES_AVAIL) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_FILES_FREE)) {
            rsp_mask[0]  |= (1 << FATTR4_FILES_AVAIL);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_fs_files_avail);
        }

        if (req_mask[0] & (1 << FATTR4_FILES_FREE) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_FILES_FREE)) {
            rsp_mask[0]  |= (1 << FATTR4_FILES_FREE);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_fs_files_free);
        }

        if (req_mask[0] & (1 << FATTR4_FILES_TOTAL) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_FILES_TOTAL)) {
            rsp_mask[0]  |= (1 << FATTR4_FILES_TOTAL);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_fs_files_total);
        }

        if (req_mask[0] & (1 << FATTR4_MAXNAME)) {
            rsp_mask[0]  |= (1 << FATTR4_MAXNAME);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 255);
        }

        if (req_mask[0] & (1 << FATTR4_MAXREAD)) {
            rsp_mask[0]  |= (1 << FATTR4_MAXREAD);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint64(&attrs, 1024 * 1024);
        }

        if (req_mask[0] & (1 << FATTR4_MAXWRITE)) {
            rsp_mask[0]  |= (1 << FATTR4_MAXWRITE);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint64(&attrs, 1024 * 1024);
        }
    }

    if (num_req_mask >= 2) {
        if ((req_mask[1] & (1 << (FATTR4_MODE - 32))) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE)) {
            rsp_mask[1]  |= (1 << (FATTR4_MODE - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint32(&attrs, attr->va_mode & ~S_IFMT);
        }

        if ((req_mask[1] & (1 << (FATTR4_NUMLINKS - 32))) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_NLINK)) {
            rsp_mask[1]  |= (1 << (FATTR4_NUMLINKS - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint32(&attrs, attr->va_nlink);
        }

        if ((req_mask[1] & (1 << (FATTR4_OWNER - 32))) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_UID)) {
            rsp_mask[1]  |= (1 << (FATTR4_OWNER - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_utf8str_from_uint64(&attrs, attr->va_uid);
        }

        if ((req_mask[1] & (1 << (FATTR4_OWNER_GROUP - 32))) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_GID)) {
            rsp_mask[1]  |= (1 << (FATTR4_OWNER_GROUP - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_utf8str_from_uint64(&attrs, attr->va_gid);
        }

        if ((req_mask[1] & (1 <<  (FATTR4_SPACE_AVAIL - 32))) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_SPACE_AVAIL)) {
            rsp_mask[1]  |= (1 <<  (FATTR4_SPACE_AVAIL - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_fs_space_avail);
        }

        if ((req_mask[1] & (1 << (FATTR4_SPACE_FREE - 32))) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_SPACE_FREE)) {
            rsp_mask[1]  |= (1 << (FATTR4_SPACE_FREE - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_fs_space_free);
        }

        if ((req_mask[1] & (1 << (FATTR4_SPACE_TOTAL - 32))) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_SPACE_TOTAL)) {
            rsp_mask[1]  |= (1 << (FATTR4_SPACE_TOTAL - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_fs_space_total);
        }

        if ((req_mask[1] & (1 << (FATTR4_SPACE_USED - 32))) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_SPACE_USED)) {
            rsp_mask[1]  |= (1 << (FATTR4_SPACE_USED - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_space_used);
        }

        if ((req_mask[1] & (1 << (FATTR4_TIME_ACCESS - 32))) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_ATIME)) {
            rsp_mask[1]  |= (1 << (FATTR4_TIME_ACCESS - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_atime.tv_sec);
            chimera_nfs4_attr_append_uint32(&attrs, attr->va_atime.tv_nsec);
        }

        if ((req_mask[1] & (1 << (FATTR4_TIME_MODIFY - 32))) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME)) {
            rsp_mask[1]  |= (1 << (FATTR4_TIME_MODIFY - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_mtime.tv_sec);
            chimera_nfs4_attr_append_uint32(&attrs, attr->va_mtime.tv_nsec);
        }

        if ((req_mask[1] & (1 << (FATTR4_TIME_METADATA - 32))) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_CTIME)) {
            rsp_mask[1]  |= (1 << (FATTR4_TIME_METADATA - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_ctime.tv_sec);
            chimera_nfs4_attr_append_uint32(&attrs, attr->va_ctime.tv_nsec);
        }
    }

    *attrvals_len = attrs - attrbase;

    return 0;
} /* chimera_nfs4_marshall_attrs */

static int
chimera_nfs4_unmarshall_attrs(
    struct chimera_vfs_attrs *attr,
    uint32_t                  num_req_mask,
    uint32_t                 *req_mask,
    void                     *attrs,
    uint32_t                  attrvals_len)
{
    void    *attrsend = attrs + attrvals_len;
    uint32_t set_it;

    attr->va_set_mask = 0;

    if (num_req_mask >= 1) {
        if (req_mask[0] & (1 << FATTR4_SIZE)) {

            if (unlikely(attrs + sizeof(uint64_t) > attrsend)) {
                return -1;
            }

            attr->va_size      = chimera_nfs_ntoh64(*(uint64_t *) attrs);
            attrs             += sizeof(uint64_t);
            attr->va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
        }
    }

    if (num_req_mask >= 2) {
        if (req_mask[1] & (1 << (FATTR4_MODE - 32))) {

            if (unlikely(attrs + sizeof(uint32_t) > attrsend)) {
                return -1;
            }

            attr->va_mode      = chimera_nfs_ntoh32(*(uint32_t *) attrs);
            attrs             += sizeof(uint32_t);
            attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
        }

        if (req_mask[1] & (1 << (FATTR4_TIME_ACCESS_SET - 32))) {

            if (unlikely(attrs + sizeof(uint32_t) > attrsend)) {
                return -1;
            }

            set_it = chimera_nfs_ntoh32(*(uint32_t *) attrs);
            attrs += sizeof(uint32_t);

            if (set_it) {

                if (unlikely(attrs + sizeof(uint64_t) + sizeof(uint32_t) > attrsend)) {
                    return -1;
                }

                attr->va_atime.tv_sec  = chimera_nfs_ntoh64(*(uint64_t *) attrs);
                attrs                 += sizeof(uint64_t);
                attr->va_atime.tv_nsec = chimera_nfs_ntoh32(*(uint32_t *) attrs);
                attrs                 += sizeof(uint32_t);
            } else {
                attr->va_atime.tv_sec  = 0;
                attr->va_atime.tv_nsec = CHIMERA_VFS_TIME_NOW;
            }

            attr->va_set_mask |= CHIMERA_VFS_ATTR_ATIME;
        }

        if (req_mask[1] & (1 << (FATTR4_TIME_MODIFY_SET - 32))) {

            if (unlikely(attrs + sizeof(uint32_t) > attrsend)) {
                return -1;
            }

            set_it = chimera_nfs_ntoh32(*(uint32_t *) attrs);
            attrs += sizeof(uint32_t);

            if (set_it) {

                if (unlikely(attrs + sizeof(uint64_t) + sizeof(uint32_t) > attrsend)) {
                    return -1;
                }

                attr->va_mtime.tv_sec  = chimera_nfs_ntoh64(*(uint64_t *) attrs);
                attrs                 += sizeof(uint64_t);
                attr->va_mtime.tv_nsec = chimera_nfs_ntoh32(*(uint32_t *) attrs);
                attrs                 += sizeof(uint32_t);
            } else {
                attr->va_mtime.tv_sec  = 0;
                attr->va_mtime.tv_nsec = CHIMERA_VFS_TIME_NOW;
            }

            attr->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;
        }


    }

    return (attrs <= attrsend) ? 0 : -1;
} /* chimera_nfs4_unmarshall_attrs */

static void
chimera_nfs4_set_changeinfo(
    struct change_info4      *cinfo,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr)
{
    cinfo->atomic = (dir_pre_attr->va_set_mask & CHIMERA_VFS_ATTR_ATOMIC) &&
        (dir_post_attr->va_set_mask & CHIMERA_VFS_ATTR_ATOMIC);
    cinfo->before = dir_pre_attr->va_mtime.tv_sec * 1000000000 + dir_pre_attr->va_mtime.tv_nsec;
    cinfo->after  = dir_post_attr->va_mtime.tv_sec * 1000000000 + dir_post_attr->va_mtime.tv_nsec;

} /* chimera_nfs4_set_changeinfo */
