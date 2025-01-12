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
                        attr_mask |= CHIMERA_VFS_ATTR_DEV;
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
                    // Add more cases as needed for other attributes
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
    uint32_t                 *words,
    int                       max_words)
{
    int max_word_used = 0;

    memset(words, 0, max_words * sizeof(uint32_t));

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_MASK_STAT && max_words > 0) {
        words[0]     |= (1 << FATTR4_SUPPORTED_ATTRS);
        max_word_used = 1;
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE && max_words > 0) {
        words[0] |= (1 << FATTR4_TYPE);
        words[0] |= (1 << FATTR4_SYMLINK_SUPPORT);
        words[0] |= (1 << FATTR4_NAMED_ATTR);
        words[0] |= (1 << FATTR4_RDATTR_ERROR);
        if (max_words > 1) {
            words[1]     |= (1 << (FATTR4_MODE - 32));
            max_word_used = 2;
        }
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_FH && max_words > 0) {
        words[0]     |= (1 << FATTR4_FILEHANDLE);
        max_word_used = 1;
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_CTIME && max_words > 0) {
        words[0] |= (1 << FATTR4_CHANGE);
        if (max_words > 1) {
            words[1]     |= (1 << (FATTR4_TIME_METADATA - 32));
            max_word_used = 2;
        }
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE && max_words > 0) {
        words[0] |= (1 << FATTR4_SIZE);
        if (max_words > 1) {
            words[1]     |= (1 << (FATTR4_SPACE_USED - 32));
            max_word_used = 2;
        }
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_NLINK && max_words > 0) {
        words[0] |= (1 << FATTR4_LINK_SUPPORT);
        if (max_words > 1) {
            words[1]     |= (1 << (FATTR4_NUMLINKS - 32));
            max_word_used = 2;
        }
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_DEV && max_words > 0) {
        words[0]     |= (1 << FATTR4_FSID);
        max_word_used = 1;
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_INUM && max_words > 0) {
        words[0]     |= (1 << FATTR4_UNIQUE_HANDLES);
        words[0]     |= (1 << FATTR4_FILEID);
        max_word_used = 1;
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_ATIME && max_words > 1) {
        words[0]     |= (1 << FATTR4_LEASE_TIME);
        words[1]     |= (1 << (FATTR4_TIME_ACCESS - 32));
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
    void *attrbase = attrs;

    memset(rsp_mask, 0, sizeof(uint32_t) * max_rsp_mask);
    *num_rsp_mask = 0;

    if (num_req_mask >= 1) {

        if (req_mask[0] & (1 << FATTR4_SUPPORTED_ATTRS)) {
            rsp_mask[0]  |= (1 << FATTR4_SUPPORTED_ATTRS);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 1);
            chimera_nfs4_attr_append_uint32(&attrs,
                                            FATTR4_SUPPORTED_ATTRS |
                                            FATTR4_TYPE |
                                            FATTR4_FH_EXPIRE_TYPE |
                                            FATTR4_CHANGE |
                                            FATTR4_SIZE |
                                            FATTR4_LINK_SUPPORT |
                                            FATTR4_SYMLINK_SUPPORT |
                                            FATTR4_NAMED_ATTR |
                                            FATTR4_FSID |
                                            FATTR4_UNIQUE_HANDLES |
                                            FATTR4_LEASE_TIME |
                                            FATTR4_RDATTR_ERROR |
                                            FATTR4_FILEHANDLE |
                                            FATTR4_FILEID |
                                            FATTR4_MODE |
                                            FATTR4_NUMLINKS |
                                            FATTR4_OWNER |
                                            FATTR4_OWNER_GROUP |
                                            FATTR4_SPACE_USED |
                                            FATTR4_TIME_ACCESS |
                                            FATTR4_TIME_MODIFY |
                                            FATTR4_TIME_METADATA);
        }

        if (req_mask[0] & (1 << FATTR4_TYPE)) {
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

            chimera_nfs4_attr_append_uint32(&attrs, 1);
        }

        if (req_mask[0] & (1 << FATTR4_CHANGE)) {
            rsp_mask[0]  |= (1 << FATTR4_CHANGE);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 0);
        }

        if (req_mask[0] & (1 << FATTR4_SIZE)) {
            rsp_mask[0]  |= (1 << FATTR4_SIZE);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_size);
        }

        if (req_mask[0] & (1 << FATTR4_LINK_SUPPORT)) {
            rsp_mask[0]  |= (1 << FATTR4_LINK_SUPPORT);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 0);
        }

        if (req_mask[0] & (1 << FATTR4_SYMLINK_SUPPORT)) {
            rsp_mask[0]  |= (1 << FATTR4_SYMLINK_SUPPORT);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 0);
        }

        if (req_mask[0] & (1 << FATTR4_NAMED_ATTR)) {
            rsp_mask[0]  |= (1 << FATTR4_NAMED_ATTR);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, 0);
        }

        if (req_mask[0] & (1 << FATTR4_FSID)) {
            rsp_mask[0]  |= (1 << FATTR4_FSID);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint64(&attrs, 42);
            chimera_nfs4_attr_append_uint64(&attrs, 42);

        }

        if (req_mask[0] & (1 << FATTR4_FILEID)) {
            rsp_mask[0]  |= (1 << FATTR4_FILEID);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_ino);
        }

        if (req_mask[0] & (1 << FATTR4_FILEHANDLE)) {
            rsp_mask[0]  |= (1 << FATTR4_FILEHANDLE);
            *num_rsp_mask = 1;

            chimera_nfs4_attr_append_uint32(&attrs, attr->va_fh_len);
            memcpy(attrs, attr->va_fh, attr->va_fh_len);
            attrs += attr->va_fh_len;
        }
    }

    if (num_req_mask >= 2) {
        if (req_mask[1] & (1 << (FATTR4_MODE - 32))) {
            rsp_mask[1]  |= (1 << (FATTR4_MODE - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint32(&attrs, attr->va_mode & ~S_IFMT);
        }

        if (req_mask[1] & (1 << (FATTR4_NUMLINKS - 32))) {
            rsp_mask[1]  |= (1 << (FATTR4_NUMLINKS - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint32(&attrs, attr->va_nlink);
        }

        if (req_mask[1] & (1 << (FATTR4_OWNER - 32))) {
            rsp_mask[1]  |= (1 << (FATTR4_OWNER - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_utf8str(&attrs, "root", 4);
        }

        if (req_mask[1] & (1 << (FATTR4_OWNER_GROUP - 32))) {
            rsp_mask[1]  |= (1 << (FATTR4_OWNER_GROUP - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_utf8str(&attrs, "root", 4);
        }

        if (req_mask[1] & (1 << (FATTR4_SPACE_USED - 32))) {
            rsp_mask[1]  |= (1 << (FATTR4_SPACE_USED - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_size);
        }

        if (req_mask[1] & (1 << (FATTR4_TIME_ACCESS - 32))) {
            rsp_mask[1]  |= (1 << (FATTR4_TIME_ACCESS - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_atime.tv_sec);
            chimera_nfs4_attr_append_uint32(&attrs, attr->va_atime.tv_nsec);
        }

        if (req_mask[1] & (1 << (FATTR4_TIME_MODIFY - 32))) {
            rsp_mask[1]  |= (1 << (FATTR4_TIME_MODIFY - 32));
            *num_rsp_mask = 2;

            chimera_nfs4_attr_append_uint64(&attrs, attr->va_mtime.tv_sec);
            chimera_nfs4_attr_append_uint32(&attrs, attr->va_mtime.tv_nsec);
        }

        if (req_mask[1] & (1 << (FATTR4_TIME_METADATA - 32))) {
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
    void *attrsend = attrs + attrvals_len;

    attr->va_req_mask = 0;

    if (num_req_mask >= 1) {
        if (req_mask[0] & (1 << FATTR4_SIZE)) {

            if (unlikely(attrs + sizeof(uint64_t) > attrsend)) {
                return -1;
            }

            attr->va_size      = chimera_nfs_ntoh64(*(uint64_t *) attrs);
            attrs             += sizeof(uint64_t);
            attr->va_req_mask |= CHIMERA_VFS_ATTR_SIZE;
        }
    }

    if (num_req_mask >= 2) {
        if (req_mask[1] & (1 << (FATTR4_MODE - 32))) {

            if (unlikely(attrs + sizeof(uint32_t) > attrsend)) {
                return -1;
            }

            attr->va_mode     |= chimera_nfs_ntoh32(*(uint32_t *) attrs);
            attrs             += sizeof(uint32_t);
            attr->va_req_mask |= CHIMERA_VFS_ATTR_MODE;
        }

        if (req_mask[1] & (1 << (FATTR4_TIME_ACCESS - 32))) {

            if (unlikely(attrs + sizeof(uint64_t) + sizeof(uint32_t) > attrsend)) {
                return -1;
            }

            attr->va_atime.tv_sec  = chimera_nfs_ntoh64(*(uint64_t *) attrs);
            attrs                 += sizeof(uint64_t);
            attr->va_atime.tv_nsec = chimera_nfs_ntoh32(*(uint32_t *) attrs);
            attrs                 += sizeof(uint32_t);
            attr->va_req_mask     |= CHIMERA_VFS_ATTR_ATIME;
        }

        if (req_mask[1] & (1 << (FATTR4_TIME_MODIFY - 32))) {

            if (unlikely(attrs + sizeof(uint64_t) + sizeof(uint32_t) > attrsend)) {
                return -1;
            }

            attr->va_mtime.tv_sec  = chimera_nfs_ntoh64(*(uint64_t *) attrs);
            attrs                 += sizeof(uint64_t);
            attr->va_mtime.tv_nsec = chimera_nfs_ntoh32(*(uint32_t *) attrs);
            attrs                 += sizeof(uint32_t);
            attr->va_req_mask     |= CHIMERA_VFS_ATTR_MTIME;
        }
    }

    if (req_mask[1] & (1 << (FATTR4_TIME_METADATA - 32))) {

        if (unlikely(attrs + sizeof(uint64_t) + sizeof(uint32_t) > attrsend)) {
            return -1;
        }

        attr->va_ctime.tv_sec  = chimera_nfs_ntoh64(*(uint64_t *) attrs);
        attrs                 += sizeof(uint64_t);
        attr->va_ctime.tv_nsec = chimera_nfs_ntoh32(*(uint32_t *) attrs);
        attrs                 += sizeof(uint32_t);
        attr->va_req_mask     |= CHIMERA_VFS_ATTR_CTIME;
    }

    return (attrs <= attrsend) ? 0 : -1;
} /* chimera_nfs4_unmarshall_attrs */
