#pragma once
static inline uint64_t
chimera_nfs4_getattr2mask(
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