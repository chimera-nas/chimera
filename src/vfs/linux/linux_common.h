#pragma once
#define chimera_linux_debug(...) chimera_debug("linux", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_linux_info(...)  chimera_info("linux", \
                                              __FILE__, \
                                              __LINE__, \
                                              __VA_ARGS__)
#define chimera_linux_error(...) chimera_error("linux", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_linux_fatal(...) chimera_fatal("linux", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_linux_abort(...) chimera_abort("linux", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)

#define chimera_linux_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "linux", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_linux_abort_if(cond, ...) \
        chimera_abort_if(cond, "linux", __FILE__, __LINE__, __VA_ARGS__)
#define TERM_STR(name, str, len, scratch) \
        char *(name) = scratch; \
        scratch     += (len) + 1; \
        memcpy((name), (str), (len)); \
        (name)[(len)] = '\0';

struct chimera_linux_mount {
    int                   mount_id;
    int                   mount_fd;
    struct UT_hash_handle hh;
};

struct chimera_linux_mount_table {
    struct chimera_linux_mount *mounts;
};

static inline enum chimera_vfs_error
chimera_linux_errno_to_status(int err)
{
    switch (err) {
        case 0:
            return CHIMERA_VFS_OK;
        case EPERM:
            return CHIMERA_VFS_EPERM;
        case ENOENT:
            return CHIMERA_VFS_ENOENT;
        case EIO:
            return CHIMERA_VFS_EIO;
        case ENXIO:
            return CHIMERA_VFS_ENXIO;
        case EACCES:
            return CHIMERA_VFS_EACCES;
        case EFAULT:
            return CHIMERA_VFS_EFAULT;
        case EEXIST:
            return CHIMERA_VFS_EEXIST;
        case EXDEV:
            return CHIMERA_VFS_EXDEV;
        case EMFILE:
            return CHIMERA_VFS_EMFILE;
        case ENOTDIR:
            return CHIMERA_VFS_ENOTDIR;
        case EISDIR:
            return CHIMERA_VFS_EISDIR;
        case EINVAL:
            return CHIMERA_VFS_EINVAL;
        case EFBIG:
            return CHIMERA_VFS_EFBIG;
        case ENOSPC:
            return CHIMERA_VFS_ENOSPC;
        case EROFS:
            return CHIMERA_VFS_EROFS;
        case EMLINK:
            return CHIMERA_VFS_EMLINK;
        case ENAMETOOLONG:
            return CHIMERA_VFS_ENAMETOOLONG;
        case ENOTEMPTY:
            return CHIMERA_VFS_ENOTEMPTY;
        case EOVERFLOW:
            return CHIMERA_VFS_EOVERFLOW;
        case EBADF:
            return CHIMERA_VFS_EBADF;
        case ENOTSUP:
            return CHIMERA_VFS_ENOTSUP;
        case EDQUOT:
            return CHIMERA_VFS_EDQUOT;
        case ESTALE:
            return CHIMERA_VFS_ESTALE;
        case ELOOP:
            return CHIMERA_VFS_ELOOP;
        default:
            return CHIMERA_VFS_UNSET;
    } /* switch */
} /* chimera_linux_errno_to_status */

static inline void
chimera_linux_stat_to_attr(
    struct chimera_vfs_attrs *attr,
    struct stat              *st)
{

    attr->va_mask |= CHIMERA_VFS_ATTR_MASK_STAT;

    attr->va_dev   = st->st_dev;
    attr->va_ino   = st->st_ino;
    attr->va_mode  = st->st_mode;
    attr->va_nlink = st->st_nlink;
    attr->va_uid   = st->st_uid;
    attr->va_gid   = st->st_gid;
    attr->va_rdev  = st->st_rdev;
    attr->va_size  = st->st_size;
    attr->va_atime = st->st_atim;
    attr->va_mtime = st->st_mtim;
    attr->va_ctime = st->st_ctim;
} /* linux_stat_to_chimera_attr */

static inline void
chimera_linux_statx_to_attr(
    struct chimera_vfs_attrs *attr,
    struct statx             *stx)
{

    attr->va_mask |= CHIMERA_VFS_ATTR_MASK_STAT;

    attr->va_dev           = ((uint64_t) stx->stx_dev_major << 32) | stx->stx_dev_minor;
    attr->va_ino           = stx->stx_ino;
    attr->va_mode          = stx->stx_mode;
    attr->va_nlink         = stx->stx_nlink;
    attr->va_uid           = stx->stx_uid;
    attr->va_gid           = stx->stx_gid;
    attr->va_rdev          = ((uint64_t) stx->stx_rdev_major << 32) | stx->stx_rdev_minor;
    attr->va_size          = stx->stx_size;
    attr->va_atime.tv_sec  = stx->stx_atime.tv_sec;
    attr->va_atime.tv_nsec = stx->stx_atime.tv_nsec;
    attr->va_mtime.tv_sec  = stx->stx_mtime.tv_sec;
    attr->va_mtime.tv_nsec = stx->stx_mtime.tv_nsec;
    attr->va_ctime.tv_sec  = stx->stx_ctime.tv_sec;
    attr->va_ctime.tv_nsec = stx->stx_ctime.tv_nsec;
} /* io_uring_stat_to_chimera_attr */

static inline void
chimera_linux_statvfs_to_attr(
    struct chimera_vfs_attrs *attr,
    struct statvfs           *stvfs)
{
    attr->va_space_total = stvfs->f_blocks * stvfs->f_bsize;
    attr->va_space_free  = stvfs->f_bavail * stvfs->f_bsize;
    attr->va_space_avail = attr->va_space_free;
    attr->va_space_used  = attr->va_space_total - attr->va_space_free;

    attr->va_files_used  = stvfs->f_files;
    attr->va_files_free  = stvfs->f_ffree;
    attr->va_files_total = attr->va_files_used + attr->va_files_free;
} /* linux_statvfs_to_chimera_attr */

static int
open_mount_path_by_id(int mount_id)
{
    int     mi_mount_id, found;
    char    mount_path[PATH_MAX];
    char   *linep;
    FILE   *fp;
    size_t  lsize;
    ssize_t nread;

    fp = fopen("/proc/self/mountinfo", "r");

    if (fp == NULL) {
        return EIO;
    }

    found = 0;

    linep = NULL;

    while (!found) {

        nread = getline(&linep, &lsize, fp);

        if (nread == -1) {
            break;
        }

        nread = sscanf(linep, "%d %*d %*s %*s %s", &mi_mount_id, mount_path);

        if (nread != 2) {
            exit(EXIT_FAILURE);
        }

        if (mi_mount_id == mount_id) {
            found = 1;
        }
    }

    free(linep);

    fclose(fp);

    if (!found) {
        errno = ENOENT;
        return -1;
    }

    return open(mount_path, O_RDONLY);
} /* open_mount_path_by_id */

static inline int
linux_get_fh(
    int         fd,
    const char *path,
    uint8_t     isdir,
    void       *fh,
    uint32_t   *fh_len)
{
    uint8_t              buf[sizeof(struct file_handle) + MAX_HANDLE_SZ];
    static const uint8_t fh_magic = CHIMERA_VFS_FH_MAGIC_LINUX;
    struct file_handle  *handle   = (struct file_handle *) buf;
    int                  rc;
    int                  mount_id;

    handle->handle_bytes = MAX_HANDLE_SZ;

    rc = name_to_handle_at(
        fd,
        path,
        handle,
        &mount_id,
        *path ? 0 : AT_EMPTY_PATH);

    if (rc < 0) {
        return -1;
    }

    chimera_linux_abort_if(13 + handle->handle_bytes > CHIMERA_VFS_FH_SIZE,
                           "Returned handle exceeds CHIMERA_VFS_FH_SIZE");

    memcpy(fh, &fh_magic, 1);
    memcpy(fh + 1, &mount_id, 4);
    memcpy(fh + 5, &isdir, 1);
    memcpy(fh + 6, handle, 8 + handle->handle_bytes);

    *fh_len = 14 + handle->handle_bytes;

    return 0;
} /* linux_get_fh */

static inline int
linux_fh_is_dir(const void *fh)
{
    return *(uint8_t *) (fh + 5);
} /* linux_fh_is_dir */

static void
linux_mount_table_destroy(struct chimera_linux_mount_table *mount_table)
{
    struct chimera_linux_mount *mount;

    while (mount_table->mounts) {
        mount = mount_table->mounts;
        HASH_DEL(mount_table->mounts, mount);
        close(mount->mount_fd);
        free(mount);
    }
} /* linux_mount_table_destroy */

static inline int
linux_open_by_handle(
    struct chimera_linux_mount_table *mount_table,
    const void                       *fh,
    uint32_t                          fh_len,
    unsigned int                      flags)
{
    struct chimera_linux_mount *mount;
    struct file_handle         *handle   = (struct file_handle *) (fh + 6);
    int                         mount_id = *(int *) (fh + 1);

    HASH_FIND_INT(mount_table->mounts, &mount_id, mount);

    if (!mount) {
        mount           = calloc(1, sizeof(*mount));
        mount->mount_id = mount_id;
        mount->mount_fd = open_mount_path_by_id(mount_id);

        if (mount->mount_fd < 0) {
            free(mount);
            return -1;
        }

        HASH_ADD_INT(mount_table->mounts, mount_id, mount);
    }

    return open_by_handle_at(mount->mount_fd, handle, flags);
} /* linux_open_by_handle */

static inline void
chimera_linux_map_attrs(
    uint64_t                  attrmask,
    struct chimera_vfs_attrs *attr,
    int                       fd,
    const void               *fh,
    int                       fhlen)
{
    int            rc;
    struct stat    st;
    struct statvfs stvfs;

    if (attrmask & (CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_FH)) {

        rc = fstat(fd, &st);

        if (rc) {
            return;
        }

        chimera_linux_stat_to_attr(attr, &st);
    }

    if (attrmask & CHIMERA_VFS_ATTR_FH) {
        if (fh) {
            attr->va_mask |= CHIMERA_VFS_ATTR_FH;
            memcpy(attr->va_fh, fh, fhlen);
            attr->va_fh_len = fhlen;
        } else {
            rc = linux_get_fh(fd, "", S_ISDIR(st.st_mode),
                              attr->va_fh,
                              &attr->va_fh_len);

            if (rc  == 0) {
                attr->va_mask |= CHIMERA_VFS_ATTR_FH;
            }
        }
    }

    if (attrmask & CHIMERA_VFS_ATTR_MASK_STATFS) {
        rc = fstatvfs(fd, &stvfs);

        if (rc == 0) {
            attr->va_mask |= CHIMERA_VFS_ATTR_MASK_STATFS;
            chimera_linux_statvfs_to_attr(attr, &stvfs);
        }
    }

} /* chimera_linux_map_attrs */

static inline void
chimera_linux_map_child_attrs(
    struct chimera_vfs_request *request,
    uint64_t                    attrmask,
    struct chimera_vfs_attrs   *attr,
    int                         dirfd,
    const char                 *name)
{
    int            rc;
    struct stat    st;
    struct statvfs stvfs;

    if (attrmask & (CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_FH)) {

        rc = fstatat(dirfd, name, &st, AT_SYMLINK_NOFOLLOW);

        if (rc) {
            request->status = chimera_linux_errno_to_status(errno);
            request->complete(request);
            return;
        }

        chimera_linux_stat_to_attr(attr, &st);
    }

    if (attrmask & CHIMERA_VFS_ATTR_FH) {
        rc = linux_get_fh(dirfd, name, S_ISDIR(st.st_mode),
                          attr->va_fh,
                          &attr->va_fh_len);

        if (rc) {
            request->status = chimera_linux_errno_to_status(errno);
            request->complete(request);
            return;
        }

        attr->va_mask |= CHIMERA_VFS_ATTR_FH;
    }

    if (attrmask & CHIMERA_VFS_ATTR_MASK_STATFS) {
        rc = fstatvfs(dirfd, &stvfs);

        if (rc == 0) {
            attr->va_mask |= CHIMERA_VFS_ATTR_MASK_STATFS;
            chimera_linux_statvfs_to_attr(attr, &stvfs);
        }
    }

    request->status = CHIMERA_VFS_OK;
} /* chimera_linux_map_child_attrs */

