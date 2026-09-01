// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/sysmacros.h>
#include <sys/statvfs.h>
#include <sys/uio.h>
#include <dirent.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <sys/xattr.h>
#include <linux/fs.h>
#include <uthash.h>
#include <utlist.h>
#include <jansson.h>
#include <linux/version.h>
#include "vfs/sdk/vfs_error.h"
#include "vfs/sdk/vfs_acl.h"

// fchmodat support for AT_SYMLINK_NOFOLLOW was added in Linux 6.6
#if defined(LINUX_VERSION_CODE) && defined(KERNEL_VERSION)
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 6, 0)
#define HAVE_FCHMODAT_AT_SYMLINK_NOFOLLOW 1
#endif /* if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 6, 0) */
#endif /* if defined(LINUX_VERSION_CODE) && defined(KERNEL_VERSION) */

#include "evpl/evpl.h"

#include "linux.h"
#include "linux_common.h"
#include "common/logging.h"
#include "common/format.h"
#include "common/misc.h"
#include "common/macros.h"

/*
 * CHIMERA_VFS_CAP_CLAIM_RANGE registry.
 *
 * A byte-range claim reaches this backend only after the claim core has
 * arbitrated it against every other claim on this node; the real fcntl() adds
 * exactly one thing, visibility to holders OUTSIDE this process (a local
 * application, or another chimera node sharing the same filesystem).
 *
 * The claim wire is file-handle based and carries no open handle, so the
 * projection owns its own descriptors: one per (file handle, claim owner),
 * refcounted by the records standing on it and closed once the last one goes
 * away.  Keying the descriptor on the owner is what makes same-owner upgrades
 * coalesce the way POSIX expects while still letting two distinct owners
 * conflict with each other in the kernel.
 *
 * Open file description locks (F_OFD_*) are used where the host provides them:
 * they belong to the descriptor we hold rather than to the process, so an
 * unrelated close() of the same file elsewhere in the server cannot silently
 * drop them, and each owner's descriptor is a distinct lock owner.  A kernel
 * without them falls back to process-wide record locks, where neither property
 * holds -- the projection is then only an approximation of cross-process
 * visibility, which is all the old CHIMERA_VFS_OP_LOCK wire ever was.
 */

#ifdef F_OFD_SETLK
#define CHIMERA_LINUX_LOCK_GET  F_OFD_GETLK
#define CHIMERA_LINUX_LOCK_SET  F_OFD_SETLK
#define CHIMERA_LINUX_LOCK_SETW F_OFD_SETLKW
#else /* ifdef F_OFD_SETLK */
#define CHIMERA_LINUX_LOCK_GET  F_GETLK
#define CHIMERA_LINUX_LOCK_SET  F_SETLK
#define CHIMERA_LINUX_LOCK_SETW F_SETLKW
#endif /* ifdef F_OFD_SETLK */

struct chimera_linux_range_file {
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                         fh_len;
    uint64_t                         fh_hash;
    struct chimera_claim_owner       owner;
    int                              fd;
    uint32_t                         refcnt;
    struct chimera_linux_range_file *next;
};

/* One granted range record, named by the token the core hands back to us on
 * CHIMERA_VFS_OP_CLAIM_RELEASE.  offset/length are absolute (SEEK_SET) so the
 * unlock reproduces exactly the bytes that were locked; length 0 means to-EOF
 * in the fcntl spelling.  projected == 0 marks a record the host cannot
 * express, which the release must not try to undo. */
struct chimera_linux_range {
    uint64_t                         token;
    struct chimera_linux_range_file *file;
    uint64_t                         offset;
    uint64_t                         length;
    uint8_t                          projected;
    struct chimera_linux_range      *next;
};

struct chimera_linux_shared {
    int                              readdir_verifier;

    pthread_mutex_t                  range_lock;
    struct chimera_linux_range_file *range_files;
    struct chimera_linux_range      *ranges;
    uint64_t                         range_next_token;
};

struct chimera_linux_thread {
    struct evpl                     *evpl;
    struct chimera_linux_shared     *shared;
    struct chimera_linux_mount_table mount_table;
    int                              readdir_verifier;
};

static void *
chimera_linux_init(
    const char                *cfgdata,
    struct prometheus_metrics *metrics)
{
    (void) metrics;
    struct chimera_linux_shared *shared;

    shared = calloc(1, sizeof(*shared));

    pthread_mutex_init(&shared->range_lock, NULL);

    if (cfgdata && cfgdata[0] != '\0') {
        json_error_t json_error;
        json_t      *cfg = json_loads(cfgdata, 0, &json_error);

        if (cfg) {
            json_t *verf = json_object_get(cfg, "readdir_verifier");

            if (json_is_boolean(verf)) {
                shared->readdir_verifier = json_boolean_value(verf);
            }

            json_decref(cfg);
        }
    }

    return shared;
} /* linux_init */ /* linux_init */

static void
chimera_linux_destroy(void *private_data)
{
    struct chimera_linux_shared     *shared = private_data;
    struct chimera_linux_range_file *file;
    struct chimera_linux_range      *range;

    while ((range = shared->ranges)) {
        LL_DELETE(shared->ranges, range);
        free(range);
    }

    while ((file = shared->range_files)) {
        LL_DELETE(shared->range_files, file);
        close(file->fd);
        free(file);
    }

    pthread_mutex_destroy(&shared->range_lock);

    free(shared);
} /* linux_destroy */ /* linux_destroy */

static void *
chimera_linux_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_linux_shared *shared = private_data;
    struct chimera_linux_thread *thread =
        (struct chimera_linux_thread *) calloc(1, sizeof(*thread));

    thread->evpl             = evpl;
    thread->shared           = shared;
    thread->readdir_verifier = shared->readdir_verifier;

    return thread;
} /* linux_thread_init */ /* linux_thread_init */

static void
chimera_linux_thread_destroy(void *private_data)
{
    struct chimera_linux_thread *thread = private_data;

    linux_mount_table_destroy(&thread->mount_table);

    free(thread);
} /* linux_thread_destroy */

/**
 * @brief Apply VFS attribute changes to a Linux filesystem object.
 *
 * Sets one or more attributes on the file or directory identified by
 * @p dirfd and @p path according to the mask in @p attr->va_set_mask.
 * Supported attributes:
 *  - @c CHIMERA_VFS_ATTR_MODE  – file permission bits (fchmodat)
 *  - @c CHIMERA_VFS_ATTR_UID   – owner user ID (fchownat)
 *  - @c CHIMERA_VFS_ATTR_GID   – owner group ID (fchownat)
 *  - @c CHIMERA_VFS_ATTR_SIZE  – file size / truncation (truncate via /proc)
 *  - @c CHIMERA_VFS_ATTR_ATIME – access time (utimensat; CHIMERA_VFS_TIME_NOW sets to current time)
 *  - @c CHIMERA_VFS_ATTR_MTIME – modification time (utimensat; CHIMERA_VFS_TIME_NOW sets to current time)
 *
 * On success the corresponding bits in @p attr->va_set_mask are set to
 * confirm which attributes were applied.
 *
 * @param dirfd  Open directory file descriptor used as the base for relative
 *               path operations (AT_EMPTY_PATH semantics).  May be an O_PATH
 *               descriptor; size changes are handled via /proc/self/fd.
 * @param path   Relative path beneath @p dirfd, or an empty string ("") to
 *               operate directly on @p dirfd itself.
 * @param attr   Pointer to a chimera_vfs_attrs structure.  @c va_set_mask
 *               selects which fields to apply; individual @c va_* fields
 *               carry the desired values.
 *
 * @return 0 on success, or a negative @c errno value on failure.
 */
static inline int
chimera_linux_set_attrs(
    int                       dirfd,
    char                     *path,
    struct chimera_vfs_attrs *attr)
{
    int      rc;
    uint64_t set_mask = attr->va_set_mask;

    /* Mode-only backend: a set-ACL request is down-projected to the equivalent
    * POSIX mode and applied as a chmod (lossy, by design -- see vfs_acl.h). */
    if ((set_mask & CHIMERA_VFS_ATTR_ACL) &&
        !(set_mask & CHIMERA_VFS_ATTR_MODE) && attr->va_acl) {
        attr->va_mode = (attr->va_mode & S_IFMT) |
            chimera_acl_to_mode(attr->va_acl);
        set_mask |= CHIMERA_VFS_ATTR_MODE;
    }

    if (set_mask & CHIMERA_VFS_ATTR_MODE) {
#ifdef HAVE_FCHMODAT_AT_SYMLINK_NOFOLLOW
        // Use fchmodat with AT_SYMLINK_NOFOLLOW on kernels >= 6.6
        rc = fchmodat(dirfd, path, attr->va_mode, AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);
#else  /* ifdef HAVE_FCHMODAT_AT_SYMLINK_NOFOLLOW */
        if (strlen(path) != 0) {
            rc = fchmodat(dirfd, path, attr->va_mode, 0);
        } else {
            // dirfd may be O_PATH; chmod via /proc symlink avoids needing
            // read/write permission on the file (chmod only requires ownership)
            char procpath[64];
            snprintf(procpath, sizeof(procpath), "/proc/self/fd/%d", dirfd);
            rc = chmod(procpath, attr->va_mode);
        }
#endif /* ifdef HAVE_FCHMODAT_AT_SYMLINK_NOFOLLOW */
        if (rc) {
            chimera_linux_error("linux_setattr: fchmod(%o) failed: %s",
                                attr->va_mode, strerror(errno));

            return -errno;
        }

        attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
    }

    if ((set_mask & (CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID)) ==
        (CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID)) {

        rc = fchownat(dirfd, path, attr->va_uid, attr->va_gid,
                      AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

        if (rc) {
            chimera_linux_error("linux_setattr: fchown(%u,%u) failed: %s",
                                attr->va_uid, attr->va_gid, strerror(errno));

            return -errno;
        }

        attr->va_set_mask |= CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
    } else if (set_mask & CHIMERA_VFS_ATTR_UID) {

        rc = fchownat(dirfd, path, attr->va_uid, -1,
                      AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

        if (rc) {
            chimera_linux_error("linux_setattr: fchown(%u,-1) failed: %s",
                                attr->va_uid,
                                strerror(errno));

            return -errno;
        }

        attr->va_set_mask |= CHIMERA_VFS_ATTR_UID;
    } else if (set_mask & CHIMERA_VFS_ATTR_GID) {

        rc = fchownat(dirfd, path, -1, attr->va_gid,
                      AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

        if (rc) {
            chimera_linux_error("linux_setattr: fchown(%u,-1) failed: %s",
                                attr->va_gid,
                                strerror(errno));

            return -errno;
        }

        attr->va_set_mask |= CHIMERA_VFS_ATTR_GID;
    }

    if (set_mask & CHIMERA_VFS_ATTR_SIZE) {
        // A size that does not fit in a signed off_t cannot be set on any
        // backing filesystem (truncate would see it as negative and fail
        // EINVAL).  Report it as "file too large" so NFS returns FBIG rather
        // than INVAL.
        if (attr->va_size > (uint64_t) INT64_MAX) {
            return -EFBIG;
        }

        // Prefer ftruncate: rights bound to the descriptor at open time
        // authorize it (POSIX), regardless of the file's current mode or
        // the impersonated fsuid.  An O_PATH dirfd has no such rights and
        // refuses ftruncate with EBADF; those are the stateless path-based
        // callers, where re-checking DAC via a path truncate through
        // /proc/self/fd is exactly right.
        rc = ftruncate(dirfd, attr->va_size);

        /* EBADF: an O_PATH descriptor.  EINVAL: a descriptor not open for
         * writing -- an NFS4 OPEN with read access carries its UNCHECKED
         * size-0 createattr here through the open's own handle.  Both fall
         * back to the path truncate, whose DAC re-check by mode is exactly
         * SETATTR's rule. */
        if (rc && (errno == EBADF || errno == EINVAL)) {
            char procpath[64];
            snprintf(procpath, sizeof(procpath), "/proc/self/fd/%d", dirfd);
            rc = truncate(procpath, attr->va_size);
        }

        if (rc) {
            chimera_linux_error("linux_setattr: truncate(%ld) failed: %s",
                                attr->va_size,
                                strerror(errno));

            return -errno;
        }

        attr->va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
    }

    if (set_mask & (CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME)) {
        struct timespec times[2];
        int             have_any = 0;

        if (set_mask & CHIMERA_VFS_ATTR_ATIME) {
            if (attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
                times[0].tv_nsec = UTIME_NOW;
                have_any         = 1;
            } else if (attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_OMIT) {
                times[0].tv_nsec = UTIME_OMIT;
            } else {
                times[0] = attr->va_atime;
                have_any = 1;
            }

            attr->va_set_mask |= CHIMERA_VFS_ATTR_ATIME;
        } else {
            times[0].tv_nsec = UTIME_OMIT;
        }

        if (set_mask & CHIMERA_VFS_ATTR_MTIME) {
            if (attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
                times[1].tv_nsec = UTIME_NOW;
                have_any         = 1;
            } else if (attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_OMIT) {
                times[1].tv_nsec = UTIME_OMIT;
            } else {
                times[1] = attr->va_mtime;
                have_any = 1;
            }

            attr->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;
        } else {
            times[1].tv_nsec = UTIME_OMIT;
        }

        if (have_any) {
            rc = utimensat(dirfd, path, times, AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

            if (rc) {
                chimera_linux_error("linux_setattr: utimensat() failed: %s",
                                    strerror(errno));

                return -errno;
            }
        }
    }

    return 0;
} /* chimera_linux_set_attrs */

static void
chimera_linux_getattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int fd;

    fd = (int) request->getattr.handle->vfs_private;

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->getattr.r_attr,
                            fd);

    /* Mode-only backend: synthesise an ACL from the POSIX mode bits when the
     * caller asked for one (lossy, one-way -- see vfs_acl.h).  This makes the
     * SMB security-descriptor path emit a DACL (no S-1-5-88-3 modefromsid ACE),
     * exactly like the engine backends (memfs/cairn/diskfs): under the cthon
     * 'modefromsid' cifs mount, a SD with no modefromsid ACE leaves the client
     * on its mount-default file_mode (0755), so copied binaries stay
     * executable.  Emitting the modefromsid ACE instead conveys the real 0644
     * mode and strips +x over SMB (cthon 'special' could not exec). */
    if ((request->getattr.r_attr.va_req_mask & CHIMERA_VFS_ATTR_ACL) &&
        (request->getattr.r_attr.va_set_mask & CHIMERA_VFS_ATTR_MODE)) {
        static __thread uint8_t scratch[sizeof(struct chimera_acl) +
                                        8 * sizeof(struct chimera_ace)];
        struct chimera_acl     *dst = (struct chimera_acl *) scratch;

        chimera_acl_from_mode(request->getattr.r_attr.va_mode, dst, 8);
        request->getattr.r_attr.va_acl       = dst;
        request->getattr.r_attr.va_set_mask |= CHIMERA_VFS_ATTR_ACL;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_getattr */

static void
chimera_linux_setattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int fd, rc;

    fd = request->setattr.handle->vfs_private;

    rc = chimera_setup_credential(request->cred, request->setattr.set_attr);
    if (rc != 0) {
        request->status = chimera_linux_errno_to_status(rc);
        request->complete(request);
        return;
    }

    rc = chimera_linux_set_attrs(fd, "", request->setattr.set_attr);

    if (rc == -EPERM &&
        chimera_linux_times_now_omit(request->setattr.set_attr)) {
        /* utimensat(2) with one field UTIME_NOW and the other omitted: POSIX
         * grants this to any process with write access, Linux insists on
         * ownership (see linux_common.h).  Settle it by write access, as the
         * engine backends do: a writer gets the change applied with
         * privilege restored, a non-writer the EACCES POSIX prescribes. */
        if (chimera_linux_cred_write_ok(fd, request->cred)) {
            chimera_restore_privilege(request->cred);
            rc = chimera_linux_set_attrs(fd, "", request->setattr.set_attr);
        } else {
            chimera_restore_privilege(request->cred);
            rc = -EACCES;
        }
    } else {
        chimera_restore_privilege(request->cred);
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->setattr.r_post_attr,
                            fd);

    request->status = chimera_linux_errno_to_status(-rc);
    request->complete(request);
} /* linux_setattr */

static void
chimera_linux_mount(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int                       mount_fd, rc;
    struct chimera_vfs_attrs *r_attr;
    char                     *scratch = (char *) request->plugin_data;

    r_attr = &request->mount.r_attr;

    TERM_STR(fullpath,
             request->mount.path,
             request->mount.pathlen,
             scratch);

    mount_fd = open(fullpath, O_DIRECTORY | O_RDONLY);

    if (mount_fd < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }
    rc = linux_get_fh(NULL, /* mount context - compute fsid */
                      mount_fd,
                      fullpath,
                      r_attr->va_fh,
                      &r_attr->va_fh_len);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        close(mount_fd);
        request->complete(request);
        return;
    }

    r_attr->va_set_mask |= CHIMERA_VFS_ATTR_FH;

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            r_attr,
                            mount_fd);

    /* Remember the backing directory's identity so lookups can clamp ".." at
     * the mount root (see linux_lookup_escapes_root). */
    {
        struct chimera_linux_mount_root *root;
        struct stat                      st;

        if (fstat(mount_fd, &st) == 0 &&
            (root = calloc(1, sizeof(*root))) != NULL) {
            root->dev                      = st.st_dev;
            root->ino                      = st.st_ino;
            request->mount.r_mount_private = root;
        }
    }

    request->status = CHIMERA_VFS_OK;

    close(mount_fd);

    request->complete(request);
} /* chimera_linux_lookup_path */

static void
chimera_linux_umount(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    free(request->umount.mount_private);
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_umount */

static void
chimera_linux_lookup_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int   parent_fd, rc;
    char *scratch = (char *) request->plugin_data;

    parent_fd = (int) request->lookup_at.handle->vfs_private;

    TERM_STR(fullname, request->lookup_at.component, request->lookup_at.component_len, scratch);

    /* ".." at the mount root resolves to the root itself. */
    if (linux_lookup_escapes_root(request->mount_private, parent_fd,
                                  fullname)) {
        fullname = ".";
    }

    rc = chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                       request,
                                       &request->lookup_at.r_attr,
                                       parent_fd,
                                       fullname);

    if (rc) {
        /* A LOOKUP whose current filehandle is a symlink must report
         * NFS4ERR_SYMLINK, not NFS4ERR_NOTDIR (RFC 7530 16.15.5).  open_fh's
         * O_DIRECTORY probe catches this on a cache miss, but when the symlink's
         * handle is already cached (opened for an earlier op) open_fh is skipped
         * and we land here with parent_fd as the symlink itself -- so the name
         * resolution returns ENOTDIR.  Distinguish a symlink parent via fstat. */
        if (rc == CHIMERA_VFS_ENOTDIR) {
            struct stat pst;
            if (fstat(parent_fd, &pst) == 0 && S_ISLNK(pst.st_mode)) {
                rc = CHIMERA_VFS_ESYMLINK;
            }
        }
        request->status = rc;
        request->complete(request);
        return;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->lookup_at.r_dir_attr,
                            parent_fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* linux_lookup_at */

static void
chimera_linux_readdir(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread *thread = private_data;
    int                          fd, dup_fd, rc;
    DIR                         *dir;
    struct dirent               *dirent;
    struct chimera_vfs_attrs     vattr;
    int                          eof = 1;

    fd = request->readdir.handle->vfs_private;

    chimera_linux_debug("linux_readdir: opening %d", fd);

    /* No credential impersonation here, deliberately: READDIR acts purely
     * through an open handle the engine already authorized, and POSIX binds
     * a directory stream's rights at opendir -- a chmod after that must not
     * break an open stream.  The per-request "." re-open below (a private
     * cursor over the same object, no path resolution) and the child statx
     * would otherwise re-check DAC against the current mode.  Stateless
     * wire callers still face per-operation DAC where it belongs: at the
     * cred-keyed open of the handle itself. */

    if (thread->readdir_verifier) {
        struct stat st;

        rc = fstat(fd, &st);

        if (rc == 0) {
            uint64_t mtime_verf = chimera_linux_mtime_to_verifier(&st);

            if (request->readdir.verifier &&
                request->readdir.verifier != mtime_verf) {
                request->status = CHIMERA_VFS_EBADCOOKIE;
                request->complete(request);
                return;
            }

            request->readdir.r_verifier = mtime_verf;
        }
    }

    dup_fd = openat(fd, ".", O_RDONLY | O_DIRECTORY);

    if (dup_fd < 0) {
        struct stat dead_st;
        int         open_errno = errno;

        /* A handle names an object, not a path: when the directory behind
         * this handle is gone (removed since the handle was minted), the
         * answer is ESTALE whatever errno the kernel chose for the re-open
         * -- kernels disagree across versions (ENOENT historically; newer
         * ones answer differently, which surfaced as SERVERFAULT from the
         * unmapped-errno fallback on ubuntu26).  The portable test is the
         * fd itself: it stays fstat-able on a deleted directory, with a
         * zero link count. */
        if (fstat(fd, &dead_st) == 0 && dead_st.st_nlink == 0) {
            open_errno = ESTALE;
        }
        chimera_linux_error("linux_readdir: openat() failed: %s",
                            strerror(open_errno));
        request->status = chimera_linux_errno_to_status(open_errno);
        request->complete(request);
        return;
    }

    dir = fdopendir(dup_fd);

    if (!dir) {
        chimera_linux_error("linux_readdir: fdopendir() failed: %s",
                            strerror(errno));
        close(dup_fd);
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    if (request->readdir.cookie) {
        seekdir(dir, request->readdir.cookie);
    }

    vattr.va_req_mask = request->readdir.attr_mask;

    while ((dirent = readdir(dir))) {

        /* Skip . and .. unless explicitly requested */
        if (!(request->readdir.flags & CHIMERA_VFS_READDIR_EMIT_DOT)) {
            if ((dirent->d_name[0] == '.' && dirent->d_name[1] == '\0') ||
                (dirent->d_name[0] == '.' && dirent->d_name[1] == '.' &&
                 dirent->d_name[2] == '\0')) {
                continue;
            }
        }

        chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                      request,
                                      &vattr,
                                      fd,
                                      dirent->d_name);

        rc = request->readdir.callback(
            dirent->d_ino,
            dirent->d_off,
            dirent->d_name,
            strlen(dirent->d_name),
            &vattr,
            request->proto_private_data);

        if (rc) {
            eof = 0;
            break;
        }

    } /* chimera_linux_readdir */

    request->readdir.r_cookie = telldir(dir);
    request->readdir.r_eof    = eof;

    closedir(dir);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* linux_readdir */ /* linux_readdir */

static int
chimera_linux_set_open_flags(uint32_t in_flags)
{
    int flags = 0;

    if (in_flags & CHIMERA_VFS_OPEN_PATH) {
        flags |= O_PATH;
    } else {
        if ((in_flags & CHIMERA_VFS_OPEN_DIRECTORY) ||
            ((in_flags & CHIMERA_VFS_OPEN_READ_ONLY) &&
             !(in_flags & CHIMERA_VFS_OPEN_WRITE_ONLY))) {
            flags |= O_RDONLY;
        } else {
            flags |= O_RDWR;
        }
        if (in_flags & CHIMERA_VFS_OPEN_CREATE) {
            flags |= O_CREAT;
        }
        if (in_flags & CHIMERA_VFS_OPEN_EXCLUSIVE) {
            flags |= O_EXCL;
        }
    }
    if (in_flags & CHIMERA_VFS_OPEN_DIRECTORY) {
        flags |= O_DIRECTORY;
    }
    if (in_flags & CHIMERA_VFS_OPEN_NOFOLLOW) {
        flags |= O_NOFOLLOW;
    }
    return flags;
} /* chimera_linux_set_open_flags */

static void
chimera_linux_open_fh(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread *thread = private_data;
    int                          flags;
    int                          fd;
    int                          probe_fd;
    struct stat                  st;

    flags = chimera_linux_set_open_flags(request->open_fh.flags);

    fd = linux_open_by_handle(&thread->mount_table,
                              request->fh,
                              request->fh_len,
                              flags);

    if (fd < 0 && errno == EISDIR &&
        !(request->open_fh.flags & (CHIMERA_VFS_OPEN_READ_ONLY |
                                    CHIMERA_VFS_OPEN_WRITE_ONLY))) {
        /* Access-unspecified (INFERRED) open of a directory: the default
         * read-write flags cannot open a directory, but the handle must
         * still be usable -- an NFS3 COMMIT of a directory filehandle
         * (POSIX fsync on a directory descriptor) arrives this way.
         * Re-open read-only; a directory is never writable anyway. */
        flags = (flags & ~O_ACCMODE) | O_RDONLY | O_DIRECTORY;
        fd    = linux_open_by_handle(&thread->mount_table,
                                     request->fh,
                                     request->fh_len,
                                     flags);
    }

    if (fd < 0) {
        if (errno == ENOTDIR && (request->open_fh.flags & CHIMERA_VFS_OPEN_DIRECTORY)) {
            probe_fd = linux_open_by_handle(&thread->mount_table,
                                            request->fh,
                                            request->fh_len,
                                            O_PATH | O_NOFOLLOW);

            if (probe_fd >= 0) {
                if (fstat(probe_fd, &st) == 0 && S_ISLNK(st.st_mode)) {
                    request->status = CHIMERA_VFS_ESYMLINK;
                } else {
                    request->status = CHIMERA_VFS_ENOTDIR;
                }
                close(probe_fd);
            } else {
                request->status = CHIMERA_VFS_ENOTDIR;
            }
        } else {
            request->status = chimera_linux_handle_open_status(errno);
        }
        request->complete(request);
        return;
    }

    request->open_fh.r_vfs_private = fd;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* linux_open */

static void
chimera_linux_open_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int      parent_fd, fd, flags, rc;
    uint32_t mode;
    int      have_mode = 0;
    char    *scratch   = (char *) request->plugin_data;

    TERM_STR(fullname, request->open_at.name, request->open_at.namelen, scratch);

    parent_fd = request->open_at.handle->vfs_private;

    if (request->open_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        mode                                    = request->open_at.set_attr->va_mode;
        have_mode                               = 1;
        request->open_at.set_attr->va_set_mask &= ~CHIMERA_VFS_ATTR_MODE;
        /* The explicit mode -- applied atomically by the openat() below -- is
         * authoritative on this mode-only backend.  An SMB create SD commonly
         * carries BOTH the modefromsid mode (recovered into va_mode) and a
         * Windows DACL; if we left va_acl set, chimera_linux_set_attrs() would
         * run its ACL->mode projection (now that ATTR_MODE is cleared) and
         * overwrite the requested mode with a lossy ACL-derived one -- e.g. a
         * 0755 binary copied over CIFS would lose +x and could no longer be
         * executed.  The ACL cannot be stored here anyway, so drop it. */
        request->open_at.set_attr->va_set_mask &= ~CHIMERA_VFS_ATTR_ACL;
    } else {
        /* No mode supplied -- an NFS3 EXCLUSIVE create defers the mode to the
         * client's follow-up SETATTR.  Use 0644, matching the memfs/cairn
         * backends (and the model): with per-op DAC now enforced, a divergent
         * initial mode (0600) would spuriously deny another user the read/write
         * access the reference backends grant on the same object before its
         * mode is set.  (Security note: this is a brief, pre-SETATTR window,
         * consistent with memfs's long-standing behavior.) */
        mode = 0644;
    }

    flags = chimera_linux_set_open_flags(request->open_at.flags);

    rc = chimera_setup_credential(request->cred, request->open_at.set_attr);
    if (rc != 0) {
        request->status = chimera_linux_errno_to_status(rc);
        request->complete(request);
        return;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->open_at.r_dir_pre_attr,
                            parent_fd);

    /* Detect whether this open created the file (vs opened an existing one) so
     * the SMB server can report the correct create_action (FILE_CREATED vs
     * FILE_OPENED / OVERWRITTEN / SUPERSEDED).  openat() does not report this,
     * so for a non-exclusive create probe with O_EXCL first: success means we
     * created the file, EEXIST means it already existed and we re-open without
     * O_EXCL (the original flags still carry O_TRUNC for OVERWRITE_IF/SUPERSEDE). */
    int created  = 0;
    int reopened = 0;

    if ((flags & O_CREAT) && !(flags & O_EXCL)) {
        fd = openat(parent_fd, fullname, flags | O_EXCL, mode);
        if (fd >= 0) {
            created = 1;
        } else if (errno == EEXIST &&
                   (request->open_at.flags & CHIMERA_VFS_OPEN_CREATE_REGULAR)) {
            /* NFS3 UNCHECKED create must yield a regular file.  Resolve the
             * leaf type without a data open first: a non-regular object is
             * never opened (directory -> EISDIR, symlink/socket/fifo ->
             * EEXIST). */
            struct stat est;

            if (fstatat(parent_fd, fullname, &est, AT_SYMLINK_NOFOLLOW) == 0 &&
                !S_ISREG(est.st_mode)) {
                chimera_restore_privilege(request->cred);
                request->status = S_ISDIR(est.st_mode) ? CHIMERA_VFS_EISDIR
                                  : CHIMERA_VFS_EEXIST;
                request->complete(request);
                return;
            }

            /* The descriptor this returns is the handle the VFS caches, and
             * the client's later READ/WRITE run against it -- an O_PATH one
             * cannot serve those at all, which reads back as a write failing
             * on a file that was just created.  fstatat has established the
             * leaf is a regular file, so open it for the access asked for,
             * minus the create and truncate bits an UNCHECKED create must not
             * apply to an object it merely found.  A stateless CREATE does not
             * require write on that object, so a caller without it still gets
             * a metadata-only handle rather than a failed create; the engine's
             * own DAC check is what refuses the write that follows. */
            fd = openat(parent_fd, fullname,
                        (flags & ~(O_CREAT | O_EXCL | O_TRUNC)) | O_NOFOLLOW,
                        0);

            if (fd < 0 && (errno == EACCES || errno == EPERM ||
                           errno == EROFS)) {
                fd = openat(parent_fd, fullname, O_PATH | O_NOFOLLOW, 0);
            }
            reopened = 1;
        } else if (errno == EEXIST) {
            /* The object exists: re-open WITHOUT O_CREAT.  Semantically
             * equivalent for an existing file, and immune to the kernel's
             * fs.protected_regular, which fails an O_CREAT open of another
             * user's existing file in a sticky world-writable directory
             * (EPERM/EACCES) where POSIX open(2) plainly opens it.  The
             * create attributes (mode and friends) apply only to an object
             * this call makes, so the reopen skips them -- applying them
             * here would chmod an existing file the caller may not own. */
            fd = openat(parent_fd, fullname, flags & ~(O_CREAT | O_EXCL),
                        mode);
            reopened = 1;
        }
    } else {
        fd = openat(parent_fd, fullname, flags, mode);
        /* A successful exclusive create (FILE_CREATE -> O_CREAT|O_EXCL) is a
         * freshly created file. */
        if (fd >= 0 && (flags & O_CREAT)) {
            created = 1;
        }
    }

    if (fd < 0 && errno == ELOOP &&
        (request->open_at.flags & CHIMERA_VFS_OPEN_NOFOLLOW) &&
        !(flags & (O_CREAT | O_EXCL))) {
        /* Symlink with O_NOFOLLOW: retry with O_PATH to get a handle */
        fd = openat(parent_fd, fullname, O_PATH | O_NOFOLLOW, 0);
    }

    if (fd < 0) {
        chimera_linux_debug("linux_open_at: openat(%d,%s,%d, 0%o) failed: %s",
                            parent_fd, fullname, flags, mode, strerror(errno));
        chimera_restore_privilege(request->cred);
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    /* An UNCHECKED create that re-opened an existing regular file leaves its
     * attributes untouched -- it found the object, it did not make it. */
    rc = reopened ? 0 :
        chimera_linux_set_attrs(fd, "", request->open_at.set_attr);

    /* openat() applies the requested mode through the process umask, so a
     * freshly created file may be missing bits the client asked for (the
     * client has already applied the caller's own umask, so the backend must
     * honor the mode verbatim).  When an explicit mode was requested on a
     * create, fchmod() it to the exact value -- this also restores the
     * set-user-ID/set-group-ID bits, which openat()'s mode argument does not
     * reliably carry. */
    if (rc >= 0 && have_mode && (flags & O_CREAT) && !reopened) {
        if (fchmod(fd, mode & 07777) < 0) {
            rc = -errno;
        }
    }

    chimera_restore_privilege(request->cred);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(-rc);
        request->complete(request);
        return;
    }

    request->open_at.r_vfs_private = fd;
    request->open_at.r_created     = created;

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->open_at.r_dir_post_attr,
                            parent_fd);

    chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                  request,
                                  &request->open_at.r_attr,
                                  parent_fd,
                                  fullname);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* linux_open_at */

static void
chimera_linux_close(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int fd = request->close.vfs_private;

    close(fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_close */

static void
chimera_linux_mkdir_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int                       fd, rc;
    char                     *scratch  = (char *) request->plugin_data;
    struct chimera_vfs_attrs *set_attr = request->mkdir_at.set_attr;
    uint32_t                  mode;

    TERM_STR(fullname, request->mkdir_at.name, request->mkdir_at.name_len, scratch);

    fd = request->mkdir_at.handle->vfs_private;

    if (set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        mode = set_attr->va_mode;
    } else {
        mode = S_IRWXU;
    }

    rc = chimera_setup_credential(request->cred, set_attr);
    if (rc != 0) {
        request->status = chimera_linux_errno_to_status(rc);
        request->complete(request);
        return;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->mkdir_at.r_dir_pre_attr,
                            fd);

    rc = mkdirat(fd, fullname, mode);

    int mkdirat_errno = errno;

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->mkdir_at.r_dir_post_attr,
                            fd);

    if (rc < 0) {
        chimera_restore_privilege(request->cred);
        if (mkdirat_errno == EEXIST) {
            chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                          request,
                                          &request->mkdir_at.r_attr,
                                          fd,
                                          fullname);
        }

        request->status = chimera_linux_errno_to_status(mkdirat_errno);
        request->complete(request);
        return;
    }

    rc = chimera_linux_set_attrs(fd, fullname, request->mkdir_at.set_attr);
    chimera_restore_privilege(request->cred);
    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(-rc);
        request->complete(request);
        return;
    }

    chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                  request,
                                  &request->mkdir_at.r_attr,
                                  fd,
                                  fullname);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_mkdir_at */

static void
chimera_linux_mknod_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int      fd, rc;
    char    *scratch = (char *) request->plugin_data;
    uint32_t mode;
    dev_t    dev = 0;

    TERM_STR(fullname, request->mknod_at.name, request->mknod_at.name_len, scratch);

    fd = request->mknod_at.handle->vfs_private;

    if (request->mknod_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        mode = request->mknod_at.set_attr->va_mode;
    } else {
        mode = S_IFREG | 0644;
    }

    if (request->mknod_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_RDEV) {
        /* va_rdev is the canonical VFS encoding (major << 32 | minor), as
         * produced by the NFS server from CREATE specdata and by statx getattr
         * here.  Convert it back to a host dev_t for mknodat(). */
        dev = makedev(request->mknod_at.set_attr->va_rdev >> 32,
                      request->mknod_at.set_attr->va_rdev & 0xFFFFFFFF);
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->mknod_at.r_dir_pre_attr,
                            fd);

    rc = chimera_setup_credential(request->cred, request->mknod_at.set_attr);
    if (rc != 0) {
        request->status = chimera_linux_errno_to_status(rc);
        request->complete(request);
        return;
    }

    rc = mknodat(fd, fullname, mode, dev);

    int mknodat_errno = errno;

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->mknod_at.r_dir_post_attr,
                            fd);

    if (rc < 0) {
        chimera_restore_privilege(request->cred);

        if (mknodat_errno == EEXIST) {
            chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                          request,
                                          &request->mknod_at.r_attr,
                                          fd,
                                          fullname);
        }

        request->status = chimera_linux_errno_to_status(mknodat_errno);
        request->complete(request);
        return;
    }

    rc = chimera_linux_set_attrs(fd, fullname, request->mknod_at.set_attr);
    chimera_restore_privilege(request->cred);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                  request,
                                  &request->mknod_at.r_attr,
                                  fd,
                                  fullname);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_mknod_at */

static void
chimera_linux_remove_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int   fd, rc;
    char *scratch = (char *) request->plugin_data;

    TERM_STR(fullname, request->remove_at.name, request->remove_at.namelen, scratch);

    fd = request->remove_at.handle->vfs_private;

    /* Honor the caller's requested attrs; in particular notify
     * dispatch in vfs_proc_remove_at needs va_mode to distinguish
     * file vs directory removals.  The fstatat happens BEFORE
     * unlinkat below, so the path is still resolvable. */
    chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                  request,
                                  &request->remove_at.r_removed_attr,
                                  fd,
                                  fullname);

    rc = chimera_setup_credential(request->cred, NULL);
    if (rc != 0) {
        request->status = chimera_linux_errno_to_status(rc);
        request->complete(request);
        return;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->remove_at.r_dir_pre_attr,
                            fd);

    /* Use the caller's type assertion to pick unlink vs rmdir: ISDIR ->
     * AT_REMOVEDIR (a non-directory then yields ENOTDIR), ISNOTDIR -> plain
     * unlink (a directory then yields EISDIR).  Neither set keeps the legacy
     * try-file-then-directory fallback. */
    if (request->remove_at.flags & CHIMERA_VFS_REMOVE_ISDIR) {
        rc = unlinkat(fd, fullname, AT_REMOVEDIR);
    } else if (request->remove_at.flags & CHIMERA_VFS_REMOVE_ISNOTDIR) {
        rc = unlinkat(fd, fullname, 0);
    } else {
        rc = unlinkat(fd, fullname, 0);
        if (rc == -1 && errno == EISDIR) {
            rc = unlinkat(fd, fullname, AT_REMOVEDIR);
        }
    }

    int unlinkat_errno = errno;
    chimera_restore_privilege(request->cred);

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->remove_at.r_dir_post_attr,
                            fd);

    if (rc) {
        request->status = chimera_linux_errno_to_status(unlinkat_errno);
    } else {
        request->status = CHIMERA_VFS_OK;
    }

    request->complete(request);
} /* chimera_linux_remove_at */

static void
chimera_linux_read(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread *thread = private_data;
    int                          fd, i;
    ssize_t                      len, left = request->read.length;
    struct iovec                *iov;
    struct stat                  st;

    (void) thread;

    /* Handle 0-byte reads specially - preadv with uninitialized iov causes EFAULT */
    if (request->read.length == 0) {
        fd = (int) request->read.handle->vfs_private;
        chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX, &request->read.r_attr, fd);
        request->read.r_niov   = 0;
        request->read.r_length = 0;
        request->read.r_eof    = 0;
        request->status        = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* The VFS core allocated the read buffers on the connection thread (linux
     * does not advertise CAP_READ_PROVIDES_BUFFERS) and placed them in
     * request->read.iov, padded to a 4 KiB boundary on both sides.  Build the
     * preadv vector from them: offset the first buffer by aligned_prefix so
     * file offset `offset` lands where the VFS core trims to on completion, and
     * cap the vector at the requested length.  The VFS core owns the buffers --
     * linux neither allocates nor releases them. */
    chimera_vfs_abort_if(request->read.buffers_provided == 0,
                         "linux read dispatched without VFS-provided buffers");

    iov = request->plugin_data;

    for (i = 0; left && i < request->read.buffers_provided; i++) {

        iov[i].iov_base = request->read.iov[i].data;
        iov[i].iov_len  = request->read.iov[i].length;

        if (i == 0) {
            iov[i].iov_base = (char *) iov[i].iov_base + request->read.aligned_prefix;
            iov[i].iov_len -= request->read.aligned_prefix;
        }

        if (iov[i].iov_len > (size_t) left) {
            iov[i].iov_len = left;
        }

        left -= iov[i].iov_len;
    }

    fd = (int) request->read.handle->vfs_private;

    len = preadv(fd,
                 iov,
                 i,
                 request->read.offset);

    if (len < 0) {
        /* Reading at/after EOF surfaces as EINVAL on some backends; report it
         * as a clean zero-length EOF read.  Regular files only: a directory
         * read must keep its real errno (EISDIR), not become a clean EOF just
         * because the offset exceeds the directory's nominal size.  The VFS
         * core owns request->read.iov and releases it on completion (r_length
         * stays 0 here). */
        if (errno == EINVAL && fstat(fd, &st) == 0) {
            if (S_ISREG(st.st_mode) &&
                request->read.offset >= (uint64_t) st.st_size) {
                chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                        &request->read.r_attr, fd);

                request->read.r_length = 0;
                request->read.r_eof    = 1;
                request->status        = CHIMERA_VFS_OK;
                request->complete(request);
                return;
            }
            /* POSIX read(2): a directory descriptor answers EISDIR whatever
             * else is also wrong with the request (the kernel checks offset
             * validity first and can answer EINVAL for a huge offset). */
            if (S_ISDIR(st.st_mode)) {
                errno = EISDIR;
            }
        }

        request->status        = chimera_linux_errno_to_status(errno);
        request->read.r_length = 0;
        request->read.r_eof    = 0;
        request->complete(request);
        return;
    }

    if (fstat(fd, &st) == 0) {
        if (request->read.r_attr.va_req_mask & CHIMERA_VFS_ATTR_MASK_STAT) {
            chimera_linux_stat_to_attr(&request->read.r_attr, &st);
        }

        request->read.r_eof = (request->read.offset + len >= (uint64_t) st.st_size);
    } else {
        chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX, &request->read.r_attr, fd);

        /* fstat() failed: we cannot compare against the file size, so report
         * EOF only when the read returned nothing for a non-zero request -- a
         * short read is NOT end-of-file (RFC 1813 §3.3.6). */
        request->read.r_eof = (len == 0 && request->read.length > 0);
    }

    request->read.r_length = len;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_linux_read */

static void
chimera_linux_write(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int           fd, i, niov = 0, flags = 0;
    uint32_t      left, chunk;
    ssize_t       len;
    struct iovec *iov;

    request->write.r_sync = request->write.sync;

    iov = request->plugin_data;

    left = request->write.length;
    for (i = 0; left && i < request->write.niov; i++) {
        if (request->write.iov[i].length <= left) {
            chunk = request->write.iov[i].length;
        } else {
            chunk = left;
        }
        iov[i].iov_base = request->write.iov[i].data;
        iov[i].iov_len  = chunk;
        left           -= chunk;
        niov++;
    }

    fd = (int) request->write.handle->vfs_private;

    if (request->write.sync) {
        flags = RWF_SYNC;
    }

    len = pwritev2(fd,
                   iov,
                   niov,
                   request->write.offset,
                   flags);

    /* Note: Write iovecs are NOT released here. They were allocated on the
     * server thread and must be released there. The server's write completion
     * callback handles the release after this request completes via doorbell.
     */

    if (len < 0) {
        request->status         = chimera_linux_errno_to_status(errno);
        request->write.r_length = 0;
        request->complete(request);
        return;
    }

    request->write.r_length = len;

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX, &request->write.r_post_attr, fd);

    /* POSIX kill-priv: a non-privileged write to a regular file clears the
     * set-user-ID bit and the set-group-ID bit (when group-executable).  The
     * server runs with CAP_FSETID, so the host kernel does NOT do this for us
     * (the write is issued under the server identity, not the caller's), so we
     * apply it explicitly against the caller's credential. */
    if (request->write.r_post_attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        uint32_t new_mode = chimera_vfs_killpriv_mode(request->cred,
                                                      request->write.r_post_attr.va_mode);

        if (new_mode != request->write.r_post_attr.va_mode) {
            if (fchmod(fd, new_mode & 07777) == 0) {
                request->write.r_post_attr.va_mode = new_mode;
            }
        }
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_linux_write */

static void
chimera_linux_commit(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int fd = (int) request->commit.handle->vfs_private;

    /* Propagate a flush failure as NFS3ERR_IO so the client retransmits the
     * UNSTABLE data instead of dropping it (RFC 1813 §3.3.21); discarding the
     * fsync result turns a recoverable error into silent data loss. */
    request->status = (fsync(fd) < 0) ? CHIMERA_VFS_EIO : CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_linux_commit */

static void
chimera_linux_allocate(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int fd   = (int) request->allocate.handle->vfs_private;
    int mode = 0;
    int rc;

    if (request->allocate.flags & CHIMERA_VFS_ALLOCATE_DEALLOCATE) {
        mode = FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE;
    }

    rc = fallocate(fd, mode, request->allocate.offset, request->allocate.length);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX, &request->allocate.r_post_attr, fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_linux_allocate */

static void
chimera_linux_copy_range(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int      src_fd, dst_fd;
    loff_t   src_off, dst_off;
    uint64_t remaining;
    uint64_t copied = 0;
    ssize_t  rc;

    if (request->copy_range.src_handle->vfs_module !=
        request->copy_range.dst_handle->vfs_module) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    src_fd    = (int) request->copy_range.src_handle->vfs_private;
    dst_fd    = (int) request->copy_range.dst_handle->vfs_private;
    src_off   = (loff_t) request->copy_range.src_offset;
    dst_off   = (loff_t) request->copy_range.dst_offset;
    remaining = request->copy_range.length;

    /* Clamp the copy to the source bytes present when the operation starts.
     * The retry loop below otherwise self-feeds on a same-file copy whose
     * destination range extends the source: each chunk grows the file, the
     * next iteration finds fresh bytes, and the total exceeds what a single
     * copy_file_range(2) call -- and the engine backends -- would move. */
    {
        struct stat src_st;

        if (fstat(src_fd, &src_st) == 0) {
            uint64_t avail = (src_st.st_size > src_off) ?
                (uint64_t) (src_st.st_size - src_off) : 0;

            if (remaining > avail) {
                remaining = avail;
            }
        }
    }

    while (remaining > 0) {
        rc = copy_file_range(src_fd, &src_off, dst_fd, &dst_off, remaining, 0);

        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            request->status = chimera_linux_errno_to_status(errno);
            request->complete(request);
            return;
        }

        if (rc == 0) {
            /* EOF on source */
            break;
        }

        copied    += (uint64_t) rc;
        remaining -= (uint64_t) rc;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->copy_range.r_post_attr, dst_fd);

    request->copy_range.r_length = copied;
    request->status              = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_copy_range */

static void
chimera_linux_clone_range(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int                     src_fd, dst_fd;
    struct file_clone_range args;
    int                     rc;

    if (request->clone_range.src_handle->vfs_module !=
        request->clone_range.dst_handle->vfs_module) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    src_fd = (int) request->clone_range.src_handle->vfs_private;
    dst_fd = (int) request->clone_range.dst_handle->vfs_private;

    args.src_fd      = src_fd;
    args.src_offset  = request->clone_range.src_offset;
    args.src_length  = request->clone_range.length;
    args.dest_offset = request->clone_range.dst_offset;

    rc = ioctl(dst_fd, FICLONERANGE, &args);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->clone_range.r_post_attr, dst_fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_clone_range */

static void
chimera_linux_seek(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int   fd = (int) request->seek.handle->vfs_private;
    int   whence;
    off_t result;

    if (request->seek.what == 0) {
        whence = SEEK_DATA;
    } else {
        whence = SEEK_HOLE;
    }

    result = lseek(fd, request->seek.offset, whence);

    if (result < 0) {
        /* No matching data/hole at or after the offset (the offset is at or
         * past EOF, or SEEK_DATA found no more data): host lseek sets ENXIO,
         * which must propagate as NFS4ERR_NXIO / POSIX ENXIO -- not a silent
         * success. */
        request->status = (errno == ENXIO) ? CHIMERA_VFS_ENXIO
                          : chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    request->seek.r_offset = result;
    request->seek.r_eof    = 0;

    /* A SEEK_HOLE that lands at the logical size is the implicit hole at EOF;
     * RFC 7862 §11.4.4 requires sr_eof TRUE there.  SEEK_DATA always lands
     * before EOF, so its eof stays false. */
    if (whence == SEEK_HOLE) {
        struct stat st;
        if (fstat(fd, &st) == 0 && (uint64_t) result >= (uint64_t) st.st_size) {
            request->seek.r_eof = 1;
        }
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_linux_seek */

static void
chimera_linux_symlink_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int                       fd, rc;
    char                     *scratch  = (char *) request->plugin_data;
    struct chimera_vfs_attrs *set_attr = request->symlink_at.set_attr;

    if (request->symlink_at.namelen + request->symlink_at.targetlen + 2 >
        CHIMERA_VFS_PLUGIN_DATA_SIZE) {
        request->status = CHIMERA_VFS_ENAMETOOLONG;
        request->complete(request);
        return;
    }

    TERM_STR(fullname, request->symlink_at.name, request->symlink_at.namelen, scratch);
    TERM_STR(target, request->symlink_at.target, request->symlink_at.targetlen, scratch);

    fd = request->symlink_at.handle->vfs_private;

    /* symlinks do not support chmod, remove mode from attr set mask */
    set_attr->va_set_mask &= ~CHIMERA_VFS_ATTR_MODE;

    rc = chimera_setup_credential(request->cred, set_attr);
    if (rc != 0) {
        request->status = chimera_linux_errno_to_status(rc);
        request->complete(request);
        return;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->symlink_at.r_dir_pre_attr,
                            fd);

    rc = symlinkat(target, fd, fullname);

    if (rc < 0) {
        chimera_restore_privilege(request->cred);
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }
    // Set attributes on the symlink itself if requested.
    rc = chimera_linux_set_attrs(fd, fullname, request->symlink_at.set_attr);
    chimera_restore_privilege(request->cred);
    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(-rc);
        request->complete(request);
        return;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX, &request->symlink_at.r_dir_post_attr, fd);

    linux_get_fh(request->fh, /* use parent's mount_id */
                 fd,
                 fullname,
                 request->symlink_at.r_attr.va_fh,
                 &request->symlink_at.r_attr.va_fh_len);

    chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                  request,
                                  &request->symlink_at.r_attr,
                                  fd,
                                  fullname);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_symlink_at */

static void
chimera_linux_readlink(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int fd, rc;

    fd = request->readlink.handle->vfs_private;

    rc = readlinkat(fd, "", request->readlink.r_target,
                    request->readlink.target_maxlength);

    if (rc < 0) {
        /* Empty-path readlinkat on a handle that is not a symlink fails
         * ENOENT (the empty-path special case exists only for links);
         * POSIX readlink(2) reports EINVAL for a non-symlink. */
        if (errno == ENOENT) {
            struct stat st;

            if (fstat(fd, &st) == 0 && !S_ISLNK(st.st_mode)) {
                errno = EINVAL;
            }
        }
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    request->readlink.r_target_length = rc;

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX, &request->readlink.r_attr, fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_readlink */

static void
chimera_linux_rename_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread *thread = private_data;
    int                          old_fd, new_fd, rc;
    char                        *scratch = (char *) request->plugin_data;

    TERM_STR(fullname, request->rename_at.name, request->rename_at.namelen, scratch);
    TERM_STR(full_newname, request->rename_at.new_name, request->rename_at.new_namelen, scratch);

    old_fd = linux_open_by_handle(&thread->mount_table,
                                  request->fh,
                                  request->fh_len,
                                  O_PATH | O_RDONLY | O_NOFOLLOW);

    if (old_fd < 0) {
        request->status = chimera_linux_handle_open_status(errno);
        request->complete(request);
        return;
    }

    new_fd = linux_open_by_handle(&thread->mount_table,
                                  request->rename_at.new_fh,
                                  request->rename_at.new_fhlen,
                                  O_PATH | O_RDONLY | O_NOFOLLOW);

    if (new_fd < 0) {
        request->status = chimera_linux_handle_open_status(errno);
        request->complete(request);
        close(old_fd);
        return;
    }

    rc = chimera_setup_credential(request->cred, NULL);
    if (rc != 0) {
        close(old_fd);
        close(new_fd);
        request->status = chimera_linux_errno_to_status(rc);
        request->complete(request);
        return;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->rename_at.r_fromdir_pre_attr,
                            old_fd);
    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->rename_at.r_todir_pre_attr,
                            new_fd);

    rc = renameat(old_fd, fullname, new_fd, full_newname);

    int renameat_errno = errno;

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->rename_at.r_fromdir_post_attr,
                            old_fd);
    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->rename_at.r_todir_post_attr,
                            new_fd);
    chimera_restore_privilege(request->cred);

    if (rc < 0 && (renameat_errno == ENOTEMPTY || renameat_errno == EEXIST)) {
        /* When the destination is an ancestor of the source, the kernel asks
         * "may the replaced directory be emptied" before the POSIX type
         * pairing, answering ENOTEMPTY where rename(2) specifies EISDIR for
         * a non-directory moved onto a directory.  Re-derive the type pair
         * (as root: DAC was settled above) and correct that corner. */
        struct stat ost, nst;

        if (fstatat(old_fd, fullname, &ost, AT_SYMLINK_NOFOLLOW) == 0 &&
            fstatat(new_fd, full_newname, &nst, AT_SYMLINK_NOFOLLOW) == 0 &&
            !S_ISDIR(ost.st_mode) && S_ISDIR(nst.st_mode)) {
            renameat_errno = EISDIR;
        }
    }

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(renameat_errno);
    } else {
        request->status = CHIMERA_VFS_OK;
    }

    close(old_fd);
    close(new_fd);

    request->complete(request);
} /* chimera_linux_rename_at */

static void
chimera_linux_link_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread *thread = private_data;
    int                          fd, dir_fd, rc;
    char                        *scratch = (char *) request->plugin_data;

    TERM_STR(fullname, request->link_at.name, request->link_at.namelen, scratch);

    fd = linux_open_by_handle(&thread->mount_table,
                              request->fh,
                              request->fh_len,
                              O_PATH | O_RDONLY | O_NOFOLLOW);

    if (fd < 0) {
        request->status = chimera_linux_handle_open_status(errno);
        request->complete(request);
        return;
    }

    dir_fd = linux_open_by_handle(&thread->mount_table,
                                  request->link_at.dir_fh,
                                  request->link_at.dir_fhlen,
                                  O_PATH | O_RDONLY | O_NOFOLLOW);

    if (dir_fd < 0) {
        close(fd);
        request->status = chimera_linux_handle_open_status(errno);
        request->complete(request);
        return;
    }

    rc = linkat(fd, "", dir_fd, fullname, AT_EMPTY_PATH);

    if (rc < 0) {
        if (errno == EPERM) {
            struct stat st;

            if (fstat(fd, &st) == 0 && S_ISDIR(st.st_mode)) {
                request->status = CHIMERA_VFS_EISDIR;
            } else {
                request->status = CHIMERA_VFS_EPERM;
            }
        } else {
            request->status = chimera_linux_errno_to_status(errno);
        }
    } else {
        request->status = CHIMERA_VFS_OK;
    }

    close(fd);
    close(dir_fd);

    request->complete(request);

} /* chimera_linux_link_at */

/* Look up the descriptor this (file handle, owner) pair locks through, without
 * opening one.  Called with range_lock held; takes no reference. */
static struct chimera_linux_range_file *
chimera_linux_range_file_find(
    struct chimera_linux_shared      *shared,
    const uint8_t                    *fh,
    uint32_t                          fh_len,
    uint64_t                          fh_hash,
    const struct chimera_claim_owner *owner)
{
    struct chimera_linux_range_file *file;

    for (file = shared->range_files; file; file = file->next) {
        if (file->fh_hash == fh_hash && file->fh_len == fh_len &&
            memcmp(file->fh, fh, fh_len) == 0 &&
            chimera_claim_owner_equal(&file->owner, owner)) {
            return file;
        }
    }

    return NULL;
} /* chimera_linux_range_file_find */

/* Find (or open) the descriptor this (file handle, owner) pair locks through
 * and take a reference on it.  Called with range_lock held. */
static struct chimera_linux_range_file *
chimera_linux_range_file_get(
    struct chimera_linux_thread      *thread,
    const uint8_t                    *fh,
    uint32_t                          fh_len,
    uint64_t                          fh_hash,
    const struct chimera_claim_owner *owner)
{
    struct chimera_linux_shared     *shared = thread->shared;
    struct chimera_linux_range_file *file;
    int                              fd;

    file = chimera_linux_range_file_find(shared, fh, fh_len, fh_hash, owner);

    if (file) {
        file->refcnt++;
        return file;
    }

    /* A read lock needs a readable descriptor and a write lock a writable one,
     * so ask for both and settle for read-only on a read-only file. */
    fd = linux_open_by_handle(&thread->mount_table, fh, fh_len, O_RDWR);

    if (fd < 0) {
        fd = linux_open_by_handle(&thread->mount_table, fh, fh_len, O_RDONLY);
    }

    if (fd < 0) {
        return NULL;
    }

    file = calloc(1, sizeof(*file));

    memcpy(file->fh, fh, fh_len);
    file->fh_len  = fh_len;
    file->fh_hash = fh_hash;
    file->owner   = *owner;
    file->fd      = fd;
    file->refcnt  = 1;

    LL_PREPEND(shared->range_files, file);

    return file;
} /* chimera_linux_range_file_get */

/* Drop a reference; the last one closes the descriptor.  No record of ours can
 * still be standing on it at that point, so nothing is unlocked by surprise.
 * Called with range_lock held. */
static void
chimera_linux_range_file_put(
    struct chimera_linux_shared     *shared,
    struct chimera_linux_range_file *file)
{
    if (--file->refcnt) {
        return;
    }

    LL_DELETE(shared->range_files, file);
    close(file->fd);
    free(file);
} /* chimera_linux_range_file_put */

/* Translate a RANGE claim into the flock the host understands.  Returns 0 if
 * the range is projectable, or -1 if it is one the kernel cannot express (a
 * genuine zero-byte range, or one starting past the largest representable
 * offset) and the caller should grant it unprojected. */
static int
chimera_linux_range_to_flock(
    const struct chimera_vfs_request *request,
    struct flock                     *fl)
{
    uint64_t offset = request->claim_acquire.offset;
    uint64_t length = request->claim_acquire.length;

    fl->l_type = request->claim_acquire.exclusive ? F_WRLCK : F_RDLCK;
    fl->l_pid  = 0;

    if (request->claim_acquire.whence == SEEK_END) {
        /* Handed to the kernel untouched so EOF is resolved atomically with
         * the lock.  offset and length are bit-casts of signed values and keep
         * the POSIX flock conventions intact -- l_len 0 is to-EOF here, and a
         * negative l_len runs backwards from l_start. */
        fl->l_whence = SEEK_END;
        fl->l_start  = (off_t) (int64_t) offset;
        fl->l_len    = (off_t) (int64_t) length;
        return 0;
    }

    fl->l_whence = SEEK_SET;
    fl->l_start  = (off_t) offset;

    if (length == UINT64_MAX) {
        fl->l_len = 0;                 /* to-EOF, which fcntl spells as 0 */
    } else if (length == 0) {
        /* A genuine zero-byte range, which fcntl cannot express at all since
         * l_len 0 already means to-EOF.  The core has arbitrated it locally;
         * granting it unprojected beats refusing an SMB zero-byte lock. */
        return -1;
    } else if (offset > (uint64_t) INT64_MAX) {
        return -1;
    } else if (length > (uint64_t) INT64_MAX - offset) {
        fl->l_len = 0;                 /* runs past the last byte an off_t has */
    } else {
        fl->l_len = (off_t) length;
    }

    return 0;
} /* chimera_linux_range_to_flock */

/* Record the absolute bytes a granted lock covers, so the release can undo
 * exactly them.  A SEEK_END lock was resolved by the kernel against the size
 * it saw; re-resolving it at release time -- arbitrarily much later -- would
 * unlock the wrong bytes, so resolve it here instead, while the size is still
 * the one the lock was placed against. */
static void
chimera_linux_range_resolve(
    const struct flock *fl,
    int                 fd,
    uint64_t           *r_offset,
    uint64_t           *r_length)
{
    struct stat st;
    int64_t     base;

    if (fl->l_whence != SEEK_END || fstat(fd, &st) < 0) {
        *r_offset = (uint64_t) fl->l_start;
        *r_length = (uint64_t) fl->l_len;
        return;
    }

    base = (int64_t) st.st_size + (int64_t) fl->l_start;

    if (fl->l_len < 0) {
        base += (int64_t) fl->l_len;
    }

    if (base < 0) {
        base = 0;
    }

    *r_offset = (uint64_t) base;
    *r_length = (fl->l_len < 0)
        ? 0 - (uint64_t) fl->l_len
        : (uint64_t) fl->l_len;
} /* chimera_linux_range_resolve */

/* Describe the holder an F_GETLK found back to the caller.  F_GETLK returns
 * the conflict in SEEK_SET terms whatever whence was asked about. */
static void
chimera_linux_range_report_conflict(
    struct chimera_vfs_request *request,
    const struct flock         *fl)
{
    if (fl->l_type == F_UNLCK) {
        return;
    }

    request->claim_acquire.r_conflict_type = (fl->l_type == F_RDLCK)
        ? CHIMERA_VFS_LOCK_READ
        : CHIMERA_VFS_LOCK_WRITE;
    request->claim_acquire.r_conflict_offset = (uint64_t) fl->l_start;
    /* fcntl reports to-EOF as 0; the claim wire spells it UINT64_MAX. */
    request->claim_acquire.r_conflict_length = (fl->l_len == 0)
        ? UINT64_MAX
        : (uint64_t) fl->l_len;
    /* An OFD holder reports l_pid -1, meaning "no process owns this". */
    request->claim_acquire.r_conflict_pid = (fl->l_pid > 0)
        ? (uint32_t) fl->l_pid
        : 0;
} /* chimera_linux_range_report_conflict */

static void
chimera_linux_claim_acquire(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread     *thread = private_data;
    struct chimera_linux_shared     *shared = thread->shared;
    struct chimera_linux_range_file *file;
    struct chimera_linux_range      *range     = NULL;
    struct flock                     fl        = { 0 };
    int                              projected = 1;
    int                              cmd, rc;

    if (request->claim_acquire.klass != CHIMERA_VFS_CLAIM_KLASS_RANGE) {
        /* This module arbitrates ranges only; it does not declare
         * CHIMERA_VFS_CAP_CLAIM_AGGREGATE. */
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    if (chimera_linux_range_to_flock(request, &fl) < 0) {
        projected = 0;
    }

    if (request->claim_acquire.flags & CHIMERA_VFS_CLAIM_TEST) {
        cmd = CHIMERA_LINUX_LOCK_GET;
    } else if (request->claim_acquire.flags & CHIMERA_VFS_CLAIM_WAIT) {
        cmd = CHIMERA_LINUX_LOCK_SETW;
    } else {
        cmd = CHIMERA_LINUX_LOCK_SET;
    }

    if (!projected) {
        /* Nothing to ask the host about.  A probe sees no conflict; an acquire
         * gets a record that releases as a no-op. */
        if (!(request->claim_acquire.flags & CHIMERA_VFS_CLAIM_TEST)) {
            range = calloc(1, sizeof(*range));

            pthread_mutex_lock(&shared->range_lock);
            range->token = ++shared->range_next_token;
            LL_PREPEND(shared->ranges, range);
            pthread_mutex_unlock(&shared->range_lock);

            request->claim_acquire.r_token   = range->token;
            request->claim_acquire.r_granted = 1;
        }

        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    pthread_mutex_lock(&shared->range_lock);

    file = chimera_linux_range_file_get(thread,
                                        request->fh,
                                        request->fh_len,
                                        request->fh_hash,
                                        &request->claim_acquire.owner);

    rc = file ? 0 : errno;

    pthread_mutex_unlock(&shared->range_lock);

    if (!file) {
        request->status = chimera_linux_errno_to_status(rc);
        request->complete(request);
        return;
    }

    /* Outside the registry lock: F_SETLKW blocks until the contending lock
     * goes away, and every other claim on this module would be stuck behind it
     * -- including the release that would let it through.  The reference taken
     * above keeps file->fd alive meanwhile. */
    rc = fcntl(file->fd, cmd, &fl);

    if (rc < 0) {
        int err = errno;

        if (err == EACCES || err == EAGAIN) {
            /* Held by somebody else: a refusal, not a failure.  Ask who, so
             * the caller can describe the denial. */
            if (chimera_linux_range_to_flock(request, &fl) == 0 &&
                fcntl(file->fd, CHIMERA_LINUX_LOCK_GET, &fl) == 0) {
                chimera_linux_range_report_conflict(request, &fl);
            }
            request->status = CHIMERA_VFS_OK;
        } else {
            request->status = chimera_linux_errno_to_status(err);
        }
    } else if (request->claim_acquire.flags & CHIMERA_VFS_CLAIM_TEST) {
        /* A probe acquires nothing: the answer is the conflict block. */
        chimera_linux_range_report_conflict(request, &fl);
        request->status = CHIMERA_VFS_OK;
    } else {
        range            = calloc(1, sizeof(*range));
        range->file      = file;
        range->projected = 1;

        chimera_linux_range_resolve(&fl, file->fd, &range->offset, &range->length);
    }

    pthread_mutex_lock(&shared->range_lock);

    if (range) {
        range->token = ++shared->range_next_token;
        LL_PREPEND(shared->ranges, range);

        request->claim_acquire.r_token   = range->token;
        request->claim_acquire.r_granted = 1;
        request->status                  = CHIMERA_VFS_OK;
    } else {
        /* Nothing standing on the descriptor from this request. */
        chimera_linux_range_file_put(shared, file);
    }

    pthread_mutex_unlock(&shared->range_lock);

    request->complete(request);
} /* chimera_linux_claim_acquire */

/* Release by GEOMETRY (claim_release.token == 0): the caller never learned the
 * absolute bytes it holds -- a SEEK_END lock is resolved down here and the
 * resolution is never reported back -- so it names the range to drop in exactly
 * the spelling it named the lock, and this side resolves EOF again.  Every
 * record of this owner's that overlaps the resolved range goes, each unlocked
 * over its own bytes so the kernel is left holding precisely what the registry
 * still describes.  Matching nothing is success. */
static void
chimera_linux_claim_release_ranged(
    struct chimera_vfs_request  *request,
    struct chimera_linux_thread *thread)
{
    struct chimera_linux_shared     *shared = thread->shared;
    struct chimera_linux_range_file *file;
    struct chimera_linux_range      *range, *tmp, *matched = NULL;
    struct flock                     fl     = { 0 };
    uint64_t                         offset = request->claim_release.offset;
    uint64_t                         length = request->claim_release.length;
    int                              err    = 0;

    pthread_mutex_lock(&shared->range_lock);

    file = chimera_linux_range_file_find(shared,
                                         request->fh,
                                         request->fh_len,
                                         request->fh_hash,
                                         &request->claim_release.owner);

    if (file) {
        /* Pin it across the syscalls below, which run unlocked. */
        file->refcnt++;
    }

    pthread_mutex_unlock(&shared->range_lock);

    if (!file) {
        /* This owner locks nothing on this file, so there is nothing of ours
         * to drop and no size worth resolving against. */
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    if (request->claim_release.whence == SEEK_END) {
        /* offset and length are bit-casts of the caller's signed l_start and
         * l_len and keep POSIX's conventions: l_len 0 is to-EOF and a negative
         * l_len runs backwards from l_start.  The descriptor is the one the
         * locks were taken through, so its size is the one the kernel would
         * resolve an F_UNLCK against. */
        struct stat st;
        int64_t     start = (int64_t) offset;
        int64_t     len   = (int64_t) length;

        if (fstat(file->fd, &st) < 0) {
            err = errno;
        } else {
            start += (int64_t) st.st_size;

            if (len < 0) {
                start += len;
                len    = -len;
            }

            if (start < 0) {
                err = EINVAL;
            } else {
                offset = (uint64_t) start;
                length = (len == 0) ? UINT64_MAX : (uint64_t) len;
            }
        }
    }

    if (err) {
        pthread_mutex_lock(&shared->range_lock);
        chimera_linux_range_file_put(shared, file);
        pthread_mutex_unlock(&shared->range_lock);

        request->status = chimera_linux_errno_to_status(err);
        request->complete(request);
        return;
    }

    pthread_mutex_lock(&shared->range_lock);

    LL_FOREACH_SAFE(shared->ranges, range, tmp)
    {
        /* One descriptor per (file handle, owner), so having been taken through
         * this one is the fh and chimera_claim_owner_equal() test already. */
        if (range->file != file) {
            continue;
        }

        /* The record keeps fcntl's spelling, where a length of 0 is to-EOF;
         * the overlap test speaks the claim wire's, where UINT64_MAX is. */
        if (!chimera_vfs_claim_range_overlap_i(range->offset,
                                               range->length ? range->length : UINT64_MAX,
                                               offset, length)) {
            continue;
        }

        LL_DELETE(shared->ranges, range);
        LL_PREPEND(matched, range);
    }

    pthread_mutex_unlock(&shared->range_lock);

    /* Outside the registry lock, as every other lock syscall on this module is.
     * F_UNLCK does not block, but the descriptor put below wants the lock and
     * there is no reason to hold it across a syscall at all. */
    LL_FOREACH(matched, range)
    {
        fl.l_type   = F_UNLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start  = (off_t) range->offset;
        fl.l_len    = (off_t) range->length;
        fl.l_pid    = 0;

        fcntl(file->fd, CHIMERA_LINUX_LOCK_SET, &fl);
    }

    pthread_mutex_lock(&shared->range_lock);

    while (matched) {
        range = matched;
        LL_DELETE(matched, range);
        chimera_linux_range_file_put(shared, range->file);
        free(range);
    }

    chimera_linux_range_file_put(shared, file);

    pthread_mutex_unlock(&shared->range_lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_claim_release_ranged */

static void
chimera_linux_claim_release(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread *thread = private_data;
    struct chimera_linux_shared *shared = thread->shared;
    struct chimera_linux_range  *range;
    struct flock                 fl = { 0 };

    /* claim_release.retained is an AGGREGATE downgrade mask; a RANGE record is
     * binding and all-or-nothing, so the release simply drops it. */

    if (request->claim_release.token == 0 &&
        request->claim_release.klass == CHIMERA_VFS_CLAIM_KLASS_RANGE) {
        chimera_linux_claim_release_ranged(request, thread);
        return;
    }

    pthread_mutex_lock(&shared->range_lock);

    for (range = shared->ranges; range; range = range->next) {
        if (range->token == request->claim_release.token) {
            LL_DELETE(shared->ranges, range);
            break;
        }
    }

    pthread_mutex_unlock(&shared->range_lock);

    if (range && range->projected) {
        fl.l_type   = F_UNLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start  = (off_t) range->offset;
        fl.l_len    = (off_t) range->length;
        fl.l_pid    = 0;

        fcntl(range->file->fd, CHIMERA_LINUX_LOCK_SET, &fl);

        pthread_mutex_lock(&shared->range_lock);
        chimera_linux_range_file_put(shared, range->file);
        pthread_mutex_unlock(&shared->range_lock);
    }

    free(range);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_claim_release */

/* Reverse path lookup: given a file's FH, return the parent's FH and
 * the entry name inside the parent.  Used by the notify resolver to
 * walk up from an event's parent dir toward subtree-watched ancestors.
 *
 * Strategy: open the FH O_PATH, fstatat to read its inode number, then
 * openat("..") for the parent and getdents the parent looking for the
 * entry whose d_ino matches.  Hardlinked regular files have multiple
 * names — we return the first match, which is acceptable for change
 * notifications.  Directories have a single parent on Linux so this is
 * unambiguous for the dirs the resolver actually walks.  */
static void
chimera_linux_getparent(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread *thread    = private_data;
    int                          child_fd  = -1;
    int                          parent_fd = -1;
    DIR                         *dir       = NULL;
    struct dirent               *de;
    struct stat                  child_st;
    uint32_t                     parent_fh_len = 0;
    int                          rc;
    int                          found = 0;

    child_fd = linux_open_by_handle(&thread->mount_table,
                                    request->fh, request->fh_len,
                                    O_PATH | O_NOFOLLOW);
    if (child_fd < 0) {
        request->status = chimera_linux_handle_open_status(errno);
        request->complete(request);
        return;
    }

    rc = fstatat(child_fd, "", &child_st, AT_EMPTY_PATH);
    if (rc < 0) {
        close(child_fd);
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    parent_fd = openat(child_fd, "..", O_RDONLY | O_DIRECTORY);
    close(child_fd);
    if (parent_fd < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    /* Encode the parent FH using the child FH's mount_id (same mount). */
    rc = linux_get_fh(request->fh, parent_fd, "",
                      request->getparent.r_parent_fh,
                      &parent_fh_len);
    if (rc < 0) {
        close(parent_fd);
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }
    request->getparent.r_parent_fh_len = (uint16_t) parent_fh_len;

    /* fdopendir takes ownership of parent_fd on success; closedir(dir)
     * below closes it.  On failure we still own the fd and must close
     * it ourselves. */
    dir = fdopendir(parent_fd);
    if (!dir) {
        close(parent_fd);
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    while ((de = readdir(dir)) != NULL) {
        if (de->d_ino != child_st.st_ino) {
            continue;
        }
        if (de->d_name[0] == '.' &&
            (de->d_name[1] == '\0' ||
             (de->d_name[1] == '.' && de->d_name[2] == '\0'))) {
            /* Skip "." and ".." — never the answer we want. */
            continue;
        }
        size_t nlen = strlen(de->d_name);
        if (nlen > sizeof(request->getparent.r_name)) {
            nlen = sizeof(request->getparent.r_name);
        }
        memcpy(request->getparent.r_name, de->d_name, nlen);
        request->getparent.r_name_len = (uint16_t) nlen;
        found                         = 1;
        break;
    }

    closedir(dir);

    if (!found) {
        /* Race: child was unlinked between getparent dispatch and our
         * readdir, or the inode resides under a different name we
         * cannot see (mount namespace boundary, etc.). */
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_getparent */

static void
chimera_linux_get_xattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int     fd      = (int) request->get_xattr.handle->vfs_private;
    char   *scratch = (char *) request->plugin_data;
    ssize_t rc;

    (void) private_data;

    TERM_STR(name, request->get_xattr.name, request->get_xattr.namelen, scratch);

    rc = fgetxattr(fd, name, request->get_xattr.value,
                   request->get_xattr.value_maxlen);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
    } else {
        request->get_xattr.r_value_len = rc;
        request->status                = CHIMERA_VFS_OK;
    }

    request->complete(request);
} /* chimera_linux_get_xattr */

static void
chimera_linux_set_xattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int         fd      = (int) request->set_xattr.handle->vfs_private;
    char       *scratch = (char *) request->plugin_data;
    int         flags   = 0;
    int         rc;
    struct stat st;

    (void) private_data;

    if (request->set_xattr.option == CHIMERA_VFS_XATTR_CREATE) {
        flags = XATTR_CREATE;
    } else if (request->set_xattr.option == CHIMERA_VFS_XATTR_REPLACE) {
        flags = XATTR_REPLACE;
    }

    if (fstat(fd, &st) < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }
    chimera_linux_stat_to_attr(&request->set_xattr.r_pre_attr, &st);

    TERM_STR(name, request->set_xattr.name, request->set_xattr.namelen, scratch);

    rc = fsetxattr(fd, name, request->set_xattr.value,
                   request->set_xattr.value_len, flags);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
    } else if (fstat(fd, &st) < 0) {
        request->status = chimera_linux_errno_to_status(errno);
    } else {
        chimera_linux_stat_to_attr(&request->set_xattr.r_post_attr, &st);
        request->status = CHIMERA_VFS_OK;
    }

    request->complete(request);
} /* chimera_linux_set_xattr */

static void
chimera_linux_list_xattrs(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int     fd = (int) request->list_xattrs.handle->vfs_private;
    ssize_t rc;
    char   *p, *end;

    (void) private_data;

    rc = flistxattr(fd, request->list_xattrs.buffer,
                    request->list_xattrs.max_bytes);
    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }
    if (rc > request->list_xattrs.max_bytes) {
        request->status = CHIMERA_VFS_ERANGE;
        request->complete(request);
        return;
    }

    request->list_xattrs.r_len    = rc;
    request->list_xattrs.r_eof    = 1;
    request->list_xattrs.r_cookie = 0;

    p   = request->list_xattrs.buffer;
    end = p + rc;
    while (p < end) {
        request->list_xattrs.r_count++;
        p += strlen(p) + 1;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_list_xattrs */

static void
chimera_linux_remove_xattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int         fd      = (int) request->remove_xattr.handle->vfs_private;
    char       *scratch = (char *) request->plugin_data;
    int         rc;
    struct stat st;

    (void) private_data;

    if (fstat(fd, &st) < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }
    chimera_linux_stat_to_attr(&request->remove_xattr.r_pre_attr, &st);

    TERM_STR(name, request->remove_xattr.name, request->remove_xattr.namelen, scratch);

    rc = fremovexattr(fd, name);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
    } else if (fstat(fd, &st) < 0) {
        request->status = chimera_linux_errno_to_status(errno);
    } else {
        chimera_linux_stat_to_attr(&request->remove_xattr.r_post_attr, &st);
        request->status = CHIMERA_VFS_OK;
    }

    request->complete(request);
} /* chimera_linux_remove_xattr */

static void
chimera_linux_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            chimera_linux_mount(request, private_data);
            break;
        case CHIMERA_VFS_OP_UMOUNT:
            chimera_linux_umount(request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP_AT:
            chimera_linux_lookup_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            chimera_linux_getattr(request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_FH:
            chimera_linux_open_fh(request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            chimera_linux_open_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            chimera_linux_close(request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR_AT:
            chimera_linux_mkdir_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_MKNOD_AT:
            chimera_linux_mknod_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            chimera_linux_readdir(request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE_AT:
            chimera_linux_remove_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_READ:
            chimera_linux_read(request, private_data);
            break;
        case CHIMERA_VFS_OP_WRITE:
            chimera_linux_write(request, private_data);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            chimera_linux_commit(request, private_data);
            break;
        case CHIMERA_VFS_OP_SYMLINK_AT:
            chimera_linux_symlink_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            chimera_linux_readlink(request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME_AT:
            chimera_linux_rename_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK_AT:
            chimera_linux_link_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            chimera_linux_setattr(request, private_data);
            break;
        case CHIMERA_VFS_OP_ALLOCATE:
            chimera_linux_allocate(request, private_data);
            break;
        case CHIMERA_VFS_OP_COPY_RANGE:
            chimera_linux_copy_range(request, private_data);
            break;
        case CHIMERA_VFS_OP_CLONE_RANGE:
            chimera_linux_clone_range(request, private_data);
            break;
        case CHIMERA_VFS_OP_SEEK:
            chimera_linux_seek(request, private_data);
            break;
        case CHIMERA_VFS_OP_CLAIM_ACQUIRE:
            chimera_linux_claim_acquire(request, private_data);
            break;
        case CHIMERA_VFS_OP_CLAIM_RELEASE:
            chimera_linux_claim_release(request, private_data);
            break;
        case CHIMERA_VFS_OP_GETPARENT:
            chimera_linux_getparent(request, private_data);
            break;
        case CHIMERA_VFS_OP_GET_XATTR:
            chimera_linux_get_xattr(request, private_data);
            break;
        case CHIMERA_VFS_OP_SET_XATTR:
            chimera_linux_set_xattr(request, private_data);
            break;
        case CHIMERA_VFS_OP_LIST_XATTRS:
            chimera_linux_list_xattrs(request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE_XATTR:
            chimera_linux_remove_xattr(request, private_data);
            break;
        default:
            chimera_linux_error("linux_dispatch: unknown operation %d",
                                request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* linux_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_linux = {
    .sdk_version  = CHIMERA_VFS_SDK_VERSION,
    .name         = "linux",
    .fh_magic     = CHIMERA_VFS_FH_MAGIC_LINUX,
    .capabilities = CHIMERA_VFS_CAP_BLOCKING | CHIMERA_VFS_CAP_OPEN_PATH_REQUIRED | CHIMERA_VFS_CAP_OPEN_FILE_REQUIRED |
        CHIMERA_VFS_CAP_FS | CHIMERA_VFS_CAP_FS_RELATIVE_OP | CHIMERA_VFS_CAP_FS_PATH_OP |
        CHIMERA_VFS_CAP_CLAIM_RANGE | CHIMERA_VFS_CAP_RPL |
        CHIMERA_VFS_CAP_COPY_RANGE | CHIMERA_VFS_CAP_CLONE_RANGE |
        CHIMERA_VFS_CAP_DELEGATES_DAC | CHIMERA_VFS_CAP_XATTR
    ,
    .init           = chimera_linux_init,
    .destroy        = chimera_linux_destroy,
    .thread_init    = chimera_linux_thread_init,
    .thread_destroy = chimera_linux_thread_destroy,
    .dispatch       = chimera_linux_dispatch,
};
