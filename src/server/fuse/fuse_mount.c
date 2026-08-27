// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mount.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/uio.h>

#include "fuse_internal.h"
#include "vfs/vfs_procs.h"
#include "vfs/sdk/vfs_attrs.h"

/*
 * Everything here runs synchronously on the caller thread inside the
 * protocol's start() hook, before any channel is armed: resolving the share
 * root walks the VFS with a private event loop (the chimera_server_mount
 * pattern), and the INIT handshake uses plain blocking I/O on the just-
 * mounted /dev/fuse fd.
 */

struct chimera_fuse_resolve_ctx {
    int                    done;
    enum chimera_vfs_error status;
    uint8_t                fh[CHIMERA_VFS_FH_SIZE];
    uint32_t               fh_len;
};

static void
chimera_fuse_resolve_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_fuse_resolve_ctx *ctx = private_data;

    ctx->status = error_code;

    if (error_code == CHIMERA_VFS_OK &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        memcpy(ctx->fh, attr->va_fh, attr->va_fh_len);
        ctx->fh_len = attr->va_fh_len;
    } else if (error_code == CHIMERA_VFS_OK) {
        ctx->status = CHIMERA_VFS_ENOENT;
    }

    ctx->done = 1;
} /* chimera_fuse_resolve_callback */

static int
chimera_fuse_resolve_root(
    struct chimera_fuse_shared *shared,
    struct chimera_fuse_mount  *mount)
{
    struct evpl                    *evpl;
    struct chimera_vfs_thread      *vfs_thread;
    struct chimera_fuse_resolve_ctx ctx = { .done = 0 };
    struct chimera_vfs_cred         cred;
    uint8_t                         root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        root_fh_len;

    chimera_vfs_cred_init_unix(&cred, 0, 0, 0, NULL);

    evpl = evpl_create(NULL);

    vfs_thread = chimera_vfs_thread_init(evpl, shared->vfs);

    chimera_vfs_get_root_fh(root_fh, &root_fh_len);

    chimera_vfs_lookup(vfs_thread, &cred,
                       root_fh, root_fh_len,
                       mount->share_path, strlen(mount->share_path),
                       CHIMERA_VFS_ATTR_FH,
                       CHIMERA_VFS_LOOKUP_FOLLOW,
                       chimera_fuse_resolve_callback, &ctx);

    while (!ctx.done) {
        evpl_continue(evpl);
    }

    chimera_vfs_thread_destroy(vfs_thread);

    evpl_destroy(evpl);

    if (ctx.status != CHIMERA_VFS_OK) {
        chimera_fuse_error("fuse mount %s: cannot resolve share path %s (error %d)",
                           mount->mountpoint, mount->share_path, ctx.status);
        return -1;
    }

    memcpy(mount->root_fh, ctx.fh, ctx.fh_len);
    mount->root_fh_len = ctx.fh_len;

    return 0;
} /* chimera_fuse_resolve_root */

static int
chimera_fuse_kernel_mount(
    struct chimera_fuse_mount *mnt,
    int                        fd)
{
    char opts[256];
    int  rc, retried = 0;
    int  n;

    n = snprintf(opts, sizeof(opts), "fd=%d,rootmode=%o,user_id=%u,group_id=%u",
                 fd, S_IFDIR | 0755, getuid(), getgid());

    if (mnt->default_permissions) {
        n += snprintf(opts + n, sizeof(opts) - n, ",default_permissions");
    }

    if (mnt->allow_other) {
        n += snprintf(opts + n, sizeof(opts) - n, ",allow_other");
    }

    /* n is accumulated so further options can be appended without reordering;
     * nothing reads it past the last one. */
    (void) n;

 again:
    rc = mount("chimera", mnt->mountpoint, "fuse.chimera",
               MS_NOSUID | MS_NODEV, opts);

    if (rc != 0 && !retried &&
        (errno == EBUSY || errno == ENOTCONN || errno == EINVAL)) {
        /* A previous daemon instance crashed and left a dead fuse mount on
         * this mountpoint; detach it and take its place. */
        chimera_fuse_info("fuse mount %s: detaching stale mount and retrying",
                          mnt->mountpoint);
        umount2(mnt->mountpoint, MNT_DETACH);
        retried = 1;
        goto again;
    }

    if (rc != 0) {
        chimera_fuse_error("fuse mount %s: mount failed: %s",
                           mnt->mountpoint, strerror(errno));
        return -1;
    }

    return 0;
} /* chimera_fuse_kernel_mount */

static int
chimera_fuse_init_handshake(
    struct chimera_fuse_mount *mount,
    int                        fd)
{
    uint8_t                      buf[FUSE_MIN_READ_BUFFER];
    const struct fuse_in_header *hdr = (const struct fuse_in_header *) buf;
    const struct fuse_init_in   *in  = (const struct fuse_init_in *) (buf + sizeof(*hdr));
    struct fuse_out_header       ohdr;
    struct fuse_init_out         out;
    struct iovec                 iov[2];
    uint32_t                     want;
    uint64_t                     want2, kernel_flags, agreed;
    ssize_t                      len;

    for (;;) {
        do {
            len = read(fd, buf, sizeof(buf));
        } while (len < 0 && errno == EINTR);

        if (len < (ssize_t) (sizeof(*hdr) + 16)) {
            chimera_fuse_error("fuse mount %s: short INIT read (%zd): %s",
                               mount->mountpoint, len,
                               len < 0 ? strerror(errno) : "truncated");
            return -1;
        }

        if (hdr->opcode != FUSE_INIT) {
            chimera_fuse_error("fuse mount %s: expected INIT, got opcode %u",
                               mount->mountpoint, hdr->opcode);
            return -1;
        }

        if (in->major < 7 ||
            (in->major == 7 && in->minor < CHIMERA_FUSE_MIN_MINOR)) {
            chimera_fuse_error("fuse mount %s: kernel FUSE ABI %u.%u too old (need >= 7.%u)",
                               mount->mountpoint, in->major, in->minor,
                               CHIMERA_FUSE_MIN_MINOR);
            return -1;
        }

        if (in->major > 7) {
            /* Tell the kernel the highest major we speak; it re-sends INIT
             * with that version. */
            memset(&out, 0, sizeof(out));
            out.major = FUSE_KERNEL_VERSION;

            ohdr.len    = sizeof(ohdr) + sizeof(out);
            ohdr.error  = 0;
            ohdr.unique = hdr->unique;

            iov[0].iov_base = &ohdr;
            iov[0].iov_len  = sizeof(ohdr);
            iov[1].iov_base = &out;
            iov[1].iov_len  = sizeof(out);

            if (writev(fd, iov, 2) < 0) {
                chimera_fuse_error("fuse mount %s: INIT downgrade reply failed: %s",
                                   mount->mountpoint, strerror(errno));
                return -1;
            }
            continue;
        }

        break;
    }

    /* AUTO_INVAL_DATA is what makes attribute-only invalidations (the
     * non-blocking kind the grant breaks use, fuse_coherence.c) drop stale
     * DATA too: the kernel revalidates attributes on the next cached read
     * and discards its pages itself when mtime/size moved -- in the
     * reader's own context, where no cross-mount lock cycle is possible. */
    want = FUSE_ASYNC_READ | FUSE_BIG_WRITES | FUSE_PARALLEL_DIROPS |
        FUSE_DO_READDIRPLUS | FUSE_READDIRPLUS_AUTO | FUSE_ASYNC_DIO |
        FUSE_MAX_PAGES | FUSE_POSIX_LOCKS | FUSE_AUTO_INVAL_DATA;

    /* Flags above bit 31 travel in the separate flags2 word, which only
     * exists from ABI 7.36 and only carries meaning when the kernel set
     * FUSE_INIT_EXT; our reply must echo that bit or the kernel ignores
     * flags2 entirely.  Build hosts older than that (ubuntu22's headers, for
     * one) have neither the flag nor the struct members, so the whole
     * high-word exchange is compiled out there and the session negotiates on
     * the 32-bit word alone.  We ask for exactly one high flag: without it, mmap
     * of a file opened FOPEN_DIRECT_IO fails, which would make the
     * direct_io mount option a functional regression rather than a
     * performance trade. */
    want2 = 0;
#ifdef FUSE_DIRECT_IO_ALLOW_MMAP
    want2 |= FUSE_DIRECT_IO_ALLOW_MMAP;
#endif /* ifdef FUSE_DIRECT_IO_ALLOW_MMAP */

    kernel_flags = in->flags;

#ifdef FUSE_INIT_EXT
    if (in->minor >= 36 && (in->flags & FUSE_INIT_EXT)) {
        kernel_flags |= (uint64_t) in->flags2 << 32;
    }
#endif /* ifdef FUSE_INIT_EXT */

    agreed = kernel_flags & (want | want2);

    memset(&out, 0, sizeof(out));

    out.major         = FUSE_KERNEL_VERSION;
    out.minor         = FUSE_KERNEL_MINOR_VERSION;
    out.flags         = (uint32_t) agreed;
    out.max_readahead = in->max_readahead;

#ifdef FUSE_INIT_EXT
    out.flags2 = (uint32_t) (agreed >> 32);

    if (agreed >> 32) {
        out.flags |= FUSE_INIT_EXT;
    }
#endif /* ifdef FUSE_INIT_EXT */

#ifdef FUSE_DIRECT_IO_ALLOW_MMAP
    mount->direct_io_mmap = (agreed & FUSE_DIRECT_IO_ALLOW_MMAP) ? 1 : 0;
#endif /* ifdef FUSE_DIRECT_IO_ALLOW_MMAP */

    if (out.flags & FUSE_MAX_PAGES) {
        mount->max_write = CHIMERA_FUSE_MAX_WRITE;
        out.max_pages    = CHIMERA_FUSE_MAX_WRITE / 4096;
    } else {
        /* Without max_pages the kernel caps a write at 32 pages. */
        mount->max_write = 32 * 4096;
    }

    out.max_write            = mount->max_write;
    out.max_background       = 64;
    out.congestion_threshold = 48;
    out.time_gran            = 1;

    mount->proto_minor = in->minor;

    ohdr.len    = sizeof(ohdr) + sizeof(out);
    ohdr.error  = 0;
    ohdr.unique = hdr->unique;

    iov[0].iov_base = &ohdr;
    iov[0].iov_len  = sizeof(ohdr);
    iov[1].iov_base = &out;
    iov[1].iov_len  = sizeof(out);

    if (writev(fd, iov, 2) < 0) {
        chimera_fuse_error("fuse mount %s: INIT reply failed: %s",
                           mount->mountpoint, strerror(errno));
        return -1;
    }

    chimera_fuse_info(
        "fuse mount %s: negotiated ABI 7.%u max_write %u flags 0x%llx%s%s",
        mount->mountpoint, mount->proto_minor,
        mount->max_write, (unsigned long long) agreed,
        mount->direct_io ? " direct_io" : "",
        (mount->direct_io && !mount->direct_io_mmap) ? " (no mmap)" : "");

    return 0;
} /* chimera_fuse_init_handshake */

int
chimera_fuse_mount_setup(
    struct chimera_fuse_shared *shared,
    struct chimera_fuse_mount  *mount)
{
    int fd, cfd, slot, flags;
    int num_channels;

    if (chimera_fuse_resolve_root(shared, mount) != 0) {
        return -1;
    }

    if (mount->synthetic_fd >= 0) {
        /* Simulated kernel: the caller supplied one end of a socketpair in
         * place of /dev/fuse.  No device to open, nothing to mount -- but the
         * INIT handshake below is byte-identical, because the peer plays the
         * kernel's half of it. */
        fd = mount->synthetic_fd;
    } else {
        fd = open("/dev/fuse", O_RDWR | O_CLOEXEC);

        if (fd < 0) {
            chimera_fuse_error("fuse mount %s: cannot open /dev/fuse: %s",
                               mount->mountpoint, strerror(errno));
            return -1;
        }

        if (chimera_fuse_kernel_mount(mount, fd) != 0) {
            close(fd);
            return -1;
        }
    }

    if (chimera_fuse_init_handshake(mount, fd) != 0) {
        if (mount->synthetic_fd < 0) {
            umount2(mount->mountpoint, MNT_DETACH);
            close(fd);
        }
        return -1;
    }

    mount->node_table = chimera_fuse_node_table_create();

#if CHIMERA_FUSE_MULTIQUEUE
    num_channels = shared->num_threads;
#else  /* if CHIMERA_FUSE_MULTIQUEUE */
    num_channels = 1;
#endif /* if CHIMERA_FUSE_MULTIQUEUE */

    /* A socketpair has no FUSE_DEV_IOC_CLONE equivalent, so a simulated
     * kernel is single-channel.  That also keeps a simulated session
     * deterministic: every request arrives in submission order. */
    if (mount->synthetic_fd >= 0) {
        num_channels = 1;
    }

    for (slot = 0; slot < CHIMERA_FUSE_MAX_THREADS; slot++) {
        mount->channel_fds[slot] = -1;
    }

    mount->channel_fds[0] = fd;
    mount->num_channels   = 1;

    for (slot = 1; slot < num_channels; slot++) {
        cfd = open("/dev/fuse", O_RDWR | O_CLOEXEC);

        if (cfd < 0 || ioctl(cfd, FUSE_DEV_IOC_CLONE, &fd) != 0) {
            chimera_fuse_error("fuse mount %s: channel clone failed: %s",
                               mount->mountpoint, strerror(errno));
            if (cfd >= 0) {
                close(cfd);
            }
            break;
        }

        mount->channel_fds[slot] = cfd;
        mount->num_channels++;
    }

    for (slot = 0; slot < num_channels; slot++) {
        if (mount->channel_fds[slot] < 0) {
            continue;
        }
        flags = fcntl(mount->channel_fds[slot], F_GETFL);
        fcntl(mount->channel_fds[slot], F_SETFL, flags | O_NONBLOCK);
    }

    mount->mounted = 1;

    chimera_fuse_info("fuse mount %s -> %s ready (%d channel%s)",
                      mount->mountpoint, mount->share_path,
                      mount->num_channels, mount->num_channels == 1 ? "" : "s");

    return 0;
} /* chimera_fuse_mount_setup */

void
chimera_fuse_mount_teardown(struct chimera_fuse_mount *mount)
{
    if (!mount->mounted) {
        return;
    }

    if (mount->synthetic_fd >= 0) {
        /* Nothing is mounted; closing our end is what tells the simulated
         * kernel the session is over. */
        mount->dead = 1;
        return;
    }

    /* Detach rather than plain umount: applications may still hold the
     * mountpoint busy, and the kernel aborts every queued and in-flight
     * request either way (channel reads report ENODEV). */
    if (umount2(mount->mountpoint, MNT_DETACH) != 0) {
        chimera_fuse_error("fuse umount %s: %s",
                           mount->mountpoint, strerror(errno));
    }

    mount->dead = 1;
} /* chimera_fuse_mount_teardown */
