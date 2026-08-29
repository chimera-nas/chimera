/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * In-process FUSE test harness: stands up the chimera FUSE server against a
 * memfs backend over an AF_UNIX SOCK_SEQPACKET socketpair in place of
 * /dev/fuse, and drives it from a hand-rolled FUSE wire client on the same
 * thread.  Nothing mounts anything, needs /dev/fuse, or needs privileges --
 * the same shape as the SMB2 and NFS3 MBT harnesses, which drive those
 * servers over libevpl's inproc transport.
 *
 * WHY A SOCKETPAIR AND NOT AN EVPL BIND.  /dev/fuse is a character device:
 * it supports read/write/readv/writev/poll but none of the socket calls
 * (recvmsg/sendmsg return ENOTSOCK), so libevpl's datagram transports
 * cannot adopt it, and libevpl's inproc transport has no descriptor at all.
 * The descriptor IS the symmetry boundary: everything in the server below
 * mount setup knows only an int fd, so a SOCK_SEQPACKET socketpair -- which
 * preserves message boundaries exactly like /dev/fuse's one-read-one-request
 * contract -- is interchangeable with the real device, including for the
 * notifier thread's blocking writes.
 *
 * WHAT THIS SIMULATES, AND WHAT IT DELIBERATELY DOES NOT.  This is a
 * minimal, CACHE-FREE kernel: no page cache, no dentry cache, no readahead.
 * That is a feature -- every operation maps to a deterministic request
 * sequence, so assertions are exact.  It follows that this harness cannot
 * reproduce kernel-side lock behaviour, and in particular cannot catch the
 * class of deadlock that arises when a kernel invalidation blocks on page
 * or directory locks held by an in-flight request.  It complements the real
 * mount suites (src/server/fuse/tests); it does not replace them.
 *
 * What it CAN see that a real mount cannot: the wire itself.  Reply TTLs,
 * FOPEN flags, the exact invalidation notifications and their ordering
 * against replies, and lookup-count accounting are all directly observable
 * here, where through a real mount they can only be inferred from kernel
 * side effects.
 */

#pragma once

#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/sysmacros.h>
#include <linux/fuse.h>
#include <linux/falloc.h>

#include "server/server.h"
#include "server/fuse/fuse.h"
#include "prometheus-c.h"
#include "common/mbt_artifacts.h"

/* Room for a max_write payload plus headers, matching what the server asks
 * the kernel for. */
#define FUSE_SIM_BUFSZ      (1024 * 1024 + 8192)
#define FUSE_SIM_MAX_NOTIFY 64

/* Inclusive end-of-range sentinel for a lock that runs to end of file.  This
 * is the value a Linux kernel puts on the wire (OFFSET_MAX); the server's own
 * CHIMERA_FUSE_LOCK_EOF must agree with it, which is part of what the harness
 * checks. */
#define FUSE_SIM_LOCK_EOF   0x7fffffffffffffffULL

/* memfs allocates in blocks and only tracks holes at that granularity, so its
 * default 64KB block would swallow every hole the model reasons about (a
 * 4KB-sparse file would report as one solid extent and SEEK_HOLE/SEEK_DATA
 * would answer for the wrong geometry).  Pin it to the harness's abstract
 * block size, exactly as posix_driver.c does, so both harnesses measure the
 * same filesystem. */
#define FUSE_SIM_BLOCK_SIZE 4096

/* One notification the server pushed at us (fuse_out_header.unique == 0):
 * FUSE_NOTIFY_INVAL_INODE, FUSE_NOTIFY_INVAL_ENTRY, ... */
struct fuse_sim_notify {
    int      code;
    uint64_t ino;      /* INVAL_INODE */
    int64_t  off;      /* INVAL_INODE: -1 means attributes only */
    int64_t  len;
    uint64_t parent;   /* INVAL_ENTRY */
    char     name[256];
};

struct fuse_sim {
    struct chimera_server     *server;
    struct prometheus_metrics *metrics;
    int                        fd;          /* our (kernel) end */
    int                        server_fd;   /* handed to the server */
    uint64_t                   unique;
    uint32_t                   proto_minor;
    uint32_t                   max_write;

    /* Credential stamped into every request header.  The model realizes its
     * processes as credential switches, exactly as the POSIX driver does.
     * cur_pid is the model's process id: the FUSE header carries it, and the
     * server keys POSIX lock ownership off it, so distinct model processes
     * must contend for locks the way distinct real ones would. */
    uint32_t                   cur_uid;
    uint32_t                   cur_gid;
    uint32_t                   cur_pid;
    uint32_t                   cur_umask;
    char                       session_dir[256];

    /* Notifications drained while waiting for replies. */
    struct fuse_sim_notify     notify[FUSE_SIM_MAX_NOTIFY];
    int                        num_notify;

    uint8_t                    buf[FUSE_SIM_BUFSZ];
};

/* ------------------------------------------------------------------ */
/* Wire plumbing                                                       */
/* ------------------------------------------------------------------ */

static void
fuse_sim_fail(const char *what)
{
    fprintf(stderr, "fuse_sim: %s: %s\n", what, strerror(errno));
    exit(1);
} /* fuse_sim_fail */

/* Send one request datagram: header + optional fixed body + optional name
 * (NUL-terminated, as the kernel sends it) + optional trailing data. */
static void
fuse_sim_send(
    struct fuse_sim *sim,
    uint32_t         opcode,
    uint64_t         nodeid,
    const void      *body,
    size_t           bodylen,
    const char      *name,
    const void      *data,
    size_t           datalen)
{
    struct fuse_in_header hdr;
    struct iovec          iov[4];
    int                   niov    = 0;
    size_t                namelen = name ? strlen(name) + 1 : 0;

    memset(&hdr, 0, sizeof(hdr));
    hdr.len    = sizeof(hdr) + bodylen + namelen + datalen;
    hdr.opcode = opcode;
    hdr.unique = ++sim->unique;
    hdr.nodeid = nodeid;
    hdr.uid    = sim->cur_uid;
    hdr.gid    = sim->cur_gid;
    hdr.pid    = sim->cur_pid ? sim->cur_pid : (uint32_t) getpid();

    iov[niov].iov_base = &hdr;
    iov[niov].iov_len  = sizeof(hdr);
    niov++;

    if (bodylen) {
        iov[niov].iov_base = (void *) body;
        iov[niov].iov_len  = bodylen;
        niov++;
    }

    if (namelen) {
        iov[niov].iov_base = (void *) name;
        iov[niov].iov_len  = namelen;
        niov++;
    }

    if (datalen) {
        iov[niov].iov_base = (void *) data;
        iov[niov].iov_len  = datalen;
        niov++;
    }

    if (writev(sim->fd, iov, niov) < 0) {
        fuse_sim_fail("writev request");
    }
} /* fuse_sim_send */

static void
fuse_sim_record_notify(
    struct fuse_sim              *sim,
    const struct fuse_out_header *hdr,
    const uint8_t                *payload,
    size_t                        paylen)
{
    struct fuse_sim_notify *n;

    if (sim->num_notify >= FUSE_SIM_MAX_NOTIFY) {
        return;
    }

    n = &sim->notify[sim->num_notify++];
    memset(n, 0, sizeof(*n));
    n->code = hdr->error;

    if (hdr->error == FUSE_NOTIFY_INVAL_INODE &&
        paylen >= sizeof(struct fuse_notify_inval_inode_out)) {
        const struct fuse_notify_inval_inode_out *o = (const void *) payload;

        n->ino = o->ino;
        n->off = o->off;
        n->len = o->len;
    } else if (hdr->error == FUSE_NOTIFY_INVAL_ENTRY &&
               paylen >= sizeof(struct fuse_notify_inval_entry_out)) {
        const struct fuse_notify_inval_entry_out *o  = (const void *) payload;
        size_t                                    nl = o->namelen;

        n->parent = o->parent;
        if (nl >= sizeof(n->name)) {
            nl = sizeof(n->name) - 1;
        }
        if (paylen >= sizeof(*o) + nl) {
            memcpy(n->name, payload + sizeof(*o), nl);
        }
    }
} /* fuse_sim_record_notify */

/*
 * Read datagrams until the reply to `unique` arrives, recording any
 * notifications that overtake it.  Returns the reply's negated errno (0 on
 * success) and copies the payload out.
 */
static int
fuse_sim_recv(
    struct fuse_sim *sim,
    uint64_t         unique,
    void            *out,
    size_t           outmax,
    size_t          *outlen)
{
    for (;;) {
        struct fuse_out_header *hdr = (struct fuse_out_header *) sim->buf;
        ssize_t                 len;
        size_t                  paylen;

        do {
            len = read(sim->fd, sim->buf, sizeof(sim->buf));
        } while (len < 0 && errno == EINTR);

        if (len < 0) {
            fuse_sim_fail("read reply");
        }

        if (len < (ssize_t) sizeof(*hdr)) {
            fprintf(stderr, "fuse_sim: short reply (%zd)\n", len);
            exit(1);
        }

        paylen = (size_t) len - sizeof(*hdr);

        if (hdr->unique == 0) {
            fuse_sim_record_notify(sim, hdr, sim->buf + sizeof(*hdr), paylen);
            continue;
        }

        if (hdr->unique != unique) {
            fprintf(stderr, "fuse_sim: reply for %llu while awaiting %llu\n",
                    (unsigned long long) hdr->unique,
                    (unsigned long long) unique);
            exit(1);
        }

        if (outlen) {
            *outlen = paylen;
        }

        if (out && paylen) {
            memcpy(out, sim->buf + sizeof(*hdr),
                   paylen < outmax ? paylen : outmax);
        }

        return -hdr->error;
    }
} /* fuse_sim_recv */

/*
 * Consume any notifications the server has pushed but nobody has read yet.
 *
 * This matters because the socketpair standing in for /dev/fuse has a finite
 * buffer, and the server's notifier writes into it.  A real kernel drains
 * continuously on many threads; this kernel only reads while it is waiting
 * for a reply, so between operations the queue can grow.  Left unread it
 * fills, the server's notifier blocks in write(), and under coherence=sync an
 * operation gated on that notification cannot complete -- a deadlock created
 * purely by the reader being slow.  Draining between operations keeps the
 * harness a well-behaved kernel rather than a pathological one.
 *
 * Only notifications (unique == 0) may arrive here; a reply would mean a
 * previous operation was abandoned, which is a harness bug worth failing on.
 */
static void
fuse_sim_drain(struct fuse_sim *sim)
{
    for (;;) {
        struct fuse_out_header *hdr;
        ssize_t                 n;

        n = recv(sim->fd, sim->buf, sizeof(sim->buf), MSG_DONTWAIT);

        if (n <= 0) {
            return;
        }
        if ((size_t) n < sizeof(*hdr)) {
            continue;
        }

        hdr = (struct fuse_out_header *) sim->buf;

        if (hdr->unique != 0) {
            fprintf(stderr,
                    "fuse_sim: stray reply to unique %llu while draining\n",
                    (unsigned long long) hdr->unique);
            continue;
        }

        fuse_sim_record_notify(sim, hdr, sim->buf + sizeof(*hdr),
                               (size_t) n - sizeof(*hdr));
    }
} /* fuse_sim_drain */

/* Send + await, the common case.  Returns 0 or a positive errno. */
static int
fuse_sim_call(
    struct fuse_sim *sim,
    uint32_t         opcode,
    uint64_t         nodeid,
    const void      *body,
    size_t           bodylen,
    const char      *name,
    const void      *data,
    size_t           datalen,
    void            *out,
    size_t           outmax,
    size_t          *outlen)
{
    fuse_sim_send(sim, opcode, nodeid, body, bodylen, name, data, datalen);
    return fuse_sim_recv(sim, sim->unique, out, outmax, outlen);
} /* fuse_sim_call */

/* ------------------------------------------------------------------ */
/* Session bring-up                                                    */
/* ------------------------------------------------------------------ */

/*
 * Start the server and complete INIT.  The INIT request is queued into the
 * socketpair BEFORE the server starts: the server's start() hook performs
 * the handshake synchronously on the calling thread, so the request has to
 * be waiting for it -- exactly as the kernel has it waiting after mount(2).
 */
static void
fuse_sim_open(
    struct fuse_sim *sim,
    const char      *fsname)
{
    struct chimera_server_config *config;
    struct fuse_init_in           init;
    struct fuse_init_out          init_out;
    char                          memfs_cfg[64];
    int                           sv[2];
    size_t                        outlen;
    int                           rc;

    memset(sim, 0, sizeof(*sim));

    snprintf(sim->session_dir, sizeof(sim->session_dir),
             "/tmp/fuse_sim_%d", (int) getpid());
    mkdir(sim->session_dir, 0755);

    if (socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, sv) != 0) {
        fuse_sim_fail("socketpair");
    }

    sim->fd        = sv[0];
    sim->server_fd = sv[1];

    mbt_debug_log_start();

    sim->metrics = prometheus_metrics_create(NULL, NULL, 0);

    config = chimera_server_config_init();
    chimera_server_config_set_state_dir(config, sim->session_dir);
    chimera_server_config_set_fuse_enabled(config, 1);
    snprintf(memfs_cfg, sizeof(memfs_cfg), "{\"block_size\": %d}",
             FUSE_SIM_BLOCK_SIZE);
    chimera_server_config_add_module(config, "memfs", NULL, memfs_cfg);

    sim->server = chimera_server_init(config, sim->metrics);

    if (chimera_server_mkfs(sim->server, "memfs", fsname, NULL) != 0) {
        fprintf(stderr, "fuse_sim: mkfs failed\n");
        exit(1);
    }

    chimera_server_mount(sim->server, "share", "memfs", fsname, NULL);

    if (chimera_server_create_fuse_synthetic_mount(sim->server, "/share",
                                                   sim->server_fd) != 0) {
        fprintf(stderr, "fuse_sim: synthetic mount registration failed\n");
        exit(1);
    }

    /* Queue INIT so the server's synchronous handshake finds it. */
    memset(&init, 0, sizeof(init));
    init.major         = FUSE_KERNEL_VERSION;
    init.minor         = FUSE_KERNEL_MINOR_VERSION;
    init.max_readahead = 128 * 1024;
    init.flags         = FUSE_ASYNC_READ | FUSE_BIG_WRITES |
        FUSE_PARALLEL_DIROPS | FUSE_DO_READDIRPLUS | FUSE_MAX_PAGES |
        FUSE_POSIX_LOCKS | FUSE_AUTO_INVAL_DATA;

    fuse_sim_send(sim, FUSE_INIT, 0, &init, sizeof(init), NULL, NULL, 0);

    chimera_server_start(sim->server);

    rc = fuse_sim_recv(sim, sim->unique, &init_out, sizeof(init_out), &outlen);

    if (rc != 0 || outlen < sizeof(init_out)) {
        fprintf(stderr, "fuse_sim: INIT failed (rc %d, %zu bytes)\n", rc, outlen);
        exit(1);
    }

    sim->proto_minor = init_out.minor;
    sim->max_write   = init_out.max_write;
} /* fuse_sim_open */

static void
fuse_sim_close(struct fuse_sim *sim)
{
    chimera_server_destroy(sim->server);
    close(sim->fd);
    mbt_metrics_dump(sim->metrics);
    prometheus_metrics_destroy(sim->metrics);
} /* fuse_sim_close */

/* ------------------------------------------------------------------ */
/* Minimal kernel-side operations                                      */
/* ------------------------------------------------------------------ */

/* LOOKUP one component under `parent`.  Returns 0 and fills `e`, or errno. */
static int
fuse_sim_lookup(
    struct fuse_sim       *sim,
    uint64_t               parent,
    const char            *name,
    struct fuse_entry_out *e)
{
    size_t outlen;

    return fuse_sim_call(sim, FUSE_LOOKUP, parent, NULL, 0, name, NULL, 0,
                         e, sizeof(*e), &outlen);
} /* fuse_sim_lookup */

static int
fuse_sim_getattr(
    struct fuse_sim      *sim,
    uint64_t              nodeid,
    struct fuse_attr_out *out)
{
    struct fuse_getattr_in in;
    size_t                 outlen;

    memset(&in, 0, sizeof(in));

    return fuse_sim_call(sim, FUSE_GETATTR, nodeid, &in, sizeof(in), NULL,
                         NULL, 0, out, sizeof(*out), &outlen);
} /* fuse_sim_getattr */

/* CREATE: one round trip yielding both an entry and an open handle. */
static int
fuse_sim_create(
    struct fuse_sim       *sim,
    uint64_t               parent,
    const char            *name,
    uint32_t               mode,
    struct fuse_entry_out *e,
    struct fuse_open_out  *o)
{
    struct fuse_create_in in;
    uint8_t               out[sizeof(*e) + sizeof(*o)];
    size_t                outlen;
    int                   rc;

    memset(&in, 0, sizeof(in));
    in.flags = O_RDWR | O_CREAT | O_EXCL;
    in.mode  = mode;
    in.umask = 0;

    outlen = 0;

    rc = fuse_sim_call(sim, FUSE_CREATE, parent, &in, sizeof(in), name,
                       NULL, 0, out, sizeof(out), &outlen);

    if (rc == 0 && outlen >= sizeof(out)) {
        memcpy(e, out, sizeof(*e));
        memcpy(o, out + sizeof(*e), sizeof(*o));
    }

    return rc;
} /* fuse_sim_create */

static int
fuse_sim_open_file(
    struct fuse_sim      *sim,
    uint64_t              nodeid,
    uint32_t              flags,
    struct fuse_open_out *o)
{
    struct fuse_open_in in;
    size_t              outlen;

    memset(&in, 0, sizeof(in));
    in.flags = flags;

    return fuse_sim_call(sim, FUSE_OPEN, nodeid, &in, sizeof(in), NULL,
                         NULL, 0, o, sizeof(*o), &outlen);
} /* fuse_sim_open_file */

static int
fuse_sim_write(
    struct fuse_sim *sim,
    uint64_t         nodeid,
    uint64_t         fh,
    uint64_t         offset,
    const void      *data,
    uint32_t         size,
    uint32_t        *written)
{
    struct fuse_write_in  in;
    struct fuse_write_out out;
    size_t                outlen;
    int                   rc;

    memset(&in, 0, sizeof(in));
    memset(&out, 0, sizeof(out));
    in.fh     = fh;
    in.offset = offset;
    in.size   = size;

    rc = fuse_sim_call(sim, FUSE_WRITE, nodeid, &in, sizeof(in), NULL,
                       data, size, &out, sizeof(out), &outlen);

    if (rc == 0 && written) {
        *written = out.size;
    }

    return rc;
} /* fuse_sim_write */

static int
fuse_sim_read(
    struct fuse_sim *sim,
    uint64_t         nodeid,
    uint64_t         fh,
    uint64_t         offset,
    uint32_t         size,
    void            *out,
    size_t          *outlen)
{
    struct fuse_read_in in;

    memset(&in, 0, sizeof(in));
    in.fh     = fh;
    in.offset = offset;
    in.size   = size;

    return fuse_sim_call(sim, FUSE_READ, nodeid, &in, sizeof(in), NULL,
                         NULL, 0, out, size, outlen);
} /* fuse_sim_read */

static int
fuse_sim_release(
    struct fuse_sim *sim,
    uint64_t         nodeid,
    uint64_t         fh)
{
    struct fuse_release_in in;

    memset(&in, 0, sizeof(in));
    in.fh = fh;

    return fuse_sim_call(sim, FUSE_RELEASE, nodeid, &in, sizeof(in), NULL,
                         NULL, 0, NULL, 0, NULL);
} /* fuse_sim_release */

static int
fuse_sim_unlink(
    struct fuse_sim *sim,
    uint64_t         parent,
    const char      *name)
{
    return fuse_sim_call(sim, FUSE_UNLINK, parent, NULL, 0, name,
                         NULL, 0, NULL, 0, NULL);
} /* fuse_sim_unlink */

/* ---- namespace ---- */

static int
fuse_sim_mkdir(
    struct fuse_sim       *sim,
    uint64_t               parent,
    const char            *name,
    uint32_t               mode,
    uint32_t               umask,
    struct fuse_entry_out *e)
{
    struct fuse_mkdir_in in;
    size_t               outlen = 0;

    memset(&in, 0, sizeof(in));
    in.mode  = mode;
    in.umask = umask;

    return fuse_sim_call(sim, FUSE_MKDIR, parent, &in, sizeof(in), name,
                         NULL, 0, e, sizeof(*e), &outlen);
} /* fuse_sim_mkdir */

static int
fuse_sim_mknod(
    struct fuse_sim       *sim,
    uint64_t               parent,
    const char            *name,
    uint32_t               mode,
    uint32_t               rdev,
    uint32_t               umask,
    struct fuse_entry_out *e)
{
    struct fuse_mknod_in in;
    size_t               outlen = 0;

    memset(&in, 0, sizeof(in));
    in.mode  = mode;
    in.rdev  = rdev;
    in.umask = umask;

    return fuse_sim_call(sim, FUSE_MKNOD, parent, &in, sizeof(in), name,
                         NULL, 0, e, sizeof(*e), &outlen);
} /* fuse_sim_mknod */

static int
fuse_sim_rmdir(
    struct fuse_sim *sim,
    uint64_t         parent,
    const char      *name)
{
    return fuse_sim_call(sim, FUSE_RMDIR, parent, NULL, 0, name,
                         NULL, 0, NULL, 0, NULL);
} /* fuse_sim_rmdir */

/* RENAME carries newdir in the body and BOTH names, NUL-separated. */
static int
fuse_sim_rename(
    struct fuse_sim *sim,
    uint64_t         olddir,
    const char      *oldname,
    uint64_t         newdir,
    const char      *newname)
{
    struct fuse_rename_in in;
    char                  names[512];
    size_t                l1 = strlen(oldname) + 1;
    size_t                l2 = strlen(newname) + 1;

    if (l1 + l2 > sizeof(names)) {
        return ENAMETOOLONG;
    }

    memset(&in, 0, sizeof(in));
    in.newdir = newdir;

    memcpy(names, oldname, l1);
    memcpy(names + l1, newname, l2);

    /* Pass the pair as trailing data: fuse_sim_send NUL-terminates `name`,
     * which is wrong for a two-name request. */
    return fuse_sim_call(sim, FUSE_RENAME, olddir, &in, sizeof(in), NULL,
                         names, l1 + l2, NULL, 0, NULL);
} /* fuse_sim_rename */

static int
fuse_sim_link(
    struct fuse_sim       *sim,
    uint64_t               oldnodeid,
    uint64_t               newparent,
    const char            *newname,
    struct fuse_entry_out *e)
{
    struct fuse_link_in in;
    size_t              outlen = 0;

    memset(&in, 0, sizeof(in));
    in.oldnodeid = oldnodeid;

    return fuse_sim_call(sim, FUSE_LINK, newparent, &in, sizeof(in), newname,
                         NULL, 0, e, sizeof(*e), &outlen);
} /* fuse_sim_link */

/* SYMLINK carries name and target, both NUL-terminated, no fixed body. */
static int
fuse_sim_symlink(
    struct fuse_sim       *sim,
    uint64_t               parent,
    const char            *name,
    const char            *target,
    struct fuse_entry_out *e)
{
    char   payload[1024];
    size_t l1     = strlen(name) + 1;
    size_t l2     = strlen(target) + 1;
    size_t outlen = 0;

    if (l1 + l2 > sizeof(payload)) {
        return ENAMETOOLONG;
    }

    memcpy(payload, name, l1);
    memcpy(payload + l1, target, l2);

    return fuse_sim_call(sim, FUSE_SYMLINK, parent, NULL, 0, NULL,
                         payload, l1 + l2, e, sizeof(*e), &outlen);
} /* fuse_sim_symlink */

static int
fuse_sim_readlink(
    struct fuse_sim *sim,
    uint64_t         nodeid,
    char            *out,
    size_t           outmax)
{
    size_t outlen = 0;
    int    rc;

    rc = fuse_sim_call(sim, FUSE_READLINK, nodeid, NULL, 0, NULL, NULL, 0,
                       out, outmax - 1, &outlen);

    if (rc == 0) {
        if (outlen >= outmax) {
            outlen = outmax - 1;
        }
        out[outlen] = '\0';
    }

    return rc;
} /* fuse_sim_readlink */

/* ---- attributes ---- */

static int
fuse_sim_setattr(
    struct fuse_sim              *sim,
    uint64_t                      nodeid,
    const struct fuse_setattr_in *in,
    struct fuse_attr_out         *out)
{
    size_t outlen = 0;

    return fuse_sim_call(sim, FUSE_SETATTR, nodeid, in, sizeof(*in), NULL,
                         NULL, 0, out, sizeof(*out), &outlen);
} /* fuse_sim_setattr */

static int
fuse_sim_access(
    struct fuse_sim *sim,
    uint64_t         nodeid,
    uint32_t         mask)
{
    struct fuse_access_in in;

    memset(&in, 0, sizeof(in));
    in.mask = mask;

    return fuse_sim_call(sim, FUSE_ACCESS, nodeid, &in, sizeof(in), NULL,
                         NULL, 0, NULL, 0, NULL);
} /* fuse_sim_access */

/* STATFS is addressed to a node, not the session: statfs(path) and
 * fstatfs(fd) name the object whose filesystem is being asked about, and the
 * object still has to exist and be reachable for the call to succeed. */
static int
fuse_sim_statfs(
    struct fuse_sim        *sim,
    uint64_t                nodeid,
    struct fuse_statfs_out *out)
{
    size_t outlen = 0;

    return fuse_sim_call(sim, FUSE_STATFS, nodeid, NULL, 0, NULL,
                         NULL, 0, out, sizeof(*out), &outlen);
} /* fuse_sim_statfs */

/* ---- directories ---- */

static int
fuse_sim_opendir(
    struct fuse_sim      *sim,
    uint64_t              nodeid,
    struct fuse_open_out *o)
{
    struct fuse_open_in in;
    size_t              outlen = 0;

    memset(&in, 0, sizeof(in));

    return fuse_sim_call(sim, FUSE_OPENDIR, nodeid, &in, sizeof(in), NULL,
                         NULL, 0, o, sizeof(*o), &outlen);
} /* fuse_sim_opendir */

static int
fuse_sim_readdir(
    struct fuse_sim *sim,
    uint64_t         nodeid,
    uint64_t         fh,
    uint64_t         offset,
    void            *out,
    size_t           outmax,
    size_t          *outlen)
{
    struct fuse_read_in in;

    memset(&in, 0, sizeof(in));
    in.fh     = fh;
    in.offset = offset;
    in.size   = (uint32_t) outmax;

    return fuse_sim_call(sim, FUSE_READDIR, nodeid, &in, sizeof(in), NULL,
                         NULL, 0, out, outmax, outlen);
} /* fuse_sim_readdir */

static int
fuse_sim_releasedir(
    struct fuse_sim *sim,
    uint64_t         nodeid,
    uint64_t         fh)
{
    struct fuse_release_in in;

    memset(&in, 0, sizeof(in));
    in.fh = fh;

    return fuse_sim_call(sim, FUSE_RELEASEDIR, nodeid, &in, sizeof(in), NULL,
                         NULL, 0, NULL, 0, NULL);
} /* fuse_sim_releasedir */

/*
 * `owner` is the closing process's lock owner, which the kernel takes from
 * its file table -- the same token its POSIX SETLKs carry.  It is what makes
 * close(2) drop that process's locks on the file (XSH close()): the server
 * has no other way to know whose locks just went away, and a FLUSH with
 * lock_owner 0 silently leaks every one of them.
 */
static int
fuse_sim_flush(
    struct fuse_sim *sim,
    uint64_t         nodeid,
    uint64_t         fh,
    uint64_t         owner)
{
    struct fuse_flush_in in;

    memset(&in, 0, sizeof(in));
    in.fh         = fh;
    in.lock_owner = owner;

    return fuse_sim_call(sim, FUSE_FLUSH, nodeid, &in, sizeof(in), NULL,
                         NULL, 0, NULL, 0, NULL);
} /* fuse_sim_flush */

static int
fuse_sim_fsync(
    struct fuse_sim *sim,
    uint64_t         nodeid,
    uint64_t         fh,
    int              datasync)
{
    struct fuse_fsync_in in;

    memset(&in, 0, sizeof(in));
    in.fh          = fh;
    in.fsync_flags = datasync ? 1 : 0;

    return fuse_sim_call(sim, FUSE_FSYNC, nodeid, &in, sizeof(in), NULL,
                         NULL, 0, NULL, 0, NULL);
} /* fuse_sim_fsync */

/* ---- extents ---- */

/*
 * SEEK_DATA / SEEK_HOLE.  The kernel resolves SEEK_SET/CUR/END itself and
 * only asks the filesystem about the two extent-aware whences, so this
 * mirrors that split: the caller handles the arithmetic ones.
 */
static int
fuse_sim_lseek(
    struct fuse_sim *sim,
    uint64_t         nodeid,
    uint64_t         fh,
    uint64_t         offset,
    uint32_t         whence,
    uint64_t        *out_off)
{
    struct fuse_lseek_in  in;
    struct fuse_lseek_out out;
    size_t                outlen = 0;
    int                   rc;

    memset(&in, 0, sizeof(in));
    memset(&out, 0, sizeof(out));
    in.fh     = fh;
    in.offset = offset;
    in.whence = whence;

    rc = fuse_sim_call(sim, FUSE_LSEEK, nodeid, &in, sizeof(in), NULL,
                       NULL, 0, &out, sizeof(out), &outlen);

    if (rc == 0 && outlen >= sizeof(out)) {
        *out_off = out.offset;
    }

    return rc;
} /* fuse_sim_lseek */

static int
fuse_sim_fallocate(
    struct fuse_sim *sim,
    uint64_t         nodeid,
    uint64_t         fh,
    uint64_t         offset,
    uint64_t         length,
    uint32_t         mode)
{
    struct fuse_fallocate_in in;

    memset(&in, 0, sizeof(in));
    in.fh     = fh;
    in.offset = offset;
    in.length = length;
    in.mode   = mode;

    return fuse_sim_call(sim, FUSE_FALLOCATE, nodeid, &in, sizeof(in), NULL,
                         NULL, 0, NULL, 0, NULL);
} /* fuse_sim_fallocate */

/* Server-side copy between two open files.  Returns bytes copied in *copied. */
static int
fuse_sim_copy_file_range(
    struct fuse_sim *sim,
    uint64_t         nodeid_in,
    uint64_t         fh_in,
    uint64_t         off_in,
    uint64_t         nodeid_out,
    uint64_t         fh_out,
    uint64_t         off_out,
    uint64_t         len,
    uint32_t        *copied)
{
    struct fuse_copy_file_range_in in;
    struct fuse_write_out          out;
    size_t                         outlen = 0;
    int                            rc;

    memset(&in, 0, sizeof(in));
    memset(&out, 0, sizeof(out));

    in.fh_in      = fh_in;
    in.off_in     = off_in;
    in.nodeid_out = nodeid_out;
    in.fh_out     = fh_out;
    in.off_out    = off_out;
    in.len        = len;

    rc = fuse_sim_call(sim, FUSE_COPY_FILE_RANGE, nodeid_in, &in, sizeof(in),
                       NULL, NULL, 0, &out, sizeof(out), &outlen);

    if (rc == 0 && outlen >= sizeof(out)) {
        *copied = out.size;
    }

    return rc;
} /* fuse_sim_copy_file_range */

/* ---- POSIX locks ---- */

/*
 * SETLK / SETLKW / GETLK.  `owner` is the lock owner the kernel would derive
 * from the opening process; the model's processes map onto distinct owners so
 * that contention between them is real.  On GETLK the conflicting lock (or
 * F_UNLCK when there is none) comes back in *conflict.
 */
static int
fuse_sim_lock(
    struct fuse_sim       *sim,
    uint64_t               nodeid,
    uint64_t               fh,
    uint64_t               owner,
    uint32_t               opcode,
    uint32_t               type,
    uint64_t               start,
    uint64_t               end,
    uint32_t               pid,
    struct fuse_file_lock *conflict)
{
    struct fuse_lk_in  in;
    struct fuse_lk_out out;
    size_t             outlen = 0;
    int                rc;

    memset(&in, 0, sizeof(in));
    memset(&out, 0, sizeof(out));

    in.fh       = fh;
    in.owner    = owner;
    in.lk.start = start;
    in.lk.end   = end;
    in.lk.type  = type;
    in.lk.pid   = pid;

    rc = fuse_sim_call(sim, opcode, nodeid, &in, sizeof(in), NULL, NULL, 0,
                       &out, sizeof(out), &outlen);

    if (conflict) {
        if (rc == 0 && outlen >= sizeof(out)) {
            *conflict = out.lk;
        } else {
            memset(conflict, 0, sizeof(*conflict));
            conflict->type = F_UNLCK;
        }
    }

    return rc;
} /* fuse_sim_lock */
