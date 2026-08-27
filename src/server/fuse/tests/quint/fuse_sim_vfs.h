/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * The kernel half of the in-process FUSE harness: path resolution, a
 * descriptor table, directory streams, and the POSIX-shaped operations the
 * model-based POSIX corpus issues, all expressed as FUSE requests over
 * fuse_sim.h.
 *
 * This is deliberately a MINIMAL, CACHE-FREE kernel.  There is no dentry
 * cache, no page cache, and no readahead: every path is resolved by walking
 * LOOKUPs from the root, and every read or write is one request.  The point
 * is determinism -- one POSIX call maps to a fixed request sequence, so a
 * divergence is attributable to the server rather than to kernel caching
 * heuristics.  The cost is that cache-dependent behaviour (and kernel-side
 * lock ordering, the class of bug that produced this branch's two deadlocks)
 * is out of reach here and stays with the real-mount suites.
 *
 * What IS modelled faithfully, because the corpus depends on it:
 *
 *   - Open file descriptions.  dup/dup2/F_DUPFD share one description, so a
 *     seek through one descriptor moves the other -- the model checks this.
 *   - Symlink resolution, with a loop budget, so ELOOP is produced by a real
 *     walk rather than asserted.
 *   - Per-process credentials and umask.  The model's processes are distinct
 *     uid/gid/umask triples and distinct lock owners; the FUSE header carries
 *     uid, gid and pid, so each request is stamped with the issuing process.
 *
 * Known limits of the FUSE protocol itself, which no harness can paper over:
 * fuse_in_header carries a single gid, so supplementary groups cannot be
 * conveyed (a real mount relies on the kernel's default_permissions check for
 * those).  Traces that turn on group membership are therefore expected to
 * diverge here for protocol reasons, not server reasons.
 *
 * Lookup counts: every LOOKUP the kernel would reference-count is counted
 * here too, and released with FORGET at teardown, so the server's
 * lookup-count accounting is exercised rather than bypassed.
 */

#pragma once

#include <sys/stat.h>
#include <stdarg.h>

#include "fuse_sim.h"

#define FSIM_MAX_FDS   64
#define FSIM_MAX_OFDS  64
#define FSIM_MAX_DIRS  64
#define FSIM_MAX_PIDS  4
#define FSIM_MAX_GIDS  8
#define FSIM_MAX_NODES 16384
#define FSIM_SYMLOOP   40
#define FSIM_PATH_MAX  4096
#define FSIM_NAME_MAX  255

/* An open file description: what dup() shares and close() drops a reference
 * to.  The file position lives here, not on the descriptor. */
struct fsim_ofd {
    int      refs;
    int      isdir;
    uint64_t nodeid;
    uint64_t fh;
    uint64_t offset;
    int      flags;
};

/* A directory stream (opendir/readdir/closedir).  The corpus addresses these
 * through their own id space, exactly as posix_driver.c does. */
struct fsim_dir {
    int      used;
    uint64_t nodeid;
    uint64_t fh;
    uint64_t cookie;   /* resume cookie for the next READDIR */
};

/* One model process: its credential, its umask, and (implicitly) its lock
 * ownership. */
struct fsim_proc {
    uint32_t uid;
    uint32_t gid;
    uint32_t umask;

    /* Supplementary groups.  fuse_in_header has no room for them, so the
     * server never sees these -- but the KERNEL has them, and under
     * default_permissions the kernel is what enforces search permission
     * during path resolution.  Keeping them on this side of the wire is
     * what makes that enforcement faithful. */
    uint32_t gids[FSIM_MAX_GIDS];
    int      ngids;
};

struct fsim {
    struct fuse_sim  sim;

    int              fds[FSIM_MAX_FDS];     /* -1 free, else an ofd index */
    struct fsim_ofd  ofds[FSIM_MAX_OFDS];
    struct fsim_dir  dirs[FSIM_MAX_DIRS];
    struct fsim_proc procs[FSIM_MAX_PIDS];
    int              cur_proc;      /* whose credentials requests carry */

    /* Every nodeid we hold a lookup count on, so teardown can FORGET them
     * and the server's accounting is exercised end to end. */
    uint64_t         held[FSIM_MAX_NODES];
    int              num_held;

    /* Filesystem recycling between traces (the "newfs" reset). */
    char             fsname[64];
    int              fs_counter;
};

/* The result of resolving a path: the leaf's parent, the leaf name, and the
 * leaf itself when it exists. */
struct fsim_path {
    uint64_t              parent;
    char                  leaf[FSIM_NAME_MAX + 1];
    uint64_t              nodeid;   /* 0 when the leaf does not exist */
    struct fuse_entry_out entry;    /* valid iff nodeid != 0 */
};

/* Progress tracing for the session-lifecycle paths, which are the ones that
 * can stall with no reply to point at.  Off unless FSIM_DEBUG is set. */
static void
fsim_dbg(
    const char *fmt,
    ...)
{
    static int enabled = -1;
    va_list    ap;

    if (enabled < 0) {
        enabled = getenv("FSIM_DEBUG") != NULL;
    }
    if (!enabled) {
        return;
    }

    va_start(ap, fmt);
    fprintf(stderr, "fsim: ");
    vfprintf(stderr, fmt, ap);
    fflush(stderr);
    va_end(ap);
} /* fsim_dbg */

/* ------------------------------------------------------------------ */
/* Node bookkeeping                                                    */
/* ------------------------------------------------------------------ */

static void
fsim_hold(
    struct fsim *f,
    uint64_t     nodeid)
{
    if (nodeid == 0 || nodeid == FUSE_ROOT_ID) {
        return;
    }
    if (f->num_held < FSIM_MAX_NODES) {
        f->held[f->num_held++] = nodeid;
    }
} /* fsim_hold */

/* Release every lookup count we took, one FORGET per LOOKUP, as a kernel
 * does when it drops the inodes. */
static void
fsim_forget_all(struct fsim *f)
{
    struct fuse_forget_in in;
    int                   i;

    memset(&in, 0, sizeof(in));
    in.nlookup = 1;

    for (i = 0; i < f->num_held; i++) {
        /* FORGET carries no reply. */
        fuse_sim_send(&f->sim, FUSE_FORGET, f->held[i], &in, sizeof(in),
                      NULL, NULL, 0);
    }

    f->num_held = 0;
} /* fsim_forget_all */

/* ------------------------------------------------------------------ */
/* Processes                                                           */
/* ------------------------------------------------------------------ */

/* Stamp the issuing model process onto the session, so every request this
 * operation generates carries its uid, gid, pid and umask. */
static void
fsim_apply_pid(
    struct fsim *f,
    int          pid)
{
    if (pid < 0 || pid >= FSIM_MAX_PIDS) {
        pid = 0;
    }

    f->cur_proc      = pid;
    f->sim.cur_uid   = f->procs[pid].uid;
    f->sim.cur_gid   = f->procs[pid].gid;
    f->sim.cur_umask = f->procs[pid].umask;

    /* pid 0 is a legitimate model process but 0 is not a legal FUSE pid, so
     * shift into a range that cannot collide with the harness's own pid. */
    f->sim.cur_pid = (uint32_t) (1000 + pid);
} /* fsim_apply_pid */

/* ------------------------------------------------------------------ */
/* Path resolution                                                     */
/* ------------------------------------------------------------------ */

/* The replay addresses the filesystem through a fixed mount prefix (see
 * MOUNT in posix_replay.py); the FUSE session's root IS that mount, so the
 * prefix is stripped rather than resolved. */
#define FSIM_MOUNT_PREFIX "/test"

/*
 * Search permission on a directory, decided the way the kernel decides it
 * (XBD 4.5 exclusive class selection): owner class if the uid matches even
 * when its bits deny what another class would grant, else group class on any
 * group match -- supplementary groups included -- else other class.  uid 0
 * searches any directory outright.
 *
 * This lives on the kernel side because that is where it lives for real: the
 * mount runs with default_permissions, so link_path_walk() checks MAY_EXEC on
 * every directory it resolves through before the filesystem is ever asked to
 * look anything up.  The server cannot do this check itself for a caller in a
 * supplementary group -- fuse_in_header carries one gid.
 */
static int
fsim_may_exec(
    const struct fsim      *f,
    const struct fuse_attr *a)
{
    const struct fsim_proc *p = &f->procs[f->cur_proc];
    int                     i;

    if (p->uid == 0) {
        return 1;
    }

    if (a->uid == p->uid) {
        return (a->mode & S_IXUSR) != 0;
    }

    if (a->gid == p->gid) {
        return (a->mode & S_IXGRP) != 0;
    }

    for (i = 0; i < p->ngids; i++) {
        if (a->gid == p->gids[i]) {
            return (a->mode & S_IXGRP) != 0;
        }
    }

    return (a->mode & S_IXOTH) != 0;
} /* fsim_may_exec */


static const char *
fsim_strip_mount(const char *path)
{
    size_t n = sizeof(FSIM_MOUNT_PREFIX) - 1;

    if (path && strncmp(path, FSIM_MOUNT_PREFIX, n) == 0 &&
        (path[n] == '\0' || path[n] == '/')) {
        return path[n] == '\0' ? "/" : path + n;
    }

    return path ? path : "/";
} /* fsim_strip_mount */

/*
 * Walk `path` one component at a time, as the kernel does: a LOOKUP per
 * component, symlinks on intermediate components always followed, the leaf
 * followed only when `follow_leaf` is set (the lstat/readlink distinction).
 *
 * Absolute paths start at the session root; relative paths start at `start`,
 * which is how the *at() forms resolve against a directory descriptor.
 *
 * Returns 0 on a completed walk -- in which case out->nodeid is 0 if the leaf
 * simply does not exist, which is not an error for a caller about to create
 * it -- or the errno the walk produced.
 */
static int
fsim_walk(
    struct fsim      *f,
    uint64_t          start,
    const char       *path_in,
    int               follow_leaf,
    struct fsim_path *out)
{
    struct fuse_entry_out e;
    char                  rem[FSIM_PATH_MAX];
    char                  next[FSIM_PATH_MAX];
    char                  comp[FSIM_PATH_MAX];
    uint64_t              stack[64];
    uint64_t              cur;
    const char           *stripped = fsim_strip_mount(path_in);
    size_t                pos      = 0;
    int                   depth    = 0;
    int                   links    = 0;
    int                   trailing_slash;
    int                   rc;

    memset(out, 0, sizeof(*out));

    if (strlen(stripped) >= sizeof(rem)) {
        return ENAMETOOLONG;
    }

    /* An empty path is never a name (POSIX: ENOENT), distinct from "/". */
    if (stripped[0] == '\0') {
        return ENOENT;
    }

    strcpy(rem, stripped);

    /* A trailing slash asserts "this names a directory" and survives symlink
     * expansion of the leaf. */
    trailing_slash = (rem[0] != '\0' && rem[strlen(rem) - 1] == '/');

    cur      = (rem[0] == '/') ? FUSE_ROOT_ID : start;
    stack[0] = cur;

    for (;;) {
        size_t len;
        int    is_last;

        while (rem[pos] == '/') {
            pos++;
        }

        if (rem[pos] == '\0') {
            /* Nothing but slashes left: the path names `cur` itself. */
            out->parent  = (depth > 0) ? stack[depth - 1] : cur;
            out->leaf[0] = '\0';
            out->nodeid  = cur;

            memset(&out->entry, 0, sizeof(out->entry));
            out->entry.nodeid = cur;

            /* The caller may need the type (a trailing slash, an EISDIR
             * check), and no LOOKUP produced it on this path. */
            {
                struct fuse_attr_out a;

                if (fuse_sim_getattr(&f->sim, cur, &a) == 0) {
                    out->entry.attr = a.attr;
                }
            }
            return 0;
        }

        /* A component remains, so `cur` is about to be searched.  The order
         * matters and is the kernel's: what `cur` is (ENOTDIR), then whether
         * this caller may search it (EACCES), and only then anything about
         * the component itself. */
        {
            struct fuse_attr_out da;

            rc = fuse_sim_getattr(&f->sim, cur, &da);

            if (rc != 0) {
                return rc;
            }

            if (!S_ISDIR(da.attr.mode)) {
                return ENOTDIR;
            }

            if (!fsim_may_exec(f, &da.attr)) {
                return EACCES;
            }
        }

        len = 0;
        while (rem[pos + len] != '\0' && rem[pos + len] != '/') {
            len++;
        }

        if (len > FSIM_NAME_MAX) {
            return ENAMETOOLONG;
        }

        memcpy(comp, rem + pos, len);
        comp[len] = '\0';

        {
            size_t after = pos + len;

            while (rem[after] == '/') {
                after++;
            }
            is_last = (rem[after] == '\0');
            pos     = pos + len;
        }

        if (strcmp(comp, ".") == 0) {
            if (is_last) {
                continue;   /* falls into the "names cur itself" branch */
            }
            continue;
        }

        if (strcmp(comp, "..") == 0) {
            if (depth > 0) {
                depth--;
                cur = stack[depth];
            } else {
                cur = FUSE_ROOT_ID;   /* .. above the mount root is the root */
            }
            if (is_last) {
                continue;
            }
            continue;
        }

        memset(&e, 0, sizeof(e));

        rc = fuse_sim_lookup(&f->sim, cur, comp, &e);

        if (rc != 0) {
            if (rc == ENOENT && is_last) {
                /* A missing leaf is a completed walk, not a failure: the
                 * caller may be creating it. */
                out->parent = cur;
                out->nodeid = 0;
                strcpy(out->leaf, comp);
                return 0;
            }
            return rc;
        }

        if (e.nodeid == 0) {
            /* A cached-negative reply means the same thing. */
            if (is_last) {
                out->parent = cur;
                out->nodeid = 0;
                strcpy(out->leaf, comp);
                return 0;
            }
            return ENOENT;
        }

        fsim_hold(f, e.nodeid);

        /* A trailing slash forces the final symlink to be followed (XBD
         * 4.16: the slash names the directory the link resolves to), even
         * for an otherwise nofollow caller such as lstat.  The ENOTDIR check
         * below then applies to the object the link resolved to. */
        if (S_ISLNK(e.attr.mode) &&
            (!is_last || follow_leaf || trailing_slash)) {
            char        target[FSIM_PATH_MAX];
            const char *tgt;
            size_t      tlen;

            if (++links > FSIM_SYMLOOP) {
                return ELOOP;
            }

            if (fuse_sim_readlink(&f->sim, e.nodeid, target,
                                  sizeof(target)) != 0) {
                return EIO;
            }

            /* Splice the target in front of whatever is still unresolved.
             * An absolute target names a path in the model's namespace, so it
             * carries the same mount prefix the input paths do and has to be
             * stripped the same way -- otherwise a self-referential link
             * resolves to a missing "test" component and reports ENOENT where
             * the walk should have run out of hops with ELOOP. */
            tgt  = fsim_strip_mount(target);
            tlen = strlen(tgt);

            if (tlen == 0) {
                return ENOENT;
            }

            {
                const char *tail  = rem + pos;
                size_t      taill = strlen(tail);
                size_t      n     = 0;

                /* target + '/' + tail + NUL */
                if (tlen + 1 + taill + 1 > sizeof(next)) {
                    return ENAMETOOLONG;
                }

                memcpy(next, tgt, tlen);
                n = tlen;

                if (taill > 0) {
                    next[n++] = '/';
                    memcpy(next + n, tail, taill);
                    n += taill;
                }

                next[n] = '\0';
                memcpy(rem, next, n + 1);
            }

            pos = 0;

            if (rem[0] == '/') {
                cur   = FUSE_ROOT_ID;
                depth = 0;
            }
            continue;
        }

        if (is_last) {
            out->parent = cur;
            out->nodeid = e.nodeid;
            out->entry  = e;
            strcpy(out->leaf, comp);

            if (trailing_slash && !S_ISDIR(e.attr.mode)) {
                return ENOTDIR;
            }
            return 0;
        }

        if (!S_ISDIR(e.attr.mode)) {
            return ENOTDIR;
        }

        if (depth < (int) (sizeof(stack) / sizeof(stack[0])) - 1) {
            stack[depth++] = cur;
        }
        cur = e.nodeid;
    }
} /* fsim_walk */

/* Resolve to an existing object.  Returns 0 and sets *out, or an errno. */
static int
fsim_resolve_at(
    struct fsim           *f,
    uint64_t               start,
    const char            *path,
    int                    follow,
    uint64_t              *out,
    struct fuse_entry_out *entry_out)
{
    struct fsim_path p;
    int              rc;

    rc = fsim_walk(f, start, path, follow, &p);

    if (rc != 0) {
        return rc;
    }
    if (p.nodeid == 0) {
        return ENOENT;
    }

    *out = p.nodeid;

    if (entry_out) {
        *entry_out = p.entry;
    }

    return 0;
} /* fsim_resolve_at */

static int
fsim_resolve(
    struct fsim           *f,
    const char            *path,
    uint64_t              *out,
    struct fuse_entry_out *entry_out)
{
    return fsim_resolve_at(f, FUSE_ROOT_ID, path, 1, out, entry_out);
} /* fsim_resolve */

/* ------------------------------------------------------------------ */
/* Descriptor table                                                    */
/* ------------------------------------------------------------------ */

static int
fsim_ofd_alloc(struct fsim *f)
{
    int i;

    for (i = 0; i < FSIM_MAX_OFDS; i++) {
        if (f->ofds[i].refs == 0) {
            memset(&f->ofds[i], 0, sizeof(f->ofds[i]));
            f->ofds[i].refs = 1;
            return i;
        }
    }

    return -1;
} /* fsim_ofd_alloc */

/* Lowest free descriptor at or above `atleast` -- the POSIX allocation rule,
 * which F_DUPFD depends on. */
static int
fsim_fd_alloc(
    struct fsim *f,
    int          atleast)
{
    int i;

    if (atleast < 0) {
        atleast = 0;
    }

    for (i = atleast; i < FSIM_MAX_FDS; i++) {
        if (f->fds[i] < 0) {
            return i;
        }
    }

    return -1;
} /* fsim_fd_alloc */

static struct fsim_ofd *
fsim_fd_get(
    struct fsim *f,
    int          fd)
{
    if (fd < 0 || fd >= FSIM_MAX_FDS || f->fds[fd] < 0) {
        return NULL;
    }
    return &f->ofds[f->fds[fd]];
} /* fsim_fd_get */

/* Drop one descriptor's reference, closing the description at zero. */
static int
fsim_fd_put(
    struct fsim *f,
    int          fd)
{
    struct fsim_ofd *o = fsim_fd_get(f, fd);

    if (!o) {
        return -EBADF;
    }

    if (--o->refs == 0) {
        if (o->isdir) {
            fuse_sim_releasedir(&f->sim, o->nodeid, o->fh);
        } else {
            fuse_sim_flush(&f->sim, o->nodeid, o->fh, f->sim.cur_pid);
            fuse_sim_release(&f->sim, o->nodeid, o->fh);
        }
    }

    f->fds[fd] = -1;

    return 0;
} /* fsim_fd_put */

static int
fsim_dup_to(
    struct fsim *f,
    int          fd,
    int          newfd)
{
    struct fsim_ofd *o = fsim_fd_get(f, fd);

    if (!o) {
        return -EBADF;
    }
    if (newfd < 0 || newfd >= FSIM_MAX_FDS) {
        return -EBADF;
    }

    if (newfd == fd) {
        return newfd;   /* dup2(fd, fd) is a no-op that validates fd */
    }

    if (f->fds[newfd] >= 0) {
        fsim_fd_put(f, newfd);
    }

    f->fds[newfd] = f->fds[fd];
    o->refs++;

    return newfd;
} /* fsim_dup_to */

static int
fsim_dup(
    struct fsim *f,
    int          fd,
    int          atleast)
{
    int newfd;

    if (!fsim_fd_get(f, fd)) {
        return -EBADF;
    }

    newfd = fsim_fd_alloc(f, atleast);

    if (newfd < 0) {
        return -EMFILE;
    }

    return fsim_dup_to(f, fd, newfd);
} /* fsim_dup */

/* ------------------------------------------------------------------ */
/* POSIX-shaped operations                                             */
/* ------------------------------------------------------------------ */

/* Returns a descriptor, or -errno. */
static int
fsim_open_at(
    struct fsim *f,
    uint64_t     start,
    const char  *path,
    int          flags,
    uint32_t     mode)
{
    struct fsim_path     p;
    struct fuse_open_out o;
    uint64_t             nodeid;
    int                  fd, ofd, rc;
    int                  isdir = 0;
    int                  nofollow;

    memset(&o, 0, sizeof(o));

    /* Neither O_CREAT|O_EXCL nor O_NOFOLLOW follows a symlink at the leaf; a
     * plain open does. */
    nofollow = (flags & O_NOFOLLOW) ||
        ((flags & O_CREAT) && (flags & O_EXCL));

    rc = fsim_walk(f, start, path, !nofollow, &p);

    if (rc != 0) {
        return -rc;
    }

    /* O_NOFOLLOW on a symlink is ELOOP -- the one case where refusing to
     * follow is an error rather than a result. */
    if (p.nodeid != 0 && (flags & O_NOFOLLOW) &&
        S_ISLNK(p.entry.attr.mode)) {
        return -ELOOP;
    }

    /* O_DIRECTORY demands a directory; anything else is ENOTDIR. */
    if (p.nodeid != 0 && (flags & O_DIRECTORY) &&
        !S_ISDIR(p.entry.attr.mode)) {
        return -ENOTDIR;
    }

    if (p.nodeid == 0) {
        struct fuse_create_in in;
        struct fuse_entry_out e;
        uint8_t               out[sizeof(e) + sizeof(o)];
        size_t                outlen = 0;

        if (!(flags & O_CREAT)) {
            return -ENOENT;
        }
        if (p.leaf[0] == '\0') {
            return -EISDIR;
        }

        memset(&in, 0, sizeof(in));
        in.flags = flags;
        in.mode  = mode | S_IFREG;
        in.umask = f->sim.cur_umask;

        rc = fuse_sim_call(&f->sim, FUSE_CREATE, p.parent, &in, sizeof(in),
                           p.leaf, NULL, 0, out, sizeof(out), &outlen);

        if (rc != 0) {
            return -rc;
        }
        if (outlen < sizeof(out)) {
            return -EIO;
        }

        memcpy(&e, out, sizeof(e));
        memcpy(&o, out + sizeof(e), sizeof(o));

        fsim_hold(f, e.nodeid);
        nodeid = e.nodeid;
        goto opened;
    }

    /* The leaf exists. */
    if ((flags & O_CREAT) && (flags & O_EXCL)) {
        return -EEXIST;
    }

    nodeid = p.nodeid;
    isdir  = S_ISDIR(p.entry.attr.mode);

    if (isdir && (flags & O_ACCMODE) != O_RDONLY) {
        return -EISDIR;
    }

    /*
     * FIFOs, sockets and device nodes never reach the filesystem on open: the
     * kernel gives a special inode its own file_operations when it builds it
     * (init_special_inode), so the open goes to the pipe code or a device
     * driver and no FUSE_OPEN is ever sent.  A blocking FIFO open would simply
     * hang here, which is why the model canonicalizes the whole class to the
     * O_NONBLOCK answer, ENXIO -- ahead of the permission and O_TRUNC checks,
     * because none of those are ever reached either.
     */
    if (!isdir && !S_ISREG(p.entry.attr.mode)) {
        return -ENXIO;
    }

    if (isdir) {
        /* A directory descriptor is what the *at() forms resolve against;
         * OPENDIR is the request a kernel issues for it. */
        rc = fuse_sim_opendir(&f->sim, nodeid, &o);
    } else {
        rc = fuse_sim_open_file(&f->sim, nodeid, flags, &o);
    }

    if (rc != 0) {
        return -rc;
    }

 opened:

    if ((flags & O_TRUNC) && !isdir) {
        struct fuse_setattr_in sa;
        struct fuse_attr_out   ao;

        memset(&sa, 0, sizeof(sa));
        memset(&ao, 0, sizeof(ao));
        sa.valid = FATTR_SIZE | FATTR_FH;
        sa.fh    = o.fh;
        sa.size  = 0;

        rc = fuse_sim_setattr(&f->sim, nodeid, &sa, &ao);

        if (rc != 0) {
            if (isdir) {
                fuse_sim_releasedir(&f->sim, nodeid, o.fh);
            } else {
                fuse_sim_release(&f->sim, nodeid, o.fh);
            }
            return -rc;
        }
    }

    ofd = fsim_ofd_alloc(f);

    if (ofd < 0) {
        return -EMFILE;
    }

    fd = fsim_fd_alloc(f, 0);

    if (fd < 0) {
        f->ofds[ofd].refs = 0;
        return -EMFILE;
    }

    f->ofds[ofd].nodeid = nodeid;
    f->ofds[ofd].fh     = o.fh;
    f->ofds[ofd].flags  = flags;
    f->ofds[ofd].isdir  = isdir;

    /* O_APPEND repositions each WRITE to end of file; it does not move the
     * initial file offset, which starts at 0 like any other open.  Starting
     * at EOF here made a read through an append-mode descriptor return 0
     * bytes and looked like a server data bug. */
    f->ofds[ofd].offset = 0;

    f->fds[fd] = ofd;

    return fd;
} /* fsim_open_at */

static int
fsim_open(
    struct fsim *f,
    const char  *path,
    int          flags,
    uint32_t     mode)
{
    return fsim_open_at(f, FUSE_ROOT_ID, path, flags, mode);
} /* fsim_open */

static int
fsim_close(
    struct fsim *f,
    int          fd)
{
    return fsim_fd_put(f, fd);
} /* fsim_close */

static long
fsim_pread(
    struct fsim *f,
    int          fd,
    void        *buf,
    size_t       count,
    uint64_t     offset,
    int          advance)
{
    struct fsim_ofd *e      = fsim_fd_get(f, fd);
    size_t           outlen = 0;
    int              rc;

    if (!e) {
        return -EBADF;
    }
    if ((e->flags & O_ACCMODE) == O_WRONLY) {
        return -EBADF;
    }
    if (e->isdir) {
        return -EISDIR;
    }

    rc = fuse_sim_read(&f->sim, e->nodeid, e->fh, offset, (uint32_t) count,
                       buf, &outlen);

    if (rc != 0) {
        return -rc;
    }

    if (advance) {
        e->offset = offset + outlen;
    }

    return (long) outlen;
} /* fsim_pread */

static long
fsim_pwrite(
    struct fsim *f,
    int          fd,
    const void  *buf,
    size_t       count,
    uint64_t     offset,
    int          advance)
{
    struct fsim_ofd *e       = fsim_fd_get(f, fd);
    uint32_t         written = 0;
    int              rc;

    if (!e) {
        return -EBADF;
    }
    if ((e->flags & O_ACCMODE) == O_RDONLY) {
        return -EBADF;
    }
    if (e->isdir) {
        return -EBADF;
    }

    rc = fuse_sim_write(&f->sim, e->nodeid, e->fh, offset, buf,
                        (uint32_t) count, &written);

    if (rc != 0) {
        return -rc;
    }

    if (advance) {
        e->offset = offset + written;
    }

    return (long) written;
} /* fsim_pwrite */

static int
fsim_stat_nodeid(
    struct fsim          *f,
    uint64_t              nodeid,
    struct fuse_attr_out *out)
{
    return fuse_sim_getattr(&f->sim, nodeid, out);
} /* fsim_stat_nodeid */

static int
fsim_stat_at(
    struct fsim          *f,
    uint64_t              start,
    const char           *path,
    int                   follow,
    struct fuse_attr_out *out)
{
    uint64_t nodeid;
    int      rc;

    rc = fsim_resolve_at(f, start, path, follow, &nodeid, NULL);

    if (rc != 0) {
        return -rc;
    }

    rc = fsim_stat_nodeid(f, nodeid, out);

    return rc == 0 ? 0 : -rc;
} /* fsim_stat_at */

/* setattr helpers: each returns 0 or -errno. */
static int
fsim_setattr_path(
    struct fsim                  *f,
    const char                   *path,
    int                           follow,
    const struct fuse_setattr_in *tmpl)
{
    struct fuse_setattr_in in = *tmpl;
    struct fuse_attr_out   out;
    uint64_t               nodeid;
    int                    rc;

    rc = fsim_resolve_at(f, FUSE_ROOT_ID, path, follow, &nodeid, NULL);

    if (rc != 0) {
        return -rc;
    }

    memset(&out, 0, sizeof(out));

    rc = fuse_sim_setattr(&f->sim, nodeid, &in, &out);

    return rc == 0 ? 0 : -rc;
} /* fsim_setattr_path */

static int
fsim_setattr_fd(
    struct fsim                  *f,
    int                           fd,
    const struct fuse_setattr_in *tmpl)
{
    struct fuse_setattr_in in = *tmpl;
    struct fuse_attr_out   out;
    struct fsim_ofd       *e = fsim_fd_get(f, fd);
    int                    rc;

    if (!e) {
        return -EBADF;
    }

    in.valid |= FATTR_FH;
    in.fh     = e->fh;

    memset(&out, 0, sizeof(out));

    rc = fuse_sim_setattr(&f->sim, e->nodeid, &in, &out);

    return rc == 0 ? 0 : -rc;
} /* fsim_setattr_fd */

/* ------------------------------------------------------------------ */
/* Directory streams                                                   */
/* ------------------------------------------------------------------ */

static int
fsim_opendir(
    struct fsim *f,
    const char  *path)
{
    struct fuse_open_out  o;
    struct fuse_entry_out e;
    uint64_t              nodeid;
    int                   sid, rc;

    memset(&o, 0, sizeof(o));
    memset(&e, 0, sizeof(e));

    rc = fsim_resolve(f, path, &nodeid, &e);

    if (rc != 0) {
        return -rc;
    }

    if (!S_ISDIR(e.attr.mode)) {
        return -ENOTDIR;
    }

    for (sid = 0; sid < FSIM_MAX_DIRS; sid++) {
        if (!f->dirs[sid].used) {
            break;
        }
    }

    if (sid == FSIM_MAX_DIRS) {
        return -EMFILE;
    }

    rc = fuse_sim_opendir(&f->sim, nodeid, &o);

    if (rc != 0) {
        return -rc;
    }

    f->dirs[sid].used   = 1;
    f->dirs[sid].nodeid = nodeid;
    f->dirs[sid].fh     = o.fh;
    f->dirs[sid].cookie = 0;

    return sid;
} /* fsim_opendir */

static struct fsim_dir *
fsim_dir_get(
    struct fsim *f,
    int          sid)
{
    if (sid < 0 || sid >= FSIM_MAX_DIRS || !f->dirs[sid].used) {
        return NULL;
    }
    return &f->dirs[sid];
} /* fsim_dir_get */

static int
fsim_closedir(
    struct fsim *f,
    int          sid)
{
    struct fsim_dir *d = fsim_dir_get(f, sid);

    if (!d) {
        return -EBADF;
    }

    fuse_sim_releasedir(&f->sim, d->nodeid, d->fh);
    d->used = 0;

    return 0;
} /* fsim_closedir */

/*
 * One full sweep of a directory from a fresh cursor, which is the shape the
 * model's RReaddir asserts (posix_driver.c rewinds before every sweep).
 * `cb` is called once per entry; "." and ".." are passed through, as
 * readdir(3) yields them and the replay filters them itself.
 */
static int
fsim_readdir_all(
    struct fsim *f,
    int sid,
    void ( *cb )(void *ctx, const char *name),
    void *ctx)
{
    struct fsim_dir *d = fsim_dir_get(f, sid);
    uint8_t          buf[8192];
    uint64_t         cookie = 0;
    int              guard  = 0;

    if (!d) {
        return -EBADF;
    }

    for (;;) {
        size_t outlen = 0;
        size_t off    = 0;
        int    rc;

        if (++guard > 4096) {
            return -EIO;   /* a server that never terminates the stream */
        }

        rc = fuse_sim_readdir(&f->sim, d->nodeid, d->fh, cookie, buf,
                              sizeof(buf), &outlen);

        if (rc != 0) {
            return -rc;
        }
        if (outlen == 0) {
            break;   /* end of stream */
        }

        while (off + FUSE_NAME_OFFSET <= outlen) {
            const struct fuse_dirent *de = (const void *) (buf + off);
            size_t                    reclen;
            char                      name[FSIM_NAME_MAX + 1];
            size_t                    nl = de->namelen;

            if (nl > FSIM_NAME_MAX) {
                return -EIO;
            }

            reclen = FUSE_DIRENT_SIZE(de);

            if (off + reclen > outlen) {
                return -EIO;
            }

            memcpy(name, de->name, nl);
            name[nl] = '\0';

            cb(ctx, name);

            cookie = de->off;
            off   += reclen;
        }
    }

    d->cookie = cookie;

    return 0;
} /* fsim_readdir_all */

/* ------------------------------------------------------------------ */
/* Session lifecycle                                                   */
/* ------------------------------------------------------------------ */

static void
fsim_procs_reset(struct fsim *f)
{
    int i;

    for (i = 0; i < FSIM_MAX_PIDS; i++) {
        f->procs[i].uid   = 0;
        f->procs[i].gid   = 0;
        f->procs[i].umask = 0;
    }

    fsim_apply_pid(f, 0);
} /* fsim_procs_reset */

static void
fsim_tables_reset(struct fsim *f)
{
    int i;

    for (i = 0; i < FSIM_MAX_FDS; i++) {
        f->fds[i] = -1;
    }

    memset(f->ofds, 0, sizeof(f->ofds));
    memset(f->dirs, 0, sizeof(f->dirs));

    f->num_held = 0;
} /* fsim_tables_reset */

static void
fsim_init(struct fsim *f)
{
    fsim_tables_reset(f);
    fsim_procs_reset(f);
    f->fs_counter = 0;
    snprintf(f->fsname, sizeof(f->fsname), "quintfs0");
} /* fsim_init */

/*
 * Normalize the mount root to the model's fsInit(0777, 0, 0), as
 * posix_driver.c's normalize_root() does.  Runs as root so it is never the
 * thing under test.
 */
static int
fsim_normalize_root(struct fsim *f)
{
    struct fuse_setattr_in in;
    struct fuse_attr_out   out;
    uint32_t               save_uid = f->sim.cur_uid;
    uint32_t               save_gid = f->sim.cur_gid;
    int                    rc;

    f->sim.cur_uid = 0;
    f->sim.cur_gid = 0;

    memset(&in, 0, sizeof(in));
    memset(&out, 0, sizeof(out));
    in.valid = FATTR_MODE | FATTR_UID | FATTR_GID;
    in.mode  = 0777 | S_IFDIR;
    in.uid   = 0;
    in.gid   = 0;

    rc = fuse_sim_setattr(&f->sim, FUSE_ROOT_ID, &in, &out);

    f->sim.cur_uid = save_uid;
    f->sim.cur_gid = save_gid;

    return rc == 0 ? 0 : -rc;
} /* fsim_normalize_root */

static void
fsim_start(
    struct fsim *f,
    const char  *fsname)
{
    fsim_tables_reset(f);
    fsim_procs_reset(f);
    fuse_sim_open(&f->sim, fsname);
    fsim_normalize_root(f);
} /* fsim_start */

/*
 * Batch isolation between traces: tear the whole session down and stand a
 * fresh one up on a new filesystem, so neither content nor a cached nodeid
 * leaks into the next trace.  posix_driver.c recycles just the filesystem
 * because its client outlives the mount; here the FUSE session IS the mount,
 * so the session is what gets recycled.
 */
static int
fsim_newfs(struct fsim *f)
{
    char name[64];
    int  i;

    fsim_dbg("newfs: closing dirs\n");

    for (i = 0; i < FSIM_MAX_DIRS; i++) {
        if (f->dirs[i].used) {
            fsim_closedir(f, i);
        }
    }

    fsim_dbg("newfs: closing fds\n");

    for (i = 0; i < FSIM_MAX_FDS; i++) {
        if (f->fds[i] >= 0) {
            fsim_fd_put(f, i);
        }
    }

    fsim_dbg("newfs: forgetting %d nodes\n", f->num_held);
    fsim_forget_all(f);

    fsim_dbg("newfs: destroying server\n");
    fuse_sim_close(&f->sim);

    snprintf(name, sizeof(name), "quintfs%d", ++f->fs_counter);
    snprintf(f->fsname, sizeof(f->fsname), "%s", name);

    fsim_dbg("newfs: starting server on %s\n", name);
    fsim_start(f, name);

    fsim_dbg("newfs: done\n");

    return 0;
} /* fsim_newfs */
