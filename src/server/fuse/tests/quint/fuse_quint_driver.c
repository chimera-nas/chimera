// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * FUSE replay driver for the POSIX model-based test suite.
 *
 * Speaks the same line-delimited JSON protocol as posix_driver.c -- one
 * request object per line in, one response object per line out -- so
 * posix_replay.py can drive it with the identical trace corpus and compare
 * against the identical model expectations.  The difference is entirely
 * below the protocol: where posix_driver.c calls chimera_posix_* directly,
 * this executes each operation as FUSE requests against the chimera FUSE
 * server over a socketpair standing in for /dev/fuse (fuse_sim.h), with a
 * minimal cache-free kernel doing path resolution and descriptor
 * bookkeeping (fuse_sim_vfs.h).
 *
 * What that measures: the FUSE server's own POSIX op mapping -- the layer
 * between the wire and the VFS -- which no existing corpus reaches, since
 * pjd and the quint traces run against the POSIX client, NFS and SMB but
 * never through FUSE.
 *
 * The protocol contract is posix_driver.c's, field for field: this file is
 * only meaningful insofar as it is substitutable for that one, so any
 * divergence in the request/response shape is a harness bug, not a finding.
 *
 * Operations the FUSE protocol has no request for (copy_file_range, which the
 * server leaves to the kernel's fallback) answer ENOSYS rather than guessing,
 * so the replay reports them as unsupported instead of silently scoring a
 * divergence against the server.
 */

/* Must precede every system header: SEEK_DATA/SEEK_HOLE and the FALLOC_FL_*
 * flags are GNU extensions, and jansson.h pulls in stdio first. */
#define _GNU_SOURCE 1

#include <jansson.h>

#include "fuse_sim_vfs.h"

static struct fsim g_fsim;

/* ------------------------------------------------------------------ */
/* JSON helpers (same shapes as posix_driver.c)                        */
/* ------------------------------------------------------------------ */

static const char *
jstr(
    json_t     *o,
    const char *k)
{
    json_t *v = json_object_get(o, k);

    return json_is_string(v) ? json_string_value(v) : NULL;
} /* jstr */

static long long
jint(
    json_t     *o,
    const char *k,
    long long   dflt)
{
    json_t *v = json_object_get(o, k);

    return json_is_integer(v) ? json_integer_value(v) : dflt;
} /* jint */

static int
jbool(
    json_t     *o,
    const char *k,
    int         dflt)
{
    json_t *v = json_object_get(o, k);

    return json_is_boolean(v) ? json_is_true(v) : dflt;
} /* jbool */

static json_t *
res_int(
    long long ret,
    int       err)
{
    json_t *res = json_object();

    json_object_set_new(res, "ret", json_integer(ret));
    json_object_set_new(res, "err", json_integer(ret < 0 ? err : 0));
    return res;
} /* res_int */

/* A negative return from the fsim layer already carries -errno. */
static json_t *
res_rc(long rc)
{
    return rc < 0 ? res_int(-1, (int) -rc) : res_int(rc, 0);
} /* res_rc */

/* ------------------------------------------------------------------ */
/* base64 (the wire encoding for read/write payloads)                  */
/* ------------------------------------------------------------------ */

static const char b64_tbl[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static int
b64_decode(
    const char    *in,
    unsigned char *out,
    size_t         out_cap)
{
    int    acc = 0, nbits = 0;
    size_t n = 0;

    for (const char *p = in; *p && *p != '='; p++) {
        const char *hit = strchr(b64_tbl, *p);

        if (!hit) {
            return -1;
        }
        acc    = (acc << 6) | (int) (hit - b64_tbl);
        nbits += 6;

        if (nbits >= 8) {
            nbits -= 8;
            if (n >= out_cap) {
                return -1;
            }
            out[n++] = (unsigned char) ((acc >> nbits) & 0xff);
        }
    }

    return (int) n;
} /* b64_decode */

static char *
b64_encode(
    const unsigned char *in,
    size_t               len)
{
    size_t olen = 4 * ((len + 2) / 3) + 1;
    char  *out  = malloc(olen);
    size_t i, o = 0;

    for (i = 0; i + 2 < len; i += 3) {
        unsigned v = (in[i] << 16) | (in[i + 1] << 8) | in[i + 2];

        out[o++] = b64_tbl[(v >> 18) & 63];
        out[o++] = b64_tbl[(v >> 12) & 63];
        out[o++] = b64_tbl[(v >> 6) & 63];
        out[o++] = b64_tbl[v & 63];
    }

    if (i + 1 == len) {
        unsigned v = in[i] << 16;

        out[o++] = b64_tbl[(v >> 18) & 63];
        out[o++] = b64_tbl[(v >> 12) & 63];
        out[o++] = '=';
        out[o++] = '=';
    } else if (i + 2 == len) {
        unsigned v = (in[i] << 16) | (in[i + 1] << 8);

        out[o++] = b64_tbl[(v >> 18) & 63];
        out[o++] = b64_tbl[(v >> 12) & 63];
        out[o++] = b64_tbl[(v >> 6) & 63];
        out[o++] = '=';
    }

    out[o] = '\0';
    return out;
} /* b64_encode */

/* ------------------------------------------------------------------ */
/* Reply shaping                                                       */
/* ------------------------------------------------------------------ */

/*
 * The stat reply posix_replay.py's check_statres consumes.  Field for field
 * the same as posix_driver.c's stat_fill; st_dev has no FUSE equivalent (the
 * kernel supplies the mount's device id), and the replay only uses it as half
 * of an identity pair, so a session-constant stands in.
 */
#define FSIM_FAKE_DEV 0x00fu

static void
stat_fill(
    json_t                 *res,
    const struct fuse_attr *a)
{
    const char *ftype = "unk";
    json_t     *at, *mt, *ct;

    switch (a->mode & S_IFMT) {
        case S_IFREG:  ftype = "reg"; break;
        case S_IFDIR:  ftype = "dir"; break;
        case S_IFLNK:  ftype = "lnk"; break;
        case S_IFIFO:  ftype = "fifo"; break;
        case S_IFSOCK: ftype = "sock"; break;
        case S_IFBLK:  ftype = "blk"; break;
        case S_IFCHR:  ftype = "chr"; break;
    } /* switch */

    json_object_set_new(res, "ftype", json_string(ftype));
    json_object_set_new(res, "mode", json_integer(a->mode & 07777));
    json_object_set_new(res, "ino", json_integer((long long) a->ino));
    json_object_set_new(res, "dev", json_integer(FSIM_FAKE_DEV));
    json_object_set_new(res, "nlink", json_integer((long long) a->nlink));
    json_object_set_new(res, "uid", json_integer(a->uid));
    json_object_set_new(res, "gid", json_integer(a->gid));
    json_object_set_new(res, "size", json_integer((long long) a->size));

    at = json_array();
    mt = json_array();
    ct = json_array();

    json_array_append_new(at, json_integer((long long) a->atime));
    json_array_append_new(at, json_integer(a->atimensec));
    json_array_append_new(mt, json_integer((long long) a->mtime));
    json_array_append_new(mt, json_integer(a->mtimensec));
    json_array_append_new(ct, json_integer((long long) a->ctime));
    json_array_append_new(ct, json_integer(a->ctimensec));

    json_object_set_new(res, "atime", at);
    json_object_set_new(res, "mtime", mt);
    json_object_set_new(res, "ctime", ct);
} /* stat_fill */

static json_t *
res_stat(
    const struct fuse_attr_out *a,
    long                        rc)
{
    json_t *res;

    if (rc < 0) {
        return res_int(-1, (int) -rc);
    }

    res = res_int(0, 0);
    stat_fill(res, &a->attr);

    return res;
} /* res_stat */

/* ------------------------------------------------------------------ */
/* Request-shape helpers                                               */
/* ------------------------------------------------------------------ */

/* The directory a relative path resolves against: the *at() forms name it
 * with a descriptor, the plain forms use the session root. */
static int
dirfd_start(
    json_t   *req,
    int       have_dirfd,
    uint64_t *start)
{
    struct fsim_ofd *o;
    int              dirfd;

    *start = FUSE_ROOT_ID;

    if (!have_dirfd) {
        return 0;
    }

    dirfd = (int) jint(req, "dirfd", -1);

    if (dirfd < 0) {
        return 0;   /* AT_FDCWD; the model's cwd is the mount root */
    }

    o = fsim_fd_get(&g_fsim, dirfd);

    if (!o) {
        return EBADF;
    }
    if (!o->isdir) {
        return ENOTDIR;
    }

    *start = o->nodeid;
    return 0;
} /* dirfd_start */

/* utimens: "omit" leaves the field alone, "val" sets it explicitly, anything
 * else means "now".  FUSE spells the last as a separate valid bit. */
static void
utimens_bits(
    json_t     *req,
    const char *typekey,
    const char *seckey,
    const char *nseckey,
    uint32_t    bit,
    uint32_t    now_bit,
    uint32_t   *valid,
    uint64_t   *sec,
    uint32_t   *nsec)
{
    const char *type = jstr(req, typekey);

    if (type && strcmp(type, "omit") == 0) {
        return;
    }

    *valid |= bit;

    if (type && strcmp(type, "val") == 0) {
        *sec  = (uint64_t) jint(req, seckey, 0);
        *nsec = (uint32_t) jint(req, nseckey, 0);
        return;
    }

    *valid |= now_bit;
} /* utimens_bits */

struct name_collect {
    json_t *arr;
};

static void
collect_name(
    void       *ctx,
    const char *name)
{
    struct name_collect *c = ctx;

    json_array_append_new(c->arr, json_string(name));
} /* collect_name */

/* ------------------------------------------------------------------ */
/* Op dispatch                                                         */
/* ------------------------------------------------------------------ */

static json_t *
fuse_exec_op(json_t *req)
{
    const char *op = jstr(req, "op");
    int         pid;

    if (!op) {
        return res_int(-1, EINVAL);
    }

    /* --- harness control: credential/umask bookkeeping is per model
     * process and must not itself issue FUSE traffic --- */

    if (strcmp(op, "setcred") == 0) {
        pid = (int) jint(req, "pid", 0);

        if (pid < 0 || pid >= FSIM_MAX_PIDS) {
            return res_int(-1, EINVAL);
        }

        g_fsim.procs[pid].uid   = (uint32_t) jint(req, "uid", 0);
        g_fsim.procs[pid].gid   = (uint32_t) jint(req, "gid", 0);
        g_fsim.procs[pid].ngids = 0;

        /* Supplementary groups never reach the server -- fuse_in_header has
         * one gid -- but they are the kernel's to enforce with, so the
         * harness's kernel side keeps them (see fsim_may_exec). */
        {
            json_t *gids = json_object_get(req, "gids");
            size_t  i;

            if (json_is_array(gids)) {
                for (i = 0; i < json_array_size(gids); i++) {
                    json_t *g = json_array_get(gids, i);

                    if (!json_is_integer(g) ||
                        g_fsim.procs[pid].ngids >= FSIM_MAX_GIDS) {
                        continue;
                    }

                    g_fsim.procs[pid].gids[g_fsim.procs[pid].ngids++] =
                        (uint32_t) json_integer_value(g);
                }
            }
        }

        return res_int(0, 0);
    }

    if (strcmp(op, "umask") == 0) {
        uint32_t old;

        pid = (int) jint(req, "pid", 0);

        if (pid < 0 || pid >= FSIM_MAX_PIDS) {
            return res_int(-1, EINVAL);
        }

        old                     = g_fsim.procs[pid].umask;
        g_fsim.procs[pid].umask = (uint32_t) jint(req, "mask", 0);

        return res_int(old, 0);
    }

    if (strcmp(op, "shutdown") == 0) {
        return NULL;
    }

    if (strcmp(op, "newfs") == 0) {
        return res_rc(fsim_newfs(&g_fsim));
    }

    /* Everything below runs as the issuing model process. */
    fsim_apply_pid(&g_fsim, (int) jint(req, "pid", 0));

    fsim_dbg("op %s (pid %d, notifications so far %d)\n", op,
             (int) jint(req, "pid", 0), g_fsim.sim.num_notify);

    /* Between operations nothing is reading the channel; clear whatever the
     * server pushed so its notifier never blocks on a full socket. */
    fuse_sim_drain(&g_fsim.sim);

    /* --- descriptors --- */

    if (strcmp(op, "open") == 0 || strcmp(op, "openat") == 0) {
        uint64_t start;
        int      flags = (int) jint(req, "flags", 0);
        int      rc;

        rc = dirfd_start(req, strcmp(op, "openat") == 0, &start);

        if (rc != 0) {
            return res_int(-1, rc);
        }

        return res_rc(fsim_open_at(&g_fsim, start, jstr(req, "path"), flags,
                                   (uint32_t) jint(req, "mode", 0)));
    }

    if (strcmp(op, "close") == 0) {
        return res_rc(fsim_close(&g_fsim, (int) jint(req, "fd", -1)));
    }

    if (strcmp(op, "dup") == 0) {
        return res_rc(fsim_dup(&g_fsim, (int) jint(req, "fd", -1), 0));
    }

    if (strcmp(op, "dup2") == 0) {
        return res_rc(fsim_dup_to(&g_fsim, (int) jint(req, "fd", -1),
                                  (int) jint(req, "nfd", -1)));
    }

    if (strcmp(op, "fcntl_dupfd") == 0) {
        return res_rc(fsim_dup(&g_fsim, (int) jint(req, "fd", -1),
                               (int) jint(req, "atleast", 0)));
    }

    if (strcmp(op, "fcntl_getfl") == 0) {
        struct fsim_ofd *o = fsim_fd_get(&g_fsim, (int) jint(req, "fd", -1));

        if (!o) {
            return res_int(-1, EBADF);
        }
        return res_int(o->flags, 0);
    }

    if (strcmp(op, "fcntl_setfl") == 0) {
        struct fsim_ofd *o     = fsim_fd_get(&g_fsim, (int) jint(req, "fd", -1));
        int              flags = (int) jint(req, "flags", 0);

        if (!o) {
            return res_int(-1, EBADF);
        }

        /* F_SETFL can change only the status flags, never the access mode. */
        o->flags &= ~O_APPEND;
        o->flags |= (flags & O_APPEND);

        return res_int(0, 0);
    }

    /* --- data --- */

    if (strcmp(op, "read") == 0 || strcmp(op, "pread") == 0 ||
        strcmp(op, "readv") == 0 || strcmp(op, "preadv") == 0) {
        struct fsim_ofd *o    = fsim_fd_get(&g_fsim, (int) jint(req, "fd", -1));
        size_t           len  = (size_t) jint(req, "len", 0);
        int              is_p = (op[0] == 'p');
        uint64_t         off;
        char            *buf;
        long             rc;
        json_t          *res;

        if (!o) {
            return res_int(-1, EBADF);
        }

        buf = malloc(len ? len : 1);

        if (!buf) {
            return res_int(-1, ENOMEM);
        }

        off = is_p ? (uint64_t) jint(req, "off", 0) : o->offset;
        rc  = fsim_pread(&g_fsim, (int) jint(req, "fd", -1), buf, len, off,
                         !is_p);

        res = res_rc(rc);

        if (rc >= 0) {
            char *enc = b64_encode((unsigned char *) buf, (size_t) rc);

            json_object_set_new(res, "data", json_string(enc));
            free(enc);
        }

        free(buf);
        return res;
    }

    if (strcmp(op, "write") == 0 || strcmp(op, "pwrite") == 0 ||
        strcmp(op, "writev") == 0 || strcmp(op, "pwritev") == 0) {
        struct fsim_ofd *o    = fsim_fd_get(&g_fsim, (int) jint(req, "fd", -1));
        const char      *data = jstr(req, "data");
        size_t           cap  = data ? strlen(data) : 0;
        int              is_p = (op[0] == 'p');
        unsigned char   *buf;
        uint64_t         off;
        int              len;
        long             rc;

        if (!o) {
            return res_int(-1, EBADF);
        }

        buf = malloc(cap ? cap : 1);

        if (!buf) {
            return res_int(-1, ENOMEM);
        }

        len = b64_decode(data ? data : "", buf, cap ? cap : 1);

        if (len < 0) {
            free(buf);
            return res_int(-1, EINVAL);
        }

        if (is_p) {
            off = (uint64_t) jint(req, "off", 0);
        } else if (o->flags & O_APPEND) {
            /* O_APPEND lands at EOF; a real kernel resolves that under the
             * inode lock, so ask the server for the size first. */
            struct fuse_attr_out a;

            memset(&a, 0, sizeof(a));

            if (fsim_stat_nodeid(&g_fsim, o->nodeid, &a) != 0) {
                free(buf);
                return res_int(-1, EIO);
            }
            off = a.attr.size;
        } else {
            off = o->offset;
        }

        rc = fsim_pwrite(&g_fsim, (int) jint(req, "fd", -1), buf,
                         (size_t) len, off, !is_p);

        /* An appending write leaves the position at the new end. */
        if (rc >= 0 && !is_p && (o->flags & O_APPEND)) {
            o->offset = off + (uint64_t) rc;
        }

        free(buf);
        return res_rc(rc);
    }

    if (strcmp(op, "lseek") == 0) {
        struct fsim_ofd *o   = fsim_fd_get(&g_fsim, (int) jint(req, "fd", -1));
        const char      *wh  = jstr(req, "whence");
        long long        off = jint(req, "off", 0);
        long long        base;

        if (!o) {
            return res_int(-1, EBADF);
        }

        if (!wh || strcmp(wh, "set") == 0) {
            base = 0;
        } else if (strcmp(wh, "cur") == 0) {
            base = (long long) o->offset;
        } else if (strcmp(wh, "end") == 0) {
            struct fuse_attr_out a;

            memset(&a, 0, sizeof(a));

            if (fsim_stat_nodeid(&g_fsim, o->nodeid, &a) != 0) {
                return res_int(-1, EIO);
            }
            base = (long long) a.attr.size;
        } else {
            /* SEEK_DATA / SEEK_HOLE are the two the kernel forwards. */
            uint64_t out_off = 0;
            uint32_t whence  = (strcmp(wh, "data") == 0) ? SEEK_DATA
                                                         : SEEK_HOLE;
            int      rc;

            rc = fuse_sim_lseek(&g_fsim.sim, o->nodeid, o->fh,
                                (uint64_t) off, whence, &out_off);

            if (rc != 0) {
                return res_int(-1, rc);
            }

            o->offset = out_off;
            return res_int((long long) out_off, 0);
        }

        if (base + off < 0) {
            return res_int(-1, EINVAL);
        }

        o->offset = (uint64_t) (base + off);

        return res_int((long long) o->offset, 0);
    }

    if (strcmp(op, "fallocate") == 0) {
        struct fsim_ofd *o    = fsim_fd_get(&g_fsim, (int) jint(req, "fd", -1));
        int              mode = (int) jint(req, "mode", 0);
        int              rc;

        /* vfs_fallocate() validates the range and the descriptor's access
         * mode before any filesystem is called, so both belong here rather
         * than on the wire.  The range check comes first, as it does in the
         * kernel: an invalid length is EINVAL even for a bad descriptor. */
        if (jint(req, "off", 0) < 0 || jint(req, "len", 0) <= 0) {
            return res_int(-1, EINVAL);
        }

        if (!o) {
            return res_int(-1, EBADF);
        }

        if ((o->flags & O_ACCMODE) == O_RDONLY) {
            return res_int(-1, EBADF);
        }

        /* mode 0 is posix_fallocate (grow); mode 1 is the
         * PUNCH_HOLE|KEEP_SIZE deallocate pair, as posix_driver.c maps it. */
        rc = fuse_sim_fallocate(&g_fsim.sim, o->nodeid, o->fh,
                                (uint64_t) jint(req, "off", 0),
                                (uint64_t) jint(req, "len", 0),
                                mode == 0 ? 0
                                : (FALLOC_FL_PUNCH_HOLE |
                                   FALLOC_FL_KEEP_SIZE));

        return rc == 0 ? res_int(0, 0) : res_int(-1, rc);
    }

    if (strcmp(op, "fsync") == 0 || strcmp(op, "fdatasync") == 0) {
        struct fsim_ofd *o = fsim_fd_get(&g_fsim, (int) jint(req, "fd", -1));
        int              rc;

        if (!o) {
            return res_int(-1, EBADF);
        }

        rc = fuse_sim_fsync(&g_fsim.sim, o->nodeid, o->fh,
                            strcmp(op, "fdatasync") == 0);

        return rc == 0 ? res_int(0, 0) : res_int(-1, rc);
    }

    /* --- metadata --- */

    if (strcmp(op, "stat") == 0 || strcmp(op, "fstatat") == 0) {
        struct fuse_attr_out a;
        uint64_t             start;
        long                 rc;

        rc = dirfd_start(req, strcmp(op, "fstatat") == 0, &start);

        if (rc != 0) {
            return res_int(-1, (int) rc);
        }

        memset(&a, 0, sizeof(a));
        rc = fsim_stat_at(&g_fsim, start, jstr(req, "path"),
                          jbool(req, "follow", 1), &a);

        return res_stat(&a, rc);
    }

    if (strcmp(op, "fstat") == 0) {
        struct fuse_attr_out a;
        struct fsim_ofd     *o = fsim_fd_get(&g_fsim, (int) jint(req, "fd", -1));
        int                  rc;

        if (!o) {
            return res_int(-1, EBADF);
        }

        memset(&a, 0, sizeof(a));
        rc = fsim_stat_nodeid(&g_fsim, o->nodeid, &a);

        return res_stat(&a, rc == 0 ? 0 : -rc);
    }

    if (strcmp(op, "statfs") == 0 || strcmp(op, "statvfs") == 0) {
        struct fuse_statfs_out sb;
        uint64_t               nodeid;
        int                    rc;

        rc = fsim_resolve(&g_fsim, jstr(req, "path"), &nodeid, NULL);

        if (rc != 0) {
            return res_int(-1, rc);
        }

        memset(&sb, 0, sizeof(sb));
        rc = fuse_sim_statfs(&g_fsim.sim, nodeid, &sb);

        return rc == 0 ? res_int(0, 0) : res_int(-1, rc);
    }

    if (strcmp(op, "fstatfs") == 0 || strcmp(op, "fstatvfs") == 0) {
        struct fuse_statfs_out sb;
        struct fsim_ofd       *o = fsim_fd_get(&g_fsim,
                                               (int) jint(req, "fd", -1));
        int                    rc;

        if (!o) {
            return res_int(-1, EBADF);
        }

        memset(&sb, 0, sizeof(sb));
        rc = fuse_sim_statfs(&g_fsim.sim, o->nodeid, &sb);

        return rc == 0 ? res_int(0, 0) : res_int(-1, rc);
    }

    if (strcmp(op, "chmod") == 0 || strcmp(op, "fchmod") == 0) {
        struct fuse_setattr_in in;

        memset(&in, 0, sizeof(in));
        in.valid = FATTR_MODE;
        in.mode  = (uint32_t) jint(req, "mode", 0);

        if (strcmp(op, "fchmod") == 0) {
            return res_rc(fsim_setattr_fd(&g_fsim,
                                          (int) jint(req, "fd", -1), &in));
        }
        /* chmod(2) and truncate(2) both resolve through a final symlink. */
        return res_rc(fsim_setattr_path(&g_fsim, jstr(req, "path"), 1, &in));
    }

    if (strcmp(op, "chown") == 0 || strcmp(op, "fchown") == 0) {
        struct fuse_setattr_in in;
        long long              uid = jint(req, "uid", -1);
        long long              gid = jint(req, "gid", -1);

        memset(&in, 0, sizeof(in));

        /* -1 means "leave alone": the field simply is not set. */
        if (uid >= 0) {
            in.valid |= FATTR_UID;
            in.uid    = (uint32_t) uid;
        }
        if (gid >= 0) {
            in.valid |= FATTR_GID;
            in.gid    = (uint32_t) gid;
        }

        if (strcmp(op, "fchown") == 0) {
            return res_rc(fsim_setattr_fd(&g_fsim,
                                          (int) jint(req, "fd", -1), &in));
        }
        /* lchown(2) (follow false) acts on the symlink itself; chown(2)
         * follows it. */
        return res_rc(fsim_setattr_path(&g_fsim, jstr(req, "path"),
                                        jbool(req, "follow", 1), &in));
    }

    if (strcmp(op, "truncate") == 0) {
        struct fuse_setattr_in in;
        long long              len = jint(req, "len", 0);

        if (len < 0) {
            return res_int(-1, EINVAL);
        }

        memset(&in, 0, sizeof(in));
        in.valid = FATTR_SIZE;
        in.size  = (uint64_t) len;

        /* chmod(2) and truncate(2) both resolve through a final symlink. */
        return res_rc(fsim_setattr_path(&g_fsim, jstr(req, "path"), 1, &in));
    }

    if (strcmp(op, "ftruncate") == 0) {
        struct fuse_setattr_in in;
        struct fsim_ofd       *o = fsim_fd_get(&g_fsim,
                                               (int) jint(req, "fd", -1));
        long long              len = jint(req, "len", 0);

        if (!o) {
            return res_int(-1, EBADF);
        }

        /* The kernel rejects a truncate through a non-writable descriptor in
         * do_sys_ftruncate, before the filesystem is ever asked; without this
         * the harness would put a request on the wire that a real mount never
         * sends.  POSIX and the model both say EINVAL. */
        if ((o->flags & O_ACCMODE) == O_RDONLY) {
            return res_int(-1, EINVAL);
        }
        if (len < 0) {
            return res_int(-1, EINVAL);
        }

        memset(&in, 0, sizeof(in));
        in.valid = FATTR_SIZE;
        in.size  = (uint64_t) len;

        return res_rc(fsim_setattr_fd(&g_fsim, (int) jint(req, "fd", -1),
                                      &in));
    }

    if (strcmp(op, "utimens") == 0 || strcmp(op, "utimensat") == 0 ||
        strcmp(op, "futimens") == 0) {
        struct fuse_setattr_in in;

        memset(&in, 0, sizeof(in));

        utimens_bits(req, "atype", "asec", "ansec", FATTR_ATIME,
                     FATTR_ATIME_NOW, &in.valid, &in.atime, &in.atimensec);
        utimens_bits(req, "mtype", "msec", "mnsec", FATTR_MTIME,
                     FATTR_MTIME_NOW, &in.valid, &in.mtime, &in.mtimensec);

        if (strcmp(op, "futimens") == 0) {
            return res_rc(fsim_setattr_fd(&g_fsim,
                                          (int) jint(req, "fd", -1), &in));
        }

        {
            struct fuse_attr_out out;
            uint64_t             start, nodeid;
            int                  rc;

            rc = dirfd_start(req, strcmp(op, "utimensat") == 0, &start);

            if (rc != 0) {
                return res_int(-1, rc);
            }

            rc = fsim_resolve_at(&g_fsim, start, jstr(req, "path"), 1,
                                 &nodeid, NULL);

            if (rc != 0) {
                return res_int(-1, rc);
            }

            memset(&out, 0, sizeof(out));
            rc = fuse_sim_setattr(&g_fsim.sim, nodeid, &in, &out);

            return rc == 0 ? res_int(0, 0) : res_int(-1, rc);
        }
    }

    if (strcmp(op, "access") == 0) {
        uint64_t nodeid;
        uint32_t mask = 0;
        int      rc;

        if (jbool(req, "r", 0)) {
            mask |= R_OK;
        }
        if (jbool(req, "w", 0)) {
            mask |= W_OK;
        }
        if (jbool(req, "x", 0)) {
            mask |= X_OK;
        }

        rc = fsim_resolve(&g_fsim, jstr(req, "path"), &nodeid, NULL);

        if (rc != 0) {
            return res_int(-1, rc);
        }

        rc = fuse_sim_access(&g_fsim.sim, nodeid, mask);

        return rc == 0 ? res_int(0, 0) : res_int(-1, rc);
    }

    /* --- namespace --- */

    if (strcmp(op, "mkdir") == 0 || strcmp(op, "mkdirat") == 0) {
        struct fuse_entry_out e;
        struct fsim_path      p;
        uint64_t              start;
        int                   rc;

        rc = dirfd_start(req, strcmp(op, "mkdirat") == 0, &start);

        if (rc != 0) {
            return res_int(-1, rc);
        }

        rc = fsim_walk(&g_fsim, start, jstr(req, "path"), 0, &p);

        if (rc != 0) {
            return res_int(-1, rc);
        }
        if (p.nodeid != 0 || p.leaf[0] == '\0') {
            return res_int(-1, EEXIST);
        }

        memset(&e, 0, sizeof(e));

        rc = fuse_sim_mkdir(&g_fsim.sim, p.parent, p.leaf,
                            (uint32_t) jint(req, "mode", 0777),
                            g_fsim.sim.cur_umask, &e);

        if (rc == 0) {
            fsim_hold(&g_fsim, e.nodeid);
        }

        return rc == 0 ? res_int(0, 0) : res_int(-1, rc);
    }

    if (strcmp(op, "mknod") == 0) {
        struct fuse_entry_out e;
        struct fsim_path      p;
        const char           *ft   = jstr(req, "ftype");
        uint32_t              mode = (uint32_t) jint(req, "mode", 0);
        uint32_t              rdev = 0;
        int                   rc;

        if (ft && strcmp(ft, "fifo") == 0) {
            mode |= S_IFIFO;
        } else if (ft && strcmp(ft, "blk") == 0) {
            mode |= S_IFBLK;
            rdev  = makedev(3, 4);
        } else if (ft && strcmp(ft, "chr") == 0) {
            mode |= S_IFCHR;
            rdev  = makedev(3, 4);
        } else {
            mode |= S_IFREG;
        }

        rc = fsim_walk(&g_fsim, FUSE_ROOT_ID, jstr(req, "path"), 0, &p);

        if (rc != 0) {
            return res_int(-1, rc);
        }
        if (p.nodeid != 0 || p.leaf[0] == '\0') {
            return res_int(-1, EEXIST);
        }

        memset(&e, 0, sizeof(e));

        rc = fuse_sim_mknod(&g_fsim.sim, p.parent, p.leaf, mode, rdev,
                            g_fsim.sim.cur_umask, &e);

        if (rc == 0) {
            fsim_hold(&g_fsim, e.nodeid);
        }

        return rc == 0 ? res_int(0, 0) : res_int(-1, rc);
    }

    if (strcmp(op, "rmdir") == 0 || strcmp(op, "unlink") == 0 ||
        strcmp(op, "unlinkat") == 0) {
        struct fsim_path p;
        uint64_t         start;
        int              isdir;
        int              rc;

        rc = dirfd_start(req, strcmp(op, "unlinkat") == 0, &start);

        if (rc != 0) {
            return res_int(-1, rc);
        }

        isdir = (strcmp(op, "rmdir") == 0) || jbool(req, "rmdir", 0);

        /* Neither form follows a symlink at the leaf: they remove the link
         * itself. */
        rc = fsim_walk(&g_fsim, start, jstr(req, "path"), 0, &p);

        if (rc != 0) {
            return res_int(-1, rc);
        }
        if (p.leaf[0] == '\0') {
            return res_int(-1, EBUSY);
        }
        if (p.nodeid == 0) {
            return res_int(-1, ENOENT);
        }

        rc = isdir ? fuse_sim_rmdir(&g_fsim.sim, p.parent, p.leaf)
                   : fuse_sim_unlink(&g_fsim.sim, p.parent, p.leaf);

        return rc == 0 ? res_int(0, 0) : res_int(-1, rc);
    }

    if (strcmp(op, "rename") == 0) {
        struct fsim_path oldp, newp;
        int              rc;

        rc = fsim_walk(&g_fsim, FUSE_ROOT_ID, jstr(req, "old"), 0, &oldp);

        if (rc != 0) {
            return res_int(-1, rc);
        }
        if (oldp.nodeid == 0) {
            return res_int(-1, ENOENT);
        }
        if (oldp.leaf[0] == '\0') {
            return res_int(-1, EBUSY);
        }

        rc = fsim_walk(&g_fsim, FUSE_ROOT_ID, jstr(req, "new"), 0, &newp);

        if (rc != 0) {
            return res_int(-1, rc);
        }
        if (newp.leaf[0] == '\0') {
            return res_int(-1, EBUSY);
        }

        rc = fuse_sim_rename(&g_fsim.sim, oldp.parent, oldp.leaf,
                             newp.parent, newp.leaf);

        return rc == 0 ? res_int(0, 0) : res_int(-1, rc);
    }

    if (strcmp(op, "link") == 0) {
        struct fuse_entry_out e;
        struct fsim_path      newp;
        uint64_t              target;
        int                   rc;

        /* link(2) does not follow a symlink at the source unless
         * AT_SYMLINK_FOLLOW is given. */
        rc = fsim_resolve_at(&g_fsim, FUSE_ROOT_ID, jstr(req, "old"),
                             jbool(req, "follow", 0), &target, NULL);

        if (rc != 0) {
            return res_int(-1, rc);
        }

        rc = fsim_walk(&g_fsim, FUSE_ROOT_ID, jstr(req, "new"), 0, &newp);

        if (rc != 0) {
            return res_int(-1, rc);
        }
        if (newp.nodeid != 0 || newp.leaf[0] == '\0') {
            return res_int(-1, EEXIST);
        }

        memset(&e, 0, sizeof(e));

        rc = fuse_sim_link(&g_fsim.sim, target, newp.parent, newp.leaf, &e);

        if (rc == 0) {
            fsim_hold(&g_fsim, e.nodeid);
        }

        return rc == 0 ? res_int(0, 0) : res_int(-1, rc);
    }

    if (strcmp(op, "symlink") == 0) {
        struct fuse_entry_out e;
        struct fsim_path      p;
        int                   rc;

        rc = fsim_walk(&g_fsim, FUSE_ROOT_ID, jstr(req, "path"), 0, &p);

        if (rc != 0) {
            return res_int(-1, rc);
        }
        if (p.nodeid != 0 || p.leaf[0] == '\0') {
            return res_int(-1, EEXIST);
        }

        memset(&e, 0, sizeof(e));

        rc = fuse_sim_symlink(&g_fsim.sim, p.parent, p.leaf,
                              jstr(req, "target"), &e);

        if (rc == 0) {
            fsim_hold(&g_fsim, e.nodeid);
        }

        return rc == 0 ? res_int(0, 0) : res_int(-1, rc);
    }

    if (strcmp(op, "readlink") == 0) {
        struct fsim_path p;
        char             target[FSIM_PATH_MAX];
        int              rc;

        /* readlink never follows the leaf -- that IS the leaf it reads. */
        rc = fsim_walk(&g_fsim, FUSE_ROOT_ID, jstr(req, "path"), 0, &p);

        if (rc != 0) {
            return res_int(-1, rc);
        }
        if (p.nodeid == 0) {
            return res_int(-1, ENOENT);
        }
        if (!S_ISLNK(p.entry.attr.mode)) {
            return res_int(-1, EINVAL);
        }

        rc = fuse_sim_readlink(&g_fsim.sim, p.nodeid, target, sizeof(target));

        if (rc != 0) {
            return res_int(-1, rc);
        }

        {
            json_t *res = res_int((long long) strlen(target), 0);

            json_object_set_new(res, "target", json_string(target));
            return res;
        }
    }

    /* --- directory streams --- */

    if (strcmp(op, "opendir") == 0) {
        return res_rc(fsim_opendir(&g_fsim, jstr(req, "path")));
    }

    if (strcmp(op, "readdir") == 0) {
        struct name_collect c;
        json_t             *res;
        int                 rc;

        c.arr = json_array();

        rc = fsim_readdir_all(&g_fsim, (int) jint(req, "sid", -1),
                              collect_name, &c);

        if (rc < 0) {
            json_decref(c.arr);
            return res_int(-1, -rc);
        }

        res = res_int(0, 0);
        json_object_set_new(res, "names", c.arr);

        return res;
    }

    if (strcmp(op, "closedir") == 0) {
        return res_rc(fsim_closedir(&g_fsim, (int) jint(req, "sid", -1)));
    }

    if (strcmp(op, "rewinddir") == 0 || strcmp(op, "seekdir") == 0) {
        struct fsim_dir *d = fsim_dir_get(&g_fsim, (int) jint(req, "sid", -1));

        if (!d) {
            return res_int(-1, EBADF);
        }

        d->cookie = (strcmp(op, "seekdir") == 0)
            ? (uint64_t) jint(req, "loc", 0) : 0;

        return res_int(0, 0);
    }

    if (strcmp(op, "telldir") == 0) {
        struct fsim_dir *d = fsim_dir_get(&g_fsim, (int) jint(req, "sid", -1));

        if (!d) {
            return res_int(-1, EBADF);
        }

        return res_int((long long) d->cookie, 0);
    }

    /* --- locks --- */

    if (strcmp(op, "fcntl_lock") == 0) {
        struct fsim_ofd      *o = fsim_fd_get(&g_fsim,
                                              (int) jint(req, "fd", -1));
        struct fuse_file_lock conflict;
        const char           *cmds   = jstr(req, "cmd");
        const char           *type   = jstr(req, "type");
        long long             start  = jint(req, "start", 0);
        long long             len    = jint(req, "len", 0);
        uint32_t              opcode = FUSE_SETLK;
        uint32_t              ltype  = F_RDLCK;
        uint64_t              end;
        int                   is_getlk;
        int                   rc;

        if (!o) {
            return res_int(-1, EBADF);
        }

        if (cmds && strcmp(cmds, "setlkw") == 0) {
            opcode = FUSE_SETLKW;
        } else if (cmds && strcmp(cmds, "getlk") == 0) {
            opcode = FUSE_GETLK;
        }

        is_getlk = (opcode == FUSE_GETLK);

        if (type && strcmp(type, "wr") == 0) {
            ltype = F_WRLCK;
        } else if (type && strcmp(type, "un") == 0) {
            ltype = F_UNLCK;
        }

        /* A lock being ACQUIRED has to be compatible with how the descriptor
         * was opened, and fcntl(2) enforces that itself -- the request never
         * reaches the filesystem.  F_GETLK only asks whether a lock would
         * conflict, so it is exempt.  Without this the harness would put a
         * lock on the wire that a real mount rejects with EBADF.
         *
         * Note what is deliberately NOT checked here: locking a non-regular
         * file.  That rule lives below the kernel in chimera, so leaving it
         * unchecked keeps it visible as a server divergence rather than
         * hiding it behind a harness-side answer. */
        if (!is_getlk &&
            ((ltype == F_RDLCK && (o->flags & O_ACCMODE) == O_WRONLY) ||
             (ltype == F_WRLCK && (o->flags & O_ACCMODE) == O_RDONLY))) {
            return res_int(-1, EBADF);
        }

        /* POSIX ranges are [start, start+len); FUSE's are inclusive, with a
         * sentinel for "to end of file". */
        end = (len > 0) ? (uint64_t) (start + len - 1)
                        : FUSE_SIM_LOCK_EOF;

        memset(&conflict, 0, sizeof(conflict));

        rc = fuse_sim_lock(&g_fsim.sim, o->nodeid, o->fh,
                           g_fsim.sim.cur_pid, opcode, ltype,
                           (uint64_t) start, end, g_fsim.sim.cur_pid,
                           &conflict);

        if (rc != 0) {
            return res_int(-1, rc);
        }

        {
            json_t *res = res_int(0, 0);

            if (is_getlk) {
                const char *lt = "un";

                if (conflict.type == F_RDLCK) {
                    lt = "rd";
                } else if (conflict.type == F_WRLCK) {
                    lt = "wr";
                }

                json_object_set_new(res, "l_type", json_string(lt));
                json_object_set_new(res, "l_start",
                                    json_integer((long long) conflict.start));
                json_object_set_new(res, "l_len",
                                    json_integer(
                                        conflict.end == FUSE_SIM_LOCK_EOF
                                        ? 0
                                        : (long long) (conflict.end -
                                                       conflict.start + 1)));
                json_object_set_new(res, "l_pid",
                                    json_integer(conflict.pid));
            }

            return res;
        }
    }

    if (strcmp(op, "lockf") == 0) {
        struct fsim_ofd *o      = fsim_fd_get(&g_fsim, (int) jint(req, "fd", -1));
        const char      *cmds   = jstr(req, "cmd");
        long long        len    = jint(req, "len", 0);
        uint32_t         opcode = FUSE_SETLKW;
        uint32_t         ltype  = F_WRLCK;
        uint64_t         start, end;
        int              rc;

        if (!o) {
            return res_int(-1, EBADF);
        }

        /* lockf(3) is defined only on a descriptor open for writing, and
         * that is checked before the filesystem is reached. */
        if ((o->flags & O_ACCMODE) == O_RDONLY) {
            return res_int(-1, EBADF);
        }

        /* lockf locks relative to the current file position and is always
         * exclusive. */
        start = o->offset;

        if (cmds && strcmp(cmds, "tlock") == 0) {
            opcode = FUSE_SETLK;
        } else if (cmds && strcmp(cmds, "ulock") == 0) {
            opcode = FUSE_SETLK;
            ltype  = F_UNLCK;
        } else if (cmds && strcmp(cmds, "test") == 0) {
            opcode = FUSE_GETLK;
        }

        end = (len > 0) ? start + (uint64_t) len - 1 : FUSE_SIM_LOCK_EOF;

        if (opcode == FUSE_GETLK) {
            struct fuse_file_lock conflict;

            memset(&conflict, 0, sizeof(conflict));

            rc = fuse_sim_lock(&g_fsim.sim, o->nodeid, o->fh,
                               g_fsim.sim.cur_pid, opcode, ltype, start, end,
                               g_fsim.sim.cur_pid, &conflict);

            if (rc != 0) {
                return res_int(-1, rc);
            }

            /* F_TEST reports EACCES/EAGAIN when the region is locked by
             * someone else, and 0 when it is free. */
            return conflict.type == F_UNLCK ? res_int(0, 0)
                                            : res_int(-1, EAGAIN);
        }

        rc = fuse_sim_lock(&g_fsim.sim, o->nodeid, o->fh, g_fsim.sim.cur_pid,
                           opcode, ltype, start, end, g_fsim.sim.cur_pid,
                           NULL);

        return rc == 0 ? res_int(0, 0) : res_int(-1, rc);
    }

    if (strcmp(op, "copy_range") == 0) {
        struct fsim_ofd *in  = fsim_fd_get(&g_fsim, (int) jint(req, "fd_in", -1));
        struct fsim_ofd *out = fsim_fd_get(&g_fsim,
                                           (int) jint(req, "fd_out", -1));
        long long        off_in  = jint(req, "off_in", 0);
        long long        off_out = jint(req, "off_out", 0);
        long long        len     = jint(req, "len", 0);
        uint32_t         copied  = 0;
        int              rc;

        /* vfs_copy_file_range() validates the descriptors before asking the
         * filesystem, and Linux orders those checks ahead of the backend's
         * EOPNOTSUPP.  The source must be readable, the destination writable,
         * and an append-mode destination is rejected outright. */
        if (!in || !out) {
            return res_int(-1, EBADF);
        }
        if ((in->flags & O_ACCMODE) == O_WRONLY ||
            (out->flags & O_ACCMODE) == O_RDONLY ||
            (out->flags & O_APPEND)) {
            return res_int(-1, EBADF);
        }
        if (off_in < 0 || off_out < 0) {
            return res_int(-1, EINVAL);
        }

        /* Overlapping ranges within one file are rejected by the kernel. */
        if (in->nodeid == out->nodeid &&
            off_in < off_out + len && off_out < off_in + len) {
            return res_int(-1, EINVAL);
        }

        rc = fuse_sim_copy_file_range(&g_fsim.sim, in->nodeid, in->fh,
                                      (uint64_t) off_in, out->nodeid,
                                      out->fh, (uint64_t) off_out,
                                      (uint64_t) len, &copied);

        return rc == 0 ? res_int(copied, 0) : res_int(-1, rc);
    }

    if (strcmp(op, "clone_range") == 0) {
        struct fsim_ofd *dst = fsim_fd_get(&g_fsim,
                                           (int) jint(req, "dst_fd", -1));
        struct fsim_ofd *src = fsim_fd_get(&g_fsim,
                                           (int) jint(req, "src_fd", -1));

        /*
         * FICLONERANGE is an ioctl, and do_clone_file_range() screens it in
         * the VFS before the filesystem is ever consulted: descriptor
         * validity, object type, then access mode -- and only THEN the
         * ->remap_file_range test that decides whether the backing
         * filesystem can reflink at all.  Those screens belong to the kernel
         * side of this harness, so run them here.
         */
        if (!dst || !src) {
            return res_int(-1, EBADF);
        }
        if (dst->isdir || src->isdir) {
            return res_int(-1, EISDIR);
        }
        if ((src->flags & O_ACCMODE) == O_WRONLY ||
            (dst->flags & O_ACCMODE) == O_RDONLY ||
            (dst->flags & O_APPEND)) {
            return res_int(-1, EBADF);
        }

        /*
         * fuse_file_operations has no ->remap_file_range, so a real FUSE
         * mount stops here: there is no FICLONERANGE request on the wire and
         * no ioctl passthrough in this server to carry one.  EOPNOTSUPP is
         * what the kernel returns, so it is what the harness returns --
         * emulating the clone with COPY_FILE_RANGE would score the emulation
         * rather than the server.
         */
        return res_int(-1, EOPNOTSUPP);
    }

    return res_int(-1, ENOSYS);
} /* fuse_exec_op */

int
main(
    int    argc,
    char **argv)
{
    char   *line = NULL;
    size_t  cap  = 0;
    json_t *req, *res;
    FILE   *proto_out;
    int     out_fd;

    (void) argc;
    (void) argv;

    /* stdout is the protocol channel; chimera's logger also writes there, so
     * hand the protocol a private duplicate and point the logger at stderr
     * (posix_driver.c uses the identical arrangement). */
    out_fd    = dup(STDOUT_FILENO);
    proto_out = out_fd >= 0 ? fdopen(out_fd, "w") : NULL;

    if (!proto_out || dup2(STDERR_FILENO, STDOUT_FILENO) < 0) {
        fprintf(stderr, "fuse_quint_driver: protocol stream setup failed\n");
        if (proto_out) {
            fclose(proto_out);
        } else if (out_fd >= 0) {
            close(out_fd);
        }
        return 1;
    }

    memset(&g_fsim, 0, sizeof(g_fsim));
    fsim_init(&g_fsim);
    fsim_start(&g_fsim, g_fsim.fsname);

    fprintf(proto_out, "{\"ready\": true, \"blocksize\": %d}\n",
            FUSE_SIM_BLOCK_SIZE);
    fflush(proto_out);

    while (getline(&line, &cap, stdin) != -1) {
        json_error_t jerr;
        char        *dump;

        req = json_loads(line, 0, &jerr);

        if (!req) {
            fprintf(stderr, "fuse_quint_driver: bad request: %s\n", jerr.text);
            continue;
        }

        res = fuse_exec_op(req);

        if (!res) {
            json_decref(req);
            break;   /* shutdown */
        }

        dump = json_dumps(res, JSON_COMPACT);

        fprintf(proto_out, "%s\n", dump);
        fflush(proto_out);

        free(dump);
        json_decref(res);
        json_decref(req);
    }

    fsim_forget_all(&g_fsim);
    fuse_sim_close(&g_fsim.sim);

    fclose(proto_out);
    free(line);

    return 0;
} /* main */
