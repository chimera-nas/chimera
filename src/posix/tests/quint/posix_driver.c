// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Replay driver for the POSIX model-based test suite (see DESIGN-POSIX.md).
 *
 * Speaks a line-delimited JSON protocol on stdin/stdout: one request object
 * per line, one response object per line.  posix_replay.py generates the
 * requests from a Quint trace and compares the responses against the model's
 * expectations; this program is a thin, stateless executor over the
 * chimera_posix_* client API backed by an in-process memfs mount.
 *
 * The model's two processes are realized as per-operation credential/umask
 * switches on a single thread (chimera_posix_set_cred and chimera_posix_umask
 * are thread-local): every request carries a pid, and the driver installs
 * that pid's credential and umask before issuing the call.  This matches the
 * DESIGN-POSIX.md harness convention of "distinct client instances with
 * distinct creds"; genuinely separate OS processes cannot share one memfs.
 *
 * Driver-side state is limited to what cannot cross the JSON boundary:
 * per-pid credentials/umasks and the CHIMERA_DIR* table for directory
 * streams.  File descriptors are chimera's own integers and travel verbatim;
 * the Python side owns the model->real mapping.
 */

#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>     /* struct statvfs for statvfs/fstatvfs */
#include <sys/sysmacros.h>   /* makedev() for block/char device mknod */
#include <sys/uio.h>         /* struct iovec for the vectored read/write ops */
#include <sys/vfs.h>         /* struct statfs for statfs/fstatfs */
#include <unistd.h>

#include <jansson.h>

#include "posix/posix_internal.h"
#include "client/client.h"
#include "server/server.h"
#include "common/logging.h"
#include "common/platform.h"
#include "common/tcp_flavor.h"
#include "prometheus-c.h"

#define DRIVER_BLOCK_SIZE 4096
#define MAX_PIDS          4
#define MAX_DIRS          64

static struct chimera_vfs_cred driver_creds[MAX_PIDS];
static mode_t                  driver_umasks[MAX_PIDS];
static CHIMERA_DIR            *driver_dirs[MAX_DIRS];

/* JSON responses go to a private dup of the original stdout; fd 1 itself is
 * redirected to stderr in main() because chimera's logger writes to stdout
 * by default and would otherwise corrupt the protocol stream. */
static FILE                   *proto_out;

/* Live filesystem/mount identity.  The batch "newfs" op cycles a fresh,
 * uniquely-named filesystem per trace (fresh fsid -> fresh FH mount-id) so no
 * content or cached FH leaks across traces -- the same isolation the NFS/SMB
 * MBT batches get from a per-trace fsname.  Set once in main(). */
static const char             *g_module;        /* VFS module (memfs/...)     */
static int                     g_nfs_version;   /* 0 = direct; 3/4 = loopback */
static int                     g_fs_counter;    /* bumped per newfs -> fsN     */
static char                    g_fsname[32] = "fs0";
static struct chimera_vfs_cred g_root_cred;

/* Decode standard base64 (no whitespace); returns length or -1. */
static int
b64_decode(
    const char    *in,
    unsigned char *out,
    size_t         out_cap)
{
    static const char tbl[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    int               acc = 0, nbits = 0;
    size_t            n = 0;

    for (const char *p = in; *p && *p != '='; p++) {
        const char *hit = strchr(tbl, *p);

        if (!hit) {
            return -1;
        }
        acc    = (acc << 6) | (int) (hit - tbl);
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
    static const char tbl[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t            olen = 4 * ((len + 2) / 3) + 1;
    char             *out  = malloc(olen);
    size_t            i, o = 0;

    for (i = 0; i + 2 < len; i += 3) {
        unsigned v = (in[i] << 16) | (in[i + 1] << 8) | in[i + 2];
        out[o++] = tbl[(v >> 18) & 63];
        out[o++] = tbl[(v >> 12) & 63];
        out[o++] = tbl[(v >> 6) & 63];
        out[o++] = tbl[v & 63];
    }
    if (i + 1 == len) {
        unsigned v = in[i] << 16;
        out[o++] = tbl[(v >> 18) & 63];
        out[o++] = tbl[(v >> 12) & 63];
        out[o++] = '=';
        out[o++] = '=';
    } else if (i + 2 == len) {
        unsigned v = (in[i] << 16) | (in[i + 1] << 8);
        out[o++] = tbl[(v >> 18) & 63];
        out[o++] = tbl[(v >> 12) & 63];
        out[o++] = tbl[(v >> 6) & 63];
        out[o++] = '=';
    }
    out[o] = '\0';
    return out;
} /* b64_encode */

/* Split a flat buffer across up to two iovecs so the vectored read/write ops
 * exercise the client's iovec-splitting path (and the readv completion's
 * multi-iovec copy loop).  Always returns at least one iovec: readv/writev
 * reject iovcnt <= 0 with EINVAL, which the scalar-length model never asks
 * for. */
static int
split_iovec(
    struct iovec *iov,
    void         *buf,
    size_t        len)
{
    if (len < 2) {
        iov[0].iov_base = buf;
        iov[0].iov_len  = len;
        return 1;
    }

    size_t half = len / 2;

    iov[0].iov_base = buf;
    iov[0].iov_len  = half;
    iov[1].iov_base = (char *) buf + half;
    iov[1].iov_len  = len - half;
    return 2;
} /* split_iovec */

static int
jint(
    json_t     *req,
    const char *key,
    int         dflt)
{
    json_t *v = json_object_get(req, key);

    return v && json_is_integer(v) ? (int) json_integer_value(v) : dflt;
} /* jint */

static long long
jint64(
    json_t     *req,
    const char *key,
    long long   dflt)
{
    json_t *v = json_object_get(req, key);

    return v && json_is_integer(v) ? (long long) json_integer_value(v) : dflt;
} /* jint64 */

static const char *
jstr(
    json_t     *req,
    const char *key)
{
    json_t *v = json_object_get(req, key);

    return v && json_is_string(v) ? json_string_value(v) : NULL;
} /* jstr */

static int
jbool(
    json_t     *req,
    const char *key,
    int         dflt)
{
    json_t *v = json_object_get(req, key);

    return v && json_is_boolean(v) ? json_is_true(v) : dflt;
} /* jbool */

/* Install the requesting model pid's credential and umask on this thread. */
static void
apply_pid(json_t *req)
{
    int pid = jint(req, "pid", 0);

    if (pid < 0 || pid >= MAX_PIDS) {
        pid = 0;
    }
    chimera_posix_set_cred(&driver_creds[pid]);
    (void) chimera_posix_umask(driver_umasks[pid]);
} /* apply_pid */

/* Reset every model pid to the root credential/umask and normalize the mount
 * root to the model's fsInit(0777, 0, 0).  Shared by the initial setup and the
 * per-trace newfs reset. */
static int
normalize_root(void)
{
    for (int i = 0; i < MAX_PIDS; i++) {
        driver_creds[i]  = g_root_cred;
        driver_umasks[i] = 0;
    }
    chimera_posix_set_cred(&g_root_cred);
    (void) chimera_posix_umask(0);
    if (chimera_posix_chmod("/test", 0777) != 0 ||
        chimera_posix_chown("/test", 0, 0) != 0) {
        return -1;
    }
    return 0;
} /* normalize_root */

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

static void
stat_fill(
    json_t            *res,
    const struct stat *st)
{
    const char *ftype = "unk";

    switch (st->st_mode & S_IFMT) {
        case S_IFREG: ftype  = "reg"; break;
        case S_IFDIR: ftype  = "dir"; break;
        case S_IFLNK: ftype  = "lnk"; break;
        case S_IFIFO: ftype  = "fifo"; break;
        case S_IFSOCK: ftype = "sock"; break;
        case S_IFBLK: ftype  = "blk"; break;
        case S_IFCHR: ftype  = "chr"; break;
    } /* switch */

    json_object_set_new(res, "ftype", json_string(ftype));
    json_object_set_new(res, "mode", json_integer(st->st_mode & 07777));
    json_object_set_new(res, "ino", json_integer((long long) st->st_ino));
    json_object_set_new(res, "dev", json_integer((long long) st->st_dev));
    json_object_set_new(res, "nlink", json_integer((long long) st->st_nlink));
    json_object_set_new(res, "uid", json_integer(st->st_uid));
    json_object_set_new(res, "gid", json_integer(st->st_gid));
    json_object_set_new(res, "size", json_integer((long long) st->st_size));

    json_t *at = json_array(), *mt = json_array(), *ct = json_array();

    json_array_append_new(at, json_integer(CHIMERA_STAT_ATIM(*st).tv_sec));
    json_array_append_new(at, json_integer(CHIMERA_STAT_ATIM(*st).tv_nsec));
    json_array_append_new(mt, json_integer(CHIMERA_STAT_MTIM(*st).tv_sec));
    json_array_append_new(mt, json_integer(CHIMERA_STAT_MTIM(*st).tv_nsec));
    json_array_append_new(ct, json_integer(CHIMERA_STAT_CTIM(*st).tv_sec));
    json_array_append_new(ct, json_integer(CHIMERA_STAT_CTIM(*st).tv_nsec));
    json_object_set_new(res, "atime", at);
    json_object_set_new(res, "mtime", mt);
    json_object_set_new(res, "ctime", ct);
} /* stat_fill */

static void
ts_from_req(
    json_t          *req,
    const char      *typekey,
    const char      *seckey,
    const char      *nseckey,
    struct timespec *ts)
{
    const char *type = jstr(req, typekey);

    if (type && strcmp(type, "omit") == 0) {
        ts->tv_sec  = 0;
        ts->tv_nsec = UTIME_OMIT;
    } else if (type && strcmp(type, "val") == 0) {
        ts->tv_sec  = (time_t) jint64(req, seckey, 0);
        ts->tv_nsec = jint64(req, nseckey, 0);
    } else {
        ts->tv_sec  = 0;
        ts->tv_nsec = UTIME_NOW;
    }
} /* ts_from_req */

static json_t *
handle(json_t *req)
{
    const char *op = jstr(req, "op");

    if (!op) {
        return res_int(-1, EINVAL);
    }

    if (strcmp(op, "setcred") == 0) {
        int      pid  = jint(req, "pid", 0);
        json_t  *gids = json_object_get(req, "gids");
        uint32_t g[CHIMERA_VFS_CRED_MAX_GIDS];
        uint32_t ngids = 0;

        if (pid < 0 || pid >= MAX_PIDS) {
            return res_int(-1, EINVAL);
        }
        if (gids && json_is_array(gids)) {
            size_t  i;
            json_t *v;
            json_array_foreach(gids, i, v)
            {
                if (ngids < CHIMERA_VFS_CRED_MAX_GIDS) {
                    g[ngids++] = (uint32_t) json_integer_value(v);
                }
            }
        }
        chimera_vfs_cred_init_unix(&driver_creds[pid],
                                   (uint32_t) jint(req, "uid", 0),
                                   (uint32_t) jint(req, "gid", 0),
                                   ngids, g);
        return res_int(0, 0);
    }

    if (strcmp(op, "umask") == 0) {
        int pid = jint(req, "pid", 0);
        int old;

        if (pid < 0 || pid >= MAX_PIDS) {
            return res_int(-1, EINVAL);
        }
        old                = (int) driver_umasks[pid];
        driver_umasks[pid] = (mode_t) jint(req, "mask", 0);
        return res_int(old, 0);
    }

    apply_pid(req);

    if (strcmp(op, "open") == 0) {
        int ret = chimera_posix_open(jstr(req, "path"),
                                     jint(req, "flags", 0),
                                     jint(req, "mode", 0));
        return res_int(ret, errno);
    }

    if (strcmp(op, "openat") == 0) {
        int ret = chimera_posix_openat(jint(req, "dirfd", -1),
                                       jstr(req, "path"),
                                       jint(req, "flags", 0),
                                       jint(req, "mode", 0));
        return res_int(ret, errno);
    }

    if (strcmp(op, "close") == 0) {
        return res_int(chimera_posix_close(jint(req, "fd", -1)), errno);
    }

    if (strcmp(op, "dup") == 0) {
        return res_int(chimera_posix_dup(jint(req, "fd", -1)), errno);
    }

    if (strcmp(op, "dup2") == 0) {
        return res_int(chimera_posix_dup2(jint(req, "fd", -1),
                                          jint(req, "nfd", -1)), errno);
    }

    if (strcmp(op, "lseek") == 0) {
        const char *wh     = jstr(req, "whence");
        int         whence = SEEK_SET;

        if (wh && strcmp(wh, "cur") == 0) {
            whence = SEEK_CUR;
        } else if (wh && strcmp(wh, "end") == 0) {
            whence = SEEK_END;
        } else if (wh && strcmp(wh, "data") == 0) {
            whence = SEEK_DATA;
        } else if (wh && strcmp(wh, "hole") == 0) {
            whence = SEEK_HOLE;
        }
        return res_int(chimera_posix_lseek(jint(req, "fd", -1),
                                           (off_t) jint64(req, "off", 0),
                                           whence), errno);
    }

    if (strcmp(op, "read") == 0 || strcmp(op, "pread") == 0) {
        size_t  len = (size_t) jint64(req, "len", 0);
        char   *buf = malloc(len ? len : 1);
        ssize_t n;

        if (strcmp(op, "read") == 0) {
            n = chimera_posix_read(jint(req, "fd", -1), buf, len);
        } else {
            n = chimera_posix_pread(jint(req, "fd", -1), buf, len,
                                    (off_t) jint64(req, "off", 0));
        }

        json_t *res = res_int(n, errno);

        if (n >= 0) {
            char *enc = b64_encode((unsigned char *) buf, (size_t) n);
            json_object_set_new(res, "data", json_string(enc));
            free(enc);
        }
        free(buf);
        return res;
    }

    if (strcmp(op, "write") == 0 || strcmp(op, "pwrite") == 0) {
        const char    *data = jstr(req, "data");
        size_t         cap  = data ? strlen(data) : 0;
        unsigned char *buf  = malloc(cap ? cap : 1);
        int            len  = b64_decode(data ? data : "", buf, cap ? cap : 1);
        ssize_t        n;

        if (len < 0) {
            free(buf);
            return res_int(-1, EINVAL);
        }
        if (strcmp(op, "write") == 0) {
            n = chimera_posix_write(jint(req, "fd", -1), buf, (size_t) len);
        } else {
            n = chimera_posix_pwrite(jint(req, "fd", -1), buf, (size_t) len,
                                     (off_t) jint64(req, "off", 0));
        }
        free(buf);
        return res_int(n, errno);
    }

    if (strcmp(op, "readv") == 0 || strcmp(op, "preadv") == 0) {
        size_t       len = (size_t) jint64(req, "len", 0);
        char        *buf = malloc(len ? len : 1);
        struct iovec iov[2];
        int          niov = split_iovec(iov, buf, len);
        ssize_t      n;

        if (strcmp(op, "readv") == 0) {
            n = chimera_posix_readv(jint(req, "fd", -1), iov, niov);
        } else {
            n = chimera_posix_preadv2(jint(req, "fd", -1), iov, niov,
                                      (off_t) jint64(req, "off", 0), 0);
        }

        json_t *res = res_int(n, errno);

        if (n >= 0) {
            char *enc = b64_encode((unsigned char *) buf, (size_t) n);
            json_object_set_new(res, "data", json_string(enc));
            free(enc);
        }
        free(buf);
        return res;
    }

    if (strcmp(op, "writev") == 0 || strcmp(op, "pwritev") == 0) {
        const char    *data = jstr(req, "data");
        size_t         cap  = data ? strlen(data) : 0;
        unsigned char *buf  = malloc(cap ? cap : 1);
        int            len  = b64_decode(data ? data : "", buf, cap ? cap : 1);
        struct iovec   iov[2];
        int            niov;
        ssize_t        n;

        if (len < 0) {
            free(buf);
            return res_int(-1, EINVAL);
        }
        niov = split_iovec(iov, buf, (size_t) len);
        if (strcmp(op, "writev") == 0) {
            n = chimera_posix_writev(jint(req, "fd", -1), iov, niov);
        } else {
            n = chimera_posix_pwritev2(jint(req, "fd", -1), iov, niov,
                                       (off_t) jint64(req, "off", 0), 0);
        }
        free(buf);
        return res_int(n, errno);
    }

    if (strcmp(op, "truncate") == 0) {
        return res_int(chimera_posix_truncate(jstr(req, "path"),
                                              (off_t) jint64(req, "len", 0)),
                       errno);
    }

    if (strcmp(op, "ftruncate") == 0) {
        return res_int(chimera_posix_ftruncate(jint(req, "fd", -1),
                                               (off_t) jint64(req, "len", 0)),
                       errno);
    }

    if (strcmp(op, "statfs") == 0) {
        struct statfs sb;
        return res_int(chimera_posix_statfs(jstr(req, "path"), &sb), errno);
    }

    if (strcmp(op, "fstatfs") == 0) {
        struct statfs sb;
        return res_int(chimera_posix_fstatfs(jint(req, "fd", -1), &sb), errno);
    }

    if (strcmp(op, "statvfs") == 0) {
        struct statvfs sb;
        return res_int(chimera_posix_statvfs(jstr(req, "path"), &sb), errno);
    }

    if (strcmp(op, "fstatvfs") == 0) {
        struct statvfs sb;
        return res_int(chimera_posix_fstatvfs(jint(req, "fd", -1), &sb), errno);
    }

    if (strcmp(op, "stat") == 0 || strcmp(op, "fstat") == 0 ||
        strcmp(op, "fstatat") == 0) {
        struct stat st;
        int         ret;

        memset(&st, 0, sizeof(st));
        if (strcmp(op, "fstat") == 0) {
            ret = chimera_posix_fstat(jint(req, "fd", -1), &st);
        } else if (strcmp(op, "fstatat") == 0) {
            ret = chimera_posix_fstatat(jint(req, "dirfd", -1),
                                        jstr(req, "path"), &st,
                                        jbool(req, "follow", 1) ? 0
                                        : AT_SYMLINK_NOFOLLOW);
        } else if (jbool(req, "follow", 1)) {
            ret = chimera_posix_stat(jstr(req, "path"), &st);
        } else {
            ret = chimera_posix_lstat(jstr(req, "path"), &st);
        }

        json_t *res = res_int(ret, errno);

        if (ret == 0) {
            stat_fill(res, &st);
        }
        return res;
    }

    if (strcmp(op, "chmod") == 0) {
        return res_int(chimera_posix_chmod(jstr(req, "path"),
                                           (mode_t) jint(req, "mode", 0)),
                       errno);
    }

    if (strcmp(op, "fchmod") == 0) {
        return res_int(chimera_posix_fchmod(jint(req, "fd", -1),
                                            (mode_t) jint(req, "mode", 0)),
                       errno);
    }

    if (strcmp(op, "chown") == 0) {
        if (jbool(req, "follow", 1)) {
            return res_int(chimera_posix_chown(jstr(req, "path"),
                                               (uid_t) jint(req, "uid", -1),
                                               (gid_t) jint(req, "gid", -1)),
                           errno);
        }
        return res_int(chimera_posix_lchown(jstr(req, "path"),
                                            (uid_t) jint(req, "uid", -1),
                                            (gid_t) jint(req, "gid", -1)),
                       errno);
    }

    if (strcmp(op, "fchown") == 0) {
        return res_int(chimera_posix_fchown(jint(req, "fd", -1),
                                            (uid_t) jint(req, "uid", -1),
                                            (gid_t) jint(req, "gid", -1)),
                       errno);
    }

    if (strcmp(op, "utimens") == 0 || strcmp(op, "futimens") == 0 ||
        strcmp(op, "utimensat") == 0) {
        struct timespec times[2];

        ts_from_req(req, "atype", "asec", "ansec", &times[0]);
        ts_from_req(req, "mtype", "msec", "mnsec", &times[1]);
        if (strcmp(op, "futimens") == 0) {
            return res_int(chimera_posix_futimens(jint(req, "fd", -1), times),
                           errno);
        }
        /* utimensat with a real dirfd resolves a relative path against it;
         * plain utimens uses AT_FDCWD. */
        return res_int(chimera_posix_utimensat(
                           strcmp(op, "utimensat") == 0 ?
                           jint(req, "dirfd", -1) : AT_FDCWD,
                           jstr(req, "path"), times, 0), errno);
    }

    if (strcmp(op, "access") == 0) {
        int mode = 0;

        if (jbool(req, "r", 0)) {
            mode |= R_OK;
        }
        if (jbool(req, "w", 0)) {
            mode |= W_OK;
        }
        if (jbool(req, "x", 0)) {
            mode |= X_OK;
        }
        return res_int(chimera_posix_faccessat(AT_FDCWD, jstr(req, "path"),
                                               mode,
                                               jbool(req, "eff", 0)
                                               ? AT_EACCESS : 0), errno);
    }

    if (strcmp(op, "mkdir") == 0) {
        return res_int(chimera_posix_mkdir(jstr(req, "path"),
                                           (mode_t) jint(req, "mode", 0)),
                       errno);
    }

    if (strcmp(op, "mkdirat") == 0) {
        return res_int(chimera_posix_mkdirat(jint(req, "dirfd", -1),
                                             jstr(req, "path"),
                                             (mode_t) jint(req, "mode", 0)),
                       errno);
    }

    if (strcmp(op, "mknod") == 0) {
        const char *ft   = jstr(req, "ftype");
        mode_t      mode = (mode_t) jint(req, "mode", 0);
        dev_t       dev  = 0;

        if (ft && strcmp(ft, "fifo") == 0) {
            mode |= S_IFIFO;
        } else if (ft && strcmp(ft, "blk") == 0) {
            mode |= S_IFBLK;
            dev   = makedev(3, 4);
        } else if (ft && strcmp(ft, "chr") == 0) {
            mode |= S_IFCHR;
            dev   = makedev(3, 4);
        } else {
            mode |= S_IFREG;
        }
        return res_int(chimera_posix_mknod(jstr(req, "path"), mode, dev),
                       errno);
    }

    if (strcmp(op, "symlink") == 0) {
        return res_int(chimera_posix_symlink(jstr(req, "target"),
                                             jstr(req, "path")), errno);
    }

    if (strcmp(op, "link") == 0) {
        if (jbool(req, "follow", 0)) {
            return res_int(chimera_posix_linkat(AT_FDCWD, jstr(req, "old"),
                                                AT_FDCWD, jstr(req, "new"),
                                                AT_SYMLINK_FOLLOW), errno);
        }
        return res_int(chimera_posix_link(jstr(req, "old"),
                                          jstr(req, "new")), errno);
    }

    if (strcmp(op, "unlink") == 0) {
        return res_int(chimera_posix_unlink(jstr(req, "path")), errno);
    }

    if (strcmp(op, "unlinkat") == 0) {
        return res_int(chimera_posix_unlinkat(jint(req, "dirfd", -1),
                                              jstr(req, "path"),
                                              jbool(req, "rmdir", 0)
                                              ? AT_REMOVEDIR : 0), errno);
    }

    if (strcmp(op, "rmdir") == 0) {
        return res_int(chimera_posix_rmdir(jstr(req, "path")), errno);
    }

    if (strcmp(op, "rename") == 0) {
        return res_int(chimera_posix_rename(jstr(req, "old"),
                                            jstr(req, "new")), errno);
    }

    if (strcmp(op, "readlink") == 0) {
        char    buf[4096];
        ssize_t n = chimera_posix_readlink(jstr(req, "path"), buf,
                                           sizeof(buf) - 1);
        json_t *res = res_int(n, errno);

        if (n >= 0) {
            buf[n] = '\0';
            json_object_set_new(res, "target", json_string(buf));
        }
        return res;
    }

    if (strcmp(op, "opendir") == 0) {
        CHIMERA_DIR *d = chimera_posix_opendir(jstr(req, "path"));

        if (!d) {
            return res_int(-1, errno);
        }
        for (int i = 0; i < MAX_DIRS; i++) {
            if (!driver_dirs[i]) {
                driver_dirs[i] = d;
                return res_int(i, 0);
            }
        }
        chimera_posix_closedir(d);
        return res_int(-1, EMFILE);
    }

    if (strcmp(op, "readdir") == 0) {
        int sid = jint(req, "sid", -1);

        if (sid < 0 || sid >= MAX_DIRS || !driver_dirs[sid]) {
            return res_int(-1, EBADF);
        }

        /* One atomic full sweep from a fresh cursor (DESIGN-POSIX.md): the
         * model's RReaddir returns the full current entry set every time. */
        chimera_posix_rewinddir(driver_dirs[sid]);

        json_t        *names = json_array();
        struct dirent *de;

        errno = 0;
        while ((de = chimera_posix_readdir(driver_dirs[sid])) != NULL) {
            json_array_append_new(names, json_string(de->d_name));
        }

        json_t        *res = res_int(0, errno);

        json_object_set_new(res, "names", names);
        return res;
    }

    if (strcmp(op, "rewinddir") == 0) {
        int sid = jint(req, "sid", -1);

        if (sid < 0 || sid >= MAX_DIRS || !driver_dirs[sid]) {
            return res_int(-1, EBADF);
        }
        chimera_posix_rewinddir(driver_dirs[sid]);
        return res_int(0, 0);
    }

    if (strcmp(op, "closedir") == 0) {
        int sid = jint(req, "sid", -1);
        int ret;

        if (sid < 0 || sid >= MAX_DIRS || !driver_dirs[sid]) {
            return res_int(-1, EBADF);
        }
        ret              = chimera_posix_closedir(driver_dirs[sid]);
        driver_dirs[sid] = NULL;
        return res_int(ret, errno);
    }

    if (strcmp(op, "telldir") == 0) {
        int sid = jint(req, "sid", -1);

        if (sid < 0 || sid >= MAX_DIRS || !driver_dirs[sid]) {
            return res_int(-1, EBADF);
        }
        return res_int((ssize_t) chimera_posix_telldir(driver_dirs[sid]),
                       errno);
    }

    if (strcmp(op, "seekdir") == 0) {
        int sid = jint(req, "sid", -1);

        if (sid < 0 || sid >= MAX_DIRS || !driver_dirs[sid]) {
            return res_int(-1, EBADF);
        }
        chimera_posix_seekdir(driver_dirs[sid], (long) jint64(req, "loc", 0));
        return res_int(0, 0);
    }

    if (strcmp(op, "fcntl_lock") == 0) {
        const char  *cmds = jstr(req, "cmd");
        const char  *type = jstr(req, "type");
        struct flock fl;
        int          cmd = F_SETLK;
        int          ret;

        memset(&fl, 0, sizeof(fl));
        if (cmds && strcmp(cmds, "setlkw") == 0) {
            cmd = F_SETLKW;
        } else if (cmds && strcmp(cmds, "getlk") == 0) {
            cmd = F_GETLK;
        }
        fl.l_type = F_RDLCK;
        if (type && strcmp(type, "wr") == 0) {
            fl.l_type = F_WRLCK;
        } else if (type && strcmp(type, "un") == 0) {
            fl.l_type = F_UNLCK;
        }
        fl.l_whence = SEEK_SET;
        fl.l_start  = (off_t) jint64(req, "start", 0);
        fl.l_len    = (off_t) jint64(req, "len", 0);

        ret = chimera_posix_fcntl(jint(req, "fd", -1), cmd, &fl);

        json_t *res = res_int(ret, errno);

        if (ret == 0 && cmd == F_GETLK) {
            const char *lt = "un";

            if (fl.l_type == F_RDLCK) {
                lt = "rd";
            } else if (fl.l_type == F_WRLCK) {
                lt = "wr";
            }
            json_object_set_new(res, "l_type", json_string(lt));
            json_object_set_new(res, "l_start",
                                json_integer((long long) fl.l_start));
            json_object_set_new(res, "l_len",
                                json_integer((long long) fl.l_len));
            json_object_set_new(res, "l_pid", json_integer(fl.l_pid));
        }
        return res;
    }

    if (strcmp(op, "fcntl_dupfd") == 0) {
        return res_int(chimera_posix_fcntl(jint(req, "fd", -1), F_DUPFD,
                                           jint(req, "atleast", 0)), errno);
    }

    if (strcmp(op, "fcntl_getfl") == 0) {
        return res_int(chimera_posix_fcntl(jint(req, "fd", -1), F_GETFL),
                       errno);
    }

    if (strcmp(op, "fcntl_setfl") == 0) {
        return res_int(chimera_posix_fcntl(jint(req, "fd", -1), F_SETFL,
                                           jint(req, "flags", 0)), errno);
    }

    if (strcmp(op, "lockf") == 0) {
        const char *cmds = jstr(req, "cmd");
        int         cmd  = F_LOCK;

        if (cmds && strcmp(cmds, "tlock") == 0) {
            cmd = F_TLOCK;
        } else if (cmds && strcmp(cmds, "ulock") == 0) {
            cmd = F_ULOCK;
        } else if (cmds && strcmp(cmds, "test") == 0) {
            cmd = F_TEST;
        }
        return res_int(chimera_posix_lockf(jint(req, "fd", -1), cmd,
                                           (off_t) jint64(req, "len", 0)),
                       errno);
    }

    if (strcmp(op, "fsync") == 0) {
        return res_int(chimera_posix_fsync(jint(req, "fd", -1)), errno);
    }

    if (strcmp(op, "fdatasync") == 0) {
        return res_int(chimera_posix_fdatasync(jint(req, "fd", -1)), errno);
    }

    if (strcmp(op, "copy_range") == 0) {
        off_t   off_in  = (off_t) jint64(req, "off_in", 0);
        off_t   off_out = (off_t) jint64(req, "off_out", 0);
        ssize_t n       = chimera_posix_copy_file_range(
            jint(req, "fd_in", -1), &off_in,
            jint(req, "fd_out", -1), &off_out,
            (size_t) jint64(req, "len", 0), 0);

        return res_int(n, errno);
    }

    if (strcmp(op, "clone_range") == 0) {
        return res_int(chimera_posix_clone_file_range(
                           jint(req, "dst_fd", -1),
                           (off_t) jint64(req, "dst_off", 0),
                           jint(req, "src_fd", -1),
                           (off_t) jint64(req, "src_off", 0),
                           (size_t) jint64(req, "len", 0)), errno);
    }

    if (strcmp(op, "fallocate") == 0) {
        int   fd   = jint(req, "fd", -1);
        int   mode = jint(req, "mode", 0);
        off_t off  = (off_t) jint64(req, "off", 0);
        off_t len  = (off_t) jint64(req, "len", 0);
        /* mode 0 == posix_fallocate (grow); mode 1 == the
         * FALLOC_FL_PUNCH_HOLE|FALLOC_FL_KEEP_SIZE deallocate pair. */
        int   ret = (mode == 0)
            ? chimera_posix_fallocate(fd, off, len)
            : chimera_posix_fallocate_mode(
            fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, off, len);

        return res_int(ret, errno);
    }

    if (strcmp(op, "newfs") == 0) {
        /* Batch isolation: tear the current filesystem down and stand up a
         * fresh, uniquely-named empty one, so neither content nor a cached FH
         * leaks into the next trace.  The Python side has already closed this
         * trace's fds/dirs (Replayer.cleanup); close any stragglers here too. */
        for (int i = 0; i < MAX_DIRS; i++) {
            if (driver_dirs[i]) {
                chimera_posix_closedir(driver_dirs[i]);
                driver_dirs[i] = NULL;
            }
        }
        if (g_nfs_version) {
            /* The NFS loopback path keeps the filesystem server-side; only the
             * direct backends are batched today (POSIX_MBT_MEMFS_ONLY). */
            return res_int(-1, ENOSYS);
        }
        if (chimera_posix_umount("/test") != 0) {
            fprintf(stderr, "posix_driver: newfs umount failed: %s\n",
                    strerror(errno));
            return res_int(-1, errno);
        }
        /* Do NOT rmfs the old filesystem: isolation comes from the *new*
         * fsname (fresh fsid -> fresh FH mount-id, so no cached entry can be
         * hit), and rmfs would need every open handle drained first -- a trace
         * can legitimately end with an open fd on an unlinked inode, which
         * keeps the fs busy.  The unmounted old fs just lingers in memory;
         * across a bounded corpus that is a few MB, and its handles reference
         * only valid (never-freed) inodes, so the close thread stays safe. */
        snprintf(g_fsname, sizeof(g_fsname), "fs%d", ++g_fs_counter);
        if (chimera_posix_mkfs(g_module, g_fsname, NULL) != 0) {
            fprintf(stderr, "posix_driver: newfs mkfs %s failed: %s\n",
                    g_fsname, strerror(errno));
            return res_int(-1, errno);
        }
        if (chimera_posix_mount("/test", g_module, g_fsname) != 0) {
            fprintf(stderr, "posix_driver: newfs mount %s failed: %s\n",
                    g_fsname, strerror(errno));
            return res_int(-1, errno);
        }
        if (normalize_root() != 0) {
            fprintf(stderr, "posix_driver: newfs normalize failed: %s\n",
                    strerror(errno));
            return res_int(-1, errno);
        }
        return res_int(0, 0);
    }

    if (strcmp(op, "shutdown") == 0) {
        return NULL;
    }

    return res_int(-1, ENOSYS);
} /* handle */

int
main(
    int    argc,
    char **argv)
{
    struct chimera_client_config *config;
    struct chimera_posix_client  *posix;
    struct chimera_server        *server = NULL;
    struct prometheus_metrics    *metrics;
    struct chimera_vfs_cred       root_cred;
    char                         *line        = NULL;
    size_t                        cap         = 0;
    const char                   *backend     = (argc > 1) ? argv[1] : "memfs";
    const char                   *storage     = (argc > 2) ? argv[2] : NULL;
    const char                   *module      = backend;
    int                           nfs_version = 0;
    char                          module_cfg[4096];

    proto_out = fdopen(dup(STDOUT_FILENO), "w");
    if (!proto_out || dup2(STDERR_FILENO, STDOUT_FILENO) < 0) {
        fprintf(stderr, "posix_driver: protocol stream setup failed\n");
        return 1;
    }

    chimera_log_init();
    ChimeraLogLevel = CHIMERA_LOG_ERROR;

    metrics = prometheus_metrics_create(NULL, NULL, 0);

    chimera_vfs_cred_init_unix(&root_cred, 0, 0, 0, NULL);

    /* Backend selection: argv[1] names the VFS module (default memfs), or an
     * NFS loopback path nfs3_<module>/nfs4_<module> (an in-process chimera
     * server exports the module and the client mounts it over localhost NFS).
     * argv[2] is a scratch directory for backends with real storage. */
    if (strncmp(backend, "nfs3_", 5) == 0) {
        nfs_version = 3;
        module      = backend + 5;
    } else if (strncmp(backend, "nfs4_", 5) == 0) {
        nfs_version = 4;
        module      = backend + 5;
    }

    /* Backend module configuration, shared by the direct-mount and
     * NFS-loopback paths. */
    module_cfg[0] = '\0';
    if (strcmp(module, "memfs") == 0) {
        /* Align memfs's internal block size with the harness's abstract
         * block size so model holes are real holes (SEEK_HOLE granularity).
         */
        snprintf(module_cfg, sizeof(module_cfg),
                 "{\"block_size\": %d}", DRIVER_BLOCK_SIZE);
    } else if (strcmp(module, "diskfs") == 0) {
        char img[3800];
        int  fd;

        if (!storage) {
            fprintf(stderr, "posix_driver: diskfs needs a storage dir\n");
            return 1;
        }
        snprintf(img, sizeof(img), "%s/device-0.img", storage);
        fd = open(img, O_CREAT | O_TRUNC | O_RDWR, 0644);
        if (fd < 0 || ftruncate(fd, 1024LL * 1024 * 1024) != 0) {
            fprintf(stderr, "posix_driver: device image %s: %s\n",
                    img, strerror(errno));
            return 1;
        }
        close(fd);
        /* 1 GiB sparse device; pin a small (64 MiB) intent log so the
        * journal fits the scratch device's first allocation group. */
        snprintf(module_cfg, sizeof(module_cfg),
                 "{\"initialize\":true,\"unsafe_async\":true,"
                 "\"intent_log_size\":67108864,"
                 "\"devices\":[{\"type\":\"libaio\",\"size\":1,\"path\":\"%s\"}]}",
                 img);
    } else if (strcmp(module, "cairn") == 0) {
        if (!storage) {
            fprintf(stderr, "posix_driver: cairn needs a storage dir\n");
            return 1;
        }
        snprintf(module_cfg, sizeof(module_cfg),
                 "{\"initialize\":true,\"path\":\"%s\"}", storage);
    } else {
        fprintf(stderr, "posix_driver: unknown backend %s\n", backend);
        return 1;
    }

    config = chimera_client_config_init();

    if (nfs_version) {
        /* Loopback NFS: an in-process server exports the module as /share and
         * the client mounts it over the nfs proxy module.  Both sides use the
         * libevpl INPROC transport (named endpoints, no real ports), so the
         * server's services never collide with concurrent drivers and no
         * network namespace, root, or Linux-specific isolation is needed --
         * every driver process gets its own inproc endpoint namespace, exactly
         * like the NFS/SMB MBT harnesses. */
        struct chimera_server_config *server_config;
        char                          mount_options[64];

        chimera_client_config_set_tcp_flavor(config, CHIMERA_TCP_FLAVOR_INPROC);

        server_config = chimera_server_config_init();
        chimera_server_config_set_tcp_flavor(server_config,
                                             CHIMERA_TCP_FLAVOR_INPROC);
        /* Protocols are opt-in (default off): the loopback path needs the NFS
         * server, or chimera_server_create_export has no nfs_shared to add to. */
        chimera_server_config_set_nfs_enabled(server_config, 1);
        chimera_server_config_add_module(server_config, module, NULL,
                                         module_cfg);

        server = chimera_server_init(server_config, metrics);
        if (!server) {
            fprintf(stderr, "posix_driver: server init failed\n");
            return 1;
        }

        /* Named-filesystem backend (memfs/diskfs/cairn): create the fs first. */
        if (chimera_server_mkfs(server, module, "fs0", NULL) != 0) {
            fprintf(stderr, "posix_driver: mkfs %s fs0 failed\n", module);
            return 1;
        }

        chimera_server_mount(server, "share", module, "fs0", NULL);

        if (chimera_server_create_export(server, "/share", "/share",
                                         4242, NULL) != 0) {
            fprintf(stderr, "posix_driver: export creation failed\n");
            return 1;
        }

        chimera_server_start(server);

        posix = chimera_posix_init(config, &root_cred, metrics);
        if (!posix) {
            fprintf(stderr, "posix_driver: client init failed\n");
            return 1;
        }

        snprintf(mount_options, sizeof(mount_options), "vers=%d",
                 nfs_version);
        if (chimera_posix_mount_with_options("/test", "nfs",
                                             "127.0.0.1:/share",
                                             mount_options) != 0) {
            fprintf(stderr, "posix_driver: nfs%d mount failed\n",
                    nfs_version);
            return 1;
        }
    } else {
        if (strcmp(module, "memfs") == 0) {
            for (int i = 0; i < config->num_modules; i++) {
                if (strcmp(config->modules[i].module_name, "memfs") == 0) {
                    snprintf(config->modules[i].config_data,
                             sizeof(config->modules[i].config_data),
                             "%s", module_cfg);
                }
            }
        } else {
            chimera_client_config_add_module(config, module, "", module_cfg);
        }

        posix = chimera_posix_init(config, &root_cred, metrics);
        if (!posix) {
            fprintf(stderr, "posix_driver: client init failed\n");
            return 1;
        }

        /* Named-filesystem backend (memfs/diskfs/cairn): create the fs first. */
        if (chimera_posix_mkfs(module, "fs0", NULL) != 0) {
            fprintf(stderr, "posix_driver: mkfs %s fs0 failed\n", module);
            return 1;
        }

        if (chimera_posix_mount("/test", module, "fs0") != 0) {
            fprintf(stderr, "posix_driver: %s mount failed\n", backend);
            return 1;
        }
    }

    /* Record the live fs identity so the batch "newfs" op can cycle it. */
    g_module      = module;
    g_nfs_version = nfs_version;
    g_root_cred   = root_cred;

    /* Normalize the root to the model's fsInit(0777, 0, 0). */
    if (normalize_root() != 0) {
        fprintf(stderr, "posix_driver: root normalization failed\n");
        return 1;
    }

    fprintf(proto_out, "{\"ready\": true, \"blocksize\": %d}\n",
            DRIVER_BLOCK_SIZE);
    fflush(proto_out);

    while (getline(&line, &cap, stdin) != -1) {
        json_error_t jerr;
        json_t      *req = json_loads(line, 0, &jerr);
        json_t      *res;

        if (!req) {
            fprintf(stderr, "posix_driver: bad request: %s\n", jerr.text);
            break;
        }

        errno = 0;
        res   = handle(req);
        json_decref(req);

        if (!res) {
            break;
        }

        char *out = json_dumps(res, JSON_COMPACT);

        fprintf(proto_out, "%s\n", out);
        fflush(proto_out);
        free(out);
        json_decref(res);
    }

    free(line);
    chimera_posix_umount("/test");
    chimera_posix_shutdown();
    if (server) {
        chimera_server_destroy(server);
    }
    prometheus_metrics_destroy(metrics);
    return 0;
} /* main */
