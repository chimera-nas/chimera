// SPDX-FileCopyrightText: 2006-2012 Pawel Jakub Dawidek <pawel@dawidek.net>
// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: BSD-2-Clause
//
// Derived from the pjdfstest POSIX filesystem test suite
// (https://github.com/pjd/pjdfstest) by Pawel Jakub Dawidek.  This harness is a
// C analog of pjdfstest's tests/misc.sh, run against the Chimera POSIX client,
// and is distributed under pjdfstest's original 2-clause BSD license.

/*
 * pjd_common.h - C harness for the pjdfstest POSIX conformance suite ported to
 * the Chimera POSIX client.  This is a C analog of pjdfstest's tests/misc.sh:
 * it provides TAP-style assertions, a harness-level working directory, per-call
 * credential / umask switching (mapping pjdfstest's -u/-g/-U flags), file
 * creation of every POSIX type, name generators, and stat-field accessors.
 *
 * Each ported .t becomes a standalone program:
 *
 *     #include "pjd_common.h"
 *     int main(int argc, char **argv) {
 *         pjd_begin(argc, argv);
 *         ... ported test body using EXPECT()/pjd_* helpers ...
 *         return pjd_end();
 *     }
 *
 * A test "passes" iff every assertion passed (pjd_end() exits non-zero
 * otherwise, failing the ctest case).
 */

#pragma once

#include <stdarg.h>
#include <limits.h>
#include <sys/sysmacros.h>
#include "posix_test_common.h"

/* ---- lifecycle / global state ------------------------------------------- */

/* Harness-side path buffer.  Larger than CHIMERA_VFS_PATH_MAX so that an
 * intentionally over-long pathname survives resolution intact and the POSIX
 * layer (not snprintf truncation) is what reports ENAMETOOLONG. */
#define PJD_PATHBUF         8192

static struct posix_test_env pjd_env;
static int                   pjd_ntest;
static int                   pjd_nfail;
static char                  pjd_cwd[PJD_PATHBUF];

/* Generated names are tracked here and freed in pjd_end() so the suite stays
 * clean under AddressSanitizer (the Debug build enables ASan). */
#define PJD_MAX_NAMES       4096
static char                 *pjd_name_registry[PJD_MAX_NAMES];
static int                   pjd_name_count;

#ifndef AT_FDCWD
#define AT_FDCWD            -100
#endif /* ifndef AT_FDCWD */
#ifndef AT_SYMLINK_NOFOLLOW
#define AT_SYMLINK_NOFOLLOW 0x100
#endif /* ifndef AT_SYMLINK_NOFOLLOW */

static inline void
pjd_begin(
    int    argc,
    char **argv)
{
    posix_test_init(&pjd_env, argv, argc);

    if (posix_test_mount(&pjd_env) != 0) {
        fprintf(stderr, "pjd: failed to mount test backend: %s\n", strerror(errno));
        posix_test_fail(&pjd_env);
    }

    snprintf(pjd_cwd, sizeof(pjd_cwd), "/test");
    pjd_ntest = 0;
    pjd_nfail = 0;
} /* pjd_begin */

static inline void
pjd_free_names(void)
{
    for (int i = 0; i < pjd_name_count; i++) {
        free(pjd_name_registry[i]);
        pjd_name_registry[i] = NULL;
    }
    pjd_name_count = 0;
} /* pjd_free_names */

static inline int
pjd_end(void)
{
    /* Restore root credential before teardown so cleanup is unrestricted. */
    chimera_posix_clear_cred();

    posix_test_umount();

    fprintf(stderr, "# %d assertions, %d failures\n", pjd_ntest, pjd_nfail);

    pjd_free_names();

    if (pjd_nfail) {
        posix_test_fail(&pjd_env);   /* noreturn, exit(EXIT_FAILURE) */
    }

    posix_test_success(&pjd_env);
    return 0;
} /* pjd_end */

/* ---- assertions --------------------------------------------------------- */

static inline int
pjd_record(
    int         ok,
    const char *file,
    int         line,
    const char *fmt,
    ...)
{
    va_list ap;

    pjd_ntest++;
    if (!ok) {
        pjd_nfail++;
    }

    fprintf(stderr, "%s %d - ", ok ? "ok" : "not ok", pjd_ntest);
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    if (!ok) {
        fprintf(stderr, "   [%s:%d]", file, line);
    }
    fprintf(stderr, "\n");
    return ok;
} /* pjd_record */

/* Expect a syscall-style call: success when rc >= 0, else errno is captured.
 * `experr` of 0 means "expect success"; otherwise expect rc < 0 && errno == experr.
 * Mirrors pjdfstest `expect <errno|0> <op> ...`. */
static inline void
pjd_expect_call(
    int         experr,
    long        rc,
    int         call_errno,
    const char *expr,
    const char *file,
    int         line)
{
    int got = (rc < 0) ? call_errno : 0;

    pjd_record(got == experr, file, line,
               "%s -> %s (want %s)", expr,
               got ? strerror(got) : "0",
               experr ? strerror(experr) : "0");
} /* pjd_expect_call */

/* EXPECT(experr, call): run `call`, then compare its outcome to experr.
 * errno is read immediately after the call. */
#define EXPECT(experr, call)                                              \
        do {                                                              \
            errno = 0;                                                    \
            long _pjd_rc = (long) (call);                                 \
            int  _pjd_en = errno;                                         \
            pjd_expect_call((experr), _pjd_rc, _pjd_en, #call,            \
                            __FILE__, __LINE__);                          \
        } while (0)

/* Compare two integral values (e.g. stat field == expected). */
#define EXPECT_EQ(want, got)                                              \
        pjd_record((long) (want) == (long) (got), __FILE__, __LINE__,     \
                   "%s == %s (%ld vs %ld)", #want, #got,                  \
                   (long) (want), (long) (got))

/* Generic boolean assertion with a message. */
#define PJD_CHECK(cond, ...) pjd_record((cond) ? 1 : 0, __FILE__, __LINE__, __VA_ARGS__)

/* ---- credential / umask switching --------------------------------------- */

static inline void
pjd_set_user(
    uid_t uid,
    gid_t gid)
{
    struct chimera_vfs_cred cred;

    chimera_vfs_cred_init_unix(&cred, uid, gid, 0, NULL);
    chimera_posix_set_cred(&cred);
} /* pjd_set_user */

static inline void
pjd_set_user_groups(
    uid_t           uid,
    gid_t           gid,
    int             ngids,
    const uint32_t *gids)
{
    struct chimera_vfs_cred cred;

    chimera_vfs_cred_init_unix(&cred, uid, gid, ngids, gids);
    chimera_posix_set_cred(&cred);
} /* pjd_set_user_groups */

static inline void
pjd_set_root(void)
{
    chimera_posix_clear_cred();
} /* pjd_set_root */

/* ---- working directory & path resolution -------------------------------- */

/* Resolve a (relative) name against the harness cwd into `out`.  Absolute
 * names are taken relative to the mount root. */
static inline const char *
pjd_resolve(
    const char *name,
    char       *out,
    size_t      outlen)
{
    /* Build base[+sep]+name into out with explicit bounds.  snprintf("%s/%s")
     * trips -Werror=format-truncation at -O3 because the operands can each be
     * the full buffer size; a bounded copy is both warning-free and correct. */
    const char *base = (name[0] == '/') ? "/test" : pjd_cwd;
    const char *sep  = (name[0] == '/') ? "" : "/"; /* absolute name keeps its '/' */
    size_t      blen = strlen(base);
    size_t      slen = strlen(sep);
    size_t      nlen = strlen(name);
    size_t      pos  = 0;

    if (outlen == 0) {
        return out;
    }
    if (blen > outlen - 1) {
        blen = outlen - 1;
    }
    memcpy(out, base, blen);
    pos = blen;
    if (pos + slen < outlen) {
        memcpy(out + pos, sep, slen);
        pos += slen;
    }
    if (pos < outlen - 1) {
        size_t cp = nlen;
        if (cp > outlen - 1 - pos) {
            cp = outlen - 1 - pos;
        }
        memcpy(out + pos, name, cp);
        pos += cp;
    }
    out[pos] = '\0';
    return out;
} /* pjd_resolve */

/* Change the harness working directory (no real chdir; resolves names). */
static inline void
pjd_cd(const char *name)
{
    char tmp[PJD_PATHBUF];

    if (strcmp(name, "..") == 0) {
        char *slash = strrchr(pjd_cwd, '/');
        if (slash && slash != pjd_cwd) {
            *slash = '\0';
        }
        return;
    }
    pjd_resolve(name, tmp, sizeof(tmp));
    snprintf(pjd_cwd, sizeof(pjd_cwd), "%s", tmp);
} /* pjd_cd */

/* ---- name generators ---------------------------------------------------- */

static unsigned int pjd_name_ctr;

static inline char *
pjd_track_name(char *b)
{
#ifdef __clang_analyzer__
    /* The static analyzer does not model the harness name registry (every
     * tracked name is freed in pjd_end()).  Make the allocation unconditionally
     * escape into the registry so scan-build does not report a false
     * unix.Malloc leak on the "registry full" branch below. */
    pjd_name_registry[0] = b;
    return b;
#else  /* ifdef __clang_analyzer__ */
    if (pjd_name_count < PJD_MAX_NAMES) {
        pjd_name_registry[pjd_name_count++] = b;
    }
    return b;
#endif /* ifdef __clang_analyzer__ */
} /* pjd_track_name */

/* Unique name. */
static inline char *
pjd_namegen(void)
{
    char *b = malloc(64);

    snprintf(b, 64, "pjd_%d_%u", (int) getpid(), pjd_name_ctr++);
    return pjd_track_name(b);
} /* pjd_namegen */

/* Name of exactly `len` bytes (for ENAMETOOLONG tests). */
static inline char *
pjd_namegen_len(size_t len)
{
    char *b = malloc(len + 1);

    memset(b, 'a', len);
    b[len] = '\0';
    return pjd_track_name(b);
} /* pjd_namegen_len */

/* A single name component exactly {NAME_MAX} bytes long. */
static inline char *
pjd_namegen_max(void)
{
    long name_max = chimera_posix_pathconf(".", _PC_NAME_MAX);

    return pjd_namegen_len((size_t) name_max);
} /* pjd_namegen_max */

/* A relative path whose total length is {PATH_MAX}-1 bytes, built from
 * {NAME_MAX}/2-byte components (mirrors misc.sh dirgen_max).  The parent
 * directories do not exist yet; use pjd_mkdir_p() to create them. */
static inline char *
pjd_dirgen_max(void)
{
    long   name_max = chimera_posix_pathconf(".", _PC_NAME_MAX);
    long   comp     = name_max / 2;
    /* The harness resolves names to "<cwd>/<name>"; size the name so the
     * resolved pathname totals {PATH_MAX}-1 bytes (the longest legal path). */
    long   prefix   = (long) strlen(pjd_cwd) + 1;
    long   path_max = chimera_posix_pathconf(".", _PC_PATH_MAX) - 1 - prefix;
    char  *b        = malloc(path_max + 2);
    size_t len      = 0;

    while ((long) len < path_max) {
        for (long i = 0; i < comp && (long) len < path_max; i++) {
            b[len++] = 'a';
        }
        if ((long) len < path_max) {
            b[len++] = '/';
        }
    }
    b[path_max] = '\0';
    /* Ensure the final byte is not a trailing slash. */
    if (b[path_max - 1] == '/') {
        b[path_max - 1] = 'x';
    }
    return pjd_track_name(b);
} /* pjd_dirgen_max */

/* ---- path-resolving operation wrappers ---------------------------------- */
/* These mirror the pjdfstest operations; each resolves its path argument(s)
 * against the harness cwd, so test bodies read like the shell originals. */

static inline int
pjd_mkdir(
    const char *name,
    mode_t      mode)
{
    char p[PJD_PATHBUF];

    return chimera_posix_mkdir(pjd_resolve(name, p, sizeof(p)), mode);
} /* pjd_mkdir */

static inline int
pjd_rmdir(const char *name)
{
    char p[PJD_PATHBUF];

    return chimera_posix_rmdir(pjd_resolve(name, p, sizeof(p)));
} /* pjd_rmdir */

/* Recursively create every parent directory of `relpath` (like mkdir -p of the
 * dirname).  Components are created mode 0755; existing ones are tolerated. */
static inline void
pjd_mkdir_p(const char *relpath)
{
    char   work[8192];
    size_t i;

    snprintf(work, sizeof(work), "%s", relpath);

    for (i = 1; work[i]; i++) {
        if (work[i] == '/') {
            work[i] = '\0';
            (void) pjd_mkdir(work, 0755);
            work[i] = '/';
        }
    }
} /* pjd_mkdir_p */

/* create: exclusive create + close, returning 0/-1 (pjdfstest `create`). */
static inline int
pjd_create(
    const char *name,
    mode_t      mode)
{
    char p[PJD_PATHBUF];
    int  fd = chimera_posix_open(pjd_resolve(name, p, sizeof(p)),
                                 O_CREAT | O_EXCL | O_WRONLY, mode);

    if (fd < 0) {
        return -1;
    }
    chimera_posix_close(fd);
    return 0;
} /* pjd_create */

static inline int
pjd_open(
    const char *name,
    int         flags,
    mode_t      mode)
{
    char p[PJD_PATHBUF];

    return chimera_posix_open(pjd_resolve(name, p, sizeof(p)), flags, mode);
} /* pjd_open */

/* open `name`; return 0 on success (closing the fd), else the errno. */
static inline int
pjd_open_e(
    const char *name,
    int         flags,
    mode_t      mode)
{
    int fd = pjd_open(name, flags, mode);

    if (fd < 0) {
        return errno ? errno : -1;
    }
    chimera_posix_close(fd);
    return 0;
} /* pjd_open_e */

static inline int
pjd_unlink(const char *name)
{
    char p[PJD_PATHBUF];

    return chimera_posix_unlink(pjd_resolve(name, p, sizeof(p)));
} /* pjd_unlink */

static inline int
pjd_chmod(
    const char *name,
    mode_t      mode)
{
    char p[PJD_PATHBUF];

    return chimera_posix_chmod(pjd_resolve(name, p, sizeof(p)), mode);
} /* pjd_chmod */

static inline int
pjd_chown(
    const char *name,
    uid_t       uid,
    gid_t       gid)
{
    char p[PJD_PATHBUF];

    return chimera_posix_chown(pjd_resolve(name, p, sizeof(p)), uid, gid);
} /* pjd_chown */

static inline int
pjd_lchown(
    const char *name,
    uid_t       uid,
    gid_t       gid)
{
    char p[PJD_PATHBUF];

    return chimera_posix_lchown(pjd_resolve(name, p, sizeof(p)), uid, gid);
} /* pjd_lchown */

static inline int
pjd_symlink(
    const char *target,
    const char *name)
{
    char p[PJD_PATHBUF];

    /* target is stored verbatim (not resolved); only the link path is. */
    return chimera_posix_symlink(target, pjd_resolve(name, p, sizeof(p)));
} /* pjd_symlink */

static inline int
pjd_link(
    const char *oldname,
    const char *newname)
{
    char op[PJD_PATHBUF], np[PJD_PATHBUF];

    return chimera_posix_link(pjd_resolve(oldname, op, sizeof(op)),
                              pjd_resolve(newname, np, sizeof(np)));
} /* pjd_link */

static inline int
pjd_rename(
    const char *oldname,
    const char *newname)
{
    char op[PJD_PATHBUF], np[PJD_PATHBUF];

    return chimera_posix_rename(pjd_resolve(oldname, op, sizeof(op)),
                                pjd_resolve(newname, np, sizeof(np)));
} /* pjd_rename */

static inline int
pjd_mknod(
    const char *name,
    mode_t      mode,
    dev_t       dev)
{
    char p[PJD_PATHBUF];

    return chimera_posix_mknod(pjd_resolve(name, p, sizeof(p)), mode, dev);
} /* pjd_mknod */

static inline int
pjd_mkfifo(
    const char *name,
    mode_t      mode)
{
    return pjd_mknod(name, S_IFIFO | mode, 0);
} /* pjd_mkfifo */

/* mknod(2) creating a FIFO (the substantive mknod/00 path). */
static inline int
pjd_mknod_fifo(
    const char *name,
    mode_t      mode)
{
    return pjd_mknod(name, S_IFIFO | mode, 0);
} /* pjd_mknod_fifo */

static inline int
pjd_truncate(
    const char *name,
    off_t       length)
{
    char p[PJD_PATHBUF];

    return chimera_posix_truncate(pjd_resolve(name, p, sizeof(p)), length);
} /* pjd_truncate */

/* ---- stat-field accessors ----------------------------------------------- */

/* File-type constant for a name (follows symlinks); returns the S_IFMT bits,
 * or 0 on error. */
static inline mode_t
pjd_stat_type(const char *name)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_stat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return 0;
    }
    return st.st_mode & S_IFMT;
} /* pjd_stat_type */

static inline mode_t
pjd_lstat_type(const char *name)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_lstat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return 0;
    }
    return st.st_mode & S_IFMT;
} /* pjd_lstat_type */

/* Permission/special bits (mode & 07777), following symlinks; -1 on error. */
static inline int
pjd_stat_mode(const char *name)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_stat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return -1;
    }
    return st.st_mode & 07777;
} /* pjd_stat_mode */

static inline int
pjd_lstat_mode(const char *name)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_lstat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return -1;
    }
    return st.st_mode & 07777;
} /* pjd_lstat_mode */

/* Full lstat/stat into caller's struct; returns 0/-1 (errno set). */
static inline int
pjd_lstat(
    const char  *name,
    struct stat *st)
{
    char p[PJD_PATHBUF];

    return chimera_posix_lstat(pjd_resolve(name, p, sizeof(p)), st);
} /* pjd_lstat */

static inline int
pjd_stat(
    const char  *name,
    struct stat *st)
{
    char p[PJD_PATHBUF];

    return chimera_posix_stat(pjd_resolve(name, p, sizeof(p)), st);
} /* pjd_stat */

static inline long
pjd_stat_nlink(const char *name)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_stat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return -1;
    }
    return (long) st.st_nlink;
} /* pjd_stat_nlink */

/* ctime/mtime as full timespec; used by "syscall updates ctime/mtime" checks.
 * Returns 0 on success, -1 on error. */
static inline int
pjd_lstat_ctime(
    const char      *name,
    struct timespec *ts)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_lstat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return -1;
    }
    *ts = st.st_ctim;
    return 0;
} /* pjd_lstat_ctime */

static inline int
pjd_lstat_mtime(
    const char      *name,
    struct timespec *ts)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_lstat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return -1;
    }
    *ts = st.st_mtim;
    return 0;
} /* pjd_lstat_mtime */

static inline long
pjd_lstat_inode(const char *name)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_lstat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return -1;
    }
    return (long) st.st_ino;
} /* pjd_lstat_inode */

static inline long
pjd_lstat_major(const char *name)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_lstat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return -1;
    }
    return (long) major(st.st_rdev);
} /* pjd_lstat_major */

static inline long
pjd_lstat_minor(const char *name)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_lstat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return -1;
    }
    return (long) minor(st.st_rdev);
} /* pjd_lstat_minor */

static inline long
pjd_lstat_uid(const char *name)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_lstat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return -1;
    }
    return (long) st.st_uid;
} /* pjd_lstat_uid */

static inline long
pjd_lstat_gid(const char *name)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_lstat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return -1;
    }
    return (long) st.st_gid;
} /* pjd_lstat_gid */

static inline long
pjd_stat_size(const char *name)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_stat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return -1;
    }
    return (long) st.st_size;
} /* pjd_stat_size */

/* stat (follow) ctime/mtime; 0 on success, -1 on error. */
static inline int
pjd_stat_ctime(
    const char      *name,
    struct timespec *ts)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_stat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return -1;
    }
    *ts = st.st_ctim;
    return 0;
} /* pjd_stat_ctime */

static inline int
pjd_stat_mtime(
    const char      *name,
    struct timespec *ts)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_stat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return -1;
    }
    *ts = st.st_mtim;
    return 0;
} /* pjd_stat_mtime */

/* Brief delay so a subsequent metadata change yields a strictly greater
 * timestamp.  The shell suite sleeps 1s (second-resolution stat); Chimera
 * exposes nanosecond timestamps, so a short sleep suffices on fine-grained
 * backends. */
static inline void
pjd_settle(void)
{
    struct timespec ts = { 0, 20 * 1000 * 1000 };  /* 20ms */

    nanosleep(&ts, NULL);
} /* pjd_settle */

/* atime/mtime seconds via lstat (no-follow); -1 on error. */
static inline long
pjd_lstat_atime(const char *name)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_lstat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return -1;
    }
    return (long) st.st_atim.tv_sec;
} /* pjd_lstat_atime */

static inline long
pjd_lstat_mtime_sec(const char *name)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_lstat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return -1;
    }
    return (long) st.st_mtim.tv_sec;
} /* pjd_lstat_mtime_sec */

/* Full atime/mtime timespec via lstat; 0 on success. */
static inline int
pjd_lstat_times(
    const char      *name,
    struct timespec *atime,
    struct timespec *mtime)
{
    char        p[PJD_PATHBUF];
    struct stat st;

    if (chimera_posix_lstat(pjd_resolve(name, p, sizeof(p)), &st) != 0) {
        return -1;
    }
    *atime = st.st_atim;
    *mtime = st.st_mtim;
    return 0;
} /* pjd_lstat_times */

/* utimensat against a real directory descriptor opened on the harness cwd
 * (mirrors pjdfstest's `open . O_RDONLY : utimensat 0 <name> ...`).  `name` is
 * resolved relative to that descriptor, so it is passed verbatim. */
static inline int
pjd_utimensat(
    const char *name,
    time_t      a_sec,
    long        a_nsec,
    time_t      m_sec,
    long        m_nsec,
    int         flags)
{
    struct timespec ts[2];
    int             dirfd = chimera_posix_open(pjd_cwd, O_RDONLY, 0);
    int             rc, e;

    if (dirfd < 0) {
        return -1;
    }
    ts[0].tv_sec  = a_sec;
    ts[0].tv_nsec = a_nsec;
    ts[1].tv_sec  = m_sec;
    ts[1].tv_nsec = m_nsec;

    rc = chimera_posix_utimensat(dirfd, name, ts, flags);
    e  = (rc < 0) ? errno : 0;
    chimera_posix_close(dirfd);
    errno = e;
    return rc;
} /* pjd_utimensat */

static inline int
pjd_timespec_lt(
    const struct timespec *a,
    const struct timespec *b)
{
    if (a->tv_sec != b->tv_sec) {
        return a->tv_sec < b->tv_sec;
    }
    return a->tv_nsec < b->tv_nsec;
} /* pjd_timespec_lt */

/* ---- create_file: all POSIX types (mirrors misc.sh create_file) --------- */

enum pjd_ftype {
    PJD_FT_NONE,
    PJD_FT_REGULAR,
    PJD_FT_DIR,
    PJD_FT_FIFO,
    PJD_FT_BLOCK,
    PJD_FT_CHAR,
    PJD_FT_SOCKET,
    PJD_FT_SYMLINK,
};

/* Create a file of the given type at `name`.  Returns 0 on success, -1 on
 * error (errno set), mirroring the misc.sh helper's per-type create.  Default
 * modes follow pjdfstest (regular/block/char/socket 0644, dir 0755). */
static inline int
pjd_create_file(
    enum pjd_ftype type,
    const char    *name)
{
    switch (type) {
        case PJD_FT_NONE:
            return 0;
        case PJD_FT_REGULAR:
            return pjd_create(name, 0644);
        case PJD_FT_DIR:
            return pjd_mkdir(name, 0755);
        case PJD_FT_FIFO:
            return pjd_mkfifo(name, 0644);
        case PJD_FT_BLOCK:
            return pjd_mknod(name, S_IFBLK | 0644, makedev(1, 2));
        case PJD_FT_CHAR:
            return pjd_mknod(name, S_IFCHR | 0644, makedev(1, 2));
        case PJD_FT_SOCKET:
            return pjd_mknod(name, S_IFSOCK | 0644, 0);
        case PJD_FT_SYMLINK:
            return pjd_symlink("test", name);
    } /* switch */
    return -1;
} /* pjd_create_file */

/* Create a file of the given type owned by uid:gid (created as the current
 * credential, then lchown'd -- mirrors misc.sh create_file <type> <name> <uid>
 * <gid>).  Returns 0 on success, -1 on the first failing step. */
static inline int
pjd_create_file_owned(
    enum pjd_ftype type,
    const char    *name,
    uid_t          uid,
    gid_t          gid)
{
    if (pjd_create_file(type, name) != 0) {
        return -1;
    }
    return pjd_lchown(name, uid, gid);
} /* pjd_create_file_owned */

/* Remove a file of the given type (dir -> rmdir, else unlink). */
static inline int
pjd_remove_file(
    enum pjd_ftype type,
    const char    *name)
{
    if (type == PJD_FT_DIR) {
        return pjd_rmdir(name);
    }
    return pjd_unlink(name);
} /* pjd_remove_file */
