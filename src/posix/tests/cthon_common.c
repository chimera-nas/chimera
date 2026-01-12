// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Common functions for cthon tests ported to Chimera POSIX API
// Based on cthon/basic/subr.c from Connectathon 2004

// Suppress string truncation warnings - these are test utilities where
// truncation is acceptable (paths are bounded by MAXPATHLEN anyway)
#pragma GCC diagnostic ignored "-Wstringop-truncation"
#pragma GCC diagnostic ignored "-Wformat-truncation"

#include "cthon_common.h"

char *cthon_Myname = "cthon";
char  cthon_cwd[MAXPATHLEN] = "/test";

static struct timeval cthon_ts, cthon_te;

// Stack for directory navigation
#define MAX_DIR_DEPTH 32
static char  cthon_dir_stack[MAX_DIR_DEPTH][MAXPATHLEN];
static int   cthon_dir_depth = 0;

void
cthon_starttime(void)
{
    gettimeofday(&cthon_ts, NULL);
}

void
cthon_endtime(struct timeval *tv)
{
    gettimeofday(&cthon_te, NULL);
    if (cthon_te.tv_usec < cthon_ts.tv_usec) {
        cthon_te.tv_sec--;
        cthon_te.tv_usec += 1000000;
    }
    tv->tv_usec = cthon_te.tv_usec - cthon_ts.tv_usec;
    tv->tv_sec  = cthon_te.tv_sec - cthon_ts.tv_sec;
}

void
cthon_error(const char *fmt, ...)
{
    int     oerrno = errno;
    va_list ap;

    va_start(ap, fmt);
    fprintf(stderr, "\t%s: (%s) ", cthon_Myname, cthon_cwd);
    vfprintf(stderr, fmt, ap);
    va_end(ap);

    if (oerrno) {
        errno = oerrno;
        perror(" ");
    } else {
        fprintf(stderr, "\n");
    }
    fflush(stderr);
}

long
cthon_getparm(const char *parm, long min, const char *label)
{
    long val = atol(parm);

    if (val < min) {
        cthon_error("Illegal %s parameter %ld, must be at least %ld",
                    label, val, min);
        exit(1);
    }
    return val;
}

void
cthon_complete(void)
{
    fprintf(stdout, "\t%s ok.\n", cthon_Myname);
}

const char *
cthon_getcwd(void)
{
    return cthon_cwd;
}

void
cthon_setcwd(const char *path)
{
    strncpy(cthon_cwd, path, MAXPATHLEN - 1);
    cthon_cwd[MAXPATHLEN - 1] = '\0';
}

void
cthon_pushdir(const char *name)
{
    if (cthon_dir_depth >= MAX_DIR_DEPTH) {
        cthon_error("Directory stack overflow");
        exit(1);
    }

    // Save current directory
    strncpy(cthon_dir_stack[cthon_dir_depth], cthon_cwd, MAXPATHLEN - 1);
    cthon_dir_stack[cthon_dir_depth][MAXPATHLEN - 1] = '\0';
    cthon_dir_depth++;

    // Build new path
    if (name[0] == '/') {
        strncpy(cthon_cwd, name, MAXPATHLEN - 1);
    } else {
        size_t len = strlen(cthon_cwd);
        if (len > 0 && cthon_cwd[len - 1] != '/') {
            strncat(cthon_cwd, "/", MAXPATHLEN - len - 1);
        }
        strncat(cthon_cwd, name, MAXPATHLEN - strlen(cthon_cwd) - 1);
    }
    cthon_cwd[MAXPATHLEN - 1] = '\0';
}

void
cthon_popdir(void)
{
    if (cthon_dir_depth <= 0) {
        cthon_error("Directory stack underflow");
        exit(1);
    }

    cthon_dir_depth--;
    strncpy(cthon_cwd, cthon_dir_stack[cthon_dir_depth], MAXPATHLEN - 1);
    cthon_cwd[MAXPATHLEN - 1] = '\0';
}

int
cthon_creat(const char *path, mode_t mode)
{
    char fullpath[MAXPATHLEN];

    if (path[0] == '/') {
        strncpy(fullpath, path, MAXPATHLEN - 1);
    } else {
        snprintf(fullpath, MAXPATHLEN, "%s/%s", cthon_cwd, path);
    }
    fullpath[MAXPATHLEN - 1] = '\0';

    return chimera_posix_open(fullpath, O_CREAT | O_WRONLY | O_TRUNC, mode);
}

// Build a directory tree
void
cthon_dirtree(
    int         lev,
    int         files,
    int         dirs,
    const char *fname,
    const char *dname,
    int        *totfiles,
    int        *totdirs)
{
    int  fd;
    int  f, d;
    char name[MAXPATHLEN];
    char fullpath[MAXPATHLEN];

    if (lev-- == 0) {
        return;
    }

    // Create files
    for (f = 0; f < files; f++) {
        snprintf(name, sizeof(name), "%s%d", fname, f);
        snprintf(fullpath, sizeof(fullpath), "%s/%s", cthon_cwd, name);

        fd = chimera_posix_open(fullpath, O_CREAT | O_WRONLY | O_TRUNC, CTHON_CHMOD_RW);
        if (fd < 0) {
            cthon_error("creat %s failed", fullpath);
            exit(1);
        }
        (*totfiles)++;
        if (chimera_posix_close(fd) < 0) {
            cthon_error("close %d failed", fd);
            exit(1);
        }
    }

    // Create directories and recurse
    for (d = 0; d < dirs; d++) {
        snprintf(name, sizeof(name), "%s%d", dname, d);
        snprintf(fullpath, sizeof(fullpath), "%s/%s", cthon_cwd, name);

        if (chimera_posix_mkdir(fullpath, 0777) < 0) {
            cthon_error("mkdir %s failed", fullpath);
            exit(1);
        }
        (*totdirs)++;

        cthon_pushdir(name);
        cthon_dirtree(lev, files, dirs, fname, dname, totfiles, totdirs);
        cthon_popdir();
    }
}

// Remove a directory tree
void
cthon_rmdirtree(
    int         lev,
    int         files,
    int         dirs,
    const char *fname,
    const char *dname,
    int        *totfiles,
    int        *totdirs,
    int         ignore)
{
    int  f, d;
    char name[MAXPATHLEN];
    char fullpath[MAXPATHLEN];

    if (lev-- == 0) {
        return;
    }

    // Remove files
    for (f = 0; f < files; f++) {
        snprintf(name, sizeof(name), "%s%d", fname, f);
        snprintf(fullpath, sizeof(fullpath), "%s/%s", cthon_cwd, name);

        if (chimera_posix_unlink(fullpath) < 0 && !ignore) {
            cthon_error("unlink %s failed", fullpath);
            exit(1);
        }
        (*totfiles)++;
    }

    // Remove directories
    for (d = 0; d < dirs; d++) {
        snprintf(name, sizeof(name), "%s%d", dname, d);
        snprintf(fullpath, sizeof(fullpath), "%s/%s", cthon_cwd, name);

        cthon_pushdir(name);
        cthon_rmdirtree(lev, files, dirs, fname, dname, totfiles, totdirs, ignore);
        cthon_popdir();

        if (chimera_posix_rmdir(fullpath) < 0 && !ignore) {
            cthon_error("rmdir %s failed", fullpath);
            exit(1);
        }
        (*totdirs)++;
    }
}

// Set up test directory
void
cthon_testdir(const char *dir)
{
    struct stat statb;
    char        fullpath[MAXPATHLEN];

    if (dir == NULL) {
        dir = "/test/nfstestdir";
    }

    // Build full path
    if (dir[0] == '/') {
        strncpy(fullpath, dir, MAXPATHLEN - 1);
    } else {
        snprintf(fullpath, MAXPATHLEN, "/test/%s", dir);
    }
    fullpath[MAXPATHLEN - 1] = '\0';

    // Remove existing directory if present
    if (chimera_posix_stat(fullpath, &statb) == 0) {
        // Directory exists - we'll create files in it, which will be cleaned up
        // In the original this would do "rm -r", but we'll just proceed
    }

    // Create directory
    if (chimera_posix_mkdir(fullpath, 0777) < 0) {
        if (errno != EEXIST) {
            cthon_error("can't create test directory %s", fullpath);
            exit(1);
        }
    }

    // Set as current working directory
    cthon_setcwd(fullpath);
}

// Move to test directory (without creating)
int
cthon_mtestdir(const char *dir)
{
    char fullpath[MAXPATHLEN];

    if (dir == NULL) {
        dir = "/test/nfstestdir";
    }

    if (dir[0] == '/') {
        strncpy(fullpath, dir, MAXPATHLEN - 1);
    } else {
        snprintf(fullpath, MAXPATHLEN, "/test/%s", dir);
    }
    fullpath[MAXPATHLEN - 1] = '\0';

    cthon_setcwd(fullpath);
    return 0;
}
