// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Common definitions and functions for cthon tests ported to Chimera POSIX API

#ifndef CTHON_COMMON_H
#define CTHON_COMMON_H

// Suppress string truncation warnings - these are test utilities where
// truncation is acceptable (paths are bounded by MAXPATHLEN anyway)
#ifdef __GNUC__
#pragma GCC diagnostic ignored "-Wformat-truncation"
#if __GNUC__ >= 8
#pragma GCC diagnostic ignored "-Wstringop-truncation"
#endif // if __GNUC__ >= 8
#endif // ifdef __GNUC__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <dirent.h>

#include "posix/posix.h"
#include "posix_test_common.h"

// Test defaults from original cthon
#define CTHON_DNAME      "dir."
#define CTHON_FNAME      "file."
#define CTHON_DDIRS      2
#define CTHON_DLEVS      5
#define CTHON_DFILS      5
#define CTHON_DCOUNT     10

#define CTHON_CHMOD_MASK 0777
#define CTHON_CHMOD_NONE 0
#define CTHON_CHMOD_RW   0666

#ifndef MAXPATHLEN
#define MAXPATHLEN       1024
#endif // ifndef MAXPATHLEN

// Global test name for error messages
extern char *cthon_Myname;

// Current working directory path (simulated since we don't have chdir)
extern char  cthon_cwd[MAXPATHLEN];

// Timing support
void cthon_starttime(
    void);
void cthon_endtime(
    struct timeval *tv);

// Error reporting
void cthon_error(
    const char *fmt,
    ...);

// Parameter parsing
long cthon_getparm(
    const char *parm,
    long        min,
    const char *label);

// Directory tree operations
void cthon_dirtree(
    int         lev,
    int         files,
    int         dirs,
    const char *fname,
    const char *dname,
    int        *totfiles,
    int        *totdirs);
void cthon_rmdirtree(
    int         lev,
    int         files,
    int         dirs,
    const char *fname,
    const char *dname,
    int        *totfiles,
    int        *totdirs,
    int         ignore);

// Test directory setup
void cthon_testdir(
    const char *dir);
int cthon_mtestdir(
    const char *dir);

// Completion
void cthon_complete(
    void);

// Path manipulation helpers
void cthon_pushdir(
    const char *name);
void cthon_popdir(
    void);
const char * cthon_getcwd(
    void);
void cthon_setcwd(
    const char *path);

// Wrapper for creating files (creat() equivalent)
int cthon_creat(
    const char *path,
    mode_t      mode);

#endif /* CTHON_COMMON_H */
