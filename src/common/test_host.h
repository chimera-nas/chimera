// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

/* Host-side fixtures only. File permissions under test belong to Chimera's
 * virtual filesystem; these helpers never pretend to implement host chown. */
#include "common/platform.h"
#include "common/dirent.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <share.h>
static inline int chimera_test_mkdir(const char *path, unsigned mode)
{
    (void) mode;
    return _mkdir(path);
}
static inline int setenv(const char *name, const char *value, int overwrite)
{
    if (!overwrite && getenv(name)) { return 0; }
    int error = _putenv_s(name, value);
    if (error) { errno = error; return -1; }
    return 0;
}
static inline int unsetenv(const char *name)
{
    int error = _putenv_s(name, "");
    if (error) { errno = error; return -1; }
    return 0;
}
static inline int chimera_test_temp_name(char *pattern)
{
    static const char alphabet[] = "abcdefghijklmnopqrstuvwxyz0123456789";
    size_t len = strlen(pattern);
    unsigned char random[6];
    if (len < 6 || strcmp(pattern + len - 6, "XXXXXX")) {
        errno = EINVAL;
        return -1;
    }
    if (chimera_getrandom(random, sizeof(random))) { return -1; }
    for (int i = 0; i < 6; i++) { pattern[len - 6 + i] = alphabet[random[i] % 36]; }
    return 0;
}
static inline char *mkdtemp(char *pattern)
{
    size_t len = strlen(pattern);
    for (int tries = 0; tries < 100; tries++) {
        if (chimera_test_temp_name(pattern)) { return NULL; }
        if (!_mkdir(pattern)) { return pattern; }
        if (errno != EEXIST) { return NULL; }
        memcpy(pattern + len - 6, "XXXXXX", 6);
    }
    errno = EEXIST;
    return NULL;
}
static inline int mkstemp(char *pattern)
{
    size_t len = strlen(pattern);
    for (int tries = 0; tries < 100; tries++) {
        int fd;
        errno_t error;
        if (chimera_test_temp_name(pattern)) { return -1; }
        error = _sopen_s(&fd, pattern, _O_CREAT | _O_EXCL | _O_RDWR | _O_BINARY,
                         _SH_DENYNO, _S_IREAD | _S_IWRITE);
        if (!error) { return fd; }
        if (error != EEXIST) { errno = error; return -1; }
        memcpy(pattern + len - 6, "XXXXXX", 6);
    }
    errno = EEXIST;
    return -1;
}
static inline const char *chimera_test_session_root(void)
{
    static char path[PATH_MAX];
    DWORD size = GetTempPathA(sizeof(path), path);
    if (!size || size + sizeof("chimera_test") > sizeof(path)) { abort(); }
    memcpy(path + size, "chimera_test", sizeof("chimera_test"));
    return path;
}
#else
#include <sys/stat.h>
#include <unistd.h>
#define chimera_test_mkdir mkdir
#endif

/* Remove only the fixture subtree; never traverse symlinks or junctions. */
static inline int chimera_test_remove_tree(const char *path)
{
    DIR *dir;
    struct dirent *entry;
#ifdef _WIN32
    DWORD attributes = GetFileAttributesA(path);
    if (attributes == INVALID_FILE_ATTRIBUTES) { return -1; }
    if (!(attributes & FILE_ATTRIBUTE_DIRECTORY)) { return _unlink(path); }
    if (attributes & FILE_ATTRIBUTE_REPARSE_POINT) { return _rmdir(path); }
#else
    struct stat st;
    if (lstat(path, &st)) { return -1; }
    if (!S_ISDIR(st.st_mode)) { return unlink(path); }
#endif
    dir = opendir(path);
    if (!dir) { return -1; }
    for (;;) {
        errno = 0;
        entry = readdir(dir);
        if (!entry) {
            int error = errno;
            closedir(dir);
            if (error) { errno = error; return -1; }
            return rmdir(path);
        }
        char *child;
        int result;
        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) { continue; }
        size_t size = strlen(path) + strlen(entry->d_name) + 2;
        child = malloc(size);
        if (!child) { closedir(dir); errno = ENOMEM; return -1; }
        snprintf(child, size, "%s/%s", path, entry->d_name);
        result = chimera_test_remove_tree(child);
        free(child);
        if (result) { closedir(dir); return -1; }
    }
}
