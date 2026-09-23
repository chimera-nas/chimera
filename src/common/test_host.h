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
static inline int
chimera_test_mkdir(
    const char *path,
    unsigned    mode)
{
    (void) mode;
    return _mkdir(path);
} // chimera_test_mkdir
static inline int
setenv(
    const char *name,
    const char *value,
    int         overwrite)
{
    if (!overwrite && getenv(name)) {
        return 0;
    }
    int error = _putenv_s(name, value);
    if (error) {
        errno = error; return -1;
    }
    return 0;
} // setenv
static inline int
unsetenv(const char *name)
{
    int error = _putenv_s(name, "");

    if (error) {
        errno = error; return -1;
    }
    return 0;
} // unsetenv
static inline int
chimera_test_temp_name(char *pattern)
{
    static const char alphabet[] = "abcdefghijklmnopqrstuvwxyz0123456789";
    size_t            len        = strlen(pattern);
    unsigned char     random[6];

    if (len < 6 || strcmp(pattern + len - 6, "XXXXXX")) {
        errno = EINVAL;
        return -1;
    }
    if (chimera_getrandom(random, sizeof(random))) {
        return -1;
    }
    for (int i = 0; i < 6; i++) {
        pattern[len - 6 + i] = alphabet[random[i] % 36];
    }
    return 0;
} // chimera_test_temp_name
static inline char *
mkdtemp(char *pattern)
{
    size_t len = strlen(pattern);

    for (int tries = 0; tries < 100; tries++) {
        if (chimera_test_temp_name(pattern)) {
            return NULL;
        }
        if (!_mkdir(pattern)) {
            return pattern;
        }
        if (errno != EEXIST) {
            return NULL;
        }
        memcpy(pattern + len - 6, "XXXXXX", 6);
    }
    errno = EEXIST;
    return NULL;
} // mkdtemp
static inline int
mkstemp(char *pattern)
{
    size_t len = strlen(pattern);

    for (int tries = 0; tries < 100; tries++) {
        int     fd;
        errno_t error;
        if (chimera_test_temp_name(pattern)) {
            return -1;
        }
        error = _sopen_s(&fd, pattern, _O_CREAT | _O_EXCL | _O_RDWR | _O_BINARY,
                         _SH_DENYNO, _S_IREAD | _S_IWRITE);
        if (!error) {
            return fd;
        }
        if (error != EEXIST) {
            errno = error; return -1;
        }
        memcpy(pattern + len - 6, "XXXXXX", 6);
    }
    errno = EEXIST;
    return -1;
} // mkstemp
static inline const char *
chimera_test_session_root(void)
{
    static char path[PATH_MAX];
    DWORD       size = GetTempPathA(sizeof(path), path);

    if (!size || size + sizeof("chimera_test") > sizeof(path)) {
        abort();
    }
    memcpy(path + size, "chimera_test", sizeof("chimera_test"));
    return path;
} // chimera_test_session_root
#else // ifdef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#define chimera_test_mkdir mkdir
#endif // ifdef _WIN32

/* Remove only the fixture subtree; never traverse symlinks or junctions. */
#ifdef _WIN32
static inline int
chimera_test_remove_tree(const char *path)
{
    DIR           *dir;
    struct dirent *entry;

    DWORD          attributes = GetFileAttributesA(path);

    if (attributes == INVALID_FILE_ATTRIBUTES) {
        return -1;
    }
    if (!(attributes & FILE_ATTRIBUTE_DIRECTORY)) {
        return _unlink(path);
    }
    if (attributes & FILE_ATTRIBUTE_REPARSE_POINT) {
        return _rmdir(path);
    }
    dir = opendir(path);
    if (!dir) {
        return -1;
    }
    for (;;) {
        errno = 0;
        entry = readdir(dir);
        if (!entry) {
            int error = errno;
            closedir(dir);
            if (error) {
                errno = error; return -1;
            }
            return rmdir(path);
        }
        char  *child;
        int    result;
        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) {
            continue;
        }
        size_t size = strlen(path) + strlen(entry->d_name) + 2;
        child = malloc(size);
        if (!child) {
            closedir(dir); errno = ENOMEM; return -1;
        }
        snprintf(child, size, "%s/%s", path, entry->d_name);
        result = chimera_test_remove_tree(child);
        free(child);
        if (result) {
            closedir(dir); return -1;
        }
    }
} // chimera_test_remove_tree
#else // ifdef _WIN32
/* Open each directory without following a link, and resolve its children
 * relative to that descriptor even if its pathname is renamed or replaced. */
static inline int
chimera_test_remove_tree_at(
    int         parent,
    const char *name)
{
    DIR           *dir;
    struct dirent *entry;
    int            fd, error;

    fd = openat(parent, name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0) {
        if (errno == ENOTDIR || errno == ELOOP) {
            return unlinkat(parent, name, 0);
        }
        return -1;
    }
    dir = fdopendir(fd);
    if (!dir) {
        error = errno;
        close(fd);
        errno = error;
        return -1;
    }
    for (;;) {
        errno = 0;
        entry = readdir(dir);
        if (!entry) {
            error = errno;
            closedir(dir);
            if (error) {
                errno = error;
                return -1;
            }
            return unlinkat(parent, name, AT_REMOVEDIR);
        }
        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) {
            continue;
        }
        if (chimera_test_remove_tree_at(fd, entry->d_name)) {
            error = errno;
            closedir(dir);
            errno = error;
            return -1;
        }
    }
} // chimera_test_remove_tree_at

static inline int
chimera_test_remove_tree(const char *path)
{
    return chimera_test_remove_tree_at(AT_FDCWD, path);
} // chimera_test_remove_tree
#endif // ifdef _WIN32

/* Heap-owned absolute fixture path, released with free(). */
static inline char *
chimera_test_absolute_path(const char *path)
{
#ifdef _WIN32
    return _fullpath(NULL, path, 0);
#else // ifdef _WIN32
    return realpath(path, NULL);
#endif // ifdef _WIN32
} // chimera_test_absolute_path

/* getline-style input for the native JSON driver; no fixed trace-line limit. */
static inline ssize_t
chimera_test_getline(
    char  **line,
    size_t *capacity,
    FILE   *input)
{
    size_t length = 0;
    int    ch;

    while ((ch = fgetc(input)) != EOF) {
        if (!*line || length + 1 >= *capacity) {
            size_t next = *line ? *capacity : 0;
            if (next > (size_t) PTRDIFF_MAX / 2) {
                errno = EOVERFLOW;
                return -1;
            }
            next = next ? next * 2 : 256;
            char  *grown = realloc(*line, next);
            if (!grown) {
                errno = ENOMEM;
                return -1;
            }
            *line     = grown;
            *capacity = next;
        }
        (*line)[length++] = (char) ch;
        if (ch == '\n') {
            break;
        }
    }
    if (ferror(input) || !length) {
        return -1;
    }
    (*line)[length] = 0;
    return (ssize_t) length;
} // chimera_test_getline
