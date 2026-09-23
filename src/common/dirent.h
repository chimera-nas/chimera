// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#ifndef _WIN32
#include <dirent.h>
#else // ifndef _WIN32
#include "common/windows.h"

#define DT_UNKNOWN 0
#define DT_FIFO    1
#define DT_CHR     2
#define DT_DIR     4
#define DT_BLK     6
#define DT_REG     8
#define DT_LNK     10
#define DT_SOCK    12

struct dirent {
    uint64_t d_ino;
    int64_t  d_off;
    uint16_t d_reclen;
    uint8_t  d_type;
    char     d_name[1024];
};
typedef struct chimera_host_dir {
    HANDLE           search;
    WIN32_FIND_DATAW current;
    int              first;
    struct dirent    entry;
} DIR;

static inline DIR *
opendir(const char *path)
{
    int    len = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, NULL, 0);
    WCHAR *pattern;
    DIR   *dir;

    if (!len) {
        errno = EINVAL;
        return NULL;
    }
    pattern = malloc(((size_t) len + 2) * sizeof(*pattern));
    dir     = calloc(1, sizeof(*dir));
    if (!pattern || !dir) {
        free(pattern);
        free(dir);
        errno = ENOMEM;
        return NULL;
    }
    MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, pattern, len);
    pattern[len - 1] = L'\\';
    pattern[len]     = L'*';
    pattern[len + 1] = 0;
    dir->search      = FindFirstFileW(pattern, &dir->current);
    free(pattern);
    if (dir->search == INVALID_HANDLE_VALUE) {
        DWORD error = GetLastError();
        free(dir);
        errno = error == ERROR_ACCESS_DENIED ? EACCES : ENOENT;
        return NULL;
    }
    dir->first = 1;
    return dir;
} // opendir
static inline struct dirent *
readdir(DIR *dir)
{
    int count;

    if (!dir->first && !FindNextFileW(dir->search, &dir->current)) {
        if (GetLastError() != ERROR_NO_MORE_FILES) {
            errno = EIO;
        }
        return NULL;
    }
    dir->first = 0;
    count      = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, dir->current.cFileName,
                                     -1, dir->entry.d_name, sizeof(dir->entry.d_name), NULL, NULL);
    if (!count) {
        errno = EILSEQ;
        return NULL;
    }
    dir->entry.d_type = dir->current.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT ? DT_LNK :
        dir->current.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY ? DT_DIR : DT_REG;
    dir->entry.d_reclen = (uint16_t) (offsetof(struct dirent, d_name) + count);
    dir->entry.d_off++;
    return &dir->entry;
} // readdir
static inline int
closedir(DIR *dir)
{
    BOOL success = FindClose(dir->search);

    free(dir);
    if (!success) {
        errno = EBADF;
        return -1;
    }
    return 0;
} // closedir
#endif // ifndef _WIN32
