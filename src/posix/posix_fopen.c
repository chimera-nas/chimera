// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "posix_internal.h"

// Helper to parse fopen mode string and return open flags
static int
chimera_posix_parse_mode(
    const char *mode,
    int        *flags_out)
{
    int flags = 0;

    if (!mode || !mode[0]) {
        return -1;
    }

    switch (mode[0]) {
        case 'r':
            flags = O_RDONLY;
            break;
        case 'w':
            flags = O_WRONLY | O_CREAT | O_TRUNC;
            break;
        case 'a':
            flags = O_WRONLY | O_CREAT | O_APPEND;
            break;
        default:
            return -1;
    } /* switch */

    // Check for '+' modifier (read+write)
    for (int i = 1; mode[i]; i++) {
        if (mode[i] == '+') {
            // Change to read+write mode
            flags &= ~(O_RDONLY | O_WRONLY);
            flags |= O_RDWR;
        } else if (mode[i] == 'b') {
            // Binary mode - no-op on POSIX
        }
        // Ignore other characters
    }

    *flags_out = flags;
    return 0;
} /* chimera_posix_parse_mode */

SYMBOL_EXPORT CHIMERA_FILE *
chimera_posix_fopen(
    const char *path,
    const char *mode)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();
    int                          flags;
    int                          fd;

    if (chimera_posix_parse_mode(mode, &flags) < 0) {
        errno = EINVAL;
        return NULL;
    }

    fd = chimera_posix_open(path, flags, 0666);
    if (fd < 0) {
        return NULL;
    }

    return chimera_posix_fd_to_file(posix, fd);
} /* chimera_posix_fopen */

SYMBOL_EXPORT CHIMERA_FILE *
chimera_posix_freopen(
    const char   *path,
    const char   *mode,
    CHIMERA_FILE *stream)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();
    int                          old_fd;
    int                          flags;
    int                          new_fd;

    if (!stream) {
        errno = EBADF;
        return NULL;
    }

    old_fd = chimera_posix_file_to_fd(posix, stream);

    // Close the old stream
    chimera_posix_close(old_fd);

    if (!path) {
        // If path is NULL, just close and return NULL
        return NULL;
    }

    if (chimera_posix_parse_mode(mode, &flags) < 0) {
        errno = EINVAL;
        return NULL;
    }

    new_fd = chimera_posix_open(path, flags, 0666);
    if (new_fd < 0) {
        return NULL;
    }

    return chimera_posix_fd_to_file(posix, new_fd);
} /* chimera_posix_freopen */
