// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "posix_internal.h"

SYMBOL_EXPORT size_t
chimera_posix_fwrite(
    const void   *ptr,
    size_t        size,
    size_t        nmemb,
    CHIMERA_FILE *stream)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();
    int                          fd;
    size_t                       total_bytes   = size * nmemb;
    size_t                       bytes_written = 0;
    ssize_t                      result;
    const char                  *buf = ptr;

    if (!stream || !ptr || size == 0 || nmemb == 0) {
        return 0;
    }

    fd = chimera_posix_file_to_fd(posix, stream);

    while (bytes_written < total_bytes) {
        result = chimera_posix_write(fd, buf + bytes_written, total_bytes - bytes_written);

        if (result < 0) {
            stream->error_flag = 1;
            break;
        }

        if (result == 0) {
            break;
        }

        bytes_written += (size_t) result;
    }

    return bytes_written / size;
} /* chimera_posix_fwrite */
