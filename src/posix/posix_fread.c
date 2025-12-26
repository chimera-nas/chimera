// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "posix_internal.h"

SYMBOL_EXPORT size_t
chimera_posix_fread(
    void         *ptr,
    size_t        size,
    size_t        nmemb,
    CHIMERA_FILE *stream)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();
    int                          fd;
    size_t                       total_bytes = size * nmemb;
    size_t                       bytes_read  = 0;
    ssize_t                      result;
    char                        *buf = ptr;

    if (!stream || !ptr || size == 0 || nmemb == 0) {
        return 0;
    }

    fd = chimera_posix_file_to_fd(posix, stream);

    // Handle ungetc character first
    if (stream->ungetc_char >= 0 && total_bytes > 0) {
        buf[0]              = (char) stream->ungetc_char;
        stream->ungetc_char = -1;
        buf++;
        bytes_read++;
        total_bytes--;
    }

    while (bytes_read < size * nmemb) {
        result = chimera_posix_read(fd, buf + bytes_read, total_bytes);

        if (result < 0) {
            stream->error_flag = 1;
            break;
        }

        if (result == 0) {
            stream->eof_flag = 1;
            break;
        }

        bytes_read  += (size_t) result;
        total_bytes -= (size_t) result;
    }

    return bytes_read / size;
} /* chimera_posix_fread */
