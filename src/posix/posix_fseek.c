// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "posix_internal.h"

SYMBOL_EXPORT int
chimera_posix_fseek(
    CHIMERA_FILE *stream,
    long          offset,
    int           whence)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();
    int                          fd;
    off_t                        result;

    if (!stream) {
        errno = EBADF;
        return -1;
    }

    fd = chimera_posix_file_to_fd(posix, stream);

    result = chimera_posix_lseek(fd, offset, whence);
    if (result < 0) {
        stream->error_flag = 1;
        return -1;
    }

    // Clear EOF flag on successful seek
    stream->eof_flag    = 0;
    stream->ungetc_char = -1;

    return 0;
} /* chimera_posix_fseek */

SYMBOL_EXPORT int
chimera_posix_fseeko(
    CHIMERA_FILE *stream,
    off_t         offset,
    int           whence)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();
    int                          fd;
    off_t                        result;

    if (!stream) {
        errno = EBADF;
        return -1;
    }

    fd = chimera_posix_file_to_fd(posix, stream);

    result = chimera_posix_lseek(fd, offset, whence);
    if (result < 0) {
        stream->error_flag = 1;
        return -1;
    }

    // Clear EOF flag on successful seek
    stream->eof_flag    = 0;
    stream->ungetc_char = -1;

    return 0;
} /* chimera_posix_fseeko */

SYMBOL_EXPORT long
chimera_posix_ftell(CHIMERA_FILE *stream)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();
    int                          fd;
    off_t                        result;

    if (!stream) {
        errno = EBADF;
        return -1L;
    }

    fd = chimera_posix_file_to_fd(posix, stream);

    result = chimera_posix_lseek(fd, 0, SEEK_CUR);
    if (result < 0) {
        return -1L;
    }

    // Adjust for ungetc character if present
    if (stream->ungetc_char >= 0) {
        result--;
    }

    return (long) result;
} /* chimera_posix_ftell */

SYMBOL_EXPORT off_t
chimera_posix_ftello(CHIMERA_FILE *stream)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();
    int                          fd;
    off_t                        result;

    if (!stream) {
        errno = EBADF;
        return -1;
    }

    fd = chimera_posix_file_to_fd(posix, stream);

    result = chimera_posix_lseek(fd, 0, SEEK_CUR);
    if (result < 0) {
        return -1;
    }

    // Adjust for ungetc character if present
    if (stream->ungetc_char >= 0) {
        result--;
    }

    return result;
} /* chimera_posix_ftello */

SYMBOL_EXPORT void
chimera_posix_rewind(CHIMERA_FILE *stream)
{
    if (!stream) {
        return;
    }

    chimera_posix_fseek(stream, 0, SEEK_SET);
    stream->error_flag = 0;
} /* chimera_posix_rewind */

SYMBOL_EXPORT int
chimera_posix_fgetpos(
    CHIMERA_FILE   *stream,
    chimera_fpos_t *pos)
{
    off_t offset;

    if (!stream || !pos) {
        errno = EINVAL;
        return -1;
    }

    offset = chimera_posix_ftello(stream);
    if (offset < 0) {
        return -1;
    }

    pos->pos = offset;
    return 0;
} /* chimera_posix_fgetpos */

SYMBOL_EXPORT int
chimera_posix_fsetpos(
    CHIMERA_FILE         *stream,
    const chimera_fpos_t *pos)
{
    if (!stream || !pos) {
        errno = EINVAL;
        return -1;
    }

    return chimera_posix_fseeko(stream, pos->pos, SEEK_SET);
} /* chimera_posix_fsetpos */
