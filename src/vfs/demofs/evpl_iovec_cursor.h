#pragma once

struct evpl_iovec_cursor {
    const struct evpl_iovec *iov;
    int                      offset;
    int                      niov;
};


static inline void
evpl_iovec_cursor_init(
    struct evpl_iovec_cursor *cursor,
    const struct evpl_iovec  *iov,
    int                       niov)
{
    cursor->iov    = iov;
    cursor->niov   = niov;
    cursor->offset = 0;
} /* evpl_iovec_cursor_init */

static void
evpl_iovec_cursor_copy(
    struct evpl_iovec_cursor *cursor,
    void                     *out,
    int                       length)
{
    int chunk, left = length;

    while (left && cursor->niov) {

        chunk = cursor->iov->length - cursor->offset;

        if (left < chunk) {
            chunk = left;
        }

        memcpy(out, cursor->iov->data + cursor->offset, chunk);

        left -= chunk;
        out  += chunk;

        cursor->offset += chunk;

        if (cursor->offset == cursor->iov->length) {
            cursor->iov++;
            cursor->niov--;
            cursor->offset = 0;
        }
    }

    if (left) {
        abort();
    }
} /* evpl_iovec_cursor_copy */

static void
evpl_iovec_cursor_skip(
    struct evpl_iovec_cursor *cursor,
    int                       length)
{
    int chunk, left = length;

    while (left && cursor->niov) {

        chunk = cursor->iov->length - cursor->offset;

        if (left < chunk) {
            chunk = left;
        }

        left -= chunk;

        cursor->offset += chunk;

        if (cursor->offset == cursor->iov->length) {
            cursor->iov++;
            cursor->niov--;
            cursor->offset = 0;
        }
    }

    if (left) {
        abort();
    }
} /* evpl_iovec_cursor_skip */

static void
evpl_iovec_cursor_zero(
    struct evpl_iovec_cursor *cursor,
    int                       length)
{
    int chunk, left = length;

    while (left && cursor->niov) {

        chunk = cursor->iov->length - cursor->offset;

        if (left < chunk) {
            chunk = left;
        }

        memset(cursor->iov->data + cursor->offset, 0, chunk);

        left -= chunk;

        cursor->offset += chunk;

        if (cursor->offset == cursor->iov->length) {
            cursor->iov++;
            cursor->niov--;
            cursor->offset = 0;
        }
    }

    if (left) {
        abort();
    }
} /* evpl_iovec_cursor_zero */

static int
evpl_iovec_cursor_move(
    struct evpl_iovec_cursor *cursor,
    struct evpl_iovec        *iov,
    int                       maxiov,
    int                       length)
{
    int chunk, left = length, niov = 0;

    while (left && cursor->niov && niov < maxiov) {
        chunk = cursor->iov->length - cursor->offset;

        if (left < chunk) {
            chunk = left;
        }

        iov[niov].data         = cursor->iov->data + cursor->offset;
        iov[niov].length       = chunk;
        iov[niov].private_data = cursor->iov->private_data;

        niov++;
        left -= chunk;

        cursor->offset += chunk;

        if (cursor->offset == cursor->iov->length) {
            cursor->iov++;
            cursor->niov--;
            cursor->offset = 0;
        }
    }

    return niov;
} /* evpl_iovec_cursor_move */
