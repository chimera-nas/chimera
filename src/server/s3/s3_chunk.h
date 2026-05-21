// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <string.h>

/*
 * Streaming decoder for the AWS "aws-chunked" content encoding used by
 * SigV4 streaming uploads (x-amz-content-sha256:
 * STREAMING-AWS4-HMAC-SHA256-PAYLOAD and STREAMING-UNSIGNED-PAYLOAD-TRAILER).
 *
 * The body is a sequence of chunks, each framed as:
 *
 *     <hex-size>[;chunk-signature=<hex>]\r\n
 *     <chunk-data>\r\n
 *
 * terminated by a zero-length chunk followed by optional trailer headers and
 * a final CRLF:
 *
 *     0[;chunk-signature=<hex>]\r\n
 *     [trailer-header\r\n]...
 *     \r\n
 *
 * The decoder is byte-oriented and fully resumable, so a chunk header, data
 * region or trailer may be split arbitrarily across successive calls (i.e.
 * across separate network reads / iovecs). We do not verify per-chunk
 * signatures; the seed signature in the Authorization header is validated by
 * the normal auth path, and the chunk-signature extension is skipped.
 */

enum s3_chunk_state {
    S3_CHUNK_SIZE,       /* accumulating the hex chunk-size */
    S3_CHUNK_EXT,        /* skipping a ;chunk-ext up to CR */
    S3_CHUNK_SIZE_CR,    /* saw CR ending the size line, expect LF */
    S3_CHUNK_DATA,       /* copying chunk-data payload bytes */
    S3_CHUNK_DATA_CR,    /* expect CR following chunk-data */
    S3_CHUNK_DATA_LF,    /* expect LF following chunk-data CR */
    S3_CHUNK_TRAILER,    /* past the zero chunk: drain trailer + final CRLF */
};

struct s3_chunk_decoder {
    enum s3_chunk_state state;
    uint64_t remaining;             /* hex accumulator, then data bytes left */
    int      done;                  /* saw the terminating zero-length chunk */
    int      error;                 /* malformed framing encountered */
};

static inline void
s3_chunk_decoder_init(struct s3_chunk_decoder *d)
{
    d->state     = S3_CHUNK_SIZE;
    d->remaining = 0;
    d->done      = 0;
    d->error     = 0;
} /* s3_chunk_decoder_init */

static inline int
s3_chunk_hexval(int c)
{
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
} /* s3_chunk_hexval */

/*
 * Decode up to in_len bytes of aws-chunked input, appending the decoded
 * payload bytes to out at offset *out_len (and advancing *out_len). Because
 * decoding only ever strips framing, the decoded output is never larger than
 * the input, so callers can size out to at least in_len. Returns 0 on success
 * or -1 if the framing is malformed (d->error is also set).
 */
static inline int
s3_chunk_decode(
    struct s3_chunk_decoder *d,
    const uint8_t           *in,
    int                      in_len,
    uint8_t                 *out,
    int                     *out_len)
{
    int i = 0;

    while (i < in_len) {
        uint8_t c = in[i];

        switch (d->state) {
            case S3_CHUNK_SIZE:
            {
                int v = s3_chunk_hexval(c);
                if (v >= 0) {
                    d->remaining = (d->remaining << 4) | (uint64_t) v;
                    i++;
                } else if (c == ';') {
                    d->state = S3_CHUNK_EXT;
                    i++;
                } else if (c == '\r') {
                    d->state = S3_CHUNK_SIZE_CR;
                    i++;
                } else {
                    d->error = 1;
                    return -1;
                }
            }
            break;
            case S3_CHUNK_EXT:
                if (c == '\r') {
                    d->state = S3_CHUNK_SIZE_CR;
                }
                i++;
                break;
            case S3_CHUNK_SIZE_CR:
                if (c != '\n') {
                    d->error = 1;
                    return -1;
                }
                i++;
                if (d->remaining == 0) {
                    /* Zero-length chunk: body data is complete. */
                    d->state = S3_CHUNK_TRAILER;
                    d->done  = 1;
                } else {
                    d->state = S3_CHUNK_DATA;
                }
                break;
            case S3_CHUNK_DATA:
            {
                uint64_t avail = (uint64_t) (in_len - i);
                uint64_t n     = d->remaining < avail ? d->remaining : avail;

                memcpy(out + *out_len, in + i, n);
                *out_len     += (int) n;
                i            += (int) n;
                d->remaining -= n;

                if (d->remaining == 0) {
                    d->state = S3_CHUNK_DATA_CR;
                }
            }
            break;
            case S3_CHUNK_DATA_CR:
                if (c != '\r') {
                    d->error = 1;
                    return -1;
                }
                d->state = S3_CHUNK_DATA_LF;
                i++;
                break;
            case S3_CHUNK_DATA_LF:
                if (c != '\n') {
                    d->error = 1;
                    return -1;
                }
                d->state     = S3_CHUNK_SIZE;
                d->remaining = 0;
                i++;
                break;
            case S3_CHUNK_TRAILER:
                /* Trailer headers and the final CRLF carry no object data. */
                i++;
                break;
        } /* switch */
    }

    return 0;
} /* s3_chunk_decode */
