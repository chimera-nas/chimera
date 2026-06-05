// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB3 transport compression codecs and message framing (MS-SMB2 §2.2.42,
 * MS-XCA).  The Plain LZ77 (LZXPRESS) codec is implemented from the MS-XCA §2.4
 * algorithm; the chained Pattern_V1 run-length and NONE pass-through payloads
 * are handled inline.  LZ77+Huffman and LZNT1 are not yet implemented.
 */

#include <string.h>
#include <stdlib.h>

#include "smb_compress.h"
#include "common/evpl_iovec_cursor.h"
#include "evpl/evpl.h"
#include "smb_internal.h"
#include "smb2.h"

/* The Plain LZ77 match offset is stored in the high 13 bits of a 16-bit token,
 * so the back-reference window is bounded at 8192 bytes. */
#define SMB_LZ77_MAX_OFFSET       8192
#define SMB_LZ77_MIN_MATCH        3
#define SMB_LZ77_HASH_BITS        15
#define SMB_LZ77_HASH_SIZE        (1 << SMB_LZ77_HASH_BITS)
#define SMB_LZ77_CHAIN_LIMIT      64

/* Bound the reconstructed plaintext size of a received compressed message. */
#define SMB_COMPRESS_MAX_ORIGINAL (16 * 1024 * 1024)

struct chimera_smb_compress_ctx {
    int32_t *head;   /* LZ77 match-finder hash heads (SMB_LZ77_HASH_SIZE) */
};

static inline uint16_t
rd16(const uint8_t *p)
{
    return (uint16_t) (p[0] | ((uint16_t) p[1] << 8));
} /* rd16 */

static inline uint32_t
rd32(const uint8_t *p)
{
    return (uint32_t) p[0] | ((uint32_t) p[1] << 8) |
           ((uint32_t) p[2] << 16) | ((uint32_t) p[3] << 24);
} /* rd32 */

static inline void
wr16(
    uint8_t *p,
    uint16_t v)
{
    p[0] = v & 0xff;
    p[1] = (v >> 8) & 0xff;
} /* wr16 */

static inline void
wr32(
    uint8_t *p,
    uint32_t v)
{
    p[0] = v & 0xff;
    p[1] = (v >> 8) & 0xff;
    p[2] = (v >> 16) & 0xff;
    p[3] = (v >> 24) & 0xff;
} /* wr32 */

struct chimera_smb_compress_ctx *
chimera_smb_compress_ctx_create(void)
{
    struct chimera_smb_compress_ctx *ctx = calloc(1, sizeof(*ctx));

    if (!ctx) {
        return NULL;
    }

    ctx->head = malloc(SMB_LZ77_HASH_SIZE * sizeof(*ctx->head));
    if (!ctx->head) {
        free(ctx);
        return NULL;
    }
    return ctx;
} /* chimera_smb_compress_ctx_create */

void
chimera_smb_compress_ctx_destroy(struct chimera_smb_compress_ctx *ctx)
{
    if (ctx) {
        free(ctx->head);
        free(ctx);
    }
} /* chimera_smb_compress_ctx_destroy */

/*
 * MS-XCA §2.4.1 Plain LZ77 decompression.  Produces exactly out_len bytes; the
 * caller always knows the original size from the transform header.  Flag dwords
 * are little-endian and consumed most-significant-bit first; a set bit selects a
 * 16-bit match token (low 3 bits = length-3 code, high 13 bits = offset-1) with
 * the 7/15/255 extended-length escape and the shared length half-byte
 * (LastLengthHalfByte).  Returns out_len on success, -1 on any inconsistency.
 */
SYMBOL_EXPORT int
chimera_smb_lz77_decompress(
    const uint8_t *in,
    int            in_len,
    uint8_t       *out,
    int            out_len)
{
    int      inpos = 0, outpos = 0;
    uint32_t flags     = 0;
    int      flagcount = 0;
    int      last_half = -1;   /* index in `in` of a byte whose high nibble is pending */

    while (outpos < out_len) {
        if (flagcount == 0) {
            if (inpos + 4 > in_len) {
                return -1;
            }
            flags     = rd32(in + inpos);
            inpos    += 4;
            flagcount = 32;
        }
        flagcount--;

        if (((flags >> flagcount) & 1u) == 0) {
            /* Literal. */
            if (inpos >= in_len) {
                return -1;
            }
            out[outpos++] = in[inpos++];
            continue;
        }

        /* Match. */
        uint16_t mb;
        int      length, offset;

        if (inpos + 2 > in_len) {
            return -1;
        }
        mb     = rd16(in + inpos);
        inpos += 2;
        length = mb & 0x7;
        offset = (mb >> 3) + 1;

        if (length == 7) {
            int nib;

            if (last_half < 0) {
                if (inpos >= in_len) {
                    return -1;
                }
                nib       = in[inpos] & 0x0f;
                last_half = inpos;
                inpos++;
            } else {
                nib       = in[last_half] >> 4;
                last_half = -1;
            }
            length = nib;
            if (length == 15) {
                if (inpos >= in_len) {
                    return -1;
                }
                length = in[inpos++];
                if (length == 255) {
                    if (inpos + 2 > in_len) {
                        return -1;
                    }
                    length = rd16(in + inpos);
                    inpos += 2;
                    /* A 16-bit value of 0 escapes to a 32-bit length (matches
                     * longer than 65538 bytes, e.g. long runs). */
                    if (length == 0) {
                        if (inpos + 4 > in_len) {
                            return -1;
                        }
                        length = (int) rd32(in + inpos);
                        inpos += 4;
                    }
                    length -= (15 + 7);
                }
                length += 15;
            }
            length += 7;
        }
        length += SMB_LZ77_MIN_MATCH;

        if (offset > outpos || outpos + length > out_len) {
            return -1;
        }
        /* Overlapping copy: byte-by-byte (offset may be < length). */
        for (int i = 0; i < length; i++) {
            out[outpos] = out[outpos - offset];
            outpos++;
        }
    }

    return out_len;
} /* chimera_smb_lz77_decompress */

/* Emit one flag bit into the current 32-bit flag group (allocated lazily as a
 * reserved dword that precedes the group's token bytes).  Returns 0, or -1 if
 * the output buffer would overflow. */
struct lz77_enc {
    uint8_t *out;
    int      cap;
    int      pos;
    int      flag_pos;   /* out index of the current reserved flag dword, or -1 */
    uint32_t flags;
    int      flagbits;   /* tokens placed in the current group (0..31) */
    int      last_half;  /* out index of a byte whose high nibble is pending, or -1 */
};

static int
lz77_emit_flag(
    struct lz77_enc *e,
    int              is_match)
{
    if (e->flagbits == 0) {
        if (e->pos + 4 > e->cap) {
            return -1;
        }
        e->flag_pos = e->pos;
        e->pos     += 4;
        e->flags    = 0;
    }
    if (is_match) {
        e->flags |= (1u << (31 - e->flagbits));
    }
    e->flagbits++;
    if (e->flagbits == 32) {
        wr32(e->out + e->flag_pos, e->flags);
        e->flag_pos = -1;
        e->flagbits = 0;
    }
    return 0;
} /* lz77_emit_flag */

static int
lz77_put(
    struct lz77_enc *e,
    uint8_t          b)
{
    if (e->pos >= e->cap) {
        return -1;
    }
    e->out[e->pos++] = b;
    return 0;
} /* lz77_put */

/* Emit a match (length >= 3, 1 <= offset <= 8192), mirroring the decoder's
 * 16-bit token, 7/15/255 extended length, and shared length half-byte. */
static int
lz77_emit_match(
    struct lz77_enc *e,
    int              offset,
    int              length)
{
    int l = length - SMB_LZ77_MIN_MATCH;

    if (lz77_emit_flag(e, 1) != 0) {
        return -1;
    }

    if (l < 7) {
        if (e->pos + 2 > e->cap) {
            return -1;
        }
        wr16(e->out + e->pos, (uint16_t) (((offset - 1) << 3) | l));
        e->pos += 2;
        return 0;
    }

    if (e->pos + 2 > e->cap) {
        return -1;
    }
    wr16(e->out + e->pos, (uint16_t) (((offset - 1) << 3) | 7));
    e->pos += 2;

    /* v is the residual length beyond the base-7 token: total length == 10 + v. */
    int v   = length - 10;
    int nib = (v < 15) ? v : 15;

    if (e->last_half < 0) {
        if (lz77_put(e, (uint8_t) nib) != 0) {   /* low nibble; high filled later */
            return -1;
        }
        e->last_half = e->pos - 1;
    } else {
        e->out[e->last_half] |= (uint8_t) (nib << 4);
        e->last_half          = -1;
    }

    if (nib == 15) {
        int b = v - 15;

        if (b < 255) {
            if (lz77_put(e, (uint8_t) b) != 0) {
                return -1;
            }
        } else {
            /* ext == length - 3.  Up to 0xffff fits the 16-bit field; longer
            * matches escape via a 16-bit 0 followed by the 32-bit length. */
            uint32_t ext = (uint32_t) (v + 7);

            if (lz77_put(e, 255) != 0) {
                return -1;
            }
            if (ext <= 0xffff) {
                if (e->pos + 2 > e->cap) {
                    return -1;
                }
                wr16(e->out + e->pos, (uint16_t) ext);
                e->pos += 2;
            } else {
                if (e->pos + 6 > e->cap) {
                    return -1;
                }
                wr16(e->out + e->pos, 0);
                wr32(e->out + e->pos + 2, ext);
                e->pos += 6;
            }
        }
    }
    return 0;
} /* lz77_emit_match */

static inline uint32_t
lz77_hash(const uint8_t *p)
{
    /* Hash a 3-byte sequence into SMB_LZ77_HASH_BITS bits. */
    uint32_t h = ((uint32_t) p[0] << 16) | ((uint32_t) p[1] << 8) | p[2];

    return (h * 2654435761u) >> (32 - SMB_LZ77_HASH_BITS);
} /* lz77_hash */

/*
 * MS-XCA §2.4 Plain LZ77 compression: greedy hash-chain match finder.  Returns
 * the compressed length, or -1 if the result would exceed out_cap (the caller
 * then sends the message uncompressed).
 */
SYMBOL_EXPORT int
chimera_smb_lz77_compress(
    const uint8_t *in,
    int            in_len,
    uint8_t       *out,
    int            out_cap)
{
    struct lz77_enc e;
    int32_t        *head;
    int32_t        *prev;
    int             i;

    e.out       = out;
    e.cap       = out_cap;
    e.pos       = 0;
    e.flag_pos  = -1;
    e.flags     = 0;
    e.flagbits  = 0;
    e.last_half = -1;

    if (in_len <= 0) {
        return -1;
    }

    head = malloc(SMB_LZ77_HASH_SIZE * sizeof(*head));
    prev = malloc((size_t) in_len * sizeof(*prev));
    if (!head || !prev) {
        free(head);
        free(prev);
        return -1;
    }
    for (i = 0; i < SMB_LZ77_HASH_SIZE; i++) {
        head[i] = -1;
    }

    i = 0;
    while (i < in_len) {
        int best_len = 0, best_off = 0;

        if (i + SMB_LZ77_MIN_MATCH <= in_len) {
            uint32_t h     = lz77_hash(in + i);
            int32_t  cand  = head[h];
            int      chain = 0;

            while (cand >= 0 && (i - cand) <= SMB_LZ77_MAX_OFFSET &&
                   chain < SMB_LZ77_CHAIN_LIMIT) {
                int max = in_len - i;
                int len = 0;

                while (len < max && in[cand + len] == in[i + len]) {
                    len++;
                }
                if (len > best_len) {
                    best_len = len;
                    best_off = i - cand;
                    if (len >= max) {
                        break;
                    }
                }
                cand = prev[cand];
                chain++;
            }
        }

        if (best_len >= SMB_LZ77_MIN_MATCH) {
            /* Insert hash entries for the matched span so later positions can
             * reference into it, then emit the match. */
            int end = i + best_len;

            while (i < end) {
                if (i + SMB_LZ77_MIN_MATCH <= in_len) {
                    uint32_t h = lz77_hash(in + i);
                    prev[i] = head[h];
                    head[h] = i;
                }
                i++;
            }
            if (lz77_emit_match(&e, best_off, best_len) != 0) {
                goto overflow;
            }
        } else {
            if (i + SMB_LZ77_MIN_MATCH <= in_len) {
                uint32_t h = lz77_hash(in + i);
                prev[i] = head[h];
                head[h] = i;
            }
            if (lz77_emit_flag(&e, 0) != 0 || lz77_put(&e, in[i]) != 0) {
                goto overflow;
            }
            i++;
        }
    }

    /* Terminate the stream for an input-exhaustion decoder (the MS Plain LZ77
     * decoder has no target length — it stops only when it reaches a match flag
     * with the input fully consumed).  Set the next flag bit after the final
     * token to 1 (a match indicator) so that decoder halts cleanly; our own
     * decoder stops at out_len and never reads it.  A full final group needs a
     * fresh terminator dword. */
    if (e.flagbits == 0) {
        if (e.pos + 4 > e.cap) {
            goto overflow;
        }
        e.flag_pos = e.pos;
        e.pos     += 4;
        e.flags    = 0;
    }
    e.flags |= (1u << (31 - e.flagbits));
    wr32(e.out + e.flag_pos, e.flags);

    free(head);
    free(prev);
    return e.pos;

 overflow:
    free(head);
    free(prev);
    return -1;
} /* chimera_smb_lz77_compress */

/* Decode a raw codec stream (no SMB2 payload framing) of in_len bytes to exactly
 * out_len plaintext bytes.  Used directly by the unchained transform (whose
 * target size comes from the header) and by the chained LZ77 payload after its
 * OriginalPayloadSize prefix is stripped.  Returns out_len, or -1 on error. */
static int
decompress_codec(
    uint16_t       alg,
    const uint8_t *in,
    int            in_len,
    uint8_t       *out,
    int            out_len)
{
    switch (alg) {
        case SMB2_COMPRESSION_LZ77:
            return chimera_smb_lz77_decompress(in, in_len, out, out_len);

        default:
            chimera_smb_error("Unsupported SMB3 compression algorithm 0x%x", alg);
            return -1;
    } /* switch */
} /* decompress_codec */

/* Decode one *chained* payload (MS-SMB2 §2.2.42.2.1) into out, bounded by
 * out_avail.  NONE is a raw pass-through and Pattern_V1 is a run-length
 * expansion (neither carries OriginalPayloadSize); the real codecs prefix a
 * 4-byte OriginalPayloadSize.  Returns the plaintext bytes produced, or -1. */
static int
decompress_payload(
    uint16_t       alg,
    const uint8_t *payload,
    int            payload_len,
    uint8_t       *out,
    int            out_avail)
{
    switch (alg) {
        case SMB2_COMPRESSION_NONE:
            if (payload_len > out_avail) {
                return -1;
            }
            memcpy(out, payload, payload_len);
            return payload_len;

        case SMB2_COMPRESSION_PATTERN_V1: {
            uint32_t reps;
            uint8_t  pattern;

            if (payload_len != (int) sizeof(struct smb2_compression_pattern_payload_v1)) {
                return -1;
            }
            pattern = payload[0];
            reps    = rd32(payload + 4);
            if (reps > (uint32_t) out_avail) {
                return -1;
            }
            memset(out, pattern, reps);
            return (int) reps;
        }

        default: {
            /* A real codec: a 4-byte OriginalPayloadSize precedes the data. */
            uint32_t orig;

            if (payload_len < 4) {
                return -1;
            }
            orig = rd32(payload);
            if (orig > (uint32_t) out_avail) {
                return -1;
            }
            if (decompress_codec(alg, payload + 4, payload_len - 4,
                                 out, (int) orig) != (int) orig) {
                return -1;
            }
            return (int) orig;
        }
    } /* switch */
} /* decompress_payload */

int
chimera_smb_decompress_message(
    struct chimera_smb_compress_ctx *ctx,
    struct evpl                     *evpl,
    struct evpl_iovec_cursor        *cursor,
    int                              length,
    struct evpl_iovec               *plain_out,
    int                             *plain_len_out)
{
    static const uint8_t proto[4] = SMB2_COMPRESSION_TRANSFORM_PROTO_ID;
    uint8_t             *cin;
    uint8_t             *pt;
    uint32_t             orig_size;
    int                  rc = -1;

    (void) ctx;

    if (length < (int) sizeof(struct smb2_compression_transform_header_chained)) {
        chimera_smb_error("Truncated SMB3 compression message (%d bytes)", length);
        return -1;
    }

    cin = malloc(length);
    if (!cin) {
        return -1;
    }
    evpl_iovec_cursor_copy(cursor, cin, length);

    if (memcmp(cin, proto, 4) != 0) {
        chimera_smb_error("Invalid SMB3 compression protocol id");
        goto out;
    }

    /* The 16-bit Flags field at byte offset 10 discriminates the two forms: 0
     * (NONE) is the unchained header, 1 (CHAINED, set on the first payload
     * header) is the chained form (MS-SMB2 §2.2.42).  OriginalCompressedSegment
     * Size (offset 4) is the decompressed size of the *compressed segment*: for
     * the unchained form the full message is the uncompressed prefix (Offset)
     * plus that segment; the chained form has no prefix, so it is the whole
     * message. */
    int      is_unchained = length >= (int) sizeof(struct smb2_compression_transform_header) &&
        rd16(cin + 10) == SMB2_COMPRESSION_FLAG_NONE;
    uint32_t segment_size = rd32(cin + 4);
    uint32_t prefix       = is_unchained ? rd32(cin + 12) : 0;

    orig_size = prefix + segment_size;
    if (orig_size < sizeof(struct smb2_header) || orig_size > SMB_COMPRESS_MAX_ORIGINAL) {
        chimera_smb_error("Invalid SMB3 compression original size %u", orig_size);
        goto out;
    }

    if (evpl_iovec_alloc(evpl, orig_size, 8, 1, 0, plain_out) < 1) {
        chimera_smb_error("Failed to allocate SMB3 decompression buffer");
        goto out;
    }
    pt = evpl_iovec_data(plain_out);

    if (is_unchained) {
        /* Unchained (MS-SMB2 §2.2.42.1): copy the uncompressed prefix verbatim,
         * then decompress the raw codec segment (no OriginalPayloadSize — that
         * field is chained-only) into the remaining segment_size bytes. */
        uint16_t alg = rd16(cin + 8);
        int      hdr = (int) sizeof(struct smb2_compression_transform_header);

        if (hdr + (int) prefix > length) {
            chimera_smb_error("Invalid SMB3 compression offset %u", prefix);
            goto release;
        }
        memcpy(pt, cin + hdr, prefix);
        if (decompress_codec(alg, cin + hdr + prefix, length - hdr - (int) prefix,
                             pt + prefix, (int) segment_size) != (int) segment_size) {
            chimera_smb_error("SMB3 unchained decompression failed");
            goto release;
        }
    } else {
        /* Chained (MS-SMB2 §2.2.42.2): 8-byte header then a chain of payload
         * headers, each optionally carrying a 4-byte OriginalPayloadSize. */
        int pos    = (int) sizeof(struct smb2_compression_transform_header_chained);
        int outpos = 0;

        while (pos < length) {
            uint16_t palg;
            uint32_t plen;
            int      produced;

            if (pos + (int) sizeof(struct smb2_compression_chained_payload_header) > length) {
                goto release;
            }
            palg = rd16(cin + pos);
            plen = rd32(cin + pos + 4);
            pos += (int) sizeof(struct smb2_compression_chained_payload_header);

            if (plen > (uint32_t) (length - pos)) {
                goto release;
            }
            produced = decompress_payload(palg, cin + pos, (int) plen,
                                          pt + outpos, (int) orig_size - outpos);
            if (produced < 0) {
                goto release;
            }
            outpos += produced;
            pos    += (int) plen;
        }

        if (outpos != (int) orig_size) {
            chimera_smb_error("SMB3 chained decompression size mismatch (%d != %u)",
                              outpos, orig_size);
            goto release;
        }
    }

    evpl_iovec_set_length(plain_out, orig_size);
    *plain_len_out = (int) orig_size;
    rc             = 0;
    goto out;

 release:
    evpl_iovec_release(evpl, plain_out);
 out:
    free(cin);
    return rc;
} /* chimera_smb_decompress_message */

/* A Pattern_V1 payload costs a 16-byte chained header+body, so only runs longer
 * than that shrink the wire; require a comfortable margin. */
#define SMB_PATTERN_MIN_RUN 32

/* Length of the leading run of identical bytes at p[0]. */
static int
run_forward(
    const uint8_t *p,
    int            len)
{
    int i = 1;

    while (i < len && p[i] == p[0]) {
        i++;
    }
    return i;
} /* run_forward */

/* Length of the trailing run of identical bytes at p[len-1]. */
static int
run_backward(
    const uint8_t *p,
    int            len)
{
    int i = 1;

    while (i < len && p[len - 1 - i] == p[len - 1]) {
        i++;
    }
    return i;
} /* run_backward */

/* Append a chained Pattern_V1 payload (8-byte header + 8-byte body) at *pos. */
static int
emit_pattern_payload(
    uint8_t *t,
    int     *pos,
    int      cap,
    uint8_t  pattern,
    uint32_t reps,
    int     *first)
{
    if (*pos + 16 > cap) {
        return -1;
    }
    wr16(t + *pos, SMB2_COMPRESSION_PATTERN_V1);
    wr16(t + *pos + 2, *first ? SMB2_COMPRESSION_FLAG_CHAINED : SMB2_COMPRESSION_FLAG_NONE);
    wr32(t + *pos + 4, (uint32_t) sizeof(struct smb2_compression_pattern_payload_v1));
    *pos       += 8;
    t[*pos]     = pattern;
    t[*pos + 1] = 0;
    wr16(t + *pos + 2, 0);
    wr32(t + *pos + 4, reps);
    *pos  += 8;
    *first = 0;
    return 0;
} /* emit_pattern_payload */

/* Build an unchained Plain-LZ77 transform of plain into t (capacity cap),
 * leaving the first buf_off bytes (the SMB2/response headers) uncompressed as
 * the transform's Offset prefix and compressing only the data buffer that
 * follows.  Returns the transform length, or -1 if it does not fit / shrink. */
static int
build_unchained_lz77(
    const uint8_t *plain,
    int            plain_len,
    int            buf_off,
    uint8_t       *t,
    int            cap)
{
    static const uint8_t proto[4] = SMB2_COMPRESSION_TRANSFORM_PROTO_ID;
    int                  hdr      = (int) sizeof(struct smb2_compression_transform_header);
    int                  seg_len  = plain_len - buf_off;
    int                  comp_len;

    if (seg_len <= 0 || hdr + buf_off > cap) {
        return -1;
    }
    /* [16-byte header][buf_off uncompressed prefix][compressed data segment]. */
    memcpy(t + hdr, plain, buf_off);
    comp_len = chimera_smb_lz77_compress(plain + buf_off, seg_len,
                                         t + hdr + buf_off, cap - hdr - buf_off);
    if (comp_len < 0 || hdr + buf_off + comp_len >= plain_len) {
        return -1;
    }
    memcpy(t, proto, 4);
    wr32(t + 4, (uint32_t) seg_len);    /* OriginalCompressedSegmentSize (segment) */
    wr16(t + 8, SMB2_COMPRESSION_LZ77);
    wr16(t + 10, SMB2_COMPRESSION_FLAG_NONE);
    wr32(t + 12, (uint32_t) buf_off);   /* Offset of the compressed segment */
    return hdr + buf_off + comp_len;
} /* build_unchained_lz77 */

/* Build a chained transform using Pattern_V1 for leading/trailing runs and
 * Plain LZ77 (or NONE) for the middle (MS-SMB2 §2.2.42.2, mirroring the WPTS
 * reference CompressWithPatternV1).  Returns the transform length, or -1 when no
 * worthwhile pattern exists or the result does not shrink. */
static int
build_chained(
    const uint8_t *plain,
    int            plain_len,
    int            buf_off,
    uint8_t       *t,
    int            cap)
{
    static const uint8_t proto[4] = SMB2_COMPRESSION_TRANSFORM_PROTO_ID;
    const uint8_t       *data     = plain + buf_off;     /* the compressible buffer */
    int                  data_len = plain_len - buf_off;
    int                  fwd, bwd, pos = 0, first = 1, mid_off, mid_len;

    if (data_len <= 0) {
        return -1;
    }

    fwd = run_forward(data, data_len);
    bwd = run_backward(data, data_len);

    if (fwd + bwd > data_len) {
        bwd = data_len - fwd;    /* don't let the two runs overlap */
    }
    if (fwd < SMB_PATTERN_MIN_RUN) {
        fwd = 0;
    }
    if (bwd < SMB_PATTERN_MIN_RUN) {
        bwd = 0;
    }
    if (fwd == 0 && bwd == 0) {
        return -1;               /* no run worth a Pattern_V1 payload */
    }

    mid_off = fwd;
    mid_len = data_len - fwd - bwd;

    /* Chained transform header (8 bytes): ProtocolId + OriginalCompressedSegment
     * Size (the full message — sum of all payloads' decompressed sizes). */
    if (cap < 8) {
        return -1;
    }
    memcpy(t, proto, 4);
    wr32(t + 4, (uint32_t) plain_len);
    pos = 8;

    /* The SMB2/response headers preceding the buffer are sent as a leading NONE
     * (raw pass-through) payload. */
    if (buf_off > 0) {
        if (pos + 8 + buf_off > cap) {
            return -1;
        }
        wr16(t + pos, SMB2_COMPRESSION_NONE);
        wr16(t + pos + 2, SMB2_COMPRESSION_FLAG_CHAINED);
        wr32(t + pos + 4, (uint32_t) buf_off);
        pos += 8;
        memcpy(t + pos, plain, buf_off);
        pos  += buf_off;
        first = 0;
    }

    if (fwd > 0 &&
        emit_pattern_payload(t, &pos, cap, data[0], (uint32_t) fwd, &first) != 0) {
        return -1;
    }

    if (mid_len > 0) {
        uint8_t *mc = malloc(mid_len);
        int      mclen;

        if (!mc) {
            return -1;
        }
        mclen = chimera_smb_lz77_compress(data + mid_off, mid_len, mc, mid_len);

        if (mclen >= 0 && mclen + 4 < mid_len) {
            /* LZ77 middle: header + OriginalPayloadSize(4) + compressed. */
            if (pos + 8 + 4 + mclen > cap) {
                free(mc);
                return -1;
            }
            wr16(t + pos, SMB2_COMPRESSION_LZ77);
            wr16(t + pos + 2, first ? SMB2_COMPRESSION_FLAG_CHAINED : SMB2_COMPRESSION_FLAG_NONE);
            wr32(t + pos + 4, (uint32_t) (mclen + 4));
            pos += 8;
            wr32(t + pos, (uint32_t) mid_len);     /* OriginalPayloadSize */
            pos += 4;
            memcpy(t + pos, mc, mclen);
            pos  += mclen;
            first = 0;
        } else {
            /* Incompressible middle: NONE pass-through. */
            if (pos + 8 + mid_len > cap) {
                free(mc);
                return -1;
            }
            wr16(t + pos, SMB2_COMPRESSION_NONE);
            wr16(t + pos + 2, first ? SMB2_COMPRESSION_FLAG_CHAINED : SMB2_COMPRESSION_FLAG_NONE);
            wr32(t + pos + 4, (uint32_t) mid_len);
            pos += 8;
            memcpy(t + pos, data + mid_off, mid_len);
            pos  += mid_len;
            first = 0;
        }
        free(mc);
    }

    if (bwd > 0 &&
        emit_pattern_payload(t, &pos, cap, data[data_len - 1], (uint32_t) bwd, &first) != 0) {
        return -1;
    }

    if (pos >= plain_len) {
        return -1;               /* did not shrink */
    }
    return pos;
} /* build_chained */

int
chimera_smb_compress_message(
    struct chimera_smb_compress_ctx *ctx,
    struct evpl                     *evpl,
    uint16_t                         alg,
    int                              chained,
    int                              buf_off,
    struct evpl_iovec               *plain_iov,
    int                              plain_niov,
    int                              plain_len,
    int                              transport_hdr_len,
    struct evpl_iovec               *out_iov,
    int                             *out_total)
{
    struct evpl_iovec_cursor cursor;
    uint8_t                 *plain;
    uint8_t                 *tbuf;
    uint8_t                 *out;
    int                      tlen = -1, total;

    (void) ctx;

    if (alg != SMB2_COMPRESSION_LZ77 || plain_len <= 0 ||
        buf_off < 0 || buf_off >= plain_len) {
        return -1;
    }

    /* Gather the plaintext SMB2 message (skipping transport framing) into a
     * contiguous scratch buffer, then build the transform into tbuf, capped at
     * plain_len so a non-shrinking result is rejected for free. */
    plain = malloc(plain_len);
    tbuf  = malloc(plain_len);
    if (!plain || !tbuf) {
        free(plain);
        free(tbuf);
        return -1;
    }

    evpl_iovec_cursor_init(&cursor, plain_iov, plain_niov);
    evpl_iovec_cursor_skip(&cursor, transport_hdr_len);
    evpl_iovec_cursor_copy(&cursor, plain, plain_len);

    /* Prefer a chained Pattern_V1 transform when chaining is negotiated and the
     * message has a worthwhile run; otherwise fall back to unchained LZ77. */
    if (chained) {
        tlen = build_chained(plain, plain_len, buf_off, tbuf, plain_len);
    }
    if (tlen < 0) {
        tlen = build_unchained_lz77(plain, plain_len, buf_off, tbuf, plain_len);
    }

    if (tlen < 0 || tlen >= plain_len) {
        free(plain);
        free(tbuf);
        return -1;
    }

    total = transport_hdr_len + tlen;
    if (evpl_iovec_alloc(evpl, total, 8, 1, 0, out_iov) < 1) {
        free(plain);
        free(tbuf);
        return -1;
    }

    out = evpl_iovec_data(out_iov);
    memcpy(out + transport_hdr_len, tbuf, tlen);

    evpl_iovec_set_length(out_iov, total);
    *out_total = tlen;

    free(plain);
    free(tbuf);
    return 0;
} /* chimera_smb_compress_message */
