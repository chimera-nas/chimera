// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB3 transport compression for the SMB2 client (MS-SMB2 3.1.4.4 / 2.2.42).
 *
 * Receive-side only, and that is the whole of the feature against this server:
 * chimera compresses exactly one thing, a READ response, and only when the
 * client set SMB2_READFLAG_REQUEST_COMPRESSED on the READ.  Compression here is
 * therefore something the client ASKS FOR rather than something that happens to
 * it, and a client that never asks never sees a COMPRESSION_TRANSFORM.
 *
 * Compressing outbound writes is deliberately not implemented.  The server does
 * decompress requests, so it would work, but nothing in the read path needs it
 * and an unused encoder is an untested encoder.
 *
 * Scope follows from what the client advertises.  We offer LZ77 alone, and
 * neither Pattern_V1 nor chaining, which pins the server to the UNCHAINED
 * framing (smb_compress.c: comp_chained requires both) and to the one codec
 * below.  Advertising more would oblige us to decode more.
 *
 * The LZ77 decoder mirrors the server's chimera_smb_lz77_decompress, including
 * its bounds handling: the 32-bit length escape is attacker-controlled and can
 * reach ~4 GB, so lengths are decoded and compared in 64-bit and the output
 * bound is written as (out_len - outpos) to keep both sides non-negative.
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "smb_internal.h"

/* Shortest match LZ77 encodes (MS-XCA 2.4). */
#define SMB_CLIENT_LZ77_MIN_MATCH 3

static inline uint16_t
smb_client_rd16(const uint8_t *p)
{
    return (uint16_t) p[0] | ((uint16_t) p[1] << 8);
} /* smb_client_rd16 */

static inline uint32_t
smb_client_rd32(const uint8_t *p)
{
    return (uint32_t) p[0] | ((uint32_t) p[1] << 8) |
           ((uint32_t) p[2] << 16) | ((uint32_t) p[3] << 24);
} /* smb_client_rd32 */

/* Plain LZ77 (MS-XCA 2.4).  Returns out_len on success, -1 on any malformed
* input -- every read of `in` and write to `out` is bounds-checked first. */
int
chimera_smb_client_lz77_decompress(
    const uint8_t *in,
    int            in_len,
    uint8_t       *out,
    int            out_len)
{
    int      inpos = 0, outpos = 0;
    uint32_t flags     = 0;
    int      flagcount = 0;
    int      last_half = -1; /* index in `in` of a byte with a pending high nibble */

    while (outpos < out_len) {
        if (flagcount == 0) {
            if (inpos + 4 > in_len) {
                return -1;
            }
            flags     = smb_client_rd32(in + inpos);
            inpos    += 4;
            flagcount = 32;
        }
        flagcount--;

        if (((flags >> flagcount) & 1u) == 0) {
            if (inpos >= in_len) {
                return -1;
            }
            out[outpos++] = in[inpos++];
            continue;
        }

        uint16_t mb;
        int      offset;
        int64_t  length; /* 64-bit: the escape below reaches ~4 GB */

        if (inpos + 2 > in_len) {
            return -1;
        }
        mb     = smb_client_rd16(in + inpos);
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
                    length = smb_client_rd16(in + inpos);
                    inpos += 2;
                    /* A 16-bit 0 escapes to a 32-bit length. */
                    if (length == 0) {
                        if (inpos + 4 > in_len) {
                            return -1;
                        }
                        length = (int64_t) smb_client_rd32(in + inpos);
                        inpos += 4;
                    }
                    length -= (15 + 7);
                }
                length += 15;
            }
            length += 7;
        }
        length += SMB_CLIENT_LZ77_MIN_MATCH;

        if (offset > outpos || length > (int64_t) (out_len - outpos)) {
            return -1;
        }
        /* Overlapping copy: byte at a time, offset may be < length. */
        for (int i = 0; i < length; i++) {
            out[outpos] = out[outpos - offset];
            outpos++;
        }
    }

    return out_len;
} /* chimera_smb_client_lz77_decompress */

int
chimera_smb_client_decompress_message(
    struct evpl              *evpl,
    struct evpl_iovec_cursor *cursor,
    int                       length,
    struct evpl_iovec        *plain_out,
    int                      *plain_len_out)
{
    struct smb2_compression_transform_header th;
    static const uint8_t                     proto[4] =
        SMB2_COMPRESSION_TRANSFORM_PROTO_ID;
    uint8_t                                 *comp = NULL;
    uint8_t                                 *pt;
    int                                      hdr_len = (int) sizeof(th);
    int                                      prefix, seg_len, comp_len, total;

    if (length < hdr_len) {
        chimera_smbclient_error("Truncated SMB3 compression transform (%d bytes)",
                                length);
        return -1;
    }

    evpl_iovec_cursor_copy(cursor, &th, sizeof(th));

    if (memcmp(th.protocol_id, proto, 4) != 0) {
        chimera_smbclient_error("Invalid SMB3 compression transform protocol id");
        return -1;
    }

    /* We advertise neither Pattern_V1 nor chaining, so the server must not have
     * chained this.  Refuse rather than guess at a framing we did not ask for. */
    if (th.flags != SMB2_COMPRESSION_FLAG_NONE) {
        chimera_smbclient_error(
            "SMB3 compression transform is chained (flags 0x%04x) but chaining "
            "was never negotiated", th.flags);
        return -1;
    }

    if (th.compression_algorithm != SMB2_COMPRESSION_LZ77) {
        chimera_smbclient_error(
            "SMB3 compression transform uses algorithm 0x%04x, which was never "
            "offered", th.compression_algorithm);
        return -1;
    }

    /* [header][Offset bytes verbatim][compressed segment -> seg_len bytes]. */
    prefix   = (int) th.offset;
    seg_len  = (int) th.original_compressed_segment_size;
    comp_len = length - hdr_len - prefix;

    if (prefix < 0 || seg_len <= 0 || comp_len <= 0 ||
        prefix > length - hdr_len) {
        chimera_smbclient_error(
            "Invalid SMB3 compression transform geometry (offset %d, segment %d, "
            "message %d)", prefix, seg_len, length);
        return -1;
    }

    total = prefix + seg_len;

    if (total < (int) sizeof(struct smb2_header)) {
        chimera_smbclient_error(
            "SMB3 compression transform expands to %d bytes, short of an SMB2 "
            "header", total);
        return -1;
    }

    if (evpl_iovec_alloc(evpl, total, 8, 1, 0, plain_out) < 1) {
        chimera_smbclient_error("Failed to allocate SMB3 decompression buffer");
        return -1;
    }

    pt = evpl_iovec_data(plain_out);

    /* The uncompressed prefix precedes the compressed segment on the wire and
     * in the plaintext alike. */
    if (prefix > 0) {
        evpl_iovec_cursor_copy(cursor, pt, prefix);
    }

    comp = malloc(comp_len);
    if (!comp) {
        chimera_smbclient_error("Out of memory decompressing an SMB3 message");
        evpl_iovec_release(evpl, plain_out);
        return -1;
    }
    evpl_iovec_cursor_copy(cursor, comp, comp_len);

    if (chimera_smb_client_lz77_decompress(comp, comp_len, pt + prefix,
                                           seg_len) != seg_len) {
        chimera_smbclient_error("SMB3 LZ77 decompression failed");
        free(comp);
        evpl_iovec_release(evpl, plain_out);
        return -1;
    }

    free(comp);

    chimera_smbclient_debug(
        "SMB3 decompressed a reply: %d wire bytes -> %d (prefix %d, segment %d)",
        length, total, prefix, seg_len);

    evpl_iovec_set_length(plain_out, total);
    *plain_len_out = total;
    return 0;
} /* chimera_smb_client_decompress_message */
