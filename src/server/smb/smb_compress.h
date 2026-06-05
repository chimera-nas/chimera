// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stddef.h>

struct evpl;
struct evpl_iovec;
struct evpl_iovec_cursor;
struct chimera_smb_compress_ctx;

/*
 * SMB3 transport compression (MS-SMB2 §3.1.4.4 / §2.2.42, MS-XCA).  Mirrors the
 * encryption context: a per-thread object holding reusable codec scratch (the
 * LZ77 match-finder hash table), since that scratch is not thread-safe.
 *
 * Codecs implemented so far: Plain LZ77 (MS-XCA §2.4) and the chained Pattern_V1
 * run-length payload (MS-SMB2 §2.2.42.2.2) plus the NONE chained pass-through.
 * LZ77+Huffman and LZNT1 are not yet implemented; the negotiation layer only
 * advertises the algorithms below, so a peer never selects an unimplemented one.
 */
struct chimera_smb_compress_ctx *
chimera_smb_compress_ctx_create(
    void);

void
chimera_smb_compress_ctx_destroy(
    struct chimera_smb_compress_ctx *ctx);

/*
 * Pure MS-XCA Plain LZ77 buffer codecs.  decompress() produces exactly out_len
 * bytes (the caller always knows the original size from the transform header)
 * and returns out_len on success or -1 on malformed input.  compress() returns
 * the compressed length, or -1 if the result would not fit in out_cap (the
 * caller then sends the message uncompressed).  Exposed for unit tests.
 */
int
chimera_smb_lz77_decompress(
    const uint8_t *in,
    int            in_len,
    uint8_t       *out,
    int            out_len);

int
chimera_smb_lz77_compress(
    const uint8_t *in,
    int            in_len,
    uint8_t       *out,
    int            out_cap);

/*
 * Decompress a received SMB2 COMPRESSION_TRANSFORM message.  cursor is
 * positioned at the compression transform header (immediately after the 4-byte
 * NetBIOS framing); length is the transform header plus payload byte count.
 * Handles both the unchained and chained (NONE / Pattern_V1 / LZ77) forms.  On
 * success allocates plain_out (caller must evpl_iovec_release it) holding the
 * reconstructed SMB2 message, sets *plain_len_out, and returns 0.  On a
 * malformed header / payload returns -1 (nothing allocated).
 */
int
chimera_smb_decompress_message(
    struct chimera_smb_compress_ctx *ctx,
    struct evpl                     *evpl,
    struct evpl_iovec_cursor        *cursor,
    int                              length,
    struct evpl_iovec               *plain_out,
    int                             *plain_len_out);

/*
 * Compress an assembled plaintext SMB2 reply for transmission.  plain_iov /
 * plain_niov describe the full reply buffer whose first transport_hdr_len bytes
 * are the (uncompressed) transport framing; plain_len is the SMB2 message length
 * that follows it.  alg is the connection's chosen algorithm.  Produces a single
 * contiguous out_iov laid out as
 *   [transport_hdr_len bytes reserved][16-byte unchained transform header][compressed],
 * leaving the SMB2 header uncompressed as the transform's uncompressed prefix.
 * On success sets *out_total to the transform + payload byte count (excluding the
 * transport framing) and returns 0.  Returns -1 — and allocates nothing — when
 * compression did not shrink the payload (the caller then sends the plaintext).
 */
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
    int                             *out_total);
