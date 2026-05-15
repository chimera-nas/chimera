// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Phase 0 Foundation: unit tests for NEGOTIATE/CREATE context dispatch.
 *
 * These tests drive the parse/emit helpers directly with synthetic wire bytes
 * — no SMB connection, no daemon, no I/O. The wire-byte builders mirror the
 * layouts in MS-SMB2 §2.2.3.1 and §2.2.13.2 so a regression in either parser
 * is caught immediately.
 *
 * Coverage:
 *   - NEGOTIATE contexts: parse one each of preauth, encryption, signing,
 *     compression, netname, transport, rdma_transform.
 *   - CREATE contexts: parse a Win11-style chain (MxAc + QFid + DH2Q),
 *     RqLs v1 vs v2 distinction.
 *   - Malformed CREATE chains: next < 16, next not 8-aligned, data overrun.
 *   - MxAc response emit: ctx_present_mask bit set, build response, decode
 *     wire bytes, assert structure and access mask.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>

#include "server/smb/smb_internal.h"
#include "server/smb/smb_procs.h"

#define TEST_PASS(name) do { fprintf(stderr, "  PASS: %s\n", name); passed++; } while (0)
#define TEST_FAIL(name) do { fprintf(stderr, "  FAIL: %s\n", name); failed++; } while (0)

static int passed = 0;
static int failed = 0;

/* Write LE16/32/64 into a buffer at the given offset. */
static void
put_le16(
    uint8_t *buf,
    uint32_t off,
    uint16_t v)
{
    buf[off]     = v & 0xff;
    buf[off + 1] = (v >> 8) & 0xff;
} /* put_le16 */

static void
put_le32(
    uint8_t *buf,
    uint32_t off,
    uint32_t v)
{
    buf[off]     = v & 0xff;
    buf[off + 1] = (v >> 8) & 0xff;
    buf[off + 2] = (v >> 16) & 0xff;
    buf[off + 3] = (v >> 24) & 0xff;
} /* put_le32 */

static void
put_le64(
    uint8_t *buf,
    uint32_t off,
    uint64_t v)
{
    put_le32(buf, off, (uint32_t) v);
    put_le32(buf, off + 4, (uint32_t) (v >> 32));
} /* put_le64 */

/* ------------------------------------------------------------------ *
*  NEGOTIATE context tests                                           *
* ------------------------------------------------------------------ */

static void
test_negotiate_preauth(void)
{
    struct chimera_smb_request req;
    uint8_t                    data[40];
    int                        i;

    memset(&req, 0, sizeof(req));

    put_le16(data, 0, 1);                              /* HashAlgorithmCount */
    put_le16(data, 2, 32);                             /* SaltLength */
    put_le16(data, 4, SMB2_PREAUTH_HASH_SHA_512);      /* HashAlgorithms[0] */
    for (i = 0; i < 32; i++) {
        data[6 + i] = (uint8_t) (0xa0 + i);
    }

    chimera_smb_parse_one_negotiate_context(
        &req, SMB2_PREAUTH_INTEGRITY_CAPABILITIES, data, 6 + 32);

    if ((req.negotiate.ctx_present_mask & CHIMERA_SMB_NEGOTIATE_CTX_PREAUTH) &&
        req.negotiate.preauth_in.hash_alg_count == 1 &&
        req.negotiate.preauth_in.hash_algs[0] == SMB2_PREAUTH_HASH_SHA_512 &&
        req.negotiate.preauth_in.salt_length == 32 &&
        req.negotiate.preauth_in.salt[0]  == 0xa0 &&
        req.negotiate.preauth_in.salt[31] == (uint8_t) 0xbf) {
        TEST_PASS("preauth: SHA-512 + 32-byte salt");
    } else {
        TEST_FAIL("preauth: SHA-512 + 32-byte salt");
    }
} /* test_negotiate_preauth */

static void
test_negotiate_encryption(void)
{
    struct chimera_smb_request req;
    uint8_t                    data[16];

    memset(&req, 0, sizeof(req));
    /* Win11 24H2 order: GCM-128, CCM-128, GCM-256, CCM-256 */
    put_le16(data, 0, 4);                              /* CipherCount */
    put_le16(data, 2, SMB2_ENCRYPTION_AES_128_GCM);
    put_le16(data, 4, SMB2_ENCRYPTION_AES_128_CCM);
    put_le16(data, 6, SMB2_ENCRYPTION_AES_256_GCM);
    put_le16(data, 8, SMB2_ENCRYPTION_AES_256_CCM);

    chimera_smb_parse_one_negotiate_context(
        &req, SMB2_ENCRYPTION_CAPABILITIES, data, 10);

    if ((req.negotiate.ctx_present_mask & CHIMERA_SMB_NEGOTIATE_CTX_ENCRYPTION) &&
        req.negotiate.encryption_in.cipher_count == 4 &&
        req.negotiate.encryption_in.ciphers[0] == SMB2_ENCRYPTION_AES_128_GCM &&
        req.negotiate.encryption_in.ciphers[3] == SMB2_ENCRYPTION_AES_256_CCM) {
        TEST_PASS("encryption: Win11 cipher order parsed");
    } else {
        TEST_FAIL("encryption: Win11 cipher order parsed");
    }
} /* test_negotiate_encryption */

static void
test_negotiate_signing(void)
{
    struct chimera_smb_request req;
    uint8_t                    data[8];

    memset(&req, 0, sizeof(req));
    put_le16(data, 0, 3);                              /* SigningAlgorithmCount */
    put_le16(data, 2, SMB2_SIGNING_AES_GMAC);
    put_le16(data, 4, SMB2_SIGNING_AES_CMAC);
    put_le16(data, 6, SMB2_SIGNING_HMAC_SHA256);

    chimera_smb_parse_one_negotiate_context(
        &req, SMB2_SIGNING_CAPABILITIES, data, 8);

    if ((req.negotiate.ctx_present_mask & CHIMERA_SMB_NEGOTIATE_CTX_SIGNING) &&
        req.negotiate.signing_in.alg_count == 3 &&
        req.negotiate.signing_in.algs[0] == SMB2_SIGNING_AES_GMAC) {
        TEST_PASS("signing: GMAC > CMAC > HMAC-SHA256");
    } else {
        TEST_FAIL("signing: GMAC > CMAC > HMAC-SHA256");
    }
} /* test_negotiate_signing */

static void
test_negotiate_compression(void)
{
    struct chimera_smb_request req;
    uint8_t                    data[18];

    memset(&req, 0, sizeof(req));
    put_le16(data, 0, 5);                              /* CompressionAlgorithmCount */
    put_le16(data, 2, 0);                              /* Padding */
    put_le32(data, 4, SMB2_COMPRESSION_FLAG_CHAINED);
    put_le16(data, 8,  0x0004);                        /* Pattern_V1 */
    put_le16(data, 10, 0x0002);                        /* LZ77 */
    put_le16(data, 12, 0x0003);                        /* LZ77+Huffman */
    put_le16(data, 14, 0x0001);                        /* LZNT1 */
    put_le16(data, 16, 0x0005);                        /* LZ4 */

    chimera_smb_parse_one_negotiate_context(
        &req, SMB2_COMPRESSION_CAPABILITIES, data, 18);

    if ((req.negotiate.ctx_present_mask & CHIMERA_SMB_NEGOTIATE_CTX_COMPRESSION) &&
        req.negotiate.compression_in.alg_count == 5 &&
        req.negotiate.compression_in.flags == SMB2_COMPRESSION_FLAG_CHAINED &&
        req.negotiate.compression_in.algs[4] == 0x0005) {
        TEST_PASS("compression: chained, 5 algs incl. LZ4");
    } else {
        TEST_FAIL("compression: chained, 5 algs incl. LZ4");
    }
} /* test_negotiate_compression */

static void
test_negotiate_netname(void)
{
    struct chimera_smb_request req;
    uint8_t                    data[64];
    /* "jrh-ubuntu-debug" in UTF-16LE = 32 bytes */
    static const char         *src = "jrh-ubuntu-debug";
    size_t                     n   = strlen(src);
    size_t                     i;

    memset(&req, 0, sizeof(req));
    memset(data, 0, sizeof(data));
    for (i = 0; i < n; i++) {
        data[i * 2]     = (uint8_t) src[i];
        data[i * 2 + 1] = 0;
    }

    chimera_smb_parse_one_negotiate_context(
        &req, SMB2_NETNAME_NEGOTIATE_CONTEXT_ID, data, (uint16_t) (n * 2));

    if ((req.negotiate.ctx_present_mask & CHIMERA_SMB_NEGOTIATE_CTX_NETNAME) &&
        req.negotiate.netname_in.length_bytes == n * 2 &&
        req.negotiate.netname_in.utf16le[0] == 'j' &&
        req.negotiate.netname_in.utf16le[1] == 0) {
        TEST_PASS("netname: UTF-16LE hostname stored");
    } else {
        TEST_FAIL("netname: UTF-16LE hostname stored");
    }
} /* test_negotiate_netname */

static void
test_negotiate_rdma_transform(void)
{
    struct chimera_smb_request req;
    uint8_t                    data[12];

    memset(&req, 0, sizeof(req));
    put_le16(data, 0, 2);                              /* TransformCount */
    put_le16(data, 2, 0);                              /* Reserved1 */
    put_le32(data, 4, 0);                              /* Reserved2 */
    put_le16(data, 8,  0x0001);                        /* Encryption */
    put_le16(data, 10, 0x0002);                        /* Signing */

    chimera_smb_parse_one_negotiate_context(
        &req, SMB2_RDMA_TRANSFORM_CAPABILITIES, data, 12);

    if ((req.negotiate.ctx_present_mask & CHIMERA_SMB_NEGOTIATE_CTX_RDMA_TRANSFORM) &&
        req.negotiate.rdma_transform_in.transform_count == 2 &&
        req.negotiate.rdma_transform_in.transforms[0] == 0x0001 &&
        req.negotiate.rdma_transform_in.transforms[1] == 0x0002) {
        TEST_PASS("rdma_transform: 2 transforms parsed");
    } else {
        TEST_FAIL("rdma_transform: 2 transforms parsed");
    }
} /* test_negotiate_rdma_transform */

static void
test_negotiate_transport(void)
{
    struct chimera_smb_request req;
    uint8_t                    data[4];

    memset(&req, 0, sizeof(req));
    put_le32(data, 0, 0x00000001);

    chimera_smb_parse_one_negotiate_context(
        &req, SMB2_TRANSPORT_CAPABILITIES, data, 4);

    if ((req.negotiate.ctx_present_mask & CHIMERA_SMB_NEGOTIATE_CTX_TRANSPORT) &&
        req.negotiate.transport_in.flags == 0x00000001) {
        TEST_PASS("transport: flags parsed");
    } else {
        TEST_FAIL("transport: flags parsed");
    }
} /* test_negotiate_transport */

static void
test_negotiate_unknown_type_ignored(void)
{
    struct chimera_smb_request req;
    uint8_t                    data[4] = { 0 };

    memset(&req, 0, sizeof(req));
    /* SMB2_CONTEXTTYPE_RESERVED = 0x0100, must be silently ignored on receipt */
    chimera_smb_parse_one_negotiate_context(&req, 0x0100, data, 4);

    if (req.negotiate.ctx_present_mask == 0) {
        TEST_PASS("unknown type: silently ignored (no mask bit set)");
    } else {
        TEST_FAIL("unknown type: silently ignored (no mask bit set)");
    }
} /* test_negotiate_unknown_type_ignored */

/* ------------------------------------------------------------------ *
*  CREATE context tests                                              *
* ------------------------------------------------------------------ */

/* Build one CREATE context entry in the buffer at `pos`, return advance. */
static uint32_t
emit_create_ctx_entry(
    uint8_t       *buf,
    uint32_t       pos,
    const char    *tag,
    const uint8_t *data,
    uint32_t       data_len,
    int            is_last)
{
    /* Layout: header(16) + name(4) + 4 pad + data + pad to 8 */
    uint32_t total   = 24 + data_len;
    uint32_t advance = (total + 7u) & ~7u;
    uint32_t next    = is_last ? 0 : advance;

    put_le32(buf, pos + 0, next);
    put_le16(buf, pos + 4, 16);   /* NameOffset */
    put_le16(buf, pos + 6, 4);    /* NameLength */
    put_le16(buf, pos + 8, 0);    /* Reserved */
    put_le16(buf, pos + 10, 24);  /* DataOffset */
    put_le32(buf, pos + 12, data_len);
    memcpy(buf + pos + 16, tag, 4);
    buf[pos + 20] = buf[pos + 21] = buf[pos + 22] = buf[pos + 23] = 0;
    if (data_len) {
        memcpy(buf + pos + 24, data, data_len);
    }
    if (advance > total) {
        memset(buf + pos + total, 0, advance - total);
    }
    return advance;
} /* emit_create_ctx_entry */

static void
test_create_win11_chain(void)
{
    /* Win11 24H2 sends DH2Q + MxAc + QFid on file create. */
    struct chimera_smb_request req;
    uint8_t                    buf[512];
    uint8_t                    dh2q_body[32];
    uint32_t                   pos = 0;
    int                        i;

    memset(&req, 0, sizeof(req));
    memset(buf, 0, sizeof(buf));

    /* DH2Q body: Timeout(4) | Flags(4) | Reserved(8) | CreateGuid(16) */
    put_le32(dh2q_body, 0, 30000);                     /* 30s timeout */
    put_le32(dh2q_body, 4, 0);                         /* flags */
    for (i = 0; i < 16; i++) {
        dh2q_body[16 + i] = (uint8_t) (0x10 + i);
    }

    pos += emit_create_ctx_entry(buf, pos, "DH2Q", dh2q_body, 32, 0);
    pos += emit_create_ctx_entry(buf, pos, "MxAc", NULL, 0, 0);
    pos += emit_create_ctx_entry(buf, pos, "QFid", NULL, 0, 1);

    if (chimera_smb_parse_create_contexts(buf, pos, &req) != 0) {
        TEST_FAIL("Win11 chain: parser returned error");
        return;
    }

    if ((req.create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_DH2Q) &&
        (req.create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_MXAC) &&
        (req.create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_QFID) &&
        req.create.dh2q.timeout_ms == 30000 &&
        req.create.dh2q.create_guid[0] == 0x10 &&
        req.create.dh2q.create_guid[15] == 0x1f) {
        TEST_PASS("Win11 chain: DH2Q + MxAc + QFid parsed");
    } else {
        TEST_FAIL("Win11 chain: DH2Q + MxAc + QFid parsed");
    }
} /* test_create_win11_chain */

static void
test_create_rqls_v1_vs_v2(void)
{
    struct chimera_smb_request req;
    uint8_t                    buf[128];
    uint8_t                    rqls_v1[32], rqls_v2[52];
    uint32_t                   pos = 0;
    int                        i;

    memset(&req, 0, sizeof(req));
    memset(buf, 0, sizeof(buf));
    memset(rqls_v1, 0, sizeof(rqls_v1));
    memset(rqls_v2, 0, sizeof(rqls_v2));

    /* v1: 16-byte key + state(4) + flags(4) + duration(8 reserved) */
    for (i = 0; i < 16; i++) {
        rqls_v1[i] = (uint8_t) (0x80 + i);
    }
    put_le32(rqls_v1, 16, 0x07);  /* RWH */
    put_le32(rqls_v1, 20, 0);
    pos += emit_create_ctx_entry(buf, pos, "RqLs", rqls_v1, 32, 1);

    if (chimera_smb_parse_create_contexts(buf, pos, &req) != 0) {
        TEST_FAIL("RqLs v1 parse");
        return;
    }
    if ((req.create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) &&
        !(req.create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS_V2) &&
        req.create.rqls.is_v2 == 0 &&
        req.create.rqls.key[0]  == 0x80 &&
        req.create.rqls.key[15] == 0x8f &&
        req.create.rqls.state == 0x07) {
        TEST_PASS("RqLs v1 (32-byte body) parsed");
    } else {
        TEST_FAIL("RqLs v1 (32-byte body) parsed");
    }

    /* Second request: v2 — sets BOTH RQLS and RQLS_V2 bits */
    memset(&req, 0, sizeof(req));
    memset(buf, 0, sizeof(buf));
    for (i = 0; i < 16; i++) {
        rqls_v2[i] = (uint8_t) (0xc0 + i);
    }
    put_le32(rqls_v2, 16, 0x07);
    put_le32(rqls_v2, 20, 0x02);  /* parent_key valid */
    for (i = 0; i < 16; i++) {
        rqls_v2[32 + i] = (uint8_t) (0xd0 + i);
    }
    put_le16(rqls_v2, 48, 7);     /* Epoch */
    pos = emit_create_ctx_entry(buf, 0, "RqLs", rqls_v2, 52, 1);

    if (chimera_smb_parse_create_contexts(buf, pos, &req) != 0) {
        TEST_FAIL("RqLs v2 parse");
        return;
    }
    if ((req.create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS_V2) &&
        req.create.rqls.is_v2 == 1 &&
        req.create.rqls.parent_key[0] == 0xd0 &&
        req.create.rqls.epoch == 7) {
        TEST_PASS("RqLs v2 (52-byte body) parsed");
    } else {
        TEST_FAIL("RqLs v2 (52-byte body) parsed");
    }
} /* test_create_rqls_v1_vs_v2 */

static void
test_create_malformed_short_next(void)
{
    struct chimera_smb_request req;
    uint8_t                    buf[64] = { 0 };

    memset(&req, 0, sizeof(req));
    /* Header alone is 16 bytes; setting Next=8 is invalid (< 16) */
    put_le32(buf, 0, 8);   /* Next = 8 — too small */
    put_le16(buf, 4, 16);
    put_le16(buf, 6, 4);
    put_le16(buf, 10, 0);
    put_le32(buf, 12, 0);
    memcpy(buf + 16, "MxAc", 4);

    if (chimera_smb_parse_create_contexts(buf, 64, &req) < 0 &&
        req.status == SMB2_STATUS_INVALID_PARAMETER) {
        TEST_PASS("malformed CREATE: Next < 16 rejected");
    } else {
        TEST_FAIL("malformed CREATE: Next < 16 rejected");
    }
} /* test_create_malformed_short_next */

static void
test_create_malformed_unaligned_next(void)
{
    struct chimera_smb_request req;
    uint8_t                    buf[64] = { 0 };

    memset(&req, 0, sizeof(req));
    put_le32(buf, 0, 20);  /* Next = 20 — not 8-aligned */
    put_le16(buf, 4, 16);
    put_le16(buf, 6, 4);
    put_le16(buf, 10, 0);
    put_le32(buf, 12, 0);
    memcpy(buf + 16, "MxAc", 4);

    if (chimera_smb_parse_create_contexts(buf, 64, &req) < 0 &&
        req.status == SMB2_STATUS_INVALID_PARAMETER) {
        TEST_PASS("malformed CREATE: Next not 8-aligned rejected");
    } else {
        TEST_FAIL("malformed CREATE: Next not 8-aligned rejected");
    }
} /* test_create_malformed_unaligned_next */

static void
test_create_malformed_data_overrun(void)
{
    struct chimera_smb_request req;
    uint8_t                    buf[32] = { 0 };

    memset(&req, 0, sizeof(req));
    put_le32(buf, 0, 0);    /* Last */
    put_le16(buf, 4, 16);
    put_le16(buf, 6, 4);
    put_le16(buf, 10, 24);
    put_le32(buf, 12, 200); /* DataLength 200 in a 32-byte buffer */
    memcpy(buf + 16, "DH2Q", 4);

    if (chimera_smb_parse_create_contexts(buf, 32, &req) < 0 &&
        req.status == SMB2_STATUS_INVALID_PARAMETER) {
        TEST_PASS("malformed CREATE: data overrun rejected");
    } else {
        TEST_FAIL("malformed CREATE: data overrun rejected");
    }
} /* test_create_malformed_data_overrun */

static void
test_create_response_mxac_on_maximum_allowed(void)
{
    struct chimera_smb_request   req;
    struct chimera_smb_open_file open_file;
    uint8_t                      ctx_buf[256];
    uint32_t                     ctx_len;

    memset(&req,       0, sizeof(req));
    memset(&open_file, 0, sizeof(open_file));

    req.create.ctx_present_mask = CHIMERA_SMB_CREATE_CTX_MXAC;
    /* Client opened with MAXIMUM_ALLOWED — this is the only case where Phase 0
     * emits the MxAc reply (per spec, MaximalAccess is the user's effective
     * rights; for specific-access opens we don't have DACL evaluation yet). */
    req.create.desired_access = SMB2_MAXIMUM_ALLOWED;
    open_file.desired_access  = 0x001f01ff;            /* FILE_ALL_ACCESS */
    req.create.r_open_file    = &open_file;

    ctx_len = chimera_smb_build_create_response_contexts(&req, ctx_buf, sizeof(ctx_buf));

    /* Expected wire: 24 + 8 = 32 bytes, single context.
     * Header @ 0..15: Next=0, NameOffset=16, NameLength=4, Reserved=0, DataOffset=24, DataLength=8.
     * Name "MxAc" @ 16..19.
     * Data @ 24..31: QueryStatus=0, MaximalAccess=0x001f01ff. */
    if (ctx_len == 32 &&
        ctx_buf[0]  == 0 && ctx_buf[1]  == 0 && ctx_buf[2] == 0 && ctx_buf[3] == 0 &&
        ctx_buf[4]  == 16 && ctx_buf[6]  == 4 &&
        ctx_buf[10] == 24 && ctx_buf[12] == 8 &&
        ctx_buf[16] == 'M' && ctx_buf[17] == 'x' && ctx_buf[18] == 'A' && ctx_buf[19] == 'c' &&
        ctx_buf[24] == 0 && ctx_buf[25] == 0 && ctx_buf[26] == 0 && ctx_buf[27] == 0 &&
        ctx_buf[28] == 0xff && ctx_buf[29] == 0x01 && ctx_buf[30] == 0x1f && ctx_buf[31] == 0x00) {
        TEST_PASS("MxAc response on MAXIMUM_ALLOWED: 32-byte chain entry");
    } else {
        TEST_FAIL("MxAc response on MAXIMUM_ALLOWED: 32-byte chain entry");
        fprintf(stderr, "    got ctx_len=%u, bytes:", ctx_len);
        for (uint32_t i = 0; i < ctx_len && i < 40; i++) {
            fprintf(stderr, " %02x", ctx_buf[i]);
        }
        fprintf(stderr, "\n");
    }
} /* test_create_response_mxac_on_maximum_allowed */

static void
test_create_response_mxac_suppressed_on_specific_access(void)
{
    struct chimera_smb_request   req;
    struct chimera_smb_open_file open_file;
    uint8_t                      ctx_buf[256];
    uint32_t                     ctx_len;

    memset(&req,       0, sizeof(req));
    memset(&open_file, 0, sizeof(open_file));

    /* Client sent MxAc but opened with specific bits — we suppress the reply
     * because we don't have effective-rights evaluation yet. */
    req.create.ctx_present_mask = CHIMERA_SMB_CREATE_CTX_MXAC;
    req.create.desired_access   = 0x00000080;          /* FILE_READ_ATTRIBUTES */
    open_file.desired_access    = 0x00000080;
    req.create.r_open_file      = &open_file;

    ctx_len = chimera_smb_build_create_response_contexts(&req, ctx_buf, sizeof(ctx_buf));

    if (ctx_len == 0) {
        TEST_PASS("MxAc suppressed for specific-access open (no DACL eval yet)");
    } else {
        TEST_FAIL("MxAc suppressed for specific-access open (no DACL eval yet)");
    }
} /* test_create_response_mxac_suppressed_on_specific_access */

static void
test_create_response_empty_when_no_mxac_bit(void)
{
    struct chimera_smb_request   req;
    struct chimera_smb_open_file open_file;
    uint8_t                      ctx_buf[256];
    uint32_t                     ctx_len;

    memset(&req,       0, sizeof(req));
    memset(&open_file, 0, sizeof(open_file));

    /* Client did not send MxAc — we must emit nothing for it. */
    req.create.ctx_present_mask = CHIMERA_SMB_CREATE_CTX_QFID;
    req.create.desired_access   = SMB2_MAXIMUM_ALLOWED;
    req.create.r_open_file      = &open_file;

    ctx_len = chimera_smb_build_create_response_contexts(&req, ctx_buf, sizeof(ctx_buf));

    if (ctx_len == 0) {
        TEST_PASS("CREATE response: empty when client didn't ask for MxAc");
    } else {
        TEST_FAIL("CREATE response: empty when client didn't ask for MxAc");
    }
} /* test_create_response_empty_when_no_mxac_bit */

/* ------------------------------------------------------------------ *
*  Per-tag CREATE-context parser tests (DHnC, DH2C, AlSi, TWrp, SecD) *
* ------------------------------------------------------------------ */

static void
test_create_ctx_dhnc(void)
{
    struct chimera_smb_request req;
    uint8_t                    buf[64];
    uint8_t                    body[32];
    uint32_t                   pos;
    int                        i;

    memset(&req, 0, sizeof(req));
    memset(buf, 0, sizeof(buf));
    memset(body, 0, sizeof(body));

    /* DHnC body: FileId(16) + 16 reserved bytes */
    put_le64(body, 0,  0x0123456789abcdefULL);  /* persistent */
    put_le64(body, 8,  0xfedcba9876543210ULL);  /* volatile */
    for (i = 16; i < 32; i++) {
        body[i] = 0;
    }

    pos = emit_create_ctx_entry(buf, 0, "DHnC", body, 32, 1);

    if (chimera_smb_parse_create_contexts(buf, pos, &req) != 0) {
        TEST_FAIL("DHnC parse");
        return;
    }
    if ((req.create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_DHNC) &&
        req.create.dhnc.persistent == 0x0123456789abcdefULL &&
        req.create.dhnc.volatile_id == 0xfedcba9876543210ULL) {
        TEST_PASS("DHnC: FileId parsed");
    } else {
        TEST_FAIL("DHnC: FileId parsed");
    }
} /* test_create_ctx_dhnc */

static void
test_create_ctx_dh2c(void)
{
    struct chimera_smb_request req;
    uint8_t                    buf[64];
    uint8_t                    body[36];
    uint32_t                   pos;
    int                        i;

    memset(&req, 0, sizeof(req));
    memset(buf, 0, sizeof(buf));
    memset(body, 0, sizeof(body));

    /* DH2C body: FileId(16) + CreateGuid(16) + Flags(4) */
    put_le64(body, 0, 0x1111111111111111ULL);
    put_le64(body, 8, 0x2222222222222222ULL);
    for (i = 0; i < 16; i++) {
        body[16 + i] = (uint8_t) (0x40 + i);
    }
    put_le32(body, 32, 0x00000002);            /* SMB2_DHANDLE_FLAG_PERSISTENT */

    pos = emit_create_ctx_entry(buf, 0, "DH2C", body, 36, 1);

    if (chimera_smb_parse_create_contexts(buf, pos, &req) != 0) {
        TEST_FAIL("DH2C parse");
        return;
    }
    if ((req.create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_DH2C) &&
        req.create.dh2c.persistent == 0x1111111111111111ULL &&
        req.create.dh2c.volatile_id == 0x2222222222222222ULL &&
        req.create.dh2c.create_guid[0] == 0x40 &&
        req.create.dh2c.create_guid[15] == 0x4f &&
        req.create.dh2c.flags == 0x00000002) {
        TEST_PASS("DH2C: FileId + CreateGuid + Flags parsed");
    } else {
        TEST_FAIL("DH2C: FileId + CreateGuid + Flags parsed");
    }
} /* test_create_ctx_dh2c */

static void
test_create_ctx_alsi(void)
{
    struct chimera_smb_request req;
    uint8_t                    buf[64];
    uint8_t                    body[8];
    uint32_t                   pos;

    memset(&req, 0, sizeof(req));
    memset(buf, 0, sizeof(buf));

    put_le64(body, 0, 0x12345678ULL);          /* allocation size */
    pos = emit_create_ctx_entry(buf, 0, "AlSi", body, 8, 1);

    if (chimera_smb_parse_create_contexts(buf, pos, &req) != 0) {
        TEST_FAIL("AlSi parse");
        return;
    }
    if ((req.create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_ALSI) &&
        req.create.alsi_alloc_size == 0x12345678ULL) {
        TEST_PASS("AlSi: 64-bit allocation size parsed");
    } else {
        TEST_FAIL("AlSi: 64-bit allocation size parsed");
    }
} /* test_create_ctx_alsi */

static void
test_create_ctx_twrp(void)
{
    struct chimera_smb_request req;
    uint8_t                    buf[64];
    uint8_t                    body[8];
    uint32_t                   pos;

    memset(&req, 0, sizeof(req));
    memset(buf, 0, sizeof(buf));

    /* TWrp timestamp is FILETIME (100ns ticks since 1601). Use an arbitrary value. */
    put_le64(body, 0, 132514560000000000ULL);  /* 2020-09-22 UTC */
    pos = emit_create_ctx_entry(buf, 0, "TWrp", body, 8, 1);

    if (chimera_smb_parse_create_contexts(buf, pos, &req) != 0) {
        TEST_FAIL("TWrp parse");
        return;
    }
    if ((req.create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_TWRP) &&
        req.create.twrp_timestamp == 132514560000000000ULL) {
        TEST_PASS("TWrp: timewarp timestamp parsed");
    } else {
        TEST_FAIL("TWrp: timewarp timestamp parsed");
    }
} /* test_create_ctx_twrp */

/* Minimal valid SecD body: 20-byte SD header with owner-SID offset, followed by
 * a single S-1-5-88-1-<uid> SID. Exercises the new dispatch table's path to
 * chimera_smb_parse_sd_to_attrs (the one production behavior already in place
 * before Phase 0). */
static void
test_create_ctx_secd(void)
{
    struct chimera_smb_request req;
    uint8_t                    buf[128];
    uint8_t                    sd[40];
    uint32_t                   pos;

    memset(&req, 0, sizeof(req));
    memset(buf, 0, sizeof(buf));
    memset(sd, 0, sizeof(sd));

    /* SD header (20 bytes): owner_offset @ bytes 4-7, group @ 8-11, DACL @ 16-19.
     * Place a single owner SID immediately after the header at offset 20. */
    put_le32(sd, 4, 20);          /* owner_offset = 20 */

    /* Owner SID at offset 20: S-1-5-88-1-5000 */
    sd[20] = 1;                   /* revision */
    sd[21] = 3;                   /* sub_authority_count */
    sd[27] = 5;                   /* authority = NT */
    sd[28] = 88;                  /* sub-authority[0] = 88 */
    put_le32(sd, 32, 1);          /* kind = 1 (UID) */
    put_le32(sd, 36, 5000);       /* value = 5000 */

    pos = emit_create_ctx_entry(buf, 0, "SecD", sd, 40, 1);

    if (chimera_smb_parse_create_contexts(buf, pos, &req) != 0) {
        TEST_FAIL("SecD parse");
        return;
    }
    if ((req.create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_SECD) &&
        (req.create.set_attr.va_set_mask & CHIMERA_VFS_ATTR_UID) &&
        req.create.set_attr.va_uid == 5000) {
        TEST_PASS("SecD: dispatch routes to SD parser, UID 5000 extracted");
    } else {
        TEST_FAIL("SecD: dispatch routes to SD parser, UID 5000 extracted");
        fprintf(stderr, "    mask=0x%x va_set_mask=0x%llx va_uid=%llu\n",
                req.create.ctx_present_mask,
                (unsigned long long) req.create.set_attr.va_set_mask,
                (unsigned long long) req.create.set_attr.va_uid);
    }
} /* test_create_ctx_secd */

/* ------------------------------------------------------------------ *
*  Hardening: malformed inputs that should be tolerated, not crashes  *
* ------------------------------------------------------------------ */

static void
test_create_ctx_name_off_zero_silently_skipped(void)
{
    /* A context with NameOffset = 0 makes the "name" overlap the header. The
     * 4-byte dispatch reads the first 4 bytes of the header as the tag, which
     * won't match anything in our table. The parser must accept this and
     * advance to the next entry (or end) without crashing. */
    struct chimera_smb_request req;
    uint8_t                    buf[64] = { 0 };

    memset(&req, 0, sizeof(req));
    put_le32(buf, 0, 0);     /* Next = 0 (last) */
    put_le16(buf, 4, 0);     /* NameOffset = 0 — points at the header */
    put_le16(buf, 6, 4);     /* NameLength = 4 */
    put_le16(buf, 10, 0);    /* DataOffset */
    put_le32(buf, 12, 0);    /* DataLength = 0 */

    if (chimera_smb_parse_create_contexts(buf, 64, &req) == 0 &&
        req.create.ctx_present_mask == 0) {
        TEST_PASS("CREATE: name_off=0 entry silently skipped (no crash)");
    } else {
        TEST_FAIL("CREATE: name_off=0 entry silently skipped (no crash)");
    }
} /* test_create_ctx_name_off_zero_silently_skipped */

static void
test_negotiate_context_oversized_rejected(void)
{
    /* Negotiate-context length cap is 512 bytes per the parser. A length
     * field declaring >512 should be rejected before we even try to copy.
     * We test the per-context parser directly with a synthetic too-long body. */
    struct chimera_smb_request req;
    uint8_t                    big_data[600] = { 0 };

    memset(&req, 0, sizeof(req));
    /* parse_one_negotiate_context's per-type parsers don't cap on data_len —
     * the cap is enforced in chimera_smb_parse_negotiate's loop. Here we just
     * confirm that a known type with malformed-but-bounded body returns 0
     * (silently ignored — bit not set) and doesn't crash. */
    if (chimera_smb_parse_one_negotiate_context(&req, SMB2_PREAUTH_INTEGRITY_CAPABILITIES,
                                                big_data, 2) == 0 &&
        (req.negotiate.ctx_present_mask & CHIMERA_SMB_NEGOTIATE_CTX_PREAUTH) == 0) {
        TEST_PASS("preauth: too-short body (< 4) is ignored, no bit set");
    } else {
        TEST_FAIL("preauth: too-short body (< 4) is ignored, no bit set");
    }
} /* test_negotiate_context_oversized_rejected */

static void
test_negotiate_duplicate_context_rejected(void)
{
    /* A duplicate context type must cause the dispatcher to return -1 and
     * set request->status = STATUS_INVALID_PARAMETER. */
    struct chimera_smb_request req;
    uint8_t                    data[8];

    memset(&req, 0, sizeof(req));
    put_le16(data, 0, 1);  /* CipherCount = 1 */
    put_le16(data, 2, SMB2_ENCRYPTION_AES_128_GCM);

    if (chimera_smb_parse_one_negotiate_context(
            &req, SMB2_ENCRYPTION_CAPABILITIES, data, 4) != 0) {
        TEST_FAIL("duplicate negotiate context: first parse should succeed");
        return;
    }
    if (chimera_smb_parse_one_negotiate_context(
            &req, SMB2_ENCRYPTION_CAPABILITIES, data, 4) >= 0) {
        TEST_FAIL("duplicate negotiate context: second parse should fail");
        return;
    }
    if (req.status == SMB2_STATUS_INVALID_PARAMETER) {
        TEST_PASS("duplicate negotiate context type rejected with STATUS_INVALID_PARAMETER");
    } else {
        TEST_FAIL("duplicate negotiate context type rejected with STATUS_INVALID_PARAMETER");
    }
} /* test_negotiate_duplicate_context_rejected */

int
main(
    int   argc,
    char *argv[])
{
    (void) argc;
    (void) argv;

    chimera_log_init();

    fprintf(stderr, "=== Phase 0: NEGOTIATE context parse ===\n");
    test_negotiate_preauth();
    test_negotiate_encryption();
    test_negotiate_signing();
    test_negotiate_compression();
    test_negotiate_netname();
    test_negotiate_rdma_transform();
    test_negotiate_transport();
    test_negotiate_unknown_type_ignored();
    test_negotiate_context_oversized_rejected();
    test_negotiate_duplicate_context_rejected();

    fprintf(stderr, "=== Phase 0: CREATE context parse ===\n");
    test_create_win11_chain();
    test_create_rqls_v1_vs_v2();
    test_create_ctx_dhnc();
    test_create_ctx_dh2c();
    test_create_ctx_alsi();
    test_create_ctx_twrp();
    test_create_ctx_secd();
    test_create_malformed_short_next();
    test_create_malformed_unaligned_next();
    test_create_malformed_data_overrun();
    test_create_ctx_name_off_zero_silently_skipped();

    fprintf(stderr, "=== Phase 0: CREATE context emit ===\n");
    test_create_response_mxac_on_maximum_allowed();
    test_create_response_mxac_suppressed_on_specific_access();
    test_create_response_empty_when_no_mxac_bit();

    fprintf(stderr, "\nTotal: %d passed, %d failed\n", passed, failed);
    return failed == 0 ? 0 : 1;
} /* main */
