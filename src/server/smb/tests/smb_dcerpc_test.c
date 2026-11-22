// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * DCE/RPC named pipes (smb_rpc.h over libdcerpc): drives the pipe glue the
 * SMB2 WRITE / READ / FSCTL_PIPE_TRANSCEIVE paths use with hand-built PDUs.
 * No server, no evpl instance; a minimal shared state stands in for the SMB
 * server's.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "common/test_host.h"
#include "server/smb/smb_internal.h"
#include "server/smb/smb_rpc.h"

static int passed = 0;
static int failed = 0;

#define CHECK(cond, name) do { \
            if (cond) { fprintf(stderr, "  PASS: %s\n", name); passed++; } \
            else { fprintf(stderr, "  FAIL: %s (%s:%d)\n", name, __FILE__, __LINE__); failed++; } \
} while (0)

#define PTYPE_REQUEST  0x00
#define PTYPE_RESPONSE 0x02
#define PTYPE_FAULT    0x03
#define PTYPE_BIND     0x0b
#define PTYPE_BIND_ACK 0x0c

/* Interface / transfer syntax ids in wire (little-endian GUID) form. */
static const uint8_t                    SRVSVC_UUID[16] = {
    0xc8, 0x4f, 0x32, 0x4b, 0x70, 0x16, 0xd3, 0x01,
    0x12, 0x78, 0x5a, 0x47, 0xbf, 0x6e, 0xe1, 0x88
};
static const uint8_t                    LSARPC_UUID[16] = {
    0x78, 0x57, 0x34, 0x12, 0x34, 0x12, 0xcd, 0xab,
    0xef, 0x00, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab
};
static const uint8_t                    NDR32_UUID[16] = {
    0x04, 0x5d, 0x88, 0x8a, 0xeb, 0x1c, 0xc9, 0x11,
    0x9f, 0xe8, 0x08, 0x00, 0x2b, 0x10, 0x48, 0x60
};
static const uint8_t                    NDR64_UUID[16] = {
    0x33, 0x05, 0x71, 0x71, 0xba, 0xbe, 0x37, 0x49,
    0x83, 0x19, 0xb5, 0xdb, 0xef, 0x9c, 0xcc, 0x36
};
/* Bind-time feature negotiation 6cb71c2c-9812-4540-0300-000000000000. */
static const uint8_t                    BTFN_UUID[16] = {
    0x2c, 0x1c, 0xb7, 0x6c, 0x12, 0x98, 0x40, 0x45,
    0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
};

static struct chimera_server_smb_shared shared;

static void
put16(
    uint8_t *p,
    uint16_t v)
{
    p[0] = v;
    p[1] = v >> 8;
} /* put16 */

static void
put32(
    uint8_t *p,
    uint32_t v)
{
    p[0] = v;
    p[1] = v >> 8;
    p[2] = v >> 16;
    p[3] = v >> 24;
} /* put32 */

static uint16_t
get16(const uint8_t *p)
{
    return p[0] | (p[1] << 8);
} /* get16 */

static uint32_t
get32(const uint8_t *p)
{
    return p[0] | (p[1] << 8) | (p[2] << 16) | ((uint32_t) p[3] << 24);
} /* get32 */

static void
put_header(
    uint8_t *p,
    uint8_t  ptype,
    uint16_t frag_len,
    uint32_t call_id)
{
    memset(p, 0, 16);
    p[0] = 5;           /* version */
    p[1] = 0;
    p[2] = ptype;
    p[3] = 0x03;        /* first | last fragment */
    p[4] = 0x10;        /* little-endian, ASCII, IEEE */
    put16(p + 8, frag_len);
    put32(p + 12, call_id);
} /* put_header */

/* A BIND offering `nctx` contexts, one transfer syntax each: NDR32, then (as
 * Windows sends) NDR64 and bind-time feature negotiation. */
static size_t
build_bind(
    uint8_t       *buf,
    const uint8_t *iface,
    uint16_t       vers,
    uint32_t       assoc_group_id,
    int            nctx)
{
    const uint8_t *ts[3]  = { NDR32_UUID, NDR64_UUID, BTFN_UUID };
    uint32_t       tsv[3] = { 2, 1, 1 };
    uint8_t       *p      = buf + 16;
    int            i;

    put16(p, 4280);
    put16(p + 2, 4280);
    put32(p + 4, assoc_group_id);
    p[8] = nctx;        /* contexts */
    p[9] = p[10] = p[11] = 0;
    p   += 12;

    for (i = 0; i < nctx; i++) {
        put16(p, i);    /* context id */
        p[2] = 1;       /* transfer syntaxes */
        p[3] = 0;
        memcpy(p + 4, iface, 16);
        put16(p + 20, vers);
        put16(p + 22, 0);
        memcpy(p + 24, ts[i], 16);
        put32(p + 40, tsv[i]);
        p += 44;
    }

    put_header(buf, PTYPE_BIND, p - buf, 2);
    return p - buf;
} /* build_bind */

static size_t
build_request(
    uint8_t       *buf,
    uint16_t       opnum,
    const uint8_t *stub,
    size_t         stub_len,
    uint32_t       call_id)
{
    put32(buf + 16, stub_len);   /* alloc_hint */
    put16(buf + 20, 0);          /* context id 0: NDR32 */
    put16(buf + 22, opnum);
    memcpy(buf + 24, stub, stub_len);
    put_header(buf, PTYPE_REQUEST, 24 + stub_len, call_id);
    return 24 + stub_len;
} /* build_request */

static int
pipe_write(
    struct chimera_smb_rpc_pipe *pipe,
    const uint8_t               *buf,
    size_t                       len)
{
    struct evpl_iovec iov;

    memset(&iov, 0, sizeof(iov));
    iov.data   = (void *) buf;
    iov.length = len;
    return chimera_smb_rpc_pipe_write(pipe, &iov, 1);
} /* pipe_write */

/* Bind pipe to iface over NDR32 only, as rpcclient does; returns 0 on a
 * BIND_ACK.  Calls then go on context 0. */
static int
bind_pipe(
    struct chimera_smb_rpc_pipe *pipe,
    const uint8_t               *iface,
    uint16_t                     vers)
{
    uint8_t  in[256], out[1024];
    size_t   n = build_bind(in, iface, vers, 0, 1);
    int      more;
    uint32_t got;

    if (pipe_write(pipe, in, n)) {
        return -1;
    }

    got = chimera_smb_rpc_pipe_read(pipe, out, sizeof(out), &more);
    return got >= 16 && out[2] == PTYPE_BIND_ACK ? 0 : -1;
} /* bind_pipe */

static void
test_bind_ack(void)
{
    struct chimera_smb_rpc_pipe *pipe;
    uint8_t                      in[256], out[1024];
    size_t                       n;
    uint32_t                     got;
    int                          more = 0;
    uint8_t                     *p;
    uint16_t                     sec_len;

    fprintf(stderr, "\nbind with assoc_group_id 0...\n");
    pipe = chimera_smb_rpc_pipe_open("srvsvc", &shared, NULL);
    CHECK(pipe != NULL, "srvsvc pipe opens");
    if (!pipe) {
        return;
    }

    n = build_bind(in, SRVSVC_UUID, 3, 0, 3);
    CHECK(pipe_write(pipe, in, n) == 0, "bind accepted");
    got = chimera_smb_rpc_pipe_read(pipe, out, sizeof(out), &more);

    CHECK(got >= 16 && out[2] == PTYPE_BIND_ACK, "reply is a bind_ack");
    CHECK(get32(out + 12) == 2, "call_id echoed");
    CHECK(get16(out + 8) == got, "frag_len matches the bytes produced");
    CHECK(!more, "bind_ack fits one read");
    CHECK(get32(out + 20) != 0, "bind_ack names a non-zero association group");

    /* Results follow the secondary address, padded to 4. */
    sec_len = get16(out + 24);
    p       = out + 26 + sec_len;
    p       = out + (((p - out) + 3) & ~3);
    CHECK(p[0] == 3, "one result per offered context");
    /* Offered both, the server takes NDR64, as Windows does. */
    CHECK(get16(p + 4) == 2, "NDR32 context declined when NDR64 is offered");
    CHECK(get16(p + 4 + 24) == 0 && memcmp(p + 4 + 24 + 4, NDR64_UUID, 16) == 0,
          "NDR64 context accepted");
    CHECK(get16(p + 4 + 24 * 2) == 3,
          "bind-time feature negotiation answered with negotiate_ack");

    chimera_smb_rpc_pipe_close(pipe);
} /* test_bind_ack */

static void
test_bind_echoes_group(void)
{
    struct chimera_smb_rpc_pipe *pipe;
    uint8_t                      in[256], out[1024];
    size_t                       n;
    int                          more;

    fprintf(stderr, "\nbind with assoc_group_id 0x1234...\n");
    pipe = chimera_smb_rpc_pipe_open("srvsvc", &shared, NULL);
    n    = build_bind(in, SRVSVC_UUID, 3, 0x1234, 3);
    pipe_write(pipe, in, n);
    chimera_smb_rpc_pipe_read(pipe, out, sizeof(out), &more);
    CHECK(get32(out + 20) == 0x1234, "client-supplied association group echoed");
    chimera_smb_rpc_pipe_close(pipe);
} /* test_bind_echoes_group */

static void
test_split_writes(void)
{
    struct chimera_smb_rpc_pipe *pipe;
    uint8_t                      in[256], out[1024];
    size_t                       n, i;
    int                          more, rc = 0;

    fprintf(stderr, "\nbind written one byte at a time...\n");
    pipe = chimera_smb_rpc_pipe_open("srvsvc", &shared, NULL);
    n    = build_bind(in, SRVSVC_UUID, 3, 0, 3);

    for (i = 0; i < n && rc == 0; i++) {
        rc = pipe_write(pipe, in + i, 1);
        if (i + 1 < n && chimera_smb_rpc_pipe_has_output(pipe)) {
            rc = -2;
        }
    }

    CHECK(rc == 0, "partial PDUs are buffered without output");
    CHECK(chimera_smb_rpc_pipe_read(pipe, out, sizeof(out), &more) > 16 &&
          out[2] == PTYPE_BIND_ACK, "reassembled bind answered");
    chimera_smb_rpc_pipe_close(pipe);
} /* test_split_writes */

static void
test_open_and_garbage(void)
{
    struct chimera_smb_rpc_pipe *pipe;
    uint8_t                      junk[64];

    fprintf(stderr, "\npipe names and non-DCE/RPC input...\n");
    CHECK(chimera_smb_rpc_pipe_open("nosuchpipe", &shared, NULL) == NULL,
          "unknown pipe name is refused");

    pipe = chimera_smb_rpc_pipe_open("LSARPC", &shared, NULL);
    CHECK(pipe != NULL, "pipe names match case-insensitively");

    memset(junk, 0x41, sizeof(junk));
    CHECK(pipe_write(pipe, junk, sizeof(junk)) != 0, "garbage is rejected");
    chimera_smb_rpc_pipe_close(pipe);
} /* test_open_and_garbage */

static void
test_server_get_info(void)
{
    struct chimera_smb_rpc_pipe *pipe;
    uint8_t                      in[256], out[4096], part[64];
    uint8_t                      stub[8];
    uint16_t                     wname[16];
    size_t                       n;
    uint32_t                     got, got2;
    int                          more = 0, i, found = 0;

    fprintf(stderr, "\nNetrServerGetInfo level 101...\n");
    pipe = chimera_smb_rpc_pipe_open("srvsvc", &shared, NULL);
    CHECK(bind_pipe(pipe, SRVSVC_UUID, 3) == 0, "bound to srvsvc");

    put32(stub, 0);      /* ServerName: NULL */
    put32(stub + 4, 101);
    n = build_request(in, 21, stub, sizeof(stub), 3);
    CHECK(pipe_write(pipe, in, n) == 0, "request accepted");

    /* A short read returns part of the fragment and flags the rest. */
    got = chimera_smb_rpc_pipe_read(pipe, part, sizeof(part), &more);
    CHECK(got == sizeof(part) && more, "short read reports more data");
    memcpy(out, part, got);
    got2 = chimera_smb_rpc_pipe_read(pipe, out + got, sizeof(out) - got, &more);
    CHECK(got2 > 0 && !more, "next read returns the rest");

    CHECK(out[2] == PTYPE_RESPONSE, "reply is a response");
    CHECK(get16(out + 8) == got + got2, "whole fragment read");

    /* The server name travels as UTF-16. */
    for (i = 0; shared.config.identity[i]; i++) {
        wname[i] = (uint8_t) shared.config.identity[i];
    }
    for (i = 0; i + 14 <= (int) (got + got2); i++) {
        if (memcmp(out + i, wname, 14) == 0) {
            found = 1;
        }
    }
    CHECK(found, "server name in the reply");
    chimera_smb_rpc_pipe_close(pipe);
} /* test_server_get_info */

static void
test_lsa_faults(void)
{
    struct chimera_smb_rpc_pipe *pipe;
    uint8_t                      in[256], out[1024], stub[64];
    size_t                       n;
    int                          more;

    fprintf(stderr, "\nlsarpc faults...\n");
    pipe = chimera_smb_rpc_pipe_open("lsarpc", &shared, NULL);
    CHECK(bind_pipe(pipe, LSARPC_UUID, 0) == 0, "bound to lsarpc");

    /* LsarClose of a handle never opened. */
    memset(stub, 0, sizeof(stub));
    stub[4] = 0x42;
    n       = build_request(in, 0, stub, 20, 4);
    pipe_write(pipe, in, n);
    chimera_smb_rpc_pipe_read(pipe, out, sizeof(out), &more);
    CHECK(out[2] == PTYPE_FAULT && get32(out + 24) == 0x1c00001a,
          "foreign handle faults with context mismatch");

    /* LsarLookupNames claiming a huge name count in a tiny stub: the decoder
     * must refuse it rather than allocate or read past the stub. */
    memset(stub, 0, sizeof(stub));
    put32(stub + 20, 0x7fffffff);   /* Count */
    put32(stub + 24, 0x7fffffff);   /* max_count */
    n = build_request(in, 14, stub, 40, 5);
    pipe_write(pipe, in, n);
    chimera_smb_rpc_pipe_read(pipe, out, sizeof(out), &more);
    CHECK(out[2] == PTYPE_FAULT, "oversized name count faults");

    /* Unknown opnum. */
    n = build_request(in, 200, stub, 4, 6);
    pipe_write(pipe, in, n);
    chimera_smb_rpc_pipe_read(pipe, out, sizeof(out), &more);
    CHECK(out[2] == PTYPE_FAULT && get32(out + 24) == 0x1c010002,
          "unknown opnum faults with op_rng_error");

    chimera_smb_rpc_pipe_close(pipe);
} /* test_lsa_faults */

int
main(void)
{
    memset(&shared, 0, sizeof(shared));
    snprintf(shared.config.identity, sizeof(shared.config.identity), "TESTSRV");
    evpl_mutex_init(&shared.shares_lock, NULL);

    if (chimera_smb_rpc_init() != 0) {
        fprintf(stderr, "chimera_smb_rpc_init failed\n");
        return 1;
    }

    test_bind_ack();
    test_bind_echoes_group();
    test_split_writes();
    test_open_and_garbage();
    test_server_get_info();
    test_lsa_faults();

    fprintf(stderr, "\n%d passed, %d failed\n", passed, failed);
    return failed ? 1 : 0;
} /* main */
