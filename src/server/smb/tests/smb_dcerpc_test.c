// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * DCE/RPC bind handling (smb_dcerpc.h): the bind_ack a Windows RPC client
 * requires.  Drives dce_rpc() directly with hand-built PDUs on stack
 * buffers; no server, no evpl instance.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "common/test_host.h"
#include "common/evpl_iovec_cursor.h"
#include "server/smb/smb_dcerpc.h"

static int                 passed = 0;
static int                 failed = 0;

#define CHECK(cond, name) do { \
            if (cond) { fprintf(stderr, "  PASS: %s\n", name); passed++; } \
            else { fprintf(stderr, "  FAIL: %s (%s:%d)\n", name, __FILE__, __LINE__); failed++; } \
} while (0)

/* srvsvc 4b324fc8-1670-01d3-1278-5a47bf6ee188 v3.0, as smb_srvsvc.c declares it. */
static const dce_if_uuid_t SRVSVC_IF = {
    .if_uuid       = { 0xc8, 0x4f, 0x32, 0x4b, 0x70, 0x16, 0xd3, 0x01,
                       0x12, 0x78, 0x5a, 0x47, 0xbf, 0x6e, 0xe1, 0x88 },
    .if_vers_major = 3,
    .if_vers_minor = 0,
};

static const p_syntax_id_t NDR64_SYNTAX = {
    .ts_uuid    = { 0x33, 0x05, 0x71, 0x71, 0xba, 0xbe, 0x37, 0x49,
                    0x83, 0x19, 0xb5, 0xdb, 0xef, 0x9c, 0xcc, 0x36 },
    .ts_version = 1,
};

/* Bind-time feature negotiation pseudo-syntax 6cb71c2c-9812-4540-0300-000000000000
 * v1 with the two feature bits Windows sets. */
static const p_syntax_id_t BIND_TIME_SYNTAX = {
    .ts_uuid    = { 0x2c, 0x1c, 0xb7, 0x6c, 0x12, 0x98, 0x40, 0x45,
                    0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
    .ts_version = 1,
};

static int
never_called(
    int                       opnum,
    struct evpl_iovec_cursor *cursor,
    void                     *output,
    void                     *private_data)
{
    (void) opnum;
    (void) cursor;
    (void) output;
    (void) private_data;
    return -1;
} /* never_called */

/* Build the bind Windows sends for srvsvc: three contexts, one transfer
 * syntax each (NDR32, NDR64, bind-time features). */
static size_t
build_windows_bind(
    uint8_t *buf,
    uint32_t assoc_group_id)
{
    dce_common_t        *hdr   = (dce_common_t *) buf;
    dce_bind_t          *bind  = (dce_bind_t *) (buf + sizeof(*hdr));
    uint8_t             *p     = buf + sizeof(*hdr) + sizeof(*bind);
    const p_syntax_id_t *ts[3] = { &NDR32_SYNTAX, &NDR64_SYNTAX, &BIND_TIME_SYNTAX };
    int                  i;

    memset(buf, 0, 256);
    hdr->ver       = 5;
    hdr->ver_minor = 0;
    hdr->ptype     = DCE_RPC_PTYPE_BIND;
    hdr->flags     = DCE_RPC_FLAG_FIRST_FRAG | DCE_RPC_FLAG_LAST_FRAG;
    hdr->drep[0]   = 0x10;
    hdr->call_id   = 2;

    bind->max_xmit_frag  = 4280;
    bind->max_recv_frag  = 4280;
    bind->assoc_group_id = assoc_group_id;
    bind->num_ctx_items  = 3;

    for (i = 0; i < 3; i++) {
        p_cont_elem_t *ce = (p_cont_elem_t *) p;
        memset(ce, 0, sizeof(*ce));
        ce->p_cont_id      = (uint16_t) i;
        ce->n_transfer_syn = 1;
        memcpy(ce->if_uuid, SRVSVC_IF.if_uuid, 16);
        ce->if_vers_major = 3;
        ce->if_vers_minor = 0;
        p                += sizeof(*ce);
        memcpy(p, ts[i], sizeof(p_syntax_id_t));
        p += sizeof(p_syntax_id_t);
    }

    hdr->frag_len = (uint16_t) (p - buf);
    return (size_t) (p - buf);
} /* build_windows_bind */

static int
run_bind(
    uint32_t assoc_group_id,
    uint8_t *out,
    size_t  *out_len)
{
    uint8_t           in[256];
    size_t            in_len  = build_windows_bind(in, assoc_group_id);
    struct evpl_iovec in_iov  = { .data = in, .length = (unsigned int) in_len };
    struct evpl_iovec out_iov = { .data = out, .length = 65535 };
    int               rc;

    rc       = dce_rpc(&SRVSVC_IF, &in_iov, 1, &out_iov, never_called, NULL);
    *out_len = out_iov.length;
    return rc;
} /* run_bind */

static void
test_bind_ack_names_a_group(void)
{
    uint8_t          out[65535];
    size_t           out_len = 0;
    dce_common_t    *hdr;
    dce_bind_ack_t  *ack;
    p_result_list_t *list;
    p_result_t      *res;

    fprintf(stderr, "\nbind with assoc_group_id 0...\n");
    CHECK(run_bind(0, out, &out_len) == 0, "dce_rpc accepts the bind");

    hdr  = (dce_common_t *) out;
    ack  = (dce_bind_ack_t *) (out + sizeof(*hdr));
    list = (p_result_list_t *) (out + sizeof(*hdr) + sizeof(*ack));
    res  = (p_result_t *) (list + 1);

    CHECK(hdr->ptype == DCE_RPC_PTYPE_BIND_ACK, "reply is a bind_ack");
    CHECK(hdr->call_id == 2, "call_id echoed");
    CHECK(hdr->frag_len == out_len, "frag_len matches the bytes produced");
    CHECK(ack->assoc_group_id != 0, "bind_ack names a non-zero association group");
    CHECK(list->num_results == 3, "one result per offered context");
    CHECK(res[0].result == 0 &&
          memcmp(res[0].transfer_syntax_accepted.ts_uuid, NDR32_SYNTAX.ts_uuid, 16) == 0,
          "NDR32 context accepted");
    CHECK(res[1].result == 2, "NDR64 context rejected");
    CHECK(res[2].result != 0, "feature negotiation context not accepted as a transfer syntax");
} /* test_bind_ack_names_a_group */

static void
test_bind_ack_echoes_client_group(void)
{
    uint8_t         out[65535];
    size_t          out_len = 0;
    dce_bind_ack_t *ack;

    fprintf(stderr, "\nbind with assoc_group_id 0x1234...\n");
    CHECK(run_bind(0x1234, out, &out_len) == 0, "dce_rpc accepts the bind");
    ack = (dce_bind_ack_t *) (out + sizeof(dce_common_t));
    CHECK(ack->assoc_group_id == 0x1234, "client-supplied association group echoed");
} /* test_bind_ack_echoes_client_group */

int
main(void)
{
    test_bind_ack_names_a_group();
    test_bind_ack_echoes_client_group();
    fprintf(stderr, "\n%d passed, %d failed\n", passed, failed);
    return failed ? 1 : 0;
} /* main */
