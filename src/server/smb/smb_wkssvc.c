// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_wkssvc.h"
#include "smb_dcerpc.h"
#include "wkssvc_ndr.h"

/* WKSSVC interface 6bffd098-a112-3610-9833-46c3f87e345a v1.0 (GUID wire form:
 * first three fields little-endian, last two big-endian). */
static const dce_if_uuid_t WKS_INTERFACE = {
    .if_uuid       = { 0x98, 0xd0, 0xff, 0x6b, 0x12, 0xa1, 0x10, 0x36,
                       0x98, 0x33, 0x46, 0xc3, 0xf8, 0x7e, 0x34, 0x5a },
    .if_vers_major = 1,
    .if_vers_minor = 0,
};

#define WKS_OP_NETWKSTAGETINFO 0

#define WKS_PLATFORM_ID_NT     500
#define WKS_WERR_UNKNOWN_LEVEL 0x0000007C

#define WKS_RPC_STUB_MAX       (65535 - (int) (sizeof(dce_common_t) + sizeof(dce_co_response_t)))

static void
chimera_smb_wkssvc_impl(
    int                               opnum,
    const void                       *in,
    void                             *out,
    struct ndr_dbuf                  *dbuf,
    struct chimera_server_smb_shared *shared)
{
    (void) in;

    switch (opnum) {
        case WKS_OP_NETWKSTAGETINFO: {
            const struct wkssvc_NetWkstaGetInfo_in *in_w = in;
            struct wkssvc_NetWkstaGetInfo_out      *o    = out;
            char                                   *name = (char *) shared->config.identity;

            o->info.switch_value = in_w->level;
            memset(&o->info.u, 0, sizeof(o->info.u));
            o->status = 0;

            switch (in_w->level) {
                case 100: {
                    struct wkssvc_NetWkstaInfo100 *i = ndr_dbuf_alloc(dbuf, sizeof(*i));
                    i->platform_id    = WKS_PLATFORM_ID_NT;
                    i->server_name    = name;
                    i->domain_name    = "WORKGROUP";
                    i->version_major  = 6;
                    i->version_minor  = 1;
                    o->info.u.info100 = i;
                    break;
                }
                case 101: {
                    struct wkssvc_NetWkstaInfo101 *i = ndr_dbuf_alloc(dbuf, sizeof(*i));
                    i->platform_id    = WKS_PLATFORM_ID_NT;
                    i->server_name    = name;
                    i->domain_name    = "WORKGROUP";
                    i->version_major  = 6;
                    i->version_minor  = 1;
                    i->lan_root       = "";
                    o->info.u.info101 = i;
                    break;
                }
                case 102: {
                    struct wkssvc_NetWkstaInfo102 *i = ndr_dbuf_alloc(dbuf, sizeof(*i));
                    i->platform_id     = WKS_PLATFORM_ID_NT;
                    i->server_name     = name;
                    i->domain_name     = "WORKGROUP";
                    i->version_major   = 6;
                    i->version_minor   = 1;
                    i->lan_root        = "";
                    i->logged_on_users = 1;
                    o->info.u.info102  = i;
                    break;
                }
                case 502: {
                    /* Tuning parameters; reported as zeros (the arena zeroes the
                     * allocation), which clients accept. */
                    o->info.u.info502 = ndr_dbuf_alloc(dbuf, sizeof(struct wkssvc_NetWkstaInfo502));
                    break;
                }
                default:
                    o->status = WKS_WERR_UNKNOWN_LEVEL;
                    break;
            } /* switch level */
            break;
        }
        default:
            break;
    } /* switch */
} /* chimera_smb_wkssvc_impl */

static int
chimera_smb_wkssvc_handler(
    int                       opnum,
    struct evpl_iovec_cursor *cursor,
    void                     *output,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;
    const struct ndr_op_desc   *op;
    struct ndr_cursor           c;
    struct ndr_writer           w;
    struct ndr_dbuf             dbuf;
    void                       *in, *out;
    int                         n;

    op = ndr_find_op(wkssvc_op_table, wkssvc_op_count, opnum);
    if (!op) {
        return -1;
    }

    ndr_cursor_init(&c, evpl_iovec_cursor_data(cursor),
                    cursor->iov->length - cursor->offset);
    ndr_dbuf_init(&dbuf, 4096);

    in  = ndr_dbuf_alloc(&dbuf, op->in_size);
    out = ndr_dbuf_alloc(&dbuf, op->out_size);

    op->pull_in(&c, in, &dbuf);
    chimera_smb_wkssvc_impl(op->opnum, in, out, &dbuf,
                            request->compound->thread->shared);

    ndr_writer_init(&w, output, WKS_RPC_STUB_MAX);
    n = op->push_out(&w, out);

    ndr_dbuf_destroy(&dbuf);
    return n;
} /* chimera_smb_wkssvc_handler */

int
chimera_smb_wkssvc_transceive(
    struct chimera_smb_request *request,
    struct evpl_iovec          *input_iov,
    int                         input_niov,
    struct evpl_iovec          *output_iov)
{
    int status;

    status = dce_rpc(&WKS_INTERFACE, input_iov, input_niov, output_iov,
                     chimera_smb_wkssvc_handler, request);

    if (status != 0) {
        chimera_smb_error("WKSSVC RPC transceive failed");
        return status;
    }

    return 0;
} /* chimera_smb_wkssvc_transceive */
