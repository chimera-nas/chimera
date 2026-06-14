// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_samr.h"
#include "smb_dcerpc.h"
#include "samr_ndr.h"

/* SAMR interface 12345778-1234-abcd-ef00-0123456789ac v1.0 (note the trailing
 * ...ac, vs LSARPC's ...ab). */
static const dce_if_uuid_t SAMR_INTERFACE = {
    .if_uuid       = { 0x78, 0x57, 0x34, 0x12, 0x34, 0x12, 0xCD, 0xAB,
                       0xEF, 0x00, 0x01, 0x23, 0x45, 0x67, 0x89, 0xac },
    .if_vers_major = 1,
    .if_vers_minor = 0,
};

#define SAMR_OP_CONNECT            0
#define SAMR_OP_CLOSE              1
#define SAMR_OP_LOOKUPDOMAIN       5
#define SAMR_OP_ENUMDOMAINS        6
#define SAMR_OP_CONNECT2           21
#define SAMR_OP_CONNECT5           64

#define SAMR_STATUS_NO_SUCH_DOMAIN 0xC00000DF

#define SAMR_RPC_STUB_MAX          (65535 - (int) (sizeof(dce_common_t) + sizeof(dce_co_response_t)))

/* ---- per-connection context (policy) handles ----
 * The same fixed per-connection set the LSARPC service uses (conn->rpc_handles):
 * a 16-byte all-zero slot is free; a handle not in the set faults
 * CONTEXT_MISMATCH. */
static int
chimera_smb_samr_handle_set(const uint8_t uuid[16])
{
    for (int i = 0; i < 16; i++) {
        if (uuid[i]) {
            return 1;
        }
    }
    return 0;
} /* chimera_smb_samr_handle_set */

static int
chimera_smb_samr_handle_open(
    struct chimera_smb_conn *conn,
    struct samr_handle      *h)
{
    for (int i = 0; i < CHIMERA_SMB_RPC_MAX_HANDLES; i++) {
        uint64_t a, b;

        if (chimera_smb_samr_handle_set(conn->rpc_handles[i])) {
            continue;
        }
        a = chimera_rand64();
        b = chimera_rand64();
        memcpy(conn->rpc_handles[i], &a, 8);
        memcpy(conn->rpc_handles[i] + 8, &b, 8);
        conn->rpc_handles[i][0] |= 1;

        h->handle_type = 0;
        memcpy(h->uuid, conn->rpc_handles[i], 16);
        return 0;
    }
    return -1;
} /* chimera_smb_samr_handle_open */

static int
chimera_smb_samr_handle_slot(
    struct chimera_smb_conn  *conn,
    const struct samr_handle *h)
{
    if (!chimera_smb_samr_handle_set(h->uuid)) {
        return -1;
    }
    for (int i = 0; i < CHIMERA_SMB_RPC_MAX_HANDLES; i++) {
        if (memcmp(conn->rpc_handles[i], h->uuid, 16) == 0) {
            return i;
        }
    }
    return -1;
} /* chimera_smb_samr_handle_slot */

static int
chimera_smb_samr_handle_valid(
    struct chimera_smb_conn  *conn,
    const struct samr_handle *h)
{
    return chimera_smb_samr_handle_slot(conn, h) >= 0;
} /* chimera_smb_samr_handle_valid */

static int
chimera_smb_samr_handle_close(
    struct chimera_smb_conn  *conn,
    const struct samr_handle *h)
{
    int slot = chimera_smb_samr_handle_slot(conn, h);

    if (slot < 0) {
        return 0;
    }
    memset(conn->rpc_handles[slot], 0, 16);
    return 1;
} /* chimera_smb_samr_handle_close */

/* ---- helpers ---- */
static void
chimera_smb_samr_set_ustr(
    struct samr_String *s,
    const char         *utf8)
{
    s->length = (uint16_t) (strlen(utf8) * 2);
    s->size   = (uint16_t) (strlen(utf8) * 2);
    s->buffer = (char *) utf8;
} /* chimera_smb_samr_set_ustr */

/* BUILTIN domain SID S-1-5-32. */
static void
chimera_smb_samr_builtin_sid(struct ndr_sid *sid)
{
    memset(sid, 0, sizeof(*sid));
    sid->revision                = 1;
    sid->identifier_authority[5] = 5;
    sid->sub_authority_count     = 1;
    sid->sub_authority[0]        = 32;
} /* chimera_smb_samr_builtin_sid */

/* The server's account-domain (machine) SID S-1-5-21-X-Y-Z. */
static void
chimera_smb_samr_account_sid(
    struct ndr_sid                         *sid,
    const struct chimera_server_smb_shared *shared)
{
    memset(sid, 0, sizeof(*sid));
    sid->revision                = 1;
    sid->identifier_authority[5] = 5;
    sid->sub_authority_count     = 4;
    sid->sub_authority[0]        = 21;
    sid->sub_authority[1]        = shared->machine_domain_sub[0];
    sid->sub_authority[2]        = shared->machine_domain_sub[1];
    sid->sub_authority[3]        = shared->machine_domain_sub[2];
} /* chimera_smb_samr_account_sid */

/*
 * Business logic for the generated SAMR ops.  Read-only foundation: connect,
 * enumerate the server's two domains (the account domain named after the
 * server, and BUILTIN), and resolve a domain name to its SID.  Returns 0, or
 * DCE_RC_FAULT_CONTEXT_MISMATCH when handed a handle this connection did not
 * issue.
 */
static int
chimera_smb_samr_impl(
    int                                     opnum,
    const void                             *in,
    void                                   *out,
    struct ndr_dbuf                        *dbuf,
    struct chimera_smb_conn                *conn,
    const struct chimera_server_smb_shared *shared)
{
    const char *account_domain = shared->config.identity;

    (void) in;

    switch (opnum) {
        case SAMR_OP_CONNECT: {
            struct samr_Connect_out *o = out;
            if (chimera_smb_samr_handle_open(conn, &o->connect_handle) != 0) {
                memset(&o->connect_handle, 0, sizeof(o->connect_handle));
                o->status = SAMR_STATUS_NO_SUCH_DOMAIN;
            } else {
                o->status = 0;
            }
            break;
        }
        case SAMR_OP_CONNECT2: {
            struct samr_Connect2_out *o = out;
            if (chimera_smb_samr_handle_open(conn, &o->connect_handle) != 0) {
                memset(&o->connect_handle, 0, sizeof(o->connect_handle));
                o->status = SAMR_STATUS_NO_SUCH_DOMAIN;
            } else {
                o->status = 0;
            }
            break;
        }
        case SAMR_OP_CONNECT5: {
            struct samr_Connect5_out *o = out;
            if (chimera_smb_samr_handle_open(conn, &o->connect_handle) != 0) {
                memset(&o->connect_handle, 0, sizeof(o->connect_handle));
                o->status = SAMR_STATUS_NO_SUCH_DOMAIN;
                break;
            }
            /* Echo a level-1 revision info (the version the client negotiated). */
            o->level_out                         = 1;
            o->info_out.level                    = 1;
            o->info_out.info1.client_version     = 1;
            o->info_out.info1.supported_features = 0;
            o->status                            = 0;
            break;
        }
        case SAMR_OP_CLOSE: {
            const struct samr_Close_in *li = in;
            struct samr_Close_out      *o  = out;
            if (!chimera_smb_samr_handle_close(conn, &li->handle)) {
                return DCE_RC_FAULT_CONTEXT_MISMATCH;
            }
            o->handle.handle_type = 0;
            memset(o->handle.uuid, 0, sizeof(o->handle.uuid));
            o->status = 0;
            break;
        }
        case SAMR_OP_ENUMDOMAINS: {
            const struct samr_EnumDomains_in *li  = in;
            struct samr_EnumDomains_out      *o   = out;
            struct samr_SamArray             *sam = ndr_dbuf_alloc(dbuf, sizeof(*sam));
            struct samr_SamEntry             *e   = ndr_dbuf_alloc(dbuf, 2 * sizeof(*e));

            if (!chimera_smb_samr_handle_valid(conn, &li->connect_handle)) {
                return DCE_RC_FAULT_CONTEXT_MISMATCH;
            }

            e[0].idx = 0;
            chimera_smb_samr_set_ustr(&e[0].name, account_domain);
            e[1].idx = 0;
            chimera_smb_samr_set_ustr(&e[1].name, "Builtin");

            sam->count       = 2;
            sam->entries     = e;
            o->sam           = sam;
            o->num_entries   = 2;
            o->resume_handle = 0;
            o->status        = 0;
            break;
        }
        case SAMR_OP_LOOKUPDOMAIN: {
            const struct samr_LookupDomain_in *li  = in;
            struct samr_LookupDomain_out      *o   = out;
            const char                        *dom = li->domain_name.buffer;
            struct ndr_sid                    *sid;

            if (!chimera_smb_samr_handle_valid(conn, &li->connect_handle)) {
                return DCE_RC_FAULT_CONTEXT_MISMATCH;
            }

            o->sid.sid = NULL;

            if (!dom) {
                o->status = SAMR_STATUS_NO_SUCH_DOMAIN;
                break;
            }

            sid = ndr_dbuf_alloc(dbuf, sizeof(*sid));
            if (strcasecmp(dom, "Builtin") == 0) {
                chimera_smb_samr_builtin_sid(sid);
                o->sid.sid = sid;
                o->status  = 0;
            } else if (strcasecmp(dom, account_domain) == 0) {
                chimera_smb_samr_account_sid(sid, shared);
                o->sid.sid = sid;
                o->status  = 0;
            } else {
                o->status = SAMR_STATUS_NO_SUCH_DOMAIN;
            }
            break;
        }
        default:
            break;
    } /* switch */

    return 0;
} /* chimera_smb_samr_impl */

static int
chimera_smb_samr_handler(
    int                       opnum,
    struct evpl_iovec_cursor *cursor,
    void                     *output,
    void                     *private_data)
{
    struct chimera_smb_request       *request = private_data;
    struct chimera_server_smb_shared *shared  = request->compound->thread->shared;
    const struct ndr_op_desc         *op;
    struct ndr_cursor                 c;
    struct ndr_writer                 w;
    struct ndr_dbuf                   dbuf;
    void                             *in, *out;
    int                               n;

    op = ndr_find_op(samr_op_table, samr_op_count, opnum);
    if (!op) {
        return -1;
    }

    ndr_cursor_init(&c, evpl_iovec_cursor_data(cursor),
                    cursor->iov->length - cursor->offset);
    ndr_dbuf_init(&dbuf, 4096);

    in  = ndr_dbuf_alloc(&dbuf, op->in_size);
    out = ndr_dbuf_alloc(&dbuf, op->out_size);

    op->pull_in(&c, in, &dbuf);
    n = chimera_smb_samr_impl(op->opnum, in, out, &dbuf,
                              request->compound->conn, shared);

    if (n < 0) {
        ndr_dbuf_destroy(&dbuf);
        return n;
    }

    ndr_writer_init(&w, output, SAMR_RPC_STUB_MAX);
    n = op->push_out(&w, out);

    ndr_dbuf_destroy(&dbuf);
    return n;
} /* chimera_smb_samr_handler */

int
chimera_smb_samr_transceive(
    struct chimera_smb_request *request,
    struct evpl_iovec          *input_iov,
    int                         input_niov,
    struct evpl_iovec          *output_iov)
{
    int status;

    status = dce_rpc(&SAMR_INTERFACE, input_iov, input_niov, output_iov,
                     chimera_smb_samr_handler, request);

    if (status != 0) {
        chimera_smb_error("SAMR RPC transceive failed");
        return status;
    }

    return 0;
} /* chimera_smb_samr_transceive */
