// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * LSARPC ground-truth probe: MS-LSAT spoken to the in-process chimera SMB
 * server over its "lsarpc" named pipe, through both pipe transports --
 * FSCTL_PIPE_TRANSCEIVE, and an SMB2 WRITE of the request PDU followed by an
 * SMB2 READ of the reply -- by the hand-built client in smb2_lsarpc_wire.h.
 *
 *   S0  the client against the server's generated NDR code, before any server
 *       runs: each request stub the client builds decodes through the
 *       generated pull_in to the values it was built from, using every byte,
 *       and each reply the generated push_out encodes parses back to the
 *       values it was encoded from
 *   S1  LookupSids of a Unix user SID names the server's local account, over
 *       both transports
 *   S2  LookupNames3 resolves the well-known name Everyone
 *   M1-M13  every lookup goes through the naming rules: well-known, Unix and
 *       account-domain SIDs and names each come back with the domain and type
 *       they belong to, over both transports and on a null session; a name
 *       that is not ASCII, surrogate pairs included, keeps its UTF-16 length;
 *       the empty name names the machine domain; and an account whose name
 *       is not UTF-8 leaves only its own entry unmapped
 *
 * Built as smb2_lsarpc_probe_wbstub (SMB2W_WBSTUB), --winbind runs S0 and
 * opens the pipe as a domain user the server validates through
 * wbclient_stub.c, then:
 *
 *   W1-W16  a SID or name no cache knows is resolved through winbind before
 *       the reply: on the first request, over both transports, behind an
 *       interim when winbind is slow, through a CANCEL and past the end of
 *       its connection; never for a null session; at most 64 per request;
 *       at most 16 lookups parked on one connection; a name lookup leaves
 *       the user cache alone; and an account winbind just said it does not
 *       know is answered again at once (W4b)
 */

#include <stdarg.h>

#include "smb2_lsarpc_wire.h"
#include "lsarpc_ndr.h"
#ifdef SMB2W_WBSTUB
#include "wbclient_stub.h"
#endif /* ifdef SMB2W_WBSTUB */

static int failures = 0;

#define CHECK(cond, ...)                             \
        do {                                         \
            if (cond) {                              \
                printf("ok   - " __VA_ARGS__);       \
                printf("\n");                        \
            } else {                                 \
                printf("FAIL - " __VA_ARGS__);       \
                printf("\n");                        \
                failures++;                          \
            }                                        \
        } while (0)

struct lsa_session {
    struct smb2_conn *c;
    uint8_t           fid[16];     /* the open "lsarpc" pipe     */
    uint8_t           handle[20];  /* the OpenPolicy2 handle     */
    uint32_t          call_id;
};

/* An exchange that broke below the LSA layer -- an SMB error, a fault, a
 * reply that does not parse -- leaves nothing to assert on, so it stops the
 * probe. */
static void
lsa_die(
    const char *fmt,
    ...) CHIMERA_PRINTF(1, 2);

CHIMERA_NORETURN
static void
lsa_die(
    const char *fmt,
    ...)
{
    va_list ap;

    printf("FAIL - ");
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
    smb2c_exit(1);
} /* lsa_die */

/* ---- transports --------------------------------------------------------- */

/* Post an FSCTL_PIPE_TRANSCEIVE carrying `pdu`, without waiting. */
static void
lsa_post_pdu(
    struct lsa_session *s,
    const uint8_t      *pdu,
    int                 len)
{
    struct smb2_conn *c      = s->c;
    int               b      = smb2c_begin(c, SMB2_IOCTL, 0);
    uint8_t          *body   = c->sbuf + b;
    int               in_off = SMB2_HDR_SIZE + 56;

    p16(body, 0, 57);                         /* StructureSize */
    p32(body, 4, LSAW_FSCTL_PIPE_TRANSCEIVE); /* CtlCode */
    memcpy(body + 8, s->fid, 16);             /* FileId */
    p32(body, 24, (uint32_t) in_off);         /* InputOffset */
    p32(body, 28, (uint32_t) len);            /* InputCount */
    p32(body, 32, 0);                         /* MaxInputResponse */
    p32(body, 36, (uint32_t) in_off);         /* OutputOffset */
    p32(body, 40, 0);                         /* OutputCount */
    p32(body, 44, LSAW_PDU_MAX);              /* MaxOutputResponse: any one PDU */
    p32(body, 48, SMB2C_IOCTL_IS_FSCTL);
    memcpy(body + 56, pdu, (size_t) len);
    smb2c_send(c, 56 + len);
} /* lsa_post_pdu */

/* The output of the IOCTL reply in c->rbuf.  Returns its SMB status; *out is
 * NULL and *out_len -1 unless that is SUCCESS with a well-formed body. */
static uint32_t
lsa_fsctl_output(
    struct smb2_conn *c,
    const uint8_t   **out,
    int              *out_len)
{
    const uint8_t *rb = c->rbuf + 4 + SMB2_HDR_SIZE;
    uint32_t       st = g32(c->rbuf + 4, 8), off, len;

    *out     = NULL;
    *out_len = -1;
    if (st != ST_SUCCESS) {
        return st;
    }
    if (c->rlen < 4 + SMB2_HDR_SIZE + 48 || g16(rb, 0) != 49) {
        printf("# lsa: the IOCTL reply has no IOCTL body (%d bytes)\n", c->rlen);
        return st;
    }
    off = g32(rb, 32);    /* OutputOffset, from the SMB2 header */
    len = g32(rb, 36);    /* OutputCount */
    if ((int64_t) 4 + off + len > c->rlen) {
        printf("# lsa: the IOCTL output runs past the reply\n");
        return st;
    }
    *out     = c->rbuf + 4 + off;
    *out_len = (int) len;
    return st;
} /* lsa_fsctl_output */

/* A reply PDU -> its stub, checked to answer this session's latest call.
 * *rstub_len is -1 (and a line says why) for a fault, a malformed PDU, or the
 * answer to some other call. */
static void
lsa_unframe(
    struct lsa_session *s,
    const uint8_t      *pdu,
    int                 len,
    const uint8_t     **rstub,
    int                *rstub_len)
{
    uint32_t fault = 0;
    int      n     = lsaw_response_stub(pdu, len, rstub, &fault);

    *rstub_len = -1;
    if (n < 0) {
        if (fault) {
            printf("# lsa: call %u faulted 0x%08x\n", s->call_id, fault);
        } else {
            printf("# lsa: call %u: malformed reply PDU (%d bytes)\n",
                   s->call_id, len);
        }
        *rstub = NULL;
        return;
    }
    if (g32(pdu, 12) != s->call_id) {
        printf("# lsa: call %u was answered as call %u\n", s->call_id,
               g32(pdu, 12));
        *rstub = NULL;
        return;
    }
    *rstub_len = n;
} /* lsa_unframe */

/* The reply stub of the FSCTL_PIPE_TRANSCEIVE whose reply is in c->rbuf, for
 * a caller that posted it and waited itself.  Returns the SMB status. */
static uint32_t
lsa_fsctl_collect(
    struct lsa_session *s,
    const uint8_t     **rstub,
    int                *rstub_len)
{
    const uint8_t *pdu;
    int            len;
    uint32_t       st = lsa_fsctl_output(s->c, &pdu, &len);

    *rstub     = NULL;
    *rstub_len = -1;
    if (pdu) {
        lsa_unframe(s, pdu, len, rstub, rstub_len);
    }
    return st;
} /* lsa_fsctl_collect */

/* Post the FSCTL without waiting (for the cancel and disconnect cases). */
static void
lsa_post_fsctl(
    struct lsa_session *s,
    uint16_t            opnum,
    const uint8_t      *stub,
    int                 stub_len)
{
    static uint8_t pdu[LSAW_PDU_MAX];
    int            len = lsaw_request_pdu(pdu, ++s->call_id, opnum, stub, stub_len);

    if (len < 0) {
        lsa_die("lsa: opnum %u: a %d-byte stub does not fit one PDU", opnum,
                stub_len);
    }
    lsa_post_pdu(s, pdu, len);
} /* lsa_post_fsctl */

/* One call over FSCTL_PIPE_TRANSCEIVE (blocking; interims are absorbed by the
 * harness).  Returns the SMB status; *rstub and *rstub_len are the reply stub,
 * pointing into the connection's reply buffer (valid until its next request),
 * and *rstub_len is -1 when no response PDU for this call came back. */
static uint32_t
lsa_call_fsctl(
    struct lsa_session *s,
    uint16_t            opnum,
    const uint8_t      *stub,
    int                 stub_len,
    const uint8_t     **rstub,
    int                *rstub_len)
{
    lsa_post_fsctl(s, opnum, stub, stub_len);
    smb2c_wait(s->c);
    return lsa_fsctl_collect(s, rstub, rstub_len);
} /* lsa_call_fsctl */

/* The same call as an SMB2 WRITE of the PDU followed by an SMB2 READ. */
static uint32_t
lsa_call_write_read(
    struct lsa_session *s,
    uint16_t            opnum,
    const uint8_t      *stub,
    int                 stub_len,
    const uint8_t     **rstub,
    int                *rstub_len)
{
    static uint8_t    pdu[LSAW_PDU_MAX];
    struct smb2_conn *c = s->c;
    const uint8_t    *rb;
    uint32_t          st, count = 0, n = 0;
    int               len;

    *rstub     = NULL;
    *rstub_len = -1;

    len = lsaw_request_pdu(pdu, ++s->call_id, opnum, stub, stub_len);
    if (len < 0) {
        lsa_die("lsa: opnum %u: a %d-byte stub does not fit one PDU", opnum,
                stub_len);
    }

    st = smb2_write(c, s->fid, 0, pdu, (uint32_t) len, &count);
    if (st != ST_SUCCESS) {
        return st;
    }
    if (count != (uint32_t) len) {
        printf("# lsa: the pipe WRITE took %u of %d bytes\n", count, len);
        return st;
    }

    st = smb2_read(c, s->fid, 0, LSAW_PDU_MAX, NULL, &n);
    if (st != ST_SUCCESS) {
        return st;
    }
    rb = c->rbuf + 4 + SMB2_HDR_SIZE;
    if (c->rlen < 4 + SMB2_HDR_SIZE + 16 || g16(rb, 0) != 17 ||
        4 + rb[2] + (int64_t) n > c->rlen) {
        printf("# lsa: the pipe READ reply is malformed (%d bytes)\n", c->rlen);
        return st;
    }
    lsa_unframe(s, c->rbuf + 4 + rb[2], (int) n, rstub, rstub_len);
    return st;
} /* lsa_call_write_read */

/* ---- the LSA session ---------------------------------------------------- */

/* TREE_CONNECT \\server\IPC$, CREATE "lsarpc", bind, OpenPolicy2; dies on failure. */
static void
lsa_open(
    struct smb2_conn   *c,
    struct lsa_session *s)
{
    struct smb2_create_out co;
    uint8_t                bind[72];
    uint8_t                stub[LSAW_STUB_MAX];
    const uint8_t         *r;
    uint32_t               st, lst = 0;
    int                    len, rlen;

    memset(s, 0, sizeof(*s));
    s->c = c;

    st = smb2_tree_connect(c, "\\\\server\\IPC$");
    if (st != ST_SUCCESS) {
        lsa_die("lsa_open: TREE_CONNECT IPC$ -> 0x%08x", st);
    }

    st = smb2_create_opts(c, "lsarpc", MBT_FILE_OPEN,
                          MBT_FILE_READ_ACCESS | MBT_FILE_WRITE_ACCESS,
                          MBT_FILE_SHARE_READ | MBT_FILE_SHARE_WRITE, 0, NULL, &co);
    if (st != ST_SUCCESS) {
        lsa_die("lsa_open: CREATE lsarpc -> 0x%08x", st);
    }
    memcpy(s->fid, co.file_id, 16);

    len = lsaw_bind_pdu(bind, ++s->call_id);
    lsa_post_pdu(s, bind, len);
    smb2c_wait(c);
    st = lsa_fsctl_output(c, &r, &rlen);
    if (st != ST_SUCCESS || !r) {
        lsa_die("lsa_open: bind -> 0x%08x", st);
    }
    if (lsaw_parse_bind_ack(r, rlen) != 0 || g32(r, 12) != s->call_id) {
        lsa_die("lsa_open: the bind was not accepted over NDR32 (%d-byte reply)",
                rlen);
    }

    len = lsaw_stub_open_policy2(stub);
    st  = lsa_call_fsctl(s, LSAW_OP_OPENPOLICY2, stub, len, &r, &rlen);
    if (st != ST_SUCCESS || rlen < 0) {
        lsa_die("lsa_open: OpenPolicy2 -> 0x%08x", st);
    }
    if (lsaw_parse_open_policy2(r, rlen, s->handle, &lst) != 0 || lst != 0) {
        lsa_die("lsa_open: OpenPolicy2 reply (%d bytes) -> LSA status 0x%08x",
                rlen, lst);
    }
} /* lsa_open */

/* Has the LSA status the severity of an error?  An error answer may carry no
 * entries at all; anything else names every one asked about. */
static int
lsa_status_is_error(uint32_t st)
{
    return (st >> 30) == 3;
} /* lsa_status_is_error */

/* A pipe transport: lsa_call_fsctl or lsa_call_write_read. */
typedef uint32_t (*lsa_call_t)(
    struct lsa_session *s,
    uint16_t            opnum,
    const uint8_t      *stub,
    int                 stub_len,
    const uint8_t     **rstub,
    int                *rstub_len);

/* LookupSids over `call`, parsed; returns the LSA status.  Dies on an SMB
 * error, a fault, a reply that does not parse, or a reply naming other than n
 * entries -- except no entries at all under an error-severity LSA status. */
static uint32_t
lsa_lookup_sids_over(
    struct lsa_session *s,
    lsa_call_t          call,
    const char *const  *sids,
    int                 n,
    struct lsaw_name   *out,
    int                *ndomains)
{
    uint8_t        stub[LSAW_STUB_MAX];
    const uint8_t *r;
    uint32_t       st, lst = 0;
    int            len, rlen, got;

    if (n > 0) {
        memset(out, 0, (size_t) n * sizeof(*out));
    }
    if (ndomains) {
        *ndomains = 0;
    }

    len = lsaw_stub_lookup_sids(stub, s->handle, sids, n);
    if (len < 0) {
        lsa_die("LookupSids: the %d SIDs do not encode", n);
    }
    st = call(s, LSAW_OP_LOOKUPSIDS, stub, len, &r, &rlen);
    if (st != ST_SUCCESS) {
        lsa_die("LookupSids: %s -> 0x%08x", call == lsa_call_fsctl ?
                "FSCTL_PIPE_TRANSCEIVE" : "SMB2 WRITE + READ", st);
    }
    if (rlen < 0) {
        lsa_die("LookupSids: no response PDU");
    }
    got = lsaw_parse_lookup_sids(r, rlen, out, n, &lst, ndomains);
    if (got < 0 || (got != n && !(got == 0 && lsa_status_is_error(lst)))) {
        lsa_die("LookupSids: a %d-byte reply does not parse into %d names (%d)",
                rlen, n, got);
    }
    return lst;
} /* lsa_lookup_sids_over */

/* LookupSids over FSCTL, as lsa_lookup_sids_over. */
static uint32_t
lsa_lookup_sids(
    struct lsa_session *s,
    const char *const  *sids,
    int                 n,
    struct lsaw_name   *out,
    int                *ndomains)
{
    return lsa_lookup_sids_over(s, lsa_call_fsctl, sids, n, out, ndomains);
} /* lsa_lookup_sids */

/* LookupSids over SMB2 WRITE + READ, as lsa_lookup_sids_over. */
static uint32_t
lsa_lookup_sids_write_read(
    struct lsa_session *s,
    const char *const  *sids,
    int                 n,
    struct lsaw_name   *out,
    int                *ndomains)
{
    return lsa_lookup_sids_over(s, lsa_call_write_read, sids, n, out, ndomains);
} /* lsa_lookup_sids_write_read */

/* LookupNames3 over FSCTL, parsed; returns the LSA status.  Dies on an SMB
 * error, a fault, a reply that does not parse, or a reply naming other than n
 * entries -- except no entries at all under an error-severity LSA status. */
static uint32_t
lsa_lookup_names3(
    struct lsa_session *s,
    const char *const  *names,
    int                 n,
    struct lsaw_sid    *out,
    int                *ndomains)
{
    uint8_t        stub[LSAW_STUB_MAX];
    const uint8_t *r;
    uint32_t       st, lst = 0;
    int            len, rlen, got;

    if (n > 0) {
        memset(out, 0, (size_t) n * sizeof(*out));
    }
    if (ndomains) {
        *ndomains = 0;
    }

    len = lsaw_stub_lookup_names3(stub, s->handle, names, n);
    if (len < 0) {
        lsa_die("LookupNames3: the %d names do not encode", n);
    }
    st = lsa_call_fsctl(s, LSAW_OP_LOOKUPNAMES3, stub, len, &r, &rlen);
    if (st != ST_SUCCESS) {
        lsa_die("LookupNames3: FSCTL_PIPE_TRANSCEIVE -> 0x%08x", st);
    }
    if (rlen < 0) {
        lsa_die("LookupNames3: no response PDU");
    }
    got = lsaw_parse_lookup_names3(r, rlen, out, n, &lst, ndomains);
    if (got < 0 || (got != n && !(got == 0 && lsa_status_is_error(lst)))) {
        lsa_die("LookupNames3: a %d-byte reply does not parse into %d SIDs (%d)",
                rlen, n, got);
    }
    return lst;
} /* lsa_lookup_names3 */

/* ---- S0: the client against the generated NDR code ---------------------- */

/* A distinct byte in every position, and a handle_type whose byte order shows. */
static const uint8_t s0_handle[20] = {
    0x04, 0x03, 0x02, 0x01, 0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5,
    0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf
};

/* Run the generated pull_in over a client stub.  Returns the bytes it used,
 * or -1 if it failed. */
static int
s0_pull(
    uint16_t         opnum,
    const uint8_t   *stub,
    int              len,
    void            *in,
    struct ndr_dbuf *d)
{
    const struct ndr_op_desc *op = ndr_find_op(lsarpc_op_table, lsarpc_op_count, opnum);
    struct ndr_cursor         c;

    if (!op || len < 0) {
        return -1;
    }
    memset(in, 0, op->in_size);
    ndr_cursor_init(&c, stub, (size_t) len);
    if (op->pull_in(&c, in, d) != 0 || c.error) {
        return -1;
    }
    return (int) c.off;
} /* s0_pull */

/* Run the generated push_out.  Returns the stub length, or -1. */
static int
s0_push(
    uint16_t    opnum,
    const void *out,
    uint8_t    *buf,
    size_t      cap)
{
    const struct ndr_op_desc *op = ndr_find_op(lsarpc_op_table, lsarpc_op_count, opnum);
    struct ndr_writer         w;

    if (!op) {
        return -1;
    }
    ndr_writer_init(&w, buf, cap);
    return op->push_out(&w, out);
} /* s0_push */

static int
s0_handle_is(const struct policy_handle *h)
{
    return h->handle_type == g32(s0_handle, 0) &&
           memcmp(h->uuid, s0_handle + 4, 16) == 0;
} /* s0_handle_is */

/* A generated-code SID as its string, through the MS-DTYP binary form. */
static const char *
s0_sid_str(
    const struct ndr_sid *sid,
    char                 *buf,
    size_t                cap)
{
    uint8_t bin[8 + 15 * 4];
    int     i;

    if (sid->sub_authority_count > 15) {
        return "?";
    }
    bin[0] = sid->revision;
    bin[1] = sid->sub_authority_count;
    memcpy(bin + 2, sid->identifier_authority, 6);
    for (i = 0; i < sid->sub_authority_count; i++) {
        p32(bin, 8 + 4 * i, sid->sub_authority[i]);
    }
    return smb2_sid_to_str(bin, sizeof(bin), buf, cap) < 0 ? "?" : buf;
} /* s0_sid_str */

/* A string SID as a generated-code SID. */
static struct ndr_sid *
s0_ndr_sid(
    struct ndr_sid *sid,
    const char     *str)
{
    uint8_t bin[8 + 15 * 4];
    int     i;

    memset(sid, 0, sizeof(*sid));
    if (smb2_sid_str_to_bin(str, bin, (int) sizeof(bin)) < 0) {
        lsa_die("S0: bad test vector %s", str);
    }
    sid->revision            = bin[0];
    sid->sub_authority_count = bin[1];
    memcpy(sid->identifier_authority, bin + 2, 6);
    for (i = 0; i < bin[1]; i++) {
        sid->sub_authority[i] = g32(bin, 8 + 4 * i);
    }
    return sid;
} /* s0_ndr_sid */

/* Point an rpc_unicode_string at `s`, `units` UTF-16 code units long. */
static void
s0_ustr(
    struct rpc_unicode_string *u,
    const char                *s,
    int                        units)
{
    u->length = (uint16_t) (2 * units);
    u->size   = (uint16_t) (2 * units);
    u->buffer = (char *) s;
} /* s0_ustr */

static int
s0_str_is(
    const char *got,
    const char *want)
{
    if (!got || !want) {
        return got == want;
    }
    return strcmp(got, want) == 0;
} /* s0_str_is */

/* The request vectors: a NULL SID, and names that are NULL, empty, two-byte
 * UTF-8 ("caf\xc3\xa9") and a surrogate pair ("x\xf0\x9f\x98\x80"), with
 * each name's length in UTF-16 code units. */
static const char *const s0_sids[] = {
    "S-1-5-21-1-2-3-1001", NULL, "S-1-22-1-1000", "S-1-1-0", "S-1-5-32"
};
static const char *const s0_names[] = {
    "Everyone",
    "BUILTIN\\Administrators",
    "",
    NULL,
    "caf\xc3\xa9",
    "x\xf0\x9f\x98\x80",
    "smbtest"
};
static const int         s0_units[] = { 8, 22, 0, 0, 4, 3, 7 };

static void
s0_requests(struct ndr_dbuf *d)
{
    static uint8_t             stub[LSAW_STUB_MAX];
    struct lsa_OpenPolicy2_in  op2;
    struct lsa_LookupSids_in   ls;
    struct lsa_LookupNames3_in ln;
    struct lsa_Close_in        cl;
    const char *const         *sids  = s0_sids;
    const char *const         *names = s0_names;
    const int                 *units = s0_units;
    char                       buf[96];
    int                        n, used, i, ok;
    const int                  nsids  = (int) (sizeof(s0_sids) / sizeof(s0_sids[0]));
    const int                  nnames = (int) (sizeof(s0_names) / sizeof(s0_names[0]));

    memset(&op2, 0, sizeof(op2));
    memset(&ls, 0, sizeof(ls));
    memset(&ln, 0, sizeof(ln));
    memset(&cl, 0, sizeof(cl));

    n    = lsaw_stub_open_policy2(stub);
    used = s0_pull(LSAW_OP_OPENPOLICY2, stub, n, &op2, d);
    CHECK(n > 0 && used == n && s0_str_is(op2.system_name, LSAW_SYSTEM_NAME) &&
          op2.access_mask == LSAW_ACCESS_MASK,
          "S0 OpenPolicy2 request decodes to system_name %s, access_mask 0x%08x "
          "(%d of %d bytes used)", op2.system_name ? op2.system_name : "(null)",
          op2.access_mask, used, n);

    n    = lsaw_stub_lookup_sids(stub, s0_handle, sids, nsids);
    used = s0_pull(LSAW_OP_LOOKUPSIDS, stub, n, &ls, d);
    ok   = n > 0 && used == n && s0_handle_is(&ls.handle) &&
        ls.sids.num_sids == (uint32_t) nsids && ls.sids.sids &&
        ls.names.count == 0 && !ls.names.names &&
        ls.level == LSAW_LOOKUP_LEVEL && ls.count == 0;
    for (i = 0; ok && i < nsids; i++) {
        if (!sids[i]) {
            ok = !ls.sids.sids[i].sid;
        } else {
            ok = ls.sids.sids[i].sid &&
                strcmp(s0_sid_str(ls.sids.sids[i].sid, buf, sizeof(buf)), sids[i]) == 0;
        }
    }
    CHECK(ok, "S0 LookupSids request decodes to the handle and the same %d SIDs, "
          "one of them NULL (%d of %d bytes used)", nsids, used, n);

    n    = lsaw_stub_lookup_names3(stub, s0_handle, names, nnames);
    used = s0_pull(LSAW_OP_LOOKUPNAMES3, stub, n, &ln, d);
    ok   = n > 0 && used == n && s0_handle_is(&ln.handle) &&
        ln.num_names == (uint32_t) nnames && ln.names &&
        ln.sids.count == 0 && !ln.sids.sids && ln.level == LSAW_LOOKUP_LEVEL &&
        ln.count == 0 && ln.lookup_options == LSAW_LOOKUP_OPTIONS &&
        ln.client_revision == LSAW_CLIENT_REVISION;
    for (i = 0; ok && i < nnames; i++) {
        ok = s0_str_is(ln.names[i].buffer, names[i]) &&
            ln.names[i].length == 2 * units[i] && ln.names[i].size == 2 * units[i];
    }
    CHECK(ok, "S0 LookupNames3 request decodes to the handle and the same %d names "
          "and lengths, one NULL, one empty, two not ASCII (%d of %d bytes used)",
          nnames, used, n);

    n    = lsaw_stub_close(stub, s0_handle);
    used = s0_pull(LSAW_OP_CLOSE, stub, n, &cl, d);
    CHECK(n > 0 && used == n && s0_handle_is(&cl.handle),
          "S0 Close request decodes to the handle (%d of %d bytes used)", used, n);
} /* s0_requests */

static int
s0_name_is(
    const struct lsaw_name *e,
    uint32_t                type,
    const char             *name,
    const char             *domain,
    const char             *domain_sid)
{
    return e->type == type && strcmp(e->name, name) == 0 &&
           strcmp(e->domain, domain) == 0 && strcmp(e->domain_sid, domain_sid) == 0;
} /* s0_name_is */

static int
s0_sid_is(
    const struct lsaw_sid *e,
    uint32_t               type,
    const char            *sid,
    const char            *domain,
    const char            *domain_sid)
{
    return e->type == type && strcmp(e->sid, sid) == 0 &&
           strcmp(e->domain, domain) == 0 && strcmp(e->domain_sid, domain_sid) == 0;
} /* s0_sid_is */

static void
s0_replies(void)
{
    static uint8_t              buf[LSAW_STUB_MAX];
    struct ndr_sid              dsid[2], esid[2];
    struct lsa_DomainInfo       dom[2];
    struct lsa_RefDomainList    rdl, empty_rdl;
    struct lsa_TranslatedName   tn[4];
    struct lsa_TranslatedSidEx2 ts[3];
    struct lsa_LookupSids_out   lso;
    struct lsa_LookupNames3_out lno;
    struct lsaw_name            names[8];
    struct lsaw_sid             sids[8];
    uint32_t                    st = 0xdeadbeef;
    int                         n, got, nd = -1, ok;

    /* LookupSids: two domains, a mapped user, an alias in the second domain,
     * an unmapped SID, and a name that is not ASCII. */
    memset(dom, 0, sizeof(dom));
    s0_ustr(&dom[0].name, "Unix User", 9);
    dom[0].sid = s0_ndr_sid(&dsid[0], "S-1-22-1");
    s0_ustr(&dom[1].name, "BUILTIN", 7);
    dom[1].sid   = s0_ndr_sid(&dsid[1], "S-1-5-32");
    rdl.count    = 2;
    rdl.domains  = dom;
    rdl.max_size = 2;
    memset(tn, 0, sizeof(tn));
    tn[0].sid_type = 1;
    s0_ustr(&tn[0].name, "smbtest", 7);
    tn[0].sid_index = 0;
    tn[1].sid_type  = 4;
    s0_ustr(&tn[1].name, "Administrators", 14);
    tn[1].sid_index = 1;
    tn[2].sid_type  = 8;
    s0_ustr(&tn[2].name, "S-1-5-21-9-9-9-9", 16);
    tn[2].sid_index = 0xffffffff;
    tn[3].sid_type  = 2;
    s0_ustr(&tn[3].name, "caf\xc3\xa9", 4);
    tn[3].sid_index = 0;
    memset(&lso, 0, sizeof(lso));
    lso.domains     = &rdl;
    lso.names.count = 4;
    lso.names.names = tn;
    lso.count       = 3;
    lso.status      = 0x00000107;

    n   = s0_push(LSAW_OP_LOOKUPSIDS, &lso, buf, sizeof(buf));
    got = lsaw_parse_lookup_sids(buf, n, names, 8, &st, &nd);
    ok  = n > 0 && got == 4 && st == 0x00000107 && nd == 2 &&
        s0_name_is(&names[0], 1, "smbtest", "Unix User", "S-1-22-1") &&
        s0_name_is(&names[1], 4, "Administrators", "BUILTIN", "S-1-5-32") &&
        s0_name_is(&names[2], 8, "S-1-5-21-9-9-9-9", "", "") &&
        s0_name_is(&names[3], 2, "caf\xc3\xa9", "Unix User", "S-1-22-1");
    CHECK(ok, "S0 a LookupSids reply parses back to its 4 names, 2 domains and "
          "status (%d entries, status 0x%08x, %d domains)", got, st, nd);

    CHECK(n > 0 && lsaw_parse_lookup_sids(buf, n - 1, names, 8, &st, &nd) < 0 &&
          lsaw_parse_lookup_sids(buf, n + 1, names, 8, &st, &nd) < 0 &&
          lsaw_parse_lookup_sids(buf, n, names, 3, &st, &nd) < 0,
          "S0 ... but not a byte short, a byte long, or into 3 entries");

    /* The answer to an empty request: a domain list with no array. */
    memset(&empty_rdl, 0, sizeof(empty_rdl));
    memset(&lso, 0, sizeof(lso));
    lso.domains = &empty_rdl;
    st          = 0xdeadbeef;
    nd          = -1;
    n           = s0_push(LSAW_OP_LOOKUPSIDS, &lso, buf, sizeof(buf));
    got         = lsaw_parse_lookup_sids(buf, n, NULL, 0, &st, &nd);
    CHECK(n > 0 && got == 0 && st == 0 && nd == 0,
          "S0 an empty LookupSids reply parses to no names and no domains "
          "(%d entries, status 0x%08x, %d domains)", got, st, nd);

    /* LookupNames3: a world-authority domain with an empty name, an alias in
     * BUILTIN, and an unmapped name. */
    memset(dom, 0, sizeof(dom));
    s0_ustr(&dom[0].name, "", 0);
    dom[0].sid = s0_ndr_sid(&dsid[0], "S-1-1");
    s0_ustr(&dom[1].name, "BUILTIN", 7);
    dom[1].sid = s0_ndr_sid(&dsid[1], "S-1-5-32");
    memset(ts, 0, sizeof(ts));
    ts[0].sid_type  = 5;
    ts[0].sid       = s0_ndr_sid(&esid[0], "S-1-1-0");
    ts[0].sid_index = 0;
    ts[1].sid_type  = 4;
    ts[1].sid       = s0_ndr_sid(&esid[1], "S-1-5-32-544");
    ts[1].sid_index = 1;
    ts[2].sid_type  = 8;
    ts[2].sid       = NULL;
    ts[2].sid_index = 0xffffffff;
    memset(&lno, 0, sizeof(lno));
    lno.domains    = &rdl;
    lno.sids.count = 3;
    lno.sids.sids  = ts;
    lno.count      = 2;
    lno.status     = 0x00000107;

    st  = 0xdeadbeef;
    nd  = -1;
    n   = s0_push(LSAW_OP_LOOKUPNAMES3, &lno, buf, sizeof(buf));
    got = lsaw_parse_lookup_names3(buf, n, sids, 8, &st, &nd);
    ok  = n > 0 && got == 3 && st == 0x00000107 && nd == 2 &&
        s0_sid_is(&sids[0], 5, "S-1-1-0", "", "S-1-1") &&
        s0_sid_is(&sids[1], 4, "S-1-5-32-544", "BUILTIN", "S-1-5-32") &&
        s0_sid_is(&sids[2], 8, "", "", "");
    CHECK(ok, "S0 a LookupNames3 reply parses back to its 3 SIDs, 2 domains and "
          "status (%d entries, status 0x%08x, %d domains)", got, st, nd);
} /* s0_replies */

/* S0 "client encodes what the server decodes, and decodes what it encodes" */
static void
run_selftest(void)
{
    struct ndr_dbuf d;

    printf("# --- S0 the client against the generated NDR code ---\n");
    ndr_dbuf_init(&d, 4096);
    s0_requests(&d);
    ndr_dbuf_destroy(&d);
    s0_replies();
} /* run_selftest */

/* ---- live cases --------------------------------------------------------- */

static const char *const s1_sids[]  = { "S-1-22-1-1000" };
static const char *const s2_names[] = { "Everyone" };

/* ---- M: every lookup through the naming rules --------------------------- */

#define LSA_ST_SOME_NOT_MAPPED   0x00000107u
#define LSA_ST_NONE_MAPPED       0xC0000073u
#define LSA_SESSION_FLAG_IS_NULL 0x0002   /* MS-SMB2 2.2.6 SessionFlags */
#define M_COUNT(a) ((int) (sizeof(a) / sizeof((a)[0])))

/* The accounts the M cases name, registered before they run. */
#define M_DOMAIN_SID             "S-1-5-21-7-8-9"
#define M_USER_SID               M_DOMAIN_SID "-1000"
#define M_GROUP_SID              M_DOMAIN_SID "-2001"
#define M_NOSUCH_SID             M_DOMAIN_SID "-4242"
#define M_LEA_SID                M_DOMAIN_SID "-1003"
/* "l", U+00E9, "a": four bytes, three UTF-16 code units.  The string is split
 * because "\xa9a" would be one hex escape. */
#define M_LEA                    "l\xc3\xa9" "a"
#define M_GRIN_SID               M_DOMAIN_SID "-1004"
/* U+1F600 then "x": five bytes, three UTF-16 code units (a surrogate pair). */
#define M_GRIN                   "\xf0\x9f\x98\x80" "x"
/* An account name that is not UTF-8: 0x80 cannot begin a sequence. */
#define M_BAD_SID                M_DOMAIN_SID "-1005"
#define M_BAD                    "bad\x80" "name"

/* One expected answer: {type, name, domain, domain_sid} for LookupSids, or
 * {type, sid, domain, domain_sid} for LookupNames3. */
struct m_entry {
    uint32_t    type;
    const char *what;
    const char *domain;
    const char *domain_sid;
};

/* Do the n LookupSids answers match `want`?  Each one that does not is shown. */
static int
m_names_are(
    const struct lsaw_name *got,
    const struct m_entry   *want,
    int                     n)
{
    int i, ok = 1;

    for (i = 0; i < n; i++) {
        if (!s0_name_is(&got[i], want[i].type, want[i].what, want[i].domain,
                        want[i].domain_sid)) {
            printf("#   entry %d is {%u, \"%s\", \"%s\", \"%s\"}, not {%u, \"%s\", "
                   "\"%s\", \"%s\"}\n", i, got[i].type, got[i].name, got[i].domain,
                   got[i].domain_sid, want[i].type, want[i].what, want[i].domain,
                   want[i].domain_sid);
            ok = 0;
        }
    }
    return ok;
} /* m_names_are */

/* The same for n LookupNames3 answers. */
static int
m_sids_are(
    const struct lsaw_sid *got,
    const struct m_entry  *want,
    int                    n)
{
    int i, ok = 1;

    for (i = 0; i < n; i++) {
        if (!s0_sid_is(&got[i], want[i].type, want[i].what, want[i].domain,
                       want[i].domain_sid)) {
            printf("#   entry %d is {%u, \"%s\", \"%s\", \"%s\"}, not {%u, \"%s\", "
                   "\"%s\", \"%s\"}\n", i, got[i].type, got[i].sid, got[i].domain,
                   got[i].domain_sid, want[i].type, want[i].what, want[i].domain,
                   want[i].domain_sid);
            ok = 0;
        }
    }
    return ok;
} /* m_sids_are */

/* uncrustify 0.78.1 does not converge on these aligned tables -- each pass
 * widens a column -- so they are guarded. */
/* *INDENT-OFF* */
static const char *const    m1_sids[] = {
    "S-1-1-0", "S-1-3-0", "S-1-5-18", "S-1-5-32-544", "S-1-5-32"
};
static const struct m_entry m1_want[] = {
    { 5, "Everyone",       "",             "S-1-1"    },
    { 5, "CREATOR OWNER",  "",             "S-1-3"    },
    { 5, "SYSTEM",         "NT AUTHORITY", "S-1-5"    },
    { 4, "Administrators", "BUILTIN",      "S-1-5-32" },
    { 3, "BUILTIN",        "BUILTIN",      "S-1-5-32" },
};

static const char *const    m2_sids[] = {
    "S-1-22-1-1000", "S-1-5-88-1-1000", "S-1-5-88-2-2001"
};
static const struct m_entry m2_want[] = {
    { 1, "smbtest",  "Unix User",  "S-1-22-1"   },
    { 1, "smbtest",  "Unix User",  "S-1-5-88-1" },
    { 2, "lsagroup", "Unix Group", "S-1-5-88-2" },
};

static const char *const    m3_sids[] = { M_USER_SID, M_GROUP_SID };
static const struct m_entry m3_want[] = {
    { 1, "lsauser",  "WORKGROUP", M_DOMAIN_SID },
    { 2, "lsagroup", "WORKGROUP", M_DOMAIN_SID },
};

static const char *const    m4_sids[] = { M_NOSUCH_SID };
static const struct m_entry m4_want[] = {
    { 8, M_NOSUCH_SID, "", "" },
};

static const char *const    m5_sids[] = { "S-1-5-18", M_NOSUCH_SID };
static const struct m_entry m5_want[] = {
    { 5, "SYSTEM",     "NT AUTHORITY", "S-1-5" },
    { 8, M_NOSUCH_SID, "",             ""      },
};

static const char *const    m6_names[] = {
    "Everyone", "BUILTIN\\Administrators", "Administrators", "lsauser",
    "WORKGROUP\\lsauser", "OTHER\\lsauser", "nosuchuser"
};
static const struct m_entry m6_want[] = {
    { 5, "S-1-1-0",      "",          "S-1-1"      },
    { 4, "S-1-5-32-544", "BUILTIN",   "S-1-5-32"   },
    { 4, "S-1-5-32-544", "BUILTIN",   "S-1-5-32"   },
    { 1, M_USER_SID,     "WORKGROUP", M_DOMAIN_SID },
    { 1, M_USER_SID,     "WORKGROUP", M_DOMAIN_SID },
    { 8, "",             "",          ""           },
    { 8, "",             "",          ""           },
};

static const char *const    m10_sids[]      = { M_LEA_SID };
static const char *const    m10_names[]     = { M_LEA };
static const struct m_entry m10_sid_want[]  = {
    { 1, M_LEA,     "WORKGROUP", M_DOMAIN_SID },
};
static const struct m_entry m10_name_want[] = {
    { 1, M_LEA_SID, "WORKGROUP", M_DOMAIN_SID },
};

static const char *const    m11_sids[]      = { M_GRIN_SID };
static const char *const    m11_names[]     = { M_GRIN };
static const struct m_entry m11_sid_want[]  = {
    { 1, M_GRIN,     "WORKGROUP", M_DOMAIN_SID },
};
static const struct m_entry m11_name_want[] = {
    { 1, M_GRIN_SID, "WORKGROUP", M_DOMAIN_SID },
};

static const char *const    m12_names[]     = { "" };

static const char *const    m13_sids[]      = { M_BAD_SID, M_USER_SID };
static const struct m_entry m13_want[]      = {
    { 8, M_BAD_SID, "",          ""           },
    { 1, "lsauser", "WORKGROUP", M_DOMAIN_SID },
};
/* *INDENT-ON* */

/* Register the accounts the M cases name; dies if the server refuses one. */
static void
m_accounts(struct smb2_env *env)
{
    if (chimera_server_add_user(env->server, "lsauser", "x", "x", M_USER_SID,
                                1001, 2001, 1, (uint32_t[]) { 2001 }, 1) != 0 ||
        chimera_server_add_group(env->server, "lsagroup", M_GROUP_SID, 2001,
                                 1) != 0 ||
        chimera_server_add_user(env->server, M_LEA, "x", "x", M_LEA_SID,
                                1003, 2001, 1, (uint32_t[]) { 2001 }, 1) != 0 ||
        chimera_server_add_user(env->server, M_GRIN, "x", "x", M_GRIN_SID,
                                1004, 2001, 1, (uint32_t[]) { 2001 }, 1) != 0 ||
        chimera_server_add_user(env->server, M_BAD, "x", "x", M_BAD_SID,
                                1005, 2001, 1, (uint32_t[]) { 2001 }, 1) != 0) {
        lsa_die("M: the server refused a test account");
    }
} /* m_accounts */

/* M1-M13 on the session `s` of the memfs env. */
static void
run_naming(
    struct smb2_env    *env,
    struct lsa_session *s)
{
    struct lsaw_name   names[8];
    struct lsaw_sid    sids[8];
    struct smb2_conn  *anon;
    struct lsa_session as;
    uint32_t           st;
    int                nd, ok, is_null;

    printf("# --- M the naming rules ---\n");

    /* M1 "well-known sids are named" */
    st = lsa_lookup_sids(s, m1_sids, M_COUNT(m1_sids), names, &nd);
    ok = m_names_are(names, m1_want, M_COUNT(m1_want));
    CHECK(st == 0 && nd == 4 && ok,
          "M1 well-known sids are named: %d SIDs -> 0x%08x, %d domains",
          M_COUNT(m1_sids), st, nd);

    /* M2 "unix sids are named in the unix domains" */
    st = lsa_lookup_sids(s, m2_sids, M_COUNT(m2_sids), names, &nd);
    ok = m_names_are(names, m2_want, M_COUNT(m2_want));
    CHECK(st == 0 && nd == 3 && ok,
          "M2 unix sids are named in the unix domains: %d SIDs -> 0x%08x, "
          "%d domains", M_COUNT(m2_sids), st, nd);

    /* M3 "account-domain sids carry the server's domain, not NT AUTHORITY" */
    st = lsa_lookup_sids(s, m3_sids, M_COUNT(m3_sids), names, &nd);
    ok = m_names_are(names, m3_want, M_COUNT(m3_want));
    CHECK(st == 0 && nd == 1 && ok,
          "M3 account-domain sids carry the server's domain, not NT AUTHORITY: "
          "%d SIDs -> 0x%08x, %d domains", M_COUNT(m3_sids), st, nd);

    /* M4 "an unknown sid echoes back unmapped" */
    st = lsa_lookup_sids(s, m4_sids, M_COUNT(m4_sids), names, &nd);
    ok = m_names_are(names, m4_want, M_COUNT(m4_want));
    CHECK(st == LSA_ST_NONE_MAPPED && nd == 0 && ok,
          "M4 an unknown sid echoes back unmapped: %s -> 0x%08x, %d domains",
          m4_sids[0], st, nd);

    /* M5 "a partial answer says so" */
    st = lsa_lookup_sids(s, m5_sids, M_COUNT(m5_sids), names, &nd);
    ok = m_names_are(names, m5_want, M_COUNT(m5_want));
    CHECK(st == LSA_ST_SOME_NOT_MAPPED && ok,
          "M5 a partial answer says so: %d SIDs -> 0x%08x", M_COUNT(m5_sids), st);

    /* M6 "names resolve in every form" */
    st = lsa_lookup_names3(s, m6_names, M_COUNT(m6_names), sids, &nd);
    ok = m_sids_are(sids, m6_want, M_COUNT(m6_want));
    CHECK(st == LSA_ST_SOME_NOT_MAPPED && nd == 3 && ok,
          "M6 names resolve in every form: %d names -> 0x%08x, %d domains",
          M_COUNT(m6_names), st, nd);

    /* M7 "the write/read transport gives the same answer" */
    st = lsa_lookup_sids_write_read(s, m3_sids, M_COUNT(m3_sids), names, &nd);
    ok = m_names_are(names, m3_want, M_COUNT(m3_want));
    CHECK(st == 0 && nd == 1 && ok,
          "M7 the write/read transport gives the same answer: %d SIDs -> "
          "0x%08x, %d domains", M_COUNT(m3_sids), st, nd);

    /* M8 "a null session gets the cached answers" */
    anon       = smb2_conn_open(env);
    anon->wire = NULL;    /* the harness default: an anonymous logon */
    st         = smb2_negotiate(anon);
    if (st == ST_SUCCESS) {
        st = smb2_session_setup(anon);
    }
    if (st != ST_SUCCESS) {
        lsa_die("M8: the anonymous logon -> 0x%08x", st);
    }
    is_null = (g16(anon->rbuf + 4 + SMB2_HDR_SIZE, 2) & LSA_SESSION_FLAG_IS_NULL) != 0;
    lsa_open(anon, &as);
    st = lsa_lookup_sids(&as, m3_sids, M_COUNT(m3_sids), names, &nd);
    ok = m_names_are(names, m3_want, M_COUNT(m3_want));
    CHECK(is_null && st == 0 && nd == 1 && ok,
          "M8 a null session gets the cached answers: null session %d, %d SIDs "
          "-> 0x%08x, %d domains", is_null, M_COUNT(m3_sids), st, nd);

    /* M9 "an empty request succeeds" */
    st = lsa_lookup_sids(s, NULL, 0, NULL, &nd);
    CHECK(st == 0 && nd == 0,
          "M9 an empty request succeeds: LookupSids {} -> 0x%08x, %d domains",
          st, nd);
    st = lsa_lookup_names3(s, NULL, 0, NULL, &nd);
    CHECK(st == 0 && nd == 0,
          "M9 ... and LookupNames3 {} -> 0x%08x, %d domains", st, nd);

    /* M10 "a non-ASCII account name round-trips" */
    st = lsa_lookup_sids(s, m10_sids, M_COUNT(m10_sids), names, &nd);
    ok = m_names_are(names, m10_sid_want, M_COUNT(m10_sid_want));
    CHECK(st == 0 && ok,
          "M10 a non-ASCII account name round-trips: LookupSids %s -> 0x%08x",
          m10_sids[0], st);
    st = lsa_lookup_names3(s, m10_names, M_COUNT(m10_names), sids, &nd);
    ok = m_sids_are(sids, m10_name_want, M_COUNT(m10_name_want));
    CHECK(st == 0 && ok,
          "M10 ... and LookupNames3 of it -> 0x%08x", st);

    /* M11 "a surrogate-pair account name round-trips" */
    st = lsa_lookup_sids(s, m11_sids, M_COUNT(m11_sids), names, &nd);
    ok = m_names_are(names, m11_sid_want, M_COUNT(m11_sid_want));
    CHECK(st == 0 && ok,
          "M11 a surrogate-pair account name round-trips: LookupSids %s -> 0x%08x",
          m11_sids[0], st);
    st = lsa_lookup_names3(s, m11_names, M_COUNT(m11_names), sids, &nd);
    ok = m_sids_are(sids, m11_name_want, M_COUNT(m11_name_want));
    CHECK(st == 0 && ok,
          "M11 ... and LookupNames3 of it -> 0x%08x", st);

    /* M12 "the empty name names the machine domain" */
    st = lsa_lookup_names3(s, m12_names, M_COUNT(m12_names), sids, &nd);
    CHECK(st == 0 && nd == 1 && sids[0].type == 3 &&
          strncmp(sids[0].sid, "S-1-5-21-", 9) == 0 &&
          strcmp(sids[0].sid, sids[0].domain_sid) == 0 &&
          strcmp(sids[0].domain, "WORKGROUP") == 0,
          "M12 the empty name names the machine domain: \"\" -> 0x%08x "
          "{%u, %s, \"%s\", %s}", st, sids[0].type, sids[0].sid, sids[0].domain,
          sids[0].domain_sid);

    /* M13 "a name that cannot be sent leaves only its own entry unmapped".
     * An account whose name is not UTF-8 has no UTF-16 form to reply with;
     * the other SIDs of the request are still named. */
    st = lsa_lookup_sids(s, m13_sids, M_COUNT(m13_sids), names, &nd);
    ok = m_names_are(names, m13_want, M_COUNT(m13_want));
    CHECK(st == LSA_ST_SOME_NOT_MAPPED && ok,
          "M13 a name that cannot be sent leaves only its own entry unmapped: "
          "%d SIDs -> 0x%08x, {%u, \"%s\"} and {%u, \"%s\"}",
          M_COUNT(m13_sids), st, names[0].type, names[0].name, names[1].type,
          names[1].name);
} /* run_naming */

static void
run_memfs(void)
{
    static uint8_t       stub[LSAW_STUB_MAX];
    struct smb2_env      env;
    struct smb2_env_opts opts = { 0 };
    struct smb2_conn    *c;
    struct lsa_session   s;
    struct lsaw_name     name[1];
    struct lsaw_sid      sid[1];
    const uint8_t       *r;
    uint32_t             st, lst = 0;
    int                  nd = -1, len, rlen, got;

    printf("# --- memfs, local account %s ---\n", SMB2W_USER);

    smb2_env_open_wire(&env, &opts, smb2_wire_profile_find("ntlmv2"));
    smb2_env_fs_setup(&env, "fs0");
    c = smb2_conn_open(&env);
    smb2_handshake_as(c, SMB2W_USER, SMB2W_PASSWORD);
    lsa_open(c, &s);

    /* S1 "a unix sid names its cached user" */
    st = lsa_lookup_sids(&s, s1_sids, 1, name, &nd);
    CHECK(st == 0 && s0_name_is(&name[0], 1, "smbtest", "Unix User", "S-1-22-1"),
          "S1 a unix sid names its cached user: %s -> 0x%08x {%u, %s, %s, %s}",
          s1_sids[0], st, name[0].type, name[0].name, name[0].domain, name[0].domain_sid);

    /* The same call through the other transport. */
    memset(name, 0, sizeof(name));
    len = lsaw_stub_lookup_sids(stub, s.handle, s1_sids, 1);
    st  = lsa_call_write_read(&s, LSAW_OP_LOOKUPSIDS, stub, len, &r, &rlen);
    got = rlen < 0 ? -1 : lsaw_parse_lookup_sids(r, rlen, name, 1, &lst, &nd);
    CHECK(st == ST_SUCCESS && got == 1 && lst == 0 &&
          s0_name_is(&name[0], 1, "smbtest", "Unix User", "S-1-22-1"),
          "S1 ... and the same over SMB2 WRITE + READ: SMB 0x%08x, LSA 0x%08x "
          "{%u, %s, %s, %s}", st, lst, name[0].type, name[0].name, name[0].domain,
          name[0].domain_sid);

    /* S2 "a well-known name resolves" */
    st = lsa_lookup_names3(&s, s2_names, 1, sid, &nd);
    CHECK(st == 0 && s0_sid_is(&sid[0], 5, "S-1-1-0", "", "S-1-1"),
          "S2 a well-known name resolves: %s -> 0x%08x {%u, %s, \"%s\", %s}",
          s2_names[0], st, sid[0].type, sid[0].sid, sid[0].domain, sid[0].domain_sid);

    m_accounts(&env);
    run_naming(&env, &s);

    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* run_memfs */

#ifdef SMB2W_WBSTUB
/* *INDENT-OFF* */
static const struct smb2_wire_profile lsa_winbind_profile = {
    .name = "lsa-winbind", .max_dialect = 0x0300, .ntlmv2 = 1, .sign = 1,
    .winbind_domain = WBSTUB_DOMAIN
};
/* *INDENT-ON* */

/* ---- W: misses resolved through winbind before the reply ---------------- */

/* A pool account of the stub's domain: its SID, and its LookupSids answer. */
#define W_P(rid) WBSTUB_DOMAIN_SID "-" #rid
#define W_U(rid) { 1, "user" #rid, WBSTUB_DOMAIN, WBSTUB_DOMAIN_SID }

#define W_FIRST_CAPPED_RID 1020u
#define W_CAPPED           70     /* SIDs in W14's request           */
#define W_RESOLVED         64     /* the misses one request resolves */
#define W_PARKED           16     /* the lookups one connection parks */

/* wbcLookupSid + wbcLookupName calls so far. */
static unsigned int
w_calls(void)
{
    return __atomic_load_n(&wbstub_lookup_calls, __ATOMIC_RELAXED);
} /* w_calls */

/* Make every stub lookup take `ms` milliseconds (0: none).  Only called with
 * no lookup in flight: the stub reads the variable on each call. */
static void
w_delay(int ms)
{
    char v[16];

    if (ms > 0) {
        snprintf(v, sizeof(v), "%d", ms);
        setenv("WBSTUB_LOOKUP_DELAY_MS", v, 1);
    } else {
        unsetenv("WBSTUB_LOOKUP_DELAY_MS");
    }
} /* w_delay */

/* Keep every stub lookup in flight (1) until released (0). */
static void
w_hold(int on)
{
    __atomic_store_n(&wbstub_hold, on, __ATOMIC_RELEASE);
} /* w_hold */

/* A new connection logged on as the stub's domain user, with the pipe open. */
static struct smb2_conn *
w_session(
    struct smb2_env    *env,
    struct lsa_session *s)
{
    struct smb2_conn *c = smb2_conn_open(env);

    smb2_handshake_as(c, WBSTUB_USER, WBSTUB_PASSWORD);
    lsa_open(c, s);
    return c;
} /* w_session */

/* Post a LookupSids over FSCTL without waiting. */
static void
w_post_sids(
    struct lsa_session *s,
    const char *const  *sids,
    int                 n)
{
    static uint8_t stub[LSAW_STUB_MAX];
    int            len = lsaw_stub_lookup_sids(stub, s->handle, sids, n);

    if (len < 0) {
        lsa_die("LookupSids: the %d SIDs do not encode", n);
    }
    lsa_post_fsctl(s, LSAW_OP_LOOKUPSIDS, stub, len);
} /* w_post_sids */

/* Parse the LookupSids reply now in s->c's reply buffer into n names and
 * return its LSA status; dies when it is not such a reply. */
static uint32_t
w_collect_sids(
    struct lsa_session *s,
    int                 n,
    struct lsaw_name   *out,
    int                *ndomains)
{
    const uint8_t *r;
    uint32_t       st, lst = 0;
    int            rlen, got;

    st = lsa_fsctl_collect(s, &r, &rlen);
    if (st != ST_SUCCESS || rlen < 0) {
        lsa_die("LookupSids: FSCTL_PIPE_TRANSCEIVE -> 0x%08x, %d-byte reply", st,
                rlen);
    }
    got = lsaw_parse_lookup_sids(r, rlen, out, n, &lst, ndomains);
    if (got != n) {
        lsa_die("LookupSids: a %d-byte reply does not parse into %d names (%d)",
                rlen, n, got);
    }
    return lst;
} /* w_collect_sids */

/* Pump until `c` has seen an interim or its reply: 1 when the interim came,
 * 0 when the reply came without one. */
static int
w_wait_interim(struct smb2_conn *c)
{
    uint64_t deadline = smb2c_now_ms() + SMB2C_HANG_MS;

    while (!c->interim_pending && !c->reply_ready) {
        smb2_pump(c->env);
        if (c->disconnected) {
            smb2c_dead(c);
        }
        if (smb2c_now_ms() >= deadline) {
            smb2c_hang(c, "an interim response");
        }
    }
    return c->interim_pending;
} /* w_wait_interim */

/* W16's replies, in the order they land: the connection's on_reply hook
 * parses each one as it arrives, since several can land in one pump. */
struct w_reply {
    uint32_t         call_id;     /* 0 when the reply is not an LSA response */
    uint32_t         smb_status;
    uint32_t         lsa_status;
    int              parsed;      /* the stub parsed into exactly one name   */
    struct lsaw_name name;
};

struct w_reply_log {
    int            n;             /* replies seen, kept or not               */
    struct w_reply r[W_PARKED + 1];
};

static void
w_log_reply(struct smb2_conn *c)
{
    struct w_reply_log *log = c->on_reply_arg;
    struct w_reply     *r;
    const uint8_t      *pdu, *stub;
    uint32_t            fault = 0;
    int                 len, n;

    if (log->n++ >= M_COUNT(log->r)) {
        return;
    }
    r = &log->r[log->n - 1];
    memset(r, 0, sizeof(*r));
    r->smb_status = lsa_fsctl_output(c, &pdu, &len);
    if (!pdu) {
        return;
    }
    n = lsaw_response_stub(pdu, len, &stub, &fault);
    if (n < 0) {
        return;
    }
    r->call_id = g32(pdu, 12);
    r->parsed  = lsaw_parse_lookup_sids(stub, n, &r->name, 1, &r->lsa_status,
                                        NULL) == 1;
} /* w_log_reply */

/* The logged reply to LSA call `call_id`, or NULL. */
static const struct w_reply *
w_logged(
    const struct w_reply_log *log,
    uint32_t                  call_id)
{
    int i;

    for (i = 0; i < log->n && i < M_COUNT(log->r); i++) {
        if (log->r[i].call_id == call_id) {
            return &log->r[i];
        }
    }
    return NULL;
} /* w_logged */

/* Pump until `c` has seen `ninterim` interims and `log` `nreply` replies, or
 * the hang ceiling passes (a named FAIL rather than a wedge). */
static void
w_pump_until(
    struct smb2_conn         *c,
    const struct w_reply_log *log,
    int                       ninterim,
    int                       nreply)
{
    uint64_t deadline = smb2c_now_ms() + SMB2C_HANG_MS;

    while (c->ninterim < ninterim || log->n < nreply) {
        smb2_pump(c->env);
        if (c->disconnected) {
            smb2c_dead(c);
        }
        if (smb2c_now_ms() >= deadline) {
            printf("#   gave up after %d ms: %d interim(s), %d repl%s\n",
                   SMB2C_HANG_MS, c->ninterim, log->n, log->n == 1 ? "y" : "ies");
            return;
        }
    }
} /* w_pump_until */

/* The owner SID of an open file, or "" when it cannot be read. */
static void
w_owner(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    char             *owner,
    size_t            cap)
{
    uint8_t        buf[2048];
    struct smb2_sd sd;
    uint32_t       st, len = 0;

    owner[0] = '\0';
    st       = smb2_query_info(c, SMB2_INFO_SECURITY_T, 0, file_id,
                               SMB2_SEC_OWNER, buf, sizeof(buf), &len);
    if (st == ST_SUCCESS && smb2_sd_parse(buf, len, &sd) == 0 && sd.have_owner) {
        snprintf(owner, cap, "%s", sd.owner);
    }
} /* w_owner */

/* *INDENT-OFF* */
static const char *const    w1_sids[]  = { W_P(1002) };
static const struct m_entry w1_want[]  = { W_U(1002) };

static const char *const    w3_sids[]  = { WBSTUB_USER_SID, WBSTUB_GROUP_SID };
static const struct m_entry w3_want[]  = {
    { 1, WBSTUB_USER,       WBSTUB_DOMAIN, WBSTUB_DOMAIN_SID },
    { 2, WBSTUB_GROUP_NAME, WBSTUB_DOMAIN, WBSTUB_DOMAIN_SID },
};

static const char *const    w4_sids[]  = { W_P(4242) };

static const char *const    w5_sids[]  = { WBSTUB_DOWN_SID };
static const struct m_entry w5_want[]  = {
    { 1, "cfguser", WBSTUB_DOMAIN, WBSTUB_DOMAIN_SID },
};

static const char *const    w6_names[] = {
    "user1010", WBSTUB_DOMAIN "\\user1010", "wbstub\\USER1010",
    WBSTUB_GROUP_NAME, WBSTUB_DOMAIN "\\nosuch"
};
static const struct m_entry w6_want[]  = {
    { 1, W_P(1010),        WBSTUB_DOMAIN, WBSTUB_DOMAIN_SID },
    { 1, W_P(1010),        WBSTUB_DOMAIN, WBSTUB_DOMAIN_SID },
    { 1, W_P(1010),        WBSTUB_DOMAIN, WBSTUB_DOMAIN_SID },
    { 2, WBSTUB_GROUP_SID, WBSTUB_DOMAIN, WBSTUB_DOMAIN_SID },
    { 8, "",               "",            ""                },
};

static const char *const    w7_names[] = { WBSTUB_USER, "user1011" };
static const struct m_entry w7_want[]  = {
    { 1, WBSTUB_USER_SID, WBSTUB_DOMAIN, WBSTUB_DOMAIN_SID },
    { 1, W_P(1011),       WBSTUB_DOMAIN, WBSTUB_DOMAIN_SID },
};

static const char *const    w8_sids[]  = { W_P(1003) };
static const struct m_entry w8_want[]  = { W_U(1003) };
static const char *const    w9_sids[]  = { W_P(1004) };
static const struct m_entry w9_want[]  = { W_U(1004) };
static const char *const    w10_sids[] = { W_P(1005) };
static const struct m_entry w10_want[] = { W_U(1005) };
static const char *const    w11_sids[] = { W_P(1006) };
static const char *const    w12_sids[] = { W_P(1007) };
static const struct m_entry w12_want[] = { W_U(1007) };

static const char *const    w13_sids[] = { NULL, W_P(1012), W_P(1012), "S-1-22-1-0" };
static const struct m_entry w13_want[] = {
    { 8, "",     "",          ""         },
    W_U(1012),
    W_U(1012),
    { 1, "root", "Unix User", "S-1-22-1" },
};

static const char *const    w15_sids[] = { W_P(1090) };
static const struct m_entry w15_want[] = { W_U(1090) };

/* W_PARKED fresh pool accounts, then the one past the bound. */
static const char *const    w16_sids[W_PARKED + 1] = {
    W_P(1013), W_P(1014), W_P(1015), W_P(1016), W_P(1017), W_P(1018),
    W_P(1019), W_P(1091), W_P(1092), W_P(1093), W_P(1094), W_P(1095),
    W_P(1096), W_P(1097), W_P(1098), W_P(1099), W_P(1008)
};
/* *INDENT-ON* */

/* W1-W16 on the session `s` (connection `c`) of the winbind env. */
static void
run_resolve(
    struct smb2_env    *env,
    struct smb2_conn   *c,
    struct lsa_session *s)
{
    static struct lsaw_name   names[W_CAPPED];
    static char               w14_buf[W_CAPPED][32];
    static const char        *w14_sids[W_CAPPED];
    static struct w_reply_log w16_log;
    struct lsaw_sid           sids[8];
    struct smb2_create_out    co;
    struct smb2_conn         *other, *anon, *a, *b;
    struct lsa_session        os, as, sa, sb;
    struct lsaw_name          name_b[1];
    const struct w_reply     *r;
    char                      owner1[SMB2_SID_STR_MAX], owner2[SMB2_SID_STR_MAX], want[32];
    uint32_t                  st, st_b = 0;
    uint32_t                  w16_ids[W_PARKED + 1];
    unsigned int              calls, asked;
    int                       early;
    int                       nd, ok, n0, parked, held, i;
    uint64_t                  t0;

    printf("# --- W misses resolved before the reply ---\n");

    if (chimera_server_add_user(env->server, "cfguser", "x", "x", WBSTUB_DOWN_SID,
                                30001, 30001, 1, (uint32_t[]) { 30001 }, 1) != 0) {
        lsa_die("W: the server refused the config user");
    }

    /* W1 "an account that never logged on is named on the first request" */
    calls = w_calls();
    st    = lsa_lookup_sids(s, w1_sids, M_COUNT(w1_sids), names, &nd);
    ok    = m_names_are(names, w1_want, M_COUNT(w1_want));
    CHECK(st == 0 && ok && w_calls() == calls + 1,
          "W1 an account that never logged on is named on the first request: "
          "%s -> 0x%08x, %u winbind lookup(s)", w1_sids[0], st, w_calls() - calls);

    /* W2 "the second request is answered from the account cache" */
    calls = w_calls();
    n0    = c->ninterim;
    st    = lsa_lookup_sids(s, w1_sids, M_COUNT(w1_sids), names, &nd);
    ok    = m_names_are(names, w1_want, M_COUNT(w1_want));
    CHECK(st == 0 && ok && w_calls() == calls && c->ninterim == n0,
          "W2 the second request is answered from the account cache: %s -> "
          "0x%08x, %u winbind lookup(s), %d interim(s)", w1_sids[0], st,
          w_calls() - calls, c->ninterim - n0);

    /* W3 "the session user and its group carry winbind's names" */
    st = lsa_lookup_sids(s, w3_sids, M_COUNT(w3_sids), names, &nd);
    ok = m_names_are(names, w3_want, M_COUNT(w3_want));
    CHECK(st == 0 && nd == 1 && ok,
          "W3 the session user and its group carry winbind's names: %d SIDs -> "
          "0x%08x, %d domains", M_COUNT(w3_sids), st, nd);

    /* W4 "an account winbind does not know stays unmapped" */
    st = lsa_lookup_sids(s, w4_sids, M_COUNT(w4_sids), names, &nd);
    CHECK(st == LSA_ST_NONE_MAPPED && names[0].type == 8,
          "W4 an account winbind does not know stays unmapped: %s -> 0x%08x, "
          "type %u", w4_sids[0], st, names[0].type);

    /* W4b "asked again, it is answered at once": the resolver keeps W4's
     * negative answer, so this round is answered inline. */
    calls = w_calls();
    n0    = c->ninterim;
    st    = lsa_lookup_sids(s, w4_sids, M_COUNT(w4_sids), names, &nd);
    CHECK(st == LSA_ST_NONE_MAPPED && names[0].type == 8 && w_calls() == calls &&
          c->ninterim == n0,
          "W4b asked again, it is answered at once: %s -> 0x%08x, type %u, %u "
          "winbind lookup(s), %d interim(s)", w4_sids[0], st, names[0].type,
          w_calls() - calls, c->ninterim - n0);

    /* W5 "winbind down falls back to the cached config user" */
    st = lsa_lookup_sids(s, w5_sids, M_COUNT(w5_sids), names, &nd);
    ok = m_names_are(names, w5_want, M_COUNT(w5_want));
    CHECK(st == 0 && ok,
          "W5 winbind down falls back to the cached config user: %s -> 0x%08x",
          w5_sids[0], st);

    /* W6 "names resolve through winbind in every case form", and a name
     * sent without a domain is asked of winbind with one. */
    asked = __atomic_load_n(&wbstub_bare_name_calls, __ATOMIC_RELAXED);
    st    = lsa_lookup_names3(s, w6_names, M_COUNT(w6_names), sids, &nd);
    ok    = m_sids_are(sids, w6_want, M_COUNT(w6_want));
    asked = __atomic_load_n(&wbstub_bare_name_calls, __ATOMIC_RELAXED) - asked;
    CHECK(st == LSA_ST_SOME_NOT_MAPPED && ok && asked == 0,
          "W6 names resolve through winbind in every case form: %d names -> "
          "0x%08x, %d domains, %u asked of winbind without a domain",
          M_COUNT(w6_names), st, nd, asked);

    /* W7 "name lookups leave the user cache alone" */
    other = smb2_conn_open(env);
    smb2_handshake_as(other, WBSTUB_USER, WBSTUB_PASSWORD);
    st = smb2_create(other, "w7.txt", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &co);
    if (st != ST_SUCCESS) {
        lsa_die("W7: CREATE w7.txt -> 0x%08x", st);
    }
    w_owner(other, co.file_id, owner1, sizeof(owner1));
    st = lsa_lookup_names3(s, w7_names, M_COUNT(w7_names), sids, &nd);
    ok = m_sids_are(sids, w7_want, M_COUNT(w7_want));
    w_owner(other, co.file_id, owner2, sizeof(owner2));
    CHECK(st == 0 && ok && strcmp(owner1, WBSTUB_USER_SID) == 0 &&
          strcmp(owner2, WBSTUB_USER_SID) == 0 &&
          chimera_server_get_user(env->server, "user1011") == NULL,
          "W7 name lookups leave the user cache alone: LookupNames3 -> 0x%08x, "
          "owner %s before and %s after, user1011 %s the user cache", st,
          owner1, owner2,
          chimera_server_get_user(env->server, "user1011") ? "in" : "not in");
    smb2_close(other, co.file_id);

    /* W8 "a slow winbind answer arrives after an interim" */
    w_delay(300);
    n0 = c->ninterim;
    st = lsa_lookup_sids(s, w8_sids, M_COUNT(w8_sids), names, &nd);
    ok = m_names_are(names, w8_want, M_COUNT(w8_want));
    w_delay(0);
    CHECK(st == 0 && ok && c->ninterim == n0 + 1,
          "W8 a slow winbind answer arrives after an interim: %s -> 0x%08x, "
          "%d interim(s)", w8_sids[0], st, c->ninterim - n0);

    /* W9 "cancel does not cut a lookup short".  The stub holds the lookup in
     * flight across the CANCEL.  The ECHO round trips sent behind the CANCEL
     * are answered only once the server has read it, and no reply to the
     * lookup may come ahead of them: it is answered when winbind is. */
    w_hold(1);
    w_post_sids(s, w9_sids, M_COUNT(w9_sids));
    parked = w_wait_interim(c) && !c->reply_ready;
    early  = 0;
    if (parked) {
        smb2_cancel_post(c, 0, c->last_async_id);
        early          = smb2_echo_barrier(c);
        c->reply_ready = 0;
    }
    w_hold(0);
    st = smb2c_wait(c);
    ok = 0;
    if (st == ST_SUCCESS) {
        st_b = w_collect_sids(s, M_COUNT(w9_sids), names, &nd);
        ok   = st_b == 0 && m_names_are(names, w9_want, M_COUNT(w9_want));
    }
    CHECK(parked && early == 0 && st == ST_SUCCESS && ok,
          "W9 cancel does not cut a lookup short: parked %d, cancelled, %d "
          "repl%s while winbind had not answered, then SMB 0x%08x (not "
          "0x%08x), LSA 0x%08x", parked, early, early == 1 ? "y" : "ies", st,
          ST_CANCELLED, st_b);

    /* W10 "write/read waits for the answer too" */
    w_delay(300);
    n0 = c->ninterim;
    st = lsa_lookup_sids_write_read(s, w10_sids, M_COUNT(w10_sids), names, &nd);
    ok = m_names_are(names, w10_want, M_COUNT(w10_want));
    w_delay(0);
    CHECK(st == 0 && ok && c->ninterim == n0 + 1,
          "W10 write/read waits for the answer too: %s -> 0x%08x after %d "
          "interim(s)", w10_sids[0], st, c->ninterim - n0);

    /* W11 "a null session never reaches winbind" */
    anon       = smb2_conn_open(env);
    anon->wire = NULL;    /* the harness default: an anonymous logon */
    st         = smb2_negotiate(anon);
    if (st == ST_SUCCESS) {
        st = smb2_session_setup(anon);
    }
    if (st != ST_SUCCESS) {
        lsa_die("W11: the anonymous logon -> 0x%08x", st);
    }
    lsa_open(anon, &as);
    calls = w_calls();
    st    = lsa_lookup_sids(&as, w11_sids, M_COUNT(w11_sids), names, &nd);
    CHECK(st == LSA_ST_NONE_MAPPED && names[0].type == 8 && w_calls() == calls &&
          anon->ninterim == 0,
          "W11 a null session never reaches winbind: %s -> 0x%08x, type %u, "
          "%u winbind lookup(s), %d interim(s)", w11_sids[0], st, names[0].type,
          w_calls() - calls, anon->ninterim);

    /* W12 "a lookup outlives its connection".  The stub holds the lookup in
     * flight while the client drops the connection, and lets it go only
     * after the server has had time to take the connection down: winbind's
     * answer then comes back to a request with no connection left.  Passes
     * when the server survives that and serves a new connection. */
    other = w_session(env, &os);
    calls = w_calls();
    w_hold(1);
    w_post_sids(&os, w12_sids, M_COUNT(w12_sids));
    parked = w_wait_interim(other) && !other->reply_ready;
    smb2_conn_disconnect(other);
    t0 = smb2c_now_ms();
    while (smb2c_now_ms() - t0 < 200) {
        smb2_pump(env);
    }
    asked = w_calls() - calls;
    w_hold(0);
    t0 = smb2c_now_ms();
    while (smb2c_now_ms() - t0 < 200) {
        smb2_pump(env);
    }
    w_session(env, &os);
    calls = w_calls();
    st    = lsa_lookup_sids(&os, w12_sids, M_COUNT(w12_sids), names, &nd);
    ok    = m_names_are(names, w12_want, M_COUNT(w12_want));
    CHECK(parked && asked == 1 && st == 0 && ok,
          "W12 a lookup outlives its connection: parked %d with %u winbind "
          "lookup(s) in flight, dropped, then a new connection's %s -> "
          "0x%08x (%u winbind lookup(s))", parked, asked, w12_sids[0], st,
          w_calls() - calls);

    /* W13 "duplicates and a null sid in one request" */
    calls = w_calls();
    st    = lsa_lookup_sids(s, w13_sids, M_COUNT(w13_sids), names, &nd);
    ok    = m_names_are(names, w13_want, M_COUNT(w13_want));
    CHECK(st == LSA_ST_SOME_NOT_MAPPED && ok && w_calls() == calls + 1,
          "W13 duplicates and a null sid in one request: %d SIDs -> 0x%08x, %u "
          "winbind lookup(s)", M_COUNT(w13_sids), st, w_calls() - calls);

    /* W14 "past 64 misses the rest stay unmapped" */
    for (i = 0; i < W_CAPPED; i++) {
        snprintf(w14_buf[i], sizeof(w14_buf[i]), WBSTUB_DOMAIN_SID "-%u",
                 W_FIRST_CAPPED_RID + (unsigned int) i);
        w14_sids[i] = w14_buf[i];
    }
    calls = w_calls();
    st    = lsa_lookup_sids(s, w14_sids, W_CAPPED, names, &nd);
    ok    = 1;
    for (i = 0; i < W_CAPPED; i++) {
        snprintf(want, sizeof(want), "user%u", W_FIRST_CAPPED_RID + (unsigned int) i);
        if (i < W_RESOLVED ?
            !s0_name_is(&names[i], 1, want, WBSTUB_DOMAIN, WBSTUB_DOMAIN_SID) :
            names[i].type != 8) {
            printf("#   entry %d is {%u, \"%s\", \"%s\", \"%s\"}\n", i,
                   names[i].type, names[i].name, names[i].domain,
                   names[i].domain_sid);
            ok = 0;
        }
    }
    CHECK(st == LSA_ST_SOME_NOT_MAPPED && ok && w_calls() == calls + W_RESOLVED,
          "W14 past 64 misses the rest stay unmapped: %d SIDs -> 0x%08x, %u "
          "winbind lookup(s)", W_CAPPED, st, w_calls() - calls);

    /* W15 "two connections waiting on one sid both get it" */
    w_delay(300);
    a = w_session(env, &sa);
    b = w_session(env, &sb);
    w_post_sids(&sa, w15_sids, M_COUNT(w15_sids));
    w_post_sids(&sb, w15_sids, M_COUNT(w15_sids));
    st   = smb2c_wait(a);
    st_b = smb2c_wait(b);
    w_delay(0);
    if (st != ST_SUCCESS || st_b != ST_SUCCESS) {
        lsa_die("W15: the lookups -> 0x%08x and 0x%08x", st, st_b);
    }
    st   = w_collect_sids(&sa, M_COUNT(w15_sids), names, &nd);
    st_b = w_collect_sids(&sb, M_COUNT(w15_sids), name_b, &nd);
    ok   = m_names_are(names, w15_want, M_COUNT(w15_want)) &&
        m_names_are(name_b, w15_want, M_COUNT(w15_want));
    CHECK(st == 0 && st_b == 0 && ok,
          "W15 two connections waiting on one sid both get it: %s -> 0x%08x "
          "and 0x%08x", w15_sids[0], st, st_b);

    /* W16 "past 16 parked lookups the rest answer from the caches".  The
     * stub holds the 16 lookups in flight until the 17th has answered, so
     * the one reply that lands while they are held has to be the 17th's. */
    other               = w_session(env, &os);
    other->on_reply     = w_log_reply;
    other->on_reply_arg = &w16_log;
    calls               = w_calls();
    n0                  = other->ninterim;
    w_hold(1);
    for (i = 0; i < W_PARKED; i++) {
        w_post_sids(&os, &w16_sids[i], 1);
        w16_ids[i] = os.call_id;
    }
    w_pump_until(other, &w16_log, n0 + W_PARKED, 0);
    parked = other->ninterim - n0;
    w_post_sids(&os, &w16_sids[W_PARKED], 1);
    w16_ids[W_PARKED] = os.call_id;
    w_pump_until(other, &w16_log, 0, 1);
    held = w16_log.n == 1 && w_logged(&w16_log, w16_ids[W_PARKED]) != NULL;
    w_hold(0);
    w_pump_until(other, &w16_log, 0, W_PARKED + 1);
    other->on_reply = NULL;

    ok = held && w16_log.n == W_PARKED + 1;
    for (i = 0; i < W_PARKED; i++) {
        r = w_logged(&w16_log, w16_ids[i]);
        snprintf(want, sizeof(want), "user%s", strrchr(w16_sids[i], '-') + 1);
        if (!r || r->smb_status != ST_SUCCESS || !r->parsed || r->lsa_status != 0 ||
            !s0_name_is(&r->name, 1, want, WBSTUB_DOMAIN, WBSTUB_DOMAIN_SID)) {
            printf("#   parked lookup %d (%s): %s {%u, \"%s\", \"%s\"}, LSA 0x%08x\n",
                   i, w16_sids[i], r ? "answered" : "no reply",
                   r ? r->name.type : 0, r ? r->name.name : "",
                   r ? r->name.domain : "", r ? r->lsa_status : 0);
            ok = 0;
        }
    }
    r = w_logged(&w16_log, w16_ids[W_PARKED]);
    CHECK(ok && parked == W_PARKED && other->ninterim == n0 + W_PARKED &&
          r && r->smb_status == ST_SUCCESS && r->parsed &&
          r->lsa_status == LSA_ST_NONE_MAPPED && r->name.type == 8 &&
          w_calls() == calls + W_PARKED,
          "W16 past 16 parked lookups the rest answer from the caches: %d "
          "parked behind %d interim(s), then %s -> 0x%08x type %u, %d "
          "interim(s) in all, %u winbind lookup(s), %s, parked replies %s",
          W_PARKED, parked, w16_sids[W_PARKED], r ? r->lsa_status : 0,
          r ? r->name.type : 0, other->ninterim - n0, w_calls() - calls,
          held ? "answered while they were held" : "not answered first",
          ok ? "all named" : "wrong");
} /* run_resolve */

static void
run_winbind(void)
{
    static const uint8_t zero[20] = { 0 };
    struct smb2_env      env;
    struct smb2_env_opts opts = { 0 };
    struct smb2_conn    *c;
    struct lsa_session   s;

    printf("# --- %s, domain user %s\\%s ---\n", lsa_winbind_profile.name,
           WBSTUB_DOMAIN, WBSTUB_USER);

    smb2_env_open_wire(&env, &opts, &lsa_winbind_profile);
    smb2_env_fs_setup(&env, "fs0");
    c = smb2_conn_open(&env);
    smb2_handshake_as(c, WBSTUB_USER, WBSTUB_PASSWORD);
    lsa_open(c, &s);

    CHECK(memcmp(s.handle, zero, sizeof(zero)) != 0 && wbstub_auth_calls > 0,
          "lsa_open over a signed session of a winbind user returns a policy "
          "handle (%u winbind logons)", wbstub_auth_calls);

    run_resolve(&env, c, &s);

    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* run_winbind */
#endif /* ifdef SMB2W_WBSTUB */

int
main(
    int   argc,
    char *argv[])
{
    int winbind = argc > 1 && strcmp(argv[1], "--winbind") == 0;

    /* The server logs to stderr; an unbuffered stdout keeps each check ahead
     * of whatever the server says after it. */
    setvbuf(stdout, NULL, _IONBF, 0);

#ifndef SMB2W_WBSTUB
    if (winbind) {
        fprintf(stderr, "--winbind needs the wbclient stub build "
                "(smb2_lsarpc_probe_wbstub)\n");
        return 2;
    }
#endif /* ifndef SMB2W_WBSTUB */

    run_selftest();

#ifdef SMB2W_WBSTUB
    if (winbind) {
        run_winbind();
    } else {
        run_memfs();
    }
#else /* ifdef SMB2W_WBSTUB */
    run_memfs();
#endif /* ifdef SMB2W_WBSTUB */

    if (failures) {
        fprintf(stderr, "%d LSARPC check(s) FAILED\n", failures);
        return 1;
    }
    printf("all LSARPC checks passed\n");
    return 0;
} /* main */
