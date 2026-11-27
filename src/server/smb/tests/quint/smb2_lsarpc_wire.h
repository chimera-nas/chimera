// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * A hand-built LSARPC (MS-LSAT) client for the SMB probes: the PDUs a client
 * sends down an "lsarpc" named pipe, and the decoding of what comes back.
 *
 * chimera generates its NDR code for the server's end of the pipe only: it
 * decodes requests and encodes replies.  Talking to the server needs the other
 * two directions, so they are written here by hand from
 * src/server/smb/idl/lsarpc.idl, deliberately sharing nothing with the
 * generated code, so that a mistake on one side cannot cancel out the same
 * mistake on the other.  smb2_lsarpc_probe.c's self-test (S0) holds the two
 * together: every request built here must decode through the server's
 * generated pull_in to the values it was built from, consuming all of it, and
 * every reply the generated push_out encodes must parse back here to the
 * values it was encoded from.
 *
 * Encoding (NDR32, DCE C706 chapter 14), exactly as the IDL declares each
 * operation's parameters:
 *   - little-endian; every primitive aligned to its own size, counted from the
 *     first stub byte
 *   - a top-level [in] or [out] pointer is a reference pointer: no referent
 *     id, its pointee follows in place.  An embedded pointer is unique: a
 *     4-byte referent id (0 for NULL), its pointee deferred until after the
 *     structure or array holding it
 *   - a conformant array is its max_count, then every element's fixed part,
 *     then the elements' deferred pointees in element order
 *   - an RPC_SID is max_count (= SubAuthorityCount), Revision,
 *     SubAuthorityCount, the six-byte BIG-endian IdentifierAuthority, then
 *     the sub-authorities
 *   - an lsa_String (rpc_unicode_string) is Length and MaximumLength in bytes
 *     and a referent id; its deferred body is max_count, offset 0 and
 *     actual_count in UTF-16 code units, then the UTF-16LE text with no
 *     terminator.  max_count is MaximumLength / 2 and actual_count is
 *     Length / 2 (MS-DTYP 2.3.10); the client sends Length == MaximumLength,
 *     as the server does
 *   - a [string] wchar_t * has the same body, counted WITH its terminator
 *
 * The decoders are strict on purpose: a reply that is not exactly what the
 * IDL describes -- counts that disagree, a SID whose max_count is not its
 * sub-authority count, a NUL inside a name, a byte left over -- is malformed
 * (-1), never parsed best-effort.  A server encoding bug is the first thing a
 * client meets, so it must not be smoothed over here.
 *
 * Framing: one DCE/RPC fragment per PDU each way (the server neither sends
 * nor accepts more), so a PDU is at most LSAW_PDU_MAX bytes, and a stub buffer
 * handed to an lsaw_stub_* builder must hold LSAW_STUB_MAX bytes.  No auth
 * verifier: the pipe rides an SMB session that is already authenticated.
 */

#pragma once

#include "smb2_mbt_common.h"

#define LSAW_FSCTL_PIPE_TRANSCEIVE 0x0011C017u
#define LSAW_OP_CLOSE              0
#define LSAW_OP_LOOKUPSIDS         15
#define LSAW_OP_OPENPOLICY2        44
#define LSAW_OP_LOOKUPNAMES3       68

/* frag_len is 16 bits, and the client never fragments. */
#define LSAW_PDU_MAX               65535
/* The 16-byte common header plus the 8-byte request or response header. */
#define LSAW_CALL_HDR              24
#define LSAW_STUB_MAX              (LSAW_PDU_MAX - LSAW_CALL_HDR)

#define LSAW_PTYPE_REQUEST         0x00
#define LSAW_PTYPE_RESPONSE        0x02
#define LSAW_PTYPE_FAULT           0x03
#define LSAW_PTYPE_BIND            0x0B
#define LSAW_PTYPE_BIND_ACK        0x0C

/* The values the requests carry where the IDL leaves them to the client:
 * OpenPolicy2's system_name and access_mask (MAXIMUM_ALLOWED), the lookups'
 * level (LsapLookupWksta), and LookupNames3's lookup_options and
 * client_revision (LSA_CLIENT_REVISION_2). */
#define LSAW_SYSTEM_NAME           "\\\\server"
#define LSAW_ACCESS_MASK           0x02000000u
#define LSAW_LOOKUP_LEVEL          1
#define LSAW_LOOKUP_OPTIONS        0
#define LSAW_CLIENT_REVISION       2

struct lsaw_name {                   /* one LookupSids answer        */
    uint32_t type;
    char     name[256];
    char     domain[256];            /* "" when sid_index is invalid */
    char     domain_sid[96];
};

struct lsaw_sid {                    /* one LookupNames3 answer      */
    uint32_t type;
    char     sid[96];                /* "" when unmapped             */
    char     domain[256];
    char     domain_sid[96];
};

/* lsarpc 12345778-1234-abcd-ef00-0123456789ab v0.0 and the NDR32 transfer
 * syntax 8a885d04-1ceb-11c9-9fe8-08002b104860 v2.0, each GUID in its wire
 * (little-endian) form. */
static const uint8_t lsaw_lsarpc_uuid[16] = {
    0x78, 0x57, 0x34, 0x12, 0x34, 0x12, 0xcd, 0xab,
    0xef, 0x00, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab
};
static const uint8_t lsaw_ndr32_uuid[16] = {
    0x04, 0x5d, 0x88, 0x8a, 0xeb, 0x1c, 0xc9, 0x11,
    0x9f, 0xe8, 0x08, 0x00, 0x2b, 0x10, 0x48, 0x60
};

/* ---- DCE/RPC framing ---------------------------------------------------- */

/* The 16-byte common header of a single-fragment PDU. */
static inline void
lsaw_common_hdr(
    uint8_t *pdu,
    uint8_t  ptype,
    int      frag_len,
    uint32_t call_id)
{
    memset(pdu, 0, 16);
    pdu[0] = 5;                       /* rpc_vers */
    pdu[1] = 0;                       /* rpc_vers_minor */
    pdu[2] = ptype;
    pdu[3] = 0x03;                    /* PFC_FIRST_FRAG | PFC_LAST_FRAG */
    pdu[4] = 0x10;                    /* drep: little-endian, ASCII, IEEE */
    p16(pdu, 8, (uint16_t) frag_len);
    p16(pdu, 10, 0);                  /* auth_length */
    p32(pdu, 12, call_id);
} /* lsaw_common_hdr */

/* A bind offering the one LSA presentation context over NDR32.  Returns the
 * PDU length. */
static inline int
lsaw_bind_pdu(
    uint8_t *buf,
    uint32_t call_id)
{
    lsaw_common_hdr(buf, LSAW_PTYPE_BIND, 72, call_id);
    /* The client takes any single fragment, so it offers the largest. */
    p16(buf, 16, LSAW_PDU_MAX);       /* max_xmit_frag */
    p16(buf, 18, LSAW_PDU_MAX);       /* max_recv_frag */
    p32(buf, 20, 0);                  /* assoc_group_id: open a new group */
    buf[24] = 1;                      /* n_context_elem */
    buf[25] = 0;
    p16(buf, 26, 0);
    p16(buf, 28, 0);                  /* p_cont_id */
    buf[30] = 1;                      /* n_transfer_syn */
    buf[31] = 0;
    memcpy(buf + 32, lsaw_lsarpc_uuid, 16);
    p16(buf, 48, 0);                  /* if_version major */
    p16(buf, 50, 0);                  /* if_version minor */
    memcpy(buf + 52, lsaw_ndr32_uuid, 16);
    p32(buf, 68, 2);                  /* NDR32 version 2.0 */
    return 72;
} /* lsaw_bind_pdu */

/* A bind_ack -> 0 when it accepts the bind's one context over NDR32, -1 for
 * anything else: another PDU, a rejection, a result list of the wrong size. */
static inline int
lsaw_parse_bind_ack(
    const uint8_t *pdu,
    int            len)
{
    int off;

    if (!pdu || len < 26 || pdu[0] != 5 || pdu[1] != 0 ||
        pdu[2] != LSAW_PTYPE_BIND_ACK || g16(pdu, 8) != len ||
        g16(pdu, 10) != 0) {
        return -1;
    }

    /* max_xmit_frag, max_recv_frag, assoc_group_id, then the secondary
     * address: a counted string, padded to 4 octets from the PDU start. */
    off = 24 + 2 + g16(pdu, 24);
    off = (off + 3) & ~3;

    if (off + 4 + 24 != len || pdu[off] != 1) {
        return -1;
    }
    off += 4;

    if (g16(pdu, off) != 0 ||                             /* acceptance */
        memcmp(pdu + off + 4, lsaw_ndr32_uuid, 16) != 0 ||
        g32(pdu, off + 20) != 2) {
        return -1;
    }
    return 0;
} /* lsaw_parse_bind_ack */

/* A request PDU carrying `stub` for `opnum` on the bind's context.  Returns
 * the PDU length, or -1 if the stub does not fit one fragment. */
static inline int
lsaw_request_pdu(
    uint8_t       *buf,
    uint32_t       call_id,
    uint16_t       opnum,
    const uint8_t *stub,
    int            stub_len)
{
    if (stub_len < 0 || stub_len > LSAW_STUB_MAX) {
        return -1;
    }
    lsaw_common_hdr(buf, LSAW_PTYPE_REQUEST, LSAW_CALL_HDR + stub_len, call_id);
    p32(buf, 16, (uint32_t) stub_len);  /* alloc_hint */
    p16(buf, 20, 0);                    /* p_cont_id */
    p16(buf, 22, opnum);
    if (stub_len > 0) {
        memcpy(buf + LSAW_CALL_HDR, stub, (size_t) stub_len);
    }
    return LSAW_CALL_HDR + stub_len;
} /* lsaw_request_pdu */

/* Response PDU -> stub.  Returns the stub length, or -1 for a fault PDU
 * (*fault = its status) or a malformed PDU (*fault = 0). */
static inline int
lsaw_response_stub(
    const uint8_t  *pdu,
    int             len,
    const uint8_t **stub,
    uint32_t       *fault)
{
    *stub  = NULL;
    *fault = 0;

    if (!pdu || len < LSAW_CALL_HDR || pdu[0] != 5 || pdu[1] != 0 ||
        pdu[4] != 0x10 || (pdu[3] & 0x03) != 0x03 ||
        g16(pdu, 8) != len || g16(pdu, 10) != 0) {
        return -1;
    }

    switch (pdu[2]) {
        case LSAW_PTYPE_RESPONSE:
            *stub = pdu + LSAW_CALL_HDR;
            return len - LSAW_CALL_HDR;
        case LSAW_PTYPE_FAULT:
            if (len >= LSAW_CALL_HDR + 4) {
                *fault = g32(pdu, LSAW_CALL_HDR);
            }
            return -1;
        default:
            return -1;
    } /* switch */
} /* lsaw_response_stub */

/* ---- UTF-8 <-> UTF-16 --------------------------------------------------- */

/* The next code point of a UTF-8 string, advancing *s; -1 if malformed. */
static inline int32_t
lsaw_utf8_next(const unsigned char **s)
{
    const unsigned char *p = *s;
    int32_t              cp;
    int                  n, i;

    if (p[0] < 0x80) {
        cp = p[0];
        n  = 1;
    } else if ((p[0] & 0xe0) == 0xc0) {
        cp = p[0] & 0x1f;
        n  = 2;
    } else if ((p[0] & 0xf0) == 0xe0) {
        cp = p[0] & 0x0f;
        n  = 3;
    } else if ((p[0] & 0xf8) == 0xf0) {
        cp = p[0] & 0x07;
        n  = 4;
    } else {
        return -1;
    }

    for (i = 1; i < n; i++) {
        if ((p[i] & 0xc0) != 0x80) {   /* also stops at the terminator */
            return -1;
        }
        cp = (cp << 6) | (p[i] & 0x3f);
    }

    if (cp > 0x10ffff) {
        return -1;
    }
    *s = p + n;
    return cp;
} /* lsaw_utf8_next */

/* UTF-16 code units in a UTF-8 string, or -1 if it is malformed. */
static inline int
lsaw_utf16_units(const char *s)
{
    const unsigned char *p = (const unsigned char *) s;
    int32_t              cp;
    int                  n = 0;

    while (*p) {
        cp = lsaw_utf8_next(&p);
        if (cp < 0) {
            return -1;
        }
        n += cp >= 0x10000 ? 2 : 1;
    }
    return n;
} /* lsaw_utf16_units */

/* `units` UTF-16LE code units -> a NUL-terminated UTF-8 string.  Returns its
 * length, or -1 if it does not fit `cap` or contains a NUL code unit (which
 * would silently shorten the C string a caller compares). */
static inline int
lsaw_utf16_to_utf8(
    const uint8_t *src,
    uint32_t       units,
    char          *out,
    size_t         cap)
{
    uint32_t i = 0, cp, lo;
    size_t   n = 0, need;

    while (i < units) {
        cp = g16(src, (int) (2 * i));
        i++;
        if (cp == 0) {
            return -1;
        }
        if (cp >= 0xd800 && cp <= 0xdbff && i < units) {
            lo = g16(src, (int) (2 * i));
            if (lo >= 0xdc00 && lo <= 0xdfff) {
                cp = 0x10000 + ((cp - 0xd800) << 10) + (lo - 0xdc00);
                i++;
            }
        }

        need = cp < 0x80 ? 1 : cp < 0x800 ? 2 : cp < 0x10000 ? 3 : 4;
        if (n + need >= cap) {
            return -1;
        }
        switch (need) {
            case 1:
                out[n++] = (char) cp;
                break;
            case 2:
                out[n++] = (char) (0xc0 | (cp >> 6));
                out[n++] = (char) (0x80 | (cp & 0x3f));
                break;
            case 3:
                out[n++] = (char) (0xe0 | (cp >> 12));
                out[n++] = (char) (0x80 | ((cp >> 6) & 0x3f));
                out[n++] = (char) (0x80 | (cp & 0x3f));
                break;
            default:
                out[n++] = (char) (0xf0 | (cp >> 18));
                out[n++] = (char) (0x80 | ((cp >> 12) & 0x3f));
                out[n++] = (char) (0x80 | ((cp >> 6) & 0x3f));
                out[n++] = (char) (0x80 | (cp & 0x3f));
                break;
        } /* switch */
    }
    out[n] = '\0';
    return (int) n;
} /* lsaw_utf16_to_utf8 */

/* ---- NDR writer --------------------------------------------------------- */

struct lsaw_w {
    uint8_t *b;
    int      off;
    int      err;       /* sticky: the stub outgrew LSAW_STUB_MAX, or bad input */
    uint32_t next_ref;  /* referent-id allocator */
};

static inline void
lsaw_w_init(
    struct lsaw_w *w,
    uint8_t       *b)
{
    w->b        = b;
    w->off      = 0;
    w->err      = 0;
    w->next_ref = 0x00020000;
} /* lsaw_w_init */

static inline int
lsaw_w_room(
    struct lsaw_w *w,
    int            n)
{
    if (w->err || w->off + n > LSAW_STUB_MAX) {
        w->err = 1;
        return 0;
    }
    return 1;
} /* lsaw_w_room */

static inline void
lsaw_w_align(
    struct lsaw_w *w,
    int            n)
{
    while (w->off % n) {
        if (!lsaw_w_room(w, 1)) {
            return;
        }
        w->b[w->off++] = 0;
    }
} /* lsaw_w_align */

static inline void
lsaw_w_u16(
    struct lsaw_w *w,
    uint16_t       v)
{
    lsaw_w_align(w, 2);
    if (!lsaw_w_room(w, 2)) {
        return;
    }
    p16(w->b, w->off, v);
    w->off += 2;
} /* lsaw_w_u16 */

static inline void
lsaw_w_u32(
    struct lsaw_w *w,
    uint32_t       v)
{
    lsaw_w_align(w, 4);
    if (!lsaw_w_room(w, 4)) {
        return;
    }
    p32(w->b, w->off, v);
    w->off += 4;
} /* lsaw_w_u32 */

static inline void
lsaw_w_bytes(
    struct lsaw_w *w,
    const void    *src,
    int            n)
{
    if (!lsaw_w_room(w, n)) {
        return;
    }
    memcpy(w->b + w->off, src, (size_t) n);
    w->off += n;
} /* lsaw_w_bytes */

/* An embedded (unique) pointer's referent id: 0 for NULL, else a fresh one. */
static inline void
lsaw_w_ref(
    struct lsaw_w *w,
    int            present)
{
    lsaw_w_u32(w, present ? w->next_ref : 0);
    if (present) {
        w->next_ref += 4;
    }
} /* lsaw_w_ref */

/* An RPC_SID from its S-R-I-S-S string. */
static inline void
lsaw_w_sid(
    struct lsaw_w *w,
    const char    *sid)
{
    uint8_t bin[8 + 15 * 4];
    int     n = smb2_sid_str_to_bin(sid, bin, (int) sizeof(bin));

    if (n < 0) {
        w->err = 1;
        return;
    }
    lsaw_w_u32(w, bin[1]);      /* max_count: the sub-authority count */
    /* Revision, count, authority, sub-authorities: the MS-DTYP form, whose
     * sub-authorities land 4-aligned behind the 8-byte head. */
    lsaw_w_bytes(w, bin, n);
} /* lsaw_w_sid */

static inline void
lsaw_w_utf16(
    struct lsaw_w *w,
    const char    *s)
{
    const unsigned char *p = (const unsigned char *) s;
    int32_t              cp;

    while (*p) {
        cp = lsaw_utf8_next(&p);
        if (cp < 0) {
            w->err = 1;
            return;
        }
        if (cp >= 0x10000) {
            cp -= 0x10000;
            lsaw_w_u16(w, (uint16_t) (0xd800 + (cp >> 10)));
            lsaw_w_u16(w, (uint16_t) (0xdc00 + (cp & 0x3ff)));
        } else {
            lsaw_w_u16(w, (uint16_t) cp);
        }
    }
} /* lsaw_w_utf16 */

/* An lsa_String's fixed part; a NULL string is a NULL buffer. */
static inline void
lsaw_w_ustr(
    struct lsaw_w *w,
    const char    *s)
{
    int units = s ? lsaw_utf16_units(s) : 0;

    if (units < 0 || units > 0x7fff) {
        w->err = 1;
        return;
    }
    lsaw_w_u16(w, (uint16_t) (units * 2));   /* Length */
    lsaw_w_u16(w, (uint16_t) (units * 2));   /* MaximumLength */
    lsaw_w_ref(w, s != NULL);
} /* lsaw_w_ustr */

/* An lsa_String's deferred body. */
static inline void
lsaw_w_ustr_body(
    struct lsaw_w *w,
    const char    *s)
{
    int units;

    if (!s) {
        return;
    }
    units = lsaw_utf16_units(s);
    if (units < 0) {
        w->err = 1;
        return;
    }
    lsaw_w_u32(w, (uint32_t) units);         /* max_count */
    lsaw_w_u32(w, 0);                        /* offset */
    lsaw_w_u32(w, (uint32_t) units);         /* actual_count */
    lsaw_w_utf16(w, s);
} /* lsaw_w_ustr_body */

/* A [string] wchar_t * body: counted with, and carrying, its terminator. */
static inline void
lsaw_w_wstring(
    struct lsaw_w *w,
    const char    *s)
{
    int units = lsaw_utf16_units(s);

    if (units < 0) {
        w->err = 1;
        return;
    }
    lsaw_w_u32(w, (uint32_t) units + 1);     /* max_count */
    lsaw_w_u32(w, 0);                        /* offset */
    lsaw_w_u32(w, (uint32_t) units + 1);     /* actual_count */
    lsaw_w_utf16(w, s);
    lsaw_w_u16(w, 0);
} /* lsaw_w_wstring */

/* ---- request stubs ------------------------------------------------------ */

/* lsa_OpenPolicy2: [in, unique, string] system_name, [in] access_mask.  The
 * IDL leaves out the LSAPR_OBJECT_ATTRIBUTES a Windows client sends between
 * the two, so this stub does too. */
static inline int
lsaw_stub_open_policy2(uint8_t *stub)
{
    struct lsaw_w w;

    lsaw_w_init(&w, stub);
    lsaw_w_ref(&w, 1);                       /* system_name */
    lsaw_w_wstring(&w, LSAW_SYSTEM_NAME);
    lsaw_w_u32(&w, LSAW_ACCESS_MASK);        /* access_mask */
    return w.err ? -1 : w.off;
} /* lsaw_stub_open_policy2 */

/* lsa_LookupSids.  A NULL entry is a NULL SID pointer. */
static inline int
lsaw_stub_lookup_sids(
    uint8_t           *stub,
    const uint8_t      handle[20],
    const char *const *sids,
    int                n)
{
    struct lsaw_w w;
    int           i;

    if (n < 0 || (n > 0 && !sids)) {
        return -1;
    }

    lsaw_w_init(&w, stub);
    lsaw_w_bytes(&w, handle, 20);            /* [in] policy_handle *handle */

    lsaw_w_u32(&w, (uint32_t) n);            /* [in] lsa_SidArray *sids: num_sids */
    lsaw_w_ref(&w, 1);                       /*   sids, present even when empty */
    lsaw_w_u32(&w, (uint32_t) n);            /*   its max_count */
    for (i = 0; i < n; i++) {
        lsaw_w_ref(&w, sids[i] != NULL);     /*   lsa_SidPtr */
    }
    for (i = 0; i < n; i++) {
        if (sids[i]) {
            lsaw_w_sid(&w, sids[i]);
        }
    }

    lsaw_w_u32(&w, 0);                       /* [in, out] lsa_TransNameArray *names: count */
    lsaw_w_ref(&w, 0);                       /*   names: none */
    lsaw_w_u16(&w, LSAW_LOOKUP_LEVEL);       /* [in] level */
    lsaw_w_u32(&w, 0);                       /* [in, out] count */
    return w.err ? -1 : w.off;
} /* lsaw_stub_lookup_sids */

/* lsa_LookupNames3.  A NULL entry is an lsa_String with a NULL buffer. */
static inline int
lsaw_stub_lookup_names3(
    uint8_t           *stub,
    const uint8_t      handle[20],
    const char *const *names,
    int                n)
{
    struct lsaw_w w;
    int           i;

    if (n < 0 || (n > 0 && !names)) {
        return -1;
    }

    lsaw_w_init(&w, stub);
    lsaw_w_bytes(&w, handle, 20);            /* [in] policy_handle *handle */
    lsaw_w_u32(&w, (uint32_t) n);            /* [in] num_names */

    lsaw_w_u32(&w, (uint32_t) n);            /* [in, size_is(num_names)] names: max_count */
    for (i = 0; i < n; i++) {
        lsaw_w_ustr(&w, names[i]);
    }
    for (i = 0; i < n; i++) {
        lsaw_w_ustr_body(&w, names[i]);
    }

    lsaw_w_u32(&w, 0);                       /* [in, out] lsa_TransSidArrayEx2 *sids: count */
    lsaw_w_ref(&w, 0);                       /*   sids: none */
    lsaw_w_u16(&w, LSAW_LOOKUP_LEVEL);       /* [in] level */
    lsaw_w_u32(&w, 0);                       /* [in, out] count */
    lsaw_w_u32(&w, LSAW_LOOKUP_OPTIONS);     /* [in] lookup_options */
    lsaw_w_u32(&w, LSAW_CLIENT_REVISION);    /* [in] client_revision */
    return w.err ? -1 : w.off;
} /* lsaw_stub_lookup_names3 */

/* lsa_Close: [in, out] policy_handle *handle. */
static inline int
lsaw_stub_close(
    uint8_t      *stub,
    const uint8_t handle[20])
{
    struct lsaw_w w;

    lsaw_w_init(&w, stub);
    lsaw_w_bytes(&w, handle, 20);
    return w.err ? -1 : w.off;
} /* lsaw_stub_close */

/* ---- NDR reader --------------------------------------------------------- */

struct lsaw_r {
    const uint8_t *b;
    int            len;
    int            off;
    int            err;   /* sticky: ran past the end, or a field is malformed */
};

static inline int
lsaw_r_have(
    struct lsaw_r *r,
    int64_t        n)
{
    if (r->err || n < 0 || r->off + n > r->len) {
        r->err = 1;
        return 0;
    }
    return 1;
} /* lsaw_r_have */

static inline void
lsaw_r_align(
    struct lsaw_r *r,
    int            n)
{
    int pad = (n - r->off % n) % n;

    if (lsaw_r_have(r, pad)) {
        r->off += pad;
    }
} /* lsaw_r_align */

static inline uint16_t
lsaw_r_u16(struct lsaw_r *r)
{
    uint16_t v;

    lsaw_r_align(r, 2);
    if (!lsaw_r_have(r, 2)) {
        return 0;
    }
    v       = g16(r->b, r->off);
    r->off += 2;
    return v;
} /* lsaw_r_u16 */

static inline uint32_t
lsaw_r_u32(struct lsaw_r *r)
{
    uint32_t v;

    lsaw_r_align(r, 4);
    if (!lsaw_r_have(r, 4)) {
        return 0;
    }
    v       = g32(r->b, r->off);
    r->off += 4;
    return v;
} /* lsaw_r_u32 */

/* An RPC_SID, as its S-R-I-S-S string. */
static inline void
lsaw_r_sid(
    struct lsaw_r *r,
    char          *out,
    size_t         cap)
{
    uint32_t max = lsaw_r_u32(r);
    int      n;

    if (!lsaw_r_have(r, 8)) {
        return;
    }
    if (max != r->b[r->off + 1]) {
        r->err = 1;
        return;
    }
    n = smb2_sid_to_str(r->b + r->off, (uint32_t) (r->len - r->off), out, cap);
    if (n < 0) {
        r->err = 1;
        return;
    }
    r->off += n;
} /* lsaw_r_sid */

/* An lsa_String's deferred body, held to the Length and MaximumLength its
 * fixed part carried, decoded into `out` as UTF-8. */
static inline void
lsaw_r_ustr_body(
    struct lsaw_r *r,
    uint16_t       length,
    uint16_t       size,
    char          *out,
    size_t         cap)
{
    uint32_t max    = lsaw_r_u32(r);
    uint32_t offset = lsaw_r_u32(r);
    uint32_t actual = lsaw_r_u32(r);

    if (r->err) {
        return;
    }
    if ((length & 1) || (size & 1) || length > size ||
        max != size / 2u || offset != 0 || actual != length / 2u) {
        r->err = 1;
        return;
    }
    if (!lsaw_r_have(r, 2 * (int64_t) actual)) {
        return;
    }
    if (lsaw_utf16_to_utf8(r->b + r->off, actual, out, cap) < 0) {
        r->err = 1;
        return;
    }
    r->off += (int) (2 * actual);
} /* lsaw_r_ustr_body */

/* One referenced domain, as read from an lsa_RefDomainList. */
struct lsaw_domain {
    uint16_t length, size;
    uint32_t name_ref, sid_ref;
    char     name[256];
    char     sid[96];
};

/* [out] lsa_RefDomainList **domains.  *doms is calloc'd (NULL when there are
 * none) and the caller frees it. */
static inline void
lsaw_r_domains(
    struct lsaw_r       *r,
    struct lsaw_domain **doms,
    uint32_t            *ndoms)
{
    struct lsaw_domain *d;
    uint32_t            count, arr_ref, i;

    *doms  = NULL;
    *ndoms = 0;

    if (!lsaw_r_u32(r)) {                    /* the list itself: unique */
        return;
    }
    count   = lsaw_r_u32(r);
    arr_ref = lsaw_r_u32(r);
    (void) lsaw_r_u32(r);                    /* max_size */
    if (r->err) {
        return;
    }
    if (!arr_ref) {
        if (count) {
            r->err = 1;                      /* entries with no array */
        }
        return;
    }
    /* Each entry's fixed part is 12 bytes, which bounds a sane count. */
    if (lsaw_r_u32(r) != count || r->err ||
        count > (uint32_t) (r->len - r->off) / 12) {
        r->err = 1;
        return;
    }

    d = calloc(count ? count : 1, sizeof(*d));
    if (!d) {
        r->err = 1;
        return;
    }
    for (i = 0; i < count; i++) {
        d[i].length   = lsaw_r_u16(r);
        d[i].size     = lsaw_r_u16(r);
        d[i].name_ref = lsaw_r_u32(r);
        d[i].sid_ref  = lsaw_r_u32(r);
        if (!d[i].name_ref && (d[i].length || d[i].size)) {
            r->err = 1;
        }
    }
    for (i = 0; i < count && !r->err; i++) {
        if (d[i].name_ref) {
            lsaw_r_ustr_body(r, d[i].length, d[i].size, d[i].name,
                             sizeof(d[i].name));
        }
        if (d[i].sid_ref) {
            lsaw_r_sid(r, d[i].sid, sizeof(d[i].sid));
        }
    }
    *doms  = d;
    *ndoms = count;
} /* lsaw_r_domains */

/* Copy the domain at `idx` (an entry's sid_index), or "" for an index that
 * names none. */
static inline void
lsaw_domain_of(
    const struct lsaw_domain *doms,
    uint32_t                  ndoms,
    uint32_t                  idx,
    char                     *name,
    size_t                    name_cap,
    char                     *sid,
    size_t                    sid_cap)
{
    if (idx < ndoms) {
        snprintf(name, name_cap, "%s", doms[idx].name);
        snprintf(sid, sid_cap, "%s", doms[idx].sid);
    } else {
        name[0] = '\0';
        sid[0]  = '\0';
    }
} /* lsaw_domain_of */

/* ---- reply parsers ------------------------------------------------------ */

/* lsa_OpenPolicy2's reply: the handle and the status.  0, or -1. */
static inline int
lsaw_parse_open_policy2(
    const uint8_t *stub,
    int            len,
    uint8_t        handle[20],
    uint32_t      *status)
{
    if (!stub || len != 24) {
        return -1;
    }
    memcpy(handle, stub, 20);
    *status = g32(stub, 20);
    return 0;
} /* lsaw_parse_open_policy2 */

/* Return the number of entries parsed, or -1 (also when they outnumber max). */
static inline int
lsaw_parse_lookup_sids(
    const uint8_t    *stub,
    int               len,
    struct lsaw_name *out,
    int               max,
    uint32_t         *status,
    int              *ndomains)
{
    struct lsaw_r       r    = { .b = stub, .len = len };
    struct lsaw_domain *doms = NULL;
    uint32_t            ndoms = 0, count, ref, i;
    uint32_t           *idx = NULL;
    uint16_t           *nlen  = NULL, *nsize = NULL;
    uint32_t           *nref = NULL;
    int                 rc   = -1;

    if (!stub || len < 0) {
        return -1;
    }

    lsaw_r_domains(&r, &doms, &ndoms);

    count = lsaw_r_u32(&r);                  /* lsa_TransNameArray: count */
    ref   = lsaw_r_u32(&r);                  /*   names */
    if (r.err || (!ref && count) || count > (uint32_t) (max > 0 ? max : 0)) {
        goto out;
    }

    if (ref) {
        if (lsaw_r_u32(&r) != count) {       /*   max_count */
            goto out;
        }
        idx   = calloc(count ? count : 1, sizeof(*idx));
        nlen  = calloc(count ? count : 1, sizeof(*nlen));
        nsize = calloc(count ? count : 1, sizeof(*nsize));
        nref  = calloc(count ? count : 1, sizeof(*nref));
        if (!idx || !nlen || !nsize || !nref) {
            goto out;
        }
        for (i = 0; i < count; i++) {        /* lsa_TranslatedName fixed parts */
            memset(&out[i], 0, sizeof(out[i]));
            out[i].type = lsaw_r_u32(&r);
            nlen[i]     = lsaw_r_u16(&r);
            nsize[i]    = lsaw_r_u16(&r);
            nref[i]     = lsaw_r_u32(&r);
            idx[i]      = lsaw_r_u32(&r);
            if (!nref[i] && (nlen[i] || nsize[i])) {
                goto out;
            }
        }
        for (i = 0; i < count && !r.err; i++) {
            if (nref[i]) {
                lsaw_r_ustr_body(&r, nlen[i], nsize[i], out[i].name,
                                 sizeof(out[i].name));
            }
        }
    }

    (void) lsaw_r_u32(&r);                   /* count: entries mapped */
    *status = lsaw_r_u32(&r);
    if (r.err || r.off != len) {
        goto out;
    }

    for (i = 0; i < count; i++) {
        lsaw_domain_of(doms, ndoms, idx[i], out[i].domain, sizeof(out[i].domain),
                       out[i].domain_sid, sizeof(out[i].domain_sid));
    }
    if (ndomains) {
        *ndomains = (int) ndoms;
    }
    rc = (int) count;

 out:
    free(doms);
    free(idx);
    free(nlen);
    free(nsize);
    free(nref);
    return rc;
} /* lsaw_parse_lookup_sids */

/* Return the number of entries parsed, or -1 (also when they outnumber max). */
static inline int
lsaw_parse_lookup_names3(
    const uint8_t   *stub,
    int              len,
    struct lsaw_sid *out,
    int              max,
    uint32_t        *status,
    int             *ndomains)
{
    struct lsaw_r       r    = { .b = stub, .len = len };
    struct lsaw_domain *doms = NULL;
    uint32_t            ndoms = 0, count, ref, i;
    uint32_t           *idx   = NULL, *sref = NULL;
    int                 rc = -1;

    if (!stub || len < 0) {
        return -1;
    }

    lsaw_r_domains(&r, &doms, &ndoms);

    count = lsaw_r_u32(&r);                  /* lsa_TransSidArrayEx2: count */
    ref   = lsaw_r_u32(&r);                  /*   sids */
    if (r.err || (!ref && count) || count > (uint32_t) (max > 0 ? max : 0)) {
        goto out;
    }

    if (ref) {
        if (lsaw_r_u32(&r) != count) {       /*   max_count */
            goto out;
        }
        idx  = calloc(count ? count : 1, sizeof(*idx));
        sref = calloc(count ? count : 1, sizeof(*sref));
        if (!idx || !sref) {
            goto out;
        }
        for (i = 0; i < count; i++) {        /* lsa_TranslatedSidEx2 fixed parts */
            memset(&out[i], 0, sizeof(out[i]));
            out[i].type = lsaw_r_u32(&r);
            sref[i]     = lsaw_r_u32(&r);
            idx[i]      = lsaw_r_u32(&r);
            (void) lsaw_r_u32(&r);           /* flags */
        }
        for (i = 0; i < count && !r.err; i++) {
            if (sref[i]) {
                lsaw_r_sid(&r, out[i].sid, sizeof(out[i].sid));
            }
        }
    }

    (void) lsaw_r_u32(&r);                   /* count: entries mapped */
    *status = lsaw_r_u32(&r);
    if (r.err || r.off != len) {
        goto out;
    }

    for (i = 0; i < count; i++) {
        lsaw_domain_of(doms, ndoms, idx[i], out[i].domain, sizeof(out[i].domain),
                       out[i].domain_sid, sizeof(out[i].domain_sid));
    }
    if (ndomains) {
        *ndomains = (int) ndoms;
    }
    rc = (int) count;

 out:
    free(doms);
    free(idx);
    free(sref);
    return rc;
} /* lsaw_parse_lookup_names3 */
