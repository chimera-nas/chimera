// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/misc.h"
#include "smb_internal.h"
#include "smb_string.h"

/* DCE/RPC common (connection-oriented) PDU types (ptype field) */
#define DCE_RPC_PTYPE_REQUEST            0x00     /* normal call request (stub data follows) */
#define DCE_RPC_PTYPE_PING               0x01     /* keepalive/probe (rare over SMB) */
#define DCE_RPC_PTYPE_RESPONSE           0x02     /* normal call response */
#define DCE_RPC_PTYPE_FAULT              0x03     /* call failed; carries nca_s_* status */
#define DCE_RPC_PTYPE_WORKING            0x04     /* server is processing (progress hint) */
#define DCE_RPC_PTYPE_NOCALL             0x05     /* server didn’t match the call (legacy) */
#define DCE_RPC_PTYPE_REJECT             0x06     /* association/call rejected */
#define DCE_RPC_PTYPE_ACK                0x07     /* acknowledge (assoc mgmt) */
#define DCE_RPC_PTYPE_CL_CANCEL          0x08     /* client-initiated cancel */
#define DCE_RPC_PTYPE_FACK               0x09     /* fragment acknowledge */
#define DCE_RPC_PTYPE_CANCEL_ACK         0x0A     /* cancel acknowledged */

#define DCE_RPC_PTYPE_BIND               0x0B     /* presentation context negotiation request */
#define DCE_RPC_PTYPE_BIND_ACK           0x0C     /* presentation context negotiation accept */
#define DCE_RPC_PTYPE_BIND_NAK           0x0D     /* bind refused */
#define DCE_RPC_PTYPE_ALTER_CONTEXT      0x0E     /* add/alter presentation contexts */
#define DCE_RPC_PTYPE_ALTER_CONTEXT_RESP 0x0F     /* response to alter-context */

#define DCE_RPC_PTYPE_AUTH3              0x10     /* 3rd leg of some auth handshakes (e.g., NTLM) */
#define DCE_RPC_PTYPE_SHUTDOWN           0x11     /* server requests association shutdown */
#define DCE_RPC_PTYPE_CO_CANCEL          0x12     /* connection-oriented cancel (server) */
#define DCE_RPC_PTYPE_ORPHANED           0x13     /* server indicates call orphaned */

/* DCE/RPC common header flags (h->flags) */
#define DCE_RPC_FLAG_FIRST_FRAG          0x01
#define DCE_RPC_FLAG_LAST_FRAG           0x02
/* (others exist but FIRST/LAST are the big ones for reassembly) */

/* Data Representation (drep[0]) canonical values you’ll see on Windows */
#define DCE_RPC_DREP_INT_LITTLE          0x10     /* little-endian ints */
#define DCE_RPC_DREP_CHAR_ASCII          0x00
#define DCE_RPC_DREP_FLOAT_IEEE          0x00

/* 16-byte common header present on every DCE/RPC PDU */
typedef struct {
    uint8_t  ver;            // = 5
    uint8_t  ver_minor;      // = 0
    uint8_t  ptype;          // 0x0B=Bind, 0x0C=BindAck, 0x00=Request, 0x02=Response, 0x03=Fault, ...
    uint8_t  flags;          // bit0 FIRST, bit1 LAST; also may carry "no frag" = FIRST|LAST
    uint8_t  drep[4];        // data rep: [0]=0x10 (little-endian ints, ASCII), usually 10 00 00 00
    uint16_t frag_len;       // total bytes of this fragment (header + body [+ auth])
    uint16_t auth_len;       // bytes of auth verifier (optional) at end of fragment
    uint32_t call_id;        // matches request/response/bind on a logical RPC call
} __attribute__((packed)) dce_common_t;

/* Optional auth verifier trailer (present only if auth_len > 0 and usually 8-byte aligned) */
typedef struct {
    uint8_t  auth_type;      // e.g., 0x0A = NTLMSSP, 0x09 = Kerberos, etc.
    uint8_t  auth_level;     // connect/integrity/privacy
    uint8_t  pad_len;        // padding up to 8-byte alignment before this
    uint8_t  reserved;
    uint32_t context_id;     // security context slot
    uint8_t  auth_value[];   // auth_len bytes total after the header
} __attribute__((packed)) dce_auth_t;

/* DCE/RPC Bind PDU body (immediately follows dce_common_t) */
typedef struct {
    uint16_t max_xmit_frag;   // client transmit frag size (e.g., 4280)
    uint16_t max_recv_frag;   // client receive frag size
    uint32_t assoc_group_id;  // 0 or existing assoc group
    uint8_t  num_ctx_items;   // number of presentation context items
    uint8_t  _pad;            // must pad so the next is 2-byte aligned
    uint16_t _pad2;           // must pad so the next is 2-byte aligned
    // then: num_ctx_items * p_cont_elem_t
} __attribute__((packed)) dce_bind_t;

/* Presentation context element: which interface UUID/version and transfer syntaxes (e.g., NDR) */
typedef struct {
    uint8_t  p_cont_id;       // small integer, increments per context
    uint8_t  n_transfer_syn;  // usually 1
    uint16_t reserved;
    uint8_t  if_uuid[16];     // interface UUID (e.g., LSARPC = 12345778-1234-abcd-ef00-0123456789ab)
    uint16_t if_vers_major;   // e.g., 0 or 1
    uint16_t if_vers_minor;   // minor
    // then: n_transfer_syn * p_syntax_id_t
} __attribute__((packed)) p_cont_elem_t;

/* Transfer syntax (e.g., NDR32 UUID 8a885d04-1ceb-11c9-9fe8-08002b104860 v2.0) */
typedef struct {
    uint8_t  ts_uuid[16];
    uint32_t ts_version;      // major<<16 | minor (e.g., 2<<16 | 0)
} __attribute__((packed)) p_syntax_id_t;

/* Interface UUID and version */
typedef struct {
    uint8_t  if_uuid[16];
    uint16_t if_vers_major;
    uint16_t if_vers_minor;
} __attribute__((packed)) dce_if_uuid_t;


static const p_syntax_id_t NDR32_SYNTAX = {
    .ts_uuid    = { 0x04, 0x5d, 0x88, 0x8a, 0xeb, 0x1c, 0xc9, 0x11, 0x9f, 0xe8, 0x08, 0x00, 0x2b, 0x10, 0x48, 0x60 },
    .ts_version = 2,
};

/* DCE/RPC BindAck PDU body */
typedef struct {
    uint16_t max_xmit_frag;   // server's max xmit
    uint16_t max_recv_frag;   // server's max recv
    uint32_t assoc_group_id;  // assigned group id
    /* secondary address: counted ASCII string (port/endpoint), always empty string
     * in SMB so we just return zero bytes plus NULL and a pad byte
     */
    uint16_t sec_addr_len;
    uint16_t sec_addr;

    // followed by:
    // uint32_t num_results;
    // num_results * p_result_t
} __attribute__((packed)) dce_bind_ack_t;

typedef struct {
    uint8_t  num_results;
    uint8_t  _pad;
    uint16_t _pad2;
} __attribute__((packed)) p_result_list_t;

/* One presentation result per context offered in the Bind */
typedef struct {
    uint16_t      result;     // 0 = acceptance, 2 = provider rejection, 3 = negotiation failure
    uint16_t      reason;     // 0 = not specified (on accept) or reason code on failure
    p_syntax_id_t transfer_syntax_accepted; // usually NDR
} __attribute__((packed)) p_result_t;


/* ------- REQUEST PDU body (immediately after dce_co_hdr_t) ------- */
typedef struct {
    uint32_t alloc_hint;   /* total stub bytes expected for this call (may exceed this fragment) */
    uint16_t p_cont_id;    /* presentation context id accepted in Bind/Ack */
    uint16_t opnum;        /* operation number within the interface */

    /* OPTIONAL (present only if header.flags has DCE_CO_FLAG_OBJECT_UUID set): */
    /* uint8_t object_uuid[16]; */

    /* Then: NDR-encoded parameters ("stub data"); may be fragmented. */
    /* Then: optional auth verifier trailer (see dce_auth_t) if hdr.auth_len > 0. */
} __attribute__((packed)) dce_co_request_t;

/* ------- RESPONSE PDU body (immediately after dce_co_hdr_t) ------- */
typedef struct {
    uint32_t alloc_hint;   /* total stub bytes returned (or remaining), advisory */
    uint16_t p_cont_id;    /* echoes request's context id */
    uint8_t  cancel_count; /* usually 0 */
    uint8_t  reserved;     /* 0 */

    /* Then: NDR-encoded return values/out parameters ("stub data"). */
    /* Then: optional auth verifier trailer if hdr.auth_len > 0. */
} __attribute__((packed)) dce_co_response_t;

typedef int (*dce_rpc_handler_t)(
    int                       opnum,
    struct evpl_iovec_cursor *cursor,
    void                     *output,
    void                     *private_data);

static inline int
dce_rpc(
    const dce_if_uuid_t *if_uuid,
    struct evpl_iovec   *input_iov,
    int                  input_niov,
    struct evpl_iovec   *output_iov,
    dce_rpc_handler_t    handler,
    void                *private_data)
{
    struct evpl_iovec_cursor input_cursor;
    dce_common_t             request_common, *reply_common;
    dce_bind_t               request_bind;
    dce_bind_ack_t          *reply_bind_ack;
    dce_co_request_t         request_call;
    dce_co_response_t       *reply_call;
    p_cont_elem_t            request_cont_elem;
    p_result_list_t         *reply_result_list;
    p_result_t              *reply_result;
    uint32_t                 i;
    void                    *outputp = output_iov->data;
    int                      rc;

    evpl_iovec_cursor_init(&input_cursor, input_iov, input_niov);

    rc = evpl_iovec_cursor_get_blob(&input_cursor, &request_common, sizeof(dce_common_t));

    if (unlikely(rc != 0)) {
        chimera_smb_error("failed to get DCE RPC common header");
        return -1;
    }

    if (unlikely(request_common.ver != 5 || request_common.ver_minor != 0)) {
        chimera_smb_error("invalid DCE RPC version");
        return -1;
    }

    if (unlikely(request_common.drep[0] != 0x10)) {
        chimera_smb_error("invalid DCE RPC data representation");
        return -1;
    }

    if (unlikely(request_common.flags != (DCE_RPC_FLAG_FIRST_FRAG | DCE_RPC_FLAG_LAST_FRAG))) {
        chimera_smb_error("invalid DCE RPC flags, only support single fragment");
        return -1;
    }


    reply_common = outputp;
    outputp     += sizeof(dce_common_t);

    reply_common->ver       = 5;
    reply_common->ver_minor = 0;
    reply_common->flags     = DCE_RPC_FLAG_FIRST_FRAG | DCE_RPC_FLAG_LAST_FRAG;
    reply_common->drep[0]   = 0x10;
    reply_common->drep[1]   = 0x00;
    reply_common->drep[2]   = 0x00;
    reply_common->drep[3]   = 0x00;
    reply_common->auth_len  = 0;
    reply_common->call_id   = request_common.call_id;

    switch (request_common.ptype) {
        case DCE_RPC_PTYPE_BIND:

            reply_common->ptype    = DCE_RPC_PTYPE_BIND_ACK;
            reply_common->frag_len = sizeof(*reply_common) + sizeof(*reply_bind_ack) +
                sizeof(*reply_result_list) + sizeof(*reply_result);

            reply_bind_ack = outputp;
            outputp       += sizeof(dce_bind_ack_t);

            reply_bind_ack->max_xmit_frag  = 65535;
            reply_bind_ack->max_recv_frag  = 65535;
            reply_bind_ack->assoc_group_id = 0;
            reply_bind_ack->sec_addr_len   = 0;
            reply_bind_ack->sec_addr       = 0;

            reply_result_list = outputp;
            outputp          += sizeof(p_result_list_t);

            reply_result_list->num_results = 1;
            reply_result_list->_pad        = 0;
            reply_result_list->_pad2       = 0;

            reply_result = outputp;
            outputp     += sizeof(p_result_t);

            reply_result->result                   = 0;
            reply_result->reason                   = 0;
            reply_result->transfer_syntax_accepted = NDR32_SYNTAX;

            rc = evpl_iovec_cursor_get_blob(&input_cursor, &request_bind, sizeof(dce_bind_t));

            if (unlikely(rc != 0)) {
                chimera_smb_error("failed to get DCE RPC bind");
                return -1;
            }

            for (i = 0; i < request_bind.num_ctx_items; i++) {

                rc = evpl_iovec_cursor_get_blob(&input_cursor, &request_cont_elem, sizeof(p_cont_elem_t));

                if (unlikely(rc != 0)) {
                    chimera_smb_error("failed to get DCE RPC cont elem");
                    return -1;
                }

                if (memcmp(request_cont_elem.if_uuid, if_uuid->if_uuid, 16) != 0 ||
                    request_cont_elem.if_vers_major != if_uuid->if_vers_major ||
                    request_cont_elem.if_vers_minor != if_uuid->if_vers_minor) {
                    chimera_smb_error("invalid DCE RPC interface UUID or version V%d.%d",
                                      (int) request_cont_elem.if_vers_major, (int) request_cont_elem.if_vers_minor);
                    reply_result->result = 2;
                }
            }

            break;
        case DCE_RPC_PTYPE_REQUEST:

            rc = evpl_iovec_cursor_get_blob(&input_cursor, &request_call, sizeof(dce_co_request_t));

            if (unlikely(rc != 0)) {
                chimera_smb_error("failed to get DCE RPC request");
                return -1;
            }

            reply_common->ptype    = DCE_RPC_PTYPE_RESPONSE;
            reply_common->frag_len = sizeof(*reply_common) + sizeof(*reply_call);

            reply_call = outputp;
            outputp   += sizeof(dce_co_response_t);

            reply_call->alloc_hint   = 0;
            reply_call->p_cont_id    = request_call.p_cont_id;
            reply_call->cancel_count = 0;
            reply_call->reserved     = 0;

            rc = handler(request_call.opnum, &input_cursor, outputp, private_data);

            if (unlikely(rc < 0)) {
                chimera_smb_error("failed to handle DCE RPC request");
                return -1;
            }

            reply_call->alloc_hint = rc;

            reply_common->frag_len += rc;
            outputp                += rc;

            break;
        default:
            chimera_smb_error("invalid DCE RPC type");
            return -1;
    } // switch

    output_iov->length = outputp - output_iov->data;

    return 0;
} // dce_rpc

static inline int
dce_append_ref_id(
    void    *outputp,
    uint32_t ref_id)
{
    uint32_t *ref_id_ptr = (uint32_t *) outputp;

    *ref_id_ptr = ref_id;
    return sizeof(uint32_t);
} // dce_append_ref_id

static inline int
dce_append_string(
    struct chimera_smb_iconv_ctx *ctx,
    void                         *output,
    uint32_t                      ref_id,
    const char                   *string)
{
    void     *outputp = output;
    int       pad, len_utf8 = strlen(string);
    uint16_t *len, *maxlen;
    uint32_t *refid, *conform_len, *vary_offset, *vary_length;

    len      = (uint16_t *) outputp;
    outputp += sizeof(uint16_t);

    maxlen   = (uint16_t *) outputp;
    outputp += sizeof(uint16_t);

    refid    = (uint32_t *) outputp;
    outputp += sizeof(uint32_t);


    conform_len = (uint32_t *) outputp;
    outputp    += sizeof(uint32_t);

    vary_offset = (uint32_t *) outputp;
    outputp    += sizeof(uint32_t);

    vary_length = (uint32_t *) outputp;
    outputp    += sizeof(uint32_t);

    *len     = chimera_smb_utf8_to_utf16le(ctx, string, len_utf8, outputp, 2 * len_utf8);
    outputp += *len;

    //*(uint8_t *) outputp = 0;
    //outputp++;

    pad = (4 - ((outputp - output) & 0x03)) & 0x03;
    memset(outputp, 0, pad);
    outputp += pad;

    while ((uintptr_t) outputp % 4 != 0) {
        *(uint8_t *) outputp = 0;
        outputp++;
    }

    *maxlen = *len + 2;

    *conform_len = len_utf8 + 1;
    *vary_offset = 0;
    *vary_length = len_utf8;

    *refid = ref_id;

    return outputp - output;
} // dce_append_string


static inline int
dce_append_string_array(
    struct chimera_smb_iconv_ctx *ctx,
    void                         *output,
    uint32_t                      ref_id,
    uint32_t                      ref_id2,
    const char                   *string)
{
    void     *outputp = output;
    int       pad, len_utf8 = strlen(string);
    uint16_t *len, *maxlen;
    uint32_t *refid, *conform_len, *vary_offset, *vary_length;
    uint32_t *conform_array_len;

    len      = (uint16_t *) outputp;
    outputp += sizeof(uint16_t);

    maxlen   = (uint16_t *) outputp;
    outputp += sizeof(uint16_t);

    refid    = (uint32_t *) outputp;
    outputp += sizeof(uint32_t);


    conform_array_len = (uint32_t *) outputp;
    outputp          += sizeof(uint32_t);

    conform_len = (uint32_t *) outputp;
    outputp    += sizeof(uint32_t);

    vary_offset = (uint32_t *) outputp;
    outputp    += sizeof(uint32_t);

    vary_length = (uint32_t *) outputp;
    outputp    += sizeof(uint32_t);

    *len     = chimera_smb_utf8_to_utf16le(ctx, string, len_utf8, outputp, 2 * len_utf8);
    outputp += *len;

    //*(uint8_t *) outputp = 0;
    //outputp++;

    pad = (4 - ((outputp - output) & 0x03)) & 0x03;
    memset(outputp, 0, pad);
    outputp += pad;

    while ((uintptr_t) outputp % 4 != 0) {
        *(uint8_t *) outputp = 0;
        outputp++;
    }

    *maxlen = *len + 2;

    *conform_len       = len_utf8 + 1;
    *conform_array_len = ref_id2;
    *vary_offset       = 0;
    *vary_length       = len_utf8;

    *refid = ref_id;

    return outputp - output;
} // dce_append_string