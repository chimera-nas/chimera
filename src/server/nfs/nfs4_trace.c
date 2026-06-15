// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NFSv4 span tracing (see nfs4_trace.h).  Runs only for a recording span.
 */

#include <string.h>
#include <stdio.h>

#include "nfs4_trace.h"
#include "common/format.h"

static const char *
nfs4_op_name(int op)
{
    switch (op) {
        case OP_ACCESS:              return "ACCESS";
        case OP_CLOSE:               return "CLOSE";
        case OP_COMMIT:              return "COMMIT";
        case OP_CREATE:              return "CREATE";
        case OP_DELEGPURGE:          return "DELEGPURGE";
        case OP_DELEGRETURN:         return "DELEGRETURN";
        case OP_GETATTR:             return "GETATTR";
        case OP_GETFH:               return "GETFH";
        case OP_LINK:                return "LINK";
        case OP_LOCK:                return "LOCK";
        case OP_LOCKT:               return "LOCKT";
        case OP_LOCKU:               return "LOCKU";
        case OP_LOOKUP:              return "LOOKUP";
        case OP_LOOKUPP:             return "LOOKUPP";
        case OP_NVERIFY:             return "NVERIFY";
        case OP_OPEN:                return "OPEN";
        case OP_OPENATTR:            return "OPENATTR";
        case OP_OPEN_CONFIRM:        return "OPEN_CONFIRM";
        case OP_OPEN_DOWNGRADE:      return "OPEN_DOWNGRADE";
        case OP_PUTFH:               return "PUTFH";
        case OP_PUTPUBFH:            return "PUTPUBFH";
        case OP_PUTROOTFH:           return "PUTROOTFH";
        case OP_READ:                return "READ";
        case OP_READDIR:             return "READDIR";
        case OP_READLINK:            return "READLINK";
        case OP_REMOVE:              return "REMOVE";
        case OP_RENAME:              return "RENAME";
        case OP_RENEW:               return "RENEW";
        case OP_RESTOREFH:           return "RESTOREFH";
        case OP_SAVEFH:              return "SAVEFH";
        case OP_SECINFO:             return "SECINFO";
        case OP_SETATTR:             return "SETATTR";
        case OP_SETCLIENTID:         return "SETCLIENTID";
        case OP_SETCLIENTID_CONFIRM: return "SETCLIENTID_CONFIRM";
        case OP_VERIFY:              return "VERIFY";
        case OP_WRITE:               return "WRITE";
        case OP_EXCHANGE_ID:         return "EXCHANGE_ID";
        case OP_CREATE_SESSION:      return "CREATE_SESSION";
        case OP_DESTROY_SESSION:     return "DESTROY_SESSION";
        case OP_SEQUENCE:            return "SEQUENCE";
        case OP_GETDEVICEINFO:       return "GETDEVICEINFO";
        case OP_LAYOUTGET:           return "LAYOUTGET";
        case OP_LAYOUTCOMMIT:        return "LAYOUTCOMMIT";
        case OP_LAYOUTRETURN:        return "LAYOUTRETURN";
        case OP_DESTROY_CLIENTID:    return "DESTROY_CLIENTID";
        case OP_RECLAIM_COMPLETE:    return "RECLAIM_COMPLETE";
        default:                     return "OP";
    } /* switch */
} /* nfs4_op_name */

void
_nfs4_trace_null(struct nfs_request *req)
{
    otel_span_set_name(&req->otel, "nfs4.null");
} /* _nfs4_trace_null */

void
_nfs4_trace_compound(
    struct nfs_request   *req,
    struct COMPOUND4args *args)
{
    char ops[256];
    int  off = 0;
    int  i;

    otel_span_set_name(&req->otel, "nfs4.COMPOUND");
    otel_span_attr_u64(&req->otel, "nfs4.numops", (uint64_t) args->num_argarray);

    /* Join the op names ("PUTFH,GETATTR,...") and attach the first PUTFH FH. */
    ops[0] = '\0';
    for (i = 0; i < args->num_argarray; i++) {
        int op = args->argarray[i].argop;

        if (off < (int) sizeof(ops) - 1) {
            off += snprintf(ops + off, sizeof(ops) - off, "%s%s",
                            off ? "," : "", nfs4_op_name(op));
        }

        if (op == OP_PUTFH) {
            struct PUTFH4args *pf = &args->argarray[i].opputfh;
            char               hex[2 * 128 + 1];

            if (pf->object.len > 0) {
                format_hex(hex, sizeof(hex), pf->object.data, pf->object.len);
                otel_span_attr_str(&req->otel, "nfs.fh", hex);
            }
        }
    }

    if (off > (int) sizeof(ops) - 1) {
        off = (int) sizeof(ops) - 1;
    }
    otel_span_attr_strn(&req->otel, "nfs4.ops", ops, (size_t) off);
} /* _nfs4_trace_compound */
