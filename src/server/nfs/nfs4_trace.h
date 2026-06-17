// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NFSv4 span tracing, modeled on nfs4_dump.h.  Called next to nfs4_dump_*; the
 * macro short-circuits unless the request's span is being recorded.  A COMPOUND
 * is an aggregate span named "nfs4.COMPOUND" annotated with the op sequence and
 * the first PUTFH file handle (hex).  Each operation inside the compound gets its
 * own child span ("nfs4.<OP>", via nfs4_trace_op_begin/op_end), and the VFS work
 * an operation issues nests under that operation's span.
 */

#pragma once

#include "nfs_common.h"
#include "nfs4_xdr.h"

void _nfs4_trace_null(
    struct nfs_request *req);
void _nfs4_trace_compound(
    struct nfs_request   *req,
    struct COMPOUND4args *args);
void _nfs4_trace_op_begin(
    struct nfs_request *req,
    struct nfs_argop4  *argop);

#define nfs4_trace_null(req) \
        do { if (otel_span_recording(&(req)->otel)) { _nfs4_trace_null(req); } } while (0)
#define nfs4_trace_compound(req, args) \
        do { if (otel_span_recording(&(req)->otel)) { _nfs4_trace_compound(req, args); } } while (0)

/* Begin a per-operation span (child of the compound span); see _nfs4_trace_op_begin. */
#define nfs4_trace_op_begin(req, argop) \
        do { if (otel_span_recording(&(req)->otel)) { _nfs4_trace_op_begin(req, argop); } } while (0)

/* End the current per-operation span if one is in flight.  Gated on the active
 * flag (not the recording bit) so it balances op_begin exactly and never
 * double-ends the reused span slot. */
#define nfs4_trace_op_end(req) \
        do { if ((req)->op_span_active) { \
                 otel_span_end(&(req)->op_otel); (req)->op_span_active = 0; } } while (0)
