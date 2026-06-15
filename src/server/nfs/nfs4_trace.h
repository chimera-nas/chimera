// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NFSv4 span tracing, modeled on nfs4_dump.h.  Called next to nfs4_dump_*; the
 * macro short-circuits unless the request's span is being recorded.  A COMPOUND
 * is a single span named "nfs4.COMPOUND" annotated with the op sequence and the
 * first PUTFH file handle (hex); the individual ops surface as the child VFS
 * spans they issue.
 */

#pragma once

#include "nfs_common.h"
#include "nfs4_xdr.h"

void _nfs4_trace_null(
    struct nfs_request *req);
void _nfs4_trace_compound(
    struct nfs_request   *req,
    struct COMPOUND4args *args);

#define nfs4_trace_null(req) \
        do { if (otel_span_recording(&(req)->otel)) { _nfs4_trace_null(req); } } while (0)
#define nfs4_trace_compound(req, args) \
        do { if (otel_span_recording(&(req)->otel)) { _nfs4_trace_compound(req, args); } } while (0)
