// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB2 span tracing, modeled on smb_dump.h.  Called next to
 * smb_dump_compound_request(); the macros short-circuit unless the compound's
 * span is being recorded.  The compound PDU is an aggregate span ("smb2.COMPOUND")
 * listing the command sequence; each request inside it gets its own child span
 * ("smb2.<Command>", via smb_trace_op_begin/op_end) carrying that command's key
 * attributes (create path, read/write offset+length, SMB FileId in hex), and the
 * request's VFS work nests under it.
 */

#pragma once

#include "smb_internal.h"

#if CHIMERA_HAVE_OTEL

void _smb_trace_compound_request(
    struct chimera_smb_compound *compound);
void _smb_trace_op_begin(
    struct chimera_smb_compound *compound,
    struct chimera_smb_request  *request);

#define smb_trace_compound_request(compound) \
        do { if (otel_span_recording(&(compound)->otel)) { \
                 _smb_trace_compound_request(compound); } } while (0)

/* Begin a per-request span (child of the aggregate span); see _smb_trace_op_begin. */
#define smb_trace_op_begin(compound, request) \
        do { if (otel_span_recording(&(compound)->otel)) { \
                 _smb_trace_op_begin(compound, request); } } while (0)

/* End the current per-request span if one is in flight.  Gated on the active
 * flag (not the recording bit) so it balances op_begin exactly and never
 * double-ends the reused span slot. */
#define smb_trace_op_end(compound) \
        do { if ((compound)->op_span_active) { \
                 otel_span_end(&(compound)->op_otel); \
                 (compound)->op_span_active = 0; } } while (0)

#else  /* !CHIMERA_HAVE_OTEL : tracing compiled out -- calls vanish entirely */

#define smb_trace_compound_request(compound)  do { } while (0)
#define smb_trace_op_begin(compound, request) do { } while (0)
#define smb_trace_op_end(compound)            do { } while (0)

#endif /* CHIMERA_HAVE_OTEL */
