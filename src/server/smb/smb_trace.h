// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB2 span tracing, modeled on smb_dump.h.  Called next to
 * smb_dump_compound_request(); the macro short-circuits unless the compound's
 * span is being recorded.  Names the compound span after its first command
 * ("smb2.<Command>"), lists the command sequence, and attaches per-command
 * attributes (the create path, read/write offset+length, and the SMB FileId in
 * hex).  The resolved VFS file handles surface on the child VFS spans.
 */

#pragma once

#include "smb_internal.h"

void _smb_trace_compound_request(
    struct chimera_smb_compound *compound);

#define smb_trace_compound_request(compound) \
        do { if (otel_span_recording(&(compound)->otel)) { \
                 _smb_trace_compound_request(compound); } } while (0)
