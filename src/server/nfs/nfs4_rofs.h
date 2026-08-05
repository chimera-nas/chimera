// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Per-export read-only policy decision for one NFSv4 operation.
 *
 * The decision is kept here as a pure function of already-extracted inputs,
 * separate from the dispatch-time adapter in nfs4_proc_compound.c that fills
 * them in from the request.  Most of these branches cannot be reached through
 * the chimera posix client -- it never sends a wire OPEN for an existing file,
 * and cross-mount RENAME/LINK fails client-side with EXDEV -- so keeping the
 * logic callable lets the unit test cover them directly.
 */

#include <stdbool.h>
#include <stdint.h>

#include "nfs4_xdr.h"
#include "nfs4_op_matrix.h"

struct nfs4_rofs_input {
    uint32_t op;

    /* The current filehandle's export is configured read-only. */
    bool     export_ro;

    /* The saved filehandle's export is configured read-only.  Only consulted
     * for RENAME and LINK, and only when have_saved_fh is set. */
    bool     saved_export_ro;
    bool     have_saved_fh;

    /* OPEN: share_access carries WRITE, or opentype is OPEN4_CREATE. */
    bool     open_writes;

    /* OPENATTR: createdir is set. */
    bool     openattr_creates;
};

/*
 * Returns NFS4_OK when the op may proceed, or the error to fail it with.
 *
 * Ops flagged NFS4_OP_FLAG_MUTATES always fail on a read-only export.  OPEN and
 * OPENATTR mutate only for some arguments, so they are decided from the flags
 * the caller extracted.  LAYOUTGET is refused with NFS4ERR_LAYOUTUNAVAILABLE
 * rather than NFS4ERR_ROFS: a read-only export hands out no layouts (a RW-iomode
 * layout would grant direct out-of-band write authority, and even an RO-iomode
 * LAYOUTGET creates the backing file on the metadata server), and
 * LAYOUTUNAVAILABLE is the error clients answer by falling back to I/O through
 * the metadata server, so reads keep working.
 *
 * RENAME and LINK also mutate the saved-filehandle side -- removal of the source
 * directory entry for RENAME, the source file's nlink for LINK -- and two exports
 * may share one backing VFS mount, so both sides must be writable.  With no
 * SAVEFH the saved side is not consulted at all; on a writable current export
 * the handler then reports NFS4ERR_NOFILEHANDLE (RFC 7530 section 13.1.2.5).
 *
 * COMMIT is deliberately never gated: it writes no new data, and refusing it
 * would strand unstable WRITE data written before a runtime flip to read-only or
 * through a read-write sibling export of the same mount.
 *
 * Ops outside the matrix range (NFS4_OP_MIN..NFS4_OP_MAX) are not gated: they
 * are not dispatchable, so the caller rejects them as illegal regardless.
 */
static inline nfsstat4
nfs4_rofs_check(const struct nfs4_rofs_input *in)
{
    if (in->op < NFS4_OP_MIN || in->op > NFS4_OP_MAX) {
        return NFS4_OK;
    }

    if (in->export_ro) {
        if (nfs4_op_support[in->op].flags & NFS4_OP_FLAG_MUTATES) {
            return NFS4ERR_ROFS;
        }

        if (in->op == OP_OPEN && in->open_writes) {
            return NFS4ERR_ROFS;
        }

        if (in->op == OP_OPENATTR && in->openattr_creates) {
            return NFS4ERR_ROFS;
        }

        if (in->op == OP_LAYOUTGET) {
            return NFS4ERR_LAYOUTUNAVAILABLE;
        }
    }

    if ((in->op == OP_RENAME || in->op == OP_LINK) &&
        in->have_saved_fh && in->saved_export_ro) {
        return NFS4ERR_ROFS;
    }

    return NFS4_OK;
} /* nfs4_rofs_check */
