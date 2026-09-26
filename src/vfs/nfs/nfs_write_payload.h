// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdlib.h>
#include "evpl/evpl.h"

/* VFS WRITE borrows caller payloads through completion (and compound retry).
 * Generated XDR marshallers instead move their inputs into the outgoing RPC.
 * Give each send its own references and discard any not moved: slot admission
 * can park an NFS4 request before marshalling and replay it later. */
static inline struct evpl_iovec *
chimera_nfs_write_payload_clone(
    struct evpl_iovec *iov,
    int                niov)
{
    struct evpl_iovec *copy = calloc(niov ? (size_t) niov : 1, sizeof(*copy));

    if (copy) {
        for (int i = 0; i < niov; i++) {
            evpl_iovec_clone(&copy[i], &iov[i]);
        }
    }
    return copy;
} // chimera_nfs_write_payload_clone

static inline void
chimera_nfs_write_payload_discard(
    struct evpl       *evpl,
    struct evpl_iovec *iov,
    int                niov)
{
    for (int i = 0; i < niov; i++) {
        if (iov[i].data) {
            /* GLOBAL clones are borrows too; public release would free their
             * owner's allocation. The internal release drops only our clone. */
            evpl_iovec_release_internal(evpl, &iov[i]);
        }
    }
    free(iov);
} // chimera_nfs_write_payload_discard
