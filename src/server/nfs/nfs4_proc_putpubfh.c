// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"

/* RFC 7530 §16.21: PUTPUBFH sets the current FH to the server's "public"
 * filehandle. Chimera does not distinguish a separate public namespace from
 * the root namespace, so PUTPUBFH is a synonym for PUTROOTFH (including the
 * "/" export resolution; see nfs4_proc_putrootfh.c). */
void
chimera_nfs4_putpubfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    (void) argop;
    (void) resop;

    chimera_nfs4_putrootfh_common(thread, req);
} /* chimera_nfs4_putpubfh */
