// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/* nfs4_compound_test_gate.h -- test-only seam into NFSv4 COMPOUND execution;
 * all zero, and never armed, in production.
 *
 * It lets an in-process test hold one COMPOUND in the window after its
 * SEQUENCE has completed and before its next operation runs, so that the
 * connection, the session or the client can be changed underneath it.  The
 * test sets `armed` and sends the COMPOUND; the next one to complete a
 * SEQUENCE takes the gate (clearing `armed`), is parked on its thread and
 * counted in `parked`, and continues once the test increments `released`.
 *
 * A parked COMPOUND is not cancelled when its connection closes.  An original
 * already inside the server keeps executing when its client goes away, and its
 * reply is dropped, as libevpl does with any reply to a closed connection.
 * Server shutdown releases it unrun. */

#include <stdatomic.h>
#include "nfs.h"

struct chimera_nfs4_compound_test_gate {
    atomic_int  armed;     /* 1: park the next COMPOUND to finish SEQUENCE */
    atomic_int  parked;    /* COMPOUNDs parked by the gate right now      */
    atomic_uint released;  /* incremented by the test: run what is parked */
};

/* CHIMERA_NFS_DATA, not SYMBOL_EXPORT: MSVC users must dllimport data. */
extern CHIMERA_NFS_DATA struct chimera_nfs4_compound_test_gate chimera_nfs4_compound_test_gate;
