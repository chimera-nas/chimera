// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/* nfs3_drc_test_gate.h -- test-only seam into the NFSv3 duplicate-request
 * cache; all zero, and never armed, in production.
 *
 * It lets an in-process test hold one request in the window the cache's
 * in-progress entries exist for: the request has arrived and reserved its
 * identity, and has not executed.  The test sets `armed` and sends the
 * request; the next request the cache reserves takes the gate (clearing
 * `armed`), is parked on its thread and counted in `parked`, and runs once the
 * test increments `released`.  Retransmits of it sent meanwhile meet the
 * entry in progress, which is the point.
 *
 * A parked request is not cancelled when its connection closes.  An original
 * already inside the server keeps executing when its client goes away; its
 * reply is captured for the cache and then dropped, as libevpl does with any
 * reply to a closed connection.  Server shutdown releases it unrun. */

#include <stdatomic.h>
#include "nfs.h"

struct chimera_nfs3_drc_test_gate {
    atomic_int  armed;     /* 1: park the next request the cache reserves */
    atomic_int  parked;    /* requests parked by the gate right now       */
    atomic_uint released;  /* incremented by the test: run what is parked */
};

/* CHIMERA_NFS_DATA, not SYMBOL_EXPORT: MSVC users must dllimport data. */
extern CHIMERA_NFS_DATA struct chimera_nfs3_drc_test_gate chimera_nfs3_drc_test_gate;
