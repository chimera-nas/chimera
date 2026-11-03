/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * How the SMB2 test client stops the whole harness.  A header of its own so
 * that every file which may stop it -- smb2_mbt_common.h, and
 * smb2_mbt_wire.h, which wbclient_stub.c includes without the rest of the
 * harness -- gets the same definition.
 */

#pragma once

#include <stdio.h>
#ifdef _WIN32
#include "common/platform.h"
#else // ifdef _WIN32
#include <unistd.h>
#endif // ifdef _WIN32

#include "common/compiler.h"
#include "common/logging.h"

/* Stop the whole harness with `status`.
 *
 * Not exit(): libevpl registers evpl_cleanup with atexit, and evpl_cleanup
 * aborts while any evpl context is alive -- which, wherever a probe or the
 * replayer stops with an env open, the server's and the client's are.  exit()
 * there turns the intended status into a SIGABRT and a core dump.  Leave
 * without running atexit handlers, so the status reaches ctest, as
 * chimera_fatal() does.  Flush first: what was printed, and the server's debug
 * log, whose flusher thread would otherwise lose its last buffer -- often the
 * server's own reason for the drop or refusal that stopped the harness. */
CHIMERA_NORETURN
static inline void
smb2c_exit(int status)
{
    chimera_log_flush();
    fflush(stdout);
    fflush(stderr);
#ifdef _WIN32
    TerminateProcess(GetCurrentProcess(), status);
#endif /* ifdef _WIN32 */
    _exit(status);
} /* smb2c_exit */
