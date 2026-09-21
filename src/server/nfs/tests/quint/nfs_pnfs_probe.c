// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"

/* Resolve two remote backing roots from an ordinary application thread,
 * before it has a VFS context or an RCU reader registration.  The full pNFS
 * corpus uses this same topology, but startup must work without a corpus. */
int
main(void)
{
    struct mbt_env      env;
    struct mbt_env_opts opts = { 0 };

    opts.pnfs_num_ds = 2;
    mbt_watchdog_arm(60);
    mbt_env_open_opts(&env, &opts);
    mbt_env_stop(&env);
    mbt_watchdog_disarm();
    printf("pNFS startup and shutdown passed\n");
    return 0;
} /* main */
