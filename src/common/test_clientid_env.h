// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Tests that start an NFS server stand in for the supervisor: NFSv4 is
 * offered only with a granted client ID namespace (src/server/nfs/
 * clientid_allocator.h), so set CHIMERA_NODE_ID / CHIMERA_INSTANCE /
 * CHIMERA_GENERATION before every server start.  Generation comes from the
 * millisecond clock (1..1048575, wrapping about every 17 minutes) and is
 * forced to increase within the process, so a test that restarts its server
 * gets a fresh one each time, as a real supervisor guarantees.
 * scripts/chimera_clientid_test_env.sh is the shell counterpart.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "common/platform.h"

static inline void
chimera_test_clientid_env(void)
{
    static uint32_t last;
    struct timespec ts;
    uint32_t        gen;
    char            text[16];

    clock_gettime(CLOCK_REALTIME, &ts);
    gen = (uint32_t) ((((uint64_t) ts.tv_sec * 1000ULL) +
                       (uint64_t) ts.tv_nsec / 1000000ULL) % 1048575ULL) + 1;
    if (gen <= last && last < 1048575) {
        gen = last + 1;
    }
    last = gen;
    snprintf(text, sizeof(text), "%u", gen);

#ifdef _WIN32
    _putenv_s("CHIMERA_NODE_ID", "1");
    _putenv_s("CHIMERA_INSTANCE", "0");
    _putenv_s("CHIMERA_GENERATION", text);
#else  /* ifdef _WIN32 */
    setenv("CHIMERA_NODE_ID", "1", 1);
    setenv("CHIMERA_INSTANCE", "0", 1);
    setenv("CHIMERA_GENERATION", text, 1);
#endif /* ifdef _WIN32 */
} /* chimera_test_clientid_env */
