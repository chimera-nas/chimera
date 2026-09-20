// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include "common/mbt_watchdog.h"
int main(int argc, char **argv)
{
    (void) argv;
    mbt_watchdog_at("fixture.itf.json", 17, "read");
    mbt_watchdog_arm(1);
    if (argc == 1) {
        mbt_watchdog_disarm();
        Sleep(1100);
        return 0;
    }
    Sleep(10000);
    return 0; /* The expected-failure test fails if the watchdog never fires. */
}
