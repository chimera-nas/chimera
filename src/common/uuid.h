// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include "common/platform.h"

/* RFC 9562 version-4 UUID bytes, independent of the host GUID field layout. */
static inline int
chimera_uuid_generate(uint8_t uuid[16])
{
    if (chimera_getrandom(uuid, 16)) {
        return -1;
    }
    uuid[6] = (uuid[6] & 0x0f) | 0x40;
    uuid[8] = (uuid[8] & 0x3f) | 0x80;
    return 0;
} // chimera_uuid_generate
static inline void
chimera_uuid_unparse(
    const uint8_t uuid[16],
    char          output[37])
{
    static const char hex[] = "0123456789abcdef";
    int               pos   = 0;

    for (int i = 0; i < 16; i++) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            output[pos++] = '-';
        }
        output[pos++] = hex[uuid[i] >> 4];
        output[pos++] = hex[uuid[i] & 15];
    }
    output[pos] = 0;
} // chimera_uuid_unparse
