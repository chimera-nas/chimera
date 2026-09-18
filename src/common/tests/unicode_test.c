// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#undef NDEBUG
#include <assert.h>
#include <string.h>
#include "server/smb/smb_string.h"

int main(void)
{
    struct chimera_smb_iconv_ctx ctx;
    const char text[] = "file-\xc3\xa9-\xf0\x9f\x98\x80";
    const uint16_t expected[] = {'f', 'i', 'l', 'e', '-', 0xe9, '-', 0xd83d, 0xde00};
    const uint16_t invalid[] = {0xd800};
    uint16_t wide[64];
    char decoded[64];
    int count;
    chimera_smb_iconv_init(&ctx);
    count = chimera_smb_utf8_to_utf16le(&ctx, text, strlen(text), wide, sizeof(wide));
    assert(count == sizeof(expected));
    assert(!memcmp(wide, expected, sizeof(expected)));
    assert(chimera_smb_utf16le_to_utf8(&ctx, wide, count, decoded, sizeof(decoded)) == strlen(text));
    assert(!strcmp(decoded, text));
    assert(chimera_smb_utf16le_to_utf8(&ctx, invalid, sizeof(invalid), decoded, sizeof(decoded)) == -1);
    assert(chimera_smb_utf16le_to_utf8(&ctx, expected, 1, decoded, sizeof(decoded)) == -1);
    assert(chimera_smb_utf8_to_utf16le(&ctx, "\xff", 1, wide, sizeof(wide)) == -1);
    decoded[0] = 'X';
    assert(chimera_smb_utf16le_to_utf8(&ctx, expected, sizeof(expected), decoded, 1) == -1);
    assert(decoded[0] == 'X');
    wide[0] = 0xffff;
    assert(chimera_smb_utf8_to_utf16le(&ctx, text, strlen(text), wide, 0) == -1);
    assert(wide[0] == 0xffff);
    chimera_smb_iconv_destroy(&ctx);
    return 0;
}
