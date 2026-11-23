// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * chimera-clientid: offline, read-only decoder for NFSv4 client IDs.
 *
 *   chimera-clientid decode [--json] 0x102a000069000001 [...]
 *
 * It prints the raw ID and the fields of a known Type/Version, or reports an
 * unknown or invalid encoding.  It does not infer which backend minted an ID,
 * and it knows nothing about live ownership or persisted client state.
 * Client IDs are printed as hex strings in JSON so 64-bit values survive.
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "clientid_format.h"

static void
usage(void)
{
    fprintf(stderr, "usage: chimera-clientid decode [--json] <clientid> [<clientid> ...]\n"
            "       <clientid> is hex (0x...) or decimal\n");
} /* usage */

static int
decode_one(
    const char *arg,
    int         json)
{
    struct clientid_decoded d;
    unsigned long long      v;
    char                   *end;
    char                    text[192];
    int                     rc;

    errno = 0;
    v     = strtoull(arg, &end, 0);
    if (errno || end == arg || *end) {
        fprintf(stderr, "chimera-clientid: \"%s\" is not a 64-bit number\n", arg);
        return 2;
    }

    rc = clientid_decode((uint64_t) v, &d);

    if (!json) {
        clientid_format((uint64_t) v, text, sizeof(text));
        printf("%s\n", text);
        return rc ? 1 : 0;
    }

    printf("{\"clientid\":\"0x%016llx\",\"type\":%u", v, d.type);
    if (rc == ENOTSUP) {
        printf(",\"status\":\"unsupported\"}\n");
        return 1;
    }
    if (rc == EINVAL) {
        printf(",\"version\":%u,\"status\":\"invalid\"}\n", d.version);
        return 1;
    }
    printf(",\"version\":%u,\"status\":\"ok\",\"node_id\":%u,\"instance\":%u,"
           "\"generation\":%u,\"sequence\":%u}\n", d.version,
           d.payload.type1_v0.node_id, d.payload.type1_v0.instance,
           d.payload.type1_v0.generation, d.payload.type1_v0.sequence);
    return 0;
} /* decode_one */

int
main(
    int    argc,
    char **argv)
{
    int i = 2, json = 0, rc = 0, one;

    if (argc < 3 || strcmp(argv[1], "decode") != 0) {
        usage();
        return 2;
    }
    if (strcmp(argv[2], "--json") == 0) {
        json = 1;
        i    = 3;
    }
    if (i >= argc) {
        usage();
        return 2;
    }
    for (; i < argc; i++) {
        one = decode_one(argv[i], json);
        if (one > rc) {
            rc = one;
        }
    }
    return rc;
} /* main */
