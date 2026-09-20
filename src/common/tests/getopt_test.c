// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#define CHIMERA_TEST_NATIVE_GETOPT 1
#include "common/getopt.h"
#include <stdlib.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "failed line %d: %s\n", __LINE__, #x); return 1; } } while (0)
int
main(void)
{
    char               *args[] = { "chimera", "-vd", "-cconfig.json", "--trace=x", "--buffer", "64", "--flag", "--",
                                   "-file" };
    int                 flag = 0, index = -1;
    const struct option options[] = {
        { "trace",  required_argument,  NULL,                  't'                  },
        { "buffer", required_argument,  NULL,                  'b'                  },
        { "flag",   no_argument,        &flag,                 7                    },
        { "tracer", no_argument,        NULL,                  'r'                  },
        { NULL,     0,                  NULL,                  0                    }
    };

    optind = 0;
    opterr = 0;
    CHECK(getopt(9, args, "vdc:") == 'v');
    CHECK(getopt(9, args, "vdc:") == 'd');
    CHECK(getopt(9, args, "vdc:") == 'c');
    CHECK(!strcmp(optarg, "config.json"));
    CHECK(getopt_long(9, args, "", options, &index) == 't');
    CHECK(index == 0 && !strcmp(optarg, "x"));
    CHECK(getopt_long(9, args, "", options, NULL) == 'b');
    CHECK(!strcmp(optarg, "64"));
    CHECK(getopt_long(9, args, "", options, NULL) == 0 && flag == 7);
    CHECK(getopt_long(9, args, "", options, NULL) == -1 && optind == 8);
    {
        char *bad[] = { "chimera", "--tra", "--flag=x", "-z", "-c" };
        optind = 0;
        CHECK(getopt_long(5, bad, ":c:", options, NULL) == '?');
        CHECK(getopt_long(5, bad, ":c:", options, NULL) == '?');
        CHECK(getopt_long(5, bad, ":c:", options, NULL) == '?');
        CHECK(optopt == 'z');
        CHECK(getopt_long(5, bad, ":c:", options, NULL) == ':');
        CHECK(optopt == 'c');
    }
    return 0;
} /* main */
