// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <libs3.h>
#include <pthread.h>

#include "libs3_test_common.h"
#include "libs3_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct test_env env;


    libs3_test_init(&env, argv, argc);

    put_object(&env, "/mydir1/mydir2/mydir3/mykey1", 4096);
    head_object(&env, "/mydir1/mydir2/mydir3/mykey1");

    libs3_test_success(&env);

    return 0;
} /* main */