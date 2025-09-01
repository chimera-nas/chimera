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
    put_object(&env, "/mydir1/mydir2/mydir3/mykey2", 4096);
    put_object(&env, "/mydir1/mydir2/mydir4/mykey3", 4096);
    put_object(&env, "/mydir1/mydir2/mydir4/mykey4", 4096);
    put_object(&env, "/mydir1/mydir3/mydir5/mykey5", 4096);
    put_object(&env, "/mydir1/mydir3/mydir5/mykey6", 4096);

    fprintf(stderr, "list /");
    list_object(&env, "/");

    fprintf(stderr, "list /mydir1/");
    list_object(&env, "/mydir1/");


    fprintf(stderr, "list /mydir1/mydir3");
    list_object(&env, "/mydir1/mydir3");

    fprintf(stderr, "list /mydir1/mydir2/mydir4");
    list_object(&env, "/mydir1/mydir2/mydir4");

    fprintf(stderr, "list /mydir1/mydir2/my");
    list_object(&env, "/mydir1/mydir2/my");

    libs3_test_success(&env);

    return 0;
} /* main */