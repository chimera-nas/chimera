// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <poll.h>
#include "libsmb2_test_common.h"

static void
test_echo_callback(
    struct smb2_context *smb2,
    int                  status,
    void                *command_data,
    void                *private_data)
{
    int *echo_status = (int *) private_data;

    *echo_status = status;
} /* test_echo_callback */

int
main(
    int   argc,
    char *argv[])
{
    struct test_env env;
    int             echo_status = -1;
    int             ret;
    struct pollfd   pfd;

    libsmb2_test_init(&env, argv, argc);

    printf("Testing SMB2 Echo (keepalive)...\n");

    // Send an echo request
    ret = smb2_echo_async(env.ctx, test_echo_callback, &echo_status);
    if (ret != 0) {
        fprintf(stderr, "Failed to send echo request: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }

    // Process the echo response
    pfd.fd     = smb2_get_fd(env.ctx);
    pfd.events = smb2_which_events(env.ctx);

    ret = poll(&pfd, 1, 5000);  // 5 second timeout
    if (ret < 0) {
        fprintf(stderr, "Poll failed\n");
        libsmb2_test_fail(&env);
    }
    if (ret == 0) {
        fprintf(stderr, "Echo request timed out\n");
        libsmb2_test_fail(&env);
    }

    if (smb2_service(env.ctx, pfd.revents) < 0) {
        fprintf(stderr, "Failed to process echo response: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }

    // Check echo status
    if (echo_status != 0) {
        fprintf(stderr, "Echo request failed with status: %d\n", echo_status);
        libsmb2_test_fail(&env);
    }

    printf("Echo test passed!\n");

    // Send multiple echo requests to test keepalive functionality
    printf("Testing multiple Echo requests...\n");
    for (int i = 0; i < 3; i++) {
        echo_status = -1;

        ret = smb2_echo_async(env.ctx, test_echo_callback, &echo_status);
        if (ret != 0) {
            fprintf(stderr, "Failed to send echo request %d: %s\n", i + 1, smb2_get_error(env.ctx));
            libsmb2_test_fail(&env);
        }

        pfd.fd     = smb2_get_fd(env.ctx);
        pfd.events = smb2_which_events(env.ctx);

        ret = poll(&pfd, 1, 5000);
        if (ret <= 0) {
            fprintf(stderr, "Echo request %d timed out or failed\n", i + 1);
            libsmb2_test_fail(&env);
        }

        if (smb2_service(env.ctx, pfd.revents) < 0) {
            fprintf(stderr, "Failed to process echo response %d: %s\n", i + 1, smb2_get_error(env.ctx));
            libsmb2_test_fail(&env);
        }

        if (echo_status != 0) {
            fprintf(stderr, "Echo request %d failed with status: %d\n", i + 1, echo_status);
            libsmb2_test_fail(&env);
        }

        printf("Echo request %d succeeded\n", i + 1);
    }

    printf("All echo tests passed!\n");
    libsmb2_test_success(&env);

    return 0;
} /* main */