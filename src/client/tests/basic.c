// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>

#include "client/client.h"
#include "evpl/evpl.h"
#include "prometheus-c.h"
static void
chimera_mkdir_complete(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    int *complete = private_data;

    *complete = 1;
} /* chimera_client_mkdir_complete */

static void
chimera_open_complete(
    struct chimera_client_thread   *client,
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_open_handle **handle = private_data;

    *handle = oh;
} /* chimera_client_open_complete */

int
main(
    int    argc,
    char **argv)
{
    struct chimera_client          *client;
    struct chimera_client_config   *config;
    struct chimera_client_thread   *thread;
    int                             rc;
    struct evpl                    *evpl;
    struct chimera_vfs_open_handle *dir_handle = NULL, *file_handle = NULL;
    int                             complete;
    struct prometheus_metrics      *metrics;

    chimera_log_init();

    metrics = prometheus_metrics_create(NULL, NULL, 0);

    ChimeraLogLevel = CHIMERA_LOG_DEBUG;

    evpl = evpl_create(NULL);

    config = chimera_client_config_init();

    client = chimera_client_init(config, metrics);

    rc = chimera_mount(client, "memfs", "memfs", "/");

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module\n");
        return 1;
    }

    thread = chimera_client_thread_init(evpl, client);

    dir_handle = NULL;

    chimera_open(thread, "/", 1, 0, chimera_open_complete, &dir_handle);

    while (!dir_handle) {
        evpl_continue(evpl);
    }

    chimera_close(thread, dir_handle);

    dir_handle = NULL;

    chimera_open(thread, "/memfs", 6, 0, chimera_open_complete, &dir_handle);

    while (!dir_handle) {
        evpl_continue(evpl);
    }

    complete = 0;

    chimera_mkdir(thread, "/memfs/test", 11, chimera_mkdir_complete, &complete);

    while (!complete) {
        evpl_continue(evpl);
    }

    chimera_close(thread, dir_handle);

    dir_handle = NULL;

    chimera_open(thread, "/memfs/test/newfile", 19, CHIMERA_VFS_OPEN_CREATE,
                 chimera_open_complete, &file_handle);

    while (!file_handle) {
        evpl_continue(evpl);
    }

    chimera_close(thread, file_handle);

    rc = chimera_mount(client, "newshare", "memfs", "/test");

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module\n");
        return 1;
    }

    file_handle = NULL;

    chimera_open(thread, "/newshare/newfile", 17, 0,
                 chimera_open_complete, &file_handle);

    while (!file_handle) {
        evpl_continue(evpl);
    }

    chimera_close(thread, file_handle);

    chimera_client_thread_shutdown(evpl, thread);

    chimera_destroy(client);

    prometheus_metrics_destroy(metrics);

    evpl_destroy(evpl);

    return 0;
} /* main */
