#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <jansson.h>

#include "server/server.h"
#include "server/server_internal.h"
#include "common/logging.h"

int SigInt = 0;

void
signal_handler(int sig)
{
    SigInt = 1;
} /* signal_handler */

int
main(
    int    argc,
    char **argv)
{
    const char                   *config_path = "/usr/local/etc/chimera.json";
    extern char                  *optarg;
    int                           opt;
    const char                   *share_name;
    const char                   *share_module;
    const char                   *share_path;
    json_t                       *config, *shares, *share, *server_params;
    json_error_t                  error;
    struct chimera_server        *server;
    struct chimera_server_config *server_config;

    chimera_log_init();

    while ((opt = getopt(argc, argv, "c:dv")) != -1) {
        switch (opt) {
            case 'c':
                config_path = optarg;
                break;
            case 'd':
                ChimeraLogLevel = CHIMERA_LOG_DEBUG;
                break;
            case 'v':
                printf("Version: %s\n", CHIMERA_VERSION);
                return 0;
        } /* switch */
    }

    config = json_load_file(config_path, 0, &error);

    if (!config) {
        fprintf(stderr, "Failed to load configuration file: %s\n", error.text);
        return 1;
    }

    signal(SIGINT, signal_handler);

    chimera_server_info("Initializing server...");

    server_params = json_object_get(config, "server");

    server_config = chimera_server_config_init();

    json_t *threads_value = json_object_get(server_params, "threads");
    if (threads_value && json_is_integer(threads_value)) {
        int threads = json_integer_value(threads_value);
        chimera_server_config_set_core_threads(server_config, threads);
    }

    json_t *delegation_threads_value = json_object_get(server_params, "delegation_threads");
    if (delegation_threads_value && json_is_integer(delegation_threads_value)) {
        int delegation_threads = json_integer_value(delegation_threads_value);
        chimera_server_config_set_delegation_threads(server_config, delegation_threads);
    }

    json_t *rdma_value = json_object_get(server_params, "rdma");
    if (rdma_value && json_is_true(rdma_value)) {
        chimera_server_config_set_nfs_rdma(server_config, 1);
    }

    json_t *rdma_hostname_value = json_object_get(server_params, "rdma_hostname");
    if (rdma_hostname_value && json_is_string(rdma_hostname_value)) {
        const char *rdma_hostname_str = json_string_value(rdma_hostname_value);
        chimera_server_config_set_nfs_rdma_hostname(server_config, rdma_hostname_str);
    }

    json_t *rdma_port_value = json_object_get(server_params, "rdma_port");
    if (rdma_port_value && json_is_integer(rdma_port_value)) {
        int rdma_port = json_integer_value(rdma_port_value);
        chimera_server_config_set_nfs_rdma_port(server_config, rdma_port);
    }


    server = chimera_server_init(server_config);

    shares = json_object_get(config, "shares");

    if (shares) {
        json_object_foreach(shares, share_name, share)
        {
            share_module = json_string_value(json_object_get(share, "module"));
            share_path   = json_string_value(json_object_get(share, "path"));

            chimera_server_info("Initializing share %s (%s://%s)...", share_name
                                ,
                                share_module, share_path);
            chimera_server_create_share(server, share_module, share_name,
                                        share_path);
        }
    }

    while (!SigInt) {
        sleep(1);
    }

    chimera_server_info("Shutting down server...");

    chimera_server_destroy(server);

    chimera_server_info("Server shutdown complete.");

    json_decref(config);

    return 0;
} /* main */