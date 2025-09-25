// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <jansson.h>

#include "evpl/evpl.h"

#include "server/server.h"
#include "server/server_internal.h"
#include "common/logging.h"
#include "metrics/metrics.h"
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
    const char                          *config_path = "/usr/local/etc/chimera.json";
    extern char                         *optarg;
    int                                  opt;
    const char                          *mount_name;
    const char                          *mount_module;
    const char                          *mount_path;
    const char                          *share_name;
    const char                          *share_path;
    const char                          *bucket_name;
    const char                          *bucket_path;
    json_t                              *config, *shares, *share, *server_params, *buckets, *bucket;
    json_t                              *mounts, *mount;
    json_error_t                         error;
    struct chimera_server               *server;
    struct chimera_server_config        *server_config;
    struct evpl_global_config           *evpl_global_config;
    struct chimera_metrics              *metrics;
    int                                  i;
    struct chimera_server_config_smb_nic smb_nic_info[16];

    chimera_log_init();

#if CHIMERA_SANITIZE != 1
    /* If we are not using address sanitizer, add a crash handler to
     * print stack on signals.   Otherwise, let address sanitizer
     * handle it.
     */
    chimera_enable_crash_handler();
 #endif /* ifndef CHIMERA_SANITIZE */

    evpl_set_log_fn(chimera_vlog, chimera_log_flush);

    evpl_global_config = evpl_global_config_init();
    evpl_global_config_set_rdmacm_datagram_size_override(evpl_global_config, 4096);
    evpl_global_config_set_spin_ns(evpl_global_config, 1000000UL);
    evpl_global_config_set_huge_pages(evpl_global_config, 1);

    evpl_init(evpl_global_config);


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

    metrics = chimera_metrics_init(9000);

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

    json_t *smb_multichannel = json_object_get(server_params, "smb_multichannel");
    if (smb_multichannel && json_is_array(smb_multichannel)) {
        json_t *smb_nic_info_json;
        json_array_foreach(smb_multichannel, i, smb_nic_info_json)
        {
            const char *smb_nic_info_name  = json_string_value(json_object_get(smb_nic_info_json, "address"));
            int         smb_nic_info_speed = json_integer_value(json_object_get(smb_nic_info_json, "speed"));

            if (!smb_nic_info_name || !smb_nic_info_speed) {
                chimera_server_error(
                    "SMB Multichannel: Invalid address or speed on SMB multichannel interface");
                return 1;
            }

            strncpy(smb_nic_info[i].address, smb_nic_info_name,
                    sizeof(smb_nic_info[i].address) - 1);
            smb_nic_info[i].speed = smb_nic_info_speed;
        }

        chimera_server_config_set_smb_nic_info(server_config, json_array_size(smb_multichannel), smb_nic_info);
    }

    json_t *vfs_modules = json_object_get(server_params, "vfs");
    if (vfs_modules && json_is_object(vfs_modules)) {
        const char *module_name;
        json_t     *module;
        json_object_foreach(vfs_modules, module_name, module)
        {
            const char *path   = json_string_value(json_object_get(module, "path"));
            const char *config = json_string_value(json_object_get(module, "config"));

            if (path) {
                chimera_server_config_add_module(server_config, module_name, path,
                                                 config ? config : "");
            }
        }
    }

    server = chimera_server_init(server_config, chimera_metrics_get(metrics));

    mounts = json_object_get(config, "mounts");

    if (mounts) {
        json_object_foreach(mounts, mount_name, mount)
        {
            mount_module = json_string_value(json_object_get(mount, "module"));
            mount_path   = json_string_value(json_object_get(mount, "path"));

            chimera_server_info("Mounting %s://%s to /%s..."
                                , mount_module, mount_path, mount_name);
            chimera_server_mount(server, mount_name, mount_module, mount_path);
        }
    }

    shares = json_object_get(config, "shares");

    if (shares) {
        json_object_foreach(shares, share_name, share)
        {
            share_path = json_string_value(json_object_get(share, "path"));

            chimera_server_create_share(server, share_name, share_path);
        }
    }

    buckets = json_object_get(config, "buckets");

    if (buckets) {
        json_object_foreach(buckets, bucket_name, bucket)
        {
            bucket_path = json_string_value(json_object_get(bucket, "path"));

            chimera_server_create_bucket(server, bucket_name, bucket_path);
        }
    }

    chimera_server_start(server);

    while (!SigInt) {
        sleep(1);
    }

    chimera_server_info("Shutting down server...");

    chimera_server_destroy(server);

    chimera_metrics_destroy(metrics);

    chimera_server_info("Server shutdown complete.");

    json_decref(config);

    return 0;
} /* main */