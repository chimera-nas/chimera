// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <jansson.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/evp.h>

#include "evpl/evpl.h"

#include "server/server.h"
#include "vfs/vfs_cred.h"
#include "server/server_internal.h"
#include "common/logging.h"
#include "metrics/metrics.h"
#include "daemon.h"

int SigInt = 0;

void
signal_handler(int sig)
{
    SigInt = 1;
} /* signal_handler */

static int
generate_self_signed_cert(
    const char *cert_path,
    const char *key_path)
{
    EVP_PKEY     *pkey = NULL;
    EVP_PKEY_CTX *pctx = NULL;
    X509         *x509 = NULL;
    X509_NAME    *name = NULL;
    FILE         *fp   = NULL;
    int           rc   = -1;

    chimera_server_info("Generating self-signed certificate...");

    /* Generate RSA key */
    pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
    if (!pctx) {
        chimera_server_error("Failed to create EVP_PKEY_CTX");
        goto cleanup;
    }

    if (EVP_PKEY_keygen_init(pctx) <= 0) {
        chimera_server_error("Failed to init keygen");
        goto cleanup;
    }

    if (EVP_PKEY_CTX_set_rsa_keygen_bits(pctx, 2048) <= 0) {
        chimera_server_error("Failed to set RSA key bits");
        goto cleanup;
    }

    if (EVP_PKEY_keygen(pctx, &pkey) <= 0) {
        chimera_server_error("Failed to generate RSA key");
        goto cleanup;
    }

    /* Create X509 certificate */
    x509 = X509_new();
    if (!x509) {
        chimera_server_error("Failed to create X509");
        goto cleanup;
    }

    /* Set version to X509v3 */
    X509_set_version(x509, 2);

    /* Set serial number */
    ASN1_INTEGER_set(X509_get_serialNumber(x509), 1);

    /* Set validity period (1 year) */
    X509_gmtime_adj(X509_get_notBefore(x509), 0);
    X509_gmtime_adj(X509_get_notAfter(x509), 365 * 24 * 60 * 60);

    /* Set public key */
    X509_set_pubkey(x509, pkey);

    /* Set subject name */
    name = X509_get_subject_name(x509);
    X509_NAME_add_entry_by_txt(name, "C", MBSTRING_ASC,
                               (unsigned char *) "US", -1, -1, 0);
    X509_NAME_add_entry_by_txt(name, "O", MBSTRING_ASC,
                               (unsigned char *) "Chimera NAS", -1, -1, 0);
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                               (unsigned char *) "localhost", -1, -1, 0);

    /* Self-signed: issuer = subject */
    X509_set_issuer_name(x509, name);

    /* Sign the certificate */
    if (!X509_sign(x509, pkey, EVP_sha256())) {
        chimera_server_error("Failed to sign certificate");
        goto cleanup;
    }

    /* Write private key */
    fp = fopen(key_path, "w");
    if (!fp) {
        chimera_server_error("Failed to open key file: %s", key_path);
        goto cleanup;
    }
    PEM_write_PrivateKey(fp, pkey, NULL, NULL, 0, NULL, NULL);
    fclose(fp);
    fp = NULL;

    /* Write certificate */
    fp = fopen(cert_path, "w");
    if (!fp) {
        chimera_server_error("Failed to open cert file: %s", cert_path);
        goto cleanup;
    }
    PEM_write_X509(fp, x509);
    fclose(fp);
    fp = NULL;

    chimera_server_info("Self-signed certificate generated: %s, %s",
                        cert_path, key_path);
    rc = 0;

 cleanup:
    if (fp) {
        fclose(fp);
    }
    if (x509) {
        X509_free(x509);
    }
    if (pkey) {
        EVP_PKEY_free(pkey);
    }
    if (pctx) {
        EVP_PKEY_CTX_free(pctx);
    }
    return rc;
} /* generate_self_signed_cert */

int
main(
    int    argc,
    char **argv)
{
    const char                          *config_path = CONFIG_PATH;
    extern char                         *optarg;
    int                                  opt;
    const char                          *name;
    const char                          *module;
    const char                          *path;
    json_t                              *config, *shares, *share, *server_params, *buckets, *bucket;
    json_t                              *mounts, *mount, *exports, *export;
    json_t                              *json_value;
    int                                  int_value;
    const char                          *str_value;
    json_error_t                         error;
    struct chimera_server               *server;
    struct chimera_server_config        *server_config;
    struct evpl_global_config           *evpl_global_config;
    struct chimera_metrics              *metrics;
    int                                  i;
    struct chimera_server_config_smb_nic smb_nic_info[16];
    const char                          *rest_ssl_cert   = NULL;
    const char                          *rest_ssl_key    = NULL;
    int                                  rest_https_port = 0;
    static char                          auto_cert_path[256];
    static char                          auto_key_path[256];

    chimera_log_init();

#if CHIMERA_SANITIZE != 1
    /* If we are not using address sanitizer, add a crash handler to
     * print stack on signals.   Otherwise, let address sanitizer
     * handle it.
     */
    chimera_enable_crash_handler();
 #endif /* ifndef CHIMERA_SANITIZE */

    evpl_set_log_fn(chimera_vlog, chimera_log_flush);

    /* Parse command line first to get config path */
    while ((opt = getopt(argc, argv, "c:dvh")) != -1) {
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
            case 'h':
            default:
                printf("Usage: chimera-daemon [options]\n"
                       "  -c <config file>   Specify configuration file (default: %s)\n"
                       "  -d                 Enable debug logging\n"
                       "  -v                 Print version information\n"
                       "  -h                 Show this help message\n",
                       CONFIG_PATH);
                return 1;
        } /* switch */
    }

    /* Load config file early to get TLS settings before evpl_init */
    config = json_load_file(config_path, 0, &error);

    if (!config) {
        fprintf(stderr, "Failed to load configuration file: %s\n", error.text);
        return 1;
    }

    /* Check for HTTPS configuration before evpl_init */
    server_params = json_object_get(config, "server");
    if (server_params) {
        json_t *https_port_value = json_object_get(server_params, "rest_https_port");
        if (https_port_value && json_is_integer(https_port_value)) {
            rest_https_port = json_integer_value(https_port_value);
        }

        json_t *ssl_cert_value = json_object_get(server_params, "rest_ssl_cert");
        if (ssl_cert_value && json_is_string(ssl_cert_value)) {
            rest_ssl_cert = json_string_value(ssl_cert_value);
        }

        json_t *ssl_key_value = json_object_get(server_params, "rest_ssl_key");
        if (ssl_key_value && json_is_string(ssl_key_value)) {
            rest_ssl_key = json_string_value(ssl_key_value);
        }
    }

    /* Initialize evpl global config */
    evpl_global_config = evpl_global_config_init();
    evpl_global_config_set_rdmacm_datagram_size_override(evpl_global_config, 8192);
    evpl_global_config_set_buffer_size(evpl_global_config, 8 * 1024 * 1024);
    evpl_global_config_set_spin_ns(evpl_global_config, 1000000UL);
    evpl_global_config_set_huge_pages(evpl_global_config, 1);

    /* Configure TLS if HTTPS is enabled */
    if (rest_https_port != 0) {
        if (rest_ssl_cert && rest_ssl_key) {
            /* Use provided certificate */
            evpl_global_config_set_tls_cert(evpl_global_config, rest_ssl_cert);
            evpl_global_config_set_tls_key(evpl_global_config, rest_ssl_key);
        } else {
            /* Generate self-signed certificate */
            snprintf(auto_cert_path, sizeof(auto_cert_path),
                     "/tmp/chimera-rest-%d.crt", getpid());
            snprintf(auto_key_path, sizeof(auto_key_path),
                     "/tmp/chimera-rest-%d.key", getpid());

            if (generate_self_signed_cert(auto_cert_path, auto_key_path) != 0) {
                fprintf(stderr, "Failed to generate self-signed certificate\n");
                json_decref(config);
                return 1;
            }

            evpl_global_config_set_tls_cert(evpl_global_config, auto_cert_path);
            evpl_global_config_set_tls_key(evpl_global_config, auto_key_path);
            rest_ssl_cert = auto_cert_path;
            rest_ssl_key  = auto_key_path;
        }
    }

    evpl_init(evpl_global_config);

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    chimera_server_info("Initializing server...");

    metrics = chimera_metrics_init(9000);

    server_config = chimera_server_config_init();

    json_value = json_object_get(server_params, "threads");
    if (json_is_integer(json_value)) {
        int_value = json_integer_value(json_value);
        chimera_server_config_set_core_threads(server_config, int_value);
    }

    json_value = json_object_get(server_params, "max_open_files");
    if (json_is_integer(json_value)) {
        int_value = json_integer_value(json_value);
        chimera_server_config_set_max_open_files(server_config, int_value);
    }

    json_value = json_object_get(server_params, "delegation_threads");
    if (json_is_integer(json_value)) {
        int_value = json_integer_value(json_value);
        chimera_server_config_set_delegation_threads(server_config, int_value);
    }

    json_value = json_object_get(server_params, "external_portmap");
    if (json_is_true(json_value)) {
        chimera_server_info("Enabling external portmap/rpcbind support");
        chimera_server_config_set_external_portmap(server_config, 1);
    }

    json_value = json_object_get(server_params, "rdma");
    if (json_is_true(json_value)) {
        chimera_server_config_set_nfs_rdma(server_config, 1);
    }

    json_value = json_object_get(server_params, "rdma_hostname");
    if (json_is_string(json_value)) {
        str_value = json_string_value(json_value);
        chimera_server_config_set_nfs_rdma_hostname(server_config, str_value);
    }

    json_value = json_object_get(server_params, "rdma_port");
    if (json_is_integer(json_value)) {
        int_value = json_integer_value(json_value);
        chimera_server_config_set_nfs_rdma_port(server_config, int_value);
    }

    json_t *rest_http_port_value = json_object_get(server_params, "rest_http_port");
    if (rest_http_port_value && json_is_integer(rest_http_port_value)) {
        int rest_http_port = json_integer_value(rest_http_port_value);
        chimera_server_config_set_rest_http_port(server_config, rest_http_port);
    }

    if (rest_https_port != 0) {
        chimera_server_config_set_rest_https_port(server_config, rest_https_port);
        if (rest_ssl_cert) {
            chimera_server_config_set_rest_ssl_cert(server_config, rest_ssl_cert);
        }
        if (rest_ssl_key) {
            chimera_server_config_set_rest_ssl_key(server_config, rest_ssl_key);
        }
    }

    json_t *smb_multichannel = json_object_get(server_params, "smb_multichannel");
    if (json_is_array(smb_multichannel)) {
        json_t *smb_nic_info_json;
        json_array_foreach(smb_multichannel, i, smb_nic_info_json)
        {
            const char *smb_nic_info_name  = json_string_value(json_object_get(smb_nic_info_json, "address"));
            int         smb_nic_info_speed = json_integer_value(json_object_get(smb_nic_info_json, "speed"));
            int         smb_nic_info_rdma  = json_boolean_value(json_object_get(smb_nic_info_json, "rdma"));

            if (!smb_nic_info_name || !smb_nic_info_speed) {
                chimera_server_error(
                    "SMB Multichannel: Invalid address or speed on SMB multichannel interface");
                return 1;
            }

            strncpy(smb_nic_info[i].address, smb_nic_info_name,
                    sizeof(smb_nic_info[i].address) - 1);
            smb_nic_info[i].speed = smb_nic_info_speed;
            smb_nic_info[i].rdma  = smb_nic_info_rdma;
        }

        chimera_server_config_set_smb_nic_info(server_config, json_array_size(smb_multichannel), smb_nic_info);
    }

    json_t *vfs_modules = json_object_get(server_params, "vfs");
    if (json_is_object(vfs_modules)) {
        const char *module_name;
        json_t     *module_cfg;
        json_object_foreach(vfs_modules, module_name, module_cfg)
        {
            const char *mod_path   = json_string_value(json_object_get(module_cfg, "path"));
            json_t     *config_obj = json_object_get(module_cfg, "config");
            char       *config_str = NULL;

            if (json_is_object(config_obj)) {
                config_str = json_dumps(config_obj, JSON_COMPACT);
            }

            chimera_server_config_add_module(server_config, module_name, mod_path,
                                             config_str ? config_str : "");
            free(config_str);
        }
    }

    server = chimera_server_init(server_config, chimera_metrics_get(metrics));

    json_t *users = json_object_get(config, "users");
    if (users && json_is_array(users)) {
        json_t *user_entry;
        size_t  user_idx;

        json_array_foreach(users, user_idx, user_entry)
        {
            const char *username  = json_string_value(json_object_get(user_entry, "username"));
            const char *password  = json_string_value(json_object_get(user_entry, "password"));
            const char *smbpasswd = json_string_value(json_object_get(user_entry, "smbpasswd"));
            int         uid       = json_integer_value(json_object_get(user_entry, "uid"));
            int         gid       = json_integer_value(json_object_get(user_entry, "gid"));
            uint32_t    user_gids[CHIMERA_VFS_CRED_MAX_GIDS];
            uint32_t    ngids      = 0;
            json_t     *gids_array = json_object_get(user_entry, "gids");

            if (gids_array && json_is_array(gids_array)) {
                json_t *gid_val;
                size_t  gid_idx;
                json_array_foreach(gids_array, gid_idx, gid_val)
                {
                    if (ngids < CHIMERA_VFS_CRED_MAX_GIDS) {
                        user_gids[ngids++] = json_integer_value(gid_val);
                    }
                }
            }

            if (!username) {
                chimera_server_error("User entry missing username, skipping");
                continue;
            }

            chimera_server_info("Adding user %s (uid=%d, gid=%d)", username, uid, gid);
            chimera_server_add_user(server, username,
                                    password ? password : "",
                                    smbpasswd ? smbpasswd : "",
                                    uid, gid, ngids, user_gids, 1);
        }
    }

    json_t *s3_access_keys = json_object_get(config, "s3_access_keys");
    if (s3_access_keys && json_is_array(s3_access_keys)) {
        json_t *key_entry;
        size_t  key_idx;

        json_array_foreach(s3_access_keys, key_idx, key_entry)
        {
            const char *access_key = json_string_value(json_object_get(key_entry, "access_key"));
            const char *secret_key = json_string_value(json_object_get(key_entry, "secret_key"));

            if (!access_key || !secret_key) {
                chimera_server_error("S3 access key entry missing access_key or secret_key, skipping");
                continue;
            }

            chimera_server_info("Adding S3 access key %s", access_key);
            chimera_server_add_s3_cred(server, access_key, secret_key, 1);
        }
    }

    mounts = json_object_get(config, "mounts");

    if (mounts) {
        json_object_foreach(mounts, name, mount)
        {
            module = json_string_value(json_object_get(mount, "module"));
            path   = json_string_value(json_object_get(mount, "path"));

            chimera_server_info("Mounting %s://%s to /%s...",
                                module, path, name);
            chimera_server_mount(server, name, module, path);
        }
    }

    shares = json_object_get(config, "shares");

    if (shares) {
        json_object_foreach(shares, name, share)
        {
            path = json_string_value(json_object_get(share, "path"));
            chimera_server_info("Adding SMB share %s -> %s", name, path);
            chimera_server_create_share(server, name, path);
        }
    }

    exports = json_object_get(config, "exports");

    if (exports) {
        json_object_foreach(exports, name, export)
        {
            path = json_string_value(json_object_get(export, "path"));
            chimera_server_info("Adding NFS export %s -> %s", name, path);
            chimera_server_create_export(server, name, path);
        }
    }

    buckets = json_object_get(config, "buckets");

    if (buckets) {
        json_object_foreach(buckets, name, bucket)
        {
            path = json_string_value(json_object_get(bucket, "path"));
            chimera_server_info("Adding S3 bucket %s -> %s", name, path);
            chimera_server_create_bucket(server, name, path);
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
