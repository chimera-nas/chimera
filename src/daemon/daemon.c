// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <unistd.h>
#include <jansson.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/evp.h>

#include "evpl/evpl.h"

#include "server/server.h"
#include "vfs/vfs_cred.h"
#include "vfs/vfs_pnfs.h"
#include "server/server_internal.h"
#include "common/logging.h"
#include "common/common_config.h"
#include "metrics/metrics.h"
#include "daemon.h"

int SigInt = 0;

void
signal_handler(int sig)
{
    SigInt = sig;
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

/*
 * Translate a human-friendly pNFS data-server address into the RFC 5665
 * universal address the flex-files layout carries on the wire.  Accepts a bare
 * host ("10.0.0.2") or host:port ("10.0.0.2:2050"); when the port is omitted it
 * defaults by transport -- 20049 for "rdma"/"rdma6", else 2049 (the IANA nfs /
 * nfsrdma ports).  The wire form appends the port as two octets,
 * "h.h.h.h.p_hi.p_lo" (port == p_hi*256 + p_lo).  Returns 0 on success.
 */
static int
chimera_pnfs_uaddr_from_human(
    const char *netid,
    const char *human,
    char       *out,
    size_t      out_size)
{
    char        host[48];   /* bounds the universal address into out[64] */
    const char *colon = strchr(human, ':');
    long        port;

    if (colon) {
        size_t hlen = (size_t) (colon - human);
        if (hlen == 0 || hlen >= sizeof(host)) {
            return -1;
        }
        memcpy(host, human, hlen);
        host[hlen] = '\0';
        port       = strtol(colon + 1, NULL, 10);
    } else {
        if (snprintf(host, sizeof(host), "%s", human) >= (int) sizeof(host)) {
            return -1;
        }
        port = (netid && (strcmp(netid, "rdma") == 0 || strcmp(netid, "rdma6") == 0))
               ? 20049 : 2049;
    }

    if (port <= 0 || port > 65535) {
        return -1;
    }

    snprintf(out, out_size, "%s.%u.%u", host,
             (unsigned) ((port >> 8) & 0xff), (unsigned) (port & 0xff));
    return 0;
} /* chimera_pnfs_uaddr_from_human */

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

    /* first we need to clear the umask to make sure file are created with porper mode.*/
    umask(0);
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
    evpl_global_config_set_libaio_max_pending(evpl_global_config, 1024);

    /* XLIO enable/disable is derived from the common tcp_flavor and applied
     * by chimera_apply_common_config() below, before evpl_init(). */

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

    /* Apply the shared "common" config section (huge pages / slab size) parsed
     * from the same file, last, so it overrides the hardcoded defaults above. */
    chimera_apply_common_config(config, evpl_global_config);

    evpl_init(evpl_global_config);

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    chimera_server_info("Initializing server...");

    /* Metrics port (default 9000); configurable so multiple daemons can share
     * a host (e.g. a pNFS MDS + data server). */
    int metrics_port = 9000;
    if (server_params) {
        json_t *mp = json_object_get(server_params, "metrics_port");
        if (json_is_integer(mp)) {
            metrics_port = json_integer_value(mp);
        }
    }

    metrics = chimera_metrics_init(metrics_port);

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

    json_value = json_object_get(server_params, "sync_delegation");
    if (json_is_boolean(json_value)) {
        chimera_server_config_set_sync_delegation(server_config, json_is_true(json_value));
    }

    json_value = json_object_get(server_params, "sync_delegation_threads");
    if (json_is_integer(json_value)) {
        int_value = json_integer_value(json_value);
        chimera_server_config_set_sync_delegation_threads(server_config, int_value);
    }

    json_value = json_object_get(server_params, "async_delegation");
    if (json_is_boolean(json_value)) {
        chimera_server_config_set_async_delegation(server_config, json_is_true(json_value));
    }

    json_value = json_object_get(server_params, "async_delegation_threads");
    if (json_is_integer(json_value)) {
        int_value = json_integer_value(json_value);
        chimera_server_config_set_async_delegation_threads(server_config, int_value);
    }

    /* The delegation pools are VFS-level; their canonical home is the shared
     * "common" section.  Apply it after the legacy "server" keys above so it
     * takes precedence. */
    {
        struct chimera_common_delegation deleg;

        chimera_common_delegation_config(config, &deleg);
        if (deleg.sync_delegation >= 0) {
            chimera_server_config_set_sync_delegation(server_config, deleg.sync_delegation);
        }
        if (deleg.sync_delegation_threads >= 0) {
            chimera_server_config_set_sync_delegation_threads(server_config, deleg.sync_delegation_threads);
        }
        if (deleg.async_delegation >= 0) {
            chimera_server_config_set_async_delegation(server_config, deleg.async_delegation);
        }
        if (deleg.async_delegation_threads >= 0) {
            chimera_server_config_set_async_delegation_threads(server_config, deleg.async_delegation_threads);
        }
    }

    /* RCU reclaim worker count is also a VFS-level setting shared with the
     * client; its canonical home is the "common" section. */
    {
        int rcu_threads = chimera_common_rcu_reclaim_threads(config);

        if (rcu_threads >= 0) {
            chimera_server_config_set_rcu_reclaim_threads(server_config, rcu_threads);
        }
    }

    json_value = json_object_get(server_params, "smb_persistent_handles");
    if (json_is_boolean(json_value)) {
        chimera_server_config_set_smb_persistent_handles(server_config, json_is_true(json_value));
    }

    json_value = json_object_get(server_params, "smb_directory_leases");
    if (json_is_boolean(json_value)) {
        chimera_server_config_set_smb_directory_leases(server_config, json_is_true(json_value));
    }

    json_value = json_object_get(server_params, "smb_named_streams");
    if (json_is_boolean(json_value)) {
        chimera_server_config_set_smb_named_streams(server_config, json_is_true(json_value));
    }

    json_value = json_object_get(server_params, "smb_leases");
    if (json_is_boolean(json_value)) {
        chimera_server_config_set_smb_leases(server_config, json_is_true(json_value));
    }

    json_value = json_object_get(server_params, "smb_oplocks");
    if (json_is_boolean(json_value)) {
        chimera_server_config_set_smb_oplocks(server_config, json_is_true(json_value));
    }

    /* smb_min_dialect: lowest SMB2 dialect to advertise ("2.0.2"|"2.1"|"3.0"|
     * "3.0.2"|"3.1.1").  Defaults to 2.1; lower it to 2.0.2 only where a client
     * (e.g. a conformance suite) explicitly needs the original SMB2 dialect. */
    json_value = json_object_get(server_params, "smb_min_dialect");
    if (json_is_string(json_value)) {
        const char *d = json_string_value(json_value);
        uint32_t    min_dialect;

        if (strcmp(d, "2.0.2") == 0) {
            min_dialect = 0x0202;
        } else if (strcmp(d, "2.1") == 0) {
            min_dialect = 0x0210;
        } else if (strcmp(d, "3.0") == 0) {
            min_dialect = 0x0300;
        } else if (strcmp(d, "3.0.2") == 0) {
            min_dialect = 0x0302;
        } else if (strcmp(d, "3.1.1") == 0) {
            min_dialect = 0x0311;
        } else {
            chimera_server_error("Invalid smb_min_dialect value '%s' "
                                 "(expected 2.0.2/2.1/3.0/3.0.2/3.1.1)", d);
            return 1;
        }
        chimera_server_config_set_smb_min_dialect(server_config, min_dialect);
    }

    /* smb_encryption: "off"|"enabled"|"required" (or a boolean/integer 0/1/2). */
    json_value = json_object_get(server_params, "smb_encryption");
    if (json_is_string(json_value)) {
        const char *enc  = json_string_value(json_value);
        int         mode = 0;
        if (strcmp(enc, "required") == 0) {
            mode = 2;
        } else if (strcmp(enc, "enabled") == 0 || strcmp(enc, "on") == 0) {
            mode = 1;
        } else if (strcmp(enc, "off") == 0 || strcmp(enc, "disabled") == 0) {
            mode = 0;
        } else {
            chimera_server_error("Invalid smb_encryption value '%s' (expected off/enabled/required)", enc);
        }
        chimera_server_config_set_smb_encryption(server_config, mode);
    } else if (json_is_integer(json_value)) {
        chimera_server_config_set_smb_encryption(server_config, (int) json_integer_value(json_value));
    } else if (json_is_boolean(json_value)) {
        chimera_server_config_set_smb_encryption(server_config, json_is_true(json_value) ? 1 : 0);
    }

    /* smb_compression: boolean (or integer 0/1) enabling SMB3 transport
     * compression negotiation. */
    json_value = json_object_get(server_params, "smb_compression");
    if (json_is_integer(json_value)) {
        chimera_server_config_set_smb_compression(server_config, (int) json_integer_value(json_value));
    } else if (json_is_boolean(json_value)) {
        chimera_server_config_set_smb_compression(server_config, json_is_true(json_value) ? 1 : 0);
    }

    json_value = json_object_get(server_params, "smb_acl_inherited_canonicalize");
    if (json_is_boolean(json_value)) {
        chimera_server_config_set_smb_acl_inherited_canonicalize(server_config, json_is_true(json_value));
    }

    json_value = json_object_get(server_params, "smb2_max_async_credits");
    if (json_is_integer(json_value)) {
        chimera_server_config_set_smb2_max_async_credits(server_config, json_integer_value(json_value));
    }

    json_value = json_object_get(server_params, "nfs4_session_slots");
    if (json_is_integer(json_value)) {
        int_value = json_integer_value(json_value);
        chimera_server_config_set_nfs4_session_slots(server_config, int_value);
    }

    json_value = json_object_get(server_params, "nfs4_delegations");
    if (json_is_boolean(json_value)) {
        chimera_server_config_set_nfs4_delegations(server_config, json_is_true(json_value));
    }

    json_value = json_object_get(server_params, "nfs4_drc");
    if (json_is_boolean(json_value)) {
        chimera_server_config_set_nfs4_drc(server_config, json_is_true(json_value));
    }

    json_value = json_object_get(server_params, "nfs3_drc");
    if (json_is_boolean(json_value)) {
        chimera_server_config_set_nfs3_drc(server_config, json_is_true(json_value));
    }

    json_value = json_object_get(server_params, "nfs4_lease_time");
    if (json_is_integer(json_value)) {
        int_value = json_integer_value(json_value);
        if (int_value > 0) {
            chimera_server_config_set_nfs4_lease_time(server_config, (uint32_t) int_value);
        }
    }

    json_value = json_object_get(server_params, "nfs4_grace_time");
    if (json_is_integer(json_value)) {
        int_value = json_integer_value(json_value);
        if (int_value > 0) {
            chimera_server_config_set_nfs4_grace_time(server_config, (uint32_t) int_value);
        }
    }

    json_value = json_object_get(server_params, "nfs4_courtesy_time");
    if (json_is_integer(json_value)) {
        int_value = json_integer_value(json_value);
        if (int_value > 0) {
            chimera_server_config_set_nfs4_courtesy_time(server_config, (uint32_t) int_value);
        }
    }

    json_value = json_object_get(server_params, "external_portmap");
    if (json_is_true(json_value)) {
        chimera_server_info("Enabling external portmap/rpcbind support");
        chimera_server_config_set_external_portmap(server_config, 1);
    }

    json_value = json_object_get(server_params, "portmap_hostname");
    if (json_is_string(json_value)) {
        str_value = json_string_value(json_value);
        chimera_server_info("Setting portmap hostname to %s", str_value);
        chimera_server_config_set_portmap_hostname(server_config, str_value);
    }

    json_value = json_object_get(server_params, "kv_module");
    if (json_is_string(json_value)) {
        str_value = json_string_value(json_value);
        chimera_server_config_set_kv_module(server_config, str_value);
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

    json_value = json_object_get(server_params, "lockmgr_port");
    if (json_is_integer(json_value)) {
        int_value = json_integer_value(json_value);
        chimera_server_config_set_nfs_lockmgr_port(server_config, int_value);
    }

    json_value = json_object_get(server_params, "nfs_port");
    if (json_is_integer(json_value)) {
        int_value = json_integer_value(json_value);
        chimera_server_config_set_nfs_port(server_config, int_value);
    }

    json_value = json_object_get(server_params, "s3_port");
    if (json_is_integer(json_value)) {
        int_value = json_integer_value(json_value);
        chimera_server_config_set_s3_port(server_config, int_value);
    }

    /* NFSv4.1 server identity (EXCHANGE_ID server scope).  Set a distinct value
     * on independent servers that do not share state -- e.g. a pNFS data server
     * co-deployed with its MDS -- so v4.1 clients do not coalesce them. */
    json_value = json_object_get(server_params, "nfs_server_scope");
    if (json_is_integer(json_value)) {
        chimera_server_config_set_nfs_server_scope(server_config,
                                                   (uint64_t) json_integer_value(json_value));
    }

    /* Data-server mode: bind only the NFSv4 service (no portmap/mount/NLM) so
     * a pNFS data server can run alongside an MDS on the same host. */
    json_value = json_object_get(server_params, "data_server");
    if (json_is_true(json_value)) {
        chimera_server_config_set_nfs_data_server(server_config, 1);
    }

    /* pNFS layout configuration (disabled unless "enabled": true). */
    json_t *pnfs = json_object_get(server_params, "pnfs");
    if (pnfs && json_is_object(pnfs)) {
        json_t *pnfs_enabled = json_object_get(pnfs, "enabled");
        if (json_is_true(pnfs_enabled)) {
            chimera_server_config_set_pnfs_enabled(server_config, 1);
        }

        json_t *data_servers = json_object_get(pnfs, "data_servers");
        if (json_is_array(data_servers)) {
            size_t  ds_i;
            json_t *ds_entry;
            json_array_foreach(data_servers, ds_i, ds_entry)
            {
                const char *tcp_str     = json_string_value(json_object_get(ds_entry, "tcp"));
                const char *rdma_str    = json_string_value(json_object_get(ds_entry, "rdma"));
                const char *backing_str = json_string_value(json_object_get(ds_entry, "backing_path"));
                json_t     *version_val = json_object_get(ds_entry, "version");
                char        wire_tcp[64];
                char        wire_rdma[64];
                char       *wire_rdma_p = NULL;

                /* "tcp" (required): the DS's TCP address handed to clients,
                 * given human-friendly as "host" or "host:port" (port defaults
                 * to 2049) and advertised as the "tcp" netaddr.
                 * "rdma" (optional): an additional RDMA address (port defaults
                 * to 20049) advertised as a preferred "rdma" netaddr alongside
                 * tcp, so RDMA-capable clients use RDMA and others fall back to
                 * tcp.  backing_path: the chimera path where this DS export is
                 * mounted via the nfs module, under which the MDS creates
                 * backing files. */
                if (!tcp_str || !backing_str) {
                    chimera_server_error(
                        "pNFS data_server[%zu] requires \"tcp\" and \"backing_path\"; skipping",
                        ds_i);
                    continue;
                }

                /* "version": which NFS version the client uses to reach the DS,
                 * advertised in ff_device_addr4.ffda_versions.  Accepts an
                 * integer (3 / 4) or a string ("3", "4.0", "4.1").  NFSv4
                 * defaults to minor version 1 (pNFS).  Default: NFSv3. */
                int ds_version = 3, ds_minor = 0;
                if (json_is_string(version_val)) {
                    const char *vs  = json_string_value(version_val);
                    const char *dot = strchr(vs, '.');
                    ds_version = atoi(vs);
                    ds_minor   = dot ? atoi(dot + 1) : (ds_version >= 4 ? 1 : 0);
                } else if (json_is_integer(version_val)) {
                    ds_version = (int) json_integer_value(version_val);
                    ds_minor   = ds_version >= 4 ? 1 : 0;
                }

                if (ds_version != 3 && ds_version != 4) {
                    chimera_server_error(
                        "pNFS data_server[%zu] version %d unsupported (use 3 or 4); skipping",
                        ds_i, ds_version);
                    continue;
                }

                if (chimera_pnfs_uaddr_from_human("tcp", tcp_str,
                                                  wire_tcp, sizeof(wire_tcp)) != 0) {
                    chimera_server_error(
                        "pNFS data_server[%zu] invalid tcp address \"%s\"; skipping",
                        ds_i, tcp_str);
                    continue;
                }

                if (rdma_str) {
                    if (chimera_pnfs_uaddr_from_human("rdma", rdma_str,
                                                      wire_rdma, sizeof(wire_rdma)) != 0) {
                        chimera_server_error(
                            "pNFS data_server[%zu] invalid rdma address \"%s\"; skipping RDMA advert",
                            ds_i, rdma_str);
                    } else {
                        wire_rdma_p = wire_rdma;
                    }
                }

                chimera_server_config_add_pnfs_ds(server_config, "tcp", wire_tcp,
                                                  wire_rdma_p, backing_str, ds_version, ds_minor);
            }
        }
    }

    json_value = json_object_get(server_params, "state_dir");
    if (json_is_string(json_value)) {
        chimera_server_config_set_state_dir(server_config, json_string_value(json_value));
    }

    json_t *rest_http_port_value = json_object_get(server_params, "rest_http_port");
    if (rest_http_port_value && json_is_integer(rest_http_port_value)) {
        int rest_http_port = json_integer_value(rest_http_port_value);
        chimera_server_config_set_rest_http_port(server_config, rest_http_port);
    }

    /* Test-only: enable the /api/v1/debug/fsop endpoint that performs
     * server-side filesystem mutations (used to drive delegation recalls in
     * the pynfs DELEG16-20 tests). Default off; never enable in production. */
    json_t *rest_debug_fsops_value = json_object_get(server_params, "rest_debug_fsops");
    if (json_is_true(rest_debug_fsops_value)) {
        chimera_server_config_set_rest_debug_fsops(server_config, 1);
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

    json_value = json_object_get(server_params, "soft_fail_bad_req");
    if (json_is_true(json_value)) {
        chimera_server_config_set_soft_fail_bad_req(server_config, 1);
    }

    chimera_server_config_set_tcp_flavor(server_config, chimera_common_tcp_flavor(config));

    // Parse SMB auth configuration
    json_t *smb_auth = json_object_get(server_params, "smb_auth");
    if (smb_auth && json_is_object(smb_auth)) {
        json_t *winbind_enabled = json_object_get(smb_auth, "winbind_enabled");
        if (winbind_enabled && json_is_true(winbind_enabled)) {
            chimera_server_config_set_smb_winbind_enabled(server_config, 1);
        }

        json_t *winbind_domain = json_object_get(smb_auth, "winbind_domain");
        if (winbind_domain && json_is_string(winbind_domain)) {
            chimera_server_config_set_smb_winbind_domain(server_config,
                                                         json_string_value(winbind_domain));
        }

        json_t *kerberos_enabled = json_object_get(smb_auth, "kerberos_enabled");
        if (kerberos_enabled && json_is_true(kerberos_enabled)) {
            chimera_server_config_set_smb_kerberos_enabled(server_config, 1);
        }

        json_t *kerberos_keytab = json_object_get(smb_auth, "kerberos_keytab");
        if (kerberos_keytab && json_is_string(kerberos_keytab)) {
            chimera_server_config_set_smb_kerberos_keytab(server_config,
                                                          json_string_value(kerberos_keytab));
        }

        json_t *kerberos_realm = json_object_get(smb_auth, "kerberos_realm");
        if (kerberos_realm && json_is_string(kerberos_realm)) {
            chimera_server_config_set_smb_kerberos_realm(server_config,
                                                         json_string_value(kerberos_realm));
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
            } else if (config_obj) {
                chimera_server_error("VFS module config for module %s is not an object, skipping", module_name);
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
                                    NULL,  // SID - synthesized for builtin users
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
            const char *mount_options;

            module        = json_string_value(json_object_get(mount, "module"));
            path          = json_string_value(json_object_get(mount, "path"));
            mount_options = json_string_value(json_object_get(mount, "options"));

            /* "create": create the backend directory path (and any missing
             * parents) before mounting, for backends initialized empty.  May be
             * `true` (mode 0755) or an object `{ "mode": "0755" }` -- the mode
             * is an octal string; created dirs are owned 0/0. */
            json_t  *create_val  = json_object_get(mount, "create");
            int      do_create   = 0;
            uint32_t create_mode = 0755;

            if (json_is_true(create_val)) {
                do_create = 1;
            } else if (json_is_object(create_val)) {
                json_t *mode_val = json_object_get(create_val, "mode");
                do_create = 1;
                if (json_is_string(mode_val)) {
                    create_mode = (uint32_t) strtol(json_string_value(mode_val), NULL, 8);
                } else if (json_is_integer(mode_val)) {
                    create_mode = (uint32_t) json_integer_value(mode_val);
                }
            }

            chimera_server_info("Mounting %s://%s to /%s%s%s%s...",
                                module, path, name,
                                mount_options ? " options=" : "",
                                mount_options ? mount_options : "",
                                do_create ? " (create)" : "");

            if (do_create && module && path) {
                if (chimera_server_mkpath(server, module, path, create_mode) != 0) {
                    /* Hard fail: the operator asked for the path to be created
                     * and it could not be, so do not silently mount a missing
                     * or wrong target. */
                    chimera_server_error("Failed to create mount path %s://%s for /%s",
                                         module, path, name);
                    exit(1);
                }
            }

            if (chimera_server_mount(server, name, module, path, mount_options) != 0) {
                /* A silently-failed mount leaves shares/exports pointing at a
                 * nonexistent root, so clients later see confusing errors
                 * (e.g. SMB NETWORK_NAME_DELETED).  Surface it here instead. */
                chimera_server_error("Failed to mount %s://%s to /%s",
                                     module, path, name);
            }
        }
    }

    /* Now that the nfs-module backing mounts exist, resolve each pNFS data
     * server's backing root so the MDS can create backing files on it. */
    chimera_server_pnfs_resolve(server);

    shares = json_object_get(config, "shares");

    if (shares) {
        json_object_foreach(shares, name, share)
        {
            json_t *ca  = json_object_get(share, "continuous_availability");
            json_t *enc = json_object_get(share, "encrypt_data");

            path = json_string_value(json_object_get(share, "path"));
            chimera_server_info("Adding SMB share %s -> %s", name, path);
            chimera_server_create_share(server, name, path,
                                        json_is_true(ca) ? 1 : 0);

            if (json_is_true(enc)) {
                chimera_server_share_set_encrypt_data(server, name);
            }
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

    /* The VFS path under which runtime CreateBucket requests materialize new
     * bucket directories. Explicit "s3_bucket_root" wins; otherwise default to
     * the first configured mount ("/<mount-name>"). Leave unset (runtime bucket
     * creation disabled) if neither is available. */
    {
        const char *bucket_root = json_string_value(
            json_object_get(config, "s3_bucket_root"));
        char        default_root[256];

        if (!bucket_root && mounts) {
            const char *first_mount;
            json_t     *mount_val;

            json_object_foreach(mounts, first_mount, mount_val)
            {
                snprintf(default_root, sizeof(default_root), "/%s", first_mount);
                bucket_root = default_root;
                break;
            }
        }

        if (bucket_root) {
            chimera_server_info("S3 runtime bucket root: %s", bucket_root);
            chimera_server_set_s3_bucket_root(server, bucket_root);
        }
    }

    chimera_server_start(server);

    while (!SigInt) {
        sleep(1);
    }

    chimera_server_info("Shutting down server (signal=%d)...", SigInt);

    chimera_server_destroy(server);

    /* Optionally persist a final metrics scrape (common.metrics_file) before
     * tearing down the registry, so short-lived runs keep their metrics. */
    {
        const char *metrics_file = chimera_common_metrics_file(config);

        if (metrics_file) {
            chimera_metrics_dump_file(chimera_metrics_get(metrics), metrics_file);
        }
    }

    chimera_metrics_destroy(metrics);

    chimera_server_info("Server shutdown complete.");

    json_decref(config);

    return 0;
} /* main */
