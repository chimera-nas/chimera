// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <jansson.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/evp.h>

#include "evpl/evpl.h"

#include "server/server.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/vfs_pnfs.h"
#include "server/server_internal.h"
#include "server/nfs/nfs.h"
#include "common/logging.h"
#include "common/common_config.h"
#include "common/chimera_tracing.h"
#include "metrics/metrics.h"
#include "daemon.h"

int SigInt = 0;

void
signal_handler(int sig)
{
    SigInt = sig;
} /* signal_handler */

/* Terminate on a startup-validation failure.  By the time the config is
 * being validated, service threads (metrics, listener, log flusher) are
 * already running, and a plain exit() would run libevpl's atexit cleanup,
 * which frees the shared allocator and registries out from under those
 * threads.  The resulting crash storm usually still ends the process, but
 * the concurrent faulting threads can wedge the sanitizer's crash reporting
 * and leave a hung daemon that ignores SIGTERM.  Nothing needs unwinding on
 * a bad config: flush the log buffer so the error reaches the user, then
 * exit without running atexit handlers. */
static void __attribute__((noreturn))
startup_validation_fail(void)
{
    chimera_log_flush();
    _exit(1);
} /* startup_validation_fail */

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

    /*
     * Write the private key with owner-only (0600) permissions.  The daemon
     * runs with umask(0) (so client file modes on the data path apply
     * verbatim), which means fopen("w") would create this control-plane RSA
     * private key world-readable AND world-writable (mode 0666) -- CWE-276
     * (Incorrect Default Permissions).  Create the file explicitly with
     * O_CREAT|O_EXCL|0600 so the mode is enforced regardless of umask and so a
     * pre-existing (potentially attacker-planted) file in the world-writable
     * /tmp path is rejected rather than overwritten (CWE-377).
     */
    {
        int key_fd = open(key_path, O_WRONLY | O_CREAT | O_EXCL, 0600);

        if (key_fd < 0) {
            chimera_server_error("Failed to open key file: %s (%s)",
                                 key_path, strerror(errno));
            goto cleanup;
        }

        /* Belt-and-suspenders: enforce 0600 explicitly (defense in depth). */
        if (fchmod(key_fd, 0600) < 0) {
            chimera_server_error("Failed to set key file permissions: %s (%s)",
                                 key_path, strerror(errno));
            close(key_fd);
            goto cleanup;
        }

        fp = fdopen(key_fd, "w");
        if (!fp) {
            chimera_server_error("Failed to open key file stream: %s", key_path);
            close(key_fd);
            goto cleanup;
        }
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

    /*
     * State the ceiling one RPC message needs rather than inheriting whatever
     * libevpl derives from the buffer size, so that raising the NFS transfer
     * size cannot silently outgrow what a message can carry.
     *
     * The relationship is not decorative.  RPCSEC_GSS privacy (sec=krb5p)
     * seals a call into a single opaque, and gss_unwrap takes one contiguous
     * token, so a sealed WRITE has to be gathered into a single allocation --
     * which cannot exceed one buffer.  A transfer size larger than a message
     * would therefore arrive on the wire and then fail to unseal, visible only
     * under krb5p and only above a size nobody routinely tests at.  Saying it
     * here means evpl_init refuses the configuration instead, with a message
     * naming both numbers.
     *
     * The margin covers the RPC and record-marking headers, the GSS credential
     * and verifier, and the framing a seal adds around the payload.
     */
    evpl_global_config_set_rpc2_max_message_size(
        evpl_global_config,
        CHIMERA_NFS_MAX_XFER + CHIMERA_NFS_RPC_OVERHEAD);
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

    /* Bring up OpenTelemetry span tracing if common.tracing.enabled (off by
    * default).  Must run after evpl_init (the gRPC exporter spins up an evpl
    * thread) and before the server's core threads start producing spans. */
    chimera_tracing_init(config);

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

    /* The scrape endpoint follows the same transport flavor as the rest of
     * the server (common.tcp_flavor), read straight from the parsed config --
     * server_config does not exist yet at this point. */
    metrics = chimera_metrics_init(metrics_port,
                                   chimera_common_tcp_flavor(config));

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

    /* Concurrent NFS export count cap.  A misconfigured limit silently
     * ignored would defeat its purpose, so reject bad values outright.  The
     * cap may not exceed the export id space (ids are unique per export, so
     * a larger count could never be reached anyway). */
    json_value = json_object_get(server_params, "nfs_max_exports");
    if (json_value) {
        json_int_t v = json_is_integer(json_value) ?
            json_integer_value(json_value) : -1;

        if (v < 1 || v > CHIMERA_NFS_EXPORT_ID_MAX) {
            chimera_server_error("Invalid nfs_max_exports (expected integer "
                                 "1..%u)", CHIMERA_NFS_EXPORT_ID_MAX);
            startup_validation_fail();
        }
        chimera_server_config_set_nfs_max_exports(server_config, (uint32_t) v);
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

    /* The VFS attribute cache (common.attr_cache) is a VFS-level facility shared
     * with the client; on by default. */
    chimera_server_config_set_attr_cache_enabled(server_config,
                                                 chimera_common_attr_cache_enabled(config));

    /* The VFS name (lookup) cache (common.name_cache) is likewise shared with
     * the client; on by default. */
    chimera_server_config_set_name_cache_enabled(server_config,
                                                 chimera_common_name_cache_enabled(config));

    /* How long umount waits for a mount's handles to drain before reporting
     * EBUSY (common.umount_timeout_ms); shared with the client. */
    chimera_server_config_set_umount_timeout(server_config,
                                             chimera_common_umount_timeout_ms(config));

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

    json_value = json_object_get(server_params, "smb_replay_pending_windows");
    if (json_is_boolean(json_value)) {
        chimera_server_config_set_smb_replay_pending_windows(server_config, json_is_true(json_value));
    }

    json_value = json_object_get(server_params, "smb2_max_async_credits");
    if (json_is_integer(json_value)) {
        chimera_server_config_set_smb2_max_async_credits(server_config, json_integer_value(json_value));
    }

    json_value = json_object_get(server_params, "smb_fs_physical_bytes_per_sector");
    if (json_is_integer(json_value)) {
        chimera_server_config_set_smb_fs_physical_bytes_per_sector(server_config, (uint32_t) json_integer_value(
                                                                       json_value));
    }

    json_value = json_object_get(server_params, "smb_fs_sector_size_flags");
    if (json_is_integer(json_value)) {
        chimera_server_config_set_smb_fs_sector_size_flags(server_config, (uint32_t) json_integer_value(json_value));
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

    json_value = json_object_get(server_params, "nfs4_node_id");
    if (json_is_integer(json_value)) {
        int_value = json_integer_value(json_value);
        if (int_value > 0 && int_value < 0xFFFF) {
            chimera_server_config_set_nfs4_node_id(server_config, int_value);
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

    json_value = json_object_get(server_params, "smb_port");
    if (json_is_integer(json_value)) {
        int_value = json_integer_value(json_value);
        chimera_server_config_set_smb_port(server_config, int_value);
    }

    /* Protocols are opt-in: nothing serves until the config enables it, so an
     * instance brought up for one purpose never surprise-binds the other
     * protocols' well-known ports.  The *_port settings above keep their
     * customary defaults and matter only once the protocol is enabled. */
    json_value = json_object_get(server_params, "nfs_enabled");
    if (json_is_boolean(json_value)) {
        chimera_server_config_set_nfs_enabled(server_config, json_is_true(json_value));
    }

    json_value = json_object_get(server_params, "smb_enabled");
    if (json_is_boolean(json_value)) {
        chimera_server_config_set_smb_enabled(server_config, json_is_true(json_value));
    }

    json_value = json_object_get(server_params, "s3_enabled");
    if (json_is_boolean(json_value)) {
        chimera_server_config_set_s3_enabled(server_config, json_is_true(json_value));
    }

    json_value = json_object_get(server_params, "fuse_enabled");
    if (json_is_boolean(json_value)) {
        chimera_server_config_set_fuse_enabled(server_config, json_is_true(json_value));
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

    /* nfs_fh_sign: sign wire file handles (default true).  nfs_fh_key: optional
     * 32-hex-char (128-bit) signing key, required when multiple nodes must mint
     * interchangeable handles. */
    json_value = json_object_get(server_params, "nfs_fh_sign");
    if (json_value) {
        chimera_server_config_set_nfs_fh_sign(server_config,
                                              json_is_true(json_value) ? 1 : 0);
    }
    json_value = json_object_get(server_params, "nfs_fh_key");
    if (json_is_string(json_value)) {
        chimera_server_config_set_nfs_fh_key(server_config, json_string_value(json_value));
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

    /* REST API authentication is enabled by default; it can be turned off
     * explicitly with "rest_auth_enabled": false. */
    json_t *rest_auth_enabled_value = json_object_get(server_params, "rest_auth_enabled");
    if (rest_auth_enabled_value && json_is_boolean(rest_auth_enabled_value)) {
        chimera_server_config_set_rest_auth_enabled(server_config,
                                                    json_is_true(rest_auth_enabled_value) ? 1 : 0);
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

    // Parse NFS auth configuration (RPCSEC_GSS / Kerberos)
    json_t *nfs_auth = json_object_get(server_params, "nfs_auth");
    if (nfs_auth && json_is_object(nfs_auth)) {
        json_t *kerberos_enabled = json_object_get(nfs_auth, "kerberos_enabled");
        if (kerberos_enabled && json_is_true(kerberos_enabled)) {
            chimera_server_config_set_nfs_kerberos_enabled(server_config, 1);
        }

        json_t *kerberos_keytab = json_object_get(nfs_auth, "kerberos_keytab");
        if (kerberos_keytab && json_is_string(kerberos_keytab)) {
            chimera_server_config_set_nfs_kerberos_keytab(server_config,
                                                          json_string_value(kerberos_keytab));
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

    json_t *s3_anon = json_object_get(config, "s3_anon");
    if (s3_anon && json_is_object(s3_anon)) {
        json_t *anon_uid = json_object_get(s3_anon, "uid");
        json_t *anon_gid = json_object_get(s3_anon, "gid");

        chimera_server_config_set_s3_anon_ids(
            server_config,
            anon_uid ? (uint32_t) json_integer_value(anon_uid) : CHIMERA_S3_ANON_UID,
            anon_gid ? (uint32_t) json_integer_value(anon_gid) : CHIMERA_S3_ANON_GID);
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
            const char *key_user   = json_string_value(json_object_get(key_entry, "username"));

            if (!access_key || !secret_key) {
                chimera_server_error("S3 access key entry missing access_key or secret_key, skipping");
                continue;
            }

            chimera_server_info("Adding S3 access key %s (user %s)", access_key,
                                key_user ? key_user : "<unbound>");
            chimera_server_add_s3_cred(server, access_key, secret_key, key_user, 1);
        }
    }

    /* "filesystems": named filesystems to ensure exist inside CAP_MKFS
     * modules before any mounts reference them.  Declarative: a filesystem
     * that already exists (EEXIST, e.g. a persistent backend rebooting
     * against the same store) is not an error. */
    json_t *filesystems = json_object_get(config, "filesystems");

    if (filesystems) {
        const char *fsname;
        json_t     *fs_entry;
        json_object_foreach(filesystems, fsname, fs_entry)
        {
            const char *fs_module  = json_string_value(json_object_get(fs_entry, "module"));
            const char *fs_options = json_string_value(json_object_get(fs_entry, "options"));
            int         fs_rc;

            if (!fs_module) {
                chimera_server_error("Filesystem %s missing module, skipping", fsname);
                continue;
            }

            chimera_server_info("Creating filesystem %s in module %s...", fsname, fs_module);

            fs_rc = chimera_server_mkfs(server, fs_module, fsname, fs_options);

            if (fs_rc == CHIMERA_VFS_EEXIST) {
                chimera_server_info("Filesystem %s already exists in module %s", fsname, fs_module);
            } else if (fs_rc != 0) {
                chimera_server_error("Failed to create filesystem %s in module %s",
                                     fsname, fs_module);
                exit(1);
            }
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
             * is an octal string; created dirs are owned by the server identity. */
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
                    startup_validation_fail();
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

    /* A share/export/bucket section whose protocol is not enabled is a
     * contradiction: nothing would ever serve it.  Fail hard like the
     * malformed-export errors below rather than silently not serving. */
    if (shares && json_object_size(shares) > 0 &&
        !chimera_server_config_get_smb_enabled(server_config)) {
        chimera_server_error("Config declares SMB shares but smb_enabled is false");
        startup_validation_fail();
    }

    if (shares) {
        json_object_foreach(shares, name, share)
        {
            json_t *ca  = json_object_get(share, "continuous_availability");
            json_t *enc = json_object_get(share, "encrypt_data");
            json_t *fl2 = json_object_get(share, "force_level2_oplock");

            path = json_string_value(json_object_get(share, "path"));
            chimera_server_info("Adding SMB share %s -> %s", name, path);
            chimera_server_create_share(server, name, path,
                                        json_is_true(ca) ? 1 : 0);

            if (json_is_true(enc)) {
                chimera_server_share_set_encrypt_data(server, name);
            }

            if (json_is_true(fl2)) {
                chimera_server_share_set_force_level2_oplock(server, name);
            }
        }
    }

    /* Test-harness aid: seed the symbolic-link fixtures the WPTS MS-SMB2
     * CreateClose symlink cases expect to pre-exist on the share.  Enabled only
     * when CHIMERA_SMB_SEED_SYMLINKS names the backend module (e.g. "memfs"),
     * since no SMB client can create a reparse-point symlink on an empty share. */
    const char *seed_module = getenv("CHIMERA_SMB_SEED_SYMLINKS");

    /* Where on the module the fixtures go.  "/" was the implicit filesystem
     * every backend used to have; a CAP_MKFS backend has none, so the harness
     * names the filesystem its shares mount (e.g. "fs0"). */
    const char *seed_path = getenv("CHIMERA_SMB_SEED_PATH");

    if (!seed_path || !seed_path[0]) {
        seed_path = "/";
    }

    if (seed_module && seed_module[0]) {
        if (chimera_server_seed_symlinks(server, seed_module, seed_path) != 0) {
            chimera_server_error("Failed to seed symlink fixtures on module %s",
                                 seed_module);
        } else {
            chimera_server_info("Seeded symlink fixtures on module %s", seed_module);
        }
    }

    /* Likewise seed the MS-FSA suite's fixtures (a directory + a regular file)
     * when CHIMERA_SMB_SEED_FSA names the backend module. */
    const char *seed_fsa_module = getenv("CHIMERA_SMB_SEED_FSA");

    if (seed_fsa_module && seed_fsa_module[0]) {
        if (chimera_server_seed_fsa(server, seed_fsa_module, seed_path) != 0) {
            chimera_server_error("Failed to seed FSA fixtures on module %s",
                                 seed_fsa_module);
        } else {
            chimera_server_info("Seeded FSA fixtures on module %s", seed_fsa_module);
        }
    }

    exports = json_object_get(config, "exports");

    if (exports && json_object_size(exports) > 0 &&
        !chimera_server_config_get_nfs_enabled(server_config)) {
        chimera_server_error("Config declares NFS exports but nfs_enabled is false");
        startup_validation_fail();
    }

    /* Two passes over the exports object: entries pinning an explicit
     * export_id are created first so auto-assignment for the remaining
     * entries cannot take an id a later entry pins; with a single pass,
     * whether startup succeeds would depend on key order in the config. */
    for (int pass = 0; exports && pass < 2; pass++) {
        json_object_foreach(exports, name, export)
        {
            json_t     *exp_id_j = json_object_get(export, "export_id");

            if ((exp_id_j != NULL) != (pass == 0)) {
                continue;
            }

            json_t     *access_j  = json_object_get(export, "access");
            json_t     *squash_j  = json_object_get(export, "squash");
            json_t     *rsq_j     = json_object_get(export, "root_squash");
            json_t     *norsq_j   = json_object_get(export, "no_root_squash");
            json_t     *allsq_j   = json_object_get(export, "all_squash");
            json_t     *anonuid_j = json_object_get(export, "anonuid");
            json_t     *anongid_j = json_object_get(export, "anongid");
            uint32_t    export_id = 0;
            const char *access_s  = json_string_value(access_j);
            const char *squash_s  = json_string_value(squash_j);
            uint32_t    access    = CHIMERA_NFS_EXPORT_ACCESS_RW;
            uint32_t    squash    = CHIMERA_NFS_SQUASH_NONE;
            uint32_t    anonuid   = chimera_server_config_get_anonuid(server_config);
            uint32_t    anongid   = chimera_server_config_get_anongid(server_config);

            path = json_string_value(json_object_get(export, "path"));

            /* A missing or non-string path would otherwise flow into the
             * export as the literal string "(null)" and boot an export that
             * fails only when clients try to mount it. */
            if (!path) {
                chimera_server_error("Export '%s': missing or non-string "
                                     "\"path\"", name);
                startup_validation_fail();
            }

            /* The access mode key was renamed from "options" to "access".
             * Ignoring the old key would silently turn an "options": "ro"
             * export read-write, so reject it outright. */
            if (json_object_get(export, "options")) {
                chimera_server_error("Export '%s': \"options\" has been "
                                     "renamed to \"access\"; update the config",
                                     name);
                startup_validation_fail();
            }

            /* Access mode: "ro" | "rw" (default rw).  A value that doesn't
             * parse is fatal: booting anyway would silently turn a requested
             * read-only export read-write. */
            if (access_j) {
                if (access_s && strcasecmp(access_s, "ro") == 0) {
                    access = CHIMERA_NFS_EXPORT_ACCESS_RO;
                } else if (access_s && strcasecmp(access_s, "rw") == 0) {
                    access = CHIMERA_NFS_EXPORT_ACCESS_RW;
                } else {
                    chimera_server_error("Invalid export '%s' access value '%s' "
                                         "(expected ro/rw)", name,
                                         access_s ? access_s : "(not a string)");
                    startup_validation_fail();
                }
            }

            /* Squash policy.  Default is no squashing.  An explicit "squash"
             * string takes precedence; otherwise the all_squash / root_squash /
             * no_root_squash booleans act as aliases. */
            if (squash_j) {
                if (squash_s && (strcasecmp(squash_s, "none") == 0 ||
                                 strcasecmp(squash_s, "no_root_squash") == 0)) {
                    squash = CHIMERA_NFS_SQUASH_NONE;
                } else if (squash_s && (strcasecmp(squash_s, "root") == 0 ||
                                        strcasecmp(squash_s, "root_squash") == 0)) {
                    squash = CHIMERA_NFS_SQUASH_ROOT;
                } else if (squash_s && (strcasecmp(squash_s, "all") == 0 ||
                                        strcasecmp(squash_s, "all_squash") == 0)) {
                    squash = CHIMERA_NFS_SQUASH_ALL;
                } else {
                    chimera_server_error("Invalid export '%s' squash value '%s' "
                                         "(expected none/root/all)", name,
                                         squash_s ? squash_s : "(not a string)");
                    startup_validation_fail();
                }
            } else {
                /* The aliases must actually be booleans: a value like
                 * "root_squash": "true" (a string) or "all_squash": 1
                 * silently ignored would leave the export unsquashed -- the
                 * same silent-permissive fallback the string form above
                 * rejects. */
                if (allsq_j && !json_is_boolean(allsq_j)) {
                    chimera_server_error("Export '%s': \"all_squash\" must be "
                                         "a boolean", name);
                    startup_validation_fail();
                }
                if (rsq_j && !json_is_boolean(rsq_j)) {
                    chimera_server_error("Export '%s': \"root_squash\" must be "
                                         "a boolean", name);
                    startup_validation_fail();
                }
                if (norsq_j && !json_is_boolean(norsq_j)) {
                    chimera_server_error("Export '%s': \"no_root_squash\" must "
                                         "be a boolean", name);
                    startup_validation_fail();
                }
                if (allsq_j && json_is_true(allsq_j)) {
                    squash = CHIMERA_NFS_SQUASH_ALL;
                } else if (rsq_j && json_is_true(rsq_j)) {
                    squash = CHIMERA_NFS_SQUASH_ROOT;
                } else if (norsq_j && json_is_true(norsq_j)) {
                    squash = CHIMERA_NFS_SQUASH_NONE;
                }
            }

            /* Anon ids must be validated before the unsigned cast: a
             * non-integer json value reads as 0, which would silently map
             * squashed callers to uid/gid 0 (root). */
            if (anonuid_j) {
                json_int_t v = json_integer_value(anonuid_j);

                if (!json_is_integer(anonuid_j) || v < 0 || v > UINT32_MAX) {
                    chimera_server_error("Export '%s': invalid anonuid "
                                         "(expected integer 0..%u)",
                                         name, UINT32_MAX);
                    startup_validation_fail();
                }
                anonuid = (uint32_t) v;
            }
            if (anongid_j) {
                json_int_t v = json_integer_value(anongid_j);

                if (!json_is_integer(anongid_j) || v < 0 || v > UINT32_MAX) {
                    chimera_server_error("Export '%s': invalid anongid "
                                         "(expected integer 0..%u)",
                                         name, UINT32_MAX);
                    startup_validation_fail();
                }
                anongid = (uint32_t) v;
            }

            /* Stable export id: the id is embedded in wire file handles, so
             * clustered servers exporting the same directory must pin the
             * same value.  A wrong id silently defeats that, so reject bad
             * values outright rather than falling back to auto-assignment. */
            if (exp_id_j) {
                json_int_t v = json_is_integer(exp_id_j) ?
                    json_integer_value(exp_id_j) : -1;

                if (!json_is_integer(exp_id_j) ||
                    v < 1 || v > CHIMERA_NFS_EXPORT_ID_MAX) {
                    chimera_server_error("Export '%s': invalid export_id "
                                         "(expected integer 1..%u)",
                                         name, CHIMERA_NFS_EXPORT_ID_MAX);
                    startup_validation_fail();
                }
                export_id = (uint32_t) v;
            }

            /* Allowed security flavors: an optional array of
             * "sys"/"krb5"/"krb5i"/"krb5p".  Absent (mask 0) permits any
             * flavor, preserving historical behavior; a non-empty list
             * restricts the export and rejects others with NFS4ERR_WRONGSEC.
             * Every malformed shape is fatal: a typo'd flavor, non-string
             * entry, or non-array value silently dropped would leave the
             * mask at 0 -- "any flavor allowed" -- turning a requested
             * Kerberos-only export wide open to AUTH_SYS. */
            json_t  *sec_j    = json_object_get(export, "sec");
            uint32_t sec_mask = 0;

            if (sec_j && !json_is_array(sec_j)) {
                chimera_server_error("Export '%s': \"sec\" must be an array "
                                     "of flavor strings", name);
                startup_validation_fail();
            }
            if (sec_j) {
                size_t  si;
                json_t *flavor_j;
                json_array_foreach(sec_j, si, flavor_j)
                {
                    const char *f = json_string_value(flavor_j);

                    if (!f) {
                        chimera_server_error("Export '%s': \"sec\" entry %zu "
                                             "is not a string", name, si);
                        startup_validation_fail();
                    }
                    if (strcasecmp(f, "sys") == 0 || strcasecmp(f, "auth_sys") == 0) {
                        sec_mask |= CHIMERA_NFS_SEC_SYS;
                    } else if (strcasecmp(f, "krb5") == 0) {
                        sec_mask |= CHIMERA_NFS_SEC_KRB5;
                    } else if (strcasecmp(f, "krb5i") == 0) {
                        sec_mask |= CHIMERA_NFS_SEC_KRB5I;
                    } else if (strcasecmp(f, "krb5p") == 0) {
                        sec_mask |= CHIMERA_NFS_SEC_KRB5P;
                    } else {
                        chimera_server_error("Invalid export '%s' sec flavor '%s' "
                                             "(expected sys/krb5/krb5i/krb5p)", name, f);
                        startup_validation_fail();
                    }
                }
            }

            char export_id_str[16] = "auto";

            if (export_id) {
                snprintf(export_id_str, sizeof(export_id_str), "%u", export_id);
            }

            chimera_server_info("Adding NFS export %s -> %s (%s, %s, export_id %s)",
                                name, path,
                                access & CHIMERA_NFS_EXPORT_ACCESS_RO ? "ro" : "rw",
                                squash == CHIMERA_NFS_SQUASH_ALL ? "all_squash" :
                                squash == CHIMERA_NFS_SQUASH_NONE ? "no_root_squash" :
                                "root_squash",
                                export_id_str);
            /* All validated settings are applied atomically at creation, so
             * the export is never live with permissive defaults while a
             * follow-up set_options/set_sec call is pending. */
            struct chimera_nfs_export_opts opts = {
                .has_access  = 1,
                .has_squash  = 1,
                .has_anonuid = 1,
                .has_anongid = 1,
                .has_sec     = 1,
                .access      = access,
                .squash      = squash,
                .anonuid     = anonuid,
                .anongid     = anongid,
                .sec_allowed = sec_mask,
            };

            if (chimera_server_create_export(server, name, path, export_id,
                                             &opts) != 0) {
                /* Duplicate name or unusable export_id: proceeding would mint
                 * file handles under a different id than the operator pinned,
                 * which breaks cluster failover in a way clients only notice
                 * later.  Fail hard like the mount-path errors above. */
                chimera_server_error("Failed to create export '%s'", name);
                startup_validation_fail();
            }
        }
    }

    buckets = json_object_get(config, "buckets");

    if (buckets && json_object_size(buckets) > 0 &&
        !chimera_server_config_get_s3_enabled(server_config)) {
        chimera_server_error("Config declares S3 buckets but s3_enabled is false");
        startup_validation_fail();
    }

    if (buckets) {
        json_object_foreach(buckets, name, bucket)
        {
            path = json_string_value(json_object_get(bucket, "path"));
            chimera_server_info("Adding S3 bucket %s -> %s", name, path);
            chimera_server_create_bucket(server, name, path);
        }
    }

    /* FUSE mountpoints: local kernel mounts of chimera namespace paths,
     * keyed by mountpoint directory.  Linux-only; on other builds
     * chimera_server_create_fuse_mount always rejects, which the
     * contradiction check below reports at startup. */
    {
        json_t     *fuse_mounts = json_object_get(config, "fuse_mounts");
        json_t     *fuse_mount;
        const char *fuse_mountpoint;

        if (fuse_mounts && json_object_size(fuse_mounts) > 0 &&
            !chimera_server_config_get_fuse_enabled(server_config)) {
            chimera_server_error("Config declares FUSE mounts but fuse_enabled is false");
            startup_validation_fail();
        }

        if (fuse_mounts) {
            json_object_foreach(fuse_mounts, fuse_mountpoint, fuse_mount)
            {
                const char *fuse_options;

                path = json_string_value(json_object_get(fuse_mount, "path"));

                if (!path) {
                    chimera_server_error("FUSE mount '%s': missing \"path\"",
                                         fuse_mountpoint);
                    startup_validation_fail();
                    continue;
                }

                fuse_options = json_string_value(json_object_get(fuse_mount, "options"));

                chimera_server_info("Adding FUSE mount %s -> %s", fuse_mountpoint, path);

                if (chimera_server_create_fuse_mount(server, fuse_mountpoint,
                                                     path, fuse_options) != 0) {
                    chimera_server_error(
                        "FUSE mount '%s' rejected (non-Linux build, fuse disabled, or bad options)",
                        fuse_mountpoint);
                    startup_validation_fail();
                }
            }
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

    /* Flush and close tracing exporters after the server's threads have drained
     * and unregistered (chimera_server_destroy joined them). */
    chimera_tracing_destroy();

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
