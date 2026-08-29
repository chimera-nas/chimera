/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * In-process harness for the control-plane tests: a chimera server with its
 * REST API and Prometheus scrape endpoint, plus an HTTP client, all in ONE
 * process over libevpl's inproc transport.  Nothing binds a port, so these
 * run with no network namespace, no resource lock, and no privileges --
 * exactly like the NFS (nfs3_mbt_common.h) and SMB2 (smb2_mbt_common.h)
 * harnesses, and unlike the older REST tests, which shell out to curl
 * against a real socket and therefore have to serialize under a netns lock.
 *
 * The REST server and the metrics server both follow the server's configured
 * transport flavor (chimera_rest_init / chimera_metrics_init), so under
 * CHIMERA_TCP_FLAVOR_INPROC a "port" is only the name of an inproc endpoint
 * ("chimera-inproc-<port>"); see common/tcp_flavor.h.  That is the whole
 * trick: the API under test is reached by its real HTTP surface -- request
 * line, headers, JSON body, status code -- with no socket underneath.
 *
 * The client half issues one request at a time: ctl_http() builds the
 * request, dispatches it, and pumps evpl_continue() until the response
 * callback lands, copying the status and body into a caller-owned ctl_res.
 * That is the same shape as the NFS harness's per-RPC wrappers.
 *
 * The protocol servers (NFS, SMB2, S3) are brought up on the same inproc
 * server so a control-plane action and its protocol-visible consequence can
 * be observed in one process: create an export over REST, then reach it over
 * NFS; delete it, and watch the same request stop working.
 */

#ifndef CTL_MBT_COMMON_H
#define CTL_MBT_COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>

#include "server/server.h"
#include "common/tcp_flavor.h"
#include "metrics/metrics.h"
#include "prometheus-c.h"

#include "evpl/evpl.h"
#include "evpl/evpl_config.h"
#include "evpl/evpl_http.h"

#include "ctl_http.h"

struct ctl_env_opts {
    int         auth_enabled;    /* REST auth; off by default here */
    int         debug_fsops;     /* route POST /api/v1/debug/fsop */
    int         nfs_enabled;
    int         smb_enabled;
    int         s3_enabled;
    int         metrics_enabled; /* stand up the scrape endpoint */
    int         max_exports;     /* nfs_max_exports; 0 = server default */
    const char *state_dir;       /* NULL = a fresh mkdtemp */
};

struct ctl_env {
    struct chimera_server     *server;
    struct chimera_metrics    *metrics;   /* NULL unless metrics_enabled */
    struct prometheus_metrics *registry;
    struct evpl               *evpl;
    struct evpl_http_agent    *agent;
    struct ctl_env_opts        opts;
    char                       session_dir[256];
};

/* ---- server lifecycle --------------------------------------------------- */

static inline void
ctl_env_open(
    struct ctl_env            *env,
    const struct ctl_env_opts *opts)
{
    struct chimera_server_config *config;

    memset(env, 0, sizeof(*env));

    if (opts) {
        env->opts = *opts;
    }

    /* Same close-sweep tightening the NFS/SMB harnesses use: a filesystem
     * stays busy for one sweep after its last handle is dropped, and the
     * control plane suite creates and destroys filesystems constantly. */
    setenv("CHIMERA_CLOSE_SWEEP_INTERVAL_MS", "10", 0);

    if (env->opts.state_dir) {
        snprintf(env->session_dir, sizeof(env->session_dir), "%s",
                 env->opts.state_dir);
    } else {
        snprintf(env->session_dir, sizeof(env->session_dir),
                 "/tmp/ctl_mbt_XXXXXX");
        if (!mkdtemp(env->session_dir)) {
            fprintf(stderr, "mkdtemp(%s) failed\n", env->session_dir);
            exit(1);
        }
    }

    /* The scrape endpoint owns its own registry (chimera_metrics_init creates
     * one on its thread), so when it is enabled the server must be handed
     * that one -- otherwise the server counts into a registry nothing
     * scrapes, and every metric reads zero over HTTP. */
    if (env->opts.metrics_enabled) {
        env->metrics = chimera_metrics_init(CTL_METRICS_PORT,
                                            CHIMERA_TCP_FLAVOR_INPROC);
        env->registry = chimera_metrics_get(env->metrics);
    } else {
        env->registry = prometheus_metrics_create(NULL, NULL, 0);
    }

    config = chimera_server_config_init();
    chimera_server_config_set_state_dir(config, env->session_dir);
    chimera_server_config_set_tcp_flavor(config, CHIMERA_TCP_FLAVOR_INPROC);

    chimera_server_config_set_rest_http_port(config, CTL_REST_PORT);
    chimera_server_config_set_rest_auth_enabled(config,
                                                env->opts.auth_enabled);
    chimera_server_config_set_rest_debug_fsops(config, env->opts.debug_fsops);

    chimera_server_config_set_nfs_enabled(config, env->opts.nfs_enabled);
    chimera_server_config_set_smb_enabled(config, env->opts.smb_enabled);
    chimera_server_config_set_s3_enabled(config, env->opts.s3_enabled);

    if (env->opts.smb_enabled) {
        chimera_server_config_set_smb_signing_required(config, 0);
    }

    if (env->opts.max_exports) {
        chimera_server_config_set_nfs_max_exports(config,
                                                  env->opts.max_exports);
    }

    env->server = chimera_server_init(config, env->registry);
    chimera_server_start(env->server);

    /* The client loop's idle poll interval.  Every wait in this harness is a
     * `while (!done) pump;`, so this only trades pump latency against CPU --
     * but a bound is required, not merely nice: the default parks
     * evpl_continue in the poller until something happens on THIS loop, and
     * the thing being waited for happens on the server's. */
    struct evpl_thread_config *tcfg = evpl_thread_config_init();

    evpl_thread_config_set_wait_ms(tcfg, 1);
    env->evpl = evpl_create(tcfg);

    env->agent = evpl_http_init(env->evpl);
} /* ctl_env_open */

static inline void
ctl_env_close(struct ctl_env *env)
{
    char cmd[512];

    evpl_http_destroy(env->agent);
    evpl_destroy(env->evpl);

    /* The server counts into the registry, so it goes first either way.  When
     * the scrape endpoint owns the registry, destroying it also destroys the
     * registry (on its own thread); otherwise this harness owns it. */
    chimera_server_destroy(env->server);

    if (env->metrics) {
        chimera_metrics_destroy(env->metrics);
    } else {
        prometheus_metrics_destroy(env->registry);
    }

    if (!env->opts.state_dir) {
        snprintf(cmd, sizeof(cmd), "rm -rf %s", env->session_dir);
        if (system(cmd) != 0) {
            fprintf(stderr, "warning: failed to remove %s\n",
                    env->session_dir);
        }
    }
} /* ctl_env_close */

#endif /* CTL_MBT_COMMON_H */
