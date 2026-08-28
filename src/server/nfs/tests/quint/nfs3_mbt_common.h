// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * In-process harness for the NFS3 model-based tests: a chimera server
 * (memfs backend) and an rpc2 NFS3/MOUNT client living in ONE process,
 * talking over libevpl's inproc transport.  Nothing binds a port, so any
 * number of these binaries run concurrently with no network namespace,
 * resource lock, or privileges -- and no daemon spawn per trace.
 *
 * The client half issues one RPC at a time: each wrapper fires the
 * generated send_call_* stub and pumps evpl_continue() until the reply
 * callback lands, copying the fields the oracle needs out of the
 * rpc2-owned reply into a caller-owned mbt_result.  The replay logic
 * began as a line-faithful port of the retired python harness
 * (replay.py, in git history), whose hand-rolled XDR independently
 * validated the wire encoding this harness shares with the server.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>

#include "server/server.h"
#include "common/tcp_flavor.h"
#include "prometheus-c.h"

#include "nfs3_xdr.h"
#include "nfs4_xdr.h"
#include "nfs_mount_xdr.h"
#include "portmap_xdr.h"
#include "nlm4_xdr.h"
#include "sm_inter_xdr.h"

#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"

#define MBT_MAX_ENTRIES  512    /* readdir entries copied out per reply */
#define MBT_NAME_MAX     256
#define MBT_MAX_DATA     (4 << 20) /* read payload copy-out bound */

/* Must match NFS_MOUNT_PORT in nfs_external_portmap.h: under inproc the
 * port number is only a service name ("chimera-inproc-20048"), but it
 * still has to be the name the server registered.  The auxiliary services
 * follow the same rule: 111 is hardwired in nfs.c, the other two are the
 * nfs_lockmgr_port / nfs_nsm_port defaults (server.c). */
#define MBT_MOUNT_PORT   20048
#define MBT_PORTMAP_PORT 111
#define MBT_NLM_PORT     32803
#define MBT_NSM_PORT     32765

struct mbt_fh {
    int      has;
    uint32_t len;
    uint8_t  data[NFS3_FHSIZE];
};

struct mbt_attr {
    int           has;
    struct fattr3 a;
};

struct mbt_entry {
    uint64_t        fileid;
    uint64_t        cookie;
    char            name[MBT_NAME_MAX];
    uint32_t        name_len;
    struct mbt_attr attrs;      /* readdirplus only */
    struct mbt_fh   fh;         /* readdirplus only */
};

/* One RPC's oracle-relevant reply fields, copied out of the rpc2-owned
 * decode inside the callback.  A single fat struct reused per call keeps
 * every wrapper's signature uniform. */
struct mbt_result {
    int              done;
    int              rpc_err;   /* transport-level status; 0 on any reply */
    uint32_t         status;    /* nfsstat3 (or mountstat3 for MNT) */

    struct mbt_fh    obj_fh;    /* LOOKUP object / post_op_fh3 / MNT fhandle */
    struct mbt_attr  obj_attrs; /* obj_attributes / attrs / file_attributes */
    struct mbt_attr  wcc_after; /* wcc_data.after of the object's wcc */

    uint32_t         access;

    char             target[MBT_NAME_MAX]; /* READLINK data */
    uint32_t         target_len;

    uint32_t         count;     /* READ/WRITE count */
    uint32_t         committed; /* WRITE stable_how granted */
    int              eof;       /* READ/READDIR eof */
    uint8_t          verf[NFS3_WRITEVERFSIZE];

    uint8_t         *data;      /* READ payload, points at mbt_env.data_buf */
    uint32_t         data_len;

    struct mbt_entry entries[MBT_MAX_ENTRIES];
    int              nentries;
    int              entries_overflow;

    /* FSSTAT / FSINFO / PATHCONF constant fields */
    uint64_t         tbytes, fbytes, abytes, tfiles, ffiles, afiles;
    uint32_t         invarsec;
    uint32_t         rtmax, rtpref, rtmult, wtmax, wtpref, wtmult, dtpref;
    uint64_t         maxfilesize;
    struct nfstime3  time_delta;
    uint32_t         properties;
    uint32_t         linkmax, name_max;
    int              no_trunc, chown_restricted;
    int              case_insensitive, case_preserving;
};

/* Optional server shaping for a suite (the NFS4 MBT suite needs all
 * three; the NFS3 defaults need none). */
struct mbt_env_opts {
    int         nfs4_delegations; /* delegations are off by default */
    int         disable_caches;   /* VFS attr+name caches: exact attr
                                   * comparison cannot tolerate staleness */
    const char *memfs_config;     /* module config JSON, e.g. block_size */
    const char *module;           /* VFS backend: NULL/"memfs", "diskfs",
                                   * "cairn".  diskfs/cairn self-provision
                                   * scratch under the env's session_dir. */
    /* Auxiliary NFS services (portmap/rpcbind, NLM, NSM).  Off by default:
     * the NFS3/NFS4 suites never touch them, and connecting three more
     * inproc services per replay process is pure overhead there. */
    int         aux;
    /* server.portmap_hostname: when set, portmap universal addresses are
     * built from this host instead of the connection's local address.  The
     * aux suite uses it to make the uaddr predictable (the inproc local
     * address is not an IP). */
    const char *portmap_hostname;
};

struct mbt_env {
    struct chimera_server     *server;
    struct prometheus_metrics *metrics;
    const char                *module;   /* backend for mkfs/mount/rmfs */

    struct evpl               *evpl;
    struct evpl_rpc2_thread   *rpc2_thread;
    struct evpl_rpc2_conn     *nfs_conn;
    struct evpl_rpc2_conn     *mount_conn;
    struct NFS_V3              nfs_v3;
    struct NFS_MOUNT_V3        mount_v3;
    struct NFS_V4              nfs_v4;
    struct NFS_V4_CB           nfs_v4_cb;
    struct evpl_rpc2_cred      cred;

    /* Auxiliary services, connected only when mbt_env_opts.aux is set.  The
     * programs are registered on the client rpc2 thread either way, which
     * costs nothing and lets the server call back into this process (NLM's
     * *_RES messages ride the same connection the *_MSG arrived on). */
    struct evpl_rpc2_conn     *portmap_conn;
    struct evpl_rpc2_conn     *nlm_conn;
    struct evpl_rpc2_conn     *nsm_conn;
    struct PORTMAP_V2          pm_v2;
    struct PORTMAP_V3          pm_v3;
    struct PORTMAP_V4          pm_v4;
    struct NLM_V4              nlm_v4;
    struct SM_INTER_V1         nsm_v1;
    /* struct mbt_aux_result *, owned by nfs_aux_mbt_common.h. */
    void                      *aux;

    char                       session_dir[256];
    char                       pt_root[256]; /* passthrough backing root
                                              * (linux/io_uring); empty otherwise */
    uint8_t                   *data_buf;   /* READ copy-out scratch */

    struct mbt_result          res;
};

/* ---- reply field copy helpers (called from rpc2 callbacks only) --------- */

static inline void
mbt_copy_fh(
    struct mbt_fh    *dst,
    const xdr_opaque *src)
{
    dst->has = 1;
    dst->len = src->len < NFS3_FHSIZE ? src->len : NFS3_FHSIZE;
    memcpy(dst->data, src->data, dst->len);
} /* mbt_copy_fh */

static inline void
mbt_copy_post_op_fh(
    struct mbt_fh            *dst,
    const struct post_op_fh3 *src)
{
    if (src->handle_follows) {
        mbt_copy_fh(dst, &src->handle.data);
    }
} /* mbt_copy_post_op_fh */

static inline void
mbt_copy_post_op_attr(
    struct mbt_attr           *dst,
    const struct post_op_attr *src)
{
    if (src->attributes_follow) {
        dst->has = 1;
        dst->a   = src->attributes;
    }
} /* mbt_copy_post_op_attr */

static inline int
mbt_fh_eq(
    const struct mbt_fh *a,
    const struct mbt_fh *b)
{
    return a->len == b->len && memcmp(a->data, b->data, a->len) == 0;
} /* mbt_fh_eq */

/* ---- server + client lifecycle ------------------------------------------ */

/* memfs/diskfs/cairn create named filesystems (mkfs); linux and io_uring are
 * passthrough backends that mount a host directory and have no mkfs. */
static inline int
mbt_module_is_passthrough(const char *module)
{
    return strcmp(module, "linux") == 0 || strcmp(module, "io_uring") == 0;
} /* mbt_module_is_passthrough */

static inline void
mbt_env_open_opts(
    struct mbt_env            *env,
    const struct mbt_env_opts *opts)
{
    struct chimera_server_config *config;
    struct evpl_rpc2_program     *programs[9];
    struct evpl_endpoint         *nfs_ep;
    struct evpl_endpoint         *mount_ep;
    int                           aux = opts && opts->aux;

    memset(env, 0, sizeof(*env));

    snprintf(env->session_dir, sizeof(env->session_dir),
             "/tmp/nfs3_mbt_XXXXXX");
    if (!mkdtemp(env->session_dir)) {
        fprintf(stderr, "mkdtemp(%s) failed\n", env->session_dir);
        exit(1);
    }

    env->metrics = prometheus_metrics_create(NULL, NULL, 0);

    /* The VFS releases closed handles on an async sweep thread, so a filesystem
     * stays busy for a window after the last close.  A fast sweep keeps the
     * per-trace rmfs recycle from stalling once the client's opens are dropped;
     * set before the server's threads start below. */
    setenv("CHIMERA_CLOSE_SWEEP_INTERVAL_MS", "10", 0);

    config = chimera_server_config_init();
    chimera_server_config_set_state_dir(config, env->session_dir);
    chimera_server_config_set_tcp_flavor(config, CHIMERA_TCP_FLAVOR_INPROC);
    chimera_server_config_set_nfs_enabled(config, 1);

    env->module = (opts && opts->module) ? opts->module : "memfs";

    /* Deliberately NOT raising common.umount_timeout_ms here.
     *
     * chimera_vfs_umount already purges the mount's cached opens and dispatches
     * their closes immediately -- it never waits out the open cache's idle
     * sweep (see chimera_vfs_open_cache_purge_by_mount).  So the only thing it
     * can be waiting on is a handle with opencnt > 0, i.e. one that is still
     * genuinely open.  The harness closes every open the trace left behind
     * (v4_close_dangling_opens) before tearing the filesystem down, so it
     * should never have to wait at all.  A longer timeout would just hide a
     * leaked handle behind a slower test. */

    if (opts) {
        if (opts->nfs4_delegations) {
            chimera_server_config_set_nfs4_delegations(config, 1);
        }
        if (opts->portmap_hostname) {
            chimera_server_config_set_portmap_hostname(config,
                                                       opts->portmap_hostname);
        }
        if (opts->disable_caches) {
            chimera_server_config_set_attr_cache_enabled(config, 0);
            chimera_server_config_set_name_cache_enabled(config, 0);
        }
    }

    /* Backend module registration.  memfs is a default module (only its
     * optional block_size config is applied); diskfs and cairn are not
     * default-registered and need a config pointing at scratch storage, which
     * we self-provision under the per-process session_dir so every replay
     * process is isolated and is cleaned up with its temp dir. */
    if (strcmp(env->module, "diskfs") == 0) {
        char img[300], cfg[512];
        int  fd;

        snprintf(img, sizeof(img), "%s/device-0.img", env->session_dir);
        fd = open(img, O_CREAT | O_TRUNC | O_RDWR, 0644);
        if (fd < 0 || ftruncate(fd, 1024LL * 1024 * 1024) != 0) {
            fprintf(stderr, "diskfs device image %s: %s\n", img,
                    strerror(errno));
            exit(1);
        }
        close(fd);
        /* 1 GiB sparse device + a 64 MiB intent log, matching the posix
         * driver's diskfs profile. */
        snprintf(cfg, sizeof(cfg),
                 "{\"initialize\":true,\"unsafe_async\":true,"
                 "\"intent_log_size\":67108864,"
                 "\"devices\":[{\"type\":\"libaio\",\"size\":1,\"path\":\"%s\"}]}",
                 img);
        chimera_server_config_add_module(config, "diskfs", NULL, cfg);
    } else if (strcmp(env->module, "cairn") == 0) {
        char dir[300], cfg[512];

        snprintf(dir, sizeof(dir), "%s/cairn", env->session_dir);
        if (mkdir(dir, 0755) != 0 && errno != EEXIST) {
            fprintf(stderr, "cairn dir %s: %s\n", dir, strerror(errno));
            exit(1);
        }
        snprintf(cfg, sizeof(cfg), "{\"initialize\":true,\"path\":\"%s\"}", dir);
        chimera_server_config_add_module(config, "cairn", NULL, cfg);
    } else if (opts && opts->memfs_config) {
        chimera_server_config_add_module(config, "memfs", NULL,
                                         opts->memfs_config);
    }

    /* Passthrough backends store their trees on a real host filesystem, which
     * must support name_to_handle_at (the linux/io_uring modules derive their
     * file handles with it) -- tmpfs and overlayfs do not, so the usual /tmp
     * session_dir will not do.  Root the backing store at $CHIMERA_MBT_SCRATCH
     * (default: the current directory, which under ctest is the build tree);
     * an unsupported filesystem surfaces as an ENOTSUP mount that fs_setup
     * turns into a clean skip. */
    if (mbt_module_is_passthrough(env->module)) {
        const char *scratch = getenv("CHIMERA_MBT_SCRATCH");
        char       *abs_scratch;

        if (!scratch || !scratch[0]) {
            scratch = ".";
        }
        /* The module opens this path from a server thread, so it must be
         * absolute; resolve $CHIMERA_MBT_SCRATCH (default cwd) to a real path. */
        abs_scratch = realpath(scratch, NULL);
        if (!abs_scratch) {
            fprintf(stderr, "realpath(%s) failed: %s\n", scratch,
                    strerror(errno));
            exit(1);
        }
        snprintf(env->pt_root, sizeof(env->pt_root),
                 "%s/nfs3_mbt_pt_XXXXXX", abs_scratch);
        free(abs_scratch);
        if (!mkdtemp(env->pt_root)) {
            fprintf(stderr, "mkdtemp(%s) failed: %s\n", env->pt_root,
                    strerror(errno));
            exit(1);
        }
    }

    env->server = chimera_server_init(config, env->metrics);

    chimera_server_start(env->server);

    /* Client half: its own evpl loop; the reply callbacks run inside
     * evpl_continue() on this (the only) test thread. */
    env->evpl = evpl_create(NULL);

    NFS_V3_init(&env->nfs_v3);
    NFS_MOUNT_V3_init(&env->mount_v3);
    NFS_V4_init(&env->nfs_v4);
    NFS_V4_CB_init(&env->nfs_v4_cb);
    PORTMAP_V2_init(&env->pm_v2);
    PORTMAP_V3_init(&env->pm_v3);
    PORTMAP_V4_init(&env->pm_v4);
    NLM_V4_init(&env->nlm_v4);
    SM_INTER_V1_init(&env->nsm_v1);

    programs[0] = &env->nfs_v3.rpc2;
    programs[1] = &env->mount_v3.rpc2;
    programs[2] = &env->nfs_v4.rpc2;
    programs[3] = &env->nfs_v4_cb.rpc2;
    programs[4] = &env->pm_v2.rpc2;
    programs[5] = &env->pm_v3.rpc2;
    programs[6] = &env->pm_v4.rpc2;
    programs[7] = &env->nlm_v4.rpc2;
    programs[8] = &env->nsm_v1.rpc2;

    env->rpc2_thread = evpl_rpc2_thread_init(env->evpl, programs, 9,
                                             NULL, NULL);

    /* Endpoint names must match what the server derived from its ports
     * (chimera-inproc-<port>); build them through the same helper. */
    nfs_ep = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                                "127.0.0.1", 2049);
    mount_ep = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                                  "127.0.0.1", MBT_MOUNT_PORT);

    env->nfs_conn = evpl_rpc2_client_connect(env->rpc2_thread,
                                             EVPL_STREAM_INPROC,
                                             nfs_ep, NULL, 0, NULL);
    env->mount_conn = evpl_rpc2_client_connect(env->rpc2_thread,
                                               EVPL_STREAM_INPROC,
                                               mount_ep, NULL, 0, NULL);

    if (!env->nfs_conn || !env->mount_conn) {
        fprintf(stderr, "failed to connect to in-process server\n");
        exit(1);
    }

    if (aux) {
        /* The NLM connection also SERVES NLM_V4: the asynchronous half of the
         * protocol (NLMPROC4_*_MSG) is answered by the server calling
         * NLMPROC4_*_RES back on this very connection, so the client end has
         * to export the program to receive them.  env rides along as the
         * dispatch private_data. */
        static struct evpl_rpc2_program *nlm_srv[1];
        struct evpl_endpoint            *pm_ep, *nlm_ep, *nsm_ep;

        pm_ep = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                                   "127.0.0.1",
                                                   MBT_PORTMAP_PORT);
        nlm_ep = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                                    "127.0.0.1", MBT_NLM_PORT);
        nsm_ep = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                                    "127.0.0.1", MBT_NSM_PORT);

        env->portmap_conn = evpl_rpc2_client_connect(env->rpc2_thread,
                                                     EVPL_STREAM_INPROC,
                                                     pm_ep, NULL, 0, NULL);
        nlm_srv[0]    = &env->nlm_v4.rpc2;
        env->nlm_conn = evpl_rpc2_client_connect(env->rpc2_thread,
                                                 EVPL_STREAM_INPROC,
                                                 nlm_ep, nlm_srv, 1, env);
        env->nsm_conn = evpl_rpc2_client_connect(env->rpc2_thread,
                                                 EVPL_STREAM_INPROC,
                                                 nsm_ep, NULL, 0, NULL);

        if (!env->portmap_conn || !env->nlm_conn || !env->nsm_conn) {
            fprintf(stderr, "failed to connect to in-process aux services\n");
            exit(1);
        }
    }

    /* AUTH_SYS root credential (uid 0, gid 0) so access
     * decisions and attr ownership are identical on both harnesses. */
    env->cred.flavor                  = EVPL_RPC2_AUTH_SYS;
    env->cred.authsys.uid             = 0;
    env->cred.authsys.gid             = 0;
    env->cred.authsys.num_gids        = 0;
    env->cred.authsys.gids            = NULL;
    env->cred.authsys.machinename     = "quintmbt";
    env->cred.authsys.machinename_len = 8;

    env->data_buf = malloc(MBT_MAX_DATA);
} /* mbt_env_open_opts */

/* Per-trace filesystem: stand up a fresh, isolated root, mount it at "share",
 * and export it.  Runs on the already-started server -- the trade that lets one
 * process amortize server/client init across every trace of a batch.
 *
 * mkfs backends get a fresh named filesystem (unique fsname => distinct fsid =>
 * distinct FH mount-id, so stale attr/name/open-cache entries from an earlier
 * trace can never be hit).  Passthrough backends instead get a fresh host
 * subdirectory: a brand-new inode is the passthrough analogue of a fresh fsid,
 * and because the dir is left in place on teardown (only unmounted) its inode
 * is never reused mid-run, so cached FHs likewise cannot collide across
 * traces.  The whole session_dir is reaped at process exit. */
static inline void
mbt_env_fs_setup_as(
    struct mbt_env *env,
    const char     *fsname,
    const char     *mntname)
{
    char path[80];

    snprintf(path, sizeof(path), "/%s", mntname);

    /* Mount and export under the *filesystem's own* name rather than a shared
     * "share", and check every call.
     *
     * A trace that diverges stops mid-replay, and from that point the model's
     * view of the server is by definition wrong -- so the dangling-open sweep,
     * which closes what the model believes is open, cannot close what the
     * server actually has.  The leftover handle keeps the filesystem
     * referenced, umount reports EBUSY and (correctly) leaves the mount
     * registered.  With one shared mount name that poisoned the entire rest of
     * the batch: every later trace mounted its own filesystem under the same
     * name, silently got the stuck one instead, and reported its predecessor's
     * leftovers as divergences.  One genuine failure turned into dozens of
     * phantom ones.
     *
     * Per-trace names make a stuck mount inert: it keeps its own name, the
     * next trace's mount cannot collide with it, and the batch goes on
     * reporting only real divergences. */
    if (mbt_module_is_passthrough(env->module)) {
        char dir[300];
        int  mrc;

        snprintf(dir, sizeof(dir), "%s/%s", env->pt_root, fsname);
        /* The model's export root is 0777, owned root:root; create it that way
         * (umask is neutralized in the replayer main) so the passthrough tree
         * starts from the same state as the mkfs backends. */
        if (mkdir(dir, 0777) != 0 && errno != EEXIST) {
            fprintf(stderr, "failed to create %s backing dir %s: %s\n",
                    env->module, dir, strerror(errno));
            exit(1);
        }
        mrc = chimera_server_mount(env->server, path + 1, env->module, dir,
                                   NULL);
        if (mrc == CHIMERA_VFS_ENOTSUP) {
            /* The backing filesystem cannot produce file handles
             * (name_to_handle_at): tmpfs/overlayfs, e.g. a /tmp or overlay
             * build tree.  Skip rather than fail -- point CHIMERA_MBT_SCRATCH
             * at an ext4/xfs/btrfs path to actually exercise the backend.
             * _exit() to bypass evpl's atexit leak-check, which would abort on
             * the buffers the failed mount left pinned. */
            fprintf(stderr, "SKIP: %s backend needs a name_to_handle_at-capable "
                    "scratch fs; %s is not one (set CHIMERA_MBT_SCRATCH)\n",
                    env->module, dir);
            _exit(77);
        }
        if (mrc != 0) {
            fprintf(stderr, "mount %s at %s failed: status=%d\n",
                    env->module, dir, mrc);
            _exit(1);
        }
    } else {
        if (chimera_server_mkfs(env->server, env->module, fsname, NULL) != 0) {
            fprintf(stderr, "failed to create %s filesystem %s\n", env->module,
                    fsname);
            exit(1);
        }
        if (chimera_server_mount(env->server, path + 1, env->module, fsname,
                                 NULL) != 0) {
            fprintf(stderr, "failed to mount %s filesystem %s\n", env->module,
                    fsname);
            exit(1);
        }
    }
    if (chimera_server_create_export(env->server, path, path, 0, NULL) != 0) {
        fprintf(stderr, "failed to create %s export\n", path);
        exit(1);
    }
} /* mbt_env_fs_setup_as */

/* The common case: mount and export the filesystem under its own name, so a
 * batch gets one mount per trace. */
static inline void
mbt_env_fs_setup(
    struct mbt_env *env,
    const char     *fsname)
{
    mbt_env_fs_setup_as(env, fsname, fsname);
} /* mbt_env_fs_setup */

/* Tear the per-trace filesystem back down.  Order matters: rmfs is EBUSY while
 * the fs still has a mount, so unmount (and drop the export) first. */
static inline void
mbt_env_fs_teardown_as(
    struct mbt_env *env,
    const char     *fsname,
    const char     *mntname)
{
    int  tries = 0;
    int  rc;

    char path[80];

    snprintf(path, sizeof(path), "/%s", mntname);

    chimera_server_remove_export(env->server, path);

    /* The unmount status was previously discarded, and that is what made a
     * batch run nondeterministic.  Every trace mounts its filesystem under the
     * same name, so an unmount that reports EBUSY -- which also leaves the
     * mount in the table -- means the next trace gets its predecessor's
     * filesystem and reports the leftovers as divergences, attributed to
     * whichever trace happened to follow a slow drain.  Across five identical
     * runs that produced 0, 16, 42, 42 and 48 divergences.
     *
     * umount blocks until the mount is genuinely idle (it purges the cached
     * opens and issues the backend closes itself), so a failure here is a real
     * leak, not a timing hiccup to retry around: something still holds a
     * reference after the configured 30s.  Fail loudly -- a hard stop is
     * debuggable, a corpus silently replayed against the wrong filesystem is
     * not. */
    rc = chimera_server_unmount(env->server, path + 1);
    if (rc != 0) {
        fprintf(stderr,
                "warning: unmount of %s returned %d -- a handle is still "
                "open, most likely because a diverging trace stopped before "
                "the model knew what to close.  The mount keeps its own name, "
                "so later traces are unaffected.\n", fsname, rc);
        return;
    }

    /* Passthrough backends have no filesystem to remove; the unmounted host
     * dir is intentionally left in place (its inode must not be reused
     * mid-run, see mbt_env_fs_setup) and is reaped with the session_dir.
     * Unlike a leaked named fs, an unmounted host dir is inert -- the VFS does
     * not walk it -- so there is no quadratic cost. */
    if (mbt_module_is_passthrough(env->module)) {
        return;
    }

    /* nfs3 is stateless, so rmfs is free once the mount is gone.  nfs4 leaves
     * the just-closed opens (v4_close_dangling_opens) draining on the server's
     * async close thread, so the fs can stay busy for a short window; retry
     * until it drains rather than leaking the filesystem (which would slow a
     * long corpus quadratically).
     *
     * Giving up here used to be a warning that carried on with the filesystem
     * still alive, which is how a slow drain turned into unexplained
     * divergences in some *later* trace.  A leaked fs makes every subsequent
     * result unreliable, so fail loudly instead: a hard stop is debuggable, a
     * corrupted corpus is not. */
    while (chimera_server_rmfs(env->server, env->module, fsname) != 0) {
        if (++tries >= 30000) {
            fprintf(stderr,
                    "fatal: rmfs %s never drained after %d retries\n",
                    fsname, tries);
            exit(1);
        }
        usleep(1000);
    }
} /* mbt_env_fs_teardown_as */

static inline void
mbt_env_fs_teardown(
    struct mbt_env *env,
    const char     *fsname)
{
    mbt_env_fs_teardown_as(env, fsname, fsname);
} /* mbt_env_fs_teardown */

/* Backward-compatible one-shot setup used by the single-trace probes: open the
 * shared server/client, then create + mount a default fs0. */
static inline void
mbt_env_start_opts(
    struct mbt_env            *env,
    const struct mbt_env_opts *opts)
{
    mbt_env_open_opts(env, opts);
    mbt_env_fs_setup(env, "fs0");
} /* mbt_env_start_opts */

static inline void
mbt_env_start(struct mbt_env *env)
{
    mbt_env_start_opts(env, NULL);
} /* mbt_env_start */

static inline void
mbt_env_stop(struct mbt_env *env)
{
    char cmd[300];

    if (env->portmap_conn) {
        evpl_rpc2_client_disconnect(env->rpc2_thread, env->portmap_conn);
    }
    if (env->nlm_conn) {
        evpl_rpc2_client_disconnect(env->rpc2_thread, env->nlm_conn);
    }
    if (env->nsm_conn) {
        evpl_rpc2_client_disconnect(env->rpc2_thread, env->nsm_conn);
    }
    evpl_rpc2_client_disconnect(env->rpc2_thread, env->nfs_conn);
    evpl_rpc2_client_disconnect(env->rpc2_thread, env->mount_conn);
    evpl_rpc2_thread_destroy(env->rpc2_thread);
    evpl_destroy(env->evpl);

    chimera_server_destroy(env->server);
    prometheus_metrics_destroy(env->metrics);

    free(env->data_buf);

    snprintf(cmd, sizeof(cmd), "rm -rf %s", env->session_dir);
    if (system(cmd) != 0) {
        fprintf(stderr, "warning: failed to remove %s\n", env->session_dir);
    }
} /* mbt_env_stop */

static inline struct mbt_result *
mbt_call_begin(struct mbt_env *env)
{
    uint8_t *data_buf = env->data_buf;

    memset(&env->res, 0, sizeof(env->res));
    env->res.data = data_buf;
    return &env->res;
} /* mbt_call_begin */

static inline void
mbt_call_wait(struct mbt_env *env)
{
    while (!env->res.done) {
        evpl_continue(env->evpl);
    }
    /* A transport-level failure in an in-process exchange is a harness or
     * server bug, never a modeled outcome -- treat it like the python
     * harness treats a socket error: die, don't let status 0 read as OK. */
    if (env->res.rpc_err != 0) {
        fprintf(stderr, "rpc2 transport error %d\n", env->res.rpc_err);
        exit(3);
    }
} /* mbt_call_wait */

/* Same pump, but an RPC-level refusal is a modeled outcome rather than a
 * harness bug: the auxiliary suite deliberately calls procedures chimera
 * does not implement and expects PROC_UNAVAIL (accept_stat 3) back.  A
 * negative status still means the transport itself failed (see
 * EVPL_RPC2_REPLY_* in evpl_rpc2_program.h) and is fatal here too. */
static inline void
mbt_call_wait_soft(struct mbt_env *env)
{
    while (!env->res.done) {
        evpl_continue(env->evpl);
    }
    if (env->res.rpc_err < 0) {
        fprintf(stderr, "rpc2 transport error %d\n", env->res.rpc_err);
        exit(3);
    }
} /* mbt_call_wait_soft */

/* ---- MOUNT --------------------------------------------------------------- */

static void
mbt_mnt_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct mountres3            *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->fhs_status;
        if (reply->fhs_status == MNT3_OK) {
            mbt_copy_fh(&env->res.obj_fh, &reply->mountinfo.fhandle);
        }
    }
    env->res.done = 1;
} /* mbt_mnt_cb */

static inline struct mbt_result *
mbt_mnt(
    struct mbt_env *env,
    const char     *path)
{
    struct mountarg3 args;

    mbt_call_begin(env);
    xdr_set_str_static(&args, path, path, (uint32_t) strlen(path));
    env->mount_v3.send_call_MOUNTPROC3_MNT(&env->mount_v3.rpc2, env->evpl,
                                           env->mount_conn, &env->cred, &args,
                                           0, 0, NULL, 0, 0, mbt_mnt_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_mnt */

/* ---- NFS3 NULL ----------------------------------------------------------- */

static void
mbt_null_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    env->res.done    = 1;
} /* mbt_null_cb */

static inline struct mbt_result *
mbt_null(struct mbt_env *env)
{
    mbt_call_begin(env);
    env->nfs_v3.send_call_NFSPROC3_NULL(&env->nfs_v3.rpc2, env->evpl,
                                        env->nfs_conn, &env->cred,
                                        0, 0, NULL, 0, 0, mbt_null_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_null */

/* ---- GETATTR ------------------------------------------------------------- */

static void
mbt_getattr_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct GETATTR3res          *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            env->res.obj_attrs.has = 1;
            env->res.obj_attrs.a   = reply->resok.obj_attributes;
        }
    }
    env->res.done = 1;
} /* mbt_getattr_cb */

static inline struct mbt_result *
mbt_getattr(
    struct mbt_env      *env,
    const struct mbt_fh *fh)
{
    struct GETATTR3args args;

    mbt_call_begin(env);
    args.object.data.data = (void *) fh->data;
    args.object.data.len  = fh->len;
    env->nfs_v3.send_call_NFSPROC3_GETATTR(&env->nfs_v3.rpc2, env->evpl,
                                           env->nfs_conn, &env->cred, &args,
                                           0, 0, NULL, 0, 0,
                                           mbt_getattr_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_getattr */

/* ---- LOOKUP -------------------------------------------------------------- */

static void
mbt_lookup_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct LOOKUP3res           *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            mbt_copy_fh(&env->res.obj_fh, &reply->resok.object.data);
            mbt_copy_post_op_attr(&env->res.obj_attrs,
                                  &reply->resok.obj_attributes);
        }
    }
    env->res.done = 1;
} /* mbt_lookup_cb */

static inline struct mbt_result *
mbt_lookup(
    struct mbt_env      *env,
    const struct mbt_fh *dir,
    const char          *name,
    uint32_t             name_len)
{
    struct LOOKUP3args args;

    mbt_call_begin(env);
    args.what.dir.data.data = (void *) dir->data;
    args.what.dir.data.len  = dir->len;
    args.what.name.str      = (char *) name;
    args.what.name.len      = name_len;
    env->nfs_v3.send_call_NFSPROC3_LOOKUP(&env->nfs_v3.rpc2, env->evpl,
                                          env->nfs_conn, &env->cred, &args,
                                          0, 0, NULL, 0, 0,
                                          mbt_lookup_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_lookup */

/* ---- CREATE / MKDIR / SYMLINK / MKNOD (shared reply shape) --------------- */

static inline void
mbt_sattr3_default(struct sattr3 *sattr)
{
    memset(sattr, 0, sizeof(*sattr));
    sattr->mode.set_it  = 0;
    sattr->uid.set_it   = 0;
    sattr->gid.set_it   = 0;
    sattr->size.set_it  = 0;
    sattr->atime.set_it = DONT_CHANGE;
    sattr->mtime.set_it = DONT_CHANGE;
} /* mbt_sattr3_default */

static void
mbt_create_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct CREATE3res           *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            mbt_copy_post_op_fh(&env->res.obj_fh, &reply->resok.obj);
            mbt_copy_post_op_attr(&env->res.obj_attrs,
                                  &reply->resok.obj_attributes);
        }
    }
    env->res.done = 1;
} /* mbt_create_cb */

static inline struct mbt_result *
mbt_create(
    struct mbt_env      *env,
    const struct mbt_fh *dir,
    const char          *name,
    uint32_t             name_len,
    createmode3          cmode,
    int                  mode,         /* < 0: omit */
    const uint8_t       *verf8)        /* EXCLUSIVE only */
{
    struct CREATE3args args;

    mbt_call_begin(env);
    args.where.dir.data.data = (void *) dir->data;
    args.where.dir.data.len  = dir->len;
    args.where.name.str      = (char *) name;
    args.where.name.len      = name_len;
    args.how.mode            = cmode;
    if (cmode == EXCLUSIVE) {
        memcpy(args.how.verf, verf8, NFS3_CREATEVERFSIZE);
    } else {
        mbt_sattr3_default(&args.how.obj_attributes);
        if (mode >= 0) {
            args.how.obj_attributes.mode.set_it = 1;
            args.how.obj_attributes.mode.mode   = (uint32_t) mode;
        }
    }
    env->nfs_v3.send_call_NFSPROC3_CREATE(&env->nfs_v3.rpc2, env->evpl,
                                          env->nfs_conn, &env->cred, &args,
                                          0, 0, NULL, 0, 0,
                                          mbt_create_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_create */

static void
mbt_mkdir_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct MKDIR3res            *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            mbt_copy_post_op_fh(&env->res.obj_fh, &reply->resok.obj);
            mbt_copy_post_op_attr(&env->res.obj_attrs,
                                  &reply->resok.obj_attributes);
        }
    }
    env->res.done = 1;
} /* mbt_mkdir_cb */

static inline struct mbt_result *
mbt_mkdir(
    struct mbt_env      *env,
    const struct mbt_fh *dir,
    const char          *name,
    uint32_t             name_len,
    int                  mode)
{
    struct MKDIR3args args;

    mbt_call_begin(env);
    args.where.dir.data.data = (void *) dir->data;
    args.where.dir.data.len  = dir->len;
    args.where.name.str      = (char *) name;
    args.where.name.len      = name_len;
    mbt_sattr3_default(&args.attributes);
    if (mode >= 0) {
        args.attributes.mode.set_it = 1;
        args.attributes.mode.mode   = (uint32_t) mode;
    }
    env->nfs_v3.send_call_NFSPROC3_MKDIR(&env->nfs_v3.rpc2, env->evpl,
                                         env->nfs_conn, &env->cred, &args,
                                         0, 0, NULL, 0, 0,
                                         mbt_mkdir_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_mkdir */

static void
mbt_symlink_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct SYMLINK3res          *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            mbt_copy_post_op_fh(&env->res.obj_fh, &reply->resok.obj);
            mbt_copy_post_op_attr(&env->res.obj_attrs,
                                  &reply->resok.obj_attributes);
        }
    }
    env->res.done = 1;
} /* mbt_symlink_cb */

static inline struct mbt_result *
mbt_symlink(
    struct mbt_env      *env,
    const struct mbt_fh *dir,
    const char          *name,
    uint32_t             name_len,
    const char          *target,
    int                  mode)
{
    struct SYMLINK3args args;

    mbt_call_begin(env);
    args.where.dir.data.data = (void *) dir->data;
    args.where.dir.data.len  = dir->len;
    args.where.name.str      = (char *) name;
    args.where.name.len      = name_len;
    mbt_sattr3_default(&args.symlink.symlink_attributes);
    if (mode >= 0) {
        args.symlink.symlink_attributes.mode.set_it = 1;
        args.symlink.symlink_attributes.mode.mode   = (uint32_t) mode;
    }
    args.symlink.symlink_data.str = (char *) target;
    args.symlink.symlink_data.len = (uint32_t) strlen(target);
    env->nfs_v3.send_call_NFSPROC3_SYMLINK(&env->nfs_v3.rpc2, env->evpl,
                                           env->nfs_conn, &env->cred, &args,
                                           0, 0, NULL, 0, 0,
                                           mbt_symlink_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_symlink */

static void
mbt_mknod_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct MKNOD3res            *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            mbt_copy_post_op_fh(&env->res.obj_fh, &reply->resok.obj);
            mbt_copy_post_op_attr(&env->res.obj_attrs,
                                  &reply->resok.obj_attributes);
        }
    }
    env->res.done = 1;
} /* mbt_mknod_cb */

static inline struct mbt_result *
mbt_mknod(
    struct mbt_env      *env,
    const struct mbt_fh *dir,
    const char          *name,
    uint32_t             name_len,
    ftype3               ftype,
    int                  mode)
{
    struct MKNOD3args args;

    mbt_call_begin(env);
    args.where.dir.data.data = (void *) dir->data;
    args.where.dir.data.len  = dir->len;
    args.where.name.str      = (char *) name;
    args.where.name.len      = name_len;
    args.what.type           = ftype;
    mbt_sattr3_default(&args.what.pipe_attributes);
    if (mode >= 0) {
        args.what.pipe_attributes.mode.set_it = 1;
        args.what.pipe_attributes.mode.mode   = (uint32_t) mode;
    }
    env->nfs_v3.send_call_NFSPROC3_MKNOD(&env->nfs_v3.rpc2, env->evpl,
                                         env->nfs_conn, &env->cred, &args,
                                         0, 0, NULL, 0, 0,
                                         mbt_mknod_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_mknod */

/* ---- SETATTR ------------------------------------------------------------- */

static void
mbt_setattr_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct SETATTR3res          *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            mbt_copy_post_op_attr(&env->res.wcc_after,
                                  &reply->resok.obj_wcc.after);
        }
    }
    env->res.done = 1;
} /* mbt_setattr_cb */

static inline struct mbt_result *
mbt_setattr(
    struct mbt_env        *env,
    const struct mbt_fh   *fh,
    int                    mode,        /* < 0: omit */
    int64_t                size,        /* < 0: omit */
    const struct nfstime3 *guard_ctime) /* NULL: no guard */
{
    struct SETATTR3args args;

    mbt_call_begin(env);
    args.object.data.data = (void *) fh->data;
    args.object.data.len  = fh->len;
    mbt_sattr3_default(&args.new_attributes);
    if (mode >= 0) {
        args.new_attributes.mode.set_it = 1;
        args.new_attributes.mode.mode   = (uint32_t) mode;
    }
    if (size >= 0) {
        args.new_attributes.size.set_it = 1;
        args.new_attributes.size.size   = (uint64_t) size;
    }
    if (guard_ctime) {
        args.guard.check     = 1;
        args.guard.obj_ctime = *guard_ctime;
    } else {
        args.guard.check = 0;
    }
    env->nfs_v3.send_call_NFSPROC3_SETATTR(&env->nfs_v3.rpc2, env->evpl,
                                           env->nfs_conn, &env->cred, &args,
                                           0, 0, NULL, 0, 0,
                                           mbt_setattr_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_setattr */

/* ---- ACCESS -------------------------------------------------------------- */

static void
mbt_access_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct ACCESS3res           *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            env->res.access = reply->resok.access;
            mbt_copy_post_op_attr(&env->res.obj_attrs,
                                  &reply->resok.obj_attributes);
        }
    }
    env->res.done = 1;
} /* mbt_access_cb */

static inline struct mbt_result *
mbt_access(
    struct mbt_env      *env,
    const struct mbt_fh *fh,
    uint32_t             mask)
{
    struct ACCESS3args args;

    mbt_call_begin(env);
    args.object.data.data = (void *) fh->data;
    args.object.data.len  = fh->len;
    args.access           = mask;
    env->nfs_v3.send_call_NFSPROC3_ACCESS(&env->nfs_v3.rpc2, env->evpl,
                                          env->nfs_conn, &env->cred, &args,
                                          0, 0, NULL, 0, 0,
                                          mbt_access_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_access */

/* ---- READLINK ------------------------------------------------------------ */

static void
mbt_readlink_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct READLINK3res         *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;
    uint32_t        n;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            n = reply->resok.data.len;
            if (n >= sizeof(env->res.target)) {
                n = sizeof(env->res.target) - 1;
            }
            memcpy(env->res.target, reply->resok.data.str, n);
            env->res.target[n]  = '\0';
            env->res.target_len = n;
            mbt_copy_post_op_attr(&env->res.obj_attrs,
                                  &reply->resok.symlink_attributes);
        }
    }
    env->res.done = 1;
} /* mbt_readlink_cb */

static inline struct mbt_result *
mbt_readlink(
    struct mbt_env      *env,
    const struct mbt_fh *fh)
{
    struct READLINK3args args;

    mbt_call_begin(env);
    args.symlink.data.data = (void *) fh->data;
    args.symlink.data.len  = fh->len;
    env->nfs_v3.send_call_NFSPROC3_READLINK(&env->nfs_v3.rpc2, env->evpl,
                                            env->nfs_conn, &env->cred, &args,
                                            0, 0, NULL, 0, 0,
                                            mbt_readlink_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_readlink */

/* ---- WRITE / READ / COMMIT ----------------------------------------------- */

static void
mbt_write_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct WRITE3res            *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            env->res.count     = reply->resok.count;
            env->res.committed = reply->resok.committed;
            memcpy(env->res.verf, reply->resok.verf, NFS3_WRITEVERFSIZE);
            mbt_copy_post_op_attr(&env->res.wcc_after,
                                  &reply->resok.file_wcc.after);
        }
    }
    env->res.done = 1;
} /* mbt_write_cb */

static inline struct mbt_result *
mbt_write(
    struct mbt_env      *env,
    const struct mbt_fh *fh,
    uint64_t             offset,
    const uint8_t       *data,
    uint32_t             len,
    stable_how           stable)
{
    struct WRITE3args args;
    struct evpl_iovec iov;

    mbt_call_begin(env);

    if (evpl_iovec_alloc(env->evpl, len, 4096, 1, 0, &iov) < 0) {
        fprintf(stderr, "evpl_iovec_alloc(%u) failed\n", len);
        exit(1);
    }
    memcpy(iov.data, data, len);
    iov.length = len;

    args.file.data.data = (void *) fh->data;
    args.file.data.len  = fh->len;
    args.offset         = offset;
    args.count          = len;
    args.stable         = stable;
    args.data.iov       = &iov;
    args.data.niov      = 1;
    args.data.length    = len;

    /* Marshalling MOVES the data iovec (xdr_iovec_move_private), so
     * ownership passes to the rpc2 layer here -- no release on our side. */
    env->nfs_v3.send_call_NFSPROC3_WRITE(&env->nfs_v3.rpc2, env->evpl,
                                         env->nfs_conn, &env->cred, &args,
                                         0, 0, NULL, 0, 0,
                                         mbt_write_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_write */

static void
mbt_read_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct READ3res             *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;
    uint32_t        off = 0;
    uint32_t        n;
    int             i;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            env->res.count = reply->resok.count;
            env->res.eof   = reply->resok.eof != 0;
            mbt_copy_post_op_attr(&env->res.obj_attrs,
                                  &reply->resok.file_attributes);
            /* The reply's data iovecs are handed to this callback WITH
             * their references (the vfs/nfs proxy passes them on to the
             * VFS layer, which releases them); we only copy, so release
             * them here or evpl's teardown leak check aborts. */
            for (i = 0; i < reply->resok.data.niov; i++) {
                n = reply->resok.data.iov[i].length;
                if (off + n > MBT_MAX_DATA) {
                    n = MBT_MAX_DATA - off;
                }
                memcpy(env->res.data + off,
                       reply->resok.data.iov[i].data, n);
                off += n;
                evpl_iovec_release(evpl, &reply->resok.data.iov[i]);
            }
            env->res.data_len = off;
        }
    }
    env->res.done = 1;
} /* mbt_read_cb */

static inline struct mbt_result *
mbt_read(
    struct mbt_env      *env,
    const struct mbt_fh *fh,
    uint64_t             offset,
    uint32_t             count)
{
    struct READ3args args;

    mbt_call_begin(env);
    args.file.data.data = (void *) fh->data;
    args.file.data.len  = fh->len;
    args.offset         = offset;
    args.count          = count;
    env->nfs_v3.send_call_NFSPROC3_READ(&env->nfs_v3.rpc2, env->evpl,
                                        env->nfs_conn, &env->cred, &args,
                                        0, 0, NULL, 0, 0,
                                        mbt_read_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_read */

static void
mbt_commit_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMMIT3res           *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            memcpy(env->res.verf, reply->resok.verf, NFS3_WRITEVERFSIZE);
        }
    }
    env->res.done = 1;
} /* mbt_commit_cb */

static inline struct mbt_result *
mbt_commit(
    struct mbt_env      *env,
    const struct mbt_fh *fh)
{
    struct COMMIT3args args;

    mbt_call_begin(env);
    args.file.data.data = (void *) fh->data;
    args.file.data.len  = fh->len;
    args.offset         = 0;
    args.count          = 0;
    env->nfs_v3.send_call_NFSPROC3_COMMIT(&env->nfs_v3.rpc2, env->evpl,
                                          env->nfs_conn, &env->cred, &args,
                                          0, 0, NULL, 0, 0,
                                          mbt_commit_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_commit */

/* ---- REMOVE / RMDIR / RENAME / LINK -------------------------------------- */

static void
mbt_remove_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct REMOVE3res           *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
    }
    env->res.done = 1;
} /* mbt_remove_cb */

static inline struct mbt_result *
mbt_remove(
    struct mbt_env      *env,
    const struct mbt_fh *dir,
    const char          *name,
    uint32_t             name_len)
{
    struct REMOVE3args args;

    mbt_call_begin(env);
    args.object.dir.data.data = (void *) dir->data;
    args.object.dir.data.len  = dir->len;
    args.object.name.str      = (char *) name;
    args.object.name.len      = name_len;
    env->nfs_v3.send_call_NFSPROC3_REMOVE(&env->nfs_v3.rpc2, env->evpl,
                                          env->nfs_conn, &env->cred, &args,
                                          0, 0, NULL, 0, 0,
                                          mbt_remove_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_remove */

static void
mbt_rmdir_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct RMDIR3res            *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
    }
    env->res.done = 1;
} /* mbt_rmdir_cb */

static inline struct mbt_result *
mbt_rmdir(
    struct mbt_env      *env,
    const struct mbt_fh *dir,
    const char          *name,
    uint32_t             name_len)
{
    struct RMDIR3args args;

    mbt_call_begin(env);
    args.object.dir.data.data = (void *) dir->data;
    args.object.dir.data.len  = dir->len;
    args.object.name.str      = (char *) name;
    args.object.name.len      = name_len;
    env->nfs_v3.send_call_NFSPROC3_RMDIR(&env->nfs_v3.rpc2, env->evpl,
                                         env->nfs_conn, &env->cred, &args,
                                         0, 0, NULL, 0, 0,
                                         mbt_rmdir_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_rmdir */

static void
mbt_rename_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct RENAME3res           *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
    }
    env->res.done = 1;
} /* mbt_rename_cb */

static inline struct mbt_result *
mbt_rename(
    struct mbt_env      *env,
    const struct mbt_fh *from_dir,
    const char          *from_name,
    uint32_t             from_name_len,
    const struct mbt_fh *to_dir,
    const char          *to_name,
    uint32_t             to_name_len)
{
    struct RENAME3args args;

    mbt_call_begin(env);
    args.from.dir.data.data = (void *) from_dir->data;
    args.from.dir.data.len  = from_dir->len;
    args.from.name.str      = (char *) from_name;
    args.from.name.len      = from_name_len;
    args.to.dir.data.data   = (void *) to_dir->data;
    args.to.dir.data.len    = to_dir->len;
    args.to.name.str        = (char *) to_name;
    args.to.name.len        = to_name_len;
    env->nfs_v3.send_call_NFSPROC3_RENAME(&env->nfs_v3.rpc2, env->evpl,
                                          env->nfs_conn, &env->cred, &args,
                                          0, 0, NULL, 0, 0,
                                          mbt_rename_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_rename */

static void
mbt_link_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct LINK3res             *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            mbt_copy_post_op_attr(&env->res.obj_attrs,
                                  &reply->resok.file_attributes);
        }
    }
    env->res.done = 1;
} /* mbt_link_cb */

static inline struct mbt_result *
mbt_link(
    struct mbt_env      *env,
    const struct mbt_fh *fh,
    const struct mbt_fh *dir,
    const char          *name,
    uint32_t             name_len)
{
    struct LINK3args args;

    mbt_call_begin(env);
    args.file.data.data     = (void *) fh->data;
    args.file.data.len      = fh->len;
    args.link.dir.data.data = (void *) dir->data;
    args.link.dir.data.len  = dir->len;
    args.link.name.str      = (char *) name;
    args.link.name.len      = name_len;
    env->nfs_v3.send_call_NFSPROC3_LINK(&env->nfs_v3.rpc2, env->evpl,
                                        env->nfs_conn, &env->cred, &args,
                                        0, 0, NULL, 0, 0,
                                        mbt_link_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_link */

/* ---- READDIR / READDIRPLUS ----------------------------------------------- */

static inline void
mbt_copy_entry_name(
    struct mbt_entry *dst,
    const xdr_string *name)
{
    dst->name_len = name->len < MBT_NAME_MAX - 1 ? name->len
                                                 : MBT_NAME_MAX - 1;
    memcpy(dst->name, name->str, dst->name_len);
    dst->name[dst->name_len] = '\0';
} /* mbt_copy_entry_name */

static void
mbt_readdir_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct READDIR3res          *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;
    struct entry3  *e;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            env->res.eof = reply->resok.reply.eof != 0;
            for (e = reply->resok.reply.entries; e; e = e->nextentry) {
                if (env->res.nentries >= MBT_MAX_ENTRIES) {
                    env->res.entries_overflow = 1;
                    break;
                }
                struct mbt_entry *dst = &env->res.entries[env->res.nentries++];
                dst->fileid = e->fileid;
                dst->cookie = e->cookie;
                mbt_copy_entry_name(dst, &e->name);
            }
        }
    }
    env->res.done = 1;
} /* mbt_readdir_cb */

static inline struct mbt_result *
mbt_readdir(
    struct mbt_env      *env,
    const struct mbt_fh *dir)
{
    struct READDIR3args args;

    mbt_call_begin(env);
    args.dir.data.data = (void *) dir->data;
    args.dir.data.len  = dir->len;
    args.cookie        = 0;
    memset(args.cookieverf, 0, sizeof(args.cookieverf));
    args.count = 65536;
    env->nfs_v3.send_call_NFSPROC3_READDIR(&env->nfs_v3.rpc2, env->evpl,
                                           env->nfs_conn, &env->cred, &args,
                                           0, 0, NULL, 0, 0,
                                           mbt_readdir_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_readdir */

static void
mbt_readdirplus_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct READDIRPLUS3res      *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env    *env = private_data;
    struct entryplus3 *e;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            env->res.eof = reply->resok.reply.eof != 0;
            for (e = reply->resok.reply.entries; e; e = e->nextentry) {
                if (env->res.nentries >= MBT_MAX_ENTRIES) {
                    env->res.entries_overflow = 1;
                    break;
                }
                struct mbt_entry *dst = &env->res.entries[env->res.nentries++];
                dst->fileid = e->fileid;
                dst->cookie = e->cookie;
                mbt_copy_entry_name(dst, &e->name);
                mbt_copy_post_op_attr(&dst->attrs, &e->name_attributes);
                mbt_copy_post_op_fh(&dst->fh, &e->name_handle);
            }
        }
    }
    env->res.done = 1;
} /* mbt_readdirplus_cb */

static inline struct mbt_result *
mbt_readdirplus(
    struct mbt_env      *env,
    const struct mbt_fh *dir)
{
    struct READDIRPLUS3args args;

    mbt_call_begin(env);
    args.dir.data.data = (void *) dir->data;
    args.dir.data.len  = dir->len;
    args.cookie        = 0;
    memset(args.cookieverf, 0, sizeof(args.cookieverf));
    args.dircount = 65536;
    args.maxcount = 1048576;
    env->nfs_v3.send_call_NFSPROC3_READDIRPLUS(&env->nfs_v3.rpc2, env->evpl,
                                               env->nfs_conn, &env->cred,
                                               &args, 0, 0, NULL, 0, 0,
                                               mbt_readdirplus_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_readdirplus */

/* ---- FSSTAT / FSINFO / PATHCONF ------------------------------------------ */

static void
mbt_fsstat_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct FSSTAT3res           *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            env->res.tbytes   = reply->resok.tbytes;
            env->res.fbytes   = reply->resok.fbytes;
            env->res.abytes   = reply->resok.abytes;
            env->res.tfiles   = reply->resok.tfiles;
            env->res.ffiles   = reply->resok.ffiles;
            env->res.afiles   = reply->resok.afiles;
            env->res.invarsec = reply->resok.invarsec;
        }
    }
    env->res.done = 1;
} /* mbt_fsstat_cb */

static inline struct mbt_result *
mbt_fsstat(
    struct mbt_env      *env,
    const struct mbt_fh *fh)
{
    struct FSSTAT3args args;

    mbt_call_begin(env);
    args.fsroot.data.data = (void *) fh->data;
    args.fsroot.data.len  = fh->len;
    env->nfs_v3.send_call_NFSPROC3_FSSTAT(&env->nfs_v3.rpc2, env->evpl,
                                          env->nfs_conn, &env->cred, &args,
                                          0, 0, NULL, 0, 0,
                                          mbt_fsstat_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_fsstat */

static void
mbt_fsinfo_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct FSINFO3res           *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            env->res.rtmax       = reply->resok.rtmax;
            env->res.rtpref      = reply->resok.rtpref;
            env->res.rtmult      = reply->resok.rtmult;
            env->res.wtmax       = reply->resok.wtmax;
            env->res.wtpref      = reply->resok.wtpref;
            env->res.wtmult      = reply->resok.wtmult;
            env->res.dtpref      = reply->resok.dtpref;
            env->res.maxfilesize = reply->resok.maxfilesize;
            env->res.time_delta  = reply->resok.time_delta;
            env->res.properties  = reply->resok.properties;
        }
    }
    env->res.done = 1;
} /* mbt_fsinfo_cb */

static inline struct mbt_result *
mbt_fsinfo(
    struct mbt_env      *env,
    const struct mbt_fh *fh)
{
    struct FSINFO3args args;

    mbt_call_begin(env);
    args.fsroot.data.data = (void *) fh->data;
    args.fsroot.data.len  = fh->len;
    env->nfs_v3.send_call_NFSPROC3_FSINFO(&env->nfs_v3.rpc2, env->evpl,
                                          env->nfs_conn, &env->cred, &args,
                                          0, 0, NULL, 0, 0,
                                          mbt_fsinfo_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_fsinfo */

static void
mbt_pathconf_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct PATHCONF3res         *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->status;
        if (reply->status == NFS3_OK) {
            env->res.linkmax          = reply->resok.linkmax;
            env->res.name_max         = reply->resok.name_max;
            env->res.no_trunc         = reply->resok.no_trunc != 0;
            env->res.chown_restricted = reply->resok.chown_restricted != 0;
            env->res.case_insensitive = reply->resok.case_insensitive != 0;
            env->res.case_preserving  = reply->resok.case_preserving != 0;
        }
    }
    env->res.done = 1;
} /* mbt_pathconf_cb */

static inline struct mbt_result *
mbt_pathconf(
    struct mbt_env      *env,
    const struct mbt_fh *fh)
{
    struct PATHCONF3args args;

    mbt_call_begin(env);
    args.object.data.data = (void *) fh->data;
    args.object.data.len  = fh->len;
    env->nfs_v3.send_call_NFSPROC3_PATHCONF(&env->nfs_v3.rpc2, env->evpl,
                                            env->nfs_conn, &env->cred, &args,
                                            0, 0, NULL, 0, 0,
                                            mbt_pathconf_cb, env);
    mbt_call_wait(env);
    return &env->res;
} /* mbt_pathconf */
