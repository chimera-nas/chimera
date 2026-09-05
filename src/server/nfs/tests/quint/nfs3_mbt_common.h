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
#include "metrics/metrics.h"
#include "prometheus-c.h"
#include "common/mbt_artifacts.h"

#include "nfs3_xdr.h"
#include "nfs4_xdr.h"
#include "nfs_mount_xdr.h"
#include "portmap_xdr.h"
#include "nlm4_xdr.h"
#include "sm_inter_xdr.h"

#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"
#include "evpl/evpl_rpc2_gss.h"

#include "krb5_local.h"

#define MBT_MAX_ENTRIES   512   /* readdir entries copied out per reply */
#define MBT_NAME_MAX      256
#define MBT_MAX_DATA      (4 << 20) /* read payload copy-out bound */

/* Must match NFS_MOUNT_PORT in nfs_external_portmap.h: under inproc the
 * port number is only a service name ("chimera-inproc-20048"), but it
 * still has to be the name the server registered.  The auxiliary services
 * follow the same rule: 111 is hardwired in nfs.c, the other two are the
 * nfs_lockmgr_port / nfs_nsm_port defaults (server.c). */
#define MBT_MOUNT_PORT    20048
#define MBT_PORTMAP_PORT  111
#define MBT_NLM_PORT      32803
#define MBT_NSM_PORT      32765

/* The NFS/RDMA service (mbt_env_opts.rdma), which the server puts on its own
 * endpoint alongside the stream one.  Must match the nfs_rdma_port default in
 * server.c, for the same reason MBT_MOUNT_PORT must match NFS_MOUNT_PORT.
 * Only the NFS service has an RDMA endpoint: MOUNT, NLM, NSM and portmap stay
 * on the stream protocol, exactly as they do over real hardware. */
#define MBT_NFS_RDMA_PORT 20049

/* pNFS topology (see mbt_env_opts.pnfs_num_ds): the data servers are
 * additional chimera servers in THIS process, each on its own inproc
 * service name, so the whole MDS+DS cluster is one test binary with no
 * ports, namespaces or daemons.  Their port numbers only have to avoid
 * the MDS's own services above. */
#define MBT_MAX_DS        4
#define MBT_DS_PORT_BASE  12050

/*
 * The security flavor the client half calls under.
 *
 * AUTH_SYS is what every suite used before this: the credential names a uid
 * and the server believes it.  The krb5 flavors run every call under a real
 * RPCSEC_GSS context instead, established against the server's own acceptor
 * with a mechanism (krb5_local) whose realm lives entirely in this process --
 * no KDC, no keytab file, no network.  What each adds over the last is what
 * rides on the wire: krb5 authenticates the caller, krb5i signs the arguments
 * and results, krb5p seals them.
 *
 * The identity is then the principal's, not the credential's, which is why a
 * trace that varies uid per operation cannot be replayed under one of these
 * (see mbt_sec_allows_cred).
 */
enum mbt_sec {
    MBT_SEC_SYS = 0,
    MBT_SEC_KRB5,
    MBT_SEC_KRB5I,
    MBT_SEC_KRB5P,
};

/* The MEMORY: keytab krb5_local populates.  MIT krb5 keys these by name
 * within a process, so naming the same one here is what lets the server's own
 * acceptor resolve the key the test's realm minted. */
#define MBT_KRB5_KEYTAB    "MEMORY:evpl_krb5_local"

/* A two-component principal, which chimera's NFS server maps to root -- the
 * identity every trace's default credential already assumes.  A one-component
 * name would squash to anonymous and no trace would pass. */
#define MBT_KRB5_PRINCIPAL "nfs/localhost"

static inline const char *
mbt_sec_name(enum mbt_sec sec)
{
    switch (sec) {
        case MBT_SEC_KRB5:  return "krb5";
        case MBT_SEC_KRB5I: return "krb5i";
        case MBT_SEC_KRB5P: return "krb5p";
        default:            return "sys";
    } /* switch */
} /* mbt_sec_name */

/* Parse a --sec= value; -1 for one this harness does not know. */
static inline int
mbt_sec_parse(const char *name)
{
    if (!name || !strcmp(name, "sys") || !strcmp(name, "auth_sys")) {
        return MBT_SEC_SYS;
    }
    if (!strcmp(name, "krb5")) {
        return MBT_SEC_KRB5;
    }
    if (!strcmp(name, "krb5i")) {
        return MBT_SEC_KRB5I;
    }
    if (!strcmp(name, "krb5p")) {
        return MBT_SEC_KRB5P;
    }
    return -1;
} /* mbt_sec_parse */

/*
 * Pick "--sec <flavor>" out of a probe's argv.
 *
 * The probes parse their few options by hand rather than through getopt, so
 * this is a scan rather than an option table; it exits on a name it does not
 * know, because silently running AUTH_SYS when the cell asked for krb5p would
 * report coverage the run never had.
 */
static inline int
mbt_sec_scan_argv(
    int    argc,
    char **argv)
{
    int i, sec;

    for (i = 1; i + 1 < argc; i++) {
        if (strcmp(argv[i], "--sec") && strcmp(argv[i], "-S")) {
            continue;
        }

        sec = mbt_sec_parse(argv[i + 1]);

        if (sec < 0) {
            fprintf(stderr, "%s: unknown security flavor '%s'\n", argv[0],
                    argv[i + 1]);
            exit(2);
        }

        return sec;
    }

    return MBT_SEC_SYS;
} /* mbt_sec_scan_argv */

static inline uint32_t
mbt_sec_service(enum mbt_sec sec)
{
    switch (sec) {
        case MBT_SEC_KRB5I: return EVPL_RPC2_GSS_SVC_INTEGRITY;
        case MBT_SEC_KRB5P: return EVPL_RPC2_GSS_SVC_PRIVACY;
        default:            return EVPL_RPC2_GSS_SVC_NONE;
    } /* switch */
} /* mbt_sec_service */

/* Connections a suite opens beyond the env's own; see mbt_env.conn_creds. */
#define MBT_MAX_CONN_CREDS 32

struct mbt_conn_cred {
    struct evpl_rpc2_conn       *conn;
    struct evpl_rpc2_gss_client *gss;
    struct evpl_rpc2_cred        cred;
};

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
    int         smb_named_streams; /* NFSv4 OPENATTR / named attributes: off by
                                    * default (the shared knob gates OPENATTR) */
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
    /* Carry NFS over RPC-over-RDMA instead of plain RPC record marking.
     *
     * The inproc transport has two protocols: STREAM_INPROC, and
     * DATAGRAM_INPROC which reports itself RDMA-capable and performs real
     * one-sided transfers against a registered-memory table (see
     * evpl_inproc_rdma_xfer).  rpc2 keys its framing off evpl_bind_is_rdma(),
     * so pointing the client at the server's RDMA endpoint exercises the read
     * and write chunk paths -- the RDMA framing is genuinely under test here,
     * not emulated away. */
    int         rdma;
    /* server.portmap_hostname: when set, portmap universal addresses are
     * built from this host instead of the connection's local address.  The
     * aux suite uses it to make the uaddr predictable (the inproc local
     * address is not an IP). */
    const char *portmap_hostname;
    int         pnfs_num_ds;      /* >0: run as a pNFS metadata server with
                                   * this many in-process data servers */
    const char *pnfs_ds_module;   /* DS backend (default "memfs") */
    /* REST API and Prometheus scrape endpoint, for the control-plane suite:
     * both follow the server's transport flavor, so under inproc these are
     * endpoint names rather than bound ports, reachable from this process by
     * an http agent on env->evpl (see server/rest/tests/quint/ctl_http.h).
     * 0 leaves the endpoint off, which is what every other suite wants. */
    int         rest_port;
    int         metrics_port;
    /* REST bearer/basic authentication.  Only meaningful with rest_port. */
    int         rest_auth;
    /* The other two protocol servers.  Off by default (the NFS suites want
     * nothing else listening); the control-plane suite turns them on because
     * shares and buckets cannot be created on a server whose SMB or S3
     * protocol was never initialized. */
    int         smb_enabled;
    int         s3_enabled;
    /* common.umount_timeout_ms: how long umount waits for a mount's open
     * handles to drain before reporting EBUSY.  0 keeps the server default
     * (1s).  A suite that unmounts often wants this short, because the wait
     * is per attempt and the handles are dropped by an asynchronous sweep. */
    int         umount_timeout_ms;
    /* The client's security flavor (see enum mbt_sec).  AUTH_SYS by default;
     * anything else stands a Kerberos realm up in this process and runs every
     * call under a context established against the server's own acceptor. */
    int         sec;
    /* server.nfs3_drc: the NFSv3 duplicate-request cache.  Off by default in
     * the server and here; the DRC suite is the only caller that turns it on.
     * The NFSv4.0 cache needs no flag -- it is always installed. */
    int         nfs3_drc;
};

struct mbt_env {
    struct chimera_server       *server;
    struct prometheus_metrics   *metrics;
    /* The scrape endpoint, when mbt_env_opts.metrics_port asks for one.  It
     * owns the registry in that case (chimera_metrics_init creates one on its
     * own thread), so the server must be handed THAT registry or every metric
     * would read zero over HTTP. */
    struct chimera_metrics      *metrics_server;
    /* HTTP agent for the control-plane suite's REST client, created by that
     * harness on this env's evpl (see server/rest/tests/quint/ctl_http.h) and
     * kept here so a replayer can open connections from anywhere.  NULL for
     * every other suite. */
    struct evpl_http_agent      *rest_agent;
    const char                  *module; /* backend for mkfs/mount/rmfs */

    int                          rdma;   /* NFS carried over RPC-over-RDMA */

    struct evpl                 *evpl;
    struct evpl_rpc2_thread     *rpc2_thread;
    struct evpl_rpc2_conn       *nfs_conn;
    struct evpl_rpc2_conn       *mount_conn;
    struct NFS_V3                nfs_v3;
    struct NFS_MOUNT_V3          mount_v3;
    struct NFS_V4                nfs_v4;
    struct NFS_V4_CB             nfs_v4_cb;
    struct evpl_rpc2_cred        cred;
    /* MOUNT and the auxiliary services call on connections of their own.  A
     * GSS context belongs to one connection, so MOUNT gets its own credential
     * naming its own context; portmap/NLM/NSM are not part of what a security
     * flavor covers here and stay on AUTH_SYS. */
    struct evpl_rpc2_cred        mount_cred;
    struct evpl_rpc2_cred        aux_cred;

    /* Credentials for connections the suites open themselves.  Under AUTH_SYS
     * one credential serves every connection; a GSS context belongs to the
     * connection it was established on, so each needs its own.  The NFSv4
     * suite gives every model client a connection, which is what this is for. */
    struct mbt_conn_cred         conn_creds[MBT_MAX_CONN_CREDS];
    int                          num_conn_creds;

    /* Kerberos, when opts.sec asked for it.  The realm outlives the server it
     * authenticates for: its MEMORY: keytab has to be populated before the
     * server's acceptor registers that identity at start-up. */
    int                          sec;
    struct krb5_local           *krb5;
    struct evpl_rpc2_gss_client *nfs_gss;
    struct evpl_rpc2_gss_client *mount_gss;

    /* Auxiliary services, connected only when mbt_env_opts.aux is set.  The
     * programs are registered on the client rpc2 thread either way, which
     * costs nothing and lets the server call back into this process (NLM's
     * *_RES messages ride the same connection the *_MSG arrived on). */
    struct evpl_rpc2_conn       *portmap_conn;
    struct evpl_rpc2_conn       *nlm_conn;
    struct evpl_rpc2_conn       *nsm_conn;
    struct PORTMAP_V2            pm_v2;
    struct PORTMAP_V3            pm_v3;
    struct PORTMAP_V4            pm_v4;
    struct NLM_V4                nlm_v4;
    struct SM_INTER_V1           nsm_v1;
    /* struct mbt_aux_result *, owned by nfs_aux_mbt_common.h. */
    void                        *aux;

    char                         session_dir[256];
    char                         pt_root[256]; /* passthrough backing root
                                                * (linux/io_uring); empty otherwise */
    uint8_t                     *data_buf; /* READ copy-out scratch */

    /* pNFS data servers: separate chimera servers in this same process,
    * reached by the MDS over the inproc transport through the nfs client
    * module (mounted at /ds<i>).  Empty unless opts.pnfs_num_ds > 0. */
    int                          num_ds;
    struct chimera_server       *ds_server[MBT_MAX_DS];
    struct prometheus_metrics   *ds_metrics[MBT_MAX_DS];

    struct mbt_result            res;
};

/* Set the uid (and gid) every subsequent call is made under.  The AUTH_SYS
 * credential is otherwise fixed at root for the whole run; the control-plane
 * suite varies it because that is the only way to tell an export's three
 * squash modes apart -- root_squash maps uid 0 and nobody else. */
static inline void
mbt_cred_set_uid(
    struct mbt_env *env,
    uint32_t        uid)
{
    env->cred.authsys.uid = uid;
    env->cred.authsys.gid = uid;
} /* mbt_cred_set_uid */

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

/*
 * Bring up one in-process pNFS data server.
 *
 * A DS is a whole second chimera server living in this process: nfs_enabled,
 * inproc, and in data_server mode so it binds only the NFSv4 service (no
 * mountd/portmap/NLM) and can therefore share the process with the MDS -- the
 * inproc registry keys services by port, exactly as a host does, so the MDS's
 * 2049/20048 and each DS's 12050+i coexist.  nfs_server_scope is distinct from
 * the MDS's for the same reason the split KVM topology gives its DS a distinct
 * scope: a real client keys server identity on it.
 *
 * The DS exports "/ds_export"; the MDS mounts that through the nfs client
 * module and creates one backing file per pNFS file under it.
 */
/* Context establishment, pumped to completion.
 *
 * evpl_rpc2_gss_client_create() is asynchronous -- the handshake is RPCs on
 * the program's NULL procedure -- but everything else in this harness is
 * synchronous, so it is driven to a conclusion here and the caller sees only
 * the finished context.
 */
struct mbt_gss_wait {
    struct evpl_rpc2_gss_client *client;
    int                          done;
};

static inline void
mbt_gss_ready(
    struct evpl_rpc2_gss_client *client,
    int                          status,
    void                        *private_data)
{
    struct mbt_gss_wait *w = private_data;

    w->client = status ? NULL : client;
    w->done   = 1;
} /* mbt_gss_ready */

static inline struct evpl_rpc2_gss_client *
mbt_gss_establish(
    struct mbt_env           *env,
    struct evpl_rpc2_program *program,
    struct evpl_rpc2_conn    *conn,
    const char               *what)
{
    struct mbt_gss_wait w = { 0 };
    int                 spins;

    evpl_rpc2_gss_client_create(env->evpl, program, conn,
                                krb5_local_initiator_provider(),
                                krb5_local_arg(env->krb5),
                                mbt_sec_service(env->sec),
                                MBT_KRB5_PRINCIPAL, mbt_gss_ready, &w);

    /* Bounded: a handshake that never completes is a failure to report, not a
     * reason to hang the suite. */
    for (spins = 0; !w.done && spins < 100000; spins++) {
        evpl_continue(env->evpl);
    }

    if (!w.client) {
        fprintf(stderr, "%s: failed to establish a %s context\n",
                mbt_sec_name(env->sec), what);
        exit(1);
    }

    return w.client;
} /* mbt_gss_establish */

/*
 * The credential for a call on `conn`.
 *
 * Under AUTH_SYS this is always the env's own: one credential describes the
 * caller wherever it calls from.  Under a Kerberos flavor it is not, because a
 * context is established on a connection and means nothing on another -- so a
 * suite that opens connections of its own (the NFSv4 replayer gives every
 * model client one, as a real client would) gets a context per connection,
 * established on first use.
 */
static inline struct evpl_rpc2_cred *
mbt_cred_for(
    struct mbt_env           *env,
    struct evpl_rpc2_conn    *conn,
    struct evpl_rpc2_program *program)
{
    struct mbt_conn_cred *cc;
    int                   i;

    if (env->sec == MBT_SEC_SYS || conn == env->nfs_conn) {
        return &env->cred;
    }

    for (i = 0; i < env->num_conn_creds; i++) {
        if (env->conn_creds[i].conn == conn) {
            return &env->conn_creds[i].cred;
        }
    }

    if (env->num_conn_creds == MBT_MAX_CONN_CREDS) {
        fprintf(stderr, "%s: more than %d connections want contexts\n",
                mbt_sec_name(env->sec), MBT_MAX_CONN_CREDS);
        exit(1);
    }

    cc       = &env->conn_creds[env->num_conn_creds++];
    cc->conn = conn;
    cc->gss  = mbt_gss_establish(env, program, conn, "connection");

    cc->cred.flavor      = EVPL_RPC2_AUTH_RPCSEC_GSS;
    cc->cred.gss.service = mbt_sec_service(env->sec);
    cc->cred.gss.client  = cc->gss;

    return &cc->cred;
} /* mbt_cred_for */

/*
 * Retire whatever context a suite's own connection carried, before the
 * connection itself goes.  A context outliving its connection has nowhere to
 * send its DESTROY.
 */
static inline void
mbt_conn_release(
    struct mbt_env        *env,
    struct evpl_rpc2_conn *conn)
{
    int i;

    for (i = 0; i < env->num_conn_creds; i++) {
        if (env->conn_creds[i].conn != conn) {
            continue;
        }

        if (env->conn_creds[i].gss) {
            evpl_rpc2_gss_client_destroy(env->evpl, env->conn_creds[i].gss);
        }

        env->conn_creds[i] = env->conn_creds[--env->num_conn_creds];
        return;
    }
} /* mbt_conn_release */

static inline void
mbt_pnfs_ds_start(
    struct mbt_env *env,
    int             idx,
    const char     *module)
{
    struct chimera_server_config *config;
    char                          dir[300];
    char                          fsname[32];

    snprintf(dir, sizeof(dir), "%s/ds%d", env->session_dir, idx);
    if (mkdir(dir, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "pnfs ds%d state dir %s: %s\n", idx, dir,
                strerror(errno));
        exit(1);
    }

    env->ds_metrics[idx] = prometheus_metrics_create(NULL, NULL, 0);

    config = chimera_server_config_init();
    chimera_server_config_set_state_dir(config, dir);
    chimera_server_config_set_tcp_flavor(config, CHIMERA_TCP_FLAVOR_INPROC);
    chimera_server_config_set_nfs_enabled(config, 1);
    chimera_server_config_set_nfs_port(config, MBT_DS_PORT_BASE + idx);
    chimera_server_config_set_nfs_data_server(config, 1);
    chimera_server_config_set_nfs_server_scope(config, 43 + idx);

    env->ds_server[idx] = chimera_server_init(config, env->ds_metrics[idx]);
    chimera_server_start(env->ds_server[idx]);

    snprintf(fsname, sizeof(fsname), "dsfs%d", idx);
    if (chimera_server_mkfs(env->ds_server[idx], module, fsname, NULL) != 0) {
        fprintf(stderr, "pnfs ds%d: mkfs %s/%s failed\n", idx, module, fsname);
        exit(1);
    }
    chimera_server_mount(env->ds_server[idx], "ds_data", module, fsname, NULL);
    if (chimera_server_create_export(env->ds_server[idx], "/ds_export",
                                     "/ds_data", 0, NULL) != 0) {
        fprintf(stderr, "pnfs ds%d: export /ds_export failed\n", idx);
        exit(1);
    }
} /* mbt_pnfs_ds_start */

/* The RFC 5665 universal address the MDS advertises for a DS: the "tcp" netid
 * form h.h.h.h.p_hi.p_lo.  Only the port distinguishes our data servers -- an
 * inproc endpoint's name is derived from it (see chimera_tcp_flavor_endpoint_
 * create), and a real client would dial the same pair. */
static inline void
mbt_pnfs_ds_uaddr(
    char  *out,
    size_t out_size,
    int    port)
{
    snprintf(out, out_size, "127.0.0.1.%u.%u",
             (unsigned) ((port >> 8) & 0xff), (unsigned) (port & 0xff));
} /* mbt_pnfs_ds_uaddr */

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
    const char                   *ds_module;
    int                           i;

    mbt_debug_log_start();

    memset(env, 0, sizeof(*env));

    snprintf(env->session_dir, sizeof(env->session_dir),
             "/tmp/nfs3_mbt_XXXXXX");
    if (!mkdtemp(env->session_dir)) {
        fprintf(stderr, "mkdtemp(%s) failed\n", env->session_dir);
        exit(1);
    }

    if (opts && opts->metrics_port) {
        env->metrics_server = chimera_metrics_init(opts->metrics_port,
                                                   CHIMERA_TCP_FLAVOR_INPROC);
        env->metrics = chimera_metrics_get(env->metrics_server);
    } else {
        env->metrics = prometheus_metrics_create(NULL, NULL, 0);
    }

    /* The VFS releases closed handles on an async sweep thread, so a filesystem
     * stays busy for a window after the last close.  A fast sweep keeps the
     * per-trace rmfs recycle from stalling once the client's opens are dropped;
     * set before the server's threads start below. */
    setenv("CHIMERA_CLOSE_SWEEP_INTERVAL_MS", "10", 0);

    /* pNFS: stand the data servers up first -- the MDS mounts their exports
     * during its own bring-up below, so they have to be serving by then. */
    env->num_ds = (opts && opts->pnfs_num_ds > MBT_MAX_DS)
        ? MBT_MAX_DS : (opts ? opts->pnfs_num_ds : 0);
    ds_module = (opts && opts->pnfs_ds_module) ? opts->pnfs_ds_module : "memfs";

    for (i = 0; i < env->num_ds; i++) {
        mbt_pnfs_ds_start(env, i, ds_module);
    }

    /* The realm has to exist before the server does: chimera registers the
     * acceptor identity when its NFS service starts, and there would be no key
     * under that name yet. */
    env->sec = opts ? opts->sec : MBT_SEC_SYS;

    if (env->sec != MBT_SEC_SYS) {
        const char *why = NULL;

        env->krb5 = krb5_local_create_as(MBT_KRB5_PRINCIPAL, &why);

        if (!env->krb5) {
            /* Skip rather than fail, as every other krb5-dependent test in
             * this tree does: a host without a usable MIT krb5 cannot run
             * these cells and has not failed them. */
            fprintf(stderr, "%s: skipping: %s\n", mbt_sec_name(env->sec),
                    why ? why : "krb5 unavailable");
            exit(77);
        }
    }

    config = chimera_server_config_init();
    chimera_server_config_set_state_dir(config, env->session_dir);
    chimera_server_config_set_tcp_flavor(config, CHIMERA_TCP_FLAVOR_INPROC);
    chimera_server_config_set_nfs_enabled(config, 1);

    if (env->sec != MBT_SEC_SYS) {
        chimera_server_config_set_nfs_kerberos_enabled(config, 1);
        chimera_server_config_set_nfs_kerberos_keytab(config,
                                                      MBT_KRB5_KEYTAB);
    }

    /* The RDMA listener is additional, not instead: the server keeps its
     * stream endpoint, and the client below chooses which one to use. */
    env->rdma = (opts && opts->rdma);
    if (env->rdma) {
        chimera_server_config_set_nfs_rdma(config, 1);
        chimera_server_config_set_nfs_rdma_port(config, MBT_NFS_RDMA_PORT);
    }

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

    /* Register each data server with the MDS.  version alternates 3 / 4.1 so
     * both arms of the flex-files device encoder (ffda_versions) are exercised;
     * it only says which NFS version a *client* would use for the direct data
     * path, and is independent of the MDS's own control-path mount below. */
    if (env->num_ds > 0) {
        chimera_server_config_set_pnfs_enabled(config, 1);

        for (i = 0; i < env->num_ds; i++) {
            char uaddr[64], backing[32];

            mbt_pnfs_ds_uaddr(uaddr, sizeof(uaddr), MBT_DS_PORT_BASE + i);
            snprintf(backing, sizeof(backing), "/ds%d", i);
            chimera_server_config_add_pnfs_ds(config, "tcp", uaddr, NULL,
                                              backing,
                                              (i & 1) ? 4 : 3,
                                              (i & 1) ? 1 : 0);
        }
    }

    if (opts) {
        if (opts->nfs4_delegations) {
            chimera_server_config_set_nfs4_delegations(config, 1);
        }
        if (opts->smb_named_streams) {
            chimera_server_config_set_smb_named_streams(config, 1);
        }
        if (opts->portmap_hostname) {
            chimera_server_config_set_portmap_hostname(config,
                                                       opts->portmap_hostname);
        }
        if (opts->umount_timeout_ms) {
            chimera_server_config_set_umount_timeout(config,
                                                     opts->umount_timeout_ms);
        }
        if (opts->smb_enabled) {
            chimera_server_config_set_smb_enabled(config, 1);
            chimera_server_config_set_smb_signing_required(config, 0);
        }
        if (opts->s3_enabled) {
            chimera_server_config_set_s3_enabled(config, 1);
        }
        if (opts->umount_timeout_ms) {
            chimera_server_config_set_umount_timeout(config,
                                                     opts->umount_timeout_ms);
        }
        if (opts->smb_enabled) {
            chimera_server_config_set_smb_enabled(config, 1);
            chimera_server_config_set_smb_signing_required(config, 0);
        }
        if (opts->s3_enabled) {
            chimera_server_config_set_s3_enabled(config, 1);
        }
        if (opts->rest_port) {
            chimera_server_config_set_rest_http_port(config, opts->rest_port);
            chimera_server_config_set_rest_auth_enabled(config,
                                                        opts->rest_auth);
        }
        if (opts->disable_caches) {
            chimera_server_config_set_attr_cache_enabled(config, 0);
            chimera_server_config_set_name_cache_enabled(config, 0);
        }
        if (opts->nfs3_drc) {
            chimera_server_config_set_nfs3_drc(config, 1);
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
                 "\"devices\":[{\"type\":\"%s\",\"size\":1,\"path\":\"%s\"}]}",
                 CHIMERA_DISKFS_DEVICE_TYPE, img);
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

    /* The MDS reaches each DS through the nfs client module.  vers=4 keeps the
     * control path off portmap/mountd (which a data_server does not run), and
     * the module inherits the inproc flavor from the VFS at mount time, so this
     * whole control path stays in-process. */
    for (i = 0; i < env->num_ds; i++) {
        char name[32], path[64], mopts[64];

        snprintf(name, sizeof(name), "ds%d", i);
        snprintf(path, sizeof(path), "127.0.0.1:/ds_export");
        snprintf(mopts, sizeof(mopts), "vers=4,port=%d", MBT_DS_PORT_BASE + i);

        if (chimera_server_mount(env->server, name, "nfs", path, mopts) != 0) {
            fprintf(stderr, "pnfs: MDS failed to mount data server %d\n", i);
            exit(1);
        }
    }

    if (env->num_ds > 0 && chimera_server_pnfs_resolve(env->server) != 0) {
        fprintf(stderr, "pnfs: MDS failed to resolve a data-server backing root\n");
        exit(1);
    }

    /* Client half: its own evpl loop; the reply callbacks run inside
     * evpl_continue() on this (the only) test thread. */
    /* Under a Kerberos flavor the loop is pumped at points where nothing is
     * outstanding -- flushing a context's DESTROY before its connection goes,
     * for one -- and the default (-1) parks evpl_continue in the poller until
     * something happens, which there never will.  A bounded wait makes those
     * pumps return.  AUTH_SYS keeps the default: every pump it does is waiting
     * on a reply that is genuinely coming. */
    if (env->sec != MBT_SEC_SYS) {
        struct evpl_thread_config *tcfg = evpl_thread_config_init();

        evpl_thread_config_set_wait_ms(tcfg, 1);
        env->evpl = evpl_create(tcfg);
    } else {
        env->evpl = evpl_create(NULL);
    }

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
                                                "127.0.0.1",
                                                env->rdma ? MBT_NFS_RDMA_PORT
                                                : 2049);
    mount_ep = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                                  "127.0.0.1", MBT_MOUNT_PORT);

    /* DATAGRAM_INPROC is the RDMA-capable inproc protocol; rpc2 picks its
     * RDMA framing from the bind, so this one choice switches both ends. */
    env->nfs_conn = evpl_rpc2_client_connect(env->rpc2_thread,
                                             env->rdma ? EVPL_DATAGRAM_INPROC
                                             : EVPL_STREAM_INPROC,
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

    env->mount_cred = env->cred;
    env->aux_cred   = env->cred;

    /* ...unless a Kerberos flavor was asked for, in which case the identity
     * comes from the principal instead and the credential names the context.
     * MOUNT gets its own: a context belongs to one connection. */
    if (env->sec != MBT_SEC_SYS) {
        env->nfs_gss = mbt_gss_establish(env, &env->nfs_v3.rpc2,
                                         env->nfs_conn, "NFS");
        env->mount_gss = mbt_gss_establish(env, &env->mount_v3.rpc2,
                                           env->mount_conn, "MOUNT");

        env->cred.flavor      = EVPL_RPC2_AUTH_RPCSEC_GSS;
        env->cred.gss.service = mbt_sec_service(env->sec);
        env->cred.gss.client  = env->nfs_gss;

        env->mount_cred.flavor      = EVPL_RPC2_AUTH_RPCSEC_GSS;
        env->mount_cred.gss.service = mbt_sec_service(env->sec);
        env->mount_cred.gss.client  = env->mount_gss;
    }

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
    int  i;

    if (env->portmap_conn) {
        evpl_rpc2_client_disconnect(env->rpc2_thread, env->portmap_conn);
    }
    if (env->nlm_conn) {
        evpl_rpc2_client_disconnect(env->rpc2_thread, env->nlm_conn);
    }
    if (env->nsm_conn) {
        evpl_rpc2_client_disconnect(env->rpc2_thread, env->nsm_conn);
    }
    /* Before the connections they were established on: destroy sends a
     * DESTROY down each one so the server drops its half now rather than at
     * expiry. */
    if (env->nfs_gss) {
        evpl_rpc2_gss_client_destroy(env->evpl, env->nfs_gss);
        env->nfs_gss = NULL;
    }
    if (env->mount_gss) {
        evpl_rpc2_gss_client_destroy(env->evpl, env->mount_gss);
        env->mount_gss = NULL;
    }

    evpl_rpc2_client_disconnect(env->rpc2_thread, env->nfs_conn);
    evpl_rpc2_client_disconnect(env->rpc2_thread, env->mount_conn);
    evpl_rpc2_thread_destroy(env->rpc2_thread);
    evpl_destroy(env->evpl);

    chimera_server_destroy(env->server);
    /* Dump before destroying, and outside the branch: env->metrics is the
     * registry the server counted into either way, and when the scrape
     * endpoint owns it (metrics_port), destroying the endpoint destroys the
     * registry with it on its own thread. */
    mbt_metrics_dump(env->metrics);
    if (env->metrics_server) {
        chimera_metrics_destroy(env->metrics_server);
    } else {
        prometheus_metrics_destroy(env->metrics);
    }

    /* The MDS is gone, so nothing holds the data servers' exports any more. */
    for (i = 0; i < env->num_ds; i++) {
        chimera_server_destroy(env->ds_server[i]);
        prometheus_metrics_destroy(env->ds_metrics[i]);
    }

    if (env->krb5) {
        krb5_local_destroy(env->krb5);
        env->krb5 = NULL;
    }

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
                                           env->mount_conn, &env->mount_cred, &args,
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
     * ownership passes to the rpc2 layer here -- no release on our side.
     *
     * ddp=1 under RDMA puts the payload in an RFC 8166 Read chunk for the
     * server to pull, instead of inline in the request.  It is what drives
     * the server's read-chunk path (evpl_rpc2_encoding_take_read_chunk in
     * nfs3_proc_write.c); without it an RDMA run still gets RPC-over-RDMA
     * framing but never a one-sided transfer, and the chunk handling would
     * go untested. */
    env->nfs_v3.send_call_NFSPROC3_WRITE(&env->nfs_v3.rpc2, env->evpl,
                                         env->nfs_conn, &env->cred, &args,
                                         env->rdma, 0, NULL, 0, 0,
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
             * them here or evpl's teardown leak check aborts.
             *
             * resok.count bounds the copy, and the iovec lengths do not.
             * Inline they agree, but an RDMA Write chunk is returned at the
             * size the client ADVERTISED, not the size the server filled --
             * a short read into a full-size chunk hands back a longer iovec
             * whose tail is undefined.  count is the authoritative length,
             * which is what chimera's own client uses (nfs3_read.c).  Every
             * iovec is still released, copied from or not. */
            for (i = 0; i < reply->resok.data.niov; i++) {
                n = reply->resok.data.iov[i].length;
                if (off + n > reply->resok.count) {
                    n = reply->resok.count - off;
                }
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
    /* The mirror of the WRITE above: advertising a Write chunk of the read
     * size lets the server place the payload directly with an RDMA write
     * rather than sending it inline. */
    env->nfs_v3.send_call_NFSPROC3_READ(&env->nfs_v3.rpc2, env->evpl,
                                        env->nfs_conn, &env->cred, &args,
                                        0, env->rdma ? (int) count : 0,
                                        NULL, 0, 0,
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
