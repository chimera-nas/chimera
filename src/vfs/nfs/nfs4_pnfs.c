// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NFSv4.1 pNFS CLIENT, flex-files layout (RFC 8435).  See nfs4_pnfs.h for the
 * overall model.  This file:
 *   - hand-decodes the opaque ff_layout4 (loc_body) and ff_device_addr4
 *     (da_addr_body), the read-side inverse of the server's encoders in
 *     src/server/nfs/nfs4_pnfs.c;
 *   - drives LAYOUTGET + GETDEVICEINFO to the MDS to acquire a layout and
 *     resolve its data server, registering the DS as another
 *     chimera_nfs_client_server;
 *   - redirects READ/WRITE to the DS over NFSv3 (reusing the v3 send path);
 *   - on close, reports the new size to the MDS (LAYOUTCOMMIT) and returns the
 *     layout (LAYOUTRETURN).
 *
 * Every failure path falls back to MDS I/O; pNFS never makes an op fail that
 * would otherwise have succeeded.
 */

#include <string.h>
#include <stdlib.h>

#include "nfs_internal.h"
#include "nfs4_open_state.h"
#include "nfs4_pnfs.h"
#include "nfs_common/nfs3_status.h"

#define CHIMERA_NFS4_PNFS_MAXCOUNT     4096

/* On-wire size of a stateid4: 4-byte seqid + NFS4_OTHER_SIZE (12) bytes. */
#define CHIMERA_NFS4_STATEID_WIRE_SIZE 16

/* Synthetic DS file handle = [mount_id][server_index][ds_fh]; size for the
 * largest possible ds_fh so the on-stack buffer can never overflow. */
#define CHIMERA_NFS4_DS_FH_MAX         (CHIMERA_VFS_MOUNT_ID_SIZE + 1 + CHIMERA_VFS_FH_SIZE)

/* ---------------------------------------------------------------------------
* Bounds-checked XDR decode cursor (read-side inverse of the server's
* pnfs_put_u32/u64/opaque).  On any short read the cursor latches `err`, and
* every subsequent get is a no-op, so a malformed body yields a clean failure
* (and an MDS fallback) rather than an out-of-bounds read.
* ------------------------------------------------------------------------- */

struct pnfs_cursor {
    const uint8_t *p;
    const uint8_t *end;
    int            err;
};

static inline void
pnfs_cursor_init(
    struct pnfs_cursor *c,
    const void         *data,
    uint32_t            len)
{
    c->p   = data;
    c->end = (const uint8_t *) data + len;
    c->err = (data == NULL && len > 0);
} /* pnfs_cursor_init */

static inline uint32_t
pnfs_get_u32(struct pnfs_cursor *c)
{
    uint32_t v;

    if (c->err || c->p + sizeof(uint32_t) > c->end) {
        c->err = 1;
        return 0;
    }
    memcpy(&v, c->p, sizeof(v));
    c->p += sizeof(uint32_t);
    return chimera_nfs_ntoh32(v);
} /* pnfs_get_u32 */

static inline uint64_t
pnfs_get_u64(struct pnfs_cursor *c)
{
    uint64_t v;

    if (c->err || c->p + sizeof(uint64_t) > c->end) {
        c->err = 1;
        return 0;
    }
    memcpy(&v, c->p, sizeof(v));
    c->p += sizeof(uint64_t);
    return chimera_nfs_ntoh64(v);
} /* pnfs_get_u64 */

/* Skip `n` raw bytes (no padding). */
static inline void
pnfs_skip(
    struct pnfs_cursor *c,
    uint32_t            n)
{
    if (c->err || c->p + n > c->end) {
        c->err = 1;
        return;
    }
    c->p += n;
} /* pnfs_skip */

/*
 * Read an XDR opaque<>/string<> (4-byte length, bytes, pad to 4) into out (up
 * to outcap bytes); *outlen receives the true length.  The bytes are skipped in
 * the cursor regardless of outcap so following fields stay aligned.
 */
static inline void
pnfs_get_opaque(
    struct pnfs_cursor *c,
    void               *out,
    uint32_t            outcap,
    uint32_t           *outlen)
{
    uint32_t len = pnfs_get_u32(c);
    uint32_t pad;

    if (c->err) {
        *outlen = 0;
        return;
    }

    pad = (4 - (len & 3)) & 3;

    if (c->p + len + pad > c->end) {
        c->err  = 1;
        *outlen = 0;
        return;
    }

    if (out && len <= outcap) {
        memcpy(out, c->p, len);
        *outlen = len;
    } else {
        /* Caller's buffer too small (or no copy requested): report the length
         * but don't copy a truncated value the caller might misuse. */
        *outlen = (out && len > outcap) ? 0 : len;
    }

    c->p += len + pad;
} /* pnfs_get_opaque */

/* ---------------------------------------------------------------------------
* Flex-files body decoders (inverse of chimera_nfs4_encode_ff_* in the server).
* ------------------------------------------------------------------------- */

/*
 * Decode an ff_layout4 (loc_body) into a single layout segment.  We expect
 * chimera's whole-file, one-mirror, one-data-server layout: read the device id
 * and the data server's native file handle, and ignore the rest (efficiency,
 * stateid, user/group, flags).  Returns 0 on success.
 */
static int
chimera_nfs4_decode_ff_layout(
    const void                        *body,
    uint32_t                           body_len,
    struct chimera_vfs_layout_segment *seg)
{
    struct pnfs_cursor c;
    uint32_t           num_mirrors, num_ds, num_fh;

    pnfs_cursor_init(&c, body, body_len);

    pnfs_get_u64(&c);                              /* ffl_stripe_unit            */

    num_mirrors = pnfs_get_u32(&c);                /* ffl_mirrors<>              */
    if (c.err || num_mirrors < 1) {
        return -1;
    }

    num_ds = pnfs_get_u32(&c);                     /* ffm_data_servers<>         */
    if (c.err || num_ds < 1) {
        return -1;
    }

    pnfs_skip(&c, CHIMERA_VFS_DEVICEID_SIZE);       /* read ffds_deviceid below   */
    if (!c.err) {
        memcpy(seg->deviceid, c.p - CHIMERA_VFS_DEVICEID_SIZE, CHIMERA_VFS_DEVICEID_SIZE);
    }

    pnfs_get_u32(&c);                              /* ffds_efficiency            */
    pnfs_skip(&c, CHIMERA_NFS4_STATEID_WIRE_SIZE); /* ffds_stateid (anonymous)   */

    num_fh = pnfs_get_u32(&c);                     /* ffds_fh_vers<> count       */
    if (c.err || num_fh < 1) {
        return -1;
    }

    seg->ds_fh_len = 0;
    pnfs_get_opaque(&c, seg->ds_fh, sizeof(seg->ds_fh), &seg->ds_fh_len);
    if (c.err || seg->ds_fh_len == 0) {
        return -1;
    }

    /* Any additional fh_vers entries, then ffds_user/ffds_group/flags, are not
    * needed by the client; stop here.  (We only required the first handle.) */
    return 0;
} /* chimera_nfs4_decode_ff_layout */

/*
 * Decode an ff_device_addr4 (da_addr_body) into a device descriptor: the first
 * netaddr4 {netid, uaddr} and the first ff_version entry (NFS version the DS
 * speaks).  Returns 0 on success.
 */
static inline int
chimera_nfs4_netid_is_rdma(const char *netid)
{
    return strcmp(netid, "rdma") == 0 || strcmp(netid, "rdma6") == 0;
} /* chimera_nfs4_netid_is_rdma */

static int
chimera_nfs4_decode_ff_device_addr(
    const void                       *body,
    uint32_t                          body_len,
    struct chimera_vfs_layout_device *dev,
    int                              *ds_version,
    int                              *ds_minorversion,
    int                               prefer_rdma)
{
    struct pnfs_cursor c;
    uint32_t           num_addrs, num_vers, i;
    int                have = 0, ideal = 0;

    pnfs_cursor_init(&c, body, body_len);

    num_addrs = pnfs_get_u32(&c);                  /* ffda_netaddrs<>            */
    if (c.err || num_addrs < 1) {
        return -1;
    }

    /* The server may advertise several transports for one DS (e.g. rdma + tcp).
     * Consume them all (to stay aligned) and keep the preferred one: an RDMA
     * netaddr when prefer_rdma, otherwise a non-RDMA (tcp) one.  Falls back to
     * the first usable netaddr if the preferred class is not offered. */
    for (i = 0; i < num_addrs; i++) {
        char     netid_tmp[sizeof(dev->netid)];
        char     uaddr_tmp[sizeof(dev->uaddr)];
        uint32_t netid_len = 0, uaddr_len = 0;
        int      preferred;

        pnfs_get_opaque(&c, netid_tmp, sizeof(netid_tmp) - 1, &netid_len);
        netid_tmp[netid_len < sizeof(netid_tmp) ? netid_len : 0] = '\0';

        pnfs_get_opaque(&c, uaddr_tmp, sizeof(uaddr_tmp) - 1, &uaddr_len);
        if (c.err) {
            return -1;
        }
        if (uaddr_len == 0 || uaddr_len >= sizeof(uaddr_tmp)) {
            continue;       /* unusable address; skip but stay aligned */
        }
        uaddr_tmp[uaddr_len] = '\0';

        preferred = prefer_rdma ? chimera_nfs4_netid_is_rdma(netid_tmp)
                                : !chimera_nfs4_netid_is_rdma(netid_tmp);

        if (!have || (preferred && !ideal)) {
            snprintf(dev->netid, sizeof(dev->netid), "%s", netid_tmp);
            snprintf(dev->uaddr, sizeof(dev->uaddr), "%s", uaddr_tmp);
            have = 1;
            if (preferred) {
                ideal = 1;
            }
        }
    }

    if (!have) {
        return -1;
    }

    num_vers = pnfs_get_u32(&c);                   /* ffda_versions<>            */
    if (c.err || num_vers < 1) {
        return -1;
    }

    *ds_version      = (int) pnfs_get_u32(&c);     /* ffdv_version               */
    *ds_minorversion = (int) pnfs_get_u32(&c);     /* ffdv_minorversion          */
    /* ffdv_rsize, ffdv_wsize, ffdv_tightly_coupled follow; not needed. */

    if (c.err) {
        return -1;
    }

    dev->layout_class = CHIMERA_VFS_LAYOUT_CLASS_FLEX;
    return 0;
} /* chimera_nfs4_decode_ff_device_addr */

/* ---------------------------------------------------------------------------
* RFC 5665 universal address parse (mirror of the server's nfs4_cb_parse_uaddr):
* "<host>.<p1>.<p2>"  =>  host string + TCP port (p1*256 + p2).
* ------------------------------------------------------------------------- */

static int
chimera_nfs4_parse_uaddr(
    const char *uaddr,
    char       *host,
    size_t      hostsz,
    int        *out_port)
{
    char   buf[80];
    char  *last_dot, *second_dot;
    int    hi, lo;
    size_t hostlen;

    if (!uaddr || uaddr[0] == '\0') {
        return -1;
    }

    snprintf(buf, sizeof(buf), "%s", uaddr);

    last_dot = strrchr(buf, '.');
    if (!last_dot) {
        return -1;
    }
    *last_dot = '\0';
    lo        = atoi(last_dot + 1);

    second_dot = strrchr(buf, '.');
    if (!second_dot) {
        return -1;
    }
    *second_dot = '\0';
    hi          = atoi(second_dot + 1);

    hostlen = strlen(buf);
    if (hostlen == 0 || hostlen >= hostsz) {
        return -1;
    }
    memcpy(host, buf, hostlen + 1);
    *out_port = (hi << 8) | lo;
    return 0;
} /* chimera_nfs4_parse_uaddr */

/* ---------------------------------------------------------------------------
* Client device cache (deviceid -> resolved DS server slot + descriptor).
* ------------------------------------------------------------------------- */

static int
chimera_nfs4_devcache_find(
    struct chimera_nfs4_client_devcache *cache,
    const uint8_t                       *deviceid,
    int                                 *server_index)
{
    uint32_t i;
    int      found = 0;

    pthread_mutex_lock(&cache->lock);
    for (i = 0; i < cache->count; i++) {
        if (cache->entries[i].valid &&
            memcmp(cache->entries[i].deviceid, deviceid, CHIMERA_VFS_DEVICEID_SIZE) == 0) {
            *server_index = cache->entries[i].server_index;
            found         = 1;
            break;
        }
    }
    pthread_mutex_unlock(&cache->lock);
    return found;
} /* chimera_nfs4_devcache_find */

static void
chimera_nfs4_devcache_put(
    struct chimera_nfs4_client_devcache    *cache,
    const struct chimera_vfs_layout_device *dev,
    int                                     server_index)
{
    uint32_t i;

    pthread_mutex_lock(&cache->lock);

    for (i = 0; i < cache->count; i++) {
        if (cache->entries[i].valid &&
            memcmp(cache->entries[i].deviceid, dev->deviceid, CHIMERA_VFS_DEVICEID_SIZE) == 0) {
            cache->entries[i].device       = *dev;
            cache->entries[i].server_index = server_index;
            pthread_mutex_unlock(&cache->lock);
            return;
        }
    }

    if (cache->count < CHIMERA_NFS4_CLIENT_DEVCACHE_MAX) {
        i = cache->count++;
        memcpy(cache->entries[i].deviceid, dev->deviceid, CHIMERA_VFS_DEVICEID_SIZE);
        cache->entries[i].device       = *dev;
        cache->entries[i].server_index = server_index;
        cache->entries[i].valid        = 1;
    }

    pthread_mutex_unlock(&cache->lock);
} /* chimera_nfs4_devcache_put */

/* ---------------------------------------------------------------------------
* Data-server registration: a DS becomes another chimera_nfs_client_server in
* shared->servers[], reusing the array-growth pattern from chimera_nfs4_mount.
* Deduplicated by (host, port).  Returns the server index, or -1.
* ------------------------------------------------------------------------- */

static int
chimera_nfs4_pnfs_register_ds(
    struct chimera_nfs_shared *shared,
    const char                *host,
    int                        port,
    int                        version,
    int                        use_rdma,
    enum evpl_protocol_id      rdma_protocol)
{
    struct chimera_nfs_client_server **new_servers, *server;
    int                                i, idx = -1;

    pthread_mutex_lock(&shared->lock);

    for (i = 0; i < shared->max_servers; i++) {
        if (shared->servers[i] && shared->servers[i]->is_ds &&
            shared->servers[i]->nfs_port == port &&
            strcmp(shared->servers[i]->hostname, host) == 0) {
            pthread_mutex_unlock(&shared->lock);
            return i;
        }
    }

    for (i = 0; i < shared->max_servers; i++) {
        if (shared->servers[i] == NULL) {
            idx = i;
            break;
        }
    }

    if (idx < 0) {
        /* Expand server array (same scheme as chimera_nfs4_mount). */
        idx                  = shared->max_servers;
        shared->max_servers *= 2;
        new_servers          = calloc(shared->max_servers, sizeof(*new_servers));
        memcpy(new_servers, shared->servers, (shared->max_servers / 2) * sizeof(*new_servers));
        free(shared->servers);
        shared->servers = new_servers;
    }

    /* A 1-byte server index is encoded into the file handle; cap accordingly. */
    if (idx > 255) {
        pthread_mutex_unlock(&shared->lock);
        return -1;
    }

    server                = calloc(1, sizeof(*server));
    server->shared        = shared;
    server->state         = CHIMERA_NFS_CLIENT_SERVER_STATE_DISCOVERED;
    server->refcnt        = 1;
    server->nfsvers       = version ? version : 3;
    server->is_ds         = 1;
    server->index         = idx;
    server->nfs_port      = port;
    server->use_rdma      = use_rdma;
    server->rdma_protocol = use_rdma ? rdma_protocol : 0;
    snprintf(server->hostname, sizeof(server->hostname), "%s", host);

    server->nfs_endpoint = evpl_endpoint_create(server->hostname, server->nfs_port);

    shared->servers[idx] = server;

    pthread_mutex_unlock(&shared->lock);

    chimera_nfsclient_info("pNFS: registered DS %s:%d as server index %d (v%d, %s)",
                           host, port, idx, server->nfsvers, use_rdma ? "rdma" : "tcp");

    return idx;
} /* chimera_nfs4_pnfs_register_ds */

/* ---------------------------------------------------------------------------
* Layout acquisition state machine (LAYOUTGET -> GETDEVICEINFO).  Its context
* lives in request->plugin_data for the duration of the acquisition; on
* completion (success or failure) it re-dispatches the original read/write,
* which then either takes the DS path (VALID) or falls back to the MDS (UNAVAIL).
* ------------------------------------------------------------------------- */

struct chimera_nfs4_acquire_ctx {
    struct chimera_nfs_thread               *thread;
    struct chimera_nfs_shared               *shared;
    void                                    *private_data;
    struct chimera_nfs_client_server_thread *mds_thread;
    struct chimera_nfs4_open_state          *open_state;
    void                                     ( *redispatch )(
        struct chimera_nfs_thread *,
        struct chimera_nfs_shared *,
        struct chimera_vfs_request *,
        void *);
    uint8_t                                  deviceid[CHIMERA_VFS_DEVICEID_SIZE];
};

/*
 * These pNFS compounds (LAYOUTGET/GETDEVICEINFO to the MDS, LAYOUTCOMMIT/
 * LAYOUTRETURN at close) go through chimera_nfs4_compound_call(), which parks
 * the request when no session slot is free and replays it via these shims when
 * one frees.  Each just re-runs its step, rebuilding args from the ctx that
 * already lives in request->plugin_data (the acquire ctx, or the close ctx).
 */
static void chimera_nfs4_getdeviceinfo(
    struct chimera_vfs_request      *request,
    struct chimera_nfs4_acquire_ctx *actx);
static void chimera_nfs4_layoutget(
    struct chimera_vfs_request      *request,
    struct chimera_nfs4_acquire_ctx *actx);
static void chimera_nfs4_pnfs_layoutcommit(
    struct chimera_vfs_request *request);
static void chimera_nfs4_pnfs_layoutreturn(
    struct chimera_vfs_request *request);

static void
chimera_nfs4_getdeviceinfo_retry(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *ctx)
{
    (void) thread;
    (void) shared;
    (void) ctx;
    chimera_nfs4_getdeviceinfo(request, request->plugin_data);
} /* chimera_nfs4_getdeviceinfo_retry */

static void
chimera_nfs4_layoutget_retry(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *ctx)
{
    (void) thread;
    (void) shared;
    (void) ctx;
    chimera_nfs4_layoutget(request, request->plugin_data);
} /* chimera_nfs4_layoutget_retry */

static void
chimera_nfs4_layoutcommit_retry(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *ctx)
{
    (void) thread;
    (void) shared;
    (void) ctx;
    chimera_nfs4_pnfs_layoutcommit(request);
} /* chimera_nfs4_layoutcommit_retry */

static void
chimera_nfs4_layoutreturn_retry(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *ctx)
{
    (void) thread;
    (void) shared;
    (void) ctx;
    chimera_nfs4_pnfs_layoutreturn(request);
} /* chimera_nfs4_layoutreturn_retry */

/* Re-dispatch a single request to the right op entry point. */
static void
chimera_nfs4_pnfs_replay(
    struct chimera_nfs4_layout *layout,
    struct chimera_vfs_request *request)
{
    if (request->opcode == CHIMERA_VFS_OP_READ) {
        chimera_nfs4_read(layout->acq_thread, layout->acq_shared, request, layout->acq_private);
    } else {
        chimera_nfs4_write(layout->acq_thread, layout->acq_shared, request, layout->acq_private);
    }
} /* chimera_nfs4_pnfs_replay */

/*
 * The DS connection reached CONNECTED (from chimera_nfs_notify).  Mark it ready
 * and replay any DS I/O that parked waiting for the RDMA QP to bind.  Runs on
 * the connection's own evpl thread, so conn_waiters is touched single-threaded.
 */
void
chimera_nfs4_pnfs_conn_connected(struct chimera_nfs_client_server_thread *server_thread)
{
    struct chimera_vfs_request *waiters, *w, *next;

    server_thread->nfs_conn_ready = 1;

    waiters                     = server_thread->conn_waiters;
    server_thread->conn_waiters = NULL;

    for (w = waiters; w; w = next) {
        next    = w->next;
        w->next = NULL;
        if (w->opcode == CHIMERA_VFS_OP_READ) {
            chimera_nfs4_read(server_thread->thread, server_thread->shared, w, server_thread->thread);
        } else {
            chimera_nfs4_write(server_thread->thread, server_thread->shared, w, server_thread->thread);
        }
    }
} /* chimera_nfs4_pnfs_conn_connected */

/*
 * The DS connection dropped (or never connected) before its parked I/O could
 * run.  Mark it not-ready and error-complete the waiters rather than leaking
 * them; the upper VFS layer decides whether to retry.
 */
void
chimera_nfs4_pnfs_conn_failed(struct chimera_nfs_client_server_thread *server_thread)
{
    struct chimera_vfs_request *waiters, *w, *next;

    server_thread->nfs_conn_ready = 0;

    waiters                     = server_thread->conn_waiters;
    server_thread->conn_waiters = NULL;

    for (w = waiters; w; w = next) {
        next      = w->next;
        w->next   = NULL;
        w->status = CHIMERA_VFS_EIO;
        w->complete(w);
    }
} /* chimera_nfs4_pnfs_conn_failed */

/* ----------------------------------------------------------------------- */
/* Layout registry + back-channel CB_LAYOUTRECALL                          */
/* ----------------------------------------------------------------------- */

void
chimera_nfs4_pnfs_layout_register(
    struct chimera_nfs_shared  *shared,
    struct chimera_nfs4_layout *layout)
{
    pthread_mutex_lock(&shared->pnfs_layout_lock);
    if (!layout->registered) {
        layout->registered   = 1;
        layout->reg_next     = shared->pnfs_layouts;
        shared->pnfs_layouts = layout;
    }
    pthread_mutex_unlock(&shared->pnfs_layout_lock);
} /* chimera_nfs4_pnfs_layout_register */

void
chimera_nfs4_pnfs_layout_unregister(
    struct chimera_nfs_shared  *shared,
    struct chimera_nfs4_layout *layout)
{
    struct chimera_nfs4_layout **pp;

    pthread_mutex_lock(&shared->pnfs_layout_lock);
    if (layout->registered) {
        for (pp = &shared->pnfs_layouts; *pp; pp = &(*pp)->reg_next) {
            if (*pp == layout) {
                *pp = layout->reg_next;
                break;
            }
        }
        layout->reg_next   = NULL;
        layout->registered = 0;
    }
    pthread_mutex_unlock(&shared->pnfs_layout_lock);
} /* chimera_nfs4_pnfs_layout_unregister */

/*
 * Fence a recalled layout: stop the client using it for DS I/O.  Storing
 * UNAVAIL makes subsequent reads/writes fall back to the MDS, and makes close
 * skip the (now invalid) LAYOUTCOMMIT/LAYOUTRETURN -- the MDS drops the layout
 * on our recall reply, so there is nothing to commit or return to.  Caller holds
 * shared->pnfs_layout_lock, so the layout cannot be unregistered/freed under us.
 */
static void
chimera_nfs4_cb_fence_layout(struct chimera_nfs4_layout *layout)
{
    atomic_store(&layout->state, CHIMERA_NFS4_LAYOUT_UNAVAIL);
} /* chimera_nfs4_cb_fence_layout */

/*
 * Strong CB_LAYOUTRECALL handler (overrides the weak default in nfs4_cb.c).
 * Runs on the back-channel control thread.  Find the recalled layout(s) for this
 * MDS in the registry and fence them, then answer NFS4ERR_NOMATCHING_LAYOUT: the
 * chimera MDS drops the layout and resumes the conflicting operation on any
 * non-OK reply (NFS4_OK would make it block waiting for a LAYOUTRETURN we are
 * not in a position to issue from here).  Fencing first guarantees the client
 * has already stopped using the layout the MDS is about to drop.
 */
nfsstat4
chimera_nfs4_cb_layoutrecall(
    struct chimera_nfs_shared        *shared,
    struct chimera_nfs_client_server *server,
    struct CB_LAYOUTRECALL4args      *args)
{
    struct chimera_nfs4_layout *layout;
    layoutrecall_type4          rtype   = args->clora_recall.lor_recalltype;
    const uint8_t              *rfh     = NULL;
    uint32_t                    rfh_len = 0;
    int                         fenced  = 0;

    if (rtype == LAYOUTRECALL4_FILE) {
        rfh     = args->clora_recall.lor_layout.lor_fh.data;
        rfh_len = args->clora_recall.lor_layout.lor_fh.len;
    }

    pthread_mutex_lock(&shared->pnfs_layout_lock);
    for (layout = shared->pnfs_layouts; layout; layout = layout->reg_next) {
        uint8_t *remote_fh;
        int      remote_fh_len;

        /* Only layouts belonging to the MDS that issued this recall.  The
         * server index is encoded at byte CHIMERA_VFS_MOUNT_ID_SIZE of the
         * local file handle. */
        if (layout->file_fh_len <= CHIMERA_VFS_MOUNT_ID_SIZE ||
            layout->file_fh[CHIMERA_VFS_MOUNT_ID_SIZE] != server->index) {
            continue;
        }

        if (rtype == LAYOUTRECALL4_FILE) {
            /* Match the remote (MDS) file handle the recall names against the
             * remote portion of our local handle. */
            chimera_nfs4_map_fh(layout->file_fh, layout->file_fh_len,
                                &remote_fh, &remote_fh_len);
            if ((uint32_t) remote_fh_len != rfh_len ||
                memcmp(remote_fh, rfh, rfh_len) != 0) {
                continue;
            }
        }
        /* LAYOUTRECALL4_FSID / _ALL: chimera's MDS only issues FILE recalls,
         * but fence every layout for this server as a safe superset. */

        chimera_nfs4_cb_fence_layout(layout);
        fenced++;
    }
    pthread_mutex_unlock(&shared->pnfs_layout_lock);

    chimera_nfsclient_info("pNFS CB_LAYOUTRECALL type=%u fenced=%d layout(s) for server %d",
                           rtype, fenced, server->index);

    /* Whether or not we held it, tell the MDS to drop it (we are not returning
     * it from the back channel); fencing already stopped our DS I/O. */
    return NFS4ERR_NOMATCHING_LAYOUT;
} /* chimera_nfs4_cb_layoutrecall */

static void
chimera_nfs4_acquire_finish(
    struct chimera_vfs_request      *request,
    struct chimera_nfs4_acquire_ctx *actx,
    int                              valid)
{
    struct chimera_nfs4_open_state *open_state = actx->open_state;
    struct chimera_nfs4_layout     *layout     = &open_state->layout;
    struct chimera_vfs_request     *waiters, *w, *next;

    /* A now-usable layout joins the registry so a back-channel CB_LAYOUTRECALL
     * can find and fence it; do this before publishing VALID so a recall that
     * races the first DS I/O always sees it. */
    if (valid) {
        chimera_nfs4_pnfs_layout_register(actx->shared, layout);
    }

    /* Publish the resolved state, then drain any requests that parked while we
     * were ACQUIRING.  The lock orders against late parkers in the read/write
     * path, which re-check the state under the same lock. */
    atomic_store(&layout->state, valid ? CHIMERA_NFS4_LAYOUT_VALID : CHIMERA_NFS4_LAYOUT_UNAVAIL);

    pthread_mutex_lock(&layout->acq_lock);
    waiters             = layout->acq_waiters;
    layout->acq_waiters = NULL;
    pthread_mutex_unlock(&layout->acq_lock);

    /* The request that triggered acquisition is re-dispatched first. */
    actx->redispatch(actx->thread, actx->shared, request, actx->private_data);

    for (w = waiters; w; w = next) {
        next    = w->next;
        w->next = NULL;
        chimera_nfs4_pnfs_replay(layout, w);
    }
} /* chimera_nfs4_acquire_finish */

static void
chimera_nfs4_getdeviceinfo_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request      *request = private_data;
    struct chimera_nfs4_acquire_ctx *actx    = request->plugin_data;
    struct GETDEVICEINFO4res        *gdi;
    struct chimera_vfs_layout_device dev;
    char                             host[80];
    int                              port, ds_version = 3, ds_minorversion = 0;
    int                              server_index;
    /* Prefer an RDMA data path when the MDS was mounted over RDMA; the device
     * may advertise both an rdma and a tcp netaddr, and we pick accordingly. */
    int                              prefer_rdma = actx->mds_thread->server->use_rdma;
    int                              ds_use_rdma;

    if (unlikely(status) || res->status != NFS4_OK || res->num_resarray < 2) {
        chimera_nfsclient_error("pNFS GETDEVICEINFO rpc failed: status=%d comp=%d nres=%d",
                                status, res ? res->status : -1, res ? res->num_resarray : -1);
        chimera_nfs4_acquire_finish(request, actx, 0);
        return;
    }

    gdi = &res->resarray[1].opgetdeviceinfo;
    if (gdi->gdir_status != NFS4_OK) {
        chimera_nfsclient_error("pNFS GETDEVICEINFO op failed: gdir_status=%d", gdi->gdir_status);
        chimera_nfs4_acquire_finish(request, actx, 0);
        return;
    }

    if (gdi->gdir_resok4.gdir_device_addr.da_layout_type != CHIMERA_NFS4_LAYOUT4_FLEX_FILES) {
        chimera_nfsclient_error("pNFS GETDEVICEINFO wrong da_layout_type=%u",
                                gdi->gdir_resok4.gdir_device_addr.da_layout_type);
        chimera_nfs4_acquire_finish(request, actx, 0);
        return;
    }

    memset(&dev, 0, sizeof(dev));
    memcpy(dev.deviceid, actx->deviceid, CHIMERA_VFS_DEVICEID_SIZE);

    if (chimera_nfs4_decode_ff_device_addr(
            gdi->gdir_resok4.gdir_device_addr.da_addr_body.data,
            gdi->gdir_resok4.gdir_device_addr.da_addr_body.len,
            &dev, &ds_version, &ds_minorversion, prefer_rdma) != 0) {
        chimera_nfsclient_error("pNFS GETDEVICEINFO ff_device_addr decode failed (len=%u)",
                                gdi->gdir_resok4.gdir_device_addr.da_addr_body.len);
        chimera_nfs4_acquire_finish(request, actx, 0);
        return;
    }
    ds_use_rdma = chimera_nfs4_netid_is_rdma(dev.netid);
    chimera_nfsclient_info("pNFS GETDEVICEINFO ok: netid=%s uaddr=%s ds_version=%d",
                           dev.netid, dev.uaddr, ds_version);

    /* First cut: only NFSv3 data servers are supported. */
    if (ds_version != 3) {
        chimera_nfsclient_info("pNFS: DS advertises NFSv%d (only v3 supported); using MDS",
                               ds_version);
        chimera_nfs4_acquire_finish(request, actx, 0);
        return;
    }

    if (chimera_nfs4_parse_uaddr(dev.uaddr, host, sizeof(host), &port) != 0) {
        chimera_nfs4_acquire_finish(request, actx, 0);
        return;
    }

    server_index = chimera_nfs4_pnfs_register_ds(actx->shared, host, port, ds_version,
                                                 ds_use_rdma,
                                                 actx->mds_thread->server->rdma_protocol);
    if (server_index < 0) {
        chimera_nfs4_acquire_finish(request, actx, 0);
        return;
    }

    chimera_nfs4_devcache_put(&actx->shared->pnfs_devcache, &dev, server_index);

    actx->open_state->layout.ds_server_index = server_index;
    chimera_nfs4_acquire_finish(request, actx, 1);
} /* chimera_nfs4_getdeviceinfo_callback */

static void
chimera_nfs4_getdeviceinfo(
    struct chimera_vfs_request      *request,
    struct chimera_nfs4_acquire_ctx *actx)
{
    struct chimera_nfs_shared               *shared        = actx->shared;
    struct chimera_nfs_client_server_thread *server_thread = actx->mds_thread;
    struct chimera_nfs4_client_session      *session       = server_thread->server->nfs4_session;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[2];
    struct evpl_rpc2_cred                    rpc2_cred;

    memset(&args, 0, sizeof(args));
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 2;

    argarray[0].argop = OP_SEQUENCE;   /* slot fields filled by compound_call */

    argarray[1].argop = OP_GETDEVICEINFO;
    memcpy(argarray[1].opgetdeviceinfo.gdia_device_id, actx->deviceid, CHIMERA_VFS_DEVICEID_SIZE);
    argarray[1].opgetdeviceinfo.gdia_layout_type      = CHIMERA_NFS4_LAYOUT4_FLEX_FILES;
    argarray[1].opgetdeviceinfo.gdia_maxcount         = CHIMERA_NFS4_PNFS_MAXCOUNT;
    argarray[1].opgetdeviceinfo.gdia_notify_types     = NULL;
    argarray[1].opgetdeviceinfo.num_gdia_notify_types = 0;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    (void) session;

    chimera_nfs4_compound_call(
        actx->thread, shared, server_thread, request,
        &args, &rpc2_cred, 0, 0, NULL, 0, 0,
        chimera_nfs4_getdeviceinfo_callback, request,
        chimera_nfs4_getdeviceinfo_retry, request);
} /* chimera_nfs4_getdeviceinfo */

static void
chimera_nfs4_layoutget_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request      *request    = private_data;
    struct chimera_nfs4_acquire_ctx *actx       = request->plugin_data;
    struct chimera_nfs4_open_state  *open_state = actx->open_state;
    struct LAYOUTGET4res            *lg;
    struct layout4                  *lo;
    int                              server_index;

    if (unlikely(status) || res->status != NFS4_OK || res->num_resarray < 3) {
        chimera_nfsclient_error("pNFS LAYOUTGET rpc failed: status=%d comp=%d nres=%d",
                                status, res ? res->status : -1, res ? res->num_resarray : -1);
        chimera_nfs4_acquire_finish(request, actx, 0);
        return;
    }

    lg = &res->resarray[2].oplayoutget;
    if (lg->logr_status != NFS4_OK || lg->logr_resok4.num_logr_layout < 1) {
        chimera_nfsclient_error("pNFS LAYOUTGET op failed: logr_status=%d num_layout=%d",
                                lg->logr_status, lg->logr_resok4.num_logr_layout);
        chimera_nfs4_acquire_finish(request, actx, 0);
        return;
    }

    lo = &lg->logr_resok4.logr_layout[0];
    if (lo->lo_content.loc_type != CHIMERA_NFS4_LAYOUT4_FLEX_FILES) {
        chimera_nfsclient_error("pNFS LAYOUTGET wrong loc_type=%u", lo->lo_content.loc_type);
        chimera_nfs4_acquire_finish(request, actx, 0);
        return;
    }

    if (chimera_nfs4_decode_ff_layout(lo->lo_content.loc_body.data,
                                      lo->lo_content.loc_body.len,
                                      &open_state->layout.segments[0]) != 0) {
        chimera_nfsclient_error("pNFS LAYOUTGET ff_layout decode failed (body_len=%u)",
                                lo->lo_content.loc_body.len);
        chimera_nfs4_acquire_finish(request, actx, 0);
        return;
    }
    chimera_nfsclient_info("pNFS LAYOUTGET ok: iomode=%u ds_fh_len=%u",
                           lo->lo_iomode, open_state->layout.segments[0].ds_fh_len);

    open_state->layout.segments[0].offset = lo->lo_offset;
    open_state->layout.segments[0].length = lo->lo_length;
    open_state->layout.segments[0].iomode = lo->lo_iomode;
    open_state->layout.num_segments       = 1;
    open_state->layout.iomode             = lo->lo_iomode;
    open_state->layout.layout_stateid     = lg->logr_resok4.logr_stateid;
    open_state->layout.return_on_close    = lg->logr_resok4.logr_return_on_close;

    memcpy(actx->deviceid, open_state->layout.segments[0].deviceid, CHIMERA_VFS_DEVICEID_SIZE);

    /* Device already resolved?  Reuse the cached DS server slot. */
    if (chimera_nfs4_devcache_find(&actx->shared->pnfs_devcache, actx->deviceid, &server_index)) {
        open_state->layout.ds_server_index = server_index;
        chimera_nfs4_acquire_finish(request, actx, 1);
        return;
    }

    chimera_nfs4_getdeviceinfo(request, actx);
} /* chimera_nfs4_layoutget_callback */

static void
chimera_nfs4_layoutget(
    struct chimera_vfs_request      *request,
    struct chimera_nfs4_acquire_ctx *actx)
{
    struct chimera_nfs_shared               *shared        = actx->shared;
    struct chimera_nfs_client_server_thread *server_thread = actx->mds_thread;
    struct chimera_nfs4_client_session      *session       = server_thread->server->nfs4_session;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[3];
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

    chimera_nfs4_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    /* Stash the file's own local handle so close can address it to the MDS. */
    memcpy(actx->open_state->layout.file_fh, request->fh, request->fh_len);
    actx->open_state->layout.file_fh_len = request->fh_len;

    memset(&args, 0, sizeof(args));
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 3;

    argarray[0].argop = OP_SEQUENCE;   /* slot fields filled by compound_call */

    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = fh;
    argarray[1].opputfh.object.len  = fhlen;

    /* Request a whole-file RW layout: it serves both reads and writes, so the
     * client never has to upgrade a read layout to write. */
    argarray[2].argop                                = OP_LAYOUTGET;
    argarray[2].oplayoutget.loga_signal_layout_avail = 0;
    argarray[2].oplayoutget.loga_layout_type         = CHIMERA_NFS4_LAYOUT4_FLEX_FILES;
    argarray[2].oplayoutget.loga_iomode              = LAYOUTIOMODE4_RW;
    argarray[2].oplayoutget.loga_offset              = 0;
    argarray[2].oplayoutget.loga_length              = UINT64_MAX;
    argarray[2].oplayoutget.loga_minlength           = 0;
    memset(&argarray[2].oplayoutget.loga_stateid, 0, sizeof(argarray[2].oplayoutget.loga_stateid));
    argarray[2].oplayoutget.loga_maxcount = CHIMERA_NFS4_PNFS_MAXCOUNT;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    (void) session;

    chimera_nfs4_compound_call(
        actx->thread, shared, server_thread, request,
        &args, &rpc2_cred, 0, 0, NULL, 0, 0,
        chimera_nfs4_layoutget_callback, request,
        chimera_nfs4_layoutget_retry, request);
} /* chimera_nfs4_layoutget */

/*
 * Kick off layout acquisition.  Caller has already won the NONE->ACQUIRING
 * transition.  Stores its context in plugin_data and starts LAYOUTGET.
 */
static void
chimera_nfs4_layout_acquire(
    struct chimera_nfs_thread               *thread,
    struct chimera_nfs_shared               *shared,
    struct chimera_vfs_request              *request,
    void                                    *private_data,
    struct chimera_nfs_client_server_thread *mds_thread,
    struct chimera_nfs4_open_state          *open_state,
    void (                                  *redispatch )(
        struct chimera_nfs_thread *,
        struct chimera_nfs_shared *,
        struct chimera_vfs_request *,
        void *))
{
    struct chimera_nfs4_acquire_ctx *actx = request->plugin_data;

    actx->thread       = thread;
    actx->shared       = shared;
    actx->private_data = private_data;
    actx->mds_thread   = mds_thread;
    actx->open_state   = open_state;
    actx->redispatch   = redispatch;

    /* Replay context for any requests that park while this acquisition runs. */
    open_state->layout.acq_thread  = thread;
    open_state->layout.acq_shared  = shared;
    open_state->layout.acq_private = private_data;

    chimera_nfs4_layoutget(request, actx);
} /* chimera_nfs4_layout_acquire */

/* ---------------------------------------------------------------------------
* Segment lookup + DS file-handle synthesis.
* ------------------------------------------------------------------------- */

/*
 * Return the layout segment fully covering [offset, offset+length), or NULL.
 * First cut: whole-file single segment, so this is a single containment test.
 */
static struct chimera_vfs_layout_segment *
chimera_nfs4_layout_find_segment(
    struct chimera_nfs4_layout *layout,
    uint64_t                    offset,
    uint64_t                    length)
{
    struct chimera_vfs_layout_segment *seg;
    uint64_t                           seg_end;

    if (layout->num_segments < 1) {
        return NULL;
    }

    seg = &layout->segments[0];

    if (offset < seg->offset) {
        return NULL;
    }

    if (seg->length != UINT64_MAX) {
        seg_end = seg->offset + seg->length;
        if (offset + length > seg_end) {
            return NULL;
        }
    }

    return seg;
} /* chimera_nfs4_layout_find_segment */

/* Build the synthetic DS file handle [mount_id][ds_index][ds_fh] in `out`. */
static int
chimera_nfs4_build_ds_fh(
    const struct chimera_vfs_request        *request,
    int                                      ds_server_index,
    const struct chimera_vfs_layout_segment *seg,
    uint8_t                                 *out)
{
    int len = 0;

    memcpy(out, request->fh, CHIMERA_VFS_MOUNT_ID_SIZE);   /* mount_id (opaque)  */
    out[CHIMERA_VFS_MOUNT_ID_SIZE] = (uint8_t) ds_server_index;
    len                            = CHIMERA_VFS_MOUNT_ID_SIZE + 1;
    memcpy(out + len, seg->ds_fh, seg->ds_fh_len);
    len += seg->ds_fh_len;
    return len;
} /* chimera_nfs4_build_ds_fh */

/* ---------------------------------------------------------------------------
* DS READ (NFSv3).
* ------------------------------------------------------------------------- */

static void
chimera_nfs4_pnfs_ds_read_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct READ3res             *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {
        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    request->read.r_length = res->resok.count;
    request->read.r_eof    = res->resok.eof;
    request->read.r_niov   = res->resok.data.niov;
    request->read.iov      = res->resok.data.iov;
    request->status        = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_pnfs_ds_read_callback */

static int
chimera_nfs4_pnfs_ds_read(
    struct chimera_nfs_thread         *thread,
    struct chimera_nfs_shared         *shared,
    struct chimera_vfs_request        *request,
    struct chimera_vfs_layout_segment *seg,
    int                                ds_server_index)
{
    struct chimera_nfs_client_server_thread *ds_thread;
    struct READ3args                         args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                  ds_fh[CHIMERA_NFS4_DS_FH_MAX];
    int                                      ds_fhlen;
    struct evpl_iovec                       *write_chunk_iov  = NULL;
    int                                      write_chunk_niov = 0;

    ds_fhlen  = chimera_nfs4_build_ds_fh(request, ds_server_index, seg, ds_fh);
    ds_thread = chimera_nfs_thread_get_server_thread(thread, ds_fh, ds_fhlen);

    if (!ds_thread || !ds_thread->nfs_conn) {
        return 0;       /* DS unreachable: caller falls back to MDS. */
    }

    if (!ds_thread->nfs_conn_ready) {
        /* Fresh RDMA DS conn: a READ may advertise an rkey for its reply data,
         * which the QP cannot do until CONNECTED.  Park until then. */
        request->next           = ds_thread->conn_waiters;
        ds_thread->conn_waiters = request;
        return 1;
    }

    args.file.data.data = seg->ds_fh;
    args.file.data.len  = seg->ds_fh_len;
    args.offset         = request->read.offset;
    args.count          = request->read.length;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    if (ds_thread->nfs_conn->rdma && request->read.dest_provided &&
        request->read.dest_niov == 1) {
        write_chunk_iov              = request->read.dest_iov;
        write_chunk_niov             = request->read.dest_niov;
        request->read.landed_in_dest = 1;
    }

    shared->nfs_v3.send_call_NFSPROC3_READ(
        &shared->nfs_v3.rpc2, thread->evpl, ds_thread->nfs_conn, &rpc2_cred,
        &args, 0, request->read.length, write_chunk_iov, write_chunk_niov, 0,
        chimera_nfs4_pnfs_ds_read_callback, request);
    return 1;
} /* chimera_nfs4_pnfs_ds_read */

/* ---------------------------------------------------------------------------
* DS WRITE (NFSv3).
* ------------------------------------------------------------------------- */

struct chimera_nfs4_pnfs_ds_write_ctx {
    struct chimera_nfs4_open_state *open_state;
    uint64_t                        end_offset;   /* offset + length */
};

static void
chimera_nfs4_pnfs_ds_write_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct WRITE3res            *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request            *request = private_data;
    struct chimera_nfs4_pnfs_ds_write_ctx *ctx     = request->plugin_data;
    struct chimera_nfs4_layout            *layout  = &ctx->open_state->layout;
    uint64_t                               cur;

    if (unlikely(status)) {
        chimera_nfsclient_error("pNFS DS write rpc failed: status=%d", status);
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {
        chimera_nfsclient_error("pNFS DS write NFS3 error: status=%d", res->status);
        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    /* Data lives on the DS; the MDS only learns the new file size when the
     * client reports it via LAYOUTCOMMIT (the layout leaves NO_LAYOUTCOMMIT
     * clear).  Track the high-water mark (atomic max, races with sibling
     * writes) and remember to commit on close. */
    cur = atomic_load(&layout->last_write_offset);
    while (ctx->end_offset > cur &&
           !atomic_compare_exchange_weak(&layout->last_write_offset, &cur, ctx->end_offset)) {
        /* cur reloaded by compare_exchange_weak; retry until our value is not
         * larger or the swap wins. */
    }
    layout->layoutcommit_needed = 1;

    /* Report UNSTABLE regardless of the DS's committed level: the data is on the
     * DS, but the MDS only learns the new file size from a LAYOUTCOMMIT, which
     * this proxy issues lazily (close-time, deferred by the open-handle cache).
     * Forcing UNSTABLE makes the upper client issue a COMMIT on close/sync, which
     * chimera_nfs4_commit turns into a LAYOUTCOMMIT -- so a re-open sees the real
     * size.  Reporting the DS's FILE_SYNC here would let the client skip COMMIT
     * and read back a stale (often zero) size, breaking close-to-open. */
    request->write.r_sync   = CHIMERA_VFS_WRITE_UNSTABLE;
    request->write.r_length = res->resok.count;
    request->status         = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_pnfs_ds_write_callback */

static int
chimera_nfs4_pnfs_ds_write(
    struct chimera_nfs_thread         *thread,
    struct chimera_nfs_shared         *shared,
    struct chimera_vfs_request        *request,
    struct chimera_nfs4_open_state    *open_state,
    struct chimera_vfs_layout_segment *seg,
    int                                ds_server_index)
{
    struct chimera_nfs_client_server_thread *ds_thread;
    struct chimera_nfs4_pnfs_ds_write_ctx   *ctx;
    struct WRITE3args                        args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                  ds_fh[CHIMERA_NFS4_DS_FH_MAX];
    int                                      ds_fhlen;

    ds_fhlen  = chimera_nfs4_build_ds_fh(request, ds_server_index, seg, ds_fh);
    ds_thread = chimera_nfs_thread_get_server_thread(thread, ds_fh, ds_fhlen);

    if (!ds_thread || !ds_thread->nfs_conn) {
        chimera_nfsclient_error("pNFS DS write: DS server %d unreachable, using MDS", ds_server_index);
        return 0;       /* DS unreachable: caller falls back to MDS. */
    }

    if (!ds_thread->nfs_conn_ready) {
        /* Fresh RDMA DS conn: the WRITE advertises an rkey for its data, which
         * the QP cannot do until CONNECTED.  Park until then (see
         * nfs_conn_ready) -- replayed by chimera_nfs4_pnfs_conn_connected. */
        request->next           = ds_thread->conn_waiters;
        ds_thread->conn_waiters = request;
        return 1;
    }

    chimera_nfsclient_debug("pNFS DS write -> server %d off=%lu len=%u",
                            ds_server_index, request->write.offset, request->write.length);

    ctx             = request->plugin_data;
    ctx->open_state = open_state;
    ctx->end_offset = request->write.offset + request->write.length;

    args.file.data.data = seg->ds_fh;
    args.file.data.len  = seg->ds_fh_len;
    args.offset         = request->write.offset;
    args.count          = request->write.length;
    args.stable         = request->write.sync;
    /* The DS WRITE3 marshaller MOVES (consumes + frees) the payload iovecs into
     * the outgoing RPC message.  Our payload is BORROWED from the NFS server
     * layer -- request->write.iov aliases the WRITE4 args, which
     * chimera_nfs4_write_complete releases on our completion -- so hand the
     * marshaller CLONES (each takes its own buffer ref, dropped when the DS RPC
     * message is released) and leave the borrowed originals intact.  Passing the
     * originals lets the marshaller free them out from under that server-side
     * release -> heap-use-after-free in evpl_iovecs_release. */
    struct evpl_iovec *ds_iov = malloc((size_t) request->write.niov *
                                       sizeof(*ds_iov));
    for (int i = 0; i < request->write.niov; i++) {
        evpl_iovec_clone(&ds_iov[i], &request->write.iov[i]);
    }
    args.data.iov    = ds_iov;
    args.data.niov   = request->write.niov;
    args.data.length = request->write.length;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    shared->nfs_v3.send_call_NFSPROC3_WRITE(
        &shared->nfs_v3.rpc2, thread->evpl, ds_thread->nfs_conn, &rpc2_cred,
        &args, 1, 0, NULL, 0, 0, chimera_nfs4_pnfs_ds_write_callback, request);

    /* The marshaller moved (and invalidated) the clones; free only the array. */
    free(ds_iov);
    return 1;
} /* chimera_nfs4_pnfs_ds_write */

/* ---------------------------------------------------------------------------
* Read/write redirect entry points (called from chimera_nfs4_read/write).
* ------------------------------------------------------------------------- */

/*
 * Try to park `request` while a layout acquisition is in flight.  Returns 1 if
 * the request was parked (it will be replayed when acquisition resolves), or 0
 * if acquisition already finished (the caller should re-evaluate the now-final
 * state).  Parking, rather than falling back to the MDS, is required: an MDS
 * I/O here would land a compound on the same NFSv4.1 session slot as the
 * in-flight LAYOUTGET/GETDEVICEINFO and be rejected as NFS4ERR_SEQ_MISORDERED.
 */
static int
chimera_nfs4_pnfs_try_park(
    struct chimera_nfs4_layout *layout,
    struct chimera_vfs_request *request)
{
    int parked = 0;

    pthread_mutex_lock(&layout->acq_lock);
    if (atomic_load(&layout->state) == CHIMERA_NFS4_LAYOUT_ACQUIRING) {
        request->next       = layout->acq_waiters;
        layout->acq_waiters = request;
        parked              = 1;
    }
    pthread_mutex_unlock(&layout->acq_lock);

    return parked;
} /* chimera_nfs4_pnfs_try_park */

int
chimera_nfs4_pnfs_read(
    struct chimera_nfs_thread               *thread,
    struct chimera_nfs_shared               *shared,
    struct chimera_vfs_request              *request,
    void                                    *private_data,
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_nfs4_open_state          *open_state)
{
    struct chimera_nfs4_layout        *layout = &open_state->layout;
    struct chimera_vfs_layout_segment *seg;
    int                                expected;

    if (!server_thread->server->mds_pnfs_capable) {
        return 0;
    }

    for ( ;;) {
        switch (atomic_load(&layout->state)) {
            case CHIMERA_NFS4_LAYOUT_VALID:
                seg = chimera_nfs4_layout_find_segment(layout, request->read.offset, request->read.length);
                if (!seg || layout->ds_server_index < 0) {
                    return 0;
                }
                return chimera_nfs4_pnfs_ds_read(thread, shared, request, seg, layout->ds_server_index);

            case CHIMERA_NFS4_LAYOUT_NONE:
                expected = CHIMERA_NFS4_LAYOUT_NONE;
                if (atomic_compare_exchange_strong(&layout->state, &expected, CHIMERA_NFS4_LAYOUT_ACQUIRING)) {
                    chimera_nfs4_layout_acquire(thread, shared, request, private_data,
                                                server_thread, open_state, chimera_nfs4_read);
                    return 1;
                }
                continue;       /* lost the race; re-evaluate (now ACQUIRING). */

            case CHIMERA_NFS4_LAYOUT_ACQUIRING:
                if (chimera_nfs4_pnfs_try_park(layout, request)) {
                    return 1;
                }
                continue;       /* acquisition just resolved; re-evaluate. */

            default:            /* UNAVAIL */
                return 0;
        } /* switch */
    }
} /* chimera_nfs4_pnfs_read */

int
chimera_nfs4_pnfs_write(
    struct chimera_nfs_thread               *thread,
    struct chimera_nfs_shared               *shared,
    struct chimera_vfs_request              *request,
    void                                    *private_data,
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_nfs4_open_state          *open_state)
{
    struct chimera_nfs4_layout        *layout = &open_state->layout;
    struct chimera_vfs_layout_segment *seg;
    int                                expected;

    if (!server_thread->server->mds_pnfs_capable) {
        return 0;
    }

    for ( ;;) {
        switch (atomic_load(&layout->state)) {
            case CHIMERA_NFS4_LAYOUT_VALID:
                if (layout->iomode != LAYOUTIOMODE4_RW) {
                    return 0;       /* read-only layout: write through the MDS. */
                }
                seg = chimera_nfs4_layout_find_segment(layout, request->write.offset, request->write.length);
                if (!seg || layout->ds_server_index < 0) {
                    return 0;
                }
                return chimera_nfs4_pnfs_ds_write(thread, shared, request, open_state, seg, layout->ds_server_index);

            case CHIMERA_NFS4_LAYOUT_NONE:
                expected = CHIMERA_NFS4_LAYOUT_NONE;
                if (atomic_compare_exchange_strong(&layout->state, &expected, CHIMERA_NFS4_LAYOUT_ACQUIRING)) {
                    chimera_nfs4_layout_acquire(thread, shared, request, private_data,
                                                server_thread, open_state, chimera_nfs4_write);
                    return 1;
                }
                continue;

            case CHIMERA_NFS4_LAYOUT_ACQUIRING:
                if (chimera_nfs4_pnfs_try_park(layout, request)) {
                    return 1;
                }
                continue;

            default:            /* UNAVAIL */
                return 0;
        } /* switch */
    }
} /* chimera_nfs4_pnfs_write */

/* ---------------------------------------------------------------------------
* Close-time LAYOUTCOMMIT + LAYOUTRETURN.
* ------------------------------------------------------------------------- */

struct chimera_nfs4_pnfs_close_ctx {
    struct chimera_nfs_thread               *thread;
    struct chimera_nfs_shared               *shared;
    struct chimera_nfs_client_server_thread *mds_thread;
    struct chimera_nfs4_open_state          *open_state;
};

static void chimera_nfs4_pnfs_layoutreturn(
    struct chimera_vfs_request *request);

static void
chimera_nfs4_pnfs_close_done(struct chimera_vfs_request *request)
{
    struct chimera_nfs4_pnfs_close_ctx *cctx = request->plugin_data;

    chimera_nfs4_open_state_free(cctx->open_state);
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_pnfs_close_done */

static void
chimera_nfs4_pnfs_layoutreturn_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    /* LAYOUTRETURN result is advisory at close: whether or not the MDS accepted
     * it, the client is done with the layout.  Always finish the close. */
    chimera_nfs4_pnfs_close_done(private_data);
} /* chimera_nfs4_pnfs_layoutreturn_callback */

static void
chimera_nfs4_pnfs_layoutcommit_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    /* Commit is best-effort; proceed to return the layout regardless. */
    chimera_nfsclient_info("pNFS LAYOUTCOMMIT done: rpc=%d comp=%d", status, res ? res->status : -1);
    chimera_nfs4_pnfs_layoutreturn(private_data);
} /* chimera_nfs4_pnfs_layoutcommit_callback */

static void
chimera_nfs4_pnfs_layoutreturn(struct chimera_vfs_request *request)
{
    struct chimera_nfs4_pnfs_close_ctx      *cctx          = request->plugin_data;
    struct chimera_nfs_shared               *shared        = cctx->shared;
    struct chimera_nfs_client_server_thread *server_thread = cctx->mds_thread;
    struct chimera_nfs4_client_session      *session       = server_thread->server->nfs4_session;
    struct chimera_nfs4_layout              *layout        = &cctx->open_state->layout;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[3];
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

    chimera_nfs4_map_fh(layout->file_fh, layout->file_fh_len, &fh, &fhlen);

    memset(&args, 0, sizeof(args));
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 3;

    argarray[0].argop = OP_SEQUENCE;   /* slot fields filled by compound_call */

    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = fh;
    argarray[1].opputfh.object.len  = fhlen;

    argarray[2].argop                                                    = OP_LAYOUTRETURN;
    argarray[2].oplayoutreturn.lora_reclaim                              = 0;
    argarray[2].oplayoutreturn.lora_layout_type                          = CHIMERA_NFS4_LAYOUT4_FLEX_FILES;
    argarray[2].oplayoutreturn.lora_iomode                               = LAYOUTIOMODE4_ANY;
    argarray[2].oplayoutreturn.lora_layoutreturn.lr_returntype           = LAYOUTRETURN4_FILE;
    argarray[2].oplayoutreturn.lora_layoutreturn.lr_layout.lrf_offset    = 0;
    argarray[2].oplayoutreturn.lora_layoutreturn.lr_layout.lrf_length    = UINT64_MAX;
    argarray[2].oplayoutreturn.lora_layoutreturn.lr_layout.lrf_stateid   = layout->layout_stateid;
    argarray[2].oplayoutreturn.lora_layoutreturn.lr_layout.lrf_body.data = NULL;
    argarray[2].oplayoutreturn.lora_layoutreturn.lr_layout.lrf_body.len  = 0;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    (void) session;

    chimera_nfs4_compound_call(
        cctx->thread, shared, server_thread, request,
        &args, &rpc2_cred, 0, 0, NULL, 0, 0,
        chimera_nfs4_pnfs_layoutreturn_callback, request,
        chimera_nfs4_layoutreturn_retry, request);
} /* chimera_nfs4_pnfs_layoutreturn */

static void
chimera_nfs4_pnfs_layoutcommit(struct chimera_vfs_request *request)
{
    struct chimera_nfs4_pnfs_close_ctx      *cctx          = request->plugin_data;
    struct chimera_nfs_shared               *shared        = cctx->shared;
    struct chimera_nfs_client_server_thread *server_thread = cctx->mds_thread;
    struct chimera_nfs4_client_session      *session       = server_thread->server->nfs4_session;
    struct chimera_nfs4_layout              *layout        = &cctx->open_state->layout;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[3];
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;
    uint64_t                                 high;

    chimera_nfs4_map_fh(layout->file_fh, layout->file_fh_len, &fh, &fhlen);

    memset(&args, 0, sizeof(args));
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 3;

    argarray[0].argop = OP_SEQUENCE;   /* slot fields filled by compound_call */

    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = fh;
    argarray[1].opputfh.object.len  = fhlen;

    argarray[2].argop                       = OP_LAYOUTCOMMIT;
    argarray[2].oplayoutcommit.loca_offset  = 0;
    argarray[2].oplayoutcommit.loca_length  = UINT64_MAX;
    argarray[2].oplayoutcommit.loca_reclaim = 0;
    argarray[2].oplayoutcommit.loca_stateid = layout->layout_stateid;
    /* Report the last byte written so the MDS can set the file size. */
    high                                                           = atomic_load(&layout->last_write_offset);
    argarray[2].oplayoutcommit.loca_last_write_offset.no_newoffset = 1;
    argarray[2].oplayoutcommit.loca_last_write_offset.no_offset    = high ? high - 1 : 0;
    argarray[2].oplayoutcommit.loca_time_modify.nt_timechanged     = 0;
    argarray[2].oplayoutcommit.loca_layoutupdate.lou_type          = CHIMERA_NFS4_LAYOUT4_FLEX_FILES;
    argarray[2].oplayoutcommit.loca_layoutupdate.lou_body.data     = NULL;
    argarray[2].oplayoutcommit.loca_layoutupdate.lou_body.len      = 0;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    (void) session;

    chimera_nfs4_compound_call(
        cctx->thread, shared, server_thread, request,
        &args, &rpc2_cred, 0, 0, NULL, 0, 0,
        chimera_nfs4_pnfs_layoutcommit_callback, request,
        chimera_nfs4_layoutcommit_retry, request);
} /* chimera_nfs4_pnfs_layoutcommit */

int
chimera_nfs4_pnfs_close(
    struct chimera_nfs_thread      *thread,
    struct chimera_nfs_shared      *shared,
    struct chimera_vfs_request     *request,
    struct chimera_nfs4_open_state *open_state)
{
    struct chimera_nfs4_layout              *layout = &open_state->layout;
    struct chimera_nfs_client_server_thread *mds_thread;
    struct chimera_nfs4_pnfs_close_ctx      *cctx;

    if (atomic_load(&layout->state) != CHIMERA_NFS4_LAYOUT_VALID ||
        layout->file_fh_len == 0) {
        return 0;       /* no layout held: caller frees + completes. */
    }

    mds_thread = chimera_nfs_thread_get_server_thread(thread, layout->file_fh, layout->file_fh_len);
    if (!mds_thread || !mds_thread->nfs_conn || !mds_thread->server->nfs4_session) {
        return 0;
    }

    cctx             = request->plugin_data;
    cctx->thread     = thread;
    cctx->shared     = shared;
    cctx->mds_thread = mds_thread;
    cctx->open_state = open_state;

    if (layout->layoutcommit_needed) {
        chimera_nfs4_pnfs_layoutcommit(request);
    } else {
        chimera_nfs4_pnfs_layoutreturn(request);
    }
    return 1;
} /* chimera_nfs4_pnfs_close */

/* ---------------------------------------------------------------------------
* Commit-time LAYOUTCOMMIT: flush the file size to the MDS WITHOUT releasing the
* layout (the file stays open).  pNFS DS writes are reported UNSTABLE (see
* chimera_nfs4_pnfs_ds_write_callback), so the upper client issues a COMMIT on
* close/sync; that COMMIT lands here and is turned into a LAYOUTCOMMIT so a
* subsequent open observes the real size instead of the stale pre-commit one
* (the close-time LAYOUTCOMMIT alone is deferred by the open-handle cache and
* loses the close-to-open race).
* ------------------------------------------------------------------------- */

static void chimera_nfs4_pnfs_commit_send(
    struct chimera_vfs_request *request);

static void
chimera_nfs4_pnfs_commit_retry(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *ctx)
{
    (void) thread;
    (void) shared;
    (void) ctx;
    chimera_nfs4_pnfs_commit_send(request);
} /* chimera_nfs4_pnfs_commit_retry */

static void
chimera_nfs4_pnfs_commit_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request         *request = private_data;
    struct chimera_nfs4_pnfs_close_ctx *cctx    = request->plugin_data;

    /* On success the MDS now reflects the high-water size, so a later
     * close/commit with no new writes can skip the round-trip.  On error leave
     * layoutcommit_needed set so close-time LAYOUTCOMMIT retries the flush. */
    if (status == 0 && res && res->status == NFS4_OK) {
        cctx->open_state->layout.layoutcommit_needed = 0;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_pnfs_commit_callback */

static void
chimera_nfs4_pnfs_commit_send(struct chimera_vfs_request *request)
{
    struct chimera_nfs4_pnfs_close_ctx      *cctx          = request->plugin_data;
    struct chimera_nfs_shared               *shared        = cctx->shared;
    struct chimera_nfs_client_server_thread *server_thread = cctx->mds_thread;
    struct chimera_nfs4_layout              *layout        = &cctx->open_state->layout;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[3];
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;
    uint64_t                                 high;

    chimera_nfs4_map_fh(layout->file_fh, layout->file_fh_len, &fh, &fhlen);

    memset(&args, 0, sizeof(args));
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 3;

    argarray[0].argop = OP_SEQUENCE;   /* slot fields filled by compound_call */

    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = fh;
    argarray[1].opputfh.object.len  = fhlen;

    argarray[2].argop                                              = OP_LAYOUTCOMMIT;
    argarray[2].oplayoutcommit.loca_offset                         = 0;
    argarray[2].oplayoutcommit.loca_length                         = UINT64_MAX;
    argarray[2].oplayoutcommit.loca_reclaim                        = 0;
    argarray[2].oplayoutcommit.loca_stateid                        = layout->layout_stateid;
    high                                                           = atomic_load(&layout->last_write_offset);
    argarray[2].oplayoutcommit.loca_last_write_offset.no_newoffset = 1;
    argarray[2].oplayoutcommit.loca_last_write_offset.no_offset    = high ? high - 1 : 0;
    argarray[2].oplayoutcommit.loca_time_modify.nt_timechanged     = 0;
    argarray[2].oplayoutcommit.loca_layoutupdate.lou_type          = CHIMERA_NFS4_LAYOUT4_FLEX_FILES;
    argarray[2].oplayoutcommit.loca_layoutupdate.lou_body.data     = NULL;
    argarray[2].oplayoutcommit.loca_layoutupdate.lou_body.len      = 0;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    chimera_nfs4_compound_call(
        cctx->thread, shared, server_thread, request,
        &args, &rpc2_cred, 0, 0, NULL, 0, 0,
        chimera_nfs4_pnfs_commit_callback, request,
        chimera_nfs4_pnfs_commit_retry, request);
} /* chimera_nfs4_pnfs_commit_send */

int
chimera_nfs4_pnfs_commit(
    struct chimera_nfs_thread      *thread,
    struct chimera_nfs_shared      *shared,
    struct chimera_vfs_request     *request,
    struct chimera_nfs4_open_state *open_state)
{
    struct chimera_nfs4_layout              *layout = &open_state->layout;
    struct chimera_nfs_client_server_thread *mds_thread;
    struct chimera_nfs4_pnfs_close_ctx      *cctx;

    if (atomic_load(&layout->state) != CHIMERA_NFS4_LAYOUT_VALID ||
        layout->file_fh_len == 0 || !layout->layoutcommit_needed) {
        return 0;       /* nothing to flush: caller completes the commit OK. */
    }

    mds_thread = chimera_nfs_thread_get_server_thread(thread, layout->file_fh, layout->file_fh_len);
    if (!mds_thread || !mds_thread->nfs_conn || !mds_thread->server->nfs4_session) {
        return 0;
    }

    cctx             = request->plugin_data;
    cctx->thread     = thread;
    cctx->shared     = shared;
    cctx->mds_thread = mds_thread;
    cctx->open_state = open_state;

    chimera_nfs4_pnfs_commit_send(request);
    return 1;
} /* chimera_nfs4_pnfs_commit */
