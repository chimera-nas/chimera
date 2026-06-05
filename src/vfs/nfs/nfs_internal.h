// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdatomic.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include <endian.h>
#include <utlist.h>
#include "vfs/vfs.h"
#include "vfs/vfs_pnfs.h"
#include "evpl/evpl.h"
#include "portmap_xdr.h"
#include "nfs_mount_xdr.h"
#include "nfs3_xdr.h"
#include "nfs4_xdr.h"
#include "uthash.h"
#include "common/misc.h"
#include "vfs/vfs_fh.h"
#include "evpl/evpl_rpc2.h"
#include "nlm4_xdr.h"

/* Byte order conversion macros */
static inline uint32_t
chimera_nfs_hton32(uint32_t value)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return __builtin_bswap32(value);
#else // if __BYTE_ORDER == __LITTLE_ENDIAN
    return value;
#endif // if __BYTE_ORDER == __LITTLE_ENDIAN
} /* chimera_nfs_hton32 */

static inline uint64_t
chimera_nfs_hton64(uint64_t value)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return __builtin_bswap64(value);
#else // if __BYTE_ORDER == __LITTLE_ENDIAN
    return value;
#endif // if __BYTE_ORDER == __LITTLE_ENDIAN
} /* chimera_nfs_hton64 */

#define chimera_nfsclient_debug(...) chimera_debug("nfsclient", \
                                                   __FILE__, \
                                                   __LINE__, \
                                                   __VA_ARGS__)
#define chimera_nfsclient_info(...)  chimera_info("nfsclient", \
                                                  __FILE__, \
                                                  __LINE__, \
                                                  __VA_ARGS__)
#define chimera_nfsclient_error(...) chimera_error("nfsclient", \
                                                   __FILE__, \
                                                   __LINE__, \
                                                   __VA_ARGS__)
#define chimera_nfsclient_fatal(...) chimera_fatal("nfsclient", \
                                                   __FILE__, \
                                                   __LINE__, \
                                                   __VA_ARGS__)
#define chimera_nfsclient_abort(...) chimera_abort("nfsclient", \
                                                   __FILE__, \
                                                   __LINE__, \
                                                   __VA_ARGS__)

#define chimera_nfsclient_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "nfsclient", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_nfsclient_abort_if(cond, ...) \
        chimera_abort_if(cond, "nfsclient", __FILE__, __LINE__, __VA_ARGS__)

enum chimera_nfs_client_server_state {
    CHIMERA_NFS_CLIENT_SERVER_STATE_DISCOVERING,
    CHIMERA_NFS_CLIENT_SERVER_STATE_DISCOVERED,
};

enum chimera_nfs_client_mount_state {
    CHIMERA_NFS_CLIENT_MOUNT_STATE_MOUNTING,
    CHIMERA_NFS_CLIENT_MOUNT_STATE_MOUNTED,
};

/*
 * NFS4 client session state
 * Holds the session information established via EXCHANGE_ID + CREATE_SESSION.
 *
 * RFC 8881 §2.10.6.1 requires one outstanding request per fore-channel slot,
 * each slot carrying its own monotonic sequenceid.  The session owns the global
 * slot space (max_slots, granted by CREATE_SESSION); each per-thread connection
 * (chimera_nfs_client_server_thread) claims a disjoint block of slot indices
 * from `next_unclaimed` (under `lock`, once) and manages them thread-locally.
 */
struct chimera_nfs4_client_session {
    pthread_mutex_t lock;          /* guards next_unclaimed / overflow_rr only   */
    uint8_t         sessionid[NFS4_SESSIONID_SIZE];
    uint64_t        clientid;
    uint32_t        max_slots;     /* fore-channel slots granted (ca_maxrequests) */
    uint32_t        next_unclaimed; /* next free global slot index to hand out    */
    uint32_t        overflow_rr;   /* round-robin alias when the pool is exhausted */
};

struct chimera_nfs_client_mount;
struct chimera_nfs4_compound_ctx;   /* in-flight wrapper context (nfs4_slot.c)   */
struct chimera_nfs4_parked;         /* queued request awaiting a slot (nfs4_slot.c) */
struct chimera_nfs4_layout;         /* per-file pNFS layout (nfs4_pnfs.h)        */

/* One owned fore-channel slot. */
struct chimera_nfs4_slot {
    uint32_t global_id;             /* sa_slotid to send on the wire               */
    uint32_t seqid;                 /* next sa_sequenceid; starts at 1             */
    uint8_t  in_use;                /* 1 == one request outstanding on this slot   */
};

/*
 * Per-(thread,server) fore-channel slot table.  Touched by exactly one evpl
 * thread (its owning chimera_nfs_thread), so all of acquire/release/park/wake
 * are lock-free; only the one-time block claim takes session->lock.
 */
struct chimera_nfs4_slot_table {
    int                               initialized;
    uint32_t                          num_slots;
    struct chimera_nfs4_slot         *slots; /* [num_slots]                */
    uint32_t                         *free_stack;   /* local indices not in use   */
    int                               free_top;
    struct chimera_nfs4_parked       *wait_head;    /* FIFO of parked requests    */
    struct chimera_nfs4_parked       *wait_tail;
    struct chimera_nfs4_parked       *parked_freelist;
    struct chimera_nfs4_compound_ctx *inflight;     /* dll, for disconnect reset  */
    struct chimera_nfs4_compound_ctx *ctx_freelist;
};

struct chimera_nfs_client_server_thread {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_shared        *shared;
    struct chimera_nfs_client_server *server;

    struct evpl_rpc2_conn            *portmap_conn;
    struct evpl_rpc2_conn            *mount_conn;
    struct evpl_rpc2_conn            *nfs_conn;
    struct evpl_rpc2_conn            *nlm_conn;

    /* nfs_conn readiness.  RDMA forbids advertising an rkey (a bulk read/write
     * RDMA chunk) before the QP is bound to a device, i.e. before
     * EVPL_RPC2_NOTIFY_CONNECTED -- so a freshly-created RDMA conn is NOT ready
     * and pNFS DS I/O parks on conn_waiters until CONNECTED fires.  TCP tolerates
     * send-before-connect, so a TCP conn is marked ready at creation. */
    int                               nfs_conn_ready;
    struct chimera_vfs_request       *conn_waiters;

    struct chimera_nfs4_slot_table    slots;        /* NFS4.1 fore-channel slots  */
};

/*
 * A request from a mount (data) thread to the back-channel control thread to
 * establish a server's NFSv4.1 session on the control thread's persistent
 * connection.  The session (and thus the back channel that rides its
 * connection) must outlive the transient mount connection, so EXCHANGE_ID +
 * CREATE_SESSION run on the control thread; the control thread then queues the
 * item back to the originating chimera_nfs_thread (server_thread->thread) and
 * rings that thread's persistent cb_resume_doorbell to resume the mount
 * (RECLAIM_COMPLETE + root FH) on its own connection (bound via SEQUENCE).
 */
struct chimera_nfs4_cb_establish {
    struct chimera_nfs_client_server_thread *server_thread;
    struct chimera_vfs_request              *request;
    struct chimera_nfs4_client_session      *session;     /* in-progress session  */
    int                                      status;      /* 0 ok, else errno     */
    struct chimera_nfs4_cb_establish        *next;        /* queue link            */
};

struct chimera_nfs_client_server {
    struct chimera_nfs_shared          *shared;
    int                                 state;
    int                                 refcnt;
    int                                 nfsvers;
    int                                 index;
    int                                 use_rdma;
    int                                 nolock;
    enum evpl_protocol_id               rdma_protocol;

    struct evpl_endpoint               *portmap_endpoint;
    struct evpl_endpoint               *mount_endpoint;
    struct evpl_endpoint               *nfs_endpoint;
    struct evpl_endpoint               *nlm_endpoint;

    uint16_t                            mount_port;
    uint16_t                            nfs_port;
    uint16_t                            nlm_port;

    struct chimera_vfs_request         *pending_mounts;

    char                                hostname[256];

    /* NFS4-specific fields (only used when nfsvers == 4) */
    struct chimera_nfs4_client_session *nfs4_session;
    uint8_t                             nfs4_verifier[NFS4_VERIFIER_SIZE];
    char                                nfs4_owner_id[128];
    int                                 nfs4_owner_id_len;

    /* Persistent back-channel / control connection, owned by the control thread
     * (shared->cb_thread).  CREATE_SESSION binds the back channel to it, so the
     * server can deliver CB_COMPOUND here for the life of the session. */
    struct evpl_rpc2_conn              *cb_conn;

    /* pNFS (flex-files).  pnfs_requested is set from the `pnfs` mount option;
     * mds_pnfs_capable is set true once EXCHANGE_ID confirms USE_PNFS_MDS.
     * is_ds marks a server that was registered as a data server (reached for
     * direct DS I/O), not an MDS mount. */
    int                                 pnfs_requested;
    int                                 mds_pnfs_capable;
    int                                 is_ds;
};

struct chimera_nfs_client_mount {
    int                               status;
    int                               nfsvers;
    struct chimera_nfs_client_server *server;
    struct chimera_nfs_client_mount  *prev;
    struct chimera_nfs_client_mount  *next;
    struct chimera_vfs_request       *mount_request;
    char                              path[CHIMERA_VFS_PATH_MAX];
};

struct chimera_nfs_client_open_handle {
    int                                    dirty;
    struct chimera_nfs_client_open_handle *next;
};

/*
 * Client-side device cache (deviceid -> resolved DS server slot + decoded
 * device), mirror of the server's nfs_pnfs_devcache.  Populated by
 * GETDEVICEINFO so device resolution happens once per deviceid.
 */
#define CHIMERA_NFS4_CLIENT_DEVCACHE_MAX 64

struct chimera_nfs4_client_devcache_entry {
    uint8_t                          deviceid[CHIMERA_VFS_DEVICEID_SIZE];
    int                              valid;
    int                              server_index;  /* resolved DS server slot */
    struct chimera_vfs_layout_device device;
};

struct chimera_nfs4_client_devcache {
    pthread_mutex_t                           lock;
    uint32_t                                  count;
    struct chimera_nfs4_client_devcache_entry entries[CHIMERA_NFS4_CLIENT_DEVCACHE_MAX];
};

struct chimera_nfs_shared {
    struct chimera_nfs_client_mount    *mounts;

    struct chimera_nfs_client_server  **servers;
    struct chimera_nfs_client_server   *servers_map;
    int                                 max_servers;
    pthread_mutex_t                     lock;

    /* Number of NFS client (evpl) threads, counted at thread_init; used to size
     * each thread's fore-channel slot block (max_slots / nfs_thread_count). */
    _Atomic int                         nfs_thread_count;

    /* pNFS client device cache (flex-files). */
    struct chimera_nfs4_client_devcache pnfs_devcache;

    /* pNFS layout registry: active (VALID / fenced) flex-files layouts, so the
     * back-channel CB_LAYOUTRECALL handler can find one by file handle and fence
     * its DS I/O.  Layouts are embedded in open states; this list links them via
     * layout->reg_next under pnfs_layout_lock. */
    pthread_mutex_t                     pnfs_layout_lock;
    struct chimera_nfs4_layout         *pnfs_layouts;

    struct PORTMAP_V2                   portmap_v2;
    struct NFS_MOUNT_V3                 mount_v3;
    struct NFS_V3                       nfs_v3;
    struct NFS_V4                       nfs_v4;
    struct NFS_V4_CB                    nfs_v4_cb;
    struct NLM_V4                       nlm_v4;

    /* Back-channel control thread (nfs4_cb.c).  A single dedicated evpl thread
     * owns a persistent connection per server (server->cb_conn) on which it runs
     * EXCHANGE_ID + CREATE_SESSION with the back channel bound, and serves
     * incoming CB_COMPOUND.  Started lazily on the first NFSv4.1 mount.  Mount
     * threads request establishment by pushing onto cb_establish_queue (under
     * cb_lock) and ringing cb_doorbell. */
    struct evpl_thread                 *cb_thread;
    struct evpl                        *cb_evpl;
    struct evpl_rpc2_thread            *cb_rpc2_thread;
    struct evpl_doorbell                cb_doorbell;
    pthread_mutex_t                     cb_lock;
    struct chimera_nfs4_cb_establish   *cb_establish_queue;
    int                                 cb_started;

    struct prometheus_histogram        *op_histogram;
    struct prometheus_metrics          *metrics;

    /* evpl stream protocol for outbound plain-TCP connections, resolved from
     * the common tcp_flavor setting (chimera_vfs->tcp_flavor) at mount time.
     * Defaults to EVPL_STREAM_SOCKET_TCP. */
    enum evpl_protocol_id tcp_protocol;
};

struct chimera_nfs_thread {
    struct evpl                              *evpl;
    struct chimera_nfs_shared                *shared;
    struct evpl_rpc2_thread                  *rpc2_thread;
    struct chimera_nfs_client_server_thread **server_threads;
    struct chimera_nfs_client_open_handle    *free_open_handles;
    int                                       max_server_threads;

    /* Back-channel session-establishment completions destined for this thread.
     * The control thread pushes finished chimera_nfs4_cb_establish items here
     * (under cb_resume_lock) and rings cb_resume_doorbell; this thread drains
     * and resumes the parked mounts.  The doorbell is persistent (added at
     * thread_init, removed at thread_destroy) -- per RFC of evpl, doorbells must
     * not be freed from their own callback. */
    struct evpl_doorbell                      cb_resume_doorbell;
    pthread_mutex_t                           cb_resume_lock;
    struct chimera_nfs4_cb_establish         *cb_resume_done;
    int                                       cb_resume_armed;
};

static inline struct chimera_nfs_client_open_handle *
chimera_nfs_thread_open_handle_alloc(struct chimera_nfs_thread *thread)
{
    struct chimera_nfs_client_open_handle *open_handle = thread->free_open_handles;

    if (open_handle) {
        LL_DELETE(thread->free_open_handles, open_handle);
    } else {
        open_handle = calloc(1, sizeof(*open_handle));
    }

    return open_handle;
} // chimera_nfs_thread_open_handle_alloc

static inline void
chimera_nfs_thread_open_handle_free(
    struct chimera_nfs_thread             *thread,
    struct chimera_nfs_client_open_handle *open_handle)
{
    LL_PREPEND(thread->free_open_handles, open_handle);
} // chimera_nfs_thread_open_handle_free

static inline struct chimera_nfs_client_server_thread *
chimera_nfs_thread_get_server_thread(
    struct chimera_nfs_thread *thread,
    const uint8_t             *fh,
    int                        fhlen)
{
    struct chimera_nfs_client_server_thread *server_thread = NULL;
    int                                      index;

    if (unlikely(fhlen < CHIMERA_VFS_MOUNT_ID_SIZE + 1)) {
        return NULL;
    }

    /* Server index is at position CHIMERA_VFS_MOUNT_ID_SIZE (first byte of fh_fragment) */
    index = fh[CHIMERA_VFS_MOUNT_ID_SIZE];

    if (unlikely(index > thread->shared->max_servers)) {
        return NULL;
    }

    if (unlikely(thread->max_server_threads != thread->shared->max_servers && index >= thread->max_server_threads)) {
        thread->max_server_threads = thread->shared->max_servers;
        thread->server_threads     = realloc(thread->server_threads,
                                             thread->max_server_threads * sizeof(*thread->server_threads));
    }

    if (unlikely(!thread->server_threads[index])) {
        thread->server_threads[index]         = calloc(1, sizeof(*thread->server_threads[index]));
        thread->server_threads[index]->thread = thread;
        thread->server_threads[index]->shared = thread->shared;
        thread->server_threads[index]->server = thread->shared->servers[index];
        /* The NFS4.1 fore-channel slot block is claimed lazily on the first
         * COMPOUND through chimera_nfs4_compound_call() (nfs4_slot.c). */
    }

    server_thread = thread->server_threads[index];

    if (unlikely(!server_thread->nfs_conn)) {
        enum evpl_protocol_id proto = server_thread->server->use_rdma
                                      ? server_thread->server->rdma_protocol
                                      : server_thread->shared->tcp_protocol;
        server_thread->nfs_conn = evpl_rpc2_client_connect(thread->rpc2_thread,
                                                           proto,
                                                           server_thread->server->nfs_endpoint,
                                                           NULL, 0, NULL);
        /* TCP can be used immediately; an RDMA conn must reach CONNECTED before
         * any rkey-advertising op (see nfs_conn_ready). */
        server_thread->nfs_conn_ready = !server_thread->server->use_rdma;
    }

    if (unlikely(!server_thread->nlm_conn && server_thread->server->nlm_endpoint)) {
        server_thread->nlm_conn = evpl_rpc2_client_connect(thread->rpc2_thread,
                                                           server_thread->shared->tcp_protocol,
                                                           server_thread->server->nlm_endpoint,
                                                           NULL, 0, NULL);
    }

    return server_thread;
} // chimera_nfs_thread_get_server_thread // chimera_nfs_thread_get_server_thread

static inline void
chimera_nfs3_map_fh(
    const uint8_t *fh,
    int            fhlen,
    uint8_t      **mapped_fh,
    int           *mapped_fhlen)
{
    /* Skip mount_id (16 bytes) + server_index (1 byte) to get remote NFS fh */
    *mapped_fh    = (uint8_t *) fh + CHIMERA_VFS_MOUNT_ID_SIZE + 1;
    *mapped_fhlen = fhlen - CHIMERA_VFS_MOUNT_ID_SIZE - 1;
} // chimera_nfs3_map_fh

/*
 * Map local file handle to remote NFS4 file handle
 * Same format as NFS3: [mount_id (16 bytes)][server_index (1 byte)][remote_fh]
 */
static inline void
chimera_nfs4_map_fh(
    const uint8_t *fh,
    int            fhlen,
    uint8_t      **mapped_fh,
    int           *mapped_fhlen)
{
    /* Skip mount_id (16 bytes) + server_index (1 byte) to get remote NFS fh */
    *mapped_fh    = (uint8_t *) fh + CHIMERA_VFS_MOUNT_ID_SIZE + 1;
    *mapped_fhlen = fhlen - CHIMERA_VFS_MOUNT_ID_SIZE - 1;
} // chimera_nfs4_map_fh

/*
 * Byte-order conversion helpers
 */
static inline uint32_t
chimera_nfs_ntoh32(uint32_t value)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return __builtin_bswap32(value);
#else // if __BYTE_ORDER == __LITTLE_ENDIAN
    return value;
#endif // if __BYTE_ORDER == __LITTLE_ENDIAN
} // chimera_nfs_ntoh32

static inline uint64_t
chimera_nfs_ntoh64(uint64_t value)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return __builtin_bswap64(value);
#else // if __BYTE_ORDER == __LITTLE_ENDIAN
    return value;
#endif // if __BYTE_ORDER == __LITTLE_ENDIAN
} // chimera_nfs_ntoh64

/*
 * Convert NFS4 status to VFS error code
 */
static inline enum chimera_vfs_error
chimera_nfs4_status_to_errno(nfsstat4 status)
{
    switch (status) {
        case NFS4_OK:
            return CHIMERA_VFS_OK;
        case NFS4ERR_PERM:
            return CHIMERA_VFS_EPERM;
        case NFS4ERR_NOENT:
            return CHIMERA_VFS_ENOENT;
        case NFS4ERR_IO:
            return CHIMERA_VFS_EIO;
        case NFS4ERR_NXIO:
            return CHIMERA_VFS_ENXIO;
        case NFS4ERR_ACCESS:
            return CHIMERA_VFS_EACCES;
        case NFS4ERR_EXIST:
            return CHIMERA_VFS_EEXIST;
        case NFS4ERR_XDEV:
            return CHIMERA_VFS_EXDEV;
        case NFS4ERR_NOTDIR:
            return CHIMERA_VFS_ENOTDIR;
        case NFS4ERR_ISDIR:
            return CHIMERA_VFS_EISDIR;
        case NFS4ERR_INVAL:
            return CHIMERA_VFS_EINVAL;
        case NFS4ERR_FBIG:
            return CHIMERA_VFS_EFBIG;
        case NFS4ERR_NOSPC:
            return CHIMERA_VFS_ENOSPC;
        case NFS4ERR_ROFS:
            return CHIMERA_VFS_EROFS;
        case NFS4ERR_MLINK:
            return CHIMERA_VFS_EMLINK;
        case NFS4ERR_NAMETOOLONG:
            return CHIMERA_VFS_ENAMETOOLONG;
        case NFS4ERR_NOTEMPTY:
            return CHIMERA_VFS_ENOTEMPTY;
        case NFS4ERR_DQUOT:
            return CHIMERA_VFS_EDQUOT;
        case NFS4ERR_STALE:
        case NFS4ERR_FHEXPIRED:
        case NFS4ERR_STALE_CLIENTID:
        case NFS4ERR_STALE_STATEID:
            return CHIMERA_VFS_ESTALE;
        case NFS4ERR_BAD_COOKIE:
            return CHIMERA_VFS_EBADCOOKIE;
        case NFS4ERR_BADHANDLE:
            return CHIMERA_VFS_EBADF;
        case NFS4ERR_NOTSUPP:
            return CHIMERA_VFS_ENOTSUP;
        case NFS4ERR_TOOSMALL:
            return CHIMERA_VFS_EOVERFLOW;
        case NFS4ERR_SERVERFAULT:
            return CHIMERA_VFS_EFAULT;
        default:
            return CHIMERA_VFS_EINVAL;
    } // switch
} // chimera_nfs4_status_to_errno

/*
 * Unmarshall a file handle from NFS4 GETFH response
 * Builds local FH: [parent_mount_id][server_index][remote_fh]
 */
static inline void
chimera_nfs4_unmarshall_fh(
    const xdr_opaque         *fh,
    int                       server_index,
    const void               *parent_fh,
    struct chimera_vfs_attrs *attr)
{
    uint8_t fragment[CHIMERA_VFS_FH_SIZE];
    int     fragment_len;

    /* Build fh_fragment: [server_index][remote_fh_data] */
    fragment[0] = server_index;
    memcpy(fragment + 1, fh->data, fh->len);
    fragment_len = 1 + fh->len;

    attr->va_set_mask |= CHIMERA_VFS_ATTR_FH;
    attr->va_fh_len    = chimera_vfs_encode_fh_parent(parent_fh, fragment, fragment_len, attr->va_fh);
} // chimera_nfs4_unmarshall_fh

/*
 * Unmarshall attributes from NFS4 GETATTR response
 * This is a simplified version - it only handles basic attributes encoded
 * in the fattr4 structure.
 */
static inline void
chimera_nfs4_unmarshall_fattr(
    const struct fattr4      *fattr,
    struct chimera_vfs_attrs *attr)
{
    void    *data    = fattr->attr_vals.data;
    void    *dataend = data + fattr->attr_vals.len;
    uint32_t type;

    if (fattr->num_attrmask < 1) {
        return;
    }

    /* Parse attributes based on bitmap - they appear in order */
    if (fattr->attrmask[0] & (1 << FATTR4_TYPE)) {
        if (data + sizeof(uint32_t) > dataend) {
            return;
        }
        type               = chimera_nfs_ntoh32(*(uint32_t *) data);
        data              += sizeof(uint32_t);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
        switch (type) {
            case NF4REG:
                attr->va_mode = S_IFREG;
                break;
            case NF4DIR:
                attr->va_mode = S_IFDIR;
                break;
            case NF4BLK:
                attr->va_mode = S_IFBLK;
                break;
            case NF4CHR:
                attr->va_mode = S_IFCHR;
                break;
            case NF4LNK:
                attr->va_mode = S_IFLNK;
                break;
            case NF4SOCK:
                attr->va_mode = S_IFSOCK;
                break;
            case NF4FIFO:
                attr->va_mode = S_IFIFO;
                break;
            default:
                attr->va_mode = S_IFREG;
                break;
        } // switch
    }

    if (fattr->attrmask[0] & (1 << FATTR4_SIZE)) {
        if (data + sizeof(uint64_t) > dataend) {
            return;
        }
        attr->va_size      = chimera_nfs_ntoh64(*(uint64_t *) data);
        data              += sizeof(uint64_t);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
    }

    if (fattr->attrmask[0] & (1 << FATTR4_FILEID)) {
        if (data + sizeof(uint64_t) > dataend) {
            return;
        }
        attr->va_ino       = chimera_nfs_ntoh64(*(uint64_t *) data);
        data              += sizeof(uint64_t);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_INUM;
    }

    if (fattr->num_attrmask < 2) {
        return;
    }

    if (fattr->attrmask[1] & (1 << (FATTR4_MODE - 32))) {
        if (data + sizeof(uint32_t) > dataend) {
            return;
        }
        attr->va_mode     |= chimera_nfs_ntoh32(*(uint32_t *) data) & ~S_IFMT;
        data              += sizeof(uint32_t);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
    }

    if (fattr->attrmask[1] & (1 << (FATTR4_NUMLINKS - 32))) {
        if (data + sizeof(uint32_t) > dataend) {
            return;
        }
        attr->va_nlink     = chimera_nfs_ntoh32(*(uint32_t *) data);
        data              += sizeof(uint32_t);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_NLINK;
    }

    if (fattr->attrmask[1] & (1 << (FATTR4_OWNER - 32))) {
        uint32_t owner_len;
        uint32_t owner_padded_len;
        if (data + sizeof(uint32_t) > dataend) {
            return;
        }
        owner_len        = chimera_nfs_ntoh32(*(uint32_t *) data);
        data            += sizeof(uint32_t);
        owner_padded_len = (owner_len + 3) & ~3;
        if (data + owner_padded_len > dataend) {
            return;
        }
        /* Convert string to numeric uid */
        attr->va_uid       = strtoul(data, NULL, 10);
        data              += owner_padded_len;
        attr->va_set_mask |= CHIMERA_VFS_ATTR_UID;
    }

    if (fattr->attrmask[1] & (1 << (FATTR4_OWNER_GROUP - 32))) {
        uint32_t group_len;
        uint32_t group_padded_len;
        if (data + sizeof(uint32_t) > dataend) {
            return;
        }
        group_len        = chimera_nfs_ntoh32(*(uint32_t *) data);
        data            += sizeof(uint32_t);
        group_padded_len = (group_len + 3) & ~3;
        if (data + group_padded_len > dataend) {
            return;
        }
        /* Convert string to numeric gid */
        attr->va_gid       = strtoul(data, NULL, 10);
        data              += group_padded_len;
        attr->va_set_mask |= CHIMERA_VFS_ATTR_GID;
    }

    if (fattr->attrmask[1] & (1 << (FATTR4_TIME_ACCESS - 32))) {
        if (data + sizeof(uint64_t) + sizeof(uint32_t) > dataend) {
            return;
        }
        attr->va_atime.tv_sec  = chimera_nfs_ntoh64(*(uint64_t *) data);
        data                  += sizeof(uint64_t);
        attr->va_atime.tv_nsec = chimera_nfs_ntoh32(*(uint32_t *) data);
        data                  += sizeof(uint32_t);
        attr->va_set_mask     |= CHIMERA_VFS_ATTR_ATIME;
    }

    if (fattr->attrmask[1] & (1 << (FATTR4_TIME_MODIFY - 32))) {
        if (data + sizeof(uint64_t) + sizeof(uint32_t) > dataend) {
            return;
        }
        attr->va_mtime.tv_sec  = chimera_nfs_ntoh64(*(uint64_t *) data);
        data                  += sizeof(uint64_t);
        attr->va_mtime.tv_nsec = chimera_nfs_ntoh32(*(uint32_t *) data);
        attr->va_set_mask     |= CHIMERA_VFS_ATTR_MTIME;
    }
} // chimera_nfs4_unmarshall_fattr

/*
 * Initialize an RPC2 credential for AUTH_SYS from a VFS credential.
 * The RPC2 cred is stack-allocated by the caller.
 *
 * @param rpc2_cred     Pointer to stack-allocated evpl_rpc2_cred
 * @param vfs_cred      VFS credential with uid/gid/groups
 * @param machine_name  Machine name string (from chimera_vfs)
 * @param machine_name_len Length of machine name
 */
static inline void
chimera_nfs_init_rpc2_cred(
    struct evpl_rpc2_cred         *rpc2_cred,
    const struct chimera_vfs_cred *vfs_cred,
    const char                    *machine_name,
    int                            machine_name_len)
{
    uint32_t ngids;

    rpc2_cred->flavor = EVPL_RPC2_AUTH_SYS;

    /* Handle NULL credential - use root (uid=0, gid=0) */
    if (!vfs_cred) {
        rpc2_cred->authsys.uid      = 0;
        rpc2_cred->authsys.gid      = 0;
        rpc2_cred->authsys.num_gids = 0;
        rpc2_cred->authsys.gids     = NULL;
    } else {
        rpc2_cred->authsys.uid = vfs_cred->uid;
        rpc2_cred->authsys.gid = vfs_cred->gid;

        /* Assign pointer to gids array (valid for duration of call) */
        ngids = vfs_cred->ngids;
        if (ngids > EVPL_RPC2_AUTH_SYS_MAX_GIDS) {
            ngids = EVPL_RPC2_AUTH_SYS_MAX_GIDS;
        }
        rpc2_cred->authsys.num_gids = ngids;
        rpc2_cred->authsys.gids     = (uint32_t *) vfs_cred->gids;
    }

    /* Assign pointer to machine name (valid for duration of call) */
    rpc2_cred->authsys.machinename     = machine_name;
    rpc2_cred->authsys.machinename_len = machine_name_len;
} /* chimera_nfs_init_rpc2_cred */

/* ---- NFSv4.1 back channel / callback receiver (nfs4_cb.c) --------------- */

/* rpc2 recv_call handler for incoming CB_COMPOUND on a back-channel connection. */
void chimera_nfs4_cb_compound(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct CB_COMPOUND4args   *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

/* CB_LAYOUTRECALL handler.  Weak default (no layouts held) returns
 * NFS4ERR_NOMATCHING_LAYOUT; the pNFS client overrides it. */
nfsstat4 chimera_nfs4_cb_layoutrecall(
    struct chimera_nfs_shared        *shared,
    struct chimera_nfs_client_server *server,
    struct CB_LAYOUTRECALL4args      *args);

/* Stop the back-channel control thread (module destroy).  Started lazily by
 * chimera_nfs4_cb_establish_session, so no explicit start entry point. */
void chimera_nfs4_cb_control_stop(
    struct chimera_nfs_shared *shared);

/* Per-thread back-channel resume doorbell lifecycle (called from the nfs
 * module's thread_init / thread_destroy). */
void chimera_nfs4_cb_thread_init(
    struct chimera_nfs_thread *thread);
void chimera_nfs4_cb_thread_destroy(
    struct chimera_nfs_thread *thread);

/*
 * Request the control thread establish `server`'s NFSv4.1 session on its
 * persistent connection (with the back channel bound), then resume the mount on
 * the calling thread via chimera_nfs4_mount_resume_after_session().  Parks
 * `request` (does not complete or send on the caller's connection).
 */
void chimera_nfs4_cb_establish_session(
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_vfs_request              *request);

/* Resume a mount on its originating thread once its session exists: send
 * RECLAIM_COMPLETE then resolve the export root (nfs4_mount.c). */
void chimera_nfs4_mount_resume_after_session(
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_vfs_request              *request);

void chimera_nfs3_dispatch(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_dispatch(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);

/* ---- NFSv4.1 session fore-channel slot layer (nfs4_slot.c) -------------- */

/* How a parked request is replayed once a slot frees.  For plain VFS ops this
 * is chimera_nfs4_dispatch (re-routes by request->opcode); internal multi-step
 * issuers (mount, pNFS) pass a shim that re-runs that step. */
typedef void (*chimera_nfs4_retry_fn)(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);

/*
 * Issue an NFSv4.1 COMPOUND with correct session-slot discipline (one
 * outstanding request per slot, monotonic per-slot seqid).  argarray[0] must be
 * an OP_SEQUENCE placeholder (argop set); this fills sa_sessionid/sa_slotid/
 * sa_sequenceid/sa_highest_slotid/sa_cachethis from an acquired slot, sends, and
 * frees the slot when the reply arrives before invoking `cb`.  If no slot is
 * free the request is parked and replayed via (retry_fn, retry_ctx) when one
 * frees.  `cb`/`cb_private` are the caller's original COMPOUND callback/arg.
 */
void chimera_nfs4_compound_call(
    struct chimera_nfs_thread *thread,
    struct chimera_nfs_shared *shared,
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_vfs_request *request,
    struct COMPOUND4args *args,
    const struct evpl_rpc2_cred *cred,
    int ddp,
    int max_rdma_write_chunk,
    struct evpl_iovec *write_chunk_iov,
    int write_chunk_niov,
    int max_rdma_reply_chunk,
    void ( *cb )(struct evpl *, const struct evpl_rpc2_verf *,
                 struct COMPOUND4res *, int, void *),
    void *cb_private,
    chimera_nfs4_retry_fn retry_fn,
    void *retry_ctx);

/* Free a server_thread's slot table (called from thread teardown). */
void chimera_nfs4_slot_table_destroy(
    struct chimera_nfs4_slot_table *st);

/* On connection loss: error-complete in-flight + parked requests and reset the
 * slot table so the next op re-establishes cleanly. */
void chimera_nfs4_slot_table_reset(
    struct evpl                    *evpl,
    struct chimera_nfs4_slot_table *st);

/* pNFS DS-connection readiness (nfs4_pnfs.c), driven from chimera_nfs_notify:
 * replay DS I/O parked until the RDMA conn connected, or error-complete it if
 * the conn dropped first. */
void chimera_nfs4_pnfs_conn_connected(
    struct chimera_nfs_client_server_thread *server_thread);
void chimera_nfs4_pnfs_conn_failed(
    struct chimera_nfs_client_server_thread *server_thread);

void chimera_nfs3_mount(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);

void chimera_nfs3_umount(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);

void chimera_nfs3_lookup_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_getattr(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_setattr(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_mkdir_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_remove_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_readdir(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_open_fh(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_open_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_close(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_read(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_write(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_commit(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_symlink_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_readlink(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_rename_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_mknod_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_link_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_lock(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);

void chimera_nfs4_mount(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_lookup_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_getattr(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_setattr(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_mkdir_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_remove_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_readdir(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_open_fh(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_open_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_close(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_umount(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_read(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_write(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_commit(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_symlink_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_readlink(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_rename_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_mknod_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_link_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
