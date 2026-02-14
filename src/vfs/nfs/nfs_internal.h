// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include <endian.h>
#include <utlist.h>
#include "vfs/vfs.h"
#include "evpl/evpl.h"
#include "portmap_xdr.h"
#include "nfs_mount_xdr.h"
#include "nfs3_xdr.h"
#include "nfs4_xdr.h"
#include "uthash.h"
#include "common/misc.h"
#include "vfs/vfs_fh.h"
#include "evpl/evpl_rpc2.h"

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
 * Holds the session information established via EXCHANGE_ID + CREATE_SESSION
 */
struct chimera_nfs4_client_session {
    uint8_t   sessionid[NFS4_SESSIONID_SIZE];
    uint64_t  clientid;
    uint32_t  max_slots;           /* Maximum slots from server (ca_maxrequests) */
    uint32_t  next_slot_id;        /* Next slot ID to assign to a thread */
    uint32_t *slot_seqids;         /* Per-slot sequence IDs (array of max_slots) */
};

struct chimera_nfs_client_mount;

struct chimera_nfs_client_server_thread {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_shared        *shared;
    struct chimera_nfs_client_server *server;

    struct evpl_rpc2_conn            *portmap_conn;
    struct evpl_rpc2_conn            *mount_conn;
    struct evpl_rpc2_conn            *nfs_conn;

    uint32_t                          slot_id;     /* This thread's assigned NFS4.1 slot */
};

struct chimera_nfs_client_server {
    struct chimera_nfs_shared          *shared;
    int                                 state;
    int                                 refcnt;
    int                                 nfsvers;
    int                                 index;
    int                                 use_rdma;
    enum evpl_protocol_id               rdma_protocol;

    struct evpl_endpoint               *portmap_endpoint;
    struct evpl_endpoint               *mount_endpoint;
    struct evpl_endpoint               *nfs_endpoint;

    uint16_t                            mount_port;
    uint16_t                            nfs_port;

    struct chimera_vfs_request         *pending_mounts;

    char                                hostname[256];

    /* NFS4-specific fields (only used when nfsvers == 4) */
    struct chimera_nfs4_client_session *nfs4_session;
    uint8_t                             nfs4_verifier[NFS4_VERIFIER_SIZE];
    char                                nfs4_owner_id[128];
    int                                 nfs4_owner_id_len;
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

struct chimera_nfs_shared {
    struct chimera_nfs_client_mount   *mounts;

    struct chimera_nfs_client_server **servers;
    struct chimera_nfs_client_server  *servers_map;
    int                                max_servers;
    pthread_mutex_t                    lock;

    struct PORTMAP_V2                  portmap_v2;
    struct NFS_MOUNT_V3                mount_v3;
    struct NFS_V3                      nfs_v3;
    struct NFS_V4                      nfs_v4;
    struct NFS_V4_CB                   nfs_v4_cb;

    struct prometheus_histogram       *op_histogram;
    struct prometheus_metrics         *metrics;
};

struct chimera_nfs_thread {
    struct evpl                              *evpl;
    struct chimera_nfs_shared                *shared;
    struct evpl_rpc2_thread                  *rpc2_thread;
    struct chimera_nfs_client_server_thread **server_threads;
    struct chimera_nfs_client_open_handle    *free_open_handles;
    int                                       max_server_threads;
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
    struct chimera_nfs4_client_session      *session;
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

        /* Assign a slot ID for NFS4.1 sessions */
        session = thread->shared->servers[index]->nfs4_session;
        if (session && session->max_slots > 0) {
            /* Round-robin slot assignment. If more threads than slots, they share. */
            thread->server_threads[index]->slot_id = session->next_slot_id % session->max_slots;
            session->next_slot_id++;
        }
    }

    server_thread = thread->server_threads[index];

    if (unlikely(!server_thread->nfs_conn)) {
        enum evpl_protocol_id proto = server_thread->server->use_rdma
                                      ? server_thread->server->rdma_protocol
                                      : EVPL_STREAM_SOCKET_TCP;
        server_thread->nfs_conn = evpl_rpc2_client_connect(thread->rpc2_thread,
                                                           proto,
                                                           server_thread->server->nfs_endpoint,
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
 * Get the next sequence ID for an NFS4 session and increment it
 */
static inline uint32_t
chimera_nfs4_get_sequenceid(
    struct chimera_nfs4_client_session *session,
    uint32_t                            slot_id)
{
    return session->slot_seqids[slot_id]++;
} // chimera_nfs4_get_sequenceid // chimera_nfs4_get_sequenceid

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
        data                  += sizeof(uint32_t);
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

void chimera_nfs3_lookup(
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
void chimera_nfs3_mkdir(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_remove(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_readdir(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_open(
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
void chimera_nfs3_symlink(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_readlink(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_rename(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_mknod(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_link(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);

void chimera_nfs4_mount(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_lookup(
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
void chimera_nfs4_mkdir(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_remove(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_readdir(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_open(
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
void chimera_nfs4_symlink(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_readlink(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_rename(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_mknod(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_link(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
