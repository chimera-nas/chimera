// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>

#include "vfs/vfs.h"
#include "vfs/vfs_fh.h"
#include "vfs/vfs_error.h"
#include "vfs/vfs_attrs.h"
#include "evpl/evpl.h"
#include "common/logging.h"
#include "common/evpl_iovec_cursor.h"
#include "server/smb/smb2.h"
#include "smb_ntlm.h"

#define chimera_smbclient_debug(...) chimera_debug("smbclient", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_smbclient_info(...)  chimera_info("smbclient", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_smbclient_error(...) chimera_error("smbclient", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_smbclient_fatal(...) chimera_fatal("smbclient", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_smbclient_abort_if(cond, ...) \
        chimera_abort_if(cond, "smbclient", __FILE__, __LINE__, __VA_ARGS__)

/* SMB2 default port for this client. */
#define CHIMERA_SMB_CLIENT_PORT             445
#define CHIMERA_SMB_CLIENT_DEFAULT_DOMAIN   "WORKGROUP"
#define CHIMERA_SMB_CLIENT_MAX_SERVERS      64

/* Dialects advertised in NEGOTIATE, ascending.  The chimera server selects the
 * highest dialect the client offers, so this list determines what we end up
 * speaking (2.1 unsigned through 3.1.1 with preauth-integrity + signing). */
#define CHIMERA_SMB_CLIENT_DIALECTS \
        { SMB2_DIALECT_2_1, SMB2_DIALECT_3_0, SMB2_DIALECT_3_0_2, SMB2_DIALECT_3_1_1 }
#define CHIMERA_SMB_CLIENT_NUM_DIALECTS     4

/* Client GUID is fixed-zero (single-instance); 3.1.1 preauth salt length. */
#define CHIMERA_SMB_CLIENT_PREAUTH_SALT_LEN 32

/* The SMB client is a PATH-ONLY VFS backend (no persistent file handles).  File
 * handles come in exactly two shapes:
 *
 *   mount root:  [ mount_id (16) ][ server_index (1) ]                = 17 bytes
 *   open token:  [ mount_id (16) ][ server_index (1) ][ FileId (16) ] = 33 bytes
 *
 * The mount root is the ONLY re-openable fh (open_fh on it CREATEs the share
 * root).  An open-token fh is an opaque per-open identity built at open time
 * from the SMB FileId; it is used only for open-cache keying and routing, never
 * to re-derive a path, and open_fh of one returns ESTALE.  All metadata is
 * addressed by full mount-relative paths carried in request->X.name. */
#define CHIMERA_SMB_FH_SERVER_OFFSET        CHIMERA_VFS_MOUNT_ID_SIZE
#define CHIMERA_SMB_ROOT_FH_LEN             (CHIMERA_VFS_MOUNT_ID_SIZE + 1)
#define CHIMERA_SMB_OPEN_FH_LEN             (CHIMERA_VFS_MOUNT_ID_SIZE + 1 + 16)
/* Longest mount-relative path the client will encode in a single SMB request. */
#define CHIMERA_SMB_PATH_MAX                1024

/* ---- little-endian wire helpers ---------------------------------------- */

static inline uint16_t
smb_wire_le16(const uint8_t *p)
{
    return (uint16_t) p[0] | ((uint16_t) p[1] << 8);
} /* smb_wire_le16 */

static inline uint32_t
smb_wire_le32(const uint8_t *p)
{
    return (uint32_t) p[0] | ((uint32_t) p[1] << 8) |
           ((uint32_t) p[2] << 16) | ((uint32_t) p[3] << 24);
} /* smb_wire_le32 */

static inline uint64_t
smb_wire_le64(const uint8_t *p)
{
    return (uint64_t) smb_wire_le32(p) | ((uint64_t) smb_wire_le32(p + 4) << 32);
} /* smb_wire_le64 */

static inline void
smb_wire_set_le16(
    uint8_t *p,
    uint16_t v)
{
    p[0] = (uint8_t) (v & 0xff);
    p[1] = (uint8_t) ((v >> 8) & 0xff);
} /* smb_wire_set_le16 */

static inline void
smb_wire_set_le32(
    uint8_t *p,
    uint32_t v)
{
    p[0] = (uint8_t) (v & 0xff);
    p[1] = (uint8_t) ((v >> 8) & 0xff);
    p[2] = (uint8_t) ((v >> 16) & 0xff);
    p[3] = (uint8_t) ((v >> 24) & 0xff);
} /* smb_wire_set_le32 */

static inline void
smb_wire_set_le64(
    uint8_t *p,
    uint64_t v)
{
    smb_wire_set_le32(p, (uint32_t) (v & 0xffffffffULL));
    smb_wire_set_le32(p + 4, (uint32_t) (v >> 32));
} /* smb_wire_set_le64 */

/* NetBIOS-over-TCP framing prefix (4 bytes: zero + 24-bit big-endian length). */
struct smb_client_netbios_header {
    uint32_t word;
} __attribute__((packed));

/* ---- module state ------------------------------------------------------ */

struct chimera_smb_client_conn;
struct chimera_smb_client_thread;

/* A 16-byte SMB2 file handle (persistent + volatile id), returned by CREATE and
 * echoed in every subsequent op on that open.  Valid on any connection bound to
 * the same session, which is what lets per-thread connections share opens. */
struct chimera_smb_client_file_id {
    uint64_t pid;
    uint64_t vid;
};

/* Per-server registration (global), holding the shared SMB session established
 * once at MOUNT.  Every per-thread connection to this server reuses session_id /
 * tree_id, so a file opened on one thread's connection is usable from another. */
struct chimera_smb_client_server {
    int                   index;
    int                   in_use;
    char                  hostname[256];
    char                  share[256];
    char                  user[256];
    char                  domain[256];
    char                  password[256];
    uint16_t              port;

    struct evpl_endpoint *endpoint;

    /* Shared session state, written once by the mount handshake. */
    uint16_t              dialect;
    uint16_t              security_mode;
    uint64_t              session_id;
    uint32_t              tree_id;
    int                   session_ready;

    /* Server-advertised maxima from NEGOTIATE; reads/writes are chunked to these
     * (0 until captured -- callers fall back to the safe per-PDU defaults). */
    uint32_t              max_transact;
    uint32_t              max_read;
    uint32_t              max_write;

    /* Signing state, derived once at SESSION_SETUP on the mount connection and
    * shared by every per-thread connection to this server (the signing key is
    * per-SESSION).  signing_active is set only when the negotiated dialect is
    * 3.x AND a signing key has been derived; the unsigned 2.x path leaves it 0.
    *
    *   signing_alg: for 3.1.1, the SMB2_SIGNING_* algorithm the server selected
    *   (GMAC/CMAC/HMAC-SHA256); unused for 3.0/3.0.2 (always AES-128-CMAC). */
    int                   signing_active;
    uint16_t              signing_alg;
    uint8_t               signing_key[16];
};

struct chimera_smb_client_shared {
    pthread_mutex_t                    lock;
    struct chimera_smb_client_server **servers;
    int                                max_servers;
    enum evpl_protocol_id tcp_protocol;
};

/* Per-open state stored in the VFS open handle's vfs_private; carries the SMB
 * FileId so any thread's connection can address the open. */
struct chimera_smb_client_open {
    struct chimera_smb_client_file_id file_id;
    uint8_t                           server_index;
    uint8_t                           is_directory;
};

/* A network-open-information block (CREATE/CLOSE/QUERY_INFO embed it). */
struct smb_open_info {
    uint64_t crttime, atime, mtime, ctime;
    uint64_t alloc_size, end_of_file;
    uint32_t file_attributes;
};

/* Parsed SMB2 CREATE response: FileId + create action + embedded attrs. */
struct smb_create_result {
    struct chimera_smb_client_file_id file_id;
    uint32_t                          create_action;
    struct smb_open_info              info;
};

/* Transient per-op state kept in request->plugin_data across a CREATE -> ... ->
* CLOSE chain (ops that open a path transiently: lookup/mkdir/remove/rename). */
struct chimera_smb_op_state {
    struct chimera_smb_client_file_id file_id;
};

/* The continuation invoked when the reply for a specific message_id arrives.
* `body` is positioned at the SMB2 body (consumed == sizeof(smb2_header)). */
typedef void (*chimera_smb_client_reply_cb)(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg);

/* The entry point that issues an op once its connection is READY. */
typedef void (*chimera_smb_client_start_fn)(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);

/* One in-flight request awaiting its reply (keyed by message_id).  `request` is
 * the VFS request whose op chain this PDU belongs to, so a connection drop can
 * error-complete it. */
struct chimera_smb_client_pending {
    uint64_t                           message_id;
    chimera_smb_client_reply_cb        cb;
    void                              *arg;
    struct chimera_vfs_request        *request;
    struct chimera_smb_client_pending *next;
};

/* One op deferred until the connection finishes connect + NEGOTIATE. */
struct chimera_smb_client_deferred {
    struct chimera_vfs_request         *request;
    chimera_smb_client_start_fn         start;
    struct chimera_smb_client_deferred *next;
};

enum chimera_smb_client_conn_state {
    CHIMERA_SMB_CONN_NEW = 0,
    CHIMERA_SMB_CONN_CONNECTING,
    CHIMERA_SMB_CONN_READY,
    CHIMERA_SMB_CONN_FAILED,
};

struct chimera_smb_client_conn {
    struct chimera_smb_client_thread   *thread;
    struct evpl                        *evpl;
    struct evpl_bind                   *bind;
    struct chimera_smb_client_server   *server;

    enum chimera_smb_client_conn_state  state;
    int                                 closing;   /* evpl_close already issued  */

    uint64_t                            next_message_id;

    struct chimera_smb_client_pending  *pending;   /* in-flight, by message_id  */
    struct chimera_smb_client_deferred *deferred;  /* queued until READY        */

    /* All connections a thread owns are linked here so thread teardown can
     * detach every one (the DISCONNECTED notify that frees a conn is deferred to
     * evpl_destroy, after the thread struct is gone). */
    struct chimera_smb_client_conn     *list_next;

    /* NTLM state + mount request, used only by the connection that establishes
     * the shared session (the MOUNT connection). */
    struct smb_ntlm_client              ntlm;
    struct chimera_vfs_request         *mount_request;

    /* 3.1.1 preauth-integrity hash, maintained ONLY on the mount connection
     * while it runs NEGOTIATE -> SESSION_SETUP.  Folded over the raw SMB2
     * messages (header+body, no NetBIOS framing, no trailing pad), mirroring the
     * server's conn->preauth_hash.  Carries the dialect/signing_alg the mount
     * conn negotiated until they are committed to the shared server struct. */
    uint8_t                             preauth_hash[SMB2_PREAUTH_HASH_SIZE];
    uint16_t                            negotiated_dialect;
    uint16_t                            negotiated_signing_alg;
};

struct chimera_smb_client_thread {
    struct evpl                      *evpl;
    struct chimera_smb_client_shared *shared;
    struct chimera_smb_client_conn  **conns;       /* routing table, by server index */
    struct chimera_smb_client_conn   *conns_list;  /* every conn this thread owns */
    int                               max_conns;
};

/* ---- status mapping ---------------------------------------------------- */

static inline enum chimera_vfs_error
chimera_smb_status_to_errno(uint32_t status)
{
    switch (status) {
        case SMB2_STATUS_SUCCESS:
            return CHIMERA_VFS_OK;
        case SMB2_STATUS_END_OF_FILE:
            return CHIMERA_VFS_OK;          /* short read at EOF, handled by caller */

        /* ---- not found ---- */
        case SMB2_STATUS_OBJECT_NAME_NOT_FOUND:
        case SMB2_STATUS_OBJECT_PATH_NOT_FOUND:
        case SMB2_STATUS_NO_SUCH_FILE:
        case SMB2_STATUS_NOT_FOUND:
        case SMB2_STATUS_BAD_NETWORK_NAME:
        case SMB2_STATUS_BAD_NETWORK_PATH:
        case SMB2_STATUS_DELETE_PENDING:    /* name is going away -> treat as gone */
            return CHIMERA_VFS_ENOENT;

        /* ---- permission / access ---- */
        case SMB2_STATUS_ACCESS_DENIED:
        case SMB2_STATUS_NETWORK_ACCESS_DENIED:
        case SMB2_STATUS_LOGON_FAILURE:
        case SMB2_STATUS_ACCOUNT_DISABLED:
        case SMB2_STATUS_ACCOUNT_LOCKED_OUT:
        case SMB2_STATUS_WRONG_PASSWORD:
            return CHIMERA_VFS_EACCES;
        case SMB2_STATUS_PRIVILEGE_NOT_HELD:
        case SMB2_STATUS_CANNOT_DELETE:
            return CHIMERA_VFS_EPERM;
        case SMB2_STATUS_MEDIA_WRITE_PROTECTED:
            return CHIMERA_VFS_EROFS;

        /* ---- existence / type ---- */
        case SMB2_STATUS_OBJECT_NAME_COLLISION:
            return CHIMERA_VFS_EEXIST;
        case SMB2_STATUS_NOT_A_DIRECTORY:
            return CHIMERA_VFS_ENOTDIR;
        case SMB2_STATUS_FILE_IS_A_DIRECTORY:
            return CHIMERA_VFS_EISDIR;
        case SMB2_STATUS_DIRECTORY_NOT_EMPTY:
            return CHIMERA_VFS_ENOTEMPTY;
        case SMB2_STATUS_NOT_SAME_DEVICE:
            return CHIMERA_VFS_EXDEV;
        case SMB2_STATUS_TOO_MANY_LINKS:
            return CHIMERA_VFS_EMLINK;
        case SMB2_STATUS_STOPPED_ON_SYMLINK:
            return CHIMERA_VFS_ELOOP;
        case SMB2_STATUS_NOT_A_REPARSE_POINT:
            return CHIMERA_VFS_EINVAL;      /* readlink of a non-symlink */

        /* ---- arguments ---- */
        case SMB2_STATUS_INVALID_PARAMETER:
        case SMB2_STATUS_INVALID_INFO_CLASS:
        case SMB2_STATUS_INFO_LENGTH_MISMATCH:
        case SMB2_STATUS_INVALID_DEVICE_REQUEST:
            return CHIMERA_VFS_EINVAL;
        case SMB2_STATUS_NAME_TOO_LONG:
            return CHIMERA_VFS_ENAMETOOLONG;
        case SMB2_STATUS_BUFFER_TOO_SMALL:
        case SMB2_STATUS_BUFFER_OVERFLOW:
            return CHIMERA_VFS_ERANGE;

        /* ---- space / quota ---- */
        case SMB2_STATUS_DISK_FULL:
        case SMB2_STATUS_ALLOTTED_SPACE_EXCEEDED:
            return CHIMERA_VFS_ENOSPC;
        case SMB2_STATUS_QUOTA_EXCEEDED:
            return CHIMERA_VFS_EDQUOT;

        /* ---- sharing / locking (transient) ---- */
        case SMB2_STATUS_SHARING_VIOLATION:
        case SMB2_STATUS_FILE_LOCK_CONFLICT:
        case SMB2_STATUS_LOCK_NOT_GRANTED:
            return CHIMERA_VFS_EAGAIN;

        /* ---- handle / file ---- */
        case SMB2_STATUS_INVALID_HANDLE:
        case SMB2_STATUS_SMB_BAD_FID:
        case SMB2_STATUS_FILE_CLOSED:
            return CHIMERA_VFS_EBADF;
        case SMB2_STATUS_TOO_MANY_OPENED_FILES:
            return CHIMERA_VFS_EMFILE;

        /* ---- EA / xattr ---- */
        case SMB2_STATUS_EAS_NOT_SUPPORTED:
            return CHIMERA_VFS_ENOTSUP;
        case SMB2_STATUS_NO_EAS_ON_FILE:
        case SMB2_STATUS_NONEXISTENT_EA_ENTRY:
            return CHIMERA_VFS_ENODATA;

        /* ---- unsupported ---- */
        case SMB2_STATUS_NOT_SUPPORTED:
        case SMB2_STATUS_NOT_IMPLEMENTED:
            return CHIMERA_VFS_ENOTSUP;

        /* ---- session / connection gone -> stale (handles must be reopened) ---- */
        case SMB2_STATUS_NETWORK_NAME_DELETED:
        case SMB2_STATUS_USER_SESSION_DELETED:
        case SMB2_STATUS_CONNECTION_DISCONNECTED:
        case SMB2_STATUS_CONNECTION_RESET:
        case SMB2_STATUS_VIRTUAL_CIRCUIT_CLOSED:
            return CHIMERA_VFS_ESTALE;

        default:
            return CHIMERA_VFS_EIO;
    } /* switch */
} /* chimera_smb_status_to_errno */

/* ---- attribute mapping ------------------------------------------------- */

/* POSIX timespec -> Windows FILETIME (100ns ticks since 1601-01-01).  A zero
 * timespec maps to 0, which SET_INFO FileBasicInformation reads as "don't
 * change", matching the convention the client uses for unset time fields. */
static inline uint64_t
smb_timespec_to_filetime(const struct timespec *ts)
{
    if (ts->tv_sec == 0 && ts->tv_nsec == 0) {
        return 0;
    }
    return (uint64_t) ts->tv_sec * 10000000ULL +
           (uint64_t) ts->tv_nsec / 100 + 116444736000000000ULL;
} /* smb_timespec_to_filetime */

/* Windows FILETIME (100ns ticks since 1601-01-01) -> POSIX timespec. */
static inline void
smb_filetime_to_timespec(
    uint64_t         filetime,
    struct timespec *ts)
{
    if (filetime == 0 || filetime == (uint64_t) -1) {
        ts->tv_sec  = 0;
        ts->tv_nsec = 0;
        return;
    }
    uint64_t unix100 = filetime - 116444736000000000ULL;
    ts->tv_sec  = (time_t) (unix100 / 10000000ULL);
    ts->tv_nsec = (long) ((unix100 % 10000000ULL) * 100);
} /* smb_filetime_to_timespec */

/* Fill a chimera_vfs_attrs from the SMB FileNetworkOpenInformation fields
* (times + sizes + dos attributes), which CREATE/CLOSE responses embed. */
static inline void
smb_fill_attrs_from_network_open(
    struct chimera_vfs_attrs *attr,
    uint64_t                  crttime,
    uint64_t                  atime,
    uint64_t                  mtime,
    uint64_t                  ctime,
    uint64_t                  alloc_size,
    uint64_t                  end_of_file,
    uint32_t                  file_attributes)
{
    int is_dir = (file_attributes & SMB2_FILE_ATTRIBUTE_DIRECTORY) != 0;

    /* SMB has no POSIX mode; synthesize type + a default permission. */
    attr->va_mode       = (is_dir ? (S_IFDIR | 0755) : (S_IFREG | 0644));
    attr->va_nlink      = 1;
    attr->va_size       = end_of_file;
    attr->va_space_used = alloc_size;

    smb_filetime_to_timespec(atime, &attr->va_atime);
    smb_filetime_to_timespec(mtime, &attr->va_mtime);
    smb_filetime_to_timespec(ctime, &attr->va_ctime);
    smb_filetime_to_timespec(crttime, &attr->va_btime);

    attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_NLINK |
        CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_SPACE_USED |
        CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME |
        CHIMERA_VFS_ATTR_CTIME | CHIMERA_VFS_ATTR_BTIME;
} /* smb_fill_attrs_from_network_open */

/* ---- file-handle helpers ----------------------------------------------- */

static inline int
chimera_smb_fh_server_index(const uint8_t *fh)
{
    return fh[CHIMERA_SMB_FH_SERVER_OFFSET];
} /* chimera_smb_fh_server_index */

/* The mount-root fh is the only re-openable one. */
static inline int
chimera_smb_fh_is_root(int fh_len)
{
    return fh_len == CHIMERA_SMB_ROOT_FH_LEN;
} /* chimera_smb_fh_is_root */

/* Build the opaque open-token fh [mount_id][server_index][FileId] from the
 * (root) fh that carries the mount_id + server_index.  Returns its length. */
static inline int
chimera_smb_encode_open_fh(
    const uint8_t                           *root_fh,
    const struct chimera_smb_client_file_id *file_id,
    uint8_t                                 *out_fh)
{
    uint8_t fragment[1 + sizeof(*file_id)];

    fragment[0] = (uint8_t) chimera_smb_fh_server_index(root_fh);
    memcpy(fragment + 1, file_id, sizeof(*file_id));

    return chimera_vfs_encode_fh_parent(root_fh, fragment, sizeof(fragment), out_fh);
} /* chimera_smb_encode_open_fh */

/* ---- transport (smb.c) ------------------------------------------------- */

void chimera_smb_client_pdu_begin(
    struct chimera_smb_client_conn *conn,
    uint16_t                        command,
    struct evpl_iovec              *iov,
    struct evpl_iovec_cursor       *cursor,
    struct smb2_header            **hdr);

void chimera_smb_client_pdu_finish(
    struct chimera_smb_client_conn *conn,
    struct evpl_iovec              *iov,
    struct evpl_iovec_cursor       *cursor,
    struct chimera_vfs_request     *request,
    chimera_smb_client_reply_cb     reply_cb,
    void                           *reply_arg);

struct evpl_bind * chimera_smb_client_connect(
    struct chimera_smb_client_conn *conn,
    struct evpl_endpoint           *endpoint);

/* ---- SMB3 signing / preauth (smb.c) ------------------------------------ */

/* Extend an SMB 3.1.1 preauth-integrity hash in place: hash = SHA512(hash||msg).
 * `msg` is a raw SMB2 message (header+body), `msg_len` its byte length. */
void chimera_smb_client_preauth_extend(
    uint8_t    *hash,
    const void *msg,
    uint32_t    msg_len);

/* Derive the per-session 16-byte signing key from the NTLM session key via the
 * SMB3 SP800-108 KDF.  `dialect` selects the label/context (and, for 3.1.1, the
 * preauth_hash binding); returns 0 on success.  Mirrors the server's
 * chimera_smb_derive_signing_key. */
int chimera_smb_client_derive_signing_key(
    uint16_t       dialect,
    const uint8_t *session_key,
    size_t         session_key_len,
    const uint8_t *preauth_hash,
    uint8_t       *out_key16);

/* Transition a connection to READY and run any deferred ops (called from the
 * mount handshake or a secondary connection's NEGOTIATE completion). */
void chimera_smb_client_conn_ready(
    struct chimera_smb_client_conn *conn);

/* Fail a connection: error-complete every in-flight and deferred request, then
 * close it (its DISCONNECTED notify frees it). */
void chimera_smb_client_conn_fail(
    struct chimera_smb_client_conn *conn,
    enum chimera_vfs_error          status);

/* Get (lazily create) this thread's connection to `server`. */
struct chimera_smb_client_conn * chimera_smb_client_get_conn(
    struct chimera_smb_client_thread *thread,
    struct chimera_smb_client_server *server);

/* Run `start` once `conn` is READY (connected + negotiated + session live),
 * deferring `request` if establishment is still in flight. */
void chimera_smb_client_ensure_ready(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request,
    chimera_smb_client_start_fn     start);

/* ---- mount / umount (smb_mount.c) -------------------------------------- */

void chimera_smb_client_mount(
    struct chimera_smb_client_thread *thread,
    struct chimera_vfs_request       *request);

void chimera_smb_client_umount(
    struct chimera_smb_client_thread *thread,
    struct chimera_vfs_request       *request);

/* Drive the NEGOTIATE -> (SESSION_SETUP -> TREE_CONNECT for the mount conn) ->
 * READY handshake once TCP connects (smb_mount.c). */
void chimera_smb_client_conn_on_connected(
    struct chimera_smb_client_conn *conn);

/* ---- file operations (smb_ops.c) --------------------------------------- */

void chimera_smb_client_getattr(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_lookup_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_open_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_open_fh(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_close(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_mkdir_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_remove_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_setattr(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_commit(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_read(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_write(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_read(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_write(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_readdir(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_rename_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_symlink_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_mknod_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
void chimera_smb_client_readlink(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);

/* ---- shared op helpers (smb_ops.c) ------------------------------------- */

/* Encode a mount-relative path as UTF-16LE (with '/'->'\\'); returns byte len. */
size_t smb_utf16le_encode(
    const char *s,
    int         len,
    uint8_t    *out);

/* Parse a FileNetworkOpenInformation block from `body` at the cursor. */
void smb_parse_open_info(
    struct evpl_iovec_cursor *body,
    struct smb_open_info     *r);

/* Parse an SMB2 CREATE response body (after the SMB2 header). */
void smb_parse_create_reply(
    struct evpl_iovec_cursor *body,
    struct smb_create_result *r);

/* Fill `attr` from SMB attrs; stamps cred as owner + a stable inode number. */
void smb_apply_attrs(
    const struct chimera_vfs_request *request,
    struct chimera_vfs_attrs         *attr,
    const struct smb_open_info       *info,
    uint64_t                          ino);

/* Send an SMB2 CREATE on `path` (full mount-relative path; "" for the root). */
void smb_send_create(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request,
    const char                     *path,
    int                             path_len,
    uint32_t                        desired_access,
    uint32_t                        share_access,
    uint32_t                        disposition,
    uint32_t                        options,
    chimera_smb_client_reply_cb     reply_cb);

/* Same, but carrying a pre-built create-context blob after the name. */
void smb_send_create_ex(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request,
    const char                     *path,
    int                             path_len,
    uint32_t                        desired_access,
    uint32_t                        share_access,
    uint32_t                        disposition,
    uint32_t                        options,
    const uint8_t                  *ctx,
    uint32_t                        ctx_len,
    chimera_smb_client_reply_cb     reply_cb);

/* An RqLs (lease request v1) create context is 56 bytes: header(16) + "RqLs"(4)
 * + pad(4) + data(32). */
#define CHIMERA_SMB_LEASE_CTX_SIZE 56

uint32_t smb_build_lease_ctx(
    uint8_t       *buf,
    const uint8_t *lease_key,
    uint32_t       lease_state);

/* Send an SMB2 CLOSE for `file_id`. */
void smb_send_close(
    struct chimera_smb_client_conn          *conn,
    struct chimera_vfs_request              *request,
    const struct chimera_smb_client_file_id *file_id,
    chimera_smb_client_reply_cb              reply_cb);

/* Recover the per-open state (FileId + server) from a VFS open handle. */
static inline struct chimera_smb_client_open *
smb_handle_open_state(struct chimera_vfs_open_handle *handle)
{
    return handle ? (struct chimera_smb_client_open *) handle->vfs_private : NULL;
} /* smb_handle_open_state */
