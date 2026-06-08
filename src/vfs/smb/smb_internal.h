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

/* SMB2 default port and dialect for this client. */
#define CHIMERA_SMB_CLIENT_PORT           445
#define CHIMERA_SMB_CLIENT_DIALECT        SMB2_DIALECT_2_1
#define CHIMERA_SMB_CLIENT_DEFAULT_DOMAIN "WORKGROUP"
#define CHIMERA_SMB_CLIENT_MAX_SERVERS    64

/* The fh_fragment layout produced by this module:
*   [ mount_id (16) ][ server_index (1) ][ path bytes (UTF-8, relative to share) ]
* The path is stored inline; the share root has an empty path.  This keeps the
* client stateless about file identity (no path table), at the cost of a path
* length bound (CHIMERA_VFS_FH_SIZE + 16 - 17). */
#define CHIMERA_SMB_FH_PATH_OFFSET        (CHIMERA_VFS_MOUNT_ID_SIZE + 1)
/* Bounded by the 48-byte request->fh that carries a non-root handle. */
#define CHIMERA_SMB_FH_PATH_MAX           (CHIMERA_VFS_FH_SIZE - CHIMERA_SMB_FH_PATH_OFFSET)

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

    enum chimera_smb_client_conn_state state;
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
        case SMB2_STATUS_ACCESS_DENIED:
        case SMB2_STATUS_LOGON_FAILURE:
            return CHIMERA_VFS_EACCES;
        case SMB2_STATUS_OBJECT_NAME_NOT_FOUND:
        case SMB2_STATUS_OBJECT_PATH_NOT_FOUND:
        case SMB2_STATUS_BAD_NETWORK_NAME:
            return CHIMERA_VFS_ENOENT;
        case SMB2_STATUS_OBJECT_NAME_COLLISION:
            return CHIMERA_VFS_EEXIST;
        case SMB2_STATUS_NOT_A_DIRECTORY:
            return CHIMERA_VFS_ENOTDIR;
        case SMB2_STATUS_FILE_IS_A_DIRECTORY:
            return CHIMERA_VFS_EISDIR;
        case SMB2_STATUS_DIRECTORY_NOT_EMPTY:
            return CHIMERA_VFS_ENOTEMPTY;
        case SMB2_STATUS_INVALID_PARAMETER:
            return CHIMERA_VFS_EINVAL;
        case SMB2_STATUS_NOT_SUPPORTED:
            return CHIMERA_VFS_ENOTSUP;
        case SMB2_STATUS_END_OF_FILE:
            return CHIMERA_VFS_OK; /* short read at EOF, handled by caller */
        default:
            return CHIMERA_VFS_EIO;
    } /* switch */
} /* chimera_smb_status_to_errno */

/* ---- attribute mapping ------------------------------------------------- */

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

/* ---- file-handle path helpers ------------------------------------------ */

/* Decode the share-relative UTF-8 path stored inline in a file handle. */
static inline const char *
chimera_smb_fh_path(
    const uint8_t *fh,
    int            fh_len,
    int           *path_len)
{
    *path_len = fh_len - CHIMERA_SMB_FH_PATH_OFFSET;
    if (*path_len < 0) {
        *path_len = 0;
    }
    return (const char *) (fh + CHIMERA_SMB_FH_PATH_OFFSET);
} /* chimera_smb_fh_path */

static inline int
chimera_smb_fh_server_index(const uint8_t *fh)
{
    return fh[CHIMERA_VFS_MOUNT_ID_SIZE];
} /* chimera_smb_fh_server_index */

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
void chimera_smb_client_readdir(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);
