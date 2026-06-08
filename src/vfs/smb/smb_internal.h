// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "vfs/vfs.h"
#include "vfs/vfs_fh.h"
#include "vfs/vfs_error.h"
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

/* Per-server registration (global).  A "server" here is a (host, port, share,
 * credential) tuple identified by the mount; indexed so a file handle's
 * fh_fragment can route back to it in later increments. */
struct chimera_smb_client_server {
    int      index;
    int      in_use;
    char     hostname[256];
    char     share[256];
    char     user[256];
    char     domain[256];
    char     password[256];
    uint16_t port;
};

struct chimera_smb_client_shared {
    pthread_mutex_t                    lock;
    struct chimera_smb_client_server **servers;
    int                                max_servers;
    enum evpl_protocol_id tcp_protocol;
};

struct chimera_smb_client_thread {
    struct evpl                      *evpl;
    struct chimera_smb_client_shared *shared;
};

/* One established mount: owns the connection, session and tree.  Stored as
 * request->mount.r_mount_private and recovered on UMOUNT.  The owning evpl
 * thread is recorded so teardown sends on the correct thread (this increment
 * assumes MOUNT and UMOUNT run on the same thread, as in the smoke test). */
struct chimera_smb_client_mount {
    struct chimera_smb_client_shared *shared;
    struct chimera_smb_client_server *server;
    struct chimera_smb_client_conn   *conn;
    uint8_t                           fsid[CHIMERA_VFS_FSID_SIZE];
};

/* The continuation invoked when a reply to the single outstanding request
* arrives.  MOUNT/UMOUNT are strictly sequential, so one slot suffices. */
typedef void (*chimera_smb_client_reply_cb)(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg);

struct chimera_smb_client_conn {
    struct chimera_smb_client_thread *thread;
    struct evpl                      *evpl;
    struct evpl_bind                 *bind;

    int                               connected;
    int                               failed;

    uint64_t                          next_message_id;
    uint16_t                          dialect;
    uint16_t                          server_security_mode;
    uint64_t                          session_id;
    uint32_t                          tree_id;

    struct smb_ntlm_client            ntlm;

    /* Single-outstanding-request demux. */
    chimera_smb_client_reply_cb       reply_cb;
    void                             *reply_arg;

    /* The VFS request driving the in-flight MOUNT or UMOUNT state machine, and
     * the mount it operates on. */
    struct chimera_vfs_request       *active_request;
    struct chimera_smb_client_mount  *mount;
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
        case SMB2_STATUS_BAD_NETWORK_NAME:
        case SMB2_STATUS_OBJECT_NAME_NOT_FOUND:
            return CHIMERA_VFS_ENOENT;
        case SMB2_STATUS_INVALID_PARAMETER:
            return CHIMERA_VFS_EINVAL;
        case SMB2_STATUS_NOT_SUPPORTED:
            return CHIMERA_VFS_ENOTSUP;
        default:
            return CHIMERA_VFS_EIO;
    } /* switch */
} /* chimera_smb_status_to_errno */

/* ---- transport (smb.c) ------------------------------------------------- */

/* Begin building an SMB2 request PDU: allocates an iovec, reserves the NetBIOS
 * + SMB2 header, fills the header for `command`, and leaves the cursor
 * positioned at the body (consumed == sizeof(smb2_header)).  Returns the iovec
 * (by out-param) and the header pointer for any post-hoc field tweaks. */
void chimera_smb_client_pdu_begin(
    struct chimera_smb_client_conn *conn,
    uint16_t                        command,
    struct evpl_iovec              *iov,
    struct evpl_iovec_cursor       *cursor,
    struct smb2_header            **hdr);

/* Finish and send a PDU begun with pdu_begin, registering the reply
 * continuation. */
void chimera_smb_client_pdu_finish(
    struct chimera_smb_client_conn *conn,
    struct evpl_iovec              *iov,
    struct evpl_iovec_cursor       *cursor,
    chimera_smb_client_reply_cb     reply_cb,
    void                           *reply_arg);

/* Open an outbound SMB2 connection bound to conn (notify/segment callbacks). */
struct evpl_bind * chimera_smb_client_connect(
    struct chimera_smb_client_conn *conn,
    struct evpl_endpoint           *endpoint);

/* Invoked from the transport layer once the TCP connection is established:
 * kicks off the NEGOTIATE -> SESSION_SETUP -> TREE_CONNECT handshake. */
void chimera_smb_client_mount_on_connected(
    struct chimera_smb_client_conn *conn);

/* ---- mount / umount (smb_mount.c / smb_umount.c) ----------------------- */

void chimera_smb_client_mount(
    struct chimera_smb_client_thread *thread,
    struct chimera_vfs_request       *request);

void chimera_smb_client_umount(
    struct chimera_smb_client_thread *thread,
    struct chimera_vfs_request       *request);
