// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <xxhash.h>

#include "smb_internal.h"
#include "smb.h"
#include "common/tcp_flavor.h"
#include "vfs/vfs_attrs.h"
#include "evpl/evpl.h"

/* ---- helpers ----------------------------------------------------------- */

static const char *
chimera_smb_client_get_option(
    const struct chimera_vfs_mount_options *options,
    const char                             *key)
{
    int i;

    for (i = 0; i < options->num_options; i++) {
        if (strcmp(options->options[i].key, key) == 0) {
            return options->options[i].value;
        }
    }
    return NULL;
} /* chimera_smb_client_get_option */

static struct chimera_smb_client_server *
chimera_smb_client_server_alloc(struct chimera_smb_client_shared *shared)
{
    struct chimera_smb_client_server *server = NULL;
    int                               i;

    pthread_mutex_lock(&shared->lock);

    for (i = 0; i < shared->max_servers; i++) {
        if (!shared->servers[i]) {
            server             = calloc(1, sizeof(*server));
            server->index      = i;
            server->in_use     = 1;
            shared->servers[i] = server;
            break;
        }
    }

    pthread_mutex_unlock(&shared->lock);

    return server;
} /* chimera_smb_client_server_alloc */

/* Fail the in-flight MOUNT: complete its request with `status`, free the
 * half-built mount, and close the connection (whose DISCONNECTED notify frees
 * the conn). */
static void
chimera_smb_client_mount_fail(
    struct chimera_smb_client_conn *conn,
    enum chimera_vfs_error          status)
{
    struct chimera_vfs_request *request = conn->active_request;

    conn->active_request = NULL;

    if (request) {
        request->status = status;
        request->complete(request);
    }

    if (conn->mount) {
        free(conn->mount);
        conn->mount = NULL;
    }

    evpl_close(conn->evpl, conn->bind);
} /* chimera_smb_client_mount_fail */

/* ---- TREE_CONNECT (final leg) ------------------------------------------ */

static void
chimera_smb_client_tree_connect_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request      *request = conn->active_request;
    struct chimera_smb_client_mount *mount   = conn->mount;
    uint8_t                          fragment[1];

    (void) body;
    (void) body_len;
    (void) arg;

    if (status != SMB2_STATUS_SUCCESS) {
        chimera_smbclient_error("TREE_CONNECT failed: status 0x%08x", status);
        chimera_smb_client_mount_fail(conn, chimera_smb_status_to_errno(status));
        return;
    }

    /* The granted TreeId arrives in the response header. */
    conn->tree_id = hdr->sync.tree_id;

    /* Build the mount root file handle: fsid = hash(host || share),
     * fh_fragment = [server_index].  Later increments append the remote
     * file id to address files within the share. */
    fragment[0] = (uint8_t) mount->server->index;

    request->mount.r_attr.va_set_mask = CHIMERA_VFS_ATTR_FH;
    request->mount.r_attr.va_fh_len   = chimera_vfs_encode_fh_mount(
        mount->fsid, fragment, 1, request->mount.r_attr.va_fh);

    request->mount.r_mount_private = mount;

    chimera_smbclient_info("SMB mount established: //%s/%s (dialect 0x%04x, session 0x%lx, tree %u)",
                           mount->server->hostname, mount->server->share,
                           conn->dialect, conn->session_id, conn->tree_id);

    conn->active_request = NULL;
    request->status      = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_client_tree_connect_reply */

static void
chimera_smb_client_tree_connect_send(struct chimera_smb_client_conn *conn)
{
    struct chimera_smb_client_mount *mount = conn->mount;
    struct evpl_iovec                iov;
    struct evpl_iovec_cursor         cursor;
    struct smb2_header              *hdr;
    uint8_t                          unc16[1200];
    char                             unc[600];
    size_t                           unc_len, i, n16;

    snprintf(unc, sizeof(unc), "\\\\%s\\%s", mount->server->hostname, mount->server->share);
    unc_len = strlen(unc);

    /* UTF-16LE encode the UNC path. */
    n16 = 0;
    for (i = 0; i < unc_len; i++) {
        unc16[n16++] = (uint8_t) unc[i];
        unc16[n16++] = 0;
    }

    chimera_smb_client_pdu_begin(conn, SMB2_TREE_CONNECT, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_TREE_CONNECT_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint16(&cursor, 0);  /* Flags/Reserved */
    evpl_iovec_cursor_append_uint16(&cursor, sizeof(struct smb2_header) + 8); /* PathOffset */
    evpl_iovec_cursor_append_uint16(&cursor, (uint16_t) n16);                 /* PathLength */
    evpl_iovec_cursor_append_blob(&cursor, unc16, n16);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor,
                                  chimera_smb_client_tree_connect_reply, NULL);
} /* chimera_smb_client_tree_connect_send */

/* ---- SESSION_SETUP (leg 2: AUTHENTICATE) ------------------------------- */

static void
chimera_smb_client_session_setup_done(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    (void) hdr;
    (void) body;
    (void) body_len;
    (void) arg;

    if (status != SMB2_STATUS_SUCCESS) {
        chimera_smbclient_error("SESSION_SETUP (authenticate) failed: status 0x%08x", status);
        chimera_smb_client_mount_fail(conn, chimera_smb_status_to_errno(status));
        return;
    }

    /* Session established and authenticated; proceed to the share. */
    chimera_smb_client_tree_connect_send(conn);
} /* chimera_smb_client_session_setup_done */

/* ---- SESSION_SETUP (leg 1: NEGOTIATE -> CHALLENGE) --------------------- */

/* Append an SMB2 SESSION_SETUP request whose security buffer is `token`. */
static void
chimera_smb_client_session_setup_send(
    struct chimera_smb_client_conn *conn,
    const uint8_t                  *token,
    size_t                          token_len,
    chimera_smb_client_reply_cb     reply_cb)
{
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;

    chimera_smb_client_pdu_begin(conn, SMB2_SESSION_SETUP, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_SESSION_SETUP_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, 0);                     /* Flags */
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_SIGNING_ENABLED); /* SecurityMode */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                    /* Capabilities */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                    /* Channel */
    evpl_iovec_cursor_append_uint16(&cursor, sizeof(struct smb2_header) + 24); /* BlobOffset */
    evpl_iovec_cursor_append_uint16(&cursor, (uint16_t) token_len); /* BlobLength */
    evpl_iovec_cursor_append_uint64(&cursor, 0);                    /* PreviousSessionId */
    evpl_iovec_cursor_append_blob(&cursor, (void *) token, token_len);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, reply_cb, NULL);
} /* chimera_smb_client_session_setup_send */

static void
chimera_smb_client_session_setup_challenge(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    uint16_t structsize, session_flags, blob_offset, blob_length;
    uint8_t  challenge[2048];
    uint8_t  authenticate[2048];
    size_t   auth_len;
    int      consumed;

    (void) arg;

    /* The interim leg returns MORE_PROCESSING_REQUIRED (not a hard error). */
    if (status != SMB2_STATUS_MORE_PROCESSING_REQUIRED &&
        status != SMB2_STATUS_SUCCESS) {
        chimera_smbclient_error("SESSION_SETUP (negotiate) failed: status 0x%08x", status);
        chimera_smb_client_mount_fail(conn, chimera_smb_status_to_errno(status));
        return;
    }

    /* The interim response header carries the allocated SessionId, which every
     * subsequent request on this session must echo. */
    conn->session_id = hdr->session_id;

    /* Parse the SESSION_SETUP response body to locate the security buffer. */
    evpl_iovec_cursor_get_uint16(body, &structsize);
    evpl_iovec_cursor_get_uint16(body, &session_flags);
    evpl_iovec_cursor_get_uint16(body, &blob_offset);
    evpl_iovec_cursor_get_uint16(body, &blob_length);

    (void) structsize;
    (void) session_flags;

    if (blob_length == 0 || blob_length > sizeof(challenge)) {
        chimera_smbclient_error("SESSION_SETUP response has invalid security buffer (%u bytes)",
                                blob_length);
        chimera_smb_client_mount_fail(conn, CHIMERA_VFS_EINVAL);
        return;
    }

    /* Skip from the current cursor position to the security buffer (offsets are
     * relative to the SMB2 header start, which is the cursor's consumed origin). */
    consumed = evpl_iovec_cursor_consumed(body);
    if (blob_offset < consumed ||
        blob_offset - consumed + blob_length > body_len + (int) sizeof(struct smb2_header)) {
        chimera_smbclient_error("SESSION_SETUP security buffer out of range");
        chimera_smb_client_mount_fail(conn, CHIMERA_VFS_EINVAL);
        return;
    }
    evpl_iovec_cursor_skip(body, blob_offset - consumed);
    evpl_iovec_cursor_copy(body, challenge, blob_length);

    if (smb_ntlm_client_parse_challenge(&conn->ntlm, challenge, blob_length) < 0) {
        chimera_smbclient_error("Failed to parse NTLM CHALLENGE");
        chimera_smb_client_mount_fail(conn, CHIMERA_VFS_EACCES);
        return;
    }

    if (smb_ntlm_client_build_authenticate(&conn->ntlm, authenticate,
                                           sizeof(authenticate), &auth_len) < 0) {
        chimera_smbclient_error("Failed to build NTLM AUTHENTICATE");
        chimera_smb_client_mount_fail(conn, CHIMERA_VFS_EACCES);
        return;
    }

    chimera_smb_client_session_setup_send(conn, authenticate, auth_len,
                                          chimera_smb_client_session_setup_done);
} /* chimera_smb_client_session_setup_challenge */

/* ---- NEGOTIATE --------------------------------------------------------- */

static void
chimera_smb_client_negotiate_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    uint16_t structsize, security_mode, dialect;
    uint8_t  negotiate[64];
    int      neg_len;

    (void) hdr;
    (void) body_len;
    (void) arg;

    if (status != SMB2_STATUS_SUCCESS) {
        chimera_smbclient_error("NEGOTIATE failed: status 0x%08x", status);
        chimera_smb_client_mount_fail(conn, chimera_smb_status_to_errno(status));
        return;
    }

    evpl_iovec_cursor_get_uint16(body, &structsize);
    evpl_iovec_cursor_get_uint16(body, &security_mode);
    evpl_iovec_cursor_get_uint16(body, &dialect);

    (void) structsize;

    conn->server_security_mode = security_mode;
    conn->dialect              = dialect;

    if (dialect != CHIMERA_SMB_CLIENT_DIALECT) {
        chimera_smbclient_error("Server selected unexpected dialect 0x%04x (wanted 0x%04x)",
                                dialect, CHIMERA_SMB_CLIENT_DIALECT);
        chimera_smb_client_mount_fail(conn, CHIMERA_VFS_ENOTSUP);
        return;
    }

    /* SMB 2.x server requiring signing is not yet supported (this increment
     * does not sign requests). */
    if (security_mode & SMB2_SIGNING_REQUIRED) {
        chimera_smbclient_error("Server requires SMB signing, which is not yet supported");
        chimera_smb_client_mount_fail(conn, CHIMERA_VFS_ENOTSUP);
        return;
    }

    neg_len = smb_ntlm_client_build_negotiate(negotiate, sizeof(negotiate));
    if (neg_len < 0) {
        chimera_smb_client_mount_fail(conn, CHIMERA_VFS_EFAULT);
        return;
    }

    chimera_smb_client_session_setup_send(conn, negotiate, neg_len,
                                          chimera_smb_client_session_setup_challenge);
} /* chimera_smb_client_negotiate_reply */

void
chimera_smb_client_mount_on_connected(struct chimera_smb_client_conn *conn)
{
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;
    uint8_t                  client_guid[SMB2_GUID_SIZE];

    memset(client_guid, 0, sizeof(client_guid));

    chimera_smb_client_pdu_begin(conn, SMB2_NEGOTIATE, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_NEGOTIATE_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint16(&cursor, 1);                   /* DialectCount */
    evpl_iovec_cursor_append_uint16(&cursor, SMB2_SIGNING_ENABLED);/* SecurityMode */
    evpl_iovec_cursor_append_uint16(&cursor, 0);                   /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                   /* Capabilities */
    evpl_iovec_cursor_append_blob(&cursor, client_guid, SMB2_GUID_SIZE);
    evpl_iovec_cursor_append_uint32(&cursor, 0);                   /* NegotiateContextOffset */
    evpl_iovec_cursor_append_uint16(&cursor, 0);                   /* NegotiateContextCount */
    evpl_iovec_cursor_append_uint16(&cursor, 0);                   /* Reserved2 */
    evpl_iovec_cursor_append_uint16(&cursor, CHIMERA_SMB_CLIENT_DIALECT); /* Dialects[0] */

    chimera_smb_client_pdu_finish(conn, &iov, &cursor,
                                  chimera_smb_client_negotiate_reply, NULL);
} /* chimera_smb_client_mount_on_connected */

/* ---- MOUNT entry point ------------------------------------------------- */

void
chimera_smb_client_mount(
    struct chimera_smb_client_thread *thread,
    struct chimera_vfs_request       *request)
{
    struct chimera_smb_client_shared *shared = thread->shared;
    struct chimera_smb_client_server *server;
    struct chimera_smb_client_mount  *mount;
    struct chimera_smb_client_conn   *conn;
    struct evpl_endpoint             *endpoint;
    const char                       *user, *password, *domain, *port_opt;
    char                              host[256];
    char                              share[256];
    const char                       *colon;
    int                               host_len;
    uint16_t                          port;
    XXH128_hash_t                     fsid_hash;
    char                              fsid_input[600];
    int                               fsid_len;

    /* Resolve the outbound TCP flavor from the common setting (constant per
     * process), mirroring the NFS client. */
    shared->tcp_protocol = chimera_tcp_flavor_to_protocol(request->thread->vfs->tcp_flavor);

    /* Mount path is "host:share". */
    colon = memchr(request->mount.path, ':', request->mount.pathlen);
    if (!colon) {
        chimera_smbclient_error("SMB mount path '%s' is not host:share", request->mount.path);
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    host_len = (int) (colon - request->mount.path);
    if (host_len <= 0 || host_len >= (int) sizeof(host)) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }
    memcpy(host, request->mount.path, host_len);
    host[host_len] = '\0';
    snprintf(share, sizeof(share), "%s", colon + 1);

    user     = chimera_smb_client_get_option(&request->mount.options, "user");
    password = chimera_smb_client_get_option(&request->mount.options, "password");
    domain   = chimera_smb_client_get_option(&request->mount.options, "domain");
    port_opt = chimera_smb_client_get_option(&request->mount.options, "port");

    if (!user || !password) {
        chimera_smbclient_error("SMB mount requires user= and password= options");
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    port = port_opt ? (uint16_t) atoi(port_opt) : CHIMERA_SMB_CLIENT_PORT;

    server = chimera_smb_client_server_alloc(shared);
    if (!server) {
        chimera_smbclient_error("SMB client server table full");
        request->status = CHIMERA_VFS_ENOSPC;
        request->complete(request);
        return;
    }

    snprintf(server->hostname, sizeof(server->hostname), "%s", host);
    snprintf(server->share, sizeof(server->share), "%s", share);
    snprintf(server->user, sizeof(server->user), "%s", user);
    snprintf(server->domain, sizeof(server->domain), "%s",
             domain ? domain : CHIMERA_SMB_CLIENT_DEFAULT_DOMAIN);
    snprintf(server->password, sizeof(server->password), "%s", password);
    server->port = port;

    mount         = calloc(1, sizeof(*mount));
    mount->shared = shared;
    mount->server = server;

    /* FSID = hash(host || share), unique per share mount. */
    fsid_len  = snprintf(fsid_input, sizeof(fsid_input), "%s/%s", host, share);
    fsid_hash = XXH3_128bits(fsid_input, fsid_len);
    memcpy(mount->fsid, &fsid_hash, CHIMERA_VFS_FSID_SIZE);

    conn                 = calloc(1, sizeof(*conn));
    conn->thread         = thread;
    conn->evpl           = thread->evpl;
    conn->mount          = mount;
    conn->active_request = request;
    mount->conn          = conn;

    smb_ntlm_client_init(&conn->ntlm, server->user, server->domain, server->password);

    endpoint   = evpl_endpoint_create(server->hostname, server->port);
    conn->bind = chimera_smb_client_connect(conn, endpoint);

    /* The handshake proceeds from chimera_smb_client_mount_on_connected once
     * the EVPL_NOTIFY_CONNECTED event fires. */
} /* chimera_smb_client_mount */
