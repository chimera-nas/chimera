// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <utlist.h>
#include <pthread.h>
#include <time.h>

#include "nfs_internal.h"
#include "evpl/evpl_rpc2.h"
#include "vfs/vfs_internal.h"
#include "vfs/vfs_fh.h"

#define CHIMERA_NFS4_DEFAULT_PORT 2049
#define CHIMERA_NFS4_RDMA_PORT    20049

struct chimera_nfs4_mount_ctx {
    struct chimera_nfs_client_server_thread *server_thread;
    struct chimera_nfs_client_mount         *mount;
};

/* Forward declarations */
static void chimera_nfs4_mount_exchange_id(
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_vfs_request              *request);

static void chimera_nfs4_mount_create_session(
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_vfs_request              *request);

static void chimera_nfs4_mount_get_root_fh(
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_vfs_request              *request);

/* Get the RDMA protocol from mount options */
static enum evpl_protocol_id
chimera_nfs4_mount_get_rdma_protocol(const struct chimera_vfs_mount_options *options)
{
    int i;

    for (i = 0; i < options->num_options; i++) {
        if (strcmp(options->options[i].key, "rdma") == 0) {
            if (options->options[i].value &&
                strcmp(options->options[i].value, "tcp") == 0) {
                return EVPL_DATAGRAM_TCP_RDMA;
            }
            return EVPL_DATAGRAM_RDMACM_RC;
        }
    }

    return 0;
} /* chimera_nfs4_mount_get_rdma_protocol */

/* Get the port from mount options */
static int
chimera_nfs4_mount_get_port(
    const struct chimera_vfs_mount_options *options,
    int                                     default_port)
{
    int i;

    for (i = 0; i < options->num_options; i++) {
        if (strcmp(options->options[i].key, "port") == 0) {
            if (options->options[i].value) {
                return atoi(options->options[i].value);
            }
        }
    }

    return default_port;
} /* chimera_nfs4_mount_get_port */

/*
 * Callback for SEQUENCE + PUTROOTFH + GETFH + GETATTR compound
 * This is the final step of mount - we have the root file handle
 */
static void
chimera_nfs4_mount_get_root_fh_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request      *request = private_data;
    struct chimera_nfs4_mount_ctx   *ctx     = request->plugin_data;
    struct chimera_nfs_client_mount *mount   = ctx->mount;
    struct chimera_nfs_shared       *shared  = mount->server->shared;
    struct nfs_resop4               *getfh_res;
    xdr_opaque                      *remote_fh;
    uint8_t                          fh_fragment[CHIMERA_VFS_FH_SIZE];
    uint8_t                          fsid_buf[CHIMERA_VFS_FSID_SIZE];
    uint8_t                          hash_input[256 + NFS4_FHSIZE];
    int                              hash_input_len;
    int                              fh_fragment_len;
    int                              hostname_len;
    XXH128_hash_t                    fsid_hash;

    if (status != 0) {
        chimera_nfsclient_error("NFS4 mount get_root_fh RPC failed: %d", status);
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    if (res->status != NFS4_OK) {
        chimera_nfsclient_error("NFS4 mount get_root_fh compound failed: %d", res->status);
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check individual operation results */
    /* res->resarray[0] = SEQUENCE */
    /* res->resarray[1] = PUTROOTFH */
    /* res->resarray[2] = LOOKUP */
    /* res->resarray[3] = GETFH */
    /* res->resarray[4] = GETATTR */

    if (res->num_resarray < 5) {
        chimera_nfsclient_error("NFS4 mount get_root_fh: incomplete response");
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    if (res->resarray[0].opsequence.sr_status != NFS4_OK) {
        chimera_nfsclient_error("NFS4 SEQUENCE failed: %d", res->resarray[0].opsequence.sr_status);
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    if (res->resarray[1].opputrootfh.status != NFS4_OK) {
        chimera_nfsclient_error("NFS4 PUTROOTFH failed: %d", res->resarray[1].opputrootfh.status);
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    if (res->resarray[2].oplookup.status != NFS4_OK) {
        chimera_nfsclient_error("NFS4 LOOKUP failed: %d", res->resarray[2].oplookup.status);
        request->status = chimera_nfs4_status_to_errno(res->resarray[2].oplookup.status);
        request->complete(request);
        return;
    }

    getfh_res = &res->resarray[3];
    if (getfh_res->opgetfh.status != NFS4_OK) {
        chimera_nfsclient_error("NFS4 GETFH failed: %d", getfh_res->opgetfh.status);
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    remote_fh = &getfh_res->opgetfh.resok4.object;

    /*
     * Build the local file handle:
     * - fh_fragment = [server_index (1 byte)][remote_root_fh]
     * - FSID = XXH3_128bits(server_hostname || remote_root_fh)
     */
    fh_fragment[0] = mount->server->index;
    memcpy(fh_fragment + 1, remote_fh->data, remote_fh->len);
    fh_fragment_len = 1 + remote_fh->len;

    /* Compute FSID by hashing server hostname + remote root FH */
    hostname_len = strlen(mount->server->hostname);
    memcpy(hash_input, mount->server->hostname, hostname_len);
    memcpy(hash_input + hostname_len, remote_fh->data, remote_fh->len);
    hash_input_len = hostname_len + remote_fh->len;

    fsid_hash = XXH3_128bits(hash_input, hash_input_len);
    memcpy(fsid_buf, &fsid_hash, CHIMERA_VFS_FSID_SIZE);

    request->mount.r_attr.va_set_mask = CHIMERA_VFS_ATTR_FH;
    request->mount.r_attr.va_fh_len   = chimera_vfs_encode_fh_mount(
        fsid_buf, fh_fragment, fh_fragment_len, request->mount.r_attr.va_fh);

    request->mount.r_mount_private = mount;

    mount->status = CHIMERA_NFS_CLIENT_MOUNT_STATE_MOUNTED;
    pthread_mutex_unlock(&shared->lock);

    chimera_nfsclient_info("NFS4 mount complete: %s", mount->path);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_mount_get_root_fh_callback */

/*
 * Send SEQUENCE + PUTROOTFH + GETFH + GETATTR compound to get root FH
 */
static void
chimera_nfs4_mount_get_root_fh(
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_vfs_request              *request)
{
    struct chimera_nfs_shared          *shared  = server_thread->shared;
    struct chimera_nfs_client_server   *server  = server_thread->server;
    struct chimera_nfs4_client_session *session = server->nfs4_session;
    struct chimera_nfs4_mount_ctx      *ctx     = request->plugin_data;
    struct chimera_nfs_client_mount    *mount   = ctx->mount;
    struct COMPOUND4args                args;
    struct nfs_argop4                   argarray[5];
    uint32_t                            attr_request[2];
    const char                         *path;
    int                                 pathlen;

    /* Skip leading '/' in mount path to get the share name */
    path = mount->path;
    if (path[0] == '/') {
        path++;
    }
    pathlen = strlen(path);

    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 5;

    /* Op 0: SEQUENCE */
    argarray[0].argop = OP_SEQUENCE;
    memcpy(argarray[0].opsequence.sa_sessionid, session->sessionid, NFS4_SESSIONID_SIZE);
    argarray[0].opsequence.sa_sequenceid     = chimera_nfs4_get_sequenceid(session, 0);
    argarray[0].opsequence.sa_slotid         = 0;  /* Use slot 0 for mount operation */
    argarray[0].opsequence.sa_highest_slotid = session->max_slots - 1;
    argarray[0].opsequence.sa_cachethis      = 0;

    /* Op 1: PUTROOTFH */
    argarray[1].argop = OP_PUTROOTFH;

    /* Op 2: LOOKUP - lookup the export/share name */
    argarray[2].argop                 = OP_LOOKUP;
    argarray[2].oplookup.objname.data = (uint8_t *) path;
    argarray[2].oplookup.objname.len  = pathlen;

    /* Op 3: GETFH */
    argarray[3].argop = OP_GETFH;

    /* Op 4: GETATTR - request basic attributes */
    argarray[4].argop                      = OP_GETATTR;
    attr_request[0]                        = (1 << FATTR4_TYPE) | (1 << FATTR4_SIZE) | (1 << FATTR4_FILEID);
    attr_request[1]                        = (1 << (FATTR4_MODE - 32)) | (1 << (FATTR4_NUMLINKS - 32));
    argarray[4].opgetattr.attr_request     = attr_request;
    argarray[4].opgetattr.num_attr_request = 2;

    shared->nfs_v4.send_call_NFSPROC4_COMPOUND(
        &shared->nfs_v4.rpc2,
        server_thread->thread->evpl,
        server_thread->nfs_conn,
        NULL,
        &args,
        0, 0, 0,
        chimera_nfs4_mount_get_root_fh_callback,
        request);
} /* chimera_nfs4_mount_get_root_fh */

/*
 * Callback for CREATE_SESSION compound
 */
static void
chimera_nfs4_mount_create_session_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request              *request       = private_data;
    struct chimera_nfs4_mount_ctx           *ctx           = request->plugin_data;
    struct chimera_nfs_client_server_thread *server_thread = ctx->server_thread;
    struct chimera_nfs_client_server        *server        = server_thread->server;
    struct chimera_nfs4_client_session      *session;
    struct CREATE_SESSION4resok             *cs_res;
    uint32_t                                 i;

    if (status != 0) {
        chimera_nfsclient_error("NFS4 CREATE_SESSION RPC failed: %d", status);
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    if (res->status != NFS4_OK) {
        chimera_nfsclient_error("NFS4 CREATE_SESSION compound failed: %d", res->status);
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    if (res->num_resarray < 1 ||
        res->resarray[0].opcreate_session.csr_status != NFS4_OK) {
        chimera_nfsclient_error("NFS4 CREATE_SESSION op failed: %d",
                                res->resarray[0].opcreate_session.csr_status);
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    cs_res = &res->resarray[0].opcreate_session.csr_resok4;

    /* Store session information */
    session = server->nfs4_session;
    memcpy(session->sessionid, cs_res->csr_sessionid, NFS4_SESSIONID_SIZE);

    /* Use the server's fore channel max requests as the number of slots */
    session->max_slots    = cs_res->csr_fore_chan_attrs.ca_maxrequests;
    session->next_slot_id = 1;  /* Start at 1, slot 0 is used by mount thread */

    /* Allocate per-slot sequence IDs */
    session->slot_seqids = calloc(session->max_slots, sizeof(uint32_t));
    for (i = 0; i < session->max_slots; i++) {
        session->slot_seqids[i] = 1; /* Start at 1 per RFC 5661 */
    }

    chimera_nfsclient_info("NFS4 CREATE_SESSION successful, clientid=%lu, max_slots=%u",
                           (unsigned long) session->clientid, session->max_slots);

    /* Now get the root file handle */
    chimera_nfs4_mount_get_root_fh(server_thread, request);
} /* chimera_nfs4_mount_create_session_callback */

/*
 * Send CREATE_SESSION compound
 */
static void
chimera_nfs4_mount_create_session(
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_vfs_request              *request)
{
    struct chimera_nfs_shared          *shared  = server_thread->shared;
    struct chimera_nfs_client_server   *server  = server_thread->server;
    struct chimera_nfs4_client_session *session = server->nfs4_session;
    struct COMPOUND4args                args;
    struct nfs_argop4                   argarray[1];
    struct CREATE_SESSION4args         *cs_args;

    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 1;

    /* CREATE_SESSION */
    argarray[0].argop = OP_CREATE_SESSION;
    cs_args           = &argarray[0].opcreate_session;

    cs_args->csa_clientid = session->clientid;
    cs_args->csa_sequence = 1;
    cs_args->csa_flags    = 0;

    /* Fore channel attributes */
    cs_args->csa_fore_chan_attrs.ca_headerpadsize          = 0;
    cs_args->csa_fore_chan_attrs.ca_maxrequestsize         = 1024 * 1024;
    cs_args->csa_fore_chan_attrs.ca_maxresponsesize        = 1024 * 1024;
    cs_args->csa_fore_chan_attrs.ca_maxresponsesize_cached = 0;
    cs_args->csa_fore_chan_attrs.ca_maxoperations          = 64;
    cs_args->csa_fore_chan_attrs.ca_maxrequests            = 64;
    cs_args->csa_fore_chan_attrs.ca_rdma_ird               = NULL;
    cs_args->csa_fore_chan_attrs.num_ca_rdma_ird           = 0;

    /* Back channel attributes (minimal, we don't use callbacks) */
    cs_args->csa_back_chan_attrs.ca_headerpadsize          = 0;
    cs_args->csa_back_chan_attrs.ca_maxrequestsize         = 4096;
    cs_args->csa_back_chan_attrs.ca_maxresponsesize        = 4096;
    cs_args->csa_back_chan_attrs.ca_maxresponsesize_cached = 0;
    cs_args->csa_back_chan_attrs.ca_maxoperations          = 2;
    cs_args->csa_back_chan_attrs.ca_maxrequests            = 1;
    cs_args->csa_back_chan_attrs.ca_rdma_ird               = NULL;
    cs_args->csa_back_chan_attrs.num_ca_rdma_ird           = 0;

    cs_args->csa_cb_program    = 0;
    cs_args->csa_sec_parms     = NULL;
    cs_args->num_csa_sec_parms = 0;

    shared->nfs_v4.send_call_NFSPROC4_COMPOUND(
        &shared->nfs_v4.rpc2,
        server_thread->thread->evpl,
        server_thread->nfs_conn,
        NULL,
        &args,
        0, 0, 0,
        chimera_nfs4_mount_create_session_callback,
        request);
} /* chimera_nfs4_mount_create_session */

/*
 * Callback for EXCHANGE_ID compound
 */
static void
chimera_nfs4_mount_exchange_id_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request              *request       = private_data;
    struct chimera_nfs4_mount_ctx           *ctx           = request->plugin_data;
    struct chimera_nfs_client_server_thread *server_thread = ctx->server_thread;
    struct chimera_nfs_client_server        *server        = server_thread->server;
    struct chimera_nfs4_client_session      *session;
    struct EXCHANGE_ID4resok                *eid_res;

    if (status != 0) {
        chimera_nfsclient_error("NFS4 EXCHANGE_ID RPC failed: %d", status);
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    if (res->status != NFS4_OK) {
        chimera_nfsclient_error("NFS4 EXCHANGE_ID compound failed: %d", res->status);
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    if (res->num_resarray < 1 ||
        res->resarray[0].opexchange_id.eir_status != NFS4_OK) {
        chimera_nfsclient_error("NFS4 EXCHANGE_ID op failed: %d",
                                res->resarray[0].opexchange_id.eir_status);
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    eid_res = &res->resarray[0].opexchange_id.eir_resok4;

    /* Allocate and initialize session */
    session              = calloc(1, sizeof(*session));
    session->clientid    = eid_res->eir_clientid;
    server->nfs4_session = session;

    chimera_nfsclient_info("NFS4 EXCHANGE_ID successful, clientid=%lu",
                           (unsigned long) session->clientid);

    /* Next step: CREATE_SESSION */
    chimera_nfs4_mount_create_session(server_thread, request);
} /* chimera_nfs4_mount_exchange_id_callback */

/*
 * Send EXCHANGE_ID compound to register with the server
 */
static void
chimera_nfs4_mount_exchange_id(
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_vfs_request              *request)
{
    struct chimera_nfs_shared        *shared = server_thread->shared;
    struct chimera_nfs_client_server *server = server_thread->server;
    struct COMPOUND4args              args;
    struct nfs_argop4                 argarray[1];
    struct EXCHANGE_ID4args          *eid_args;
    struct timespec                   now;

    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 1;

    /* EXCHANGE_ID */
    argarray[0].argop = OP_EXCHANGE_ID;
    eid_args          = &argarray[0].opexchange_id;

    /* Generate client owner ID based on hostname and time */
    clock_gettime(CLOCK_REALTIME, &now);
    memcpy(server->nfs4_verifier, &now.tv_sec, sizeof(now.tv_sec));

    /* Client owner: use verifier and a unique owner ID string */
    memcpy(eid_args->eia_clientowner.co_verifier, server->nfs4_verifier, NFS4_VERIFIER_SIZE);

    /* Owner ID: hostname + pid for uniqueness - stored in server struct for lifetime */
    server->nfs4_owner_id_len = snprintf(server->nfs4_owner_id, sizeof(server->nfs4_owner_id),
                                         "chimera-%s-%d", server->hostname, getpid());
    eid_args->eia_clientowner.co_ownerid.data = (uint8_t *) server->nfs4_owner_id;
    eid_args->eia_clientowner.co_ownerid.len  = server->nfs4_owner_id_len;

    eid_args->eia_flags                 = EXCHGID4_FLAG_USE_NON_PNFS;
    eid_args->eia_state_protect.spa_how = SP4_NONE;
    eid_args->eia_client_impl_id        = NULL;
    eid_args->num_eia_client_impl_id    = 0;

    shared->nfs_v4.send_call_NFSPROC4_COMPOUND(
        &shared->nfs_v4.rpc2,
        server_thread->thread->evpl,
        server_thread->nfs_conn,
        NULL,
        &args,
        0, 0, 0,
        chimera_nfs4_mount_exchange_id_callback,
        request);
} /* chimera_nfs4_mount_exchange_id */

/*
 * Process mount after server connection is established
 */
static void
chimera_nfs4_mount_process_mount(
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_vfs_request              *request)
{
    struct chimera_nfs_client_server *server = server_thread->server;
    struct chimera_nfs_shared        *shared = server_thread->shared;
    struct chimera_nfs_client_mount  *mount;
    struct chimera_nfs4_mount_ctx    *ctx;
    int                               i;
    const char                       *path = NULL;

    /* Parse path from "hostname:path" format */
    for (i = 0; i < request->mount.pathlen; i++) {
        if (request->mount.path[i] == ':') {
            path = request->mount.path + i + 1;
            break;
        }
    }

    if (!path) {
        chimera_nfsclient_error("NFS4 mount failed: invalid path %s", request->mount.path);
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    mount = calloc(1, sizeof(*mount));

    mount->server        = server;
    mount->nfsvers       = 4;
    mount->status        = CHIMERA_NFS_CLIENT_MOUNT_STATE_MOUNTING;
    mount->mount_request = request;

    memcpy(mount->path, path, strlen(path) + 1);

    pthread_mutex_lock(&shared->lock);
    DL_APPEND(shared->mounts, mount);

    /* Store mount context in request */
    ctx                = request->plugin_data;
    ctx->server_thread = server_thread;
    ctx->mount         = mount;

    /* Start the NFS4 session establishment */
    chimera_nfs4_mount_exchange_id(server_thread, request);
} /* chimera_nfs4_mount_process_mount */

/*
 * Callback after NFS4 NULL call (connection test)
 */
static void
chimera_nfs4_mount_null_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request              *request       = private_data;
    struct chimera_nfs4_mount_ctx           *ctx           = request->plugin_data;
    struct chimera_nfs_client_server_thread *server_thread = ctx->server_thread;
    struct chimera_nfs_client_server        *server        = server_thread->server;
    struct chimera_nfs_shared               *shared        = server_thread->shared;

    if (status != 0) {
        chimera_nfsclient_error("NFS4 NULL call failed: %d", status);
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    pthread_mutex_lock(&shared->lock);
    server->state = CHIMERA_NFS_CLIENT_SERVER_STATE_DISCOVERED;
    pthread_mutex_unlock(&shared->lock);

    chimera_nfs4_mount_process_mount(server_thread, request);
} /* chimera_nfs4_mount_null_callback */

void
chimera_nfs4_mount(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    const char                              *path     = request->mount.path;
    const char                              *hostname = NULL;
    struct chimera_nfs_client_server       **new_servers, *server = NULL;
    struct chimera_nfs_client_server_thread *server_thread;
    struct chimera_nfs4_mount_ctx           *ctx;
    int                                      hostnamelen = 0;
    int                                      i, idx = -1;
    int                                      need_discover = 0;
    int                                      port;

    /* Parse hostname from "hostname:path" */
    for (i = 0; i < request->mount.pathlen; i++) {
        if (path[i] == ':') {
            hostname    = path;
            hostnamelen = i;
            break;
        }
    }

    if (hostnamelen == 0) {
        chimera_nfsclient_error("NFS4 mount: invalid path format (expected hostname:path)");
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    pthread_mutex_lock(&shared->lock);

    /* Check if we already have a server connection for this host */
    for (i = 0; i < shared->max_servers; i++) {
        if (shared->servers[i] &&
            strncmp(shared->servers[i]->hostname, hostname, hostnamelen) == 0 &&
            shared->servers[i]->hostname[hostnamelen] == '\0' &&
            shared->servers[i]->nfsvers == 4) {
            server = shared->servers[i];
            break;
        }
    }

    if (server) {
        server->refcnt++;

        if (server->state == CHIMERA_NFS_CLIENT_SERVER_STATE_DISCOVERING) {
            /* Someone else is discovering this server, wait for them */
            DL_APPEND(server->pending_mounts, request);
            pthread_mutex_unlock(&shared->lock);
            return;
        }
    } else {
        /* Find a free slot for the new server */
        for (i = 0; i < shared->max_servers; i++) {
            if (shared->servers[i] == NULL) {
                idx = i;
                break;
            }
        }

        if (idx < 0 || idx >= shared->max_servers) {
            /* Expand server array */
            shared->max_servers *= 2;
            new_servers          = calloc(shared->max_servers, sizeof(*new_servers));
            memcpy(new_servers, shared->servers, (shared->max_servers / 2) * sizeof(*new_servers));
            free(shared->servers);
            shared->servers = new_servers;
            idx             = i;
        }

        server          = calloc(1, sizeof(*server));
        server->state   = CHIMERA_NFS_CLIENT_SERVER_STATE_DISCOVERING;
        server->refcnt  = 1;
        server->nfsvers = 4;
        server->shared  = shared;

        /* Parse RDMA options */
        server->rdma_protocol = chimera_nfs4_mount_get_rdma_protocol(&request->mount.options);
        server->use_rdma      = server->rdma_protocol != 0;

        /* Get port (default 2049 for TCP, 20049 for RDMA) */
        port = chimera_nfs4_mount_get_port(&request->mount.options,
                                           server->use_rdma ? CHIMERA_NFS4_RDMA_PORT : CHIMERA_NFS4_DEFAULT_PORT);
        server->nfs_port = port;

        strncpy(server->hostname, hostname, hostnamelen);
        server->hostname[hostnamelen] = '\0';

        shared->servers[idx] = server;
        server->index        = idx;

        need_discover = 1;

        DL_APPEND(server->pending_mounts, request);
    }

    pthread_mutex_unlock(&shared->lock);

    /* Create server thread context */
    server_thread         = calloc(1, sizeof(*server_thread));
    server_thread->thread = thread;
    server_thread->shared = shared;
    server_thread->server = server;

    /* Store context in request plugin_data */
    ctx                = request->plugin_data;
    ctx->server_thread = server_thread;

    if (thread->max_server_threads != shared->max_servers) {
        thread->max_server_threads = shared->max_servers;
        thread->server_threads     = realloc(thread->server_threads,
                                             thread->max_server_threads * sizeof(*thread->server_threads));
    }

    thread->server_threads[server->index] = server_thread;

    if (need_discover) {
        enum evpl_protocol_id proto;

        /* Create NFS endpoint and connect */
        server->nfs_endpoint = evpl_endpoint_create(server->hostname, server->nfs_port);

        proto = server->use_rdma ? server->rdma_protocol : EVPL_STREAM_SOCKET_TCP;

        server_thread->nfs_conn = evpl_rpc2_client_connect(
            thread->rpc2_thread,
            proto,
            server->nfs_endpoint,
            NULL, 0, NULL);

        if (!server_thread->nfs_conn) {
            chimera_nfsclient_error("NFS4 mount: failed to connect to %s:%d",
                                    server->hostname, server->nfs_port);
            request->status = CHIMERA_VFS_EIO;
            request->complete(request);
            return;
        }

        chimera_nfsclient_info("NFS4 connecting to %s:%d", server->hostname, server->nfs_port);

        /* Send NULL call to verify connection */
        shared->nfs_v4.send_call_NFSPROC4_NULL(
            &shared->nfs_v4.rpc2,
            thread->evpl,
            server_thread->nfs_conn,
            NULL,
            0, 0, 0,
            chimera_nfs4_mount_null_callback,
            request);
    } else {
        /* Server already discovered, proceed with mount */
        chimera_nfs4_mount_process_mount(server_thread, request);
    }
} /* chimera_nfs4_mount */
