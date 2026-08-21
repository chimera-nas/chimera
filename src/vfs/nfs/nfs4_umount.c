// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"


/*
 * Session teardown on the last unmount, the mirror of the EXCHANGE_ID +
 * CREATE_SESSION that established it.
 *
 * Without it the client simply stops talking, and a clean shutdown is
 * indistinguishable to the server from a client that crashed: it keeps the
 * clientid's state -- and the open handles behind it -- until the lease
 * expires.  For that whole window the export cannot be unmounted, and a
 * restarting client strands a fresh clientid on every cycle.
 *
 * This runs as part of the umount request rather than at module teardown
 * because that is the only point where both halves are still true: the
 * server's transport is up, and an ordinary event loop is running to carry
 * the replies.  By the time the module is destroyed the per-thread rpc2
 * transports are already gone.
 */

struct chimera_nfs4_umount_teardown {
    struct chimera_nfs_thread          *thread;
    struct chimera_nfs_shared          *shared;
    struct chimera_vfs_request         *request;
    struct chimera_nfs_client_server   *server;
    struct chimera_nfs4_client_session *session;
    struct evpl_rpc2_conn              *conn;
};

static void chimera_nfs4_umount_send_destroy_session(
    struct chimera_nfs4_umount_teardown *td);

static void
chimera_nfs4_umount_teardown_done(struct chimera_nfs4_umount_teardown *td)
{
    struct chimera_vfs_request *request = td->request;

    /* Release the server's reference.  The pool stays mapped until every
     * per-thread slot table has noticed the session is gone and let go. */
    chimera_nfs4_session_put(td->session);

    free(td);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_umount_teardown_done */

static void
chimera_nfs4_umount_destroy_clientid_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_nfs4_umount_teardown *td = private_data;

    (void) evpl;
    (void) verf;

    if (status || !res || res->status != NFS4_OK) {
        chimera_nfsclient_debug(
            "NFS4 DESTROY_CLIENTID to %s did not succeed (rpc %d, nfs %d); "
            "the server will expire the clientid with the lease",
            td->server->hostname, status, res ? res->status : -1);
    }

    chimera_nfs4_umount_teardown_done(td);
} /* chimera_nfs4_umount_destroy_clientid_callback */

static void
chimera_nfs4_umount_send_destroy_clientid(struct chimera_nfs4_umount_teardown *td)
{
    struct COMPOUND4args args;
    struct nfs_argop4    argarray[1];

    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 1;

    argarray[0].argop                           = OP_DESTROY_CLIENTID;
    argarray[0].opdestroy_clientid.dca_clientid = td->session->clientid;

    td->shared->nfs_v4.send_call_NFSPROC4_COMPOUND(
        &td->shared->nfs_v4.rpc2,
        td->thread->evpl,
        td->conn,
        NULL,
        &args,
        0, 0, NULL, 0, 0,
        chimera_nfs4_umount_destroy_clientid_callback,
        td);
} /* chimera_nfs4_umount_send_destroy_clientid */

static void
chimera_nfs4_umount_bind_conn_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_nfs4_umount_teardown *td = private_data;

    (void) evpl;
    (void) verf;

    if (status || !res || res->status != NFS4_OK) {
        chimera_nfsclient_debug(
            "NFS4 BIND_CONN_TO_SESSION to %s did not succeed (rpc %d, nfs %d); "
            "the session cannot be destroyed from here and will lapse with the "
            "lease",
            td->server->hostname, status, res ? res->status : -1);

        chimera_nfs4_umount_teardown_done(td);
        return;
    }

    chimera_nfs4_umount_send_destroy_session(td);
} /* chimera_nfs4_umount_bind_conn_callback */

/*
 * Bind this connection to the session before destroying it.
 *
 * The umount can land on any thread, so the connection it gets may be one
 * created for this teardown and never used for anything else.  A connection
 * becomes associated with a session by carrying a SEQUENCE, and a fresh one
 * never has, so the server refuses DESTROY_SESSION on it with
 * NFS4ERR_CONN_NOT_BOUND_TO_SESSION.  Binding it explicitly is the operation
 * NFSv4.1 provides for exactly this.  Fore channel only: nothing is going to
 * arrive on a connection that is about to be closed.
 */
static void
chimera_nfs4_umount_send_bind_conn(struct chimera_nfs4_umount_teardown *td)
{
    struct COMPOUND4args args;
    struct nfs_argop4    argarray[1];

    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 1;

    argarray[0].argop = OP_BIND_CONN_TO_SESSION;
    memcpy(argarray[0].opbind_conn_to_session.bctsa_sessid,
           td->session->sessionid, NFS4_SESSIONID_SIZE);
    argarray[0].opbind_conn_to_session.bctsa_dir                   = CDFC4_FORE;
    argarray[0].opbind_conn_to_session.bctsa_use_conn_in_rdma_mode = 0;

    td->shared->nfs_v4.send_call_NFSPROC4_COMPOUND(
        &td->shared->nfs_v4.rpc2,
        td->thread->evpl,
        td->conn,
        NULL,
        &args,
        0, 0, NULL, 0, 0,
        chimera_nfs4_umount_bind_conn_callback,
        td);
} /* chimera_nfs4_umount_send_bind_conn */

static void
chimera_nfs4_umount_destroy_session_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_nfs4_umount_teardown *td = private_data;

    (void) evpl;
    (void) verf;

    if (status || !res || res->status != NFS4_OK) {
        chimera_nfsclient_debug(
            "NFS4 DESTROY_SESSION to %s did not succeed (rpc %d, nfs %d)",
            td->server->hostname, status, res ? res->status : -1);
        /* Carry on to the clientid regardless: the session may already be
         * gone, and it is the clientid that pins the state worth releasing. */
    }

    chimera_nfs4_umount_send_destroy_clientid(td);
} /* chimera_nfs4_umount_destroy_session_callback */

static void
chimera_nfs4_umount_send_destroy_session(struct chimera_nfs4_umount_teardown *td)
{
    struct COMPOUND4args args;
    struct nfs_argop4    argarray[1];

    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 1;

    /* Sent alone, with no SEQUENCE: a compound may not carry the session it is
     * destroying. */
    argarray[0].argop = OP_DESTROY_SESSION;
    memcpy(argarray[0].opdestroy_session.dsa_sessionid,
           td->session->sessionid, NFS4_SESSIONID_SIZE);

    td->shared->nfs_v4.send_call_NFSPROC4_COMPOUND(
        &td->shared->nfs_v4.rpc2,
        td->thread->evpl,
        td->conn,
        NULL,
        &args,
        0, 0, NULL, 0, 0,
        chimera_nfs4_umount_destroy_session_callback,
        td);
} /* chimera_nfs4_umount_send_destroy_session */

void
chimera_nfs4_umount(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_mount         *mount  = request->umount.mount_private;
    struct chimera_nfs_client_server        *server = mount->server;

    struct chimera_nfs4_client_session      *session = NULL;
    struct chimera_nfs_client_server_thread *server_thread;
    struct chimera_nfs4_umount_teardown     *td;

    pthread_mutex_lock(&shared->lock);

    DL_DELETE(shared->mounts, mount);

    free(mount);

    server->refcnt--;

    /*
     * With the last mount gone, tell the server we are finished with the
     * session rather than leaving it to the lease.  Unpublishing it here is
     * what makes that safe for the slot pool: a remount establishes a fresh
     * session, and each per-thread slot table compares the session its ids
     * came from against the published one and rebuilds itself when they
     * differ, so no thread can carry old ids onto a new session.
     */
    if (server->refcnt == 0 && server->nfs4_session) {
        session              = server->nfs4_session;
        server->nfs4_session = NULL;
    }

    pthread_mutex_unlock(&shared->lock);

    if (session) {
        /* This thread may never have talked to the server -- the umount can
         * land anywhere -- so get a connection the same way any other op
         * does, creating one on demand. */
        server_thread = chimera_nfs_thread_get_server_thread(thread,
                                                             request->fh,
                                                             request->fh_len);

        if (server_thread && server_thread->nfs_conn) {
            td          = calloc(1, sizeof(*td));
            td->thread  = thread;
            td->shared  = shared;
            td->request = request;
            td->server  = server;
            td->session = session;
            td->conn    = server_thread->nfs_conn;

            chimera_nfs4_umount_send_bind_conn(td);
            return;
        }

        /* No connection left to say it on -- drop the reference and let the
         * server's lease do the work. */
        chimera_nfs4_session_put(session);
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_nfs4_umount */
