// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs4_open_state.h"

struct chimera_nfs4_open_fh_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_shared        *shared;
    struct chimera_nfs_client_server *server;
    void                             *dispatch_private;
};

static void
chimera_nfs4_open_fh_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request       *request = private_data;
    struct chimera_nfs4_open_fh_ctx  *ctx     = request->plugin_data;
    struct chimera_nfs_client_server *server  = ctx->server;
    struct nfs_resop4                *open_res;
    struct chimera_nfs4_open_state   *state;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(res->status);
        request->complete(request);
        return;
    }

    if (res->num_resarray < 3 ||
        res->resarray[0].opsequence.sr_status != NFS4_OK ||
        res->resarray[1].opputfh.status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    open_res = &res->resarray[2];
    if (open_res->opopen.status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(open_res->opopen.status);
        request->complete(request);
        return;
    }

    /* Count this handle against the file's open on the server, which every
     * handle on the file shares (see nfs4_open_state.h).  A refusal means
     * this OPEN raced an in-flight CLOSE of the same file and coalesced into
     * the state that CLOSE destroys -- the returned stateid is dead on
     * arrival, so re-send the OPEN; once the CLOSE has landed the retry
     * receives a fresh state. */
    if (chimera_nfs4_open_file_get(server, request->fh, request->fh_len,
                                   &open_res->opopen.resok4.stateid) != 0) {
        chimera_nfs4_open_fh(ctx->thread, ctx->shared, request,
                             ctx->dispatch_private);
        return;
    }

    state = chimera_nfs4_open_state_alloc();

    if (!state) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    state->server_index = server->index;
    state->stateid      = open_res->opopen.resok4.stateid;

    request->open_fh.r_vfs_private = (uint64_t) state;
    request->status                = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_open_fh_callback */

void
chimera_nfs4_open_fh(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread;
    struct chimera_nfs_client_server        *server;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[3];
    struct evpl_rpc2_cred                    rpc2_cred;
    struct OPEN4args                        *open_args;
    uint8_t                                 *fh;
    int                                      fhlen;

    /*
     * For inferred opens (internal opens used for path traversal, like
     * opening a parent directory before open_at), we don't need to do an
     * actual NFS4 OPEN operation.  Just return OK with no state.
     *
     * Similarly, directories don't need NFS4 OPEN - they're accessed via
     * READDIR, LOOKUP etc.
     */
    if ((request->open_fh.flags & CHIMERA_VFS_OPEN_INFERRED) ||
        (request->open_fh.flags & CHIMERA_VFS_OPEN_DIRECTORY)) {
        request->open_fh.r_vfs_private = 0;
        request->status                = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /*
     * Data open of an existing regular file by handle: a real NFSv4.1 OPEN
     * with CLAIM_FH, so the server holds an open (and a stateid) for this
     * handle.  Anything less loses unlink-while-open: without a server-side
     * open, removing the file's last name reclaims the object, and the
     * still-open descriptor's anonymous-stateid I/O starts failing STALE the
     * moment the file's other (stateful) opens close.
     */
    server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                         request->fh_len);

    if (!server_thread || !server_thread->server->nfs4_session) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    server = server_thread->server;
    {
        struct chimera_nfs4_open_fh_ctx *ctx = request->plugin_data;

        ctx->thread           = thread;
        ctx->shared           = shared;
        ctx->server           = server;
        ctx->dispatch_private = private_data;
    }

    chimera_nfs4_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    /* Build compound: SEQUENCE + PUTFH(file) + OPEN(CLAIM_FH) */
    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 3;

    argarray[0].argop = OP_SEQUENCE;

    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = fh;
    argarray[1].opputfh.object.len  = fhlen;

    argarray[2].argop = OP_OPEN;
    open_args         = &argarray[2].opopen;

    open_args->seqid = 0;

    /* Share access from the open flags; O_RDWR requests both.  A by-handle
     * reopen cannot know less than the caller asked for. */
    if ((request->open_fh.flags & CHIMERA_VFS_OPEN_READ_ONLY) &&
        !(request->open_fh.flags & CHIMERA_VFS_OPEN_WRITE_ONLY)) {
        open_args->share_access = OPEN4_SHARE_ACCESS_READ;
    } else if ((request->open_fh.flags & CHIMERA_VFS_OPEN_WRITE_ONLY) &&
               !(request->open_fh.flags & CHIMERA_VFS_OPEN_READ_ONLY)) {
        open_args->share_access = OPEN4_SHARE_ACCESS_WRITE;
    } else {
        open_args->share_access = OPEN4_SHARE_ACCESS_READ |
            OPEN4_SHARE_ACCESS_WRITE;
    }
    open_args->share_deny = OPEN4_SHARE_DENY_NONE;

    open_args->owner.clientid   = server->nfs4_session->clientid;
    open_args->owner.owner.data = (uint8_t *) server->nfs4_owner_id;
    open_args->owner.owner.len  = server->nfs4_owner_id_len;

    open_args->openhow.opentype = OPEN4_NOCREATE;

    /* CLAIM_FH (RFC 8881 §18.16): the object is the current filehandle set
     * by the preceding PUTFH; no name travels. */
    open_args->claim.claim = CLAIM_FH;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    chimera_nfs4_compound_call(
        thread,
        shared,
        server_thread,
        request,
        &args,
        &rpc2_cred,
        0, 0, NULL, 0, 0,
        chimera_nfs4_open_fh_callback,
        request,
        chimera_nfs4_dispatch, private_data);
} /* chimera_nfs4_open_fh */
