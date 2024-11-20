#include <stdio.h>
#include "nfs.h"
#include "vfs/protocol.h"
#include "rpc2/rpc2.h"
#include "nfs3_xdr.h"
#include "nfs4_xdr.h"

struct chimera_server_nfs_shared {
    struct NFS_V3    nfs_v3;
    struct NFS_V4    nfs_v4;
    struct NFS_V4_CB nfs_v4_cb;
};

struct chimera_server_nfs_thread {
    struct chimera_server_nfs_shared *shared;
    struct evpl_rpc2_agent           *rpc2_agent;
    struct evpl_rpc2_server          *server;
    struct evpl_endpoint             *endpoint;
};

static nfsstat4
handle_op_getfh(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_msg             *msg,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    // Handle GETFH operation
    // For now, return a dummy response
    //uint64_t token = 2;

    resop->resop                     = OP_GETFH;
    resop->opgetfh.status            = NFS4_OK; // Dummy status
    resop->opgetfh.resok4.object.len = 0;
#if 0
    xdr_dbuf_memcpy(&resop->opgetfh.resok4.object,
                    &token,
                    sizeof(token),
                    msg->dbuf);
#endif /* if 0 */

    return NFS4_OK;
} /* handle_op_getfh */

/* Example helper functions for NFS operations */
static nfsstat4
handle_op_putrootfh(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_msg             *msg,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    /* Handle PUTROOTFH operation */
    /* ...implementation... */
    resop->resop              = OP_PUTROOTFH;
    resop->opputrootfh.status = NFS4_OK;
    return NFS4_OK;
} /* handle_op_putrootfh */

static nfsstat4
handle_op_getattr(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_msg             *msg,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    /* Handle GETATTR operation */
    /* ...implementation... */
    resop->resop                                            = OP_GETATTR;
    resop->opgetattr.status                                 = NFS4_OK;
    resop->opgetattr.resok4.obj_attributes.num_attrmask     = 0;
    resop->opgetattr.resok4.obj_attributes.attr_vals.length = 0;

    /* Set attributes as needed */
    return NFS4_OK;
} /* handle_op_getattr */
static nfsstat4
handle_op_lookup(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_msg             *msg,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    // Handle LOOKUP operation
    // TODO: Implement the actual logic for LOOKUP
    // 1. Validate the current filehandle is a directory.
    // 2. Search for the object name in the directory.
    // 3. If found, update the current filehandle to the object's filehandle.
    // 4. Set the appropriate status in the response.

    // For now, return a dummy response
    resop->resop           = OP_LOOKUP;
    resop->oplookup.status = NFS4_OK; // Dummy status
    return NFS4_OK;
} /* handle_op_lookup */

/* Helper function for SETCLIENTID operation */
static nfsstat4
handle_op_setclientid(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_msg             *msg,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    //SETCLIENTID4args *args = &argop->opsetclientid;

    resop->resop = OP_SETCLIENTID;

    resop->opsetclientid.status          = NFS4_OK;
    resop->opsetclientid.resok4.clientid = random();

    uint64_t clientid = random();

    memcpy(&resop->opsetclientid.resok4.setclientid_confirm,
           &clientid,
           sizeof(clientid));

    return NFS4_OK;
} /* handle_op_setclientid */

/* Helper function for SETCLIENTID_CONFIRM operation */
static nfsstat4
handle_op_setclientid_confirm(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_msg             *msg,
    nfs_argop4                       *argop,
    nfs_resop4                       *resop)
{
    //SETCLIENTID_CONFIRM4args *args = &argop->opsetclientid_confirm;

    resop->resop = OP_SETCLIENTID_CONFIRM;

    /*
     * In a real implementation, we would:
     * 1. Verify the clientid matches one from a previous SETCLIENTID
     * 2. Verify the confirmation verifier matches
     * 3. Create/update the confirmed client record
     * 4. Remove any unconfirmed records for this client
     */

    resop->opsetclientid_confirm.status = NFS4_OK;
    return NFS4_OK;
} /* handle_op_setclientid_confirm */

/* Add more helper functions for other operations as needed */

static void
chimera_nfs4_compound(
    struct evpl          *evpl,
    struct COMPOUND4args *args,
    struct evpl_rpc2_msg *msg,
    void                 *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    COMPOUND4res                      res;
    uint32_t                          i;

    fprintf(stderr, "nfs4 compound call entry\n");

    /* Initialize the response */
    res.status       = NFS4_OK;
    res.tag.length   = 0;
    res.num_resarray = args->num_argarray;

    /* Allocate memory for the response array */
    res.resarray     = msg->dbuf->buffer + msg->dbuf->used;
    msg->dbuf->used += res.num_resarray * sizeof(nfs_resop4);

    /* Process each operation in the compound request */
    for (i = 0; i < args->num_argarray; i++) {
        nfs_argop4 *argop  = &args->argarray[i];
        nfs_resop4 *resop  = &res.resarray[i];
        nfsstat4    status = NFS4_OK;

        fprintf(stderr, "nfs4 compound operation: %d\n", argop->argop);
        switch (argop->argop) {
            case OP_GETFH:
                status = handle_op_getfh(thread, msg, argop, resop);
                break;
            case OP_PUTROOTFH:
                status = handle_op_putrootfh(thread, msg, argop, resop);
                break;
            case OP_GETATTR:
                status = handle_op_getattr(thread, msg, argop, resop);
                break;
            case OP_LOOKUP:
                status = handle_op_lookup(thread, msg, argop, resop);
                break;
            case OP_SETCLIENTID:
                status = handle_op_setclientid(thread, msg, argop, resop);
                break;
            case OP_SETCLIENTID_CONFIRM:
                status = handle_op_setclientid_confirm(thread, msg, argop, resop
                                                       );
                break;
            /* Add cases for other operations */
            default:
                fprintf(stderr, "Unsupported operation: %d\n", argop->argop);
                status = NFS4ERR_OP_ILLEGAL;
                break;
        } /* switch */

        if (status != NFS4_OK) {
            /* If an error occurred, set the overall status and stop processing */
            res.status = status;
            break;
        }
    }

    /* Send the response */

    fprintf(stderr, "nfs4 compound call dispatching reply\n");

    shared->nfs_v4.send_reply_NFSPROC4_COMPOUND(evpl, &res, msg);

} /* chimera_nfs4_compound */

static void
chimera_nfs4_null(
    struct evpl          *evpl,
    struct evpl_rpc2_msg *msg,
    void                 *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;

    fprintf(stderr, "nfs4 null call\n");

    shared->nfs_v4.send_reply_NFSPROC4_NULL(evpl, msg);
} /* chimera_nfs4_null */

static void *
nfs_server_init(void)
{
    struct chimera_server_nfs_shared *shared;

    shared = calloc(1, sizeof(*shared));

    NFS_V3_init(&shared->nfs_v3);
    NFS_V4_init(&shared->nfs_v4);
    NFS_V4_CB_init(&shared->nfs_v4_cb);

    shared->nfs_v4.recv_call_NFSPROC4_NULL     = chimera_nfs4_null;
    shared->nfs_v4.recv_call_NFSPROC4_COMPOUND = chimera_nfs4_compound;

    return shared;
} /* nfs_server_init */

static void
nfs_server_destroy(void *data)
{
    struct chimera_server_nfs_shared *shared = data;

    free(shared);
} /* nfs_server_destroy */

static void *
nfs_server_thread_init(
    struct evpl *evpl,
    void        *data)
{
    struct chimera_server_nfs_shared *shared = data;
    struct chimera_server_nfs_thread *thread;
    struct evpl_rpc2_program         *programs[3];

    programs[0] = &shared->nfs_v3.rpc2;
    programs[1] = &shared->nfs_v4.rpc2;
    programs[2] = &shared->nfs_v4_cb.rpc2;

    thread             = calloc(1, sizeof(*thread));
    thread->shared     = data;
    thread->rpc2_agent = evpl_rpc2_init(evpl);

    thread->endpoint = evpl_endpoint_create(evpl, "0.0.0.0", 2049);

    thread->server = evpl_rpc2_listen(thread->rpc2_agent,
                                      EVPL_STREAM_SOCKET_TCP,
                                      thread->endpoint,
                                      programs,
                                      3,
                                      thread);
    return thread;
} /* nfs_server_thread_init */

static void
nfs_server_thread_destroy(
    struct evpl *evpl,
    void        *data)
{
    struct chimera_server_nfs_thread *thread = data;

    evpl_rpc2_destroy(thread->rpc2_agent);
    free(thread);
} /* nfs_server_thread_destroy */

struct chimera_server_protocol nfs_protocol = {
    .init           = nfs_server_init,
    .destroy        = nfs_server_destroy,
    .thread_init    = nfs_server_thread_init,
    .thread_destroy = nfs_server_thread_destroy,
};
