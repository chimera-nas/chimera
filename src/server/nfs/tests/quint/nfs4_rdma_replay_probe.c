// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_drc_mbt_common.h"
#include "common/mbt_watchdog.h"

#define REPLAY_BYTES 65536
#define FILE_BYTES   8192
#define WRITE_OFFSET 7000

struct drc_session {
    uint64_t clientid;
    uint32_t seqid;              /* csr_sequence from CREATE_SESSION */
    uint8_t  id[NFS4_SESSIONID_SIZE];
};

static void
v41_establish(
    struct drc_ctx              *c,
    const struct evpl_rpc2_cred *cred,
    struct drc_session          *out)
{
    struct nfs_argop4                 a[1];
    struct drc_v4_res                 res;
    struct channel_attrs4             chan = {
        .ca_headerpadsize          = 0,
        .ca_maxrequestsize         = 1048576,
        .ca_maxresponsesize        = 1048576,
        .ca_maxresponsesize_cached = 65536,
        .ca_maxoperations          = 16,
        .ca_maxrequests            = 32,
        .num_ca_rdma_ird           = 0,
    };
    static struct callback_sec_parms4 sec     = { .cb_secflavor = 0 };
    static uint8_t                    verf[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    static const char                *owner   = "quintdrc-client";

    memset(a, 0, sizeof(a));
    a[0].argop = OP_EXCHANGE_ID;
    memcpy(a[0].opexchange_id.eia_clientowner.co_verifier, verf, 8);
    a[0].opexchange_id.eia_clientowner.co_ownerid.data = (void *) owner;
    a[0].opexchange_id.eia_clientowner.co_ownerid.len  =
        (uint32_t) strlen(owner);
    a[0].opexchange_id.eia_flags                 = 0;
    a[0].opexchange_id.eia_state_protect.spa_how = SP4_NONE;
    a[0].opexchange_id.num_eia_client_impl_id    = 0;

    drc_v4_call(c, 0, cred, 800001, a, 1, 1, &res);
    if (res.status != NFS4_OK) {
        fprintf(stderr, "drc probe: EXCHANGE_ID failed: %u\n", res.status);
        exit(2);
    }
    out->clientid = res.clientid;
    out->seqid    = res.seqid;

    memset(a, 0, sizeof(a));
    a[0].argop                                = OP_CREATE_SESSION;
    a[0].opcreate_session.csa_clientid        = out->clientid;
    a[0].opcreate_session.csa_sequence        = out->seqid;
    a[0].opcreate_session.csa_flags           = 0;
    a[0].opcreate_session.csa_fore_chan_attrs = chan;
    a[0].opcreate_session.csa_back_chan_attrs = chan;
    a[0].opcreate_session.csa_cb_program      = 0x40000000;
    a[0].opcreate_session.num_csa_sec_parms   = 1;
    a[0].opcreate_session.csa_sec_parms       = &sec;

    drc_v4_call(c, 0, cred, 800002, a, 1, 1, &res);
    if (res.status != NFS4_OK) {
        fprintf(stderr, "drc probe: CREATE_SESSION failed: %u\n", res.status);
        exit(2);
    }
    memcpy(out->id, res.sessionid, NFS4_SESSIONID_SIZE);
} /* v41_establish */

/* Re-encode the decoded answer without chunks for byte-for-byte comparison.
 * This consumes the decoder's READ references, including the second inline
 * READ; it also checks that the client decoded the entire compound correctly. */
struct replay_answer {
    struct evpl_iovec *destination;
    int                done;
    int                error;
    int                length;
    uint8_t            bytes[REPLAY_BYTES];
};

static void
replay_reply(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *reply,
    int                          status,
    void                        *private_data)
{
    struct replay_answer *answer = private_data;

    answer->error = status;
    if (!status) {
        struct evpl_iovec scratch, output[260], borrowed;
        int               niov = 260, offset = 0;
        if (!reply || reply->status != NFS4_OK ||
            evpl_iovec_alloc(evpl, REPLAY_BYTES, 8, 1, 0, &scratch) != 1) {
            fprintf(stderr, "replay returned NFS status %u\n", reply ? reply->status : UINT32_MAX);
            exit(2);
        }
        /* read-into lends the caller's destination; the decoded first READ
         * does not own a reference that the marshaller may consume. */
        if (answer->destination) {
            for (uint32_t i = 0; i < reply->num_resarray; i++) {
                struct nfs_resop4 *op = &reply->resarray[i];
                if (op->resop == OP_READ && op->opread.status == NFS4_OK) {
                    xdr_iovecr *data = &op->opread.resok4.data;
                    if (data->length) {
                        if (data->niov != 1 || data->iov[0].data != answer->destination->data) {
                            exit(2);
                        }
                        evpl_iovec_clone_segment(&borrowed, answer->destination, 0, data->length);
                        data->iov = &borrowed;
                    }
                    break;
                }
            }
        }
        int length = marshall_COMPOUND4res(reply, &scratch, output, &niov, NULL, 0);
        if (length < 0 || length > REPLAY_BYTES) {
            exit(2);
        }
        for (int i = 0; i < niov; i++) {
            if (output[i].length > (uint32_t) (length - offset)) {
                exit(2);
            }
            memcpy(answer->bytes + offset, output[i].data, output[i].length);
            offset += output[i].length;
        }
        evpl_iovecs_release(evpl, output, niov);
        evpl_iovec_release(evpl, &scratch);
        if (offset != length) {
            exit(2);
        }
        answer->length = length;
    }
    answer->done = 1;
} /* replay_reply */

static void
replay_call(
    struct drc_ctx              *ctx,
    const struct evpl_rpc2_cred *cred,
    struct COMPOUND4args        *args,
    int                          connection,
    uint32_t                     xid,
    int                          write_offer,
    int                          reply_offer,
    struct evpl_iovec           *destination,
    struct replay_answer        *answer)
{
    struct evpl_iovec      write;
    struct evpl_rpc2_conn *conn = drc_conn(ctx, connection);

    memset(answer, 0, sizeof(*answer));
    answer->destination = write_offer ? destination : NULL;
    if (evpl_iovec_alloc(ctx->env->evpl, 1, 8, 1, 0, &write) != 1) {
        exit(2);
    }
    *(uint8_t *) write.data = 'W';
    for (uint32_t i = 0; i < args->num_argarray; i++) {
        if (args->argarray[i].argop == OP_WRITE) {
            xdr_set_ref(&args->argarray[i].opwrite, data, &write, 1, 1);
        }
    }
    evpl_rpc2_conn_set_next_xid(conn, xid);
    ctx->env->nfs_v4.send_call_NFSPROC4_COMPOUND(&ctx->env->nfs_v4.rpc2,
                                                 ctx->env->evpl, conn, cred, args, 0, write_offer,
                                                 write_offer ? destination : NULL, write_offer ? 1 : 0,
                                                 reply_offer, replay_reply, answer);
    while (!answer->done) {
        evpl_continue(ctx->env->evpl);
    }
} /* replay_call */

static int
bytes_equal(
    const struct evpl_iovec *iov,
    uint8_t                  byte,
    uint32_t                 start)
{
    const uint8_t *p = iov->data;

    for (uint32_t i = start; i < iov->length; i++) {
        if (p[i] != byte) {
            return 0;
        }
    }
    return 1;
} /* bytes_equal */

int
main(
    int    argc,
    char **argv)
{
    struct mbt_env        env;
    struct mbt_env_opts   opts    = { .module = "memfs", .rdma = 1, .nfs3_drc = 1 };
    struct drc_ctx        ctx     = { .env = &env };
    struct drc_session    session = { 0 };
    struct evpl_rpc2_cred cred;
    struct replay_answer  original, retry;
    struct mbt_fh         file;
    uint8_t               data[FILE_BYTES];
    uint32_t              minor    = argc > 1 ? (uint32_t) atoi(argv[1]) : 1;
    uint32_t              sequence = 0;
    int                   failures = 0;
    static const uint32_t counts[] = { 4093, 0, 3 };
    /* Original: Write+Reply, Reply only, Write only, stream. */
    static const int      origins[][3] = {
        { 0, 8192, 8192, },
        { 0, 0,    8192, },
        { 0, 8192, 0,    },
        { 1, 0,    0,    },
    };
    /* Retry: new chunk buffers, no chunks, Reply only, Write only, stream,
     * insufficient Write capacity, insufficient Reply capacity. */
    static const int      retries[][3] = {
        { 0, 8192, 8192, },
        { 0, 0,    0,    },
        { 0, 0,    8192, },
        { 0, 8192, 0,    },
        { 1, 0,    0,    },
        { 0, 1,    8192, },
        { 0, 8192, 1,    },
    };

    mbt_watchdog_arm(90);
    umask(0);
    mbt_env_open_opts(&env, &opts);
    mbt_env_fs_setup(&env, "fs0");
    snprintf(ctx.mntpath, sizeof(ctx.mntpath), "/fs0");
    drc_trace_setup(&ctx);
    drc_cred_init(&env, &cred, 1000);
    if (minor) {
        v41_establish(&ctx, &cred, &session);
    }
    /* A session retry may arrive on another transport. The v4.0 DRC is
     * connection-scoped, so those cases deliberately stay on connection 0. */
    env.rdma = 0;
    drc_conn(&ctx, 1);
    env.rdma = 1;

    struct mbt_result *made = mbt_create(&env, &ctx.dir_fh, "replay", 6, UNCHECKED, 0666, NULL);
    if (made->status != NFS3_OK) {
        exit(2);
    }
    file = made->obj_fh;

    for (unsigned count_index = 0; count_index < sizeof(counts) / sizeof(counts[0]); count_index++) {
        uint32_t count = counts[count_index];
        for (unsigned origin = 0; origin < (minor ? 4U : 3U); origin++) {
            struct nfs_argop4    ops[8] = { 0 };
            struct COMPOUND4args args   = { 0 };
            struct evpl_iovec    original_dest;
            uint32_t             bitmap = (1U << FATTR4_TYPE) | (1U << FATTR4_SIZE);
            int                  n      = 0;
            uint32_t             xid    = 970000 + ++sequence;
            memset(data, 'A', sizeof(data));
            if (mbt_write(&env, &file, 0, data, sizeof(data), FILE_SYNC)->status != NFS3_OK) {
                exit(2);
            }
            if (minor) {
                ops[n].argop = OP_SEQUENCE;
                memcpy(ops[n].opsequence.sa_sessionid, session.id, sizeof(session.id));
                ops[n].opsequence.sa_sequenceid     = sequence;
                ops[n].opsequence.sa_highest_slotid = 31;
                ops[n++].opsequence.sa_cachethis    = true;
            }
            drc_v4_putfh(&ops[n++], &file);
            ops[n].argop                        = OP_GETATTR;
            ops[n].opgetattr.attr_request       = &bitmap;
            ops[n++].opgetattr.num_attr_request = 1;
            ops[n].argop                        = OP_READ;
            ops[n++].opread.count               = count;
            ops[n].argop                        = OP_READ;
            ops[n].opread.offset                = 4096;
            ops[n++].opread.count               = 1003;
            ops[n].argop                        = OP_WRITE;
            ops[n].opwrite.offset               = WRITE_OFFSET;
            ops[n++].opwrite.stable             = FILE_SYNC4;
            ops[n++].argop                      = OP_SETATTR; /* Makes v4.0 eligible for its DRC. */
            ops[n++].argop                      = OP_GETFH;
            args.tag.data                       = (void *) "odd";
            args.tag.len                        = 3;
            args.minorversion                   = minor;
            args.num_argarray                   = n;
            args.argarray                       = ops;
            if (evpl_iovec_alloc(env.evpl, 8192, 8, 1, 0, &original_dest) != 1) {
                exit(2);
            }
            memset(original_dest.data, 0xCC, original_dest.length);
            replay_call(&ctx, &cred, &args, origins[origin][0], xid, origins[origin][1],
                        origins[origin][2], &original_dest, &original);
            if (original.error) {
                fprintf(stderr, "original RPC failed: %d\n", original.error);
                exit(2);
            }
            if (origins[origin][1] &&
                (!bytes_equal(&original_dest, 0xCC, count) ||
                 (count && memcmp(original_dest.data, data, count)))) {
                fprintf(stderr, "original READ placement mismatch\n");
                failures++;
            }
            /* Keep the original registered memory alive at another address,
             * then replace the file. A replay must return A, leave B on disk,
             * and never write into the original buffer again. */
            memset(original_dest.data, 0xD7, original_dest.length);
            memset(data, 'B', sizeof(data));
            if (mbt_write(&env, &file, 0, data, sizeof(data), FILE_SYNC)->status != NFS3_OK) {
                exit(2);
            }
            for (unsigned attempt = 0; attempt < sizeof(retries) / sizeof(retries[0]); attempt++) {
                if (!minor && retries[attempt][0]) {
                    continue;
                }
                struct evpl_iovec destination;
                if (evpl_iovec_alloc(env.evpl, 8192, 8, 1, 0, &destination) != 1) {
                    exit(2);
                }
                memset(destination.data, 0xCC, destination.length);
                int               expected_error = (attempt == 5 && count > 1) || attempt == 6;
                replay_call(&ctx, &cred, &args, retries[attempt][0],
                            minor ? xid + 10000 + attempt : xid, retries[attempt][1],
                            retries[attempt][2], &destination, &retry);
                int               ok = bytes_equal(&original_dest, 0xD7, 0);
                if (expected_error) {
                    ok &= retry.error == EVPL_RPC2_REPLY_RDMA_ERROR && bytes_equal(&destination, 0xCC, 0);
                } else {
                    ok &= !retry.error && retry.length == original.length &&
                        !memcmp(retry.bytes, original.bytes, original.length);
                    uint32_t placed = retries[attempt][1] ? count : 0;
                    ok &= bytes_equal(&destination, 0xCC, placed);
                    for (uint32_t i = 0; i < placed; i++) {
                        ok &= ((uint8_t *) destination.data)[i] == 'A';
                    }
                }
                if (!ok) {
                    fprintf(stderr, "minor%u count%u origin%u retry%u error%d failed\n",
                            minor, count, origin, attempt, retry.error);
                    failures++;
                }
                evpl_iovec_release(env.evpl, &destination);
            }
            struct mbt_result *after = mbt_read(&env, &file, WRITE_OFFSET, 1);
            if (after->status != NFS3_OK || after->data_len != 1 || after->data[0] != 'B') {
                fprintf(stderr, "cached WRITE was re-executed\n");
                failures++;
            }
            evpl_iovec_release(env.evpl, &original_dest);
        }
    }
    drc_conns_close(&ctx);
    mbt_env_fs_teardown(&env, "fs0");
    mbt_env_stop(&env);
    printf("NFSv4.%u RDMA replay: %d failures\n", minor, failures);
    return failures ? 1 : 0;
} /* main */
