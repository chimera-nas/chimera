// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: Unlicense

/* Exercise the production TEST_STATEID handler and compound completion with
 * constrained request arenas. State lookup is deterministic here: the test
 * concerns admission before lookup and before dispatching a WRITE successor. */
#undef NDEBUG
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "nfs4_procs.h"
#include "nfs4_reply.h"

static unsigned validated;

nfsstat4
nfs_state_table_validate(
    struct nfs_state_table *table,
    const struct stateid4  *sid)
{
    (void) table;
    (void) sid;
    validated++;
    return NFS4ERR_BAD_STATEID;
} /* nfs_state_table_validate */

void
nfs4_change_finish(
    struct nfs4_change_table        *table,
    struct nfs4_change_observation **pending,
    bool                             accepted)
{
    (void) table;
    assert(accepted && *pending == NULL);
} /* nfs4_change_finish */

void
chimera_nfs4_compound_process(
    struct nfs_request *req,
    nfsstat4            status)
{
    (void) req;
    (void) status;
    /* The active dispatcher must receive its continuation through 'again'. */
    assert(0);
} /* chimera_nfs4_compound_process */

static void
check_stateid_reply(
    bool     rdma,
    uint32_t count,
    uint64_t remaining,
    bool     success)
{
    struct chimera_server_nfs_shared *shared = calloc(1, sizeof(*shared));
    struct chimera_server_nfs_thread  thread = { .shared = shared, .active = 1 };
    struct evpl_rpc2_conn             conn   = { .rdma = rdma };
    xdr_dbuf                          dbuf;
    struct evpl_rpc2_encoding         encoding = { .dbuf = &dbuf };
    struct nfs_argop4                 args[2]  = { { .argop = OP_TEST_STATEID }, { .argop = OP_WRITE } };
    struct nfs_resop4                 res[2]   = { { .resop = OP_TEST_STATEID }, { .resop = OP_WRITE } };
    struct COMPOUND4args              compound = { .num_argarray = 2, .argarray = args };
    struct nfs_request                req      = {
        .thread        = &thread,
        .conn          = &conn,
        .encoding      = &encoding,
        .args_compound = &compound,
        .res_compound  = { .num_resarray = 2, .resarray = res },
    };
    struct stateid4                   ids[512] = { 0 };

    assert(shared);
    xdr_dbuf_init(&dbuf, 128 * 1024);
    assert(dbuf.buffer && remaining <= (uint64_t) dbuf.size);
    dbuf.used = dbuf.size - remaining;
    int                               mark = dbuf.used;
    args[0].optest_stateid.num_ts_stateids = count;
    args[0].optest_stateid.ts_stateids     = ids;
    validated                              = 0;

    chimera_nfs4_test_stateid(&thread, &req, &args[0], &res[0]);

    assert(thread.again);
    if (success) {
        assert(count <= sizeof(ids) / sizeof(ids[0]));
        assert(res[0].optest_stateid.tsr_status == NFS4_OK);
        assert(res[0].optest_stateid.tsr_resok4.num_tsr_status_codes == count);
        assert(validated == count);
        for (uint32_t i = 0; i < count; i++) {
            assert(res[0].optest_stateid.tsr_resok4.tsr_status_codes[i] == NFS4ERR_BAD_STATEID);
        }
        assert(req.res_compound.num_resarray == 2 && req.index == 0);
    } else {
        assert(res[0].optest_stateid.tsr_status == NFS4ERR_REP_TOO_BIG);
        assert(res[0].optest_stateid.tsr_resok4.num_tsr_status_codes == 0);
        assert(res[0].optest_stateid.tsr_resok4.tsr_status_codes == NULL);
        assert(validated == 0 && dbuf.used == mark);
        /* Production completion truncates the result before the WRITE and
         * advances past that terminal result, so it cannot run the suffix. */
        assert(req.res_compound.num_resarray == 1 && req.index == 1);
    }

    if (remaining >= chimera_nfs4_reply_transport_bytes(&req)) {
        /* Allocate the actual generated adapter's array after completion.
         * RDMA must additionally retain the bounded cursor-move view space. */
        uint64_t reserve = chimera_nfs4_reply_transport_bytes(&req);
        assert(xdr_dbuf_alloc_space(CHIMERA_NFS4_REPLY_MAX_IOV * sizeof(struct evpl_iovec), &dbuf));
        uint64_t views = reserve - CHIMERA_NFS4_REPLY_MAX_IOV * sizeof(struct evpl_iovec);
        assert((uint64_t) (dbuf.size - dbuf.used) >= views);
    }
    xdr_dbuf_destroy(&dbuf);
    free(shared);
} /* check_stateid_reply */

static void
check_wire_budget(void)
{
    struct evpl_rpc2_encoding encoding = { .reply_overhead = 24 };
    struct nfs4_session       session  = { .nfs4_session_fore_attrs = {
                                               .ca_maxresponsesize        = 4096, .ca_maxresponsesize_cached = 1024
                                           } };
    struct nfs_argop4         ops[] = { { .argop = OP_SEQUENCE }, { .argop = OP_READ }, { .argop = OP_SETATTR } };
    struct COMPOUND4args      args  = { .argarray = ops, .num_argarray = 3 };
    struct nfs_request        req   = {
        .encoding      = &encoding,
        .session       = &session,
        .seen_sequence = true,
        .args_compound = &args,
    };

    /* Reserve SETATTR's 12-byte failure, including its empty bitmap. */
    assert(chimera_nfs4_reply_check(&req, 1, 4060, 0, true) == NFS4_OK);
    assert(chimera_nfs4_reply_check(&req, 1, 4064, 0, true) == NFS4ERR_REP_TOO_BIG);
    ops[0].opsequence.sa_cachethis = true;
    assert(chimera_nfs4_reply_check(&req, 1, 988, 0, true) == NFS4_OK);
    assert(chimera_nfs4_reply_check(&req, 1, 992, 0, true) == NFS4ERR_REP_TOO_BIG_TO_CACHE);
    /* Write-chunk reduction does not reduce negotiated or cached wire size. */
    assert(chimera_nfs4_reply_check(&req, 1, 992, 800, true) == NFS4ERR_REP_TOO_BIG_TO_CACHE);
    ops[0].opsequence.sa_cachethis = false;
    encoding.reply_chunk_present   = true;
    encoding.reply_chunk_capacity  = 1024;
    assert(chimera_nfs4_reply_check(&req, 1, 2012, 1024, true) == NFS4_OK);
    assert(chimera_nfs4_reply_check(&req, 1, 2016, 1024, true) == NFS4ERR_RESOURCE);
    encoding.reply_chunk_capacity = 0;
    assert(chimera_nfs4_reply_check(&req, 1, 1500, 1024, true) == NFS4_OK);
    assert(chimera_nfs4_reply_check(&req, 1, 1504, 1024, true) == NFS4ERR_RESOURCE);
    /* The transport supplies a conservative security-wrapping bound. */
    encoding.reply_chunk_present = false;
    encoding.reply_overhead      = 24 + 524;
    assert(chimera_nfs4_reply_check(&req, 1, 3536, 0, true) == NFS4_OK);
    assert(chimera_nfs4_reply_check(&req, 1, 3540, 0, true) == NFS4ERR_REP_TOO_BIG);
    /* CREATE_SESSION later in a compound must not replace its owning limits. */
    struct nfs4_session new_session = { .nfs4_session_fore_attrs = { .ca_maxresponsesize = 65536 } };
    req.replay_session = &session;
    req.session        = &new_session;
    assert(chimera_nfs4_reply_check(&req, 1, 3540, 0, true) == NFS4ERR_REP_TOO_BIG);
} /* check_wire_budget */

int
main(void)
{
    struct evpl_rpc2_conn conn = { .rdma = 1 };
    struct nfs_request    req  = { .conn = &conn };
    uint64_t              rdma = chimera_nfs4_reply_transport_bytes(&req);

    conn.rdma = 0;
    uint64_t              tcp = chimera_nfs4_reply_transport_bytes(&req);

    /* Exactly fitting results, including 8-byte arena alignment. */
    check_stateid_reply(false, 3, tcp + 16, true);
    check_stateid_reply(true, 3, rdma + 16, true);
    check_stateid_reply(false, 3, tcp + 8, false);
    check_stateid_reply(true, 3, rdma + 8, false);
    check_stateid_reply(false, 0, tcp, true);
    check_stateid_reply(true, 0, rdma, true);

    /* The former 8192-byte floor is insufficient for RDMA; for TCP it also
     * needlessly rejects small fitting payloads. */
    assert(tcp < 8192 && rdma > 8192);
    check_stateid_reply(false, 256, 8192, true);
    check_stateid_reply(true, 3, 8192, false);
    check_stateid_reply(true, UINT32_MAX, 128 * 1024, false);
    check_wire_budget();
    return 0;
} /* main */
