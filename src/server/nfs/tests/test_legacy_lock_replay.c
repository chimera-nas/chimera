// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Drive the real legacy LOCK, LOCKU, and OPEN_CONFIRM procedures with real state.
 * No backend, RPC loop, or range surgery is needed to exercise its owner
 * sequencing and consumed-error completion paths. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "nfs4_procs.h"
#include "nfs4_state.h"

/* Control the already separately tested recovery policy to make call ordering
 * observable while exercising the real LOCK procedure and owner journals. */
static nfsstat4           recovery_result;
static int                recovery_calls;
static struct nfs_client *recovery_client;

nfsstat4
nfs_recovery_open_check(
    struct nfs_recovery     *recovery,
    const struct nfs_client *client,
    bool                     reclaim)
{
    (void) recovery;
    (void) reclaim;
    if (client != recovery_client) {
        abort();
    }
    recovery_calls++;
    return recovery_result;
} /* nfs_recovery_open_check */

#define CHECK(c) do { if (!(c)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #c); abort(); \
                      } } while (0)

/* The fixture sets thread.active and has no range leases, so neither of
 * these unrelated, library-private continuation paths should be reached. */
void
chimera_nfs4_compound_process(
    struct nfs_request *req,
    nfsstat4            status)
{
    (void) req;
    (void) status;
    abort();
} /* chimera_nfs4_compound_process */

struct nfs4_range_lease *
nfs4_range_lease_insert(
    struct chimera_vfs_state         *vfs_state,
    struct nfs_lock_state            *lock_state,
    struct chimera_vfs_file_state    *file_state,
    const struct chimera_claim_owner *owner,
    bool                              exclusive,
    uint64_t                          start,
    uint64_t                          end,
    void                             *cb_private)
{
    (void) vfs_state;
    (void) lock_state;
    (void) file_state;
    (void) owner;
    (void) exclusive;
    (void) start;
    (void) end;
    (void) cb_private;
    abort();
} /* nfs4_range_lease_insert */

void
nfs4_range_lease_free(
    struct chimera_vfs_state *state,
    struct nfs4_range_lease  *lease)
{
    (void) state;
    (void) lease;
    abort();
} /* nfs4_range_lease_free */

void
nfs4_fill_denied_owner(
    struct nfs4_client_table                *table,
    const struct chimera_vfs_claim_conflict *conflict,
    struct state_owner4                     *owner,
    struct xdr_dbuf                         *dbuf)
{
    (void) table;
    (void) conflict;
    (void) owner;
    (void) dbuf;
    abort();
} /* nfs4_fill_denied_owner */

static struct LOCKU4res
unlock(
    struct chimera_server_nfs_thread *thread,
    struct stateid4                   sid,
    uint32_t                          seqid,
    uint64_t                          offset,
    uint64_t                          length)
{
    struct nfs_request   req  = { .thread = thread, .minorversion = 0, .fhlen = 1 };
    struct nfs_argop4    arg  = { .argop = OP_LOCKU };
    struct nfs_resop4    res  = { .resop = OP_LOCKU };
    struct COMPOUND4args args = { .argarray = &arg, .num_argarray = 1 };

    req.args_compound             = &args;
    req.res_compound.resarray     = &res;
    req.res_compound.num_resarray = 1;
    arg.oplocku.lock_stateid      = sid;
    arg.oplocku.seqid             = seqid;
    arg.oplocku.locktype          = WRITE_LT;
    arg.oplocku.offset            = offset;
    arg.oplocku.length            = length;
    thread->again                 = 0;
    chimera_nfs4_locku(thread, &req, &arg, &res);
    CHECK(thread->again);
    CHECK(req.nfs_state_ref == NULL);
    return res.oplocku;
} /* unlock */

static struct OPEN_CONFIRM4res
confirm(
    struct chimera_server_nfs_thread *thread,
    struct stateid4                   sid,
    uint32_t                          seqid,
    uint8_t                           fh)
{
    struct nfs_request   req  = { .thread = thread, .minorversion = 0, .fhlen = 1 };
    struct nfs_argop4    arg  = { .argop = OP_OPEN_CONFIRM };
    struct nfs_resop4    res  = { .resop = OP_OPEN_CONFIRM };
    struct COMPOUND4args args = { .argarray = &arg, .num_argarray = 1 };

    req.fh[0]                       = fh;
    req.args_compound               = &args;
    req.res_compound.resarray       = &res;
    req.res_compound.num_resarray   = 1;
    arg.opopen_confirm.open_stateid = sid;
    arg.opopen_confirm.seqid        = seqid;
    thread->again                   = 0;
    chimera_nfs4_open_confirm(thread, &req, &arg, &res);
    CHECK(thread->again);
    return res.opopen_confirm;
} /* confirm */

static struct LOCK4res
lock_request(
    struct chimera_server_nfs_thread *thread,
    struct stateid4                   sid,
    uint32_t                          seqid,
    struct nfs_client                *new_client)
{
    struct nfs_request   req  = { .thread = thread, .minorversion = 0, .fhlen = 1 };
    struct nfs_argop4    arg  = { .argop = OP_LOCK };
    struct nfs_resop4    res  = { .resop = OP_LOCK };
    struct COMPOUND4args args = { .argarray = &arg, .num_argarray = 1 };

    req.fh[0]                        = 1;
    req.args_compound                = &args;
    req.res_compound.resarray        = &res;
    req.res_compound.num_resarray    = 1;
    arg.oplock.locktype              = WRITE_LT;
    arg.oplock.length                = 1;
    arg.oplock.reclaim               = true;
    arg.oplock.locker.new_lock_owner = new_client != NULL;
    if (new_client) {
        arg.oplock.locker.open_owner.open_seqid            = seqid;
        arg.oplock.locker.open_owner.open_stateid          = sid;
        arg.oplock.locker.open_owner.lock_seqid            = 5;
        arg.oplock.locker.open_owner.lock_owner.clientid   = new_client->client_id;
        arg.oplock.locker.open_owner.lock_owner.owner.data = "reclaim";
        arg.oplock.locker.open_owner.lock_owner.owner.len  = 7;
    } else {
        arg.oplock.locker.lock_owner.lock_seqid   = seqid;
        arg.oplock.locker.lock_owner.lock_stateid = sid;
    }
    thread->again = 0;
    chimera_nfs4_lock(thread, &req, &arg, &res);
    CHECK(thread->again && !req.nfs_state_ref);
    CHECK(!req.lock_4_0_open_owner && !req.lock_4_0_lock_owner);
    return res.oplock;
} /* lock_request */

int
main(void)
{
    struct chimera_server_nfs_shared *shared = calloc(1, sizeof(*shared));
    struct chimera_vfs                vfs    = { 0 };
    struct chimera_server_nfs_thread  thread = {
        .shared = shared, .vfs = &vfs, .active = 1
    };
    struct nfs_client                *client;
    struct nfs_open_owner            *oo;
    struct nfs_open_state            *os;
    struct nfs_lock_owner            *lo;
    struct nfs_lock_state            *ls;
    struct stateid4                   open_sid, sid, old, future;
    struct LOCKU4res                  res;
    uint8_t                           fh = 1;
    bool                              created;

    CHECK(shared);
    nfs_state_table_init(&shared->nfs4_state_table, 1);
    client = nfs_client_alloc(91, "locku-client", 12, 123, 0);
    oo     = nfs_open_owner_find_or_create(client, "open", 4, &created);
    os     = nfs_open_state_create(oo, 0, NULL, 0, &fh, 1,
                                   OPEN4_SHARE_ACCESS_BOTH, OPEN4_SHARE_DENY_NONE, NULL,
                                   &shared->nfs4_state_table, &open_sid);
    CHECK(os);
    lo = nfs_lock_owner_find_or_create(client, "lock", 4, &created);
    ls = nfs_lock_state_create(lo, os, NULL, &shared->nfs4_state_table, &sid);
    CHECK(ls);
    ls->seqid = sid.seqid = 3;
    lo->seqid = 10;
    nfs4_replay_record(&lo->replay, 10, OP_LOCKU, NFS4_OK, &sid);

    /* OLD_STATEID consumes the accepted owner seqid without changing ranges
     * or the lock state's version; retransmission returns that same error. */
    old = sid;
    old.seqid--;
    res = unlock(&thread, old, 11, 0, 1);
    CHECK(res.status == NFS4ERR_OLD_STATEID);
    CHECK(lo->seqid == 11 && lo->replay.status == NFS4ERR_OLD_STATEID);
    CHECK(ls->seqid == 3 && ls->range_leases == NULL);
    res = unlock(&thread, old, 11, 0, 1);
    CHECK(res.status == NFS4ERR_OLD_STATEID && lo->seqid == 11);

    res = unlock(&thread, sid, 12, 0, 0);
    CHECK(res.status == NFS4ERR_INVAL && lo->seqid == 12);
    res = unlock(&thread, sid, 12, 0, 0);
    CHECK(res.status == NFS4ERR_INVAL && ls->seqid == 3);
    res = unlock(&thread, sid, 13, UINT64_MAX - 1, 3);
    CHECK(res.status == NFS4ERR_INVAL && lo->seqid == 13);

    /* BAD_STATEID does not consume; the corrected request can reuse 14. */
    future = sid;
    future.seqid++;
    res = unlock(&thread, future, 14, 0, 1);
    CHECK(res.status == NFS4ERR_BAD_STATEID && lo->seqid == 13);
    res = unlock(&thread, sid, 14, 0, 1);
    CHECK(res.status == NFS4_OK && lo->seqid == 14 && ls->seqid == 4);
    CHECK(res.lock_stateid.seqid == 4);
    res = unlock(&thread, sid, 14, 0, 1);
    CHECK(res.status == NFS4_OK && res.lock_stateid.seqid == 4 && ls->seqid == 4);

    /* Equal seqids are not a replay of a different owner-sequenced op. */
    nfs4_replay_record(&lo->replay, 14, OP_LOCK, NFS4_OK, &sid);
    res = unlock(&thread, sid, 14, 0, 1);
    CHECK(res.status == NFS4ERR_BAD_SEQID && lo->seqid == 14 && ls->seqid == 4);

    /* Exercise the actual fallback OPEN_CONFIRM procedure, including the
     * consuming OLD_STATEID path that previously left its owner one behind. */
    struct nfs_open_owner  *confirm_owner = nfs_open_owner_find_or_create(client, "confirm", 7, &created);
    struct stateid4         confirm_sid;
    struct nfs_open_state  *confirm_state = nfs_open_state_create(confirm_owner, 0, NULL, 0, &fh, 1,
                                                                  OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE, NULL,
                                                                  &shared->nfs4_state_table, &confirm_sid);
    CHECK(confirm_state);
    confirm_state->seqid = confirm_sid.seqid = 3;
    confirm_owner->seqid = 20;
    nfs4_replay_record(&confirm_owner->replay, 20, OP_OPEN, NFS4_OK, &confirm_sid);
    struct OPEN_CONFIRM4res confirmed = confirm(&thread, confirm_sid, 20, fh);
    CHECK(confirmed.status == NFS4ERR_BAD_SEQID && !confirm_owner->confirmed);
    old = confirm_sid;
    old.seqid--;
    confirmed = confirm(&thread, old, 21, fh);
    CHECK(confirmed.status == NFS4ERR_OLD_STATEID && confirm_owner->seqid == 21);
    CHECK(confirm_owner->replay.op == OP_OPEN_CONFIRM && !confirm_owner->confirmed && confirm_state->seqid == 3);
    confirmed = confirm(&thread, old, 21, fh);
    CHECK(confirmed.status == NFS4ERR_OLD_STATEID && confirm_owner->seqid == 21);
    future = confirm_sid;
    future.seqid++;
    confirmed = confirm(&thread, future, 22, fh);
    CHECK(confirmed.status == NFS4ERR_BAD_STATEID && confirm_owner->seqid == 21);
    confirmed = confirm(&thread, confirm_sid, 22, fh + 1);
    CHECK(confirmed.status == NFS4ERR_BAD_STATEID && confirm_owner->seqid == 21);
    confirmed = confirm(&thread, confirm_sid, 22, fh);
    CHECK(confirmed.status == NFS4_OK && confirm_owner->confirmed && confirm_state->seqid == 4);
    CHECK(confirmed.resok4.open_stateid.seqid == 4 && confirm_owner->seqid == 22);
    confirmed = confirm(&thread, confirm_sid, 22, fh);
    CHECK(confirmed.status == NFS4_OK && confirmed.resok4.open_stateid.seqid == 4 && confirm_state->seqid == 4);
    confirmed = confirm(&thread, confirmed.resok4.open_stateid, 23, fh);
    CHECK(confirmed.status == NFS4ERR_BAD_STATEID && confirm_owner->seqid == 22);

    recovery_client   = client;
    recovery_result   = NFS4ERR_NO_GRACE;
    confirm_sid.seqid = confirm_state->seqid;
    struct LOCK4res        locked = lock_request(&thread, confirm_sid, 23, client);
    CHECK(locked.status == NFS4ERR_NO_GRACE && confirm_owner->seqid == 23 && recovery_calls == 1);
    struct nfs_lock_owner *reclaim_owner = nfs_lock_owner_find_or_create(client, "reclaim", 7, &created);
    CHECK(!created && reclaim_owner->seqid == 5 && !reclaim_owner->states);
    CHECK(reclaim_owner->replay.op == OP_LOCK && reclaim_owner->replay.status == NFS4ERR_NO_GRACE);
    recovery_result = NFS4ERR_GRACE;
    locked          = lock_request(&thread, confirm_sid, 23, client);
    CHECK(locked.status == NFS4ERR_NO_GRACE && recovery_calls == 1);
    nfs4_replay_record(&confirm_owner->replay, 23, OP_CLOSE, NFS4_OK, &confirm_sid);
    locked = lock_request(&thread, confirm_sid, 23, client);
    CHECK(locked.status == NFS4ERR_BAD_SEQID && recovery_calls == 1);
    nfs_lock_owner_put(reclaim_owner);

    sid.seqid = ls->seqid;
    locked    = lock_request(&thread, sid, 15, NULL);
    CHECK(locked.status == NFS4ERR_GRACE && lo->seqid == 15 && recovery_calls == 2);
    recovery_result = NFS4ERR_NO_GRACE;
    locked          = lock_request(&thread, sid, 15, NULL);
    CHECK(locked.status == NFS4ERR_GRACE && recovery_calls == 2);
    locked = lock_request(&thread, sid, 16, NULL);
    CHECK(locked.status == NFS4ERR_NO_GRACE && lo->seqid == 16 && recovery_calls == 3);
    recovery_result = NFS4ERR_GRACE;
    old             = sid;
    old.seqid--;
    locked = lock_request(&thread, old, 17, NULL);
    CHECK(locked.status == NFS4ERR_GRACE && lo->seqid == 17 && ls->seqid == 4);
    nfs_open_owner_put(confirm_owner);

    nfs_lock_owner_put(lo);
    nfs_open_owner_put(oo);
    nfs_client_destroy(client, &shared->nfs4_state_table, NULL, true);
    nfs_state_table_free(&shared->nfs4_state_table, NULL);
    free(shared);
    puts("ok: legacy LOCKU and OPEN_CONFIRM consumed errors and typed replay");
    return 0;
} /* main */
