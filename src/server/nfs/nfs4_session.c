// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <uuid/uuid.h>
#include "prometheus-c.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs4_callback.h"
#include "nfs4_drc.h"
#include "nfs_internal.h"
#include "nfs_nlm_state.h"
#include "nfs_common.h"
#include "evpl/evpl_rpc2.h"
#include "vfs/vfs_release.h"

/* Bump a replay-cache counter on the shared struct.  Safe when the
 * thread/shared are not set up (e.g. in the unit test). */
static inline void
nfs4_replay_metric_inc(
    struct nfs_request *req,
    struct prometheus_counter_instance *(*field)(struct nfs4_replay_metrics *))
{
    struct nfs4_replay_metrics         *rm;
    struct prometheus_counter_instance *inst;

    if (!req || !req->thread || !req->thread->shared) {
        return;
    }
    rm   = &req->thread->shared->replay_metrics;
    inst = field(rm);
    if (inst) {
        prometheus_counter_increment(inst);
    }
} /* nfs4_replay_metric_inc */

/* Adjust the replay-cache bytes gauge.  Safe when not set up. */
static inline void
nfs4_replay_bytes_delta(
    struct nfs_request *req,
    int64_t             delta)
{
    struct prometheus_gauge_instance *g;

    if (!req || !req->thread || !req->thread->shared) {
        return;
    }
    g = req->thread->shared->replay_metrics.bytes_in_use;
    if (g) {
        prometheus_gauge_add(g, delta);
    }
} /* nfs4_replay_bytes_delta */

static struct prometheus_counter_instance *
replay_field_hit(struct nfs4_replay_metrics *rm)
{
    return rm->hit;
} /* replay_field_hit */
static struct prometheus_counter_instance *
replay_field_seq_misordered(struct nfs4_replay_metrics *rm)
{
    return rm->seq_misordered;
} /* replay_field_seq_misordered */
static struct prometheus_counter_instance *
replay_field_bad_slot(struct nfs4_replay_metrics *rm)
{
    return rm->bad_slot;
} /* replay_field_bad_slot */
static struct prometheus_counter_instance *
replay_field_retry_uncached(struct nfs4_replay_metrics *rm)
{
    return rm->retry_uncached;
} /* replay_field_retry_uncached */

void
nfs4_client_table_init(
    struct nfs4_client_table *table,
    uint16_t                  node_id)
{
    table->nfs4_ct_clients_by_owner = NULL;
    table->nfs4_ct_clients_by_id    = NULL;
    table->nfs4_ct_sessions         = NULL;
    table->nfs4_ct_next_client_id   = 1;
    table->nfs4_ct_node_id          = node_id;
    table->nfs4_ct_next_confirm     = 1;

    pthread_mutex_init(&table->nfs4_ct_lock, NULL);
} /* nfs4_client_table_init */

void
nfs4_client_table_free(struct nfs4_client_table *table)
{

#ifndef __clang_analyzer__

    struct nfs4_client  *cur, *tmp;
    struct nfs4_session *sess, *sesstmp;


    /* HASH_DEL blows clangs mind so we disable this block under analyzer */

    HASH_ITER(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id, cur, tmp)
    {
        HASH_DELETE(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner,
                    cur);
        HASH_DELETE(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id, cur);
        free(cur);
    }

    HASH_ITER(nfs4_session_hh, table->nfs4_ct_sessions, sess, sesstmp)
    {
        HASH_DELETE(nfs4_session_hh, table->nfs4_ct_sessions, sess);
        atomic_store_explicit(&sess->destroyed, true, memory_order_release);
        /* Drop the table's ref.  By this point in shutdown evpl_destroy()
         * has already drained all conns and fired NOTIFY_DISCONNECTED for
         * each, so no conns hold refs and refcount is exactly 1 -- the
         * session is freed here. */
        nfs4_session_put(sess);
    }

#endif /* ifndef __clang_analyzer__ */

} /* nfs4_client_table_free */

uint64_t
nfs4_client_register(
    struct nfs4_client_table *table,
    const void               *owner,
    int                       owner_len,
    uint64_t                  verifier,
    uint32_t                  proto,
    const char               *nii_domain,
    const char               *nii_name)
{
    struct nfs4_client *client;
    uint64_t            id;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner,
              owner, owner_len, client);

    if (client) {
        if (verifier != client->nfs4_client_verifier) {
            /* XXX handle this */
            chimera_nfs_info("Client has a new verifier!");
        }
    } else {
        client = calloc(1, sizeof(*client));

        client->nfs4_client_id = nfs4_make_clientid(table->nfs4_ct_node_id,
                                                    table->nfs4_ct_next_client_id++);

        memcpy(client->nfs4_client_owner, owner, owner_len);
        client->nfs4_client_owner_len = owner_len;
        client->nfs4_client_verifier  = verifier;
        client->nfs4_client_refcnt    = 1;

        /* Bring up the unified state hierarchy in lockstep with the
         * registration record.  minor is encoded as proto/40 today
         * (4.0 only path uses different proto values); pass 0 here and
         * let Phase 4 thread the real minor through. */
        client->unified = nfs_client_alloc(client->nfs4_client_id,
                                           owner, owner_len, verifier, 0);

        if (nii_domain) {
            snprintf(client->nfs4_client_domain,
                     sizeof(client->nfs4_client_domain), "%s", nii_domain);
        } else {
            strncpy(client->nfs4_client_domain, "unidentified",
                    NFS4_OPAQUE_LIMIT);
        }

        if (nii_name) {
            snprintf(client->nfs4_client_name,
                     sizeof(client->nfs4_client_name),
                     "%s", nii_name);
        } else {
            strncpy(client->nfs4_client_name, "unidentified", NFS4_OPAQUE_LIMIT)
            ;
        }


        HASH_ADD(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner,
                 nfs4_client_owner, client->nfs4_client_owner_len, client);
        HASH_ADD(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
                 nfs4_client_id, sizeof(client->nfs4_client_id), client);

        chimera_nfs_info("NFS4 Registered new client %lu (%s %s)",
                         client->nfs4_client_id,
                         client->nfs4_client_domain,
                         client->nfs4_client_name);
    }

    id = client->nfs4_client_id;

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    return id;
} /* nfs4_client_register */

static int
nfs4_principal_matches(
    const struct nfs4_client           *c,
    const struct nfs4_client_principal *p)
{
    if (c->nfs4_client_princ_flavor != p->flavor ||
        c->nfs4_client_princ_uid != p->uid ||
        c->nfs4_client_princ_gid != p->gid ||
        c->nfs4_client_princ_mach_len != p->machinename_len) {
        return 0;
    }
    return p->machinename_len == 0 ||
           memcmp(c->nfs4_client_princ_mach, p->machinename,
                  p->machinename_len) == 0;
} /* nfs4_principal_matches */

/* Caller must hold table->nfs4_ct_lock.  Allocates a fresh (unconfirmed)
 * client record, brings up its unified state hierarchy, and inserts it into
 * both hash tables. */
static struct nfs4_client *
nfs4_client_new_locked(
    struct nfs4_client_table           *table,
    const void                         *owner,
    int                                 owner_len,
    uint64_t                            verifier,
    const struct nfs4_client_principal *p,
    uint8_t                             minorversion,
    bool                                add_to_owner)
{
    struct nfs4_client *c = calloc(1, sizeof(*c));

    c->nfs4_client_id = nfs4_make_clientid(table->nfs4_ct_node_id,
                                           table->nfs4_ct_next_client_id++);
    c->nfs4_client_owner_len     = owner_len;
    c->nfs4_client_verifier      = verifier;
    c->nfs4_client_refcnt        = 1;
    c->nfs4_client_confirmed     = 0;
    c->nfs4_client_supersedes_id = 0;
    memcpy(c->nfs4_client_owner, owner, owner_len);

    c->nfs4_client_princ_flavor   = p->flavor;
    c->nfs4_client_princ_uid      = p->uid;
    c->nfs4_client_princ_gid      = p->gid;
    c->nfs4_client_princ_mach_len = p->machinename_len > NFS4_OPAQUE_LIMIT ?
        NFS4_OPAQUE_LIMIT : p->machinename_len;
    if (c->nfs4_client_princ_mach_len) {
        memcpy(c->nfs4_client_princ_mach, p->machinename,
               c->nfs4_client_princ_mach_len);
    }

    strncpy(c->nfs4_client_domain, "unidentified", NFS4_OPAQUE_LIMIT);
    strncpy(c->nfs4_client_name, "unidentified", NFS4_OPAQUE_LIMIT);

    c->unified = nfs_client_alloc(c->nfs4_client_id, owner, owner_len,
                                  verifier, minorversion);

    /* A by-id-only record (add_to_owner=false) is an unconfirmed SETCLIENTID
     * reboot record that coexists with a still-confirmed record holding the
     * by-owner slot; it joins the by-owner table when it is confirmed. */
    if (add_to_owner) {
        HASH_ADD(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner,
                 nfs4_client_owner, c->nfs4_client_owner_len, c);
    }
    HASH_ADD(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
             nfs4_client_id, sizeof(c->nfs4_client_id), c);

    return c;
} /* nfs4_client_new_locked */

/* Caller must hold table->nfs4_ct_lock.  Unhashes a record from both tables
 * and frees the bookkeeping struct, returning its unified state hierarchy for
 * the caller to tear down once the lock is dropped.  The record must be
 * present in both hash tables (i.e. not a superseded record, which has
 * already left the by-owner table). */
static struct nfs_client *
nfs4_client_remove_locked(
    struct nfs4_client_table *table,
    struct nfs4_client       *c)
{
    struct nfs_client *unified = c->unified;

    HASH_DELETE(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner, c);
    HASH_DELETE(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id, c);
    c->unified = NULL;
    free(c);
    return unified;
} /* nfs4_client_remove_locked */

/* Caller must hold table->nfs4_ct_lock. */
static int
nfs4_client_has_session_locked(
    struct nfs4_client_table *table,
    uint64_t                  client_id)
{
    struct nfs4_session *s, *tmp;

    HASH_ITER(nfs4_session_hh, table->nfs4_ct_sessions, s, tmp)
    {
        if (s->nfs4_session_clientid == client_id) {
            return 1;
        }
    }
    return 0;
} /* nfs4_client_has_session_locked */

void
nfs4_client_exchange_id(
    struct nfs4_client_table           *table,
    const void                         *owner,
    int                                 owner_len,
    uint64_t                            verifier,
    const struct nfs4_client_principal *principal,
    bool                                update,
    uint8_t                             minorversion,
    struct nfs4_exchange_id_result     *out)
{
    struct nfs4_client *existing, *nc;

    out->status          = NFS4_OK;
    out->clientid        = 0;
    out->confirmed       = 0;
    out->destroy_unified = NULL;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner,
              owner, owner_len, existing);

    if (update) {
        /* EXCHGID4_FLAG_UPD_CONFIRMED_REC_A: an update targets an existing
         * *confirmed* record (RFC 8881 §18.35.4 cases 6/8/9). */
        if (!existing || !existing->nfs4_client_confirmed) {
            out->status = NFS4ERR_NOENT;
        } else if (!nfs4_principal_matches(existing, principal)) {
            out->status = NFS4ERR_PERM;
        } else if (existing->nfs4_client_verifier != verifier) {
            out->status = NFS4ERR_NOT_SAME;
        } else {
            out->clientid  = existing->nfs4_client_id;
            out->confirmed = 1;
        }
        goto out_unlock;
    }

    if (!existing) {
        /* Case 1: brand-new owner. */
        nc = nfs4_client_new_locked(table, owner, owner_len,
                                    verifier, principal, minorversion, true);
        out->clientid = nc->nfs4_client_id;
        goto out_unlock;
    }

    if (!existing->nfs4_client_confirmed) {
        /* Case 4: an unconfirmed record is always replaced -- the client
         * never proved ownership of its clientid, so a fresh one is issued. */
        out->destroy_unified = nfs4_client_remove_locked(table, existing);
        nc                   = nfs4_client_new_locked(table, owner, owner_len,
                                                      verifier, principal, minorversion, true);
        out->clientid = nc->nfs4_client_id;
        goto out_unlock;
    }

    /* Confirmed record below. */
    if (nfs4_principal_matches(existing, principal)) {
        if (existing->nfs4_client_verifier == verifier) {
            /* Case 2: identical owner+verifier+principal -- return the same
             * clientid, leaving existing state intact. */
            out->clientid  = existing->nfs4_client_id;
            out->confirmed = 1;
            goto out_unlock;
        }

        /* Case 5: same principal, new boot verifier -- the client rebooted.
         * Issue a new clientid via a superseding unconfirmed record but keep
         * the old record (and its sessions) live until the new one is
         * confirmed at CREATE_SESSION. */
        HASH_DELETE(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner,
                    existing);
        nc = nfs4_client_new_locked(table, owner, owner_len, verifier,
                                    principal, minorversion, true);
        nc->nfs4_client_supersedes_id = existing->nfs4_client_id;
        out->clientid                 = nc->nfs4_client_id;
        goto out_unlock;
    }

    /* Confirmed record owned by a different principal (RFC 8881 §18.35.4
     * case 3).  If the old client still holds session state this is a
     * collision (CLID_INUSE); otherwise the stale record is replaced. */
    if (nfs4_client_has_session_locked(table, existing->nfs4_client_id)) {
        out->status = NFS4ERR_CLID_INUSE;
        goto out_unlock;
    }

    out->destroy_unified = nfs4_client_remove_locked(table, existing);
    nc                   = nfs4_client_new_locked(table, owner, owner_len,
                                                  verifier, principal, minorversion, true);
    out->clientid = nc->nfs4_client_id;

 out_unlock:
    pthread_mutex_unlock(&table->nfs4_ct_lock);
} /* nfs4_client_exchange_id */

/* Caller holds the table lock.  Remove a record that lives in the by-id table
 * only (an in-flight superseding SETCLIENTID record), returning its unified
 * hierarchy for teardown outside the lock. */
static struct nfs_client *
nfs4_client_remove_byid_locked(
    struct nfs4_client_table *table,
    struct nfs4_client       *c)
{
    struct nfs_client *unified = c->unified;

    HASH_DELETE(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id, c);
    c->unified = NULL;
    free(c);
    return unified;
} /* nfs4_client_remove_byid_locked */

/* Caller holds the table lock.  Stamp a fresh, unique setclientid_confirm
 * verifier onto a record and mark a confirm pending. */
static void
nfs4_scid_set_confirm(
    struct nfs4_client_table *table,
    struct nfs4_client       *c)
{
    uint64_t v = table->nfs4_ct_next_confirm++;

    memcpy(c->nfs4_client_scid_confirm, &v, NFS4_VERIFIER_SIZE);
    c->nfs4_client_scid_confirm_valid = 1;
} /* nfs4_scid_set_confirm */

/* Does the client hold any open/lock-owner state?  Used for the SETCLIENTID
 * CLID_INUSE rule (RFC 7530 §16.33.5). */
static int
nfs4_client_has_state(const struct nfs4_client *c)
{
    return c->unified &&
           (c->unified->open_owners_by_str != NULL ||
            c->unified->lock_owners_by_str != NULL);
} /* nfs4_client_has_state */

void
nfs4_client_setclientid(
    struct nfs4_client_table           *table,
    const void                         *owner,
    int                                 owner_len,
    uint64_t                            verifier,
    const struct nfs4_client_principal *principal,
    uint8_t                             minorversion,
    struct nfs4_setclientid_result     *out)
{
    struct nfs4_client *existing, *nc, *pending;

    out->status          = NFS4_OK;
    out->clientid        = 0;
    out->destroy_unified = NULL;
    memset(out->confirm, 0, NFS4_VERIFIER_SIZE);

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner,
              owner, owner_len, existing);

    if (!existing) {
        /* No record for this owner: create an unconfirmed one. */
        nc = nfs4_client_new_locked(table, owner, owner_len, verifier,
                                    principal, minorversion, true);
        nfs4_scid_set_confirm(table, nc);
        out->clientid = nc->nfs4_client_id;
        memcpy(out->confirm, nc->nfs4_client_scid_confirm, NFS4_VERIFIER_SIZE);
        goto out_unlock;
    }

    if (!existing->nfs4_client_confirmed) {
        /* The by-owner record is itself unconfirmed (no confirmed record for
         * this owner yet) -- RFC 7530 §16.33.5: any unconfirmed record is
         * replaced by a fresh one with a new clientid. */
        out->destroy_unified = nfs4_client_remove_locked(table, existing);
        nc                   = nfs4_client_new_locked(table, owner, owner_len,
                                                      verifier, principal,
                                                      minorversion, true);
        nfs4_scid_set_confirm(table, nc);
        out->clientid = nc->nfs4_client_id;
        memcpy(out->confirm, nc->nfs4_client_scid_confirm, NFS4_VERIFIER_SIZE);
        goto out_unlock;
    }

    /* A confirmed record exists for this owner. */

    /* Different principal while the confirmed client still holds state is a
     * collision (RFC 7530 §16.33.5 -- NFS4ERR_CLID_INUSE). */
    if (!nfs4_principal_matches(existing, principal) &&
        nfs4_client_has_state(existing)) {
        out->status = NFS4ERR_CLID_INUSE;
        goto out_unlock;
    }

    /* A new SETCLIENTID supersedes any earlier in-flight unconfirmed record
     * for this owner (RFC 7530 §16.33.5 cases 4c/4d/4e). */
    if (existing->nfs4_client_scid_pending_id) {
        HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
                  &existing->nfs4_client_scid_pending_id,
                  sizeof(existing->nfs4_client_scid_pending_id), pending);
        existing->nfs4_client_scid_pending_id = 0;
        if (pending) {
            out->destroy_unified = nfs4_client_remove_byid_locked(table,
                                                                  pending);
        }
    }

    if (nfs4_principal_matches(existing, principal) &&
        existing->nfs4_client_verifier == verifier) {
        /* Same verifier + principal: a callback-info update of the confirmed
         * record.  Same clientid, a fresh confirm verifier; the confirmed
         * record stays usable until (if ever) the update is confirmed. */
        nfs4_scid_set_confirm(table, existing);
        out->clientid = existing->nfs4_client_id;
        memcpy(out->confirm, existing->nfs4_client_scid_confirm,
               NFS4_VERIFIER_SIZE);
        goto out_unlock;
    }

    /* Different verifier (client reboot) or different principal with no state:
     * issue a new clientid via a superseding unconfirmed record kept in the
     * by-id table only.  The confirmed record remains live and usable until
     * this one is confirmed. */
    nc = nfs4_client_new_locked(table, owner, owner_len, verifier,
                                principal, minorversion, false);
    nc->nfs4_client_supersedes_id = existing->nfs4_client_id;
    nfs4_scid_set_confirm(table, nc);
    existing->nfs4_client_scid_pending_id = nc->nfs4_client_id;
    out->clientid                         = nc->nfs4_client_id;
    memcpy(out->confirm, nc->nfs4_client_scid_confirm, NFS4_VERIFIER_SIZE);

 out_unlock:
    pthread_mutex_unlock(&table->nfs4_ct_lock);
} /* nfs4_client_setclientid */

nfsstat4
nfs4_client_setclientid_confirm(
    struct nfs4_client_table *table,
    uint64_t                  clientid,
    const uint8_t            *confirm,
    struct nfs_client       **destroy_unified)
{
    struct nfs4_client *r, *old;
    nfsstat4            status;

    *destroy_unified = NULL;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &clientid, sizeof(clientid), r);

    if (!r) {
        status = NFS4ERR_STALE_CLIENTID;
        goto out_unlock;
    }

    if (!r->nfs4_client_scid_confirm_valid ||
        memcmp(confirm, r->nfs4_client_scid_confirm, NFS4_VERIFIER_SIZE) != 0) {
        /* The (clientid, verifier) pair does not name a record awaiting
         * confirmation (RFC 7530 §16.34.5). */
        status = NFS4ERR_STALE_STATEID;
        goto out_unlock;
    }

    if (!r->nfs4_client_confirmed) {
        /* Promote the unconfirmed record.  If it superseded a confirmed one
         * (client reboot), tear that record and its sessions down now and move
         * this record into the by-owner table. */
        if (r->nfs4_client_supersedes_id) {
            uint64_t oldid = r->nfs4_client_supersedes_id;

            r->nfs4_client_supersedes_id = 0;

            HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
                      &oldid, sizeof(oldid), old);
            if (old) {
                struct nfs4_session *s, *stmp;

                HASH_ITER(nfs4_session_hh, table->nfs4_ct_sessions, s, stmp)
                {
                    if (s->nfs4_session_clientid == oldid) {
                        HASH_DELETE(nfs4_session_hh, table->nfs4_ct_sessions, s);
                        atomic_store_explicit(&s->destroyed, true,
                                              memory_order_release);
                        nfs4_session_put(s);
                    }
                }

                old->nfs4_client_scid_pending_id = 0;
                HASH_DELETE(nfs4_client_hh_by_owner,
                            table->nfs4_ct_clients_by_owner, old);
                HASH_DELETE(nfs4_client_hh_by_id,
                            table->nfs4_ct_clients_by_id, old);
                *destroy_unified = old->unified;
                old->unified     = NULL;
                free(old);
            }

            HASH_ADD(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner,
                     nfs4_client_owner, r->nfs4_client_owner_len, r);
        }
        r->nfs4_client_confirmed = 1;
    }

    /* The confirm verifier stays valid so a retransmitted SETCLIENTID_CONFIRM
     * with the same {clientid, verifier} is idempotent (NFS4_OK); a later
     * SETCLIENTID overwrites it. */
    status = NFS4_OK;

 out_unlock:
    pthread_mutex_unlock(&table->nfs4_ct_lock);
    return status;
} /* nfs4_client_setclientid_confirm */

void
nfs4_client_create_session_classify(
    struct nfs4_client_table           *table,
    uint64_t                            client_id,
    uint32_t                            csa_sequence,
    const struct nfs4_client_principal *principal,
    struct nfs4_cs_classify            *out)
{
    struct nfs4_client *c;

    memset(out, 0, sizeof(*out));

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &client_id, sizeof(client_id), c);

    if (c && c->unified && !c->nfs4_client_confirmed &&
        (c->unified->expired ||
         atomic_load_explicit(&c->unified->courtesy, memory_order_acquire))) {
        out->action = NFS4_CS_ERROR;
        out->status = NFS4ERR_STALE_CLIENTID;
        goto out_unlock;
    }

    if (c && c->unified) {
        /* CREATE_SESSION is client liveness on a clientid the server still
         * holds.  If the lease sweep had marked the client courtesy-expired
         * (e.g. its EXCHANGE_ID -> CREATE_SESSION handshake stalled past the
         * lease under load), revive it rather than returning STALE_CLIENTID:
         * the clientid is valid and the client is plainly alive.  Clear
         * expired first so the renew (a no-op on an expired client) takes
         * hold, then renew so the window to the client's first SEQUENCE cannot
         * trip the sweep. */
        c->unified->expired = 0;
        nfs_client_touch(c->unified);
    }

    if (!c) {
        out->action = NFS4_CS_ERROR;
        out->status = NFS4ERR_STALE_CLIENTID;
    } else if (csa_sequence == c->nfs4_client_cs_seqid) {
        /* Retransmit of the last request, or -- before any request -- the
         * contrived (eir_sequenceid - 1) value (RFC 8881 §18.36.4). */
        if (c->nfs4_client_cs_have_reply) {
            out->action   = NFS4_CS_REPLAY;
            out->status   = c->nfs4_client_cs_reply_status;
            out->sequence = c->nfs4_client_cs_reply_sequence;
            out->flags    = c->nfs4_client_cs_reply_flags;
            out->fore     = c->nfs4_client_cs_reply_fore;
            out->back     = c->nfs4_client_cs_reply_back;
            memcpy(out->sessionid, c->nfs4_client_cs_reply_sessionid,
                   NFS4_SESSIONID_SIZE);
        } else {
            out->action = NFS4_CS_ERROR;
            out->status = NFS4ERR_SEQ_MISORDERED;
        }
    } else if (csa_sequence == c->nfs4_client_cs_seqid + 1) {
        out->action       = NFS4_CS_NEW;
        out->confirmed    = c->nfs4_client_confirmed;
        out->principal_ok = nfs4_principal_matches(c, principal);
    } else {
        out->action = NFS4_CS_ERROR;
        out->status = NFS4ERR_SEQ_MISORDERED;
    }

 out_unlock:
    pthread_mutex_unlock(&table->nfs4_ct_lock);
} /* nfs4_client_create_session_classify */

void
nfs4_client_create_session_cache(
    struct nfs4_client_table    *table,
    uint64_t                     client_id,
    uint32_t                     csa_sequence,
    nfsstat4                     status,
    const uint8_t               *sessionid,
    uint32_t                     sequence,
    uint32_t                     flags,
    const struct channel_attrs4 *fore,
    const struct channel_attrs4 *back)
{
    struct nfs4_client *c;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &client_id, sizeof(client_id), c);

    if (c) {
        c->nfs4_client_cs_seqid        = csa_sequence;
        c->nfs4_client_cs_have_reply   = 1;
        c->nfs4_client_cs_reply_status = status;

        if (status == NFS4_OK) {
            memcpy(c->nfs4_client_cs_reply_sessionid, sessionid,
                   NFS4_SESSIONID_SIZE);
            c->nfs4_client_cs_reply_sequence = sequence;
            c->nfs4_client_cs_reply_flags    = flags;
            c->nfs4_client_cs_reply_fore     = *fore;
            c->nfs4_client_cs_reply_back     = *back;
            /* Drop the borrowed ca_rdma_ird pointers; see header. */
            c->nfs4_client_cs_reply_fore.num_ca_rdma_ird = 0;
            c->nfs4_client_cs_reply_fore.ca_rdma_ird     = NULL;
            c->nfs4_client_cs_reply_back.num_ca_rdma_ird = 0;
            c->nfs4_client_cs_reply_back.ca_rdma_ird     = NULL;
        }
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);
} /* nfs4_client_create_session_cache */

bool
nfs4_client_mark_reclaim_complete(
    struct nfs4_client_table *table,
    uint64_t                  client_id)
{
    struct nfs4_client *c;
    bool                already = false;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &client_id, sizeof(client_id), c);

    if (c) {
        already                         = c->nfs4_client_reclaim_complete;
        c->nfs4_client_reclaim_complete = 1;
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    return already;
} /* nfs4_client_mark_reclaim_complete */

bool
nfs4_client_reclaim_complete(
    struct nfs4_client_table *table,
    uint64_t                  client_id)
{
    struct nfs4_client *c;
    bool                complete = false;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &client_id, sizeof(client_id), c);

    if (c) {
        complete = c->nfs4_client_reclaim_complete;
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    return complete;
} /* nfs4_client_reclaim_complete */

nfsstat4
nfs4_client_destroy_clientid(
    struct nfs4_client_table  *table,
    struct nfs_state_table    *state_table,
    struct chimera_vfs_thread *vfs_thread,
    uint64_t                   client_id)
{
    struct nfs4_client *c;
    struct nfs_client  *unified;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &client_id, sizeof(client_id), c);

    if (!c) {
        pthread_mutex_unlock(&table->nfs4_ct_lock);
        return NFS4ERR_STALE_CLIENTID;
    }

    /* RFC 8881 §18.50.3: a client that still owns sessions cannot be
     * destroyed -- the client must DESTROY_SESSION them first. */
    if (nfs4_client_has_session_locked(table, client_id)) {
        pthread_mutex_unlock(&table->nfs4_ct_lock);
        return NFS4ERR_CLIENTID_BUSY;
    }

    unified = nfs4_client_remove_locked(table, c);

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    if (unified) {
        nfs_client_destroy(unified, state_table, vfs_thread, false);
    }

    return NFS4_OK;
} /* nfs4_client_destroy_clientid */

bool
nfs4_client_confirm(
    struct nfs4_client_table *table,
    uint64_t                  client_id,
    struct nfs_client       **destroy_unified)
{
    struct nfs4_client *c, *old;

    *destroy_unified = NULL;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &client_id, sizeof(client_id), c);

    if (!c) {
        pthread_mutex_unlock(&table->nfs4_ct_lock);
        return false;
    }

    c->nfs4_client_confirmed = 1;

    if (c->nfs4_client_supersedes_id) {
        uint64_t oldid = c->nfs4_client_supersedes_id;

        c->nfs4_client_supersedes_id = 0;

        HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
                  &oldid, sizeof(oldid), old);

        if (old) {
            struct nfs4_session *s, *tmp;

            /* Tear down the superseded client's sessions so a stale-session
             * reference returns NFS4ERR_BADSESSION. */
            HASH_ITER(nfs4_session_hh, table->nfs4_ct_sessions, s, tmp)
            {
                if (s->nfs4_session_clientid == oldid) {
                    HASH_DELETE(nfs4_session_hh, table->nfs4_ct_sessions, s);
                    atomic_store_explicit(&s->destroyed, true,
                                          memory_order_release);
                    nfs4_session_put(s);
                }
            }

            /* The superseded record already left the by-owner table at reboot
            * time; drop it from by-id and hand its state out for teardown. */
            HASH_DELETE(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
                        old);
            *destroy_unified = old->unified;
            old->unified     = NULL;
            free(old);
        }
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);
    return true;
} /* nfs4_client_confirm */

void
nfs4_client_unregister(
    struct nfs4_client_table  *table,
    struct nfs_state_table    *state_table,
    struct chimera_vfs_thread *vfs_thread,
    uint64_t                   client_id)
{
    struct nfs4_client *client;
    struct nfs_client  *unified = NULL;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &client_id, sizeof(client_id), client);

    if (client) {
        chimera_nfs_info("NFS4 Unregistered client %lu", client_id);
        HASH_DELETE(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner,
                    client);
        HASH_DELETE(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id, client);
        unified         = client->unified;
        client->unified = NULL;
        free(client);
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    /* Tear down the unified state hierarchy outside the table lock so
     * nfs_client_destroy can take per-client / per-owner locks without
     * inversion against the table lock.  RFC 8881 §18.50.4 requires that
     * the client have no remaining state at DESTROY_CLIENTID time -- if
     * any survived, this releases their slots + dup'd VFS handles. */
    if (unified) {
        nfs_client_destroy(unified, state_table, vfs_thread, false);
    }
} /* nfs4_client_unregister */

struct nfs4_session *
nfs4_create_session(
    struct nfs4_client_table    *table,
    uint64_t                     client_id,
    uint32_t                     implicit,
    uint32_t                     replay_max_slots,
    uint32_t                     replay_maxresp_cached,
    const struct channel_attrs4 *fore_attrs,
    const struct channel_attrs4 *back_attrs,
    const uint8_t               *restore_sessionid)
{
    struct nfs4_client  *client  = NULL;
    struct nfs4_session *session = NULL;
    char                 session_id_str[80];

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &client_id, sizeof(client_id), client);

    if (client) {
        session = calloc(1, sizeof(*session));

        session->magic          = NFS4_SESSION_MAGIC;
        session->client_unified = client->unified;
        /* refcount: 1 for the hash table, 1 for the caller (so the session
         * cannot be destroyed by another thread before the caller binds it
         * to a conn). */
        atomic_init(&session->refcount, 2);
        atomic_init(&session->destroyed, false);

        /* Cold-start reload reconstructs a session with its original id so a
         * persistent client's retransmit on the old sessionid still resolves
         * (see nfs4_drc_reload); the live path mints a fresh one. */
        if (restore_sessionid) {
            memcpy(session->nfs4_session_id, restore_sessionid,
                   NFS4_SESSIONID_SIZE);
        } else {
            uuid_generate(session->nfs4_session_id);
        }

        session->nfs4_session_implicit = implicit;
        session->nfs4_session_clientid = client_id;

        /* NFS4.1 SEQUENCE replay cache slot table.  Implicit (NFS4.0)
         * sessions get zero slots -- v4.0 uses per-owner replay caches in
         * nfs_open_owner / nfs_lock_owner instead. */
        session->replay_max_slots      = implicit ? 0 : replay_max_slots;
        session->replay_maxresp_cached = replay_maxresp_cached;
        atomic_init(&session->replay_bytes_in_use, 0);
        session->replay_slots = NULL;

        if (session->replay_max_slots) {
            session->replay_slots = calloc(session->replay_max_slots,
                                           sizeof(*session->replay_slots));
        }

        if (fore_attrs) {
            session->nfs4_session_fore_attrs = *fore_attrs;
            if (fore_attrs->num_ca_rdma_ird) {
                session->nfs4_session_fore_attrs_rdma_ird    = fore_attrs->ca_rdma_ird[0];
                session->nfs4_session_fore_attrs.ca_rdma_ird = &session->nfs4_session_fore_attrs_rdma_ird;
            }

        }

        if (back_attrs) {
            session->nfs4_session_back_attrs = *back_attrs;
            if (back_attrs->num_ca_rdma_ird) {
                session->nfs4_session_back_attrs_rdma_ird    = back_attrs->ca_rdma_ird[0];
                session->nfs4_session_back_attrs.ca_rdma_ird = &session->nfs4_session_back_attrs_rdma_ird;
            }
        }

        HASH_ADD(nfs4_session_hh, table->nfs4_ct_sessions,
                 nfs4_session_id, NFS4_SESSIONID_SIZE, session);

    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    if (!client) {
        return NULL;
    }

    uuid_unparse(session->nfs4_session_id, session_id_str);

    chimera_nfs_info("NFS4 Created new session %s for client %lu",
                     session_id_str, client_id);

    return session;

} /* nfs4_create_session */

struct nfs4_session *
nfs4_session_lookup(
    struct nfs4_client_table *table,
    const void               *sessionid)
{
    struct nfs4_session *session = NULL;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_session_hh, table->nfs4_ct_sessions,
              sessionid, NFS4_SESSIONID_SIZE, session);

    if (session) {
        /* take a +1 ref for the caller while holding the table lock so a
         * concurrent nfs4_destroy_session cannot race the free. */
        atomic_fetch_add_explicit(&session->refcount, 1,
                                  memory_order_acq_rel);
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    return session;
} /* nfs4_session_lookup */

struct nfs4_session *
nfs4_session_find_by_clientid(
    struct nfs4_client_table *table,
    uint64_t                  client_id)
{
    struct nfs4_session *session = NULL;
    struct nfs4_session *cur, *tmp;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_ITER(nfs4_session_hh, table->nfs4_ct_sessions, cur, tmp)
    {
        if (cur->nfs4_session_clientid == client_id) {
            session = cur;
            break;
        }
    }

    if (session) {
        /* take a +1 ref for the caller while holding the table lock. */
        atomic_fetch_add_explicit(&session->refcount, 1,
                                  memory_order_acq_rel);
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    return session;
} /* nfs4_session_find_by_clientid */

void
nfs4_destroy_session(
    struct nfs4_client_table *table,
    const void               *session_id)
{
    struct nfs4_session *session = NULL;
    char                 session_id_str[80];

    uuid_unparse(session_id, session_id_str);

    chimera_nfs_info("NFS4 Destroying session %s", session_id_str);

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_session_hh, table->nfs4_ct_sessions, session_id,
              NFS4_SESSIONID_SIZE, session);

    if (session) {
        HASH_DELETE(nfs4_session_hh, table->nfs4_ct_sessions, session);
        atomic_store_explicit(&session->destroyed, true,
                              memory_order_release);
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    if (session) {
        /* Drop the hash table's ref.  The session is freed only when the
         * last conn that still caches it in private_data also drops its
         * ref (in the disconnect notify). */
        nfs4_session_put(session);
    }

} /* nfs4_destroy_session */

void
nfs4_client_table_destroy_unified(
    struct nfs4_client_table  *table,
    struct nfs_state_table    *state_table,
    struct chimera_vfs_thread *vfs_thread)
{
#ifndef __clang_analyzer__
    struct nfs4_client *cur, *tmp;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_ITER(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id, cur, tmp)
    {
        if (cur->unified) {
            nfs_client_destroy(cur->unified, state_table, vfs_thread, true);
            cur->unified = NULL;
        }
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);
#endif /* ifndef __clang_analyzer__ */
} /* nfs4_client_table_destroy_unified */

void
nfs4_session_get(struct nfs4_session *session)
{
    atomic_fetch_add_explicit(&session->refcount, 1, memory_order_acq_rel);
} /* nfs4_session_get */

void
nfs4_session_put(struct nfs4_session *session)
{
    uint32_t prev;

    prev = atomic_fetch_sub_explicit(&session->refcount, 1,
                                     memory_order_acq_rel);

    chimera_nfs_abort_if(prev == 0,
                         "nfs4_session_put: refcount underflow on session %p",
                         session);

    if (prev == 1) {
        /* Last ref -- safe to tear down. */
        if (session->replay_slots) {
            uint32_t i;
            for (i = 0; i < session->replay_max_slots; i++) {
                free(session->replay_slots[i].cached_buf);
            }
            free(session->replay_slots);
            session->replay_slots = NULL;
        }
        session->magic = 0;
        free(session);
    }
} /* nfs4_session_put */

void
nfs4_session_bind_conn(
    struct evpl_rpc2_conn *conn,
    struct nfs4_session   *session)
{
    void    *existing;
    uint32_t magic = 0;

    existing = evpl_rpc2_conn_get_private_data(conn);

    if (existing) {
        memcpy(&magic, existing, sizeof(magic));
    }

    if (magic == NFS4_SESSION_MAGIC) {
        if (existing == session) {
            /* Already bound to this session. */
            return;
        }
        /* Bound to a different session -- unbind the old one. */
        nfs4_session_put((struct nfs4_session *) existing);
    } else if (magic == NLM_CLIENT_MAGIC) {
        /* NLM and NFS4 should never share a conn (different ports). */
        chimera_nfs_abort(
            "nfs4_session_bind_conn: conn %p already holds an NLM client",
            conn);
    }

    nfs4_session_get(session);
    evpl_rpc2_conn_set_private_data(conn, session);
} /* nfs4_session_bind_conn */

void
nfs4_session_unbind_conn(struct evpl_rpc2_conn *conn)
{
    void                *existing;
    uint32_t             magic = 0;
    struct nfs4_session *session;

    existing = evpl_rpc2_conn_get_private_data(conn);

    if (!existing) {
        return;
    }

    memcpy(&magic, existing, sizeof(magic));

    if (magic != NFS4_SESSION_MAGIC) {
        return;
    }

    session = (struct nfs4_session *) existing;
    /* Drop the backchannel pointer if this was the conn carrying it, so a
     * later recall does not send on a dead connection. */
    if (session->nfs4_session_backchannel_conn == conn) {
        session->nfs4_session_backchannel_conn = NULL;
    }
    evpl_rpc2_conn_set_private_data(conn, NULL);
    nfs4_session_put(session);
} /* nfs4_session_unbind_conn */

/*
 * Reply-capture callback invoked from inside evpl_rpc2_send_reply, just
 * before the encoded reply iovecs are queued onto the wire (and the
 * request -- including the encoding's dbuf -- is freed).  We have one
 * chance to copy the bytes out for later replay.
 *
 * private_data is the struct nfs_request whose SEQUENCE handler armed
 * the capture (only when sa_cachethis was set and the slot is in
 * IN_PROGRESS for this request).
 *
 * If the reply exceeds the per-slot or per-session byte cap, the
 * capture is silently demoted: cached_buf stays NULL and finalize will
 * transition the slot to COMPLETED instead of CACHED.  A future retry
 * will then return NFS4ERR_RETRY_UNCACHED_REP, which RFC 5661 2.10.6.1
 * explicitly permits (the server may override sa_cachethis).
 */
static void
nfs4_replay_capture_reply(
    const struct evpl_iovec *iov,
    int                      niov,
    int                      total_length,
    void                    *private_data)
{
    struct nfs_request      *req     = private_data;
    struct nfs4_session     *session = req->session;
    struct nfs4_replay_slot *slot    = req->replay_slot;
    uint8_t                 *buf;
    size_t                   offset = 0;
    size_t                   total  = (size_t) total_length;
    size_t                   cur;
    int                      i;

    if (!slot || !session || total_length <= 0) {
        return;
    }

    if ((uint32_t) total_length > session->replay_maxresp_cached) {
        return;
    }

    /* Reserve against the per-session byte cap with a CAS loop -- no lock.
     * The slot is IN_PROGRESS for this (owning) thread, so no other thread
     * touches slot->cached_buf concurrently; only the session-wide counter is
     * shared. */
    cur = atomic_load_explicit(&session->replay_bytes_in_use, memory_order_relaxed);
    do {
        if (cur + total > NFS4_MAX_REPLY_CACHE_BYTES) {
            return;  /* demote: over the per-session cap */
        }
    } while (!atomic_compare_exchange_weak_explicit(
                 &session->replay_bytes_in_use, &cur, cur + total,
                 memory_order_relaxed, memory_order_relaxed));

    buf = malloc(total_length);
    if (!buf) {
        atomic_fetch_sub_explicit(&session->replay_bytes_in_use, total,
                                  memory_order_relaxed);
        return;
    }

    /* Concatenate iovecs into a single contiguous buffer.  Loses
     * zerocopy on replay; acceptable since cachethis=true is rare for
     * iovec-heavy ops (READ/READLINK) and replay is rare in general. */
    for (i = 0; i < niov; i++) {
        memcpy(buf + offset, iov[i].data, iov[i].length);
        offset += iov[i].length;
    }

    /* Same thread runs finalize next, which release-publishes these via the
     * state_word store -- a later reader that observes CACHED sees them. */
    slot->cached_buf = buf;
    slot->cached_len = total_length;

    nfs4_replay_bytes_delta(req, total_length);
} /* nfs4_replay_capture_reply */

/*
 * Install the capture callback on the encoding so it fires from inside
 * the next send_reply call.  Caller has won the slot transition and set
 * req->replay_slot. */
static inline void
nfs4_replay_arm_capture(struct nfs_request *req)
{
    if (req->encoding) {
        req->encoding->reply_capture_cb      = nfs4_replay_capture_reply;
        req->encoding->reply_capture_private = req;
    }
} /* nfs4_replay_arm_capture */

nfsstat4
nfs4_replay_slot_acquire(
    struct nfs4_session *session,
    uint32_t             slotid,
    uint32_t             seqid,
    bool                 cachethis,
    struct nfs_request  *req,
    bool                *out_is_replay)
{
    struct nfs4_replay_slot *slot;
    nfsstat4                 status      = NFS4_OK;
    uint32_t                 freed_bytes = 0;

    *out_is_replay = false;

    if (slotid >= session->replay_max_slots) {
        nfs4_replay_metric_inc(req, replay_field_bad_slot);
        return NFS4ERR_BADSLOT;
    }

    slot = &session->replay_slots[slotid];

    /* Lock-free state machine.  Distinct slots never collide; the CAS only
     * ever actually contends on a same-slot retransmit racing the original
     * (possibly on another connection).  Losers re-read and observe
     * IN_PROGRESS -> RETRY_UNCACHED_REP, exactly as the old lock produced. */
    for ( ;; ) {
        uint64_t             cur = atomic_load_explicit(&slot->state_word,
                                                        memory_order_acquire);
        enum nfs4_slot_state state = (enum nfs4_slot_state) (cur & NFS4_SLOT_STATE_MASK);
        uint32_t             cseq  = (uint32_t) (cur >> NFS4_SLOT_SEQID_SHIFT);

        if (state == NFS4_SLOT_UNUSED) {
            /* First-ever use.  RFC 5661 sets csr_sequence = 1 at
             * CREATE_SESSION; the client's first SEQUENCE on a slot must
             * therefore use seqid = 1. */
            if (seqid != 1) {
                status = NFS4ERR_SEQ_MISORDERED;
                break;
            }
            if (!atomic_compare_exchange_weak_explicit(
                    &slot->state_word, &cur,
                    nfs4_slot_word(seqid, NFS4_SLOT_IN_PROGRESS),
                    memory_order_acq_rel, memory_order_acquire)) {
                continue;  /* lost the race; re-read and re-evaluate */
            }
            req->replay_slot        = slot;
            req->replay_slot_id     = slotid;
            req->replay_action      = NFS4_REPLAY_ACTION_NEW;
            slot->in_progress_since = nfs4_slot_now();
            slot->last_stuck_report = 0;
            if (cachethis) {
                nfs4_replay_arm_capture(req);
            }
            break;
        } else if (state == NFS4_SLOT_CACHED || state == NFS4_SLOT_COMPLETED) {
            if (seqid == (uint32_t) (cseq + 1)) {
                /* Normal advance. */
                if (!atomic_compare_exchange_weak_explicit(
                        &slot->state_word, &cur,
                        nfs4_slot_word(seqid, NFS4_SLOT_IN_PROGRESS),
                        memory_order_acq_rel, memory_order_acquire)) {
                    continue;
                }
                /* We won the transition; reclaim any prior cached reply -- the
                 * client acknowledged it by moving on.  Only the CAS winner
                 * reaches here, so the free is unraced. */
                if (state == NFS4_SLOT_CACHED && slot->cached_buf) {
                    freed_bytes = slot->cached_len;
                    free(slot->cached_buf);
                    slot->cached_buf = NULL;
                    slot->cached_len = 0;
                    atomic_fetch_sub_explicit(&session->replay_bytes_in_use,
                                              freed_bytes, memory_order_relaxed);
                    /* The client acknowledged the prior seqid by advancing;
                     * drop its persisted reply so the KV store keeps at most
                     * one entry per slot (the in-memory invariant). */
                    if (session->nfs4_session_persist && req->thread) {
                        nfs4_drc_delete_reply(req->thread->vfs_thread,
                                              req->thread->shared->node_id,
                                              session->nfs4_session_id,
                                              slotid, cseq);
                    }
                }
                req->replay_slot        = slot;
                req->replay_slot_id     = slotid;
                req->replay_action      = NFS4_REPLAY_ACTION_NEW;
                slot->in_progress_since = nfs4_slot_now();
                slot->last_stuck_report = 0;
                if (cachethis) {
                    nfs4_replay_arm_capture(req);
                }
            } else if (seqid == cseq) {
                /* Retransmit. */
                if (state == NFS4_SLOT_CACHED && slot->cached_buf) {
                    req->replay_slot    = slot;
                    req->replay_slot_id = slotid;
                    req->replay_action  = NFS4_REPLAY_ACTION_FROM_CACHE;
                    *out_is_replay      = true;
                } else {
                    /* Original ran with cachethis=false (or was demoted
                     * for size).  We cannot replay the bytes. */
                    status = NFS4ERR_RETRY_UNCACHED_REP;
                }
            } else {
                status = NFS4ERR_SEQ_MISORDERED;
            }
            break;
        } else { /* NFS4_SLOT_IN_PROGRESS */
            /* Original compound is still running.  Whether seqid matches
             * (true retry) or not (client jumped ahead), we cannot
             * produce a reply yet.  Both paths return errors. */
            if (seqid == cseq) {
                status = NFS4ERR_RETRY_UNCACHED_REP;
                /* A retry landing on a slot that has been IN_PROGRESS for many
                 * seconds means the original compound is wedged server-side;
                 * the client will retry RETRY_UNCACHED_REP silently forever
                 * (observed as fsstress writeback hanging in CI with 14k
                 * errored WRITE RPCs and nothing in the server log).  Make the
                 * wedge visible, rate-limited per slot. */
                {
                    time_t now = nfs4_slot_now();

                    if (slot->in_progress_since &&
                        now - slot->in_progress_since >= 5 &&
                        now - slot->last_stuck_report >= 10) {
                        slot->last_stuck_report = now;
                        chimera_nfs_error(
                            "nfs4 session slot %u seqid %u stuck IN_PROGRESS "
                            "for %ld sec; replying RETRY_UNCACHED_REP "
                            "(original compound has not completed)",
                            slotid, seqid,
                            (long) (now - slot->in_progress_since));
                    }
                }
            } else {
                status = NFS4ERR_SEQ_MISORDERED;
            }
            break;
        }
    } /* for */

    /* Update metrics outside the hot path to avoid serializing the
     * shared prometheus counters on per-session traffic. */
    if (freed_bytes) {
        nfs4_replay_bytes_delta(req, -(int64_t) freed_bytes);
    }
    if (*out_is_replay) {
        nfs4_replay_metric_inc(req, replay_field_hit);
    } else if (status == NFS4ERR_SEQ_MISORDERED) {
        nfs4_replay_metric_inc(req, replay_field_seq_misordered);
    } else if (status == NFS4ERR_RETRY_UNCACHED_REP) {
        nfs4_replay_metric_inc(req, replay_field_retry_uncached);
    }
    return status;
} /* nfs4_replay_slot_acquire */

void
nfs4_replay_slot_finalize(struct nfs_request *req)
{
    struct nfs4_session     *session = req->session;
    struct nfs4_replay_slot *slot    = req->replay_slot;
    uint64_t                 cur;

    if (!slot || !session) {
        return;
    }

    /* NB: at this point req->encoding has already been freed inside
     * send_reply_NFSPROC4_COMPOUND -- do not dereference it. */

    /* If the session has been destroyed under us, the slot table may
     * still be valid (it lives until the last conn ref is dropped) but
     * caching the reply is pointless. */
    if (atomic_load_explicit(&session->destroyed, memory_order_acquire)) {
        req->replay_slot = NULL;
        return;
    }

    /* Runs on the same thread that won the acquire; while IN_PROGRESS no other
     * thread writes the word (retransmits only read it and error out), so a
     * plain load + release store is sufficient.  The release pairs with the
     * acquire load in nfs4_replay_slot_acquire so a reader that observes CACHED
     * also observes cached_buf/cached_len written by the capture callback.
     * The seqid is unchanged -- it was set to the in-flight value at acquire. */
    cur = atomic_load_explicit(&slot->state_word, memory_order_relaxed);
    if ((cur & NFS4_SLOT_STATE_MASK) == NFS4_SLOT_IN_PROGRESS) {
        enum nfs4_slot_state new_state = slot->cached_buf ?
            NFS4_SLOT_CACHED : NFS4_SLOT_COMPLETED;
        uint32_t             fseqid = (uint32_t) (cur >> NFS4_SLOT_SEQID_SHIFT);

        atomic_store_explicit(&slot->state_word,
                              nfs4_slot_word(fseqid, new_state),
                              memory_order_release);

        /* Write-through the cached reply to the KV store for a persistent
         * session.  Write-only on the hot path: a copy of the bytes is handed
         * to an async put; the in-memory slot stays authoritative for
         * retransmit detection.  Cold start reloads these (nfs4_drc_reload).
         * nfs4_session_persist is only set when nfs4_drc is enabled. */
        if (new_state == NFS4_SLOT_CACHED && session->nfs4_session_persist &&
            req->thread) {
            nfs4_drc_persist_reply(req->thread->vfs_thread,
                                   req->thread->shared->node_id, session,
                                   req->replay_slot_id, fseqid,
                                   slot->cached_buf, slot->cached_len);
        }
    }

    req->replay_slot = NULL;
} /* nfs4_replay_slot_finalize */

/*
 * Record the delegation callback path for a client onto its unified state
 * record.  Called from the SETCLIENTID handler (4.0, with a cb_client4) and
 * from CREATE_SESSION (4.1, program only -- the backchannel rides the fore
 * conn).  Re-setting the path (e.g. a fresh SETCLIENTID) resets the probe
 * state so the callback channel is re-validated before any new delegation.
 *
 * The actual outbound channel (cb_path.cb_client) is established lazily by
 * the callback subsystem; here we only capture the addressing the client
 * supplied.
 */
void
nfs4_client_set_cb_path(
    struct nfs4_client_table *table,
    uint64_t                  client_id,
    uint32_t                  cb_program,
    uint32_t                  cb_ident,
    uint8_t                   minorversion,
    const char               *netid,
    int                       netid_len,
    const char               *addr,
    int                       addr_len)
{
    struct nfs4_client  *c;
    struct nfs_client   *u;
    struct nfs4_cb_path *cb;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &client_id, sizeof(client_id), c);

    if (!c || !c->unified) {
        pthread_mutex_unlock(&table->nfs4_ct_lock);
        return;
    }

    u  = c->unified;
    cb = &u->cb_path;

    if (addr_len >= (int) sizeof(cb->cb_addr)) {
        addr_len = sizeof(cb->cb_addr) - 1;
    }

    pthread_mutex_lock(&u->lock);

    /* Detect a genuine callback-address change: the client re-registered a new
     * callback server (SETCLIENTID with the same verifier, RFC 7530 §16.33).
     * An existing channel points at the OLD server, so it must be torn down --
     * otherwise a pending recall would be delivered to the address the client
     * just abandoned.  The new channel is rebuilt lazily (next delegation grant)
     * or eagerly at SETCLIENTID_CONFIRM when delegations are already held. */
    bool addr_changed = cb->cb_program != cb_program ||
        (int) strlen(cb->cb_addr) != addr_len ||
        (addr_len > 0 && memcmp(cb->cb_addr, addr, addr_len) != 0);

    cb->cb_program      = cb_program;
    cb->cb_ident        = cb_ident;
    cb->cb_minorversion = minorversion;

    if (netid_len >= (int) sizeof(cb->cb_netid)) {
        netid_len = sizeof(cb->cb_netid) - 1;
    }
    if (netid_len > 0) {
        memcpy(cb->cb_netid, netid, netid_len);
    }
    cb->cb_netid[netid_len > 0 ? netid_len : 0] = '\0';

    if (addr_len > 0) {
        memcpy(cb->cb_addr, addr, addr_len);
    }
    cb->cb_addr[addr_len > 0 ? addr_len : 0] = '\0';

    /* New addressing: force re-probe before the next delegation grant. */
    atomic_store_explicit(&cb->cb_state, NFS4_CB_UNINIT, memory_order_relaxed);

    if (addr_changed && cb->cb_client) {
        nfs4_cb_path_teardown(cb, false);
    }

    pthread_mutex_unlock(&u->lock);
    pthread_mutex_unlock(&table->nfs4_ct_lock);
} /* nfs4_client_set_cb_path */

/*
 * Record the RPC auth flavor/credentials the client wants the server to use on
 * its callback channel (CREATE_SESSION csa_sec_parms / BACKCHANNEL_CTL).
 * flavor is an ONC RPC flavor (AUTH_NONE=0, AUTH_SYS=1); uid/gid apply only to
 * AUTH_SYS.  Optionally also updates the callback program number.
 */
void
nfs4_client_set_cb_sec(
    struct nfs4_client_table *table,
    uint64_t                  client_id,
    uint32_t                  cb_program,
    uint32_t                  flavor,
    uint32_t                  uid,
    uint32_t                  gid)
{
    struct nfs4_client *c;
    struct nfs_client  *u;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &client_id, sizeof(client_id), c);

    if (!c || !c->unified) {
        pthread_mutex_unlock(&table->nfs4_ct_lock);
        return;
    }

    u = c->unified;

    pthread_mutex_lock(&u->lock);
    if (cb_program) {
        u->cb_path.cb_program = cb_program;
    }
    u->cb_path.cb_sec_flavor = flavor;
    u->cb_path.cb_sec_uid    = uid;
    u->cb_path.cb_sec_gid    = gid;
    pthread_mutex_unlock(&u->lock);

    pthread_mutex_unlock(&table->nfs4_ct_lock);
} /* nfs4_client_set_cb_sec */
