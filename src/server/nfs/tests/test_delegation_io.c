// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include "nfs4_state.h"
#include "nfs4_layout_table.h"

/* RFC 8435 layout type, absent from the generated base NFSv4 header. */
#define LAYOUT4_FLEX_FILES 4

#define CHECK(c) do { if (!(c)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #c); abort(); \
                      } } while (0)

static void
check_io(
    struct nfs_state_table *table,
    struct nfs_delegation  *deleg,
    struct stateid4         sid,
    struct nfs_client      *client,
    const uint8_t          *fh,
    uint32_t                len,
    uint32_t                access,
    nfsstat4                expected)
{
    uint64_t touched  = client->last_touch_ns;
    uint32_t pins     = atomic_load(&client->compound_pins);
    uint32_t refs     = atomic_load(&deleg->refcount);
    uint32_t sequence = atomic_load(&deleg->seqid);
    uint8_t  recall   = atomic_load(&deleg->cb_recall_state);
    uint8_t  retries  = atomic_load(&deleg->cb_recall_retries);

    for (int i = 0; i < 4; i++) {
        struct chimera_claim_actor actor;
        memset(&actor, 0xa5, sizeof(actor));
        CHECK(nfs_state_table_delegation_io(table, &sid, client, fh, len, access, &actor) == expected);
        if (expected == NFS4_OK) {
            CHECK(actor.owner.proto == CHIMERA_CLAIM_PROTO_NFSV4);
            CHECK(actor.owner.client_key == client->client_id);
            CHECK(actor.owner.owner_lo == deleg->fh_hash && actor.owner.owner_hi == 0);
        } else {
            struct chimera_claim_actor zero = { 0 };
            CHECK(!memcmp(&actor, &zero, sizeof(actor)));
        }
        CHECK(client->last_touch_ns == touched && atomic_load(&client->compound_pins) == pins);
        CHECK(atomic_load(&deleg->refcount) == refs && atomic_load(&deleg->seqid) == sequence);
        CHECK(atomic_load(&deleg->cb_recall_state) == recall && atomic_load(&deleg->cb_recall_retries) == retries);
    }
} /* check_io */

static void
test_combine(
    struct nfs_state_table *table,
    struct nfs_delegation  *deleg)
{
    struct nfs_delegation_combine_journal journal = { 0 }, competing = { 0 };
    uint32_t                              refs = atomic_load(&deleg->refcount);
    uint64_t                              change;
    bool                                  modified;

    CHECK(nfs_delegation_combine_reserve(deleg, &journal, &journal) == NFS4_OK);
    CHECK(atomic_load(&deleg->refcount) == refs + 1);
    CHECK(nfs_delegation_combine_reserve(deleg, &competing, &competing) == NFS4ERR_DELAY);
    CHECK(!competing.deleg && deleg->combine_reservation == &journal);
    CHECK(nfs_delegation_combine_apply(&journal, 50, &modified, &change) == NFS4_OK && !modified);
    CHECK(journal.valid && journal.sc == 50 && !deleg->combine_valid);
    nfs_delegation_combine_finish(&journal, false, table, NULL);
    CHECK(!deleg->combine_valid && !deleg->combine_reservation && atomic_load(&deleg->refcount) == refs);

    CHECK(nfs_delegation_combine_reserve(deleg, &journal, &journal) == NFS4_OK);
    CHECK(nfs_delegation_combine_apply(&journal, 60, &modified, &change) == NFS4_OK && !modified);
    nfs_delegation_combine_finish(&journal, true, table, NULL);
    CHECK(deleg->combine_valid && deleg->combine_sc == 60 && deleg->combine_last == 60);

    CHECK(nfs_delegation_combine_reserve(deleg, &journal, &journal) == NFS4_OK);
    CHECK(nfs_delegation_combine_apply(&journal, 70, &modified, &change) == NFS4_OK && modified && change == 70);
    CHECK(nfs_delegation_combine_apply(&journal, 65, &modified, &change) == NFS4_OK && modified && change == 71);
    CHECK(deleg->combine_sc == 60 && deleg->combine_last == 60);
    nfs_delegation_combine_reset(&journal);
    CHECK(!journal.applied && journal.sc == 60 && journal.last == 60);
    CHECK(nfs_delegation_combine_apply(&journal, 70, &modified, &change) == NFS4_OK && modified && change == 70);
    nfs_delegation_combine_finish(&journal, true, table, NULL);
    CHECK(deleg->combine_sc == 70 && deleg->combine_last == 70 && atomic_load(&deleg->refcount) == refs);
    /* Accepted disposal of an unused journal does not rewrite the baseline. */
    CHECK(nfs_delegation_combine_reserve(deleg, &journal, &journal) == NFS4_OK);
    nfs_delegation_combine_finish(&journal, true, table, NULL);
    CHECK(deleg->combine_sc == 70 && !deleg->combine_reservation);

    deleg->combine_sc = deleg->combine_last = UINT64_MAX;
    CHECK(nfs_delegation_combine_reserve(deleg, &journal, &journal) == NFS4_OK);
    CHECK(nfs_delegation_combine_apply(&journal, 1, &modified, &change) == NFS4ERR_RESOURCE && !modified);
    CHECK(!journal.applied && journal.sc == UINT64_MAX);
    nfs_delegation_combine_finish(&journal, true, table, NULL);
    CHECK(deleg->combine_sc == UINT64_MAX && atomic_load(&deleg->refcount) == refs);
} /* test_combine */

static void
test_combine_attributes(
    struct nfs_state_table *table,
    struct nfs_delegation  *deleg)
{
    struct nfs_delegation_combine_journal journal = { 0 };
    const struct timespec                 now     = { .tv_sec = 123, .tv_nsec = 456 };
    struct chimera_vfs_attrs              local   = {
        .va_set_mask = CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_SIZE,
        .va_change   = 3,                                              .va_size = 9,
    }, attrs;

    deleg->combine_sc    = deleg->combine_last = 3;
    deleg->combine_valid = true;
    deleg->combine_dirty = false;
    CHECK(nfs_delegation_combine_reserve(deleg, &journal, &journal) == NFS4_OK);
    attrs = local;
    CHECK(nfs_delegation_combine_attrs(&journal, 3, true, 9, &now, &attrs) == NFS4_OK);
    CHECK(!journal.dirty && attrs.va_change == 3 && attrs.va_size == 9);
    attrs = local;
    CHECK(nfs_delegation_combine_attrs(&journal, 4, true, 14, &now, &attrs) == NFS4_OK);
    CHECK(attrs.va_change == 4 && attrs.va_size == 14 && journal.dirty);
    CHECK(!deleg->combine_dirty && deleg->combine_sc == 3);
    nfs_delegation_combine_reset(&journal); /* rejected finish: no double advance */
    CHECK(!journal.dirty && journal.sc == 3);
    attrs = local;
    CHECK(nfs_delegation_combine_attrs(&journal, 4, true, 14, &now, &attrs) == NFS4_OK);
    CHECK(attrs.va_change == 4 && attrs.va_size == 14);
    attrs = local;
    CHECK(nfs_delegation_combine_attrs(&journal, 4, true, 14, &now, &attrs) == NFS4_OK);
    CHECK(attrs.va_change == 5 && attrs.va_size == 14); /* identical cc stays dirty */
    CHECK(attrs.va_mtime.tv_sec == now.tv_sec && attrs.va_mtime.tv_nsec == now.tv_nsec);
    nfs_delegation_combine_finish(&journal, true, table, NULL);
    CHECK(deleg->combine_dirty && deleg->combine_sc == 5);

    CHECK(nfs_delegation_combine_reserve(deleg, &journal, &journal) == NFS4_OK);
    attrs = local;
    CHECK(nfs_delegation_combine_attrs(&journal, 5, true, 12, &now, &attrs) == NFS4_OK);
    CHECK(attrs.va_change == 6 && attrs.va_size == 12); /* cc equal to nsc also dirty */
    nfs_delegation_combine_finish(&journal, false, table, NULL);
    CHECK(deleg->combine_sc == 5 && deleg->combine_dirty); /* aborted suffix */

    CHECK(nfs_delegation_combine_reserve(deleg, &journal, &journal) == NFS4_OK);
    attrs           = local;
    attrs.va_change = 20; /* holder flushed data between callback queries */
    CHECK(nfs_delegation_combine_attrs(&journal, 5, true, 12, &now, &attrs) == NFS4_OK);
    CHECK(attrs.va_change == 21 && attrs.va_size == 12);
    nfs_delegation_combine_finish(&journal, false, table, NULL);

    deleg->combine_sc    = deleg->combine_last = 3;
    deleg->combine_dirty = false;
    CHECK(nfs_delegation_combine_reserve(deleg, &journal, &journal) == NFS4_OK);
    attrs = local;
    CHECK(nfs_delegation_combine_attrs(&journal, 3, true, 14, &now, &attrs) == NFS4_OK);
    CHECK(journal.dirty && attrs.va_change == 4 && attrs.va_size == 14); /* size alone */
    nfs_delegation_combine_finish(&journal, false, table, NULL);
    CHECK(!deleg->combine_dirty);

    deleg->combine_valid = false; /* no attrs available when CLAIM_FH granted */
    CHECK(nfs_delegation_combine_reserve(deleg, &journal, &journal) == NFS4_OK);
    attrs = local;
    CHECK(nfs_delegation_combine_attrs(&journal, 4, true, 9, &now, &attrs) == NFS4_OK);
    CHECK(journal.dirty && attrs.va_change == 4);
    nfs_delegation_combine_finish(&journal, true, table, NULL);

    deleg->combine_sc = deleg->combine_last = UINT64_MAX;
    CHECK(nfs_delegation_combine_reserve(deleg, &journal, &journal) == NFS4_OK);
    attrs = local;
    CHECK(nfs_delegation_combine_attrs(&journal, UINT64_MAX, true, 14, &now, &attrs) == NFS4ERR_RESOURCE);
    CHECK(attrs.va_change == 3 && attrs.va_size == 9 && !journal.applied);
    nfs_delegation_combine_finish(&journal, false, table, NULL);
} /* test_combine_attributes */

static void
test_change_floor(void)
{
    struct nfs4_change_table       *table = nfs4_change_table_create();
    struct nfs4_change_observation *pending = NULL, *observation;
    uint8_t                         fh[]  = { 1, 9, 2 };
    struct chimera_vfs_attrs        attrs = { .va_set_mask = CHIMERA_VFS_ATTR_CHANGE, .va_change = 3 };

    CHECK(table && nfs4_change_register(table, fh, sizeof(fh)));
    CHECK(nfs4_change_project(table, fh, sizeof(fh), &attrs, &pending, &observation) == NFS4_OK);
    attrs.va_change = 10; /* accepted dirty delegation response */
    nfs4_change_observe(observation, &attrs);
    nfs4_change_finish(table, &pending, false);
    attrs.va_change = 3;
    CHECK(nfs4_change_project(table, fh, sizeof(fh), &attrs, &pending, &observation) == NFS4_OK);
    CHECK(attrs.va_change == 3); /* discarded attempt never published */
    attrs.va_change = 10;
    nfs4_change_observe(observation, &attrs);
    nfs4_change_finish(table, &pending, true);
    attrs.va_change = 3;
    CHECK(nfs4_change_project(table, fh, sizeof(fh), &attrs, &pending, &observation) == NFS4_OK);
    CHECK(attrs.va_change == 10); /* floor survives delegation return */
    attrs.va_change = 4;
    CHECK(nfs4_change_project(table, fh, sizeof(fh), &attrs, &pending, &observation) == NFS4_OK);
    CHECK(attrs.va_change == 11); /* normal backend mutation remains visible */
    nfs4_change_finish(table, &pending, true);
    attrs.va_change = 1; /* backend reset/wrap */
    CHECK(nfs4_change_project(table, fh, sizeof(fh), &attrs, &pending, &observation) == NFS4ERR_DELAY);
    nfs4_change_finish(table, &pending, false);
    CHECK(nfs4_change_project(table, fh, sizeof(fh), &attrs, &pending, &observation) == NFS4ERR_DELAY);
    nfs4_change_finish(table, &pending, true);
    CHECK(nfs4_change_project(table, fh, sizeof(fh), &attrs, &pending, &observation) == NFS4_OK);
    CHECK(attrs.va_change == 12);
    attrs.va_change = UINT64_MAX;
    nfs4_change_observe(observation, &attrs);
    nfs4_change_finish(table, &pending, true);
    attrs.va_change = 2;
    CHECK(nfs4_change_project(table, fh, sizeof(fh), &attrs, &pending, &observation) == NFS4ERR_RESOURCE);
    CHECK(!pending); /* never silently clamp and hide subsequent mutation */
    nfs4_change_table_free(table);

    table = nfs4_change_table_create();
    CHECK(table);
    for (uint32_t i = 0; i < NFS4_CHANGE_FLOOR_LIMIT; i++) {
        CHECK(nfs4_change_register(table, (uint8_t *) &i, sizeof(i)));
    }
    uint32_t extra = NFS4_CHANGE_FLOOR_LIMIT;
    CHECK(!nfs4_change_register(table, (uint8_t *) &extra, sizeof(extra)));
    extra = 0;
    CHECK(nfs4_change_register(table, (uint8_t *) &extra, sizeof(extra)));
    nfs4_change_table_free(table);
} /* test_change_floor */

static void
test_layout_client_lifetime(struct nfs_state_table *table)
{
    struct nfs_layout_table  layouts = { 0 };
    struct nfs_client       *client;
    struct nfs_layout_state *layout;
    struct stateid4          sid;
    uint8_t                  fh[] = { 9, 8, 7 };

    for (int i = 0; i < NFS_LAYOUT_TABLE_SHARDS; i++) {
        evpl_mutex_init(&layouts.shards[i].lock, NULL);
    }
    client = nfs_client_alloc(703, "layout-lifetime", 15, 1, 1);
    layout = nfs_layout_state_create(client, fh, sizeof(fh), 1,
                                     LAYOUTIOMODE4_RW, 0, table, &layouts, &sid);
    void                    *acquired = NULL;
    CHECK(nfs_state_table_acquire_no_renew(table, &sid, NFS4_SLOT_TYPE_LAYOUT, &acquired, NULL) == NFS4_OK);
    CHECK(acquired == layout && atomic_load(&layout->refcount) == 2);
    nfs_state_table_release(table, acquired, NFS4_SLOT_TYPE_LAYOUT, NULL);
    CHECK(atomic_load(&layout->refcount) == 1);
    struct stateid4          forged = sid, untouched;
    forged.other[sizeof(forged.other) - 1] ^= 1;
    CHECK(nfs_layout_state_check(client, fh, sizeof(fh), &forged, table) == NFS4ERR_BAD_STATEID);
    CHECK(nfs_layout_state_grant(client, fh, sizeof(fh), 1, LAYOUTIOMODE4_RW,
                                 LAYOUT4_FLEX_FILES, &forged, table, &layouts, &untouched) == NFS4ERR_BAD_STATEID);
    CHECK(nfs_layout_state_return_file(client, fh, sizeof(fh), &forged,
                                       LAYOUT4_FLEX_FILES, table, NULL) == NFS4ERR_BAD_STATEID);
    CHECK(atomic_load(&layout->refcount) == 1 && layout->seqid == sid.seqid && !atomic_load(&layout->destroyed));
    atomic_fetch_add(&layout->refcount, 1); /* recall snapshot borrow */
    CHECK(atomic_load(&client->refcount) == 2);
    nfs_client_destroy(client, table, NULL, true);
    CHECK(client->teardown_started && atomic_load(&client->refcount) == 1);
    CHECK(atomic_load(&layout->destroyed));
    CHECK(!nfs_layout_table_has_holders(&layouts, fh, sizeof(fh)));
    CHECK(!nfs_layout_state_reserve_client(layout));
    nfs_client_destroy(client, table, NULL, true); /* no second base-ref drop */
    CHECK(atomic_load(&client->refcount) == 1);
    /* Generic state-table borrows must release client memory too. */
    nfs_state_table_release(table, layout, NFS4_SLOT_TYPE_LAYOUT, NULL);

    client = nfs_client_alloc(704, "layout-pending", 14, 1, 1);
    layout = nfs_layout_state_create(client, fh, sizeof(fh), 1,
                                     LAYOUTIOMODE4_RW, 0, table, &layouts, &sid);
    atomic_fetch_add(&layout->refcount, 1);
    CHECK(nfs_client_reserve_compound(client));
    nfs_client_destroy(client, table, NULL, true);
    CHECK(client->compound_destroy_pending && !client->teardown_started);
    CHECK(!nfs_client_reserve_compound(client)); /* new operations forbidden */
    CHECK(nfs_layout_state_reserve_client(layout) == client); /* recall proceeds */
    nfs_client_duplicate_compound_pin(client); /* callback continuation borrow */
    CHECK(atomic_load(&client->compound_pins) == 3);
    nfs_client_finish_compound(client, table, NULL);
    nfs_layout_state_destroy(layout, table, NULL);
    CHECK(!nfs_layout_table_has_holders(&layouts, fh, sizeof(fh)));
    nfs_client_finish_compound(client, table, NULL);
    CHECK(!client->teardown_started && atomic_load(&client->compound_pins) == 1);
    nfs_client_finish_compound(client, table, NULL);
    CHECK(client->teardown_started && atomic_load(&client->refcount) == 1);
    CHECK(!nfs_layout_state_reserve_client(layout));
    nfs_layout_state_put(layout);
    for (int i = 0; i < NFS_LAYOUT_TABLE_SHARDS; i++) {
        CHECK(!layouts.shards[i].by_fh);
        evpl_mutex_destroy(&layouts.shards[i].lock);
    }
} /* test_layout_client_lifetime */

int
main(void)
{
    struct nfs_state_table     table;
    struct nfs_layout_table    layouts = { 0 };
    struct nfs_client         *client, *other;
    struct nfs_delegation     *read_deleg, *write_deleg;
    struct nfs_layout_state   *layout;
    struct stateid4            read_sid, write_sid, layout_sid, changed;
    uint8_t                    fh[] = { 1, 2, 3 }, alternate[] = { 1, 2, 4 };
    struct chimera_claim_actor actor;

    nfs_state_table_init(&table, 1);
    client      = nfs_client_alloc(701, "deleg-client", 12, 1, 1);
    other       = nfs_client_alloc(702, "other-client", 12, 2, 1);
    read_deleg  = nfs_delegation_create(client, OPEN_DELEGATE_READ, fh, sizeof(fh), 12345, 1, &table, &read_sid);
    write_deleg = nfs_delegation_create(client, OPEN_DELEGATE_WRITE, alternate, sizeof(alternate), 54321, 1, &table, &
                                        write_sid);

    check_io(&table, read_deleg, read_sid, client, fh, sizeof(fh), OPEN4_SHARE_ACCESS_READ, NFS4_OK);
    check_io(&table, read_deleg, read_sid, client, fh, sizeof(fh), OPEN4_SHARE_ACCESS_WRITE, NFS4ERR_OPENMODE);
    check_io(&table, write_deleg, write_sid, client, alternate, sizeof(alternate), OPEN4_SHARE_ACCESS_BOTH, NFS4_OK);
    check_io(&table, read_deleg, read_sid, other, fh, sizeof(fh), OPEN4_SHARE_ACCESS_READ, NFS4ERR_BAD_STATEID);
    check_io(&table, read_deleg, read_sid, client, alternate, sizeof(alternate), OPEN4_SHARE_ACCESS_READ,
             NFS4ERR_BAD_STATEID);
    check_io(&table, read_deleg, read_sid, client, fh, sizeof(fh) - 1, OPEN4_SHARE_ACCESS_READ, NFS4ERR_BAD_STATEID);
    check_io(&table, read_deleg, read_sid, client, NULL, 0, OPEN4_SHARE_ACCESS_READ, NFS4ERR_BAD_STATEID);
    check_io(&table, read_deleg, read_sid, client, fh, sizeof(fh), 0, NFS4ERR_INVAL);

    atomic_store(&read_deleg->seqid, 3);
    check_io(&table, read_deleg, read_sid, client, fh, sizeof(fh), OPEN4_SHARE_ACCESS_READ, NFS4ERR_OLD_STATEID);
    changed       = read_sid;
    changed.seqid = 4;
    check_io(&table, read_deleg, changed, client, fh, sizeof(fh), OPEN4_SHARE_ACCESS_READ, NFS4ERR_BAD_STATEID);
    changed.seqid = 0;
    check_io(&table, read_deleg, changed, client, fh, sizeof(fh), OPEN4_SHARE_ACCESS_READ, NFS4_OK);
    atomic_store(&read_deleg->revoked, 1);
    check_io(&table, read_deleg, changed, client, fh, sizeof(fh), OPEN4_SHARE_ACCESS_READ, NFS4ERR_DELEG_REVOKED);
    atomic_store(&read_deleg->revoked, 0);
    atomic_store(&client->reclaim_pending, 1);
    check_io(&table, read_deleg, changed, client, fh, sizeof(fh), OPEN4_SHARE_ACCESS_READ, NFS4ERR_EXPIRED);
    atomic_store(&client->reclaim_pending, 0);
    memset(&changed, 0, sizeof(changed));
    check_io(&table, read_deleg, changed, client, fh, sizeof(fh), OPEN4_SHARE_ACCESS_READ, NFS4ERR_BAD_STATEID);
    nfs4_stateid_encode(&changed, 0, NFS4_STATEID_TYPE_DELEG, read_deleg->shard,
                        read_deleg->slot_idx, read_deleg->generation, table.epoch + 1);
    check_io(&table, read_deleg, changed, client, fh, sizeof(fh), OPEN4_SHARE_ACCESS_READ, NFS4ERR_STALE_STATEID);

    /* The pure holder query neither starts recall nor changes layout refs.
     * Read layouts count too, matching the existing all-holder barrier. */
    for (int i = 0; i < NFS_LAYOUT_TABLE_SHARDS; i++) {
        evpl_mutex_init(&layouts.shards[i].lock, NULL);
    }
    CHECK(!nfs_layout_table_has_holders(&layouts, fh, sizeof(fh)));
    layout = nfs_layout_state_create(client, fh, sizeof(fh), 1, LAYOUTIOMODE4_READ, 0, &table, &layouts, &layout_sid);
    CHECK(layout);
    uint32_t layout_refs = atomic_load(&layout->refcount);
    for (int i = 0; i < 4; i++) {
        CHECK(nfs_layout_table_has_holders(&layouts, fh, sizeof(fh)));
        CHECK(!nfs_layout_table_has_holders(&layouts, alternate, sizeof(alternate)));
        CHECK(atomic_load(&layout->refcount) == layout_refs);
        CHECK(nfs_state_table_delegation_io(&table, &layout_sid, client, fh, sizeof(fh),
                                            OPEN4_SHARE_ACCESS_READ, &actor) == NFS4ERR_BAD_STATEID);
        for (int j = 0; j < NFS_LAYOUT_TABLE_SHARDS; j++) {
            struct nfs_layout_entry *entry = layouts.shards[j].by_fh;
            CHECK(!entry || !entry->waiters);
        }
    }
    nfs_layout_state_destroy(layout, &table, NULL);
    CHECK(!nfs_layout_table_has_holders(&layouts, fh, sizeof(fh)));
    for (int i = 0; i < NFS_LAYOUT_TABLE_SHARDS; i++) {
        CHECK(!layouts.shards[i].by_fh);
        evpl_mutex_destroy(&layouts.shards[i].lock);
    }
    nfs_delegation_destroy(read_deleg, &table, NULL);
    CHECK(nfs_state_table_delegation_io(&table, &read_sid, client, fh, sizeof(fh),
                                        OPEN4_SHARE_ACCESS_READ, &actor) == NFS4ERR_STALE_STATEID);
    test_combine(&table, write_deleg);
    test_combine_attributes(&table, write_deleg);
    struct nfs_delegation_combine_journal pinned = { 0 };
    CHECK(nfs_delegation_combine_reserve(write_deleg, &pinned, &pinned) == NFS4_OK);
    nfs_delegation_destroy(write_deleg, &table, NULL);
    CHECK(atomic_load(&pinned.deleg->destroyed) && atomic_load(&pinned.deleg->refcount) == 1);
    nfs_delegation_combine_finish(&pinned, false, &table, NULL);
    CHECK(!pinned.deleg);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_client_destroy(other, &table, NULL, true);
    test_layout_client_lifetime(&table);
    test_change_floor();
    nfs_state_table_free(&table, NULL);
    puts("ok: pure delegation I/O authorization and layout holder snapshots");
    return 0;
} /* main */
