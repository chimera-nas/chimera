// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Force the OPEN lookup / CLOSE interleaving from root_export_nfs4_linux:
 * the lookup returns, CLOSE drops the state's lifetime and acquire refs,
 * then OPEN accesses its handle.  This is an internal lifetime race, below
 * the atomic protocol operations exercised by the Quint wire replayer. */

#include <stdio.h>
#include <stdlib.h>

#include "nfs4_state.h"

#define CHECK(cond) \
        do { \
            if (!(cond)) { \
                fprintf(stderr, "%s:%d: CHECK failed: %s\n", \
                        __FILE__, __LINE__, #cond); \
                abort(); \
            } \
        } while (0)

static void
test_lookup_lifetime(bool expire)
{
    struct nfs_state_table         table;
    struct nfs_client             *client;
    struct nfs_open_owner         *owner;
    struct nfs_open_state         *state, *borrowed;
    struct chimera_vfs_open_handle handle = { 0 };
    struct stateid4                sid;
    void                          *close_state;
    uint8_t                        close_type;
    bool                           created;
    const uint8_t                  fh[]      = { 1, 2, 3, 4 };
    const uint8_t                  missing[] = { 5, 6, 7, 8 };

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(1, "client", 6, 0x1234, 0);
    owner  = nfs_open_owner_find_or_create(client, "owner", 5, &created);
    CHECK(created);
    handle.access_mode = CHIMERA_VFS_ACCESS_MODE_RO;
    state              = nfs_open_state_create(owner, 0, NULL, 0, fh, sizeof(fh),
                                               OPEN4_SHARE_ACCESS_READ,
                                               OPEN4_SHARE_DENY_NONE, &handle, &table, &sid);
    CHECK(state != NULL);
    CHECK(nfs_open_owner_find_state(owner, missing, sizeof(missing)) == NULL);
    CHECK(nfs_open_owner_find_state(owner, fh, NFS4_FHSIZE + 1) == NULL);

    borrowed = nfs_open_owner_find_state(owner, fh, sizeof(fh));
    CHECK(borrowed == state);

    if (expire) {
        nfs_client_expire_state(client, &table, NULL);
        CHECK(HASH_COUNT(client->open_owners_by_str) == 0);
    } else {
        /* Match CLOSE's acquire -> destroy -> release sequence. */
        CHECK(nfs_state_table_acquire(&table, &sid, NFS4_SLOT_TYPE_OPEN,
                                      &close_state, &close_type) == NFS4_OK);
        CHECK(close_state == state);
        nfs_open_state_destroy(close_state, &table, NULL);
        nfs_state_table_release(&table, close_state, close_type, NULL);
    }

    /* Pre-fix this is a deterministic ASan use-after-free.  A lookup ref
     * keeps both the state and its handle alive until OPEN is finished. */
    CHECK(borrowed->handle == &handle);
    CHECK(borrowed->handle->access_mode == CHIMERA_VFS_ACCESS_MODE_RO);
    CHECK(borrowed->owner == owner);
    CHECK(atomic_load(&borrowed->destroyed) == 1);
    CHECK(atomic_load(&borrowed->refcount) == 1);
    CHECK(nfs_open_owner_find_state(owner, fh, sizeof(fh)) == NULL);
    CHECK(nfs_state_table_validate(&table, &sid) != NFS4_OK);
    nfs_state_table_release(&table, borrowed, NFS4_SLOT_TYPE_OPEN, NULL);

    /* A normal lookup/release must leave the live state installed, without
     * leaking a reference.  Use a fresh owner if expiry unpublished it. */
    nfs_open_owner_put(owner);
    owner = nfs_open_owner_find_or_create(client, "owner", 5, &created);
    state = nfs_open_state_create(owner, 0, NULL, 0, fh, sizeof(fh),
                                  OPEN4_SHARE_ACCESS_READ,
                                  OPEN4_SHARE_DENY_NONE, NULL, &table, &sid);
    CHECK(state != NULL);
    borrowed = nfs_open_owner_find_state(owner, fh, sizeof(fh));
    CHECK(borrowed == state);
    CHECK(atomic_load(&state->refcount) == 2);
    nfs_state_table_release(&table, borrowed, NFS4_SLOT_TYPE_OPEN, NULL);
    CHECK(atomic_load(&state->refcount) == 1);
    CHECK(nfs_state_table_validate(&table, &sid) == NFS4_OK);
    nfs_open_state_destroy(state, &table, NULL);
    nfs_open_owner_put(owner);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
} /* test_lookup_lifetime */

int
main(void)
{
    test_lookup_lifetime(false);
    test_lookup_lifetime(true);
    puts("PASS: open state lookup survives CLOSE and lease expiry");
    return 0;
} /* main */
