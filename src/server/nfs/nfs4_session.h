// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <uthash.h>

#include "nfs3_xdr.h"
#include "nfs4_xdr.h"
#include "nfs_internal.h"
#include "vfs/vfs.h"

#define NFS4_SESSION_MAX_STATE 1024

struct nfs4_state {
    struct stateid4                 nfs4_state_id;
    uint16_t                        nfs4_state_type;
    uint16_t                        nfs4_state_active;
    struct chimera_vfs_open_handle *nfs4_state_handle;
};

struct nfs4_client {
    uint64_t              nfs4_client_id;
    uint32_t              nfs4_client_owner_len;
    uint32_t              nfs4_client_refcnt;
    uint32_t              nfs4_client_proto;
    uint64_t              nfs4_client_verifier;
    struct UT_hash_handle nfs4_client_hh_by_owner;
    struct UT_hash_handle nfs4_client_hh_by_id;
    uint8_t               nfs4_client_owner[NFS4_OPAQUE_LIMIT];
    char                  nfs4_client_domain[NFS4_OPAQUE_LIMIT];
    char                  nfs4_client_name[NFS4_OPAQUE_LIMIT];
};

struct nfs4_session {
    uint8_t               nfs4_session_id[NFS4_SESSIONID_SIZE];
    uint64_t              nfs4_session_clientid;
    uint32_t              num_free_slots;
    struct nfs4_state     nfs4_session_state[NFS4_SESSION_MAX_STATE];
    uint16_t              free_slot[NFS4_SESSION_MAX_STATE];
    uint32_t              nfs4_session_implicit;
    struct nfs4_client   *nfs4_session_client;
    struct channel_attrs4 nfs4_session_fore_attrs;
    struct channel_attrs4 nfs4_session_back_attrs;
    uint32_t              nfs4_session_fore_attrs_rdma_ird;
    uint32_t              nfs4_session_back_attrs_rdma_ird;
    struct UT_hash_handle nfs4_session_hh;
};

struct nfs4_client_table {
    struct nfs4_client  *nfs4_ct_clients_by_owner;
    struct nfs4_client  *nfs4_ct_clients_by_id;
    struct nfs4_session *nfs4_ct_sessions;
    uint64_t             nfs4_ct_next_client_id;
    pthread_mutex_t      nfs4_ct_lock;
};

void
nfs4_client_table_init(
    struct nfs4_client_table *table);

void
nfs4_client_table_free(
    struct nfs4_client_table *table);

void
nfs4_client_table_release_handles(
    struct nfs4_client_table  *table,
    struct chimera_vfs_thread *vfs_thread);

uint64_t
nfs4_client_register(
    struct nfs4_client_table *table,
    const void               *owner,
    int                       owner_len,
    uint64_t                  verifier,
    uint32_t                  proto,
    const char               *nii_domain,
    const char               *nii_name);

void
nfs4_client_unregister(
    struct nfs4_client_table *table,
    uint64_t                  client_id);

struct nfs4_session *
nfs4_create_session(
    struct nfs4_client_table    *table,
    uint64_t                     client_id,
    uint32_t                     implicit,
    const struct channel_attrs4 *fore_attrs,
    const struct channel_attrs4 *back_attrs);

struct nfs4_session *
nfs4_session_lookup(
    struct nfs4_client_table *table,
    const void               *sessionid);

void
nfs4_destroy_session(
    struct nfs4_client_table *table,
    const void               *session_id);

struct nfs4_session *
nfs4_session_find_by_clientid(
    struct nfs4_client_table *table,
    uint64_t                  client_id);

static inline struct nfs4_state *
nfs4_session_alloc_slot(struct nfs4_session *session)
{
    uint32_t           slot;
    struct nfs4_state *state;

    chimera_nfs_abort_if(session->num_free_slots == 0, "no free session slots");

    slot  = session->free_slot[--session->num_free_slots];
    state = &session->nfs4_session_state[slot];

    state->nfs4_state_id.seqid = 1;

    *(uint32_t *) state->nfs4_state_id.other       = slot;
    *(uint64_t *) (state->nfs4_state_id.other + 4) = session->nfs4_session_clientid;

    state->nfs4_state_active = 1;

    return state;
} /* nfs4_session_alloc_slot */

static inline void
nfs4_session_free_slot(
    struct nfs4_session *session,
    struct nfs4_state   *state)
{
    uint32_t slot = *(uint32_t *) state->nfs4_state_id.other;

    state->nfs4_state_active = 0;

    session->free_slot[session->num_free_slots++] = slot;

} /* nfs4_session_free_slot */

static inline struct nfs4_state *
nfs4_session_get_state(
    struct nfs4_session   *session,
    const struct stateid4 *stateid)
{
    uint32_t slot = *(uint32_t *) stateid->other;

    return &session->nfs4_session_state[slot];
} /* nfs4_session_get_state */

static inline struct nfs4_session *
nfs4_resolve_session(
    struct nfs4_session      *session,
    const struct stateid4    *stateid,
    struct nfs4_client_table *table)
{
    uint64_t client_id;

    if (session) {
        return session;
    }

    client_id = *(uint64_t *) (stateid->other + 4);

    return nfs4_session_find_by_clientid(table, client_id);
} /* nfs4_resolve_session */
