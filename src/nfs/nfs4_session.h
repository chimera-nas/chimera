#pragma once

#include "nfs3_xdr.h"
#include "nfs4_xdr.h"
#include "uthash/uthash.h"
#include "vfs/vfs.h"

#define NFS4_SESSION_MAX_STATE 64

struct nfs4_state {
    struct stateid4                nfs4_state_id;
    uint32_t                       nfs4_state_type;
    uint32_t                       nfs4_state_active;
    struct chimera_vfs_open_handle nfs4_state_handle;
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
    struct nfs4_state     nfs4_session_state[NFS4_SESSION_MAX_STATE];
    int32_t               nfs4_session_max_slot;
    uint32_t              nfs4_session_implicit;
    struct nfs4_client   *nfs4_session_client;
    struct channel_attrs4 nfs4_session_fore_attrs;
    struct channel_attrs4 nfs4_session_back_attrs;
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

uint64_t
nfs4_client_register(
    struct nfs4_client_table *table,
    const void               *owner,
    int                       owner_len,
    uint64_t                  verifier,
    uint32_t                  proto,
    const char               *nii_domain,
    const char               *nii_name);

struct nfs4_session *
nfs4_create_session(
    struct nfs4_client_table    *table,
    uint64_t                     client_id,
    uint32_t                     implicit,
    const struct channel_attrs4 *fore_attrs,
    const struct channel_attrs4 *back_attrs);

void
nfs4_destroy_session(
    struct nfs4_client_table *table,
    const void               *session_id);

static inline struct nfs4_state *
nfs4_session_alloc_slot(struct nfs4_session *session)
{
    uint32_t           slot  = ++session->nfs4_session_max_slot;
    struct nfs4_state *state = &session->nfs4_session_state[slot];

    state->nfs4_state_id.seqid = slot;
    state->nfs4_state_active   = 1;

    return state;
} /* nfs4_session_alloc_slot */

static inline void
nfs4_session_free_slot(
    struct nfs4_session *session,
    uint32_t             slot)
{

    session->nfs4_session_state[slot].nfs4_state_active = 0;

    while (session->nfs4_session_max_slot >= 0 &&
           session->nfs4_session_state[session->nfs4_session_max_slot].
           nfs4_state_active) {
        --session->nfs4_session_max_slot;
    }

} /* nfs4_session_free_slot */