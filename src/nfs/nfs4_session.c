#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <uuid/uuid.h>
#include "nfs4_session.h"
#include "nfs_internal.h"

void
nfs4_client_table_init(struct nfs4_client_table *table)
{
    table->nfs4_ct_clients_by_owner = NULL;
    table->nfs4_ct_clients_by_id    = NULL;
    table->nfs4_ct_next_client_id   = 1;

    pthread_mutex_init(&table->nfs4_ct_lock, NULL);
} /* nfs4_client_table_init */

void
nfs4_client_table_free(struct nfs4_client_table *table)
{
    struct nfs4_client  *cur, *tmp;
    struct nfs4_session *sess, *sesstmp;

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
        free(sess);
    }
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

        client->nfs4_client_id = table->nfs4_ct_next_client_id++;

        memcpy(client->nfs4_client_owner, owner, owner_len);
        client->nfs4_client_owner_len = owner_len;
        client->nfs4_client_verifier  = verifier;
        client->nfs4_client_refcnt    = 1;

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

void
nfs4_client_unregister(
    struct nfs4_client_table *table,
    uint64_t                  client_id)
{
    struct nfs4_client *client;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &client_id, sizeof(client_id), client);

    if (client) {
        chimera_nfs_info("NFS4 Unregistered client %lu", client_id);
        HASH_DELETE(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner,
                    client);
        HASH_DELETE(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id, client);
        free(client);
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);

} /* nfs4_client_unregister */

struct nfs4_session *
nfs4_create_session(
    struct nfs4_client_table    *table,
    uint64_t                     client_id,
    uint32_t                     implicit,
    const struct channel_attrs4 *fore_attrs,
    const struct channel_attrs4 *back_attrs)
{
    struct nfs4_client  *client  = NULL;
    struct nfs4_session *session = NULL;
    char                 session_id_str[80];
    uint32_t             i;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &client_id, sizeof(client_id), client);

    if (client) {
        session = calloc(1, sizeof(*session));

        uuid_generate(session->nfs4_session_id);

        session->nfs4_session_implicit = implicit;
        session->nfs4_session_clientid = client_id;

        for (i = 0; i < NFS4_SESSION_MAX_STATE; i++) {
            session->free_slot[i] = NFS4_SESSION_MAX_STATE - (i + 1);
        }
        session->num_free_slots = NFS4_SESSION_MAX_STATE;

        if (fore_attrs) {
            session->nfs4_session_fore_attrs = *fore_attrs;
        }

        if (back_attrs) {
            session->nfs4_session_back_attrs = *back_attrs;
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

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    return session;
} /* nfs4_session_lookup */

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
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    if (session) {
        free(session);
    }

} /* nfs4_destroy_session */