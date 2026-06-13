// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

#include "nfs4_xdr.h"

struct chimera_vfs_thread;
struct chimera_server_nfs_thread;
struct nfs4_session;
struct nfs4_client_principal;

/*
 * Persistent NFSv4.1 reply cache (DRC), gated by server.nfs4_drc.
 *
 * Write path (hot, write-only): when a persistent session caches a reply,
 * nfs4_drc_persist_reply write-throughs {sessionid,slot,seqid -> bytes} to the
 * KV store; nfs4_drc_delete_reply drops the prior seqid on slot advance.  A
 * session-metadata record (nfs4_drc_persist_session) lets the cold-start
 * reload rebuild the session -- with its original sessionid -- and its owning
 * client, so a post-restart retransmit resolves and replays.  None of these
 * read the KV store.
 *
 * Read path (cold start only): nfs4_drc_reload scans the session + reply bands
 * and repopulates the in-memory tables.  Driven from the recovery cold-start
 * load once the client identity set is in place.
 */

/* Write-through one cached reply (fire-and-forget; copies the bytes). */
void
nfs4_drc_persist_reply(
    struct chimera_vfs_thread *vfs_thread,
    uint16_t                   node_id,
    struct nfs4_session       *session,
    uint32_t                   slotid,
    uint32_t                   seqid,
    const void                *buf,
    uint32_t                   len);

/* Delete one persisted reply (fire-and-forget). */
void
nfs4_drc_delete_reply(
    struct chimera_vfs_thread *vfs_thread,
    uint16_t                   node_id,
    const uint8_t             *sessionid,
    uint32_t                   slotid,
    uint32_t                   seqid);

/* Persist a session's metadata so it can be reconstructed at cold start.
 * Reads owner/verifier/clientid from session->client_unified; principal and
 * channel attrs come from the CREATE_SESSION that established it.  node_id
 * scopes the record to this server instance in a shared store. */
void
nfs4_drc_persist_session(
    struct chimera_vfs_thread          *vfs_thread,
    uint16_t                            node_id,
    struct nfs4_session                *session,
    const struct nfs4_client_principal *principal,
    uint32_t                            cb_program,
    uint32_t                            flags);

/* Delete a session's metadata + all of its reply entries (DESTROY_SESSION). */
void
nfs4_drc_forget_session(
    struct chimera_vfs_thread *vfs_thread,
    uint16_t                   node_id,
    const uint8_t             *sessionid);

/* Cold-start reload: reconstruct persistent sessions (+ their owning client
 * records) and repopulate slot reply caches from the KV store.  `done` is
 * invoked (with `done_arg`) once the whole reload has completed, so the caller
 * can flip recovery to READY only after reconstruction is in place. */
typedef void (*nfs4_drc_reload_done_t)(
    void *arg);

void
nfs4_drc_reload(
    struct chimera_server_nfs_thread *thread,
    nfs4_drc_reload_done_t            done,
    void                             *done_arg);

/* ----------------------------------------------------------------------- *
*  Session-record (de)serialization.  Exposed for unit tests; the wire     *
*  layout is documented in nfs4_drc.c.                                     *
* ----------------------------------------------------------------------- */

struct nfs4_drc_session_record {
    uint64_t              clientid;
    uint64_t              verifier;
    uint32_t              princ_flavor;
    uint32_t              princ_uid;
    uint32_t              princ_gid;
    uint32_t              replay_max_slots;
    uint32_t              replay_maxresp_cached;
    uint32_t              cb_program;
    uint32_t              flags;
    struct channel_attrs4 fore;            /* scalar ca_* fields only */
    struct channel_attrs4 back;
    uint16_t              mach_len;
    uint16_t              owner_len;
    uint8_t               mach[NFS4_OPAQUE_LIMIT];
    uint8_t               owner[NFS4_OPAQUE_LIMIT];
};

/* Returns bytes written (0 on overflow). */
uint32_t
nfs4_drc_session_serialize(
    uint8_t                              *buf,
    uint32_t                              buf_size,
    const struct nfs4_drc_session_record *rec);

/* Returns 0 on success, -1 on a bad/short buffer. */
int
nfs4_drc_session_deserialize(
    const uint8_t                  *buf,
    uint32_t                        len,
    struct nfs4_drc_session_record *out);

/* Reply record: {seqid, len, bytes}.  serialize returns bytes written (0 on
 * overflow); parse returns 0 and points data at the in-buffer payload. */
uint32_t
nfs4_drc_reply_serialize(
    uint8_t    *buf,
    uint32_t    buf_size,
    uint32_t    seqid,
    const void *data,
    uint32_t    data_len);

int
nfs4_drc_reply_parse(
    const uint8_t  *buf,
    uint32_t        len,
    uint32_t       *out_seqid,
    const uint8_t **out_data,
    uint32_t       *out_data_len);

struct nfs4_client_table;

/* Reconstruct a persistent session (and its owning confirmed client, with the
 * persisted clientid) into `table`, keyed by the original `sessionid`.  Exposed
 * for the cold-start reload and for unit tests (test_nfs_persist). */
void
nfs4_drc_reconstruct_session(
    struct nfs4_client_table             *table,
    const uint8_t                        *sessionid,
    const struct nfs4_drc_session_record *rec,
    uint64_t                              boot_id);

/* Repopulate one reply-cache slot from persisted bytes (CACHED state) so a
 * post-restart retransmit on {slotid,seqid} replays. */
void
nfs4_drc_repopulate_slot(
    struct nfs4_session *session,
    uint32_t             slotid,
    uint32_t             seqid,
    const void          *data,
    uint32_t             len);
