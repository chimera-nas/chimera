// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>
#include <uthash.h>

#include "vfs/vfs.h"
#include "vfs/vfs_cred.h"
#include "vfs/vfs_state.h"
#include "smb2.h"

struct chimera_smb_share;
struct chimera_smb_conn;

struct chimera_smb_file_id {
    uint64_t pid;
    uint64_t vid;
};

#define CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY       0x00000001
#define CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE 0x00000002
#define CHIMERA_SMB_OPEN_FILE_CLOSED               0x00000004
/* Open survived its owning connection's teardown and is parked in the shared
 * durable-handle table awaiting reconnect or expiry.  While set, the open is
 * not present in any tree's open_files[] hash and is owned solely by the
 * durable table (refcnt == 1). Cleared when a reconnect re-homes it. */
#define CHIMERA_SMB_OPEN_FILE_PARKED               0x00000008
/* A persistent-handle record for this open was written to the share's backend
 * (atomically with the open).  On final close the record must be deleted from
 * that backend so a restart doesn't resurrect a closed handle. */
#define CHIMERA_SMB_OPEN_FILE_PERSISTED            0x00000010
/* The client explicitly set the LastWriteTime on this handle (a real value, not
 * an omit/freeze sentinel).  Per MS-FSA, the handle has "taken control" of the
 * write time: subsequent implicit updates from this handle (data writes,
 * EndOfFile/Allocation sets) must not advance it for the life of the open. */
#define CHIMERA_SMB_OPEN_FILE_WRITE_TIME_STICKY    0x00000020
/* This open targets a named stream (SMB ADS).  open_file->handle is the VFS
 * stream handle; stream_name + base_fh identify the stream for enumeration and
 * delete-on-close (which removes only the stream, not the base file). */
#define CHIMERA_SMB_OPEN_FILE_FLAG_STREAM          0x00000040

/* Bits identifying which CREATE contexts a client supplied on the open. Mirrored
 * from request->create.ctx_present_mask into the open file so later phases
 * (lease break, durable reconnect) can tell which contracts the client expects. */
#define CHIMERA_SMB_CREATE_CTX_SECD                (1u << 0)
#define CHIMERA_SMB_CREATE_CTX_EXTA                (1u << 1)
#define CHIMERA_SMB_CREATE_CTX_DHNQ                (1u << 2)
#define CHIMERA_SMB_CREATE_CTX_DHNC                (1u << 3)
#define CHIMERA_SMB_CREATE_CTX_DH2Q                (1u << 4)
#define CHIMERA_SMB_CREATE_CTX_DH2C                (1u << 5)
#define CHIMERA_SMB_CREATE_CTX_RQLS                (1u << 6)
#define CHIMERA_SMB_CREATE_CTX_RQLS_V2             (1u << 7)
#define CHIMERA_SMB_CREATE_CTX_MXAC                (1u << 8)
#define CHIMERA_SMB_CREATE_CTX_TWRP                (1u << 9)
#define CHIMERA_SMB_CREATE_CTX_QFID                (1u << 10)
#define CHIMERA_SMB_CREATE_CTX_ALSI                (1u << 11)
/* App-instance contexts (SMB2_CREATE_APP_INSTANCE_ID / *_VERSION) use 16-byte
 * GUID names, matched in a dedicated name_len==16 branch of the context parser.
 * CTX_APP marks that an AppInstanceId was supplied (drives the version-gated
 * force-close in MS-SMB2 3.3.5.9.7); CTX_APP_VERSION marks that an
 * AppInstanceVersion (3.3.5.9.16) accompanied it. */
#define CHIMERA_SMB_CREATE_CTX_APP                 (1u << 12)
#define CHIMERA_SMB_CREATE_CTX_APP_VERSION         (1u << 13)

/* Bits for chimera_smb_open_file.durable_flags */
#define CHIMERA_SMB_DURABLE_V1                     (1u << 0)
#define CHIMERA_SMB_DURABLE_V2                     (1u << 1)
#define CHIMERA_SMB_DURABLE_PERSISTENT             (1u << 2)

enum chimera_smb_open_file_type {
    CHIMERA_SMB_OPEN_FILE_TYPE_FILE,
    CHIMERA_SMB_OPEN_FILE_TYPE_PIPE,
};

enum chimera_smb_pipe_magic {
    CHIMERA_SMB_OPEN_FILE_LSA_RPC,
};

struct chimera_smb_request;

typedef int (*chimera_smb_pipe_transceive_t)(
    struct chimera_smb_request *request,
    struct evpl_iovec          *input_iov,
    int                         input_niov,
    struct evpl_iovec          *output_iov);

struct chimera_smb_notify_state;
struct chimera_smb_lock_entry;

struct chimera_smb_open_file {
    enum chimera_smb_open_file_type   type;
    enum chimera_smb_pipe_magic       pipe_magic;
    chimera_smb_pipe_transceive_t     pipe_transceive;
    struct UT_hash_handle             hh;
    struct chimera_smb_file_id        file_id;
    struct chimera_vfs_open_handle   *handle;
    uint32_t                          desired_access;
    /* Access mask actually granted on this handle (FileAccessInformation).
     * Resolved from the object ACL at open: equals desired_access for a
     * specific-bits open, or the maximal available set for MAXIMUM_ALLOWED. */
    uint32_t                          granted_access;
    /* The caller's full effective rights against the object's ACL, regardless
     * of what was requested -- reported in the MxAc create-context response. */
    uint32_t                          maximal_access;
    uint32_t                          share_access;
    uint32_t                          name_len;
    uint32_t                          flags;
    uint64_t                          position;
    uint32_t                          parent_fh_len;
    uint32_t                          refcnt;
    /* MS-SMB2 §3.3.5.2.10 channel-sequence tracking.  channel_sequence holds
     * the highest ChannelSequence observed on this Open; a mutating op
     * (WRITE/SET_INFO/IOCTL) carrying a stale sequence -- behind by
     * [0x8000..0xFFFF] mod 0x10000 -- is rejected with FILE_NOT_AVAILABLE.
     * READ never rejects but advances the value.  _valid gates the first op. */
    uint16_t                          channel_sequence;
    uint8_t                           channel_sequence_valid;
    /* The CREATE Action reported when this handle was first opened
     * (CREATED/OPENED/...); replayed verbatim on a DH2Q create_guid replay. */
    uint32_t                          create_action;
    /* Phase-0 plumbing: fields populated by later phases. Zeroed on alloc. */
    uint32_t                          ctx_present_mask;
    uint8_t                           oplock_level;
    /* Set when an outstanding legacy oplock break to this handle expects an
     * acknowledgment (it broke an exclusive/batch oplock).  A break from
     * LEVEL_II to NONE is NOT acknowledged; a client ack for one is a protocol
     * error (MS-SMB2 -> NT_STATUS_INVALID_OPLOCK_PROTOCOL). */
    uint8_t                           oplock_break_ack_required;
    uint8_t                           lease_state;
    uint16_t                          lease_epoch;
    uint32_t                          lease_flags;
    uint32_t                          durable_flags;
    uint64_t                          durable_timeout_ms;
    /* FSCTL_LMR_REQUEST_RESILIENCY (MS-SMB2 2.2.31.3 / 3.3.5.15.9): set once a
     * client has been granted handle resiliency on this open.  resilient_timeout_ms
     * is the (server-capped) duration the open survives a network disconnect for
     * reclaim-by-reconnect. */
    bool                              resilient;
    uint64_t                          resilient_timeout_ms;
    uint8_t                           lease_key[16];
    uint8_t                           parent_lease_key[16];
    uint8_t                           create_guid[16];
    /* SMB2_CREATE_APP_INSTANCE_ID / *_VERSION (MS-SMB2 2.2.13.2.13/14).
     * Recorded when the open carried an AppInstanceId so a later CREATE on a
     * different connection can match it and apply the version-gated
     * force-close rule (3.3.5.9.7 / 3.3.5.9.16). */
    uint8_t                           app_instance_id[16];
    uint64_t                          app_version_high;
    uint64_t                          app_version_low;
    uint8_t                           app_version_present;
    struct chimera_smb_open_file     *next;
    struct chimera_smb_notify_state  *notify_state;
    /* Byte-range locks (SMB2_LOCK) held against this open.  Allocated
     * and linked on each granted lock op; freed on UNLOCK or on close. */
    struct chimera_smb_lock_entry    *lock_entries;
    /* SHARE lease (whole-file deny mode reservation) held by this open
     * once CREATE succeeds.  Released at close. */
    struct chimera_vfs_lease          share_lease;
    struct chimera_vfs_file_state    *share_file_state;
    bool                              share_lease_inserted;
    /* CACHING lease (SMB2 lease / oplock) held by this open.  The lease is now a
     * VFS-owned, owner-keyed, refcounted grant (chimera_vfs_caching_grant) that
     * may be shared by several opens under one lease key; this open holds one
     * reference, dropped at close or on OPLOCK_BREAK ack with mode=0.  NULL if
     * the open holds no caching lease.  caching_file_state holds this open's
     * reference on the per-file state (balanced separately from the grant ref). */
    struct chimera_vfs_caching_grant *grant;
    struct chimera_vfs_file_state    *caching_file_state;
    bool                              caching_lease_inserted;
    /* Intrusive link on grant->holders: every open referencing a shared caching
     * grant is threaded here so a break callback can pick a live member to notify
     * (the grant may outlive the open that created it once opens coalesce).
     * Manipulated under the grant's file->lock. */
    struct chimera_smb_open_file     *grant_member_next;
    /* Conn on which this open was created — used by the break path to
     * send unsolicited OPLOCK_BREAK Notifications.  Cleared by the
     * disconnect handler before the conn is freed; if NULL when a
     * break is needed, the lease is forcibly revoked instead. */
    struct chimera_smb_conn          *create_conn;
    /* Tree this open is hashed into (tree->open_files[]).  Lets an
     * AppInstanceId force-close locate and unhash the conflicting open
     * directly from the lease back-reference. */
    struct chimera_smb_tree          *tree;
    uint8_t                           parent_fh[CHIMERA_VFS_FH_SIZE];
    char                              name[SMB_FILENAME_MAX];
    uint16_t                          pattern[SMB_FILENAME_MAX];
    /* Named-stream (ADS) identity, valid when CHIMERA_SMB_OPEN_FILE_FLAG_STREAM
     * is set.  base_fh is the file the stream hangs off (open_file->handle's fh
     * points at the stream itself). */
    uint16_t                          stream_name_len;
    char                              stream_name[SMB_FILENAME_MAX];
    uint16_t                          base_fh_len;
    uint8_t                           base_fh[CHIMERA_VFS_FH_SIZE];
};

#define CHIMERA_SMB_OPEN_FILE_BUCKETS     256
#define CHIMERA_SMB_OPEN_FILE_BUCKET_MASK (CHIMERA_SMB_OPEN_FILE_BUCKETS - 1)

enum chimera_smb_tree_type {
    CHIMERA_SMB_TREE_TYPE_PIPE,
    CHIMERA_SMB_TREE_TYPE_SHARE,
};

struct chimera_smb_tree {
    enum chimera_smb_tree_type    type;
    uint32_t                      tree_id;
    uint32_t                      refcnt;
    uint64_t                      next_file_id;
    struct chimera_smb_share     *share;

    struct chimera_smb_open_file *open_files[CHIMERA_SMB_OPEN_FILE_BUCKETS];
    pthread_mutex_t               open_files_lock[CHIMERA_SMB_OPEN_FILE_BUCKETS];

    struct chimera_smb_tree      *prev;
    struct chimera_smb_tree      *next;

    unsigned int                  fh_len;
    struct timespec               fh_expiration;
    uint8_t                       fh[CHIMERA_VFS_FH_SIZE];
};

#define CHIMERA_SMB_SESSION_AUTHORIZED   0x1
/* The session was closed out from under its connection by a SESSION_SETUP that
 * named it in PreviousSessionId (MS-SMB2 3.3.5.5.3).  It is unlinked from the
 * global table but kept alive while its original connection still references it;
 * requests arriving on that connection are answered STATUS_USER_SESSION_DELETED. */
#define CHIMERA_SMB_SESSION_DELETED      0x2
#define CHIMERA_SMB_SESSION_ENCRYPT_DATA 0x4

/* Maximum number of channels a single session may bind, matching the
 * Windows Server 2012R2/2016 limit asserted by smb2.multichannel.num_channels. */
#define SMB2_MAX_CHANNELS                32

struct chimera_smb_session {
    uint64_t                    session_id;
    /* Stable per-CLIENT identity used as the owner key for CACHING leases,
     * SHARE reservations and byte-range locks (chimera_vfs_lease_owner.client_key).
     * Derived from the connection's ClientGuid, NOT the session id, because
     * MS-SMB2 keys leases by the client (3.3.5.9.8): two sessions of the same
     * client (same ClientGuid, e.g. a reconnect or a second channel) share one
     * lease namespace and must coalesce/upgrade rather than conflict.  Falls
     * back to session_id for guid-less clients (pre-3.0 dialects send a zero
     * ClientGuid) so distinct legacy clients are not collapsed onto one key. */
    uint64_t                    client_key;
    uint32_t                    refcnt;
    uint32_t                    flags;
    /* Dialect of the connection this session was established on (MS-SMB2
     * Session.Connection.Dialect).  A PreviousSessionId reconnect whose
     * connection negotiated a different dialect is rejected. */
    uint16_t                    dialect;
    /* Session.SupportsNotifications (MS-SMB2 3.3.5.5): copied from
     * Connection.SupportsNotifications on the SESSION_SETUP that first
     * authorized the session.  A later binding SESSION_SETUP on a connection
     * whose SupportsNotifications differs is rejected (3.1.1 only). */
    uint8_t                     supports_notifications;
    struct UT_hash_handle       hh;
    struct chimera_smb_session *prev;
    struct chimera_smb_session *next;

    pthread_mutex_t             lock;
    struct chimera_smb_tree   **trees;

    int                         max_trees;
    uint8_t                     signing_key[16];

    /* Number of channels (connections) bound to this session, including the
     * primary.  Bounded at SMB2_MAX_CHANNELS so a client cannot bind an
     * unlimited number of channels (MS-SMB2 §3.3.5.5.3 returns
     * STATUS_INSUFFICIENT_RESOURCES once the server's limit is reached).
     * Guarded by shared->sessions_lock. */
    int                         num_channels;

    /* SMB3 transport encryption (set when CHIMERA_SMB_SESSION_ENCRYPT_DATA).
     * enc_key encrypts server->client responses; dec_key decrypts
     * client->server requests.  enc_nonce_counter is the server's strictly
     * monotonic per-session message counter (never reused for a key — GCM nonce
     * reuse is catastrophic), shared across all channels of the session. */
    uint8_t                     enc_key[32];
    uint8_t                     dec_key[32];
    size_t                      enc_key_len;
    uint16_t                    cipher_id;
    _Atomic uint64_t            enc_nonce_counter;

    struct chimera_vfs_cred     cred;
};

/* Derive the stable per-client lease owner key from a 16-byte ClientGuid.
 * FNV-1a over the guid bytes; a guid of all zeros (pre-3.0 clients send none)
 * falls back to the unique session_id so distinct legacy clients keep distinct
 * keys rather than colliding on the zero-guid hash.  Never returns 0. */
static inline uint64_t
chimera_smb_lease_client_key(
    const uint8_t *client_guid,
    uint64_t       session_id)
{
    uint64_t h       = 1469598103934665603ULL; /* FNV-1a 64-bit offset basis */
    int      nonzero = 0;

    for (int i = 0; i < 16; i++) {
        nonzero |= client_guid[i];
        h       ^= client_guid[i];
        h       *= 1099511628211ULL;     /* FNV-1a 64-bit prime */
    }

    if (!nonzero) {
        return session_id;
    }
    return h ? h : 1;
} /* chimera_smb_lease_client_key */

static struct chimera_smb_session *
chimera_smb_session_create()
{
    struct chimera_smb_session *session = calloc(1, sizeof(struct chimera_smb_session));

    pthread_mutex_init(&session->lock, NULL);

    session->max_trees = 32;
    session->flags     = 0;

    session->trees = calloc(session->max_trees, sizeof(struct chimera_smb_tree *));

    /* Initialize credentials to root for now.
     * TODO: Map authenticated SMB user to appropriate UID/GID
     */
    chimera_vfs_cred_init_attr(&session->cred, 0, 0, 0, NULL);

    return session;
} /* chimera_smb_session_create */

static void
chimera_smb_session_destroy(struct chimera_smb_session *session)
{
    pthread_mutex_destroy(&session->lock);
    free(session->trees);
    free(session);
} /* chimera_smb_session_release */
