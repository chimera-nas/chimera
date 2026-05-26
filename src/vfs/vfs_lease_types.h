// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <time.h>

/*
 * Unified VFS lease/lock vocabulary.
 *
 * These types are the lingua franca shared between the lease state layer
 * (vfs_state.h) and the VFS request path (vfs.h), so they live in their own
 * header to avoid an include cycle (vfs_state.h includes vfs.h).
 */

struct chimera_vfs_lease;
struct chimera_vfs_file_state;

/* -------------------------------------------------------------------- */
/* Lease vocabulary                                                     */
/* -------------------------------------------------------------------- */

enum chimera_vfs_lease_kind {
    CHIMERA_VFS_LEASE_RANGE   = 0, /* byte-range lock */
    CHIMERA_VFS_LEASE_SHARE   = 1, /* whole-file share/deny reservation */
    CHIMERA_VFS_LEASE_CACHING = 2, /* breakable caching lease */
    CHIMERA_VFS_LEASE_KIND_MAX
};

/* Mode bits — SMB2 RWH triple, used as the lingua franca.
 *   R = read-cache / shared range lock / SMB FILE_READ_DATA share-allow
 *   W = write-cache / exclusive range lock / SMB FILE_WRITE_DATA share-allow
 *   H = handle-cache (SMB only)
 *   D = delete (SMB FILE_SHARE_DELETE)
 *
 * RANGE leases use {R} for shared, {W} for exclusive; H and D are ignored.
 * SHARE leases use granted = access bits, denied = deny bits.
 * CACHING leases use granted directly (e.g., {R,W,H} for an SMB lease).
 */
#define CHIMERA_VFS_LEASE_MODE_R         0x01
#define CHIMERA_VFS_LEASE_MODE_W         0x02
#define CHIMERA_VFS_LEASE_MODE_H         0x04
#define CHIMERA_VFS_LEASE_MODE_D         0x08

struct chimera_vfs_lease_mode {
    uint8_t granted; /* bits the holder has been granted */
    uint8_t denied;  /* SHARE only: bits this holder denies to others */
};

/* Protocol identifiers — used in owner.protocol so the conflict matrix
 * can apply protocol-specific same-owner coalescing rules. */
#define CHIMERA_VFS_LEASE_PROTO_NLM      1
#define CHIMERA_VFS_LEASE_PROTO_NFSV4    2
#define CHIMERA_VFS_LEASE_PROTO_SMB2     3
/* Chimera itself, acquiring an implicit lease on behalf of an actor that
 * does not (or cannot) request a protocol lease — e.g. an NFSv3 write, an
 * S3 PUT, or NFSv4 data I/O through a plain stateid.  The lease is held by
 * the server and is itself breakable (drained, then dropped) when another
 * holder needs it or it goes idle. */
#define CHIMERA_VFS_LEASE_PROTO_INTERNAL 4

/* Break callback — invoked synchronously by vfs_state when this lease must
 * downgrade or release.  The callback is expected to kick off the protocol-
 * specific break (build SMB2 OPLOCK_BREAK, send NFSv4 CB_RECALL) and return
 * promptly; vfs_state does not wait inside the callback.  The protocol
 * server eventually calls chimera_vfs_lease_ack() (or chimera_vfs_lease_
 * revoke() on its own timeout) to unstick the pending acquire. */
typedef void (*chimera_vfs_lease_break_cb_t)(
    struct chimera_vfs_lease *lease,
    uint8_t                   needed_mode,
    void                     *private_data);

/* Liveness probe — vfs_state calls this to test whether a lease's owning
 * client/session is still considered alive.  Returns false on session
 * expiry, connection drop past grace, etc.  May be NULL (always alive). */
typedef bool (*chimera_vfs_lease_is_alive_cb_t)(
    const struct chimera_vfs_lease *lease,
    void                           *private_data);

/* Revocation notice — vfs_state calls this when it forcibly revokes a lease
 * (recall deadline elapsed, etc.) rather than the holder releasing it.  The
 * protocol layer uses it to mark its own state revoked (e.g. so a later use of
 * an NFSv4 delegation stateid reports NFS4ERR_DELEG_REVOKED).  May be NULL.
 * Invoked outside the file lock. */
typedef void (*chimera_vfs_lease_revoked_cb_t)(
    struct chimera_vfs_lease *lease,
    void                     *private_data);

struct chimera_vfs_lease_owner {
    uint32_t                        protocol;
    uint64_t                        client_key;
    uint64_t                        owner_lo;
    uint64_t                        owner_hi;
    chimera_vfs_lease_break_cb_t    break_cb;
    chimera_vfs_lease_is_alive_cb_t is_alive_cb;
    chimera_vfs_lease_revoked_cb_t  revoked_cb;
    void                           *cb_private;
};

enum chimera_vfs_break_state {
    CHIMERA_VFS_BREAK_IDLE     = 0, /* no break in progress */
    CHIMERA_VFS_BREAK_BREAKING = 1, /* break_cb invoked, awaiting ack */
    CHIMERA_VFS_BREAK_ACKED    = 2, /* protocol acked, lease downgraded */
    CHIMERA_VFS_BREAK_REVOKED  = 3, /* forcibly revoked (timeout or error) */
};

struct chimera_vfs_lease {
    enum chimera_vfs_lease_kind kind;
    struct chimera_vfs_lease_mode  mode;
    uint64_t                       offset; /* RANGE only; SHARE/CACHING use 0 */
    uint64_t                       length; /* RANGE only; 0 = to EOF */
    struct chimera_vfs_lease_owner owner;
    struct chimera_vfs_file_state *file;

    enum chimera_vfs_break_state break_state;
    uint8_t                        break_needed_mode;
    struct timespec                break_deadline;

    /* For a SHARE probe only: a caching (handle) lease held under this same
     * key is the requester's own lease (SMB2 same-client, same lease key) and
     * must NOT be broken when acquiring the share — the opens coalesce.  Set by
     * the SMB server when a lease-bearing open takes its share reservation;
     * left zero (no skip) by every other caller. */
    uint8_t                        has_break_skip_key;
    uint64_t                       break_skip_lo;
    uint64_t                       break_skip_hi;

    /* Intrusive linkage on the appropriate file->{range,share,caching} list. */
    struct chimera_vfs_lease      *prev;
    struct chimera_vfs_lease      *next;
};

/* -------------------------------------------------------------------- */
/* Lease result enum                                                    */
/* -------------------------------------------------------------------- */

/* Conflict-test result.  Returned by chimera_vfs_state_would_conflict(),
 * chimera_vfs_state_try_insert(), and chimera_vfs_lease_test(). */
enum chimera_vfs_lease_result {
    CHIMERA_VFS_LEASE_GRANTED  = 0, /* no conflict; lease would be / was inserted */
    CHIMERA_VFS_LEASE_DENIED   = 1, /* hard conflict with non-breakable holder */
    CHIMERA_VFS_LEASE_BREAKING = 2, /* breakable holder must downgrade first */
};

/* -------------------------------------------------------------------- */
/* Async-acquire ticket                                                 */
/* -------------------------------------------------------------------- */

/* Result of an async acquire.  GRANTED carries `granted_lease`; DENIED
 * carries `conflict` (a pointer to a conflicting holder — owned by
 * vfs_state, valid only until the callback returns). */
typedef void (*chimera_vfs_lease_acquire_cb_t)(
    enum chimera_vfs_lease_result result,
    struct chimera_vfs_lease     *granted_lease,
    struct chimera_vfs_lease     *conflict,
    void                         *private_data);

/* Caller-allocated ticket holding pending-acquire bookkeeping.  Its
 * lifetime must extend until the acquire callback fires.  Protocol
 * layers typically embed this in their existing per-lock state struct
 * (nlm_lock_entry, nfs4_state, smb_open_file's lock slot). */
struct chimera_vfs_pending_acquire {
    struct chimera_vfs_lease           *lease;
    chimera_vfs_lease_acquire_cb_t      cb;
    void                               *private_data;
    struct chimera_vfs_file_state      *file;
    bool                                queued;
    struct chimera_vfs_pending_acquire *prev;
    struct chimera_vfs_pending_acquire *next;
};
