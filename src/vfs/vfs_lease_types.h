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
struct chimera_vfs_open_handle;
struct chimera_vfs_caching_grant;

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
    /* True when this actor holds/requests an SMB2 RqLs lease (owner_lo/hi carry
     * the 16-byte LeaseKey).  False for a plain open (owner_lo/hi carry the
     * file_id) and for non-SMB actors.  Lets a same-client *non-lease* open skip
     * recalling that client's own caching lease in break_caching_for_open, while
     * a 2nd LeaseKey from the same client still breaks (MS-SMB2 SameLeaseKey). */
    uint8_t                         is_lease;
    chimera_vfs_lease_break_cb_t    break_cb;
    chimera_vfs_lease_is_alive_cb_t is_alive_cb;
    chimera_vfs_lease_revoked_cb_t  revoked_cb;
    void                           *cb_private;
    /* The open handle this lease is anchored to, when the holder is a single
     * open (SMB oplock/lease).  A metadata mutation (setattr / set-EOF) issued
     * through this very handle must NOT recall this lease -- the holder is
     * coherent with its own change -- so chimera_vfs_break_caching_file skips a
     * lease whose op_handle matches the mutating request's handle.  NULL for
     * holders not tied to a single handle (e.g. NFSv4 delegations), which are
     * always recalled. */
    struct chimera_vfs_open_handle *op_handle;
};

enum chimera_vfs_break_state {
    CHIMERA_VFS_BREAK_IDLE     = 0, /* no break in progress */
    CHIMERA_VFS_BREAK_BREAKING = 1, /* break_cb invoked, awaiting ack */
    CHIMERA_VFS_BREAK_ACKED    = 2, /* protocol acked, lease downgraded */
    CHIMERA_VFS_BREAK_REVOKED  = 3, /* forcibly revoked (timeout or error) */
};

struct chimera_vfs_lease {
    enum chimera_vfs_lease_kind kind;
    struct chimera_vfs_lease_mode     mode;
    uint64_t                          offset; /* RANGE only; SHARE/CACHING use 0 */
    uint64_t                          length; /* RANGE only; 0 = to EOF */
    struct chimera_vfs_lease_owner    owner;
    struct chimera_vfs_file_state    *file;

    enum chimera_vfs_break_state break_state;
    /* The level the current outstanding break notification asked the holder to
     * downgrade to -- ONE caching bit below its granted mode (W, then H, then R
     * in priority order).  A lease never jumps straight to its floor: each
     * conflicting open/write drives the holder down a single bit, and the next
     * step is sent only after the client acks the previous one (MS-SMB2
     * 3.3.5.9.x cascading break, e.g. RWH -> RH -> R -> NONE). */
    uint8_t                           break_needed_mode;
    /* The ultimate target of the in-flight cascade: the maximal mode the holder
     * may keep given every current waiter.  NONE for a writing/truncating open
     * or a namespace mutation; R for a read-only conflicting open; R|H for a
     * coexisting caching lease (R and H are shared).  The cascade keeps
     * break_state == BREAKING and re-fires one bit per ack until granted reaches
     * this floor, at which point the break completes. */
    uint8_t                           break_floor;
    uint64_t                          break_deadline; /* stopwatch ticks */

    /* For a SHARE probe only: a caching (handle) lease held under this same
     * key is the requester's own lease (SMB2 same-client, same lease key) and
     * must NOT be broken when acquiring the share — the opens coalesce.  Set by
     * the SMB server when a lease-bearing open takes its share reservation;
     * left zero (no skip) by every other caller. */
    uint8_t                           has_break_skip_key;
    uint64_t                          break_skip_lo;
    uint64_t                          break_skip_hi;

    /* For a CACHING lease that is owned by a VFS caching grant (the shared,
     * owner-keyed, refcounted object that lets N opens under one owner share a
     * single lease): back-pointer to that grant, so begin_break/ack/revoke can
     * route through it.  NULL for RANGE/SHARE leases, the implicit lease, and
     * any CACHING lease not (yet) managed by a grant. */
    struct chimera_vfs_caching_grant *grant;

    /* Intrusive linkage on the appropriate file->{range,share,caching} list. */
    struct chimera_vfs_lease         *prev;
    struct chimera_vfs_lease         *next;
};

/* -------------------------------------------------------------------- */
/* Caching grant — VFS-owned shared caching lease                       */
/* -------------------------------------------------------------------- */

/*
 * A CACHING lease (SMB oplock/lease, NFSv4 delegation) is logically ONE grant
 * per (file, owner) shared by every open that holds that owner key.  Unlike
 * RANGE/SHARE leases — which are genuinely per-instance and stay embedded in
 * the protocol object — a caching grant is allocated and lifetime-managed by
 * the VFS layer and reference-counted across the opens that share it.  This
 * generalizes what the NFSv4 server already does by hand for delegations (one
 * nfs_delegation per (client, FH)) so SMB and NFSv4 share one implementation.
 *
 * The grant WRAPS a chimera_vfs_lease: the embedded lease is what the conflict
 * matrix links onto file->caching_leases and walks, so the matrix/break state
 * machine are unchanged.  The grant adds owner-keyed lookup, the open refcount,
 * and the SMB lease epoch.
 */
struct chimera_vfs_caching_grant {
    struct chimera_vfs_lease          lease;      /* kind=CACHING; the matrix node */
    struct chimera_vfs_file_state    *file;       /* owning file (back-ptr) */
    uint32_t                          refcount;   /* # of opens referencing this grant */
    uint32_t                          epoch;      /* SMB lease epoch (3.3.5.9.11) */
    /* True for a legacy SMB oplock (LEVEL_II/EXCLUSIVE/BATCH), false for an SMB2
     * RqLs lease.  A legacy batch oplock's handle is broken BEFORE the share-mode
     * check so the holder can close and dissolve a sharing conflict; an RqLs lease
     * keeps its handle cache on a conflicting open (only its write cache is
     * exclusive).  The break-on-open path uses this to handle-break only legacy
     * oplocks.  Unused by NFSv4 (its grants are delegations, never oplocks). */
    uint8_t                           is_oplock;
    /* True for an SMB2.1+ lease v2 (RqLs v2, carries an epoch).  A v1 lease and a
     * legacy oplock do not version their state, so their break notifications carry
     * epoch 0 and `epoch` is not advanced for them. */
    uint8_t                           is_v2;
    /* Set while this grant's lease is mid-break and the break requires a client
     * acknowledgment (write or handle caching was stripped).  A no-ack break (only
     * read caching dropped, or break to NONE) leaves this clear so an open does not
     * park waiting for an ack that never comes. */
    uint8_t                           break_ack_required;
    struct chimera_vfs_caching_grant *grant_next; /* link on file->caching_grants */
    /* Protocol holder list — opaque to the VFS; the protocol server threads its
     * per-open holder objects through here so a break callback can select a LIVE
     * holder to notify (e.g. SMB picks an open whose channel is still connected;
     * if none is live the lease is revoked).  Manipulated by the protocol under
     * file->lock.  NFSv4 leaves this NULL (member-set-1; cb_private points at the
     * delegation directly). */
    void                             *holders;
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
