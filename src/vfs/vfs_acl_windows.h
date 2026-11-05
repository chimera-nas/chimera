// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/export.h"
#include "sdk/vfs_acl.h"

struct chimera_vfs_attrs;
struct chimera_vfs_request;
struct chimera_vfs_thread;

/*
 * Strip to SID-only: every USER/GROUP principal carrying a native SID becomes
 * CHIMERA_PRINCIPAL_SID (id dropped); every USER/GROUP without one gets the
 * algorithmic modefromsid SID (S-1-5-88-1-<uid> / S-1-5-88-2-<gid>) encoded
 * into a CHIMERA_PRINCIPAL_SID; SPECIAL and PRINCIPAL_SID pass through.
 * Returns the number of principals that were numeric-only (callers that want
 * native SIDs resolve those BY_UID/BY_GID first and call this after).
 */
SYMBOL_EXPORT int chimera_vfs_acl_windows_strip(
    struct chimera_acl *acl);

/*
 * Synchronous enrichment: decode every PRINCIPAL_SID whose SID is an
 * algorithmic modefromsid form into USER/GROUP with the numeric id and no
 * stored native SID.  Returns the number of PRINCIPAL_SID entries remaining
 * (native SIDs needing the resolver).
 */
SYMBOL_EXPORT int chimera_vfs_acl_windows_enrich_sync(
    struct chimera_acl *acl);

/*
 * The reply attrs whose ACL the read path enriches: the op's r_attr (for
 * SETATTR, r_post_attr), or NULL for an op that returns no such attrs.
 * Covers every op whose reply attrs a backend fills from the object's
 * metadata, so may carry va_acl when it was requested: getattr, lookup_at,
 * open_at, mkdir_at, mknod_at, symlink_at, create_unlinked, open_stream,
 * link_at, readlink, read and setattr.  readdir is not among them: its
 * entries are delivered by a callback that cannot park (see
 * vfs_proc_readdir.c).  Directory pre/post attrs are not covered either; no
 * caller asks for an ACL in them.
 */
SYMBOL_EXPORT struct chimera_vfs_attrs *
chimera_vfs_acl_windows_reply_attrs(
    struct chimera_vfs_request *request);

/*
 * Called from chimera_vfs_dispatch, after chimera_vfs_notify_gate_install (so
 * this wrapper wraps the notify gate and runs first at completion).  When the
 * request's mount has CHIMERA_VFS_MOUNT_ATTR_WINDOWS_ACL and the op's reply
 * attrs ask for the ACL -- or the op carries a normalized set_attr copy
 * (acl_windows.saved_set_attr) that must be handed back -- swaps the
 * request's completion for one that, on a successful reply:
 *
 *   - decodes algorithmic S-1-5-88 principals in place;
 *   - copies the reply ACL and owner/group SIDs out of module scratch into
 *     request memory if native SIDs remain, then resolves each distinct one
 *     BY_SID, parking until every resolve has answered;
 *   - rewrites each ACE bearing a resolved SID as USER/GROUP with the
 *     resolved id, keeping the native SID; an authoritatively unknown SID
 *     stays an opaque CHIMERA_PRINCIPAL_SID (it matches no caller);
 *   - fails the op with CHIMERA_VFS_EIO if any SID came back UNAVAILABLE --
 *     never a partially resolved ACL -- releasing the backend open first when
 *     the op is one that opened something.
 *
 * Otherwise the request is left untouched: a mount without the flag costs a
 * pointer test, an opcode switch and a mask test, and only an op that asked
 * for the ACL pays the mount-attrs lookup.  Idempotent across re-dispatch.
 */
SYMBOL_EXPORT void
chimera_vfs_acl_windows_gate_install(
    struct chimera_vfs_request *request);

/*
 * Called at the top of chimera_vfs_dispatch, before the notify-gate install.
 * Returns 1 when it has taken the request (parked on resolves, or completed
 * it with an error); it re-enters chimera_vfs_dispatch when done, with
 * request->acl_windows.saved_set_attr non-NULL marking the normalized state
 * so re-entry falls through.  Applies to ops carrying a set_attr:
 * setattr/fsetattr, open_at (create), mkdir_at, mknod_at, symlink_at,
 * create_unlinked -- and open_stream, whose set_attr a module applies to
 * the base file -- on a mount with CHIMERA_VFS_MOUNT_ATTR_WINDOWS_ACL whose
 * set_attr carries an ACL, a uid or a gid.
 *
 * The op is handed a private copy of the set_attr (and its ACL) in request
 * memory; the caller's buffers are never written, and the completion
 * wrapper gives the caller its own set_attr back.  On the copy:
 *
 *   - each USER/GROUP principal without a native SID is resolved BY_UID /
 *     BY_GID (one resolve per distinct id, parking on misses): a resolved
 *     native SID is written into the principal; an authoritative NOT_FOUND
 *     (or an identity with no SID) leaves it numeric;
 *   - a uid (gid) change naming no owner (group) SID companion is resolved
 *     the same way, and the native SID attached as the companion; NOT_FOUND
 *     leaves it absent, which clears a stored one as before;
 *   - then chimera_vfs_acl_windows_strip makes the ACL SID-only: a principal
 *     with a native SID keeps only that, a numeric one gets its algorithmic
 *     SID.  SPECIAL and PRINCIPAL_SID principals pass through untouched.
 *
 * Any UNAVAILABLE answer fails the op with CHIMERA_VFS_EIO without
 * dispatching it.  It runs after the setattr permission gate (which ran on
 * the request as the client sent it, before chimera_vfs_setattr_dispatch
 * reached chimera_vfs_dispatch), so the SIDs attached here are the engine's
 * own derivation and do not face the client-facing owner-SID discipline.
 * Otherwise the request is left untouched: off a flagged mount, or with no
 * relevant set_attr bits, it costs a switch and a mask test (and, for a
 * relevant op on an ACL-native module, the mount-attrs lookup); a read-only
 * mount is left to refuse the op with EROFS.
 */
SYMBOL_EXPORT int
chimera_vfs_acl_windows_pre_dispatch(
    struct chimera_vfs_request *request);

/*
 * Synchronous, cache-only enrichment of one readdir entry's attrs, called per
 * entry when the directory's mount has CHIMERA_VFS_MOUNT_ATTR_WINDOWS_ACL and
 * the entry carries an ACL.  A readdir entry is delivered by a callback that
 * cannot park, so nothing here may block or wait on the resolver: algorithmic
 * S-1-5-88 principals decode in place (enrich_sync), and each remaining
 * native SID is resolved through the identity cache alone -- a positive hit
 * rewrites the ACE as USER/GROUP with the SID kept, a cached authoritative
 * miss leaves it an opaque PRINCIPAL_SID (it matches no caller, as on the
 * getattr path).  A SID the cache knows nothing about is unresolved: the
 * partially resolved ACL never leaves core, so CHIMERA_VFS_ATTR_ACL is
 * cleared from va_set_mask, CHIMERA_VFS_ATTR_ACL_WITHHELD set so a protocol
 * does not synthesise one, and va_acl set NULL (the entry is still
 * delivered), and one fire-and-forget BY_SID resolve per unresolved SID warms
 * the cache for the next pass.  The warm callback touches nothing
 * request-scoped.  Edits are made in place in the module's ACL scratch, which
 * is live only for the duration of the per-entry callback.
 *
 * The _pass form additionally dedupes warms across one readdir pass through
 * `seen`/`nseen` (hashes of SIDs already warmed, capacity `max`); the plain
 * form dedupes within the entry only.
 */
SYMBOL_EXPORT void
chimera_vfs_acl_windows_enrich_dirent(
    struct chimera_vfs_thread *thread,
    struct chimera_vfs_attrs  *attrs);

SYMBOL_EXPORT void
chimera_vfs_acl_windows_enrich_dirent_pass(
    struct chimera_vfs_thread *thread,
    struct chimera_vfs_attrs  *attrs,
    uint64_t                  *seen,
    uint32_t                  *nseen,
    uint32_t                   max);

/* True when `dir_fh`'s mount runs with windows-identity ACLs. */
SYMBOL_EXPORT int
chimera_vfs_acl_windows_fh_flagged(
    struct chimera_vfs_thread *thread,
    const uint8_t             *fh,
    uint32_t                   fh_len);
