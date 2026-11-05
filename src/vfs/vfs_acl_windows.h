// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/export.h"
#include "sdk/vfs_acl.h"

struct chimera_vfs_attrs;
struct chimera_vfs_request;

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
