// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Central access-control gate.
 *
 * A single implementation of "may this credential perform these operations on
 * this object" that every protocol server shares, so NFSv3, NFSv4 and SMB2 all
 * reach identical decisions regardless of which protocol stored the ACL.  The
 * caller supplies attributes it has already fetched (mode, uid, gid, and -- if
 * CHIMERA_VFS_ATTR_ACL is set -- the canonical ACL via va_acl); this routine
 * evaluates them with the shared engine in vfs_acl.c.
 */

#include <stdint.h>

#include "vfs_error.h"

struct chimera_vfs_attrs;
struct chimera_vfs_cred;
struct chimera_vfs_thread;

/*
 * Return the subset of `requested` (canonical CHIMERA_ACE_* mask bits) that is
 * granted to `cred` for the object described by `attr`.  If `attr` carries a
 * native ACL it is evaluated; otherwise the decision falls back to the POSIX
 * mode bits.  Callers test `(granted & requested) == requested` for a hard
 * allow, or inspect individual bits (e.g. for an NFS ACCESS reply).
 */
uint32_t chimera_vfs_access_check(
    const struct chimera_vfs_attrs *attr,
    const struct chimera_vfs_cred  *cred,
    uint32_t                        requested);

/* Convenience: non-zero if every bit in `requested` is granted. */
static inline int
chimera_vfs_access_allowed(
    const struct chimera_vfs_attrs *attr,
    const struct chimera_vfs_cred  *cred,
    uint32_t                        requested)
{
    return chimera_vfs_access_check(attr, cred, requested) == requested;
} /* chimera_vfs_access_allowed */

/*
 * Should the central VFS gate enforce the canonical ACL for an operation on a
 * `module_capabilities` backend by `cred`?  Returns 0 (skip -- no fetch, no
 * check) when the backend delegates DAC to a real enforcer
 * (CHIMERA_VFS_CAP_DELEGATES_DAC), or when the credential is exempt anyway
 * (AUTH_NONE, or root): in those cases the engine would grant unconditionally,
 * so the wrapper can avoid the pre-step attr/ACL fetch entirely.  Returns 1
 * when the engine is the sole authority and must run.
 */
int chimera_vfs_gate_needed(
    uint64_t                       module_capabilities,
    const struct chimera_vfs_cred *cred);

/*
 * Like chimera_vfs_gate_needed() but for DAC that the kernel cannot enforce on
 * a handle-based passthrough backend: it ignores CHIMERA_VFS_CAP_DELEGATES_DAC
 * and returns 1 for any non-root UNIX credential.  Used for path-prefix search
 * (EXECUTE) during lookup and for link/rename destination-directory WRITE,
 * which a passthrough backend resolves via open_by_handle_at and therefore
 * never traverses under the kernel's DAC.  Root and AUTH_NONE remain exempt.
 */
int chimera_vfs_gate_needed_dac(
    const struct chimera_vfs_cred *cred);

/*
 * The enforcement decision itself: CHIMERA_VFS_OK if every bit in `required`
 * (canonical CHIMERA_ACE_* mask) is granted to `cred` on the object described
 * by `attr` (which must carry mode/uid/gid and, for a natively-stored ACL, the
 * ACL via va_acl), else CHIMERA_VFS_EACCES.  Callers gate on
 * chimera_vfs_gate_needed() first and fetch attrs+ACL before calling this.
 */
enum chimera_vfs_error chimera_vfs_gate(
    const struct chimera_vfs_attrs *attr,
    const struct chimera_vfs_cred  *cred,
    uint32_t                        required);

/*
 * Decide whether `cred` may delete the entry described by `child_attr` from the
 * directory described by `parent_attr`, combining the NFSv4/Windows
 * DELETE / DELETE_CHILD rule with POSIX sticky-bit (S_ISVTX) ownership.  Both
 * attrs must carry mode/uid (+ ACL where present); `child_attr` may be NULL when
 * only the parent's DELETE_CHILD grant matters.  Returns non-zero to allow.
 */
int chimera_vfs_delete_allowed(
    const struct chimera_vfs_attrs *parent_attr,
    const struct chimera_vfs_attrs *child_attr,
    const struct chimera_vfs_cred  *cred);

/*
 * Async enforcement pre-steps for the namespace wrappers.  Each is a no-op
 * (completes OK without any backend I/O) when chimera_vfs_gate_needed() is false
 * for the target backend/cred; otherwise it fetches the object's attrs+ACL,
 * evaluates the engine, and completes with CHIMERA_VFS_OK or CHIMERA_VFS_EACCES
 * (or a fetch error).  The wrapper resumes its real operation from the callback.
 */
typedef void (*chimera_vfs_gate_callback_t)(
    enum chimera_vfs_error status,
    void                  *private_data);

/* Require `required` (CHIMERA_ACE_* mask) on the object named by `fh`. */
void chimera_vfs_gate_fh(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    uint32_t                       required,
    chimera_vfs_gate_callback_t    callback,
    void                          *private_data);

/* As chimera_vfs_gate_fh(), but enforced even for DELEGATES_DAC (passthrough)
 * backends -- for DAC the kernel cannot see on handle-based lookups (path-prefix
 * search, link/rename destination-directory write). */
void chimera_vfs_gate_fh_dac(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    uint32_t                       required,
    chimera_vfs_gate_callback_t    callback,
    void                          *private_data);

/* Authorize deleting `child_fh` from directory `parent_fh` (delete_allowed). */
void chimera_vfs_gate_delete(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *parent_fh,
    int                            parent_fhlen,
    const void                    *child_fh,
    int                            child_fhlen,
    chimera_vfs_gate_callback_t    callback,
    void                          *private_data);
