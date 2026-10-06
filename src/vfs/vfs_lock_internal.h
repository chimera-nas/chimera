// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

#ifndef CHIMERA_VFS_INTERNAL_API
#error "Lock attempts are private; frontend consumers must use compound lock operations"
#endif // ifndef CHIMERA_VFS_INTERNAL_API

#include "vfs_lock.h"

/* Executor-private interface. A projected change rejects an installed finish
 * adapter BEFORE mutation: legacy backend locks do not support rollback.
 * Mandatory RELEASE_OWNER runs independently of finish acceptance, drains
 * legacy backend state first, and is never replayed by compound_retry. A finish
 * error remains visible to the caller even though mandatory cleanup ran. */
struct chimera_vfs_lock_attempt * chimera_vfs_lock_attempt_alloc(
    struct chimera_vfs_thread             *thread,
    struct chimera_vfs_lock_domain        *domain,
    struct chimera_vfs_open_handle        *handle,
    const struct chimera_vfs_lock_request *request,
    bool                                   test,
    bool                                   release);
struct chimera_vfs_lock_attempt * chimera_vfs_lock_attempt_alloc_fh(
    struct chimera_vfs_thread             *thread,
    struct chimera_vfs_lock_domain        *domain,
    const uint8_t                         *fh,
    uint32_t                               fh_len,
    const struct chimera_vfs_lock_request *request);
bool chimera_vfs_lock_attempt_matches_fh(
    struct chimera_vfs_lock_attempt *attempt,
    const uint8_t                   *fh,
    uint32_t                         fh_len);
void chimera_vfs_lock_attempt_execute(
    struct chimera_vfs_lock_attempt *attempt,
    bool finish_adapter,
    void ( *complete )(enum chimera_vfs_error, void *),
    void *private_data);
enum chimera_vfs_error chimera_vfs_lock_attempt_accept(
    struct chimera_vfs_lock_attempt *attempt);
void chimera_vfs_lock_attempt_reset(
    struct chimera_vfs_lock_attempt *attempt);
void chimera_vfs_lock_attempt_free(
    struct chimera_vfs_lock_attempt *attempt);
bool chimera_vfs_lock_attempt_cancel(
    struct chimera_vfs_lock_attempt *attempt);
void chimera_vfs_lock_attempt_result(
    struct chimera_vfs_lock_attempt   *attempt,
    struct chimera_vfs_claim_conflict *conflict,
    uint32_t                          *pid);

bool chimera_vfs_lock_attempt_retryable(
    struct chimera_vfs_lock_attempt *attempt);
