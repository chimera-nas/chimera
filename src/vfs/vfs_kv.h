// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs.h"

/*
 * The VFS key-value side-store.
 *
 * These are NOT file-system operations, and that is why they are here rather
 * than among the sequence ops.  Every op a sequence carries addresses an
 * OBJECT -- through the current file handle, the saved one, the current open
 * handle or the saved one -- and these address none of the four: they take a
 * KEY.  There is no object for a cursor to name, nothing for a later op in the
 * sequence to inherit, and nothing in a sequence's state that a put or a
 * delete could invalidate.  Putting them in a sequence would mean inventing a
 * fifth thing for an op to address, for records that have no namespace entry,
 * no attributes, and no lease.
 *
 * What they hold is protocol-side state that must outlive a process but is not
 * part of anyone's file system: an NFSv3 or NFSv4 duplicate-request cache
 * entry, an NSM monitor record, an NFSv4 client-recovery record, an SMB2
 * durable handle's reconnect record.  A server writes them on its own behalf,
 * under its own identity, at moments that have nothing to do with a request's
 * path walk.
 *
 * The plain forms go to the configured default KV module.  The `_at` forms are
 * routed by a file handle to the backend serving it -- a per-share record, for
 * a backend that keeps its own store -- and take a credential for that reason;
 * they still address a key, not the object the handle names.
 */

void
chimera_vfs_put_key(
    struct chimera_vfs_thread     *thread,
    const void                    *key,
    uint32_t                       key_len,
    const void                    *value,
    uint32_t                       value_len,
    chimera_vfs_put_key_callback_t callback,
    void                          *private_data);

/* fh-routed put: store a key/value associated with the backend serving `fh`
 * (used to persist handle-state for backends without native KV; see
 * chimera_vfs_kv_route_fh). */
void
chimera_vfs_put_key_at(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const void                    *key,
    uint32_t                       key_len,
    const void                    *value,
    uint32_t                       value_len,
    chimera_vfs_put_key_callback_t callback,
    void                          *private_data);

void
chimera_vfs_get_key(
    struct chimera_vfs_thread     *thread,
    const void                    *key,
    uint32_t                       key_len,
    chimera_vfs_get_key_callback_t callback,
    void                          *private_data);

/* True if a handle-state record can be persisted for an open on `handle`'s
 * backend: either the backend persists it atomically (CAP_ATOMIC_HANDLE_STATE)
 * or a default KV module is configured to hold it.  Used by the SMB server to
 * decide whether a durable/persistent open can be granted.  Synchronous, and
 * touches no store -- it answers from the module's capabilities. */
int
chimera_vfs_can_persist_handle_state(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle);

void
chimera_vfs_delete_key(
    struct chimera_vfs_thread        *thread,
    const void                       *key,
    uint32_t                          key_len,
    chimera_vfs_delete_key_callback_t callback,
    void                             *private_data);

/* fh-routed variants: operate on the backend serving `fh` rather than the
 * global kv_module (used for per-share handle-state records). */
void
chimera_vfs_delete_key_at(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    const void                       *fh,
    int                               fhlen,
    const void                       *key,
    uint32_t                          key_len,
    chimera_vfs_delete_key_callback_t callback,
    void                             *private_data);

void
chimera_vfs_search_keys(
    struct chimera_vfs_thread         *thread,
    const void                        *start_key,
    uint32_t                           start_key_len,
    const void                        *end_key,
    uint32_t                           end_key_len,
    uint32_t                           flags,
    chimera_vfs_search_keys_callback_t callback,
    chimera_vfs_search_keys_complete_t complete,
    void                              *private_data);

void
chimera_vfs_search_keys_at(
    struct chimera_vfs_thread         *thread,
    const struct chimera_vfs_cred     *cred,
    const void                        *fh,
    int                                fhlen,
    const void                        *start_key,
    uint32_t                           start_key_len,
    const void                        *end_key,
    uint32_t                           end_key_len,
    uint32_t                           flags,
    chimera_vfs_search_keys_callback_t callback,
    chimera_vfs_search_keys_complete_t complete,
    void                              *private_data);
