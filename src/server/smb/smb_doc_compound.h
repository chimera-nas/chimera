// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

#include <stdbool.h>
#include <stdint.h>
#include "vfs/vfs_compound.h"

struct smb_vfs_command;
struct smb_vfs_command_ops;
struct smb_doc_batch;
struct chimera_smb_open_file;

struct smb_doc_batch * smb_doc_batch_alloc(
    unsigned int capacity);
/* Before submit, wait for the complete known file set with no partial holds.
 * The runtime pins the batch and wire lifetime until done is called. */
void smb_doc_batch_preflight(
    struct smb_doc_batch *batch,
    void (               *done )(void *),
    void                 *private_data);
/* Drops dynamically acquired attempt coordination resources before producer
 * replay. Static preflight fences remain; no tentative path state is published. */
void smb_doc_batch_reset(
    struct smb_doc_batch *batch);
void smb_doc_batch_free(
    struct smb_doc_batch *batch);

/* Runtime-owned accessors. A batch outlives all callbacks and borrowed intent
 * credentials; free the DOC context after VFS journal completion. */
struct smb_doc_batch * chimera_smb_compound_doc_batch(
    struct smb_vfs_command *command);
bool chimera_smb_compound_open_closed(
    struct smb_vfs_command             *command,
    const struct chimera_smb_open_file *open);

extern const struct smb_vfs_command_ops chimera_smb_disposition_compound_ops;
int smb_doc_query_pending(
    struct smb_vfs_command *command,
    int                     public_value);
int smb_doc_fh_pending(
    struct smb_vfs_command *command,
    const uint8_t          *fh,
    uint32_t                fh_len,
    int                     public_value);

/* Existing ordinary handles only. The coordination must precede atomic claim
* retirement; the preallocated tail follows retirement. Its selection checkpoint prepares
* identity-matched REMOVE (when last opener); inactive operations are skipped.
* The returned result is the final VFS CLOSE; call close_complete from the
* descriptor completion callback. No suffix allocation follows retirement. */
bool smb_doc_close_allowed(
    struct smb_vfs_command *command);
int smb_doc_close_add_coordinate(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command);
int smb_doc_close_build_tail(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command);
void smb_doc_close_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data);
bool smb_doc_close_committed(
    struct smb_vfs_command *command);
/* Call whenever committed, including an accepted unlink failure: SMB CLOSE
 * still retires the handle. No filesystem I/O or allocation occurs here. The
 * runtime then unhashes and plain-releases the original handle; it must not
 * call legacy release_doc a second time. */
void smb_doc_close_publish(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command);

struct smb_doc_batch * chimera_smb_compound_doc_batch_ensure(
    struct smb_vfs_command *command);

struct chimera_vfs_claim;
bool chimera_smb_compound_claim_closed(
    struct smb_vfs_command         *command,
    const struct chimera_vfs_claim *claim);

struct chimera_smb_namespace_path;
/* Link-specific path overlay; peers on distinct hardlinks retain their paths.
 * Producer slots bind file-state identity only in explicit coordination. */
void smb_doc_command_path(
    struct smb_vfs_command            *command,
    struct chimera_smb_namespace_path *path);
bool smb_doc_path_reserve(
    struct smb_vfs_command *command);
/* Explicit coordination only: acquires cross-view producer admission when
 * parents differ, then validates the preallocated before/after path overlay. */
bool smb_doc_path_prepare_move(
    struct smb_vfs_command                  *command,
    const struct chimera_smb_namespace_path *after);
void smb_doc_path_complete(
    struct smb_vfs_command *command,
    bool                    success);

void smb_doc_path_publish(
    struct smb_vfs_command *command);
