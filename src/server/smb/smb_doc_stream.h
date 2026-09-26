// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include "vfs/sdk/vfs_error.h"

struct chimera_server_smb_thread;
struct chimera_smb_open_file;
struct chimera_smb_stream_delete;

/* Retire one logical opener before releasing its handle or claims. The last
 * opener takes an owned action independent of the pooled open_file. */
struct chimera_smb_stream_delete *
chimera_smb_stream_doc_retire(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file);

/* Consume action and invoke done exactly once, including on failure. */
void chimera_smb_stream_doc_run(
    struct chimera_smb_stream_delete *action,
    void ( *done )(enum chimera_vfs_error, void *),
    void *private_data);

/* Accepted base-link rename only. Caller holds registry.lock and the stream
 * file lock. Repath pending intent even when its original DOC opener has gone
 * and the surviving stream opener used another hardlink alias. */
struct chimera_vfs_file_state;
struct chimera_smb_namespace_path;
void chimera_smb_stream_doc_repath_locked(
    struct chimera_vfs_file_state *file,
    const struct chimera_vfs_file_state *base_file,
    const struct chimera_smb_namespace_path *before,
    const struct chimera_smb_namespace_path *after);
