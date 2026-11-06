// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

#include <stddef.h>
#include <stdint.h>
#include "vfs/sdk/chimera_vfs_sdk.h"

/* The codec deliberately has no DFS dependency. */
#define VFS_DAOS_FH_SIZE          33
#define VFS_DAOS_FH_FRAGMENT_SIZE 17
struct vfs_daos_fh {
    uint64_t hi;
    uint64_t lo;
    uint8_t  type;
};
int
vfs_daos_fh_mount(
    const uint8_t             uuid[16],
    const struct vfs_daos_fh *key,
    uint8_t                  *fh,
    uint32_t                 *length);
int
vfs_daos_fh_encode(
    const uint8_t             id[16],
    const struct vfs_daos_fh *key,
    uint8_t                  *fh,
    uint32_t                 *length);
int
vfs_daos_fh_decode(
    const uint8_t       id[16],
    const void         *fh,
    size_t              length,
    struct vfs_daos_fh *key);

#ifndef VFS_DAOS_FH_ONLY
#include <pthread.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <daos.h>
#include <daos_fs.h>

struct vfs_daos_config {
    char            *pool;
    char            *container;
    char            *system;
    uint8_t          uuid[16];
    bool             read_only;
    size_t           registry_max_entries;
    size_t           open_max_handles;
    daos_oclass_id_t oclass;
    daos_size_t      chunk_size;
    uint32_t         batch_entries;
};

struct vfs_daos_object {
    struct vfs_daos_object *next;
    dfs_obj_t              *obj;
    daos_obj_id_t           oid;
    uint32_t                flags;
    bool                    removed;
};

/* Each successful DFS allocation has one owner record, including quarantine.
 * The registry retains one reference on its canonical handle. Other handles
 * attached to an entry pin it until their final release (including core CLOSE).
 * All fields are protected by the mount mutex.
 */
struct vfs_daos_owned {
    struct vfs_daos_owned *next;
    dfs_obj_t             *obj;
    struct vfs_daos_entry *entry;
    size_t                 refs; /* Canonical retention plus request and root pins. */
    bool                   uncertain;
};

struct vfs_daos_entry {
    struct vfs_daos_entry *next;
    struct vfs_daos_owned *owned;
    daos_obj_id_t          oid;
    uint64_t               incarnation;
    uint64_t               touched;
    size_t                 pins; /* Attached noncanonical handles, including core wrappers. */
    uint8_t                type;
    bool                   invalid;
    bool                   removed;
    bool                   repair_failed;
    bool                   parent_valid;
    daos_obj_id_t          parent_oid;
};

/* A scan owns every checkpoint until its whole slot is recycled. */
struct vfs_daos_checkpoint {
    struct vfs_daos_checkpoint *next;
    uint64_t                    token;
    uint32_t                    dot;
    daos_anchor_t               anchor;
};

struct vfs_daos_cursor {
    struct vfs_daos_cursor     *next;
    struct vfs_daos_checkpoint *checkpoints;
    uint64_t                    generation;
    daos_obj_id_t               oid;
    uint32_t                    flags;
    uint64_t                    touched;
};

struct vfs_daos_state {
    struct vfs_daos_state  *next_quarantined;
    pthread_mutex_t         mutex;
    pthread_cond_t          drained;
    struct vfs_daos_config  config;
    dfs_t                  *dfs;
    dfs_obj_t              *root;
    uint8_t                 mount_id[16];
    struct vfs_daos_entry  *registry;
    struct vfs_daos_owned  *owned;
    struct vfs_daos_owned  *reserved_handles;
    struct vfs_daos_entry  *reserved_entry;
    struct vfs_daos_object *reserved_wrapper;
    size_t                  registry_count;
    size_t                  handle_count;
    size_t                  handle_reserved;
    uint64_t                registry_clock;
    int                     error;
    bool                    initialized;
    bool                    active;
    bool                    unavailable;
    bool                    draining;
    uint64_t                incarnation;
    uint64_t                generation;
    uint64_t                next_cookie;
    struct vfs_daos_object *objects;
    struct vfs_daos_cursor *cursors;
    size_t                  cursor_count;
    uint64_t                cursor_clock;
    uint64_t                operations[CHIMERA_VFS_OP_NUM];
    uint64_t                latency_ns[CHIMERA_VFS_OP_NUM];
    uint64_t                lock_wait_ns;
    uint64_t                sync_ns;
    uint64_t                cursor_evictions;
    uint64_t                fh_decode_failures;
    uint64_t                registry_admissions;
    uint64_t                handle_admissions;
    uint64_t                registry_hits;
    uint64_t                registry_misses;
    uint64_t                registry_evictions;
    uint64_t                registry_admission_failures;
    uint64_t                handle_admission_failures;
    size_t                  uncertain_release_count;
};

int vfs_daos_admit(
    struct vfs_daos_state *s,
    dfs_obj_t            **obj,
    dfs_obj_t             *parent,
    bool                   created);
int vfs_daos_parent(
    struct vfs_daos_state *s,
    dfs_obj_t             *obj,
    dfs_obj_t            **parent);
int vfs_daos_registry_add(
    struct vfs_daos_state  *s,
    dfs_obj_t              *obj,
    struct vfs_daos_entry **entry);
int vfs_daos_resolve(
    struct vfs_daos_state *s,
    const void            *fh,
    size_t                 length,
    dfs_obj_t            **obj);
int vfs_daos_validate(
    struct vfs_daos_state *s,
    dfs_obj_t             *obj);
int vfs_daos_registry_clear(
    struct vfs_daos_state *s);
void vfs_daos_invalidate(
    struct vfs_daos_state *s,
    daos_obj_id_t          oid);
void vfs_daos_repair(
    struct vfs_daos_state *s,
    daos_obj_id_t          oid,
    dfs_obj_t             *moved,
    dfs_obj_t             *parent,
    const char            *name,
    int                    error);
int vfs_daos_hold(
    struct vfs_daos_state *s,
    dfs_obj_t             *obj);
int vfs_daos_reserve_root(
    struct vfs_daos_state *s);
int vfs_daos_reserve(
    struct vfs_daos_state *s,
    size_t                 handles,
    bool                   entry);
void vfs_daos_unreserve(
    struct vfs_daos_state *s);
int vfs_daos_dup(
    struct vfs_daos_state *s,
    dfs_obj_t             *obj,
    int                    flags,
    dfs_obj_t            **copy);
int vfs_daos_lookup_rel(
    struct vfs_daos_state *s,
    dfs_obj_t             *parent,
    const char            *name,
    int                    flags,
    dfs_obj_t            **obj,
    mode_t                *mode,
    struct stat           *st);
int vfs_daos_lookup_root(
    struct vfs_daos_state *s,
    dfs_obj_t            **obj);
int vfs_daos_open(
    struct vfs_daos_state *s,
    dfs_obj_t             *parent,
    const char            *name,
    mode_t                 mode,
    int                    flags,
    daos_oclass_id_t       oclass,
    daos_size_t            chunk,
    const char            *value,
    dfs_obj_t            **obj);

int
vfs_daos_config_parse(
    const char             *json,
    struct vfs_daos_config *config);
void
vfs_daos_config_free(
    struct vfs_daos_config *config);
enum chimera_vfs_error
dfs_errno_to_vfs(
    int rc);
int
vfs_daos_connect(
    struct vfs_daos_state *state);
int
vfs_daos_disconnect(
    struct vfs_daos_state *state);
int
vfs_daos_release(
    struct vfs_daos_state *state,
    dfs_obj_t             *obj);
void
vfs_daos_cursors_clear(
    struct vfs_daos_state *state);
enum chimera_vfs_error
vfs_daos_operation(
    struct vfs_daos_state      *state,
    struct chimera_vfs_request *request);
#endif // ifndef VFS_DAOS_FH_ONLY
