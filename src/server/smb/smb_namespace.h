// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <pthread.h>
#include "smb_common/smb2.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"

struct chimera_smb_namespace_edit;
struct chimera_smb_namespace_participant;
struct chimera_smb_doc_fence;
struct chimera_smb_namespace_view_fence;
struct chimera_smb_namespace_open_token;
struct chimera_smb_namespace_replace_token;

/* Independent of ACCESS admission: a closing open stays here until its path
 * and DOC work has drained, even after its cache/share rights are released. */
struct chimera_smb_namespace_registry {
    pthread_mutex_t                           lock;
    struct chimera_smb_namespace_edit        *active;
    struct chimera_smb_namespace_participant *participants;
    struct chimera_smb_doc_fence             *doc_fences;
    struct chimera_smb_namespace_view_fence  *view_fences;
    /* Shared reader list: constructors plus RENAME/LINK mutators. */
    struct chimera_smb_namespace_open_token *constructors;
    struct chimera_smb_namespace_replace_token *replacements;
};

/* Shared namespace readers cover every frontend handle constructor and every
 * native/legacy RENAME/LINK before lookup or mutation. This is a held token,
 * never a mutex held over async I/O. A
 * distinct-target replacement excludes foreign constructors and mutators until acceptance;
 * otherwise an already acquired old target could publish after replacement.
 * Owners are wire compounds. Same-owner tokens compose, but replacement must
 * still reject an existing/pending participant for the destination identity.
 * Global scope is deliberately conservative until exact path admission exists.
 * A conflict holds nothing: never wait with a partial namespace/ACCESS fence. */
struct chimera_smb_namespace_open_token {
    struct chimera_smb_namespace_registry *registry;
    struct chimera_smb_namespace_open_token *next;
    const void *owner;
};
struct chimera_smb_namespace_replace_token {
    struct chimera_smb_namespace_registry *registry;
    struct chimera_smb_namespace_replace_token *next;
    const void *owner;
};
__attribute__((visibility("default"))) bool chimera_smb_namespace_open_begin(
    struct chimera_smb_namespace_open_token *token,
    struct chimera_smb_namespace_registry *registry, const void *owner);
__attribute__((visibility("default"))) void chimera_smb_namespace_open_end(struct chimera_smb_namespace_open_token *token);
bool chimera_smb_namespace_replace_begin(
    struct chimera_smb_namespace_replace_token *token,
    struct chimera_smb_namespace_registry *registry, const void *owner);
void chimera_smb_namespace_replace_end(struct chimera_smb_namespace_replace_token *token);
/* Includes unpublished bound constructors and closing opens. Caller holds
 * registry.lock; no backend acquisition can start under its replacement token. */
bool chimera_smb_namespace_identity_present_locked(
    struct chimera_smb_namespace_registry *registry,
    const uint8_t *fh, uint32_t fh_len);
/* Accepted identity migration only. Caller holds registry.lock, excludes new
 * constructors, namespace mutators and FileId consumers, and owns both identities' DOC/ACCESS
 * fences. Path, context and DOC metadata remain attached without allocation. */
bool chimera_smb_namespace_rebind_locked(
    struct chimera_smb_namespace_participant *participant,
    const uint8_t *old_fh, uint32_t old_fh_len,
    const uint8_t *new_fh, uint32_t new_fh_len);

struct chimera_smb_namespace_path {
    uint32_t view_fh_len;
    uint8_t  view_fh[CHIMERA_VFS_FH_SIZE]; /* share root identity */
    uint32_t parent_fh_len, name_len, full_path_len;
    uint8_t  parent_fh[CHIMERA_VFS_FH_SIZE];
    char     name[SMB_FILENAME_MAX + 1];
    /* Share-relative; cross-share aliases need a caller-provided translation
     * before this path can be published to their participants. */
    char     full_path[SMB_PATH_MAX];
};

/* Directory-move admission under a constructor/mutator writer. Same-view
 * source/root identities and exact root-child siblings are safe: all other
 * RENAME/LINK operations need a reader before they can relocate those siblings.
 * Private batch paths must use the same predicate after applying their journal.
 * Other share views and deeper paths still require ancestry/path translation. */
bool chimera_smb_namespace_directory_peer_allowed(
    const uint8_t *source_fh, uint32_t source_fh_len,
    const struct chimera_smb_namespace_path *source_path,
    const uint8_t *peer_fh, uint32_t peer_fh_len,
    const struct chimera_smb_namespace_path *peer_path);
/* Includes attached, closing and pending participants. Unresolved or invalid
 * pending paths reject. Caller holds registry.lock and a writer through
 * accepted publication; private earlier handles are the caller's responsibility. */
bool chimera_smb_namespace_directory_isolated_locked(
    struct chimera_smb_namespace_registry *registry,
    const uint8_t *source_fh, uint32_t source_fh_len,
    const struct chimera_smb_namespace_path *path);

/* A cross-parent move requires a destination path in each share view.
 * Until such translation is available, this coordination fence prevents a
 * new foreign-view invisible producer from crossing path validation. */
struct chimera_smb_namespace_view_fence {
    struct chimera_smb_namespace_registry   *registry;
    struct chimera_smb_namespace_view_fence *next;
    struct chimera_smb_namespace_path        before;
    uint8_t                                  fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                                 fh_len;
};
bool chimera_smb_namespace_view_fence_acquire(
    struct chimera_smb_namespace_view_fence *fence,
    struct chimera_smb_namespace_registry   *registry,
    const uint8_t                           *fh,
    uint32_t                                 fh_len,
    const struct chimera_smb_namespace_path *before);
void chimera_smb_namespace_view_fence_release(
    struct chimera_smb_namespace_view_fence *fence);

struct chimera_smb_namespace_snapshot {
    struct chimera_smb_namespace_path path;
    struct chimera_vfs_cred           doc_cred;
    bool                              delete_on_close, doc_posix, closing;
};

/* Publish is synchronous and infallible. It runs under registry.lock and may
 * not allocate, recurse into this API, perform I/O, or release the participant.
 * Integration must establish registry -> open/file/cache lock ordering. */
typedef void (*chimera_smb_namespace_publish_t)(
    void                                        *context,
    const struct chimera_smb_namespace_snapshot *snapshot);
typedef void (*chimera_smb_namespace_context_t)(
    void *context);

void chimera_smb_namespace_init(
    struct chimera_smb_namespace_registry *registry);
/* Requires all edits and participants drained. */
void chimera_smb_namespace_destroy(
    struct chimera_smb_namespace_registry *registry);

/* A constructor may allocate an unbound private slot before its VFS handle
 * exists. Binding is a pure copy into that unpublished slot on each attempt. */
struct chimera_smb_namespace_participant * chimera_smb_namespace_participant_reserve(
    void                           *context,
    chimera_smb_namespace_context_t retain,
    chimera_smb_namespace_context_t release,
    chimera_smb_namespace_publish_t publish);
bool chimera_smb_namespace_participant_bind(
    struct chimera_smb_namespace_participant    *participant,
    const uint8_t                               *fh,
    uint32_t                                     fh_len,
    const struct chimera_smb_namespace_snapshot *snapshot);

/* Preallocate/pin before backend acceptance. attach performs no allocation;
 * holding a covering own_guard guarantees exclusion from foreign edits.
 * Attach precedes the first staged namespace mutation. */
struct chimera_smb_namespace_participant * chimera_smb_namespace_participant_alloc(
    const uint8_t                               *fh,
    uint32_t                                     fh_len,
    const struct chimera_smb_namespace_snapshot *snapshot,
    void                                        *context,
    chimera_smb_namespace_context_t              retain,
    chimera_smb_namespace_context_t              release,
    chimera_smb_namespace_publish_t              publish);
bool chimera_smb_namespace_attach(
    struct chimera_smb_namespace_registry    *registry,
    struct chimera_smb_namespace_participant *participant,
    const struct chimera_smb_namespace_edit  *own_guard);
/* Accepted-only atomic stream publication: promote the bound pending base
 * name token, copy its current accepted path into the prepared stream snapshot,
 * and attach the actual stream identity under one registry lock. Preserves the
 * stream snapshot's DOC metadata. Uses ordinary attach guard checks; any
 * validation/conflict failure leaves both participants unchanged. All storage
 * and the stream's own snapshot must be prepared before acceptance. */
bool chimera_smb_namespace_promote_stream_pair(
    struct chimera_smb_namespace_registry *registry,
    struct chimera_smb_namespace_participant *base_pending,
    struct chimera_smb_namespace_participant *stream_participant,
    const uint8_t *stream_fh, uint32_t stream_fh_len,
    const struct chimera_smb_namespace_edit *own_guard);
/* Only for a participant that was never attached. */
void chimera_smb_namespace_participant_free(
    struct chimera_smb_namespace_participant *participant);

/* Registration is rejected while an edit owns this identity. The caller must
 * defer publication of that open, not bypass this gate. Context is retained
 * once on success and released once by detach, outside the registry lock. */
struct chimera_smb_namespace_participant * chimera_smb_namespace_register(
    struct chimera_smb_namespace_registry       *registry,
    const uint8_t                               *fh,
    uint32_t                                     fh_len,
    const struct chimera_smb_namespace_snapshot *snapshot,
    void                                        *context,
    chimera_smb_namespace_context_t              retain,
    chimera_smb_namespace_context_t              release,
    chimera_smb_namespace_publish_t              publish);
/* DOC scans use registry -> file lock order, keeping discovery independent of
 * ACCESS membership. Callback may inspect/write protocol fields protected by
 * that file lock, but may not enter registry APIs. */
void chimera_smb_namespace_lock(
    struct chimera_smb_namespace_registry *registry);
void chimera_smb_namespace_unlock(
    struct chimera_smb_namespace_registry *registry);
typedef void (*chimera_smb_namespace_visit_t)(
    void                                        *context,
    const struct chimera_smb_namespace_snapshot *snapshot,
    void                                        *private_data);
__attribute__((visibility("default"))) void chimera_smb_namespace_foreach_locked(
    struct chimera_smb_namespace_registry *registry,
    const uint8_t                         *fh,
    uint32_t                               fh_len,
    chimera_smb_namespace_visit_t          visit,
    void                                  *private_data);

void chimera_smb_namespace_begin_close(
    struct chimera_smb_namespace_participant *participant);
void chimera_smb_namespace_snapshot(
    struct chimera_smb_namespace_participant *participant,
    struct chimera_smb_namespace_snapshot    *snapshot);
/* own_guard may be NULL; a foreign reservation prevents detach. DOC/path I/O
 * must hold its own guard, snapshot after admission, then detach after I/O. */
bool chimera_smb_namespace_detach(
    struct chimera_smb_namespace_participant *participant,
    const struct chimera_smb_namespace_edit  *own_guard);

/* All storage is allocated before execution. One edit belongs to the complete
 * wire batch. try_acquire reserves its complete identity set atomically; busy
 * leaves it holding nothing. A global legacy guard covers unresolved identities
 * (e.g. destructive CREATE/directory rename) until narrower admission exists.
 * Never wait while holding another partial namespace or ACCESS edit lease. */
struct chimera_smb_namespace_edit * chimera_smb_namespace_edit_alloc(
    struct chimera_smb_namespace_registry *registry,
    uint32_t                               objects,
    uint32_t                               changes,
    bool                                   global);
int chimera_smb_namespace_edit_add(
    struct chimera_smb_namespace_edit *edit,
    const uint8_t                     *fh,
    uint32_t                           fh_len);
bool chimera_smb_namespace_edit_try_acquire(
    struct chimera_smb_namespace_edit *edit);
/* Each added object is a namespace link, even when another object has the
 * same inode FH. Guards still conflict at inode scope. Seed only after acquiring
 * the guard, while the source path cannot change. Directory moves and differing
 * share-root path translations are unsupported and must remain boundaries. */
bool chimera_smb_namespace_edit_seed(
    struct chimera_smb_namespace_edit       *edit,
    uint32_t                                 object,
    const struct chimera_smb_namespace_path *path);
/* Resolve by the open's initial parent/name and share-root identity, so peers
 * on other hardlinks never inherit a moved alias's path. */
const struct chimera_smb_namespace_path * chimera_smb_namespace_edit_path(
    const struct chimera_smb_namespace_edit *edit,
    const uint8_t                           *fh,
    uint32_t                                 fh_len,
    const struct chimera_smb_namespace_path *initial_link);
/* Reserve an event slot BEFORE the filesystem mutation; completion cannot fail.
 * Only successful completion updates the private path seen by following ops. */
int chimera_smb_namespace_edit_prepare(
    struct chimera_smb_namespace_edit       *edit,
    uint32_t                                 object,
    const struct chimera_smb_namespace_path *next,
    const uint8_t                            parent_lease_key[16]);
void chimera_smb_namespace_edit_complete(
    struct chimera_smb_namespace_edit *edit,
    uint32_t                           change,
    bool                               success);
/* Retains guards/participants across a rejected attempt, discards all tentative
 * paths/events. Must precede the next attempt's operation callbacks. */
void chimera_smb_namespace_edit_reset(
    struct chimera_smb_namespace_edit *edit);
/* Accepted-only: applies successful events in execution order. Notification
 * records remain accessible until free; emit them after peer repath. */
void chimera_smb_namespace_edit_publish(
    struct chimera_smb_namespace_edit *edit);
struct chimera_smb_namespace_change {
    uint32_t                          object;
    struct chimera_smb_namespace_path before, after;
    uint8_t                           parent_lease_key[16];
    bool                              completed, success;
};
const struct chimera_smb_namespace_change * chimera_smb_namespace_edit_change(
    const struct chimera_smb_namespace_edit *edit,
    uint32_t                                 index);
void chimera_smb_namespace_edit_free(
    struct chimera_smb_namespace_edit *edit);

/* SMB open integration, implemented separately from the generic journal.
 * Physical recycle must detach membership before freeing/pooling the context.
 * Production edit dispatch remains disabled until all mutation cutoffs are
 * guarded; accepted CREATE attach relies on that explicit integration gate. */
struct chimera_server_smb_thread;
struct chimera_smb_open_file;
struct chimera_vfs_open_handle;
bool chimera_smb_open_namespace_prepare(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open,
    bool                              base);
bool chimera_smb_open_namespace_bind(
    struct chimera_smb_open_file   *open,
    struct chimera_vfs_open_handle *handle,
    const uint8_t                  *root_fh,
    uint32_t                        root_fh_len);
void chimera_smb_open_namespace_attach(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open);
void chimera_smb_open_namespace_detach(
    struct chimera_smb_open_file *open);

/* A DOC-only attempt reservation. It does not block cache acknowledgments or
 * releasing cache/share rights. Acquire through typed COORDINATE, before claim
 * retirement; a conflict holds nothing and must not park with partial claims.
 * Same-batch tokens compose. Storage is caller-owned and preallocated. */
struct chimera_smb_doc_fence {
    struct chimera_smb_doc_fence          *next;
    struct chimera_smb_namespace_registry *registry;
    const void                            *owner;
    uint32_t                               fh_len;
    uint8_t                                fh[CHIMERA_VFS_FH_SIZE];
    bool                                   acquired;
};
bool chimera_smb_doc_fence_acquire(
    struct chimera_smb_doc_fence          *fence,
    struct chimera_smb_namespace_registry *registry,
    const uint8_t                         *fh,
    uint32_t                               fh_len,
    const void                            *owner);
void chimera_smb_doc_fence_release(
    struct chimera_smb_doc_fence *fence);
/* A successful begin holds registry.lock through the caller's synchronous
 * metadata change. It must precede file/cache locks. Failure holds no lock and
 * leaves metadata untouched. Callers may retry before effects, never wait here.
 * owner is NULL for legacy mutations, or the acquiring batch for publication. */
bool chimera_smb_doc_mutation_begin(
    struct chimera_smb_namespace_registry *registry,
    const uint8_t                         *fh,
    uint32_t                               fh_len,
    const void                            *owner);
void chimera_smb_doc_mutation_end(
    struct chimera_smb_namespace_registry *registry);

/* Caller holds registry.lock and the matching VFS file-state lock. Includes
* closing participants whose ACCESS reservation has already been removed. */
bool chimera_smb_open_namespace_has_doc_locked(
    struct chimera_smb_namespace_registry *registry,
    const uint8_t                         *fh,
    uint32_t                               fh_len);

struct chimera_smb_request;
/* Legacy path mutations retain a DOC fence until common request completion. */
uint32_t chimera_smb_namespace_legacy_begin(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open);
void chimera_smb_namespace_legacy_end(
    struct chimera_smb_request *request);

void chimera_smb_namespace_set_path_locked(
    struct chimera_smb_namespace_participant *participant,
    const struct chimera_smb_namespace_path  *path);

/* Invisible pre-OPEN name tokens are explicit coordination input. External
 * accepted renames edit only the token; the producer reads it under registry
 * lock and publishes its own open. Retry preserves accepted updates for the
 * same original link and inode, while rebinding another inode discards them. */
bool chimera_smb_namespace_attached(
    const struct chimera_smb_namespace_participant *p);
bool chimera_smb_namespace_pending_begin(
    struct chimera_smb_namespace_registry       *registry,
    struct chimera_smb_namespace_participant    *p,
    const struct chimera_smb_namespace_snapshot *initial);
bool chimera_smb_namespace_pending_bind(
    struct chimera_smb_namespace_participant *p,
    const uint8_t                            *fh,
    uint32_t                                  fh_len);
bool chimera_smb_namespace_pending_snapshot(
    struct chimera_smb_namespace_participant *p,
    struct chimera_smb_namespace_snapshot    *snapshot);
bool chimera_smb_namespace_pending_rename_check_locked(
    struct chimera_smb_namespace_registry   *registry,
    const uint8_t                           *fh,
    uint32_t                                 fh_len,
    const struct chimera_smb_namespace_path *before,
    uint32_t                                 name_len);
void chimera_smb_namespace_pending_rename_locked(
    struct chimera_smb_namespace_registry   *registry,
    const uint8_t                           *fh,
    uint32_t                                 fh_len,
    const struct chimera_smb_namespace_path *before,
    const char                              *name,
    uint32_t                                 name_len);

bool chimera_smb_open_namespace_pending_begin(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open,
    const uint8_t                    *root_fh,
    uint32_t                          root_fh_len);
bool chimera_smb_open_namespace_pending_bind(
    struct chimera_smb_open_file   *open,
    struct chimera_vfs_open_handle *handle);
bool chimera_smb_open_namespace_pending_path(
    struct chimera_smb_open_file      *open,
    struct chimera_smb_namespace_path *path);

/* Accepted legacy moves: exact parent/name for all matching tokens, destination
 * full_path when the view is shared, per-view basename for same-parent rename. */
void chimera_smb_namespace_pending_move_locked(
    struct chimera_smb_namespace_registry   *registry,
    const uint8_t                           *fh,
    uint32_t                                 fh_len,
    const struct chimera_smb_namespace_path *before,
    const struct chimera_smb_namespace_path *after);
