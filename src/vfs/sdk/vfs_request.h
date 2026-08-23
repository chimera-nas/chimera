// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Chimera VFS module SDK: the request contract.
 *
 * Everything a VFS backend module needs to interpret a dispatched
 * request: opcodes, per-op flag vocabularies, the open-handle and
 * request structures, and the completion contract -- the module calls
 * request->complete(request) when the operation is done.
 *
 * SDK headers carry type/struct definitions, constants, and trivial
 * inline accessors only; substantive logic lives behind exported
 * functions (see vfs_utils.h).
 */

#include <stdint.h>
#include <sys/types.h>
#include <time.h>

#include "vfs_attrs.h"
#include "vfs_error.h"
#include "vfs_cred.h"
#include "vfs_claim_types.h"
#include "vfs_pnfs_layout.h"

#include "evpl/evpl.h"
#include "prometheus-c.h"
#include "oteltracing.h"

#define CHIMERA_VFS_PATH_MAX         4096
#define CHIMERA_VFS_NAME_MAX         256

/* Size of the per-request scratch buffer (request->plugin_data) available
 * to the dispatching module.  Must be large enough for the largest
 * operation (symlink: name + target + 2 NULs). */
#define CHIMERA_VFS_PLUGIN_DATA_SIZE 8192

struct chimera_vfs;
struct chimera_vfs_thread;
struct chimera_vfs_module;
struct chimera_vfs_mount;
struct chimera_vfs_file_state;
struct chimera_vfs_find_result;
struct chimera_vfs_handle_state;

/* FSSTAT values used with builtin backends until statvfs tracking is implemented */
#define CHIMERA_VFS_SYNTHETIC_FS_BYTES   ((uint64_t) 100 * 1024 * 1024 * 1024)
#define CHIMERA_VFS_SYNTHETIC_FS_INODES  (1024 * 1024)

/* Flags for chimera_vfs_lookup */
#define CHIMERA_VFS_LOOKUP_FOLLOW        (1U << 0) /* Follow symlinks in final component */

/* Maximum number of symlinks to follow before returning ELOOP */
#define CHIMERA_VFS_SYMLOOP_MAX          40

#define CHIMERA_VFS_MOUNT_OPT_MAX        16
#define CHIMERA_VFS_MOUNT_OPT_BUFFER_MAX 1024

struct chimera_vfs_mount_option {
    const char *key;
    const char *value;   /* NULL if no value */
};

struct chimera_vfs_mount_options {
    int                             num_options;
    struct chimera_vfs_mount_option options[CHIMERA_VFS_MOUNT_OPT_MAX];
};

#define CHIMERA_VFS_OP_MOUNT                    1
#define CHIMERA_VFS_OP_UMOUNT                   2
#define CHIMERA_VFS_OP_LOOKUP_AT                3
#define CHIMERA_VFS_OP_GETATTR                  4
#define CHIMERA_VFS_OP_READDIR                  5
#define CHIMERA_VFS_OP_READLINK                 6
#define CHIMERA_VFS_OP_OPEN_FH                  7
#define CHIMERA_VFS_OP_OPEN_AT                  8
#define CHIMERA_VFS_OP_CLOSE                    9
#define CHIMERA_VFS_OP_READ                     10
#define CHIMERA_VFS_OP_WRITE                    11
#define CHIMERA_VFS_OP_REMOVE_AT                12
#define CHIMERA_VFS_OP_MKDIR_AT                 13
#define CHIMERA_VFS_OP_COMMIT                   14
#define CHIMERA_VFS_OP_SYMLINK_AT               15
#define CHIMERA_VFS_OP_RENAME_AT                16
#define CHIMERA_VFS_OP_SETATTR                  17
#define CHIMERA_VFS_OP_LINK_AT                  18
#define CHIMERA_VFS_OP_CREATE_UNLINKED          19
#define CHIMERA_VFS_OP_MKNOD_AT                 20
#define CHIMERA_VFS_OP_PUT_KEY                  21
#define CHIMERA_VFS_OP_GET_KEY                  22
#define CHIMERA_VFS_OP_DELETE_KEY               23
#define CHIMERA_VFS_OP_SEARCH_KEYS              24
#define CHIMERA_VFS_OP_ALLOCATE                 25
#define CHIMERA_VFS_OP_SEEK                     26
/* 27 was CHIMERA_VFS_OP_LOCK; byte ranges now ride the claim wire. */
#define CHIMERA_VFS_OP_GETPARENT                28
#define CHIMERA_VFS_OP_COPY_RANGE               29
#define CHIMERA_VFS_OP_CLONE_RANGE              30
#define CHIMERA_VFS_OP_MOVE_RANGE               31
#define CHIMERA_VFS_OP_GET_XATTR                32
#define CHIMERA_VFS_OP_SET_XATTR                33
#define CHIMERA_VFS_OP_LIST_XATTRS              34
#define CHIMERA_VFS_OP_REMOVE_XATTR             35
#define CHIMERA_VFS_OP_GET_LAYOUT               36
#define CHIMERA_VFS_OP_OPEN_STREAM              37
#define CHIMERA_VFS_OP_LIST_STREAMS             38
#define CHIMERA_VFS_OP_REMOVE_STREAM            39
#define CHIMERA_VFS_OP_MKFS                     40
#define CHIMERA_VFS_OP_RMFS                     41
#define CHIMERA_VFS_OP_CLAIM_ACQUIRE            42
#define CHIMERA_VFS_OP_CLAIM_RELEASE            43
#define CHIMERA_VFS_OP_READ_PLUS                44
#define CHIMERA_VFS_OP_WRITE_SAME               45
#define CHIMERA_VFS_OP_NUM                      46

#define CHIMERA_VFS_OPEN_CREATE                 (1U << 0)
#define CHIMERA_VFS_OPEN_PATH                   (1U << 1)
#define CHIMERA_VFS_OPEN_INFERRED               (1U << 2)
#define CHIMERA_VFS_OPEN_DIRECTORY              (1U << 3)
#define CHIMERA_VFS_OPEN_READ_ONLY              (1U << 4)
#define CHIMERA_VFS_OPEN_EXCLUSIVE              (1U << 5)
#define CHIMERA_VFS_OPEN_NOFOLLOW               (1U << 6)
/* Replace an existing file's contents on open: truncate to zero and apply
 * set_attr (used for the SMB OVERWRITE / OVERWRITE_IF / SUPERSEDE
 * dispositions).  Backends that do not honor it simply open the file. */
#define CHIMERA_VFS_OPEN_TRUNCATE               (1U << 7)
/* Access mode: READ_ONLY for O_RDONLY, WRITE_ONLY for O_WRONLY, neither for
 * O_RDWR.  Read access is required unless WRITE_ONLY; write access is required
 * unless READ_ONLY.  Used by the open path to authorize the requested access. */
#define CHIMERA_VFS_OPEN_WRITE_ONLY             (1U << 8)
/* Stop if the final path component is an existing symbolic link: the backend
 * returns CHIMERA_VFS_ELOOP instead of opening, colliding with (O_EXCL), or
 * truncating it -- the check precedes the existence/EXCLUSIVE test.  Set by the
 * SMB create path so a non-FILE_OPEN_REPARSE_POINT open of a symlink leaf yields
 * STATUS_STOPPED_ON_SYMLINK regardless of the create disposition (MS-SMB2
 * 3.3.5.9).  POSIX/NFS callers leave it clear and keep their existing
 * symlink-leaf semantics. */
#define CHIMERA_VFS_OPEN_STOP_SYMLINK           (1U << 9)
/* The create must yield a *regular* file (POSIX/NFS3 CREATE semantics): if the
 * name already exists as a non-regular object, the backend returns an error
 * instead of opening it -- a directory gives CHIMERA_VFS_EISDIR, any other
 * non-regular type (symlink/socket/fifo/...) gives CHIMERA_VFS_EEXIST.  Set by
 * the NFS3 UNCHECKED create path (GUARDED/EXCLUSIVE already collide via
 * OPEN_EXCLUSIVE).  Native backends answer from the inode metadata they already
 * hold; passthrough resolves the leaf type without a data open.  SMB leaves it
 * clear and keeps its open-any-type disposition. */
#define CHIMERA_VFS_OPEN_CREATE_REGULAR         (1U << 10)

/* Suppress the VFS core's FILE_ADDED change-notify emission when this open
 * creates a file.  Set by the SMB create path, which owns a richer emission
 * of its own (disposition policy, DIR/STREAM_NAME classes, directory-lease
 * key sparing) and would otherwise deliver duplicate CHANGE_NOTIFY events.
 * Every other caller leaves it clear so a create is observable by change
 * watchers and directory-lease holders regardless of arrival protocol. */
#define CHIMERA_VFS_OPEN_NO_NOTIFY              (1U << 11)

/* remove_at flags: an optional assertion about the target's type, letting the
 * single VFS remove op express the rmdir(2)/RMDIR vs unlink(2)/REMOVE
 * distinction the callers know but the op otherwise loses.  Enforcing backends
 * (memfs, cairn, diskfs) reject a mismatch (ENOTDIR when ISDIR is set on a
 * non-directory, EISDIR when ISNOTDIR is set on a directory); passthrough
 * backends (linux, io_uring) use it to choose unlinkat's AT_REMOVEDIR flag.
 * With neither set the op removes whatever the name resolves to (legacy
 * behavior). */
#define CHIMERA_VFS_REMOVE_ISDIR                (1U << 0) /* target must be a directory */
#define CHIMERA_VFS_REMOVE_ISNOTDIR             (1U << 1) /* target must not be a directory */
/* The caller wants the VFS to recall any cross-protocol caching holder on the
 * victim before the unlink.  When set (and caching is enabled and no child_fh
 * was supplied), the remove path resolves the name to its FH and drives a
 * synchronous recall.  Callers with their own recall scheme (e.g. NFSv4, which
 * breaks the delegation and returns NFS4ERR_DELAY) leave it clear. */
#define CHIMERA_VFS_REMOVE_RECALL               (1U << 2)

/* Allocate flags */
#define CHIMERA_VFS_ALLOCATE_DEALLOCATE         0x01

/* Copy-range flags */
/* Preserve source holes: when a full destination block's source is entirely a
 * hole, drop the destination block rather than materializing zeroes, so
 * SEEK_HOLE still sees the hole after the copy.  This is POSIX
 * copy_file_range() semantics; the POSIX client sets it.  SMB copychunk, NFS4
 * COPY and S3 copy leave it clear and get the materializing behavior their
 * conformance suites expect. */
#define CHIMERA_VFS_COPY_PRESERVE_HOLES         0x01

/* Lock types.  These survive the fold of the old OP_LOCK wire into the
 * claim wire as the vocabulary for DESCRIBING a conflicting range back to a
 * caller (F_GETLK); a claim itself says what it wants with `exclusive`. */
#define CHIMERA_VFS_LOCK_READ                   0 /* shared / read lock */
#define CHIMERA_VFS_LOCK_WRITE                  1 /* exclusive / write lock */
#define CHIMERA_VFS_LOCK_UNLOCK                 2 /* no lock / release lock */

/* RANGE claim flags (claim_acquire.flags) */
#define CHIMERA_VFS_CLAIM_WAIT                  (1U << 0) /* block until grantable (F_SETLKW) */
#define CHIMERA_VFS_CLAIM_TEST                  (1U << 1) /* probe only, do not acquire (F_GETLK) */

/* Readdir flags */
#define CHIMERA_VFS_READDIR_EMIT_DOT            (1U << 0) /* Emit "." and ".." entries */

#define CHIMERA_VFS_OPEN_ID_SYNTHETIC           0
#define CHIMERA_VFS_OPEN_ID_PATH                1
#define CHIMERA_VFS_OPEN_ID_FILE                2

#define CHIMERA_VFS_OPEN_HANDLE_EXCLUSIVE       0x1
#define CHIMERA_VFS_OPEN_HANDLE_PENDING         0x2
#define CHIMERA_VFS_OPEN_HANDLE_FILE_ID         0x4
#define CHIMERA_VFS_OPEN_HANDLE_DETACHED        0x8
/* A named-stream (ADS) handle.  Its file handle is distinct from the base
 * file's, but its metadata (mode/owner/timestamps/DOS attributes) is the base
 * inode's and mutates out-of-band relative to the stream fh, so the per-fh attr
 * cache must not serve or store attributes for it (chimera_vfs_getattr). */
#define CHIMERA_VFS_OPEN_HANDLE_STREAM          0x10
#define CHIMERA_VFS_OPEN_HANDLE_NO_BACKEND_OPEN 0x20

#define CHIMERA_VFS_ACCESS_MODE_RW              0
#define CHIMERA_VFS_ACCESS_MODE_RO              1

struct chimera_vfs_open_handle {
    /* Identity: the owning module plus the handle's place in the open cache. */
    struct chimera_vfs_module      *vfs_module;
    uint64_t                        fh_hash;
    uint16_t                        fh_len;
    uint8_t                         cache_id;
    uint8_t                         flags;
    uint8_t                         access_mode;

    /* Whether the most recent open via this handle created the file (vs opened
     * an existing one).  Refreshed on every open completion and read by the SMB
     * create path immediately afterwards to report OPENED vs CREATED. */
    uint8_t                         r_created;
    uint32_t                        opencnt;

    /* Identity hash of the credential that opened this handle: the open cache
     * is keyed by (fh, access_mode, cred_hash) so each caller gets its own
     * handle and its own authorization result (chimera_vfs_cred_hash). */
    uint64_t                        cred_hash;

    /* Cached effective access mask for this handle's credential, computed once
     * (lazily, on the first gated read/write) and reused for the handle's life
     * so the ACL check amortises across a caller's I/O.  granted_valid is 0
     * until computed. */
    uint32_t                        granted_access;
    uint8_t                         granted_valid;

    /* Open-serialization and the backend's per-open slot: requests parked
    * behind a pending open, and the value the backend stashed at open. */
    struct chimera_vfs_request     *blocked_requests;
    uint64_t                        vfs_private;

    /* Backend-generic per-file lease/state anchor.  Attached lazily (once) on
     * the first I/O through this cached handle via an acquire/release CAS in
     * chimera_vfs_io_lease_acquire, and holds one long-lived reference for the
     * handle's lifetime so per-I/O lease acquire/release can skip the
     * bucket-locked chimera_vfs_state_get/put.  NULL for synthetic/transient
     * handles and until the first I/O.  Released (state_put) at handle teardown,
     * outside the open-cache shard lock. */
    struct chimera_vfs_file_state  *file_state;

    /* In-flight open bookkeeping: the completion the core invokes when a
     * pending open resolves, and the request currently opening. */
    void                            ( *callback )(
        struct chimera_vfs_request     *request,
        struct chimera_vfs_open_handle *handle);
    struct chimera_vfs_request     *request;
    uint64_t                        timestamp; /* stopwatch ticks */

    /* Open-cache linkage (hash bucket + LRU/free list) and the handle. */
    struct chimera_vfs_open_handle *bucket_next;
    struct chimera_vfs_open_handle *bucket_prev;
    struct chimera_vfs_open_handle *prev;
    struct chimera_vfs_open_handle *next;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE + 16];

    /* Delete-on-close state: when doc_delete_on_close is set and opencnt
     * drops to zero, the file at (doc_parent_fh, doc_name) is removed
     * before the handle is recycled. */
    uint8_t                         doc_delete_on_close;
    uint16_t                        doc_parent_fh_len;
    uint16_t                        doc_name_len;
    struct chimera_vfs_cred         doc_cred;
    uint8_t                         doc_parent_fh[CHIMERA_VFS_FH_SIZE];
    char                            doc_name[CHIMERA_VFS_NAME_MAX];
};

typedef void (*chimera_vfs_lookup_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

typedef void (*chimera_vfs_create_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

/* Path-based operation callbacks */

typedef void (*chimera_vfs_open_callback_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data);

typedef void (*chimera_vfs_mkdir_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

typedef void (*chimera_vfs_remove_callback_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

typedef void (*chimera_vfs_rename_callback_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

typedef void (*chimera_vfs_symlink_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

typedef void (*chimera_vfs_link_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

typedef void (*chimera_vfs_mknod_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

typedef void (*chimera_vfs_complete_callback_t)(
    struct chimera_vfs_request *request);

typedef int (*chimera_vfs_readdir_callback_t)(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg);

typedef void (*chimera_vfs_readdir_complete_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data);

typedef int (*chimera_vfs_filter_callback_t)(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data);

typedef int (*chimera_vfs_find_callback_t)(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data);

typedef void (*chimera_vfs_find_complete_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

/* KV operation callbacks */

typedef void (*chimera_vfs_put_key_callback_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

typedef void (*chimera_vfs_get_key_callback_t)(
    enum chimera_vfs_error error_code,
    const void            *value,
    uint32_t               value_len,
    void                  *private_data);

typedef void (*chimera_vfs_delete_key_callback_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

/* Flags for chimera_vfs_search_keys / chimera_vfs_search_keys_at.
 *
 * By default the range searched is [start_key, end_key] (both bounds
 * inclusive).  END_EXCLUSIVE makes the end bound exclusive, i.e. the range
 * becomes [start_key, end_key); a key byte-equal to end_key is not returned.
 * The flag has no effect when no end key is supplied (the range is then
 * open-ended). */
#define CHIMERA_VFS_SEARCH_KEYS_END_EXCLUSIVE 0x00000001U

/* Returns non-zero to abort the search */
typedef int (*chimera_vfs_search_keys_callback_t)(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data);

typedef void (*chimera_vfs_search_keys_complete_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

#define CHIMERA_VFS_ACCESS_READ         0x01
#define CHIMERA_VFS_ACCESS_WRITE        0x02
#define CHIMERA_VFS_ACCESS_EXECUTE      0x04

struct chimera_vfs_request_handle {
    uint8_t slot;
};

/* Max chained sub-operations (SQE slots) a single request may have outstanding.
 * The io_uring open_at uses four: the O_EXCL create-probe, the EEXIST re-open,
 * and the child + parent statx. */
#define CHIMERA_VFS_REQUEST_MAX_HANDLES 4

/* Size of the gate resume area carved out of a pooled request (see the `gate`
 * arm of chimera_vfs_request).  Each gated wrapper static-asserts its own
 * resume struct against this; raise it if one legitimately outgrows it.  It is
 * a union member, so it costs nothing until it is the largest arm -- and the
 * request union is already an order of magnitude bigger than this. */
#define CHIMERA_VFS_GATE_SCRATCH_SIZE   384

/* One enumerated named stream, packed back-to-back in the list_streams reply
 * buffer.  `name_len` bytes of (un-terminated) stream name follow this header;
 * the next record begins at the following 8-byte-aligned offset. */
struct chimera_vfs_stream_entry {
    uint64_t size;        /* stream end-of-file */
    uint64_t alloc;       /* stream allocation size */
    uint16_t name_len;    /* bytes of name that follow this struct */
};

/*
 * Requested/achieved write stability, carried in write.sync (request) and
 * returned in write.r_sync (achieved).  Values match the NFS3/NFS4 stable_how
 * enums (UNSTABLE/DATA_SYNC/FILE_SYNC = 0/1/2), so backends that only test
 * truthiness for FUA still behave correctly (UNSTABLE=0 is the only falsy one).
 * A backend that makes data durable but defers metadata durability returns
 * DATASYNC; one that makes everything durable returns FILESYNC.
 */
#define CHIMERA_VFS_WRITE_UNSTABLE 0
#define CHIMERA_VFS_WRITE_DATASYNC 1
#define CHIMERA_VFS_WRITE_FILESYNC 2

struct chimera_vfs_notify_gate;

struct chimera_vfs_request {
    struct chimera_vfs_thread         *thread;
    const struct chimera_vfs_cred     *cred;
    uint32_t                           opcode;
    enum chimera_vfs_error             status;
    chimera_vfs_complete_callback_t    complete;
    chimera_vfs_complete_callback_t    complete_delegate;
    struct prometheus_stopwatch        start_time;
    uint64_t                           elapsed_ns;

    /* Temporary diagnostics: where a long-lived request is parked. */
    const char                        *wait_reason;
    uint64_t                           wait_since_ns;
    uint64_t                           wait_arg0;
    uint64_t                           wait_arg1;
    uint64_t                           wait_arg2;

    /* OpenTelemetry span for this VFS op.  Started in chimera_vfs_request_alloc_
     * common() as a child of thread->otel_parent (the protocol op that issued
     * it), annotated + ended in chimera_vfs_complete().  Zero-cost when tracing
     * is disabled; never recorded unless a sampled protocol parent was set. */
    struct otel_span                   otel;
    /* The trace parent captured at alloc.  chimera_vfs_complete() re-publishes it
     * as thread->otel_parent right before the proto completion callback runs, so a
     * chained sibling VFS op issued from that callback inherits the same protocol
     * parent without the protocol having to set it again. */
    struct otel_span                  *otel_parent;

    /* Points to one page of memory that the plugin may use as desired */
    void                              *plugin_data;

    /* For use by the plugin if desired, see io_uring for example */
    struct chimera_vfs_request_handle  handle[CHIMERA_VFS_REQUEST_MAX_HANDLES];
    uint8_t                            token_count;

    struct chimera_vfs_module         *module;

    /* The mount_private the backend returned from MOUNT for the mount that
     * owns this request's file handle -- for a backend hosting several named
     * filesystems, the one this operation targets.  Resolved from the mount
     * table at alloc, which is authoritative now that a handle cannot outlive
     * its mount: umount holds the mount live until every handle referencing
     * it is gone, so this is set for every operation the backend sees,
     * including the closes umount itself issues. */
    void                              *mount_private;
    void                              *proto_callback;
    void                              *proto_private_data;

    /* VFS plugins may use these while processing the request */
    struct chimera_vfs_request        *prev;
    struct chimera_vfs_request        *next;

    /* For use by vfs core only */
    struct chimera_vfs_request        *active_prev;
    struct chimera_vfs_request        *active_next;

    uint8_t                            fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                           fh_len;
    uint64_t                           fh_hash;

    /* Implicit I/O lease mediation (chimera_vfs_io_lease_acquire).  For a
     * lease-holding client (NFSv4 delegation, SMB oplock) the protocol fills
     * io_owner + io_owner_valid so the I/O coalesces with the client's own
     * lease instead of recalling it.  io_next is the continuation invoked
     * once the lease is held (normally chimera_vfs_dispatch); io_lease_file
     * is the per-file state whose implicit lease this request has pinned
     * (NULL on the fast path where nothing was pinned). */
    struct chimera_claim_actor         io_owner;
    uint8_t                            io_owner_valid;
    /* Set when a lease-holding writer must wait out sync_break read caches:
     * the request is parked on the file's io-wait queue until every such
     * holder's break acks (chimera_vfs_sync_read_pending_locked), then
     * proceeds without taking the implicit lease. */
    uint8_t                            io_sync_wait;

    /* Synchronous notify gating (vfs_notify.c): a namespace mutation's
     * completion is swapped for the gate wrapper at dispatch
     * (notify_gate_wrapped), which parks the real completion
     * (notify_saved_complete) until every sync watcher acks its
     * invalidation.  notify_gate_resume routes the parked request's resume
     * (an ack or the deadline sweep) to request->complete in the owning
     * thread's drain loop. */
    struct chimera_vfs_notify_gate    *notify_gate;
    void                               ( *notify_saved_complete )(
        struct chimera_vfs_request *request);
    uint8_t                            notify_gate_wrapped;
    uint8_t                            notify_gate_resume;
    /* Set by chimera_vfs_io_recall(): this request is a namespace/metadata
     * mutation that must recall every caching lease on a target file (regardless
     * of owner) rather than hold an implicit lease. */
    uint8_t                            io_recall_all;
    /* Set by chimera_vfs_io_recall() for a data-coherence recall (a setattr that
     * touches data, e.g. SIZE/EOF) as opposed to a namespace recall (remove /
     * rename / link).  A flush recall downgrades a write-caching (W) holder to
     * its read+handle cache (forcing the client to flush dirty data without
     * losing its read cache or oplock), rather than revoking the whole lease to
     * NONE.  This decouples the load-bearing dirty-cache flush from full
     * revocation -- the churn source for metadata-heavy single-client workloads
     * (rewinddir/fsstress) -- while preserving coherence. */
    uint8_t                            io_recall_flush_only;
    /* Single-step namespace recall (smb2.lease.unlink delete-on-close): break
     * each OTHER holder exactly ONCE to io_recall_retain (R) -- not a cascade to
     * NONE -- and park until those breaks are acked.  io_recall_retain is the
     * floor handed to begin_break for this flavor. */
    uint8_t                            io_recall_single;
    uint8_t                            io_recall_retain;
    /* Owned write parked until every awaited-class sync grant (FUSE
     * coherence=sync) it invalidated has acked; the pump resume re-fires
     * the write trigger before re-checking (rearm-on-demand races). */
    uint8_t                            io_sync_write;
    void                               ( *io_next )(
        struct chimera_vfs_request *request);
    struct chimera_vfs_file_state     *io_lease_file;
    /* Cached open handle backing this I/O, set by the read/write dispatch.  When
     * present (and non-synthetic) chimera_vfs_io_lease_acquire attaches the
     * per-file lease state to handle->file_state once and reuses it, skipping the
     * per-I/O bucket-locked chimera_vfs_state_get.  io_owns_lease_ref is then 0
     * (the handle owns the long-lived reference, so io_lease_release must not
     * state_put); 1 = the legacy path that takes and drops its own per-I/O ref. */
    struct chimera_vfs_open_handle    *io_handle;
    uint8_t                            io_owns_lease_ref;
    struct chimera_vfs_pending_acquire io_lease_ticket;

    struct chimera_vfs_open_handle    *pending_handle;

    void                               ( *unblock_callback )(
        struct chimera_vfs_request     *request,
        struct chimera_vfs_open_handle *handle);

    union {
        /*
         * Resume state for an asynchronous access gate.
         *
         * A gated operation has to park its arguments somewhere while the
         * gate's own nested requests (open, getattr) run, and it cannot use
         * either of them: those come and go underneath it, and the real
         * operation's request does not exist yet.  It can, however, have a
         * request of its own.  A request holding this arm is never dispatched
         * and never joins the active list -- chimera_vfs_gate_scratch_alloc()
         * takes it straight off the thread's free list and
         * chimera_vfs_gate_scratch_free() returns it -- which is what keeps a
         * gated operation off the heap entirely.
         *
         * Each gated wrapper defines its own resume layout in its own
         * translation unit and asserts that it fits; the union member gives the
         * storage its alignment.
         */
        union {
            uint64_t align;
            uint8_t  data[CHIMERA_VFS_GATE_SCRATCH_SIZE];
        } gate;

        struct {
            char                           *path;
            char                           *pathc;
            int                             pathlen;
            struct chimera_vfs_open_handle *handle;
            uint64_t                        attr_mask;
            uint32_t                        flags;
            uint32_t                        symlink_count;
            chimera_vfs_lookup_callback_t   callback;
            void                           *private_data;
            uint8_t                         next_fh[CHIMERA_VFS_FH_SIZE];
            uint8_t                         parent_fh[CHIMERA_VFS_FH_SIZE];
            int                             parent_fh_len;
        } lookup;

        struct {
            char                           *path;
            char                           *pathc;
            int                             pathlen;
            struct chimera_vfs_open_handle *handle;
            struct chimera_vfs_attrs       *set_attr;
            uint64_t                        attr_mask;
            chimera_vfs_lookup_callback_t   callback;
            void                           *private_data;
            uint8_t                         next_fh[CHIMERA_VFS_FH_SIZE];
        } create;

        struct {
            char                           *path;
            int                             pathlen;
            int                             parent_len;
            int                             name_offset;
            uint32_t                        flags;
            struct chimera_vfs_attrs       *set_attr;
            uint64_t                        attr_mask;
            struct chimera_vfs_open_handle *parent_handle;
            chimera_vfs_open_callback_t     callback;
            void                           *private_data;
            uint8_t                         parent_fh[CHIMERA_VFS_FH_SIZE];
            int                             parent_fh_len;
            /* Open-time effective-access grant computed by the lookup gate,
             * stamped onto the handle at completion (POSIX rights bind at
             * open).  granted_valid is 0 unless the gated non-create path
             * ran the access check. */
            uint32_t                        granted_access;
            uint8_t                         granted_valid;
            /* Zeroed stand-in handed to open_at when the caller passes no
             * set_attr (a non-create open) -- open_at requires a non-NULL
             * set_attr that the backend only consults when creating. */
            struct chimera_vfs_attrs        scratch_set_attr;
        } open;

        struct {
            char                           *path;
            int                             pathlen;
            int                             parent_len;
            int                             name_offset;
            struct chimera_vfs_attrs       *set_attr;
            uint64_t                        attr_mask;
            struct chimera_vfs_open_handle *parent_handle;
            chimera_vfs_mkdir_callback_t    callback;
            void                           *private_data;
            uint8_t                         parent_fh[CHIMERA_VFS_FH_SIZE];
            int                             parent_fh_len;
        } mkdir;

        struct {
            char                           *path;
            int                             pathlen;
            int                             parent_len;
            int                             name_offset;
            unsigned int                    flags; /* CHIMERA_VFS_REMOVE_* type assertion */
            struct chimera_vfs_open_handle *parent_handle;
            chimera_vfs_remove_callback_t   callback;
            void                           *private_data;
            uint8_t                         parent_fh[CHIMERA_VFS_FH_SIZE];
            int                             parent_fh_len;
            uint8_t                         child_fh[CHIMERA_VFS_FH_SIZE];
            int                             child_fh_len;
        } remove;

        struct {
            char                         *path;
            int                           pathlen;
            int                           parent_len;
            int                           name_offset;
            char                         *new_path;
            int                           new_pathlen;
            int                           new_parent_len;
            int                           new_name_offset;
            chimera_vfs_rename_callback_t callback;
            void                         *private_data;
            uint8_t                       old_parent_fh[CHIMERA_VFS_FH_SIZE];
            int                           old_parent_fh_len;
            uint8_t                       new_parent_fh[CHIMERA_VFS_FH_SIZE];
            int                           new_parent_fh_len;
            uint8_t                       target_fh[CHIMERA_VFS_FH_SIZE];
            int                           target_fh_len;
        } rename;

        struct {
            char                           *path;
            int                             pathlen;
            int                             parent_len;
            int                             name_offset;
            const char                     *target;
            int                             targetlen;
            struct chimera_vfs_attrs       *set_attr;
            uint64_t                        attr_mask;
            struct chimera_vfs_open_handle *parent_handle;
            chimera_vfs_symlink_callback_t  callback;
            void                           *private_data;
            uint8_t                         parent_fh[CHIMERA_VFS_FH_SIZE];
            int                             parent_fh_len;
        } symlink;

        struct {
            char                       *path;
            int                         pathlen;
            char                       *new_path;
            int                         new_pathlen;
            int                         new_parent_len;
            int                         new_name_offset;
            unsigned int                replace;
            uint64_t                    attr_mask;
            chimera_vfs_link_callback_t callback;
            void                       *private_data;
            uint8_t                     source_fh[CHIMERA_VFS_FH_SIZE];
            int                         source_fh_len;
            uint8_t                     dest_parent_fh[CHIMERA_VFS_FH_SIZE];
            int                         dest_parent_fh_len;
        } link;

        struct {
            char                           *path;
            int                             pathlen;
            int                             parent_len;
            int                             name_offset;
            struct chimera_vfs_attrs       *set_attr;
            uint64_t                        attr_mask;
            struct chimera_vfs_open_handle *parent_handle;
            chimera_vfs_mknod_callback_t    callback;
            void                           *private_data;
            uint8_t                         parent_fh[CHIMERA_VFS_FH_SIZE];
            int                             parent_fh_len;
        } mknod;

        struct {
            char                           *path;
            int                             path_len;
            int16_t                         is_complete;
            int16_t                         complete_called;
            uint64_t                        attr_mask;
            struct chimera_vfs_request     *root;
            struct chimera_vfs_find_result *parent;
            struct chimera_vfs_find_result *results;
            chimera_vfs_filter_callback_t   filter;
            chimera_vfs_find_callback_t     callback;
            chimera_vfs_find_complete_t     complete;
            void                           *private_data;
        } find;

        struct {
            uint8_t                          fh_magic;
            const char                      *path;
            uint32_t                         pathlen;
            struct chimera_vfs_module       *module;
            const char                      *mount_path;
            uint32_t                         mount_pathlen;
            struct chimera_vfs_mount_options options;
            char                             options_buffer[CHIMERA_VFS_MOUNT_OPT_BUFFER_MAX];
            const char                      *raw_options;
            void                            *r_mount_private;
            struct chimera_vfs_attrs         r_attr;
        } mount;

        struct {
            struct chimera_vfs_mount *mount;
            void                     *mount_private;
            /* Handle closes still outstanding before the backend UMOUNT may
             * be dispatched.  Carries a self-reference while a sweep is
             * issuing them, so a close completing inline cannot finish the
             * umount mid-sweep. */
            int                       pending_closes;
            /* Poll state while waiting for handles still referenced by
             * someone else to be dropped (chimera_vfs_umount_wait). */
            void                     *wait;
        } umount;

        struct {
            const char                      *name;
            uint32_t                         namelen;
            struct chimera_vfs_mount_options options;
            char                             options_buffer[CHIMERA_VFS_MOUNT_OPT_BUFFER_MAX];
            const char                      *raw_options;
        } mkfs;

        struct {
            const char *name;
            uint32_t    namelen;
        } rmfs;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        component_hash;
            const char                     *component;
            uint32_t                        component_len;
            struct chimera_vfs_attrs        r_attr;
            struct chimera_vfs_attrs        r_dir_attr;
        } lookup_at;

        struct {
            struct chimera_vfs_request *getattr;
            struct chimera_vfs_request *dir_getattr;
        } lookup_hit;

        struct {
            struct chimera_vfs_open_handle *handle;
            struct chimera_vfs_attrs        r_attr;
        } getattr;

        struct {
            struct chimera_vfs_open_handle *handle;
            struct chimera_vfs_attrs       *set_attr;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } setattr;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        cookie;
            uint64_t                        verifier;
            uint64_t                        attr_mask;
            uint32_t                        flags;
            uint64_t                        r_cookie;
            uint64_t                        r_verifier;
            uint32_t                        r_eof;
            struct chimera_vfs_attrs        r_dir_attr;
            chimera_vfs_readdir_callback_t  callback;

            struct evpl_iovec               bounce_iov;
            int                             bounce_offset;
            chimera_vfs_readdir_callback_t  orig_callback;
            void                           *orig_private_data;
            /* Optional SMB-style wildcard filter applied in the VFS core so the
             * backend stays oblivious: only entries matching match_pattern are
             * forwarded to inner_callback (the path's real per-entry callback).
             * match_pattern == NULL disables filtering. */
            const char                     *match_pattern;
            int                             match_pattern_len;
            chimera_vfs_readdir_callback_t  inner_callback;
            void                           *inner_arg;
        } readdir;

        struct {
            struct chimera_vfs_open_handle *handle;
            const char                     *name;
            uint32_t                        name_len;
            uint64_t                        name_hash;
            struct chimera_vfs_attrs       *set_attr;
            struct chimera_vfs_attrs        r_attr;
            struct chimera_vfs_attrs        r_dir_pre_attr;
            struct chimera_vfs_attrs        r_dir_post_attr;
        } mkdir_at;

        struct {
            struct chimera_vfs_open_handle *handle;
            const char                     *name;
            uint32_t                        name_len;
            uint64_t                        name_hash;
            struct chimera_vfs_attrs       *set_attr;
            struct chimera_vfs_attrs        r_attr;
            struct chimera_vfs_attrs        r_dir_pre_attr;
            struct chimera_vfs_attrs        r_dir_post_attr;
        } mknod_at;

        struct {
            uint32_t                         flags;
            /* Optional: opaque record to persist atomically with the open,
             * honored only by modules advertising CAP_ATOMIC_HANDLE_STATE. */
            struct chimera_vfs_handle_state *handle_state;
            uint64_t                         r_vfs_private;
        } open_fh;

        struct {
            struct chimera_vfs_open_handle  *handle;
            const char                      *name;
            uint64_t                         name_hash;
            int                              namelen;
            uint32_t                         flags;
            struct chimera_vfs_attrs        *set_attr;
            /* Optional: opaque record to persist atomically with the open,
             * honored only by modules advertising CAP_ATOMIC_HANDLE_STATE. */
            struct chimera_vfs_handle_state *handle_state;
            struct chimera_vfs_attrs         r_attr;
            struct chimera_vfs_attrs         r_dir_pre_attr;
            struct chimera_vfs_attrs         r_dir_post_attr;
            uint64_t                         r_vfs_private;
            /* Set by the module when the open created a new file (vs opened an
             * existing one); lets the SMB server report OPENED vs CREATED.
             * Modules that don't set it leave it 0 (treated as "opened"). */
            uint8_t                          r_created;
        } open_at;

        struct {
            uint32_t                  flags;
            struct chimera_vfs_attrs *set_attr;
            struct chimera_vfs_attrs  r_attr;
            uint64_t                  r_vfs_private;
        } create_unlinked;

        struct {
            uint64_t vfs_private;
        } close;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint32_t                        length;
            uint64_t                        attrmask;
            struct evpl_iovec              *iov;
            int                             niov;
            int                             r_niov;
            uint32_t                        r_length;
            uint32_t                        r_eof;
            struct chimera_vfs_attrs        r_attr;
            /* Set by the VFS core when it pre-allocated the read buffers on the
             * connection thread (backend lacks CAP_READ_PROVIDES_BUFFERS).
             * buffers_provided = number of iovecs placed in iov[]; aligned_prefix
             * = offset - (offset & ~4095), the leading pad bytes the backend read
             * but the client did not ask for.  The backend fills the buffers
             * (data for file offset `offset` lands at buffer position
             * aligned_prefix) and sets r_length/r_eof; the VFS core trims the
             * prefix/tail and sets r_niov in chimera_vfs_read_complete(). */
            int                             buffers_provided;
            uint32_t                        aligned_prefix;
            /* read_into: the caller supplied its own destination buffers
             * (dest_iov/dest_niov) and wants the data landed there.  iov/niov
             * above are the scratch the core/backend works in.  On completion
             * the VFS core scatter-copies the result into dest_iov -- unless a
             * backend was able to land the data directly in dest (e.g. the NFS
             * proxy used dest as the RDMA write chunk), in which case it sets
             * landed_in_dest and the core skips the copy. */
            struct evpl_iovec              *dest_iov;
            int                             dest_niov;
            int                             dest_provided;
            int                             landed_in_dest;
        } read;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint32_t                        length;
            uint32_t                        sync;
            struct evpl_iovec              *iov;
            int                             niov;
            uint32_t                        r_sync;
            uint32_t                        r_length;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } write;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint64_t                        length;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } commit;

        struct {
            struct chimera_vfs_open_handle *handle;
            const char                     *name;
            int                             namelen;
            uint64_t                        name_hash;
            uint32_t                        flags;        /* CHIMERA_VFS_REMOVE_* type assertion */
            const uint8_t                  *child_fh;     /* Optional: child FH if known */
            int                             child_fh_len; /* 0 if child_fh not provided */
            /* Owned copy of the child FH.  The source pointer may not outlive
             * this async op -- a name-resolved sticky FH lives in a gate struct
             * that is freed the moment dispatch returns -- so dispatch copies
             * the FH here and points child_fh at it.  (The completion path
             * hashes child_fh for the FILE_DELETE notify, well after the gate
             * is gone.) */
            uint8_t                         child_fh_store[CHIMERA_VFS_FH_SIZE];
            /* Inode-scoped removal: when set (with child_fh), the backend MUST
             * only unlink the name while it still resolves to child_fh -- if the
             * original object was removed and a new one created with the same
             * name, the name is left intact.  Used by delete-on-close so an
             * async unlink cannot destroy an unrelated file another opener
             * created at the same path.  Default 0 = unconditional by-name
             * remove (every existing caller's behaviour is unchanged). */
            uint8_t                         match_child_fh;
            /* Backend-set: the match_child_fh guard found the name resolving to
             * a different object and left it intact, so NO unlink happened.
             * The op still completes OK, but the post-removal bookkeeping
             * (negative name-cache entry, FILE_REMOVED notify) must be skipped. */
            uint8_t                         r_unmatched;
            /* SMB3 directory-lease self-exemption (see link_at): spare the dir
             * lease named by the deleting open's ParentLeaseKey from the
             * FILE_REMOVED break on the parent.  NULL caller = break all. */
            uint8_t                         parent_lease_skip[16];
            uint8_t                         parent_lease_skip_valid;
            struct chimera_vfs_attrs        r_dir_pre_attr;
            struct chimera_vfs_attrs        r_dir_post_attr;
            struct chimera_vfs_attrs        r_removed_attr;
        } remove_at;

        struct {
            struct chimera_vfs_open_handle *handle;
            const char                     *name;
            int                             namelen;
            uint64_t                        name_hash;
            const char                     *target;
            int                             targetlen;
            struct chimera_vfs_attrs       *set_attr;
            struct chimera_vfs_attrs        r_attr;
            struct chimera_vfs_attrs        r_dir_pre_attr;
            struct chimera_vfs_attrs        r_dir_post_attr;
        } symlink_at;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint32_t                        target_maxlength;
            uint32_t                        r_target_length;
            void                           *r_target;
            struct chimera_vfs_attrs        r_attr;
        } readlink;

        struct {
            const char              *name;
            int                      namelen;
            uint64_t                 name_hash;
            uint64_t                 new_fh_hash;
            const void              *new_fh;
            int                      new_fhlen;
            uint64_t                 new_name_hash;
            const char              *new_name;
            int                      new_namelen;
            unsigned int             flags;  /* CHIMERA_VFS_REMOVE_* (RECALL) */
            const uint8_t           *target_fh; /* Optional: target FH if known (for silly rename) */
            int                      target_fh_len; /* 0 if target_fh not provided */
            /* Backing store for a target FH the VFS resolved itself (RECALL). */
            uint8_t                  resolved_target_fh[CHIMERA_VFS_FH_SIZE];
            uint8_t                  source_fh[CHIMERA_VFS_FH_SIZE]; /* resolved source FH, for delegation recall */
            int                      source_fh_len; /* 0 if source FH could not be resolved */
            /* SMB3 directory-lease self-exemption (see link_at): spare the dir
             * lease named by the operating open's ParentLeaseKey from the RENAMED
             * break on the source/dest parent.  NULL caller = no skip. */
            uint8_t                  parent_lease_skip[16];
            uint8_t                  parent_lease_skip_valid;
            struct chimera_vfs_attrs r_fromdir_pre_attr;
            struct chimera_vfs_attrs r_fromdir_post_attr;
            struct chimera_vfs_attrs r_todir_pre_attr;
            struct chimera_vfs_attrs r_todir_post_attr;
        } rename_at;

        struct {
            const void              *dir_fh;
            int                      dir_fhlen;
            uint64_t                 dir_fh_hash;
            const char              *name;
            int                      namelen;
            unsigned int             replace;
            uint64_t                 name_hash;
            /* SMB3 directory-lease self-exemption: when this link/rename is
             * issued through a handle that supplied a ParentLeaseKey, that
             * directory lease must not be broken by the FILE_ADDED emit on the
             * parent.  Copied from the caller (NULL = no skip, break all). */
            uint8_t                  parent_lease_skip[16];
            uint8_t                  parent_lease_skip_valid;
            struct chimera_vfs_attrs r_attr;
            struct chimera_vfs_attrs r_replaced_attr;
            struct chimera_vfs_attrs r_dir_pre_attr;
            struct chimera_vfs_attrs r_dir_post_attr;
        } link_at;

        struct {
            const void *key;
            uint32_t    key_len;
            const void *value;
            uint32_t    value_len;
        } put_key;

        struct {
            const void *key;
            uint32_t    key_len;
            const void *r_value;
            uint32_t    r_value_len;
        } get_key;

        struct {
            const void *key;
            uint32_t    key_len;
        } delete_key;

        struct {
            const void                        *start_key;
            uint32_t                           start_key_len;
            const void                        *end_key;
            uint32_t                           end_key_len;
            uint32_t                           flags;
            chimera_vfs_search_keys_callback_t callback;
        } search_keys;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint64_t                        length;
            uint32_t                        flags;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } allocate;

        struct {
            struct chimera_vfs_open_handle *src_handle;
            struct chimera_vfs_open_handle *dst_handle;
            uint64_t                        src_offset;
            uint64_t                        dst_offset;
            uint64_t                        length;
            uint64_t                        r_length;
            uint32_t                        flags;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } copy_range;

        struct {
            struct chimera_vfs_open_handle *src_handle;
            struct chimera_vfs_open_handle *dst_handle;
            uint64_t                        src_offset;
            uint64_t                        dst_offset;
            uint64_t                        length;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } clone_range;

        struct {
            struct chimera_vfs_open_handle *src_handle;
            struct chimera_vfs_open_handle *dst_handle;
            uint64_t                        src_offset;
            uint64_t                        dst_offset;
            uint64_t                        length;
            struct chimera_vfs_attrs        r_src_post_attr;
            struct chimera_vfs_attrs        r_dst_pre_attr;
            struct chimera_vfs_attrs        r_dst_post_attr;
        } move_range;

        /* READ_PLUS: classify the leading byte-run at `offset` as DATA or HOLE
         * from the backend's block/extent map, in one round trip.  The backend
         * sets r_is_data (1 = DATA run, 0 = HOLE run), r_length (the run length
         * to the next boundary or EOF, clamped to the requested `length`), and
         * r_eof (the segment reaches end-of-file).  The data bytes themselves are
         * fetched by the caller via a normal read, so this op never moves data
         * and avoids the read path's buffer/thread-ownership machinery. */
        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint64_t                        length;
            uint32_t                        r_is_data;
            uint64_t                        r_length;
            uint32_t                        r_eof;
        } read_plus;

        /* WRITE_SAME: write `block_count` blocks of `block_size` bytes from
         * `offset`, each block zero-filled then `pattern` (pattern_len bytes)
         * placed at reloff_pattern.  Per-block-number stamping is not requested
         * (the NFS layer rejects it).  r_count = total bytes written. */
        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint32_t                        block_size;
            uint64_t                        block_count;
            uint32_t                        reloff_pattern;
            const void                     *pattern;
            uint32_t                        pattern_len;
            uint32_t                        sync;
            uint64_t                        r_count;
            uint32_t                        r_sync;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } write_same;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint32_t                        what;
            int                             r_eof;
            uint64_t                        r_offset;
        } seek;

        struct {
            uint8_t  r_parent_fh[CHIMERA_VFS_FH_SIZE];
            uint16_t r_parent_fh_len;
            char     r_name[CHIMERA_VFS_NAME_MAX];
            uint16_t r_name_len;
        } getparent;

        struct {
            struct chimera_vfs_open_handle *handle;
            const char                     *name;
            uint32_t                        namelen;
            void                           *value;        /* caller-provided buffer */
            uint32_t                        value_maxlen;
            uint32_t                        r_value_len;   /* bytes written to value */
        } get_xattr;

        struct {
            /* Backend lease acquire (CHIMERA_VFS_OP_CLAIM_ACQUIRE).  Kindless
             * wire: masks + a cluster-meaningful owner, never a protocol
             * construct.  klass selects the claim shape. */
            uint8_t                    klass;      /* CHIMERA_VFS_CLAIM_KLASS_AGGREGATE
                                                    * / CHIMERA_VFS_CLAIM_KLASS_RANGE  */
            uint8_t                    rev_used;   /* AGGREGATE: revocable use
                                                    * union (CHIMERA_CLAIM_R|W)  */
            uint8_t                    bind_deny;  /* AGGREGATE: binding deny
                                                    * union (R|W|D)              */
            uint8_t                    exclusive;  /* RANGE only                 */
            /* RANGE only.  CHIMERA_VFS_CLAIM_WAIT: block until grantable
             * (F_SETLKW); the backend may complete asynchronously, and only
             * callers that can wait ask for it.  CHIMERA_VFS_CLAIM_TEST:
             * probe without acquiring (F_GETLK), answered in the r_conflict_*
             * fields. */
            uint8_t                    flags;
            /* RANGE only.  SEEK_END is passed through rather than resolved
             * here so the backend resolves EOF atomically with the lock --
             * resolving it locally would reintroduce the fstat TOCTOU.
             *
             * With SEEK_END, offset and length are bit-casts of the caller's
             * SIGNED l_start/l_len, and length keeps the POSIX spelling
             * where 0 means to-EOF and a negative length extends backwards.
             * With SEEK_SET they are the claim spelling: absolute, with
             * UINT64_MAX for to-EOF and 0 for a genuine zero-byte range.
             * The two disagree about 0 precisely because one is POSIX's
             * vocabulary and the other is the core's; a backend translating
             * between them must key on whence. */
            int32_t                    whence;
            uint64_t                   offset;     /* RANGE only                 */
            uint64_t                   length;     /* RANGE only; UINT64_MAX =
                                                    * to-EOF, 0 = zero-byte      */
            struct chimera_claim_owner owner;      /* AGGREGATE: the node owner;
                                                    *  RANGE: the lock's cluster-
                                                    *  stable owner identity      */
            uint64_t                   prev_token; /* AGGREGATE escalate: the
                                                    * currently held token (0 =
                                                    * fresh); the backend
                                                    * replaces it atomically     */
            /* Recall path, captured at grant: the backend invokes recall_cb
            * (any thread) to demand the token back down to `retain`; the
            * node's eventual LEASE_RELEASE with that token is the ack.  The
            * core marshals internally; backends store the pair verbatim. */
            void                       ( *recall_cb )(
                void          *recall_arg,
                const uint8_t *fh,
                uint8_t        fh_len,
                uint64_t       fh_hash,
                uint64_t       token,
                uint8_t        retain);
            void                      *recall_arg;
            uint64_t                   r_token;    /* backend-opaque, 0 = none  */
            uint8_t                    r_granted;  /* AGGREGATE: granted subset
                                                    *  of rev_used (deny bits are
                                                    *  all-or-nothing with the
                                                    *  grant); RANGE: nonzero =
                                                    *  granted (all-or-nothing)  */
            /* RANGE refusal detail: who holds the conflicting range, so
             * F_GETLK can describe it.  Filled on TEST, and on a refused
             * acquire when the backend knows; r_conflict_type
             * CHIMERA_VFS_LOCK_UNLOCK means "no conflict". */
            uint8_t                    r_conflict_type;
            uint64_t                   r_conflict_offset;
            uint64_t                   r_conflict_length;
            uint32_t                   r_conflict_pid;
        } claim_acquire;

        struct {
            /* Backend lease release (CHIMERA_VFS_OP_CLAIM_RELEASE): drop or
             * downgrade the record behind `token`.  retained == 0 releases
             * outright; for an AGGREGATE under recall, the release with the
             * retained mask IS the recall acknowledgment.  `klass` says
             * which capability this release is addressed to, since a
             * backend may arbitrate ranges without aggregates. */
            uint64_t                   token;
            uint8_t                    retained;
            uint8_t                    klass;
            /* RANGE releases may instead name a range: token == 0 means
             * "release whatever this owner holds over this geometry",
             * which is how an unlock whose geometry only the backend can
             * resolve (whence == SEEK_END) is expressed.  A caller that
             * holds a token uses the token; one that never learned the
             * absolute range uses this, and the backend resolves EOF for
             * the unlock exactly as it did for the lock. */
            int32_t                    whence;
            uint64_t                   offset;
            uint64_t                   length;
            struct chimera_claim_owner owner;
        } claim_release;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint32_t                        option;       /* setxattr_option4 */
            const char                     *name;
            uint32_t                        namelen;
            const void                     *value;
            uint32_t                        value_len;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } set_xattr;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        cookie;
            void                           *buffer;        /* caller-provided buffer */
            uint32_t                        max_bytes;
            /* buffer is filled with NUL-terminated names, back to back */
            uint32_t                        r_len;         /* bytes written to buffer */
            uint32_t                        r_count;       /* number of names written */
            uint32_t                        r_eof;
            uint64_t                        r_cookie;
        } list_xattrs;

        struct {
            struct chimera_vfs_open_handle *handle;
            const char                     *name;
            uint32_t                        namelen;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } remove_xattr;

        /* Named streams (SMB Alternate Data Streams).  Gated by
         * CHIMERA_VFS_CAP_NAMED_STREAMS.  open_stream opens/creates a named
         * data fork on the base file referenced by `handle`; it returns a
         * file handle (in r_attr.va_fh) and r_vfs_private exactly like open_at
         * so the VFS open cache can wrap it.  list_streams enumerates the
         * file's streams (the default unnamed fork first), each as a packed
         * struct chimera_vfs_stream_entry. */
        struct {
            struct chimera_vfs_open_handle *handle;       /* base file handle */
            const char                     *name;         /* stream name (no ":$DATA") */
            uint32_t                        namelen;
            uint32_t                        flags;        /* CHIMERA_VFS_OPEN_* */
            struct chimera_vfs_attrs       *set_attr;
            struct chimera_vfs_attrs        r_attr;       /* base meta + stream size; va_fh = stream fh */
            uint64_t                        r_vfs_private;
            uint8_t                         r_created;
        } open_stream;

        struct {
            struct chimera_vfs_open_handle *handle;       /* base file handle */
            uint64_t                        cookie;
            void                           *buffer;       /* caller-provided buffer */
            uint32_t                        max_bytes;
            /* buffer is filled with packed chimera_vfs_stream_entry records,
             * each followed by name_len bytes of (un-terminated) name. */
            uint32_t                        r_len;        /* bytes written to buffer */
            uint32_t                        r_count;      /* number of streams written */
            uint32_t                        r_eof;
            uint64_t                        r_cookie;
        } list_streams;

        struct {
            struct chimera_vfs_open_handle *handle;       /* base file handle */
            const char                     *name;
            uint32_t                        namelen;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } remove_stream;

        /* pNFS: a layout-sourcing backend (CHIMERA_VFS_CAP_LAYOUT_SOURCE)
         * describes where the file's data physically lives.  The NFS server
         * encodes the returned segments/devices into a flex-files or block
         * layout; the descriptors are protocol-neutral. */
        struct {
            struct chimera_vfs_open_handle   *handle;
            uint64_t                          offset;
            uint64_t                          length;
            uint32_t                          iomode;        /* LAYOUTIOMODE4_*           */
            uint32_t                          layout_class;  /* requested CHIMERA_VFS_LAYOUT_CLASS_* */
            uint32_t                          max_segments;
            uint32_t                          r_layout_class;/* class actually produced   */
            uint32_t                          r_num_segments;
            uint32_t                          r_num_devices;
            struct chimera_vfs_layout_segment r_segments[CHIMERA_VFS_LAYOUT_MAX_SEGMENTS];
            struct chimera_vfs_layout_device  r_devices[CHIMERA_VFS_LAYOUT_MAX_DEVICES];
        } get_layout;
    };
};
