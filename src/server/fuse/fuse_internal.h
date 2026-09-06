// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdatomic.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <utlist.h>
#include <uthash.h>
#include <xxhash.h>
#include <linux/fuse.h>

#include "evpl/evpl.h"
#include "common/logging.h"
#include "vfs/vfs.h"
#include "vfs/sdk/vfs_error.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_acl.h"
#include "vfs/sdk/vfs_access.h"
#include "vfs/vfs_claim_types.h"
#include "fuse_node_table.h"

struct chimera_vfs_state;
struct chimera_vfs_file_state;
struct chimera_vfs_notify_watch;

#define chimera_fuse_debug(...) chimera_debug("fuse", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_fuse_info(...)  chimera_info("fuse", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_fuse_error(...) chimera_error("fuse", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_fuse_fatal(...) chimera_fatal("fuse", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_fuse_abort(...) chimera_abort("fuse", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_fuse_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "fuse", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_fuse_abort_if(cond, ...) \
        chimera_abort_if(cond, "fuse", __FILE__, __LINE__, __VA_ARGS__)

#define CHIMERA_FUSE_MAX_MOUNTS      16
#define CHIMERA_FUSE_MAX_THREADS     64

/* What we ask the kernel to cap a single WRITE payload at.  The negotiated
 * value lands in mount->max_write and can only be smaller. */
#define CHIMERA_FUSE_MAX_WRITE       (1024 * 1024)

/* A /dev/fuse read must always offer room for max_write plus the request
 * headers, or the kernel fails the read with EINVAL. */
#define CHIMERA_FUSE_READ_LEN        (CHIMERA_FUSE_MAX_WRITE + 4096)

/*
 * Where the request is placed inside the buffer.
 *
 * The kernel lays a WRITE out contiguously as
 * [fuse_in_header][fuse_write_in][payload], so the payload always begins
 * exactly sizeof(fuse_in_header) + sizeof(fuse_write_in) == 80 bytes into
 * the request.  Reading at that many bytes BELOW a page boundary puts the
 * payload ON the boundary, which is what lets a backend DMA straight out of
 * this buffer instead of bouncing it through an aligned staging copy --
 * diskfs's zero-copy device write requires a single 4 KiB-aligned segment
 * (its fast path additionally wants a 4 KiB-aligned file offset, which is
 * the workload's business, not ours).  Costs one page of address space per
 * request buffer and nothing else.
 */
#define CHIMERA_FUSE_REQ_OFF \
        (4096 - (int) (sizeof(struct fuse_in_header) + sizeof(struct fuse_write_in)))

#define CHIMERA_FUSE_BUFSZ           (CHIMERA_FUSE_REQ_OFF + CHIMERA_FUSE_READ_LEN)

/* The invariant the offset above exists to create. */
_Static_assert((CHIMERA_FUSE_REQ_OFF + sizeof(struct fuse_in_header) +
                sizeof(struct fuse_write_in)) % 4096 == 0,
               "FUSE write payload must land on a page boundary");

/* Scratch iovec array for VFS reads: enough for max_write in 4KB pieces. */
#define CHIMERA_FUSE_IOV_MAX         260

/* Oldest FUSE minor we speak; 7.23 dates to Linux 3.15, so the compat
 * variable-size INIT/SETATTR shapes older kernels need are out of scope. */
#define CHIMERA_FUSE_MIN_MINOR       23

/* Requests drained from a channel per read-callback invocation before
 * yielding the loop to other work; the event stays readable so the next pass
 * resumes immediately. */
#define CHIMERA_FUSE_READ_BATCH      16

/* Pooled requests kept per thread; each pins a CHIMERA_FUSE_BUFSZ buffer. */
#define CHIMERA_FUSE_MAX_POOLED_REQS 64

/* Clone one /dev/fuse channel per core thread (FUSE_DEV_IOC_CLONE), so each
 * thread reads and replies on its own kernel queue.  Set to 0 to fall back
 * to a single channel on thread slot 0 for debugging. */
#define CHIMERA_FUSE_MULTIQUEUE      1

struct chimera_fuse_thread;
struct chimera_fuse_shared;
struct chimera_fuse_request;
struct chimera_fuse_mount;
struct chimera_vfs_compound;

/* Retries for a whole-request compound replay (CHIMERA_VFS_ECOMPOUND_CONFLICT)
 * before the request is failed with the retriable EAGAIN. */
#define CHIMERA_FUSE_COMPOUND_MAX_RETRIES 8

/* Internal errno sentinel produced by chimera_fuse_errno() for
 * CHIMERA_VFS_ECOMPOUND_CONFLICT.  It never reaches the kernel: the reply
 * helpers intercept it, abort the request's compound, and replay the whole
 * request from the top. */
#define CHIMERA_FUSE_ECONFLICT            0x434f4e46

/* The reply shape parked in the request while its compound's end settles
 * (see the reply helpers in fuse_dispatch.c): the deliver path sends the
 * parked reply from the end callback, so nothing reaches the kernel before
 * the compound commits -- which is what makes a commit-time conflict safe
 * to replay. */
enum chimera_fuse_reply_kind {
    CHIMERA_FUSE_REPLY_NONE = 0,    /* nothing parked (no-reply op, conflict) */
    CHIMERA_FUSE_REPLY_SIMPLE,      /* error or one payload blob */
    CHIMERA_FUSE_REPLY_READ,        /* header + data iovecs (FUSE_READ) */
    CHIMERA_FUSE_REPLY_ENTRY,       /* entry-shaped (LOOKUP/creates/LINK) */
    CHIMERA_FUSE_REPLY_OPENFILE,    /* fuse_open_out built at deliver (OPEN/OPENDIR) */
};

/*
 * One POSIX byte-range lock held on behalf of a local process.  The embedded
 * RANGE lease is what the shared vfs_state conflict matrix walks, so these
 * locks conflict correctly with NLM, NFSv4, and SMB2 locks.  The range is
 * POSIX-inclusive [start, end]; end == CHIMERA_FUSE_LOCK_EOF means to-EOF.
 */
#define CHIMERA_FUSE_LOCK_EOF 0x7fffffffffffffffULL

struct chimera_fuse_lock {
    struct chimera_fuse_lock_file     *lf;
    uint64_t                           start;
    uint64_t                           end;
    int                                exclusive;
    struct chimera_vfs_claim           claim;
    struct chimera_vfs_pending_acquire ticket;
    struct chimera_fuse_lock          *prev;
    struct chimera_fuse_lock          *next;
};

/* Per-(owner, file) lock bookkeeping: the unit FLUSH's lock_owner releases.
 * Keyed by {owner token, fh_hash} in the mount's lock table; holds a
 * vfs_state file-state reference for the life of its entries. */
struct chimera_fuse_lock_key {
    uint64_t owner;
    uint64_t fh_hash;
};

struct chimera_fuse_lock_file {
    struct chimera_fuse_lock_key   key;
    uint8_t                        fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                       fh_len;
    struct chimera_vfs_file_state *file_state;
    struct chimera_fuse_lock      *locks;
    /* Live blocked acquires against this (owner, file); counted so the
     * bucket outlives a parked SETLKW even with no granted locks. */
    int                            pending;
    UT_hash_handle                 hh;
};

/*
 * Per-(mount, file) caching lease whose break drives kernel cache
 * invalidation (FUSE_NOTIFY_INVAL_INODE).  One kernel = one lease owner per
 * mount.  The NFSv4 delegation shape: a bare CACHING lease inserted with
 * try_insert, declined on contention.
 *
 * Lifetime follows the NODEID, not the open: the kernel serves cached
 * attributes and pages for as long as it holds the inode -- well past the
 * last close -- so the grant lives from the first open until the kernel
 * FORGETs the node (plus a transient notifier reference mid-break).
 */
enum chimera_fuse_grant_state {
    CHIMERA_FUSE_GRANT_ACTIVE   = 0,
    CHIMERA_FUSE_GRANT_BREAKING = 1, /* break_cb fired; notifier owns resolution */
    CHIMERA_FUSE_GRANT_BROKEN   = 2, /* lease gone; struct lives on open refs */
};

struct chimera_fuse_grant {
    struct chimera_fuse_mount     *mount;
    uint64_t                       nodeid; /* hash key */
    uint64_t                       fh_hash;
    uint8_t                        fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                       fh_len;
    struct chimera_vfs_file_state *file_state;
    struct chimera_vfs_claim       claim;
    /* Break/revoke callbacks fire on arbitrary threads inside vfs_state and
     * may touch only these atomics plus the notifier queue. */
    _Atomic int                    state; /* enum chimera_fuse_grant_state */
    /* open references + one held by the notifier while a break resolves */
    _Atomic int                    refcount;
    _Atomic int                    revoked;
    UT_hash_handle                 hh;
};

/* A directory the kernel holds dentries under: a change-notify watch whose
 * events become FUSE_NOTIFY_INVAL_ENTRY / NOTIFY_DELETE.  Keyed by the
 * directory's nodeid; dropped when the kernel forgets the directory. */
struct chimera_fuse_dirwatch {
    uint64_t                         nodeid;
    struct chimera_fuse_mount       *mount;
    struct chimera_vfs_notify_watch *watch;
    int                              queued; /* on the notifier queue */
    UT_hash_handle                   hh;
};

/* Work items for the dedicated notifier thread.  Kernel-cache invalidation
 * writes to /dev/fuse can block inside the kernel until conflicting pages
 * settle, so they must never run on an event-loop thread (whose blocked
 * loop could be the one that must complete the settling I/O). */
enum chimera_fuse_notice_type {
    CHIMERA_FUSE_NOTICE_INVAL_FILE = 0, /* lease broke: invalidate + ack */
    CHIMERA_FUSE_NOTICE_DIR_EVENTS = 1, /* drain a dirwatch into entry invals */
};

struct chimera_fuse_notice {
    enum chimera_fuse_notice_type type;
    struct chimera_fuse_mount  *mount;
    struct chimera_fuse_grant  *grant;    /* INVAL_FILE */
    uint64_t                    nodeid;   /* DIR_EVENTS */
    struct chimera_fuse_notice *next;
};

struct chimera_fuse_mount {
    struct chimera_fuse_shared *shared;
    char                        mountpoint[256];
    char                        share_path[256];
    int                         allow_other;
    int                         default_permissions;
    uint32_t                    attr_timeout_ms;
    uint32_t                    entry_timeout_ms;
    uint32_t                    negative_timeout_ms;
    /* coherence=sync (default): replies carry cache TTLs only under a live
     * grant/watch, grants demand synchronous breaks (owner.sync_break), and
     * namespace mutations elsewhere gate on this kernel's invalidation acks.
     * coherence=ttl: the async model; timeouts alone bound staleness. */
    int                         coherence_sync;
    /* direct_io: reply FOPEN_DIRECT_IO on every open, so the kernel never
     * stages this mount's file data in its page cache -- reads and writes
     * become exact (offset, length) requests built straight from the
     * caller's pinned pages.  This is OUR choice, not the application's:
     * it applies to a plain open() and imposes none of O_DIRECT's alignment
     * requirements on the caller.  It removes one copy per operation in
     * each direction and, for a page-aligned writer, is what lets a write
     * reach the backend's DMA path; it also removes readahead, read
     * caching, and write batching, which is far more expensive for small
     * or repeated I/O.  Off by default; for bulk streaming workloads only.
     *
     * parallel_direct_writes additionally sets FOPEN_PARALLEL_DIRECT_WRITES
     * so concurrent writers to one file are not serialized on the inode
     * lock (the kernel's default under direct I/O).  It relaxes write
     * atomicity between overlapping concurrent writes, so it is separately
     * opt-in -- but without it a multi-threaded write benchmark measures
     * the inode lock rather than the data path. */
    int                             direct_io;
    int                             parallel_direct_writes;
    /* Negotiated: the kernel permits shared mmap on FOPEN_DIRECT_IO opens
     * (FUSE_DIRECT_IO_ALLOW_MMAP, ABI 7.39+).  Without it, mmap of a file on
     * a direct_io mount fails. */
    int                             direct_io_mmap;
    uint32_t                        max_write;   /* negotiated */
    uint32_t                        proto_minor; /* negotiated */
    int                             mounted;
    int                             dead;
    /* Test transport (chimera_fuse_add_synthetic_mount): when >= 0, setup
     * adopts this descriptor as the mount's sole channel instead of opening
     * /dev/fuse and issuing mount(2).  Everything downstream is unchanged --
     * the channel is an int fd either way -- so the server cannot tell a
     * simulated kernel from a real one.  -1 for every real mount. */
    int                             synthetic_fd;
    int                             num_channels;
    int                             channel_fds[CHIMERA_FUSE_MAX_THREADS];
    struct chimera_fuse_node_table *node_table;
    /* Open files the kernel has not RELEASEd yet, so shutdown can release
    * their VFS handles.  Shared across threads (multi-queue delivery). */
    pthread_mutex_t                 open_lock;
    struct chimera_fuse_open_file  *open_files;
    /* POSIX byte-range lock table: (owner, file) buckets plus the parked
     * SETLKW requests INTERRUPT may cancel.  All under lock_lock. */
    pthread_mutex_t                 lock_lock;
    struct chimera_fuse_lock_file  *lock_files;
    struct chimera_fuse_request    *parked_locks;
    /* Kernel-cache coherence: per-file caching grants and per-directory
     * change watches, both under grant_lock (a leaf: never held while
     * calling into vfs_state or vfs_notify). */
    pthread_mutex_t                 grant_lock;
    struct chimera_fuse_grant      *grants;
    struct chimera_fuse_dirwatch   *dirwatches;
    /* Per-mount directory-event notifier: INVAL_ENTRY writes can block on
     * THIS kernel's directory locks (a namespace syscall in flight), so
     * each mount drains its own directory events on its own thread --
     * one mount's blocked entry invalidation must never sit ahead of
     * another mount's acks, or of the never-blocking grant lane on the
     * shared notifier (see chimera_fuse_dir_notifier). */
    pthread_t                       dir_notifier;
    pthread_mutex_t                 dir_notifier_lock;
    pthread_cond_t                  dir_notifier_cond;
    struct chimera_fuse_notice     *dir_notices;
    int                             dir_notifier_running;
    int                             dir_notifier_stop;
    uint8_t                         root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        root_fh_len;
};

struct chimera_fuse_shared {
    struct chimera_vfs         *vfs;
    struct prometheus_metrics  *metrics;
    pthread_mutex_t             lock;
    int                         num_mounts;
    struct chimera_fuse_mount   mounts[CHIMERA_FUSE_MAX_MOUNTS];
    int                         num_threads;
    int                         threads_alive;
    struct chimera_fuse_thread *threads[CHIMERA_FUSE_MAX_THREADS];
    int                         started;
    /* Dedicated invalidation-notifier thread (see chimera_fuse_notice). */
    pthread_t                   notifier;
    pthread_mutex_t             notifier_lock;
    pthread_cond_t              notifier_cond;
    struct chimera_fuse_notice *notices;
    int                         notifier_running;
    int                         notifier_stop;
};

struct chimera_fuse_channel {
    struct chimera_fuse_thread *thread;
    struct chimera_fuse_mount  *mount;
    int                         fd;
    int                         armed;
    int                         dead;
    struct evpl_fd_event        event;
};

/* One per kernel OPEN/OPENDIR/CREATE; fuse_open_out.fh carries its pointer.
* The VFS handle is captured into the request at dispatch time, so nothing
* but OPEN/RELEASE and the shutdown sweep touches this struct afterwards. */
struct chimera_fuse_open_file {
    struct chimera_vfs_open_handle *handle;
    struct chimera_fuse_mount      *mount;
    uint64_t                        readdir_verifier;
    struct chimera_fuse_open_file  *prev;
    struct chimera_fuse_open_file  *next;
};

struct chimera_fuse_thread {
    struct evpl                 *evpl;
    struct chimera_vfs_thread   *vfs_thread;
    struct chimera_fuse_shared  *shared;
    int                          thread_slot;
    struct evpl_doorbell         attach_doorbell;
    struct chimera_fuse_channel  channels[CHIMERA_FUSE_MAX_MOUNTS];
    int                          num_channels;
    struct chimera_fuse_request *free_requests;
    int                          num_free_requests;
    int                          active_requests;
    /* Requests completed off-thread (a blocked lock granted or cancelled)
     * marshalled home for their reply, the cb_doorbell pattern. */
    pthread_mutex_t              resume_lock;
    struct chimera_fuse_request *resume_queue;
    struct evpl_doorbell         resume_doorbell;
};

struct chimera_fuse_request {
    struct chimera_fuse_thread     *thread;
    struct chimera_fuse_channel    *channel;
    struct chimera_fuse_request    *next;
    struct chimera_vfs_cred         cred;
    uint64_t                        unique;
    uint32_t                        opcode;
    uint64_t                        nodeid;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    /* Second handle for the two-node ops (LINK's target, RENAME's newdir). */
    uint8_t                         fh2[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh2_len;
    /* Transient VFS handle for the op in flight; released by the terminal
     * completion before the reply. */
    struct chimera_vfs_open_handle *handle;
    /* OPEN/CREATE result carrier. */
    struct chimera_fuse_open_file  *file;
    /* Receive buffer; request field pointers (names, write payload) point
     * into it, so it is not reused until the request is freed. */
    struct evpl_iovec               buf;
    int                             buf_allocated;
    uint32_t                        buf_len;

    /* Coverage captured at request ENTRY (before the backend op) by ops that
     * condition reply TTLs on it -- a CHIMERA_FUSE_COVER_* value.  See the
     * COVER_* comment: only pre-existing coverage protects state the backend
     * fetched before the call. */
    int                             entry_cover;

    /* One FUSE request == one VFS compound (CHIMERA_VFS_COMPOUND_RETRYABLE).
     * Begun lazily at the request's first VFS call (chimera_fuse_req_compound;
     * NULL for ops that make none -- the FORGET class, byte-range locks) and
     * ended in the reply path BEFORE the reply is sent, so a commit-time
     * ECOMPOUND_CONFLICT can replay the whole request without the kernel
     * ever seeing a doomed reply.  compound_ts is the wait-die priority,
     * assigned once at dispatch and reused across replays so a conflicting
     * request cannot starve; compound_attempt bounds the replays. */
    struct chimera_vfs_compound    *compound;
    uint64_t                        compound_ts;
    int                             compound_attempt;

    /* Reply parked while the compound end settles (fuse_dispatch.c).  SIMPLE
    * payloads are either copied into pending_copy (small stack-built reply
    * structs) or left pointing into the request buffer's reply-staging area
    * (readdir/xattr/readlink), which outlives the asynchronous end.  ENTRY
    * captures the attrs (the VFS callback's storage does not survive it)
    * plus the small extra blob (CREATE's fuse_open_out); the node insert,
    * coverage arm, TTL policy, and send all run at deliver, after commit. */
    enum chimera_fuse_reply_kind    pending_kind;
    int                             pending_error;
    const void                     *pending_payload;
    size_t                          pending_payload_len;
    struct evpl_iovec              *pending_iov;
    int                             pending_niov;
    size_t                          pending_data_len;
    size_t                          pending_extra_len;
    uint8_t                         pending_copy[256];
    uint8_t                         pending_extra[64];
    struct chimera_vfs_attrs        pending_attr;
    /* The request allocated req->file itself (OPEN/OPENDIR/CREATE): the
     * deliver path undoes it (unlink, release, free) when the kernel never
     * learns the fh -- an error reply, a failed send, or a conflict replay
     * -- and clears the flag once the kernel owns it. */
    uint8_t                         file_owned;

    union {
        struct {
            uint32_t size;      /* kernel's reply size limit */
            uint32_t used;      /* bytes packed so far */
            int      plus;      /* READDIRPLUS */
        } readdir;
        struct {
            struct chimera_vfs_attrs set_attr;
        } setattr;
        struct {
            struct chimera_vfs_attrs set_attr;
        } create;               /* also mkdir/mknod/symlink */
        struct {
            struct evpl_iovec iov;
            uint32_t          size;
        } write;
        struct {
            struct evpl_iovec iov[CHIMERA_FUSE_IOV_MAX];
        } read;
        struct {
            uint32_t size;      /* getxattr/listxattr size probe or limit */
        } xattr;
        struct {
            /* Heap entry embedding the lease/ticket; the lease's address
             * must be stable once inserted, so it never lives here. */
            struct chimera_fuse_lock      *entry;
            struct chimera_fuse_lock_file *lf;
            /* 0 = dispatch in progress, 1 = callback completed inline,
             * 2 = dispatch returned (a later callback must marshal home) */
            atomic_int                     phase;
            int                            result_errno;
            uint64_t                       start;
            uint64_t                       end;
            int                            exclusive;
            int                            wait;
            int                            parked; /* on mount->parked_locks */
            struct chimera_fuse_request   *park_prev;
            struct chimera_fuse_request   *park_next;
        } lock;
        /* DAC pre-check context for ops that must authorize before opening.
         * Lives until the gate's callback fires, so it cannot share storage
         * with the op's own scratch. */
        struct chimera_vfs_gate_ctx gate;
        struct {
            struct chimera_vfs_gate_ctx gate;
            unsigned int                vfs_flags;
            /* Access mask the gate authorized, bound onto the handle so
             * later I/O does not re-derive it from a since-changed mode. */
            uint32_t                    granted;
        } open;
    } u;
};

/* fuse_dispatch.c */
void
chimera_fuse_channel_readable(
    struct evpl          *evpl,
    struct evpl_fd_event *event);

void
chimera_fuse_channel_error(
    struct evpl          *evpl,
    struct evpl_fd_event *event);

void
chimera_fuse_channel_dead(
    struct chimera_fuse_channel *channel);

/* The request's compound, begun lazily at its first VFS call.  Pass its
 * result as the `compound` argument of every VFS call a request makes; the
 * reply helpers end it (commit-before-send) and replay on conflict. */
struct chimera_vfs_compound *
chimera_fuse_req_compound(
    struct chimera_fuse_request *req);

/* Reply helpers: park the reply on the request, end the request's compound
 * (COMMIT; COMMIT_DURABLE for FSYNC/FSYNCDIR), and deliver the parked reply
 * from the end callback -- releasing any transient handle and recycling the
 * request.  Nothing reaches the kernel before the compound end settles; an
 * ECOMPOUND_CONFLICT (from a member op via the CHIMERA_FUSE_ECONFLICT errno
 * sentinel, or from the commit itself) aborts and replays the whole request
 * instead of delivering.  A reply that hands the kernel a reference (an
 * entry's lookup count, an open's fh) is undone by the deliver path when the
 * kernel never takes it. */
void
chimera_fuse_reply(
    struct chimera_fuse_request *req,
    int                          error,
    const void                  *payload,
    size_t                       payload_len);

/* READ replies: sends header + data iovecs, then releases the iovecs on the
 * request's thread before the request is recycled. */
void
chimera_fuse_reply_read(
    struct chimera_fuse_request *req,
    int                          error,
    struct evpl_iovec           *iov,
    int                          niov,
    size_t                       data_len);

/* Entry-shaped reply (LOOKUP, CREATE, MKDIR, MKNOD, SYMLINK, LINK): captures
 * attr + extra now; the node registration, coverage arm, and TTL policy run
 * at deliver, after the compound commits.  Requires attr to carry a file
 * handle (EIO reply otherwise). */
void
chimera_fuse_reply_entry(
    struct chimera_fuse_request    *req,
    const struct chimera_vfs_attrs *attr,
    const void                     *extra,
    size_t                          extra_len);

/* OPEN/OPENDIR reply: req->file (owned) carries the open; the fuse_open_out
 * -- including OPEN's cache-flag grant arm -- is built at deliver. */
void
chimera_fuse_reply_open(
    struct chimera_fuse_request *req);

void
chimera_fuse_request_finish(
    struct chimera_fuse_request *req);

void
chimera_fuse_request_free(
    struct chimera_fuse_thread  *thread,
    struct chimera_fuse_request *req);

/* fuse_mount.c */
int
chimera_fuse_mount_setup(
    struct chimera_fuse_shared *shared,
    struct chimera_fuse_mount  *mount);

void
chimera_fuse_mount_teardown(
    struct chimera_fuse_mount *mount);

/* Per-opcode handlers (fuse_proc_*.c); each owns the request until it calls
 * a reply helper (or frees it directly for the no-reply ops).  A handler must
 * be re-runnable from the top against its intact request buffer: a compound
 * conflict replays it (chimera_fuse_request_replay) after the driver has
 * released the attempt's transient handle and owned open_file. */
typedef void (*chimera_fuse_handler_t)(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);

#define CHIMERA_FUSE_OPCODE_MAX 64

extern const chimera_fuse_handler_t chimera_fuse_handlers[CHIMERA_FUSE_OPCODE_MAX];

/* fuse_proc_lookup.c */
void chimera_fuse_op_lookup(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_forget(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_batch_forget(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);

/* fuse_proc_getattr.c */
void chimera_fuse_op_getattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_setattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_readlink(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_statfs(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_access(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);

/* fuse_proc_dir.c */
void chimera_fuse_op_opendir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_readdir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_releasedir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_fsyncdir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);

/* fuse_proc_io.c */
void chimera_fuse_op_open(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_create(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_read(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_write(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_flush(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_fsync(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_release(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_fallocate(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_lseek(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_copy_file_range(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);

/* fuse_proc_namespace.c */
void chimera_fuse_op_mkdir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_mknod(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_symlink(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_link(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_unlink(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_rmdir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_rename(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);

/* fuse_proc_xattr.c */
void chimera_fuse_op_getxattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_setxattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_listxattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_removexattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);

/* fuse_proc_lock.c */
void chimera_fuse_op_getlk(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_setlk(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);

/* Release every lock the given owner holds on the given file (the FLUSH
 * lock_owner contract: any close by the owning process drops its locks). */
void
chimera_fuse_locks_release_owner(
    struct chimera_fuse_thread *thread,
    struct chimera_fuse_mount  *mount,
    uint64_t                    fh_hash,
    uint64_t                    owner);

/* Cancel the parked SETLKW with the given unique, if any.  Returns 1 when a
 * parked lock was found and cancellation initiated (the original request
 * replies EINTR via its owning thread), 0 when the unique is unknown. */
int
chimera_fuse_locks_interrupt(
    struct chimera_fuse_mount *mount,
    struct chimera_vfs_state  *state,
    uint64_t                   unique);

/* Teardown: cancel parked acquires and release every granted lock. */
void
chimera_fuse_locks_shutdown(
    struct chimera_fuse_shared *shared,
    struct chimera_fuse_mount  *mount);

/* Reply path for a blocked lock resumed on its owning thread. */
void
chimera_fuse_lock_resume(
    struct chimera_fuse_request *req);

/* fuse_dispatch.c: marshal a request home for its reply (any thread). */
void
chimera_fuse_resume_post(
    struct chimera_fuse_request *req);

void
chimera_fuse_resume_doorbell(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell);

/* fuse_coherence.c */
void
chimera_fuse_notifier_start(
    struct chimera_fuse_shared *shared);

void
chimera_fuse_notifier_stop(
    struct chimera_fuse_shared *shared);

/* Coverage state reported by grant_ensure / watch_dir.  HELD means the
 * grant/watch was already in force before this call -- data fetched from
 * the backend BEFORE the call is protected (a conflicting foreign mutation
 * would have broken it / been gated on it).  FRESH means coverage begins
 * NOW: caches seeded through us from here on are protected, but state
 * fetched before the call raced an uncovered window and must not be
 * cached with a TTL. */
#define CHIMERA_FUSE_COVER_NONE  0
#define CHIMERA_FUSE_COVER_FRESH 1
#define CHIMERA_FUSE_COVER_HELD  2

/* Ensure a node's invalidation lease exists / is re-armed (best-effort,
 * idempotent; callers gate on S_ISREG).  These are the rearm-on-demand
 * sites: every kernel-driven touch of a file -- LOOKUP, READDIRPLUS,
 * GETATTR, OPEN, READ, WRITE -- re-arms a previously broken grant, which
 * self-throttles against a remote write burst (at most one break per
 * kernel re-touch). */
int
chimera_fuse_grant_ensure(
    struct chimera_fuse_thread *thread,
    struct chimera_fuse_mount  *mount,
    uint64_t                    nodeid,
    const uint8_t              *fh,
    uint32_t                    fh_len,
    uint64_t                    fh_hash);

/* True while the node's grant is ACTIVE right now.  The reply-time check
 * behind sync-mode attribute TTLs: armed before the backend op AND still
 * unbroken at reply time means no foreign write raced the fetch. */
int
chimera_fuse_grant_active(
    struct chimera_fuse_mount *mount,
    uint64_t                   nodeid);

/* Request-ENTRY coverage for a node whose type is not yet known (GETATTR):
 * an existing grant is re-armed (COVER_FRESH/HELD), an existing dirwatch
 * reports HELD, and an unknown node reports NONE -- coverage then begins at
 * the completion's arm, protecting the NEXT fetch. */
int
chimera_fuse_cover_touch(
    struct chimera_fuse_thread *thread,
    struct chimera_fuse_mount  *mount,
    uint64_t                    nodeid,
    const uint8_t              *fh,
    uint32_t                    fh_len);

int
chimera_fuse_grant_open(
    struct chimera_fuse_thread     *thread,
    struct chimera_fuse_mount      *mount,
    uint64_t                        nodeid,
    struct chimera_vfs_open_handle *handle);

/* The kernel forgot this nodeid: nothing is cached any more, so drop its
 * invalidation lease. */
void
chimera_fuse_grant_forget(
    struct chimera_fuse_mount *mount,
    struct chimera_vfs_state  *state,
    uint64_t                   nodeid);

/* Ensure a change-notify watch exists for a directory the kernel is about
 * to hold dentries under (best-effort, idempotent).  Returns the COVER_*
 * state; in coherence=sync the watch also gates foreign namespace
 * mutations on our invalidation ack. */
int
chimera_fuse_watch_dir(
    struct chimera_fuse_thread *thread,
    struct chimera_fuse_mount  *mount,
    uint64_t                    nodeid,
    const uint8_t              *fh,
    uint32_t                    fh_len);

/* The kernel forgot this nodeid; drop its directory watch if one exists. */
void
chimera_fuse_watch_forget(
    struct chimera_fuse_mount *mount,
    struct chimera_vfs        *vfs,
    uint64_t                   nodeid);

/* Teardown: destroy remaining watches and release remaining grants. */
void
chimera_fuse_coherence_shutdown(
    struct chimera_fuse_shared *shared,
    struct chimera_fuse_mount  *mount);

/* File-handle hash for vfs_state keying.  Must match the open-cache hashing
 * (chimera_vfs_hash in vfs_internal.h: XXH3 masked non-negative) so a grant
 * taken from a bare handle lands on the same file_state as the leases taken
 * through open handles. */
static inline uint64_t
chimera_fuse_fh_hash(
    const uint8_t *fh,
    uint32_t       fh_len)
{
    return XXH3_64bits(fh, fh_len) & INT64_MAX;
} /* chimera_fuse_fh_hash */

/* The lease-owner identity FUSE I/O runs under, matching the mount's
 * invalidation grants so a mount's own reads and writes never invalidate
 * its own kernel cache (the kernel wrote through us; its cache is right).
 * Locks use owner_hi = 1 so a kernel lock-owner token can never collide
 * with a grant identity. */
static inline void
chimera_fuse_grant_owner(
    struct chimera_claim_owner *owner,
    struct chimera_fuse_mount  *mount,
    uint64_t                    fh_hash)
{
    memset(owner, 0, sizeof(*owner));
    owner->proto      = CHIMERA_CLAIM_PROTO_FUSE;
    owner->client_key = (uint64_t) (uintptr_t) mount;
    owner->owner_lo   = fh_hash;
    owner->owner_hi   = 0;
} /* chimera_fuse_grant_owner */

static inline void
chimera_fuse_lock_owner(
    struct chimera_claim_owner *owner,
    struct chimera_fuse_mount  *mount,
    uint64_t                    token)
{
    memset(owner, 0, sizeof(*owner));
    owner->proto      = CHIMERA_CLAIM_PROTO_FUSE;
    owner->client_key = (uint64_t) (uintptr_t) mount;
    owner->owner_lo   = token;
    owner->owner_hi   = 1;
} /* chimera_fuse_lock_owner */

/*
 * The kernel-cache policy bits for an open reply, given the invalidation
 * coverage the open ended up with.  One function so OPEN and CREATE cannot
 * drift apart.
 *
 * direct_io wins outright and is mutually exclusive with KEEP_CACHE: the
 * mount has asked that the kernel never stage its file data, so there are no
 * cached pages for coverage to protect.  Otherwise coverage decides -- a
 * grant in force makes cached pages safe across open/close cycles, and no
 * coverage under sync coherence falls back to uncached rather than risk
 * serving pages we could not invalidate.
 */
static inline uint32_t
chimera_fuse_open_cache_flags(
    struct chimera_fuse_mount *mount,
    int                        cover)
{
    if (mount->direct_io) {
        uint32_t flags = FOPEN_DIRECT_IO;

#ifdef FOPEN_PARALLEL_DIRECT_WRITES
        if (mount->parallel_direct_writes) {
            flags |= FOPEN_PARALLEL_DIRECT_WRITES;
        }
#endif /* ifdef FOPEN_PARALLEL_DIRECT_WRITES */
        return flags;
    }

    if (cover != CHIMERA_FUSE_COVER_NONE) {
        return FOPEN_KEEP_CACHE;
    }

    if (mount->coherence_sync) {
        return FOPEN_DIRECT_IO;
    }

    return 0;
} /* chimera_fuse_open_cache_flags */

/* Byte offset into the request buffer where reply payloads are staged.  It
 * sits one page past the request start so no reply-staging opcode's request
 * can reach it: the ops that stage (READDIR, GETXATTR, LISTXATTR, READLINK)
 * all carry bodies of at most a header plus one name.  The opcodes with
 * genuinely large bodies -- WRITE and SETXATTR -- stage no reply at all, so
 * their payloads may run past this offset freely. */
#define CHIMERA_FUSE_REPLY_OFF (CHIMERA_FUSE_REQ_OFF + 4096)

static inline uint8_t *
chimera_fuse_reply_space(struct chimera_fuse_request *req)
{
    return (uint8_t *) evpl_iovec_data(&req->buf) + CHIMERA_FUSE_REPLY_OFF;
} /* chimera_fuse_reply_space */

/* The request header, which does NOT sit at the buffer base (see
 * CHIMERA_FUSE_REQ_OFF).  Callbacks re-derive their request body from here. */
static inline const struct fuse_in_header *
chimera_fuse_request_hdr(struct chimera_fuse_request *req)
{
    return (const struct fuse_in_header *)
           ((uint8_t *) evpl_iovec_data(&req->buf) + CHIMERA_FUSE_REQ_OFF);
} /* chimera_fuse_request_hdr */

static inline struct chimera_fuse_open_file *
chimera_fuse_file(uint64_t fh)
{
    return (struct chimera_fuse_open_file *) (uintptr_t) fh;
} /* chimera_fuse_file */

static inline void
chimera_fuse_file_link(
    struct chimera_fuse_mount     *mount,
    struct chimera_fuse_open_file *file)
{
    pthread_mutex_lock(&mount->open_lock);
    DL_APPEND(mount->open_files, file);
    pthread_mutex_unlock(&mount->open_lock);
} /* chimera_fuse_file_link */

static inline void
chimera_fuse_file_unlink(
    struct chimera_fuse_mount     *mount,
    struct chimera_fuse_open_file *file)
{
    pthread_mutex_lock(&mount->open_lock);
    DL_DELETE(mount->open_files, file);
    pthread_mutex_unlock(&mount->open_lock);
} /* chimera_fuse_file_unlink */

/* Resolve the request's nodeid to a file handle; -1 means the kernel named a
 * node we no longer know (reply ESTALE). */
static inline int
chimera_fuse_resolve_nodeid(struct chimera_fuse_request *req)
{
    struct chimera_fuse_mount *mount = req->channel->mount;

    if (req->nodeid == FUSE_ROOT_ID) {
        memcpy(req->fh, mount->root_fh, mount->root_fh_len);
        req->fh_len = mount->root_fh_len;
        return 0;
    }

    return chimera_fuse_node_get_fh(mount->node_table, req->nodeid,
                                    req->fh, &req->fh_len);
} /* chimera_fuse_resolve_nodeid */

/* Same for a second node named inside the request body (LINK, RENAME). */
static inline int
chimera_fuse_resolve_nodeid2(
    struct chimera_fuse_request *req,
    uint64_t                     nodeid)
{
    struct chimera_fuse_mount *mount = req->channel->mount;

    if (nodeid == FUSE_ROOT_ID) {
        memcpy(req->fh2, mount->root_fh, mount->root_fh_len);
        req->fh2_len = mount->root_fh_len;
        return 0;
    }

    return chimera_fuse_node_get_fh(mount->node_table, nodeid,
                                    req->fh2, &req->fh2_len);
} /* chimera_fuse_resolve_nodeid2 */

/*
 * chimera_vfs_error values are Linux errno numbers apart from two synthetic
 * sentinels, so the mapping to the (positive) errno a FUSE reply carries is
 * nearly the identity.
 */
static inline int
chimera_fuse_errno(enum chimera_vfs_error error_code)
{
    switch (error_code) {
        case CHIMERA_VFS_OK:
            return 0;
        case CHIMERA_VFS_EBADCOOKIE:
            return EINVAL;
        case CHIMERA_VFS_UNSET:
            return EIO;
        case CHIMERA_VFS_ECOMPOUND_CONFLICT:
            /* Never sent: the reply helpers intercept the sentinel and
             * replay the whole request (see fuse_dispatch.c). */
            return CHIMERA_FUSE_ECONFLICT;
        case CHIMERA_VFS_ECOMPOUND_EXHAUSTED:
            /* Retriable but never replayed (a mutating op already committed
             * standalone, or the replay budget ran out). */
            return EAGAIN;
        default:
            return (int) error_code;
    } /* switch */
} /* chimera_fuse_errno */

/* The FUSE header carries only uid/gid (no supplementary groups); mounting
 * with default_permissions makes the kernel do mode-bit checks with the
 * caller's full group list against the attrs we return.  The mount is
 * stamped as the credential's origin so the synchronous notify gate never
 * blocks this kernel's own mutation on its own invalidation ack. */
static inline void
chimera_fuse_map_cred(
    struct chimera_vfs_cred     *cred,
    const struct fuse_in_header *hdr,
    struct chimera_fuse_mount   *mnt)
{
    chimera_vfs_cred_init_unix(cred, hdr->uid, hdr->gid, 0, NULL);
    cred->origin = mnt;
} /* chimera_fuse_map_cred */
