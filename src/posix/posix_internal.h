// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#ifndef CHIMERA_POSIX_INTERNAL_H
#define CHIMERA_POSIX_INTERNAL_H

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include <dirent.h>
#include <utlist.h>
#include "common/macros.h"
#include "../client/client.h"
#include "../client/client_internal.h"
#include "vfs/vfs.h"
#include "vfs/vfs_claim.h"
#include "posix.h"

// Directory stream for opendir/readdir/closedir
struct chimera_posix_dir {
    int                   fd;        // File descriptor for the directory
    uint64_t              cookie;    // Current position cookie
    int                   eof;       // End of directory reached
    int                   buf_valid; // Whether buf contains valid entry
    struct chimera_dirent buf;       // Buffer for current entry
    struct dirent         entry;     // POSIX dirent to return
};

struct chimera_posix_completion {
    pthread_mutex_t                mutex;
    pthread_cond_t                 cond;
    struct chimera_client_request *request;
    enum chimera_vfs_error status;
    int                            done;
};

#define CHIMERA_POSIX_FD_IO_ACTIVE 0x01
#define CHIMERA_POSIX_FD_CLOSING   0x02
#define CHIMERA_POSIX_FD_CLOSED    0x04

/* One open file description (POSIX XBD): the file offset and status flags
 * shared by every descriptor duplicated from one open() -- dup/dup2/
 * F_DUPFD return descriptors referencing the SAME description, so an
 * lseek or F_SETFL through one duplicate is visible through the others.
 * Independent open() calls create independent descriptions.
 *
 * refcnt counts fd entries referencing the description and is managed
 * under the client's fd_lock.  offset/oflags carry the same (loose)
 * concurrency discipline the per-entry fields had: the per-descriptor
 * IO_ACTIVE gate serializes offset-consuming I/O on one descriptor;
 * concurrent I/O through two duplicates of one description is not
 * additionally serialized here. */
struct chimera_posix_ofd {
    uint64_t                        offset;
    unsigned int                    oflags; // Raw open(2) flags (for O_ACCMODE checks)
    int                             refcnt;
    /* Byte-range lock claims this description holds in the local claim core
     * (heap chimera_posix_ofd_lock nodes, claim embedded first).  Guarded by
     * the client's fd_lock, like the description refcount.  Released when the
     * last duplicate of the description closes. */
    struct chimera_posix_ofd_lock  *locks;
    /* Backend range records this description holds that the local core does
     * NOT track: a SEEK_END lock, whose absolute range only the backend
     * resolved.  The old lock wire took these on the file's own descriptor,
     * so closing the file dropped them; the claim wire records them against
     * a descriptor of the backend's own, so they have to be released
     * explicitly.  Released by token at last close, same as `locks`. */
    struct chimera_posix_ofd_token *backend_tokens;
};

/* A backend range record held without a local claim (see backend_tokens). */
struct chimera_posix_ofd_token {
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint8_t                         fh_len;
    uint64_t                        fh_hash;
    uint64_t                        token;
    struct chimera_posix_ofd_token *next;
};

/* One byte-range lock claim held by an open file description in the local
 * claim core.  The embedded claim MUST be the first member: the core hands
 * back released fragments (chimera_vfs_claim_range_replace) as bare claim
 * pointers, and the posix layer recovers the node by cast.  Each node holds
 * its own chimera_vfs_state_get reference on `file` so the anchor outlives
 * the claim. */
struct chimera_posix_ofd_lock {
    struct chimera_vfs_claim       claim;
    struct chimera_vfs_file_state *file;
    struct chimera_posix_ofd      *ofd;  /* owning description; NULL until tracked */
    struct chimera_posix_ofd_lock *prev;
    struct chimera_posix_ofd_lock *next;
};

struct chimera_posix_fd_entry {
    pthread_mutex_t                 lock;
    pthread_cond_t                  cond;
    struct chimera_vfs_open_handle *handle;
    struct chimera_posix_fd_entry  *next;
    struct chimera_posix_ofd       *ofd;
    unsigned int                    flags;
    int                             refcnt;
    int                             io_waiters;
    int                             pending_close;
    int                             close_waiters;
    int                             eof_flag;    // For FILE* feof() support
    int                             error_flag;  // For FILE* ferror() support
    int                             ungetc_char; // For FILE* ungetc() support (-1 = none)
} __attribute__((aligned(64)));

// CHIMERA_FILE is a pointer to an fd_entry for FILE* operations
typedef struct chimera_posix_fd_entry CHIMERA_FILE;

struct chimera_posix_worker {
    pthread_mutex_t                lock;
    struct chimera_client_request *pending_requests;
    struct evpl_doorbell           doorbell;
    struct chimera_client_thread  *client_thread;
    struct chimera_posix_client   *parent;
    int                            index;
    struct evpl                   *evpl;
} __attribute__((aligned(64)));

struct chimera_posix_client {
    struct chimera_client         *client;
    struct evpl_threadpool        *pool;
    struct chimera_posix_worker   *workers;
    int                            nworkers;
    atomic_uint                    next_worker;
    pthread_mutex_t                fd_lock;
    struct chimera_posix_fd_entry *fds;
    struct chimera_posix_fd_entry *free_list;
    int                            max_fds;
    atomic_int                     init_cursor;
    int                            owns_config;
} __attribute__((aligned(64)));

extern struct chimera_posix_client *chimera_posix_global;

/*
 * Per-thread effective credential and umask overrides.
 *
 * The POSIX client is initialized with one client-global credential, but POSIX
 * semantics (and conformance suites such as pjdfstest) require switching the
 * effective uid/gid/groups per operation to exercise permission checks.  These
 * thread-locals let a caller temporarily override the credential and umask for
 * subsequent calls on the same thread; when unset, operations fall back to the
 * client-global credential and apply no umask (matching prior behavior).
 */
extern __thread int                     chimera_posix_tls_has_cred;
extern __thread struct chimera_vfs_cred chimera_posix_tls_cred;
extern __thread int                     chimera_posix_tls_has_umask;
extern __thread mode_t                  chimera_posix_tls_umask;

static FORCE_INLINE const struct chimera_vfs_cred *
chimera_posix_effective_cred(void)
{
    return chimera_posix_tls_has_cred ? &chimera_posix_tls_cred : NULL;
} // chimera_posix_effective_cred

static FORCE_INLINE mode_t
chimera_posix_effective_umask(void)
{
    return chimera_posix_tls_has_umask ? chimera_posix_tls_umask : 0;
} // chimera_posix_effective_umask

static FORCE_INLINE struct chimera_posix_client *
chimera_posix_get_global(void)
{
    return chimera_posix_global;
} // chimera_posix_get_global

/* --------------------------------------------------------------------
 * Local claim-core bridge for byte-range locks (posix_lock_claims.c).
 *
 * The posix client's byte-range locks go through the embedded VFS's claim
 * core FIRST (arbitrating against protocol claims and other posix threads
 * in this process), with the OP_LOCK backend passthrough retained as the
 * kernel projection so cross-PROCESS conflicts keep working (each process
 * has its own core instance; the kernel is the shared arbiter).
 * -------------------------------------------------------------------- */

/* The POSIX lock owner: per-process identity (classic fcntl lock scope).
 * No key and no callbacks -- posix claims are binding (unbreakable). */
static FORCE_INLINE void
chimera_posix_lock_owner_init(struct chimera_claim_owner *owner)
{
    memset(owner, 0, sizeof(*owner));
    owner->proto      = CHIMERA_CLAIM_PROTO_POSIX;
    owner->client_key = (uint64_t) getpid();
    owner->owner_lo   = (uint64_t) getpid();
    owner->owner_hi   = 0;
} // chimera_posix_lock_owner_init

/* Allocate a lock node with an initialized range claim and its own
 * file-state anchor reference.  offset/length are in core geometry
 * (length UINT64_MAX = to-EOF).  Returns NULL on allocation failure. */
struct chimera_posix_ofd_lock *
chimera_posix_ofd_lock_alloc(
    struct chimera_posix_client    *posix,
    struct chimera_vfs_open_handle *handle,
    bool                            exclusive,
    uint64_t                        offset,
    uint64_t                        length);

/* Free an UNTRACKED node whose claim is not (or no longer) inserted. */
void chimera_posix_ofd_lock_free(
    struct chimera_posix_client   *posix,
    struct chimera_posix_ofd_lock *node);

/* Link a granted node onto the description's lock list. */
void chimera_posix_ofd_lock_track(
    struct chimera_posix_client   *posix,
    struct chimera_posix_ofd      *ofd,
    struct chimera_posix_ofd_lock *node);

/* Unlink a tracked node, release its claim from the core, and free it
 * (backend projection denied after a local grant). */
void chimera_posix_ofd_lock_untrack_release(
    struct chimera_posix_client   *posix,
    struct chimera_posix_ofd_lock *node);

/* F_UNLCK: carve the owner's local coverage of [offset, offset+length)
 * out of the claim core (REPLACE geometry, new_mask 0). */
void chimera_posix_ofd_lock_carve(
    struct chimera_posix_client    *posix,
    struct chimera_posix_ofd       *ofd,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint64_t                        length);

/* Acquire the local claim, bridged onto the calling app thread's condvar.
 * `wait` is the F_SETLKW contract (queue on BREAKING and on a hard DENIED
 * conflict); without it the call is a try.  On a CAP_LEASE backend the
 * acquire runs on a worker's VFS thread so a granted range is confirmed
 * with the backend before it is reported. */
enum chimera_vfs_claim_result
chimera_posix_lock_claim_acquire(
    struct chimera_posix_client   *posix,
    struct chimera_posix_ofd_lock *node,
    bool                           wait);

/* Record a backend range token this description holds without a local
 * claim (a SEEK_END grant), so last close can release it. */
void
chimera_posix_ofd_track_token(
    struct chimera_posix_client    *posix,
    struct chimera_posix_ofd       *ofd,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        token);

/* F_UNLCK: carve the local coverage and wait for any backend releases it
* produced to complete, so the range really is free when this returns. */
void
chimera_posix_lock_claim_unlock(
    struct chimera_posix_client    *posix,
    struct chimera_posix_ofd       *ofd,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint64_t                        length);

/* SEEK_END unlock: release the backend record by geometry, since this node
 * never learned the absolute range. */
int
chimera_posix_lock_claim_unlock_ranged(
    struct chimera_posix_client    *posix,
    struct chimera_vfs_open_handle *handle,
    int32_t                         whence,
    uint64_t                        offset,
    uint64_t                        length);

/* F_GETLK's backend half: ask a range-arbitrating backend whether anything
 * outside this process holds the probed range.  Returns false when nothing
 * arbitrates ranges or the range is free; true fills *conflict. */
bool
chimera_posix_lock_claim_test(
    struct chimera_posix_client       *posix,
    struct chimera_vfs_open_handle    *handle,
    const struct chimera_vfs_claim    *probe,
    struct chimera_vfs_claim_conflict *conflict);

/* SEEK_END ranges: the offset is resolved by the backend, atomically with
 * the operation, so the local core cannot arbitrate them at all and the
 * whole fcntl is answered by the backend.  Returns the fcntl return value
 * and sets errno on failure. */
int
chimera_posix_lock_claim_seek_end(
    struct chimera_posix_client    *posix,
    struct chimera_posix_ofd       *ofd,
    struct chimera_vfs_open_handle *handle,
    int                             cmd,
    struct flock                   *fl,
    uint32_t                        lock_type,
    int32_t                         whence,
    uint64_t                        offset,
    uint64_t                        length);

/* Release every lock claim the description still holds.  Caller holds
 * fd_lock. */
void chimera_posix_ofd_locks_release(
    struct chimera_posix_client *posix,
    struct chimera_posix_ofd    *ofd);

/* Project a whole-file backend unlock before a description's last close on a
 * CAP_FS_LOCK backend (see posix_lock_claims.c). */
void chimera_posix_project_ofd_unlock(
    struct chimera_posix_client    *posix,
    struct chimera_posix_worker    *worker,
    struct chimera_vfs_open_handle *handle,
    struct chimera_posix_ofd       *ofd);

/* Drop an fd entry's reference on its open file description, freeing the
 * description when the last duplicate goes.  Caller holds fd_lock. */
static FORCE_INLINE void
chimera_posix_ofd_release_locked(struct chimera_posix_fd_entry *entry)
{
    if (entry->ofd && --entry->ofd->refcnt == 0) {
        /* Last posix close of the description: release its lock claims from
         * the local core (fixes the shipped leak of claims surviving close).
         * CLAIMTODO: POSIX drops a process's locks on ANY close of any fd
         * referring to the file; we release on the LAST close of the OFD
         * (OFD-lock/BSD-style), a deliberate divergence. */
        chimera_posix_ofd_locks_release(chimera_posix_get_global(), entry->ofd);
        free(entry->ofd);
    }
    entry->ofd = NULL;
} // chimera_posix_ofd_release_locked

/* Point `entry` at `src`'s open file description (dup/dup2/F_DUPFD: the
 * duplicate SHARES the description), releasing whatever description the
 * entry held. */
static FORCE_INLINE void
chimera_posix_ofd_adopt(
    struct chimera_posix_client   *posix,
    struct chimera_posix_fd_entry *entry,
    struct chimera_posix_fd_entry *src)
{
    pthread_mutex_lock(&posix->fd_lock);
    chimera_posix_ofd_release_locked(entry);
    entry->ofd = src->ofd;
    entry->ofd->refcnt++;
    pthread_mutex_unlock(&posix->fd_lock);
} // chimera_posix_ofd_adopt

/* chimera_vfs_error values are a protocol enum whose numbers happen to follow
 * the Linux errno layout; the host's errno numbering differs on other
 * platforms (Darwin: ENAMETOOLONG is 63, 36 is EINPROGRESS), so map through
 * the host macros rather than casting the raw value. */
static FORCE_INLINE int
chimera_posix_errno_from_status(enum chimera_vfs_error status)
{
    switch (status) {
        case CHIMERA_VFS_OK:           return 0;
        case CHIMERA_VFS_EPERM:        return EPERM;
        case CHIMERA_VFS_ENOENT:       return ENOENT;
        case CHIMERA_VFS_EIO:          return EIO;
        case CHIMERA_VFS_ENXIO:        return ENXIO;
        case CHIMERA_VFS_EAGAIN:       return EAGAIN;
        case CHIMERA_VFS_EACCES:       return EACCES;
        case CHIMERA_VFS_EFAULT:       return EFAULT;
        case CHIMERA_VFS_EBUSY:        return EBUSY;
        case CHIMERA_VFS_EEXIST:       return EEXIST;
        case CHIMERA_VFS_EXDEV:        return EXDEV;
        case CHIMERA_VFS_ENOTDIR:      return ENOTDIR;
        case CHIMERA_VFS_EISDIR:       return EISDIR;
        case CHIMERA_VFS_EINVAL:       return EINVAL;
        case CHIMERA_VFS_EMFILE:       return EMFILE;
        case CHIMERA_VFS_EFBIG:        return EFBIG;
        case CHIMERA_VFS_ENOSPC:       return ENOSPC;
        case CHIMERA_VFS_EROFS:        return EROFS;
        case CHIMERA_VFS_EMLINK:       return EMLINK;
        case CHIMERA_VFS_ENAMETOOLONG: return ENAMETOOLONG;
        case CHIMERA_VFS_ENOTEMPTY:    return ENOTEMPTY;
        case CHIMERA_VFS_ELOOP:        return ELOOP;
        case CHIMERA_VFS_EOVERFLOW:    return EOVERFLOW;
        case CHIMERA_VFS_EBADF:        return EBADF;
        case CHIMERA_VFS_ENOTSUP:      return ENOTSUP;
        case CHIMERA_VFS_EDQUOT:       return EDQUOT;
        case CHIMERA_VFS_ESTALE:       return ESTALE;
        case CHIMERA_VFS_ENODATA:      return ENODATA;
        case CHIMERA_VFS_ERANGE:       return ERANGE;
        /* Internal statuses with no host errno equivalent. */
        case CHIMERA_VFS_ESYMLINK:     return ELOOP;
        case CHIMERA_VFS_EBADCOOKIE:   return EINVAL;
        default:                       return EIO;
    } // switch
} // chimera_posix_errno_from_status

static FORCE_INLINE void
chimera_posix_complete(
    struct chimera_posix_completion *comp,
    enum chimera_vfs_error           status)
{
    pthread_mutex_lock(&comp->mutex);
    comp->status = status;
    comp->done   = 1;
    pthread_cond_signal(&comp->cond);
    pthread_mutex_unlock(&comp->mutex);
} // chimera_posix_complete

static FORCE_INLINE void
chimera_posix_completion_init(
    struct chimera_posix_completion *comp,
    struct chimera_client_request   *req)
{
    pthread_mutex_init(&comp->mutex, NULL);
    pthread_cond_init(&comp->cond, NULL);
    comp->request = req;
    comp->status  = CHIMERA_VFS_OK;
    comp->done    = 0;

    req->heap_allocated = 0;

    /* Capture the calling thread's effective credential override (if any) into
     * the request, so the worker thread authorizes this operation as the
     * intended user rather than the client-global credential. */
    {
        const struct chimera_vfs_cred *eff = chimera_posix_effective_cred();

        if (eff) {
            req->req_cred = *eff;
            req->has_cred = 1;
        } else {
            req->has_cred = 0;
        }
    }
} // chimera_posix_completion_init

static FORCE_INLINE void
chimera_posix_completion_destroy(struct chimera_posix_completion *comp)
{
    pthread_mutex_destroy(&comp->mutex);
    pthread_cond_destroy(&comp->cond);
} // chimera_posix_completion_destroy

static FORCE_INLINE void
chimera_posix_worker_enqueue(
    struct chimera_posix_worker    *worker,
    struct chimera_client_request  *request,
    chimera_client_request_callback callback)
{
    request->sync_callback = callback;

    pthread_mutex_lock(&worker->lock);
    DL_APPEND(worker->pending_requests, request);
    pthread_mutex_unlock(&worker->lock);

    evpl_ring_doorbell(&worker->doorbell);
} // chimera_posix_worker_enqueue

static FORCE_INLINE struct chimera_posix_worker *
chimera_posix_choose_worker(struct chimera_posix_client *posix)
{
    unsigned int idx = atomic_fetch_add(&posix->next_worker, 1);

    return &posix->workers[idx % (unsigned int) posix->nworkers];
} // chimera_posix_choose_worker

static FORCE_INLINE int
chimera_posix_wait(
    struct chimera_posix_completion *comp);

static FORCE_INLINE void
chimera_posix_close_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_posix_completion *comp = request->close_sync.private_data;

    chimera_close(thread, request->sync_open_handle);
    chimera_posix_complete(comp, CHIMERA_VFS_OK);
} // chimera_posix_close_exec

/*
 * Close (release) an open handle ON its worker's thread.  chimera_close
 * releases the handle into the open cache, and a release that must close
 * immediately -- a detached duplicate, delete-on-close, a cache-full
 * eviction -- dispatches the module close op inline on the calling thread.
 * For the NFS proxy that transmits the CLOSE RPC on the request thread's
 * connection, which is only safe on the OS thread that owns it; a caller
 * thread borrowing worker->client_thread trips libevpl's cross-thread iovec
 * assertions (and in release builds races the refcounts).  So the close is
 * enqueued to the worker and waited, like every other operation.
 */
static FORCE_INLINE void
chimera_posix_close_on_worker(
    struct chimera_posix_worker    *worker,
    struct chimera_vfs_open_handle *handle)
{
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;

    chimera_posix_completion_init(&comp, &req);

    req.sync_open_handle        = handle;
    req.close_sync.private_data = &comp;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_close_exec);

    (void) chimera_posix_wait(&comp);
    chimera_posix_completion_destroy(&comp);
} // chimera_posix_close_on_worker

static FORCE_INLINE int
chimera_posix_wait(struct chimera_posix_completion *comp)
{
    pthread_mutex_lock(&comp->mutex);
    while (!comp->done) {
        pthread_cond_wait(&comp->cond, &comp->mutex);
    }
    pthread_mutex_unlock(&comp->mutex);

    return chimera_posix_errno_from_status(comp->status);
} // chimera_posix_wait

static FORCE_INLINE void
chimera_posix_fill_stat(
    struct stat               *dst,
    const struct chimera_stat *src)
{
    dst->st_dev   = src->st_dev;
    dst->st_ino   = src->st_ino;
    dst->st_mode  = src->st_mode;
    dst->st_nlink = src->st_nlink;
    dst->st_uid   = src->st_uid;
    dst->st_gid   = src->st_gid;
    dst->st_rdev  = src->st_rdev;
    dst->st_size  = src->st_size;
    /* dst is the host struct stat, whose nanosecond timestamps are not spelled
     * the same on every platform; src is chimera's own. */
    CHIMERA_STAT_ATIM(*dst) = src->st_atim;
    CHIMERA_STAT_MTIM(*dst) = src->st_mtim;
    CHIMERA_STAT_CTIM(*dst) = src->st_ctim;
    dst->st_blksize         = 4096;
    dst->st_blocks          = (src->st_size + 511) / 512;
} // chimera_posix_fill_stat

static FORCE_INLINE void
chimera_posix_iovec_memcpy(
    struct evpl_iovec *iov,
    const void        *buf,
    size_t             len)
{
    size_t      copied = 0;
    const char *p      = buf;

    for (int i = 0; copied < len; i++) {
        size_t chunk = iov[i].length;

        if (chunk > len - copied) {
            chunk = len - copied;
        }

        memcpy(iov[i].data, p + copied, chunk);
        copied += chunk;
    }
} // chimera_posix_iovec_memcpy

static FORCE_INLINE unsigned int
chimera_posix_to_chimera_flags(int flags)
{
    unsigned int out = 0;

    if (flags & O_CREAT) {
        out |= CHIMERA_VFS_OPEN_CREATE;
    }

    if (flags & O_EXCL) {
        out |= CHIMERA_VFS_OPEN_EXCLUSIVE;
    }

    if (flags & O_DIRECTORY) {
        out |= CHIMERA_VFS_OPEN_DIRECTORY;
    }

    if (flags & O_TRUNC) {
        out |= CHIMERA_VFS_OPEN_TRUNCATE;
    }

#ifdef O_NOFOLLOW
    if (flags & O_NOFOLLOW) {
        out |= CHIMERA_VFS_OPEN_NOFOLLOW;
    }
#endif /* ifdef O_NOFOLLOW */

    if ((flags & O_ACCMODE) == O_RDONLY) {
        out |= CHIMERA_VFS_OPEN_READ_ONLY;
    } else if ((flags & O_ACCMODE) == O_WRONLY) {
        out |= CHIMERA_VFS_OPEN_WRITE_ONLY;
    } else { /* O_RDWR: request both read and write access */
        out |= CHIMERA_VFS_OPEN_READ_ONLY | CHIMERA_VFS_OPEN_WRITE_ONLY;
    }

    return out;
} // chimera_posix_to_chimera_flags

/* Initialize a create-path set_attr to request the given creation mode, with
 * the calling thread's umask applied to the permission bits.  umask is masked
 * to 0777, so file-type and special bits in `mode` (e.g. for mknod) survive. */
static FORCE_INLINE void
chimera_posix_set_create_mode(
    struct chimera_vfs_attrs *sa,
    mode_t                    mode)
{
    sa->va_req_mask = 0;
    sa->va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sa->va_mode     = mode & ~chimera_posix_effective_umask();
} // chimera_posix_set_create_mode

/* Initialize a create-path set_attr that carries no explicit mode (backend
 * default applies). */
static FORCE_INLINE void
chimera_posix_no_create_mode(struct chimera_vfs_attrs *sa)
{
    sa->va_req_mask = 0;
    sa->va_set_mask = 0;
} // chimera_posix_no_create_mode

/* Validate a caller-supplied pathname length before it is copied into a fixed
 * CHIMERA_VFS_PATH_MAX request buffer.  Returns the length on success; on a path
 * that would not fit (>= CHIMERA_VFS_PATH_MAX including the terminating null)
 * sets errno=ENAMETOOLONG and returns -1.  This both yields the POSIX errno and
 * prevents a buffer overflow of the request path field. */
static FORCE_INLINE int
chimera_posix_check_path(const char *path)
{
    size_t len = strlen(path);

    if (len >= CHIMERA_VFS_PATH_MAX) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return (int) len;
} /* chimera_posix_check_path */

/* Allocate the lowest free descriptor slot with index >= minfd (POSIX
 * open()/dup() require the lowest available descriptor; F_DUPFD the lowest
 * >= its argument).  The free list is unordered, so scan it for the best
 * index; it holds at most max_fds entries and allocation is not a hot
 * path in this compatibility layer. */
static FORCE_INLINE int
chimera_posix_fd_alloc_at_least(
    struct chimera_posix_client    *posix,
    struct chimera_vfs_open_handle *handle,
    int                             minfd)
{
    struct chimera_posix_fd_entry  *entry;
    struct chimera_posix_fd_entry **pp, **best_pp = NULL;
    int                             fd, best = posix->max_fds;

    pthread_mutex_lock(&posix->fd_lock);

    for (pp = &posix->free_list; *pp; pp = &(*pp)->next) {
        int idx = (int) (*pp - posix->fds);

        if (idx >= minfd && idx < best) {
            best    = idx;
            best_pp = pp;
        }
    }

    if (!best_pp) {
        pthread_mutex_unlock(&posix->fd_lock);
        return -1;
    }

    entry       = *best_pp;
    *best_pp    = entry->next;
    entry->next = NULL;

    pthread_mutex_unlock(&posix->fd_lock);

    fd = (int) (entry - posix->fds);

    /* A fresh open gets a fresh open file description (not shared with
     * anyone yet, so no lock needed for the refcount). */
    entry->ofd = calloc(1, sizeof(*entry->ofd));

    if (!entry->ofd) {
        pthread_mutex_lock(&posix->fd_lock);
        entry->next      = posix->free_list;
        posix->free_list = entry;
        pthread_mutex_unlock(&posix->fd_lock);
        return -1;
    }

    entry->ofd->refcnt = 1;

    entry->handle        = handle;
    entry->flags         = 0;
    entry->refcnt        = 0;
    entry->io_waiters    = 0;
    entry->pending_close = 0;
    entry->close_waiters = 0;
    entry->eof_flag      = 0;
    entry->error_flag    = 0;
    entry->ungetc_char   = -1;

    return fd;
} // chimera_posix_fd_alloc_at_least

static FORCE_INLINE int
chimera_posix_fd_alloc(
    struct chimera_posix_client    *posix,
    struct chimera_vfs_open_handle *handle)
{
    return chimera_posix_fd_alloc_at_least(posix, handle, 0);
} // chimera_posix_fd_alloc

static FORCE_INLINE void
chimera_posix_fd_free(
    struct chimera_posix_client *posix,
    int                          fd)
{
    struct chimera_posix_fd_entry *entry;

    if (fd < 0 || fd >= posix->max_fds) {
        return;
    }

    entry = &posix->fds[fd];

    entry->handle      = NULL;
    entry->eof_flag    = 0;
    entry->error_flag  = 0;
    entry->ungetc_char = -1;

    pthread_mutex_lock(&posix->fd_lock);
    chimera_posix_ofd_release_locked(entry);
    entry->next      = posix->free_list;
    posix->free_list = entry;
    pthread_mutex_unlock(&posix->fd_lock);
} // chimera_posix_fd_free

static FORCE_INLINE struct chimera_posix_fd_entry *
chimera_posix_fd_acquire(
    struct chimera_posix_client *posix,
    int                          fd,
    unsigned int                 flags_to_set)
{
    struct chimera_posix_fd_entry *entry;

    if (fd < 0 || fd >= posix->max_fds) {
        errno = EBADF;
        return NULL;
    }

    entry = &posix->fds[fd];

    pthread_mutex_lock(&entry->lock);

    // If CLOSED, return error
    if (entry->flags & CHIMERA_POSIX_FD_CLOSED) {
        pthread_mutex_unlock(&entry->lock);
        errno = EBADF;
        return NULL;
    }

    // If caller wants IO_ACTIVE flag (read/write operations)
    if (flags_to_set & CHIMERA_POSIX_FD_IO_ACTIVE) {
        // Wait for existing IO to complete
        while (entry->flags & CHIMERA_POSIX_FD_IO_ACTIVE) {
            entry->io_waiters++;
            pthread_cond_wait(&entry->cond, &entry->lock);
            entry->io_waiters--;
        }

        // Check if fd was closed or is closing
        if (entry->flags & (CHIMERA_POSIX_FD_CLOSED | CHIMERA_POSIX_FD_CLOSING)) {
            pthread_mutex_unlock(&entry->lock);
            errno = EBADF;
            return NULL;
        }

        entry->flags |= CHIMERA_POSIX_FD_IO_ACTIVE;
    }

    // If caller wants CLOSING flag (close operation)
    if (flags_to_set & CHIMERA_POSIX_FD_CLOSING) {
        // If CLOSING is already set by another thread, wait for it to complete
        if (entry->flags & CHIMERA_POSIX_FD_CLOSING) {
            entry->close_waiters++;
            while (!(entry->flags & CHIMERA_POSIX_FD_CLOSED)) {
                pthread_cond_wait(&entry->cond, &entry->lock);
            }
            entry->close_waiters--;
            pthread_mutex_unlock(&entry->lock);
            errno = EBADF;
            return NULL;
        }

        // Set CLOSING and pending_close
        entry->flags        |= CHIMERA_POSIX_FD_CLOSING;
        entry->pending_close = 1;

        // Wait for existing operations to complete
        while (entry->refcnt > 0) {
            pthread_cond_wait(&entry->cond, &entry->lock);
        }
    }

    entry->refcnt++;
    pthread_mutex_unlock(&entry->lock);

    return entry;
} // chimera_posix_fd_acquire

static FORCE_INLINE void
chimera_posix_fd_release(
    struct chimera_posix_fd_entry *entry,
    unsigned int                   flags_to_clear)
{
    pthread_mutex_lock(&entry->lock);

    // If completing an IO operation
    if (flags_to_clear & CHIMERA_POSIX_FD_IO_ACTIVE) {
        entry->flags &= ~CHIMERA_POSIX_FD_IO_ACTIVE;
        if (entry->io_waiters > 0) {
            pthread_cond_signal(&entry->cond);
        }
    }

    // If completing a close operation
    if (flags_to_clear & CHIMERA_POSIX_FD_CLOSING) {
        entry->flags        &= ~CHIMERA_POSIX_FD_CLOSING;
        entry->flags        |= CHIMERA_POSIX_FD_CLOSED;
        entry->pending_close = 0;
        if (entry->close_waiters > 0) {
            pthread_cond_broadcast(&entry->cond);
        }
    }

    entry->refcnt--;

    // Signal if refcnt is zero and a close is pending
    if (entry->refcnt == 0 && entry->pending_close) {
        pthread_cond_signal(&entry->cond);
    }

    pthread_mutex_unlock(&entry->lock);
} // chimera_posix_fd_release

/* POSIX ties I/O rights to the descriptor's access mode, checked at open
 * and carried in the shared description's oflags: read-family calls need a
 * descriptor open for reading and write-family calls one open for writing,
 * otherwise EBADF (read()/write() ERRORS). */
static FORCE_INLINE int
chimera_posix_fd_may_read(const struct chimera_posix_fd_entry *entry)
{
    return (entry->ofd->oflags & O_ACCMODE) != O_WRONLY;
} /* chimera_posix_fd_may_read */

static FORCE_INLINE int
chimera_posix_fd_may_write(const struct chimera_posix_fd_entry *entry)
{
    return (entry->ofd->oflags & O_ACCMODE) != O_RDONLY;
} /* chimera_posix_fd_may_write */


static FORCE_INLINE off_t
chimera_posix_fd_lseek(
    struct chimera_posix_client *posix,
    int                          fd,
    off_t                        offset,
    int                          whence,
    off_t                        file_size)
{
    struct chimera_posix_fd_entry *entry;
    off_t                          new_offset;

    if (fd < 0 || fd >= posix->max_fds) {
        errno = EBADF;
        return -1;
    }

    entry = &posix->fds[fd];

    pthread_mutex_lock(&entry->lock);

    // If CLOSED, return error
    if (entry->flags & CHIMERA_POSIX_FD_CLOSED) {
        pthread_mutex_unlock(&entry->lock);
        errno = EBADF;
        return -1;
    }

    // Wait for any IO to complete
    while (entry->flags & CHIMERA_POSIX_FD_IO_ACTIVE) {
        entry->io_waiters++;
        pthread_cond_wait(&entry->cond, &entry->lock);
        entry->io_waiters--;
    }

    // Check again if fd was closed while waiting
    if (entry->flags & (CHIMERA_POSIX_FD_CLOSED | CHIMERA_POSIX_FD_CLOSING)) {
        pthread_mutex_unlock(&entry->lock);
        errno = EBADF;
        return -1;
    }

    // Calculate new offset based on whence
    switch (whence) {
        case SEEK_SET:
            new_offset = offset;
            break;
        case SEEK_CUR:
            new_offset = (off_t) entry->ofd->offset + offset;
            break;
        case SEEK_END:
            new_offset = file_size + offset;
            break;
        default:
            pthread_mutex_unlock(&entry->lock);
            errno = EINVAL;
            return -1;
    } // switch

    // Validate new offset
    if (new_offset < 0) {
        pthread_mutex_unlock(&entry->lock);
        errno = EINVAL;
        return -1;
    }

    entry->ofd->offset = (uint64_t) new_offset;

    pthread_mutex_unlock(&entry->lock);

    return new_offset;
} // chimera_posix_fd_lseek

/* Resolve the current EOF of the file behind an fd entry, for O_APPEND
 * write placement.  Returns 0 and fills *eof, or an errno value.
 * Defined in posix_write.c. */
int chimera_posix_fd_eof(
    struct chimera_posix_worker   *worker,
    struct chimera_posix_fd_entry *entry,
    uint64_t                      *eof);

void * chimera_posix_worker_init(
    struct evpl *evpl,
    void        *private_data);
void chimera_posix_worker_shutdown(
    struct evpl *evpl,
    void        *private_data);
void chimera_posix_worker_doorbell(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell);

// FILE* helper functions
static FORCE_INLINE int
chimera_posix_file_to_fd(
    struct chimera_posix_client *posix,
    CHIMERA_FILE                *file)
{
    if (!file) {
        return -1;
    }

    return (int) (file - posix->fds);
} // chimera_posix_file_to_fd

static FORCE_INLINE CHIMERA_FILE *
chimera_posix_fd_to_file(
    struct chimera_posix_client *posix,
    int                          fd)
{
    if (fd < 0 || fd >= posix->max_fds) {
        return NULL;
    }

    return &posix->fds[fd];
} // chimera_posix_fd_to_file

#endif /* CHIMERA_POSIX_INTERNAL_H */
