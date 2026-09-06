// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/uio.h>

#include "fuse_internal.h"
#include "fuse_attr.h"
#include "common/macros.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

/*
 * Channel read loop, the per-request compound driver, and reply writers.
 *
 * Each /dev/fuse read returns exactly one complete request, and each reply
 * is one atomic writev to the channel the request was read from -- the
 * kernel tracks a request on the per-device queue of the fd that read it,
 * so a reply on any other channel would not find it.  Requests are
 * dispatched on the thread that owns the channel and every VFS completion
 * fires on the issuing thread, so a request never changes threads.
 *
 * One FUSE request == one VFS compound (RETRYABLE).  The compound begins
 * lazily at the request's first VFS call and is ended by the reply helpers
 * BEFORE anything is written to the kernel: the helpers park the reply on
 * the request, commit (durably for FSYNC/FSYNCDIR), and only the end
 * callback delivers the parked reply.  A wait-die/optimistic-commit
 * conflict -- surfaced by a member op as the CHIMERA_FUSE_ECONFLICT errno
 * sentinel or by the commit itself -- aborts the compound and replays the
 * whole request from the top, reusing the stable compound_ts so it cannot
 * starve.  On a backend without compound support the compound never binds,
 * every op autocommits standalone, and the end is a synchronous OK -- the
 * deliver runs inline, exactly the old behaviour.
 */

static struct chimera_fuse_request *
chimera_fuse_request_alloc(
    struct chimera_fuse_thread  *thread,
    struct chimera_fuse_channel *channel)
{
    struct chimera_fuse_request *req;
    int                          niov;

    if (thread->free_requests) {
        req                   = thread->free_requests;
        thread->free_requests = req->next;
        thread->num_free_requests--;
    } else {
        req         = calloc(1, sizeof(*req));
        req->thread = thread;
    }

    if (!req->buf_allocated) {
        niov = evpl_iovec_alloc(thread->evpl, CHIMERA_FUSE_BUFSZ, 4096, 1, 0,
                                &req->buf);
        chimera_fuse_abort_if(niov != 1,
                              "fuse request buffer allocation failed (%d)", niov);
        req->buf_allocated = 1;
    }

    req->channel = channel;
    req->handle  = NULL;
    req->file    = NULL;

    thread->active_requests++;

    return req;
} /* chimera_fuse_request_alloc */

void
chimera_fuse_request_free(
    struct chimera_fuse_thread  *thread,
    struct chimera_fuse_request *req)
{
    /* Every begun compound must reach exactly one end before the request is
     * recycled (the reply helpers and the replay driver own that). */
    chimera_fuse_abort_if(req->compound,
                          "fuse request freed with live compound (opcode %u)",
                          req->opcode);

    thread->active_requests--;

    if (thread->num_free_requests >= CHIMERA_FUSE_MAX_POOLED_REQS) {
        if (req->buf_allocated) {
            evpl_iovec_release(thread->evpl, &req->buf);
        }
        free(req);
        return;
    }

    req->next             = thread->free_requests;
    thread->free_requests = req;
    thread->num_free_requests++;
} /* chimera_fuse_request_free */

/*
 * Deliver a reply.  Returns 0 once the kernel has taken it, -1 when it never
 * will: the request was aborted (ENOENT), the channel is dead (ENODEV), or
 * the write failed outright.  Callers whose reply hands the kernel a
 * reference (an entry's lookup count, an open's fh) undo it on -1.
 */
static int
chimera_fuse_send(
    struct chimera_fuse_request *req,
    int                          error,
    const void                  *payload,
    size_t                       payload_len,
    struct evpl_iovec           *data_iov,
    int                          data_niov,
    size_t                       data_len)
{
    struct fuse_out_header hdr;
    struct iovec           iov[2 + CHIMERA_FUSE_IOV_MAX];
    int                    niov = 0, i;
    size_t                 total;
    ssize_t                rc;

    if (req->channel->dead) {
        return -1;
    }

    hdr.error  = -error;
    hdr.unique = req->unique;

    iov[niov].iov_base = &hdr;
    iov[niov].iov_len  = sizeof(hdr);
    niov++;

    total = sizeof(hdr);

    if (error == 0 && payload_len) {
        iov[niov].iov_base = (void *) payload;
        iov[niov].iov_len  = payload_len;
        niov++;
        total += payload_len;
    }

    if (error == 0 && data_len) {
        size_t remain = data_len;

        for (i = 0; i < data_niov && remain; i++) {
            size_t chunk = evpl_iovec_length(&data_iov[i]);

            if (chunk > remain) {
                chunk = remain;
            }

            iov[niov].iov_base = evpl_iovec_data(&data_iov[i]);
            iov[niov].iov_len  = chunk;
            niov++;
            remain -= chunk;
        }

        total += data_len;
    }

    hdr.len = total;

    do {
        rc = writev(req->channel->fd, iov, niov);
    } while (rc < 0 && errno == EINTR);

    if (rc < 0) {
        switch (errno) {
            case ENOENT:
                /* The request was interrupted/aborted before we replied. */
                break;
            case ENODEV:
                chimera_fuse_channel_dead(req->channel);
                break;
            default:
                chimera_fuse_error("fuse reply write failed (opcode %u unique %llu): %s",
                                   req->opcode,
                                   (unsigned long long) req->unique,
                                   strerror(errno));
                break;
        } /* switch */
        return -1;
    }

    return 0;
} /* chimera_fuse_send */

void
chimera_fuse_request_finish(struct chimera_fuse_request *req)
{
    struct chimera_fuse_thread *thread = req->thread;

    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
        req->handle = NULL;
    }

    chimera_fuse_request_free(thread, req);
} /* chimera_fuse_request_finish */

/* --- the per-request compound driver --- */

static void chimera_fuse_reply_deliver(
    struct chimera_fuse_request *req);
static void chimera_fuse_request_replay(
    struct chimera_fuse_request *req);

/* WRITE for the mutating opcodes, READ otherwise.  FLUSH/FSYNC/FSYNCDIR
 * count as mutating: their commit publishes dirty data.  OPEN does not --
 * O_TRUNC arrives as a separate SETATTR (no FUSE_ATOMIC_O_TRUNC). */
static enum chimera_vfs_compound_mode
chimera_fuse_opcode_mode(uint32_t opcode)
{
    switch (opcode) {
        case FUSE_SETATTR:
        case FUSE_MKNOD:
        case FUSE_MKDIR:
        case FUSE_UNLINK:
        case FUSE_RMDIR:
        case FUSE_SYMLINK:
        case FUSE_RENAME:
        case FUSE_RENAME2:
        case FUSE_LINK:
        case FUSE_CREATE:
        case FUSE_WRITE:
        case FUSE_FLUSH:
        case FUSE_FSYNC:
        case FUSE_FSYNCDIR:
        case FUSE_FALLOCATE:
        case FUSE_COPY_FILE_RANGE:
        case FUSE_SETXATTR:
        case FUSE_REMOVEXATTR:
            return CHIMERA_VFS_COMPOUND_WRITE;
        default:
            return CHIMERA_VFS_COMPOUND_READ;
    } /* switch */
} /* chimera_fuse_opcode_mode */

struct chimera_vfs_compound *
chimera_fuse_req_compound(struct chimera_fuse_request *req)
{
    if (!req->compound) {
        /* Begin never returns NULL.  The hint is the request's resolved
         * node when one is in hand (req->fh, set by resolve_nodeid or the
         * FH-bearing getattr path); a handle-based op begins unbound and
         * lazy-binds at its first enlisted op.  RETRYABLE: the reply path
         * replays the whole request on ECOMPOUND_CONFLICT, reusing the
         * stable ts assigned at dispatch. */
        req->compound = chimera_vfs_compound_begin(
            req->thread->vfs_thread, &req->cred,
            req->fh_len ? req->fh : NULL, req->fh_len,
            chimera_fuse_opcode_mode(req->opcode),
            req->compound_ts,
            CHIMERA_VFS_COMPOUND_RETRYABLE);
    }

    return req->compound;
} /* chimera_fuse_req_compound */

/* Undo an open_file the request itself created (OPEN/OPENDIR/CREATE) whose
 * fh the kernel will never learn: error reply, undeliverable reply, or a
 * conflict replay. */
static void
chimera_fuse_file_undo(struct chimera_fuse_request *req)
{
    struct chimera_fuse_open_file *file = req->file;

    if (!req->file_owned || !file) {
        return;
    }

    req->file       = NULL;
    req->file_owned = 0;

    chimera_fuse_file_unlink(file->mount, file);
    chimera_vfs_release(req->thread->vfs_thread, file->handle);
    free(file);
} /* chimera_fuse_file_undo */

/* Walk a packed READDIRPLUS reply undoing the lookup-count bumps of entries
 * the kernel never received (undelivered reply, or a conflict replay). */
static void
chimera_fuse_readdirplus_unwind(struct chimera_fuse_request *req)
{
    struct chimera_fuse_mount *mount = req->channel->mount;
    uint8_t                   *base  = chimera_fuse_reply_space(req);
    struct fuse_direntplus    *plus;
    uint32_t                   off = 0;

    while (off < req->u.readdir.used) {
        plus = (struct fuse_direntplus *) (base + off);

        if (plus->entry_out.nodeid &&
            chimera_fuse_node_forget(mount->node_table,
                                     plus->entry_out.nodeid, 1)) {
            /* The undo retired the node: drop its coverage too. */
            chimera_fuse_watch_forget(mount, req->thread->vfs_thread->vfs,
                                      plus->entry_out.nodeid);
            chimera_fuse_grant_forget(mount,
                                      req->thread->vfs_thread->vfs->vfs_state,
                                      plus->entry_out.nodeid);
        }

        off += FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET_DIRENTPLUS +
                                 plus->dirent.namelen);
    }

    req->u.readdir.used = 0;
} /* chimera_fuse_readdirplus_unwind */

static void
chimera_fuse_compound_committed(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_fuse_request *req = private_data;

    if (error_code == CHIMERA_VFS_ECOMPOUND_CONFLICT) {
        chimera_fuse_request_replay(req);
        return;
    }

    if (error_code != CHIMERA_VFS_OK && req->pending_error == 0) {
        /* The op chain succeeded but the commit did not: the reply must not
         * promise effects the backend discarded. */
        req->pending_error = chimera_fuse_errno(error_code);
    }

    chimera_fuse_reply_deliver(req);
} /* chimera_fuse_compound_committed */

/* End the request's compound (the parked reply is already staged); the
 * commit callback delivers it -- or replays on conflict. */
static void
chimera_fuse_compound_commit(struct chimera_fuse_request *req)
{
    struct chimera_vfs_compound *compound = req->compound;

    req->compound = NULL;

    chimera_vfs_compound_end(req->thread->vfs_thread, &req->cred, compound,
                             (req->opcode == FUSE_FSYNC ||
                              req->opcode == FUSE_FSYNCDIR) ?
                             CHIMERA_VFS_COMPOUND_COMMIT_DURABLE :
                             CHIMERA_VFS_COMPOUND_COMMIT,
                             chimera_fuse_compound_committed, req);
} /* chimera_fuse_compound_commit */

static void
chimera_fuse_compound_aborted(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    (void) error_code;   /* the abort's own result is immaterial: replaying */

    chimera_fuse_request_replay(private_data);
} /* chimera_fuse_compound_aborted */

/* A member op failed with ECOMPOUND_CONFLICT (the CHIMERA_FUSE_ECONFLICT
 * errno sentinel): abort the compound and replay the request. */
static void
chimera_fuse_compound_conflict(struct chimera_fuse_request *req)
{
    struct chimera_vfs_compound *compound = req->compound;

    req->pending_kind = CHIMERA_FUSE_REPLY_NONE;
    req->compound     = NULL;

    chimera_vfs_compound_end(req->thread->vfs_thread, &req->cred, compound,
                             CHIMERA_VFS_COMPOUND_ABORT,
                             chimera_fuse_compound_aborted, req);
} /* chimera_fuse_compound_conflict */

/* Re-run the whole request from the top after a compound conflict: drop the
 * failed attempt's residue (transient handle, owned open_file, parked read
 * buffers, READDIRPLUS lookup counts), then re-invoke the handler against
 * the intact request buffer.  The compound is re-begun lazily with the same
 * wait-die ts, so the request cannot starve; the replay budget converts a
 * livelock into a retriable EAGAIN. */
static void
chimera_fuse_request_replay(struct chimera_fuse_request *req)
{
    struct chimera_fuse_thread  *thread = req->thread;
    const struct fuse_in_header *hdr;

    if (req->pending_kind == CHIMERA_FUSE_REPLY_READ) {
        evpl_iovecs_release(thread->evpl, req->pending_iov, req->pending_niov);
    }

    if (req->opcode == FUSE_READDIRPLUS) {
        chimera_fuse_readdirplus_unwind(req);
    }

    chimera_fuse_file_undo(req);

    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
        req->handle = NULL;
    }

    req->pending_kind = CHIMERA_FUSE_REPLY_NONE;
    req->entry_cover  = CHIMERA_FUSE_COVER_NONE;

    if (++req->compound_attempt > CHIMERA_FUSE_COMPOUND_MAX_RETRIES) {
        /* The compound is already retired; deliver the retriable failure
         * directly. */
        req->pending_kind        = CHIMERA_FUSE_REPLY_SIMPLE;
        req->pending_error       = EAGAIN;
        req->pending_payload     = NULL;
        req->pending_payload_len = 0;
        chimera_fuse_reply_deliver(req);
        return;
    }

    hdr = chimera_fuse_request_hdr(req);

    chimera_fuse_handlers[req->opcode](req, hdr, hdr + 1,
                                       req->buf_len - sizeof(*hdr));
} /* chimera_fuse_request_replay */

/* --- deliver: the parked reply reaches the kernel (post-commit) --- */

/* Entry-shaped deliver (LOOKUP, CREATE, MKDIR, MKNOD, SYMLINK, LINK):
 * registers the child in the nodeid table, arms its coverage, applies the
 * TTL policy, sends, and undoes the lookup-count bump if the kernel never
 * saw the entry.  Runs only after the compound committed, so a replay can
 * never double-register the child. */
static void
chimera_fuse_entry_deliver(struct chimera_fuse_request *req)
{
    struct chimera_fuse_mount      *mount = req->channel->mount;
    const struct chimera_vfs_attrs *attr  = &req->pending_attr;
    struct fuse_entry_out           entry;
    uint8_t                         payload[sizeof(entry) + 64];
    uint32_t                        entry_ms    = mount->entry_timeout_ms;
    uint32_t                        attr_ms     = mount->attr_timeout_ms;
    int                             child_cover = CHIMERA_FUSE_COVER_NONE;
    int                             rc;

    /* The kernel is about to hold a dentry under this directory; keep its
     * namespace coherent with the other protocols (every entry-shaped op
     * resolved req->nodeid's handle into req->fh).  Normally already done
     * at request entry (req->entry_cover) -- this is the backstop. */
    chimera_fuse_watch_dir(req->thread, mount, req->nodeid,
                           req->fh, req->fh_len);

    memset(&entry, 0, sizeof(entry));

    entry.nodeid = chimera_fuse_node_insert(mount->node_table,
                                            attr->va_fh, attr->va_fh_len);
    entry.generation = 1;

    chimera_fuse_attr_from_vfs(&entry.attr, attr);

    /* The kernel is about to cache this entry's attributes for
     * attr_timeout: a regular file gets (or re-arms) an invalidation
     * grant, a directory gets a change watch so its dentries and its own
     * attributes track foreign namespace ops. */
    if (S_ISREG(attr->va_mode)) {
        child_cover = chimera_fuse_grant_ensure(req->thread, mount,
                                                entry.nodeid,
                                                attr->va_fh, attr->va_fh_len,
                                                chimera_fuse_fh_hash(attr->va_fh,
                                                                     attr->va_fh_len));
    } else if (S_ISDIR(attr->va_mode)) {
        child_cover = chimera_fuse_watch_dir(req->thread, mount, entry.nodeid,
                                             attr->va_fh, attr->va_fh_len);
    }

    /* coherence=sync: the kernel may cache only what a live grant/watch
     * protects.  The DENTRY is safe when the parent watch predates this
     * request (req->entry_cover, captured at entry): a foreign namespace
     * op completing mid-flight then gates on our watch, and its
     * INVAL_ENTRY serializes behind the in-flight operation on the
     * parent's kernel lock, so it lands after -- and kills -- the entry
     * we are about to reply.  A watch born only now missed any such gate.
     * The ATTRIBUTES were fetched from the backend before the child's
     * grant existed, so they get a TTL only when the grant was ALREADY
     * held (a prior touch armed it and nothing broke it since).  A fresh
     * grant still pays off: it covers the pages and attrs of every
     * operation after this one. */
    if (mount->coherence_sync) {
        if (req->entry_cover != CHIMERA_FUSE_COVER_HELD) {
            entry_ms = 0;
        }
        if (child_cover != CHIMERA_FUSE_COVER_HELD) {
            attr_ms = 0;
        }
    }

    entry.entry_valid      = entry_ms / 1000;
    entry.entry_valid_nsec = (entry_ms % 1000) * 1000000;
    entry.attr_valid       = attr_ms / 1000;
    entry.attr_valid_nsec  = (attr_ms % 1000) * 1000000;

    memcpy(payload, &entry, sizeof(entry));

    if (req->pending_extra_len) {
        memcpy(payload + sizeof(entry), req->pending_extra,
               req->pending_extra_len);
    }

    /* CREATE's combined reply: the open flags depend on the child grant
     * armed above (the child nodeid did not exist when the caller built the
     * fuse_open_out), so patch them in here.  Pages are only seeded through
     * us after the arm, so a FRESH grant fully covers KEEP_CACHE. */
    if (req->opcode == FUSE_CREATE &&
        req->pending_extra_len == sizeof(struct fuse_open_out)) {
        struct fuse_open_out *oo =
            (struct fuse_open_out *) (payload + sizeof(entry));

        oo->open_flags |= chimera_fuse_open_cache_flags(mount, child_cover);
    }

    rc = chimera_fuse_send(req, 0, payload,
                           sizeof(entry) + req->pending_extra_len,
                           NULL, 0, 0);

    if (rc != 0) {
        if (chimera_fuse_node_forget(mount->node_table, entry.nodeid, 1)) {
            /* The undo retired the node: drop the coverage taken above. */
            chimera_fuse_watch_forget(mount, req->thread->vfs_thread->vfs,
                                      entry.nodeid);
            chimera_fuse_grant_forget(mount,
                                      req->thread->vfs_thread->vfs->vfs_state,
                                      entry.nodeid);
        }
        /* CREATE's owned open_file is undone by the deliver tail. */
    } else {
        /* The kernel owns the entry -- and, for CREATE, the open fh. */
        req->file_owned = 0;
        if (req->opcode == FUSE_CREATE) {
            req->file = NULL;
        }
    }
} /* chimera_fuse_entry_deliver */

/* OPEN/OPENDIR deliver: builds the fuse_open_out from the owned open_file.
 * OPEN's cache flags arm the invalidation grant here, post-commit, so a
 * replay never double-arms it. */
static void
chimera_fuse_openfile_deliver(struct chimera_fuse_request *req)
{
    struct chimera_fuse_mount *mount = req->channel->mount;
    struct fuse_open_out       out;
    int                        rc;

    memset(&out, 0, sizeof(out));
    out.fh = (uint64_t) (uintptr_t) req->file;

    /* With an invalidation grant in force from here on, the kernel's cached
     * pages are guaranteed to be dropped when any other party changes the
     * file, so letting them survive across open/close cycles is coherent --
     * and a real read-cache win.  (Pages, unlike attributes, are only
     * seeded through us AFTER the arm, so a fresh grant fully covers
     * them.)  No grant (contention) means no coverage: ttl mode keeps the
     * kernel's default invalidate-on-open behavior, sync mode goes further
     * and bypasses the page cache entirely so an uncovered open can never
     * serve stale data. */
    if (req->opcode == FUSE_OPEN) {
        out.open_flags |= chimera_fuse_open_cache_flags(
            mount, chimera_fuse_grant_open(req->thread, mount, req->nodeid,
                                           req->file->handle));
    }

    rc = chimera_fuse_send(req, 0, &out, sizeof(out), NULL, 0, 0);

    if (rc == 0) {
        /* The kernel owns the fh now; RELEASE(DIR) will retire it. */
        req->file       = NULL;
        req->file_owned = 0;
    }
    /* rc != 0: the kernel never learned this fh, so no RELEASE will come;
     * the deliver tail undoes the open_file. */
} /* chimera_fuse_openfile_deliver */

static void
chimera_fuse_reply_deliver(struct chimera_fuse_request *req)
{
    struct chimera_fuse_thread *thread = req->thread;
    int                         rc;

    if (req->pending_error != 0) {
        /* Error replies carry no payload regardless of kind; drop whatever
         * the successful-op path had staged. */
        chimera_fuse_send(req, req->pending_error, NULL, 0, NULL, 0, 0);

        if (req->pending_kind == CHIMERA_FUSE_REPLY_READ) {
            evpl_iovecs_release(thread->evpl, req->pending_iov,
                                req->pending_niov);
        }
        if (req->opcode == FUSE_READDIRPLUS) {
            chimera_fuse_readdirplus_unwind(req);
        }
    } else {
        switch (req->pending_kind) {
            case CHIMERA_FUSE_REPLY_READ:
                chimera_fuse_send(req, 0, NULL, 0, req->pending_iov,
                                  req->pending_niov, req->pending_data_len);
                /* The iovec array lives inside the request, so the backend's
                 * buffers are dropped before the request is recycled. */
                evpl_iovecs_release(thread->evpl, req->pending_iov,
                                    req->pending_niov);
                break;
            case CHIMERA_FUSE_REPLY_ENTRY:
                chimera_fuse_entry_deliver(req);
                break;
            case CHIMERA_FUSE_REPLY_OPENFILE:
                chimera_fuse_openfile_deliver(req);
                break;
            case CHIMERA_FUSE_REPLY_SIMPLE:
            default:
                rc = chimera_fuse_send(req, 0, req->pending_payload,
                                       req->pending_payload_len, NULL, 0, 0);
                if (rc != 0 && req->opcode == FUSE_READDIRPLUS) {
                    /* The kernel never received the packed entries. */
                    chimera_fuse_readdirplus_unwind(req);
                }
                break;
        } /* switch */
    }

    /* An owned open_file whose fh the kernel never learned (error reply, or
     * a failed entry/open send left the flag set) must be undone. */
    chimera_fuse_file_undo(req);

    req->pending_kind = CHIMERA_FUSE_REPLY_NONE;

    chimera_fuse_request_finish(req);
} /* chimera_fuse_reply_deliver */

/* --- reply helpers: park the reply, end the compound --- */

void
chimera_fuse_reply(
    struct chimera_fuse_request *req,
    int                          error,
    const void                  *payload,
    size_t                       payload_len)
{
    if (error == CHIMERA_FUSE_ECONFLICT) {
        chimera_fuse_compound_conflict(req);
        return;
    }

    req->pending_kind        = CHIMERA_FUSE_REPLY_SIMPLE;
    req->pending_error       = error;
    req->pending_payload_len = (error == 0) ? payload_len : 0;

    if (req->pending_payload_len == 0) {
        req->pending_payload = NULL;
    } else if (req->pending_payload_len <= sizeof(req->pending_copy)) {
        /* Small payloads are built on the caller's stack; copy them so they
         * survive the asynchronous compound end. */
        memcpy(req->pending_copy, payload, req->pending_payload_len);
        req->pending_payload = req->pending_copy;
    } else {
        /* Larger payloads are staged in the request buffer's reply area by
         * construction (readdir/xattr/readlink), which lives until the
         * request is recycled. */
        req->pending_payload = payload;
    }

    chimera_fuse_compound_commit(req);
} /* chimera_fuse_reply */

void
chimera_fuse_reply_read(
    struct chimera_fuse_request *req,
    int                          error,
    struct evpl_iovec           *iov,
    int                          niov,
    size_t                       data_len)
{
    if (error == CHIMERA_FUSE_ECONFLICT) {
        evpl_iovecs_release(req->thread->evpl, iov, niov);
        chimera_fuse_compound_conflict(req);
        return;
    }

    req->pending_kind     = CHIMERA_FUSE_REPLY_READ;
    req->pending_error    = error;
    req->pending_iov      = iov;
    req->pending_niov     = niov;
    req->pending_data_len = (error == 0) ? data_len : 0;

    chimera_fuse_compound_commit(req);
} /* chimera_fuse_reply_read */

void
chimera_fuse_reply_entry(
    struct chimera_fuse_request    *req,
    const struct chimera_vfs_attrs *attr,
    const void                     *extra,
    size_t                          extra_len)
{
    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        /* An owned open_file (CREATE) is undone by the deliver tail. */
        chimera_fuse_reply(req, EIO, NULL, 0);
        return;
    }

    chimera_fuse_abort_if(extra_len > sizeof(req->pending_extra),
                          "fuse entry reply extra too large");

    req->pending_kind  = CHIMERA_FUSE_REPLY_ENTRY;
    req->pending_error = 0;
    req->pending_attr  = *attr;
    /* The ACL pointer is scoped to the VFS completion callback and the entry
     * reply never reads it; do not let it dangle across the end. */
    req->pending_attr.va_acl = NULL;
    req->pending_extra_len   = extra_len;

    if (extra_len) {
        memcpy(req->pending_extra, extra, extra_len);
    }

    chimera_fuse_compound_commit(req);
} /* chimera_fuse_reply_entry */

void
chimera_fuse_reply_open(struct chimera_fuse_request *req)
{
    req->pending_kind  = CHIMERA_FUSE_REPLY_OPENFILE;
    req->pending_error = 0;

    chimera_fuse_compound_commit(req);
} /* chimera_fuse_reply_open */

void
chimera_fuse_channel_dead(struct chimera_fuse_channel *channel)
{
    if (channel->dead) {
        return;
    }

    channel->dead        = 1;
    channel->mount->dead = 1;

    if (channel->armed) {
        evpl_remove_fd_event(channel->thread->evpl, &channel->event);
        channel->armed = 0;
    }

    /* The fd stays open until the protocol destroy hook: in-flight requests
     * may still attempt replies on it, and closing early would let the
     * number be reused by an unrelated descriptor.  Open VFS handles the
     * kernel will now never RELEASE are swept at shutdown. */

    chimera_fuse_info("fuse mount %s: connection closed by kernel",
                      channel->mount->mountpoint);
} /* chimera_fuse_channel_dead */

/* Marshal a request completed off-thread home for its reply. */
void
chimera_fuse_resume_post(struct chimera_fuse_request *req)
{
    struct chimera_fuse_thread *thread = req->thread;

    pthread_mutex_lock(&thread->resume_lock);
    req->next            = thread->resume_queue;
    thread->resume_queue = req;
    pthread_mutex_unlock(&thread->resume_lock);

    evpl_ring_doorbell(&thread->resume_doorbell);
} /* chimera_fuse_resume_post */

void
chimera_fuse_resume_doorbell(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_fuse_thread  *thread = container_of(doorbell, struct chimera_fuse_thread, resume_doorbell);
    struct chimera_fuse_request *queue, *req;

    pthread_mutex_lock(&thread->resume_lock);
    queue                = thread->resume_queue;
    thread->resume_queue = NULL;
    pthread_mutex_unlock(&thread->resume_lock);

    while (queue) {
        req   = queue;
        queue = req->next;

        chimera_fuse_lock_resume(req);
    }
} /* chimera_fuse_resume_doorbell */

static void
chimera_fuse_op_interrupt(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_interrupt_in *in = arg;

    /* Interrupts matter for parked blocking locks; everything else here
     * completes promptly on its own.  A swallowed interrupt (unknown
     * unique, or one that raced ahead of its target) just means the
     * original request completes normally.  No reply either way: an
     * ENOSYS reply would disable interrupts connection-wide. */
    if (arglen >= sizeof(*in)) {
        chimera_fuse_locks_interrupt(req->channel->mount,
                                     req->thread->vfs_thread->vfs->vfs_state,
                                     in->unique);
    }

    chimera_fuse_request_free(req->thread, req);
} /* chimera_fuse_op_interrupt */

static void
chimera_fuse_op_destroy(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    chimera_fuse_reply(req, 0, NULL, 0);
} /* chimera_fuse_op_destroy */

const chimera_fuse_handler_t chimera_fuse_handlers[CHIMERA_FUSE_OPCODE_MAX] = {
    [FUSE_LOOKUP]          = chimera_fuse_op_lookup,
    [FUSE_FORGET]          = chimera_fuse_op_forget,
    [FUSE_BATCH_FORGET]    = chimera_fuse_op_batch_forget,
    [FUSE_GETATTR]         = chimera_fuse_op_getattr,
    [FUSE_SETATTR]         = chimera_fuse_op_setattr,
    [FUSE_READLINK]        = chimera_fuse_op_readlink,
    [FUSE_STATFS]          = chimera_fuse_op_statfs,
    [FUSE_ACCESS]          = chimera_fuse_op_access,
    [FUSE_OPENDIR]         = chimera_fuse_op_opendir,
    [FUSE_READDIR]         = chimera_fuse_op_readdir,
    [FUSE_READDIRPLUS]     = chimera_fuse_op_readdir,
    [FUSE_RELEASEDIR]      = chimera_fuse_op_releasedir,
    [FUSE_FSYNCDIR]        = chimera_fuse_op_fsyncdir,
    [FUSE_OPEN]            = chimera_fuse_op_open,
    [FUSE_CREATE]          = chimera_fuse_op_create,
    [FUSE_READ]            = chimera_fuse_op_read,
    [FUSE_WRITE]           = chimera_fuse_op_write,
    [FUSE_FLUSH]           = chimera_fuse_op_flush,
    [FUSE_FSYNC]           = chimera_fuse_op_fsync,
    [FUSE_RELEASE]         = chimera_fuse_op_release,
    [FUSE_FALLOCATE]       = chimera_fuse_op_fallocate,
    [FUSE_LSEEK]           = chimera_fuse_op_lseek,
    [FUSE_COPY_FILE_RANGE] = chimera_fuse_op_copy_file_range,
    [FUSE_MKDIR]           = chimera_fuse_op_mkdir,
    [FUSE_MKNOD]           = chimera_fuse_op_mknod,
    [FUSE_SYMLINK]         = chimera_fuse_op_symlink,
    [FUSE_LINK]            = chimera_fuse_op_link,
    [FUSE_UNLINK]          = chimera_fuse_op_unlink,
    [FUSE_RMDIR]           = chimera_fuse_op_rmdir,
    [FUSE_RENAME]          = chimera_fuse_op_rename,
    [FUSE_RENAME2]         = chimera_fuse_op_rename,
    [FUSE_GETXATTR]        = chimera_fuse_op_getxattr,
    [FUSE_SETXATTR]        = chimera_fuse_op_setxattr,
    [FUSE_LISTXATTR]       = chimera_fuse_op_listxattr,
    [FUSE_REMOVEXATTR]     = chimera_fuse_op_removexattr,
    [FUSE_GETLK]           = chimera_fuse_op_getlk,
    [FUSE_SETLK]           = chimera_fuse_op_setlk,
    [FUSE_SETLKW]          = chimera_fuse_op_setlk,
    [FUSE_INTERRUPT]       = chimera_fuse_op_interrupt,
    [FUSE_DESTROY]         = chimera_fuse_op_destroy,
};

static void
chimera_fuse_dispatch(
    struct chimera_fuse_request *req,
    uint32_t                     len)
{
    const struct fuse_in_header *hdr = chimera_fuse_request_hdr(req);
    chimera_fuse_handler_t       handler;

    if (len < sizeof(*hdr) || hdr->len != len) {
        chimera_fuse_error("fuse request framing mismatch (read %u, header %u)",
                           len, len >= sizeof(*hdr) ? hdr->len : 0);
        chimera_fuse_request_free(req->thread, req);
        return;
    }

    req->unique      = hdr->unique;
    req->opcode      = hdr->opcode;
    req->nodeid      = hdr->nodeid;
    req->buf_len     = len;
    req->entry_cover = CHIMERA_FUSE_COVER_NONE;

    /* Per-request compound state: the compound itself begins lazily at the
     * first VFS call; the wait-die ts is fixed here and reused across
     * conflict replays.  The fh scratch is cleared so the lazy begin's hint
     * (and the getattr completion's rearm guard) never sees a previous
     * request's handle. */
    req->compound         = NULL;
    req->compound_ts      = chimera_vfs_compound_alloc_ts(req->thread->vfs_thread);
    req->compound_attempt = 0;
    req->pending_kind     = CHIMERA_FUSE_REPLY_NONE;
    req->pending_error    = 0;
    req->file_owned       = 0;
    req->fh_len           = 0;
    req->fh2_len          = 0;

    if (req->opcode == FUSE_READDIRPLUS) {
        /* The unwind paths key off u.readdir.used, which the handler only
         * initializes after its argument checks. */
        req->u.readdir.used = 0;
        req->u.readdir.plus = 1;
    }

    chimera_fuse_map_cred(&req->cred, hdr, req->channel->mount);

    handler = hdr->opcode < CHIMERA_FUSE_OPCODE_MAX ?
        chimera_fuse_handlers[hdr->opcode] : NULL;

    if (!handler) {
        chimera_fuse_reply(req, ENOSYS, NULL, 0);
        return;
    }

    handler(req, hdr, hdr + 1, len - sizeof(*hdr));
} /* chimera_fuse_dispatch */

void
chimera_fuse_channel_readable(
    struct evpl          *evpl,
    struct evpl_fd_event *event)
{
    struct chimera_fuse_channel *channel = container_of(event, struct chimera_fuse_channel, event);
    struct chimera_fuse_thread  *thread  = channel->thread;
    struct chimera_fuse_request *req;
    ssize_t                      len;
    int                          i;

    for (i = 0; i < CHIMERA_FUSE_READ_BATCH; i++) {

        req = chimera_fuse_request_alloc(thread, channel);

        len = read(channel->fd,
                   (uint8_t *) evpl_iovec_data(&req->buf) + CHIMERA_FUSE_REQ_OFF,
                   CHIMERA_FUSE_READ_LEN);

        if (len < 0) {
            chimera_fuse_request_free(thread, req);

            switch (errno) {
                case EINTR:
                    continue;
                case EAGAIN:
                    evpl_fd_event_mark_unreadable(evpl, event);
                    return;
                case ENOENT:
                    /* Request aborted between wakeup and read. */
                    continue;
                case ENODEV:
                    chimera_fuse_channel_dead(channel);
                    return;
                default:
                    chimera_fuse_error("fuse channel read failed: %s",
                                       strerror(errno));
                    evpl_fd_event_mark_unreadable(evpl, event);
                    return;
            } /* switch */
        }

        chimera_fuse_dispatch(req, len);
    }

    /* Batch cap reached: the event stays readable, so the loop re-invokes us
     * on the next pass after giving other work a turn. */
} /* chimera_fuse_channel_readable */

void
chimera_fuse_channel_error(
    struct evpl          *evpl,
    struct evpl_fd_event *event)
{
    struct chimera_fuse_channel *channel = container_of(event, struct chimera_fuse_channel, event);

    chimera_fuse_channel_dead(channel);
} /* chimera_fuse_channel_error */
