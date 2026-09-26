// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "fuse_internal.h"
#include "common/macros.h"

/*
 * FUSE over io_uring.
 *
 * With FUSE_OVER_IO_URING negotiated, the server hands the kernel "ring
 * entries" -- a header buffer and a payload buffer each -- by submitting
 * IORING_OP_URING_CMD against a /dev/fuse fd.  The kernel keeps one queue per
 * possible CPU and routes a request to the queue of the CPU the calling task
 * runs on; the command completes when a request has been copied into the
 * entry's buffers.  The reply goes back in the same buffers, and a single
 * COMMIT_AND_FETCH command both delivers it and re-arms the entry for the
 * next request.  One syscall-free round trip per request replaces a read(2)
 * and a writev(2) on the channel fd.
 *
 * The kernel only switches the connection over once EVERY queue has at least
 * one entry, so each thread registers its share of the queues (qid modulo
 * the mount's channel count) on a ring of its own; until the last one lands
 * -- or for good, if registration fails -- requests keep arriving on the
 * channel fds, which stay armed regardless because FORGET and INTERRUPT
 * never travel through the ring.
 *
 * Buffer layout.  The kernel splits a request three ways: the fuse_in_header
 * into the header buffer's in_out area, the opcode's fixed argument struct
 * into op_in, and everything after it (names, write data) into the payload
 * buffer.  The opcode handlers expect the /dev/fuse wire image, so the entry's
 * payload buffer is placed at the page boundary inside the request buffer
 * (CHIMERA_FUSE_URING_PAYLOAD_OFF) and the two headers are copied back in
 * front of it -- which leaves a WRITE's data exactly where a channel read puts
 * it, page-aligned for the backends' zero-copy paths.  The reply is staged at
 * the same payload address, where the kernel copies it from.
 *
 * Each entry owns one request for good: the request's buffer IS the entry's
 * payload buffer, so the request is never pooled, and it is only handed back
 * to the kernel (by the COMMIT_AND_FETCH its reply submits) once the reply
 * helpers have finished with it.  Submission is deferred to the end of the
 * loop pass, which is also what keeps the kernel from refilling the buffer
 * while a reply helper is still reading the request it replaced.
 */

#if CHIMERA_FUSE_HAVE_URING

#include <liburing.h>

#define CHIMERA_FUSE_URING_PAYLOAD_OFF \
        (CHIMERA_FUSE_REQ_OFF + (int) (sizeof(struct fuse_in_header) + \
                                       sizeof(struct fuse_write_in)))

#define CHIMERA_FUSE_URING_PAYLOAD_LEN (CHIMERA_FUSE_BUFSZ - CHIMERA_FUSE_URING_PAYLOAD_OFF)

_Static_assert(CHIMERA_FUSE_URING_PAYLOAD_OFF % 4096 == 0,
               "ring payload buffer must start on a page boundary");
_Static_assert(CHIMERA_FUSE_URING_PAYLOAD_LEN >= CHIMERA_FUSE_MAX_WRITE,
               "ring payload buffer must hold a maximal request");

#define CHIMERA_FUSE_URING_SQ_ENTRIES  256
#define CHIMERA_FUSE_URING_CQ_ENTRIES  4096
#define CHIMERA_FUSE_URING_REAP_BATCH  64

enum chimera_fuse_uring_ent_state {
    /* A REGISTER or COMMIT_AND_FETCH is queued or in the kernel. */
    CHIMERA_FUSE_URING_ENT_KERNEL = 0,
    /* Holds a request the server is working on. */
    CHIMERA_FUSE_URING_ENT_USER   = 1,
    /* The kernel refused or tore down the entry; it is never resubmitted. */
    CHIMERA_FUSE_URING_ENT_DEAD   = 2,
};

struct chimera_fuse_uring_ent {
    /* Written by the kernel on fetch and read by it on commit. */
    struct fuse_uring_req_header   hdr;
    /* REGISTER's two segments: hdr, then the payload buffer. */
    struct iovec                   iov[2];
    struct chimera_fuse_uring     *uring;
    struct chimera_fuse_channel   *channel;
    struct chimera_fuse_request   *req;
    uint64_t                       commit_id;
    uint16_t                       qid;
    int                            state;
    /* A replacement entry was registered while this one sat parked. */
    int                            replaced;
    struct chimera_fuse_uring_ent *next;
};

struct chimera_fuse_uring {
    struct chimera_fuse_thread    *thread;
    struct io_uring                ring;
    struct evpl_doorbell           doorbell;
    struct evpl_poll              *poll;
    struct evpl_deferral           deferral;
    /* Every entry this ring ever registered, for teardown. */
    struct chimera_fuse_uring_ent *ents;
    /* Ring setup failed; the thread serves only its channel fds. */
    int                            failed;
};

/*
 * The size of the fixed argument struct each opcode's first in-arg carries,
 * which the kernel moves into op_in instead of the payload.  Opcodes whose
 * first argument is a name or nothing at all send a zero-sized placeholder
 * there, so everything they carry lands in the payload.  Sizes follow the
 * flags we negotiate: without FUSE_SETXATTR_EXT, SETXATTR's struct is the
 * 8-byte compat form.  Opcodes missing here get ENOSYS from the dispatcher
 * whatever their layout.
 */
static uint32_t
chimera_fuse_uring_op_in_size(uint32_t opcode)
{
    switch (opcode) {
        case FUSE_GETATTR:
            return sizeof(struct fuse_getattr_in);
        case FUSE_SETATTR:
            return sizeof(struct fuse_setattr_in);
        case FUSE_MKNOD:
            return sizeof(struct fuse_mknod_in);
        case FUSE_MKDIR:
            return sizeof(struct fuse_mkdir_in);
        case FUSE_RENAME:
            return sizeof(struct fuse_rename_in);
        case FUSE_RENAME2:
            return sizeof(struct fuse_rename2_in);
        case FUSE_LINK:
            return sizeof(struct fuse_link_in);
        case FUSE_OPEN:
        case FUSE_OPENDIR:
            return sizeof(struct fuse_open_in);
        case FUSE_CREATE:
            return sizeof(struct fuse_create_in);
        case FUSE_READ:
        case FUSE_READDIR:
        case FUSE_READDIRPLUS:
            return sizeof(struct fuse_read_in);
        case FUSE_WRITE:
            return sizeof(struct fuse_write_in);
        case FUSE_RELEASE:
        case FUSE_RELEASEDIR:
            return sizeof(struct fuse_release_in);
        case FUSE_FSYNC:
        case FUSE_FSYNCDIR:
            return sizeof(struct fuse_fsync_in);
        case FUSE_FLUSH:
            return sizeof(struct fuse_flush_in);
        case FUSE_SETXATTR:
            return FUSE_COMPAT_SETXATTR_IN_SIZE;
        case FUSE_GETXATTR:
        case FUSE_LISTXATTR:
            return sizeof(struct fuse_getxattr_in);
        case FUSE_GETLK:
        case FUSE_SETLK:
        case FUSE_SETLKW:
            return sizeof(struct fuse_lk_in);
        case FUSE_ACCESS:
            return sizeof(struct fuse_access_in);
        case FUSE_FALLOCATE:
            return sizeof(struct fuse_fallocate_in);
        case FUSE_LSEEK:
            return sizeof(struct fuse_lseek_in);
        case FUSE_COPY_FILE_RANGE:
            return sizeof(struct fuse_copy_file_range_in);
        case FUSE_INTERRUPT:
            return sizeof(struct fuse_interrupt_in);
        case FUSE_FORGET:
            return sizeof(struct fuse_forget_in);
        case FUSE_BATCH_FORGET:
            return sizeof(struct fuse_batch_forget_in);
        default:
            /* LOOKUP, UNLINK, RMDIR, SYMLINK, READLINK, STATFS,
             * REMOVEXATTR, DESTROY */
            return 0;
    } /* switch */
} /* chimera_fuse_uring_op_in_size */

static struct io_uring_sqe *
chimera_fuse_uring_get_sqe(struct chimera_fuse_uring *uring)
{
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&uring->ring);

    if (!sqe) {
        /* SQ full: push what is queued and take the slot it frees. */
        io_uring_submit(&uring->ring);
        sqe = io_uring_get_sqe(&uring->ring);
    }

    chimera_fuse_abort_if(!sqe, "fuse io_uring: no submission queue entry");

    return sqe;
} /* chimera_fuse_uring_get_sqe */

static void
chimera_fuse_uring_submit_ent(
    struct chimera_fuse_uring_ent *ent,
    uint32_t                       cmd_op)
{
    struct chimera_fuse_uring *uring = ent->uring;
    struct io_uring_sqe       *sqe;
    struct fuse_uring_cmd_req *cmd;

    sqe = chimera_fuse_uring_get_sqe(uring);

    /* A SQE128 slot: the fuse command lives in the extended area, which
     * io_uring_get_sqe() does not clear. */
    memset(sqe, 0, 2 * sizeof(*sqe));

    sqe->opcode = IORING_OP_URING_CMD;
    sqe->fd     = ent->channel->fd;
    sqe->cmd_op = cmd_op;

    if (cmd_op == FUSE_IO_URING_CMD_REGISTER) {
        sqe->addr = (uint64_t) (uintptr_t) ent->iov;
        sqe->len  = 2;
    }

    cmd            = (struct fuse_uring_cmd_req *) sqe->cmd;
    cmd->qid       = ent->qid;
    cmd->commit_id = ent->commit_id;
    cmd->flags     = 0;

    io_uring_sqe_set_data(sqe, ent);

    ent->state = CHIMERA_FUSE_URING_ENT_KERNEL;

    evpl_defer(uring->thread->evpl, &uring->deferral);
} /* chimera_fuse_uring_submit_ent */

static void
chimera_fuse_uring_ent_create(
    struct chimera_fuse_uring   *uring,
    struct chimera_fuse_channel *channel,
    uint16_t                     qid)
{
    struct chimera_fuse_thread    *thread = uring->thread;
    struct chimera_fuse_uring_ent *ent;
    struct chimera_fuse_request   *req;
    int                            niov;

    ent = calloc(1, sizeof(*ent));
    req = calloc(1, sizeof(*req));

    niov = evpl_iovec_alloc(thread->evpl, CHIMERA_FUSE_BUFSZ, 4096, 1, 0,
                            &req->buf);
    chimera_fuse_abort_if(niov != 1,
                          "fuse ring buffer allocation failed (%d)", niov);

    req->buf_allocated = 1;
    req->thread        = thread;
    req->ring_ent      = ent;

    ent->uring   = uring;
    ent->channel = channel;
    ent->req     = req;
    ent->qid     = qid;

    ent->iov[0].iov_base = &ent->hdr;
    ent->iov[0].iov_len  = sizeof(ent->hdr);
    ent->iov[1].iov_base = (uint8_t *) evpl_iovec_data(&req->buf) +
        CHIMERA_FUSE_URING_PAYLOAD_OFF;
    ent->iov[1].iov_len = CHIMERA_FUSE_URING_PAYLOAD_LEN;

    ent->next   = uring->ents;
    uring->ents = ent;

    chimera_fuse_uring_submit_ent(ent, FUSE_IO_URING_CMD_REGISTER);
} /* chimera_fuse_uring_ent_create */

/* A request landed in the entry: rebuild the wire image and dispatch it. */
static void
chimera_fuse_uring_receive(struct chimera_fuse_uring_ent *ent)
{
    struct chimera_fuse_request  *req    = ent->req;
    struct chimera_fuse_thread   *thread = req->thread;
    struct chimera_fuse_mount    *mount  = ent->channel->mount;
    const struct fuse_in_header  *ih     = (const struct fuse_in_header *) ent->hdr.in_out;
    struct fuse_uring_ent_in_out *io     = &ent->hdr.ring_ent_in_out;
    struct fuse_in_header        *hdr;
    uint8_t                      *base;
    uint32_t                      op_sz, len;

    ent->state     = CHIMERA_FUSE_URING_ENT_USER;
    ent->commit_id = io->commit_id;

    req->channel        = ent->channel;
    req->handle         = NULL;
    req->file           = NULL;
    req->ring_committed = 0;

    thread->active_requests++;

    if (!atomic_exchange(&mount->uring_active, 1)) {
        chimera_fuse_info("fuse mount %s: serving requests over io_uring "
                          "(%d queues x %u entries)",
                          mount->mountpoint, mount->uring_nr_queues,
                          mount->uring_depth);
    }

    op_sz = chimera_fuse_uring_op_in_size(ih->opcode);

    if (io->payload_sz > CHIMERA_FUSE_URING_PAYLOAD_LEN) {
        chimera_fuse_error("fuse io_uring: opcode %u payload %u exceeds the entry",
                           ih->opcode, io->payload_sz);
        req->unique = ih->unique;
        req->opcode = ih->opcode;
        chimera_fuse_reply(req, EIO, NULL, 0);
        return;
    }

    base         = evpl_iovec_data(&req->buf);
    req->hdr_off = CHIMERA_FUSE_URING_PAYLOAD_OFF - op_sz - sizeof(*ih);
    hdr          = (struct fuse_in_header *) (base + req->hdr_off);

    memcpy(hdr, ih, sizeof(*ih));
    memcpy(hdr + 1, ent->hdr.op_in, op_sz);

    /* The ring carries the length in payload_sz; make the header agree so
     * the dispatcher's framing check holds as it does for a channel read. */
    len      = sizeof(*ih) + op_sz + io->payload_sz;
    hdr->len = len;

    chimera_fuse_dispatch(req, len);
} /* chimera_fuse_uring_receive */

static void
chimera_fuse_uring_complete(
    struct chimera_fuse_uring_ent *ent,
    int                            res)
{
    struct chimera_fuse_mount *mount = ent->channel->mount;

    if (res == 0) {
        chimera_fuse_uring_receive(ent);
        return;
    }

    ent->state = CHIMERA_FUSE_URING_ENT_DEAD;

    switch (-res) {
        case ENOTCONN:
        case ECONNABORTED:
        case ECANCELED:
        case ENODEV:
            /* Connection teardown. */
            break;
        default:
            /* Most likely the very first REGISTER being refused; the channel
             * fds keep serving the mount either way. */
            if (!atomic_exchange(&mount->uring_failed, 1)) {
                chimera_fuse_error("fuse mount %s: io_uring entry rejected (qid %u): %s; "
                                   "continuing on /dev/fuse",
                                   mount->mountpoint, ent->qid, strerror(-res));
            }
            break;
    } /* switch */
} /* chimera_fuse_uring_complete */

static int
chimera_fuse_uring_reap(struct chimera_fuse_uring *uring)
{
    struct io_uring_cqe           *cqes[CHIMERA_FUSE_URING_REAP_BATCH];
    struct chimera_fuse_uring_ent *ents[CHIMERA_FUSE_URING_REAP_BATCH];
    int                            res[CHIMERA_FUSE_URING_REAP_BATCH];
    int                            i, n, total = 0;

    do {
        n = io_uring_peek_batch_cqe(&uring->ring, cqes,
                                    CHIMERA_FUSE_URING_REAP_BATCH);

        /* Retire the batch before dispatching it: the handlers may submit,
         * and the CQ slots are better free by then. */
        for (i = 0; i < n; i++) {
            ents[i] = io_uring_cqe_get_data(cqes[i]);
            res[i]  = cqes[i]->res;
        }

        io_uring_cq_advance(&uring->ring, n);

        for (i = 0; i < n; i++) {
            chimera_fuse_uring_complete(ents[i], res[i]);
        }

        if (n) {
            evpl_activity(uring->thread->evpl);
        }

        total += n;
    } while (n == CHIMERA_FUSE_URING_REAP_BATCH);

    return total;
} /* chimera_fuse_uring_reap */

static void
chimera_fuse_uring_doorbell(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_fuse_uring *uring = container_of(doorbell, struct chimera_fuse_uring, doorbell);

    chimera_fuse_uring_reap(uring);
} /* chimera_fuse_uring_doorbell */

static void
chimera_fuse_uring_poll(
    struct evpl *evpl,
    void        *private_data)
{
    chimera_fuse_uring_reap(private_data);
} /* chimera_fuse_uring_poll */

/* While the loop busy-polls, CQEs are reaped every pass and the eventfd would
 * only generate wakeups nobody needs (the libevpl io_uring pattern). */
static void
chimera_fuse_uring_poll_enter(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_fuse_uring *uring = private_data;

    io_uring_unregister_eventfd(&uring->ring);
    chimera_fuse_uring_reap(uring);
} /* chimera_fuse_uring_poll_enter */

static void
chimera_fuse_uring_poll_exit(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_fuse_uring *uring = private_data;

    io_uring_register_eventfd(&uring->ring, evpl_doorbell_fd(&uring->doorbell));
    chimera_fuse_uring_reap(uring);
} /* chimera_fuse_uring_poll_exit */

/* Last look before the loop sleeps: completions posted between the final
 * reap and the wait would otherwise sit until the eventfd fires.  Anything
 * dispatched here may have armed the submit deferral, so the loop must take
 * another pass rather than sleep. */
static int
chimera_fuse_uring_prepare(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_fuse_uring *uring = private_data;

    return chimera_fuse_uring_reap(uring) != 0 ||
           io_uring_cq_ready(&uring->ring) != 0;
} /* chimera_fuse_uring_prepare */

static void
chimera_fuse_uring_flush(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_fuse_uring *uring = private_data;
    int                        rc;

    rc = io_uring_submit(&uring->ring);

    chimera_fuse_abort_if(rc < 0, "fuse io_uring submit failed: %s", strerror(-rc));
} /* chimera_fuse_uring_flush */

static struct chimera_fuse_uring *
chimera_fuse_uring_get(struct chimera_fuse_thread *thread)
{
    struct chimera_fuse_uring *uring = thread->uring;
    struct io_uring_params     params;
    int                        rc;

    if (uring) {
        return uring->failed ? NULL : uring;
    }

    uring         = calloc(1, sizeof(*uring));
    uring->thread = thread;
    thread->uring = uring;

    /* SQE128 is mandatory: the fuse command does not fit a 64-byte SQE.
     * The kernel copies each request in task work on this thread; with
     * COOP_TASKRUN that work waits for our next kernel entry, which the
     * TASKRUN_FLAG lets liburing's CQ peek perform only when needed (the
     * eventfd wakeup covers a sleeping loop).  Created here, on the thread
     * that will be the ring's only submitter. */
    memset(&params, 0, sizeof(params));
    params.flags = IORING_SETUP_SQE128 | IORING_SETUP_SINGLE_ISSUER |
        IORING_SETUP_COOP_TASKRUN | IORING_SETUP_TASKRUN_FLAG |
        IORING_SETUP_CQSIZE;
    params.cq_entries = CHIMERA_FUSE_URING_CQ_ENTRIES;

    rc = io_uring_queue_init_params(CHIMERA_FUSE_URING_SQ_ENTRIES,
                                    &uring->ring, &params);

    if (rc < 0) {
        chimera_fuse_error("fuse io_uring: ring setup failed: %s; "
                           "this thread stays on /dev/fuse", strerror(-rc));
        uring->failed = 1;
        return NULL;
    }

    evpl_add_doorbell(thread->evpl, &uring->doorbell, chimera_fuse_uring_doorbell);

    rc = io_uring_register_eventfd(&uring->ring, evpl_doorbell_fd(&uring->doorbell));

    chimera_fuse_abort_if(rc < 0, "fuse io_uring: eventfd registration failed: %s",
                          strerror(-rc));

    evpl_deferral_init(&uring->deferral, chimera_fuse_uring_flush, uring);

    uring->poll = evpl_add_poll(thread->evpl,
                                chimera_fuse_uring_poll_enter,
                                chimera_fuse_uring_poll_exit,
                                chimera_fuse_uring_poll,
                                uring);

    evpl_poll_set_prepare_callback(uring->poll, chimera_fuse_uring_prepare);

    return uring;
} /* chimera_fuse_uring_get */

void
chimera_fuse_uring_attach(
    struct chimera_fuse_thread  *thread,
    struct chimera_fuse_channel *channel)
{
    struct chimera_fuse_mount *mount = channel->mount;
    struct chimera_fuse_uring *uring;
    int                        qid, d, n = 0;

    if (thread->thread_slot >= mount->num_channels) {
        return;
    }

    uring = chimera_fuse_uring_get(thread);

    if (!uring) {
        return;
    }

    for (qid = thread->thread_slot; qid < mount->uring_nr_queues;
         qid += mount->num_channels) {
        for (d = 0; d < (int) mount->uring_depth; d++) {
            chimera_fuse_uring_ent_create(uring, channel, qid);
            n++;
        }
    }

    chimera_fuse_debug("fuse mount %s: thread %d registering %d io_uring entries",
                       mount->mountpoint, thread->thread_slot, n);
} /* chimera_fuse_uring_attach */

int
chimera_fuse_uring_commit(
    struct chimera_fuse_request *req,
    int                          error,
    const void                  *payload,
    size_t                       payload_len,
    struct evpl_iovec           *data_iov,
    int                          data_niov,
    size_t                       data_len)
{
    struct chimera_fuse_uring_ent *ent = req->ring_ent;
    struct fuse_out_header        *oh  = (struct fuse_out_header *) ent->hdr.in_out;
    uint8_t                       *dst;
    size_t                         total = 0, chunk;
    int                            i;

    dst = (uint8_t *) evpl_iovec_data(&req->buf) + CHIMERA_FUSE_URING_PAYLOAD_OFF;

    if (error == 0) {
        chimera_fuse_abort_if(payload_len + data_len > CHIMERA_FUSE_URING_PAYLOAD_LEN,
                              "fuse io_uring reply too large (%zu)",
                              payload_len + data_len);

        /* Staged replies (chimera_fuse_reply_space) live further along this
         * same buffer, so the regions may overlap. */
        if (payload_len) {
            memmove(dst, payload, payload_len);
            total = payload_len;
        }

        for (i = 0; i < data_niov && total < payload_len + data_len; i++) {
            chunk = evpl_iovec_length(&data_iov[i]);

            if (chunk > payload_len + data_len - total) {
                chunk = payload_len + data_len - total;
            }

            memcpy(dst + total, evpl_iovec_data(&data_iov[i]), chunk);
            total += chunk;
        }
    }

    memset(oh, 0, sizeof(*oh));
    oh->len    = sizeof(*oh) + total;
    oh->error  = -error;
    oh->unique = req->unique;

    ent->hdr.ring_ent_in_out.payload_sz = total;

    req->ring_committed = 1;

    chimera_fuse_uring_submit_ent(ent, FUSE_IO_URING_CMD_COMMIT_AND_FETCH);

    /* The kernel's verdict arrives only as the entry's next completion; a
     * refused commit means the connection is going away, when nothing a
     * caller could undo matters any more. */
    return 0;
} /* chimera_fuse_uring_commit */

void
chimera_fuse_uring_release(struct chimera_fuse_request *req)
{
    if (req->ring_committed) {
        return;
    }

    /* Freed without a reply (a framing error, or a dead channel): the kernel
     * still waits on this request, and the entry comes back only through a
     * commit. */
    chimera_fuse_uring_commit(req, EIO, NULL, 0, NULL, 0, 0);
} /* chimera_fuse_uring_release */

void
chimera_fuse_uring_parked(struct chimera_fuse_request *req)
{
    struct chimera_fuse_uring_ent *ent = req->ring_ent;

    /* The kernel feeds a queue only from its own entries, so a queue whose
     * every entry held a blocked SETLKW could not deliver the UNLOCK (or
     * FLUSH, or RELEASE) that would free them.  Give the queue a fresh entry
     * instead; there is no way to retire one, so the queue keeps it after
     * the parked request completes -- bounded by the peak of concurrently
     * blocked locks. */
    if (!ent || ent->replaced) {
        return;
    }

    ent->replaced = 1;

    chimera_fuse_uring_ent_create(ent->uring, ent->channel, ent->qid);
} /* chimera_fuse_uring_parked */

void
chimera_fuse_uring_thread_destroy(struct chimera_fuse_thread *thread)
{
    struct chimera_fuse_uring     *uring = thread->uring;
    struct chimera_fuse_uring_ent *ent;

    if (!uring) {
        return;
    }

    thread->uring = NULL;

    if (!uring->failed) {
        evpl_remove_deferral(thread->evpl, &uring->deferral);
        evpl_remove_poll(thread->evpl, uring->poll);

        /* Cancels the entries still parked in the kernel before their
         * buffers go away below. */
        io_uring_queue_exit(&uring->ring);

        evpl_remove_doorbell(thread->evpl, &uring->doorbell);
    }

    while (uring->ents) {
        ent         = uring->ents;
        uring->ents = ent->next;

        evpl_iovec_release(thread->evpl, &ent->req->buf);
        free(ent->req);
        free(ent);
    }

    free(uring);
} /* chimera_fuse_uring_thread_destroy */

int
chimera_fuse_uring_nr_queues(void)
{
    FILE *f;
    char  buf[256], *p, *end;
    long  lo, hi, max = -1;

    /* nr_cpu_ids, which the kernel sizes its queue array by: one past the
     * highest possible CPU ("0-47", "0,2-5"). */
    f = fopen("/sys/devices/system/cpu/possible", "r");

    if (!f) {
        return -1;
    }

    if (!fgets(buf, sizeof(buf), f)) {
        fclose(f);
        return -1;
    }

    fclose(f);

    p = buf;

    while (*p && *p != '\n') {
        lo = strtol(p, &end, 10);

        if (end == p) {
            return -1;
        }

        hi = lo;
        p  = end;

        if (*p == '-') {
            p++;
            hi = strtol(p, &end, 10);

            if (end == p) {
                return -1;
            }
            p = end;
        }

        if (hi > max) {
            max = hi;
        }

        if (*p == ',') {
            p++;
        }
    }

    return max < 0 || max >= UINT16_MAX ? -1 : (int) max + 1;
} /* chimera_fuse_uring_nr_queues */

#else  /* if CHIMERA_FUSE_HAVE_URING */

void
chimera_fuse_uring_attach(
    struct chimera_fuse_thread  *thread,
    struct chimera_fuse_channel *channel)
{
} /* chimera_fuse_uring_attach */

int
chimera_fuse_uring_commit(
    struct chimera_fuse_request *req,
    int                          error,
    const void                  *payload,
    size_t                       payload_len,
    struct evpl_iovec           *data_iov,
    int                          data_niov,
    size_t                       data_len)
{
    return -1;
} /* chimera_fuse_uring_commit */

void
chimera_fuse_uring_release(struct chimera_fuse_request *req)
{
} /* chimera_fuse_uring_release */

void
chimera_fuse_uring_parked(struct chimera_fuse_request *req)
{
} /* chimera_fuse_uring_parked */

void
chimera_fuse_uring_thread_destroy(struct chimera_fuse_thread *thread)
{
} /* chimera_fuse_uring_thread_destroy */

int
chimera_fuse_uring_nr_queues(void)
{
    return -1;
} /* chimera_fuse_uring_nr_queues */

#endif /* if CHIMERA_FUSE_HAVE_URING */
