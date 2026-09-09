// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * pNFS MDS-path data redirect.
 *
 * When a file is DS-resident its bytes live in a backing file on a data server,
 * not in the metadata server's own backend -- the MDS inode carries only the
 * namespace entry, the mode/owner/ACL, and the opaque layout blob naming the
 * backing file.  An NFSv4.1 client with a layout reads and writes that backing
 * file directly and never troubles the MDS.  Every other caller -- SMB, NFSv3,
 * S3, and a pNFS client that has fallen back to the MDS as RFC 8435 permits at
 * any time -- issues its I/O against the MDS, and without a redirect that I/O
 * lands on an empty local inode: the two paths see different bytes.
 *
 * So a data operation on a DS-resident file is reissued against the backing
 * file.  This is deliberately a VFS-layer concern rather than a per-backend one.
 * The backends (memfs, diskfs, cairn) have no concept of a data server; they
 * persist the blob and nothing more.  The device table, the steering and the
 * blob codec already live here, and putting the redirect anywhere else would
 * mean three copies of one subtle invariant plus a silent regression for the
 * next backend that advertises CHIMERA_VFS_CAP_LAYOUT.
 *
 * The redirect is resolved lazily, on the first data operation that needs it,
 * and never at open: a pure pNFS client does its I/O on the data server and no
 * MDS-path data operation ever arrives, so it must not pay for a fallback it
 * will not use.  A file that is not DS-resident, and any file at all when pNFS
 * is not configured, costs one branch.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <sys/stat.h>

#include "vfs/vfs.h"
#include "vfs/vfs_pnfs.h"
#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "common/macros.h"

struct chimera_vfs_pnfs_io_ctx {
    struct chimera_vfs_thread      *thread;
    const struct chimera_vfs_cred  *cred;
    struct chimera_vfs_open_handle *mds_handle;
    chimera_vfs_pnfs_io_callback_t  callback;
    void                           *private_data;
    uint8_t                         for_write;

    /* Materialization state (see chimera_vfs_pnfs_io_materialize). */
    struct chimera_vfs_ds          *ds;
    struct chimera_vfs_open_handle *ds_root_handle;
    struct chimera_vfs_open_handle *backing_handle;
    uint64_t                        fileid;
    char                            backing_name[CHIMERA_VFS_PNFS_BACKING_NAME_MAX];
    /* Doubles as the backing-file create attrs and, later, the layout-blob
     * setattr: the two never overlap in time, and va_pnfs is where the packed
     * blob has to end up anyway. */
    struct chimera_vfs_attrs        attr;
    uint8_t                         backing_fh[CHIMERA_VFS_FH_SIZE + 16];
    uint32_t                        backing_fh_len;
};

_Static_assert(sizeof(struct chimera_vfs_pnfs_io_ctx) <= CHIMERA_VFS_GATE_SCRATCH_SIZE,
               "pnfs io context outgrew the request gate scratch area");

static void
chimera_vfs_pnfs_io_done(
    struct chimera_vfs_pnfs_io_ctx *ctx,
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *io_handle,
    int                             redirected)
{
    chimera_vfs_pnfs_io_callback_t callback     = ctx->callback;
    void                          *private_data = ctx->private_data;
    struct chimera_vfs_thread     *thread       = ctx->thread;

    chimera_vfs_gate_scratch_free(thread, ctx);
    callback(error_code, io_handle, redirected, private_data);
} /* chimera_vfs_pnfs_io_done */

static void
chimera_vfs_pnfs_io_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_pnfs_io_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        /* The blob names a backing file we cannot open.  Failing the I/O is the
         * only honest answer: the bytes are on the data server, so serving the
         * MDS inode instead would silently return the wrong (empty) content. */
        chimera_vfs_pnfs_io_done(ctx, error_code, NULL, 0);
        return;
    }

    chimera_vfs_pnfs_io_done(ctx, CHIMERA_VFS_OK, oh, 1);
} /* chimera_vfs_pnfs_io_open_cb */

/*
 * Give a file that has no layout yet a home on a data server: create its
 * backing file and record the blob naming it.
 *
 * This runs on the first WRITE, before any byte has been stored, and that
 * timing is the whole point.  Assigning residency later -- when a client first
 * asks for a layout, as LAYOUTGET used to do unconditionally -- splits the file
 * in two: whatever was already written stays on the metadata server while
 * everything after it goes to the data server, and no reader can see both
 * halves.  Deciding before the first byte means there is never anything to
 * migrate and never a boundary inside the file.
 *
 * A file that ALREADY has bytes is therefore left alone permanently: it stays
 * metadata-server-resident and LAYOUTGET declines with LAYOUTUNAVAILABLE, which
 * is a legal answer clients handle by falling back to MDS I/O.  Migrating such
 * a file is a separate feature, not a correctness requirement.
 */
static void
chimera_vfs_pnfs_io_setattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_pnfs_io_ctx *ctx     = private_data;
    struct chimera_vfs_open_handle *backing = ctx->backing_handle;

    if (error_code != CHIMERA_VFS_OK) {
        if (getenv("CHIMERA_PNFS_IO_TRACE")) {
            fprintf(stderr, "PNFSIO: MATERIALIZE FAIL setattr err=%d\n", error_code);
        }
        /* The backing file exists but nothing points at it.  Serve the I/O
         * locally rather than failing it; the file simply stays MDS-resident
         * and the orphan is inert (a later attempt makes its own). */
        chimera_vfs_release(ctx->thread, backing);
        chimera_vfs_pnfs_io_done(ctx, CHIMERA_VFS_OK, ctx->mds_handle, 0);
        return;
    }

    chimera_vfs_pnfs_io_done(ctx, CHIMERA_VFS_OK, backing, 1);
} /* chimera_vfs_pnfs_io_setattr_cb */

static void
chimera_vfs_pnfs_io_blob_setattr(
    struct chimera_vfs_pnfs_io_ctx *ctx,
    const uint8_t                  *backing_fh,
    uint32_t                        backing_fh_len);

/* The leftover backing file has been emptied; record the blob that adopts it. */
static void
chimera_vfs_pnfs_io_reset_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_pnfs_io_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        /* Could not empty it, so it cannot safely become this file's data. */
        chimera_vfs_release(ctx->thread, ctx->backing_handle);
        ctx->backing_handle = NULL;
        chimera_vfs_pnfs_io_done(ctx, CHIMERA_VFS_OK, ctx->mds_handle, 0);
        return;
    }

    chimera_vfs_pnfs_io_blob_setattr(ctx, ctx->backing_fh, ctx->backing_fh_len);
} /* chimera_vfs_pnfs_io_reset_cb */

static void
chimera_vfs_pnfs_io_create_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_vfs_pnfs_io_ctx *ctx = private_data;

    chimera_vfs_release(ctx->thread, ctx->ds_root_handle);
    ctx->ds_root_handle = NULL;

    if (error_code != CHIMERA_VFS_OK || !oh ||
        !(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        if (getenv("CHIMERA_PNFS_IO_TRACE")) {
            fprintf(stderr, "PNFSIO: MATERIALIZE FAIL create err=%d oh=%d fh=%d\n",
                    error_code, !!oh,
                    attr ? !!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH) : -1);
        }
        chimera_vfs_pnfs_io_done(ctx, CHIMERA_VFS_OK, ctx->mds_handle, 0);
        return;
    }

    ctx->backing_handle = oh;

    /* A backing file that was NOT created by this open is a leftover: the name
     * is the metadata server's fileid, and a fileid is only unique among LIVE
     * inodes, so it is reused once the original is gone.  Its bytes belong to a
     * dead file and must not become this one's.
     *
     * CHIMERA_VFS_OPEN_TRUNCATE is passed above and handles this for a local
     * data server, but the nfs module -- how a REMOTE data server is reached --
     * ignores the flag entirely, so the truncate is issued explicitly here. */
    if (!oh->r_created) {
        memcpy(ctx->backing_fh, attr->va_fh, attr->va_fh_len);
        ctx->backing_fh_len = attr->va_fh_len;

        memset(&ctx->attr, 0, sizeof(ctx->attr));
        ctx->attr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
        ctx->attr.va_size     = 0;

        chimera_vfs_setattr(ctx->thread, ctx->cred, oh, &ctx->attr, 0, 0,
                            chimera_vfs_pnfs_io_reset_cb, ctx);
        return;
    }

    chimera_vfs_pnfs_io_blob_setattr(ctx, attr->va_fh, attr->va_fh_len);
} /* chimera_vfs_pnfs_io_create_cb */

static void
chimera_vfs_pnfs_io_blob_setattr(
    struct chimera_vfs_pnfs_io_ctx *ctx,
    const uint8_t                  *backing_fh,
    uint32_t                        backing_fh_len)
{
    memset(&ctx->attr, 0, sizeof(ctx->attr));
    ctx->attr.va_set_mask = CHIMERA_VFS_ATTR_PNFS_LAYOUT;
    ctx->attr.va_pnfs_len = chimera_vfs_pnfs_blob_pack(ctx->attr.va_pnfs,
                                                       ctx->ds->deviceid,
                                                       backing_fh,
                                                       backing_fh_len);

    chimera_vfs_setattr(ctx->thread, ctx->cred, ctx->mds_handle, &ctx->attr,
                        0, 0, chimera_vfs_pnfs_io_setattr_cb, ctx);
} /* chimera_vfs_pnfs_io_blob_setattr */

static void
chimera_vfs_pnfs_io_dsroot_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct chimera_vfs_pnfs_io_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        if (getenv("CHIMERA_PNFS_IO_TRACE")) {
            fprintf(stderr, "PNFSIO: MATERIALIZE FAIL dsroot err=%d\n", error_code);
        }
        chimera_vfs_pnfs_io_done(ctx, CHIMERA_VFS_OK, ctx->mds_handle, 0);
        return;
    }

    ctx->ds_root_handle = handle;

    /* One backing file per MDS file, flat on the DS.  Shared with the LAYOUTGET
     * path, which must resolve the very same file. */
    chimera_vfs_pnfs_backing_name(ctx->backing_name, ctx->mds_handle->fh,
                                  ctx->fileid);

    /* TRUNCATE, not just CREATE: backing files are named by the metadata
     * server's fileid, and a fileid is only unique among LIVE inodes -- it is
     * reused once the original is gone.  Opening such a name without truncating
     * adopts the previous occupant's bytes, so a brand-new empty file would
     * start life holding a dead file's data.
     *
     * World-rw for the same reason LAYOUTGET creates them so: flex-files steers
     * data-server I/O with synthetic per-iomode principals, and a co-located
     * data server enforces permissions against them.  Real access control is
     * the caller's authorization against the MDS file, which has already
     * happened by the time a write reaches here. */
    memset(&ctx->attr, 0, sizeof(ctx->attr));
    ctx->attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    ctx->attr.va_mode     = S_IFREG | 0666;

    if (getenv("CHIMERA_PNFS_IO_TRACE")) {
        fprintf(stderr, "PNFSIO: materialize mds_fh=%016llx fileid=%llu name=%s\n",
                (unsigned long long) ctx->mds_handle->fh_hash,
                (unsigned long long) ctx->fileid, ctx->backing_name);
    }

    chimera_vfs_open_at(ctx->thread, ctx->cred, ctx->ds_root_handle,
                        ctx->backing_name, strlen(ctx->backing_name),
                        CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_TRUNCATE |
                        CHIMERA_VFS_OPEN_INFERRED,
                        &ctx->attr, CHIMERA_VFS_ATTR_FH, 0, 0,
                        chimera_vfs_pnfs_io_create_cb, ctx);
} /* chimera_vfs_pnfs_io_dsroot_cb */

static void
chimera_vfs_pnfs_io_materialize(struct chimera_vfs_pnfs_io_ctx *ctx)
{
    ctx->ds = chimera_vfs_pnfs_steer(ctx->thread->vfs);

    if (!ctx->ds) {
        if (getenv("CHIMERA_PNFS_IO_TRACE")) {
            fprintf(stderr, "PNFSIO: MATERIALIZE FAIL no-ds\n");
        }
        /* No data server ready: stay MDS-resident.  Legal and self-consistent,
         * since LAYOUTGET will decline for the same reason. */
        chimera_vfs_pnfs_io_done(ctx, CHIMERA_VFS_OK, ctx->mds_handle, 0);
        return;
    }

    chimera_vfs_open_fh(ctx->thread, ctx->cred,
                        ctx->ds->root_fh, ctx->ds->root_fh_len,
                        CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_INFERRED,
                        chimera_vfs_pnfs_io_dsroot_cb, ctx);
} /* chimera_vfs_pnfs_io_materialize */

static void
chimera_vfs_pnfs_io_getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_pnfs_io_ctx *ctx = private_data;
    const uint8_t                  *backing_fh;
    uint32_t                        backing_fh_len;
    uint64_t                        size;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_pnfs_io_done(ctx, error_code, NULL, 0);
        return;
    }

    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_PNFS_LAYOUT) ||
        chimera_vfs_pnfs_blob_unpack(attr->va_pnfs, attr->va_pnfs_len,
                                     NULL, &backing_fh, &backing_fh_len) != 0) {
        size = (attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) ? attr->va_size : 0;

        /* No layout yet.  A write to a still-empty file is the moment to give
         * it one; anything else leaves it where it is -- a read has nothing to
         * redirect to, and a file that already holds bytes must not acquire a
         * backing file that would strand them. */
        if (getenv("CHIMERA_PNFS_IO_TRACE")) {
            fprintf(stderr, "PNFSIO: noblob w=%d size=%llu inum=%d\n",
                    ctx->for_write, (unsigned long long) size,
                    !!(attr->va_set_mask & CHIMERA_VFS_ATTR_INUM));
        }

        if (ctx->for_write && size == 0 &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_INUM)) {
            ctx->fileid = attr->va_ino;
            chimera_vfs_pnfs_io_materialize(ctx);
            return;
        }

        chimera_vfs_pnfs_io_done(ctx, CHIMERA_VFS_OK, ctx->mds_handle, 0);
        return;
    }

    /* The blob stores the backing handle as the MDS holds it, so it opens
     * directly -- through the nfs module for a remote data server, or in the
     * local backing mount for a co-located one.  Deriving the CLIENT-facing
     * handle from it is the NFS server's job and is a different transform. */
    chimera_vfs_open_fh(ctx->thread, ctx->cred, backing_fh, backing_fh_len,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_vfs_pnfs_io_open_cb, ctx);
} /* chimera_vfs_pnfs_io_getattr_cb */

/*
 * Point an allocated-but-not-yet-dispatched request at a different file.
 *
 * The routing fields are stamped from the caller's handle when the request is
 * allocated, so a redirect has to restamp all of them: the module and its mount
 * private decide which backend runs the op, and the handle decides which
 * delegation thread it runs on.  Safe only before chimera_vfs_dispatch, which
 * is the only place any of them is read.
 *
 * request->io_handle is left pointing at the file the CALLER named, because
 * that is the identity leases and oplocks arbitrate on (chimera_vfs_io_claim_key).
 */
static void
chimera_vfs_pnfs_request_retarget(
    struct chimera_vfs_request     *request,
    struct chimera_vfs_open_handle *backing)
{
    void *mount_private = NULL;

    request->module = chimera_vfs_resolve_mount(request->thread, backing->fh,
                                                backing->fh_len, 0,
                                                &mount_private);
    request->mount_private = mount_private;

    memcpy(request->fh, backing->fh, backing->fh_len);
    request->fh_len  = backing->fh_len;
    request->fh_hash = backing->fh_hash;

    request->io_pnfs_backing = backing;

    switch (request->opcode) {
        case CHIMERA_VFS_OP_SEEK:
            request->seek.handle = backing;
            break;
        case CHIMERA_VFS_OP_COMMIT:
            request->commit.handle = backing;
            break;
        case CHIMERA_VFS_OP_ALLOCATE:
            request->allocate.handle = backing;
            break;
        case CHIMERA_VFS_OP_READ_PLUS:
            request->read_plus.handle = backing;
            break;
        case CHIMERA_VFS_OP_WRITE_SAME:
            request->write_same.handle = backing;
            break;
        default:
            chimera_vfs_abort("pnfs retarget: opcode %d has no data handle",
                              request->opcode);
    }
} /* chimera_vfs_pnfs_request_retarget */

static struct chimera_vfs_open_handle *
chimera_vfs_pnfs_request_handle(struct chimera_vfs_request *request)
{
    switch (request->opcode) {
        case CHIMERA_VFS_OP_SEEK:       return request->seek.handle;
        case CHIMERA_VFS_OP_COMMIT:     return request->commit.handle;
        case CHIMERA_VFS_OP_ALLOCATE:   return request->allocate.handle;
        case CHIMERA_VFS_OP_READ_PLUS:  return request->read_plus.handle;
        case CHIMERA_VFS_OP_WRITE_SAME: return request->write_same.handle;
        default:                        return NULL;
    }
} /* chimera_vfs_pnfs_request_handle */

static void
chimera_vfs_pnfs_dispatch_resolved(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *io_handle,
    int                             redirected,
    void                           *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->status = error_code;
        request->complete(request);
        return;
    }

    if (redirected) {
        chimera_vfs_pnfs_request_retarget(request, io_handle);

        /* The capability that gated this op was checked against the metadata
         * server's backend, but the op will now run on the data server's, which
         * may not implement it.  Re-check and surface ENOTSUP so the protocol
         * layer falls back exactly as it would for a backend that never had it. */
        if (request->pnfs_required_cap &&
            !(request->module->capabilities & request->pnfs_required_cap)) {
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            return;
        }
    }

    chimera_vfs_dispatch(request);
} /* chimera_vfs_pnfs_dispatch_resolved */

/*
 * Dispatch a data operation to wherever this file's bytes live.  Drop-in
 * replacement for chimera_vfs_dispatch() in any op whose work is confined to
 * one file's data; `required_cap` is the capability the caller already checked
 * against the metadata server, re-checked after a redirect.
 */
SYMBOL_EXPORT void
chimera_vfs_pnfs_dispatch(
    struct chimera_vfs_request *request,
    int                         for_write,
    uint64_t                    required_cap)
{
    struct chimera_vfs_open_handle *handle =
        chimera_vfs_pnfs_request_handle(request);

    request->pnfs_required_cap = required_cap;

    if (!handle ||
        !chimera_vfs_pnfs_io_possible(request->thread, handle)) {
        chimera_vfs_dispatch(request);
        return;
    }

    request->io_handle = handle;

    chimera_vfs_pnfs_resolve_io(request->thread, request->cred, handle,
                                for_write, chimera_vfs_pnfs_dispatch_resolved,
                                request);
} /* chimera_vfs_pnfs_dispatch */

/* Completion of the MDS size/mtime sync: hand the MDS's own attributes back as
 * the op's post-attrs, since those are what the caller asked about and what a
 * following GETATTR will return, then resume the op's own completion. */
static void
chimera_vfs_pnfs_sync_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_request      *request = private_data;
    chimera_vfs_pnfs_sync_callback_t next    = request->pnfs_sync_next;

    if (error_code == CHIMERA_VFS_OK) {
        request->io_pnfs_sync_attr = *post_attr;
    } else {
        request->status = error_code;
    }

    next(request);
} /* chimera_vfs_pnfs_sync_cb */

/* The backing file's backend returned no post-op size, so fetch it before
 * publishing it on the metadata server. */
static void
chimera_vfs_pnfs_sync_size_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_attrs   *sync    = &request->io_pnfs_sync_attr;

    if (error_code != CHIMERA_VFS_OK ||
        !(attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE)) {
        /* Nothing trustworthy to publish; leave the metadata server's size
         * alone rather than move it to a value we are guessing at. */
        request->pnfs_sync_next(request);
        return;
    }

    sync->va_req_mask = 0;
    sync->va_set_mask = CHIMERA_VFS_ATTR_SIZE;
    sync->va_size     = attr->va_size;

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME) {
        sync->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;
        sync->va_mtime     = attr->va_mtime;
    }

    if (getenv("CHIMERA_PNFS_IO_TRACE")) {
        fprintf(stderr, "PNFSIO: sync(fetched) op=%d fh=%016llx -> size=%llu\n",
                request->opcode,
                (unsigned long long) request->io_handle->fh_hash,
                (unsigned long long) sync->va_size);
    }

    chimera_vfs_setattr_nopnfs(request->thread, request->cred,
                               request->io_handle, sync, 0,
                               CHIMERA_VFS_ATTR_MASK_STAT,
                               chimera_vfs_pnfs_sync_cb, request);
} /* chimera_vfs_pnfs_sync_size_cb */

/*
 * Push the size and mtime a redirected op produced on the backing file back
 * onto the metadata server's inode, then continue to `next`.
 *
 * Every protocol stats the MDS inode, and the MDS backend saw none of the bytes
 * that just landed on the data server, so without this an SMB or S3 client
 * writes a file and then reads its length as zero.  Shared by every op that can
 * extend a file (write, write_same, allocate).
 *
 * This covers the MDS path only.  Bytes a pNFS client writes DIRECTLY to a data
 * server through a layout stay invisible to the MDS until the client sends
 * LAYOUTCOMMIT; nothing can tell us sooner, since CB_GETATTR is defined only for
 * write delegations and never for layouts.  So the MDS size is exact for
 * everything written through the MDS and lags only for layout-direct writes.
 */
SYMBOL_EXPORT void
chimera_vfs_pnfs_sync_mds(
    struct chimera_vfs_request     *request,
    const struct chimera_vfs_attrs *backing_post,
    uint64_t                        end_offset,
    chimera_vfs_pnfs_sync_callback_t next)
{
    struct chimera_vfs_attrs *sync = &request->io_pnfs_sync_attr;

    if (!request->io_pnfs_backing || request->status != CHIMERA_VFS_OK ||
        !request->io_handle) {
        next(request);
        return;
    }

    request->pnfs_sync_next = next;

    /* The size must come from the backing file itself, never from this
     * operation's own end offset.  A write that fills an earlier hole ends
     * below the file's true length, and setattr sets the size absolutely, so
     * using the end offset makes the metadata server's size go BACKWARDS on
     * every overwrite -- the file appears to shrink whenever a client rewrites
     * something it already wrote.
     *
     * Backends that return post-op attributes give us the length for free.  The
     * NFS proxy, which is how a remote data server is reached, does not, so ask
     * the backing file directly rather than guessing. */
    if (!backing_post ||
        !(backing_post->va_set_mask & CHIMERA_VFS_ATTR_SIZE)) {
        chimera_vfs_getattr(request->thread, request->cred,
                            request->io_pnfs_backing,
                            /* PNFS_LAYOUT is requested purely to force the
                             * attr cache to be bypassed: SIZE and MTIME are
                             * both cacheable, so on their own this fetch can be
                             * answered from a cached entry that predates the
                             * write (or a truncate) we are trying to publish --
                             * and we would then push a stale size onto the
                             * metadata server.  vfs_proc_getattr only consults
                             * the cache when every requested attribute is
                             * cacheable, and PNFS_LAYOUT is not. */
                            CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_MTIME |
                            CHIMERA_VFS_ATTR_PNFS_LAYOUT,
                            chimera_vfs_pnfs_sync_size_cb, request);
        return;
    }

    sync->va_req_mask = 0;
    sync->va_set_mask = CHIMERA_VFS_ATTR_SIZE;
    sync->va_size     = backing_post->va_size;

    if (backing_post->va_set_mask & CHIMERA_VFS_ATTR_MTIME) {
        sync->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;
        sync->va_mtime     = backing_post->va_mtime;
    }

    if (getenv("CHIMERA_PNFS_IO_TRACE")) {
        fprintf(stderr, "PNFSIO: sync fh=%016llx set=%llu backing_has=%d end=%llu\n",
                (unsigned long long) request->io_handle->fh_hash,
                (unsigned long long) sync->va_size,
                !!(backing_post &&
                   (backing_post->va_set_mask & CHIMERA_VFS_ATTR_SIZE)),
                (unsigned long long) end_offset);
    }

    chimera_vfs_setattr_nopnfs(request->thread, request->cred,
                               request->io_handle, sync, 0,
                               CHIMERA_VFS_ATTR_MASK_STAT,
                               chimera_vfs_pnfs_sync_cb, request);
} /* chimera_vfs_pnfs_sync_mds */

SYMBOL_EXPORT int
chimera_vfs_pnfs_io_possible(
    struct chimera_vfs_thread            *thread,
    const struct chimera_vfs_open_handle *handle)
{
    return chimera_vfs_pnfs_enabled(thread->vfs) &&
           !chimera_vfs_pnfs_fh_is_ds_backing(thread->vfs, handle->fh,
                                              handle->fh_len);
} /* chimera_vfs_pnfs_io_possible */

SYMBOL_EXPORT void
chimera_vfs_pnfs_resolve_io(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    int                             for_write,
    chimera_vfs_pnfs_io_callback_t  callback,
    void                           *private_data)
{
    struct chimera_vfs_pnfs_io_ctx *ctx;

    /* Fast path.  No data servers configured means no file can be DS-resident,
     * and nothing under a data server's own backing mount ever is (that mount
     * IS the storage -- redirecting there would re-enter this path). */
    if (!chimera_vfs_pnfs_enabled(thread->vfs) ||
        chimera_vfs_pnfs_fh_is_ds_backing(thread->vfs, handle->fh,
                                          handle->fh_len)) {
        callback(CHIMERA_VFS_OK, handle, 0, private_data);
        return;
    }

    ctx = chimera_vfs_gate_scratch_alloc(thread);

    ctx->thread         = thread;
    ctx->cred           = cred;
    ctx->mds_handle     = handle;
    ctx->callback       = callback;
    ctx->private_data   = private_data;
    ctx->for_write      = for_write ? 1 : 0;
    ctx->ds             = NULL;
    ctx->ds_root_handle = NULL;
    ctx->backing_handle = NULL;

    /* SIZE and INUM ride along with the blob probe: a write that finds no
     * layout needs both to decide whether it may materialize one (empty only)
     * and to name the backing file.  One getattr either way. */
    chimera_vfs_getattr(thread, cred, handle,
                        CHIMERA_VFS_ATTR_PNFS_LAYOUT |
                        CHIMERA_VFS_ATTR_MASK_STAT,
                        chimera_vfs_pnfs_io_getattr_cb, ctx);
} /* chimera_vfs_pnfs_resolve_io */
