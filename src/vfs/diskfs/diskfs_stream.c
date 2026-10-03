// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Named data streams.  Each stream's data lives in a hidden regular inode of
 * its own; the file names it with a DISKFS_REC_STREAM record in its b+tree,
 * keyed by a hash of the case-folded stream name.  A stream handle is the
 * file's handle with the stream inode's inum and generation appended, so the
 * metadata operations through it act on the file and the data operations on
 * the stream (diskfs_inode_get_data_fh_async).  The stream inode holds one
 * link, the record's; removing the record orphans it, and a file reclaimed
 * with streams still recorded has them orphaned by its drain.
 */

#include <ctype.h>
#include "diskfs_internal.h"

struct diskfs_stream_list {
    uint64_t inum;
    uint32_t gen;
    uint64_t seq;
    uint16_t name_len;
    char     name[256];
};

static uint64_t
diskfs_stream_name_hash(
    const char *name,
    uint32_t    name_len)
{
    char     folded[256];
    uint32_t i;

    for (i = 0; i < name_len && i < sizeof(folded); i++) {
        folded[i] = (char) tolower((unsigned char) name[i]);
    }
    return chimera_vfs_hash(folded, i);
} /* diskfs_stream_name_hash */


/* Stream names compare case-insensitively, as NTFS's do. */
static int
diskfs_stream_rec_matches(
    const struct diskfs_stream_rec *rec,
    int                             rec_len,
    const char                     *name,
    uint32_t                        name_len)
{
    uint32_t i;

    if (rec_len < (int) sizeof(*rec) || rec->name_len != name_len ||
        rec_len < (int) (sizeof(*rec) + rec->name_len)) {
        return 0;
    }
    for (i = 0; i < name_len; i++) {
        if (tolower((unsigned char) rec->name[i]) != tolower((unsigned char) name[i])) {
            return 0;
        }
    }
    return 1;
} /* diskfs_stream_rec_matches */


static inline struct diskfs_bt_key
diskfs_stream_key(uint64_t hash)
{
    struct diskfs_bt_key key = { .type = DISKFS_REC_STREAM, .subkey = hash };

    return key;
} /* diskfs_stream_key */


/* Look up the record of the stream whose name hashes to hash into
 * p->rec_scratch; cb sees the record's length, or < 0 if there is none. */
static void
diskfs_stream_lookup(
    struct chimera_vfs_request *request,
    struct diskfs_inode        *base,
    uint64_t                    hash,
    diskfs_bt_cb_t              cb)
{
    struct diskfs_request_private *p   = request->plugin_data;
    struct diskfs_bt_key           key = diskfs_stream_key(hash);
    struct diskfs_bt_op           *op  = diskfs_bt_op_alloc(p->thread);

    if (diskfs_bt_lookup_async(op, p->thread, base, DISKFS_BT_OP_LOOKUP_EXACT, &key,
                               NULL, p->rec_scratch, sizeof(p->rec_scratch), cb, request)) {
        cb(op, op->result, request);
    }
} /* diskfs_stream_lookup */


/* Write the stream record for (p->stream_inum, p->stream_gen, p->stream_seq)
 * under hash. */
static void
diskfs_stream_insert(
    struct chimera_vfs_request *request,
    struct diskfs_inode        *base,
    uint64_t                    hash,
    const char                 *name,
    uint32_t                    name_len,
    diskfs_bt_cb_t              cb)
{
    struct diskfs_request_private *p   = request->plugin_data;
    struct diskfs_stream_rec      *rec = (struct diskfs_stream_rec *) p->rec_scratch;
    struct diskfs_bt_key           key = diskfs_stream_key(hash);
    struct diskfs_bt_op           *op  = diskfs_bt_op_alloc(p->thread);

    rec->inum     = p->stream_inum;
    rec->gen      = p->stream_gen;
    rec->seq      = p->stream_seq;
    rec->name_len = name_len;
    memcpy(rec->name, name, name_len);

    if (diskfs_bt_insert_async(op, p->thread, p->txn, base, &key, rec,
                               sizeof(*rec) + name_len, cb, request)) {
        cb(op, op->result, request);
    }
} /* diskfs_stream_insert */


static void
diskfs_stream_remove(
    struct chimera_vfs_request *request,
    struct diskfs_inode        *base,
    uint64_t                    hash,
    diskfs_bt_cb_t              cb)
{
    struct diskfs_request_private *p   = request->plugin_data;
    struct diskfs_bt_key           key = diskfs_stream_key(hash);
    struct diskfs_bt_op           *op  = diskfs_bt_op_alloc(p->thread);

    if (diskfs_bt_remove_async(op, p->thread, p->txn, base, &key, cb, request)) {
        cb(op, op->result, request);
    }
} /* diskfs_stream_remove */


static inline void
diskfs_stream_stamp(
    struct diskfs_inode *inode,
    int                  mtime)
{
    struct timespec now;

    clock_gettime(CLOCK_REALTIME, &now);
    if (mtime) {
        inode->mtime_sec  = now.tv_sec;
        inode->mtime_nsec = now.tv_nsec;
    }
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;
    inode->change++;
} /* diskfs_stream_stamp */


/* ------------------------------------------------------------------ */
/* OPEN_STREAM                                                         */
/* ------------------------------------------------------------------ */

/* inode_stash[0] = base file */

static void
diskfs_open_stream_finish(
    struct chimera_vfs_request *request,
    struct diskfs_inode        *stream)
{
    struct diskfs_request_private *p    = request->plugin_data;
    struct diskfs_inode           *base = p->inode_stash[0];
    uint8_t                        sfh[CHIMERA_VFS_FH_SIZE];
    uint32_t                       sfh_len;

    sfh_len = diskfs_stream_to_fh(p->fs, sfh, base->inum, base->gen,
                                  stream->inum, stream->gen);

    /* The open handle pins the stream inode; the data operations and the
     * close work off it. */
    diskfs_inode_ref_get(p->thread, stream);
    request->open_stream.r_vfs_private = (uint64_t) stream;

    diskfs_map_attrs(p->thread, &request->open_stream.r_attr, base);
    diskfs_stream_overlay_attrs(&request->open_stream.r_attr, stream, sfh, sfh_len);

    diskfs_op_ok(request, p->txn);
} /* diskfs_open_stream_finish */


static void
diskfs_open_stream_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_open_stream_finish(request, p->inode_stash[1]);
} /* diskfs_open_stream_inserted_cb */


static void
diskfs_open_stream_alloc_cb(
    struct diskfs_inode *stream,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *base    = p->inode_stash[0];
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    stream->size           = 0;
    stream->space_used     = 0;
    stream->alloc_size     = 0;
    stream->uid            = base->uid;
    stream->gid            = base->gid;
    stream->nlink          = 1;          /* the stream record's */
    stream->rdev           = 0;
    stream->mode           = S_IFREG | (base->mode & 07777);
    stream->atime_sec      = now.tv_sec;
    stream->atime_nsec     = now.tv_nsec;
    stream->mtime_sec      = now.tv_sec;
    stream->mtime_nsec     = now.tv_nsec;
    stream->ctime_sec      = now.tv_sec;
    stream->ctime_nsec     = now.tv_nsec;
    stream->btime_sec      = now.tv_sec;
    stream->btime_nsec     = now.tv_nsec;
    stream->dos_attributes = 0;
    stream->change++;

    /* A stream shares its file's metadata: the attributes a create asks for
     * (its FileAttributes and times) land on the file, as a regular create
     * stamps them (smb2.streams.attributes2). */
    if (request->open_stream.set_attr) {
        struct chimera_vfs_attrs attrs = *request->open_stream.set_attr;

        attrs.va_set_mask &= CHIMERA_VFS_ATTR_DOS_ATTRIBUTES | CHIMERA_VFS_ATTR_ATIME |
            CHIMERA_VFS_ATTR_MTIME | CHIMERA_VFS_ATTR_BTIME;
        diskfs_apply_attrs(base, &attrs);
    }
    diskfs_stream_stamp(base, 1);

    p->inode_stash[1]              = stream;
    p->stream_inum                 = stream->inum;
    p->stream_gen                  = stream->gen;
    p->stream_seq                  = (uint64_t) now.tv_sec * 1000000000ULL + (uint64_t) now.tv_nsec;
    request->open_stream.r_created = 1;

    diskfs_stream_insert(request, base, p->stream_hash, request->open_stream.name,
                         request->open_stream.namelen, diskfs_open_stream_inserted_cb);
} /* diskfs_open_stream_alloc_cb */


static void
diskfs_open_stream_existing_cb(
    struct diskfs_inode *stream,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ESTALE);
        return;
    }

    /* SMB replaces a stream's content through the stream handle once the open
     * is granted; an open that asks for it here is honoured only while there
     * is nothing to drop. */
    if ((request->open_stream.flags & CHIMERA_VFS_OPEN_TRUNCATE) && stream->size) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTSUP);
        return;
    }

    diskfs_open_stream_finish(request, stream);
} /* diskfs_open_stream_existing_cb */


static void
diskfs_open_stream_lookup_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_stream_rec      *rec     = (struct diskfs_stream_rec *) p->rec_scratch;
    unsigned int                   flags   = request->open_stream.flags;

    diskfs_bt_op_free(p->thread, op);

    if (result >= 0) {
        if (!diskfs_stream_rec_matches(rec, result, request->open_stream.name,
                                       request->open_stream.namelen)) {
            /* Another stream's name hashes alike: this one cannot be made. */
            diskfs_op_fail(request, p->txn, (flags & CHIMERA_VFS_OPEN_CREATE) ?
                           CHIMERA_VFS_EEXIST : CHIMERA_VFS_ENOENT);
            return;
        }
        if (flags & CHIMERA_VFS_OPEN_EXCLUSIVE) {
            diskfs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
            return;
        }
        diskfs_inode_get_inum_async(p->thread, p->txn, p->fs, rec->inum, rec->gen,
                                    diskfs_open_stream_existing_cb, request);
        return;
    }

    if (!(flags & CHIMERA_VFS_OPEN_CREATE)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    diskfs_inode_alloc_async(p->thread, p->txn, p->fs, diskfs_open_stream_alloc_cb, request);
} /* diskfs_open_stream_lookup_cb */


static void
diskfs_open_stream_base_cb(
    struct diskfs_inode *base,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    /* Files and directories carry streams; links and special files do not. */
    if (!S_ISREG(base->mode) && !S_ISDIR(base->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EINVAL);
        return;
    }

    p->inode_stash[0] = base;
    diskfs_stream_lookup(request, base, p->stream_hash, diskfs_open_stream_lookup_cb);
} /* diskfs_open_stream_base_cb */


void
diskfs_open_stream(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    if (request->open_stream.namelen == 0 || request->open_stream.namelen > 255) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    p->thread      = thread;
    p->stream_hash = diskfs_stream_name_hash(request->open_stream.name,
                                             request->open_stream.namelen);
    p->txn = diskfs_txn_begin(thread, (request->open_stream.flags & CHIMERA_VFS_OPEN_CREATE) ?
                              DISKFS_TXN_WRITE : DISKFS_TXN_READ);

    diskfs_inode_get_fh_async(thread, p->txn, p->fs, request->fh, request->fh_len,
                              diskfs_open_stream_base_cb, request);
} /* diskfs_open_stream */


/* ------------------------------------------------------------------ */
/* LIST_STREAMS                                                        */
/* ------------------------------------------------------------------ */

/* inode_stash[0] = base file; loop_pos = bytes emitted, loop_left = entries
 * emitted, loop_off = next subkey to collect / next listed stream to emit. */

static int
diskfs_stream_list_cmp(
    const void *a,
    const void *b)
{
    const struct diskfs_stream_list *x = a, *y = b;

    return x->seq < y->seq ? -1 : x->seq > y->seq ? 1 : 0;
} /* diskfs_stream_list_cmp */


static void
diskfs_list_streams_fail(
    struct chimera_vfs_request *request,
    int                         status)
{
    struct diskfs_request_private *p = request->plugin_data;

    free(p->stream_list);
    p->stream_list = NULL;
    diskfs_op_fail(request, p->txn, status);
} /* diskfs_list_streams_fail */


/* Append one entry to the caller's buffer; returns 0, or -1 if it does not
 * fit. */
static int
diskfs_list_streams_emit(
    struct chimera_vfs_request *request,
    uint64_t                    size,
    uint64_t                    alloc,
    const char                 *name,
    uint16_t                    name_len,
    const uint8_t              *fh,
    uint16_t                    fh_len)
{
    struct diskfs_request_private  *p   = request->plugin_data;
    uint8_t                        *buf = request->list_streams.buffer;
    struct chimera_vfs_stream_entry entry;
    uint32_t                        rec = sizeof(entry) + name_len + fh_len;

    if (p->loop_pos + rec > request->list_streams.max_bytes) {
        return -1;
    }

    entry.size     = size;
    entry.alloc    = alloc;
    entry.name_len = name_len;
    entry.fh_len   = fh_len;
    memcpy(buf + p->loop_pos, &entry, sizeof(entry));
    memcpy(buf + p->loop_pos + sizeof(entry), name, name_len);
    memcpy(buf + p->loop_pos + sizeof(entry) + name_len, fh, fh_len);
    p->loop_pos = (p->loop_pos + rec + 7) & ~7ULL;
    p->loop_left++;
    return 0;
} /* diskfs_list_streams_emit */


static void diskfs_list_streams_next(
    struct chimera_vfs_request *request);


static void
diskfs_list_streams_stream_cb(
    struct diskfs_inode *stream,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *base    = p->inode_stash[0];
    struct diskfs_stream_list     *e       = &p->stream_list[p->loop_off];
    uint8_t                        fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                       fh_len = 0;
    int                            rc;

    if (status == CHIMERA_VFS_OK) {
        if (request->list_streams.want_fh) {
            fh_len = diskfs_stream_to_fh(p->fs, fh, base->inum, base->gen,
                                         stream->inum, stream->gen);
        }
        rc = diskfs_list_streams_emit(request, stream->size,
                                      stream->space_used > stream->alloc_size ?
                                      stream->space_used : stream->alloc_size,
                                      e->name, e->name_len, fh, fh_len);
        /* Done with it: a file with many streams must not hold them all. */
        diskfs_txn_unlock_inode(p->txn, stream);
        if (rc) {
            diskfs_list_streams_fail(request, CHIMERA_VFS_ERANGE);
            return;
        }
    }

    p->loop_off++;
    diskfs_list_streams_next(request);
} /* diskfs_list_streams_stream_cb */


/* Emit the collected streams, in the order they were created. */
static void
diskfs_list_streams_next(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p = request->plugin_data;
    struct diskfs_stream_list     *e;

    if (p->loop_off >= p->stream_count) {
        free(p->stream_list);
        p->stream_list                 = NULL;
        request->list_streams.r_len    = (uint32_t) p->loop_pos;
        request->list_streams.r_count  = (uint32_t) p->loop_left;
        request->list_streams.r_eof    = 1;
        request->list_streams.r_cookie = 0;
        diskfs_op_ok(request, p->txn);
        return;
    }

    e = &p->stream_list[p->loop_off];
    diskfs_inode_get_inum_async(p->thread, p->txn, p->fs, e->inum, e->gen,
                                diskfs_list_streams_stream_cb, request);
} /* diskfs_list_streams_next */


static void diskfs_list_streams_collect(
    struct chimera_vfs_request *request);


/* Consume one collected record; returns 1 once the collection is done. */
static int
diskfs_list_streams_consume(
    struct chimera_vfs_request *request,
    struct diskfs_bt_op        *op,
    int                         result)
{
    struct diskfs_request_private *p     = request->plugin_data;
    struct diskfs_stream_rec      *rec   = (struct diskfs_stream_rec *) p->rec_scratch;
    struct diskfs_bt_key           found = op->found_key;
    struct diskfs_stream_list     *e;

    diskfs_bt_op_free(p->thread, op);

    if (result < 0 || found.type != DISKFS_REC_STREAM) {
        return 1;
    }

    if (result >= (int) sizeof(*rec) && rec->name_len <= 255 &&
        result >= (int) (sizeof(*rec) + rec->name_len)) {
        if (p->stream_count == p->stream_cap) {
            struct diskfs_stream_list *grown;

            p->stream_cap = p->stream_cap ? p->stream_cap * 2 : 8;
            grown         = realloc(p->stream_list, p->stream_cap * sizeof(*grown));
            chimera_diskfs_abort_if(!grown, "Out of memory listing streams");
            p->stream_list = grown;
        }
        e           = &p->stream_list[p->stream_count++];
        e->inum     = rec->inum;
        e->gen      = rec->gen;
        e->seq      = rec->seq;
        e->name_len = rec->name_len;
        memcpy(e->name, rec->name, rec->name_len);
    }

    if (found.subkey == UINT64_MAX) {
        return 1;
    }
    p->loop_off = found.subkey + 1;
    return 0;
} /* diskfs_list_streams_consume */


static void
diskfs_list_streams_collected(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p = request->plugin_data;

    if (p->stream_count > 1) {
        qsort(p->stream_list, p->stream_count, sizeof(*p->stream_list),
              diskfs_stream_list_cmp);
    }
    p->loop_off = 0;
    diskfs_list_streams_next(request);
} /* diskfs_list_streams_collected */


static void
diskfs_list_streams_lookup_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (diskfs_list_streams_consume(request, op, result)) {
        diskfs_list_streams_collected(request);
        return;
    }
    diskfs_list_streams_collect(request);
} /* diskfs_list_streams_lookup_cb */


static void
diskfs_list_streams_collect(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p = request->plugin_data;

    for (;;) {
        struct diskfs_bt_key key = diskfs_stream_key(p->loop_off);
        struct diskfs_bt_op *op  = diskfs_bt_op_alloc(p->thread);

        if (!diskfs_bt_lookup_async(op, p->thread, p->inode_stash[0],
                                    DISKFS_BT_OP_LOOKUP_GE, &key, &op->found_key,
                                    p->rec_scratch, sizeof(p->rec_scratch),
                                    diskfs_list_streams_lookup_cb, request)) {
            return;     /* suspended; the callback resumes the walk */
        }
        if (diskfs_list_streams_consume(request, op, op->result)) {
            diskfs_list_streams_collected(request);
            return;
        }
    }
} /* diskfs_list_streams_collect */


static void
diskfs_list_streams_base_cb(
    struct diskfs_inode *base,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    p->inode_stash[0] = base;
    p->loop_pos       = 0;
    p->loop_left      = 0;
    p->loop_off       = 0;
    p->stream_list    = NULL;
    p->stream_count   = 0;
    p->stream_cap     = 0;

    /* The unnamed data stream first, for a file; a directory has none. */
    if (S_ISREG(base->mode) &&
        diskfs_list_streams_emit(request, base->size,
                                 base->space_used > base->alloc_size ?
                                 base->space_used : base->alloc_size,
                                 "", 0, request->fh,
                                 request->list_streams.want_fh ? request->fh_len : 0)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ERANGE);
        return;
    }

    diskfs_list_streams_collect(request);
} /* diskfs_list_streams_base_cb */


void
diskfs_list_streams(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_READ);

    diskfs_inode_get_fh_async(thread, p->txn, p->fs, request->fh, request->fh_len,
                              diskfs_list_streams_base_cb, request);
} /* diskfs_list_streams */


/* ------------------------------------------------------------------ */
/* REMOVE_STREAM                                                       */
/* ------------------------------------------------------------------ */

/* inode_stash[0] = base file, inode_stash[1] = the stream's inode */

static void
diskfs_remove_stream_finish(void *priv)
{
    struct chimera_vfs_request    *request = priv;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *base    = p->inode_stash[0];

    diskfs_stream_stamp(base, 0);
    diskfs_map_attrs(p->thread, &request->remove_stream.r_post_attr, base);
    diskfs_op_ok(request, p->txn);
} /* diskfs_remove_stream_finish */


static void
diskfs_remove_stream_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *stream  = p->inode_stash[1];

    (void) result;
    diskfs_bt_op_free(p->thread, op);

    if (!stream) {
        diskfs_remove_stream_finish(request);
        return;
    }

    /* The record held the stream inode's only link. */
    stream->nlink = 0;
    diskfs_stream_stamp(stream, 0);
    diskfs_inode_orphaned(p->thread, p->txn, stream, diskfs_remove_stream_finish, request);
} /* diskfs_remove_stream_removed_cb */


static void
diskfs_remove_stream_inode_cb(
    struct diskfs_inode *stream,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    /* An inode already gone leaves just the record to drop. */
    p->inode_stash[1] = (status == CHIMERA_VFS_OK && stream->nlink) ? stream : NULL;
    diskfs_stream_remove(request, p->inode_stash[0], p->stream_hash,
                         diskfs_remove_stream_removed_cb);
} /* diskfs_remove_stream_inode_cb */


static void
diskfs_remove_stream_lookup_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_stream_rec      *rec     = (struct diskfs_stream_rec *) p->rec_scratch;

    diskfs_bt_op_free(p->thread, op);

    if (result < 0 ||
        !diskfs_stream_rec_matches(rec, result, request->remove_stream.name,
                                   request->remove_stream.namelen)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    diskfs_inode_acquire(p->thread, p->txn, p->fs, rec->inum, rec->gen,
                         DISKFS_INODE_LOCK_WRITE, diskfs_remove_stream_inode_cb, request);
} /* diskfs_remove_stream_lookup_cb */


static void
diskfs_remove_stream_base_cb(
    struct diskfs_inode *base,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    p->inode_stash[0] = base;
    diskfs_map_attrs(p->thread, &request->remove_stream.r_pre_attr, base);
    diskfs_stream_lookup(request, base, p->stream_hash, diskfs_remove_stream_lookup_cb);
} /* diskfs_remove_stream_base_cb */


void
diskfs_remove_stream(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread      = thread;
    p->stream_hash = diskfs_stream_name_hash(request->remove_stream.name,
                                             request->remove_stream.namelen);
    p->txn = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    diskfs_inode_get_fh_async(thread, p->txn, p->fs, request->fh, request->fh_len,
                              diskfs_remove_stream_base_cb, request);
} /* diskfs_remove_stream */


/* ------------------------------------------------------------------ */
/* RENAME_STREAM                                                       */
/* ------------------------------------------------------------------ */

/* Rename a named stream of the file (MS-FSA 2.1.5.15.12.1): its record moves
* to the new name, so open handles to it -- which name its inode -- stay
* valid.  An existing stream of the new name is replaced only when asked and
* only while empty.
*
* inode_stash[0] = base file, inode_stash[2] = the replaced stream's inode */

static void
diskfs_rename_stream_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *base    = p->inode_stash[0];

    (void) result;
    diskfs_bt_op_free(p->thread, op);

    diskfs_stream_stamp(base, 0);
    diskfs_map_attrs(p->thread, &request->rename_stream.r_post_attr, base);
    diskfs_op_ok(request, p->txn);
} /* diskfs_rename_stream_inserted_cb */


static void
diskfs_rename_stream_src_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);

    diskfs_stream_insert(request, p->inode_stash[0], p->stream_new_hash,
                         request->rename_stream.new_name,
                         request->rename_stream.new_namelen,
                         diskfs_rename_stream_inserted_cb);
} /* diskfs_rename_stream_src_removed_cb */


static void
diskfs_rename_stream_move(void *priv)
{
    struct chimera_vfs_request    *request = priv;
    struct diskfs_request_private *p       = request->plugin_data;

    diskfs_stream_remove(request, p->inode_stash[0], p->stream_hash,
                         diskfs_rename_stream_src_removed_cb);
} /* diskfs_rename_stream_move */


static void
diskfs_rename_stream_dst_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *target  = p->inode_stash[2];

    (void) result;
    diskfs_bt_op_free(p->thread, op);

    if (!target) {
        diskfs_rename_stream_move(request);
        return;
    }

    target->nlink = 0;
    diskfs_stream_stamp(target, 0);
    diskfs_inode_orphaned(p->thread, p->txn, target, diskfs_rename_stream_move, request);
} /* diskfs_rename_stream_dst_removed_cb */


static void
diskfs_rename_stream_target_cb(
    struct diskfs_inode *target,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    if (status == CHIMERA_VFS_OK && target->size) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EINVAL);
        return;
    }

    p->inode_stash[2] = (status == CHIMERA_VFS_OK && target->nlink) ? target : NULL;
    diskfs_stream_remove(request, p->inode_stash[0], p->stream_new_hash,
                         diskfs_rename_stream_dst_removed_cb);
} /* diskfs_rename_stream_target_cb */


static void
diskfs_rename_stream_dst_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_stream_rec      *rec     = (struct diskfs_stream_rec *) p->rec_scratch;

    diskfs_bt_op_free(p->thread, op);

    if (result < 0 || (rec->inum == p->stream_inum && rec->gen == p->stream_gen)) {
        /* No stream of that name, or only this one under another case. */
        diskfs_rename_stream_move(request);
        return;
    }

    if (!diskfs_stream_rec_matches(rec, result, request->rename_stream.new_name,
                                   request->rename_stream.new_namelen) ||
        !(request->rename_stream.flags & CHIMERA_VFS_RENAME_STREAM_REPLACE)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
        return;
    }

    diskfs_inode_acquire(p->thread, p->txn, p->fs, rec->inum, rec->gen,
                         DISKFS_INODE_LOCK_WRITE, diskfs_rename_stream_target_cb, request);
} /* diskfs_rename_stream_dst_cb */


static void
diskfs_rename_stream_src_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_stream_rec      *rec     = (struct diskfs_stream_rec *) p->rec_scratch;

    diskfs_bt_op_free(p->thread, op);

    if (result < 0 ||
        !diskfs_stream_rec_matches(rec, result, request->rename_stream.name,
                                   request->rename_stream.namelen)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    p->stream_inum = rec->inum;
    p->stream_gen  = rec->gen;
    p->stream_seq  = rec->seq;

    diskfs_stream_lookup(request, p->inode_stash[0], p->stream_new_hash,
                         diskfs_rename_stream_dst_cb);
} /* diskfs_rename_stream_src_cb */


static void
diskfs_rename_stream_base_cb(
    struct diskfs_inode *base,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    p->inode_stash[0] = base;
    p->inode_stash[2] = NULL;
    diskfs_map_attrs(p->thread, &request->rename_stream.r_pre_attr, base);
    diskfs_stream_lookup(request, base, p->stream_hash, diskfs_rename_stream_src_cb);
} /* diskfs_rename_stream_base_cb */


void
diskfs_rename_stream(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    /* Renames to or from the unnamed data stream would move the file's data
     * between inodes; diskfs does not do that. */
    if (request->rename_stream.namelen == 0 || request->rename_stream.new_namelen == 0) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }
    if (request->rename_stream.new_namelen > 255) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    p->thread      = thread;
    p->stream_hash = diskfs_stream_name_hash(request->rename_stream.name,
                                             request->rename_stream.namelen);
    p->stream_new_hash = diskfs_stream_name_hash(request->rename_stream.new_name,
                                                 request->rename_stream.new_namelen);
    p->txn = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    diskfs_inode_get_fh_async(thread, p->txn, p->fs, request->fh, request->fh_len,
                              diskfs_rename_stream_base_cb, request);
} /* diskfs_rename_stream */
