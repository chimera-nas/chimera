// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "fuse_internal.h"
#include "fuse_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

/* --- OPENDIR --- */

/*
 * opendir(3) requires read permission on the directory, and opening by file
 * handle does not check it: chimera_vfs_open_fh opens by handle without a DAC
 * gate, so an unreadable directory opened successfully and READDIR then listed
 * it.  A default mount hides this because default_permissions has the kernel
 * check the mode bits first -- but no_default_permissions is a supported
 * option, and there the server is the only thing standing in the way.
 *
 * The sequence asks for the rights and then opens, and this veto is what turns
 * the answer into a refusal.  It is answered from the ACCESS result already in
 * the sequence, so it takes no I/O and gives the same answer if asked twice.
 */
static void
chimera_fuse_opendir_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *op;

    if (*status != CHIMERA_VFS_OK) {
        return;
    }

    op = chimera_vfs_compound_op(compound, index);

    if (op->type != CHIMERA_VFS_COMPOUND_OP_ACCESS) {
        return;
    }

    if (!(op->granted & CHIMERA_ACE_READ_DATA)) {
        *status = CHIMERA_VFS_EACCES;
    }
} /* chimera_fuse_opendir_gate */

static void
chimera_fuse_opendir_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request    *req    = private_data;
    struct chimera_fuse_thread     *thread = req->thread;
    struct chimera_fuse_mount      *mount  = req->channel->mount;
    struct chimera_fuse_open_file  *file;
    struct chimera_vfs_open_handle *oh;
    struct fuse_open_out            out;
    enum chimera_vfs_error          status;

    status = chimera_vfs_compound_status(compound);

    if (status != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(status), NULL, 0);
        return;
    }

    /* The open's handle outlives the sequence -- it is what the kernel's fh
     * will name -- so it has to be taken from the compound. */
    oh = chimera_vfs_compound_take_handle(
        compound, chimera_vfs_compound_num_ops(compound) - 1);

    file = calloc(1, sizeof(*file));

    file->handle = oh;
    file->mount  = mount;

    chimera_fuse_file_link(mount, file);

    memset(&out, 0, sizeof(out));
    out.fh = (uint64_t) (uintptr_t) file;

    if (chimera_fuse_reply(req, 0, &out, sizeof(out)) != 0) {
        /* The kernel never learned this fh, so no RELEASEDIR will come. */
        chimera_fuse_file_unlink(mount, file);
        chimera_vfs_release(thread->vfs_thread, file->handle);
        free(file);
    }
} /* chimera_fuse_opendir_sequence_complete */

void
chimera_fuse_op_opendir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    chimera_vfs_compound_set_gate(req->compound, chimera_fuse_opendir_gate,
                                  req);

    chimera_vfs_compound_add_putfh(req->compound, req->fh, (int) req->fh_len);

    /* One open, and ACCESS shares it: a directory open carries everything a
    * metadata op asks for and sits on the same side of OPEN_PATH, so the
    * sequence does not open the object twice to ask two questions of it. */
    chimera_vfs_compound_add_open_current(req->compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY,
                                          0);

    chimera_vfs_compound_add_access(req->compound, CHIMERA_ACE_READ_DATA);

    /* The handle outlives the sequence -- it is what the kernel's fh names. */
    chimera_vfs_compound_add_gethandle(req->compound);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_opendir_sequence_complete, req);
} /* chimera_fuse_op_opendir */

/* --- READDIR / READDIRPLUS --- */

/* Attempt-private entry identity. Node lookup counts and coherence grants
 * are installed only after the compound's finish has accepted the attempt. */
struct chimera_fuse_readdir_pending {
    uint32_t offset;
    uint32_t fh_len;
    uint8_t  fh[CHIMERA_VFS_FH_SIZE];
};

static int
chimera_fuse_readdir_entry(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct chimera_fuse_request *req  = arg;
    uint8_t                     *base = chimera_fuse_reply_space(req);
    struct fuse_dirent          *dirent;
    struct fuse_direntplus      *plus;
    size_t                       entsize;
    int                          dot;

    dot = (namelen == 1 && name[0] == '.') ||
        (namelen == 2 && name[0] == '.' && name[1] == '.');

    if (req->u.readdir.plus) {
        entsize = FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET_DIRENTPLUS + namelen);

        if (req->u.readdir.used + entsize > req->u.readdir.size) {
            return -1;
        }

        plus = (struct fuse_direntplus *) (base + req->u.readdir.used);

        memset(plus, 0, sizeof(*plus));

        /* The kernel instantiates an inode per entry; "." and ".." are
         * skipped by it and carry nodeid 0 (no lookup count). */
        if (!dot && (attrs->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
            struct chimera_fuse_readdir_pending *pending =
                &req->u.readdir.pending[req->u.readdir.num_pending++];
            pending->offset = req->u.readdir.used;
            pending->fh_len = attrs->va_fh_len;
            memcpy(pending->fh, attrs->va_fh, attrs->va_fh_len);
            plus->entry_out.generation = 1;
            chimera_fuse_attr_from_vfs(&plus->entry_out.attr, attrs);
        } else {
            plus->entry_out.attr.ino = inum;
        }

        plus->dirent.ino     = inum;
        plus->dirent.off     = cookie;
        plus->dirent.namelen = namelen;
        plus->dirent.type    = (attrs->va_set_mask & CHIMERA_VFS_ATTR_MODE) ?
            (attrs->va_mode >> 12) & 0xf : 0;

        memcpy(plus->dirent.name, name, namelen);
        memset((uint8_t *) plus + FUSE_NAME_OFFSET_DIRENTPLUS + namelen, 0,
               entsize - (FUSE_NAME_OFFSET_DIRENTPLUS + namelen));
    } else {
        entsize = FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET + namelen);

        if (req->u.readdir.used + entsize > req->u.readdir.size) {
            return -1;
        }

        dirent = (struct fuse_dirent *) (base + req->u.readdir.used);

        dirent->ino     = inum;
        dirent->off     = cookie;
        dirent->namelen = namelen;
        dirent->type    = (attrs->va_set_mask & CHIMERA_VFS_ATTR_MODE) ?
            (attrs->va_mode >> 12) & 0xf : 0;

        memcpy(dirent->name, name, namelen);
        memset((uint8_t *) dirent + FUSE_NAME_OFFSET + namelen, 0,
               entsize - (FUSE_NAME_OFFSET + namelen));
    }

    req->u.readdir.used += entsize;

    return 0;
} /* chimera_fuse_readdir_entry */

static void
chimera_fuse_readdirplus_publish(struct chimera_fuse_request *req)
{
    struct chimera_fuse_mount *mount = req->channel->mount;
    uint8_t                   *base  = chimera_fuse_reply_space(req);

    for (uint32_t i = 0; i < req->u.readdir.num_pending; i++) {
        struct chimera_fuse_readdir_pending *pending  = &req->u.readdir.pending[i];
        struct fuse_direntplus              *plus     = (struct fuse_direntplus *) (base + pending->offset);
        uint32_t                             entry_ms = mount->entry_timeout_ms;
        uint32_t                             attr_ms  = mount->attr_timeout_ms;

        plus->entry_out.nodeid = chimera_fuse_node_insert(mount->node_table,
                                                          pending->fh, pending->fh_len);
        if (S_ISREG(plus->entry_out.attr.mode)) {
            chimera_fuse_grant_ensure(req->thread, mount, plus->entry_out.nodeid,
                                      pending->fh, pending->fh_len,
                                      chimera_fuse_fh_hash(pending->fh, pending->fh_len));
        }
        /* A grant can break/rearm and a directory invalidation can be sent
         * before this parked reply. Boolean coverage cannot prove continuity
         * from enumeration through accepted publication. Until there is a
         * generation/reply interlock, sync READDIRPLUS entries and attributes
         * remain uncached; later LOOKUP/GETATTR establish protected state. */
        if (mount->coherence_sync) {
            attr_ms = entry_ms = 0;
        }
        plus->entry_out.entry_valid      = entry_ms / 1000;
        plus->entry_out.entry_valid_nsec = (entry_ms % 1000) * 1000000;
        plus->entry_out.attr_valid       = attr_ms / 1000;
        plus->entry_out.attr_valid_nsec  = (attr_ms % 1000) * 1000000;
    }
} /* chimera_fuse_readdirplus_publish */

/* Walk a packed READDIRPLUS reply undoing the lookup-count bumps of entries
 * the kernel never received. */
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
} /* chimera_fuse_readdirplus_unwind */

/* Discard only private staging; no node or grant was published by this attempt. */
static void
chimera_fuse_readdir_reset(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data)
{
    struct chimera_fuse_request *req = private_data;

    req->u.readdir.used        = 0;
    req->u.readdir.num_pending = 0;
} /* chimera_fuse_readdir_reset */

static void
chimera_fuse_readdir_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request          *req = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    int                                   rc;

    status = chimera_vfs_compound_status(compound);

    if (status != CHIMERA_VFS_OK) {
        chimera_fuse_readdir_reset(compound, 0, req);
        free(req->u.readdir.pending);
        req->u.readdir.pending = NULL;
        chimera_fuse_reply(req, chimera_fuse_errno(status), NULL, 0);
        return;
    }

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    req->file->readdir_verifier = op->r_verifier;

    if (req->u.readdir.plus) {
        chimera_fuse_readdirplus_publish(req);
    }
    free(req->u.readdir.pending);
    req->u.readdir.pending = NULL;

    rc = chimera_fuse_send_only(req, 0, chimera_fuse_reply_space(req),
                                req->u.readdir.used);

    if (rc != 0 && req->u.readdir.plus) {
        chimera_fuse_readdirplus_unwind(req);
    }

    chimera_fuse_request_finish(req);
} /* chimera_fuse_readdir_sequence_complete */

void
chimera_fuse_op_readdir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_read_in     *in = arg;
    struct chimera_fuse_open_file *file;
    uint64_t                       attr_mask;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    file      = chimera_fuse_file(in->fh);
    req->file = file;

    req->u.readdir.plus = (req->opcode == FUSE_READDIRPLUS);

    if (req->u.readdir.plus) {
        /* Keep directory invalidation coverage armed. Sync-mode replies use
         * zero TTL until enumeration and publication have a version interlock. */
        req->entry_cover = chimera_fuse_watch_dir(req->thread,
                                                  req->channel->mount,
                                                  req->nodeid,
                                                  file->handle->fh,
                                                  file->handle->fh_len);
    }
    req->u.readdir.used        = 0;
    req->u.readdir.size        = in->size;
    req->u.readdir.pending     = NULL;
    req->u.readdir.num_pending = 0;

    if (req->u.readdir.size > CHIMERA_FUSE_BUFSZ - CHIMERA_FUSE_REPLY_OFF) {
        req->u.readdir.size = CHIMERA_FUSE_BUFSZ - CHIMERA_FUSE_REPLY_OFF;
    }
    if (req->u.readdir.plus) {
        uint32_t capacity = req->u.readdir.size / FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET_DIRENTPLUS + 1);
        if (capacity) {
            req->u.readdir.pending = calloc(capacity, sizeof(*req->u.readdir.pending));
            if (!req->u.readdir.pending) {
                chimera_fuse_reply(req, ENOMEM, NULL, 0);
                return;
            }
        }
    }

    attr_mask = req->u.readdir.plus ?
        CHIMERA_FUSE_ATTR_MASK :
        (CHIMERA_VFS_ATTR_INUM | CHIMERA_VFS_ATTR_MODE);

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    /* Streaming: entries are packed into the reply as the backend produces
    * them, so the page ends on the entry that does not fit rather than at an
    * entry count this would otherwise have to guess from a byte budget. */
    /* The kernel named an open directory; the sequence borrows it and never
     * releases it. */
    chimera_vfs_compound_add_puthandle(req->compound, file->handle,
                                       CHIMERA_VFS_OPEN_INFERRED |
                                       CHIMERA_VFS_OPEN_PATH |
                                       CHIMERA_VFS_OPEN_DIRECTORY);

    chimera_vfs_compound_add_readdir_stream(req->compound,
                                            in->offset,
                                            file->readdir_verifier,
                                            attr_mask,
                                            chimera_fuse_readdir_reset,
                                            chimera_fuse_readdir_entry,
                                            req);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_readdir_sequence_complete, req);
} /* chimera_fuse_op_readdir */

/* --- RELEASEDIR --- */

void
chimera_fuse_op_releasedir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_release_in  *in = arg;
    struct chimera_fuse_open_file *file;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    file = chimera_fuse_file(in->fh);

    chimera_fuse_file_unlink(file->mount, file);

    chimera_vfs_release(req->thread->vfs_thread, file->handle);

    free(file);

    chimera_fuse_reply(req, 0, NULL, 0);
} /* chimera_fuse_op_releasedir */

/* --- FSYNCDIR --- */

static void
chimera_fuse_fsyncdir_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request *req = private_data;

    chimera_fuse_reply(req,
                       chimera_fuse_errno(chimera_vfs_compound_status(compound)),
                       NULL, 0);
} /* chimera_fuse_fsyncdir_sequence_complete */

void
chimera_fuse_op_fsyncdir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_fsync_in *in = arg;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    /* The kernel named an open directory; the sequence acts on that handle,
     * which stays owned by the open, so there is no current object at all. */
    chimera_vfs_compound_add_puthandle(req->compound,
                                       chimera_fuse_file(in->fh)->handle,
                                       CHIMERA_VFS_OPEN_INFERRED);

    chimera_vfs_compound_add_commit(req->compound, 0, 0, 0);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_fsyncdir_sequence_complete, req);
} /* chimera_fuse_op_fsyncdir */
