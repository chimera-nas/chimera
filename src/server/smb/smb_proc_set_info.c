// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "vfs/vfs_internal_procs.h"
#include "smb_doc_compound.h"
#include "smb_procs.h"
#include "smb_ea.h"
#include "smb_async_interim.h"
#include "common/misc.h"
#include "common/compound_retry.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_notify.h"

/*
 * Every SET_INFO class that touches the VFS runs as a sequence on the object
 * the FileId names.  Four of them are one SETATTR through the handle the client
 * already holds; two of those need a reading first, and take it in the SAME
 * sequence with the gate writing the answer into the SETATTR that follows --
 * which is what an argument the caller can only compute from an earlier op's
 * result looks like once the VFS owns the sequence.
 *
 * The attributes are applied through `in_handle`, so they are authorized by the
 * descriptor's own grant rather than re-checked against the object's mode --
 * the ftruncate(2) rule, and the one MS-FSA states: a SetInfo through a handle
 * rides on the access the CREATE granted.  It only ever relaxes, and only for a
 * mutation whose whole requirement is WRITE_DATA (a size set, a timestamp set
 * to now) made through a handle the VFS opened for writing; everything else --
 * an explicit timestamp, the DOS word, the security descriptor -- needs more
 * than WRITE_DATA and is checked exactly as it was.
 */
#define CHIMERA_SMB_SET_INFO_OP_GETATTR 1
#define CHIMERA_SMB_SET_INFO_OP_SETATTR 2

static void
chimera_smb_set_info_finish(
    struct chimera_smb_request *request,
    enum chimera_vfs_error      error_code)
{
    if (!error_code && request->set_info.open_file->parent_fh_len > 0) {
        struct chimera_server_smb_thread *thread = request->compound->thread;
        uint32_t                          mask   = request->set_info.notify_mask ?
            request->set_info.notify_mask : CHIMERA_VFS_NOTIFY_ATTRS_CHANGED;

        /* Self-exempt the parent directory lease named by the operating open's
         * ParentLeaseKey: a setinfo issued through a handle that supplied the
         * correct parent key must not break that directory's lease (MS-SMB2
         * dirlease; dirlease.set{eof,dos,*time} cases 1.1/2.1). */
        uint64_t                          skip_lo, skip_hi;
        bool                              has_skip = chimera_smb_parent_lease_skip(
            request->set_info.open_file->parent_lease_key, &skip_lo, &skip_hi);

        chimera_vfs_notify_emit_lease(thread->shared->vfs->vfs_notify,
                                      request->set_info.open_file->parent_fh,
                                      request->set_info.open_file->parent_fh_len,
                                      mask,
                                      request->set_info.open_file->name,
                                      request->set_info.open_file->name_len,
                                      NULL, 0,
                                      skip_lo, skip_hi, has_skip);
    }

    chimera_smb_open_file_release(request, request->set_info.open_file);

    chimera_smb_complete_request(request, error_code ? SMB2_STATUS_INTERNAL_ERROR : SMB2_STATUS_SUCCESS);
} /* chimera_smb_set_info_finish */

/* PUTHANDLE, [GETATTR], SETATTR: the completion of every setattr-shaped class. */
static void
chimera_smb_set_info_setattr_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request *request = private_data;
    enum chimera_vfs_error      status;

    status = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    chimera_smb_set_info_finish(request, status);
} /* chimera_smb_set_info_setattr_sequence_complete */

/* Preserve the open's cache owner before a descriptor size mutation. The
 * gate only fills a later op's arguments; it has no externally visible effect. */
static void
chimera_smb_set_info_owner_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_smb_request     *request   = private_data;
    struct chimera_smb_open_file   *open_file = request->set_info.open_file;
    uint32_t                        last      = chimera_vfs_compound_num_ops(compound) - 1;
    struct chimera_vfs_compound_op *op;

    if (*status != CHIMERA_VFS_OK || index >= last) {
        return;
    }
    op           = chimera_vfs_compound_op_edit(compound, last);
    op->io_owner = (struct chimera_claim_actor) {
        .owner = {
            .proto      = CHIMERA_CLAIM_PROTO_SMB2,
            .client_key = request->session_handle->session->client_key,
            .owner_lo   = open_file->file_id.pid,
            .owner_hi   = open_file->file_id.vid,
        },
        .op_handle = open_file->handle,
    };
    if (open_file->grant) {
        op->io_owner.owner = open_file->grant->claim.owner;
    }
    op->have_io_owner = 1;
} /* chimera_smb_set_info_owner_gate */

/* PUTHANDLE, SETATTR(in_handle): FileBasicInformation and
 * FileEndOfFileInformation, whose attributes are wholly decided before the
 * sequence is built. */
static void
chimera_smb_set_info_size(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open_file = request->set_info.open_file;

    request->vfs_compound = chimera_vfs_compound_alloc(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred);

    chimera_vfs_compound_add_puthandle(request->vfs_compound,
                                       open_file->handle,
                                       open_file->open_flags);

    chimera_vfs_compound_add_setattr(request->vfs_compound,
                                     open_file->handle,
                                     &request->set_info.vfs_attrs,
                                     0, 0);

    chimera_vfs_compound_set_gate(request->vfs_compound,
                                  chimera_smb_set_info_owner_gate, request);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_set_info_setattr_sequence_complete,
                                request);
} /* chimera_smb_set_info_size */



/* Map a VFS error from a SetInfo operation (hard link, rename, etc.) to the
 * SMB2 status the client expects, instead of collapsing every failure to
 * INTERNAL_ERROR (which surfaces as EIO).  A hard link onto an existing name is
 * the important one: OBJECT_NAME_COLLISION -> EEXIST, matching link(2). */
static inline uint32_t
chimera_smb_set_info_error_status(enum chimera_vfs_error error_code)
{
    switch (error_code) {
        case CHIMERA_VFS_OK:           return SMB2_STATUS_SUCCESS;
        case CHIMERA_VFS_EEXIST:       return SMB2_STATUS_OBJECT_NAME_COLLISION;
        case CHIMERA_VFS_ENOENT:       return SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:        return SMB2_STATUS_ACCESS_DENIED;
        case CHIMERA_VFS_EISDIR:       return SMB2_STATUS_FILE_IS_A_DIRECTORY;
        case CHIMERA_VFS_ENOTDIR:      return SMB2_STATUS_NOT_A_DIRECTORY;
        case CHIMERA_VFS_ENOTEMPTY:    return SMB2_STATUS_DIRECTORY_NOT_EMPTY;
        case CHIMERA_VFS_EXDEV:        return SMB2_STATUS_NOT_SAME_DEVICE;
        case CHIMERA_VFS_EMLINK:       return SMB2_STATUS_TOO_MANY_LINKS;
        case CHIMERA_VFS_ENOSPC:
        case CHIMERA_VFS_EDQUOT:       return SMB2_STATUS_DISK_FULL;
        case CHIMERA_VFS_ENAMETOOLONG: return SMB2_STATUS_NAME_TOO_LONG;
        case CHIMERA_VFS_EROFS:        return SMB2_STATUS_MEDIA_WRITE_PROTECTED;
        default:                       return SMB2_STATUS_INTERNAL_ERROR;
    } /* switch */
} /* chimera_smb_set_info_error_status */

/*
 * FileLinkInformation: PUTFH(source), SAVEFH, PUTFH(tree root),
 * [LOOKUP_PATH(destination parent)], LINK(new name).
 *
 * LINK reads the SAVED file handle for the object and the CURRENT one for the
 * directory, which is what link_at takes -- so nothing here is opened at all,
 * where the per-op chain opened the destination parent for the sole purpose of
 * handing link_at its file handle.  The destination parent's own path, when the
 * client gave one, is resolved by the path-walking LOOKUP whose per-op twin the
 * chain called; a failure there is the path, not the link.
 */
static void
chimera_smb_set_info_link_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request *request = private_data;
    enum chimera_vfs_error      status;
    uint32_t                    completed, nops, smb_status;

    status    = chimera_vfs_compound_status(compound);
    completed = chimera_vfs_compound_num_completed(compound);
    nops      = chimera_vfs_compound_num_ops(compound);

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (status != CHIMERA_VFS_OK && completed < nops) {
        /* The destination parent path did not resolve. */
        smb_status = SMB2_STATUS_OBJECT_PATH_NOT_FOUND;
    } else {
        smb_status = chimera_smb_set_info_error_status(status);
    }

    chimera_smb_open_file_release(request, request->set_info.open_file);
    chimera_smb_complete_request(request, smb_status);
} /* chimera_smb_set_info_link_sequence_complete */

static void
chimera_smb_set_info_link_process(struct chimera_smb_request *request)
{
    struct chimera_smb_tree        *tree        = request->tree;
    struct chimera_smb_rename_info *rename_info = &request->set_info.rename_info;
    struct chimera_smb_open_file   *open_file   = request->set_info.open_file;
    int                             index;

    request->vfs_compound = chimera_vfs_compound_alloc(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred);

    chimera_vfs_compound_add_putfh(request->vfs_compound,
                                   open_file->handle->fh,
                                   open_file->handle->fh_len);
    chimera_vfs_compound_add_savefh(request->vfs_compound);

    chimera_vfs_compound_add_putfh(request->vfs_compound,
                                   tree->fh, tree->fh_len);

    if (rename_info->new_parent_len) {
        chimera_vfs_compound_add_lookup_path(request->vfs_compound,
                                             rename_info->new_parent,
                                             rename_info->new_parent_len,
                                             CHIMERA_VFS_ATTR_FH, 0);
    }

    index = chimera_vfs_compound_add_link(request->vfs_compound,
                                          rename_info->new_name,
                                          rename_info->new_name_len,
                                          0, 0, 0);

    chimera_vfs_compound_op_set_link_opts(
        request->vfs_compound, index,
        rename_info->replace_if_exist,
        /* SMB rename via link: self-exempt the directory lease named by the
         * operating open's ParentLeaseKey (dirlease.rename correct-parent case). */
        open_file->parent_lease_key,
        /* ...and self-exempt the linker's own file lease from the source recall. */
        open_file->handle);

    struct chimera_vfs_compound_op *op =
        chimera_vfs_compound_op_args(request->vfs_compound, index);
    op->io_owner.owner     = chimera_smb_open_actor_owner(open_file);
    op->io_owner.op_handle = open_file->handle;
    op->have_io_owner      = 1;

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_set_info_link_sequence_complete,
                                request);
} /* chimera_smb_set_info_link_process */

/*
 * FileAllocationInformation: the size to set is a function of the size just
 * read, which is what the gate's argument edit is for.  Truncate (and advance
 * LastWriteTime) only when the requested allocation is below the current EOF;
 * otherwise leave the data and the EOF alone and re-set the unchanged size, so
 * the backend advances ChangeTime without disturbing LastWriteTime
 * (MS-FSCC 2.4.4).
 *
 * Everything written here is computed from what the caller already had -- the
 * allocation the client asked for, and the handle's sticky-write-time flag --
 * plus the size this GETATTR just reported, and is ASSIGNED rather than
 * accumulated, so a re-executed sequence re-derives exactly the same SETATTR.
 */
static void
chimera_smb_set_info_allocation_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    chimera_smb_set_info_owner_gate(compound, index, status, private_data);
    struct chimera_smb_request           *request = private_data;
    const struct chimera_vfs_compound_op *getattr;
    struct chimera_vfs_compound_op       *setattr;
    uint64_t                              alloc_size, cur_size;

    if (index != CHIMERA_SMB_SET_INFO_OP_GETATTR || *status != CHIMERA_VFS_OK) {
        return;
    }

    getattr = chimera_vfs_compound_op(compound, index);
    setattr = chimera_vfs_compound_op_edit(compound,
                                           CHIMERA_SMB_SET_INFO_OP_SETATTR);

    alloc_size = request->set_info.attrs.smb_size;
    cur_size   = getattr->attr.va_size;

    memset(&setattr->set_attr, 0, sizeof(setattr->set_attr));
    setattr->set_attr.va_req_mask = CHIMERA_VFS_ATTR_SIZE;
    setattr->set_attr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;

    if (alloc_size < cur_size) {
        setattr->set_attr.va_size = alloc_size;
        if (!(request->set_info.open_file->flags &
              CHIMERA_SMB_OPEN_FILE_WRITE_TIME_STICKY)) {
            setattr->set_attr.va_mtime.tv_nsec = CHIMERA_VFS_TIME_NOW;
            setattr->set_attr.va_req_mask     |= CHIMERA_VFS_ATTR_MTIME;
            setattr->set_attr.va_set_mask     |= CHIMERA_VFS_ATTR_MTIME;
        }
    } else {
        setattr->set_attr.va_size = cur_size;
    }
} /* chimera_smb_set_info_allocation_gate */

/* PUTHANDLE, GETATTR(SIZE), SETATTR(in_handle) -- one sequence, the gate
 * supplying the size. */
static void
chimera_smb_set_info_allocation(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open_file = request->set_info.open_file;

    request->vfs_compound = chimera_vfs_compound_alloc(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred);

    chimera_vfs_compound_add_puthandle(request->vfs_compound,
                                       open_file->handle,
                                       open_file->open_flags);

    chimera_vfs_compound_add_getattr(request->vfs_compound,
                                     CHIMERA_VFS_ATTR_SIZE);

    chimera_vfs_compound_add_setattr(request->vfs_compound,
                                     open_file->handle,
                                     &request->set_info.vfs_attrs,
                                     0, 0);

    chimera_vfs_compound_set_gate(request->vfs_compound,
                                  chimera_smb_set_info_allocation_gate,
                                  request);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_set_info_setattr_sequence_complete,
                                request);
} /* chimera_smb_set_info_allocation */
/* Preserve the Open's cache identity across the VFS authorization/recall
* sequence. In particular, a same-key sibling is coherent with this handle,
* whereas a legacy LEVEL_II grant must break even on its own size set. */

/* Resolve a FileAllocationInformation set once the current size is known.
 * Truncate (and advance LastWriteTime) only when the requested allocation is
 * below the current EOF; otherwise leave the data/EOF alone and just touch the
 * inode so ChangeTime advances. */

/* Completion for the delete-on-close caching-lease recall: the recall has
 * drained (every OTHER holder's handle lease was broken and acknowledged), so the
 * peer's break is now visible to the client.  Reply to the SetInfo.  Replying
 * only after the recall drains is what makes the break deterministically precede
 * the reply (the client checks lease_break_info.count synchronously right after
 * smb2_setinfo_file -- smb2.lease.unlink). */

/*
 * PUTHANDLE, RECALL(CHIMERA_CLAIM_CR).
 *
 * The PARKING shape, not NOWAIT: the protocol needs the break to have been
 * ACKNOWLEDGED before the SetInfo reply goes out, because the client reads
 * lease_break_info.count synchronously on the next line after smb2_setinfo_file
 * (smb2.lease.unlink).  A NOWAIT recall would kick the break and reply into the
 * race it is supposed to close.  Nothing else in the run depends on the answer,
 * so `recall_still_open` is not read: a peer that acked without closing keeps
 * its open, and a delete-on-close that the object outlives is decided at the
 * last close, not here.
 *
 * The recall addresses the lent handle, so that handle's own lease is spared --
 * the operating client must not break the lease it holds on the file it is
 * marking (chimera_vfs_recall_handle_lease's io_handle, expressed as the op
 * addressing a handle).
 */








/* ---- FILE_FULL_EA_INFORMATION apply engine (shared by SetInfo and CREATE
 * ExtA): applies a client EA list to the VFS xattr store one entry at a time
 * (zero-length value deletes; names canonicalized case-insensitively against the
 * existing user.* set, Samba-style).  The caller owns ea_buf for the duration
 * and is invoked via `done` with the resulting NTSTATUS.  State lives in a heap
 * context so the engine is independent of the request union. ---- */

/* Successful earlier mutations override the initial LIST snapshot. A removal
* is a tombstone: recreating that logical EA uses the new requested spelling.
* Storage is reserved from the immutable input length before any mutation. */
struct smb_ea_name_change {
    uint32_t length;
    bool     removed;
    char     name[CHIMERA_VFS_XATTR_NAME_MAX + 1];
};

static int
smb_ea_resolve_name(
    char                              *name,
    size_t                             size,
    const struct chimera_smb_ea_entry *entry,
    const char                        *names,
    uint32_t                           names_len,
    const struct smb_ea_name_change   *changes,
    uint32_t                           count)
{
    int length = chimera_vfs_xattr_build_user(name, size, entry->name, entry->name_len);

    if (length < 0) {
        return -1;
    }
    for (uint32_t i = count; i > 0; i--) {
        const struct smb_ea_name_change *change = &changes[i - 1];
        if (!chimera_smb_ea_name_eq(change->name + CHIMERA_VFS_XATTR_USER_PREFIX_LEN,
                                    change->length - CHIMERA_VFS_XATTR_USER_PREFIX_LEN, entry->name, entry->name_len)) {
            continue;
        }
        if (!change->removed) {
            memcpy(name, change->name, change->length);
            name[change->length] = 0;
            length               = change->length;
        }
        return length;
    }
    for (uint32_t pos = 0; names && pos < names_len;) {
        size_t len = strnlen(names + pos, names_len - pos);
        if (len == names_len - pos) {
            break;
        }
        if (len < size && chimera_vfs_xattr_is_user(names + pos, len) &&
            chimera_smb_ea_name_eq(names + pos + CHIMERA_VFS_XATTR_USER_PREFIX_LEN,
                                   len - CHIMERA_VFS_XATTR_USER_PREFIX_LEN, entry->name, entry->name_len)) {
            memcpy(name, names + pos, len + 1);
            return len;
        }
        pos += len + 1;
    }
    return length;
} /* smb_ea_resolve_name */

static void
smb_ea_record_name(
    struct smb_ea_name_change *change,
    const char                *name,
    uint32_t                   length,
    bool                       removed)
{
    change->length  = length;
    change->removed = removed;
    memcpy(change->name, name, length);
} /* smb_ea_record_name */

struct chimera_smb_ea_apply {
    struct chimera_vfs_thread      *thread;
    struct chimera_vfs_cred         cred;
    struct chimera_vfs_open_handle *handle;
    const uint8_t                  *ea_buf;
    uint32_t                        ea_buf_len, ea_off, chunk_start;
    struct smb_ea_name_change      *changes;
    uint32_t                        change_count, chunk_changes;
    uint32_t                        list_len;
    char                            list[4096];
    uint32_t                        status;
    void                            (*done)(
        uint32_t status,
        void    *arg);
    void                           *arg;
};

/* The remaining legacy CREATE lifecycle can call this adapter, but all of its
 * EA filesystem work belongs to VFS compounds, split only at operation capacity.
 * Callouts update only private name history; the caller resumes after the final
 * accepted chunk (or terminal error), never while finish can reject an attempt. */
static void
smb_ea_apply_next(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_smb_ea_apply          *a    = private_data;
    const struct chimera_vfs_compound_op *done = chimera_vfs_compound_op(compound, index);

    if (done->type == CHIMERA_VFS_COMPOUND_OP_LISTXATTRS) {
        a->list_len = *status == CHIMERA_VFS_OK ? done->buffer_len : 0;
        if (a->list_len) {
            memcpy(a->list, done->buffer, a->list_len);
        }
        /* Preserve optional canonicalization on backends unable to list. */
        *status = CHIMERA_VFS_OK;
    } else if (done->type == CHIMERA_VFS_COMPOUND_OP_SETXATTR ||
               done->type == CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR) {
        if (*status == CHIMERA_VFS_ENODATA && done->type == CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR) {
            *status = CHIMERA_VFS_OK;
        }
        if (*status != CHIMERA_VFS_OK) {
            a->status = chimera_smb_ea_status(*status); return;
        }
        smb_ea_record_name(&a->changes[a->change_count++], done->name, done->name_len,
                           done->type == CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR);
    }
    /* A long list crosses explicit capacity boundaries. Do not consume the
     * next entry until this chunk has been accepted. The original name snapshot
     * and committed history survive, including when LIST exceeds backend limits. */
    if (a->ea_off >= a->ea_buf_len ||
        chimera_vfs_compound_num_ops(compound) == CHIMERA_VFS_COMPOUND_MAX_OPS) {
        return;
    }
    struct chimera_smb_ea_entry entry;
    if (chimera_smb_ea_full_parse_one(a->ea_buf, a->ea_buf_len, &a->ea_off, &entry)) {
        a->status = SMB2_STATUS_EA_LIST_INCONSISTENT;
        *status   = CHIMERA_VFS_EINVAL;
        return;
    }
    if (!entry.name_len || !chimera_smb_ea_name_valid(entry.name, entry.name_len) ||
        (entry.flags & ~FILE_NEED_EA)) {
        a->status = SMB2_STATUS_INVALID_EA_NAME;
        *status   = CHIMERA_VFS_EINVAL;
        return;
    }
    char name[CHIMERA_VFS_XATTR_NAME_MAX + 1];
    int  length = smb_ea_resolve_name(name, sizeof(name), &entry,
                                      a->list, a->list_len, a->changes, a->change_count);
    if (length < 0) {
        a->status = SMB2_STATUS_INVALID_EA_NAME;
        *status   = CHIMERA_VFS_EINVAL;
        return;
    }
    int  op = entry.value_len ? chimera_vfs_compound_add_setxattr(compound,
                                                                  CHIMERA_VFS_XATTR_EITHER, name, length, entry.value,
                                                                  entry.value_len) :
        chimera_vfs_compound_add_removexattr(compound, name, length);
    if (op >= 0) {
        chimera_vfs_compound_op_set_handle(compound, op, a->handle);
        chimera_vfs_compound_set_op_callbacks(compound, op, NULL, smb_ea_apply_next, a);
    }
} /* smb_ea_apply_next */

static void
smb_ea_apply_reset(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_ea_apply *a = private_data;

    (void) compound;
    a->ea_off       = a->chunk_start;
    a->change_count = a->chunk_changes;
    if (!a->chunk_start) {
        a->list_len = 0;
    }
    a->status = SMB2_STATUS_SUCCESS;
} /* smb_ea_apply_reset */

/* PUTHANDLE, SETXATTR or REMOVEXATTR -- one sequence per EA.
 *
 * The list is the client's and its length is the client's too, so the fan-out
 * is unbounded: the gate edits the ops that are there, it does not add any, and
 * a sequence holds at most CHIMERA_VFS_COMPOUND_MAX_OPS.  So this is the
 * honest shape, consecutive sequences within the one request -- and it is also
 * the one that keeps the per-entry status, which the apply needs: an EA that
 * fails stops the apply THERE and reports its own error, where a batched run
 * would report only the first failure's index. */
static void smb_ea_apply_start(
    struct chimera_smb_ea_apply *a);

static void
smb_ea_apply_done(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_ea_apply *a      = private_data;
    uint32_t                     status = chimera_vfs_compound_finish_status(compound) == CHIMERA_VFS_OK &&
        a->status != SMB2_STATUS_SUCCESS ? a->status :
        chimera_smb_ea_status(chimera_vfs_compound_status(compound));

    void                         (*done)(
        uint32_t,
        void *) = a->done;
    void                        *arg = a->arg;
    chimera_vfs_compound_free(compound);
    if (status == SMB2_STATUS_SUCCESS && a->ea_off < a->ea_buf_len) {
        smb_ea_apply_start(a);
        return;
    }
    chimera_vfs_release(a->thread, a->handle);
    free(a->changes);
    free(a);
    done(status, arg);
} /* smb_ea_apply_done */

/* PUTHANDLE, LISTXATTRS: the existing user.* names, so each set can match
 * case-insensitively and reuse the stored spelling.  The page is the
 * compound's, so it is copied out before the sequence is freed. */
static void
smb_ea_apply_start(struct chimera_smb_ea_apply *a)
{
    a->chunk_start   = a->ea_off;
    a->chunk_changes = a->change_count;
    a->status        = SMB2_STATUS_SUCCESS;
    struct chimera_vfs_compound *compound = chimera_vfs_compound_alloc(a->thread, &a->cred);
    if (!compound) {
        void  (*done)(
            uint32_t,
            void *) = a->done;
        void *arg = a->arg;
        chimera_vfs_release(a->thread, a->handle);
        free(a->changes); free(a);
        done(SMB2_STATUS_INSUFFICIENT_RESOURCES, arg);
        return;
    }
    chimera_vfs_compound_add_puthandle(compound, a->handle, a->handle->flags);
    int first = a->chunk_start ? chimera_vfs_compound_add_checkpoint(compound) :
        chimera_vfs_compound_add_listxattrs(compound, 0, sizeof(a->list));
    chimera_vfs_compound_set_op_callbacks(compound, first, NULL, smb_ea_apply_next, a);
    chimera_vfs_compound_set_attempt_reset(compound, smb_ea_apply_reset, a);
    chimera_frontend_compound_submit(compound, smb_ea_apply_done, a);
} /* smb_ea_apply_start */

void
chimera_smb_ea_apply(
    struct chimera_server_smb_thread *thread,
    const struct chimera_vfs_cred *cred,
    struct chimera_vfs_open_handle *handle,
    const uint8_t *ea_buf,
    uint32_t ea_buf_len,
    void ( *done )(uint32_t status, void *arg),
    void *arg)
{
    if (!(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_XATTR)) {
        done(SMB2_STATUS_EAS_NOT_SUPPORTED, arg);
        return;
    }
    if (!ea_buf_len) {
        done(SMB2_STATUS_SUCCESS, arg); return;
    }
    struct chimera_smb_ea_apply *a = calloc(1, sizeof(*a));
    if (!a) {
        done(SMB2_STATUS_INSUFFICIENT_RESOURCES, arg); return;
    }
    a->changes = calloc(ea_buf_len / 9 + 1, sizeof(*a->changes));
    if (!a->changes) {
        free(a); done(SMB2_STATUS_INSUFFICIENT_RESOURCES, arg); return;
    }
    a->thread     = thread->vfs_thread;
    a->cred       = *cred;
    a->ea_buf     = ea_buf;
    a->ea_buf_len = ea_buf_len;
    a->done       = done;
    a->arg        = arg;
    a->handle     = chimera_smb_retain_vfs_handle(thread->vfs_thread, handle);
    smb_ea_apply_start(a);
} /* chimera_smb_ea_apply */

static void
chimera_smb_set_ea_done(
    uint32_t status,
    void    *arg)
{
    struct chimera_smb_request *request = arg;

    if (request->set_info.ea_buf) {
        free(request->set_info.ea_buf);
        request->set_info.ea_buf = NULL;
        request->flags          &= ~CHIMERA_SMB_REQUEST_FLAG_SET_EA_OWNED;
    }

    /* A successful EA change fires FILE_NOTIFY_CHANGE_EA on the parent
     * (smb2.change_notify ChangeEa). */
    if (status == SMB2_STATUS_SUCCESS &&
        request->set_info.open_file->parent_fh_len > 0) {
        struct chimera_server_smb_thread *thread = request->compound->thread;

        chimera_vfs_notify_emit(thread->shared->vfs->vfs_notify,
                                request->set_info.open_file->parent_fh,
                                request->set_info.open_file->parent_fh_len,
                                CHIMERA_VFS_NOTIFY_ATTRS_CHANGED,
                                request->set_info.open_file->name,
                                request->set_info.open_file->name_len,
                                NULL, 0);
    }

    chimera_smb_open_file_release(request, request->set_info.open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_set_ea_done */

static void
chimera_smb_set_ea(struct chimera_smb_request *request)
{
    chimera_smb_ea_apply(request->compound->thread,
                         &request->session_handle->session->cred,
                         request->set_info.open_file->handle,
                         request->set_info.ea_buf,
                         request->set_info.ea_buf_len,
                         chimera_smb_set_ea_done, request);
} /* chimera_smb_set_ea */


/* Readers cover all legacy RENAME/LINK variants, including stream and
 * occupied-target branches. Admission precedes FileId resolution, protocol
 * changes, parent lookup and source DOC fences. A directory writer can then
 * trust that an existing sibling cannot move/link into its checked subtree. */


struct chimera_smb_request;
static void
chimera_smb_set_info_disposition(
    struct chimera_smb_request *request);

static void
smb_set_info_admitted(struct chimera_smb_request *request)
{
    request->set_info.parent_handle                 = NULL;
    request->set_info.rename_info.new_parent_handle = NULL;
    request->set_info.open_file                     = chimera_smb_open_file_resolve(request, &request->set_info.file_id)
    ;
    /* Default change-notify event for this SET_INFO; info classes that mutate
     * size override it below.  Cleared here since the request is pooled. */
    request->set_info.notify_mask = 0;

    if (unlikely(!request->set_info.open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    /* Named-pipe FIDs carry open_file->handle == NULL (see
     * chimera_smb_create_gen_open_file_pipe); every set-info branch below
     * eventually calls chimera_vfs_setattr / chimera_vfs_getattr on that
     * handle and would deref NULL.  SET_INFO is not defined for named pipe
     * FIDs -- reject cleanly with STATUS_INVALID_DEVICE_REQUEST (what a real
     * pipe filesystem returns). */
    if (unlikely(request->set_info.open_file->type == CHIMERA_SMB_OPEN_FILE_TYPE_PIPE)) {
        chimera_smb_open_file_release(request, request->set_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_DEVICE_REQUEST);
        return;
    }

    /* MS-SMB2 §3.3.5.2.10: SET_INFO is a mutating op; reject a stale
     * ChannelSequence with FILE_NOT_AVAILABLE. */
    if (chimera_smb_channel_sequence_stale(request->set_info.open_file,
                                           request->channel_sequence, 1)) {
        chimera_smb_open_file_release(request, request->set_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_NOT_AVAILABLE);
        return;
    }

    switch (request->set_info.info_type) {
        case SMB2_INFO_FILE:
            switch (request->set_info.info_class) {
                case SMB2_FILE_BASIC_INFO:

                    /* FILE_ATTRIBUTE_TEMPORARY is meaningful only for a data
                     * stream; a directory has none, so setting it on a directory
                     * is STATUS_INVALID_PARAMETER (MS-FSCC 2.6 / smb2.create
                     * dosattr_tmp_dir). */
                    if ((request->set_info.open_file->flags &
                         CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) &&
                        (request->set_info.attrs.smb_attributes &
                         SMB2_FILE_ATTRIBUTE_TEMPORARY)) {
                        chimera_smb_open_file_release(request, request->set_info.open_file);
                        chimera_smb_complete_request(request,
                                                     SMB2_STATUS_INVALID_PARAMETER);
                        break;
                    }

                    /* MS-FSA 2.1.5.14.2: a FileBasicInformation timestamp is
                     * either a valid (non-negative) FILETIME or one of the
                     * sentinels 0 (omit), -1 (freeze) or -2 (thaw); any value
                     * below -2 is rejected with STATUS_INVALID_PARAMETER (WPTS
                     * MS-FSAModel SetFileBasicInformation {Creation,LastAccess,
                     * LastWrite,Change}TimeLessthanM1). */
                    if ((int64_t) request->set_info.attrs.smb_crttime < -2 ||
                        (int64_t) request->set_info.attrs.smb_atime < -2 ||
                        (int64_t) request->set_info.attrs.smb_mtime < -2 ||
                        (int64_t) request->set_info.attrs.smb_ctime < -2) {
                        chimera_smb_open_file_release(request, request->set_info.open_file);
                        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
                        break;
                    }

                    chimera_smb_unmarshal_basic_info(&request->set_info.attrs, &request->set_info.vfs_attrs);

                    /* An explicit (non-sentinel) write-time set hands control of
                     * the LastWriteTime to this handle: its own subsequent writes
                     * and size-sets must stop advancing it (MS-FSA sticky mtime). */
                    if (request->set_info.vfs_attrs.va_set_mask & CHIMERA_VFS_ATTR_MTIME) {
                        request->set_info.open_file->flags |= CHIMERA_SMB_OPEN_FILE_WRITE_TIME_STICKY;
                    }

                    chimera_smb_set_info_size(request);
                    break;
                case SMB2_FILE_ENDOFFILE_INFO:

                    /* EndOfFile/Allocation apply to a data stream; a directory
                     * has none, so Windows fails the set with
                     * STATUS_INVALID_PARAMETER (MS-FSCC 2.4.14 / MS-FSA
                     * 2.1.5.14.13; issue #1261). */
                    if (request->set_info.open_file->flags &
                        CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) {
                        chimera_smb_open_file_release(request, request->set_info.open_file);
                        chimera_smb_complete_request(request,
                                                     SMB2_STATUS_INVALID_PARAMETER);
                        break;
                    }

                    /* MS-FSA 2.1.5.16: a SetEndOfFile whose size exceeds the
                     * maximum supported file size is rejected with
                     * STATUS_INVALID_PARAMETER -- the same bound the write path
                     * enforces, which also covers every value that is negative
                     * when read as a signed LONGLONG (WPTS MS-FSAModel
                     * SetFileEndOfFileInformation isEndOfFileGreatThanMaxSize). */
                    if (request->set_info.attrs.smb_size > CHIMERA_SMB_MAX_FILE_SIZE) {
                        chimera_smb_open_file_release(request, request->set_info.open_file);
                        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
                        break;
                    }

                    chimera_smb_unmarshal_end_of_file_info(&request->set_info.attrs, &request->set_info.vfs_attrs);

                    /* A size change fires FILE_NOTIFY_CHANGE_SIZE (and, via the
                     * advanced LastWriteTime below, FILE_NOTIFY_CHANGE_LAST_WRITE). */
                    request->set_info.notify_mask = CHIMERA_VFS_NOTIFY_SIZE_CHANGED |
                        CHIMERA_VFS_NOTIFY_FILE_MODIFIED |
                        CHIMERA_VFS_NOTIFY_STREAM_SIZE;

                    /* Setting EndOfFile advances the LastWriteTime (it changes
                     * the file's data extent), unless this handle has taken
                     * sticky control of the write time. */
                    if (!(request->set_info.open_file->flags & CHIMERA_SMB_OPEN_FILE_WRITE_TIME_STICKY)) {
                        request->set_info.vfs_attrs.va_mtime.tv_nsec = CHIMERA_VFS_TIME_NOW;
                        request->set_info.vfs_attrs.va_req_mask     |= CHIMERA_VFS_ATTR_MTIME;
                        request->set_info.vfs_attrs.va_set_mask     |= CHIMERA_VFS_ATTR_MTIME;
                    }

                    chimera_smb_set_info_size(request);
                    break;
                case SMB2_FILE_ALLOCATION_INFO:

                    /* A directory has no data stream to (de)allocate; reject with
                    * STATUS_INVALID_PARAMETER (MS-FSCC 2.4.4 / MS-FSA; #1261). */
                    if (request->set_info.open_file->flags &
                        CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) {
                        chimera_smb_open_file_release(request, request->set_info.open_file);
                        chimera_smb_complete_request(request,
                                                     SMB2_STATUS_INVALID_PARAMETER);
                        break;
                    }

                    /* AllocationInfo only changes the file when the requested
                     * allocation is below the current EOF, in which case the
                     * file is truncated to it (MS-FSCC §2.4.4).  A truncation
                     * advances LastWriteTime; a grow/hint only touches
                     * ChangeTime.  Both need the current size to decide, so this
                     * is resolved in the getattr callback. */
                    /* MS-FSA 2.1.5.13: a SetAllocation whose size exceeds the
                     * maximum supported file size is rejected with
                     * STATUS_INVALID_PARAMETER (also covers every value negative
                     * when read as a signed LONGLONG) -- WPTS MS-FSAModel
                     * SetFileAllocOrObjIdInformation. */
                    if (request->set_info.attrs.smb_size > CHIMERA_SMB_MAX_FILE_SIZE) {
                        chimera_smb_open_file_release(request, request->set_info.open_file);
                        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
                        break;
                    }

                    request->set_info.notify_mask = CHIMERA_VFS_NOTIFY_SIZE_CHANGED |
                        CHIMERA_VFS_NOTIFY_FILE_MODIFIED |
                        CHIMERA_VFS_NOTIFY_STREAM_SIZE;
                    chimera_smb_unmarshal_end_of_file_info(&request->set_info.attrs, &request->set_info.vfs_attrs);

                    chimera_smb_set_info_allocation(request);
                    break;
                case SMB2_FILE_DISPOSITION_INFO:
                case SMB2_FILE_DISPOSITION_INFO_EX:
                    chimera_smb_set_info_disposition(request);
                    break;
                case SMB2_FILE_RENAME_INFO:
                    chimera_smb_set_info_rename_process(request);
                    break;
                case SMB2_FILE_LINK_INFO:
                    chimera_smb_set_info_link_process(request);
                    break;
                case SMB2_FILE_FULL_EA_INFO:
                    chimera_smb_set_ea(request);
                    break;
                case SMB2_FILE_POSITION_INFO:
                    /* MS-FSA 2.1.5.20: CurrentByteOffset is a signed LONGLONG; a
                     * negative value is rejected with STATUS_INVALID_PARAMETER
                     * (WPTS MS-FSAModel SetFilePositionInformation). */
                    if ((int64_t) request->set_info.attrs.smb_position < 0) {
                        chimera_smb_open_file_release(request, request->set_info.open_file);
                        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
                        break;
                    }
                    request->set_info.open_file->position = request->set_info.attrs.smb_position;
                    chimera_smb_open_file_release(request, request->set_info.open_file);
                    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
                    break;
                case SMB2_FILE_MODE_INFO:
                    /* FileModeInformation (MS-FSCC 2.4.26) is a single Mode DWORD
                     * of per-handle I/O hints (write-through, sequential-only,
                     * no-buffering, ...); chimera tracks none of them, so accept
                     * the set and report success (pike set test_set_file_mode_info). */
                    chimera_smb_open_file_release(request, request->set_info.open_file);
                    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
                    break;
                default:
                    chimera_smb_error("SET_INFO info_class %u not implemented", request->set_info.info_class);
                    chimera_smb_open_file_release(request, request->set_info.open_file);
                    chimera_smb_complete_request(request, SMB2_STATUS_NOT_IMPLEMENTED);
            } /* switch */
            break;
        case SMB2_INFO_SECURITY:
            chimera_smb_set_security(request);
            break;
        default:
            chimera_smb_error("SET_INFO info_type %u not implemented", request->set_info.info_type);
            chimera_smb_open_file_release(request, request->set_info.open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_NOT_IMPLEMENTED);
    } /* switch */
} /* chimera_smb_set_info */

static uint32_t
smb_set_info_namespace_begin(struct chimera_smb_request *request)
{
    if (!request->namespace_open_token) {
        request->namespace_open_token = calloc(1, sizeof(*request->namespace_open_token));
        if (!request->namespace_open_token) {
            return SMB2_STATUS_INSUFFICIENT_RESOURCES;
        }
    }
    return chimera_smb_namespace_open_begin(request->namespace_open_token,
                                            &request->compound->thread->shared->namespace_registry, request->compound) ?
           SMB2_STATUS_SUCCESS : SMB2_STATUS_PENDING;
} /* smb_set_info_namespace_begin */

static void
smb_set_info_namespace_poll(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_smb_request  *request = (void *) ((char *) timer -
                                                     offsetof(struct chimera_smb_request, async.timer));
    struct chimera_smb_compound *wire = request->compound;

    evpl_mutex_lock(&wire->thread->shared->sessions_lock);
    bool                         session_deleted = request->session_handle->session->flags & CHIMERA_SMB_SESSION_DELETED
    ;
    evpl_mutex_unlock(&wire->thread->shared->sessions_lock);
    uint32_t                     status = wire->conn->generation != wire->conn_generation || wire->conn->disconnecting
        ||
        request->tree->compound_tearing_down || session_deleted ? SMB2_STATUS_CANCELLED :
        smb_set_info_namespace_begin(request);
    if (status == SMB2_STATUS_PENDING) {
        evpl_add_oneshot_timer(evpl, timer, smb_set_info_namespace_poll, 1000);
        return;
    }
    request->namespace_mutation_wait = false;
    chimera_smb_async_interim_cancel(request);
    if (status == SMB2_STATUS_SUCCESS) {
        smb_set_info_admitted(request);
    } else {
        chimera_smb_complete_request(request, status);
    }
} /* smb_set_info_namespace_poll */

void
chimera_smb_set_info(struct chimera_smb_request *request)
{
    bool mutation = request->set_info.info_type == SMB2_INFO_FILE &&
        (request->set_info.info_class == SMB2_FILE_RENAME_INFO ||
         request->set_info.info_class == SMB2_FILE_LINK_INFO);

    if (!mutation || request->tree->type == CHIMERA_SMB_TREE_TYPE_PIPE) {
        smb_set_info_admitted(request);
        return;
    }
    /* A parked request has no FileId/open ownership. Context pins survive
     * disconnect/logoff, and CANCEL/drain remove its timer before completion. */
    request->set_info.open_file     = NULL;
    request->set_info.parent_handle = NULL;
    if (!chimera_smb_request_pin_context(request)) {
        chimera_smb_complete_request(request, SMB2_STATUS_INSUFFICIENT_RESOURCES);
        return;
    }
    uint32_t status = smb_set_info_namespace_begin(request);
    if (status == SMB2_STATUS_SUCCESS) {
        smb_set_info_admitted(request); return;
    }
    if (status != SMB2_STATUS_PENDING) {
        chimera_smb_complete_request(request, status);
        return;
    }
    request->namespace_mutation_wait = true;
    chimera_smb_async_interim_begin(request);
    evpl_add_oneshot_timer(request->compound->thread->evpl, &request->async.timer,
                           smb_set_info_namespace_poll, 1000);
} /* chimera_smb_set_info */


void
chimera_smb_set_info_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_SET_INFO_REPLY_SIZE);
} /* chimera_smb_set_info_reply */


SYMBOL_EXPORT int
chimera_smb_parse_set_info(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    int rc = 0;

    if (unlikely(request->request_struct_size != SMB2_SET_INFO_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 SET_INFO request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_SET_INFO_REQUEST_SIZE);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    int prc = 0;
    prc |= evpl_iovec_cursor_try_get_uint8(request_cursor, &request->set_info.info_type);
    prc |= evpl_iovec_cursor_try_get_uint8(request_cursor, &request->set_info.info_class);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->set_info.buffer_length);
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &request->set_info.buffer_offset);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->set_info.addl_info);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->set_info.file_id.pid);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->set_info.file_id.vid);

    if (unlikely(prc)) {
        chimera_smb_error("Received SMB2 SET_INFO request truncated in fixed body");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

    /* Seek to the client-declared input buffer and fence the cursor to exactly
     * buffer_length bytes, so the per-info-class sub-parsers below can read only
     * within the declared buffer (and reject cleanly if it is too short). */
    if (unlikely(smb_cursor_seek_to(request_cursor, request->set_info.buffer_offset) != 0 ||
                 request->set_info.buffer_length > (uint32_t) evpl_iovec_cursor_remaining(request_cursor))) {
        chimera_smb_error("Received SMB2 SET_INFO with input buffer out of range");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }
    evpl_iovec_cursor_set_limit(request_cursor, request->set_info.buffer_length);

    request->set_info.attrs.smb_attr_mask = 0;

    switch (request->set_info.info_type) {
        case SMB2_INFO_FILE:
            switch (request->set_info.info_class) {
                case SMB2_FILE_BASIC_INFO:
                    /* [MS-FSA] 2.1.5.14.2 / [MS-FSCC] 2.4.7: the input buffer
                     * must be at least sizeof(FILE_BASIC_INFORMATION) (40 bytes:
                     * four 8-byte timestamps + 4-byte FileAttributes + 4-byte
                     * Reserved).  A shorter buffer is rejected with
                     * STATUS_INFO_LENGTH_MISMATCH rather than silently accepted
                     * (WPTS MS-FSAModel SetFileBasicInformation cases). */
                    if (unlikely(request->set_info.buffer_length < SMB2_FILE_BASIC_INFO_SIZE)) {
                        return chimera_smb_parse_reject(request, SMB2_STATUS_INFO_LENGTH_MISMATCH);
                    }
                    rc = chimera_smb_parse_basic_info(request_cursor, &request->set_info.attrs);
                    break;
                case SMB2_FILE_DISPOSITION_INFO:
                    if (request->set_info.buffer_length < 1) {
                        return chimera_smb_parse_reject(request, SMB2_STATUS_INFO_LENGTH_MISMATCH);
                    }
                    rc = chimera_smb_parse_disposition_info(request_cursor, &request->set_info.attrs);
                    break;
                case SMB2_FILE_DISPOSITION_INFO_EX:
                    if (request->set_info.buffer_length < 4) {
                        return chimera_smb_parse_reject(request, SMB2_STATUS_INFO_LENGTH_MISMATCH);
                    }
                    rc = chimera_smb_parse_disposition_info_ex(request_cursor, &request->set_info.attrs);
                    break;
                case SMB2_FILE_ENDOFFILE_INFO:
                case SMB2_FILE_ALLOCATION_INFO:
                    rc = chimera_smb_parse_end_of_file_info(
                        request_cursor,
                        &request->set_info.attrs);
                    break;
                case SMB2_FILE_RENAME_INFO:
                case SMB2_FILE_LINK_INFO:
                    rc = chimera_smb_parse_rename_info(request_cursor, request);
                    break;
                case SMB2_FILE_FULL_EA_INFO:
                    /* Capture the client's FILE_FULL_EA_INFORMATION buffer; the
                     * process phase parses and applies it to the VFS xattr
                     * store one EA at a time. */
                    request->set_info.ea_buf     = NULL;
                    request->set_info.ea_buf_len = 0;
                    if (request->set_info.buffer_length > CHIMERA_SMB_EA_VALUE_MAX) {
                        request->status = SMB2_STATUS_EA_TOO_LARGE;
                        rc              = -1;
                        break;
                    }
                    if (request->set_info.buffer_length) {
                        request->set_info.ea_buf = malloc(request->set_info.buffer_length);
                        if (!request->set_info.ea_buf) {
                            return chimera_smb_parse_reject(request,
                                                            SMB2_STATUS_INSUFFICIENT_RESOURCES);
                        }
                        request->flags |= CHIMERA_SMB_REQUEST_FLAG_SET_EA_OWNED;
                        if (evpl_iovec_cursor_try_copy(request_cursor,
                                                       request->set_info.ea_buf,
                                                       request->set_info.buffer_length) != 0) {
                            free(request->set_info.ea_buf);
                            request->set_info.ea_buf = NULL;
                            request->flags          &= ~CHIMERA_SMB_REQUEST_FLAG_SET_EA_OWNED;
                            return chimera_smb_parse_reject(request,
                                                            SMB2_STATUS_INFO_LENGTH_MISMATCH);
                        }
                        request->set_info.ea_buf_len = request->set_info.buffer_length;
                    }
                    break;
                case SMB2_FILE_POSITION_INFO:
                    rc = chimera_smb_parse_position_info(request_cursor, &request->set_info.attrs);
                    break;
                case SMB2_FILE_MODE_INFO:
                    /* FileModeInformation: a 4-byte Mode DWORD (MS-FSCC 2.4.26).
                     * Nothing to capture (the flags are per-handle I/O hints we
                     * do not track); just validate the length. */
                    if (unlikely(request->set_info.buffer_length < SMB2_FILE_MODE_INFO_SIZE)) {
                        return chimera_smb_parse_reject(request,
                                                        SMB2_STATUS_INFO_LENGTH_MISMATCH);
                    }
                    break;

                default:
                    chimera_smb_error("parse_set_info: SET_INFO info_class %u not implemented",
                                      request->set_info.info_class);
                    request->status = SMB2_STATUS_NOT_IMPLEMENTED;
                    rc              = -1;
                    break;
            } /* switch */
            break;
        case SMB2_INFO_SECURITY:
            if (request->set_info.buffer_length <= sizeof(request->set_info.sec_buf)) {
                if (unlikely(evpl_iovec_cursor_try_copy(request_cursor, request->set_info.sec_buf,
                                                        request->set_info.buffer_length) != 0)) {
                    return chimera_smb_parse_reject(request, SMB2_STATUS_INFO_LENGTH_MISMATCH);
                }
                request->set_info.sec_buf_len = request->set_info.buffer_length;
            } else {
                chimera_smb_error("parse_set_info: security descriptor too large (%u bytes)",
                                  request->set_info.buffer_length);
                request->status = SMB2_STATUS_INVALID_PARAMETER;
                rc              = -1;
            }
            break;
        default:
            chimera_smb_error("parse_set_info: SET_INFO info_type %u not implemented", request->set_info.info_type);
            request->status = SMB2_STATUS_NOT_IMPLEMENTED;
            rc              = -1;
            break;
    } /* switch */

    /* A sub-parser that ran off the end of the declared buffer returns -1
     * without setting a status; answer that cleanly as a length mismatch rather
     * than letting the dispatcher tear down the connection. */
    if (rc != 0 && request->status == SMB2_STATUS_SUCCESS) {
        return chimera_smb_parse_reject(request, SMB2_STATUS_INFO_LENGTH_MISMATCH);
    }

    return rc;
} /* chimera_smb_parse_set_info */

struct smb_set_compound {
    struct chimera_vfs_attrs   attrs;
    int                        allocation_getattr;
    uint32_t                   ea_offset;
    int                        ea_list;
    bool                       ea_list_valid;
    struct smb_ea_name_change *ea_changes;
    uint32_t                   ea_change_count;
    uint32_t                   notify_mask;
    uint32_t                   parent_fh_len;
    uint32_t                   name_len;
    uint8_t                    parent_fh[CHIMERA_VFS_FH_SIZE];
    uint8_t                    parent_lease_key[16];
    char                       name[SMB_FILENAME_MAX];
};

static int
smb_set_info_compound_eligible(struct chimera_smb_request *request)
{
    if (request->set_info.info_type != SMB2_INFO_FILE) {
        return 0;
    }
    switch (request->set_info.info_class) {
        case SMB2_FILE_BASIC_INFO:
        case SMB2_FILE_ENDOFFILE_INFO:
        case SMB2_FILE_ALLOCATION_INFO:
        case SMB2_FILE_POSITION_INFO:
        case SMB2_FILE_MODE_INFO:
        case SMB2_FILE_FULL_EA_INFO:
            return 1;
        case SMB2_FILE_LINK_INFO:
            /* First attempt ordinary atomic LINK. ReplaceIfExists only needs
             * destination lifecycle handling when that attempt finds EEXIST. */
            return request->set_info.rename_info.new_name_len > 0 &&
                   request->set_info.rename_info.new_name_len <= CHIMERA_VFS_COMPOUND_NAME_MAX;
        default:
            /* Namespace/disposition need the shared admission/delete journal;
             * named streams keep their dedicated lifecycle for the next wave. */
            return 0;
    } /* switch */
} /* smb_set_info_compound_eligible */

static void
smb_set_ea_compound_next(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command               *command = private_data;
    struct smb_set_compound              *ctx     = command->private_data;
    struct chimera_smb_request           *request = command->request;
    const struct chimera_vfs_compound_op *done    = chimera_vfs_compound_op(compound, index);

    if (*status == CHIMERA_VFS_ENODATA && done->type == CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR) {
        *status = CHIMERA_VFS_OK;
    }
    if (*status != CHIMERA_VFS_OK) {
        command->status = chimera_smb_ea_status(*status);
        return;
    }
    if (done->type == CHIMERA_VFS_COMPOUND_OP_SETXATTR ||
        done->type == CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR) {
        smb_ea_record_name(&ctx->ea_changes[ctx->ea_change_count++], done->name,
                           done->name_len, done->type == CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR);
    }
    if (ctx->ea_offset >= request->set_info.ea_buf_len) {
        return;
    }
    struct chimera_smb_ea_entry entry;
    if (chimera_smb_ea_full_parse_one(request->set_info.ea_buf,
                                      request->set_info.ea_buf_len, &ctx->ea_offset, &entry) ||
        !entry.name_len || !chimera_smb_ea_name_valid(entry.name, entry.name_len) ||
        (entry.flags & ~FILE_NEED_EA)) {
        command->status = SMB2_STATUS_INVALID_EA_NAME;
        *status         = CHIMERA_VFS_EINVAL;
        return;
    }
    const struct chimera_vfs_compound_op *list = chimera_vfs_compound_op(compound, ctx->ea_list);
    char                                  name[CHIMERA_VFS_XATTR_NAME_MAX + 1];
    int                                   length = smb_ea_resolve_name(name, sizeof(name), &entry,
                                                                       ctx->ea_list_valid ? list->buffer : NULL, list->
                                                                       buffer_len,
                                                                       ctx->ea_changes, ctx->ea_change_count);
    if (length < 0) {
        command->status = SMB2_STATUS_INVALID_EA_NAME;
        *status         = CHIMERA_VFS_EINVAL;
        return;
    }
    int added = entry.value_len ? chimera_vfs_compound_add_setxattr(compound,
                                                                    CHIMERA_VFS_XATTR_EITHER,
                                                                    name, length, entry.value,
                                                                    entry.value_len) :
        chimera_vfs_compound_add_removexattr(compound, name, length);
    if (added >= 0) {
        chimera_vfs_compound_op_set_handle(compound, added, command->handle);
        chimera_vfs_compound_set_op_callbacks(compound, added, NULL, smb_set_ea_compound_next, command);
    }
} /* smb_set_ea_compound_next */

static void
smb_set_info_result_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command         *command = private_data;
    struct smb_set_compound        *ctx     = command->private_data;
    struct chimera_smb_request     *request = command->request;
    struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op_args(compound, index);

    (void) status;
    if (request->set_info.info_class == SMB2_FILE_ALLOCATION_INFO) {
        const struct chimera_vfs_compound_op *get =
            chimera_vfs_compound_op(compound, ctx->allocation_getattr);
        if (ctx->attrs.va_size >= get->attr.va_size) {
            ctx->attrs.va_size      = get->attr.va_size;
            ctx->attrs.va_set_mask &= ~CHIMERA_VFS_ATTR_MTIME;
            ctx->attrs.va_req_mask &= ~CHIMERA_VFS_ATTR_MTIME;
        }
    }
    op->set_attr = ctx->attrs;
    if (ctx->attrs.va_set_mask & CHIMERA_VFS_ATTR_SIZE) {
        op->io_owner      = command->actor;
        op->have_io_owner = 1;
    }
} /* smb_set_info_result_prepare */

static void
smb_set_link_parent_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command *command = private_data;

    if (*status != CHIMERA_VFS_OK) {
        command->status = chimera_vfs_compound_op(compound, index)->type ==
            CHIMERA_VFS_COMPOUND_OP_LOOKUP_PATH ? SMB2_STATUS_OBJECT_PATH_NOT_FOUND :
            SMB2_STATUS_INTERNAL_ERROR;
    }
} /* smb_set_link_parent_complete */

static void
smb_set_link_root_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command           *command = private_data;
    struct chimera_smb_namespace_path path;

    smb_doc_command_path(command, &path);
    if (!path.view_fh_len) {
        *status = CHIMERA_VFS_EINVAL; return;
    }
    struct chimera_vfs_compound_op   *op = chimera_vfs_compound_op_args(compound, index);
    op->arg_fh_len = path.view_fh_len;
    memcpy(op->arg_fh, path.view_fh, path.view_fh_len);
} /* smb_set_link_root_prepare */

static void
smb_set_link_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command         *command = private_data;
    struct smb_set_compound        *ctx     = command->private_data;
    struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op_args(compound, index);
    const uint8_t                  *fh      = chimera_vfs_compound_current_fh(compound, &ctx->parent_fh_len);

    if (!fh || !ctx->parent_fh_len) {
        *status = CHIMERA_VFS_EINVAL;
        return;
    }
    memcpy(ctx->parent_fh, fh, ctx->parent_fh_len);
    /* Never turn a wire replacement permission into unguarded VFS replacement.
     * EEXIST is handled by completion before any namespace effect. */
    op->open_opts       = 0;
    op->namespace_flags = CHIMERA_VFS_LINK_NO_NOTIFY;
    memcpy(op->namespace_parent_lease_key, ctx->parent_lease_key, 16);
    op->namespace_parent_lease_key_valid = 1;
    op->io_owner                         = command->actor;
    op->have_io_owner                    = 1;
} /* smb_set_link_prepare */

static int
smb_set_info_compound_build(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    struct chimera_smb_request *request = command->request;
    struct smb_set_compound    *ctx     = calloc(1, sizeof(*ctx));

    command->private_data = ctx;
    if (!ctx) {
        return -1;
    }
    if (request->set_info.info_class == SMB2_FILE_FULL_EA_INFO) {
        ctx->ea_changes = calloc(request->set_info.ea_buf_len / 9 + 1, sizeof(*ctx->ea_changes));
        if (!ctx->ea_changes) {
            return -1;
        }
    }
    if (request->set_info.info_class == SMB2_FILE_LINK_INFO) {
        struct chimera_smb_rename_info *link = &request->set_info.rename_info;
        chimera_vfs_compound_add_savefh(compound);
        /* A related CREATE may resolve a cold tree's root only at execution. */
        uint8_t                         root_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                        root_fh_len;
        chimera_vfs_get_root_fh(root_fh, &root_fh_len);
        int                             root = chimera_vfs_compound_add_putfh(compound, root_fh, root_fh_len);
        chimera_vfs_compound_set_op_prepare(compound, root, smb_set_link_root_prepare, command);
        if (link->new_parent_len) {
            int parent = chimera_vfs_compound_add_lookup_path(compound,
                                                              link->new_parent, link->new_parent_len,
                                                              CHIMERA_VFS_ATTR_FH, 0);
            chimera_vfs_compound_set_op_callbacks(compound, parent, NULL,
                                                  smb_set_link_parent_complete, command);
        }
        int parent = chimera_vfs_compound_add_open_current(compound,
                                                           CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
                                                           CHIMERA_VFS_OPEN_DIRECTORY, 0);
        chimera_vfs_compound_set_op_callbacks(compound, parent, NULL,
                                              smb_set_link_parent_complete, command);
        int result = chimera_vfs_compound_add_link(compound, link->new_name, link->new_name_len, 0, 0, 0);
        chimera_vfs_compound_set_op_prepare(compound, result, smb_set_link_prepare, command);
        return result;
    }
    if (request->set_info.info_class == SMB2_FILE_FULL_EA_INFO) {
        int list = chimera_vfs_compound_add_listxattrs(compound, 0, 4096);
        chimera_vfs_compound_op_set_handle(compound, list, command->handle);
        if (ctx) {
            ctx->ea_list = list;
        }
        return list;
    }
    if (request->set_info.info_class == SMB2_FILE_POSITION_INFO ||
        request->set_info.info_class == SMB2_FILE_MODE_INFO) {
        return chimera_vfs_compound_add_checkpoint(compound);
    }
    if (request->set_info.info_class == SMB2_FILE_ALLOCATION_INFO) {
        int get = chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_SIZE);
        chimera_vfs_compound_op_set_handle(compound, get, command->handle);
        if (ctx) {
            ctx->allocation_getattr = get;
        }
    }
    int op = chimera_vfs_compound_add_setattr(compound, command->handle, NULL, 0, 0);
    chimera_vfs_compound_set_op_prepare(compound, op, smb_set_info_result_prepare, command);
    return op;
} /* smb_set_info_compound_build */

static void
smb_set_info_compound_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command     *command = private_data;
    struct smb_set_compound    *ctx     = command->private_data;
    struct chimera_smb_request *request = command->request;
    struct smb_vfs_open_state  *state   = command->state;
    uint32_t                    access  = 0;

    (void) compound; (void) index; (void) status;
    if (!ctx) {
        command->status = SMB2_STATUS_INSUFFICIENT_RESOURCES;
        return;
    }
    memset(&ctx->attrs, 0, sizeof(ctx->attrs));
    ctx->notify_mask     = 0;
    ctx->ea_offset       = 0;
    ctx->ea_change_count = 0;
    ctx->ea_list_valid   = false;
    struct chimera_smb_namespace_path path;
    smb_doc_command_path(command, &path);
    ctx->parent_fh_len = path.parent_fh_len;
    ctx->name_len      = path.name_len;
    memcpy(ctx->parent_fh, path.parent_fh, ctx->parent_fh_len);
    memcpy(ctx->name, path.name, ctx->name_len);
    memcpy(ctx->parent_lease_key, command->open->parent_lease_key, 16);
    if (state->channel_sequence_valid &&
        (uint16_t) (request->channel_sequence - state->channel_sequence) >= 0x8000) {
        command->status = SMB2_STATUS_FILE_NOT_AVAILABLE;
        return;
    }
    state->channel_sequence       = request->channel_sequence;
    state->channel_sequence_valid = state->sequence_dirty = 1;
    switch (request->set_info.info_class) {
        case SMB2_FILE_BASIC_INFO: access   = SMB2_FILE_WRITE_ATTRIBUTES; break;
        case SMB2_FILE_FULL_EA_INFO: access = SMB2_FILE_WRITE_EA; break;
        case SMB2_FILE_ENDOFFILE_INFO:
        case SMB2_FILE_ALLOCATION_INFO: access = SMB2_FILE_WRITE_DATA; break;
    } /* switch */
    if ((command->open->granted_access & access) != access) {
        command->status = SMB2_STATUS_ACCESS_DENIED;
        return;
    }
    switch (request->set_info.info_class) {
        case SMB2_FILE_LINK_INFO:
            ctx->name_len = request->set_info.rename_info.new_name_len;
            memcpy(ctx->name, request->set_info.rename_info.new_name, ctx->name_len);
            ctx->notify_mask = CHIMERA_VFS_NOTIFY_FILE_ADDED;
            break;
        case SMB2_FILE_FULL_EA_INFO: {
            uint32_t                    offset = 0, needed = 0;
            struct chimera_smb_ea_entry entry;
            while (offset < request->set_info.ea_buf_len) {
                needed++;
                if (chimera_smb_ea_full_parse_one(request->set_info.ea_buf,
                                                  request->set_info.ea_buf_len, &offset, &entry)) {
                    break;
                }
            }
            /* All groups are constructed now, including earlier dynamic tails.
             * Defer before mutation if the complete EA list cannot fit. */
            if (needed > CHIMERA_VFS_COMPOUND_MAX_OPS - chimera_vfs_compound_num_ops(compound)) {
                chimera_smb_compound_defer(command);
                *status = CHIMERA_VFS_EINTR;
                return;
            }
            if (!(command->handle->vfs_module->capabilities & CHIMERA_VFS_CAP_XATTR)) {
                command->status = SMB2_STATUS_EAS_NOT_SUPPORTED;
            }
            ctx->notify_mask = CHIMERA_VFS_NOTIFY_ATTRS_CHANGED;
            break;
        }
        case SMB2_FILE_BASIC_INFO:
            if (((state->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) &&
                 (request->set_info.attrs.smb_attributes & SMB2_FILE_ATTRIBUTE_TEMPORARY)) ||
                (int64_t) request->set_info.attrs.smb_crttime < -2 ||
                (int64_t) request->set_info.attrs.smb_atime < -2 ||
                (int64_t) request->set_info.attrs.smb_mtime < -2 ||
                (int64_t) request->set_info.attrs.smb_ctime < -2) {
                command->status = SMB2_STATUS_INVALID_PARAMETER;
                return;
            }
            chimera_smb_unmarshal_basic_info(&request->set_info.attrs, &ctx->attrs);
            ctx->notify_mask = CHIMERA_VFS_NOTIFY_ATTRS_CHANGED;
            break;
        case SMB2_FILE_ENDOFFILE_INFO:
        case SMB2_FILE_ALLOCATION_INFO:
            if ((state->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) ||
                request->set_info.attrs.smb_size > CHIMERA_SMB_MAX_FILE_SIZE) {
                command->status = SMB2_STATUS_INVALID_PARAMETER;
                return;
            }
            chimera_smb_unmarshal_end_of_file_info(&request->set_info.attrs, &ctx->attrs);
            if (!(state->flags & CHIMERA_SMB_OPEN_FILE_WRITE_TIME_STICKY)) {
                ctx->attrs.va_mtime.tv_nsec = CHIMERA_VFS_TIME_NOW;
                ctx->attrs.va_req_mask     |= CHIMERA_VFS_ATTR_MTIME;
                ctx->attrs.va_set_mask     |= CHIMERA_VFS_ATTR_MTIME;
            }
            command->actor.owner = chimera_smb_open_actor_owner(command->open);
            ctx->notify_mask     = CHIMERA_VFS_NOTIFY_SIZE_CHANGED |
                CHIMERA_VFS_NOTIFY_FILE_MODIFIED | CHIMERA_VFS_NOTIFY_STREAM_SIZE;
            break;
        case SMB2_FILE_POSITION_INFO:
            if ((int64_t) request->set_info.attrs.smb_position < 0) {
                command->status = SMB2_STATUS_INVALID_PARAMETER;
            }
            break;
    } /* switch */
} /* smb_set_info_compound_prepare */

static void
smb_set_info_compound_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command     *command = private_data;
    struct smb_set_compound    *ctx     = command->private_data;
    struct chimera_smb_request *request = command->request;

    if (request->set_info.info_class == SMB2_FILE_LINK_INFO) {
        if (*status == CHIMERA_VFS_EEXIST && request->set_info.rename_info.replace_if_exist) {
            ctx->notify_mask = 0;
            chimera_smb_compound_defer(command);
            *status = CHIMERA_VFS_EINTR;
            return;
        }
        if (*status != CHIMERA_VFS_OK) {
            command->status = chimera_smb_set_info_error_status(*status);
        }
        return;
    }
    if (request->set_info.info_class == SMB2_FILE_FULL_EA_INFO) {
        /* Match existing canonicalization: a failed list does not prevent
         * applying a valid EA list with its requested spelling. */
        ctx->ea_list_valid = *status == CHIMERA_VFS_OK;
        *status            = CHIMERA_VFS_OK;
        smb_set_ea_compound_next(compound, index, status, command);
        return;
    }
    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    if (request->set_info.info_class == SMB2_FILE_BASIC_INFO &&
        (ctx->attrs.va_set_mask & CHIMERA_VFS_ATTR_MTIME)) {
        command->state->flags       |= CHIMERA_SMB_OPEN_FILE_WRITE_TIME_STICKY;
        command->state->flags_dirty |= CHIMERA_SMB_OPEN_FILE_WRITE_TIME_STICKY;
    }
    if (request->set_info.info_class == SMB2_FILE_POSITION_INFO) {
        command->state->position       = request->set_info.attrs.smb_position;
        command->state->position_dirty = 1;
    }
} /* smb_set_info_compound_complete */

static void
smb_set_info_compound_publish(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    struct smb_set_compound          *ctx    = command->private_data;
    struct chimera_server_smb_thread *thread = command->request->compound->thread;

    (void) compound;
    if (command->status != SMB2_STATUS_SUCCESS ||
        !ctx->notify_mask || !ctx->parent_fh_len) {
        return;
    }
    struct chimera_claim_actor        actor = command->actor;
    memcpy(actor.owner.key, ctx->parent_lease_key, 16);
    chimera_vfs_notify_emit_actor(thread->shared->vfs->vfs_notify,
                                  ctx->parent_fh, ctx->parent_fh_len, ctx->notify_mask,
                                  ctx->name, ctx->name_len, NULL, 0,
                                  chimera_claim_owner_has_key(&actor.owner) ? &actor : NULL);
} /* smb_set_info_compound_publish */

static void
smb_set_info_compound_release(struct smb_vfs_command *command)
{
    struct smb_set_compound *ctx = command->private_data;

    if (ctx) {
        free(ctx->ea_changes);
    }
    free(ctx);
    command->private_data = NULL;
} /* smb_set_info_compound_release */

static struct chimera_smb_file_id
smb_set_info_compound_file_id(struct chimera_smb_request *request)
{
    return request->set_info.file_id;
} /* smb_set_info_compound_file_id */

const struct smb_vfs_command_ops chimera_smb_set_info_compound_ops = {
    .file_id  = smb_set_info_compound_file_id,
    .eligible = smb_set_info_compound_eligible,
    .build    = smb_set_info_compound_build,
    .prepare  = smb_set_info_compound_prepare,
    .complete = smb_set_info_compound_complete,
    .publish  = smb_set_info_compound_publish,
    .release  = smb_set_info_compound_release,
};





struct chimera_smb_request;
static void
chimera_smb_set_info_disposition_apply(
    struct chimera_smb_request *request);

struct chimera_smb_request;
static void
chimera_smb_set_info_disposition_fail(
    struct chimera_smb_request *request,
    uint32_t                    status);

struct chimera_vfs_attrs;
static void
chimera_smb_set_info_disposition_getattr(
    enum chimera_vfs_error    error,
    struct chimera_vfs_attrs *attrs,
    void                     *private_data);

static void
chimera_smb_set_info_disposition(struct chimera_smb_request *request)
{
    uint32_t flags = request->set_info.attrs.smb_disposition_flags;

    request->set_info.disposition_validation_handle = NULL;

    /* POSIX deletes the link on the deleting handle's CLOSE, rather than
     * waiting for its peers. Image-section and ON_CLOSE mode updates remain
     * unsupported. */
    if (flags & ~0x1fU) {
        chimera_smb_set_info_disposition_fail(request, SMB2_STATUS_INVALID_PARAMETER);
    } else if (flags & 0x0cU) {
        chimera_smb_set_info_disposition_fail(request, SMB2_STATUS_NOT_SUPPORTED);
    } else if (!request->set_info.attrs.smb_disposition) {
        chimera_smb_set_info_disposition_apply(request);
    } else {
        request->set_info.disposition_validation_handle = chimera_smb_disposition_pin_handle(
            request->compound->thread->vfs_thread, request->set_info.open_file);
        if (!request->set_info.disposition_validation_handle) {
            chimera_smb_set_info_disposition_fail(request, SMB2_STATUS_FILE_CLOSED);
            return;
        }
        chimera_vfs_getattr(request->compound->thread->vfs_thread,
                            &request->session_handle->session->cred,
                            request->set_info.disposition_validation_handle,
                            CHIMERA_VFS_ATTR_DOS_ATTRIBUTES,
                            chimera_smb_set_info_disposition_getattr, request);
    }
} /* chimera_smb_set_info_disposition */

struct chimera_smb_request;
static void
chimera_smb_set_info_disposition_unpin(
    struct chimera_smb_request *request);


static void
chimera_smb_set_info_doc_recall_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data);

static void
chimera_smb_set_info_disposition_apply(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file          *open_file = request->set_info.open_file;
    struct chimera_vfs_file_state         *file      = open_file->share_file_state;
    struct chimera_vfs_thread             *thread    = request->compound->thread->vfs_thread;
    bool                                   stream    = open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM;
    bool                                   deleting  = request->set_info.attrs.smb_disposition;

    struct chimera_smb_namespace_registry *registry =
        &request->compound->thread->shared->namespace_registry;

    chimera_smb_set_info_disposition_unpin(request);
    request->set_info.disposition_recall_handle = NULL;

    /* CLOSE validation and intent publication serialize at this fence. Failed
     * admission has no public effects and never waits with a file/claim lock. */
    if (file && !chimera_smb_doc_mutation_begin(registry, file->fh, file->fh_len, NULL)) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_NOT_AVAILABLE);
        return;
    }

    if (file) {
        evpl_mutex_lock(&file->lock);
    }
    if (!open_file->handle ||
        (stream ? open_file->doc_stream_close_started : open_file->doc_close_started) ||
        (file && file->smb_delete_started)) {
        if (file) {
            evpl_mutex_unlock(&file->lock);
            chimera_smb_doc_mutation_end(registry);
        }
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }
    if (deleting) {
        open_file->flags             |= CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE;
        open_file->doc_posix          = !!(request->set_info.attrs.smb_disposition_flags & 0x02);
        open_file->stream_delete_cred = request->session_handle->session->cred;
        if (stream) {
            open_file->stream_delete_cred = request->session_handle->session->cred;
            if (file) {
                file->delete_pending = 1;
            }
        }
        if (!stream) {
            if (file) {
                file->delete_pending = 1;
            }
            chimera_vfs_set_delete_on_close(thread, open_file->handle,
                                            open_file->parent_fh, open_file->parent_fh_len,
                                            open_file->name, open_file->name_len,
                                            &request->session_handle->session->cred);
        }
    } else {
        open_file->flags &= ~CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE;
        /* Plain FileDispositionInformation clears the shared deletion mark,
         * not the CREATE FILE_DELETE_ON_CLOSE mode (including named streams). */
        open_file->doc_posix = 0;
        if (stream && file) {
            file->delete_pending = 0;
            free(file->smb_pending_delete);
            file->smb_pending_delete = NULL;
        }
        if (!stream) {
            chimera_vfs_clear_delete_on_close(thread, open_file->handle);
            if (file) {
                file->delete_pending = 0;
                free(file->smb_pending_delete);
                file->smb_pending_delete = NULL;
            }
        }
    }
    if (deleting && !stream) {
        request->set_info.disposition_recall_handle =
            chimera_smb_retain_vfs_handle(thread, open_file->handle);
    }
    if (file) {
        evpl_mutex_unlock(&file->lock);
        chimera_smb_doc_mutation_end(registry);
    }
    if (deleting && !stream) {
        chimera_vfs_recall_handle_lease(thread, &request->session_handle->session->cred,
                                        request->set_info.disposition_recall_handle,
                                        chimera_smb_set_info_doc_recall_callback, request);
        return;
    }
    chimera_smb_open_file_release(request, open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_set_info_disposition_apply */

static void
chimera_smb_set_info_disposition_fail(
    struct chimera_smb_request *request,
    uint32_t                    status)
{
    chimera_smb_set_info_disposition_unpin(request);
    chimera_smb_open_file_release(request, request->set_info.open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_set_info_disposition_fail */

struct chimera_vfs_attrs;
static int
chimera_smb_set_info_disposition_entry(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *private_data);

struct chimera_vfs_attrs;
struct chimera_vfs_open_handle;
static void
chimera_smb_set_info_disposition_readdir(
    enum chimera_vfs_error          error,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *attrs,
    void                           *private_data);

static void
chimera_smb_set_info_disposition_getattr(
    enum chimera_vfs_error    error,
    struct chimera_vfs_attrs *attrs,
    void                     *private_data)
{
    struct chimera_smb_request   *request   = private_data;
    struct chimera_smb_open_file *open_file = request->set_info.open_file;
    struct chimera_vfs_thread    *thread    = request->compound->thread->vfs_thread;

    if (error) {
        chimera_smb_set_info_disposition_fail(request, chimera_smb_set_info_error_status(error));
        return;
    }
    if (!(request->set_info.attrs.smb_disposition_flags & 0x10) &&
        (attrs->va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) &&
        (attrs->va_dos_attributes & SMB2_FILE_ATTRIBUTE_READONLY)) {
        chimera_smb_set_info_disposition_fail(request, SMB2_STATUS_CANNOT_DELETE);
        return;
    }
    if ((open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) &&
        !(open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM)) {
        request->set_info.disposition_nonempty = 0;
        chimera_vfs_readdir(thread, &request->session_handle->session->cred,
                            request->set_info.disposition_validation_handle,
                            0, 0, 0, 0, 0, NULL, 0,
                            chimera_smb_set_info_disposition_entry,
                            chimera_smb_set_info_disposition_readdir, request);
        return;
    }
    chimera_smb_set_info_disposition_apply(request);
} /* chimera_smb_set_info_disposition_getattr */

static int
chimera_smb_set_info_disposition_entry(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *private_data)
{
    struct chimera_smb_request *request = private_data;

    if ((namelen == 1 && name[0] == '.') ||
        (namelen == 2 && name[0] == '.' && name[1] == '.')) {
        return 0;
    }
    request->set_info.disposition_nonempty = 1;
    return 1;
} /* chimera_smb_set_info_disposition_entry */

static void
chimera_smb_set_info_disposition_readdir(
    enum chimera_vfs_error          error,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *attrs,
    void                           *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (error) {
        chimera_smb_set_info_disposition_fail(request, chimera_smb_set_info_error_status(error));
    } else if (request->set_info.disposition_nonempty) {
        chimera_smb_set_info_disposition_fail(request, SMB2_STATUS_DIRECTORY_NOT_EMPTY);
    } else if (!eof) {
        /* Do not turn a partial enumeration into a false empty verdict. */
        chimera_smb_set_info_disposition_fail(request, SMB2_STATUS_RETRY);
    } else {
        chimera_smb_set_info_disposition_apply(request);
    }
} /* chimera_smb_set_info_disposition_readdir */

static void
chimera_smb_set_info_disposition_unpin(struct chimera_smb_request *request)
{
    struct chimera_vfs_open_handle *handle = request->set_info.disposition_validation_handle;

    request->set_info.disposition_validation_handle = NULL;
    if (handle) {
        chimera_vfs_release(request->compound->thread->vfs_thread, handle);
    }
} /* chimera_smb_set_info_disposition_unpin */

static void
chimera_smb_set_info_doc_recall_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_smb_request *request = private_data;

    (void) error_code;

    if (request->set_info.disposition_recall_handle) {
        chimera_vfs_release(request->compound->thread->vfs_thread,
                            request->set_info.disposition_recall_handle);
    }
    request->set_info.disposition_recall_handle = NULL;
    chimera_smb_open_file_release(request, request->set_info.open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_set_info_doc_recall_callback */
