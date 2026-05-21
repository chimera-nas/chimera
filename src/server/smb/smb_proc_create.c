// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/time.h>
#include <strings.h>
#include "server/smb/smb2.h"
#include "server/smb/smb_session.h"
#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_string.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_access.h"
#include "vfs/vfs_notify.h"
#include "vfs/vfs_release.h"
#include "smb_attr.h"
#include "smb_lsarpc.h"

#define SMB2_WRITE_MASK       (SMB2_FILE_WRITE_DATA | \
                               SMB2_FILE_APPEND_DATA | \
                               SMB2_FILE_WRITE_EA | \
                               SMB2_FILE_WRITE_ATTRIBUTES | \
                               SMB2_FILE_DELETE_CHILD | \
                               SMB2_FILE_ADD_FILE | \
                               SMB2_FILE_ADD_SUBDIRECTORY | \
                               SMB2_DELETE | \
                               SMB2_WRITE_DACL | \
                               SMB2_WRITE_OWNER | \
                               SMB2_GENERIC_WRITE | \
                               SMB2_GENERIC_ALL)

/* Bits in DesiredAccess that imply the caller will read or write file
 * data.  Without any of these set, the open is metadata-only — common
 * for tools probing for rename/delete — and is satisfiable with an
 * O_PATH-style handle on both files and directories. */
#define SMB2_DATA_ACCESS_MASK (SMB2_FILE_READ_DATA | \
                               SMB2_FILE_WRITE_DATA | \
                               SMB2_FILE_APPEND_DATA | \
                               SMB2_FILE_READ_EA | \
                               SMB2_FILE_WRITE_EA | \
                               SMB2_FILE_EXECUTE | \
                               SMB2_GENERIC_READ | \
                               SMB2_GENERIC_WRITE | \
                               SMB2_GENERIC_EXECUTE | \
                               SMB2_GENERIC_ALL | \
                               SMB2_MAXIMUM_ALLOWED)

/* Map a VFS error from an open-or-create path to the SMB2 status that
 * Windows clients expect.  EISDIR/ENOTDIR are critical here: cmd.exe and
 * other tools probe with FILE_NON_DIRECTORY_FILE first and retry with
 * FILE_DIRECTORY_FILE on STATUS_FILE_IS_A_DIRECTORY — collapsing both to
 * STATUS_OBJECT_NAME_NOT_FOUND breaks directory rename. */
static inline uint32_t
chimera_smb_create_error_status(enum chimera_vfs_error error_code)
{
    switch (error_code) {
        case CHIMERA_VFS_OK:           return SMB2_STATUS_SUCCESS;
        case CHIMERA_VFS_EISDIR:       return SMB2_STATUS_FILE_IS_A_DIRECTORY;
        case CHIMERA_VFS_ENOTDIR:      return SMB2_STATUS_NOT_A_DIRECTORY;
        case CHIMERA_VFS_EEXIST:       return SMB2_STATUS_OBJECT_NAME_COLLISION;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:        return SMB2_STATUS_ACCESS_DENIED;
        case CHIMERA_VFS_ENOSPC:
        case CHIMERA_VFS_EDQUOT:       return SMB2_STATUS_DISK_FULL;
        case CHIMERA_VFS_ENAMETOOLONG: return SMB2_STATUS_NAME_TOO_LONG;
        case CHIMERA_VFS_EROFS:        return SMB2_STATUS_MEDIA_WRITE_PROTECTED;
        default:                       return SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
    } /* switch */
} /* chimera_smb_create_error_status */

static inline struct chimera_smb_open_file *
chimera_smb_create_gen_open_file(
    struct chimera_smb_request     *request,
    enum chimera_smb_open_file_type type,
    chimera_smb_pipe_transceive_t   transceive,
    uint64_t                        pid,
    const void                     *parent_fh,
    int                             parent_fh_len,
    const char                     *name,
    int                             name_len,
    int                             delete_on_close,
    struct chimera_vfs_open_handle *oh)
{
    struct chimera_smb_compound      *compound = request->compound;
    struct chimera_server_smb_thread *thread   = compound->thread;
    struct chimera_smb_tree          *tree     = request->tree;
    struct chimera_smb_open_file     *open_file;
    uint64_t                          open_file_bucket;

    open_file = chimera_smb_open_file_alloc(thread);

    open_file->type = type;

    if (parent_fh_len) {
        memcpy(open_file->parent_fh, parent_fh, parent_fh_len);
    }

    open_file->parent_fh_len   = parent_fh_len;
    open_file->file_id.pid     = pid;
    open_file->file_id.vid     = chimera_rand64();
    open_file->handle          = oh;
    open_file->desired_access  = request->create.desired_access;
    open_file->share_access    = request->create.share_access;
    open_file->flags           = delete_on_close ? CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE : 0;
    open_file->position        = 0;
    open_file->pipe_transceive = transceive;
    open_file->refcnt          = 2;

    /* Phase-0 plumbing state: zeroed; Phases 1/3 will populate from CREATE contexts. */
    open_file->ctx_present_mask   = 0;
    open_file->oplock_level       = 0;
    open_file->lease_state        = 0;
    open_file->lease_epoch        = 0;
    open_file->lease_flags        = 0;
    open_file->durable_flags      = 0;
    open_file->durable_timeout_ms = 0;
    memset(open_file->lease_key,        0, sizeof(open_file->lease_key));
    memset(open_file->parent_lease_key, 0, sizeof(open_file->parent_lease_key));
    memset(open_file->create_guid,      0, sizeof(open_file->create_guid));

    open_file->name_len = name_len;
    memcpy(open_file->name, name, open_file->name_len);

    /* Check share mode conflicts for regular file opens.
     * Attribute-only opens (READ_ATTRIBUTES, SYNCHRONIZE, etc.)
     * bypass share mode enforcement, matching Windows/NTFS behavior.
     * Generic rights (MAXIMUM_ALLOWED, GENERIC_READ, etc.) expand to
     * data-level access inside the conflict check and must participate.
     *
     * Stage C: route through the unified vfs_state SHARE layer.  The
     * legacy per-tree sharemode table is bypassed entirely — its
     * behavior matrix is preserved by chimera_vfs_share_conflict in
     * vfs_state.c. */
    if (type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE && tree->share &&
        (open_file->desired_access & SMB2_SHAREMODE_ACCESS_MASK) && oh) {
        struct chimera_vfs_state      *vfs_state = thread->vfs_thread->vfs->vfs_state;
        struct chimera_vfs_file_state *file_state;
        uint32_t                       da = open_file->desired_access;
        uint32_t                       sa = open_file->share_access;
        uint8_t                        granted = 0, denied = 0;
        struct chimera_vfs_lease      *conflict = NULL;
        enum chimera_vfs_lease_result  result;

        /* Map desired_access -> RWH grant.  Only data-access rights
         * participate in share-mode conflicts (matching Windows and the
         * legacy smb_sharemode_check_conflict): READ_DATA/EXECUTE are
         * "read", WRITE_DATA/APPEND_DATA are "write", DELETE is "delete".
         * EA and attribute rights (READ_EA, WRITE_EA, *_ATTRIBUTES) do NOT
         * count.  Generic + MAXIMUM_ALLOWED expand to the data rights they
         * imply, as smb_sharemode_expand_access used to do. */
        if (da & (SMB2_FILE_READ_DATA | SMB2_FILE_EXECUTE |
                  SMB2_GENERIC_READ | SMB2_GENERIC_EXECUTE |
                  SMB2_GENERIC_ALL | SMB2_MAXIMUM_ALLOWED)) {
            granted |= CHIMERA_VFS_LEASE_MODE_R;
        }
        if (da & (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA |
                  SMB2_GENERIC_WRITE |
                  SMB2_GENERIC_ALL | SMB2_MAXIMUM_ALLOWED)) {
            granted |= CHIMERA_VFS_LEASE_MODE_W;
        }
        if (da & (SMB2_DELETE | SMB2_GENERIC_ALL | SMB2_MAXIMUM_ALLOWED)) {
            granted |= CHIMERA_VFS_LEASE_MODE_D;
        }

        /* Map share_access -> RWH deny.  Each access bit NOT shared
         * becomes a deny on the corresponding bit. */
        if (!(sa & SMB2_FILE_SHARE_READ)) {
            denied |= CHIMERA_VFS_LEASE_MODE_R;
        }
        if (!(sa & SMB2_FILE_SHARE_WRITE)) {
            denied |= CHIMERA_VFS_LEASE_MODE_W;
        }
        if (!(sa & SMB2_FILE_SHARE_DELETE)) {
            denied |= CHIMERA_VFS_LEASE_MODE_D;
        }

        file_state = chimera_vfs_state_get(vfs_state,
                                           oh->fh, oh->fh_len,
                                           oh->fh_hash, true);
        if (!file_state) {
            open_file->handle = NULL;
            chimera_smb_open_file_free(thread, open_file);
            return NULL;
        }

        open_file->share_lease.kind             = CHIMERA_VFS_LEASE_SHARE;
        open_file->share_lease.mode.granted     = granted;
        open_file->share_lease.mode.denied      = denied;
        open_file->share_lease.owner.protocol   = CHIMERA_VFS_LEASE_PROTO_SMB2;
        open_file->share_lease.owner.client_key = request->session_handle->session->session_id;
        /* The open itself is the owner — different opens, even by the
         * same client, must satisfy share-mode constraints between
         * themselves. */
        open_file->share_lease.owner.owner_lo = open_file->file_id.pid;
        open_file->share_lease.owner.owner_hi = open_file->file_id.vid;

        result = chimera_vfs_state_try_insert(vfs_state, file_state,
                                              &open_file->share_lease, &conflict);
        if (result != CHIMERA_VFS_LEASE_GRANTED) {
            chimera_vfs_state_put(vfs_state, file_state);
            open_file->handle = NULL;
            chimera_smb_open_file_free(thread, open_file);
            return NULL;
        }

        open_file->share_file_state     = file_state;
        open_file->share_lease_inserted = true;
    }

    /* Acquire a CACHING lease (SMB2 lease via RqLs, or legacy oplock
     * via requested_oplock_level).  This is opportunistic — failure to
     * grant just means the open succeeds with no lease.  Conflicts are
     * resolved by vfs_state's conflict matrix; without break_cb wired
     * (next task in Stage D), conflicting other-client leases force
     * DENIED here, so we silently end up with no lease. */
    if (type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE && oh) {
        struct chimera_vfs_state      *vfs_state = thread->vfs_thread->vfs->vfs_state;
        struct chimera_vfs_file_state *file_state;
        uint8_t                        req_smb  = 0;
        uint8_t                        req_vfs  = 0;
        bool                           via_rqls = false;
        struct chimera_vfs_lease      *conflict = NULL;
        enum chimera_vfs_lease_result  result;

        if (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) {
            via_rqls = true;
            req_smb  = (uint8_t) (request->create.rqls.state & 0x07);
        } else {
            switch (request->create.requested_oplock_level) {
                case SMB2_OPLOCK_LEVEL_II:
                    req_smb = SMB2_LEASE_READ_CACHING;
                    break;
                case SMB2_OPLOCK_LEVEL_EXCLUSIVE:
                    req_smb = SMB2_LEASE_READ_CACHING |
                        SMB2_LEASE_WRITE_CACHING;
                    break;
                case SMB2_OPLOCK_LEVEL_BATCH:
                    req_smb = SMB2_LEASE_READ_CACHING |
                        SMB2_LEASE_WRITE_CACHING |
                        SMB2_LEASE_HANDLE_CACHING;
                    break;
                default:
                    req_smb = 0;
                    break;
            } /* switch */
        }

        /* SMB lease bits use R=0x01, H=0x02, W=0x04 — different layout
         * from vfs_state's R/W/H mask, so map field-by-field. */
        if (req_smb & SMB2_LEASE_READ_CACHING) {
            req_vfs |= CHIMERA_VFS_LEASE_MODE_R;
        }
        if (req_smb & SMB2_LEASE_HANDLE_CACHING) {
            req_vfs |= CHIMERA_VFS_LEASE_MODE_H;
        }
        if (req_smb & SMB2_LEASE_WRITE_CACHING) {
            req_vfs |= CHIMERA_VFS_LEASE_MODE_W;
        }

        if (req_vfs != 0) {
            file_state = chimera_vfs_state_get(vfs_state,
                                               oh->fh, oh->fh_len,
                                               oh->fh_hash, true);
            if (file_state) {
                open_file->caching_lease.kind             = CHIMERA_VFS_LEASE_CACHING;
                open_file->caching_lease.mode.granted     = req_vfs;
                open_file->caching_lease.owner.protocol   = CHIMERA_VFS_LEASE_PROTO_SMB2;
                open_file->caching_lease.owner.client_key = request->session_handle->session->session_id;
                /* For RqLs, owner identity is the lease_key (so multiple
                 * opens with the same key by the same client coalesce —
                 * the Samba locking.tdb rule).  For legacy oplocks,
                 * each open is its own owner (use file_id). */
                if (via_rqls) {
                    memcpy(&open_file->caching_lease.owner.owner_lo,
                           request->create.rqls.key, 8);
                    memcpy(&open_file->caching_lease.owner.owner_hi,
                           request->create.rqls.key + 8, 8);
                    /* Mirror the lease_key onto the open_file for the
                     * break notification builder. */
                    memcpy(open_file->lease_key, request->create.rqls.key, 16);
                    if (request->create.rqls.is_v2) {
                        memcpy(open_file->parent_lease_key,
                               request->create.rqls.parent_key, 16);
                        open_file->lease_epoch = request->create.rqls.epoch;
                    }
                } else {
                    open_file->caching_lease.owner.owner_lo = open_file->file_id.pid;
                    open_file->caching_lease.owner.owner_hi = open_file->file_id.vid;
                }
                /* Wire the break callback so vfs_state can notify the
                 * client when another acquirer needs this lease. */
                open_file->caching_lease.owner.break_cb   = chimera_smb_lease_break_cb;
                open_file->caching_lease.owner.cb_private = open_file;
                open_file->create_conn                    = request->compound->conn;

                result = chimera_vfs_state_try_insert(vfs_state, file_state,
                                                      &open_file->caching_lease,
                                                      &conflict);
                if (result == CHIMERA_VFS_LEASE_GRANTED) {
                    open_file->caching_file_state     = file_state;
                    open_file->caching_lease_inserted = true;
                    /* Record granted SMB-side state on the open_file.
                     * lease_state stores the SMB lease bits, which is
                     * what the response context emitter wants. */
                    open_file->lease_state = 0;
                    if (req_vfs & CHIMERA_VFS_LEASE_MODE_R) {
                        open_file->lease_state |= SMB2_LEASE_READ_CACHING;
                    }
                    if (req_vfs & CHIMERA_VFS_LEASE_MODE_H) {
                        open_file->lease_state |= SMB2_LEASE_HANDLE_CACHING;
                    }
                    if (req_vfs & CHIMERA_VFS_LEASE_MODE_W) {
                        open_file->lease_state |= SMB2_LEASE_WRITE_CACHING;
                    }
                    if (via_rqls) {
                        open_file->oplock_level = SMB2_OPLOCK_LEVEL_LEASE;
                    } else {
                        /* Translate granted RWH back to legacy oplock
                         * level.  Without H, the closest legacy match
                         * is EXCLUSIVE for RW or II for R only. */
                        if ((req_vfs & (CHIMERA_VFS_LEASE_MODE_R |
                                        CHIMERA_VFS_LEASE_MODE_W |
                                        CHIMERA_VFS_LEASE_MODE_H)) ==
                            (CHIMERA_VFS_LEASE_MODE_R |
                             CHIMERA_VFS_LEASE_MODE_W |
                             CHIMERA_VFS_LEASE_MODE_H)) {
                            open_file->oplock_level = SMB2_OPLOCK_LEVEL_BATCH;
                        } else if (req_vfs & CHIMERA_VFS_LEASE_MODE_W) {
                            open_file->oplock_level = SMB2_OPLOCK_LEVEL_EXCLUSIVE;
                        } else {
                            open_file->oplock_level = SMB2_OPLOCK_LEVEL_II;
                        }
                    }
                } else {
                    chimera_vfs_state_put(vfs_state, file_state);
                    open_file->lease_state  = 0;
                    open_file->oplock_level = SMB2_OPLOCK_LEVEL_NONE;
                }
            }
        }
    }

    /* Propagate delete-on-close to the VFS handle so the file is
    * removed when the last reference (opencnt) drops to zero. */
    if (delete_on_close && oh) {
        chimera_vfs_set_delete_on_close(thread->vfs_thread, oh,
                                        parent_fh, parent_fh_len,
                                        name, name_len,
                                        &request->session_handle->session->cred);
    }

    open_file_bucket = open_file->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    pthread_mutex_lock(&tree->open_files_lock[open_file_bucket]);

    HASH_ADD(hh, tree->open_files[open_file_bucket], file_id, sizeof(open_file->file_id), open_file);

    pthread_mutex_unlock(&tree->open_files_lock[open_file_bucket]);

    compound->saved_file_id = open_file->file_id;

    return open_file;
} /* chimera_smb_create_gen_open_file */


static inline struct chimera_smb_open_file *
chimera_smb_create_gen_open_file_normal(
    struct chimera_smb_request     *request,
    const void                     *parent_fh,
    int                             parent_fh_len,
    const char                     *name,
    int                             name_len,
    int                             delete_on_close,
    int                             is_directory,
    struct chimera_vfs_open_handle *oh)
{
    struct chimera_smb_open_file *open_file;

    open_file = chimera_smb_create_gen_open_file(request,
                                                 CHIMERA_SMB_OPEN_FILE_TYPE_FILE,
                                                 NULL,
                                                 ++request->tree->next_file_id,
                                                 parent_fh,
                                                 parent_fh_len,
                                                 name,
                                                 name_len,
                                                 delete_on_close, oh);

    if (open_file && is_directory) {
        open_file->flags |= CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY;
    }

    return open_file;
} /* chimera_smb_create_gen_open_file_normal */

static inline struct chimera_smb_open_file *
chimera_smb_create_gen_open_file_pipe(
    struct chimera_smb_request   *request,
    enum chimera_smb_pipe_magic   pipe_magic,
    chimera_smb_pipe_transceive_t transceive,
    const char                   *name,
    int                           name_len)
{
    return chimera_smb_create_gen_open_file(request,
                                            CHIMERA_SMB_OPEN_FILE_TYPE_PIPE,
                                            transceive,
                                            pipe_magic,
                                            NULL,
                                            0,
                                            name,
                                            name_len,
                                            0,
                                            NULL);
} /* chimera_smb_create_gen_open_file_pipe */

static inline void
chimera_smb_create_mkdir_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request   *request    = private_data;
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_release(vfs_thread, request->create.parent_handle);
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
        return;
    }

    open_file = chimera_smb_create_gen_open_file_normal(request,
                                                        request->create.parent_handle->fh,
                                                        request->create.parent_handle->fh_len,
                                                        request->create.name,
                                                        request->create.name_len,
                                                        request->create.create_options & SMB2_FILE_DELETE_ON_CLOSE,
                                                        1,
                                                        oh);

    if (!open_file) {
        chimera_vfs_release(vfs_thread, oh);
        chimera_vfs_release(vfs_thread, request->create.parent_handle);
        chimera_smb_complete_request(request, SMB2_STATUS_SHARING_VIOLATION);
        return;
    }

    request->create.r_open_file = open_file;

    chimera_vfs_release(vfs_thread, request->create.parent_handle);
    chimera_smb_open_file_release(request, open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);

} /* chimera_smb_create_mkdir_open_callback */

static inline void
chimera_smb_create_open_at_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data);

static inline uint32_t
chimera_smb_create_check_access(
    struct chimera_smb_request     *request,
    const struct chimera_vfs_attrs *attr);

static inline uint32_t
chimera_smb_create_granted_access(
    struct chimera_smb_request     *request,
    const struct chimera_vfs_attrs *attr);

static inline void
chimera_smb_create_mkdir_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{

    struct chimera_smb_request       *request    = private_data;
    struct chimera_smb_compound      *compound   = request->compound;
    struct chimera_server_smb_thread *thread     = compound->thread;
    struct chimera_vfs_thread        *vfs_thread = thread->vfs_thread;

    if (error_code != CHIMERA_VFS_OK) {
        if (error_code == CHIMERA_VFS_EEXIST &&
            request->create.create_disposition == SMB2_FILE_OPEN_IF) {
            /* Directory already exists — fall through to open it */
            chimera_vfs_open_at(
                vfs_thread,
                &request->session_handle->session->cred,
                request->create.parent_handle,
                request->create.name,
                request->create.name_len,
                CHIMERA_VFS_OPEN_DIRECTORY,
                &request->create.set_attr,
                CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                0,
                0,
                chimera_smb_create_open_at_callback,
                request);
            return;
        }
        chimera_vfs_release(vfs_thread, request->create.parent_handle);
        chimera_smb_complete_request(request, error_code == CHIMERA_VFS_EEXIST ?
                                     SMB2_STATUS_OBJECT_NAME_COLLISION :
                                     SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
        return;
    }

    chimera_smb_marshal_attrs(
        attr,
        &request->create.r_attrs);

    request->create.r_attrs.smb_attributes |= SMB2_FILE_ATTRIBUTE_DIRECTORY;

    chimera_vfs_open_fh(
        vfs_thread,
        &request->session_handle->session->cred,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH,
        chimera_smb_create_mkdir_open_callback,
        request);

} /* chimera_smb_create_mkdir_callback */

static inline void
chimera_smb_create_open_at_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_smb_request   *request    = private_data;
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_release(vfs_thread, request->create.parent_handle);
        chimera_smb_complete_request(request, chimera_smb_create_error_status(error_code));
        return;
    }

    /* Enforce the requested access against the object's ACL for every
     * disposition that can open an existing object.  Pure FILE_CREATE always
     * makes a new object (and fails with a collision otherwise), so the creator
     * implicitly holds it and we do not gate it here.  A newly-created object
     * reached via OPEN_IF/OVERWRITE_IF/SUPERSEDE carries the owner-full-control
     * default (or inherited) ACL, so the creator passes this check naturally. */
    if (request->create.create_disposition != SMB2_FILE_CREATE &&
        chimera_smb_create_check_access(request, attr) != SMB2_STATUS_SUCCESS) {
        chimera_vfs_release(vfs_thread, oh);
        chimera_vfs_release(vfs_thread, request->create.parent_handle);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    open_file = chimera_smb_create_gen_open_file_normal(request,
                                                        request->create.parent_handle->fh,
                                                        request->create.parent_handle->fh_len,
                                                        request->create.name,
                                                        request->create.name_len,
                                                        request->create.create_options & SMB2_FILE_DELETE_ON_CLOSE,
                                                        S_ISDIR(attr->va_mode),
                                                        oh);

    if (!open_file) {
        chimera_vfs_release(vfs_thread, oh);
        chimera_vfs_release(vfs_thread, request->create.parent_handle);
        chimera_smb_complete_request(request, SMB2_STATUS_SHARING_VIOLATION);
        return;
    }

    request->create.r_open_file = open_file;

    open_file->granted_access = chimera_smb_create_granted_access(request, attr);
    open_file->maximal_access =
        chimera_vfs_access_check(attr, &request->session_handle->session->cred,
                                 CHIMERA_ACE_MASK_ALL);

    chimera_smb_marshal_attrs(
        attr,
        &request->create.r_attrs);

    /* Emit notification on parent directory for any disposition that can
     * create a new file.  OPEN never creates; CREATE always creates;
     * OPEN_IF / OVERWRITE_IF / SUPERSEDE may create or may open/truncate
     * an existing file.  We emit ADDED for all create-capable
     * dispositions — this can yield a spurious ADDED when an existing
     * file was opened or truncated, but that is preferable to missing
     * notifications for newly-created files (Windows CREATE_ALWAYS maps
     * to OVERWRITE_IF and is the common case).
     *
     * Pick FILE_ADDED vs DIR_ADDED based on the result attrs.  A
     * directory creation (FILE_DIRECTORY_FILE create option) must
     * emit DIR_ADDED so SMB clients filtering on DIR_NAME only
     * receive the event. */
    if (request->create.create_disposition == SMB2_FILE_CREATE        ||
        request->create.create_disposition == SMB2_FILE_OPEN_IF       ||
        request->create.create_disposition == SMB2_FILE_OVERWRITE_IF  ||
        request->create.create_disposition == SMB2_FILE_SUPERSEDE) {
        struct chimera_server_smb_thread *thread = request->compound->thread;
        uint32_t                          action = S_ISDIR(attr->va_mode) ?
            CHIMERA_VFS_NOTIFY_DIR_ADDED : CHIMERA_VFS_NOTIFY_FILE_ADDED;

        chimera_vfs_notify_emit(thread->shared->vfs->vfs_notify,
                                request->create.parent_handle->fh,
                                request->create.parent_handle->fh_len,
                                action,
                                request->create.name,
                                request->create.name_len,
                                NULL, 0);
    }

    chimera_vfs_release(vfs_thread, request->create.parent_handle);
    chimera_smb_open_file_release(request, open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_create_open_at_callback */

/*
 * Map the CREATE DesiredAccess to canonical access-mask bits and evaluate it
 * against the opened object's ACL via the shared engine.  Returns
 * SMB2_STATUS_SUCCESS when every requested right is granted, otherwise
 * SMB2_STATUS_ACCESS_DENIED.  The SMB2 specific + standard access bits share
 * the canonical ACE mask layout exactly, so they map straight through; the four
 * NT generic bits are expanded.  MAXIMUM_ALLOWED / ACCESS_SYSTEM_SECURITY are
 * not gated here.  `attr` must carry the ACL (request CHIMERA_VFS_ATTR_ACL).
 */
static inline uint32_t
chimera_smb_create_check_access(
    struct chimera_smb_request     *request,
    const struct chimera_vfs_attrs *attr)
{
    uint32_t da = request->create.desired_access;
    uint32_t req;
    uint32_t granted;

    /* Requested rights, with the four NT generic bits expanded to their
     * specific rights and the generic bits themselves dropped.  MAXIMUM_ALLOWED
     * and ACCESS_SYSTEM_SECURITY are not gated here.  Any other requested bit
     * (including reserved/undefined ones) stays in `req`, so an open asking for
     * a right the object does not grant is denied -- the SMB2 specific+standard
     * bits share the canonical ACE mask layout exactly. */
    /* DELETE is a parent-directory operation: deleting a name is governed by
     * the parent's FILE_DELETE_CHILD (or the file's own DELETE), mirroring POSIX
     * where directory write/execute governs unlink.  We do not gate DELETE at
     * the file-open level here -- doing so would wrongly deny an owner removing
     * a child whose inherited ACL omits DELETE.  (Full parent DELETE_CHILD
     * evaluation is a follow-up.) */
    req = da & ~(SMB2_MAXIMUM_ALLOWED | SMB2_ACCESS_SYSTEM_SECURITY |
                 SMB2_DELETE |
                 SMB2_GENERIC_READ | SMB2_GENERIC_WRITE |
                 SMB2_GENERIC_EXECUTE | SMB2_GENERIC_ALL);

    if (da & SMB2_GENERIC_READ) {
        req |= 0x00120089; /* READ_DATA|READ_NAMED_ATTRS|READ_ATTRS|READ_ACL|SYNC */
    }
    if (da & SMB2_GENERIC_WRITE) {
        req |= 0x00120116; /* WRITE_DATA|APPEND|WRITE_NAMED_ATTRS|WRITE_ATTRS|READ_ACL|SYNC */
    }
    if (da & SMB2_GENERIC_EXECUTE) {
        req |= 0x001200a0; /* EXECUTE|READ_ATTRS|READ_ACL|SYNC */
    }
    if (da & SMB2_GENERIC_ALL) {
        req |= CHIMERA_ACE_MASK_ALL;
    }

    if (!req) {
        return SMB2_STATUS_SUCCESS;
    }

    /* Evaluate the full grantable universe and require every requested bit to
     * be present; a requested right outside what the ACL grants (e.g. an
     * undefined specific bit) is therefore denied. */
    granted = chimera_vfs_access_check(
        attr, &request->session_handle->session->cred, CHIMERA_ACE_MASK_ALL);

    return (req & ~granted) == 0 ?
           SMB2_STATUS_SUCCESS : SMB2_STATUS_ACCESS_DENIED;
} /* chimera_smb_create_check_access */

/*
 * Resolve the access mask granted on a successful open, for the open handle's
 * FileAccessInformation / MxAc reporting.  A MAXIMUM_ALLOWED open is granted the
 * full set the caller's ACL evaluation yields; a specific-bits open is granted
 * exactly the bits it asked for (the open already passed the access check).
 */
static inline uint32_t
chimera_smb_create_granted_access(
    struct chimera_smb_request     *request,
    const struct chimera_vfs_attrs *attr)
{
    uint32_t da = request->create.desired_access;
    uint32_t g;

    if (da & SMB2_MAXIMUM_ALLOWED) {
        return chimera_vfs_access_check(
            attr, &request->session_handle->session->cred,
            CHIMERA_ACE_MASK_ALL);
    }

    /* GrantedAccess is reported in resolved specific rights, never the NT
     * generic bits; expand them.  A specific-bits open that reached here was
     * granted everything it asked for. */
    g = da & ~(SMB2_MAXIMUM_ALLOWED | SMB2_ACCESS_SYSTEM_SECURITY |
               SMB2_GENERIC_READ | SMB2_GENERIC_WRITE |
               SMB2_GENERIC_EXECUTE | SMB2_GENERIC_ALL);

    if (da & SMB2_GENERIC_READ) {
        g |= 0x00120089;
    }
    if (da & SMB2_GENERIC_WRITE) {
        g |= 0x00120116;
    }
    if (da & SMB2_GENERIC_EXECUTE) {
        g |= 0x001200a0;
    }
    if (da & SMB2_GENERIC_ALL) {
        g |= CHIMERA_ACE_MASK_ALL;
    }

    return g;
} /* chimera_smb_create_granted_access */

static inline void
chimera_smb_create_open_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, request->create.r_open_file);
        /* XXX open file */
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
        return;
    }

    if (S_ISDIR(attr->va_mode)) {
        request->create.r_open_file->flags |= CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY;
    }

    if (chimera_smb_create_check_access(request, attr) != SMB2_STATUS_SUCCESS) {
        chimera_smb_open_file_release(request, request->create.r_open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    request->create.r_open_file->granted_access =
        chimera_smb_create_granted_access(request, attr);
    request->create.r_open_file->maximal_access =
        chimera_vfs_access_check(attr, &request->session_handle->session->cred,
                                 CHIMERA_ACE_MASK_ALL);

    chimera_smb_open_file_release(request, request->create.r_open_file);

    chimera_smb_marshal_attrs(
        attr,
        &request->create.r_attrs);

    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);

} /* chimera_smb_create_open_getattr_callback */

static inline void
chimera_smb_create_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request       *request    = private_data;
    struct chimera_smb_compound      *compound   = request->compound;
    struct chimera_server_smb_thread *thread     = compound->thread;
    struct chimera_vfs_thread        *vfs_thread = thread->vfs_thread;
    struct chimera_smb_open_file     *open_file;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_complete_request(request, chimera_smb_create_error_status(error_code));
        return;
    }

    open_file = chimera_smb_create_gen_open_file_normal(request,
                                                        NULL, 0,
                                                        request->create.name,
                                                        request->create.name_len * 2,
                                                        request->create.create_options & SMB2_FILE_DELETE_ON_CLOSE,
                                                        request->create.create_options & SMB2_FILE_DIRECTORY_FILE,
                                                        oh);

    if (!open_file) {
        chimera_vfs_release(vfs_thread, oh);
        chimera_smb_complete_request(request, SMB2_STATUS_SHARING_VIOLATION);
        return;
    }

    request->create.r_open_file = open_file;

    chimera_vfs_getattr(vfs_thread,
                        &request->session_handle->session->cred,
                        oh,
                        CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT |
                        CHIMERA_VFS_ATTR_ACL,
                        chimera_smb_create_open_getattr_callback,
                        request);

} /* chimera_smb_create_open_at_callback */


static inline void
chimera_smb_create_open_parent_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request       *request    = private_data;
    struct chimera_server_smb_thread *thread     = request->compound->thread;
    struct chimera_vfs_thread        *vfs_thread = thread->vfs_thread;
    unsigned int                      flags      = 0;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_error("Open parent error_code %d", error_code);
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_PATH_NOT_FOUND);
        return;
    }

    request->create.parent_handle = oh;

    if ((request->create.create_options & SMB2_FILE_DIRECTORY_FILE) &&
        (request->create.create_disposition == SMB2_FILE_CREATE ||
         request->create.create_disposition == SMB2_FILE_OPEN_IF)) {

        chimera_vfs_mkdir_at(
            vfs_thread,
            &request->session_handle->session->cred,
            oh,
            request->create.name,
            request->create.name_len,
            &request->create.set_attr,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
            0,
            0,
            chimera_smb_create_mkdir_callback,
            request);

    } else {
        if (request->create.create_options & SMB2_FILE_DIRECTORY_FILE) {
            flags |= CHIMERA_VFS_OPEN_DIRECTORY;
        }

        /* Metadata-only open: when the caller doesn't request any
         * data-access bits, satisfy the open with an O_PATH-style
         * handle.  This lets tools (e.g. cmd.exe `ren`, Explorer) open
         * a directory with DELETE | READ_ATTRIBUTES | SYNCHRONIZE plus
         * FILE_NON_DIRECTORY_FILE — Windows servers honor this opening
         * pattern for metadata operations like rename and delete-on-
         * close, and refusing it breaks directory rename over SMB. */
        if (!(request->create.desired_access & SMB2_DATA_ACCESS_MASK) &&
            request->create.create_disposition == SMB2_FILE_OPEN) {
            flags |= CHIMERA_VFS_OPEN_PATH;
        }

        if (!(request->create.desired_access & SMB2_WRITE_MASK)) {
            flags |= CHIMERA_VFS_OPEN_READ_ONLY;
        }

        if ((request->create.create_options & SMB2_FILE_OPEN_REPARSE_POINT) &&
            request->create.create_disposition == SMB2_FILE_OPEN) {
            flags |= CHIMERA_VFS_OPEN_NOFOLLOW;
        }

        switch (request->create.create_disposition) {
            case SMB2_FILE_SUPERSEDE:
                break;
            case SMB2_FILE_OPEN:
                break;
            case SMB2_FILE_OPEN_IF:
                flags |= CHIMERA_VFS_OPEN_CREATE;
                break;
            case SMB2_FILE_CREATE:
                flags |= CHIMERA_VFS_OPEN_CREATE;
                break;
            case SMB2_FILE_OVERWRITE:
                flags |= CHIMERA_VFS_OPEN_CREATE;
                break;
            case SMB2_FILE_OVERWRITE_IF:
                flags |= CHIMERA_VFS_OPEN_CREATE;
                break;
        } /* switch */

        chimera_vfs_open_at(
            vfs_thread,
            &request->session_handle->session->cred,
            oh,
            request->create.name,
            request->create.name_len,
            flags,
            &request->create.set_attr,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT |
            CHIMERA_VFS_ATTR_ACL,
            0,
            0,
            chimera_smb_create_open_at_callback,
            request);
    }

} /* chimera_smb_create_open_at_callback */

static inline void
chimera_smb_create_lookup_parent_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_PATH_NOT_FOUND);
        return;
    }

    chimera_vfs_open_fh(
        vfs_thread,
        &request->session_handle->session->cred,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_smb_create_open_parent_callback,
        request);
} /* chimera_smb_create_lookup_parent_callback */

static inline void
chimera_smb_create_process(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_tree   *tree       = request->tree;

    if (request->create.parent_path_len) {
        chimera_vfs_lookup(
            vfs_thread,
            &request->session_handle->session->cred,
            tree->fh,
            tree->fh_len,
            request->create.parent_path,
            request->create.parent_path_len,
            CHIMERA_VFS_ATTR_FH,
            CHIMERA_VFS_LOOKUP_FOLLOW,
            chimera_smb_create_lookup_parent_callback,
            request);
    } else if (request->create.name_len) {
        chimera_vfs_open_fh(
            vfs_thread,
            &request->session_handle->session->cred,
            request->tree->fh,
            request->tree->fh_len,
            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
            chimera_smb_create_open_parent_callback,
            request);
    } else {
        chimera_vfs_open_fh(
            vfs_thread,
            &request->session_handle->session->cred,
            request->tree->fh,
            request->tree->fh_len,
            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED,
            chimera_smb_create_open_callback,
            request);

    }
} /* chimera_smb_create_process */


static void
chimera_smb_revalidate_tree_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;
    struct chimera_smb_tree    *tree    = request->tree;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_error("Revalidate error_code %d", error_code);
        chimera_smb_complete_request(request, SMB2_STATUS_NETWORK_NAME_DELETED);
        return;
    }

    tree->fh_len = attr->va_fh_len;
    memcpy(&tree->fh, &attr->va_fh, attr->va_fh_len);

    clock_gettime(CLOCK_MONOTONIC, &tree->fh_expiration);
    tree->fh_expiration.tv_sec += 60;

    chimera_smb_create_process(request);
} /* chimera_smb_revalidate_tree_callback */

static inline void
chimera_smb_revalidate_tree(
    struct chimera_smb_tree    *tree,
    struct chimera_smb_request *request)
{
    struct chimera_vfs_thread *vfs_thread = request->compound->thread->vfs_thread;
    uint8_t                    root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                   root_fh_len;

    chimera_vfs_get_root_fh(root_fh, &root_fh_len);

    chimera_vfs_lookup(
        vfs_thread,
        &request->session_handle->session->cred,
        root_fh,
        root_fh_len,
        tree->share->path,
        strlen(tree->share->path),
        CHIMERA_VFS_ATTR_FH,
        CHIMERA_VFS_LOOKUP_FOLLOW,
        chimera_smb_revalidate_tree_callback,
        request);

} /* chimera_smb_revalidate_tree */

void
chimera_smb_create(struct chimera_smb_request *request)
{
    struct timespec               now;
    struct chimera_smb_open_file *open_file;
    enum chimera_smb_pipe_magic   pipe_magic;
    chimera_smb_pipe_transceive_t transceive;

    /* Reject stream-name syntax (file:stream[:$DATA]) — streams unsupported.
     * Done here rather than in parse so the OBJECT_NAME_INVALID response is
     * returned to the client instead of triggering a parse-error disconnect. */
    if (request->create.name_len > 0 &&
        memchr(request->create.name, ':', request->create.name_len)) {
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_INVALID);
        return;
    }

    if (request->tree->type == CHIMERA_SMB_TREE_TYPE_PIPE) {

        if (strcasecmp(request->create.name, "lsarpc") == 0) {
            pipe_magic = CHIMERA_SMB_OPEN_FILE_LSA_RPC;
            transceive = chimera_smb_lsarpc_transceive;
        } else {
            chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
            return;
        }

        open_file = chimera_smb_create_gen_open_file_pipe(request,
                                                          pipe_magic,
                                                          transceive,
                                                          request->create.name,
                                                          request->create.name_len);

        request->create.r_open_file = open_file;

        request->create.r_attrs.smb_crttime    = 0;
        request->create.r_attrs.smb_atime      = 0;
        request->create.r_attrs.smb_mtime      = 0;
        request->create.r_attrs.smb_ctime      = 0;
        request->create.r_attrs.smb_alloc_size = 0;
        request->create.r_attrs.smb_size       = 0;
        request->create.r_attrs.smb_attributes = 0x80;
        request->create.r_attrs.smb_attr_mask  = SMB_ATTR_MASK_NETWORK_OPEN;

        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);

    } else {

        /* MS-SMB2 3.3.5.9: if FILE_DELETE_ON_CLOSE is set in CreateOptions,
         * the create must also request DELETE access (DELETE, GENERIC_ALL, or
         * MAXIMUM_ALLOWED, which resolves to a superset). Otherwise fail with
         * STATUS_ACCESS_DENIED rather than silently honoring delete-on-close. */
        if ((request->create.create_options & SMB2_FILE_DELETE_ON_CLOSE) &&
            !(request->create.desired_access &
              (SMB2_DELETE | SMB2_GENERIC_ALL | SMB2_MAXIMUM_ALLOWED))) {
            chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
            return;
        }

        clock_gettime(CLOCK_MONOTONIC, &now);

        if (chimera_timespec_cmp(&now, &request->tree->fh_expiration) > 0) {
            chimera_smb_revalidate_tree(request->tree, request);
        } else {
            chimera_smb_create_process(request);
        }
    }
} /* chimera_smb_create */

/* CREATE-context response emit helpers.
 *
 * Each response context on the wire has this 16-byte header (mirroring 2.2.13.2):
 *   Next(4) NameOffset(2) NameLength(2) Reserved(2) DataOffset(2) DataLength(4)
 * followed by the 4-byte name at offset 16, 4 bytes of zero padding (to 8-byte
 * align the data), then the data at offset 24. Chain advance = 24 + data_len
 * rounded up to 8 bytes. Last context has Next = 0. */

static const uint32_t SMB2_CREATE_CTX_FIXED_OVERHEAD = 24;

static inline uint32_t
smb_create_ctx_chain_advance(uint32_t data_len)
{
    uint32_t total = SMB2_CREATE_CTX_FIXED_OVERHEAD + data_len;

    return (total + 7u) & ~7u;
} /* smb_create_ctx_chain_advance */

static uint32_t
emit_create_response_context(
    uint8_t       *buf,
    uint32_t       buf_size,
    uint32_t       pos,
    const char    *tag,
    const uint8_t *data,
    uint32_t       data_len)
{
    uint32_t advance = smb_create_ctx_chain_advance(data_len);

    if (pos + advance > buf_size) {
        return 0;
    }

    /* Header. Next is filled in later (or zeroed for the last entry); write
     * the advance value for now so chained reads work as we go, and the caller
     * zeroes the field on the final context. */
    buf[pos + 0]  = advance & 0xff;
    buf[pos + 1]  = (advance >> 8) & 0xff;
    buf[pos + 2]  = (advance >> 16) & 0xff;
    buf[pos + 3]  = (advance >> 24) & 0xff;
    buf[pos + 4]  = 16; buf[pos + 5]  = 0;                  /* NameOffset = 16 */
    buf[pos + 6]  = 4;  buf[pos + 7]  = 0;                  /* NameLength = 4 */
    buf[pos + 8]  = 0;  buf[pos + 9]  = 0;                  /* Reserved */
    buf[pos + 10] = 24; buf[pos + 11] = 0;                  /* DataOffset = 24 */
    buf[pos + 12] = data_len & 0xff;
    buf[pos + 13] = (data_len >> 8) & 0xff;
    buf[pos + 14] = (data_len >> 16) & 0xff;
    buf[pos + 15] = (data_len >> 24) & 0xff;
    buf[pos + 16] = (uint8_t) tag[0];
    buf[pos + 17] = (uint8_t) tag[1];
    buf[pos + 18] = (uint8_t) tag[2];
    buf[pos + 19] = (uint8_t) tag[3];
    buf[pos + 20] = 0; buf[pos + 21] = 0; buf[pos + 22] = 0; buf[pos + 23] = 0;
    if (data_len > 0) {
        memcpy(buf + pos + 24, data, data_len);
    }
    /* Zero any trailing pad bytes so we never leak stack contents. */
    if (advance > SMB2_CREATE_CTX_FIXED_OVERHEAD + data_len) {
        memset(buf + pos + SMB2_CREATE_CTX_FIXED_OVERHEAD + data_len,
               0,
               advance - SMB2_CREATE_CTX_FIXED_OVERHEAD - data_len);
    }
    return advance;
} /* emit_create_response_context */

/* Build the MxAc response body. 8 bytes: QueryStatus(4) | MaximalAccess(4).
 *
 * MaximalAccess is the caller's effective rights against the object's DACL --
 * what the user *could* obtain, independent of what was requested.  It was
 * computed from the ACL at open and stored on the open handle. */
static int
build_mxac_response(
    struct chimera_smb_request *request,
    uint8_t                    *out,
    uint32_t                    out_size)
{
    uint32_t max_access;

    if (out_size < 8) {
        return -1;
    }

    if (!request->create.r_open_file) {
        return -1;
    }

    max_access = request->create.r_open_file->maximal_access;

    out[0] = 0; out[1] = 0; out[2] = 0; out[3] = 0;  /* QueryStatus = STATUS_SUCCESS */
    out[4] = max_access & 0xff;
    out[5] = (max_access >> 8) & 0xff;
    out[6] = (max_access >> 16) & 0xff;
    out[7] = (max_access >> 24) & 0xff;
    return 8;
} /* build_mxac_response */

struct chimera_smb_create_response_emitter {
    uint32_t    need_mask_bit;
    const char *tag;  /* points at a 4-char literal; no NUL on the wire */
    int         (*build)(
        struct chimera_smb_request *r,
        uint8_t                    *out,
        uint32_t                    out_size);
};

/* Build the RqLs response body.  v1 = 32 bytes, v2 = 52 bytes.  The
 * granted state was decided at CREATE time and stored on the open_file
 * by chimera_smb_create_gen_open_file. */
static int
build_rqls_response(
    struct chimera_smb_request *request,
    uint8_t                    *out,
    uint32_t                    out_size)
{
    struct chimera_smb_open_file *of = request->create.r_open_file;
    bool                          v2 = request->create.rqls.is_v2 != 0;
    int                           needed;

    if (!of) {
        return -1;
    }

    needed = v2 ? SMB2_CREATE_REQUEST_LEASE_V2_SIZE
                : SMB2_CREATE_REQUEST_LEASE_SIZE;
    if (out_size < (uint32_t) needed) {
        return -1;
    }

    /* LeaseKey (16) */
    memcpy(out, of->lease_key, 16);
    /* LeaseState (4) — lo byte from open_file->lease_state, upper bytes 0. */
    out[16] = of->lease_state;
    out[17] = 0;
    out[18] = 0;
    out[19] = 0;
    /* LeaseFlags (4) — reserved at CREATE response time. */
    out[20] = 0; out[21] = 0; out[22] = 0; out[23] = 0;
    /* LeaseDuration (8) — reserved. */
    memset(out + 24, 0, 8);
    if (v2) {
        memcpy(out + 32, of->parent_lease_key, 16);
        out[48] = of->lease_epoch & 0xff;
        out[49] = (of->lease_epoch >> 8) & 0xff;
        out[50] = 0; out[51] = 0;
    }
    return needed;
} /* build_rqls_response */

/* *INDENT-OFF* */ /* uncrustify oscillates on aligned struct-init tables */
static const struct chimera_smb_create_response_emitter smb_create_response_emitters[] = {
    { CHIMERA_SMB_CREATE_CTX_MXAC, "MxAc", build_mxac_response  },
    { CHIMERA_SMB_CREATE_CTX_RQLS, "RqLs", build_rqls_response  },
    /* Phase 3: + DH2Q (durable handle grant), DHnQ
     * Phase 8: + QFid (32-byte on-disk id) */
    { 0,                           NULL,   NULL                },
};
/* *INDENT-ON* */

/* Returns total bytes written into ctx_buf. Zero if no contexts to emit.
 * Exposed for unit tests in tests/phase0_contexts_test.c. */
SYMBOL_EXPORT uint32_t
chimera_smb_build_create_response_contexts(
    struct chimera_smb_request *request,
    uint8_t                    *ctx_buf,
    uint32_t                    ctx_buf_size)
{
    uint32_t                                          pos      = 0;
    uint32_t                                          last_pos = 0;
    int                                               emitted  = 0;
    const struct chimera_smb_create_response_emitter *e;

    if (!request->create.r_open_file) {
        return 0;
    }

    for (e = smb_create_response_emitters; e->need_mask_bit != 0; e++) {
        uint8_t  data_buf[64];
        int      data_len;
        uint32_t advance;

        if ((request->create.ctx_present_mask & e->need_mask_bit) == 0) {
            continue;
        }
        data_len = e->build(request, data_buf, sizeof(data_buf));
        if (data_len < 0) {
            continue;
        }
        advance = emit_create_response_context(ctx_buf, ctx_buf_size, pos,
                                               e->tag, data_buf, (uint32_t) data_len);
        /* Silently dropping a context the client asked for would create
         * subtle interop failures (missing lease grant, missing durable
         * grant, etc.) that are very hard to debug from a pcap. The caller
         * controls ctx_buf_size; if the budget is wrong, it should grow,
         * not silently truncate. Phase 2/3 will add more emitters and this
         * fires the moment the buffer is undersized for the new mix. */
        chimera_smb_abort_if(advance == 0,
                             "CREATE response context %c%c%c%c overflowed ctx_buf "
                             "(pos=%u data_len=%d size=%u)",
                             e->tag[0], e->tag[1], e->tag[2], e->tag[3],
                             pos, data_len, ctx_buf_size);
        last_pos = pos;
        pos     += advance;
        emitted++;
    }

    if (emitted > 0) {
        /* Patch the last context's Next field to zero. */
        ctx_buf[last_pos + 0] = 0;
        ctx_buf[last_pos + 1] = 0;
        ctx_buf[last_pos + 2] = 0;
        ctx_buf[last_pos + 3] = 0;
    }

    return pos;
} /* chimera_smb_build_create_response_contexts */

void
chimera_smb_create_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    uint8_t  ctx_buf[256];
    uint32_t ctx_len;
    uint32_t ctx_off;

    ctx_len = chimera_smb_build_create_response_contexts(request, ctx_buf, sizeof(ctx_buf));
    /* Absolute offset from the start of the SMB2 header. CREATE reply fixed body
     * is 88 bytes; contexts start in the Buffer field immediately after. The
     * sum (header + 88) is naturally 8-byte aligned for the standard header. */
    ctx_off = (ctx_len > 0) ? (uint32_t) (sizeof(struct smb2_header) + 88) : 0;

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_CREATE_REPLY_SIZE);

    /* Oplock level — granted at CREATE time and stored on the open_file
     * by the CACHING acquire above; defaults to NONE if no lease. */
    evpl_iovec_cursor_append_uint8(reply_cursor,
                                   request->create.r_open_file
                                   ? request->create.r_open_file->oplock_level
                                   : SMB2_OPLOCK_LEVEL_NONE);

    /* Flags */
    evpl_iovec_cursor_append_uint8(reply_cursor, 0);

    /* Create Action */

    if (request->create.create_disposition == SMB2_FILE_OPEN) {
        evpl_iovec_cursor_append_uint32(reply_cursor, SMB2_CREATE_ACTION_OPENED);
    } else {
        evpl_iovec_cursor_append_uint32(reply_cursor, SMB2_CREATE_ACTION_CREATED);
    }

    chimera_smb_append_network_open_info(reply_cursor, &request->create.r_attrs);

    /* File Id (persistent) */
    evpl_iovec_cursor_append_uint64(reply_cursor, request->create.r_open_file ?
                                    request->create.r_open_file->file_id.pid : 0);

    /* File Id (volatile) */
    evpl_iovec_cursor_append_uint64(reply_cursor, request->create.r_open_file ?
                                    request->create.r_open_file->file_id.vid : 0);

    /* Create Context Offset / Length */
    evpl_iovec_cursor_append_uint32(reply_cursor, ctx_off);
    evpl_iovec_cursor_append_uint32(reply_cursor, ctx_len);

    if (ctx_len > 0) {
        evpl_iovec_cursor_append_blob(reply_cursor, ctx_buf, ctx_len);
    } else {
        /* Preserve the 4-byte zero pad that the original implementation emitted
         * here so the Buffer field is non-empty (StructureSize encodes +1). */
        evpl_iovec_cursor_append_uint32(reply_cursor, 0);
    }

    /* Phase-0 housekeeping: propagate which CREATE contexts the client supplied
     * onto the open file so later phases (lease/durable break, reconnect) can
     * tell which contracts the client expects. */
    if (request->create.r_open_file) {
        request->create.r_open_file->ctx_present_mask = request->create.ctx_present_mask;
    }

} /* chimera_smb_create_reply */

/* CREATE-context request handlers. Each fills typed fields in request->create.
 * NULL handler = presence-only (the bit in ctx_present_mask is all that matters
 * for Phase 0; Phase 1/3 will add real semantics for the open-after-CREATE step). */

static void
parse_ctx_secd(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request)
{
    chimera_smb_parse_sd_to_acl(data, data_len, &request->create.set_attr,
                                request->create.acl_storage,
                                sizeof(request->create.acl_storage));
} /* parse_ctx_secd */

static void
parse_ctx_dhnc(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request)
{
    /* DHnC body: 16-byte SMB2_FILEID (persistent | volatile) + 16 reserved bytes. */
    if (data_len < 16) {
        return;
    }
    request->create.dhnc.persistent  = smb_wire_le64(data);
    request->create.dhnc.volatile_id = smb_wire_le64(data + 8);
} /* parse_ctx_dhnc */

static void
parse_ctx_dh2q(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request)
{
    /* DH2Q body: Timeout(4) | Flags(4) | Reserved(8) | CreateGuid(16) = 32 bytes. */
    if (data_len < 32) {
        return;
    }
    request->create.dh2q.timeout_ms = smb_wire_le32(data);
    request->create.dh2q.flags      = smb_wire_le32(data + 4);
    memcpy(request->create.dh2q.create_guid, data + 16, 16);
} /* parse_ctx_dh2q */

static void
parse_ctx_dh2c(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request)
{
    /* DH2C body: FileId(16) | CreateGuid(16) | Flags(4) = 36 bytes. */
    if (data_len < 36) {
        return;
    }
    request->create.dh2c.persistent  = smb_wire_le64(data);
    request->create.dh2c.volatile_id = smb_wire_le64(data + 8);
    memcpy(request->create.dh2c.create_guid, data + 16, 16);
    request->create.dh2c.flags = smb_wire_le32(data + 32);
} /* parse_ctx_dh2c */

static void
parse_ctx_rqls(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request)
{
    /* RqLs v1 body (32 bytes): LeaseKey(16) | LeaseState(4) | LeaseFlags(4) | LeaseDuration(8 reserved).
     * RqLs v2 body (52 bytes): adds ParentLeaseKey(16) | Epoch(2) | Reserved(2).
     * Differentiate by data_len. Other sizes: malformed; skip without setting fields. */
    if (data_len != 32 && data_len != 52) {
        return;
    }
    memcpy(request->create.rqls.key, data, 16);
    request->create.rqls.state = smb_wire_le32(data + 16);
    request->create.rqls.flags = smb_wire_le32(data + 20);
    if (data_len == 52) {
        request->create.rqls.is_v2 = 1;
        memcpy(request->create.rqls.parent_key, data + 32, 16);
        request->create.rqls.epoch = smb_wire_le16(data + 48);
    } else {
        request->create.rqls.is_v2 = 0;
    }
} /* parse_ctx_rqls */

static void
parse_ctx_alsi(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request)
{
    if (data_len < 8) {
        return;
    }
    request->create.alsi_alloc_size = smb_wire_le64(data);
} /* parse_ctx_alsi */

static void
parse_ctx_twrp(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request)
{
    if (data_len < 8) {
        return;
    }
    request->create.twrp_timestamp = smb_wire_le64(data);
} /* parse_ctx_twrp */

typedef void (*chimera_smb_create_ctx_handler_t)(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request);

struct chimera_smb_create_ctx_parser {
    const char                      *tag;  /* points at a 4-char literal */
    uint32_t                         mask_bit;
    chimera_smb_create_ctx_handler_t handler;
};

/* Dispatch table for 4-byte tagged CREATE contexts. 16-byte GUID-named contexts
 * (AppInstanceId, AppInstanceVersion, SVHDX_*) are silently skipped in Phase 0. */
/* *INDENT-OFF* */ /* uncrustify oscillates on aligned struct-init tables */
static const struct chimera_smb_create_ctx_parser smb_create_ctx_parsers[] = {
    { "SecD", CHIMERA_SMB_CREATE_CTX_SECD, parse_ctx_secd },
    { "ExtA", CHIMERA_SMB_CREATE_CTX_EXTA, NULL           },
    { "DHnQ", CHIMERA_SMB_CREATE_CTX_DHNQ, NULL           },
    { "DHnC", CHIMERA_SMB_CREATE_CTX_DHNC, parse_ctx_dhnc },
    { "DH2Q", CHIMERA_SMB_CREATE_CTX_DH2Q, parse_ctx_dh2q },
    { "DH2C", CHIMERA_SMB_CREATE_CTX_DH2C, parse_ctx_dh2c },
    { "AlSi", CHIMERA_SMB_CREATE_CTX_ALSI, parse_ctx_alsi },
    { "MxAc", CHIMERA_SMB_CREATE_CTX_MXAC, NULL           },
    { "TWrp", CHIMERA_SMB_CREATE_CTX_TWRP, parse_ctx_twrp },
    { "QFid", CHIMERA_SMB_CREATE_CTX_QFID, NULL           },
    { "RqLs", CHIMERA_SMB_CREATE_CTX_RQLS, parse_ctx_rqls },
    { NULL,   0,                           NULL           },
};
/* *INDENT-ON* */

/* Exposed for unit tests in tests/phase0_contexts_test.c. */
SYMBOL_EXPORT int
chimera_smb_parse_create_contexts(
    const uint8_t              *buf,
    uint32_t                    buf_len,
    struct chimera_smb_request *request)
{
    uint32_t pos = 0;

    while (pos < buf_len) {
        uint32_t       next, data_len;
        uint16_t       name_off, name_len, data_off;
        const uint8_t *name;
        const uint8_t *data = NULL;

        if (buf_len - pos < 16) {
            chimera_smb_error("CREATE-context chain truncated before header (pos=%u, remaining=%u)",
                              pos, buf_len - pos);
            request->status = SMB2_STATUS_INVALID_PARAMETER;
            return -1;
        }

        next     = smb_wire_le32(buf + pos);
        name_off = smb_wire_le16(buf + pos + 4);
        name_len = smb_wire_le16(buf + pos + 6);
        /* buf[pos+8..pos+10] is the 2-byte Reserved */
        data_off = smb_wire_le16(buf + pos + 10);
        data_len = smb_wire_le32(buf + pos + 12);

        if ((uint32_t) name_off + name_len > buf_len - pos) {
            chimera_smb_error("CREATE-context name out of bounds (pos=%u, name_off=%u, name_len=%u)",
                              pos, name_off, name_len);
            request->status = SMB2_STATUS_INVALID_PARAMETER;
            return -1;
        }

        if (data_len > 0) {
            if (data_off == 0 || (uint32_t) data_off + data_len > buf_len - pos) {
                chimera_smb_error("CREATE-context data out of bounds (pos=%u, data_off=%u, data_len=%u)",
                                  pos, data_off, data_len);
                request->status = SMB2_STATUS_INVALID_PARAMETER;
                return -1;
            }
            data = buf + pos + data_off;
        }

        name = buf + pos + name_off;

        /* Dispatch on 4-byte tags only. 16-byte GUID-named contexts
         * (AppInstanceId, AppInstanceVersion, SVHDX_*) fall through. */
        if (name_len == 4) {
            const struct chimera_smb_create_ctx_parser *p;
            for (p = smb_create_ctx_parsers; p->tag != NULL; p++) {
                if (memcmp(name, p->tag, 4) == 0) {

                    /* Tag-aware special case: RqLs v2 (52-byte body) sets a
                     * distinct mask bit so the response emit can tell which
                     * lease variant the client requested. */
                    if (p->mask_bit == CHIMERA_SMB_CREATE_CTX_RQLS && data_len == 52) {
                        request->create.ctx_present_mask |= CHIMERA_SMB_CREATE_CTX_RQLS_V2;
                    } else {
                        request->create.ctx_present_mask |= p->mask_bit;
                    }
                    if (p->handler) {
                        p->handler(data, data_len, request);
                    }
                    break;
                }
            }
        }

        if (next == 0) {
            break;
        }

        if (next < 16 || (next & 0x7) != 0 || pos + next > buf_len) {
            chimera_smb_error("CREATE-context Next field invalid (pos=%u, next=%u, buf_len=%u)",
                              pos, next, buf_len);
            request->status = SMB2_STATUS_INVALID_PARAMETER;
            return -1;
        }
        pos += next;
    }

    return 0;
} /* chimera_smb_parse_create_contexts */

/*
 * Parse SMB2 CREATE request
 * Offset  Size  Field
 * ------  ----  -----------------------------------------------------------
 * 0x00    2     StructureSize = 57 (0x0039)   // fixed for request
 * 0x02    1     SecurityFlags = 0 (reserved)
 * 0x03    1     RequestedOplockLevel          // NONE/II/EXCLUSIVE/BATCH/LEASE
 * 0x04    4     ImpersonationLevel            // Anonymous/Ident./Impersonation/Delegate
 * 0x08    8     SmbCreateFlags = 0 (reserved; ignore on server)
 * 0x10    8     Reserved (ignore on server)
 * 0x18    4     DesiredAccess                 // access mask (see §2.2.13.1)
 * 0x1C    4     FileAttributes                // FILE_ATTRIBUTE_* (dirs use DIRECTORY)
 * 0x20    4     ShareAccess                   // READ/WRITE/DELETE mask
 * 0x24    4     CreateDisposition             // SUPERSEDE, OPEN, CREATE, OPEN_IF, OVERWRITE, OVERWRITE_IF
 * 0x28    4     CreateOptions                 // FILE_* options (e.g., DIRECTORY_FILE, NON_DIRECTORY_FILE)
 * 0x2C    2     NameOffset                    // from start of SMB2 header to file name
 * 0x2E    2     NameLength (bytes; UTF‑16LE; not NUL‑terminated)
 * 0x30    4     CreateContextsOffset          // 8‑byte aligned if present; 0 if none
 * 0x34    4     CreateContextsLength          // bytes of concatenated contexts
 * 0x38    ...   Buffer: FileName then SMB2_CREATE_CONTEXT blobs (if any)
 */
int
chimera_smb_parse_create(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    uint16_t name_offset;
    uint32_t blob_offset, blob_length;
    uint16_t name16[SMB_FILENAME_MAX];
    int      name_size;
    char    *slash;

    if (unlikely(request->request_struct_size != SMB2_CREATE_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 CREATE request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_CREATE_REQUEST_SIZE);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    evpl_iovec_cursor_get_uint8(request_cursor, &request->create.requested_oplock_level);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.impersonation_level);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->create.flags);
    evpl_iovec_cursor_skip(request_cursor, 8);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.desired_access);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.file_attributes);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.share_access);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.create_disposition);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.create_options);
    evpl_iovec_cursor_get_uint16(request_cursor, &name_offset);
    evpl_iovec_cursor_get_uint16(request_cursor, &request->create.name_len);
    evpl_iovec_cursor_get_uint32(request_cursor, &blob_offset);
    evpl_iovec_cursor_get_uint32(request_cursor, &blob_length);

    if (request->create.impersonation_level > SMB2_IMPERSONATION_DELEGATE) {
        request->status = SMB2_STATUS_BAD_IMPERSONATION_LEVEL;
        return -1;
    }

    if (request->create.name_len >= SMB_FILENAME_MAX) {
        chimera_smb_error("Create request: UTF-16 name too long (%u bytes)",
                          request->create.name_len);
        request->status = SMB2_STATUS_NAME_TOO_LONG;
        return -1;
    }

    evpl_iovec_cursor_copy(request_cursor, name16, request->create.name_len);

    name_size = chimera_smb_utf16le_to_utf8(&request->compound->thread->iconv_ctx,
                                            name16,
                                            request->create.name_len,
                                            request->create.parent_path,
                                            sizeof(request->create.parent_path));
    if (name_size < 0) {
        chimera_smb_error("Failed to convert CREATE name from UTF-16LE to UTF-8");
        request->status = SMB2_STATUS_OBJECT_NAME_INVALID;
        return -1;
    }
    request->create.parent_path_len = name_size;

    /* Reject paths with a leading backslash separator */
    if (name_size > 0 && request->create.parent_path[0] == '\\') {
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    slash = rindex(request->create.parent_path, '\\');

    if (slash) {
        *slash                          = '\0';
        request->create.name            = slash + 1;
        request->create.name_len        = request->create.parent_path_len - (slash - request->create.parent_path) - 1;
        request->create.parent_path_len = slash - request->create.parent_path;

        chimera_smb_slash_back_to_forward(request->create.parent_path, request->create.parent_path_len);
    } else {
        request->create.name            = request->create.parent_path;
        request->create.name_len        = request->create.parent_path_len;
        request->create.parent_path_len = 0;
    }

    /* Initialize create-time attributes (may be populated by SD create context) */
    request->create.set_attr.va_req_mask = 0;
    request->create.set_attr.va_set_mask = 0;
    request->create.ctx_present_mask     = 0;

    /* Persist any settable DOS attribute bits supplied at create time. */
    if (request->create.file_attributes & SMB_DOS_ATTR_SETTABLE) {
        request->create.set_attr.va_dos_attributes =
            request->create.file_attributes & SMB_DOS_ATTR_SETTABLE;
        request->create.set_attr.va_req_mask |= CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
        request->create.set_attr.va_set_mask |= CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
    }

    if (blob_offset > 0 && blob_length > 0 && blob_length <= 1024) {
        uint8_t  ctx_buf[1024];
        uint32_t skip = blob_offset - evpl_iovec_cursor_consumed(request_cursor);

        evpl_iovec_cursor_skip(request_cursor, skip);

        if (evpl_iovec_cursor_get_blob(request_cursor, ctx_buf, blob_length) == 0) {
            if (chimera_smb_parse_create_contexts(ctx_buf, blob_length, request) < 0) {
                return -1;
            }
        }
    }

    return 0;
} /* chimera_smb_parse_create */
