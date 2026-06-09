// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/time.h>
#include <strings.h>
#include "server/smb/smb2.h"
#include "server/smb/smb_session.h"
#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_async_interim.h"
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

/*
 * Decide what to do when a CREATE carrying an SMB2_CREATE_APP_INSTANCE_ID
 * conflicts with an existing Open that holds the same AppInstanceId on a
 * different connection (MS-SMB2 3.3.5.9.7 + 3.3.5.9.16 AppInstanceVersion).
 *
 * AppInstanceVersion is the lexicographic (High, then Low) pair.  Empirically
 * (WPTS AppInstanceVersion cases) the existing open is force-closed and the new
 * CREATE allowed to proceed iff the existing open carried no AppInstanceVersion,
 * OR both carry one and the new version is strictly greater than the existing
 * one.  Otherwise -- the existing open has a version but the new CREATE has none,
 * or the new version is equal-or-lower -- the existing open is left intact and
 * the new CREATE fails with STATUS_FILE_FORCED_CLOSED.
 *
 * Returns 1 to force-close + proceed, -1 to reject, 0 if the rule does not
 * apply (no AppInstanceId on the request, no match, or same connection).
 */
static int
chimera_smb_app_instance_decision(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *existing)
{
    if (!(request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_APP)) {
        return 0;
    }

    if (!existing ||
        !(existing->ctx_present_mask & CHIMERA_SMB_CREATE_CTX_APP)) {
        return 0;
    }

    if (memcmp(existing->app_instance_id,
               request->create.app_instance_id, 16) != 0) {
        return 0;
    }

    /* Same AppInstanceId but on the same connection is not an application
     * failover replacement -- leave it to normal share semantics. */
    if (existing->create_conn == request->compound->conn) {
        return 0;
    }

    /* existing.V ABSENT -> force-close (an old open with no version always
     * yields to a new AppInstanceId open). */
    if (!existing->app_version_present) {
        return 1;
    }

    /* existing.V present, new.V ABSENT -> reject. */
    if (!request->create.app_version_present) {
        return -1;
    }

    /* Both present: force-close iff new.V > existing.V (High, then Low);
     * equal or lower versions reject. */
    if (request->create.app_version_high != existing->app_version_high) {
        return request->create.app_version_high > existing->app_version_high ?
               1 : -1;
    }
    return request->create.app_version_low > existing->app_version_low ?
           1 : -1;
} /* chimera_smb_app_instance_decision */

/*
 * Force-close a conflicting live open during AppInstanceId failover: unhash it
 * from its tree, release its leases / byte-range locks / VFS handle, and drop
 * its outstanding handle reference so the share reservation is gone before the
 * new CREATE retries.  A live (non-parked) open holds a single handle reference
 * once its own CREATE completed; this consumes it and frees the object, so the
 * owning connection's later CLOSE will simply miss it and report FILE_CLOSED.
 * Returns true if the open was force-closed by this call.
 */
static bool
chimera_smb_app_instance_force_close(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file)
{
    struct chimera_smb_tree      *tree = open_file->tree;
    int                           bucket;
    bool                          unhashed = false;
    struct chimera_smb_open_file *to_free  = NULL;

    if (!tree) {
        return false;
    }

    bucket = open_file->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    pthread_mutex_lock(&tree->open_files_lock[bucket]);
    if (!(open_file->flags & CHIMERA_SMB_OPEN_FILE_CLOSED)) {
        open_file->flags |= CHIMERA_SMB_OPEN_FILE_CLOSED;
        HASH_DELETE(hh, tree->open_files[bucket], open_file);
        unhashed = true;
    }
    pthread_mutex_unlock(&tree->open_files_lock[bucket]);

    if (!unhashed) {
        return false;
    }

    /* Drop the durable registry entry so a reconnect can't reclaim it. */
    if (open_file->durable_flags) {
        chimera_smb_durable_forget(thread->shared, open_file->file_id.pid);
    }

    /* Release the share + caching leases and byte-range locks so the share
     * reservation no longer blocks the new open. */
    chimera_smb_open_file_drain_locks(thread, open_file);

    if (open_file->handle) {
        chimera_vfs_release(thread->vfs_thread, open_file->handle);
        open_file->handle = NULL;
    }

    /* Consume the remaining handle reference.  The owning connection's CLOSE
     * will no longer find the open in the hash, so it never releases — this is
     * the last reference. */
    pthread_mutex_lock(&tree->open_files_lock[bucket]);
    chimera_smb_abort_if(open_file->refcnt == 0,
                         "app-instance force-close: refcnt 0");
    open_file->refcnt--;
    if (open_file->refcnt == 0) {
        to_free = open_file;
    }
    pthread_mutex_unlock(&tree->open_files_lock[bucket]);

    if (to_free) {
        chimera_smb_open_file_free(thread, to_free);
    }

    return true;
} /* chimera_smb_app_instance_force_close */

/* Finish an open once its share reservation is held (or not required): acquire
 * the caching (oplock/lease) grant, propagate delete-on-close, grant the durable
 * handle, and register the open in the tree.  Split out of gen_open_file so the
 * share acquire can be made asynchronous (parking on a batch-oplock break)
 * without duplicating this tail.  Always succeeds (the caching grant is
 * opportunistic); returns open_file. */
static struct chimera_smb_open_file *
chimera_smb_create_after_share(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file);

static void
chimera_smb_create_open_finish(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file);

/* Apply the held (post-truncate-downgrade) share grant and mark the reservation
 * inserted; shared by the synchronous and parked share-acquire paths. */
static inline void
chimera_smb_create_finish_share_grant(
    struct chimera_smb_open_file  *open_file,
    struct chimera_vfs_file_state *file_state,
    uint8_t                        held_granted);

/* Resume a parked share acquire once the conflicting batch oplock has been
 * relinquished (GRANTED -> finish the open) or kept (DENIED -> SHARING_VIOLATION). */
static void
chimera_smb_create_share_park_cb(
    enum chimera_vfs_lease_result result,
    struct chimera_vfs_lease     *lease,
    struct chimera_vfs_lease     *conflict,
    void                         *private_data);

/*
 * Single break-decision point for a conflicting open (MS-FSA open-vs-oplock
 * ordering), factored out of the two create-path call sites that previously
 * duplicated the trigger/owner computation verbatim.
 *
 *   phase 1 (pre-share-check, trigger H): a batch / handle-caching holder breaks
 *     BEFORE the share-mode decision, because it may close its deferred handle
 *     and dissolve the conflict -- so the break fires even when the open is
 *     ultimately refused with SHARING_VIOLATION (smb2.oplock.batch5).  It retains
 *     R unless the open truncates.
 *   phase 2 (post-share-check, trigger W): once the open is granted, a
 *     conflicting exclusive (W-only) holder breaks to LEVEL_II; a truncating open
 *     replaces the data and instead invalidates every cached holder to NONE
 *     (break_on_write).
 *
 * The opener is identified by its lease key when it carries an RqLs context (all
 * of one client's opens of a file share that key, so a re-open by the lease
 * holder coalesces instead of self-breaking), else by file_id.  It is a no-op on
 * a pure stat-open (no data/delete/truncate access) and when the only holder is
 * the opener's own.
 */
static inline void
chimera_smb_create_break_for_open(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_request       *request,
    struct chimera_smb_open_file     *open_file,
    struct chimera_vfs_open_handle   *oh,
    int                               phase)
{
    uint32_t da        = open_file->desired_access;
    uint32_t disp      = request->create.create_disposition;
    bool     truncates =
        disp == SMB2_FILE_OVERWRITE ||
        disp == SMB2_FILE_OVERWRITE_IF ||
        disp == SMB2_FILE_SUPERSEDE;
    /* Access rights that make an open a "real" open (vs a pure stat open) and so
     * break a conflicting caching holder.  Per MS-FSA / smb2.lease.statopen4:
     * data read/write/append/execute, EA read/write, DELETE, and WRITE_DAC /
     * WRITE_OWNER all break; READ_ATTRIBUTES, WRITE_ATTRIBUTES, READ_CONTROL and
     * SYNCHRONIZE do NOT (they are compatible with a cached handle). */
    bool break_trigger =
        (da & (SMB2_FILE_READ_DATA | SMB2_FILE_WRITE_DATA |
               SMB2_FILE_APPEND_DATA | SMB2_FILE_EXECUTE |
               SMB2_FILE_READ_EA | SMB2_FILE_WRITE_EA |
               SMB2_WRITE_DACL | SMB2_WRITE_OWNER |
               SMB2_GENERIC_READ | SMB2_GENERIC_WRITE |
               SMB2_GENERIC_EXECUTE | SMB2_GENERIC_ALL |
               SMB2_MAXIMUM_ALLOWED | SMB2_DELETE)) ||
        (request->create.create_options & SMB2_FILE_DELETE_ON_CLOSE) ||
        truncates;
    struct chimera_vfs_state      *vfs_state = thread->vfs_thread->vfs->vfs_state;
    struct chimera_vfs_lease_owner io_owner  = {
        .protocol   = CHIMERA_VFS_LEASE_PROTO_SMB2,
        .client_key = request->session_handle->session->client_key,
        .owner_lo   = open_file->file_id.pid,
        .owner_hi   = open_file->file_id.vid,
    };

    if (!break_trigger) {
        return;
    }

    /* An RqLs open is owned by its lease key, not its file_id; identify the
     * opener by the key so break_caching_for_open coalesces a lease this client
     * already holds under the same key instead of recalling it. */
    if (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) {
        memcpy(&io_owner.owner_lo, request->create.rqls.key, 8);
        memcpy(&io_owner.owner_hi, request->create.rqls.key + 8, 8);
        io_owner.is_lease = 1;
    }

    if (phase == 1) {
        chimera_vfs_state_break_caching_for_open(
            vfs_state, oh->fh, oh->fh_len, oh->fh_hash, &io_owner,
            CHIMERA_VFS_LEASE_MODE_H,
            truncates ? 0 : CHIMERA_VFS_LEASE_MODE_R);
    } else if (truncates) {
        chimera_vfs_state_break_on_write(vfs_state, oh->fh, oh->fh_len,
                                         oh->fh_hash, &io_owner);
    } else {
        /* A conflicting (non-truncating) open invalidates the (exclusive) write
         * cache of other holders, but read + handle caching stay shared: break
         * W-holders down to R|H, not R (MS-SMB2 / smb2.lease.v2_epoch2: a plain
         * OPEN against an RWH lease yields a single RWH->RH break and the holder
         * keeps RH).  A holder cascades below RH only when an actual write or a
         * truncating open follows (chimera_vfs_break_on_write deepens the floor
         * to NONE; smb2.lease.breaking3).  For a legacy oplock R|H collapses to
         * LEVEL_II. */
        chimera_vfs_state_break_caching_for_open(
            vfs_state, oh->fh, oh->fh_len, oh->fh_hash, &io_owner,
            CHIMERA_VFS_LEASE_MODE_W,
            CHIMERA_VFS_LEASE_MODE_R | CHIMERA_VFS_LEASE_MODE_H);
    }
} /* chimera_smb_create_break_for_open */

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
    int                             is_directory,
    struct chimera_vfs_open_handle *oh)
{
    struct chimera_smb_compound      *compound = request->compound;
    struct chimera_server_smb_thread *thread   = compound->thread;
    struct chimera_smb_tree          *tree     = request->tree;
    struct chimera_smb_open_file     *open_file;

    open_file = chimera_smb_open_file_alloc(thread);

    open_file->type = type;

    if (parent_fh_len) {
        memcpy(open_file->parent_fh, parent_fh, parent_fh_len);
    }

    open_file->parent_fh_len  = parent_fh_len;
    open_file->file_id.pid    = pid;
    open_file->file_id.vid    = chimera_rand64();
    open_file->handle         = oh;
    open_file->desired_access = request->create.desired_access;
    open_file->share_access   = request->create.share_access;
    open_file->flags          = delete_on_close ? CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE : 0;
    /* Set the directory flag up front so the caching-lease grant below can skip
     * directories: a leased directory is recalled on every create/remove of a
     * child (chimera_vfs_io_recall on the parent), which is a break round-trip
     * per operation -- so directory opens take no oplock/lease. */
    if (is_directory) {
        open_file->flags |= CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY;
    }
    open_file->position        = 0;
    open_file->pipe_transceive = transceive;
    open_file->refcnt          = 2;
    /* Seed MS-SMB2 §3.3.5.2.10 channel-sequence tracking from the CREATE's
     * sequence so the first mutating op is compared against the right baseline
     * (the open_file pool is reused without zeroing). */
    open_file->channel_sequence       = request->channel_sequence;
    open_file->channel_sequence_valid = 1;
    /* Identify the owning conn/tree up front so an AppInstanceId force-close can
     * locate this open from the share-lease back-reference even when no caching
     * lease (which also sets create_conn) is granted. */
    open_file->create_conn = request->compound->conn;
    open_file->tree        = tree;

    /* Phase-0 plumbing state: zeroed; Phases 1/3 will populate from CREATE contexts. */
    open_file->ctx_present_mask   = 0;
    open_file->oplock_level       = 0;
    open_file->lease_state        = 0;
    open_file->lease_epoch        = 0;
    open_file->lease_flags        = 0;
    open_file->durable_flags      = 0;
    open_file->durable_timeout_ms = 0;
    /* No caching grant until the caching block below acquires one (open_file is
     * reused from a free list without being zeroed). */
    open_file->grant = NULL;
    memset(open_file->lease_key,        0, sizeof(open_file->lease_key));
    memset(open_file->parent_lease_key, 0, sizeof(open_file->parent_lease_key));
    memset(open_file->create_guid,      0, sizeof(open_file->create_guid));

    /* Record the AppInstanceId/AppInstanceVersion this open carried so a later
     * CREATE on a different connection can match it and apply the version-gated
     * force-close (MS-SMB2 3.3.5.9.7 / 3.3.5.9.16). */
    if (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_APP) {
        memcpy(open_file->app_instance_id, request->create.app_instance_id, 16);
        open_file->ctx_present_mask   |= CHIMERA_SMB_CREATE_CTX_APP;
        open_file->app_version_high    = request->create.app_version_high;
        open_file->app_version_low     = request->create.app_version_low;
        open_file->app_version_present = request->create.app_version_present;
    } else {
        memset(open_file->app_instance_id, 0, sizeof(open_file->app_instance_id));
        open_file->app_version_high    = 0;
        open_file->app_version_low     = 0;
        open_file->app_version_present = 0;
    }

    open_file->name_len = name_len;
    memcpy(open_file->name, name, open_file->name_len);

    /* Stream identity is set by the named-stream create path; default to none. */
    open_file->stream_name_len = 0;
    open_file->base_fh_len     = 0;

    /* MS-SMB2 3.3.5.9.16: an AppInstanceId failover replaces a prior open held
     * under the same AppInstanceId on another connection.  It must run BEFORE the
     * phase-1 batch break below, because the failover force-closes that prior
     * open SILENTLY -- if the break fired first the doomed open would receive a
     * real OPLOCK_BREAK (smb2.durable-v2-open app-instance expects break count 0).
     * Decision > 0 force-closes the prior open and lets this CREATE proceed;
     * decision < 0 rejects this CREATE with FILE_FORCED_CLOSED. */
    if (type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE && tree->share && oh &&
        (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_APP)) {
        struct chimera_vfs_state      *vfs_state  = thread->vfs_thread->vfs->vfs_state;
        struct chimera_vfs_file_state *file_state =
            chimera_vfs_state_get(vfs_state, oh->fh, oh->fh_len, oh->fh_hash, true);

        if (file_state) {
            struct chimera_smb_open_file *match    = NULL;
            int                           decision = 0;

            pthread_mutex_lock(&file_state->lock);
            for (struct chimera_vfs_lease *l = file_state->share_resvs;
                 l; l = l->next) {
                struct chimera_smb_open_file *of =
                    (struct chimera_smb_open_file *) l->owner.cb_private;
                int                           d = chimera_smb_app_instance_decision(request, of);
                if (d != 0) {
                    match    = of;
                    decision = d;
                    break;
                }
            }
            pthread_mutex_unlock(&file_state->lock);

            chimera_vfs_state_put(vfs_state, file_state);

            if (decision < 0) {
                /* Reject: leave the existing open intact. */
                open_file->handle = NULL;
                chimera_smb_open_file_free(thread, open_file);
                request->create.force_close_status = SMB2_STATUS_FILE_FORCED_CLOSED;
                return NULL;
            } else if (decision > 0) {
                chimera_smb_app_instance_force_close(thread, match);
            }
        }
    }

    /* A *batch* (handle-caching) oplock breaks BEFORE the share-mode check:
     * the holder may close its deferred handle in response, after which the
     * sharing conflict disappears.  Fire that break here so it happens even
     * when this open is ultimately refused with a sharing violation (MS-FSA
     * drives the break off the access attempt -- smb2.oplock.batch5 expects
     * the break AND a SHARING_VIOLATION).  An exclusive (W-only) oplock is
     * different: it breaks only once the open is granted (handled after the
     * share-mode check), so we do NOT touch W-only holders here.  A truncating
     * disposition replaces the data, so a batch holder breaks to NONE;
     * otherwise it drops to LEVEL_II.  Pure attribute/EA opens break nothing
     * (stat-open).  No-op on the first open; skips the opener's own lease. */
    if (type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE && tree->share && oh) {
        chimera_smb_create_break_for_open(thread, request, open_file, oh, 1);
    }

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
    if (type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE && tree->share && oh) {
        struct chimera_vfs_state      *vfs_state = thread->vfs_thread->vfs->vfs_state;
        struct chimera_vfs_file_state *file_state;
        uint32_t                       da = open_file->desired_access;
        uint32_t                       sa = open_file->share_access;
        uint8_t                        granted = 0, denied = 0;
        uint8_t                        held_granted = 0;
        struct chimera_vfs_lease      *conflict     = NULL;
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

        /* A truncating disposition must obtain write access at open time to
         * overwrite the data, so it conflicts with an existing opener that
         * denies write -- but it does not *hold* write for the handle's
         * lifetime (the granted access is what was requested).  Request write
         * transiently for the conflict check, then downgrade the held grant
         * once the lease is inserted. */
        held_granted = granted;
        if (request->create.create_disposition == SMB2_FILE_SUPERSEDE ||
            request->create.create_disposition == SMB2_FILE_OVERWRITE ||
            request->create.create_disposition == SMB2_FILE_OVERWRITE_IF) {
            granted |= CHIMERA_VFS_LEASE_MODE_W;
        }

        /* Attribute-only opens (no data/delete access bits, e.g. a
         * READ_ATTRIBUTES stat-open) hold no share-mode rights and impose no
         * deny.  Register them anyway as an inert (granted=0, denied=0) SHARE
         * entry so the layer can answer "is there another opener?" (sole-access
         * oplock grant), delete-pending, and stat-open classification -- without
         * changing any conflict outcome (chimera_vfs_share_conflict and the
         * CACHING sole-access loop both treat a (0,0) entry as absent). */
        if (!(da & SMB2_SHAREMODE_ACCESS_MASK)) {
            granted      = 0;
            denied       = 0;
            held_granted = 0;
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
        open_file->share_lease.owner.client_key = request->session_handle->session->client_key;
        /* The open itself is the owner — different opens, even by the
         * same client, must satisfy share-mode constraints between
         * themselves. */
        open_file->share_lease.owner.owner_lo = open_file->file_id.pid;
        open_file->share_lease.owner.owner_hi = open_file->file_id.vid;
        /* Back-reference to the owning open, used two ways: a conflicting CREATE
         * carrying a matching AppInstanceId can locate it for the force-close rule
         * below, and the lease layer can match it to this open's caching (batch)
         * lease so a hard share conflict against a batch-oplock holder parks on the
         * batch break rather than denying (chimera_vfs_share_batch_escape). */
        open_file->share_lease.owner.cb_private = open_file;

        /* If this open also requests a lease, a handle-caching lease already
         * held under the same key is its own (a second open under one lease
         * key) and must coalesce, not be broken by this share acquire. */
        if (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) {
            open_file->share_lease.has_break_skip_key = 1;
            memcpy(&open_file->share_lease.break_skip_lo,
                   request->create.rqls.key, 8);
            memcpy(&open_file->share_lease.break_skip_hi,
                   request->create.rqls.key + 8, 8);
        } else {
            open_file->share_lease.has_break_skip_key = 0;
        }

        /* AppInstanceId failover (MS-SMB2 3.3.5.9.7 / 3.3.5.9.16) was already
         * resolved before the phase-1 break above (so the displaced open is
         * force-closed silently); nothing to do here. */

        result = chimera_vfs_state_try_insert(vfs_state, file_state,
                                              &open_file->share_lease, &conflict);

        /* A new open that conflicts with a *disconnected* durable handle must
         * not be refused: MS-SMB2 has the disconnected (non-persistent) handle
         * yield.  The conflict may be the parked open's share reservation
         * (DENIED) or its HANDLE-caching lease that the share acquire must break
         * (BREAKING).  Resolve the conflicting lease back to its owning open and,
         * if that open is a parked durable, purge it and retry.  Live,
         * persistent, and non-durable holders are left intact, so they still
         * produce a real SHARING_VIOLATION.  Bounded by the parked-owner count. */
        int purge_guard = 64;
        while (result != CHIMERA_VFS_LEASE_GRANTED && conflict && purge_guard-- > 0) {
            uint64_t conflict_pid;

            if (conflict->kind == CHIMERA_VFS_LEASE_SHARE) {
                /* Share reservation: owner_lo carries the open's persistent id. */
                conflict_pid = conflict->owner.owner_lo;
            } else if (conflict->kind == CHIMERA_VFS_LEASE_CACHING &&
                       conflict->grant &&
                       ((struct chimera_smb_open_file *) conflict->grant->holders)) {
                /* Caching lease: owner_lo is the lease key; resolve the owning open
                 * via the grant's member list (cb_private now points at the grant,
                 * which may be shared by several opens -- any member identifies the
                 * parked durable handle to purge). */
                conflict_pid = ((struct chimera_smb_open_file *)
                                conflict->grant->holders)->file_id.pid;
            } else {
                break;
            }

            if (!chimera_smb_durable_purge_parked(thread, conflict_pid)) {
                break;
            }
            conflict = NULL;
            result   = chimera_vfs_state_try_insert(vfs_state, file_state,
                                                    &open_file->share_lease, &conflict);
        }

        if (result == CHIMERA_VFS_LEASE_BREAKING &&
            request->create.gen_finish_cb) {
            /* A batch (handle-caching) oplock holder is mid-break and may close
             * its deferred handle, freeing this share conflict.  Park the open:
             * lease_acquire re-probes and enqueues the ticket, resuming
             * chimera_smb_create_share_park_cb when the holder closes (GRANTED)
             * or merely acks while keeping the handle (DENIED -> SHARING_VIOLATION).
             * The share_lease is not yet inserted, so lease_acquire owns it now. */
            request->create.gen_parked_open  = open_file;
            request->create.gen_parked_fs    = file_state;
            request->create.gen_held_granted = held_granted;
            request->create.gen_parked       = 1;
            chimera_vfs_lease_acquire(vfs_state, file_state,
                                      &open_file->share_lease,
                                      &request->create.gen_ticket, true,
                                      chimera_smb_create_share_park_cb, request);
            return NULL;
        }

        if (result != CHIMERA_VFS_LEASE_GRANTED) {
            chimera_vfs_state_put(vfs_state, file_state);
            open_file->handle = NULL;
            chimera_smb_open_file_free(thread, open_file);
            return NULL;
        }

        chimera_smb_create_finish_share_grant(open_file, file_state,
                                              held_granted);
    }

    return chimera_smb_create_after_share(request, open_file);
} /* chimera_smb_create_gen_open_file */

static struct chimera_smb_open_file *
chimera_smb_create_after_share(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file)
{
    struct chimera_smb_compound      *compound        = request->compound;
    struct chimera_server_smb_thread *thread          = compound->thread;
    struct chimera_smb_tree          *tree            = request->tree;
    enum chimera_smb_open_file_type   type            = open_file->type;
    struct chimera_vfs_open_handle   *oh              = open_file->handle;
    int                               delete_on_close =
        (open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE) != 0;
    const void                       *parent_fh     = open_file->parent_fh_len ? open_file->parent_fh : NULL;
    int                               parent_fh_len = open_file->parent_fh_len;
    const char                       *name          = open_file->name;
    int                               name_len      = open_file->name_len;
    uint64_t                          open_file_bucket;

    /* Acquire a CACHING lease (SMB2 lease via RqLs, or legacy oplock
     * via requested_oplock_level).  This is opportunistic — failure to
     * grant just means the open succeeds with no lease.  Conflicts are
     * resolved by vfs_state's conflict matrix; without break_cb wired
     * (next task in Stage D), conflicting other-client leases force
     * DENIED here, so we silently end up with no lease. */
    if (type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE && oh &&
        !(open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY)) {
        struct chimera_vfs_state      *vfs_state = thread->vfs_thread->vfs->vfs_state;
        struct chimera_vfs_file_state *file_state;
        uint8_t                        req_smb  = 0;
        uint8_t                        req_vfs  = 0;
        bool                           via_rqls = false;
        struct chimera_vfs_lease      *conflict = NULL;
        enum chimera_vfs_lease_result  result;
        bool                           durable_request =
            thread->shared->config.persistent_handles &&
            (request->create.ctx_present_mask &
             (CHIMERA_SMB_CREATE_CTX_DHNQ | CHIMERA_SMB_CREATE_CTX_DH2Q)) != 0;

        /* Phase 2 of the conflicting-open oplock break.  Phase 1 (the batch /
         * handle-caching break that must precede the share-mode check) already
         * ran before the share reservation above.  Now that this open is
         * granted, break a conflicting *exclusive* (W-only) holder down to
         * LEVEL_II; a truncating open replaces the data, so it instead
         * invalidates every cached holder all the way to NONE.  This fires
         * whether or not this open requests an oplock of its own (so it covers
         * a plain reader / writer that takes no lease), and is a no-op when
         * there is no conflicting holder or only the opener's own. */
        chimera_smb_create_break_for_open(thread, request, open_file, oh, 2);

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

        /* SMB lease bits use R=0x01, H=0x02, W=0x04 — a different layout from
         * vfs_state's R/W/H mask, so map field-by-field.  We grant the full
         * requested set: read caching (R → LEVEL_II), write caching
         * (W → EXCLUSIVE) and handle caching (H → BATCH).  The vfs_state
         * conflict matrix keeps W single-writer-exclusive and H
         * single-holder-exclusive, and the SMB layer breaks holders when a
         * conflicting open / write / byte-range-lock / delete arrives.
         *
         * The two coherence hazards that previously forced a read-only policy
         * (W and H withheld) are now handled in the break path rather than by
         * withholding the cache:
         *   - delete-of-open-file: unlink / rename / delete-on-close break the
         *     H (handle-caching) holder so its deferred-close handle is recalled
         *     before the remove (smb_proc_close.c / set_info / rename) — fixes
         *     the old cthon op_unlk EBUSY.
         *   - server-side copy: COPYCHUNK breaks the source's caching lease to
         *     force a flush before it reads (smb_proc_copychunk.c) — fixes the
         *     old fsx READ BAD DATA. */
        req_vfs = chimera_smb_lease_bits_to_vfs(req_smb);

        /* Oplocks are about data caching: an attribute-only open (no
         * read/write/execute data access) neither acquires nor breaks an
         * oplock — UNLESS its disposition replaces the file's data
         * (OVERWRITE / OVERWRITE_IF / SUPERSEDE all truncate), which is a
         * data-modifying open and does break.  So gate on data access OR a
         * data-replacing disposition; a plain READ_ATTRIBUTES|SYNCHRONIZE
         * probe with OPEN/OPEN_IF leaves an existing holder's oplock intact. */
        bool caching_touches_data =
            (request->create.desired_access & SMB2_DATA_ACCESS_MASK) ||
            request->create.create_disposition == SMB2_FILE_OVERWRITE ||
            request->create.create_disposition == SMB2_FILE_OVERWRITE_IF ||
            request->create.create_disposition == SMB2_FILE_SUPERSEDE;

        /* Only advertise a read-caching oplock on backends that can serve a
         * server-side copy (FSCTL_SRV_COPYCHUNK -> chimera_vfs_copy_range).
         * Under a read cache the client offloads copy_file_range to the
         * server; if the backend can't do server-side copy the client falls
         * back to a cache-mediated copy that reads stale data (fsx READ BAD
         * DATA on diskfs/cairn).  Where copy_range is unavailable we leave the
         * file uncached (write-through), which is correct.
         *
         * Durable-aware opens are the exception: MS-SMB2 3.3.5.9 requires a
         * durable grant to be paired with a batch oplock or HANDLE-caching
         * lease, and durable-aware clients (WPTS, smbtorture durable suites)
         * verify that response context on the initial CREATE.  Without a
         * caching grant the durable request is silently dropped and the suite
         * fails before it even disconnects.  Durable-aware Windows clients do
         * not stream copy_file_range through a durable handle, so opting in
         * for those opens does not re-introduce the fsx hazard. */
        bool backend_copy_safe =
            (oh->vfs_module->capabilities & CHIMERA_VFS_CAP_COPY_RANGE) != 0 ||
            durable_request;

        /* An explicitly requested SMB2 lease (RqLs) is acquired even for an
         * attribute-only ("stat") open: a handle/read-caching lease is about the
         * handle, not data access, and such an open is still eligible for a
         * durable handle (MS-SMB2).  The caching_touches_data gate only governs
         * the implicit legacy-oplock path, where an attribute probe must not
         * acquire an oplock. */
        /* A caching lease (oplock / SMB2 lease) lives in a VFS-owned, owner-keyed,
         * refcounted grant.  Opens by one client under one lease key (RqLs) COALESCE
         * onto a single grant; legacy oplocks are keyed by file id and so are always
         * sole members.  We ENTER this block for EVERY RqLs open -- even one that
         * requests no caching bits -- so a re-open under an existing lease key joins
         * (and never downgrades) the shared lease (MS-SMB2 3.3.5.9.8: a lease is not
         * reduced by a subsequent open).  A *fresh* grant is created only when the
         * open warrants caching (real bits, on a copy-safe backend). */
        bool want_fresh_caching =
            (req_vfs != 0 && (caching_touches_data || via_rqls) && backend_copy_safe);

        if (via_rqls || want_fresh_caching) {
            file_state = chimera_vfs_state_get(vfs_state,
                                               oh->fh, oh->fh_len,
                                               oh->fh_hash, true);
            if (file_state) {
                struct chimera_vfs_caching_grant *grant = NULL;
                struct chimera_vfs_lease_owner    owner;
                struct chimera_vfs_lease_mode     want;

                memset(&owner, 0, sizeof(owner));
                owner.protocol   = CHIMERA_VFS_LEASE_PROTO_SMB2;
                owner.client_key = request->session_handle->session->client_key;
                /* vfs_state invokes this to notify the client when another acquirer
                 * needs the lease; the callback resolves the grant from lease->grant
                 * and picks a live member to deliver the OPLOCK_BREAK on. */
                owner.break_cb = chimera_smb_lease_break_cb;
                /* Anchor to THIS open's VFS handle so a setattr through the same
                 * handle does not recall the lease against itself.  A coalesced
                 * second open keeps the first open's handle; that per-open self-skip
                 * is then conservative, not load-bearing. */
                owner.op_handle = oh;
                if (via_rqls) {
                    /* RqLs: owner identity is the lease key, so same-key opens by
                     * one client coalesce (the Samba locking.tdb rule). */
                    memcpy(&owner.owner_lo, request->create.rqls.key, 8);
                    memcpy(&owner.owner_hi, request->create.rqls.key + 8, 8);
                    /* Mirror the lease_key onto the open_file for the break
                     * notification builder and the lease-key resolver. */
                    memcpy(open_file->lease_key, request->create.rqls.key, 16);
                    if (request->create.rqls.is_v2) {
                        memcpy(open_file->parent_lease_key,
                               request->create.rqls.parent_key, 16);
                    }
                } else {
                    /* Legacy oplock: each open is its own owner (file id). */
                    owner.owner_lo = open_file->file_id.pid;
                    owner.owner_hi = open_file->file_id.vid;
                }
                open_file->create_conn = request->compound->conn;

                want.granted = req_vfs;
                want.denied  = 0;

                /* First coalesce onto an existing same-owner grant: refcount + a
                 * conflict-free in-place upgrade, never a downgrade.  This is what a
                 * lease re-open does -- including one requesting fewer/no bits, which
                 * keeps the lease at its current state (3.3.5.9.8). */
                grant = chimera_vfs_caching_grant_coalesce(file_state, &owner,
                                                           want, 1 /*upgrade_ok*/);
                if (grant) {
                    /* Joined a live grant (it already has member(s)): registering
                     * this open cannot race a memberless break. */
                    chimera_smb_grant_add_member(grant, open_file);
                } else if (want_fresh_caching) {
                    /* No existing grant: create one and arbitrate against other
                     * owners, stepping W|H -> R on conflict (the sole-access rule,
                     * or a holder still mid-downgrade) until a shared read cache is
                     * grantable.  Reuse the one grant across retries so a kicked-off
                     * break is not restarted with the wrong needed-mode. */
                    grant = calloc(1, sizeof(*grant));
                    if (grant) {
                        int settle_guard = 6;

                        grant->file     = file_state;
                        grant->refcount = 1;
                        /* Legacy oplock vs RqLs lease: a legacy oplock's handle is
                         * broken before the share check, a lease's is not. */
                        grant->is_oplock = !via_rqls;
                        /* Only a v2 lease versions its state with an epoch; v1
                         * leases and legacy oplocks break with epoch 0. */
                        grant->is_v2 = via_rqls && request->create.rqls.is_v2;
                        /* The grant owns the epoch so coalesced opens and breaks
                         * share one counter; a v2 lease is granted at the client's
                         * epoch + 1 (1 for a brand-new lease, 3.3.5.9.11). */
                        grant->epoch =
                            (via_rqls && request->create.rqls.is_v2)
                            ? request->create.rqls.epoch + 1 : 1;
                        grant->lease.grant            = grant;
                        grant->lease.kind             = CHIMERA_VFS_LEASE_CACHING;
                        grant->lease.mode             = want;
                        grant->lease.owner            = owner;
                        grant->lease.owner.cb_private = grant;

                        /* Register the member BEFORE the lease becomes visible
                         * (try_insert links it onto caching_leases): once linked a
                         * conflicting acquirer can fire the break callback, which
                         * must find a live member or it revokes the fresh lease. */
                        chimera_smb_grant_add_member(grant, open_file);

                        result = chimera_vfs_state_try_insert(vfs_state, file_state,
                                                              &grant->lease, &conflict);
                        while (result != CHIMERA_VFS_LEASE_GRANTED &&
                               settle_guard-- > 0) {
                            /* W (write cache) is exclusive across lease keys; a
                             * conflicting holder forces it off but R+H stay shared.
                             * Drop only W first (-> R|H), then -- if R|H itself is
                             * still hard-denied (a cross-client handle conflict) --
                             * step down to R; give up only when even R is
                             * unobtainable and we are not merely awaiting a break. */
                            if (grant->lease.mode.granted & CHIMERA_VFS_LEASE_MODE_W) {
                                grant->lease.mode.granted &= ~CHIMERA_VFS_LEASE_MODE_W;
                            } else if (grant->lease.mode.granted !=
                                       CHIMERA_VFS_LEASE_MODE_R &&
                                       result == CHIMERA_VFS_LEASE_DENIED) {
                                grant->lease.mode.granted = CHIMERA_VFS_LEASE_MODE_R;
                            } else if (result != CHIMERA_VFS_LEASE_BREAKING) {
                                break;
                            }
                            result = chimera_vfs_state_try_insert(
                                vfs_state, file_state, &grant->lease, &conflict);
                        }

                        if (result == CHIMERA_VFS_LEASE_GRANTED) {
                            /* Link into the owner index so later same-key opens
                             * coalesce onto this grant. */
                            chimera_vfs_caching_grant_link(file_state, grant);
                        } else {
                            /* try_insert never linked the lease; drop the member and
                             * free the grant. */
                            chimera_smb_grant_remove_member(grant, open_file);
                            free(grant);
                            grant = NULL;
                        }
                    }
                }

                if (grant) {
                    uint8_t granted_vfs = grant->lease.mode.granted;

                    open_file->grant                  = grant;
                    open_file->caching_file_state     = file_state;
                    open_file->caching_lease_inserted = true;
                    /* If this open coalesced onto a grant whose lease is currently
                     * mid-break, the client is told its lease state but with
                     * BREAK_IN_PROGRESS set (MS-SMB2 3.3.5.9.11: a lease-key re-open
                     * during a break succeeds and reports the break is underway). */
                    open_file->lease_flags =
                        (grant->lease.break_state == CHIMERA_VFS_BREAK_BREAKING)
                        ? SMB2_LEASE_FLAG_BREAK_IN_PROGRESS : 0;
                    /* Report the grant's ACTUAL granted mode: a coalesced open
                     * inherits the shared lease's current state (an upgrade may have
                     * widened it; a conflicting peer may have capped it). */
                    open_file->lease_state =
                        chimera_smb_vfs_to_lease_bits(granted_vfs);
                    if (via_rqls) {
                        open_file->oplock_level = SMB2_OPLOCK_LEVEL_LEASE;
                        /* Report the lease's epoch on any open that joins a v2
                         * lease -- including a v1 request coalescing onto an
                         * existing v2 lease (the response follows the lease's
                         * version, set at first grant). */
                        if (grant->is_v2) {
                            open_file->lease_epoch = grant->epoch;
                        }
                    } else {
                        open_file->oplock_level =
                            chimera_smb_vfs_to_oplock_level(granted_vfs);
                    }
                } else {
                    /* No caching grant taken.  A bare RqLs lease is still a lease
                     * (report OPLOCK_LEVEL_LEASE and echo key/epoch in the response);
                     * a legacy open with no oplock keeps its NONE defaults. */
                    chimera_vfs_state_put(vfs_state, file_state);
                    file_state = NULL;
                    if (via_rqls) {
                        if (request->create.rqls.is_v2) {
                            open_file->lease_epoch = request->create.rqls.epoch + 1;
                        }
                        open_file->lease_state  = 0;
                        open_file->oplock_level = SMB2_OPLOCK_LEVEL_LEASE;
                    }
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

    /* Durable / persistent handle grant.  MS-SMB2 3.3.5.9: a durable handle is
     * granted only when the open also holds a batch oplock or a lease that
     * includes HANDLE caching — otherwise a survived handle could not be safely
     * reclaimed.  oplock_level / lease_state were set by the caching-lease block
     * above. */
    if (type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE && tree->share &&
        thread->shared->config.persistent_handles) {
        uint32_t ctx = request->create.ctx_present_mask;
        /* A durable handle requires a batch oplock or a HANDLE-caching lease
         * (MS-SMB2 3.3.5.9.10/9.11).  A *persistent* handle (DH2Q + PERSISTENT
         * on a continuous-availability share, 3.3.5.9.12) is exempt — the CA
         * share provides the durability guarantee, so it is granted with no
         * oplock/lease. */
        bool     has_caching = (open_file->oplock_level == SMB2_OPLOCK_LEVEL_BATCH) ||
            (open_file->lease_state & SMB2_LEASE_HANDLE_CACHING);

        if (ctx & CHIMERA_SMB_CREATE_CTX_DH2Q) {
            bool persistent = (request->create.dh2q.flags & SMB2_DHANDLE_FLAG_PERSISTENT) &&
                tree->share->continuous_availability;

            if (has_caching || persistent) {
                open_file->durable_flags = CHIMERA_SMB_DURABLE_V2;
                memcpy(open_file->create_guid, request->create.dh2q.create_guid, 16);
                if (persistent) {
                    open_file->durable_flags |= CHIMERA_SMB_DURABLE_PERSISTENT;
                }
                if (request->create.dh2q.timeout_ms == 0) {
                    open_file->durable_timeout_ms = CHIMERA_SMB_DURABLE_TIMEOUT_DEFAULT_MS;
                } else if (request->create.dh2q.timeout_ms > CHIMERA_SMB_DURABLE_TIMEOUT_MAX_MS) {
                    open_file->durable_timeout_ms = CHIMERA_SMB_DURABLE_TIMEOUT_MAX_MS;
                } else {
                    open_file->durable_timeout_ms = request->create.dh2q.timeout_ms;
                }
            }
        } else if ((ctx & CHIMERA_SMB_CREATE_CTX_DHNQ) && has_caching) {
            /* Durable v1: no timeout or create-guid on the wire. */
            open_file->durable_flags      = CHIMERA_SMB_DURABLE_V1;
            open_file->durable_timeout_ms = CHIMERA_SMB_DURABLE_TIMEOUT_DEFAULT_MS;
        }
    }

    open_file_bucket = open_file->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    pthread_mutex_lock(&tree->open_files_lock[open_file_bucket]);

    HASH_ADD(hh, tree->open_files[open_file_bucket], file_id, sizeof(open_file->file_id), open_file);

    pthread_mutex_unlock(&tree->open_files_lock[open_file_bucket]);

    /* Register the durable handle in the shared registry so it survives a
     * disconnect and can be reclaimed on reconnect.  Done outside the bucket
     * lock to keep the bucket -> registry lock order (see smb_durable.c). */
    if (open_file->durable_flags) {
        if (request->create.persist_pid != 0) {
            open_file->flags |= CHIMERA_SMB_OPEN_FILE_PERSISTED;
        }
        chimera_smb_durable_register(thread->shared, open_file,
                                     request->session_handle->session->session_id,
                                     request->compound->conn->client_guid,
                                     open_file->name, open_file->name_len,
                                     request->create.persist_pid != 0);
    }

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

    /* Persistent ids are drawn from a process-global monotonic counter rather
     * than a per-tree one so they stay unique across tree teardowns — durable
     * reconnect looks an open up by persistent id alone.  A persistent grant
     * (fresh or cold reclaim) pre-allocates its id so the on-disk record can be
     * written atomically with the open; adopt it here. */
    uint64_t                      pid = request->create.persist_pid ?
        request->create.persist_pid :
        atomic_fetch_add(&request->compound->thread->shared->next_persistent_id, 1);

    open_file = chimera_smb_create_gen_open_file(request,
                                                 CHIMERA_SMB_OPEN_FILE_TYPE_FILE,
                                                 NULL,
                                                 pid,
                                                 parent_fh,
                                                 parent_fh_len,
                                                 name,
                                                 name_len,
                                                 delete_on_close,
                                                 is_directory, oh);

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
                                            0,
                                            NULL);
} /* chimera_smb_create_gen_open_file_pipe */

static inline void
chimera_smb_create_release_handle(
    struct chimera_vfs_thread      *vfs_thread,
    struct chimera_vfs_open_handle *oh)
{
    if (oh) {
        chimera_vfs_release(vfs_thread, oh);
    }
} /* chimera_smb_create_release_handle */

static inline void
chimera_smb_create_release_parent(struct chimera_smb_request *request)
{
    if (request->create.parent_handle) {
        chimera_vfs_release(request->compound->thread->vfs_thread,
                            request->create.parent_handle);
        request->create.parent_handle = NULL;
    }
} /* chimera_smb_create_release_parent */

static inline void
chimera_smb_create_finish_share_grant(
    struct chimera_smb_open_file  *open_file,
    struct chimera_vfs_file_state *file_state,
    uint8_t                        held_granted)
{
    /* Drop the transient truncate-write grant: the handle holds only the
     * access it requested, so it must not block a later reader. */
    if (held_granted != open_file->share_lease.mode.granted) {
        pthread_mutex_lock(&file_state->lock);
        open_file->share_lease.mode.granted = held_granted;
        pthread_mutex_unlock(&file_state->lock);
    }

    open_file->share_file_state     = file_state;
    open_file->share_lease_inserted = true;
} /* chimera_smb_create_finish_share_grant */

static void
chimera_smb_create_share_park_cb(
    enum chimera_vfs_lease_result result,
    struct chimera_vfs_lease     *lease,
    struct chimera_vfs_lease     *conflict,
    void                         *private_data)
{
    struct chimera_smb_request       *request    = private_data;
    struct chimera_server_smb_thread *thread     = request->compound->thread;
    struct chimera_vfs_thread        *vfs_thread = thread->vfs_thread;
    struct chimera_vfs_state         *vfs_state  = vfs_thread->vfs->vfs_state;
    struct chimera_smb_open_file     *open_file  = request->create.gen_parked_open;
    struct chimera_vfs_file_state    *file_state = request->create.gen_parked_fs;

    (void) lease;
    (void) conflict;

    request->create.gen_parked = 0;

    if (result == CHIMERA_VFS_LEASE_GRANTED) {
        /* The batch holder closed; the share reservation is now held. */
        chimera_smb_create_finish_share_grant(open_file, file_state,
                                              request->create.gen_held_granted);
        chimera_smb_create_after_share(request, open_file);
        request->create.gen_finish_cb(request, open_file);
        return;
    }

    /* DENIED: the holder acked its oplock but kept the handle open, so the
     * share conflict stands. */
    chimera_vfs_state_put(vfs_state, file_state);
    if (open_file->handle) {
        chimera_vfs_release(vfs_thread, open_file->handle);
        open_file->handle = NULL;
    }
    chimera_smb_open_file_free(thread, open_file);
    chimera_smb_create_release_parent(request);
    chimera_smb_complete_request(request, SMB2_STATUS_SHARING_VIOLATION);
} /* chimera_smb_create_share_park_cb */

static inline uint32_t
chimera_smb_create_granted_access(
    struct chimera_smb_request     *request,
    const struct chimera_vfs_attrs *attr);

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
        chimera_smb_create_release_parent(request);
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
        chimera_smb_create_release_handle(vfs_thread, oh);
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, request->create.force_close_status);
        return;
    }

    request->create.r_open_file = open_file;

    /* Record the access this open was granted.  The other create/open
     * completion paths do this; without it a freshly-created directory
     * handle carries a stale granted_access (a recycled open_file can hold
     * 0), which breaks any access decision keyed on it (e.g. the
     * FILE_LIST_DIRECTORY gate on CHANGE_NOTIFY).  The creator of a new
     * object holds full control, so MAXIMUM_ALLOWED resolves to the full
     * mask; a specific-bits open was granted exactly what it requested. */
    if (request->create.desired_access & SMB2_MAXIMUM_ALLOWED) {
        open_file->granted_access = CHIMERA_ACE_MASK_ALL;
    } else {
        open_file->granted_access = chimera_smb_create_granted_access(request, NULL);
    }
    open_file->maximal_access = CHIMERA_ACE_MASK_ALL;

    chimera_smb_create_release_parent(request);
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
                CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT |
                CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_BTIME,
                0,
                0,
                chimera_smb_create_open_at_callback,
                request);
            return;
        }
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, error_code == CHIMERA_VFS_EEXIST ?
                                     SMB2_STATUS_OBJECT_NAME_COLLISION :
                                     SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
        return;
    }

    /* mkdir_at succeeded — the directory was created by this CREATE. */
    request->create.r_created = 1;

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

/* Completion of the chained chimera_vfs_open_stream for a "file:stream" CREATE.
 * Builds the SMB open_file around the STREAM handle (so reads/writes/setinfo
 * target the stream) and marshals the reply (base metadata + stream size). */
static void
chimera_smb_create_open_stream_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *stream_oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_smb_request     *request    = private_data;
    struct chimera_vfs_thread      *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_vfs_open_handle *base_oh    = request->create.base_oh;
    struct chimera_smb_open_file   *open_file;

    if (error_code != CHIMERA_VFS_OK) {
        uint32_t status;

        if (error_code == CHIMERA_VFS_ENOENT) {
            /* Opening a non-existent stream (FILE_OPEN / FILE_OVERWRITE). */
            status = SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
        } else if (error_code == CHIMERA_VFS_EEXIST) {
            status = SMB2_STATUS_OBJECT_NAME_COLLISION;
        } else {
            status = chimera_smb_create_error_status(error_code);
        }
        chimera_vfs_release(vfs_thread, base_oh);
        request->create.base_oh = NULL;
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, status);
        return;
    }

    request->create.r_created = stream_oh ? stream_oh->r_created : 0;

    open_file = chimera_smb_create_gen_open_file_normal(
        request,
        request->create.parent_handle->fh,
        request->create.parent_handle->fh_len,
        request->create.name,
        request->create.name_len,
        request->create.create_options & SMB2_FILE_DELETE_ON_CLOSE,
        0,             /* a named stream is never a directory */
        stream_oh);

    if (!open_file) {
        chimera_smb_create_release_handle(vfs_thread, stream_oh);
        chimera_vfs_release(vfs_thread, base_oh);
        request->create.base_oh = NULL;
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, request->create.force_close_status);
        return;
    }

    open_file->flags          |= CHIMERA_SMB_OPEN_FILE_FLAG_STREAM;
    open_file->stream_name_len = request->create.stream_name_len;
    memcpy(open_file->stream_name, request->create.stream_name,
           request->create.stream_name_len);
    open_file->base_fh_len = base_oh->fh_len;
    memcpy(open_file->base_fh, base_oh->fh, base_oh->fh_len);

    request->create.r_open_file = open_file;

    open_file->granted_access = chimera_smb_create_granted_access(request, attr);
    open_file->maximal_access =
        chimera_vfs_access_check(attr, &request->session_handle->session->cred,
                                 CHIMERA_ACE_MASK_ALL);

    /* attr carries the base file's metadata with the stream's size/alloc. */
    chimera_smb_marshal_attrs(attr, &request->create.r_attrs);

    /* The base handle is no longer needed; the stream handle owns the I/O. */
    chimera_vfs_release(vfs_thread, base_oh);
    request->create.base_oh = NULL;

    chimera_smb_create_release_parent(request);
    chimera_smb_open_file_release(request, open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_create_open_stream_callback */

/* The base file is open; open (and per the disposition create/truncate) the
 * named stream on it.  Gated on the backend advertising named-stream support. */
static void
chimera_smb_create_open_stream_chain(
    struct chimera_smb_request     *request,
    struct chimera_vfs_open_handle *base_oh)
{
    struct chimera_vfs_thread *vfs_thread = request->compound->thread->vfs_thread;
    unsigned int               sflags     = 0;

    if (!(base_oh->vfs_module->capabilities & CHIMERA_VFS_CAP_NAMED_STREAMS)) {
        chimera_vfs_release(vfs_thread, base_oh);
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_INVALID);
        return;
    }

    /* The disposition applies to the stream. */
    switch (request->create.create_disposition) {
        case SMB2_FILE_OPEN:
            break;
        case SMB2_FILE_OVERWRITE:
            sflags |= CHIMERA_VFS_OPEN_TRUNCATE;
            break;
        case SMB2_FILE_CREATE:
            sflags |= CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE;
            break;
        case SMB2_FILE_OPEN_IF:
            sflags |= CHIMERA_VFS_OPEN_CREATE;
            break;
        case SMB2_FILE_OVERWRITE_IF:
        case SMB2_FILE_SUPERSEDE:
            sflags |= CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_TRUNCATE;
            break;
    } /* switch */

    request->create.base_oh = base_oh;

    chimera_vfs_open_stream(
        vfs_thread,
        &request->session_handle->session->cred,
        base_oh,
        request->create.stream_name,
        request->create.stream_name_len,
        sflags,
        NULL,
        CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME | CHIMERA_VFS_ATTR_ACL,
        chimera_smb_create_open_stream_callback,
        request);
} /* chimera_smb_create_open_stream_chain */

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
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, chimera_smb_create_error_status(error_code));
        return;
    }

    /* Enforce the requested access against the object's ACL for every
     * disposition that can open an existing object.  Pure FILE_CREATE always
     * makes a new object (and fails with a collision otherwise), so the creator
     * implicitly holds it and we do not gate it here.  A newly-created object
     * reached via OPEN_IF/OVERWRITE_IF/SUPERSEDE carries the owner-full-control
     * default (or inherited) ACL, so the creator passes this check naturally. */
    if (request->create.create_disposition != SMB2_FILE_CREATE) {
        uint32_t access_status = chimera_smb_create_check_access(request, attr);

        if (access_status != SMB2_STATUS_SUCCESS) {
            chimera_vfs_release(vfs_thread, oh);
            chimera_vfs_release(vfs_thread, request->create.parent_handle);
            chimera_smb_complete_request(request, access_status);
            return;
        }
    }

    /* A lease key may be bound to only one file per client (MS-SMB2 3.3.5.9.8).
     * If this client already holds the requested lease key on a different file,
     * reject the open with STATUS_INVALID_PARAMETER (smb2.lease.request /
     * duplicate_create / duplicate_open).  A re-open of the same file under the
     * key coalesces and is not a conflict. */
    if ((request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) &&
        chimera_smb_session_lease_key_conflict(request->session_handle->session,
                                               request->create.rqls.key,
                                               oh->fh, oh->fh_len)) {
        chimera_vfs_release(vfs_thread, oh);
        chimera_vfs_release(vfs_thread, request->create.parent_handle);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* Named-stream open: the base file is now open; open the stream on it and
     * build the SMB open_file around the stream handle. */
    if (request->create.has_stream) {
        chimera_smb_create_open_stream_chain(request, oh);
        return;
    }

    /* Record whether the VFS open created the file (vs opened an existing one)
     * so the reply reports the correct Create Action. */
    request->create.r_created = oh ? oh->r_created : 0;

    /* Compute the attr-derived results up front -- they do not depend on the
     * share/oplock outcome, and `attr` is not available later if the open parks
     * on a batch-oplock break (chimera_smb_create_share_park_cb resumes without
     * it).  chimera_smb_create_open_finish applies them on the synchronous OR
     * parked grant. */
    request->create.r_is_directory   = S_ISDIR(attr->va_mode) ? 1 : 0;
    request->create.r_granted_access = chimera_smb_create_granted_access(request, attr);
    request->create.r_maximal_access =
        chimera_vfs_access_check(attr, &request->session_handle->session->cred,
                                 CHIMERA_ACE_MASK_ALL);
    chimera_smb_marshal_attrs(attr, &request->create.r_attrs);
    /* The park-on-batch-break activation (gen_finish_cb) is wired in the
     * follow-up that adds the cross-thread resume bounce; until then the open
     * resolves synchronously (a batch-share conflict yields SHARING_VIOLATION). */

    open_file = chimera_smb_create_gen_open_file_normal(request,
                                                        request->create.parent_handle->fh,
                                                        request->create.parent_handle->fh_len,
                                                        request->create.name,
                                                        request->create.name_len,
                                                        request->create.create_options & SMB2_FILE_DELETE_ON_CLOSE,
                                                        S_ISDIR(attr->va_mode),
                                                        oh);

    if (request->create.gen_parked) {
        /* Parked on a batch-oplock break; the park cb finishes the open. */
        return;
    }

    if (!open_file) {
        chimera_smb_create_release_handle(vfs_thread, oh);
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, request->create.force_close_status);
        return;
    }

    chimera_smb_create_open_finish(request, open_file);
} /* chimera_smb_create_open_at_callback */

/* Break-deadline for a CREATE parked on a lease break (MS-SMB2 3.3.5.9 / the
 * oplock-break timeout): the holder never acknowledged within the deadline, so
 * forcibly revoke the still-breaking holder(s) and complete the pending open with
 * its already-acquired lease.  Runs on the open's own conn thread (the timer is
 * armed on that thread's evpl).  An ack that resumes the open first removes this
 * timer. */
static void
chimera_smb_create_park_deadline_cb(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_smb_request       *request = (struct chimera_smb_request *)
        ((char *) timer - offsetof(struct chimera_smb_request, async.timer));
    struct chimera_server_smb_thread *thread    = request->compound->thread;
    struct chimera_vfs_state         *vfs_state =
        thread->vfs_thread->vfs->vfs_state;
    struct chimera_smb_open_file     *open_file = request->create.r_open_file;

    (void) evpl;

    chimera_vfs_state_revoke_breaks(vfs_state, request->create.park_fh,
                                    request->create.park_fh_len,
                                    request->create.park_fh_hash,
                                    open_file ? open_file->grant : NULL);

    chimera_smb_open_file_release(request, open_file);
    /* complete_request retires the async-interim (unlink from parked_requests +
     * clear armed); the fired one-shot is already out of the timer heap so its
     * removal there no-ops.  async_id stays set -> final reply is ASYNC-tagged. */
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_create_park_deadline_cb */

/* Finish a regular-file open: apply the precomputed access masks, emit the
 * parent-directory create notification, release the parent handle, and complete
 * the request.  Invoked on a synchronous grant from open_at_callback and on a
 * parked grant from chimera_smb_create_share_park_cb. */
static void
chimera_smb_create_open_finish(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file)
{
    request->create.r_open_file = open_file;

    open_file->granted_access = request->create.r_granted_access;
    open_file->maximal_access = request->create.r_maximal_access;

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
        uint32_t                          action = request->create.r_is_directory ?
            CHIMERA_VFS_NOTIFY_DIR_ADDED : CHIMERA_VFS_NOTIFY_FILE_ADDED;

        /* Creating/opening a (regular) file's data fork introduces its
         * $DATA stream into the file's stream namespace, so also raise the
         * STREAM_NAME class for non-directories.  This lets a watcher that
         * requested only FILE_NOTIFY_CHANGE_STREAM_NAME observe the stream
         * appearing (WPTS BVT_SMB2Basic_ChangeNotify_ChangeStreamName). */
        if (!request->create.r_is_directory) {
            action |= CHIMERA_VFS_NOTIFY_STREAM_NAME;
        }

        chimera_vfs_notify_emit(thread->shared->vfs->vfs_notify,
                                request->create.parent_handle->fh,
                                request->create.parent_handle->fh_len,
                                action,
                                request->create.name,
                                request->create.name_len,
                                NULL, 0);
    }

    chimera_smb_create_release_parent(request);

    /* MS-SMB2 3.3.5.9 pending-open: if this open triggered an ack-required lease
     * break on another holder, hold the SUCCESS response until the holder
     * acknowledges (the break is mid-flight).  Emit an async-interim
     * STATUS_PENDING now (so the client knows the create is in flight, can cancel
     * it, and does not time out across the up-to-break-timeout wait); the request
     * lands on conn->parked_requests, and an inbound OPLOCK_BREAK ack that settles
     * the file's caching leases resumes it (chimera_smb_create_resume_parked).
     * The open_file ref is held across the wait and dropped on resume. */
    if (open_file->handle) {
        struct chimera_server_smb_thread *thread    = request->compound->thread;
        struct chimera_vfs_state         *vfs_state =
            thread->vfs_thread->vfs->vfs_state;
        struct chimera_vfs_open_handle   *oh = open_file->handle;

        if (chimera_vfs_state_caching_breaking(vfs_state, oh->fh, oh->fh_len,
                                               oh->fh_hash, open_file->grant)) {
            memcpy(request->create.park_fh, oh->fh, oh->fh_len);
            request->create.park_fh_len  = oh->fh_len;
            request->create.park_fh_hash = oh->fh_hash;
            chimera_smb_async_interim_begin(request);
            /* Arm a deadline: if the holder never acks the break, fire at the
             * break timeout to revoke it and complete this open anyway (MS-SMB2
             * break-timeout).  Cancelled when an ack resumes the open first. */
            evpl_add_oneshot_timer(thread->evpl, &request->async.timer,
                                   chimera_smb_create_park_deadline_cb,
                                   (uint64_t) vfs_state->default_break_deadline_ms
                                   * 1000);
            return;
        }
    }

    chimera_smb_open_file_release(request, open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_create_open_finish */

/* Resume the settled parked CREATEs on one connection.  Pull every parked
 * CREATE whose triggered lease break has now settled (no caching lease on the
 * file is mid-break) off `conn`'s parked list, then complete them on this
 * thread.  `conn` must be owned by `thread` so the deferred response's
 * thread-local iovecs are produced on the right thread.  The
 * chimera_vfs_state_caching_breaking() re-check is what makes a blind sweep
 * safe: only genuinely-settled creates complete. */
static void
chimera_smb_create_resume_parked_conn(
    struct chimera_server_smb_thread *thread,
    struct chimera_vfs_state         *vfs_state,
    struct chimera_smb_conn          *conn)
{
    struct chimera_smb_request *req, **pp, *resume = NULL;

    /* Collect first (unlinking + clearing armed) so the completion path does not
     * re-walk / re-cancel the list we are iterating. */
    pp = &conn->parked_requests;
    while ((req = *pp)) {
        if (req->smb2_hdr.command == SMB2_CREATE &&
            !chimera_vfs_state_caching_breaking(vfs_state,
                                                req->create.park_fh,
                                                req->create.park_fh_len,
                                                req->create.park_fh_hash,
                                                req->create.r_open_file
                                                ? req->create.r_open_file->grant
                                                : NULL)) {
            *pp                  = req->async.park_next;
            req->async.park_next = resume;
            req->async.armed     = 0; /* already unlinked; skip re-cancel */
            /* Cancel the break-deadline timer so it cannot fire after the open
             * has been completed here. */
            evpl_remove_timer(thread->evpl, &req->async.timer);
            resume = req;
        } else {
            pp = &req->async.park_next;
        }
    }

    while (resume) {
        req                  = resume;
        resume               = req->async.park_next;
        req->async.park_next = NULL;
        chimera_smb_open_file_release(req, req->create.r_open_file);
        /* async_id is still set, so the final reply carries
         * SMB2_FLAGS_ASYNC_COMMAND matching the interim. */
        chimera_smb_complete_request(req, SMB2_STATUS_SUCCESS);
    }
} /* chimera_smb_create_resume_parked_conn */

/* Resume any CREATE parked on `ack_request`'s OWN connection whose triggered
 * lease break has now settled.  Called from the inbound OPLOCK_BREAK ack handler
 * after the lease is acked.  This is the single-client fast path: a client that
 * breaks its own lease (e.g. a lease upgrade) has its pending create on the same
 * connection the ack arrived on, so the deferred response's thread-local iovecs
 * are produced on the right thread.  The two-client case -- where B's CREATE
 * parked on B's connection/thread triggered the break A just acked on A's
 * thread -- is handled by chimera_smb_create_resume_parked_broadcast, which
 * rings every peer thread's resume doorbell so each completes its own parked
 * CREATEs locally. */
void
chimera_smb_create_resume_parked(struct chimera_smb_request *ack_request)
{
    struct chimera_server_smb_thread *thread    = ack_request->compound->thread;
    struct chimera_vfs_state         *vfs_state = thread->vfs_thread->vfs->vfs_state;
    struct chimera_smb_conn          *conn      = ack_request->compound->conn;

    if (conn) {
        chimera_smb_create_resume_parked_conn(thread, vfs_state, conn);
    }

    /* Wake peer threads to resume CREATEs parked on their own connections (the
     * two-client lease break: the opener's parked CREATE lives on a different
     * connection/thread than the one this ack arrived on). */
    chimera_smb_create_resume_parked_broadcast(thread);
} /* chimera_smb_create_resume_parked */

/* Resume doorbell handler: runs on its owning SMB thread.  An OPLOCK_BREAK ack
 * settled a lease on some (possibly different) thread; re-scan every connection
 * this thread owns and complete any parked CREATE whose break has now settled.
 * The per-CREATE caching_breaking() re-check gates correctness, so this blind
 * sweep only completes genuinely-settled opens; a CREATE still waiting on an
 * unsettled break is left parked (its own deadline / a later ack resumes it). */
SYMBOL_EXPORT void
chimera_smb_create_resume_doorbell_callback(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_server_smb_thread *thread;
    struct chimera_vfs_state         *vfs_state;
    struct chimera_smb_conn          *conn, *tmp;

    (void) evpl;

    thread = container_of(doorbell, struct chimera_server_smb_thread,
                          lease_resume_doorbell);
    vfs_state = thread->vfs_thread->vfs->vfs_state;

    DL_FOREACH_SAFE2(thread->active_conns, conn, tmp, active_next)
    {
        chimera_smb_create_resume_parked_conn(thread, vfs_state, conn);
    }
} /* chimera_smb_create_resume_doorbell_callback */

/* Ring every PEER SMB thread's resume doorbell so each re-scans its own
 * connections for parked CREATEs the just-settled lease break unblocked.
 * `origin` is skipped because the ack handler already swept its own connection
 * inline; the per-CREATE caching_breaking() re-check would make a re-sweep of
 * `origin` harmless anyway.  The deferred CREATE responses' iovecs are
 * thread-local, so each thread must complete its own. */
void
chimera_smb_create_resume_parked_broadcast(struct chimera_server_smb_thread *origin)
{
    struct chimera_server_smb_shared *shared = origin->shared;
    struct chimera_server_smb_thread *t;

    pthread_mutex_lock(&shared->threads_lock);
    for (t = shared->threads; t; t = t->next_thread) {
        if (t == origin) {
            continue;
        }
        evpl_ring_doorbell(&t->lease_resume_doorbell);
    }
    pthread_mutex_unlock(&shared->threads_lock);
} /* chimera_smb_create_resume_parked_broadcast */

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

    /* ACCESS_SYSTEM_SECURITY (open a handle to the SACL) requires
     * SeSecurityPrivilege.  chimera has no privilege model and grants it to no
     * unprivileged caller, so deny it as Windows does -- STATUS_PRIVILEGE_NOT_HELD
     * -- rather than silently ignoring the bit. */
    if ((da & SMB2_ACCESS_SYSTEM_SECURITY) &&
        request->session_handle->session->cred.uid != 0) {
        return SMB2_STATUS_PRIVILEGE_NOT_HELD;
    }

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
    /* SYNCHRONIZE is not a meaningful open-time access gate: Windows includes it
     * in every generic right and clients request it on essentially every open,
     * so it is always grantable on a successful open.  Treat it like
     * MAXIMUM_ALLOWED and do not require an explicit ACE for it (otherwise a
     * plain mode-derived object such as a freshly mounted share root, whose
     * synthesised ACEs carry no SYNCHRONIZE bit, would deny every open). */
    req = da & ~(SMB2_MAXIMUM_ALLOWED | SMB2_ACCESS_SYSTEM_SECURITY |
                 SMB2_DELETE | SMB2_SYNCHRONIZE |
                 SMB2_GENERIC_READ | SMB2_GENERIC_WRITE |
                 SMB2_GENERIC_EXECUTE | SMB2_GENERIC_ALL);

    if (da & SMB2_GENERIC_READ) {
        req |= 0x00120089; /* READ_DATA|READ_NAMED_ATTRS|READ_ATTRS|READ_ACL|SYNC */
    }
    if (da & SMB2_GENERIC_WRITE) {
        req |= 0x00120116; /* WRITE_DATA|APPEND|WRITE_NAMED_ATTRS|WRITE_ATTRS|READ_ACL|SYNC */
    }
    if (da & SMB2_GENERIC_EXECUTE) {
        /* FILE_GENERIC_EXECUTE without the standalone FILE_EXECUTE data right:
         * a caller able to read a file may also open it for generic-execute, so
         * its required bits are a subset of GENERIC_READ.  (A bare FILE_EXECUTE
         * request keeps the 0x20 bit below and is still gated on an execute
         * grant.) */
        req |= 0x00120080; /* READ_ATTRS|READ_ACL|SYNC */
    }
    if (da & SMB2_GENERIC_ALL) {
        req |= CHIMERA_ACE_MASK_ALL;
    }

    /* Truncating dispositions implicitly require write access to the existing
     * data, regardless of what the caller asked for: overwriting a read-only
     * file is denied even for a read-only open. */
    if (request->create.create_disposition == SMB2_FILE_SUPERSEDE ||
        request->create.create_disposition == SMB2_FILE_OVERWRITE ||
        request->create.create_disposition == SMB2_FILE_OVERWRITE_IF) {
        req |= CHIMERA_ACE_WRITE_DATA;
    }

    if (!req) {
        return SMB2_STATUS_SUCCESS;
    }

    /* Windows owner semantics on a mode-only object: a file's owner always holds
     * an implicit WRITE_DAC, so it can rewrite the security descriptor at will
     * and is therefore granted the access it asks for over its own object.  The
     * engine backends (e.g. memfs) encode this as an owner-full-control default
     * DACL stamped on an SMB-created object, so this check passes naturally
     * there.  The mode-only backends (linux/io_uring/cairn/diskfs) report only
     * the POSIX mode and no explicit ACL; under POSIX mode evaluation the owner
     * is NOT granted the WRITE_OWNER / WRITE_ATTRIBUTES / SYNCHRONIZE / EXECUTE
     * bits a Windows CREATE routinely requests with FILE_ALL_ACCESS, so an owner
     * opening its own file would be wrongly denied.  Recognise the owner of a
     * no-explicit-ACL object here and grant it, matching the owner-full-control
     * default DACL the engine backends materialise.  POSIX (NFS/S3) evaluation
     * is unaffected -- this is the SMB protocol layer applying Windows owner
     * rules, not a change to the shared engine.  A *different* caller still falls
     * through to the normal mode/ACL evaluation below and is gated correctly. */
    {
        const struct chimera_vfs_cred *cred =
            &request->session_handle->session->cred;
        int                            has_acl = (attr->va_set_mask & CHIMERA_VFS_ATTR_ACL) &&
            attr->va_acl && attr->va_acl->num_aces > 0;

        if (!has_acl &&
            cred->flavor != CHIMERA_VFS_AUTH_NONE &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_UID) &&
            (uint64_t) cred->uid == attr->va_uid) {
            return SMB2_STATUS_SUCCESS;
        }
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

    /* A durable reconnect reclaims an already-open handle: access was granted at
     * the original open and the reconnect's DesiredAccess field is ignored
     * (MS-SMB2 3.3.5.9.7/.12), so do not re-run the ACL check or recompute the
     * granted/maximal access — the surviving open carries them already. */
    if (!request->create.reconnect) {
        uint32_t access_status = chimera_smb_create_check_access(request, attr);

        if (access_status != SMB2_STATUS_SUCCESS) {
            chimera_smb_open_file_release(request, request->create.r_open_file);
            chimera_smb_complete_request(request, access_status);
            return;
        }

        request->create.r_open_file->granted_access =
            chimera_smb_create_granted_access(request, attr);
        request->create.r_open_file->maximal_access =
            chimera_vfs_access_check(attr, &request->session_handle->session->cred,
                                     CHIMERA_ACE_MASK_ALL);
    }

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
        chimera_smb_create_release_handle(vfs_thread, oh);
        chimera_smb_complete_request(request, request->create.force_close_status);
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


/* True for the dispositions that replace an existing file's contents
 * (OVERWRITE / OVERWRITE_IF / SUPERSEDE). */
static inline bool
chimera_smb_disposition_overwrites(uint32_t disposition)
{
    return disposition == SMB2_FILE_OVERWRITE ||
           disposition == SMB2_FILE_OVERWRITE_IF ||
           disposition == SMB2_FILE_SUPERSEDE;
} /* chimera_smb_disposition_overwrites */

/* Decide whether this open is a persistent-handle grant whose record must be
 * persisted with the open, and if so build the handle-state descriptor
 * (request->create.persist_*).  Returns true if persisting; the caller then
 * opens via chimera_vfs_open_at_hs.  The record is persisted atomically by
 * backends advertising CAP_ATOMIC_HANDLE_STATE, or by the VFS core into the
 * default KV for backends without native KV (see
 * chimera_vfs_can_persist_handle_state). */
static bool
chimera_smb_create_persist_prepare(
    struct chimera_smb_request     *request,
    struct chimera_vfs_open_handle *parent_handle)
{
    struct chimera_server_smb_shared *shared = request->compound->thread->shared;
    struct chimera_smb_tree          *tree   = request->tree;
    struct chimera_smb_durable_record rec;
    uint64_t                          pid;
    uint32_t                          name_len;

    if (!shared->config.persistent_handles ||
        tree->type != CHIMERA_SMB_TREE_TYPE_SHARE ||
        !tree->share || !tree->share->continuous_availability) {
        return false;
    }

    if (!(request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_DH2Q) ||
        !(request->create.dh2q.flags & SMB2_DHANDLE_FLAG_PERSISTENT)) {
        return false;
    }

    if (!chimera_vfs_can_persist_handle_state(request->compound->thread->vfs_thread,
                                              parent_handle)) {
        return false;
    }

    if (request->create.persist_pid == 0) {
        request->create.persist_pid = atomic_fetch_add(&shared->next_persistent_id, 1);
    }
    pid = request->create.persist_pid;

    memset(&rec, 0, sizeof(rec));
    rec.persistent_id = pid;
    memcpy(rec.create_guid, request->create.dh2q.create_guid, 16);
    memcpy(rec.client_guid, request->compound->conn->client_guid, 16);
    rec.session_id         = request->session_handle->session->session_id;
    rec.durable_flags      = CHIMERA_SMB_DURABLE_V2 | CHIMERA_SMB_DURABLE_PERSISTENT;
    rec.durable_timeout_ms = request->create.dh2q.timeout_ms == 0 ?
        CHIMERA_SMB_DURABLE_TIMEOUT_DEFAULT_MS :
        (request->create.dh2q.timeout_ms > CHIMERA_SMB_DURABLE_TIMEOUT_MAX_MS ?
         CHIMERA_SMB_DURABLE_TIMEOUT_MAX_MS : request->create.dh2q.timeout_ms);
    rec.desired_access = request->create.desired_access;
    rec.share_access   = request->create.share_access;

    name_len = request->create.name_len;
    if (name_len > SMB_FILENAME_MAX) {
        name_len = SMB_FILENAME_MAX;
    }
    rec.name_len = name_len;
    memcpy(rec.name, request->create.name, name_len);

    request->create.persist_hs.key       = request->create.persist_key;
    request->create.persist_hs.key_len   = chimera_smb_durable_key(request->create.persist_key, pid);
    request->create.persist_hs.value     = request->create.persist_value;
    request->create.persist_hs.value_len = chimera_smb_durable_serialize(
        request->create.persist_value, sizeof(request->create.persist_value), &rec);

    return request->create.persist_hs.value_len > 0;
} /* chimera_smb_create_persist_prepare */

/* Issue the open_at against the (already opened) parent handle in
 * request->create.parent_handle.  Shared by the plain path and the
 * post-overwrite-check path. */
static void
chimera_smb_create_issue_open(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread *vfs_thread = request->compound->thread->vfs_thread;
    unsigned int               flags      = 0;

    if (request->create.create_options & SMB2_FILE_DIRECTORY_FILE) {
        flags |= CHIMERA_VFS_OPEN_DIRECTORY;
    }

    /* Metadata-only open: when the caller doesn't request any data-access
     * bits, satisfy the open with an O_PATH-style handle. */
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

    if (request->create.has_stream) {
        /* For a named-stream open the disposition applies to the STREAM, not
         * the base file.  Open the base file, creating it if the disposition
         * can create, but never truncate it and never collide on it — the
         * stream's create/exclusive/truncate semantics are applied by the
         * chained chimera_vfs_open_stream. */
        switch (request->create.create_disposition) {
            case SMB2_FILE_OPEN:
            case SMB2_FILE_OVERWRITE:
                /* Base must already exist. */
                break;
            default:
                flags |= CHIMERA_VFS_OPEN_CREATE;
                break;
        } /* switch */
    } else {
        switch (request->create.create_disposition) {
            case SMB2_FILE_OPEN:
            case SMB2_FILE_OVERWRITE:
                /* Open existing only; never create. */
                break;
            case SMB2_FILE_CREATE:
                /* FILE_CREATE must fail if the file already exists; open it
                 * exclusively so the backend returns EEXIST
                 * (-> OBJECT_NAME_COLLISION) rather than opening the existing
                 * file. */
                flags |= CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE;
                break;
            case SMB2_FILE_SUPERSEDE:
            case SMB2_FILE_OPEN_IF:
            case SMB2_FILE_OVERWRITE_IF:
                flags |= CHIMERA_VFS_OPEN_CREATE;
                break;
        } /* switch */

        /* Replacing an existing file's contents truncates it; OPEN_TRUNCATE
         * makes the backend do that (a fresh create already starts empty). */
        if (chimera_smb_disposition_overwrites(request->create.create_disposition)) {
            flags |= CHIMERA_VFS_OPEN_TRUNCATE;
        }
    }

    /* A file is stamped FILE_ATTRIBUTE_ARCHIVE when it is created, and again
     * when an overwrite/supersede replaces its contents (MS-FSCC).  Both reduce
     * to "request ARCHIVE on any create-capable open": the backend applies
     * set_attr only when it actually creates the inode or truncates it on an
     * overwrite, so an existing file opened without truncation keeps its stored
     * attributes (e.g. one explicitly cleared to NORMAL).  ARCHIVE must be
     * persisted at create time, not synthesized on read (see
     * chimera_smb_marshal_basic_attrs).  OPEN_TRUNCATE covers a plain OVERWRITE
     * of an existing file (which never carries OPEN_CREATE). */
    if (flags & (CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_TRUNCATE)) {
        request->create.set_attr.va_dos_attributes |= SMB2_FILE_ATTRIBUTE_ARCHIVE;
        request->create.set_attr.va_req_mask       |= CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
        request->create.set_attr.va_set_mask       |= CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
    }

    /* Persistent-handle grants are keyed by the base file name; skip them for
     * stream opens this phase. */
    if (!request->create.has_stream &&
        chimera_smb_create_persist_prepare(request, request->create.parent_handle)) {
        chimera_vfs_open_at_hs(
            vfs_thread,
            &request->session_handle->session->cred,
            request->create.parent_handle,
            request->create.name,
            request->create.name_len,
            flags,
            &request->create.set_attr,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME |
            CHIMERA_VFS_ATTR_ACL,
            0,
            0,
            &request->create.persist_hs,
            chimera_smb_create_open_at_callback,
            request);
    } else {
        chimera_vfs_open_at(
            vfs_thread,
            &request->session_handle->session->cred,
            request->create.parent_handle,
            request->create.name,
            request->create.name_len,
            flags,
            &request->create.set_attr,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME |
            CHIMERA_VFS_ATTR_ACL,
            0,
            0,
            chimera_smb_create_open_at_callback,
            request);
    }
} /* chimera_smb_create_issue_open */

/* MS-FSA create with an overwriting disposition: before replacing an
 * existing file, reject the request when the target is READONLY, or when
 * it is HIDDEN/SYSTEM and the request does not also carry that bit (which
 * would otherwise silently clear it).  Run a getattr first so the check
 * happens before any attribute change. */
static void
chimera_smb_create_overwrite_check_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;
    uint32_t                    existing, requested;

    if (error_code == CHIMERA_VFS_ENOENT) {
        /* OVERWRITE requires an existing file; OVERWRITE_IF / SUPERSEDE
         * create it. */
        if (request->create.create_disposition == SMB2_FILE_OVERWRITE) {
            chimera_smb_create_release_parent(request);
            chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
            return;
        }
        chimera_smb_create_issue_open(request);
        return;
    }

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, chimera_smb_create_error_status(error_code));
        return;
    }

    existing = (attr->va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES)
                ? attr->va_dos_attributes : 0;
    requested = request->create.file_attributes;

    if ((existing & SMB2_FILE_ATTRIBUTE_READONLY) ||
        ((existing & SMB2_FILE_ATTRIBUTE_HIDDEN) &&
         !(requested & SMB2_FILE_ATTRIBUTE_HIDDEN)) ||
        ((existing & SMB2_FILE_ATTRIBUTE_SYSTEM) &&
         !(requested & SMB2_FILE_ATTRIBUTE_SYSTEM))) {
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    chimera_smb_create_issue_open(request);
} /* chimera_smb_create_overwrite_check_callback */

static inline void
chimera_smb_create_open_parent_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request       *request    = private_data;
    struct chimera_server_smb_thread *thread     = request->compound->thread;
    struct chimera_vfs_thread        *vfs_thread = thread->vfs_thread;

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
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME,
            0,
            0,
            chimera_smb_create_mkdir_callback,
            request);

    } else if (!request->create.has_stream &&
               chimera_smb_disposition_overwrites(request->create.create_disposition)) {
        /* Check the existing file's DOS attributes before overwriting.  A
         * stream open never truncates the base file, so this base-file check is
         * skipped for streams (the disposition applies to the stream). */
        chimera_vfs_lookup_at(
            vfs_thread,
            &request->session_handle->session->cred,
            oh,
            request->create.name,
            request->create.name_len,
            CHIMERA_VFS_ATTR_DOS_ATTRIBUTES | CHIMERA_VFS_ATTR_MASK_STAT,
            0,
            chimera_smb_create_overwrite_check_callback,
            request);
    } else {
        chimera_smb_create_issue_open(request);
    }
} /* chimera_smb_create_open_parent_callback */


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

    /* First time we hold this CA share's root handle, scan its backend for
     * persisted handle records left by a previous server instance and rebuild
     * cold registry entries so they can be reclaimed on reconnect. */
    if (request->compound->thread->shared->config.persistent_handles &&
        tree->share && tree->share->continuous_availability &&
        !tree->share->durable_recovered) {
        tree->share->durable_recovered = true;
        chimera_smb_durable_recover_share(request->compound->thread,
                                          tree->fh, tree->fh_len);
    }

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

/* Durable-handle reconnect (DH2C / DHnC).  Reclaims a parked open that
 * survived its previous connection and re-homes it into the reconnecting
 * tree, then replies as a normal OPEN of the surviving file. */
static void
chimera_smb_durable_reconnect(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_server_smb_shared *shared = thread->shared;
    struct chimera_smb_tree          *tree   = request->tree;
    struct chimera_smb_open_file     *open_file;
    const uint8_t                    *create_guid = NULL;
    uint64_t                          persistent_id;
    uint32_t                          status    = SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
    uint32_t                          ctx       = request->create.ctx_present_mask;
    const uint8_t                    *lease_key = NULL;
    bool                              has_lease_ctx;
    bool                              cold = false;
    int                               bucket;

    /* Reject malformed reconnect-context combinations:
     *   - DH2C (3.3.5.9.12) must stand alone: combining it with ANY other
     *     durable context (DHnQ, DH2Q, or DHnC) is INVALID_PARAMETER.
     *   - DHnC (3.3.5.9.7) may carry a v1 durable *request* (DHnQ) -- the request
     *     is simply ignored -- but combining it with a v2 context (DH2Q or DH2C)
     *     is INVALID_PARAMETER.
     * So a lone DHnC, a lone DH2C, or DHnC+DHnQ are all valid reconnects. */
    if ((ctx & CHIMERA_SMB_CREATE_CTX_DH2C) &&
        (ctx & (CHIMERA_SMB_CREATE_CTX_DHNQ | CHIMERA_SMB_CREATE_CTX_DH2Q |
                CHIMERA_SMB_CREATE_CTX_DHNC))) {
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }
    if ((ctx & CHIMERA_SMB_CREATE_CTX_DHNC) &&
        (ctx & (CHIMERA_SMB_CREATE_CTX_DH2Q | CHIMERA_SMB_CREATE_CTX_DH2C))) {
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    if (ctx & CHIMERA_SMB_CREATE_CTX_DH2C) {
        persistent_id = request->create.dh2c.persistent;
        create_guid   = request->create.dh2c.create_guid;
    } else {
        persistent_id = request->create.dhnc.persistent;
    }

    has_lease_ctx = (ctx & CHIMERA_SMB_CREATE_CTX_RQLS) != 0;
    if (has_lease_ctx) {
        lease_key = request->create.rqls.key;
    }

    open_file = chimera_smb_durable_claim(shared, persistent_id, create_guid,
                                          request->compound->conn->client_guid,
                                          request->create.name, request->create.name_len,
                                          has_lease_ctx, lease_key,
                                          &cold, &status);

    if (cold) {
        /* Recovered-after-restart persistent handle: there is no live open to
         * re-home.  Re-open the file from scratch via the normal create path
         * (the reconnect CREATE carries the name + access), forcing the
         * recovered persistent id so the open re-adopts its identity and the
         * backend record is refreshed atomically. */
        request->create.persist_pid = persistent_id;
        chimera_smb_create_process(request);
        return;
    }

    if (!open_file) {
        chimera_smb_complete_request(request, status);
        return;
    }

    /* Re-home the surviving open into the reconnecting tree.  The persistent
     * id is unchanged; the volatile id is reissued (spec permits this).  The
     * registry reference becomes the new tree reference; we add one more for
     * this in-flight request, which the getattr callback releases below. */
    open_file->flags      &= ~CHIMERA_SMB_OPEN_FILE_PARKED;
    open_file->create_conn = request->compound->conn;
    open_file->file_id.vid = chimera_rand64();
    open_file->refcnt      = 2;
    /* Reseed the channel-sequence baseline from the reconnecting CREATE. */
    open_file->channel_sequence       = request->channel_sequence;
    open_file->channel_sequence_valid = 1;

    bucket = open_file->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    pthread_mutex_lock(&tree->open_files_lock[bucket]);
    HASH_ADD(hh, tree->open_files[bucket], file_id, sizeof(open_file->file_id), open_file);
    pthread_mutex_unlock(&tree->open_files_lock[bucket]);

    request->create.r_open_file        = open_file;
    request->create.create_disposition = SMB2_FILE_OPEN; /* reply emits OPENED */
    request->create.reconnect          = 1; /* skip ACL re-check on the reply path */
    request->compound->saved_file_id   = open_file->file_id;

    /* Refresh the network-open-info from the surviving handle, then reply.
     * Reuses the normal create getattr callback (releases the request ref). */
    chimera_vfs_getattr(thread->vfs_thread,
                        &request->session_handle->session->cred,
                        open_file->handle,
                        CHIMERA_VFS_ATTR_MASK_STAT,
                        chimera_smb_create_open_getattr_callback,
                        request);
} /* chimera_smb_durable_reconnect */

/* Parse an SMB stream-name suffix out of the final path component
 * ("file:stream[:$DATA]").  On return *r_base_len is the length of the base
 * file name (the bytes before the first ':'); when a non-empty named stream is
 * present *r_has_stream is set and *r_stream / *r_stream_len point at the bare
 * stream name.  The unnamed default data fork ("file::$DATA") parses with
 * has_stream = 0 (open the base file).  Returns SMB2_STATUS_SUCCESS, or
 * SMB2_STATUS_OBJECT_NAME_INVALID for malformed/unsupported syntax (only the
 * $DATA stream type is supported). */
static uint32_t
chimera_smb_parse_stream_name(
    const char  *name,
    uint16_t     name_len,
    uint16_t    *r_base_len,
    const char **r_stream,
    uint16_t    *r_stream_len,
    uint8_t     *r_has_stream)
{
    const char *colon = memchr(name, ':', name_len);
    const char *rest, *type, *colon2;
    uint16_t    base_len, rest_len, stream_len;

    *r_has_stream = 0;

    if (!colon) {
        *r_base_len = name_len;
        return SMB2_STATUS_SUCCESS;
    }

    base_len = (uint16_t) (colon - name);
    rest     = colon + 1;
    rest_len = name_len - base_len - 1;

    /* Split an optional ":$TYPE" suffix off the stream name. */
    colon2 = memchr(rest, ':', rest_len);
    if (colon2) {
        stream_len = (uint16_t) (colon2 - rest);
        type       = colon2 + 1;
        uint16_t type_len = rest_len - stream_len - 1;

        /* Only the $DATA stream type is supported. */
        if (type_len != 5 || strncasecmp(type, "$DATA", 5) != 0) {
            return SMB2_STATUS_OBJECT_NAME_INVALID;
        }
    } else {
        stream_len = rest_len;   /* implied $DATA */
    }

    /* Stream names cannot contain path separators. */
    if (memchr(rest, '\\', stream_len) || memchr(rest, '/', stream_len)) {
        return SMB2_STATUS_OBJECT_NAME_INVALID;
    }

    if (stream_len == 0) {
        /* "file::$DATA" is the default data fork (open the base file); a bare
         * trailing "file:" with neither name nor type is malformed. */
        if (!colon2) {
            return SMB2_STATUS_OBJECT_NAME_INVALID;
        }
        *r_base_len = base_len;
        return SMB2_STATUS_SUCCESS;
    }

    *r_has_stream = 1;
    *r_base_len   = base_len;
    *r_stream     = rest;
    *r_stream_len = stream_len;
    return SMB2_STATUS_SUCCESS;
} /* chimera_smb_parse_stream_name */

/* Return non-zero if a '/'-separated path contains a ".." component.  After
 * parsing, the CREATE parent path is '/'-separated (the wire backslashes were
 * converted), so this scans for an exact ".." between separators. */
static inline int
chimera_smb_path_has_dotdot(
    const char *path,
    int         len)
{
    const char *p   = path;
    const char *end = path + len;

    while (p < end) {
        const char *comp = p;

        while (p < end && *p != '/') {
            p++;
        }
        if (p - comp == 2 && comp[0] == '.' && comp[1] == '.') {
            return 1;
        }
        if (p < end) {
            p++;
        }
    }
    return 0;
} /* chimera_smb_path_has_dotdot */

/* Return non-zero if a '/'-separated path's first component is exactly "..".
 * MS-SMB2 3.3.5.9 distinguishes a name that *begins* with ".." (rejected with
 * STATUS_OBJECT_PATH_SYNTAX_BAD, e.g. smbtorture smb2.mkdir "..\..\..") from a
 * "subfolder\..\" form (rejected with STATUS_INVALID_PARAMETER, e.g. WPTS
 * *DoubleDotDir "x\..\y.txt"). */
static inline int
chimera_smb_path_begins_with_dotdot(
    const char *path,
    int         len)
{
    return (len == 2 && path[0] == '.' && path[1] == '.') ||
           (len >= 3 && path[0] == '.' && path[1] == '.' && path[2] == '/');
} /* chimera_smb_path_begins_with_dotdot */

/*
 * MS-SMB2 §3.3.5.9.7/.10: a CREATE that carries a DurableHandleRequestV2 (DH2Q)
 * create_guid matching a still-live open from the same client is a *replay* of
 * the original create -- the server returns the existing open instead of opening
 * a second handle (or returning a sharing violation).  This is distinct from a
 * DH2C/DHnC reconnect (which targets a *parked* handle by persistent id).
 *
 * Returns 1 if the request was handled here (replay matched, or rejected with
 * ACCESS_DENIED on a type/key mismatch) -- the caller must return.  Returns 0 if
 * no live open carries this create_guid, so the caller proceeds with a fresh
 * create.
 */
static int
chimera_smb_create_guid_replay(struct chimera_smb_request *request)
{
    struct chimera_smb_tree          *tree = request->tree;
    struct chimera_smb_open_file     *of, *tmp, *match = NULL;
    struct chimera_server_smb_thread *thread = request->compound->thread;
    int                               b;
    bool                              req_is_lease, open_is_lease;

    for (b = 0; b < CHIMERA_SMB_OPEN_FILE_BUCKETS && !match; b++) {
        pthread_mutex_lock(&tree->open_files_lock[b]);
        HASH_ITER(hh, tree->open_files[b], of, tmp)
        {
            if ((of->ctx_present_mask & CHIMERA_SMB_CREATE_CTX_DH2Q) &&
                !(of->flags & (CHIMERA_SMB_OPEN_FILE_CLOSED |
                               CHIMERA_SMB_OPEN_FILE_PARKED)) &&
                memcmp(of->create_guid, request->create.dh2q.create_guid, 16) == 0) {
                of->refcnt++;   /* held for this request; getattr cb releases it */
                match = of;
                break;
            }
        }
        pthread_mutex_unlock(&tree->open_files_lock[b]);
    }

    if (!match) {
        return 0;
    }

    /* A replay that asks for a different handle type (oplock vs lease), or a
     * lease with a different key than the live open holds, is rejected with
     * ACCESS_DENIED (MS-SMB2 dhv2 oplock_lease / lease_oplock / lease3). */
    req_is_lease  = (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) != 0;
    open_is_lease = (match->oplock_level == SMB2_OPLOCK_LEVEL_LEASE);

    if (req_is_lease != open_is_lease ||
        (req_is_lease &&
         memcmp(match->lease_key, request->create.rqls.key, 16) != 0)) {
        chimera_smb_open_file_release(request, match);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return 1;
    }

    /* A coalesced lease may have been upgraded by a sibling open since this
     * handle was first granted; report the grant's CURRENT granted mode rather
     * than this open's possibly-stale cached copy (MS-SMB2 replay-dhv2-lease1/2,
     * which upgrade RH->RWH then replay the original RH create). */
    if (open_is_lease && match->grant) {
        match->lease_state =
            chimera_smb_vfs_to_lease_bits(match->grant->lease.mode.granted);
    }

    /* Replay: return the existing open via the reconnect reply path.  reconnect=1
     * skips the ACL re-check; replay=1 makes the reply echo the original
     * create_action and (for an oplock handle) the requested oplock level.  The
     * create_disposition is left as the request's own so it is not consulted. */
    request->create.r_open_file      = match;
    request->create.reconnect        = 1;
    request->create.replay           = 1;
    request->compound->saved_file_id = match->file_id;

    chimera_vfs_getattr(thread->vfs_thread,
                        &request->session_handle->session->cred,
                        match->handle,
                        CHIMERA_VFS_ATTR_MASK_STAT,
                        chimera_smb_create_open_getattr_callback,
                        request);
    return 1;
} /* chimera_smb_create_guid_replay */

void
chimera_smb_create(struct chimera_smb_request *request)
{
    struct timespec               now;
    struct chimera_smb_open_file *open_file;
    enum chimera_smb_pipe_magic   pipe_magic;
    chimera_smb_pipe_transceive_t transceive;

    request->create.has_stream = 0;
    request->create.base_oh    = NULL;
    /* Only the regular-file open path (open_at_callback) registers a finish
     * callback and may park on a batch-oplock break; clear it so the mkdir /
     * stream / pipe paths never inherit a stale callback and park. */
    request->create.gen_finish_cb = NULL;
    request->create.gen_parked    = 0;

    /* Handle stream-name syntax (file:stream[:$DATA]).  When named streams are
     * disabled (config off, or the backend lacks the capability) the legacy
     * behavior is preserved: reject any ':' name with OBJECT_NAME_INVALID.
     * Done here rather than in parse so the response is returned to the client
     * instead of triggering a parse-error disconnect. */
    if (request->create.name_len > 0 &&
        memchr(request->create.name, ':', request->create.name_len)) {

        if (!request->compound->thread->shared->config.named_streams) {
            chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_INVALID);
            return;
        }

        const char *sname      = NULL;
        uint16_t    base_len   = request->create.name_len;
        uint16_t    sname_len  = 0;
        uint8_t     has_stream = 0;
        uint32_t    pstatus    = chimera_smb_parse_stream_name(
            request->create.name, request->create.name_len,
            &base_len, &sname, &sname_len, &has_stream);

        if (pstatus != SMB2_STATUS_SUCCESS) {
            chimera_smb_complete_request(request, pstatus);
            return;
        }

        /* Trim the final component to the base file; the stream (if any) is
         * opened after the base file via chimera_vfs_open_stream. */
        request->create.name_len       = base_len;
        request->create.name[base_len] = '\0';

        if (has_stream) {
            request->create.has_stream      = 1;
            request->create.stream_name_len = sname_len;
            memcpy(request->create.stream_name, sname, sname_len);
        }
    }

    /* A durable-handle reconnect (DH2C/DHnC) reclaims an already-open handle and
     * ignores the create fields entirely -- CreateDisposition, file name, access
     * and the rest are not interpreted (MS-SMB2 3.3.5.9.7/.12) -- so the field
     * validations below must not run for it; it is dispatched straight to
     * chimera_smb_durable_reconnect. */
    bool is_durable_reconnect =
        request->compound->thread->shared->config.persistent_handles &&
        (request->create.ctx_present_mask &
         (CHIMERA_SMB_CREATE_CTX_DH2C | CHIMERA_SMB_CREATE_CTX_DHNC)) != 0;

    /* Reject create dispositions outside the defined range
     * (SUPERSEDE..OVERWRITE_IF).  MS-SMB2 returns STATUS_INVALID_PARAMETER for
     * an undefined CreateDisposition. */
    if (!is_durable_reconnect &&
        request->create.create_disposition > SMB2_FILE_OVERWRITE_IF) {
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* Reject any ".." path component.  Windows clients canonicalize these away
     * before sending, so the server never needs to resolve one; letting it
     * reach the VFS would walk above the share root (a traversal escape on the
     * passthrough backends).  The status depends on the form (MS-SMB2 3.3.5.9):
     * a name that *begins* with ".." (e.g. "..\..\..") gets
     * STATUS_OBJECT_PATH_SYNTAX_BAD (smbtorture smb2.mkdir), while a
     * "subfolder\..\" form (e.g. "x\..\y.txt") gets STATUS_INVALID_PARAMETER
     * (WPTS *DoubleDotDir).  Completed here (not in parse) so the client gets a
     * response rather than a connection drop. */
    if (!is_durable_reconnect &&
        ((request->create.name_len == 2 &&
          request->create.name[0] == '.' && request->create.name[1] == '.') ||
         chimera_smb_path_has_dotdot(request->create.parent_path,
                                     request->create.parent_path_len))) {
        int begins_dotdot =
            chimera_smb_path_begins_with_dotdot(request->create.parent_path,
                                                request->create.parent_path_len) ||
            (request->create.parent_path_len == 0 &&
             request->create.name_len == 2 &&
             request->create.name[0] == '.' && request->create.name[1] == '.');

        chimera_smb_complete_request(request,
                                     begins_dotdot ?
                                     SMB2_STATUS_OBJECT_PATH_SYNTAX_BAD :
                                     SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* No persistent-handle grant by default; set by the reconnect path (cold
     * reclaim) or chimera_smb_create_persist_prepare for a fresh grant. */
    request->create.persist_pid = 0;
    request->create.reconnect   = 0;
    request->create.replay      = 0;
    /* Default share-conflict status; an AppInstanceId version reject overrides
     * it with STATUS_FILE_FORCED_CLOSED inside gen_open_file. */
    request->create.force_close_status = SMB2_STATUS_SHARING_VIOLATION;

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

        /* Durable-handle reconnect short-circuits the normal open path: the
         * file is already open (parked); we just reclaim and re-home it.  It
         * ignores all create fields, so it runs before the DOC access check. */
        if (is_durable_reconnect) {
            chimera_smb_durable_reconnect(request);
            return;
        }

        /* MS-SMB2 3.3.5.9.7: a DH2Q create whose create_guid matches a live
         * open is a replay of the original create -- return the existing handle
         * rather than opening a second one.  Runs before the field validations
         * below since a replay re-presents the original (already-validated)
         * request. */
        if ((request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_DH2Q) &&
            chimera_smb_create_guid_replay(request)) {
            return;
        }

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
    bool                          v2;
    int                           needed;

    if (!of) {
        return -1;
    }

    /* The response lease version follows the LEASE's version (set when the lease
     * was first granted), not this request's: a v1 open that coalesces onto an
     * existing v2 lease still gets a v2 response, and vice versa (MS-SMB2
     * 3.3.5.9.11; smb2.lease.v2_epoch2/3).  The grant records is_v2; fall back to
     * the request only when this open holds no grant (a bare LEASE_NONE). */
    v2 = of->grant ? of->grant->is_v2 : (request->create.rqls.is_v2 != 0);

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
    /* LeaseFlags (4) — BREAK_IN_PROGRESS when this open joined a lease whose
     * break is still outstanding; otherwise 0. */
    out[20] = of->lease_flags & 0xff;
    out[21] = (of->lease_flags >> 8) & 0xff;
    out[22] = (of->lease_flags >> 16) & 0xff;
    out[23] = (of->lease_flags >> 24) & 0xff;
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

/* Build the DH2Q (durable-handle-request-v2) response body — 8 bytes:
 * Timeout(4) | Flags(4).  Emitted only when a durable-v2 grant was made;
 * Flags echoes PERSISTENT when the grant is persistent. */
static int
build_dh2q_response(
    struct chimera_smb_request *request,
    uint8_t                    *out,
    uint32_t                    out_size)
{
    struct chimera_smb_open_file *of = request->create.r_open_file;
    uint32_t                      timeout, flags;

    if (!of || !(of->durable_flags & CHIMERA_SMB_DURABLE_V2) || out_size < 8) {
        return -1;
    }

    /* On an oplock (non-lease) create_guid replay, a durable response is emitted
     * only if the REQUESTED oplock could itself have been granted a durable
     * handle (batch); a replay asking for a lesser oplock gets no durable
     * response at all (MS-SMB2 replay-dhv2-oplock2 expects durable_open_v2=false,
     * timeout=0, no blobs). */
    if (request->create.replay &&
        of->oplock_level != SMB2_OPLOCK_LEVEL_LEASE &&
        request->create.requested_oplock_level != SMB2_OPLOCK_LEVEL_BATCH) {
        return -1;
    }

    timeout = (uint32_t) of->durable_timeout_ms;
    flags   = (of->durable_flags & CHIMERA_SMB_DURABLE_PERSISTENT) ?
        SMB2_DHANDLE_FLAG_PERSISTENT : 0;

    out[0] = timeout & 0xff;
    out[1] = (timeout >> 8) & 0xff;
    out[2] = (timeout >> 16) & 0xff;
    out[3] = (timeout >> 24) & 0xff;
    out[4] = flags & 0xff;
    out[5] = (flags >> 8) & 0xff;
    out[6] = (flags >> 16) & 0xff;
    out[7] = (flags >> 24) & 0xff;
    return 8;
} /* build_dh2q_response */

/* Build the DHnQ (durable-handle-request v1) response body — 8 reserved
 * (zero) bytes.  Emitted only when a durable-v1 grant was made. */
static int
build_dhnq_response(
    struct chimera_smb_request *request,
    uint8_t                    *out,
    uint32_t                    out_size)
{
    struct chimera_smb_open_file *of = request->create.r_open_file;

    if (!of || !(of->durable_flags & CHIMERA_SMB_DURABLE_V1) || out_size < 8) {
        return -1;
    }

    memset(out, 0, 8);
    return 8;
} /* build_dhnq_response */

/* *INDENT-OFF* */ /* uncrustify oscillates on aligned struct-init tables */
static const struct chimera_smb_create_response_emitter smb_create_response_emitters[] = {
    { CHIMERA_SMB_CREATE_CTX_MXAC, "MxAc", build_mxac_response  },
    { CHIMERA_SMB_CREATE_CTX_RQLS, "RqLs", build_rqls_response  },
    { CHIMERA_SMB_CREATE_CTX_DH2Q, "DH2Q", build_dh2q_response  },
    { CHIMERA_SMB_CREATE_CTX_DHNQ, "DHnQ", build_dhnq_response  },
    /* Phase 8: + QFid (32-byte on-disk id) */
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
     * by the CACHING acquire above; defaults to NONE if no lease.  On a DH2Q
     * create_guid replay of an oplock (non-lease) handle the response echoes the
     * REQUESTED oplock level, not the granted one (MS-SMB2 replay-dhv2-oplock2);
     * a lease handle always reports its current (shared, possibly upgraded)
     * state, so it is exempt. */
    uint8_t reply_oplock_level = SMB2_OPLOCK_LEVEL_NONE;
    if (request->create.r_open_file) {
        if (request->create.replay &&
            request->create.r_open_file->oplock_level != SMB2_OPLOCK_LEVEL_LEASE) {
            reply_oplock_level = request->create.requested_oplock_level;
        } else {
            reply_oplock_level = request->create.r_open_file->oplock_level;
        }
    }
    evpl_iovec_cursor_append_uint8(reply_cursor, reply_oplock_level);

    /* Flags */
    evpl_iovec_cursor_append_uint8(reply_cursor, 0);

    /* Create Action: distinguish OPENED / CREATED / OVERWRITTEN / SUPERSEDED.
     * For the "*_IF" / SUPERSEDE dispositions the outcome depends on whether the
     * file already existed — the VFS open reports that via handle->r_created. */
    uint32_t create_action;
    bool     created = request->create.r_created;

    if (request->create.replay && request->create.r_open_file) {
        /* A replay re-presents the original create; report the action recorded
         * when the handle was first opened (MS-SMB2 replay-dhv2-*). */
        create_action = request->create.r_open_file->create_action;
        goto have_create_action;
    }

    switch (request->create.create_disposition) {
        case SMB2_FILE_CREATE:
            create_action = SMB2_CREATE_ACTION_CREATED;
            break;
        case SMB2_FILE_OVERWRITE:
            create_action = SMB2_CREATE_ACTION_OVERWRITTEN;
            break;
        case SMB2_FILE_OVERWRITE_IF:
            create_action = created ? SMB2_CREATE_ACTION_CREATED
                                    : SMB2_CREATE_ACTION_OVERWRITTEN;
            break;
        case SMB2_FILE_SUPERSEDE:
            create_action = created ? SMB2_CREATE_ACTION_CREATED
                                    : SMB2_CREATE_ACTION_SUPERSEDED;
            break;
        case SMB2_FILE_OPEN_IF:
            create_action = created ? SMB2_CREATE_ACTION_CREATED
                                    : SMB2_CREATE_ACTION_OPENED;
            break;
        case SMB2_FILE_OPEN:
        default:
            create_action = SMB2_CREATE_ACTION_OPENED;
            break;
    } /* switch */

    /* Record the action on a fresh open so a later DH2Q create_guid replay can
     * report it verbatim (a replay branches above and does not re-record). */
    if (request->create.r_open_file) {
        request->create.r_open_file->create_action = create_action;
    }

 have_create_action:

    evpl_iovec_cursor_append_uint32(reply_cursor, create_action);

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
    /* The create-context parser is also exercised standalone by
     * phase0_contexts_test with a zeroed request (no compound/thread/shared),
     * so fall back to canonical behaviour when the server context is not
     * reachable. */
    int canonicalize = 1;

    if (request->compound) {
        canonicalize = request->compound->thread->shared->config.
            acl_inherited_canonicalize;
    }

    chimera_smb_parse_sd_to_acl(data, data_len, &request->create.set_attr,
                                request->create.acl_storage,
                                sizeof(request->create.acl_storage),
                                canonicalize);
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
        /* The ParentLeaseKey is meaningful only when the client set
         * SMB2_LEASE_FLAG_PARENT_LEASE_KEY_SET; otherwise the bytes are reserved
         * and must be ignored (and echoed back as zero -- smb2.lease.
         * v2_flags_parentkey). */
        if (request->create.rqls.flags & SMB2_LEASE_FLAG_PARENT_LEASE_KEY_SET) {
            memcpy(request->create.rqls.parent_key, data + 32, 16);
        } else {
            memset(request->create.rqls.parent_key, 0, 16);
        }
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

/* GUID-named CREATE contexts (MS-SMB2 2.2.13.2.13 / 2.2.13.2.14).  The context
 * "name" is a 16-byte GUID identifier.  The on-wire identifier bytes used by
 * the WPTS MS-SMB2 client were confirmed empirically from a live capture (the
 * mixed-endian GUID quoted in the spec text differs from what is on the wire). */
/* SMB2_CREATE_APP_INSTANCE_ID */
static const uint8_t smb_app_instance_id_guid[16] = {
    0x45, 0xBC, 0xA6, 0x6A, 0xEF, 0xA7, 0xF7, 0x4A,
    0x90, 0x08, 0xFA, 0x46, 0x2E, 0x14, 0x4D, 0x74
};
/* SMB2_CREATE_APP_INSTANCE_VERSION */
static const uint8_t smb_app_instance_version_guid[16] = {
    0xB9, 0x82, 0xD0, 0xB7, 0x3B, 0x56, 0x07, 0x4F,
    0xA0, 0x7B, 0x52, 0x4A, 0x81, 0x16, 0xA0, 0x10
};

static void
parse_ctx_app_instance_id(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request)
{
    /* Body (confirmed on the wire): StructureSize(4) | AppInstanceId(16). */
    if (data_len < 20) {
        return;
    }
    memcpy(request->create.app_instance_id, data + 4, 16);
    request->create.ctx_present_mask |= CHIMERA_SMB_CREATE_CTX_APP;
} /* parse_ctx_app_instance_id */

static void
parse_ctx_app_instance_version(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request)
{
    /* Body (confirmed on the wire): StructureSize(4) | Reserved(4) |
     * AppInstanceVersionHigh(8) | AppInstanceVersionLow(8). */
    if (data_len < 24) {
        return;
    }
    request->create.app_version_high    = smb_wire_le64(data + 8);
    request->create.app_version_low     = smb_wire_le64(data + 16);
    request->create.app_version_present = 1;
    request->create.ctx_present_mask   |= CHIMERA_SMB_CREATE_CTX_APP_VERSION;
} /* parse_ctx_app_instance_version */

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

                    /* Always set the base mask bit; RqLs v2 (52-byte body)
                     * additionally sets RQLS_V2 so the response emit can tell
                     * which lease variant the client requested.  (The base RQLS
                     * bit must be set for v2 too — gen_open_file keys lease
                     * handling off it.) */
                    request->create.ctx_present_mask |= p->mask_bit;
                    if (p->mask_bit == CHIMERA_SMB_CREATE_CTX_RQLS && data_len == 52) {
                        request->create.ctx_present_mask |= CHIMERA_SMB_CREATE_CTX_RQLS_V2;
                    }
                    if (p->handler) {
                        p->handler(data, data_len, request);
                    }
                    break;
                }
            }
        } else if (name_len == 16) {
            /* GUID-named contexts (AppInstanceId / AppInstanceVersion). */
            if (memcmp(name, smb_app_instance_id_guid, 16) == 0) {
                parse_ctx_app_instance_id(data, data_len, request);
            } else if (memcmp(name, smb_app_instance_version_guid, 16) == 0) {
                parse_ctx_app_instance_version(data, data_len, request);
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
                          request->request_struct_size,
                          SMB2_CREATE_REQUEST_SIZE);
        /* MS-SMB2 3.3.5.2.2 / 2.2.13: an invalid CREATE StructureSize is a
         * per-request failure answered with STATUS_INVALID_PARAMETER, NOT a
         * connection-fatal parse error.  Returning -1 here would make the
         * dispatcher tear down the transport (and the in-flight reply would
         * race the close, so the client just times out — smb2.create
         * InvalidCreateRequestStructureSize).  Defer the status instead so the
         * compound completes it in order and the connection survives. */
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        request->flags |= CHIMERA_SMB_REQUEST_FLAG_PARSE_FAILED;
        return 0;
    }

    /* SMB2 CREATE fixed body (after the already-consumed 2-byte StructureSize):
     * SecurityFlags(1) | RequestedOplockLevel(1) | ImpersonationLevel(4) | ...
     * SecurityFlags is reserved (clients send 0) but MUST be consumed; the
     * aligning cursor would otherwise swallow RequestedOplockLevel as padding
     * before the uint32 ImpersonationLevel read, leaving oplock level always 0. */
    uint8_t security_flags;
    evpl_iovec_cursor_get_uint8(request_cursor, &security_flags);
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

    /* ImpersonationLevel is advisory and Windows servers do not validate it
     * (a durable reconnect, for one, fills it with a don't-care sentinel) — so
     * accept any value rather than returning STATUS_BAD_IMPERSONATION_LEVEL. */

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

    /* Initialize create-time attributes (may be populated by SD create context).
     * The request slot is pooled and its set_attr.va_acl pointer survives from
     * a prior CREATE; clear it explicitly so the backend's create-time ACL
     * precedence check (which keys off the va_acl pointer, since
     * <backend>_apply_attrs strips the va_set_mask ACL bit before
     * <backend>_inherit_acl runs) doesn't fire on a stale pointer. */
    request->create.set_attr.va_req_mask = 0;
    request->create.set_attr.va_set_mask = 0;
    request->create.set_attr.va_acl      = NULL;
    request->create.ctx_present_mask     = 0;

    /* The request slot is pooled, so the AppInstanceId/AppInstanceVersion
     * fields survive from a prior CREATE on this same slot.  Each create
     * context is parsed only when present on the wire, so a CREATE that
     * carries an AppInstanceId but no AppInstanceVersion (or no AppInstanceId
     * at all) would otherwise inherit the stale version/GUID and apply the
     * wrong AppInstanceVersion force-close rule.  This is invisible when each
     * test runs against a fresh daemon but corrupts the decision in a
     * sequential run.  Reset them explicitly before parsing the contexts. */
    request->create.app_version_present = 0;
    request->create.app_version_high    = 0;
    request->create.app_version_low     = 0;
    memset(request->create.app_instance_id, 0,
           sizeof(request->create.app_instance_id));

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
