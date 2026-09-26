// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/time.h>
#include <strings.h>
#include "smb_common/smb2.h"
#include "server/smb/smb_session.h"
#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_async_interim.h"
#include "smb_string.h"
#include "smb_ea.h"
#include "smb_lease_key.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/sdk/vfs_access.h"
#include "vfs/vfs_notify.h"
#include "vfs/vfs_release.h"
#include "smb_attr.h"
#include "smb_lsarpc.h"
#include "smb_srvsvc.h"
#include "smb_samr.h"
#include "smb_wkssvc.h"

static void
smb_create_notify_lease(struct chimera_smb_request *request,
    const uint8_t *dir_fh, uint16_t dir_fh_len, uint32_t action,
    const char *name, uint16_t name_len, const char *old_name, uint16_t old_name_len,
    uint64_t skip_lo, uint64_t skip_hi, bool has_skip)
{
    struct chimera_claim_actor actor = { .owner = {
        .proto = CHIMERA_CLAIM_PROTO_SMB2,
        .client_key = request->session_handle->session->client_key,
    } };
    memcpy(actor.owner.key, &skip_lo, 8);
    memcpy(actor.owner.key + 8, &skip_hi, 8);
    chimera_vfs_notify_emit_actor(request->compound->thread->shared->vfs->vfs_notify,
        dir_fh, dir_fh_len, action, name, name_len, old_name, old_name_len,
        has_skip ? &actor : NULL);
}

static const uint8_t *
smb_create_ea_data(const struct chimera_smb_request *request)
{
    return request->create.ea_overflow ? request->create.ea_overflow : request->create.ea_buf;
}

/* Keep parsed DesiredAccess immutable for retries and discovered redispatch.
 * Generic/MAXIMUM_ALLOWED expansion is filtered through the same rule below. */
static uint32_t
smb_create_effective_access(const struct chimera_smb_request *request, uint32_t access)
{
    return request->create.create_options & SMB2_FILE_NO_INTERMEDIATE_BUFFERING ?
        access & ~SMB2_FILE_APPEND_DATA : access;
}

/* A reserved, already-bound key can only open an existing identity. Name
 * preflight is advisory: another frontend may unlink that name immediately
 * afterward, so no following backend operation may create a replacement. */
static bool
smb_create_bound_missing(struct chimera_smb_request *request, enum chimera_vfs_error status)
{
    return status == CHIMERA_VFS_ENOENT &&
        request->create.create_disposition != SMB2_FILE_OPEN &&
        request->create.create_disposition != SMB2_FILE_OVERWRITE &&
        chimera_smb_lease_key_conflict(request, NULL, 0);
}

static uint32_t
smb_create_desired_access(const struct chimera_smb_request *request)
{
    return smb_create_effective_access(request, request->create.desired_access);
}

/* Final step of a successful create: apply any SMB2_CREATE_EA_BUFFER ("ExtA")
 * EAs to the new/opened object, then release the open and reply.  When no ExtA
 * context was supplied (ea_buf_len == 0) this is exactly the prior behaviour
 * (release + SUCCESS), so it is safe to route every immediate create-success
 * completion through it. */
static void
chimera_smb_create_ea_done(
    uint32_t status,
    void    *arg)
{
    struct chimera_smb_request *request = arg;
    struct chimera_smb_open_file *open_file = request->create.r_open_file;

    if (status != SMB2_STATUS_SUCCESS) {
        chimera_smb_create_failed_open(request, status);
        return;
    }
    chimera_smb_open_file_release(request, open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_create_ea_done */

static inline uint32_t chimera_smb_create_error_status(
    enum chimera_vfs_error error_code);

static void chimera_smb_create_finish_with_eas(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file);

static void
chimera_smb_create_overwrite_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request   *request   = private_data;
    struct chimera_smb_open_file *open_file = request->create.r_open_file;

    (void) pre_attr;
    (void) set_attr;
    if (request->create.overwrite_share_held) {
        request->create.overwrite_share_held = 0;
        chimera_vfs_claim_shrink(open_file->share_file_state,
                                 chimera_smb_share_claim(open_file),
                                 request->create.gen_held_granted,
                                 request->create.gen_held_denied);
    }
    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_create_failed_open(request, chimera_smb_create_error_status(error_code));
        return;
    }
    chimera_smb_marshal_open_attrs(post_attr,
        open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM, &request->create.r_attrs);
    if (!request->create.r_is_directory &&
        request->create.r_attrs.smb_alloc_size < request->create.alsi_alloc_size) {
        request->create.r_attrs.smb_alloc_size = request->create.alsi_alloc_size;
    }
    chimera_smb_create_finish_with_eas(request, open_file);
} /* chimera_smb_create_overwrite_complete */

static void
chimera_smb_create_finish_with_eas(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file)
{
    if (request->create.overwrite_pending) {
        struct chimera_claim_actor actor = {
            .owner     = chimera_smb_share_claim(open_file)->owner,
            .op_handle = open_file->handle,
        };
        request->create.overwrite_pending           = 0;
        request->create.r_open_file                 = open_file;
        request->create.overwrite_attr.va_size      = 0;
        request->create.overwrite_attr.va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
        request->create.overwrite_attr.va_req_mask |= CHIMERA_VFS_ATTR_SIZE;
        chimera_vfs_overwrite(request->compound->thread->vfs_thread,
                              &request->session_handle->session->cred,
                              open_file->handle, &request->create.overwrite_attr,
                              CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME, &actor,
                              chimera_smb_create_overwrite_complete, request);
        return;
    }
    if (request->create.ea_buf_len == 0) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    request->create.r_open_file = open_file;
    chimera_smb_ea_apply(request->compound->thread,
                         &request->session_handle->session->cred,
                         open_file->handle,
                         smb_create_ea_data(request),
                         request->create.ea_buf_len,
                         chimera_smb_create_ea_done, request);
} /* chimera_smb_create_finish_with_eas */

#define SMB2_WRITE_MASK                            (SMB2_FILE_WRITE_DATA | \
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
#define SMB2_DATA_ACCESS_MASK                      (SMB2_FILE_READ_DATA | \
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

/* Re-getattr budget for an ACCESS_DENIED that looks like the transient
 * server-owned state of an object a concurrent CREATE is still constructing
 * (see chimera_smb_create_open_at_callback).  Each retry is a full backend
 * round trip, which dwarfs the racing creator's create-to-chown window.
 * 32 retries to accommodate slow arm64 Debug / io_uring builds where the
 * ASan + async overhead can extend the racing creator's chown window well
 * past 8 round trips. */
#define CHIMERA_SMB_CREATE_ACCESS_RETRIES          32

/* Retry budget + interval for a share conflict against a durable holder whose
 * owning connection is mid-disconnect (the holder will park + yield shortly).
 * The window is just the time for the peer connection's disconnect callback to
 * be scheduled and park the handle, so a short interval with a bounded budget
 * (~3 s, matching the durable-reconnect retry) covers a loaded CI runner without
 * delaying a genuinely-live conflict.  The budget depends on how the conflict was
 * classified (enum chimera_smb_durable_yield): a CONFIRMED disconnect (or an
 * already-parked holder) gets the full ~3 s; a SPECULATIVE conflict (a durable
 * holder whose FIN may not have been read yet, so the disconnect is not yet
 * observable) gets only a short budget sufficient to cover the FIN-read latency --
 * once the FIN is read a re-probe upgrades it to CONFIRMED and the full budget
 * applies, while a genuinely-live durable holder lapses the short budget into the
 * same immediate SHARING_VIOLATION. */
#define CHIMERA_SMB_CREATE_SHARE_RETRY_INTERVAL_US 20000  /* 20 ms */
#define CHIMERA_SMB_CREATE_SHARE_RETRY_MAX         150    /* ~3 s total */
#define CHIMERA_SMB_CREATE_SHARE_SPEC_RETRY_MAX    25     /* ~0.5 s total */

/* ---------------------------------------------------------------------------
 * In-flight (deferred, not-yet-hashed) DH2Q creates -- tree->pending_creates.
 *
 * A create is hashed into tree->open_files[] only once its share reservation is
 * granted (chimera_smb_create_after_share), so a create that is deferred BEFORE
 * that -- parked on a conflicting holder's handle-caching break, or retrying a
 * disconnecting durable holder -- carries a create_guid no lookup can see.  A
 * replay of such a create would fall through to a fresh open and hit the very
 * conflict the original is waiting out, answering SHARING_VIOLATION where the
 * pending-open rule requires FILE_NOT_AVAILABLE (smb2.replay.dhv2-pending1n-vs-
 * violation-lease-{ack,close}-sane).  These three helpers make that window
 * visible to chimera_smb_create_guid_replay.  Entries are stored in the deferred
 * requests themselves, so an entry cannot outlive its request.
 * --------------------------------------------------------------------------- */

static void
chimera_smb_create_pending_register(struct chimera_smb_request *request)
{
    struct chimera_smb_tree *tree = request->tree;

    if (request->create.pending_linked || !tree ||
        !(request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_DH2Q)) {
        return;
    }

    memcpy(request->create.pending_client_guid,
           request->compound->conn->client_guid, 16);

    pthread_mutex_lock(&tree->pending_creates_lock);
    request->create.pending_link   = tree->pending_creates;
    tree->pending_creates          = request;
    request->create.pending_linked = 1;
    pthread_mutex_unlock(&tree->pending_creates_lock);
} /* chimera_smb_create_pending_register */

void
chimera_smb_create_pending_unregister(struct chimera_smb_request *request)
{
    struct chimera_smb_tree     *tree = request->tree;
    struct chimera_smb_request **pp;

    if (!request->create.pending_linked || !tree) {
        return;
    }

    pthread_mutex_lock(&tree->pending_creates_lock);
    for (pp = &tree->pending_creates; *pp; pp = &(*pp)->create.pending_link) {
        if (*pp == request) {
            *pp = request->create.pending_link;
            break;
        }
    }
    request->create.pending_link   = NULL;
    request->create.pending_linked = 0;
    pthread_mutex_unlock(&tree->pending_creates_lock);
} /* chimera_smb_create_pending_unregister */

/* Non-zero if a create carrying `create_guid` from `client_guid` is deferred on
 * this tree right now (MS-SMB2 3.3.5.9.10 matches Open.CreateGuid together with
 * Open.ClientGuid).  Scoped to one tree connect, like the hashed open scan. */
static int
chimera_smb_create_pending_match(
    struct chimera_smb_tree *tree,
    const uint8_t           *create_guid,
    const uint8_t           *client_guid)
{
    struct chimera_smb_request *req;
    int                         found = 0;

    pthread_mutex_lock(&tree->pending_creates_lock);
    for (req = tree->pending_creates; req; req = req->create.pending_link) {
        if (memcmp(req->create.dh2q.create_guid, create_guid, 16) == 0 &&
            memcmp(req->create.pending_client_guid, client_guid, 16) == 0) {
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&tree->pending_creates_lock);
    return found;
} /* chimera_smb_create_pending_match */

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
        case CHIMERA_VFS_EIO:          return SMB2_STATUS_IO_DEVICE_ERROR;
        case CHIMERA_VFS_EISDIR:       return SMB2_STATUS_FILE_IS_A_DIRECTORY;
        case CHIMERA_VFS_ENOTDIR:      return SMB2_STATUS_NOT_A_DIRECTORY;
        case CHIMERA_VFS_EEXIST:       return SMB2_STATUS_OBJECT_NAME_COLLISION;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:        return SMB2_STATUS_ACCESS_DENIED;
        case CHIMERA_VFS_ENOSPC:
        case CHIMERA_VFS_EDQUOT:       return SMB2_STATUS_DISK_FULL;
        case CHIMERA_VFS_ENAMETOOLONG: return SMB2_STATUS_NAME_TOO_LONG;
        case CHIMERA_VFS_EROFS:        return SMB2_STATUS_MEDIA_WRITE_PROTECTED;
        case CHIMERA_VFS_ELOOP:        return SMB2_STATUS_STOPPED_ON_SYMLINK;
        default:                       return SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
    } /* switch */
} /* chimera_smb_create_error_status */

/* As above, but for a failure resolving the *parent* path of a create.  A
 * missing component is an OBJECT_PATH_NOT_FOUND (an intermediate path element),
 * not OBJECT_NAME_NOT_FOUND (the final name); ENOTDIR/EACCES/ELOOP still map to
 * their specific statuses so, e.g., mkdir under a non-directory reports
 * NOT_A_DIRECTORY (ENOTDIR) rather than a bare ENOENT. */
static inline uint32_t
chimera_smb_create_parent_error_status(enum chimera_vfs_error error_code)
{
    uint32_t status = chimera_smb_create_error_status(error_code);

    if (status == SMB2_STATUS_OBJECT_NAME_NOT_FOUND) {
        status = SMB2_STATUS_OBJECT_PATH_NOT_FOUND;
    }
    return status;
} /* chimera_smb_create_parent_error_status */

/* Emit the SMB2 ERROR Response carrying a SymbolicLinkErrorResponse body
 * (MS-SMB2 2.2.2 / 2.2.2.2.1) for a create that STATUS_STOPPED_ON_SYMLINK'd.
 * The link target (UTF-8, already '\'-separated) and its relative flag were
 * stashed on the request by the create's readlink step.  Called only when
 * request->create.r_symlink_error is set; a UTF-16 conversion failure degrades
 * to the bare 9-byte error body so the client still sees STOPPED_ON_SYMLINK. */
void
chimera_smb_create_emit_symlink_error(
    struct evpl_iovec_cursor   *cursor,
    struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    uint16_t                          utf16[CHIMERA_VFS_PATH_MAX];
    int                               name_len;
    uint16_t                          reparse_data_length;
    uint32_t                          sym_link_length, byte_count, flags;

    name_len = chimera_smb_utf8_to_utf16le(
        &thread->iconv_ctx,
        request->create.r_symlink_target,
        request->create.r_symlink_target_len,
        utf16,
        sizeof(utf16));

    if (name_len <= 0) {
        evpl_iovec_cursor_append_uint16(cursor, SMB2_ERROR_REPLY_SIZE);
        evpl_iovec_cursor_append_uint16(cursor, 0);
        evpl_iovec_cursor_append_uint16(cursor, 0);
        evpl_iovec_cursor_append_uint16(cursor, 0);
        evpl_iovec_cursor_append_uint8(cursor, 0);
        return;
    }

    flags = request->create.r_symlink_relative ?
        SMB2_SYMLINK_FLAG_RELATIVE : SMB2_SYMLINK_FLAG_ABSOLUTE;

    /* ReparseDataLength counts the SubstituteNameOffset..PathBuffer fields; the
     * substitute and print names are stored back to back (substitute first). */
    reparse_data_length = 12 + 2 * name_len;
    /* SymLinkLength counts everything after itself (SymLinkErrorTag onward). */
    sym_link_length = 4 /* SymLinkErrorTag */ + 4 /* ReparseTag */ +
        2 /* ReparseDataLength */ + 2 /* UnparsedPathLength */ + reparse_data_length;
    byte_count = 4 /* SymLinkLength */ + sym_link_length;

    /* SMB2 ERROR Response header (StructureSize=9, ErrorContextCount=0). */
    evpl_iovec_cursor_append_uint16(cursor, SMB2_ERROR_REPLY_SIZE);
    evpl_iovec_cursor_append_uint16(cursor, 0); /* ErrorContextCount + Reserved */
    evpl_iovec_cursor_append_uint32(cursor, byte_count);

    /* SymbolicLinkErrorResponse (MS-SMB2 2.2.2.2.1). */
    evpl_iovec_cursor_append_uint32(cursor, sym_link_length);
    evpl_iovec_cursor_append_uint32(cursor, SMB2_SYMLINK_ERROR_TAG);     /* 'SYML' */
    evpl_iovec_cursor_append_uint32(cursor, SMB2_IO_REPARSE_TAG_SYMLINK);
    evpl_iovec_cursor_append_uint16(cursor, reparse_data_length);
    evpl_iovec_cursor_append_uint16(cursor, request->create.r_symlink_unparsed); /* UnparsedPathLength */
    evpl_iovec_cursor_append_uint16(cursor, 0);        /* SubstituteNameOffset */
    evpl_iovec_cursor_append_uint16(cursor, name_len); /* SubstituteNameLength */
    evpl_iovec_cursor_append_uint16(cursor, name_len); /* PrintNameOffset */
    evpl_iovec_cursor_append_uint16(cursor, name_len); /* PrintNameLength */
    evpl_iovec_cursor_append_uint32(cursor, flags);
    evpl_iovec_cursor_append_blob(cursor, utf16, name_len); /* SubstituteName */
    evpl_iovec_cursor_append_blob(cursor, utf16, name_len); /* PrintName */
} /* chimera_smb_create_emit_symlink_error */

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

    /* The AppInstanceId force-close is a client *failover*: it replaces an open
     * held by a DIFFERENT client (one whose ClientGuid differs from the
     * requesting connection's).  A second open carrying the same AppInstanceId
     * from the SAME client (same ClientGuid, different connection) is not a
     * failover and must leave the prior open intact.  (SMB2Model AppInstanceId
     * SameClientGuid cases; the MS-SMB2 AppInstanceVersion failover uses two
     * distinct clients, so its ClientGuids differ and the close still applies.)
     *
     * Compare against the open's stored ClientGuid, not its create_conn: a
     * parked (disconnected) handle has had its create_conn cleared, and reading
     * the live connection would miss the same-client match -- wrongly failover-
     * displacing a parked persistent handle of the requester's own client
     * instead of leaving it to block the open with FILE_NOT_AVAILABLE (pike
     * appinstanceid test_appinstanceid_reconnect_same_clientguid). */
    if (memcmp(existing->client_guid,
               request->compound->conn->client_guid, 16) == 0) {
        return 0;
    }

    /* The replacement applies to the open of the same file on the SAME share:
     * the prior open is located by file handle, which spans share names that map
     * to one local path, so an open under a DIFFERENT share name (even of the
     * same underlying file) is not the failover target and is left intact.
     * (SMB2Model AppInstanceId DifferentShare* cases.) */
    if (!existing->tree || !request->tree ||
        existing->tree->share != request->tree->share) {
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

static void
smb_app_instance_retired(struct chimera_server_smb_thread *thread,
                         struct chimera_smb_open_file *open, void *private_data)
{
    (void) private_data;
    chimera_smb_open_file_drain_locks(thread, open);
    if (open->handle) { chimera_vfs_release(thread->vfs_thread, open->handle); open->handle = NULL; }
    atomic_store(&open->refcnt, 0);
    chimera_smb_open_file_free(thread, open);
}

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
    struct chimera_smb_tree      *tree;
    int                           bucket;
    bool                          unhashed = false;

    /* A parked (disconnected durable/persistent) open is no longer in any tree's
     * open_files hash -- chimera_smb_tree_free already HASH_DELETEd it and
     * returned its tree to the free pool, leaving open_file->tree dangling at a
     * recycled tree.  Taking the live-open path below (which locks that tree and
     * HASH_DELETEs again) would double-delete a stale hash node on a recycled
     * tree and crash.  Tear it down through the durable registry instead, the
     * same way the grace-timer sweeper does -- and force the persistent case,
     * since an AppInstanceId failover displaces even a persistent handle
     * (MS-SMB2 3.3.5.9.7). */
    if (open_file->flags & CHIMERA_SMB_OPEN_FILE_PARKED) {
        bool purged = chimera_smb_durable_purge_parked(thread, open_file->file_id.pid, true);
        chimera_smb_open_file_release_nr(thread, open_file->tree, open_file);
        return purged;
    }

    tree = open_file->tree;

    chimera_smb_abort_if(!tree, "AppInstance open has no tree owner");

    bucket = open_file->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    pthread_mutex_lock(&tree->open_files_lock[bucket]);
    if (!(open_file->flags & CHIMERA_SMB_OPEN_FILE_CLOSED)) {
        tree->compound_pins++; /* bridge unhash to the async retirement pin */
        open_file->flags |= CHIMERA_SMB_OPEN_FILE_CLOSED;
        HASH_DELETE(hh, tree->open_files[bucket], open_file);
        unhashed = true;
    }
    pthread_mutex_unlock(&tree->open_files_lock[bucket]);

    if (!unhashed) {
        chimera_smb_open_file_release_nr(thread, tree, open_file);
        return false;
    }

    /* Drop the durable registry entry so a reconnect can't reclaim it. */
    if (open_file->durable_flags) {
        chimera_smb_durable_forget(thread->shared, open_file->file_id.pid);
    }

    /* Long-lived notify/LOCK waiters own ordinary references. Logical
     * cutoff must wake them before waiting for the final reference. */
    struct chimera_smb_notify_state *notify = chimera_smb_notify_detach(open_file);
    if (notify) { chimera_smb_notify_close(thread->shared->vfs->vfs_notify, notify); }
    pthread_mutex_lock(&tree->open_files_lock[bucket]);
    chimera_smb_lock_abort_parked(open_file);
    pthread_mutex_unlock(&tree->open_files_lock[bucket]);
    /* The detached tree reference now owns retirement. Drop the temporary
     * claim-lookup pin first so an otherwise idle open retires synchronously. */
    chimera_smb_open_file_release_nr(thread, tree, open_file);
    open_file->retire_skip_doc = true;
    chimera_smb_open_file_retire_async(thread, open_file, smb_app_instance_retired, NULL);
    chimera_smb_tree_memory_unpin(thread->shared, tree);

    return true;
} /* chimera_smb_app_instance_force_close */

/* Finish an open once its share reservation is held (or not required): acquire
 * the caching (oplock/lease) grant, propagate delete-on-close, grant the durable
 * handle, and register the open in the tree.  Split out of gen_open_file so the
 * share acquire can be made asynchronous (parking on a batch-oplock break)
 * without duplicating this tail. Rejected admission frees the private open
 * and returns NULL; the caller retains responsibility for its VFS handle. */
static uint32_t chimera_smb_stream_register_base_delete(
    struct chimera_smb_request *, struct chimera_smb_open_file *, struct chimera_vfs_open_handle *);

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
    struct chimera_smb_request    *request,
    struct chimera_smb_open_file  *open_file,
    struct chimera_vfs_file_state *file_state,
    uint8_t                        held_granted,
    uint8_t                        held_denied);

/* Resume a parked share acquire once the conflicting batch oplock has been
 * relinquished (GRANTED -> finish the open) or kept (DENIED -> SHARING_VIOLATION). */
static void
chimera_smb_create_share_park_cb(
    enum chimera_vfs_claim_result            result,
    struct chimera_vfs_claim                *claim,
    const struct chimera_vfs_claim_conflict *conflict,
    void                                    *private_data);

static void
chimera_smb_create_resume_rearbitrate(
    struct chimera_smb_request *request);

/* Durable / persistent handle grant decision (MS-SMB2 3.3.5.9.10/.11/.12): a
 * durable handle is granted only when the open holds a batch oplock or a
 * HANDLE-caching lease; a persistent handle on a continuous-availability share
 * is exempt.  Reads oplock_level / lease_state off the open_file, so it can be
 * re-run by the parked-CREATE resume path after a deferred caching upgrade
 * makes the open durable-eligible. */
static void
chimera_smb_create_grant_durable(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_smb_tree          *tree   = request->tree;
    uint32_t                          ctx    = request->create.ctx_present_mask;
    bool                              has_caching;

    if (!thread->shared->config.persistent_handles) {
        return;
    }

    has_caching = (open_file->oplock_level == SMB2_OPLOCK_LEVEL_BATCH) ||
        (open_file->lease_state & SMB2_LEASE_HANDLE_CACHING);

    if (ctx & CHIMERA_SMB_CREATE_CTX_DH2Q) {
        /* DH2Q + CA is a request, not proof of recoverability. Stream and
         * mkdir paths currently have no recovery record; a failed KV write
         * must never gain persistence merely because an ID was reserved. */
        bool persistent = (request->create.dh2q.flags & SMB2_DHANDLE_FLAG_PERSISTENT) &&
            tree->share->continuous_availability && request->create.persist_record_written;

        /* Record the DH2Q create_guid and mark the open replay-eligible even
         * when no durable handle is granted (e.g. the request took no
         * batch/HANDLE-caching lease).  The replay machinery (MS-SMB2 3.3.5.9.10)
         * keys on the create_guid the client supplied and on IsReplayEligible,
         * not on whether a durable grant was made -- so a replayed create can be
         * matched against a still-pending or still-eligible non-durable open
         * (smb2.replay.dhv2-pending*-vs-* with a NONE oplock).  Eligibility is
         * cleared by the first non-replay op on the handle. */
        memcpy(open_file->create_guid, request->create.dh2q.create_guid, 16);
        open_file->flags |= CHIMERA_SMB_OPEN_FILE_REPLAY_ELIGIBLE;

        if (has_caching || persistent) {
            open_file->durable_flags = CHIMERA_SMB_DURABLE_V2;
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
} /* chimera_smb_create_grant_durable */

/* Register a freshly durable-granted open in the shared registry so it
 * survives a disconnect and can be reclaimed on reconnect.  Must be called
 * without the tree bucket lock held (bucket -> registry order, smb_durable.c). */
static void
chimera_smb_create_register_durable(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file)
{
    if (request->create.persist_record_written) {
        open_file->flags |= CHIMERA_SMB_OPEN_FILE_PERSISTED;
    }
    chimera_smb_durable_register(request->compound->thread->shared, open_file,
                                 request->session_handle->session->session_id,
                                 request->session_handle->session->cred.uid,
                                 request->compound->conn->client_guid,
                                 open_file->name, open_file->name_len,
                                 request->create.persist_record_written);
} /* chimera_smb_create_register_durable */

/*
 * Before a conflicting open breaks a caching holder, evict any *parked*
 * (disconnected durable) holder that still holds a write cache on this file: a
 * doomed break would only mark its lease BREAKING (the parked handle has no
 * connection to ack it), masking the conflict so the new open silently caps its
 * own grant -- and the orphaned handle leaks until its grace timer.  MS-SMB2 has
 * the disconnected handle yield to a conflicting open, so purge it instead.  A
 * parked holder with no write cache is courtesy-held and left to coexist
 * (keep-disconnected-rh-*), except that a truncating intent also invalidates
 * read caching.  Admission must revoke that disconnected cache even when a
 * different live open later denies the overwrite; no file data changes here.
 * pids are collected under file->lock, then purged
 * after it is dropped (purge takes the registry lock and tears down VFS state).
 */
static void
chimera_smb_create_purge_parked_writers(
    struct chimera_server_smb_thread *thread,
    const uint8_t                    *fh,
    uint8_t                           fh_len,
    uint64_t                          fh_hash,
    bool                              truncates)
{
    struct chimera_vfs_state      *vfs_state = thread->vfs_thread->vfs->vfs_state;
    struct chimera_vfs_file_state *file_state;
    struct chimera_vfs_claim      *claim;
    uint64_t                       pids[16];
    int                            npids = 0, i;
    uint8_t                        invalidate = CHIMERA_CLAIM_CW |
        (truncates ? CHIMERA_CLAIM_CR : 0);

    file_state = chimera_vfs_state_get(vfs_state, fh, fh_len, fh_hash, false);
    if (!file_state) {
        return;
    }

    pthread_mutex_lock(&file_state->lock);
    for (claim = file_state->claims[CHIMERA_CLAIM_CLASS_CACHE];
         claim && npids < 16; claim = claim->next) {
        if (claim->parked &&
            (claim->used & invalidate) &&
            claim->grant && claim->grant->members) {
            pids[npids++] =
                ((struct chimera_smb_open_file *) claim->grant->members)->file_id.pid;
        }
    }
    pthread_mutex_unlock(&file_state->lock);

    /* Classify each parked write holder (registry lock; must NOT be held under
     * file->lock per the bucket -> registry -> vfs_state order).  A durable-only
     * or expired holder YIELDS; a resilient holder within its timeout is
     * courtesy-held (CAP).  (A persistent holder never reaches here -- the
     * conflicting open was already rejected with FILE_NOT_AVAILABLE upstream.) */
    enum chimera_smb_durable_hold holds[16];
    for (i = 0; i < npids; i++) {
        holds[i] = chimera_smb_durable_parked_hold(thread->shared, pids[i]);
    }

    /* Courtesy-downgrade the CAP holders' write caching IN PLACE: a resilient
     * parked handle cannot ack a break (its connection is gone) and must not be
     * evicted, so instead of the doomed break/revoke we drop its write caching
     * here under file->lock (the parked flag already masks its advertised H).
     * A truncating intent also drops read caching while retaining the resilient
     * handle itself.
     * The conflicting open then caps its own grant to R against the holder's
     * retained read cache, while the holder stays parked and reclaimable (pike
     * durable test_resiliency_reconnect_before_timeout).  Re-find the claim by
     * pid under the lock rather than trusting a pointer cached across the
     * unlock. */
    /* CLAIMTODO: direct used/advertised mutation kept per the port spec; this
     * should become a core verb (a locked in-place downgrade). */
    pthread_mutex_lock(&file_state->lock);
    for (claim = file_state->claims[CHIMERA_CLAIM_CLASS_CACHE];
         claim; claim = claim->next) {
        if (!claim->parked || !claim->grant || !claim->grant->members) {
            continue;
        }
        uint64_t lpid =
            ((struct chimera_smb_open_file *) claim->grant->members)->file_id.pid;
        for (i = 0; i < npids; i++) {
            if (pids[i] == lpid && holds[i] == CHIMERA_SMB_DURABLE_HOLD_CAP) {
                claim->used      &= ~invalidate;
                claim->advertised = claim->used;
                break;
            }
        }
    }
    pthread_mutex_unlock(&file_state->lock);

    chimera_vfs_state_put(vfs_state, file_state);

    /* The yielding (durable-only / expired) holders are purged so the
     * conflicting open gets its full grant. */
    for (i = 0; i < npids; i++) {
        if (holds[i] == CHIMERA_SMB_DURABLE_HOLD_NONE) {
            chimera_smb_durable_purge_parked(thread, pids[i], false);
        }
    }
} /* chimera_smb_create_purge_parked_writers */

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
    struct chimera_vfs_state  *vfs_state = thread->vfs_thread->vfs->vfs_state;
    struct chimera_claim_actor actor     = {
        .owner          = {
            .proto      = CHIMERA_CLAIM_PROTO_SMB2,
            .client_key = request->session_handle->session->client_key,
            .owner_lo   = open_file->file_id.pid,
            .owner_hi   = open_file->file_id.vid,
        },
        .op_handle      = oh,
    };

    if (!break_trigger) {
        return;
    }

    /* An RqLs open is owned by its lease key, not its file_id; identify the
     * opener by the key so the trigger engine's KEY circle coalesces a lease
     * this client already holds under the same key instead of recalling it. */
    if (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) {
        memcpy(actor.owner.key, request->create.rqls.key, 16);
        memcpy(&actor.owner.owner_lo, request->create.rqls.key, 8);
        memcpy(&actor.owner.owner_hi, request->create.rqls.key + 8, 8);
    }

    if (phase == 1) {
        /* Evict parked write-cache holders before the (otherwise doomed) break. */
        chimera_smb_create_purge_parked_writers(thread, oh->fh, oh->fh_len, oh->fh_hash,
                                                truncates);
        chimera_vfs_claim_invalidate(
            vfs_state, oh->fh, oh->fh_len, oh->fh_hash,
            CHIMERA_TRIGGER_OPEN_H, &actor,
            truncates ? 0 : CHIMERA_CLAIM_CR
            /* spare leases pre-share-check; a lease keeps H on a
             * compatible open (the OPEN_H row is legacy-batch only) */);
    } else if (truncates) {
        chimera_vfs_claim_invalidate(vfs_state, oh->fh, oh->fh_len,
                                     oh->fh_hash, CHIMERA_TRIGGER_WRITE,
                                     &actor, 0);
    } else {
        /* A conflicting (non-truncating) open invalidates the (exclusive) write
         * cache of other holders, but read + handle caching stay shared: break
         * W-holders down to R|H, not R (MS-SMB2 / smb2.lease.v2_epoch2: a plain
         * OPEN against an RWH lease yields a single RWH->RH break and the holder
         * keeps RH).  A holder cascades below RH only when an actual write or a
         * truncating open follows (the WRITE trigger deepens the floor to NONE;
         * smb2.lease.breaking3).  For a legacy oplock R|H collapses to
         * LEVEL_II. */
        chimera_vfs_claim_invalidate(
            vfs_state, oh->fh, oh->fh_len, oh->fh_hash,
            CHIMERA_TRIGGER_OPEN_W, &actor,
            CHIMERA_CLAIM_CR | CHIMERA_CLAIM_H);
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

    open_file->parent_fh_len      = parent_fh_len;
    open_file->file_id.pid        = pid;
    open_file->file_id.vid        = chimera_rand64();
    open_file->handle             = oh;
    open_file->desired_access     = request->create.desired_access;
    open_file->share_access       = request->create.share_access;
    open_file->flags              = delete_on_close ? CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE : 0;
    open_file->doc_from_create    = !!delete_on_close;
    open_file->stream_delete_cred = request->session_handle->session->cred;
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
    if (type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE &&
        !chimera_smb_open_namespace_prepare(thread, open_file, request->create.stream_name_len != 0)) {
        request->create.force_close_status = SMB2_STATUS_INSUFFICIENT_RESOURCES;
        chimera_smb_open_file_free(thread, open_file);
        return NULL;
    }


    /* Phase-0 plumbing state: zeroed; Phases 1/3 will populate from CREATE contexts. */
    open_file->ctx_present_mask   = 0;
    open_file->oplock_level       = 0;
    open_file->lease_state        = 0;
    open_file->lease_epoch        = 0;
    open_file->lease_flags        = 0;
    open_file->durable_flags      = 0;
    open_file->durable_timeout_ms = 0;
    /* Clear the resiliency state too (open_file is reused from a free list
     * without being zeroed): a slot recycled from a prior FSCTL_LMR_REQUEST_
     * RESILIENCY handle would otherwise carry a stale resilient==true, making
     * this open's own resiliency request skip its durable-registry registration
     * (smb_proc_ioctl.c gates registration on !resilient) -- so a later reconnect
     * finds no parked handle and fails OBJECT_NAME_NOT_FOUND (#845:
     * BVT_ResilientHandle_LockSequence_Lease flaked under batch load). */
    open_file->resilient            = 0;
    open_file->resilient_timeout_ms = 0;
    /* No caching grant until the caching block below acquires one (open_file is
     * reused from a free list without being zeroed). */
    open_file->grant = NULL;
    /* No base-file delete reservation until a named-stream open registers one
     * (chimera_smb_stream_register_base_delete); cleared so a recycled slot
     * never carries a stale base reservation into drain_locks. */
    open_file->base_share_lease_inserted = false;
    open_file->base_share_file_state     = NULL;
    open_file->base_handle               = NULL;
    memset(open_file->lease_key,        0, sizeof(open_file->lease_key));
    memset(open_file->parent_lease_key, 0, sizeof(open_file->parent_lease_key));
    memset(open_file->create_guid,      0, sizeof(open_file->create_guid));
    memcpy(open_file->client_guid, request->compound->conn->client_guid,
           sizeof(open_file->client_guid));
    open_file->integrity_algo  = 0;
    open_file->integrity_flags = 0;

    /* Record the AppInstanceId/AppInstanceVersion this open carried so a later
     * CREATE on a different connection can match it and apply the version-gated
     * force-close (MS-SMB2 3.3.5.9.7 / 3.3.5.9.16).  SMB2_CREATE_APP_INSTANCE_ID
     * is an SMB 3.x create context: on a pre-3.x (2.0.2 / 2.1) connection the
     * server does not process it, so no failover identity is established and it
     * must not later match (and close) a conflicting open.  (SMB2Model
     * AppInstanceId cases whose initial open is on an Smb2002 connection.) */
    if ((request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_APP) &&
        request->compound->conn->dialect >= SMB2_DIALECT_3_0) {
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

    /* Build the canonical full path (relative to the share root, backslash
     * separated, no leading separator) reported in FileAllInformation /
     * FileNameInformation.  request->create.parent_path holds the parent
     * portion forward-slash converted; reassemble it with the leaf using
     * backslashes.  Bounded by SMB_PATH_MAX (the on-wire path limit). */
    {
        uint32_t parent_len = request->create.parent_path_len;
        uint32_t total;

        if (parent_len + 1 + name_len > SMB_PATH_MAX - 1) {
            /* Defensive clamp: the wire path is already bounded to SMB_PATH_MAX,
             * so this cannot legitimately overflow, but never write past the
             * buffer if a future caller passes a longer composite. */
            parent_len = 0;
        }

        if (parent_len > 0) {
            memcpy(open_file->full_path, request->create.parent_path, parent_len);
            chimera_smb_slash_forward_to_back(open_file->full_path, parent_len);
            open_file->full_path[parent_len] = '\\';
            memcpy(open_file->full_path + parent_len + 1, name, name_len);
            total = parent_len + 1 + name_len;
        } else {
            memcpy(open_file->full_path, name, name_len);
            total = name_len;
        }
        open_file->full_path[total] = '\0';
        open_file->full_path_len    = total;
    }

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
            for (struct chimera_vfs_claim *l =
                     file_state->claims[CHIMERA_CLAIM_CLASS_ACCESS];
                 l; l = l->next) {
                struct chimera_smb_open_file *of =
                    (struct chimera_smb_open_file *) l->cb_private;
                int                           d;

                if (!of) {
                    continue;
                }
                d = chimera_smb_app_instance_decision(request, of);
                if (d != 0) {
                    if (d > 0) {
                        /* Claim storage protects the pointer under file->lock;
                         * never resurrect an open already at final release. */
                        uint32_t refs = atomic_load(&of->refcnt);
                        while (refs && !atomic_compare_exchange_weak(&of->refcnt, &refs, refs + 1)) {}
                        if (!refs) { continue; }
                    }
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
                /* The claim-side pin cannot take a bucket while holding the
                 * file lock. Validate the exclusive migration gate after
                 * dropping that lock: either its earlier gate wins, or this
                 * retained reference prevents a new migration from starting. */
                unsigned int bucket = match->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
                pthread_mutex_lock(&match->tree->open_files_lock[bucket]);
                bool migrating = match->identity_rebind != NULL;
                pthread_mutex_unlock(&match->tree->open_files_lock[bucket]);
                if (migrating) {
                    chimera_smb_open_file_release_nr(thread, match->tree, match);
                    open_file->handle = NULL;
                    chimera_smb_open_file_free(thread, open_file);
                    request->create.force_close_status = SMB2_STATUS_FILE_NOT_AVAILABLE;
                    return NULL;
                }
                /* Consumes the temporary claim-lookup reference. */
                chimera_smb_app_instance_force_close(thread, match);
            }
        }
    }

    /* MS-SMB2 disconnected-persistent-handle rule: a fresh open of a file held
     * by a *parked* persistent (CA) handle that is still within its timeout
     * fails with STATUS_FILE_NOT_AVAILABLE -- the file is reserved for the
     * persistent handle's reclaim and a conflicting open must not coexist (pike
     * persistent test_open_while_disconnected, test_resiliency_*_before_timeout).
     * A *resilient* (non-persistent) parked holder does NOT block here: it
     * courtesy-caps a conflicting lease open instead (in the break path).  The
     * persistent holder's own DH2C reclaim takes the durable-reconnect path, not
     * this fresh-create path, so it is never self-blocked.  We are pre-share
     * here (no leases acquired yet), so reject exactly like the AppInstanceId
     * path: NULL out the handle, free the open, set force_close_status. */
    if (type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE && tree->share && oh) {
        struct chimera_vfs_state      *vfs_state  = thread->vfs_thread->vfs->vfs_state;
        struct chimera_vfs_file_state *file_state =
            chimera_vfs_state_get(vfs_state, oh->fh, oh->fh_len, oh->fh_hash, false);
        bool                           blocked = false;

        if (file_state) {
            uint64_t block_pids[16];
            int      nblock = 0, k;

            pthread_mutex_lock(&file_state->lock);
            for (struct chimera_vfs_claim *l =
                     file_state->claims[CHIMERA_CLAIM_CLASS_ACCESS];
                 l && nblock < 16; l = l->next) {
                if (l->parked) {
                    block_pids[nblock++] = l->owner.owner_lo;
                }
            }
            pthread_mutex_unlock(&file_state->lock);
            chimera_vfs_state_put(vfs_state, file_state);

            for (k = 0; k < nblock; k++) {
                if (chimera_smb_durable_parked_hold(thread->shared, block_pids[k]) ==
                    CHIMERA_SMB_DURABLE_HOLD_BLOCK) {
                    blocked = true;
                    break;
                }
            }
        }

        if (blocked) {
            open_file->handle = NULL;
            chimera_smb_open_file_free(thread, open_file);
            request->create.force_close_status = SMB2_STATUS_FILE_NOT_AVAILABLE;
            return NULL;
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
        struct chimera_vfs_state         *vfs_state = thread->vfs_thread->vfs->vfs_state;
        struct chimera_vfs_file_state    *file_state;
        uint32_t                          da = open_file->desired_access;
        uint32_t                          sa = open_file->share_access;
        uint8_t                           granted = 0, denied = 0;
        uint8_t                           held_granted = 0, held_denied = 0;
        struct chimera_claim_owner        share_owner;
        struct chimera_vfs_claim_conflict conflict;
        enum chimera_vfs_claim_result     result;

        /* Map desired_access -> R/W/D access.  Only data-access rights
         * participate in share-mode conflicts (matching Windows and the
         * legacy smb_sharemode_check_conflict): READ_DATA/EXECUTE are
         * "read", WRITE_DATA/APPEND_DATA are "write", DELETE is "delete".
         * EA and attribute rights (READ_EA, WRITE_EA, *_ATTRIBUTES) do NOT
         * count.  Generic + MAXIMUM_ALLOWED expand to the data rights they
         * imply, as smb_sharemode_expand_access used to do. */
        if (da & (SMB2_FILE_READ_DATA | SMB2_FILE_EXECUTE |
                  SMB2_GENERIC_READ | SMB2_GENERIC_EXECUTE |
                  SMB2_GENERIC_ALL | SMB2_MAXIMUM_ALLOWED)) {
            granted |= CHIMERA_CLAIM_R;
        }
        if (da & (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA |
                  SMB2_GENERIC_WRITE |
                  SMB2_GENERIC_ALL | SMB2_MAXIMUM_ALLOWED)) {
            granted |= CHIMERA_CLAIM_W;
        }
        if (da & (SMB2_DELETE | SMB2_GENERIC_ALL | SMB2_MAXIMUM_ALLOWED)) {
            granted |= CHIMERA_CLAIM_D;
        }

        /* Map share_access -> R/W/D deny.  Each access bit NOT shared
         * becomes a deny on the corresponding bit (D is 0x04 in the claim
         * vocabulary, not the old lease core's 0x08). */
        if (!(sa & SMB2_FILE_SHARE_READ)) {
            denied |= CHIMERA_CLAIM_R;
        }
        if (!(sa & SMB2_FILE_SHARE_WRITE)) {
            denied |= CHIMERA_CLAIM_W;
        }
        if (!(sa & SMB2_FILE_SHARE_DELETE)) {
            denied |= CHIMERA_CLAIM_D;
        }

        /* A truncating disposition must obtain write access at open time to
         * overwrite the data, so it conflicts with an existing opener that
         * denies write -- but it does not *hold* write for the handle's
         * lifetime (the granted access is what was requested).  Request write
         * transiently for the conflict check, then shrink the held grant
         * once the claim is inserted (chimera_vfs_claim_shrink). */
        held_granted = granted;
        held_denied  = denied;

        bool truncating =
            request->create.create_disposition == SMB2_FILE_SUPERSEDE ||
            request->create.create_disposition == SMB2_FILE_OVERWRITE ||
            request->create.create_disposition == SMB2_FILE_OVERWRITE_IF;

        if (truncating) {
            granted |= CHIMERA_CLAIM_W;
        }

        /* Attribute-only opens (no data/delete access bits, e.g. a
         * READ_ATTRIBUTES stat-open) hold no share-mode rights and impose no
         * deny.  Register them anyway as an inert (granted=0, denied=0) SHARE
         * entry so the layer can answer "is there another opener?" (sole-access
         * oplock grant), delete-pending, and stat-open classification -- without
         * changing any conflict outcome (chimera_vfs_share_conflict and the
         * CACHING sole-access loop both treat a (0,0) entry as absent). */
        if (!(da & SMB2_SHAREMODE_ACCESS_MASK)) {
            /* Whatever it asserted while opening, an attribute-only handle
             * RETAINS nothing: the truncate's write was transient, so the
             * handle blocks nobody afterwards. */
            held_granted = 0;
            held_denied  = 0;

            /* But only a true STAT open -- attribute-only AND non-truncating
             * -- is exempt from arbitration in the first place.  A truncating
             * open is going to modify the file, so it asserts the write it
             * just took AND its ShareAccess, in both directions, exactly like
             * an ordinary writer.  Zeroing them here discarded the very write
             * bit added just above, which is the one case where the truncate
             * is the ONLY source of it (MS-FSA 2.1.5.1.2; found by replaying
             * the spec corpus against Samba, which refuses these). */
            if (!truncating) {
                granted = 0;
                denied  = 0;
            }
        }

        file_state = chimera_vfs_state_get(vfs_state,
                                           oh->fh, oh->fh_len,
                                           oh->fh_hash, true);
        if (!file_state) {
            open_file->handle = NULL;
            chimera_smb_open_file_free(thread, open_file);
            return NULL;
        }

        /* The open itself is the owner — different opens, even by the
         * same client, must satisfy share-mode constraints between
         * themselves.  A RqLs open additionally carries its 16-byte LeaseKey
         * in owner.key: the KEY circle replaces the old break-skip logic (a
         * handle-caching lease already held under the same key is its own —
         * a second open under one lease key — and must coalesce, not be
         * broken by this share acquire). */
        memset(&share_owner, 0, sizeof(share_owner));
        share_owner.proto      = CHIMERA_CLAIM_PROTO_SMB2;
        share_owner.client_key = request->session_handle->session->client_key;
        share_owner.owner_lo   = open_file->file_id.pid;
        share_owner.owner_hi   = open_file->file_id.vid;
        if (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) {
            memcpy(share_owner.key, request->create.rqls.key, 16);
        }

        /* The constructor zeroes the claim (including a stale parked=1 a
         * recycled open_file slot could otherwise carry from a previously-
         * disconnected durable handle). */
        chimera_vfs_claim_init_smb_open(chimera_smb_share_claim(open_file),
                                        granted, denied, &share_owner);
        chimera_smb_share_claim(open_file)->op_handle = oh;
        /* Back-reference to the owning open so a conflicting CREATE carrying a
         * matching AppInstanceId can locate it for the force-close rule below;
         * policy_tag carries the open's pid by value in conflict reports (the
         * durable purge loop).  The batch-escape link to this open's own cache
         * grant (own_cache) is stamped once the grant exists, in
         * chimera_smb_create_after_share. */
        chimera_smb_share_claim(open_file)->cb_private = open_file;
        chimera_smb_share_claim(open_file)->policy_tag = open_file->file_id.pid;
        open_file->access_owner = chimera_vfs_claim_access_owner_alloc(file_state, chimera_smb_share_claim(open_file));
        if (!open_file->access_owner) {
            chimera_vfs_state_put(vfs_state, file_state);
            open_file->handle = NULL;
            chimera_smb_open_file_free(thread, open_file);
            return NULL;
        }


        /* AppInstanceId failover (MS-SMB2 3.3.5.9.7 / 3.3.5.9.16) was already
         * resolved before the phase-1 break above (so the displaced open is
         * force-closed silently); nothing to do here. */

        memset(&conflict, 0, sizeof(conflict));
        result = chimera_vfs_claim_try_acquire(vfs_state, file_state,
                                               chimera_smb_share_claim(open_file), &conflict);

        /* A new open that conflicts with a *disconnected* durable handle must
         * not be refused: MS-SMB2 has the disconnected (non-persistent) handle
         * yield.  The conflict may be the parked open's share reservation
         * (DENIED) or its HANDLE-caching lease that the share acquire must break
         * (BREAKING).  The conflict arrives BY VALUE carrying the holder's
         * policy_tag — the SMB open's persistent id, stamped on every SMB claim
         * at build time — so resolve it to a pid directly; if that open is a
         * parked durable, purge it and retry.  Live, persistent, and non-durable
         * holders are left intact, so they still produce a real
         * SHARING_VIOLATION.  Bounded by the parked-owner count. */
        int purge_guard = 64;
        while (result != CHIMERA_CLAIM_GRANTED && !conflict.admission_fenced && purge_guard-- > 0) {
            uint64_t conflict_pid = conflict.policy_tag;

            if (conflict_pid == 0) {
                /* Not an SMB open (or no conflict reported): nothing to purge. */
                break;
            }

            if (!chimera_smb_durable_purge_parked(thread, conflict_pid, false)) {
                /* The durable holder is not parked yet.  If its owning connection
                * is on its way out, the park (and the MS-SMB2 yield) is imminent
                * but has not landed -- this is the disconnect-vs-conflicting-open
                * race (open2-lease).  Flag the open to retry on a timer rather
                * than deny: CONFIRMED (disconnect observed, or already parked)
                * gets the full budget; SPECULATIVE (a durable holder whose FIN may
                * not be read yet) gets a short budget.  Only a persistent or
                * unknown holder is NONE, standing as a real SHARING_VIOLATION. */
                request->create.gen_share_retry =
                    chimera_smb_durable_conn_disconnecting(thread->shared,
                                                           conflict_pid);
                break;
            }
            memset(&conflict, 0, sizeof(conflict));
            result = chimera_vfs_claim_try_acquire(vfs_state, file_state,
                                                   chimera_smb_share_claim(open_file),
                                                   &conflict);
        }

        if (result != CHIMERA_CLAIM_GRANTED && conflict.admission_fenced) {
            /* A tentative last-close blocks admission, not a durable holder.
             * Never evict peers or send caching recalls for this conflict. */
            request->create.gen_share_retry = CHIMERA_SMB_DURABLE_YIELD_NONE;
            request->create.force_close_status = SMB2_STATUS_FILE_NOT_AVAILABLE;
            chimera_vfs_state_put(vfs_state, file_state);
            open_file->handle = NULL;
            chimera_smb_open_file_free(thread, open_file);
            return NULL;
        }

        if (result == CHIMERA_CLAIM_BREAKING &&
            conflict.construct != CHIMERA_CONSTRUCT_RQLS &&
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
            request->create.gen_held_denied  = held_denied;
            request->create.gen_parked       = 1;
            /* Publish this create's create_guid before the park: while it waits,
             * it is not in tree->open_files[], so only tree->pending_creates lets
             * a replay of it be recognised as a pending create. */
            chimera_smb_create_pending_register(request);
            /* Emit the interim STATUS_PENDING now, exactly as the break-park path
             * does (MS-SMB2 3.3.5.9 pending open): the client must learn the
             * create is in flight so it can cancel it and does not time out across
             * the break wait, which lasts until the holder acks/closes (or the
             * break deadline revokes it).  Without this a conflicting open looks
             * simply unanswered -- smbtorture's WAIT_FOR_ASYNC_RESPONSE spins to
             * its own request timeout and the test's later break ack arrives after
             * the deadline has already revoked the lease
             * (smb2.replay.dhv2-pending1n-vs-violation-lease-*).  The request lands
             * on conn->parked_requests for CANCEL/teardown, but it resumes through
             * its vfs ticket, NOT the parked-CREATE break sweep -- see the
             * gen_parked guard in chimera_smb_create_resume_parked_conn. */
            chimera_smb_async_interim_begin(request);
            /* SHARE claims never project (range-only at the backend);
             * NULL skips the projection probe. */
            chimera_vfs_claim_acquire(NULL, vfs_state, file_state,
                                      chimera_smb_share_claim(open_file),
                                      &request->create.gen_ticket,
                                      true /* wait */, false /* wait_hard */,
                                      chimera_smb_create_share_park_cb, NULL,
                                      request);
            return NULL;
        }

        if (result != CHIMERA_CLAIM_GRANTED) {
            /* A genuine share conflict (SHARING_VIOLATION).  A handle-caching
             * RqLs lease holder must still be told to relinquish its handle
             * cache (RWH->RW) even though this open is refused -- the holder may
             * close its deferred handle so a later retry succeeds (MS-SMB2;
             * smb2.lease.break_twice).  Batch oplocks already broke via the
             * BREAKING/park path above; this covers leases, which admission
             * spares -- the OPEN_H_FORCE trigger strips an RqLs lease's H too.
             * A share-conflicting RqLs lease was already asked to drop H by
             * admission.  Refuse immediately while its acknowledgement is
             * pending: waiting here deadlocks a client that must receive this
             * failed CREATE before sending the lease acknowledgement.
             * The opener is identified by lease key (RqLs) or file id so the
             * holder's own lease is not self-broken. */
            struct chimera_claim_actor brk_actor = {
                .owner     = chimera_smb_share_claim(open_file)->owner,
                .op_handle = oh,
            };
            if (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) {
                memcpy(&brk_actor.owner.owner_lo, request->create.rqls.key, 8);
                memcpy(&brk_actor.owner.owner_hi, request->create.rqls.key + 8, 8);
            }
            chimera_vfs_claim_invalidate(
                vfs_state, oh->fh, oh->fh_len, oh->fh_hash,
                CHIMERA_TRIGGER_OPEN_H_FORCE, &brk_actor,
                CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW
                /* retain read+write caching; only H is stripped on a real
                 * share conflict (break_twice) */);

            chimera_vfs_state_put(vfs_state, file_state);
            open_file->handle = NULL;
            chimera_smb_open_file_free(thread, open_file);
            return NULL;
        }

        chimera_smb_create_finish_share_grant(request, open_file, file_state,
                                              held_granted,
                                              held_denied);
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

    /* Base-file ACCESS admission is part of stream admission, before cache
     * grants and registry publication. In particular a DOC admission fence
     * must reject this open rather than silently omit its base reservation. */
    if (request->create.stream_name_len && request->create.base_oh) {
        struct chimera_vfs_open_handle *base = request->create.base_oh;
        open_file->flags |= CHIMERA_SMB_OPEN_FILE_FLAG_STREAM;
        open_file->stream_name_len = request->create.stream_name_len;
        memcpy(open_file->stream_name, request->create.stream_name, open_file->stream_name_len);
        open_file->base_fh_len = base->fh_len;
        memcpy(open_file->base_fh, base->fh, base->fh_len);
        uint32_t status = chimera_smb_stream_register_base_delete(request, open_file, base);
        if (status != SMB2_STATUS_SUCCESS) {
            chimera_smb_open_file_drain_locks(thread, open_file);
            open_file->handle = NULL;
            chimera_smb_open_file_free(thread, open_file);
            request->create.force_close_status = status;
            return NULL;
        }
    }

    /* A directory open is eligible for an SMB3 directory lease when the feature
     * is enabled and the client requested it via an RqLs v2 context (directory
     * leases are SMB 3.0+ / lease-v2 only — never a legacy oplock, which is what
     * the dirlease.oplocks test asserts is refused).  A directory lease grants
     * R/H only (W is masked off below) and is broken on a directory content
     * change (the DIR_CONTENT trigger via the notify chokepoint). */
    int                               is_directory = (open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) != 0;
    /* Directory leasing is an SMB 3.0+ feature (MS-SMB2 3.3.5.9.11): the lease-v2
     * context that carries it is only valid on a 3.x dialect. A v2 RqLs sent over
     * SMB 2.0.2 / 2.1 must be ignored so the open returns OPLOCK_LEVEL_NONE
     * (dirlease.Negative_SMB2002 / _SMB21). */
    int                               dir_lease_ok = is_directory && oh &&
        thread->shared->config.directory_leases &&
        request->compound->conn->dialect >= SMB2_DIALECT_3_0 &&
        (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) &&
        request->create.rqls.is_v2;

    /* Acquire a cache claim (SMB2 lease via RqLs, or legacy oplock
     * via requested_oplock_level).  This is opportunistic — failure to
     * grant just means the open succeeds with no lease.  Conflicts are
     * resolved by the claim core's admission tables. */
    if ((type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE && oh && !is_directory) ||
        dir_lease_ok) {
        struct chimera_vfs_state         *vfs_state = thread->vfs_thread->vfs->vfs_state;
        struct chimera_vfs_file_state    *file_state;
        uint8_t                           req_smb  = 0;
        uint8_t                           req_vfs  = 0;
        bool                              via_rqls = false;
        struct chimera_vfs_claim_conflict conflict;
        enum chimera_vfs_claim_result     result;

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
            /* SMB2 leases are opt-in (smb_leases).  When disabled, grant no
             * caching bits: via_rqls stays set so the open still reports a lease
             * with state NONE (the existing bare-RqLs path), but nothing is
             * cached, so no lease break can ever stall a conflicting open. */
            req_smb = thread->shared->config.leases
                ? (uint8_t) (request->create.rqls.state & 0x07) : 0;
            /* MS-SMB2 3.3.5.9.8 / smb2.lease.request: WRITE- and HANDLE-caching
             * are meaningful only alongside READ caching.  A requested lease
             * state with W or H but no R is invalid and granted NONE (H->"",
             * W->"", HW->""); R/RH/RW/RHW pass through. */
            if (!(req_smb & SMB2_LEASE_READ_CACHING)) {
                req_smb = 0;
            }
        } else if (thread->shared->config.oplocks) {
            /* Legacy SMB oplocks are opt-in (smb_oplocks); when disabled no
             * oplock is granted (req_smb stays 0 -> reply OPLOCK_LEVEL_NONE). */
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

        /* SMB2_SHAREFLAG_FORCE_LEVELII_OPLOCK: on a force-level-2 share the
         * server grants at most a read (LEVEL_II) cache, so an exclusive/batch
         * oplock or a write/handle lease is downgraded by clearing the W and H
         * caching bits (MS-SMB2 3.3.5.9.x; WPTS OplockOnShareWithForceLevel2).
         * A bare R (or already-NONE) request is unaffected. */
        if (tree->share && tree->share->force_level2_oplock) {
            req_smb &= SMB2_LEASE_READ_CACHING;
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

        /* A directory lease never carries write caching: only read (cached
         * enumeration) and handle (deferred close) caching apply to a directory
         * (MS-SMB2 — requesting RHW on a directory grants RH). */
        if (is_directory) {
            req_vfs &= ~CHIMERA_CLAIM_CW;
        }

        /* A caching lease (oplock / SMB2 lease) lives in a VFS-owned, owner-keyed,
         * refcounted grant.  Opens by one client under one lease key (RqLs) COALESCE
         * onto a single grant; legacy oplocks are keyed by file id and so are always
         * sole members.  We ENTER this block for EVERY RqLs open -- even one that
         * requests no caching bits -- so a re-open under an existing lease key joins
         * (and never downgrades) the shared lease (MS-SMB2 3.3.5.9.8: a lease is not
         * reduced by a subsequent open).  A *fresh* grant is created only when the
         * open actually requests caching bits (req_vfs != 0).
         *
         * An attribute-only ("stat") open is eligible for an oplock/lease just
         * like a data open: MS-SMB2 3.3.5.9 grants the requested oplock based on
         * the existing-open set, not the requester's data-access bits (Windows
         * grants a BATCH oplock to a READ_ATTRIBUTES|SYNCHRONIZE create --
         * smb2.oplock.batch9/batch9a/statopen1).  A purely passive probe that
         * requests no oplock asks for NONE, so req_vfs == 0 and no grant is taken
         * -- which is what leaves an existing holder's oplock intact.  The break
         * side (a later real open breaking this holder) is handled separately by
         * the conflict matrix, independent of how this open's access looked. */
        bool want_fresh_caching = (req_vfs != 0);

        /* A stat-open (attribute-only access, non-data-replacing disposition) is
         * "oplock-transparent" (MS-FSA): it must NEVER break an existing holder.
         * It may still ACQUIRE the oplock it requested, but only the subset
         * grantable WITHOUT breaking anyone -- so a sole stat-open gets its full
         * BATCH (smb2.oplock.batch9/batch9a/statopen1), while a stat-open behind
         * an existing holder caps to NONE and fires no break
         * (smb2.oplock.batch8/exclusive4).  This is the same cap-self rule the
         * RqLs lease path uses; for a stat-open we apply it to legacy oplocks too
         * (a *data* legacy oplock keeps the break-the-peer arbitration). */
        /* "stat-open" == an open that does NOT break a conflicting holder: the
         * exact inverse of break_for_open's break_trigger (MS-FSA / smb2.oplock.
         * statopen1).  DELETE, WRITE_DAC, WRITE_OWNER, EA and any data/generic
         * access -- plus delete-on-close or a truncating disposition -- make it a
         * "real" open that breaks; only READ_ATTRIBUTES / WRITE_ATTRIBUTES /
         * READ_CONTROL / SYNCHRONIZE leave it transparent.  Keep this mask in
         * sync with chimera_smb_create_break_for_open. */
        bool stat_open =
            !((request->create.desired_access &
               (SMB2_FILE_READ_DATA | SMB2_FILE_WRITE_DATA |
                SMB2_FILE_APPEND_DATA | SMB2_FILE_EXECUTE |
                SMB2_FILE_READ_EA | SMB2_FILE_WRITE_EA |
                SMB2_WRITE_DACL | SMB2_WRITE_OWNER |
                SMB2_GENERIC_READ | SMB2_GENERIC_WRITE |
                SMB2_GENERIC_EXECUTE | SMB2_GENERIC_ALL |
                SMB2_MAXIMUM_ALLOWED | SMB2_DELETE)) ||
              (request->create.create_options & SMB2_FILE_DELETE_ON_CLOSE) ||
              request->create.create_disposition == SMB2_FILE_OVERWRITE ||
              request->create.create_disposition == SMB2_FILE_OVERWRITE_IF ||
              request->create.create_disposition == SMB2_FILE_SUPERSEDE);

        if (via_rqls || want_fresh_caching) {
            file_state = chimera_vfs_state_get(vfs_state,
                                               oh->fh, oh->fh_len,
                                               oh->fh_hash, true);
            if (file_state) {
                struct chimera_vfs_claim_grant *grant = NULL;
                struct chimera_claim_owner      owner;
                uint8_t                         want_used;
                uint8_t                         grant_is_v2;

                memset(&owner, 0, sizeof(owner));
                owner.proto      = CHIMERA_CLAIM_PROTO_SMB2;
                owner.client_key = request->session_handle->session->client_key;
                if (via_rqls) {
                    /* RqLs: owner identity is the lease key, so same-key opens by
                     * one client coalesce (the Samba locking.tdb rule).  The
                     * 16-byte key lives in owner.key (the KEY circle — it is what
                     * drives the same-lease-key write/open coherence exemption,
                     * never applied to a legacy oplock); the lo/hi halves are
                     * kept too for owner-identity comparisons. */
                    memcpy(owner.key, request->create.rqls.key, 16);
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
                /* Atomic coalescing makes this member walkable before it
                 * returns. Break delivery chooses the lease wire dialect
                 * from this tag, so initialize it before attachment. Rights
                 * and epochs are always taken from the actual grant. */
                if (via_rqls) { open_file->oplock_level = SMB2_OPLOCK_LEVEL_LEASE; }

                want_used   = req_vfs;
                grant_is_v2 = via_rqls && request->create.rqls.is_v2;

                /* A legacy oplock and an SMB2 RqLs lease held by the SAME client on
                * the SAME file interact per MS-SMB2 3.3.5.9 (smb2.lease.oplock
                * loop 1), and a legacy oplock request must NEVER recall the
                * requesting client's own lease:
                *   - the client holds an H (handle) lease (RH / RHW): the oplock
                *     is granted NONE (handle caching already owns the handle);
                *   - the client holds a non-H lease (R / RW): the oplock is capped
                *     to level-II (R), which coexists with the lease's read cache.
                * Either way we step the legacy oplock's requested mode DOWN here so
                * its try_insert never fires a self-break against the client's lease
                * (the exclusive/batch W bit would otherwise recall the R lease). */
                if (!via_rqls && want_fresh_caching) {
                    if (chimera_vfs_claim_client_holds_handle_cache(
                            file_state, owner.client_key)) {
                        want_fresh_caching = false;
                    } else if (chimera_vfs_claim_client_holds_cache(
                                   file_state, owner.client_key)) {
                        /* Cap the oplock to a level-II read cache so it coexists
                         * with the client's own R/RW lease without breaking it. */
                        req_vfs  &= CHIMERA_CLAIM_CR;
                        want_used = req_vfs;
                    }
                }

                /* First coalesce onto an existing same-owner grant: refcount + a
                 * conflict-free in-place upgrade, never a downgrade.  This is what a
                 * lease re-open does -- including one requesting fewer/no bits, which
                 * keeps the lease at its current state (3.3.5.9.8). */
                grant = chimera_vfs_claim_grant_coalesce_member(file_state, &owner,
                    want_used, 1 /*upgrade_ok*/, open_file, &open_file->grant_member_next);
                if (!grant && want_fresh_caching) {
                    /* No existing grant: have the CORE create one and arbitrate
                     * against other owners, stepping W|H -> R on conflict (the
                     * sole-access rule, or a holder still mid-downgrade) until a
                     * shared read cache is grantable.  The core allocates and
                     * frees grants; we supply a template claim carrying the
                     * construct, owner, mode and the break callback. */
                    struct chimera_vfs_claim tmpl;
                    int                      settle_guard = 6;

                    /* Build (or rebuild, after a settle-loop downgrade) the
                     * template at `mode`.  The oplock construct (II/EX/BATCH) is
                     * derived from the used bits, so the constructor is re-run on
                     * every mode change. */
                    #define CHIMERA_SMB_CREATE_TMPL_INIT(mode)                      \
                            do {                                                        \
                                if (is_directory) {                                     \
                                    chimera_vfs_claim_init_dir_lease(&tmpl, (mode),     \
                                                                     &owner);           \
                                } \
                                else if (via_rqls) {                                  \
                                    chimera_vfs_claim_init_rqls(&tmpl, (mode), &owner); \
                                } else {                                                \
                                    chimera_vfs_claim_init_oplock(&tmpl, (mode),        \
                                                                  &owner);              \
                                }                                                       \
                                /* The break cb resolves the grant from claim->grant \
                                 * and picks a live member to deliver the OPLOCK_BREAK \
                                 * on.  Anchor to THIS open's VFS handle so a setattr \
                                 * through the same handle does not recall the lease \
                                 * against itself. */                               \
                                tmpl.break_cb   = chimera_smb_lease_break_cb;           \
                                tmpl.op_handle  = oh;                                   \
                                tmpl.cb_private = NULL;                                 \
                                tmpl.policy_tag = open_file->file_id.pid;               \
                            } \
                            while (0)

                    CHIMERA_SMB_CREATE_TMPL_INIT(want_used);

                    /* MS-SMB2 3.3.5.9: granting an oplock/lease never breaks a
                     * peer that the requester can simply coexist with.  Cap
                     * the requested mode to the subset grantable against the
                     * current holders:
                     *   - behind a peer's READ cache (LEVEL_II / R lease) an
                     *     exclusive/batch request caps to a shared R(H) cache,
                     *     so two readers coexist with NO break
                     *     (smb2.oplock.batch9 phase 3; smb2.lease.nobreakself);
                     *   - behind a peer's EXCLUSIVE/BATCH holder the non-strict
                     *     cap still returns the CR floor with a residual
                     *     conflict, so grant_acquire below breaks that holder
                     *     down (the exclusive-arbitration path -- batch1..8).
                     * Every stat-open caps STRICTLY (0 rather than CR) so an
                     * oplock-transparent probe never breaks anyone, including
                     * a metadata-only RqLs request behind a write cache. */
                    want_used = chimera_vfs_claim_grant_cap_mode(
                        file_state, &tmpl, stat_open);

                    /* A stat-open capped to no bits takes no grant. The
                     * reporting path emits legacy NONE or a bare RqLs NONE
                     * response without advancing the lease epoch. */
                    if (stat_open && want_used == 0) {
                        grant = NULL;
                        goto report_caching;
                    }

                    CHIMERA_SMB_CREATE_TMPL_INIT(want_used);

                    /* Seed the fresh grant's member head under the core's
                     * insert so a break can never observe a memberless
                     * grant (the old pre-registered-member discipline).
                     * The seed must be walk-ready: no stale next link. */
                    open_file->grant_member_next = NULL;

                    if (!via_rqls) {
                        open_file->oplock_level = chimera_smb_vfs_to_oplock_level(want_used);
                    }
                    memset(&conflict, 0, sizeof(conflict));
                    result = chimera_vfs_claim_grant_acquire_member(
                        vfs_state, file_state, &tmpl, 0 /* upgrade_ok */,
                        grant_is_v2, grant_is_v2 ? (uint16_t) (request->create.rqls.epoch + 1) : 1,
                        CHIMERA_CLAIM_GRANT_EXACT,
                        open_file, &open_file->grant_member_next,
                        &grant, &conflict);
                    while (result != CHIMERA_CLAIM_GRANTED &&
                           settle_guard-- > 0) {
                        /* CW (write cache) is exclusive across lease keys; a
                         * conflicting holder forces it off but CR+H stay shared.
                         * Drop only CW first (-> CR|H), then -- if CR|H itself is
                         * still hard-denied (a cross-client handle conflict) --
                         * step down to CR; give up only when even CR is
                         * unobtainable and we are not merely awaiting a break. */
                        if (want_used & CHIMERA_CLAIM_CW) {
                            want_used &= ~CHIMERA_CLAIM_CW;
                        } else if (want_used != CHIMERA_CLAIM_CR &&
                                   result == CHIMERA_CLAIM_DENIED) {
                            want_used = CHIMERA_CLAIM_CR;
                        } else if (result != CHIMERA_CLAIM_BREAKING) {
                            break;
                        }
                        CHIMERA_SMB_CREATE_TMPL_INIT(want_used);
                        if (!via_rqls) {
                            open_file->oplock_level = chimera_smb_vfs_to_oplock_level(want_used);
                        }
                        memset(&conflict, 0, sizeof(conflict));
                        result = chimera_vfs_claim_grant_acquire_member(
                            vfs_state, file_state, &tmpl, 0 /* upgrade_ok */,
                            grant_is_v2, grant_is_v2 ? (uint16_t) (request->create.rqls.epoch + 1) : 1,
                        CHIMERA_CLAIM_GRANT_EXACT,
                            open_file, &open_file->grant_member_next,
                            &grant, &conflict);
                    }
                    #undef CHIMERA_SMB_CREATE_TMPL_INIT

                    if (result != CHIMERA_CLAIM_GRANTED) { grant = NULL; }
                }

 report_caching:

                if (grant) {
                    /* Atomic attachment makes this member visible to break
                     * callbacks. Snapshot and publish its result under their
                     * lock so a stale reply assignment cannot undo a break. */
                    pthread_mutex_lock(&file_state->lock);
                    uint8_t granted_vfs = grant->claim.used;

                    open_file->grant                  = grant;
                    open_file->caching_file_state     = file_state;
                    open_file->caching_lease_inserted = true;
                    /* Link the open's share claim to its own cache grant: a hard
                     * share conflict against this open may park on own_cache's H
                     * break instead of denying (the batch escape, R8).  Replaces
                     * the old own_lease_key three-arm matching. */
                    if (open_file->share_lease_inserted) {
                        chimera_smb_share_claim(open_file)->own_cache = grant;
                    }
                    /* If this open coalesced onto a grant whose lease is currently
                     * mid-break, the client is told its lease state but with
                     * BREAK_IN_PROGRESS set (MS-SMB2 3.3.5.9.11: a lease-key re-open
                     * during a break succeeds and reports the break is underway). */
                    open_file->lease_flags =
                        (grant->claim.break_state == CHIMERA_CLAIM_BREAK_BREAKING)
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
                    pthread_mutex_unlock(&file_state->lock);
                } else {
                    /* No caching grant taken.  A bare RqLs lease is still a lease
                     * (report OPLOCK_LEVEL_LEASE and echo key/epoch in the response);
                     * a legacy open with no oplock keeps its NONE defaults. */
                    chimera_vfs_state_put(vfs_state, file_state);
                    file_state = NULL;
                    open_file->oplock_level = SMB2_OPLOCK_LEVEL_NONE;
                    if (via_rqls) {
                        /* No caching state was established (granted NONE), so the
                         * v2 epoch does NOT advance -- it is echoed unchanged.
                         * The epoch increments only when a lease actually holds
                         * or changes caching bits (MS-SMB2 3.3.5.9.11;
                         * smb2.lease.lease-epoch: a lease capped to NONE behind a
                         * byte-range lock keeps its requested epoch). */
                        if (request->create.rqls.is_v2) {
                            open_file->lease_epoch = request->create.rqls.epoch;
                        }
                        open_file->lease_state  = 0;
                        open_file->oplock_level = SMB2_OPLOCK_LEVEL_LEASE;
                    }
                }
            }
        }
    }

    /* MS-SMB2 3.3.5.9.9: FILE_OPEN_REQUIRING_OPLOCK is an atomic "open only if I
     * am granted the oplock/lease" request.  When the requested caching could
     * not be granted (the effective oplock is NONE), the server MUST close the
     * open and fail the CREATE with STATUS_CANNOT_BREAK_OPLOCK (#1016).  The open
     * holds its share + caching leases and VFS handle by this point but is not
     * yet hashed into the tree or durable-registered, so drain its leases here
     * and return NULL with force_close_status set; the caller releases the VFS
     * handle and completes the request, matching the AppInstanceId reject
     * contract in gen_open_file. */
    if ((request->create.create_options & SMB2_FILE_OPEN_REQUIRING_OPLOCK) &&
        type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE) {
        bool got_cache =
            (open_file->oplock_level == SMB2_OPLOCK_LEVEL_LEASE)
            ? (open_file->lease_state != 0)
            : (open_file->oplock_level != SMB2_OPLOCK_LEVEL_NONE);

        if (!got_cache) {
            chimera_smb_open_file_drain_locks(thread, open_file);
            open_file->handle = NULL;
            chimera_smb_open_file_free(thread, open_file);
            request->create.force_close_status =
                SMB2_STATUS_CANNOT_BREAK_OPLOCK;
            return NULL;
        }
    }

    /* Propagate delete-on-close to the VFS handle so the file is
    * removed when the last reference (opencnt) drops to zero. */
    if (delete_on_close && oh) {
        struct chimera_smb_namespace_registry *registry = &thread->shared->namespace_registry;
        if (!chimera_smb_doc_mutation_begin(registry, oh->fh, oh->fh_len, NULL)) {
            chimera_smb_open_file_drain_locks(thread, open_file);
            open_file->handle = NULL;
            chimera_smb_open_file_free(thread, open_file);
            request->create.force_close_status = SMB2_STATUS_FILE_NOT_AVAILABLE;
            return NULL;
        }
        chimera_vfs_set_delete_on_close(thread->vfs_thread, oh,
                                        parent_fh, parent_fh_len,
                                        name, name_len,
                                        &request->session_handle->session->cred);
        chimera_smb_doc_mutation_end(registry);

        /* An open created with FILE_DELETE_ON_CLOSE is a pending namespace
         * mutation: recall every OTHER holder's HANDLE cache (RqLs RH -> R) so a
         * peer that cached the open handle re-validates against the impending
         * removal (smb2.lease.unlink: open + delete-on-close + close).  The
         * operating open's own lease is spared.  (The deferred removal itself runs
         * at last-close through chimera_vfs_remove_at, whose io_recall parks until
         * the break drains; firing the recall here makes the break visible as soon
         * as the delete intent is registered.) */
        if (type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE) {
            chimera_smb_break_caching_for_namespace(request, open_file);
        }
    }

    /* Durable / persistent handle grant.  MS-SMB2 3.3.5.9: a durable handle is
     * granted only when the open also holds a batch oplock or a lease that
     * includes HANDLE caching — otherwise a survived handle could not be safely
     * reclaimed.  oplock_level / lease_state were set by the caching-lease block
     * above. */
    if (type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE && tree->share) {
        chimera_smb_create_grant_durable(request, open_file);
    }

    /* Record which CREATE contexts the client supplied BEFORE the open becomes
     * findable, because the replay lookup keys on it.
     *
     * chimera_smb_create_guid_replay's live-open arm matches a replayed DH2Q
     * create against an existing open by (ctx_present_mask & DH2Q) plus the
     * create_guid.  It used to be stamped only when the reply was marshaled
     * (chimera_smb_create_reply), which leaves a window: from the HASH_ADD
     * below until the reply, the open is hashed, live and replay-eligible, has
     * already been dropped from tree->pending_creates, and is NOT CREATE_PENDING
     * (it did not park) -- so a replay arriving in that window missed all three
     * lookups and opened a second handle instead of returning the first.  The
     * window is not narrow: the reply is emitted only once every request in the
     * compound has completed, and an EA-carrying create issues async setxattrs
     * inside it.  A compound whose reply is abandoned (connection recycled or
     * closing) never stamped it at all, so the open stayed unmatchable for its
     * whole life.  Stamping here makes the lookup key and the open's
     * findability become true at the same instant. */
    open_file->ctx_present_mask = request->create.ctx_present_mask;
    if (open_file->oplock_level == SMB2_OPLOCK_LEVEL_LEASE) {
        chimera_smb_lease_key_attach(request, open_file, oh);
    }

    if (type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE) {
        chimera_smb_abort_if(!chimera_smb_open_namespace_bind(open_file, open_file->handle,
            tree->fh, tree->fh_len), "invalid namespace identity at CREATE");
        chimera_smb_open_namespace_attach(thread, open_file);
    }

    open_file_bucket = open_file->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    pthread_mutex_lock(&tree->open_files_lock[open_file_bucket]);

    HASH_ADD(hh, tree->open_files[open_file_bucket], file_id, sizeof(open_file->file_id), open_file);

    pthread_mutex_unlock(&tree->open_files_lock[open_file_bucket]);

    /* Register the durable handle in the shared registry so it survives a
     * disconnect and can be reclaimed on reconnect.  Done outside the bucket
     * lock to keep the bucket -> registry lock order (see smb_durable.c). */
    if (open_file->durable_flags) {
        chimera_smb_create_register_durable(request, open_file);
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
    struct chimera_smb_request    *request,
    struct chimera_smb_open_file  *open_file,
    struct chimera_vfs_file_state *file_state,
    uint8_t                        held_granted,
    uint8_t                        held_denied)
{
    /* Drop the transient truncate-write grant, and with it the ShareAccess an
     * attribute-only truncating open asserted only to be arbitrated: the
     * handle holds only the access it requested and denies only what such a
     * handle may deny, so it must not block a later reader.  Shrink is the
     * core's in-place downgrade verb (never conflicts; pumps waiters). */
    if (request->create.overwrite_pending) {
        request->create.gen_held_granted     = held_granted;
        request->create.gen_held_denied      = held_denied;
        request->create.overwrite_share_held = 1;
    } else if (held_granted != chimera_smb_share_claim(open_file)->used ||
               held_denied != chimera_smb_share_claim(open_file)->denied) {
        chimera_vfs_claim_shrink(file_state, chimera_smb_share_claim(open_file),
                                 held_granted,
                                 held_denied);
    }

    open_file->share_file_state     = file_state;
    open_file->share_lease_inserted = true;
} /* chimera_smb_create_finish_share_grant */

static void
chimera_smb_create_share_park_cb(
    enum chimera_vfs_claim_result            result,
    struct chimera_vfs_claim                *claim,
    const struct chimera_vfs_claim_conflict *conflict,
    void                                    *private_data)
{
    struct chimera_smb_request       *request    = private_data;
    struct chimera_server_smb_thread *thread     = request->compound->thread;
    struct chimera_vfs_thread        *vfs_thread = thread->vfs_thread;
    struct chimera_vfs_state         *vfs_state  = vfs_thread->vfs->vfs_state;
    struct chimera_smb_open_file     *open_file  = request->create.gen_parked_open;
    struct chimera_vfs_file_state    *file_state = request->create.gen_parked_fs;

    (void) claim;
    if (conflict && conflict->admission_fenced) {
        request->create.force_close_status = SMB2_STATUS_FILE_NOT_AVAILABLE;
    }

    /* The vfs share-acquire ticket may resolve on ANY thread (whichever closed
     * the holder or acked the break).  The CREATE response's iovecs are
     * thread-local, so stash the result and bounce to the parked open's owning
     * thread, where chimera_smb_create_share_resume_doorbell completes it.  An
     * already-on-thread resolution still goes through the doorbell (one extra
     * hop); correctness over a fast path. */
    request->create.gen_resume_result = result;

    pthread_mutex_lock(&thread->lease_break_lock);
    request->create.gen_resume_next = thread->share_resume_head;
    thread->share_resume_head       = request;
    pthread_mutex_unlock(&thread->lease_break_lock);

    evpl_ring_doorbell(&thread->lease_resume_doorbell);

    (void) vfs_thread;
    (void) vfs_state;
    (void) open_file;
    (void) file_state;
} /* chimera_smb_create_share_park_cb */

/* Finish a share-park CREATE on its OWN thread (driven by the resume doorbell).
 * GRANTED: the holder closed and freed the conflict -> hold the share and finish
 * the open.  DENIED: the holder kept its handle (acked a dir-lease/oplock break
 * without closing) -> SHARING_VIOLATION. */
static void
chimera_smb_create_share_park_finish(
    struct chimera_smb_request   *request,
    enum chimera_vfs_claim_result result)
{
    struct chimera_server_smb_thread *thread     = request->compound->thread;
    struct chimera_vfs_thread        *vfs_thread = thread->vfs_thread;
    struct chimera_vfs_state         *vfs_state  = vfs_thread->vfs->vfs_state;
    struct chimera_smb_open_file     *open_file  = request->create.gen_parked_open;
    struct chimera_vfs_file_state    *file_state = request->create.gen_parked_fs;

    request->create.gen_parked = 0;

    if (result == CHIMERA_CLAIM_GRANTED) {
        /* The holder closed; the share reservation is now held. */
        chimera_smb_create_finish_share_grant(request, open_file, file_state,
                                              request->create.gen_held_granted,
                                              request->create.gen_held_denied);
        struct chimera_vfs_open_handle *handle = open_file->handle;
        open_file = chimera_smb_create_after_share(request, open_file);
        /* after_share hashed the open, so drop the in-flight registration only
         * now: a replay is covered by open_files[] from here on, with no window
         * in which neither lookup can see this create. */
        chimera_smb_create_pending_unregister(request);
        if (!open_file) {
            chimera_smb_create_release_handle(vfs_thread, handle);
            chimera_smb_create_release_parent(request);
            chimera_smb_complete_request(request, request->create.force_close_status);
            return;
        }
        request->create.gen_finish_cb(request, open_file);
        return;
    }

    chimera_smb_create_pending_unregister(request);

    /* DENIED: the holder acked its oplock / dir-lease break but kept the handle
     * open, so the share conflict stands. */
    chimera_vfs_state_put(vfs_state, file_state);
    if (open_file->handle) {
        chimera_vfs_release(vfs_thread, open_file->handle);
        open_file->handle = NULL;
    }
    chimera_smb_open_file_free(thread, open_file);
    chimera_smb_create_release_parent(request);
    chimera_smb_complete_request(request,
        request->create.force_close_status == SMB2_STATUS_FILE_NOT_AVAILABLE ?
        SMB2_STATUS_FILE_NOT_AVAILABLE : SMB2_STATUS_SHARING_VIOLATION);
} /* chimera_smb_create_share_park_finish */

/* Abandon a CREATE that is parked on a share-acquire ticket because its
 * connection is being torn down (chimera_smb_async_interim_drain): cancel the
 * ticket so the VFS pump can never resume into a dead request, then release the
 * half-built open exactly as the DENIED resume does.  No reply is sent -- the
 * connection is gone.  Runs on the conn's own thread. */
void
chimera_smb_create_abandon_share_park(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread     = request->compound->thread;
    struct chimera_vfs_thread        *vfs_thread = thread->vfs_thread;
    struct chimera_vfs_state         *vfs_state  = vfs_thread->vfs->vfs_state;
    struct chimera_smb_open_file     *open_file  = request->create.gen_parked_open;
    struct chimera_vfs_file_state    *file_state = request->create.gen_parked_fs;

    request->create.gen_parked = 0;

    /* If the ticket had already fired (its resume is queued on a thread's resume
     * doorbell) the cancel returns false and that resume still owns the open:
     * leave the teardown to it. */
    if (!chimera_vfs_claim_cancel(vfs_state, &request->create.gen_ticket)) {
        return;
    }

    chimera_smb_create_pending_unregister(request);
    chimera_vfs_state_put(vfs_state, file_state);
    if (open_file) {
        if (open_file->handle) {
            chimera_vfs_release(vfs_thread, open_file->handle);
            open_file->handle = NULL;
        }
        chimera_smb_open_file_free(thread, open_file);
        request->create.gen_parked_open = NULL;
    }
    chimera_smb_create_release_parent(request);
    chimera_smb_complete_request(request, SMB2_STATUS_CANCELLED);
} /* chimera_smb_create_abandon_share_park */

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

    /* A lease key is bound to exactly one file per client (MS-SMB2 3.3.5.9.8).
     * The file-create path enforces this in chimera_smb_create_open_finish, but
     * the directory (mkdir) path completes here without passing through it, so a
     * client opening a directory under a lease key already held on another file
     * would slip past the check (smb2.lease.request opens fname2 as a directory
     * under LEASE1 and expects STATUS_INVALID_PARAMETER). */
    /* Gated on leasing actually being advertised: MS-SMB2 3.3.5.9.8 conditions
     * RqLs processing -- and with it the lease-key binding -- on the server
     * supporting leases.  With smb_leases off the context is answered with a
     * bare, cacheless RqLs reply and binds nothing, so enforcing the binding
     * would refuse an open no lease was ever granted for.  (Found by replaying
     * the spec corpus: the model made the same mistake, and Samba with
     * `leases = no` ignores the context outright.) */
    if (request->compound->thread->shared->config.leases &&
        (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) &&
        chimera_smb_lease_key_conflict(request,
                                               oh->fh, oh->fh_len)) {
        chimera_smb_create_release_handle(vfs_thread, oh);
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
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
    chimera_smb_create_finish_with_eas(request, open_file);

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

static inline int
chimera_smb_create_access_deny_transient(
    struct chimera_smb_request     *request,
    const struct chimera_vfs_attrs *attr);

static void
chimera_smb_create_access_retry_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

static void
chimera_smb_create_share_retry_cb(
    struct evpl       *evpl,
    struct evpl_timer *timer);

/* A directory-create collided with an existing entry: if that entry is a
 * symbolic link, SMB stops on it (STATUS_STOPPED_ON_SYMLINK) rather than
 * reporting a name collision (MS-SMB2 3.3.5.9).  Only reached on the already-
 * failing EEXIST path, so the extra lookup never burdens a successful mkdir. */
static inline void
chimera_smb_create_collision_symlink_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;
    uint32_t                    status  = SMB2_STATUS_OBJECT_NAME_COLLISION;

    if (error_code == CHIMERA_VFS_OK && attr &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && S_ISLNK(attr->va_mode)) {
        status = SMB2_STATUS_STOPPED_ON_SYMLINK;
    }

    chimera_smb_create_release_parent(request);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_create_collision_symlink_cb */

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
        unsigned int reopen_flags = CHIMERA_VFS_OPEN_DIRECTORY;

        if (!(request->create.create_options & SMB2_FILE_OPEN_REPARSE_POINT)) {
            reopen_flags |= CHIMERA_VFS_OPEN_STOP_SYMLINK;
        }

        if (error_code == CHIMERA_VFS_EEXIST &&
            request->create.create_disposition == SMB2_FILE_OPEN_IF) {
            /* Directory already exists — fall through to open it (stopping if
             * the existing entry is a symbolic link). */
            chimera_vfs_open_at(
                vfs_thread,
                &request->session_handle->session->cred,
                request->create.parent_handle,
                request->create.name,
                request->create.name_len,
                reopen_flags,
                &request->create.set_attr,
                CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT |
                CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_BTIME,
                0,
                0,
                chimera_smb_create_open_at_callback,
                request);
            return;
        }

        /* A FILE_CREATE collision may actually be a symbolic link — SMB stops
         * on it rather than reporting a name collision.  Probe the colliding
         * entry's mode (only on this already-failing path). */
        if (error_code == CHIMERA_VFS_EEXIST &&
            !(request->create.create_options & SMB2_FILE_OPEN_REPARSE_POINT)) {
            chimera_vfs_lookup_at(
                vfs_thread,
                &request->session_handle->session->cred,
                request->create.parent_handle,
                request->create.name,
                request->create.name_len,
                CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE,
                0,
                chimera_smb_create_collision_symlink_cb,
                request);
            return;
        }

        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request,
                                     smb_create_bound_missing(request, error_code) ?
            SMB2_STATUS_INVALID_PARAMETER : chimera_smb_create_error_status(error_code));
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

/* Register a named-stream open's file-level DELETE share reservation against
 * the BASE file's state.  Stream R/W share modes are per-stream (the open_file's
 * own share_lease, keyed by the stream fh); DELETE is a file-level property
 * (smb2.streams.delete): a stream opened without FILE_SHARE_DELETE must block
 * the base file's deletion.  Even a stream that shares delete registers an inert
 * (0,0) entry so the base state knows it is still referenced and a base delete
 * can defer.  Returns SMB2_STATUS_SUCCESS, or SMB2_STATUS_SHARING_VIOLATION if
 * the base file's existing share state forbids this stream's delete intent. */
static uint32_t
chimera_smb_stream_register_base_delete(
    struct chimera_smb_request     *request,
    struct chimera_smb_open_file   *open_file,
    struct chimera_vfs_open_handle *base_oh)
{
    struct chimera_server_smb_thread *thread    = request->compound->thread;
    struct chimera_vfs_state         *vfs_state = thread->vfs_thread->vfs->vfs_state;
    struct chimera_vfs_file_state    *file_state;
    struct chimera_claim_owner        owner;
    struct chimera_vfs_claim_conflict conflict;
    enum chimera_vfs_claim_result     result;
    uint32_t                          da = open_file->desired_access;
    uint32_t                          sa = open_file->share_access;
    uint8_t                           granted = 0, denied = 0;

    if (da & (SMB2_DELETE | SMB2_GENERIC_ALL | SMB2_MAXIMUM_ALLOWED)) {
        granted = CHIMERA_CLAIM_D;
    }
    if (!(sa & SMB2_FILE_SHARE_DELETE)) {
        denied = CHIMERA_CLAIM_D;
    }

    file_state = chimera_vfs_state_get(vfs_state, base_oh->fh, base_oh->fh_len,
                                       base_oh->fh_hash, true);
    if (!file_state) {
        /* No state available: nothing to enforce, treat as granted. */
        return SMB2_STATUS_SUCCESS;
    }

    memset(&owner, 0, sizeof(owner));
    owner.proto      = CHIMERA_CLAIM_PROTO_SMB2;
    owner.client_key = request->session_handle->session->client_key;
    owner.owner_lo   = open_file->file_id.pid;
    owner.owner_hi   = open_file->file_id.vid;

    chimera_vfs_claim_init_smb_open(chimera_smb_base_share_claim(open_file),
                                    granted, denied, &owner);
    chimera_smb_base_share_claim(open_file)->op_handle  = base_oh;
    chimera_smb_base_share_claim(open_file)->cb_private = open_file;
    chimera_smb_base_share_claim(open_file)->policy_tag = open_file->file_id.pid;
    open_file->base_access_owner = chimera_vfs_claim_access_owner_alloc(file_state, chimera_smb_base_share_claim(open_file));
    if (!open_file->base_access_owner) {
        chimera_vfs_state_put(vfs_state, file_state);
        return SMB2_STATUS_INSUFFICIENT_RESOURCES;
    }


    memset(&conflict, 0, sizeof(conflict));
    result = chimera_vfs_claim_try_acquire(vfs_state, file_state,
                                           chimera_smb_base_share_claim(open_file),
                                           &conflict);

    if (result != CHIMERA_CLAIM_GRANTED) {
        /* A pre-existing base opener forbids this stream's delete intent.  Fail
         * open rather than tear down the half-built stream open_file: the
         * blocking direction the conformance tests exercise (a stream blocking a
         * later base delete) is enforced by the reservation we would have added
         * here being absent only in this rare reverse-conflict case. */
        chimera_vfs_claim_access_owner_retire(open_file->base_access_owner);
        chimera_vfs_claim_access_owner_put(open_file->base_access_owner);
    open_file->base_access_owner = NULL;
        chimera_vfs_state_put(vfs_state, file_state);
        return conflict.admission_fenced ? SMB2_STATUS_FILE_NOT_AVAILABLE : SMB2_STATUS_SUCCESS;
    }

    open_file->base_share_file_state     = file_state;
    open_file->base_share_lease_inserted = true;
    chimera_vfs_state_stream_holder_inc(file_state);
    return SMB2_STATUS_SUCCESS;
} /* chimera_smb_stream_register_base_delete */

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
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_vfs_thread      *vfs_thread = thread->vfs_thread;
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
            status = smb_create_bound_missing(request, error_code) ?
            SMB2_STATUS_INVALID_PARAMETER : chimera_smb_create_error_status(error_code);
        }
        chimera_vfs_release(vfs_thread, base_oh);
        request->create.base_oh = NULL;
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, status);
        return;
    }

    /* A named fork has its own lease identity; checking the base handle
     * rejects a valid same-key reopen and accepts reuse of a base-file key. */
    if (thread->shared->config.leases &&
        (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) &&
        chimera_smb_lease_key_conflict(request, stream_oh->fh, stream_oh->fh_len)) {
        chimera_smb_create_release_handle(vfs_thread, stream_oh);
        chimera_vfs_release(vfs_thread, base_oh);
        request->create.base_oh = NULL;
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    request->create.r_created      = stream_oh ? stream_oh->r_created : 0;
    request->create.r_is_directory = 0;

    /* A previously closed stream opener can leave deletion pending until its
     * last peer closes.  The base file has a separate pending-delete state. */
    if (!request->create.r_created) {
        struct chimera_vfs_state      *state = vfs_thread->vfs->vfs_state;
        struct chimera_vfs_file_state *file  = chimera_vfs_state_get(state,
                                                                     stream_oh->fh, stream_oh->fh_len, stream_oh->
                                                                     fh_hash, false);
        bool                           pending = file && chimera_vfs_state_is_delete_pending(file);

        if (file) {
            chimera_vfs_state_put(state, file);
        }
        if (pending) {
            chimera_smb_create_release_handle(vfs_thread, stream_oh);
            chimera_vfs_release(vfs_thread, base_oh);
            request->create.base_oh = NULL;
            chimera_smb_create_release_parent(request);
            chimera_smb_complete_request(request, SMB2_STATUS_DELETE_PENDING);
            return;
        }
    }

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

    /* Base-file admission already completed before registry publication. */
    chimera_smb_abort_if(!chimera_smb_open_namespace_bind(open_file, stream_oh,
        request->tree->fh, request->tree->fh_len), "invalid stream namespace identity");
    chimera_smb_open_namespace_attach(thread, open_file);


    request->create.r_open_file = open_file;

    open_file->granted_access = chimera_smb_create_granted_access(request, attr);
    open_file->maximal_access =
        chimera_vfs_access_check(attr, &request->session_handle->session->cred,
                                 CHIMERA_ACE_MASK_ALL);

    /* attr carries the base file's metadata with the stream's size/alloc. */
    chimera_smb_marshal_open_attrs(attr, true, &request->create.r_attrs);

    /* The stream owns I/O, but its base ACCESS claim still borrows this
     * handle as an actor identity. Keep that anchor until final claim drain. */
    if (open_file->base_access_owner) {
        open_file->base_handle = base_oh;
    } else {
        chimera_vfs_release(vfs_thread, base_oh);
    }
    request->create.base_oh = NULL;

    /*
     * A stream that did not exist before now does: MS-FSA 2.1.5.1 raises
     * FILE_NOTIFY_CHANGE_STREAM_NAME for it, and a client watching the
     * containing directory expects to be woken (WPTS
     * BVT_SMB2Basic_ChangeNotify_ChangeStreamName registers exactly that filter
     * on the directory, with WATCH_TREE, and then has a second client create a
     * stream on a file that already existed).
     *
     * This path returns through chimera_smb_create_finish_with_eas rather than
     * chimera_smb_create_open_finish, so it never reached the emit there and a
     * stream create notified nobody at all.
     *
     * STREAM_NAME alone: no directory entry appeared.  Raising FILE_ADDED as
     * the file-create path does would wake FILE_NOTIFY_CHANGE_FILE_NAME
     * watchers for a file that was already there.  The name reported is the
     * base file's -- the entry the directory actually holds -- since the stream
     * is not one of its entries.
     */
    if (request->create.r_created && request->create.parent_handle) {
        uint64_t skip_lo, skip_hi;
        bool     has_skip = chimera_smb_parent_lease_skip(
            open_file->parent_lease_key, &skip_lo, &skip_hi);

        smb_create_notify_lease(request,
                                      request->create.parent_handle->fh,
                                      request->create.parent_handle->fh_len,
                                      CHIMERA_VFS_NOTIFY_STREAM_NAME,
                                      request->create.name,
                                      request->create.name_len,
                                      NULL, 0,
                                      skip_lo, skip_hi, has_skip);
    }

    chimera_smb_create_release_parent(request);
    chimera_smb_create_finish_with_eas(request, open_file);
} /* chimera_smb_create_open_stream_callback */

/* A bound lease key may open only its existing stream. Discover that exact
 * fork without CREATE before any new stream can stamp shared base metadata. */
static void
chimera_smb_create_stream_lease_checked(
    enum chimera_vfs_error error, struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs *attr, void *private_data)
{
    struct chimera_smb_request *request = private_data;
    uint32_t disposition = request->create.create_disposition;
    uint32_t status = SMB2_STATUS_SUCCESS;
    if (error == CHIMERA_VFS_ENOENT && disposition != SMB2_FILE_OPEN &&
        disposition != SMB2_FILE_OVERWRITE) { status = SMB2_STATUS_INVALID_PARAMETER; }
    else if (error == CHIMERA_VFS_OK &&
        chimera_smb_lease_key_conflict(request, handle->fh, handle->fh_len)) {
        status = SMB2_STATUS_INVALID_PARAMETER;
    }
    else if (error == CHIMERA_VFS_OK && disposition == SMB2_FILE_CREATE) {
        status = SMB2_STATUS_OBJECT_NAME_COLLISION;
    }
    if (status != SMB2_STATUS_SUCCESS) {
        struct chimera_vfs_thread *thread = request->compound->thread->vfs_thread;
        if (handle) { chimera_smb_create_release_handle(thread, handle); }
        chimera_vfs_release(thread, request->create.base_oh);
        request->create.base_oh = NULL;
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, status);
        return;
    }
    chimera_smb_create_open_stream_callback(error, handle, attr, request);
}

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
        /* Gate 2: the feature is configured on, but this backend cannot do it.
         * Distinct from the gate in chimera_smb_create(), which fires when the
         * feature is off entirely -- both return OBJECT_NAME_INVALID, so the log
         * line is the only way to tell them apart after the fact.  Debug level:
         * the rejected name is client-controlled and clients probe stream paths
         * on essentially every file, so this must not be on by default. */
        chimera_smb_debug(
            "named streams: backend '%s' does not advertise CHIMERA_VFS_CAP_"
            "NAMED_STREAMS; rejecting stream CREATE '%.*s:%.*s'",
            base_oh->vfs_module->name,
            request->create.name_len, request->create.name,
            request->create.stream_name_len, request->create.stream_name);
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

    if (sflags & CHIMERA_VFS_OPEN_TRUNCATE) {
        request->create.overwrite_pending = 1;
        request->create.overwrite_attr    = request->create.set_attr;
        sflags                           &= ~CHIMERA_VFS_OPEN_TRUNCATE;
    }
    request->create.base_oh = base_oh;
    if (request->compound->thread->shared->config.leases &&
        (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) &&
        chimera_smb_lease_key_conflict(request, NULL, 0)) {
        chimera_vfs_open_stream(vfs_thread, &request->session_handle->session->cred,
            base_oh, request->create.stream_name, request->create.stream_name_len,
            0, NULL, CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME | CHIMERA_VFS_ATTR_ACL,
            chimera_smb_create_stream_lease_checked, request);
        return;
    }


    chimera_vfs_open_stream(
        vfs_thread,
        &request->session_handle->session->cred,
        base_oh,
        request->create.stream_name,
        request->create.stream_name_len,
        sflags,
        /* The create's FileAttributes (e.g. HIDDEN|ARCHIVE) live on the base
         * file, shared by all its streams.  Stamp them when the stream open
         * creates or overwrites the stream so a stream create reflects the
         * requested attributes on the base (smb2.streams.attributes2). */
        &request->create.set_attr,
        CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME | CHIMERA_VFS_ATTR_ACL,
        chimera_smb_create_open_stream_callback,
        request);
} /* chimera_smb_create_open_stream_chain */

/* STATUS_STOPPED_ON_SYMLINK must carry a SymbolicLinkErrorResponse body (MS-SMB2
 * 2.2.2.2.1) so the client can resolve the link and retry.  The target is read
 * back from a handle on the link (r_symlink_handle) and stashed for the error-
 * reply builder (chimera_smb_create_emit_symlink_error in smb.c); r_symlink_error
 * gates the emission.  On any failure we still return the bare error -- a missing
 * body costs the client only one extra resolve round-trip, never correctness. */
static void
chimera_smb_create_symlink_readlink_cb(
    enum chimera_vfs_error    error_code,
    int                       target_length,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;
    int                         i;

    if (request->create.r_symlink_handle) {
        chimera_vfs_release(vfs_thread, request->create.r_symlink_handle);
        request->create.r_symlink_handle = NULL;
    }
    chimera_vfs_release(vfs_thread, request->create.parent_handle);

    if (error_code == CHIMERA_VFS_OK && target_length > 0 &&
        target_length < CHIMERA_VFS_PATH_MAX) {
        /* SMB substitute names use backslash separators; a leading separator (or
         * its absence) sets the response's relative/absolute Flags. */
        request->create.r_symlink_relative =
            (request->create.r_symlink_target[0] != '/');
        for (i = 0; i < target_length; i++) {
            if (request->create.r_symlink_target[i] == '/') {
                request->create.r_symlink_target[i] = '\\';
            }
        }
        request->create.r_symlink_target_len = target_length;
        request->create.r_symlink_unparsed   = 0;  /* final component: no suffix */
        request->create.r_symlink_error      = 1;
    }

    chimera_smb_complete_request(request, SMB2_STATUS_STOPPED_ON_SYMLINK);
} /* chimera_smb_create_symlink_readlink_cb */

/* An intermediate (parent-path) component is a symbolic link: readlink it so the
 * STATUS_STOPPED_ON_SYMLINK reply carries the target AND the unparsed suffix
 * ("/" + create name), letting the client splice the link and retry.  Without
 * the body the client can only surface ELOOP -- so a create/stat/rmdir THROUGH a
 * symlinked directory would wrongly fail where POSIX follows the link. */
static void
chimera_smb_create_parent_symlink_readlink_cb(
    enum chimera_vfs_error    error_code,
    int                       target_length,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;
    int                         i;

    (void) attr;

    if (request->create.r_symlink_handle) {
        chimera_vfs_release(vfs_thread, request->create.r_symlink_handle);
        request->create.r_symlink_handle = NULL;
    }

    if (error_code == CHIMERA_VFS_OK && target_length > 0 &&
        target_length < CHIMERA_VFS_PATH_MAX) {
        request->create.r_symlink_relative =
            (request->create.r_symlink_target[0] != '/');
        for (i = 0; i < target_length; i++) {
            if (request->create.r_symlink_target[i] == '/') {
                request->create.r_symlink_target[i] = '\\';
            }
        }
        request->create.r_symlink_target_len = target_length;
        /* Unparsed suffix = "/" + create name, in UTF-16 bytes (the part of the
         * path after the parent-path symlink the server stopped on). */
        request->create.r_symlink_unparsed =
            (uint16_t) ((request->create.name_len + 1) * 2);
        request->create.r_symlink_error = 1;
    }

    chimera_smb_complete_request(request, SMB2_STATUS_STOPPED_ON_SYMLINK);
} /* chimera_smb_create_parent_symlink_readlink_cb */

/* Reopen the parent-path symlink (by the fh the lookup returned) to read its
 * target for the SymbolicLinkErrorResponse body. */
static void
chimera_smb_create_parent_symlink_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_complete_request(request, SMB2_STATUS_STOPPED_ON_SYMLINK);
        return;
    }

    request->create.r_symlink_handle = oh;

    chimera_vfs_readlink(
        vfs_thread,
        &request->session_handle->session->cred,
        oh,
        request->create.r_symlink_target,
        CHIMERA_VFS_PATH_MAX,
        0,
        chimera_smb_create_parent_symlink_readlink_cb,
        request);
} /* chimera_smb_create_parent_symlink_open_cb */

static void
chimera_smb_create_symlink_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    if (error_code != CHIMERA_VFS_OK) {
        /* Could not reopen the link to read its target -- return the bare error. */
        chimera_vfs_release(vfs_thread, request->create.parent_handle);
        chimera_smb_complete_request(request, SMB2_STATUS_STOPPED_ON_SYMLINK);
        return;
    }

    request->create.r_symlink_handle = oh;

    chimera_vfs_readlink(
        vfs_thread,
        &request->session_handle->session->cred,
        oh,
        request->create.r_symlink_target,
        CHIMERA_VFS_PATH_MAX,
        0,
        chimera_smb_create_symlink_readlink_cb,
        request);
} /* chimera_smb_create_symlink_open_cb */

/* A create stopped on a symbolic-link leaf (memfs returned ELOOP under the
 * STOP_SYMLINK open).  Reopen the link itself (NOFOLLOW) to read its target for
 * the SymbolicLinkErrorResponse body, then complete.  parent_handle is held
 * until the readlink callback releases it. */
static void
chimera_smb_create_stopped_on_symlink(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread *vfs_thread = request->compound->thread->vfs_thread;

    request->create.r_symlink_handle = NULL;

    /* open_at requires a (non-NULL) set_attr even for a pure open; this open
     * carries no CREATE flag so the attrs are not applied. */
    memset(&request->create.set_attr, 0, sizeof(request->create.set_attr));

    chimera_vfs_open_at(
        vfs_thread,
        &request->session_handle->session->cred,
        request->create.parent_handle,
        request->create.name,
        request->create.name_len,
        /* O_PATH|O_NOFOLLOW: open the link itself (memfs returns ELOOP for a bare
        * NOFOLLOW data open of a symlink leaf; PATH requests the link handle). */
        CHIMERA_VFS_OPEN_NOFOLLOW | CHIMERA_VFS_OPEN_PATH,
        &request->create.set_attr,
        CHIMERA_VFS_ATTR_FH,
        0,
        0,
        chimera_smb_create_symlink_open_cb,
        request);
} /* chimera_smb_create_stopped_on_symlink */

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
        if (error_code == CHIMERA_VFS_ELOOP) {
            /* Stopped on a symbolic-link leaf: read the link target for the
             * SymbolicLinkErrorResponse body before completing (the readlink
             * callback releases parent_handle). */
            chimera_smb_create_stopped_on_symlink(request);
            return;
        }
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, smb_create_bound_missing(request, error_code) ?
            SMB2_STATUS_INVALID_PARAMETER : chimera_smb_create_error_status(error_code));
        return;
    }

    /* The opened leaf is itself a symbolic link.  Unless the caller asked to
     * open the reparse point directly (FILE_OPEN_REPARSE_POINT), SMB does not
     * follow it on the server: return STATUS_STOPPED_ON_SYMLINK so the client
     * resolves the link and retries (MS-SMB2 2.2.2.2.1).  memfs returns the link
     * inode's attributes for a non-NOFOLLOW open of a final-component symlink. */
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && S_ISLNK(attr->va_mode) &&
        !(request->create.create_options & SMB2_FILE_OPEN_REPARSE_POINT)) {
        /* Read the link target for the error body, then complete (the callback
         * releases oh via r_symlink_handle and parent_handle). */
        request->create.r_symlink_handle = oh;
        chimera_vfs_readlink(
            vfs_thread,
            &request->session_handle->session->cred,
            oh,
            request->create.r_symlink_target,
            CHIMERA_VFS_PATH_MAX,
            0,
            chimera_smb_create_symlink_readlink_cb,
            request);
        return;
    }

    if (!request->create.has_stream && request->create.create_disposition == SMB2_FILE_CREATE &&
        chimera_smb_lease_key_conflict(request, NULL, 0)) {
        chimera_smb_create_release_handle(vfs_thread, oh);
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_COLLISION);
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

        /* A CREATE that loses a same-name race can observe a half-constructed
         * object: the passthrough backends (linux/io_uring) execute an SMB
         * (AUTH_ATTR) create as the server identity and apply the client's
         * ownership with a separate fchown, so the loser's open can see the
         * object still owned by the server -- the owner fast-path misses and
         * a mode-only evaluation denies rights the final owner holds (e.g.
         * smbtorture smb2.create.mkdir-dup asking SEC_RIGHTS_FILE_ALL).  When
         * a denial carries that exact signature (no explicit ACL, object owned
         * by the server identity, caller is someone else), re-fetch the
         * attributes and re-evaluate, a bounded number of times.  The retry
         * mask includes ATTR_ACL, which is never attr-cache-served, so each
         * retry reaches the backend and sees the racing creator's chown once
         * it lands.  A genuinely server-owned object still denies after the
         * budget, and the extra getattrs run only on an already-failing path. */
        if (access_status == SMB2_STATUS_ACCESS_DENIED &&
            request->create.access_retries < CHIMERA_SMB_CREATE_ACCESS_RETRIES &&
            chimera_smb_create_access_deny_transient(request, attr)) {
            request->create.access_retries++;
            request->create.access_retry_oh = oh;
            chimera_vfs_getattr(
                vfs_thread,
                &request->session_handle->session->cred,
                oh,
                CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT |
                CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_BTIME,
                chimera_smb_create_access_retry_callback,
                request);
            return;
        }

        if (access_status != SMB2_STATUS_SUCCESS) {
            chimera_vfs_release(vfs_thread, oh);
            chimera_vfs_release(vfs_thread, request->create.parent_handle);
            chimera_smb_complete_request(request, access_status);
            return;
        }

        /* MS-FSA 2.1.5.1.2 "Check Access to an Existing File" (MS-FSA_R53 /
         * R2422 / R54): the FILE_ATTRIBUTE_READONLY DOS bit is a write/delete
         * fence separate from the DACL evaluated above.  For an existing
         * non-directory file marked READONLY: a DesiredAccess asking for
         * WRITE_DATA/APPEND_DATA is denied with ACCESS_DENIED, and a
         * FILE_DELETE_ON_CLOSE with CANNOT_DELETE (a read-only file cannot be
         * deleted).  Truncating dispositions are gated earlier, so this covers
         * the OPEN / OPEN_IF paths.  Skipped for a durable reconnect, whose
         * access was settled at the original open, and when THIS open just
         * created the file (oh->r_created): the readonly attribute set by a
         * create does not fence that same creating handle from writing
         * (smb2.durable-open.read-only).  FSAModel OpenFileTestCase S44/S46
         * (write/append) and S48 (delete-on-close).  Only the plain OPEN /
         * OPEN_IF dispositions are fenced: a truncating disposition
         * (SUPERSEDE/OVERWRITE/OVERWRITE_IF) replaces the file and the READONLY
         * bit it carries is the NEW state being applied, not a pre-existing
         * constraint (smb2.openattr trunc-opens a file to set READONLY); those
         * are gated separately. */
        if (!request->create.reconnect &&
            !oh->r_created &&
            request->create.create_disposition != SMB2_FILE_SUPERSEDE &&
            request->create.create_disposition != SMB2_FILE_OVERWRITE &&
            request->create.create_disposition != SMB2_FILE_OVERWRITE_IF &&
            !S_ISDIR(attr->va_mode) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) &&
            (attr->va_dos_attributes & SMB2_FILE_ATTRIBUTE_READONLY)) {

            if (request->create.create_options & SMB2_FILE_DELETE_ON_CLOSE) {
                chimera_vfs_release(vfs_thread, oh);
                chimera_vfs_release(vfs_thread, request->create.parent_handle);
                chimera_smb_complete_request(request, SMB2_STATUS_CANNOT_DELETE);
                return;
            }

            if (request->create.desired_access &
                (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA)) {
                chimera_vfs_release(vfs_thread, oh);
                chimera_vfs_release(vfs_thread, request->create.parent_handle);
                chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
                return;
            }
        }
    }

    /* A lease key may be bound to only one file per client (MS-SMB2 3.3.5.9.8).
     * If this client already holds the requested lease key on a different file,
     * reject the open with STATUS_INVALID_PARAMETER (smb2.lease.request /
     * duplicate_create / duplicate_open).  A re-open of the same file under the
     * key coalesces and is not a conflict. */
    /* Gated on leasing actually being advertised: MS-SMB2 3.3.5.9.8 conditions
     * RqLs processing -- and with it the lease-key binding -- on the server
     * supporting leases.  With smb_leases off the context is answered with a
     * bare, cacheless RqLs reply and binds nothing, so enforcing the binding
     * would refuse an open no lease was ever granted for.  (Found by replaying
     * the spec corpus: the model made the same mistake, and Samba with
     * `leases = no` ignores the context outright.) */
    if (!request->create.has_stream && request->compound->thread->shared->config.leases &&
        (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) &&
        chimera_smb_lease_key_conflict(request,
                                               oh->fh, oh->fh_len)) {
        chimera_vfs_release(vfs_thread, oh);
        chimera_vfs_release(vfs_thread, request->create.parent_handle);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* The resolved file is delete-pending: its deletion was deferred because a
     * named stream still holds it open, so a name open of the base file or any
     * of its streams is answered STATUS_DELETE_PENDING (smb2.streams.delete).
     * A fresh create (r_created) cannot be delete-pending.  state_get with
     * create=false is a cheap miss for any file that has no live state. */
    if (!(oh->r_created)) {
        struct chimera_vfs_state      *vfs_state = vfs_thread->vfs->vfs_state;
        struct chimera_vfs_file_state *fs        =
            chimera_vfs_state_get(vfs_state, oh->fh, oh->fh_len, oh->fh_hash, false);

        if (fs) {
            bool pending = chimera_vfs_state_is_delete_pending(fs);
            chimera_vfs_state_put(vfs_state, fs);
            if (pending) {
                chimera_vfs_release(vfs_thread, oh);
                chimera_vfs_release(vfs_thread, request->create.parent_handle);
                chimera_smb_complete_request(request, SMB2_STATUS_DELETE_PENDING);
                return;
            }
        }
    }

    /* "DNAME::$DATA" explicitly names the default data fork of the target.  A
     * directory has no data fork, so reject it now that the open has revealed
     * the target is a directory: NOT_A_DIRECTORY when the caller also requested
     * FILE_DIRECTORY_FILE, FILE_IS_A_DIRECTORY otherwise (smb2.streams.dir).
     * This fires only for the explicit "::$DATA" form on a directory, a path
     * that otherwise opens the directory as a plain file. */
    if (request->create.explicit_data_fork && S_ISDIR(attr->va_mode)) {
        chimera_smb_create_release_handle(vfs_thread, oh);
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request,
                                     (request->create.create_options & SMB2_FILE_DIRECTORY_FILE)
                                     ? SMB2_STATUS_NOT_A_DIRECTORY
                                     : SMB2_STATUS_FILE_IS_A_DIRECTORY);
        return;
    }

    /* "DNAME::$INDEX_ALLOCATION" names a directory's index stream -- the
     * directory itself, so it is valid only when the target really is a
     * directory; reject it on a file (WPTS MS-FSAModel directory-index-stream
     * cases). */
    if (request->create.explicit_index_fork && !S_ISDIR(attr->va_mode)) {
        chimera_smb_create_release_handle(vfs_thread, oh);
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, SMB2_STATUS_NOT_A_DIRECTORY);
        return;
    }

    /* FILE_NON_DIRECTORY_FILE on a target that resolved to a directory is
     * STATUS_FILE_IS_A_DIRECTORY (MS-SMB2 3.3.5.9 / MS-FSA 2.1.5.1).  chimera
     * only translates FILE_DIRECTORY_FILE into the VFS open, so without this an
     * open of a directory with the non-directory option silently succeeded.
     * Applies only when the base object itself is the target: a named-stream
     * open ("dir:stream:$DATA") opens a data fork, so the non-directory option
     * is about the stream, not the directory carrying it (FSA ADS-on-dir). */
    if ((request->create.create_options & SMB2_FILE_NON_DIRECTORY_FILE) &&
        !request->create.has_stream && S_ISDIR(attr->va_mode)) {
        chimera_smb_create_release_handle(vfs_thread, oh);
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_IS_A_DIRECTORY);
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

    /* MS-SMB2 2.2.13.2 AlSi: for creates, overwrites, and supersedes (not
     * plain opens of existing files), report max(backend_alloc, requested_AlSi).
     * Directories never report an AlSi reservation.
     * r_created covers newly created files; the disposition checks cover
     * truncated/replaced cases where the file existed but content was reset. */
    bool alsi_applies = request->create.r_created ||
        request->create.create_disposition == SMB2_FILE_OVERWRITE ||
        request->create.create_disposition == SMB2_FILE_OVERWRITE_IF ||
        request->create.create_disposition == SMB2_FILE_SUPERSEDE;

    if (request->create.alsi_alloc_size > 0 &&
        !request->create.r_is_directory && alsi_applies &&
        request->create.r_attrs.smb_alloc_size < request->create.alsi_alloc_size) {
        request->create.r_attrs.smb_alloc_size = request->create.alsi_alloc_size;
    }

    /* Activate the park-on-conflict-break path: if gen_open_file parks on a
     * handle-caching break (a batch oplock, or an SMB3 directory lease whose
     * HANDLE must be relinquished for a conflicting open), the share-park resume
     * (chimera_smb_create_share_park_cb) finishes the open via this callback once
     * the holder closes (GRANTED) or denies with SHARING_VIOLATION if it keeps
     * the handle.  Without it a conflict resolves synchronously to
     * SHARING_VIOLATION and never waits for the holder to close. */
    request->create.gen_finish_cb = chimera_smb_create_open_finish;

    request->create.gen_share_retry = 0;
    open_file                       = chimera_smb_create_gen_open_file_normal(request,
                                                                              request->create.parent_handle->fh,
                                                                              request->create.parent_handle->fh_len,
                                                                              request->create.name,
                                                                              request->create.name_len,
                                                                              request->create.create_options &
                                                                              SMB2_FILE_DELETE_ON_CLOSE,
                                                                              S_ISDIR(attr->va_mode),
                                                                              oh);

    if (request->create.gen_parked) {
        /* Parked on a batch-oplock break; the park cb finishes the open. */
        return;
    }

    if (!open_file) {
        /* Share conflict against a durable holder whose owning connection is
         * mid-disconnect: it will park + yield once that disconnect runs, so
         * re-attempt the open completion on a short timer (holding oh + parent)
         * rather than failing.  gen_share_retry was set by
         * chimera_smb_create_gen_open_file_normal on this attempt and reflects the
         * latest classification: CONFIRMED (disconnect observed, or already parked)
         * gets the full budget; SPECULATIVE (a durable holder whose disconnect is
         * not yet observable) gets the short budget -- if its FIN is read on a
         * later probe the classification flips to CONFIRMED and the full budget
         * then applies.  The budget lapses into the original SHARING_VIOLATION if
         * the holder never yields (persistent or genuinely live -- gen_share_retry
         * would be NONE in that case). */
        uint16_t share_budget =
            (request->create.gen_share_retry == CHIMERA_SMB_DURABLE_YIELD_CONFIRMED)
            ? CHIMERA_SMB_CREATE_SHARE_RETRY_MAX
            : CHIMERA_SMB_CREATE_SHARE_SPEC_RETRY_MAX;

        if (request->create.gen_share_retry != CHIMERA_SMB_DURABLE_YIELD_NONE &&
            request->create.share_conflict_retries < share_budget) {
            request->create.share_conflict_retries++;
            request->create.share_conflict_oh = oh;
            /* Deferred without a hashed open, as for the share park above: keep
             * the create_guid visible to a replay for the length of the retry. */
            chimera_smb_create_pending_register(request);
            evpl_add_oneshot_timer(request->compound->thread->evpl,
                                   &request->async.timer,
                                   chimera_smb_create_share_retry_cb,
                                   CHIMERA_SMB_CREATE_SHARE_RETRY_INTERVAL_US);
            return;
        }

        chimera_smb_create_pending_unregister(request);
        chimera_smb_create_release_handle(vfs_thread, oh);
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, request->create.force_close_status);
        return;
    }

    /* The open is hashed now (chimera_smb_create_after_share), so any replay of
     * it is served by the CREATE_PENDING / live-open scan from here on. */
    chimera_smb_create_pending_unregister(request);

    chimera_smb_create_open_finish(request, open_file);
} /* chimera_smb_create_open_at_callback */

/* Completion of the access-retry getattr (see the transient-denial comment in
 * chimera_smb_create_open_at_callback): re-enter the open completion with the
 * fresh attributes.  The re-entry re-runs the access check -- passing once the
 * racing creator's chown has landed, retrying while budget remains, and issuing
 * the final denial otherwise.  The directory pre/post attrs are not consumed by
 * the open completion path, so they are not re-supplied. */
static void
chimera_smb_create_access_retry_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request     *request = private_data;
    struct chimera_vfs_open_handle *oh      = request->create.access_retry_oh;

    request->create.access_retry_oh = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_vfs_thread *vfs_thread =
            request->compound->thread->vfs_thread;

        chimera_vfs_release(vfs_thread, oh);
        chimera_vfs_release(vfs_thread, request->create.parent_handle);
        chimera_smb_complete_request(request,
                                     chimera_smb_create_error_status(error_code));
        return;
    }

    chimera_smb_create_open_at_callback(CHIMERA_VFS_OK, oh,
                                        &request->create.set_attr, attr,
                                        NULL, NULL, request);
} /* chimera_smb_create_access_retry_callback */

/* Completion of the share-conflict-retry getattr: the timer fired and re-fetched
 * the (unchanged) attributes so the open completion can be re-entered with a live
 * attr pointer, exactly as the access-retry path does.  Re-runs the share
 * acquire; the conflicting durable holder's disconnect has by now (or on a later
 * retry) parked + yielded it, so the open is granted.  Shares async.timer with
 * the access-retry/park machinery -- only one retry path is active at a time. */
static void
chimera_smb_create_share_retry_getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request     *request = private_data;
    struct chimera_vfs_open_handle *oh      = request->create.share_conflict_oh;

    request->create.share_conflict_oh = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_vfs_thread *vfs_thread =
            request->compound->thread->vfs_thread;

        chimera_vfs_release(vfs_thread, oh);
        chimera_vfs_release(vfs_thread, request->create.parent_handle);
        chimera_smb_complete_request(request,
                                     chimera_smb_create_error_status(error_code));
        return;
    }

    chimera_smb_create_open_at_callback(CHIMERA_VFS_OK, oh,
                                        &request->create.set_attr, attr,
                                        NULL, NULL, request);
} /* chimera_smb_create_share_retry_getattr_cb */

/* Share-conflict retry timer: re-fetch attributes (so the open completion has a
 * live attr) and re-drive the open.  Runs on the request's own conn thread. */
static void
chimera_smb_create_share_retry_cb(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_smb_request     *request = (struct chimera_smb_request *)
        ((char *) timer - offsetof(struct chimera_smb_request, async.timer));
    struct chimera_vfs_thread      *vfs_thread =
        request->compound->thread->vfs_thread;
    struct chimera_vfs_open_handle *oh = request->create.share_conflict_oh;

    (void) evpl;

    chimera_vfs_getattr(vfs_thread,
                        &request->session_handle->session->cred,
                        oh,
                        CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT |
                        CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_BTIME,
                        chimera_smb_create_share_retry_getattr_cb,
                        request);
} /* chimera_smb_create_share_retry_cb */

/* Break-deadline for a CREATE parked on a lease break (MS-SMB2 3.3.5.9 / the
 * oplock-break timeout): the holder never acknowledged within the deadline, so
 * forcibly revoke the still-breaking holder(s) and complete the pending open with
 * its already-acquired lease.  Runs on the open's own conn thread (the timer is
 * armed on that thread's evpl).  An ack that resumes the open first removes this
 * timer. */
static void chimera_smb_create_finish_deferred_dir_break(
    struct chimera_smb_request *request);

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

    chimera_smb_create_break_waiter_retire(request);

    /* No ack arrived for the whole break deadline, so the holder is revoked and
     * this open completes anyway (MS-SMB2 break timeout).  Info, not error: a
     * client is entitled not to ack, and smb2.lease.v2_complex1 and
     * smb2.oplock.batch21 reach here on every run without failing.
     *
     * It is still worth a line, because recovery costs the full deadline
     * (default 30 s) and a client that gives up sooner sees the create simply
     * never answered -- a WPTS adapter times out at 20 s, so any create that
     * gets here has already failed the test 10 s earlier. */
    chimera_smb_info(
        "CREATE park deadline expired with no break ack: revoking holders and completing open; fh_hash %016lx conn %p",
        (unsigned long) request->create.park_fh_hash,
        (void *) request->compound->conn);

    chimera_vfs_claim_revoke_breaks(vfs_state, request->create.park_fh,
                                    request->create.park_fh_len,
                                    request->create.park_fh_hash,
                                    open_file ? open_file->grant : NULL);

    /* The holder never acked and has now been revoked; lift the capped
     * lease/durable decision if nothing else conflicts. */
    chimera_smb_create_resume_rearbitrate(request);

    if (open_file) {
        open_file->flags &= ~CHIMERA_SMB_OPEN_FILE_CREATE_PENDING;
    }

    /* The open succeeds even though the file break timed out; still emit the
     * deferred parent dir-lease content break (the directory's contents did
     * change). */
    chimera_smb_create_finish_deferred_dir_break(request);

    /* complete_request retires the async-interim (unlink from parked_requests +
     * clear armed); the fired one-shot is already out of the timer heap so its
     * removal there no-ops.  async_id stays set -> final reply is ASYNC-tagged. */
    chimera_smb_create_finish_with_eas(request, open_file);
} /* chimera_smb_create_park_deadline_cb */

/* Finish a regular-file open: apply the precomputed access masks, emit the
 * parent-directory create notification, release the parent handle, and complete
 * the request.  Invoked on a synchronous grant from open_at_callback and on a
 * parked grant from chimera_smb_create_share_park_cb. */
/* Emit a parent directory-lease content break that was deferred while the open
 * parked on a FILE caching break (overwrite), then release the parent handle that
 * was held alive across the park.  Sequencing the dir break after the file break
 * is acked is what dirlease.overwrite requires (break -> ack -> break); emitting
 * it up-front would deliver both breaks before the first ack and the skip_ack
 * test would lose the second to its inter-ack reset. */
static void
chimera_smb_create_finish_deferred_dir_break(struct chimera_smb_request *request)
{
    if (!request->create.dir_break_pending) {
        return;
    }

    request->create.dir_break_pending = 0;

    if (request->create.parent_handle) {
        smb_create_notify_lease(request,
                                      request->create.parent_handle->fh,
                                      request->create.parent_handle->fh_len,
                                      request->create.dir_break_action,
                                      request->create.name,
                                      request->create.name_len,
                                      NULL, 0,
                                      request->create.dir_break_skip_lo,
                                      request->create.dir_break_skip_hi,
                                      request->create.dir_break_has_skip);
        chimera_smb_create_release_parent(request);
    }
} /* chimera_smb_create_finish_deferred_dir_break */

static void
chimera_smb_create_open_finish(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file)
{
    struct chimera_server_smb_thread *thread    = request->compound->thread;
    struct chimera_vfs_state         *vfs_state =
        thread->vfs_thread->vfs->vfs_state;
    bool                              will_park = false;

    request->create.r_open_file          = open_file;
    request->create.dir_break_pending    = 0;
    request->create.park_on_notify       = 0;
    request->create.break_waiter_counted = 0;

    open_file->granted_access = request->create.r_granted_access;
    open_file->maximal_access = request->create.r_maximal_access;

    /* Decide up front whether this open must park on a FILE caching break still
     * in flight (a conflicting/overwriting open whose ack-required break has not
     * settled).  Computed here so a content break can be DEFERRED past the park. */
    if (open_file->handle) {
        bool delete_on_close =
            (request->create.create_options & SMB2_FILE_DELETE_ON_CLOSE) != 0;

        /* A delete-on-close open is a namespace mutation: it completes ON NOTIFY.
         * The conflicting holder's handle-cache break (RqLs RH->R / a batch
         * oplock's deferred handle) is informational -- the holder relinquishes
         * asynchronously and the deferred removal at LAST close still drains the
         * break (chimera_vfs_remove_at's io_recall) -- so this open must NOT wait
         * for a break ack the holder is never obliged to send (the WPTS
         * MS-SMB2Model BreakRead{,Write}HandleLease holder deliberately never
         * acks; waiting for an ack here deadlocks the open for the model's ~20s
         * timeout).
         *
         * But "on notify" means once the break is actually SENT, not merely
         * queued: park until its notification reaches the holder so this open's
         * reply cannot overtake the break on the wire.  smbtorture's
         * smb2.lease.unlink checks lease_break_info.count the instant the unlink
         * returns -- a cross-connection RH->R break delivered a hair late (the
         * holder thread had not yet flushed its doorbell) read as count 0 (the
         * dominant CI flake).  This park resumes the moment the break flush marks
         * it delivered (chimera_vfs_claim_break_pending_notify ->
         * mark_break_notified), so an ack the holder never sends is irrelevant.
         *
         * A DATA open (read / write / truncate) still pends on a write/read-cache
         * break for flush coherence (smb2.lease.breaking3), and a delete-on-close
         * that ALSO requested its own oplock still parks to re-arbitrate that
         * grant once the holder's break settles (smb2.durable-open.oplock). */
        struct chimera_vfs_open_handle *oh = open_file->handle;

        if (!delete_on_close || open_file->grant != NULL) {
            will_park = chimera_vfs_claim_ack_pending(vfs_state, oh->fh,
                                                      oh->fh_len, oh->fh_hash,
                                                      open_file->grant);
        } else if (chimera_vfs_claim_break_pending_notify(
                       vfs_state, oh->fh, oh->fh_len, oh->fh_hash)) {
            will_park                      = true;
            request->create.park_on_notify = 1;
        }
    }

    /* What this open reports to watchers of the parent directory (MS-FSA
     * 2.1.5.1).  Three outcomes, keyed on what the open actually DID rather
     * than on the disposition it asked for:
     *
     *   created      the entry appeared: FILE_ADDED (DIR_ADDED for a
     *                directory), plus STREAM_NAME for a regular file, whose
     *                default $DATA stream appears with it (WPTS
     *                BVT_SMB2Basic_ChangeNotify_ChangeStreamName)
     *   overwritten  the entry was already there and its data was replaced:
     *                that is a MODIFIED, not an ADDED, and it changes the
     *                size and the write time
     *   opened       nothing.  Opening a file does not change the directory
     *                that holds it, and a watcher must not be woken for it.
     *
     * Reporting ADDED for every create-capable disposition -- what this used
     * to do, on the grounds that a spurious ADDED beat a missed one -- is
     * wrong in both directions once a filter is involved: a client watching
     * FILE_NOTIFY_CHANGE_SIZE or _ATTRIBUTES is told nothing about an
     * overwrite that changed both (ADDED maps to neither filter), while a
     * client watching FILE_NOTIFY_CHANGE_FILE_NAME is woken for opens that
     * created nothing. */
    {
        uint32_t action = 0;

        if (request->create.r_created) {
            action = request->create.r_is_directory ?
                CHIMERA_VFS_NOTIFY_DIR_ADDED :
                (CHIMERA_VFS_NOTIFY_FILE_ADDED |
                 CHIMERA_VFS_NOTIFY_STREAM_NAME);
        } else if (request->create.create_disposition == SMB2_FILE_OVERWRITE ||
                   request->create.create_disposition == SMB2_FILE_OVERWRITE_IF ||
                   request->create.create_disposition == SMB2_FILE_SUPERSEDE) {
            action = CHIMERA_VFS_NOTIFY_FILE_MODIFIED |
                CHIMERA_VFS_NOTIFY_SIZE_CHANGED |
                CHIMERA_VFS_NOTIFY_ATTRS_CHANGED |
                CHIMERA_VFS_NOTIFY_STREAM_SIZE;
        }

        /* A directory read lease breaks exactly when there is an event to
         * raise: the directory's contents changed.  An open that changed
         * nothing raises nothing and so recalls nothing -- otherwise
         * re-opening a file would spuriously recall the parent directory lease
         * (smbtorture dirlease.rename re-opens before renaming), which is why
         * the previous shape needed a separate break-free emit path for the
         * opened-an-existing-file case.  With the event gone, so is that path. */
        if (action) {
            /* Self-exempt the directory lease whose ParentLeaseKey this create
            * supplied: the client creating the file holds (or is updating) that
            * directory's cached view and must not have its own lease broken
            * (MS-SMB2 dirlease ParentLeaseKey; dirlease.v2_request_parent). */
            uint64_t skip_lo, skip_hi;
            bool     has_skip = chimera_smb_parent_lease_skip(
                open_file->parent_lease_key, &skip_lo, &skip_hi);

            if (will_park) {
                /* This open is about to park on a FILE caching break; defer the
                 * parent dir-lease content break to the resume so it is sequenced
                 * AFTER that file break is acked (dirlease.overwrite).  Keep the
                 * parent handle alive for the deferred emit. */
                request->create.dir_break_pending  = 1;
                request->create.dir_break_action   = action;
                request->create.dir_break_skip_lo  = skip_lo;
                request->create.dir_break_skip_hi  = skip_hi;
                request->create.dir_break_has_skip = has_skip;
            } else {
                smb_create_notify_lease(request,
                                              request->create.parent_handle->fh,
                                              request->create.parent_handle->fh_len,
                                              action,
                                              request->create.name,
                                              request->create.name_len,
                                              NULL, 0,
                                              skip_lo, skip_hi, has_skip);
            }
        }
    }

    /* Release the parent now unless a deferred dir break still needs it (it is
     * released by chimera_smb_create_finish_deferred_dir_break on resume). */
    if (!request->create.dir_break_pending) {
        chimera_smb_create_release_parent(request);
    }

    /* MS-SMB2 3.3.5.9 pending-open: if this open triggered an ack-required lease
     * break on another holder, hold the SUCCESS response until the holder
     * acknowledges (the break is mid-flight).  Emit an async-interim
     * STATUS_PENDING now (so the client knows the create is in flight, can cancel
     * it, and does not time out across the up-to-break-timeout wait); the request
     * lands on conn->parked_requests, and an inbound OPLOCK_BREAK ack that settles
     * the file's caching leases resumes it (chimera_smb_create_resume_parked).
     * The open_file ref is held across the wait and dropped on resume. */
    if (will_park) {
        struct chimera_vfs_open_handle *oh = open_file->handle;

        memcpy(request->create.park_fh, oh->fh, oh->fh_len);
        request->create.park_fh_len  = oh->fh_len;
        request->create.park_fh_hash = oh->fh_hash;
        /* The CREATE is now pending on a break ack; mark the handle so a
         * concurrent replayed DH2Q create with this create_guid is answered
         * FILE_NOT_AVAILABLE instead of parking too (MS-SMB2 3.3.5.9.10,
         * smb2.replay.dhv2-pending*).  Keyed on the DH2Q create_guid the
         * client supplied -- recorded on the open even without a durable
         * grant (so the NONE-oplock pending variants match too). */
        if (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_DH2Q) {
            open_file->flags |= CHIMERA_SMB_OPEN_FILE_CREATE_PENDING;
        }
        chimera_smb_async_interim_begin(request);

        /* Count this park on the file's claim state: it is what tells the claim
         * layer that an opener is blocked on this break, which is the signal a
         * mid-break channel loss has to be resolved against. */
        chimera_smb_create_break_waiter_register(request);

        /* A parked CREATE sends no reply until an ack settles the break, so
         * from outside it is indistinguishable from a lost request.  Say so at
         * info level: this is the only record that a create waited, and which
         * connection it waited on -- both are needed to tell "the ack never
         * came" from "the ack came on another channel of this session and we
         * missed it".  Parks are rare relative to I/O. */
        chimera_smb_info(
            "CREATE parked on a caching break: fh_hash %016lx conn %p session %p deadline %u ms",
            (unsigned long) request->create.park_fh_hash,
            (void *) request->compound->conn,
            (void *) (request->session_handle ? request->session_handle->session : NULL),
            vfs_state->default_break_deadline_ms);

        /* Arm a deadline: if the holder never acks the break, fire at the
         * break timeout to revoke it and complete this open anyway (MS-SMB2
         * break-timeout).  Cancelled when an ack resumes the open first. */
        evpl_add_oneshot_timer(thread->evpl, &request->async.timer,
                               chimera_smb_create_park_deadline_cb,
                               (uint64_t) vfs_state->default_break_deadline_ms
                               * 1000);
        return;
    }

    chimera_smb_create_finish_with_eas(request, open_file);
} /* chimera_smb_create_open_finish */

/* Re-arbitrate a parked CREATE's caching grant as its break wait resolves.
 *
 * The open's oplock/lease -- and the durable grant that hinges on holding
 * batch / HANDLE caching -- were decided while the conflicting holder was
 * still mid-break, capping them below what the client requested.  If that
 * holder has since gone away entirely (a disconnected durable handle closed
 * by its connection teardown, purged on conflict, or force-revoked), the
 * deferred reply can carry the full grant -- which is what Windows returns,
 * and what smb2.durable-open.{lease,oplock} assert when the second client's
 * open races the first client's TCP disconnect.  A holder that instead acked
 * its break and kept the handle still conflicts, so the upgrade probe refuses
 * and the capped decision stands (the live-holder outcome is unchanged).
 *
 * Runs on the request's owning thread, immediately before the deferred
 * completion marshals the reply from the open_file. */
static void
chimera_smb_create_resume_rearbitrate(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread    = request->compound->thread;
    struct chimera_smb_open_file     *open_file = request->create.r_open_file;
    uint8_t                           req_smb   = 0;
    uint8_t                           want_vfs;
    bool                              via_rqls;

    if (!open_file || !open_file->grant || !open_file->caching_file_state) {
        return;
    }

    /* Recompute the requested caching bits exactly as the original acquire
     * did (chimera_smb_create_after_share). */
    via_rqls = (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) != 0;
    if (via_rqls) {
        req_smb = thread->shared->config.leases
            ? (uint8_t) (request->create.rqls.state & 0x07) : 0;
    } else if (thread->shared->config.oplocks) {
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
                break;
        } /* switch */
    }

    /* Honor the share's force-level-2 cap on the re-arbitrate path too, so the
     * deferred reply cannot upgrade past LEVEL_II on a force-level-2 share. */
    if (request->tree && request->tree->share &&
        request->tree->share->force_level2_oplock) {
        req_smb &= SMB2_LEASE_READ_CACHING;
    }

    want_vfs = chimera_smb_lease_bits_to_vfs(req_smb);
    if (want_vfs == 0) {
        return;
    }

    if (chimera_vfs_claim_grant_try_upgrade(open_file->caching_file_state,
                                            open_file->grant,
                                            want_vfs) != want_vfs) {
        /* Still capped by a surviving holder (or the grant is shared / mid-
         * break): the original decision stands. */
        return;
    }

    if (via_rqls) {
        open_file->lease_state = chimera_smb_vfs_to_lease_bits(want_vfs);
    } else {
        open_file->oplock_level = chimera_smb_vfs_to_oplock_level(want_vfs);
    }

    /* The upgrade may have made the open durable-eligible (batch / HANDLE
     * caching); grant and register it now so the deferred reply carries the
     * durable context and a disconnect can park it. */
    if (!open_file->durable_flags && open_file->handle &&
        open_file->type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE &&
        request->tree && request->tree->share) {
        chimera_smb_create_grant_durable(request, open_file);
        if (open_file->durable_flags) {
            chimera_smb_create_register_durable(request, open_file);
        }
    }
} /* chimera_smb_create_resume_rearbitrate */

/* Resume the settled parked CREATEs on one connection.  Pull every parked
 * CREATE whose triggered lease break has now settled (no caching lease on the
 * file is mid-break) off `conn`'s parked list, then complete them on this
 * thread.  `conn` must be owned by `thread` so the deferred response's
 * thread-local iovecs are produced on the right thread.  The
 * chimera_vfs_claim_ack_pending() re-check is what makes a blind sweep
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
        /* req->create is only valid for a parked CREATE; the command check must
         * gate the create-field reads below (the parked list also carries other
         * commands, e.g. a byte-range LOCK parked on a lease break).  A
         * delete-on-close open parked for delivery (park_on_notify) resumes the
         * moment its break leaves the pending-notify state -- i.e. the
         * notification has been sent -- whereas an ordinary ack-required park
         * resumes only once the file's caching leases actually settle. */
        /* A share-parked CREATE (gen_parked) is on this list only so CANCEL and
         * teardown can find it: it waits on a vfs share-acquire ticket, has no
         * park_fh, and completes through chimera_smb_create_share_park_finish.
         * Sweeping it here would complete a create that never got its share
         * reservation -- with SUCCESS, since park_fh_len 0 reads as "settled". */
        if (req->smb2_hdr.command == SMB2_CREATE &&
            !req->compound_coordinate_wait &&
            !req->create.gen_parked &&
            (req->create.park_on_notify
             ? !chimera_vfs_claim_break_pending_notify(
                 vfs_state, req->create.park_fh, req->create.park_fh_len,
                 req->create.park_fh_hash)
             : !chimera_vfs_claim_ack_pending(
                 vfs_state, req->create.park_fh, req->create.park_fh_len,
                 req->create.park_fh_hash,
                 req->create.r_open_file ? req->create.r_open_file->grant
                                         : NULL))) {
            *pp                  = req->async.park_next;
            req->async.park_next = resume;
            req->async.armed     = 0; /* already unlinked; skip re-cancel */
            chimera_smb_create_break_waiter_retire(req);
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
        /* The break this CREATE waited on has settled; if the conflicting
         * holder vanished outright, lift the capped lease/durable decision
         * before the deferred reply is marshaled. */
        chimera_smb_create_resume_rearbitrate(req);

        /* Pairs with the "CREATE parked" line: a park with no matching resume
         * and no deadline line is a create that was still waiting when its
         * connection went away.  conn is logged on both so a resume arriving
         * on a different channel of the same session is visible as such. */
        chimera_smb_info("CREATE resumed after break ack: fh_hash %016lx conn %p",
                         (unsigned long) req->create.park_fh_hash,
                         (void *) conn);

        if (req->create.r_open_file) {
            req->create.r_open_file->flags &=
                ~CHIMERA_SMB_OPEN_FILE_CREATE_PENDING;
        }
        /* The file break this open parked on is now acked; emit the deferred
         * parent dir-lease content break (sequenced after that ack). */
        chimera_smb_create_finish_deferred_dir_break(req);
        /* async_id is still set, so the final reply carries
         * SMB2_FLAGS_ASYNC_COMMAND matching the interim. */
        chimera_smb_create_finish_with_eas(req, req->create.r_open_file);
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
 * The per-CREATE ack_pending() re-check gates correctness, so this blind
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

    /* Drain CREATEs whose share-park ticket resolved on another thread and were
     * bounced here (chimera_smb_create_share_park_cb) to finish on their owning
     * thread with the stashed GRANTED/DENIED result. */
    for ( ; ;) {
        struct chimera_smb_request *req;

        pthread_mutex_lock(&thread->lease_break_lock);
        req = thread->share_resume_head;
        if (req) {
            thread->share_resume_head = req->create.gen_resume_next;
        }
        pthread_mutex_unlock(&thread->lease_break_lock);

        if (!req) {
            break;
        }

        req->create.gen_resume_next = NULL;
        chimera_smb_create_share_park_finish(req, req->create.gen_resume_result);
    }
} /* chimera_smb_create_resume_doorbell_callback */

/* Ring every PEER SMB thread's resume doorbell so each re-scans its own
 * connections for parked CREATEs the just-settled lease break unblocked.
 * `origin` is skipped because the ack handler already swept its own connection
 * inline; the per-CREATE ack_pending() re-check would make a re-sweep of
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
    uint32_t da = smb_create_desired_access(request);
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

    req = smb_create_effective_access(request, req);

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
 * True when an ACCESS_DENIED verdict from chimera_smb_create_check_access may
 * be the transient state of an object a concurrent CREATE is still
 * constructing rather than a real denial.  The passthrough backends create as
 * the server identity and chown to the client afterwards, so the signature is:
 * no explicit ACL, object owned by the server identity, caller is someone
 * else.  Gates the bounded re-getattr in chimera_smb_create_open_at_callback.
 */
static inline int
chimera_smb_create_access_deny_transient(
    struct chimera_smb_request     *request,
    const struct chimera_vfs_attrs *attr)
{
    const struct chimera_vfs_cred *sc   = chimera_vfs_get_server_cred();
    const struct chimera_vfs_cred *cred =
        &request->session_handle->session->cred;
    int                            has_acl =
        (attr->va_set_mask & CHIMERA_VFS_ATTR_ACL) &&
        attr->va_acl && attr->va_acl->num_aces > 0;

    return !has_acl &&
           cred->flavor != CHIMERA_VFS_AUTH_NONE &&
           (attr->va_set_mask & CHIMERA_VFS_ATTR_UID) &&
           attr->va_uid == (uint64_t) sc->uid &&
           (uint64_t) cred->uid != (uint64_t) sc->uid;
} /* chimera_smb_create_access_deny_transient */

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
    uint32_t da = smb_create_desired_access(request);
    uint32_t g;

    if (da & SMB2_MAXIMUM_ALLOWED) {
        return smb_create_effective_access(request, chimera_vfs_access_check(
            attr, &request->session_handle->session->cred,
            CHIMERA_ACE_MASK_ALL) | (da & SMB2_ACCESS_SYSTEM_SECURITY));
    }

    /* GrantedAccess is reported in resolved specific rights, never the NT
     * generic bits; expand them.  A specific-bits open that reached here was
     * granted everything it asked for. */
    /* check_access already admitted an explicitly requested security right
     * under the server's privilege policy. Preserve it on the open; neither
     * GENERIC_ALL nor MAXIMUM_ALLOWED implicitly requests SACL access. */
    g = da & ~(SMB2_MAXIMUM_ALLOWED |
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

    return smb_create_effective_access(request, g);
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

    bool stream = request->create.r_open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM;
    if (!stream && S_ISDIR(attr->va_mode)) {
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

    chimera_smb_marshal_open_attrs(attr, stream, &request->create.r_attrs);

    chimera_smb_open_file_release(request, request->create.r_open_file);

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

/* Only the successful handle-state OPEN proves that its record exists. The
 * ID/serialization buffers can already be populated on a failed operation. */
static void
chimera_smb_create_persist_open_at_callback(
    enum chimera_vfs_error error_code, struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs *set_attr, struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr, struct chimera_vfs_attrs *dir_post_attr,
    void *private_data)
{
    struct chimera_smb_request *request = private_data;
    request->create.persist_record_written = error_code == CHIMERA_VFS_OK;
    if (request->create.persist_record_written) {
        request->create.persist_fh_len = oh->fh_len;
        memcpy(request->create.persist_fh, oh->fh, oh->fh_len);
    } else if (attr && (attr->va_set_mask & CHIMERA_VFS_ATTR_FH) &&
               attr->va_fh_len > 0 && attr->va_fh_len <= CHIMERA_VFS_FH_SIZE) {
        /* A failing PUT may have reached storage. Retain its route for
         * best-effort deletion without claiming the record was committed. */
        request->create.persist_fh_len = attr->va_fh_len;
        memcpy(request->create.persist_fh, attr->va_fh, attr->va_fh_len);
    }
    if (error_code != CHIMERA_VFS_OK && request->create.persist_hs.r_created) {
        /* Persistence failure does not undo the successful namespace prefix.
         * The VFS honored NO_NOTIFY, so publish this creation before its error. */
        uint64_t skip_lo = 0, skip_hi = 0;
        bool has_skip = (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS_V2) &&
            (request->create.rqls.flags & SMB2_LEASE_FLAG_PARENT_LEASE_KEY_SET) &&
            chimera_smb_parent_lease_skip(request->create.rqls.parent_key, &skip_lo, &skip_hi);
        smb_create_notify_lease(request,
            request->create.parent_handle->fh, request->create.parent_handle->fh_len,
            CHIMERA_VFS_NOTIFY_FILE_ADDED | CHIMERA_VFS_NOTIFY_STREAM_NAME,
            request->create.name, request->create.name_len, NULL, 0,
            skip_lo, skip_hi, has_skip);
    }
    chimera_smb_create_open_at_callback(error_code, oh, set_attr, attr,
        dir_pre_attr, dir_post_attr, private_data);
}

/* Issue the open_at against the (already opened) parent handle in
 * request->create.parent_handle.  Shared by the plain path and the
 * post-overwrite-check path. */
static void
chimera_smb_create_issue_open(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread *vfs_thread = request->compound->thread->vfs_thread;
    /* NO_NOTIFY: the SMB create path emits its own CHANGE_NOTIFY event
     * (disposition policy, DIR/STREAM_NAME classes, directory-lease key
     * sparing), so the VFS core's generic created-file emission must stay
     * quiet or watchers would see the create twice. */
    unsigned int               flags = CHIMERA_VFS_OPEN_NO_NOTIFY;

    if (request->create.create_options & SMB2_FILE_DIRECTORY_FILE) {
        flags |= CHIMERA_VFS_OPEN_DIRECTORY;
    }

    /* Metadata-only open: when the caller doesn't request any data-access
     * bits, satisfy the open with an O_PATH-style handle. */
    if (!(request->create.desired_access & SMB2_DATA_ACCESS_MASK) &&
        request->create.create_disposition == SMB2_FILE_OPEN) {
        flags |= CHIMERA_VFS_OPEN_PATH;
    }

    if (!(request->create.desired_access & SMB2_WRITE_MASK) &&
        !chimera_smb_disposition_overwrites(request->create.create_disposition)) {
        flags |= CHIMERA_VFS_OPEN_READ_ONLY;
    }

    if ((request->create.create_options & SMB2_FILE_OPEN_REPARSE_POINT) &&
        request->create.create_disposition == SMB2_FILE_OPEN) {
        flags |= CHIMERA_VFS_OPEN_NOFOLLOW;
    }

    /* Unless the caller explicitly opens the reparse point, stop if the leaf is
     * a symbolic link: the backend returns ELOOP (-> STATUS_STOPPED_ON_SYMLINK)
     * before colliding/opening/truncating it, so a FILE_CREATE on a symlink leaf
     * stops at the link instead of reporting OBJECT_NAME_COLLISION (MS-SMB2
     * 3.3.5.9). */
    if (!(request->create.create_options & SMB2_FILE_OPEN_REPARSE_POINT)) {
        flags |= CHIMERA_VFS_OPEN_STOP_SYMLINK;
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

        /* An AllocationSize create context reserves space for the new (or
         * overwritten, hence 0-length) file.  Carry it to the backend so the
         * reservation is reflected in the reported AllocationSize (MS-SMB2
         * 2.2.13.2 "AlSi"; smb2.create.blob).  Backends that do not persist a
         * reservation simply ignore the mask. */
        if (request->create.alsi_alloc_size > 0) {
            request->create.set_attr.va_alloc_size = request->create.alsi_alloc_size;
            request->create.set_attr.va_req_mask  |= CHIMERA_VFS_ATTR_ALLOC_SIZE;
            request->create.set_attr.va_set_mask  |= CHIMERA_VFS_ATTR_ALLOC_SIZE;
        }
    }

    /* Admission must precede destructive overwrite.  Keep the exact opened
     * object alive and mutate it by handle after the share/cache gates. */
    if (flags & CHIMERA_VFS_OPEN_TRUNCATE) {
        request->create.overwrite_pending = 1;
        request->create.overwrite_attr    = request->create.set_attr;
        flags                            &= ~CHIMERA_VFS_OPEN_TRUNCATE;
    }

    if (chimera_smb_lease_key_conflict(request, NULL, 0)) {
        flags &= ~(CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE);
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
            chimera_smb_create_persist_open_at_callback,
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

static void
chimera_smb_create_parent_ready(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread      *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_vfs_open_handle *oh         = request->create.parent_handle;

    if (!chimera_smb_lease_key_conflict(request, NULL, 0) &&
        (request->create.create_options & SMB2_FILE_DIRECTORY_FILE) &&
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
} /* chimera_smb_create_parent_ready */

/* An already-bound lease key cannot acquire a newly-created file.  Resolve
 * this before create/mkdir changes the namespace; the post-open check still
 * validates the actual resolved handle for existing files and rename races. */
static void
chimera_smb_create_lease_name_checked(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request     = private_data;
    uint32_t                    disposition = request->create.create_disposition;

    (void) attr;
    (void) dir_attr;
    if (error_code == CHIMERA_VFS_ENOENT &&
        disposition != SMB2_FILE_OPEN && disposition != SMB2_FILE_OVERWRITE &&
        chimera_smb_lease_key_conflict(request, NULL, 0)) {
        chimera_smb_create_release_parent(request);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }
    if (error_code == CHIMERA_VFS_OK && disposition == SMB2_FILE_CREATE &&
        !request->create.has_stream) {
        if (attr && (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && S_ISLNK(attr->va_mode) &&
            !(request->create.create_options & SMB2_FILE_OPEN_REPARSE_POINT)) {
            chimera_smb_create_stopped_on_symlink(request);
        } else {
            chimera_smb_create_release_parent(request);
            chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_COLLISION);
        }
        return;
    }
    chimera_smb_create_parent_ready(request);
} /* chimera_smb_create_lease_name_checked */

static inline void
chimera_smb_create_open_parent_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request       *request = private_data;
    struct chimera_server_smb_thread *thread  = request->compound->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_complete_request(request,
                                     chimera_smb_create_parent_error_status(error_code));
        return;
    }
    request->create.parent_handle = oh;
    if (thread->shared->config.leases &&
        (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) &&
        chimera_smb_lease_key_conflict(request, NULL, 0)) {
        chimera_vfs_lookup_at(thread->vfs_thread,
                              &request->session_handle->session->cred,
                              oh, request->create.name, request->create.name_len,
                              CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE, 0,
                              chimera_smb_create_lease_name_checked, request);
        return;
    }
    chimera_smb_create_parent_ready(request);
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
        chimera_smb_complete_request(request,
                                     chimera_smb_create_parent_error_status(error_code));
        return;
    }

    /* The create path traverses a symbolic link: SMB never silently follows a
     * reparse point on the server, it returns STATUS_STOPPED_ON_SYMLINK so the
     * client resolves the link and retries (MS-SMB2 2.2.2.2.1).  The parent-path
     * lookup is issued without LOOKUP_FOLLOW, so a symlink as the final component
     * of the parent path resolves to the link itself here.  Readlink it (by the
     * fh the lookup returned) so the reply carries the target AND the unparsed
     * suffix -- a bare STOPPED body leaves the client with only ELOOP, breaking
     * every op that traverses a symlinked directory. */
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && S_ISLNK(attr->va_mode)) {
        request->create.r_symlink_handle = NULL;
        chimera_vfs_open_fh(
            vfs_thread,
            &request->session_handle->session->cred,
            attr->va_fh,
            attr->va_fh_len,
            CHIMERA_VFS_OPEN_NOFOLLOW | CHIMERA_VFS_OPEN_PATH,
            chimera_smb_create_parent_symlink_open_cb,
            request);
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
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE,
            0,
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

/* A durable reconnect can reach this server thread before the previous
 * connection's disconnect has been processed (the two connections live on
 * independent event loops, so there is no ordering between the new CREATE and
 * the old TCP teardown).  In that window the surviving open is still flagged
 * live and the claim is refused -- spuriously, because the legitimate owner IS
 * reconnecting.  So a refused-because-not-parked claim is retried on a short
 * timer until the disconnect is processed (the handle parks and the retry
 * reclaims it) or this budget elapses (a genuinely-live handle never parks).
 * ~3s comfortably covers the teardown latency even on a loaded ASan runner,
 * while bounding the wait for the rare genuinely-conflicting reconnect. */
#define CHIMERA_SMB_DURABLE_RECONNECT_INTERVAL_US 20000  /* 20 ms */
#define CHIMERA_SMB_DURABLE_RECONNECT_MAX_RETRIES 150    /* ~3 s total */

/* Re-home a reclaimed (formerly parked) durable open into the reconnecting
 * tree and emit the open reply via the create getattr callback.  Shared by the
 * DH2C/DHnC reconnect path and the DH2Q create_guid replay-reclaim path.  When
 * replay != 0 the reply echoes the ORIGINAL create_action (CREATED) instead of
 * OPENED -- the replay re-presents the create that established the handle. */
static void
chimera_smb_durable_rehome(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file,
    int                           replay)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_smb_tree          *tree   = request->tree;
    int                               bucket;

    /* The surviving open keeps its FileId intact across the reconnect -- both
     * the persistent id AND the volatile id.  Although MS-SMB2 permits the
     * volatile id to be reissued, OneFS (and the pike durable/persistent
     * reconnect tests) require the reconnected handle to report the same
     * FileId.Volatile it had before the disconnect, so the open_file's
     * file_id is left untouched here.  The registry reference becomes the new
     * tree reference; we add one more for this in-flight request, which the
     * getattr callback releases below. */
    open_file->flags &= ~CHIMERA_SMB_OPEN_FILE_PARKED;
    /* A reclaimed durable open becomes replay-eligible again (MS-SMB2
     * 3.3.5.9.10): a replayed create matching it reports the original
     * create_action until the next non-replay op uses the handle. */
    open_file->flags      |= CHIMERA_SMB_OPEN_FILE_REPLAY_ELIGIBLE;
    open_file->create_conn = request->compound->conn;
    open_file->refcnt      = 2;
    /* Reseed the channel-sequence baseline from the reconnecting CREATE. */
    open_file->channel_sequence       = request->channel_sequence;
    open_file->channel_sequence_valid = 1;
    /* No longer a courtesy-held disconnected holder. */
    if (open_file->grant) {
        chimera_vfs_claim_park(&open_file->grant->claim, false);
    }
    if (open_file->share_lease_inserted) {
        chimera_vfs_claim_park(chimera_smb_share_claim(open_file), false);
    }

    /* Re-home the open onto the RECONNECTING tree.  open_file->tree is the
     * authority for which bucket lock protects this open and which hash it
     * must be removed from -- chimera_smb_open_file_release and
     * chimera_smb_open_file_close_hashed both read it rather than
     * request->tree, precisely so an open reached through another tree-connect
     * is handled correctly.  Without this the open stays pointed at the tree it
     * was parked from, which chimera_smb_tree_free already returned to
     * shared->free_trees (with share = NULL, and not zeroed on reuse): the next
     * release would lock a recycled tree's mutex and HASH_DELETE the open from
     * the wrong table, corrupting both it and the table the open actually
     * lives in. */
    open_file->tree = tree;
    if (open_file->durable_tree_pin) {
        struct chimera_smb_tree *old_tree = open_file->durable_tree_pin;
        open_file->durable_tree_pin = NULL;
        chimera_smb_tree_memory_unpin(thread->shared, old_tree);
    }

    bucket = open_file->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    pthread_mutex_lock(&tree->open_files_lock[bucket]);
    HASH_ADD(hh, tree->open_files[bucket], file_id, sizeof(open_file->file_id), open_file);
    pthread_mutex_unlock(&tree->open_files_lock[bucket]);

    request->create.r_open_file        = open_file;
    request->create.create_disposition = SMB2_FILE_OPEN; /* reply default OPENED */
    request->create.reconnect          = 1; /* skip ACL re-check on the reply path */
    request->create.replay             = replay; /* echo original create_action */
    request->compound->saved_file_id   = open_file->file_id;

    /* Refresh the network-open-info from the surviving handle, then reply.
     * Reuses the normal create getattr callback (releases the request ref). */
    chimera_vfs_getattr(thread->vfs_thread,
                        &request->session_handle->session->cred,
                        open_file->handle,
                        CHIMERA_VFS_ATTR_MASK_STAT,
                        chimera_smb_create_open_getattr_callback,
                        request);
} /* chimera_smb_durable_rehome */

/* One durable-reclaim attempt.  Returns true if the request has been completed
 * or handed off (cold reopen / success reply / terminal failure); false if the
 * handle is still racing its previous connection's disconnect and the caller
 * should retry. */
static bool
chimera_smb_durable_reconnect_attempt(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_server_smb_shared *shared = thread->shared;
    struct chimera_smb_open_file     *open_file;
    const uint8_t                    *create_guid = NULL;
    const uint8_t                    *lease_key   = NULL;
    uint64_t                          persistent_id;
    uint32_t                          status = SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
    uint32_t                          ctx    = request->create.ctx_present_mask;
    bool                              has_lease_ctx;
    bool                              cold  = false;
    bool                              retry = false;

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

    /* A DH2C reconnect that sets SMB2_DHANDLE_FLAG_PERSISTENT must target a
     * persistent Open; the claim rejects it with INVALID_PARAMETER otherwise
     * (MS-SMB2 3.3.5.9.12).  DHnC (v1) has no Flags field, so this is false. */
    bool reconnect_persistent = (ctx & CHIMERA_SMB_CREATE_CTX_DH2C) &&
        (request->create.dh2c.flags & SMB2_DHANDLE_FLAG_PERSISTENT);

    open_file = chimera_smb_durable_claim(shared, persistent_id, create_guid,
                                          request->compound->conn->client_guid,
                                          request->session_handle->session->cred.uid,
                                          request->create.name, request->create.name_len,
                                          has_lease_ctx, lease_key, reconnect_persistent,
                                          &cold, &retry, &status);

    if (cold) {
        /* Recovered-after-restart persistent handle: there is no live open to
         * re-home.  Re-open the file from scratch via the normal create path
         * (the reconnect CREATE carries the name + access), forcing the
         * recovered persistent id so the open re-adopts its identity and the
         * backend record is refreshed atomically. */
        request->create.persist_pid = persistent_id;
        chimera_smb_create_process(request);
        return true;
    }

    if (!open_file) {
        if (retry) {
            return false;
        }
        chimera_smb_complete_request(request, status);
        return true;
    }

    /* Re-home the surviving open into the reconnecting tree.  A pure DH2C/DHnC
     * reconnect reports OPENED; if the reconnect ALSO carries the replay flag
     * (the client lost the original create's reply), report the original
     * create_action instead. */
    chimera_smb_durable_rehome(request, open_file, request->is_replay);
    return true;
} /* chimera_smb_durable_reconnect_attempt */

/* Retry timer for a reclaim that raced its previous connection's disconnect.
 * Re-attempts; on continued racing it re-arms until the budget lapses, then
 * answers OBJECT_NAME_NOT_FOUND.  Runs on the reconnecting request's own
 * thread (the timer is armed on that thread's evpl). */
static void
chimera_smb_durable_reconnect_retry_cb(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_smb_request *request = (struct chimera_smb_request *)
        ((char *) timer - offsetof(struct chimera_smb_request, async.timer));

    if (chimera_smb_durable_reconnect_attempt(request)) {
        return;
    }

    if (--request->create.reconnect_retries == 0) {
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
        return;
    }

    evpl_add_oneshot_timer(evpl, &request->async.timer,
                           chimera_smb_durable_reconnect_retry_cb,
                           CHIMERA_SMB_DURABLE_RECONNECT_INTERVAL_US);
} /* chimera_smb_durable_reconnect_retry_cb */

/* Durable-handle reconnect (DH2C / DHnC).  Reclaims a parked open that
 * survived its previous connection and re-homes it into the reconnecting
 * tree, then replies as a normal OPEN of the surviving file. */
static void
chimera_smb_durable_reconnect(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    uint32_t                          ctx    = request->create.ctx_present_mask;

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

    if (chimera_smb_durable_reconnect_attempt(request)) {
        return;
    }

    /* The handle is racing its previous connection's disconnect: tell the
     * client the create is pending (so it does not time out or cancel), then
     * retry on a short timer until the disconnect is processed. */
    request->create.reconnect_retries = CHIMERA_SMB_DURABLE_RECONNECT_MAX_RETRIES;
    chimera_smb_async_interim_begin(request);
    evpl_add_oneshot_timer(thread->evpl, &request->async.timer,
                           chimera_smb_durable_reconnect_retry_cb,
                           CHIMERA_SMB_DURABLE_RECONNECT_INTERVAL_US);
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
    uint8_t     *r_has_stream,
    uint8_t     *r_explicit_data_fork,
    uint8_t     *r_explicit_index_fork)
{
    const char *colon = memchr(name, ':', name_len);
    const char *rest, *type, *colon2;
    uint16_t    base_len, rest_len, stream_len;

    *r_has_stream          = 0;
    *r_explicit_data_fork  = 0;
    *r_explicit_index_fork = 0;

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

        /* $DATA names a data fork; $INDEX_ALLOCATION (only as the bare
         * "::$INDEX_ALLOCATION" form) names a directory's index stream -- i.e.
         * the directory itself.  Every other stream type is unsupported. */
        if (type_len == 5 && strncasecmp(type, "$DATA", 5) == 0) {
            /* data fork -- falls through to the stream-name handling below */
        } else if (type_len == 17 && stream_len == 0 &&
                   strncasecmp(type, "$INDEX_ALLOCATION", 17) == 0) {
            *r_base_len            = base_len;
            *r_explicit_index_fork = 1;
            return SMB2_STATUS_SUCCESS;
        } else {
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
         * trailing "file:" with neither name nor type is malformed.  Flag the
         * explicit default-data-fork form so the open path can reject it on a
         * directory (a directory has no data fork -- smb2.streams.dir). */
        if (!colon2) {
            return SMB2_STATUS_OBJECT_NAME_INVALID;
        }
        *r_base_len           = base_len;
        *r_explicit_data_fork = 1;
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
    struct chimera_smb_open_file     *pending_of = NULL;

    for (b = 0; b < CHIMERA_SMB_OPEN_FILE_BUCKETS && !match && !pending_of; b++) {
        pthread_mutex_lock(&tree->open_files_lock[b]);
        HASH_ITER(hh, tree->open_files[b], of, tmp)
        {
            if (of->identity_rebind && request->is_replay &&
                !memcmp(of->create_guid, request->create.dh2q.create_guid, 16)) {
                pthread_mutex_unlock(&tree->open_files_lock[b]);
                chimera_smb_complete_request(request, SMB2_STATUS_FILE_NOT_AVAILABLE);
                return 1;
            }
            /* A replayed durable create whose create_guid matches an open whose
             * own CREATE is still pending on a break ack must be answered
             * FILE_NOT_AVAILABLE (MS-SMB2 3.3.5.9.10): the original create has
             * not completed, so we neither return its (not-yet-final) handle nor
             * park a second time.  The pending open's ctx_present_mask is not yet
             * populated (set on reply), so match on the durable create_guid that
             * grant_durable already stored.  smb2.replay.dhv2-pending*. */
            if (request->is_replay &&
                (of->flags & CHIMERA_SMB_OPEN_FILE_CREATE_PENDING) &&
                memcmp(of->create_guid, request->create.dh2q.create_guid, 16) == 0) {
                of->refcnt++;   /* held while we decide reject vs. evict */
                pending_of = of;
                break;
            }

            if ((of->ctx_present_mask & CHIMERA_SMB_CREATE_CTX_DH2Q) &&
                !(of->flags & (CHIMERA_SMB_OPEN_FILE_CLOSED |
                               CHIMERA_SMB_OPEN_FILE_PARKED)) &&
                /* Only a replayed create returns the existing live open, and only
                 * while it remains replay-eligible.  A non-replay create with a
                 * matching create_guid, or a replay after a non-replay op cleared
                 * eligibility, is NOT returned here (MS-SMB2 3.3.5.9.10): it falls
                 * to the registry classification below (DUPLICATE_OBJECTID for a
                 * non-replay collision, or a fresh open for an ineligible replay).
                 * smb2.replay.replay-twice-durable, replay6. */
                request->is_replay &&
                (of->flags & CHIMERA_SMB_OPEN_FILE_REPLAY_ELIGIBLE) &&
                memcmp(of->create_guid, request->create.dh2q.create_guid, 16) == 0) {
                of->refcnt++;   /* held for this request; getattr cb releases it */
                match = of;
                break;
            }
        }
        pthread_mutex_unlock(&tree->open_files_lock[b]);
    }

    /* The create this replay matches may be deferred BEFORE it was hashed (parked
     * on a conflicting holder's handle-cache break, or retrying a disconnecting
     * durable holder), in which case the scan above cannot see it: consult the
     * in-flight registry and answer the replay the same way a hashed pending open
     * is answered.  Under the Windows profile the replay is not recognised here at
     * all -- Windows lets it run the ordinary create path, where the unresolved
     * conflict answers it SHARING_VIOLATION, which is what
     * smb2.replay.dhv2-pending1n-vs-violation-lease-{ack,close}-windows assert;
     * the -sane variants require the pending answer below. */
    if (!pending_of && !match && request->is_replay &&
        !thread->shared->config.replay_pending_windows &&
        chimera_smb_create_pending_match(tree,
                                         request->create.dh2q.create_guid,
                                         request->compound->conn->client_guid)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_NOT_AVAILABLE);
        return 1;
    }

    if (pending_of) {
        /* A replay matched a still-pending create carrying this create_guid.
         * Normally the original create is genuinely in flight, so the replay is
         * answered FILE_NOT_AVAILABLE -- the profile default; the Windows profile
         * answers ACCESS_DENIED instead (see
         * chimera_server_config_set_smb_replay_pending_windows: MS-SMB2 3.3.5.9 /
         * 3.3.5.9.10 do not specify this race and the two real implementations
         * diverge).  But if that create was
         * orphaned when its connection disconnected (CREATE_PENDING never resumed)
         * AND the break it parked on has since settled -- the conflicting holder
         * closed or acked -- the original create can never complete: evict the
         * resolved zombie (unhash + full teardown) and fall through to a fresh open.
         * A still-active break, or a non-orphaned pending create (which resumes
         * normally), still yields the pending answer.
         * smb2.replay.dhv2-pending{1,2,3}*-vs-{lease,oplock}-{sane,windows}. */
        struct chimera_vfs_state *vfs_state = thread->vfs_thread->vfs->vfs_state;
        bool                      evict, won = false;

        evict = (pending_of->flags & CHIMERA_SMB_OPEN_FILE_PENDING_ORPHANED) &&
            pending_of->handle &&
            !chimera_vfs_claim_ack_pending(vfs_state,
                                           pending_of->handle->fh,
                                           pending_of->handle->fh_len,
                                           pending_of->handle->fh_hash,
                                           pending_of->grant);

        if (!evict) {
            chimera_smb_open_file_release(request, pending_of);
            chimera_smb_complete_request(request,
                                         thread->shared->config.replay_pending_windows
                                         ? SMB2_STATUS_ACCESS_DENIED
                                         : SMB2_STATUS_FILE_NOT_AVAILABLE);
            return 1;
        }

        /* Unhash under the bucket lock (idempotent via CLOSED so concurrent
         * evictors don't double-unhash); the winner drops the orphan's surviving
         * handle ref so the open is torn down + freed exactly once.  Every evictor
         * always drops the scan ref it took above. */
        b = pending_of->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
        pthread_mutex_lock(&tree->open_files_lock[b]);
        if (!(pending_of->flags & CHIMERA_SMB_OPEN_FILE_CLOSED)) {
            pending_of->flags |= CHIMERA_SMB_OPEN_FILE_CLOSED;
            HASH_DELETE(hh, tree->open_files[b], pending_of);
            won = true;
        }
        pthread_mutex_unlock(&tree->open_files_lock[b]);
        chimera_smb_open_file_release(request, pending_of);       /* scan ref */
        if (won) {
            chimera_smb_open_file_release(request, pending_of);   /* handle ref */
        }
        /* fall through: no pending match remains -> proceed to a fresh open. */
    }

    if (!match) {
        /* No live open in this tree returned the existing handle.  Consult the
         * durable registry to classify the create_guid against ALL of this
         * client's durable opens (parked or live on any connection), per
         * MS-SMB2 3.3.5.9.10.  This covers: a replay reclaiming a parked open
         * (durable-reconnect-replay1, replay-twice-durable); a collision with a
         * still-live durable open -> DUPLICATE_OBJECTID (durable-reconnect-replay2,
         * replay6); and an ineligible replay that falls through to a fresh open
         * (replay-twice-durable, replay6). */
        if (request->compound->thread->shared->config.persistent_handles) {
            struct chimera_smb_open_file       *parked        = NULL;
            bool                                has_lease_ctx =
                (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) != 0;
            const uint8_t                      *lease_key =
                has_lease_ctx ? request->create.rqls.key : NULL;
            enum chimera_smb_guid_replay_result gr =
                chimera_smb_durable_claim_by_guid(
                    request->compound->thread->shared,
                    request->create.dh2q.create_guid,
                    request->compound->conn->client_guid,
                    request->compound->conn,
                    request->is_replay,
                    has_lease_ctx,
                    lease_key,
                    &parked);

            switch (gr) {
                case CHIMERA_SMB_GUID_REPLAY_RETRY:
                    chimera_smb_complete_request(request, SMB2_STATUS_FILE_NOT_AVAILABLE);
                    return 1;
                case CHIMERA_SMB_GUID_REPLAY_RECLAIM:
                    chimera_smb_durable_rehome(request, parked, /* replay */ 1);
                    return 1;
                case CHIMERA_SMB_GUID_REPLAY_DUPLICATE:
                    chimera_smb_complete_request(request,
                                                 SMB2_STATUS_DUPLICATE_OBJECTID);
                    return 1;
                case CHIMERA_SMB_GUID_REPLAY_DENIED:
                    chimera_smb_complete_request(request,
                                                 SMB2_STATUS_ACCESS_DENIED);
                    return 1;
                case CHIMERA_SMB_GUID_REPLAY_NONE:
                    break;
            } /* switch */
        }
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
            chimera_smb_vfs_to_lease_bits(match->grant->claim.used);
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

static void
smb_create_admitted(struct chimera_smb_request *request)
{
    struct timespec               now;
    struct chimera_smb_open_file *open_file;
    enum chimera_smb_pipe_magic   pipe_magic;
    chimera_smb_pipe_transceive_t transceive;

    request->create.overwrite_pending    = 0;
    request->create.overwrite_share_held = 0;
    request->create.has_stream           = 0;
    request->create.break_waiter_counted = 0;
    request->create.explicit_data_fork   = 0;
    request->create.explicit_index_fork  = 0;
    request->create.base_oh              = NULL;
    /* This reply pointer is a tagged CREATE field in a recycled union. A
     * legacy/redispatched request has not published an open yet; stale data
     * must not suppress failed-record cleanup or identify a previous FileId. */
    request->create.r_open_file          = NULL;
    /* Only the regular-file open path (open_at_callback) registers a finish
     * callback and may park on a batch-oplock break; clear it so the mkdir /
     * stream / pipe paths never inherit a stale callback and park. */
    request->create.gen_finish_cb          = NULL;
    request->create.gen_parked             = 0;
    request->create.gen_share_retry        = 0;
    request->create.access_retries         = 0;
    request->create.access_retry_oh        = NULL;
    request->create.share_conflict_retries = 0;
    request->create.share_conflict_oh      = NULL;
    request->create.pending_link           = NULL;
    request->create.pending_linked         = 0;
    request->create.r_symlink_error        = 0;
    request->create.r_symlink_handle       = NULL;

    /* A TWRP ("@GMT-..." timewarp) create context opens a file as of a VSS
     * snapshot.  chimera exposes no snapshots, so any non-zero timewarp names a
     * version that cannot exist -- per MS-SMB2 3.3.5.9 report OBJECT_NAME_NOT_
     * FOUND rather than silently opening the live file (smb2.create.blob). */
    if (request->create.twrp_timestamp != 0) {
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
        return;
    }

    /* Handle stream-name syntax (file:stream[:$DATA]).  When named streams are
     * disabled (config off, or the backend lacks the capability) the legacy
     * behavior is preserved: reject any ':' name with OBJECT_NAME_INVALID.
     * Done here rather than in parse so the response is returned to the client
     * instead of triggering a parse-error disconnect. */
    if (request->create.name_len > 0 &&
        memchr(request->create.name, ':', request->create.name_len)) {

        if (!request->compound->thread->shared->config.named_streams) {
            /* Gate 1: the feature is off in config, so the name is refused
             * before any backend is consulted.  Indistinguishable on the wire
             * from gate 2 in chimera_smb_create_open_stream_chain() ("backend
             * cannot do this") -- both are OBJECT_NAME_INVALID, which is correct
             * but hid a cluster of misconfigured smbtorture subtests.  Log
             * which one fired: it is where investigating such a subtest starts.
             * What decides whether that subtest belongs in
             * smbtorture_ads_subtests.txt is the off/on outcome pair described
             * there, not this line -- a subtest can trip this gate and pass
             * BECAUSE it does.
             *
             * Debug level, not info: named_streams is off by default, and macOS
             * clients probe ':AFP_AfpInfo' / ':com.apple.ResourceFork' while
             * Windows probes ':Zone.Identifier' on essentially every file, so at
             * info a directory browse would emit a line per file -- unbounded,
             * client-driven log volume carrying a client-controlled path.  The
             * sweep raises the level itself via SMBTORTURE_LOG_LEVEL. */
            chimera_smb_debug(
                "named streams: disabled by config; rejecting stream CREATE '%.*s'",
                request->create.name_len, request->create.name);
            chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_INVALID);
            return;
        }

        const char *sname               = NULL;
        uint16_t    base_len            = request->create.name_len;
        uint16_t    sname_len           = 0;
        uint8_t     has_stream          = 0;
        uint8_t     explicit_data_fork  = 0;
        uint8_t     explicit_index_fork = 0;
        uint32_t    pstatus             = chimera_smb_parse_stream_name(
            request->create.name, request->create.name_len,
            &base_len, &sname, &sname_len, &has_stream, &explicit_data_fork,
            &explicit_index_fork);

        if (pstatus != SMB2_STATUS_SUCCESS) {
            chimera_smb_complete_request(request, pstatus);
            return;
        }

        /* "DNAME::$DATA" names the default data fork explicitly; "DNAME::
         * $INDEX_ALLOCATION" names a directory's index stream.  Remember which
         * so the open-completion path can reject the data form on a directory
         * and the index form on a non-directory. */
        request->create.explicit_data_fork  = explicit_data_fork;
        request->create.explicit_index_fork = explicit_index_fork;

        /* A wildcard/invalid character in the BASE file name of a stream path
         * is rejected with OBJECT_NAME_INVALID (smb2.streams.names "stream*").
         * Wildcards are permitted in the stream name itself ("?Stream*"), so
         * this only screens the base component, not the stream suffix. */
        for (uint16_t bi = 0; bi < base_len; bi++) {
            char bc = request->create.name[bi];
            if (bc == '*' || bc == '?' || bc == '<' || bc == '>' ||
                bc == '|' || bc == '"') {
                chimera_smb_complete_request(request,
                                             SMB2_STATUS_OBJECT_NAME_INVALID);
                return;
            }
        }

        /* Trim the final component to the base file; the stream (if any) is
         * opened after the base file via chimera_vfs_open_stream. */
        request->create.name_len       = base_len;
        request->create.name[base_len] = '\0';

        if (has_stream) {
            /* A named stream is never a directory; a CREATE that names a stream
             * and also requests FILE_DIRECTORY_FILE is invalid
             * (smb2.streams.dir expects NOT_A_DIRECTORY). */
            if (request->create.create_options & SMB2_FILE_DIRECTORY_FILE) {
                chimera_smb_complete_request(request,
                                             SMB2_STATUS_NOT_A_DIRECTORY);
                return;
            }

            request->create.has_stream      = 1;
            request->create.stream_name_len = sname_len;
            memcpy(request->create.stream_name, sname, sname_len);
        }
    }

    /* A durable-handle reconnect (DH2C/DHnC) reclaims an already-open handle and
     * ignores the create fields entirely -- CreateDisposition, file name, access
     * and the rest are not interpreted (MS-SMB2 3.3.5.9.7/.12) -- so the field
     * validations below must not run for it; it is dispatched straight to
     * chimera_smb_durable_reconnect.  Gated on the persistent-handles config:
     * that knob enables chimera's whole durable/resilient reconnect machinery
     * (durable grants, the registry, park/reclaim).  With it off, durable
     * contexts are not granted, so a DH2C/DHnC carries no reclaimable handle and
     * is treated as an ordinary create (the context is ignored, per 3.3.5.9). */
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
    request->create.persist_record_written = false;
    request->create.persist_fh_len = 0;
    request->create.reconnect   = 0;
    request->create.replay      = 0;
    /* Default share-conflict status; an AppInstanceId version reject overrides
     * it with STATUS_FILE_FORCED_CLOSED inside gen_open_file. */
    request->create.force_close_status = SMB2_STATUS_SHARING_VIOLATION;

    if (request->tree->type == CHIMERA_SMB_TREE_TYPE_PIPE) {

        if (strcasecmp(request->create.name, "lsarpc") == 0) {
            pipe_magic = CHIMERA_SMB_OPEN_FILE_LSA_RPC;
            transceive = chimera_smb_lsarpc_transceive;
        } else if (strcasecmp(request->create.name, "srvsvc") == 0) {
            pipe_magic = CHIMERA_SMB_OPEN_FILE_SRV_RPC;
            transceive = chimera_smb_srvsvc_transceive;
        } else if (strcasecmp(request->create.name, "samr") == 0) {
            pipe_magic = CHIMERA_SMB_OPEN_FILE_SAMR_RPC;
            transceive = chimera_smb_samr_transceive;
        } else if (strcasecmp(request->create.name, "wkssvc") == 0) {
            pipe_magic = CHIMERA_SMB_OPEN_FILE_WKSSVC_RPC;
            transceive = chimera_smb_wkssvc_transceive;
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

        /* Drop the create-time reference (gen_open_file sets refcnt=2: one for
         * the tree's open_files hash, one held by the originating request).  The
         * file path drops it in chimera_smb_create_finish; the pipe path
         * completes inline, so release it here.  Without this the open stays at
         * refcnt 1 after the single decrement in the disconnect/tree teardown
         * sweep and is never freed -- a leak per pipe open that smbtorture's RPC
         * suites (which disconnect without an explicit CLOSE) expose. */
        chimera_smb_open_file_release(request, open_file);

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

        /* MS-SMB2 2.2.13 / MS-FSA CreateOptions validation (smb2.create.gentest).
        * An undefined high CreateOptions bit is STATUS_INVALID_PARAMETER; a
        * defined-but-unsupported one (TREE_CONNECTION / OPEN_BY_FILE_ID /
        * RESERVE_OPFILTER) is STATUS_NOT_SUPPORTED.  INVALID takes precedence. */
        /* MS-SMB2 2.2.13: ShareAccess is built only from FILE_SHARE_READ/WRITE/
         * DELETE; any other (reserved) bit set is STATUS_INVALID_PARAMETER. */
        if (request->create.share_access &
            ~(SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE)) {
            chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
            return;
        }

        if (request->create.create_options & SMB2_CREATE_OPTIONS_INVALID_MASK) {
            chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
            return;
        }
        if (request->create.create_options & SMB2_CREATE_OPTIONS_UNSUPPORTED_MASK) {
            chimera_smb_complete_request(request, SMB2_STATUS_NOT_SUPPORTED);
            return;
        }

        /* MS-SMB2 3.3.5.9 / MS-FSA 2.1.5.1: FILE_DIRECTORY_FILE and
         * FILE_NON_DIRECTORY_FILE are mutually exclusive; both set together is
         * STATUS_INVALID_PARAMETER. */
        if ((request->create.create_options & SMB2_FILE_DIRECTORY_FILE) &&
            (request->create.create_options & SMB2_FILE_NON_DIRECTORY_FILE)) {
            chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
            return;
        }

        /* MS-FSA 2.1.5.1 Phase 1 (MS-FSA_R2374 / R2376): a CreateOptions of
         * FILE_DIRECTORY_FILE combined with a destructive CreateDisposition is
         * a contradiction -- SUPERSEDE/OVERWRITE/OVERWRITE_IF replace the data
         * of a target that, as a directory, has none -- so the open MUST fail
         * with STATUS_INVALID_PARAMETER.  This validation runs in Phase 1,
         * BEFORE the open reaches the backend and resolves a directory-vs-file
         * type mismatch (which would otherwise surface as NOT_A_DIRECTORY and
         * mask the parameter error).  FSAModel CreateFileTestCaseS24/S26,
         * OpenFileTestCaseS22/S24. */
        if ((request->create.create_options & SMB2_FILE_DIRECTORY_FILE) &&
            (request->create.create_disposition == SMB2_FILE_SUPERSEDE ||
             request->create.create_disposition == SMB2_FILE_OVERWRITE ||
             request->create.create_disposition == SMB2_FILE_OVERWRITE_IF)) {
            chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
            return;
        }

        /* MS-FSCC 2.6 FileAttributes validation (smb2.create.gentest).  Any bit
        * outside the settable/valid set -- e.g. FILE_ATTRIBUTE_VOLUME (0x08) or
        * FILE_ATTRIBUTE_DEVICE (0x40) -- is rejected with INVALID_PARAMETER. */
        if (request->create.file_attributes & ~SMB2_CREATE_FILE_ATTR_VALID_MASK) {
            chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
            return;
        }

        /* MS-SMB2 2.2.13: ImpersonationLevel must be one of the four defined
         * SECURITY_IMPERSONATION_LEVEL values (0..3); anything else is
         * STATUS_BAD_IMPERSONATION_LEVEL (smb2.create.impersonation).  A durable
         * reconnect carries a don't-care sentinel and was already dispatched
         * above, so this only applies to a fresh open. */
        if (request->create.impersonation_level > SMB2_IMPERSONATION_DELEGATE) {
            chimera_smb_complete_request(request,
                                         SMB2_STATUS_BAD_IMPERSONATION_LEVEL);
            return;
        }

        /* MS-FSA 2.1.5.1 Phase 1 - Parameter Validation (MS-FSA_R376): when
         * CreateOptions carries FILE_NO_INTERMEDIATE_BUFFERING the server MUST
         * clear FILE_APPEND_DATA from DesiredAccess (unbuffered I/O cannot
         * append).  Strip it before the zero-DesiredAccess check so the residual
         * mask is what gets validated (FSAModel OpenFileTestCaseS28). */
        if (request->create.create_options & SMB2_FILE_NO_INTERMEDIATE_BUFFERING) {
            request->create.desired_access &= ~SMB2_FILE_APPEND_DATA;
        }

        /* MS-FSA 2.1.5.1 Phase 1 - Parameter Validation (MS-FSA_R377): a
         * DesiredAccess of zero is not a valid open and MUST be failed with
         * STATUS_ACCESS_DENIED (FSAModel OpenFileTestCaseS0/S28). */
        if (request->create.desired_access == 0) {
            chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
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

/* Reserve constructor identity before any backend handle acquisition. A
 * contending legacy request holds no namespace/ACCESS fence while parked. */
static uint32_t
smb_create_admission_begin(struct chimera_smb_request *request)
{
    if (request->tree->type == CHIMERA_SMB_TREE_TYPE_PIPE) { return SMB2_STATUS_SUCCESS; }
    if (!request->namespace_open_token) {
        request->namespace_open_token = calloc(1, sizeof(*request->namespace_open_token));
        if (!request->namespace_open_token) { return SMB2_STATUS_INSUFFICIENT_RESOURCES; }
    }
    if (!chimera_smb_namespace_open_begin(request->namespace_open_token,
            &request->compound->thread->shared->namespace_registry, request->compound)) {
        return SMB2_STATUS_PENDING;
    }
    uint32_t status = chimera_smb_lease_key_begin(request);
    if (status != SMB2_STATUS_SUCCESS) {
        chimera_smb_namespace_open_end(request->namespace_open_token);
    }
    return status;
}

static void
smb_create_admission_poll(struct evpl *evpl, struct evpl_timer *timer)
{
    struct chimera_smb_request *request = (void *) ((char *) timer -
        offsetof(struct chimera_smb_request, async.timer));
    struct chimera_smb_compound *wire = request->compound;
    uint32_t status;
    pthread_mutex_lock(&wire->thread->shared->sessions_lock);
    bool session_deleted = request->session_handle->session->flags & CHIMERA_SMB_SESSION_DELETED;
    pthread_mutex_unlock(&wire->thread->shared->sessions_lock);
    if (wire->conn->generation != wire->conn_generation || wire->conn->disconnecting ||
        request->tree->compound_tearing_down || session_deleted) {
        status = SMB2_STATUS_CANCELLED;
    } else {
        status = smb_create_admission_begin(request);
    }
    if (status == SMB2_STATUS_PENDING) {
        evpl_add_oneshot_timer(evpl, timer, smb_create_admission_poll, 1000);
        return;
    }
    request->create_admission_wait = false;
    chimera_smb_async_interim_cancel(request);
    if (status == SMB2_STATUS_SUCCESS) { smb_create_admitted(request); }
    else { chimera_smb_complete_request(request, status); }
}

SYMBOL_EXPORT void
chimera_smb_create(struct chimera_smb_request *request)
{
    /* Disconnect draining must not inspect recycled union bytes while waiting
     * before the ordinary CREATE handler has initialized its lifetime fields. */
    request->create.r_open_file = NULL;
    request->create.parent_handle = NULL;
    request->create.gen_parked = 0;
    request->create.break_waiter_counted = 0;
    request->create.pending_link = NULL;
    request->create.pending_linked = false;
    request->create.r_symlink_error = false;
    request->create.r_symlink_handle = NULL;
    if (!chimera_smb_request_pin_context(request)) {
        chimera_smb_complete_request(request, SMB2_STATUS_INSUFFICIENT_RESOURCES);
        return;
    }
    uint32_t status = smb_create_admission_begin(request);
    if (status == SMB2_STATUS_SUCCESS) { smb_create_admitted(request); return; }
    if (status != SMB2_STATUS_PENDING) {
        chimera_smb_complete_request(request, status);
        return;
    }
    request->create_admission_wait = true;
    chimera_smb_async_interim_begin(request);
    evpl_add_oneshot_timer(request->compound->thread->evpl, &request->async.timer,
        smb_create_admission_poll, 1000);
}

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
     * break is still outstanding; PARENT_LEASE_KEY_SET (and the echoed
     * ParentLeaseKey below) when the v2 open supplied a parent directory lease
     * key (MS-SMB2 2.2.14.2.11; dirlease.v2_request_parent checks this flag). */
    uint32_t lease_flags = of->lease_flags;

    if (v2) {
        uint64_t plk_lo, plk_hi;

        memcpy(&plk_lo, of->parent_lease_key, 8);
        memcpy(&plk_hi, of->parent_lease_key + 8, 8);
        if (plk_lo | plk_hi) {
            lease_flags |= SMB2_LEASE_FLAG_PARENT_LEASE_KEY_SET;
        }
    }
    out[20] = lease_flags & 0xff;
    out[21] = (lease_flags >> 8) & 0xff;
    out[22] = (lease_flags >> 16) & 0xff;
    out[23] = (lease_flags >> 24) & 0xff;
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

/* Build the QFid (SMB2_CREATE_QUERY_ON_DISK_ID) response body.  32 bytes:
 * DiskFileId(8) | VolumeId(8) | Reserved(16) (MS-SMB2 2.2.14.2.9).  The
 * DiskFileId must be the SAME unique file id the client also obtains via
 * FileInternalInformation (IndexNumber) and directory queries -- the VFS inode
 * number (va_ino).  The Linux kernel cifs client cross-checks the two: if the
 * QFid DiskFileId disagrees with the IndexNumber it re-resolves the open to a
 * fresh, empty inode after a write and loses the data (cthon basic test5: a
 * written file is then seen with size 0).  Use the marshaled inode number,
 * falling back to the handle hash only if it was somehow not reported.
 * VolumeId/Reserved are left zero. */
static int
build_qfid_response(
    struct chimera_smb_request *request,
    uint8_t                    *out,
    uint32_t                    out_size)
{
    struct chimera_smb_open_file *of = request->create.r_open_file;
    uint64_t                      disk_id;
    int                           i;

    if (out_size < 32 || !of || !of->handle) {
        return -1;
    }

    if (request->create.r_attrs.smb_attr_mask & SMB_ATTR_INODE) {
        disk_id = request->create.r_attrs.smb_ino;
    } else {
        disk_id = of->handle->fh_hash;
    }

    memset(out, 0, 32);
    for (i = 0; i < 8; i++) {
        out[i] = (disk_id >> (8 * i)) & 0xff;  /* DiskFileId, little-endian */
    }
    return 32;
} /* build_qfid_response */

/* *INDENT-OFF* */ /* uncrustify oscillates on aligned struct-init tables */
static const struct chimera_smb_create_response_emitter smb_create_response_emitters[] = {
    { CHIMERA_SMB_CREATE_CTX_MXAC, "MxAc", build_mxac_response  },
    { CHIMERA_SMB_CREATE_CTX_RQLS, "RqLs", build_rqls_response  },
    { CHIMERA_SMB_CREATE_CTX_DH2Q, "DH2Q", build_dh2q_response  },
    { CHIMERA_SMB_CREATE_CTX_DHNQ, "DHnQ", build_dhnq_response  },
    { CHIMERA_SMB_CREATE_CTX_QFID, "QFid", build_qfid_response  },
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

    /* A directory has no data stream: the CREATE response must report EndOfFile
     * and AllocationSize as 0 regardless of the backend's on-disk directory size
     * (memfs reports a block size).  Keyed on the DIRECTORY attribute so it holds
     * for an OPEN of an existing directory as well as a fresh mkdir (MS-SMB2
     * 3.3.5.9 SMB2_CREATE response; smbtorture dirlease.v2_request CHECK_CREATED). */
    if (request->create.r_attrs.smb_attributes & SMB2_FILE_ATTRIBUTE_DIRECTORY) {
        request->create.r_attrs.smb_size       = 0;
        request->create.r_attrs.smb_alloc_size = 0;
    }

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

    /* ctx_present_mask is stamped onto the open in
     * chimera_smb_create_after_share, before it is hashed -- the replay lookup
     * keys on it, so it cannot wait until the reply is marshaled.  A reclaimed
     * (durable rehome) open never runs through that path, so refresh it here
     * for the reconnect case; it is the same value either way. */
    if (request->create.r_open_file) {
        request->create.r_open_file->ctx_present_mask = request->create.ctx_present_mask;
    }

} /* chimera_smb_create_reply */

/* CREATE-context request handlers. Each fills typed fields in request->create.
 * NULL handler = presence-only (the bit in ctx_present_mask is all that matters
 * for Phase 0; Phase 1/3 will add real semantics for the open-after-CREATE step). */

static bool
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
    return true;
} /* parse_ctx_secd */

static bool
parse_ctx_exta(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request)
{
    /* Context input is retained by the wire request through every attempt,
     * accepted output formatting, parser failure and discovered redispatch. */
    if (request->flags & CHIMERA_SMB_REQUEST_FLAG_CREATE_EA_OWNED) {
        free(request->create.ea_overflow);
        request->flags &= ~CHIMERA_SMB_REQUEST_FLAG_CREATE_EA_OWNED;
    }
    request->create.ea_overflow = NULL;
    request->create.ea_buf_len = 0;
    if (data_len > CHIMERA_SMB_MAX_TRANSACT_SIZE) {
        request->status = SMB2_STATUS_EA_TOO_LARGE;
        request->flags |= CHIMERA_SMB_REQUEST_FLAG_PARSE_FAILED;
        return false;
    }
    if (data_len > sizeof(request->create.ea_buf)) {
        request->create.ea_overflow = malloc(data_len);
        if (!request->create.ea_overflow) {
            request->status = SMB2_STATUS_INSUFFICIENT_RESOURCES;
            request->flags |= CHIMERA_SMB_REQUEST_FLAG_PARSE_FAILED;
            return false;
        }
        request->flags |= CHIMERA_SMB_REQUEST_FLAG_CREATE_EA_OWNED;
    }
    if (data_len) {
        memcpy(request->create.ea_overflow ? request->create.ea_overflow : request->create.ea_buf,
            data, data_len);
    }
    request->create.ea_buf_len = data_len;
    return true;
} /* parse_ctx_exta */

static bool
parse_ctx_dhnc(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request)
{
    /* DHnC body: 16-byte SMB2_FILEID (persistent | volatile) + 16 reserved bytes. */
    if (data_len < 16) {
        return false;
    }
    request->create.dhnc.persistent  = smb_wire_le64(data);
    request->create.dhnc.volatile_id = smb_wire_le64(data + 8);
    return true;
} /* parse_ctx_dhnc */

static bool
parse_ctx_dh2q(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request)
{
    /* DH2Q body: Timeout(4) | Flags(4) | Reserved(8) | CreateGuid(16) = 32 bytes. */
    if (data_len < 32) {
        return false;
    }
    request->create.dh2q.timeout_ms = smb_wire_le32(data);
    request->create.dh2q.flags      = smb_wire_le32(data + 4);
    memcpy(request->create.dh2q.create_guid, data + 16, 16);
    return true;
} /* parse_ctx_dh2q */

static bool
parse_ctx_dh2c(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request)
{
    /* DH2C body: FileId(16) | CreateGuid(16) | Flags(4) = 36 bytes. */
    if (data_len < 36) {
        return false;
    }
    request->create.dh2c.persistent  = smb_wire_le64(data);
    request->create.dh2c.volatile_id = smb_wire_le64(data + 8);
    memcpy(request->create.dh2c.create_guid, data + 16, 16);
    request->create.dh2c.flags = smb_wire_le32(data + 32);
    return true;
} /* parse_ctx_dh2c */

static bool
parse_ctx_rqls(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request)
{
    /* RqLs v1 body (32 bytes): LeaseKey(16) | LeaseState(4) | LeaseFlags(4) | LeaseDuration(8 reserved).
     * RqLs v2 body (52 bytes): adds ParentLeaseKey(16) | Epoch(2) | Reserved(2).
     * Differentiate by data_len. Other sizes: malformed; reject so the caller
     * does not set the RQLS present-mask bit over stale pooled fields (#1116). */
    if (data_len != 32 && data_len != 52) {
        return false;
    }
    memcpy(request->create.rqls.key, data, 16);
    request->create.rqls.state = smb_wire_le32(data + 16);
    request->create.rqls.flags = smb_wire_le32(data + 20);
    /* The lease-v2 context (ParentLeaseKey/Epoch) is only valid on an SMB 3.x
     * dialect (MS-SMB2 2.2.13.2.10 / 3.3.5.9.11).  On 2.0.2 / 2.1 a 52-byte RqLs
     * must be processed as a v1 lease -- its v1 fields are a prefix of the v2
     * body, so we keep LeaseKey/State/Flags and drop the v2 tail, answering with
     * a 32-byte v1 response and tracking an unversioned lease (#1281). */
    /* request->compound is NULL when the parser is exercised standalone by
     * phase0_contexts_test; treat that as v2-capable (canonical) like the
     * other handlers do. */
    if (data_len == 52 &&
        (!request->compound ||
         request->compound->conn->dialect >= SMB2_DIALECT_3_0)) {
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
    return true;
} /* parse_ctx_rqls */

static bool
parse_ctx_alsi(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request)
{
    if (data_len < 8) {
        return false;
    }
    request->create.alsi_alloc_size = smb_wire_le64(data);
    return true;
} /* parse_ctx_alsi */

static bool
parse_ctx_twrp(
    const uint8_t              *data,
    uint32_t                    data_len,
    struct chimera_smb_request *request)
{
    if (data_len < 8) {
        return false;
    }
    request->create.twrp_timestamp = smb_wire_le64(data);
    return true;
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

/* Returns true when the context body was well-formed and its typed fields were
 * populated.  A malformed body (wrong DataLength) returns false so the caller
 * leaves the present-mask bit CLEAR -- otherwise a stale lease key / create
 * GUID left in the pooled request slot by a prior CREATE would be honored
 * (#1116). */
typedef bool (*chimera_smb_create_ctx_handler_t)(
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
    { "ExtA", CHIMERA_SMB_CREATE_CTX_EXTA, parse_ctx_exta },
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

        /* avail > 0 (loop condition) and >= 16 (header check above).  All bounds
         * tests are written as subtractions against avail so a hostile 32-bit
         * length/offset cannot wrap an addition past the check. */
        uint32_t avail = buf_len - pos;

        if (name_off > avail || name_len > avail - name_off) {
            chimera_smb_error("CREATE-context name out of bounds (pos=%u, name_off=%u, name_len=%u)",
                              pos, name_off, name_len);
            request->status = SMB2_STATUS_INVALID_PARAMETER;
            return -1;
        }

        /* A create-context name (tag) is a 4-byte identifier.  Windows rejects a
         * 1-3 byte name as malformed (>=4 is accepted; an unknown 4+ byte tag is
         * simply ignored).  Fail the whole CREATE with INVALID_PARAMETER, but
         * gracefully (PARSE_FAILED, not a connection teardown) so the client
         * gets an in-order error -- smb2.create.blob bad-tag-length 1/2/3. */
        if (name_len > 0 && name_len < 4) {
            request->status = SMB2_STATUS_INVALID_PARAMETER;
            request->flags |= CHIMERA_SMB_REQUEST_FLAG_PARSE_FAILED;
            return 0;
        }

        if (data_len > 0) {
            if (data_off == 0 || data_off > avail || data_len > avail - data_off) {
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

                    /* Run the body handler first.  A malformed body (wrong
                     * DataLength) returns false: leave the present-mask bit
                     * CLEAR so a stale lease key / create GUID left in this
                     * pooled request slot by a prior CREATE is not honored
                     * (#1116).  Presence-only contexts (NULL handler) always set
                     * the bit. */
                    if (p->handler && !p->handler(data, data_len, request)) {
                        break;
                    }

                    /* The base RQLS bit must be set for v2 too — gen_open_file
                     * keys lease handling off it.  RQLS_V2 tracks whether a
                     * versioned (epoch-bearing) lease was actually accepted; that
                     * is gated on the SMB 3.x dialect inside parse_ctx_rqls
                     * (#1281), so key off rqls.is_v2, not the raw 52-byte body. */
                    request->create.ctx_present_mask |= p->mask_bit;
                    if (p->mask_bit == CHIMERA_SMB_CREATE_CTX_RQLS &&
                        request->create.rqls.is_v2) {
                        request->create.ctx_present_mask |= CHIMERA_SMB_CREATE_CTX_RQLS_V2;
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

        if (next < 16 || (next & 0x7) != 0 || next > avail) {
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
    uint16_t name16[SMB_PATH_MAX];
    int      name_size;
    char    *slash;

    request->create.ea_overflow = NULL;
    request->create.ea_buf_len = 0;

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

    /* Create-context outputs default to "absent".  The request struct is pooled
     * and reused, and these are only written when their context is present, so
     * reset them here for every CREATE or a prior request's value would leak
     * (e.g. a stale TWRP would spuriously fail an unrelated open). */
    request->create.twrp_timestamp  = 0;
    request->create.alsi_alloc_size = 0;

    /* SMB2 CREATE fixed body (after the already-consumed 2-byte StructureSize):
     * SecurityFlags(1) | RequestedOplockLevel(1) | ImpersonationLevel(4) | ...
     * SecurityFlags is reserved (clients send 0) but MUST be consumed; the
     * aligning cursor would otherwise swallow RequestedOplockLevel as padding
     * before the uint32 ImpersonationLevel read, leaving oplock level always 0. */
    uint8_t security_flags;
    int     prc = 0;
    prc |= evpl_iovec_cursor_try_get_uint8(request_cursor, &security_flags);
    prc |= evpl_iovec_cursor_try_get_uint8(request_cursor, &request->create.requested_oplock_level);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->create.impersonation_level);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->create.flags);
    prc |= evpl_iovec_cursor_try_skip(request_cursor, 8);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->create.desired_access);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->create.file_attributes);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->create.share_access);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->create.create_disposition);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->create.create_options);
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &name_offset);
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &request->create.name_len);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &blob_offset);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &blob_length);

    if (unlikely(prc)) {
        chimera_smb_error("Received SMB2 CREATE request truncated in fixed body");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

    /* ImpersonationLevel is range-validated (0..3 -> else
     * STATUS_BAD_IMPERSONATION_LEVEL) in the dispatcher chimera_smb_create, after
     * the durable-reconnect short-circuit (a reconnect carries a don't-care
     * sentinel and must not be validated). */

    /* NameLength is the whole path (relative to the share root), so it is
     * bounded by the full-path limit, not the per-component one. name16 holds
     * SMB_PATH_MAX UTF-16 code units (2*SMB_PATH_MAX bytes). */
    if (request->create.name_len > 2 * SMB_PATH_MAX) {
        chimera_smb_error("Create request: UTF-16 name too long (%u bytes)",
                          request->create.name_len);
        request->status = SMB2_STATUS_NAME_TOO_LONG;
        return -1;
    }

    if (unlikely(evpl_iovec_cursor_try_copy(request_cursor, name16, request->create.name_len) != 0)) {
        chimera_smb_error("Received SMB2 CREATE request with name running past message");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

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

    /* Split the final path component off at the last backslash.  A named-stream
     * suffix (everything from the first ':' onward) is part of the final
     * component and may itself contain backslashes (e.g. an invalid stream name
     * "Stream\"); those must not be treated as path separators, or the parent
     * lookup would fail with OBJECT_PATH_NOT_FOUND instead of the stream-name
     * validation returning OBJECT_NAME_INVALID (smb2.streams.names2).  So limit
     * the backslash search to the base portion before the first ':'. */
    {
        char *colon = memchr(request->create.parent_path, ':',
                             request->create.parent_path_len);
        if (colon) {
            *colon = '\0';
            slash  = rindex(request->create.parent_path, '\\');
            *colon = ':';
        } else {
            slash = rindex(request->create.parent_path, '\\');
        }
    }

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

    /* The wire path buffer is larger than the open/namespace basename buffers.
     * Validate the base component before either constructor can copy it;
     * named-stream suffixes have their own separate validation below. */
    {
        const char *colon = memchr(request->create.name, ':', request->create.name_len);
        size_t base_len = colon ? (size_t) (colon - request->create.name) : request->create.name_len;
        if (base_len > SMB_FILENAME_MAX) {
            return chimera_smb_parse_reject(request, SMB2_STATUS_NAME_TOO_LONG);
        }
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
    /* issue_open adds ARCHIVE with |= even when this CREATE supplies no DOS
     * bits. Do not copy a previous pooled request's READONLY/HIDDEN/etc. */
    request->create.set_attr.va_dos_attributes = 0;
    request->create.ctx_present_mask           = 0;
    request->create.ea_buf_len                 = 0;

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

    if (blob_offset > 0 && blob_length > 0) {
        uint8_t  inline_buf[1024];
        uint8_t *ctx_buf  = inline_buf;
        uint8_t *heap_buf = NULL;
        int      crc;

        /* The context chain must fit within a single request, which the
         * transport already capped at the negotiated MaxTransactSize; a larger
         * declared length is malformed.  Reject it rather than truncating
         * (STATUS_INVALID_PARAMETER). */
        if (blob_length > CHIMERA_SMB_MAX_TRANSACT_SIZE) {
            chimera_smb_error("CREATE create-context blob too large (%u)", blob_length);
            return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
        }

        /* A chain larger than the inline buffer is heap-allocated so every
         * context (durable/lease/SD/EA/...) is still parsed.  Previously such a
         * CREATE had ALL its contexts silently dropped -- the client believed it
         * held a durable lease the server never granted (#1115). */
        if (blob_length > sizeof(inline_buf)) {
            heap_buf = malloc(blob_length);
            if (!heap_buf) {
                return chimera_smb_parse_reject(request,
                                                SMB2_STATUS_INSUFFICIENT_RESOURCES);
            }
            ctx_buf = heap_buf;
        }

        /* Seek to the client-declared create-context offset (validated to be
         * forward and within this request's window) and pull the blob with the
         * bounds-checked reader; a malformed offset/length rejects cleanly
         * rather than driving an out-of-bounds skip or read. */
        if (unlikely(smb_cursor_seek_to(request_cursor, blob_offset) != 0)) {
            free(heap_buf);
            chimera_smb_error("CREATE create-context offset %u out of range", blob_offset);
            return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
        }

        if (evpl_iovec_cursor_try_copy(request_cursor, ctx_buf, blob_length) == 0) {
            crc = chimera_smb_parse_create_contexts(ctx_buf, blob_length, request);
            free(heap_buf);
            if (crc < 0) {
                return -1;
            }
        } else {
            free(heap_buf);
        }
    }

    /* Leasing was introduced in SMB 2.1.  Per MS-SMB2 3.3.5.9, if the negotiated
     * dialect does not support leasing (i.e. it is "2.0.2"), the server MUST
     * ignore the "RqLs" create context: it acquires no lease and emits no lease
     * response context (ReturnLeaseContextNotIncluded).  Drop the RQLS request
     * bits here so neither the lease-acquisition path nor the response-context
     * emitter acts on them.  (The standalone context-parser unit test has no
     * connection, so this dialect gate lives in the wire-parse path, not the
     * shared parser.) */
    if (request->compound && request->compound->conn &&
        request->compound->conn->dialect < SMB2_DIALECT_2_1) {
        request->create.ctx_present_mask &= ~(CHIMERA_SMB_CREATE_CTX_RQLS |
                                              CHIMERA_SMB_CREATE_CTX_RQLS_V2);
    }

    return 0;
} /* chimera_smb_parse_create */

/* A CREATE owns a private open slot until the VFS accepts the whole batch.
 * The private hash table reserves the only storage registry insertion can
 * require, so accepted publication cannot fail after filesystem commit. */
#include "smb_compound.h"
#include "smb_doc_compound.h"

struct smb_create_recall {
    struct smb_vfs_command *command;
    struct chimera_vfs_compound *compound;
    struct evpl_timer timer;
    uint64_t token;
    uint64_t fh_hash;
    struct timespec deadline;
    uint8_t fh[CHIMERA_VFS_FH_SIZE];
    uint32_t fh_len;
    unsigned int phase;
};

struct smb_create_ea_step {
    struct smb_vfs_command *command;
    struct chimera_smb_ea_entry entry;
    uint32_t error;
    uint32_t canonical_len;
    char canonical[CHIMERA_VFS_COMPOUND_NAME_MAX + 1];
    bool applied;
};

struct smb_create_attempt {
    struct chimera_smb_open_file reply_open;
    /* Reply RqLs version survives a suffix CLOSE retiring the live grant. */
    struct chimera_vfs_claim_grant reply_grant;
    struct chimera_vfs_open_handle reply_handle;
    struct chimera_smb_open_file *reserved_hash;
    /* Unlinked storage only until accepted publication; retries never expose
     * tentative caching rights or a grant member. */
    struct chimera_vfs_claim_grant *cache_candidate;
    /* Input policy before asynchronous cache recall settles. A later ACK
     * must not turn a same-client H-lease refusal into a new legacy grant. */
    uint8_t legacy_cache_cap;
    struct chimera_vfs_attrs set_attr;
    struct chimera_vfs_attrs overwrite_attr;
    struct chimera_smb_namespace_path overwrite_path;
    struct chimera_smb_attrs attrs;
    struct smb_create_ea_step *ea_steps;
    unsigned int ea_count;
    int ea_budget_op;
    int ea_list;
    bool ea_list_valid;
    int parent_op;
    int open_op;
    int reserve_op;
    int base_op;
    int base_reserve_op;
    bool stream;
    bool base_created;
    char symlink_target[CHIMERA_VFS_PATH_MAX];
    unsigned int symlink_length;
    uint16_t symlink_unparsed;
    int created;
    int overwritten;
    uint8_t held_used;
    uint8_t held_denied;
    uint8_t root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t root_fh_len;
    int access_pending;
    unsigned int access_retries;
    struct smb_create_recall recall[2];
};

static bool
smb_create_data_open(struct chimera_smb_request *request)
{
    return chimera_smb_disposition_overwrites(request->create.create_disposition) ||
        ((request->create.create_disposition == SMB2_FILE_OPEN ||
            request->create.create_disposition == SMB2_FILE_OPEN_IF) &&
        (smb_create_desired_access(request) &
         ~(SMB2_FILE_READ_ATTRIBUTES | SMB2_FILE_WRITE_ATTRIBUTES |
           SMB2_READ_CONTROL | SMB2_SYNCHRONIZE)));
}

/* Only unique legacy oplocks are opportunistically published here. A lease
 * key may join an existing grant even when requesting NONE, and durable grants
 * need an independently reserved registry entry and accepted persistence. */
static bool
smb_create_wants_legacy_cache(struct chimera_smb_request *request)
{
    return request->compound->thread->shared->config.oplocks &&
        !(request->create.create_options & SMB2_FILE_DIRECTORY_FILE) &&
        (request->create.requested_oplock_level == SMB2_OPLOCK_LEVEL_II ||
         request->create.requested_oplock_level == SMB2_OPLOCK_LEVEL_EXCLUSIVE ||
         request->create.requested_oplock_level == SMB2_OPLOCK_LEVEL_BATCH);
}

/* Lease requests use repeatable recall coordination and publish preallocated
 * grants only after acceptance. Actual directory identity selects DIR_LEASE
 * admission; parent-key notification exemptions publish only after acceptance.
 * Durable lifecycle remains a separate boundary. */
static bool
smb_create_lease_request(struct chimera_smb_request *request)
{
    return (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) &&
        request->create.requested_oplock_level == SMB2_OPLOCK_LEVEL_LEASE;
}

/* Match legacy directory policy using the actual inode type. A hintless OPEN
 * can discover an unsupported directory lease only during execution. */
static bool
smb_create_directory_lease_supported(struct chimera_smb_request *request)
{
    return request->compound->thread->shared->config.directory_leases &&
        request->compound->conn->dialect >= SMB2_DIALECT_3_0 &&
        request->create.rqls.is_v2 &&
        !chimera_smb_disposition_overwrites(request->create.create_disposition);
}

/* No tentative grant is installed. A restricted suffix can operate through
 * this private ACCESS owner while cache admission remains accepted-only.
 * Commands which inspect/change grant lifecycle still require publication. */
static bool
smb_create_private_suffix_eligible(struct smb_vfs_command *producer,
                                   struct chimera_smb_request *next)
{
    if (!smb_create_wants_legacy_cache(producer->request) &&
        !smb_create_lease_request(producer->request)) { return true; }
    if (!(next->smb2_hdr.flags & SMB2_FLAGS_RELATED_OPERATIONS)) { return false; }

    const struct chimera_smb_file_id *id;
    switch (next->smb2_hdr.command) {
        case SMB2_QUERY_INFO: id = &next->query_info.file_id; break;
        case SMB2_READ: id = &next->read.file_id; break;
        case SMB2_FLUSH: id = &next->flush.file_id; break;
        case SMB2_WRITE:
            /* RqLs is coherent with its own writes, even at READ-only cache
             * level. A legacy LEVEL_II oplock instead self-breaks on WRITE;
             * its actual grant must be known before such a suffix executes. */
            if (!smb_create_lease_request(producer->request)) { return false; }
            id = &next->write.file_id;
            break;
        default:
            /* CLOSE needs the CREATE's cache reply and grant version/epoch
             * even if no final open survives; LOCK/namespace/another CREATE
             * need cache admission/displacement represented in the journal. */
            return false;
    }
    return id->pid == UINT64_MAX && id->vid == UINT64_MAX;
}

static struct chimera_claim_owner
smb_create_private_actor_owner(struct smb_vfs_command *producer)
{
    /* OPEN completion initializes this template before ready. Unlike the
     * public-open helper, preserve its requested RqLs key while grant is NULL:
     * a same-key reopen's suffix must exempt the already-existing grant. */
    return producer->open->share_lease.owner;
}

static int
smb_create_compound_eligible(struct chimera_smb_request *request)
{
    uint16_t base_len = request->create.name_len, stream_len = 0;
    const char *stream_name = NULL;
    uint8_t stream = 0, data_fork = 0, index_fork = 0;
    if (memchr(request->create.name, ':', request->create.name_len)) {
        if (!request->compound->thread->shared->config.named_streams ||
            chimera_smb_parse_stream_name(request->create.name, request->create.name_len,
                &base_len, &stream_name, &stream_len, &stream, &data_fork, &index_fork) != SMB2_STATUS_SUCCESS ||
            !stream || !base_len || stream_len > SMB_FILENAME_MAX ||
            (request->create.create_options & (SMB2_FILE_DIRECTORY_FILE | SMB2_FILE_OPEN_REPARSE_POINT))) {
            return 0;
        }
        for (uint16_t i = 0; i < base_len; i++) {
            if (strchr("*?<>|\"", request->create.name[i])) { return 0; }
        }
    }
    uint32_t access = smb_create_desired_access(request);
    bool create = request->create.create_disposition == SMB2_FILE_CREATE;
    bool open_if = request->create.create_disposition == SMB2_FILE_OPEN_IF;
    bool overwrite = chimera_smb_disposition_overwrites(request->create.create_disposition);
    /* Parked durable lifecycle is discovered after resolving the file and
     * becomes an explicit accepted-prefix boundary before admission. */
    bool stat_open = request->create.create_disposition == SMB2_FILE_OPEN &&
        !(access & ~(SMB2_FILE_READ_ATTRIBUTES | SMB2_FILE_WRITE_ATTRIBUTES |
                     SMB2_READ_CONTROL | SMB2_SYNCHRONIZE));
    return (create || open_if || overwrite || request->create.create_disposition == SMB2_FILE_OPEN) &&
           (!overwrite || !(request->create.create_options & SMB2_FILE_DIRECTORY_FILE)) &&
           (!(request->create.create_options & SMB2_FILE_DIRECTORY_FILE) ||
            !smb_create_lease_request(request) || smb_create_directory_lease_supported(request)) &&
           (request->create.requested_oplock_level == SMB2_OPLOCK_LEVEL_NONE ||
            request->create.requested_oplock_level == SMB2_OPLOCK_LEVEL_II ||
            request->create.requested_oplock_level == SMB2_OPLOCK_LEVEL_EXCLUSIVE ||
            request->create.requested_oplock_level == SMB2_OPLOCK_LEVEL_BATCH ||
            smb_create_lease_request(request)) &&
           (!stream || request->create.requested_oplock_level == SMB2_OPLOCK_LEVEL_NONE) &&
           /* DHnQ has no replay identity; without possible BATCH/H caching
            * its durable request is guaranteed to be declined. */
           (!(request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) ||
            (smb_create_lease_request(request) &&
             !(request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_DHNQ))) &&
           (!(request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_DHNQ) ||
            !request->compound->thread->shared->config.persistent_handles ||
            !smb_create_wants_legacy_cache(request) ||
            request->create.requested_oplock_level != SMB2_OPLOCK_LEVEL_BATCH) &&
           !(request->create.ctx_present_mask &
             ~(CHIMERA_SMB_CREATE_CTX_MXAC | CHIMERA_SMB_CREATE_CTX_QFID |
               CHIMERA_SMB_CREATE_CTX_ALSI | CHIMERA_SMB_CREATE_CTX_SECD |
               CHIMERA_SMB_CREATE_CTX_EXTA | CHIMERA_SMB_CREATE_CTX_DHNQ |
               CHIMERA_SMB_CREATE_CTX_RQLS | CHIMERA_SMB_CREATE_CTX_RQLS_V2)) &&
           base_len <= SMB_FILENAME_MAX &&
           (request->create.name_len > 0 || (stat_open && !request->create.parent_path_len)) &&
           request->create.parent_path_len + request->create.name_len +
               (request->create.parent_path_len != 0) < SMB_PATH_MAX &&
           !chimera_smb_path_has_dotdot(request->create.parent_path, request->create.parent_path_len) &&
           !(request->create.name_len == 1 && request->create.name[0] == '.') &&
           !(request->create.name_len == 2 && request->create.name[0] == '.' && request->create.name[1] == '.') &&
           !(request->create.create_options &
             (SMB2_FILE_DELETE_ON_CLOSE | SMB2_FILE_OPEN_REQUIRING_OPLOCK |
              SMB2_CREATE_OPTIONS_INVALID_MASK | SMB2_CREATE_OPTIONS_UNSUPPORTED_MASK)) &&
           !((request->create.create_options & SMB2_FILE_DIRECTORY_FILE) &&
             (request->create.create_options & SMB2_FILE_NON_DIRECTORY_FILE)) &&
           !(request->create.share_access &
             ~(SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE)) &&
           !(request->create.file_attributes & ~SMB2_CREATE_FILE_ATTR_VALID_MASK) &&
           request->create.impersonation_level <= SMB2_IMPERSONATION_DELEGATE &&
           request->create.desired_access != 0 &&
           request->tree && request->tree->share &&
           /* Cold persistent recovery still owns external registry changes. */
           !(request->compound->thread->shared->config.persistent_handles &&
             request->tree->share->continuous_availability &&
             !request->tree->share->durable_recovered);
}

static int
smb_create_compound_initialize(struct smb_vfs_command *command)
{
    struct chimera_smb_request *request = command->request;
    struct chimera_smb_open_file *open = calloc(1, sizeof(*open));
    struct smb_create_attempt *attempt = calloc(1, sizeof(*attempt));
    if (!open || !attempt) { free(open); free(attempt); return -1; }
    if (smb_create_wants_legacy_cache(request) || smb_create_lease_request(request)) {
        attempt->cache_candidate = calloc(1, sizeof(*attempt->cache_candidate));
        if (!attempt->cache_candidate) { free(open); free(attempt); return -1; }
    }
    command->open = open;
    command->private_data = attempt;
    attempt->legacy_cache_cap = CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW | CHIMERA_CLAIM_H;
    open->type = CHIMERA_SMB_OPEN_FILE_TYPE_FILE;
    open->file_id.pid = atomic_fetch_add(&request->compound->thread->shared->next_persistent_id, 1);
    open->file_id.vid = chimera_rand64();
    open->desired_access = smb_create_desired_access(request);
    open->share_access = request->create.share_access;
    open->ctx_present_mask = request->create.ctx_present_mask;
    if (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) {
        open->oplock_level = SMB2_OPLOCK_LEVEL_LEASE;
        open->lease_epoch = request->create.rqls.epoch;
        memcpy(open->lease_key, request->create.rqls.key, sizeof(open->lease_key));
        if (request->create.rqls.is_v2) {
            memcpy(open->parent_lease_key, request->create.rqls.parent_key,
                   sizeof(open->parent_lease_key));
        }
    }
    open->stream_delete_cred = request->session_handle->session->cred;
    open->channel_sequence = request->channel_sequence;
    open->channel_sequence_valid = 1;
    open->create_conn = request->compound->conn;
    open->tree = request->tree;
    open->refcnt = 1; /* Reply/slot reference; tree reference is acquired at publication. */
    open->create_action = request->create.create_disposition == SMB2_FILE_CREATE ?
                          SMB2_CREATE_ACTION_CREATED : SMB2_CREATE_ACTION_OPENED;
    open->name_len = request->create.name_len;
    if (memchr(request->create.name, ':', request->create.name_len)) {
        const char *stream_name = NULL;
        uint16_t base_len = request->create.name_len, stream_len = 0;
        uint8_t stream = 0, data_fork = 0, index_fork = 0;
        chimera_smb_parse_stream_name(request->create.name, request->create.name_len,
            &base_len, &stream_name, &stream_len, &stream, &data_fork, &index_fork);
        attempt->stream = true;
        open->flags |= CHIMERA_SMB_OPEN_FILE_FLAG_STREAM;
        open->name_len = base_len;
        open->stream_name_len = stream_len;
        memcpy(open->stream_name, stream_name, stream_len);
    }
    memcpy(open->name, request->create.name, open->name_len);
    open->full_path_len = request->create.parent_path_len;
    if (open->full_path_len) {
        memcpy(open->full_path, request->create.parent_path, open->full_path_len);
        chimera_smb_slash_forward_to_back(open->full_path, open->full_path_len);
        open->full_path[open->full_path_len++] = '\\';
    }
    memcpy(open->full_path + open->full_path_len, open->name, open->name_len);
    open->full_path_len += open->name_len;
    open->full_path[open->full_path_len] = '\0';
    open->parent_fh_len = request->tree->fh_len;
    memcpy(open->parent_fh, request->tree->fh, open->parent_fh_len);
    memcpy(open->client_guid, request->compound->conn->client_guid, sizeof(open->client_guid));
    if (!chimera_smb_open_namespace_prepare(request->compound->thread, open, attempt->stream)) {
        chimera_smb_open_namespace_detach(open);
        free(attempt->cache_candidate); free(open); free(attempt);
        command->open = NULL; command->private_data = NULL;
        return -1;
    }
    HASH_ADD(hh, attempt->reserved_hash, file_id, sizeof(open->file_id), open);
    attempt->set_attr = request->create.set_attr;
    attempt->set_attr.va_dos_attributes |= SMB2_FILE_ATTRIBUTE_ARCHIVE;
    attempt->set_attr.va_req_mask |= CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
    attempt->set_attr.va_set_mask |= CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
    if (request->create.alsi_alloc_size) {
        attempt->set_attr.va_alloc_size = request->create.alsi_alloc_size;
        attempt->set_attr.va_req_mask |= CHIMERA_VFS_ATTR_ALLOC_SIZE;
        attempt->set_attr.va_set_mask |= CHIMERA_VFS_ATTR_ALLOC_SIZE;
    }
    attempt->overwrite_attr = attempt->set_attr;
    attempt->overwrite_attr.va_size = 0;
    attempt->overwrite_attr.va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
    attempt->overwrite_attr.va_req_mask |= CHIMERA_VFS_ATTR_SIZE;
    request->create.r_symlink_error = 0;
    request->create.r_symlink_handle = NULL;
    request->create.reconnect = 0;
    request->create.replay = 0;
    request->create.r_open_file = NULL;
    request->create.parent_handle = NULL;
    request->create.gen_parked = 0;
    request->create.break_waiter_counted = 0;
    request->create.park_fh_len = 0;
    for (unsigned int i = 0; i < 2; i++) {
        attempt->recall[i].command = command;
        attempt->recall[i].phase = i + 1;
    }
    return 0;
}

static void
smb_create_compound_prepare(struct chimera_vfs_compound *compound, uint32_t index,
                            enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    (void) compound; (void) index; (void) status;
    if (!command->open->desired_access) {
        command->status = SMB2_STATUS_ACCESS_DENIED;
        return;
    }
    if ((command->open->desired_access & SMB2_ACCESS_SYSTEM_SECURITY) &&
        command->request->session_handle->session->cred.uid != 0) {
        command->status = SMB2_STATUS_PRIVILEGE_NOT_HELD;
    }
}

static void
smb_create_symlink_complete(struct chimera_vfs_compound *compound, uint32_t index,
                            enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);
    if (*status == CHIMERA_VFS_OK && op->target_len > 0 && op->target_len < CHIMERA_VFS_PATH_MAX) {
        memcpy(attempt->symlink_target, op->target, op->target_len);
        attempt->symlink_length = op->target_len;
    }
}

static void
smb_create_reserve_prepare(struct chimera_vfs_compound *compound, uint32_t index,
                           enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    (void) status;
    struct smb_create_attempt *attempt = command->private_data;
    if (command->status != SMB2_STATUS_SUCCESS || attempt->access_pending) {
        chimera_vfs_compound_op_skip(compound, index);
        return;
    }
    struct chimera_vfs_compound_op *args = chimera_vfs_compound_op_args(compound, index);
    if (args->type == CHIMERA_VFS_COMPOUND_OP_OPEN) {
        /* Reconstruct mutable flags on every attempt: a last holder may close
         * between retries, making a previously bound key unbound again. */
        if (index == (uint32_t) attempt->open_op ||
            (attempt->stream && index == (uint32_t) attempt->base_op)) {
            uint32_t disposition = command->request->create.create_disposition;
            args->open_flags &= ~(CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE);
            if (disposition != SMB2_FILE_OPEN && disposition != SMB2_FILE_OVERWRITE) {
                args->open_flags |= CHIMERA_VFS_OPEN_CREATE;
                if (!attempt->stream && disposition == SMB2_FILE_CREATE) {
                    args->open_flags |= CHIMERA_VFS_OPEN_EXCLUSIVE;
                }
            }
        }
        if (chimera_smb_lease_key_conflict(command->request, NULL, 0)) {
            args->open_flags &= ~(CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE);
        }
    }
    if (args->type == CHIMERA_VFS_COMPOUND_OP_COORDINATE) {
        /* Set execution arguments in prepare, including when the whole
         * pipeline was appended from the EA budget checkpoint. */
        args->coordinate_each_attempt = 1;
    }
    if (chimera_vfs_compound_op(compound, index)->type == CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS) {
        struct chimera_vfs_open_handle *handle =
            chimera_vfs_compound_op(compound, args->handle_from)->out_handle;
        if (handle && smb_doc_fh_pending(command, handle->fh, handle->fh_len, 0)) {
            command->status = SMB2_STATUS_DELETE_PENDING;
            *status = CHIMERA_VFS_EACCES;
        }
    }
}

/* Coordination can recall live caching rights, but parked durable eviction can
 * run DOC and must remain a separate accepted-prefix lifecycle boundary. */
static bool
smb_create_recall_blocked(struct smb_create_recall *recall, bool check_cache)
{
    struct chimera_server_smb_thread *thread = recall->command->request->compound->thread;
    struct chimera_vfs_state *state = thread->vfs_thread->vfs->vfs_state;
    struct chimera_vfs_file_state *file = chimera_vfs_state_get(state,
        recall->fh, recall->fh_len, recall->fh_hash, false);
    bool blocked = false;
    if (!file) { return false; }
    pthread_mutex_lock(&file->lock);
    for (struct chimera_vfs_claim *claim = file->claims[CHIMERA_CLAIM_CLASS_ACCESS];
         claim; claim = claim->next) {
        if (claim->parked) { blocked = true; break; }
    }
    for (struct chimera_vfs_claim *claim = file->claims[CHIMERA_CLAIM_CLASS_CACHE];
         !blocked && claim; claim = claim->next) {
        if (claim->parked || (check_cache &&
            ((recall->phase == 1 && claim->construct == CHIMERA_CONSTRUCT_OPLOCK_BATCH &&
              (claim->used & CHIMERA_CLAIM_H)) ||
             (recall->phase == 2 && (claim->used & CHIMERA_CLAIM_CW))))) {
            blocked = true;
        }
    }
    pthread_mutex_unlock(&file->lock);
    chimera_vfs_state_put(state, file);
    return blocked;
}

/* File lock held. This is the legacy per-client input policy, independent
 * of ordinary claim admission and the grant's later accepted live-state cap. */
static uint8_t
smb_create_client_cache_cap(struct chimera_vfs_file_state *file, uint64_t client_key)
{
    uint8_t cap = CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW | CHIMERA_CLAIM_H;
    for (struct chimera_vfs_claim *claim = file->claims[CHIMERA_CLAIM_CLASS_CACHE];
         claim; claim = claim->next) {
        if (claim->construct != CHIMERA_CONSTRUCT_RQLS ||
            claim->owner.client_key != client_key ||
            claim->break_state == CHIMERA_CLAIM_BREAK_REVOKED) { continue; }
        if (claim->used & CHIMERA_CLAIM_H) { return 0; }
        cap &= CHIMERA_CLAIM_CR;
    }
    return cap;
}

static void
smb_create_recall_prepare(struct chimera_vfs_compound *compound, uint32_t index,
                           enum chimera_vfs_error *status, void *private_data)
{
    struct smb_create_recall *recall = private_data;
    struct smb_vfs_command *command = recall->command;
    struct smb_create_attempt *attempt = command->private_data;
    smb_create_reserve_prepare(compound, index, status, command);
    if (command->status != SMB2_STATUS_SUCCESS || attempt->access_pending) { return; }
    struct chimera_vfs_open_handle *handle =
        chimera_vfs_compound_op(compound, attempt->open_op)->out_handle;
    if (!handle) { *status = CHIMERA_VFS_EBADF; return; }
    if (recall->fh_len != handle->fh_len || memcmp(recall->fh, handle->fh, handle->fh_len)) {
        recall->fh_len = handle->fh_len;
        memcpy(recall->fh, handle->fh, handle->fh_len);
        recall->fh_hash = handle->fh_hash;
    }
    if (recall->phase == 2 && smb_create_wants_legacy_cache(command->request)) {
        struct chimera_vfs_state *state = command->request->compound->thread->shared->vfs->vfs_state;
        struct chimera_vfs_file_state *file = chimera_vfs_state_get(state,
            handle->fh, handle->fh_len, handle->fh_hash, false);
        if (file) {
            pthread_mutex_lock(&file->lock);
            attempt->legacy_cache_cap = smb_create_client_cache_cap(file,
                command->request->session_handle->session->client_key);
            pthread_mutex_unlock(&file->lock);
            chimera_vfs_state_put(state, file);
        }
    }
    if (smb_create_recall_blocked(recall, false)) {
        if (attempt->created) {
            command->status = SMB2_STATUS_FILE_NOT_AVAILABLE;
            *status = CHIMERA_VFS_EBUSY;
        } else {
            chimera_smb_compound_defer(command);
            *status = CHIMERA_VFS_EINTR;
        }
    }
}

static void
smb_create_recall_settle(struct smb_create_recall *recall, enum chimera_vfs_error status)
{
    struct chimera_smb_request *request = recall->command->request;
    request->compound_coordinate_wait = false;
    chimera_smb_create_break_waiter_retire(request);
    /* Keep a sent AsyncId for the final response, but remove the request from
     * legacy CREATE sweeps throughout finish/retry and possible redispatch. */
    if (request->async.armed) { chimera_smb_async_interim_cancel(request); }
    chimera_vfs_compound_coordinate_done(recall->compound, recall->token, status);
}

/* Coordination may pin the requester's shared grant to exclude its existing
 * break from our wait. Same-key reopen reports BREAK_IN_PROGRESS immediately;
 * it must neither wait for its own ACK nor revoke its own lease on timeout. */
static bool
smb_create_recall_pending(struct smb_create_recall *recall, bool revoke)
{
    struct chimera_smb_request *request = recall->command->request;
    struct chimera_vfs_state *state = request->compound->thread->shared->vfs->vfs_state;
    struct chimera_vfs_file_state *file = NULL;
    struct chimera_vfs_claim_grant *own = NULL;
    if (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) {
        file = chimera_vfs_state_get(state, recall->fh, recall->fh_len, recall->fh_hash, false);
        if (file) {
            pthread_mutex_lock(&file->lock);
            for (struct chimera_vfs_claim_grant *g = file->grants; g; g = g->grant_next) {
                if ((g->claim.construct == CHIMERA_CONSTRUCT_RQLS ||
                     g->claim.construct == CHIMERA_CONSTRUCT_DIR_LEASE) &&
                    g->claim.owner.client_key == request->session_handle->session->client_key &&
                    !memcmp(g->claim.owner.key, request->create.rqls.key, 16)) {
                    own = chimera_vfs_claim_pin_grant(&g->claim);
                    break;
                }
            }
            pthread_mutex_unlock(&file->lock);
        }
    }
    bool pending = false;
    if (revoke) {
        chimera_vfs_claim_revoke_breaks(state, recall->fh, recall->fh_len, recall->fh_hash, own);
    } else {
        pending = chimera_vfs_claim_ack_pending(state, recall->fh, recall->fh_len, recall->fh_hash, own);
    }
    if (own) { chimera_vfs_claim_grant_release(state, own, false); }
    if (file) { chimera_vfs_state_put(state, file); }
    return pending;
}

static void
smb_create_recall_poll(struct evpl *evpl, struct evpl_timer *timer)
{
    struct smb_create_recall *recall = (void *) ((char *) timer - offsetof(struct smb_create_recall, timer));
    struct smb_vfs_command *command = recall->command;
    struct chimera_smb_request *request = command->request;
    struct chimera_smb_compound *wire = request->compound;
    if (wire->conn->generation != wire->conn_generation || wire->conn->disconnecting ||
        request->tree->compound_tearing_down ||
        (request->session_handle->session->flags & CHIMERA_SMB_SESSION_DELETED) ||
        chimera_vfs_compound_is_canceled(recall->compound)) {
        command->status = SMB2_STATUS_CANCELLED;
        smb_create_recall_settle(recall, CHIMERA_VFS_EINTR);
        return;
    }
    if (smb_create_recall_blocked(recall, false)) {
        struct smb_create_attempt *attempt = command->private_data;
        if (attempt->created) {
            command->status = SMB2_STATUS_FILE_NOT_AVAILABLE;
            smb_create_recall_settle(recall, CHIMERA_VFS_EBUSY);
        } else {
            chimera_smb_compound_defer(command);
            smb_create_recall_settle(recall, CHIMERA_VFS_EINTR);
        }
        return;
    }
    if (!smb_create_recall_pending(recall, false)) {
        smb_create_recall_settle(recall, CHIMERA_VFS_OK);
        return;
    }
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    if (chimera_timespec_cmp(&now, &recall->deadline) >= 0) {
        smb_create_recall_pending(recall, true);
        smb_create_recall_settle(recall, CHIMERA_VFS_OK);
        return;
    }
    if (!request->compound_coordinate_wait) {
        request->compound_coordinate_wait = true;
        memcpy(request->create.park_fh, recall->fh, recall->fh_len);
        request->create.park_fh_len = recall->fh_len;
        request->create.park_fh_hash = recall->fh_hash;
        chimera_smb_create_break_waiter_register(request);
        if (!request->async_id) { chimera_smb_async_interim_begin(request); }
        /* Same-thread break queues must drain while this compound is parked. */
        chimera_smb_lease_break_flush(wire->thread);
    }
    evpl_add_oneshot_timer(evpl, &recall->timer, smb_create_recall_poll, 1000);
}

static void
smb_create_recall_start(struct chimera_vfs_compound *compound, uint32_t index,
                       uint64_t token, const uint8_t *fh, uint32_t fh_len, void *private_data)
{
    struct smb_create_recall *recall = private_data;
    struct smb_vfs_command *command = recall->command;
    struct chimera_smb_request *request = command->request;
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_vfs_state *state = thread->vfs_thread->vfs->vfs_state;
    struct smb_create_attempt *attempt = command->private_data;
    struct chimera_claim_actor actor = {
        .owner = { .proto = CHIMERA_CLAIM_PROTO_SMB2,
            .client_key = request->session_handle->session->client_key,
            .owner_lo = command->open->file_id.pid, .owner_hi = command->open->file_id.vid },
        .op_handle = chimera_vfs_compound_op(compound, attempt->open_op)->out_handle,
    };
    if (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) {
        memcpy(actor.owner.key, request->create.rqls.key, 16);
        memcpy(&actor.owner.owner_lo, request->create.rqls.key, 8);
        memcpy(&actor.owner.owner_hi, request->create.rqls.key + 8, 8);
    }
    (void) index; (void) fh; (void) fh_len;
    recall->compound = compound;
    recall->token = token;
    clock_gettime(CLOCK_MONOTONIC, &recall->deadline);
    recall->deadline.tv_sec += state->default_break_deadline_ms / 1000;
    recall->deadline.tv_nsec += (state->default_break_deadline_ms % 1000) * 1000000L;
    if (recall->deadline.tv_nsec >= 1000000000L) {
        recall->deadline.tv_sec++;
        recall->deadline.tv_nsec -= 1000000000L;
    }
    bool overwrite = chimera_smb_disposition_overwrites(request->create.create_disposition);
    if (smb_create_data_open(request) &&
        (recall->phase == 1 || !(command->state->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY))) {
        chimera_vfs_claim_invalidate(state, recall->fh, recall->fh_len, recall->fh_hash,
            recall->phase == 1 ? CHIMERA_TRIGGER_OPEN_H :
                (overwrite ? CHIMERA_TRIGGER_WRITE : CHIMERA_TRIGGER_OPEN_W),
            &actor, overwrite ? 0 :
                (recall->phase == 1 ? CHIMERA_CLAIM_CR : CHIMERA_CLAIM_CR | CHIMERA_CLAIM_H));
    }
    smb_create_recall_poll(thread->evpl, &recall->timer);
}

static void
smb_create_parent_complete(struct chimera_vfs_compound *compound, uint32_t index,
                           enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    struct chimera_smb_request *request = command->request;
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);
    if (command->status != SMB2_STATUS_SUCCESS) { return; }
    if (*status != CHIMERA_VFS_OK) {
        command->status = chimera_smb_create_parent_error_status(*status);
        return;
    }
    if ((op->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) && S_ISLNK(op->attr.va_mode)) {
        uint16_t leaf[SMB_PATH_MAX];
        int len = chimera_smb_utf8_to_utf16le(&request->compound->thread->iconv_ctx,
            request->create.name, request->create.name_len, leaf, sizeof(leaf));
        command->status = SMB2_STATUS_STOPPED_ON_SYMLINK;
        attempt->symlink_unparsed = len > 0 ? len + 2 : 0;
        chimera_vfs_compound_add_open_current(compound,
            CHIMERA_VFS_OPEN_NOFOLLOW | CHIMERA_VFS_OPEN_PATH, 0);
        int link = chimera_vfs_compound_add_readlink(compound);
        chimera_vfs_compound_set_op_callbacks(compound, link, NULL,
                                              smb_create_symlink_complete, command);
        return;
    }
    if (op->out_handle) {
        command->open->parent_fh_len = op->out_handle->fh_len;
        memcpy(command->open->parent_fh, op->out_handle->fh, op->out_handle->fh_len);
    }
}

static void smb_create_reserve_complete(struct chimera_vfs_compound *, uint32_t,
                                         enum chimera_vfs_error *, void *);
static int smb_create_add_admission(struct chimera_vfs_compound *, struct smb_vfs_command *);

static void
smb_create_admission_coordinate(struct chimera_vfs_compound *compound, uint32_t index,
                                 uint64_t token, const uint8_t *fh, uint32_t fh_len,
                                 void *private_data)
{
    struct smb_vfs_command *command = private_data;
    (void) index; (void) fh; (void) fh_len;
    uint32_t status = smb_create_admission_begin(command->request);
    if (status == SMB2_STATUS_PENDING) { chimera_smb_compound_defer(command); }
    else if (status != SMB2_STATUS_SUCCESS) { command->status = status; }
    chimera_vfs_compound_coordinate_done(compound, token,
        status == SMB2_STATUS_SUCCESS ? CHIMERA_VFS_OK :
        status == SMB2_STATUS_PENDING ? CHIMERA_VFS_EINTR : CHIMERA_VFS_ENOSPC);
}

static void
smb_create_pending_begin(struct chimera_vfs_compound *compound, uint32_t index,
                          uint64_t token, const uint8_t *fh, uint32_t fh_len, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    struct chimera_smb_request *request = command->request;
    (void) index; (void) fh; (void) fh_len;
    bool ready = chimera_smb_open_namespace_pending_begin(request->compound->thread,
        command->open, attempt->root_fh_len ? attempt->root_fh : request->tree->fh,
        attempt->root_fh_len ? attempt->root_fh_len : request->tree->fh_len);
    if (!ready) { chimera_smb_compound_defer(command); }
    chimera_vfs_compound_coordinate_done(compound, token,
        ready ? CHIMERA_VFS_OK : CHIMERA_VFS_EINTR);
}

static void
smb_create_pending_bind(struct chimera_vfs_compound *compound, uint32_t index,
                         uint64_t token, const uint8_t *fh, uint32_t fh_len, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    struct chimera_vfs_open_handle *handle =
        chimera_vfs_compound_op(compound, attempt->stream ? attempt->base_op : attempt->open_op)->out_handle;
    (void) index; (void) fh; (void) fh_len;
    bool ready = chimera_smb_open_namespace_pending_bind(command->open, handle);
    /* A fresh CREATE already changed the namespace, so failure is an ordinary
     * accepted-prefix error rather than permission to execute it twice. */
    if (!ready) { command->status = SMB2_STATUS_INTERNAL_ERROR; }
    chimera_vfs_compound_coordinate_done(compound, token,
        ready ? CHIMERA_VFS_OK : CHIMERA_VFS_EINVAL);
}

/* Truncating dispositions must check the old shared metadata before opening
 * a possibly new named fork: OPEN_STREAM creation can itself stamp base attrs. */
static bool
smb_create_overwrite_attrs_allowed(const struct chimera_smb_request *request,
                                   const struct chimera_vfs_attrs *attr)
{
    uint32_t old = (attr->va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) ?
        attr->va_dos_attributes : 0;
    return !(old & SMB2_FILE_ATTRIBUTE_READONLY) &&
        (!(old & SMB2_FILE_ATTRIBUTE_HIDDEN) ||
         (request->create.file_attributes & SMB2_FILE_ATTRIBUTE_HIDDEN)) &&
        (!(old & SMB2_FILE_ATTRIBUTE_SYSTEM) ||
         (request->create.file_attributes & SMB2_FILE_ATTRIBUTE_SYSTEM));
}

static void
smb_create_open_complete(struct chimera_vfs_compound *compound, uint32_t index,
                         enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    struct chimera_smb_open_file *open = command->open;
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);
    struct chimera_claim_owner owner = {
        .proto = CHIMERA_CLAIM_PROTO_SMB2,
        .client_key = command->request->session_handle->session->client_key,
        .owner_lo = open->file_id.pid,
        .owner_hi = open->file_id.vid,
    };
    uint32_t da = open->desired_access, sa = open->share_access;
    uint8_t used = 0, denied = 0;
    struct chimera_smb_request *request = command->request;
    struct chimera_vfs_open_handle *handle =
        chimera_vfs_compound_op(compound, attempt->open_op)->out_handle;
    if (command->status != SMB2_STATUS_SUCCESS) { return; }
    bool reparse_boundary = (request->create.create_options & SMB2_FILE_OPEN_REPARSE_POINT) &&
        request->create.create_disposition != SMB2_FILE_OPEN;
    if (*status == CHIMERA_VFS_ELOOP ||
        (*status == CHIMERA_VFS_OK && (op->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
         S_ISLNK(op->attr.va_mode) &&
         (!(request->create.create_options & SMB2_FILE_OPEN_REPARSE_POINT) || reparse_boundary))) {
        if (reparse_boundary) {
            /* Legacy non-OPEN dispositions have distinct symlink follow and
             * collision rules. STOP_SYMLINK discovers this boundary before
             * the leaf operation can mutate either link or target. */
            chimera_smb_compound_defer(command);
            *status = CHIMERA_VFS_EINTR;
            return;
        }
        struct chimera_vfs_attrs empty = { 0 };
        int link;
        /* Recover only the protocol error payload. The CREATE remains failed
         * in the command journal; no share reservation or FileId is exposed. */
        command->status = SMB2_STATUS_STOPPED_ON_SYMLINK;
        if (*status != CHIMERA_VFS_OK) {
            chimera_vfs_compound_add_puthandle_from(compound, attempt->parent_op,
                                                    CHIMERA_VFS_OPEN_INFERRED);
            chimera_vfs_compound_add_open_at(compound, command->request->create.name,
                command->request->create.name_len, CHIMERA_VFS_OPEN_NOFOLLOW | CHIMERA_VFS_OPEN_PATH,
                &empty, CHIMERA_VFS_ATTR_FH);
        }
        link = chimera_vfs_compound_add_readlink(compound);
        chimera_vfs_compound_set_op_callbacks(compound, link, NULL,
                                              smb_create_symlink_complete, command);
        *status = CHIMERA_VFS_OK;
        return;
    }
    if (smb_create_bound_missing(request, *status)) {
        command->status = SMB2_STATUS_INVALID_PARAMETER;
        *status = CHIMERA_VFS_EINVAL;
    }
    if (*status != CHIMERA_VFS_OK) { return; }
    if (request->create.create_disposition == SMB2_FILE_CREATE &&
        chimera_smb_lease_key_conflict(request, NULL, 0)) {
        command->status = SMB2_STATUS_OBJECT_NAME_COLLISION;
        *status = CHIMERA_VFS_EEXIST;
        return;
    }
    attempt->created |= handle->r_created;
    if (smb_create_lease_request(request) &&
        S_ISDIR(op->attr.va_mode) && !smb_create_directory_lease_supported(request)) {
        /* Revalidate actual type after name lookup. Unsupported directory
         * policy defers before overwrite/EAs or admission can change it. */
        chimera_smb_compound_defer(command);
        *status = CHIMERA_VFS_EINTR;
        return;
    }
    if (request->compound->thread->shared->config.leases &&
        (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) &&
        chimera_smb_lease_key_conflict(request, handle->fh, handle->fh_len)) {
        command->status = SMB2_STATUS_INVALID_PARAMETER;
        *status = CHIMERA_VFS_EINVAL;
        return;
    }
    bool overwrite = chimera_smb_disposition_overwrites(request->create.create_disposition);
    if (overwrite && !attempt->stream && S_ISDIR(op->attr.va_mode)) {
        command->status = SMB2_STATUS_FILE_IS_A_DIRECTORY;
        *status = CHIMERA_VFS_EISDIR;
        return;
    }
    if (overwrite && !attempt->created && !smb_create_overwrite_attrs_allowed(request, &op->attr)) {
        command->status = SMB2_STATUS_ACCESS_DENIED;
        *status = CHIMERA_VFS_EACCES;
        return;
    }
    if ((request->create.create_options & SMB2_FILE_NON_DIRECTORY_FILE) &&
        !attempt->stream && S_ISDIR(op->attr.va_mode)) {
        command->status = SMB2_STATUS_FILE_IS_A_DIRECTORY;
        *status = CHIMERA_VFS_EISDIR;
        return;
    }
    if ((request->create.create_options & SMB2_FILE_DIRECTORY_FILE) && !S_ISDIR(op->attr.va_mode)) {
        command->status = SMB2_STATUS_NOT_A_DIRECTORY;
        *status = CHIMERA_VFS_ENOTDIR;
        return;
    }
    if (request->create.create_disposition != SMB2_FILE_CREATE) {
        unsigned int access_status = chimera_smb_create_check_access(request, &op->attr);
        if (access_status == SMB2_STATUS_ACCESS_DENIED &&
            attempt->access_retries < CHIMERA_SMB_CREATE_ACCESS_RETRIES &&
            chimera_smb_create_access_deny_transient(request, &op->attr)) {
            attempt->access_retries++;
            attempt->access_pending = 1;
            int attrs = chimera_vfs_compound_add_getattr(compound,
                CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_BTIME);
            chimera_vfs_compound_op_set_handle(compound, attrs, handle);
            chimera_vfs_compound_set_op_callbacks(compound, attrs, NULL, smb_create_open_complete, command);
            return;
        }
        if (access_status != SMB2_STATUS_SUCCESS) {
            command->status = access_status;
            *status = CHIMERA_VFS_EACCES;
            return;
        }
        /* The READONLY DOS bit is independent of ACL access. A new object
         * carrying READONLY does not revoke its creating handle's access. */
        if (!attempt->created && !S_ISDIR(op->attr.va_mode) &&
            (op->attr.va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) &&
            (op->attr.va_dos_attributes & SMB2_FILE_ATTRIBUTE_READONLY) &&
            (smb_create_desired_access(request) & (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA))) {
            command->status = SMB2_STATUS_ACCESS_DENIED;
            *status = CHIMERA_VFS_EACCES;
            return;
        }
        struct chimera_vfs_state *vfs_state = request->compound->thread->vfs_thread->vfs->vfs_state;
        struct chimera_vfs_file_state *file = chimera_vfs_state_get(vfs_state,
            handle->fh, handle->fh_len, handle->fh_hash, false);
        bool pending = file && chimera_vfs_state_is_delete_pending(file);
        if (file) { chimera_vfs_state_put(vfs_state, file); }
        if (smb_doc_fh_pending(command, handle->fh, handle->fh_len, pending)) {
            command->status = SMB2_STATUS_DELETE_PENDING;
            *status = CHIMERA_VFS_EACCES;
            return;
        }
    }
    open->flags &= ~CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY;
    command->state->flags &= ~CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY;
    if (!attempt->stream && S_ISDIR(op->attr.va_mode)) {
        open->flags |= CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY;
        command->state->flags |= CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY;
    }
    if (!chimera_smb_open_namespace_bind(open, handle,
        attempt->root_fh_len ? attempt->root_fh : request->tree->fh,
        attempt->root_fh_len ? attempt->root_fh_len : request->tree->fh_len)) {
        command->status = SMB2_STATUS_INTERNAL_ERROR;
        *status = CHIMERA_VFS_EINVAL;
        return;
    }
    /* Reply-only identity snapshot, never an owned VFS reference. QFid must
     * remain available even when a later command closes this private slot. */
    attempt->reply_handle.fh_hash = handle->fh_hash;
    chimera_smb_marshal_open_attrs(&op->attr, attempt->stream, &attempt->attrs);
    if (attempt->created && request->create.alsi_alloc_size > attempt->attrs.smb_alloc_size) {
        attempt->attrs.smb_alloc_size = request->create.alsi_alloc_size;
    }
    open->granted_access = chimera_smb_create_granted_access(command->request, &op->attr);
    open->maximal_access = chimera_vfs_access_check(&op->attr,
        &command->request->session_handle->session->cred, CHIMERA_ACE_MASK_ALL);
    if (da & (SMB2_FILE_READ_DATA | SMB2_FILE_EXECUTE | SMB2_GENERIC_READ |
              SMB2_GENERIC_EXECUTE | SMB2_GENERIC_ALL | SMB2_MAXIMUM_ALLOWED)) { used |= CHIMERA_CLAIM_R; }
    if (da & (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA | SMB2_GENERIC_WRITE |
              SMB2_GENERIC_ALL | SMB2_MAXIMUM_ALLOWED)) { used |= CHIMERA_CLAIM_W; }
    if (da & (SMB2_DELETE | SMB2_GENERIC_ALL | SMB2_MAXIMUM_ALLOWED)) { used |= CHIMERA_CLAIM_D; }
    if (!(sa & SMB2_FILE_SHARE_READ)) { denied |= CHIMERA_CLAIM_R; }
    if (!(sa & SMB2_FILE_SHARE_WRITE)) { denied |= CHIMERA_CLAIM_W; }
    if (!(sa & SMB2_FILE_SHARE_DELETE)) { denied |= CHIMERA_CLAIM_D; }
    attempt->held_used = used;
    attempt->held_denied = denied;
    if (!(da & SMB2_SHAREMODE_ACCESS_MASK)) {
        /* Metadata-only handles retain no SHARE rights. A truncating OPEN
         * must still arbitrate both its transient WRITE and requested deny
         * bits before mutation, then narrow both to zero after OVERWRITE. */
        attempt->held_used = attempt->held_denied = 0;
        if (!overwrite) { used = denied = 0; }
    }
    if (overwrite) { used |= CHIMERA_CLAIM_W; }
    if (request->create.ctx_present_mask & CHIMERA_SMB_CREATE_CTX_RQLS) {
        memcpy(owner.key, request->create.rqls.key, 16);
    }
    chimera_vfs_claim_init_smb_open(&open->share_lease, used, denied, &owner);
    open->share_lease.op_handle = handle;
    open->share_lease.policy_tag = open->file_id.pid;
    /* A provisional claim arbitrates admission, but must not expose an open
     * back-reference to legacy force-close/durable registry scans. */
    open->share_lease.cb_private = NULL;
    if (attempt->access_pending) {
        attempt->access_pending = 0;
        smb_create_add_admission(compound, command);
    }
}

static void
smb_create_reserve_complete(struct chimera_vfs_compound *compound, uint32_t index,
                            enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    (void) compound; (void) index;
    struct smb_create_attempt *attempt = command->private_data;
    if (*status == CHIMERA_VFS_OK && command->status == SMB2_STATUS_SUCCESS && !attempt->access_pending) {
        attempt->reserve_op = index;
        command->state->access_owner = chimera_vfs_compound_op(compound, index)->access_owner;
    } else if (*status == CHIMERA_VFS_EBUSY &&
               chimera_vfs_compound_op(compound, index)->claim_conflict.admission_fenced) {
        command->status = SMB2_STATUS_FILE_NOT_AVAILABLE;
    } else if (*status == CHIMERA_VFS_EACCES && command->status == SMB2_STATUS_SUCCESS) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);
        bool simple_denial = op->claim_result == CHIMERA_CLAIM_DENIED && op->claim_file;
        if (simple_denial) {
            pthread_mutex_lock(&op->claim_file->lock);
            simple_denial = op->claim_file->claims[CHIMERA_CLAIM_CLASS_CACHE] == NULL;
            pthread_mutex_unlock(&op->claim_file->lock);
            if (simple_denial && op->claim_conflict.policy_tag) {
                simple_denial = chimera_smb_durable_conn_disconnecting(
                    command->request->compound->thread->shared, op->claim_conflict.policy_tag) ==
                    CHIMERA_SMB_DURABLE_YIELD_NONE;
            }
        }
        if (smb_create_data_open(command->request) && !attempt->created && !simple_denial) {
            /* Legacy sharing admission also handles disconnect-in-progress
             * durable yield and batch-holder ticket waits. No claim was held. */
            chimera_smb_compound_defer(command);
            *status = CHIMERA_VFS_EINTR;
        } else {
            command->status = SMB2_STATUS_SHARING_VIOLATION;
        }
    }
}

static void
smb_create_ready_complete(struct chimera_vfs_compound *compound, uint32_t index,
                           enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    (void) compound; (void) index;
    if (*status == CHIMERA_VFS_OK && command->status == SMB2_STATUS_SUCCESS &&
        !attempt->access_pending && command->state->access_owner) {
        chimera_smb_compound_open_available(command);
    }
}

static void
smb_create_overwrite_prepare(struct chimera_vfs_compound *compound, uint32_t index,
                              enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    smb_create_reserve_prepare(compound, index, status, command);
    if (command->status != SMB2_STATUS_SUCCESS || attempt->access_pending) { return; }
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(compound, index);
    op->in_handle = chimera_vfs_compound_op(compound, attempt->open_op)->out_handle;
    op->have_io_owner = 1;
    op->io_owner.owner = command->open->share_lease.owner;
    op->io_owner.op_handle = op->in_handle;
    /* An accepted foreign rename may have updated the pending name token
     * while admission waited. Capture the identity used by this mutation,
     * before a later command in this batch can rename it again. */
    smb_doc_command_path(command, &attempt->overwrite_path);
}

static void
smb_create_overwrite_result(struct chimera_vfs_compound *compound, uint32_t index,
                             enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    if (*status != CHIMERA_VFS_OK || attempt->access_pending) { return; }
    attempt->overwritten = 1;
    chimera_smb_marshal_open_attrs(&chimera_vfs_compound_op(compound, index)->attr,
        attempt->stream, &attempt->attrs);
    if (command->request->create.alsi_alloc_size > attempt->attrs.smb_alloc_size) {
        attempt->attrs.smb_alloc_size = command->request->create.alsi_alloc_size;
    }
}

static void
smb_create_narrow_prepare(struct chimera_vfs_compound *compound, uint32_t index,
                         enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    smb_create_reserve_prepare(compound, index, status, command);
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(compound, index);
    op->access_narrow_used = attempt->held_used;
    op->access_narrow_denied = attempt->held_denied;
}

/* Decode immutable input before dispatch, but report each malformed entry at
 * its original position: valid earlier entries remain an accepted prefix. */
static bool
smb_create_ea_plan(struct smb_vfs_command *command)
{
    struct smb_create_attempt *attempt = command->private_data;
    struct chimera_smb_request *request = command->request;
    if (!request->create.ea_buf_len || attempt->ea_steps) { return true; }
    attempt->ea_steps = calloc(request->create.ea_buf_len / 9 + 1, sizeof(*attempt->ea_steps));
    if (!attempt->ea_steps) { return false; }
    uint32_t offset = 0;
    while (offset < request->create.ea_buf_len) {
        struct smb_create_ea_step *step = &attempt->ea_steps[attempt->ea_count++];
        step->command = command;
        if (chimera_smb_ea_full_parse_one(smb_create_ea_data(request),
                request->create.ea_buf_len, &offset, &step->entry)) {
            step->error = SMB2_STATUS_EA_LIST_INCONSISTENT;
        } else if (!step->entry.name_len ||
                   !chimera_smb_ea_name_valid(step->entry.name, step->entry.name_len) ||
                   (step->entry.flags & ~FILE_NEED_EA) ||
                   step->entry.name_len + CHIMERA_VFS_XATTR_USER_PREFIX_LEN > CHIMERA_VFS_COMPOUND_NAME_MAX) {
            step->error = SMB2_STATUS_INVALID_EA_NAME;
        }
        if (step->error) { break; }
    }
    return true;
}

static void
smb_create_ea_list_prepare(struct chimera_vfs_compound *compound, uint32_t index,
                           enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    smb_create_reserve_prepare(compound, index, status, command);
    if (*status != CHIMERA_VFS_OK || command->status != SMB2_STATUS_SUCCESS || attempt->access_pending) { return; }
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(compound, index);
    op->in_handle = chimera_vfs_compound_op(compound,
        attempt->stream ? attempt->base_op : attempt->open_op)->out_handle;
    attempt->ea_list = index;
    attempt->ea_list_valid = false;
    if (!(op->in_handle->vfs_module->capabilities & CHIMERA_VFS_CAP_XATTR)) {
        command->status = SMB2_STATUS_EAS_NOT_SUPPORTED;
        *status = CHIMERA_VFS_ENOTSUP;
    }
}

static void
smb_create_ea_list_complete(struct chimera_vfs_compound *compound, uint32_t index,
                            enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    if (command->status != SMB2_STATUS_SUCCESS || attempt->access_pending ||
        chimera_vfs_compound_op(compound, index)->skipped) { return; }
    attempt->ea_list_valid = *status == CHIMERA_VFS_OK;
    /* Legacy canonicalization is optional on list error (including ERANGE). */
    *status = CHIMERA_VFS_OK;
}

static void
smb_create_ea_prepare(struct chimera_vfs_compound *compound, uint32_t index,
                      enum chimera_vfs_error *status, void *private_data)
{
    struct smb_create_ea_step *step = private_data;
    struct smb_vfs_command *command = step->command;
    struct smb_create_attempt *attempt = command->private_data;
    smb_create_reserve_prepare(compound, index, status, command);
    if (*status != CHIMERA_VFS_OK || command->status != SMB2_STATUS_SUCCESS || attempt->access_pending) { return; }
    if (step->error) { command->status = step->error; *status = CHIMERA_VFS_EINVAL; return; }
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(compound, index);
    int length = chimera_vfs_xattr_build_user(op->name, sizeof(op->name), step->entry.name, step->entry.name_len);
    if (length < 0) { command->status = SMB2_STATUS_INVALID_EA_NAME; *status = CHIMERA_VFS_EINVAL; return; }
    op->name_len = length;
    op->in_handle = chimera_vfs_compound_op(compound,
        attempt->stream ? attempt->base_op : attempt->open_op)->out_handle;
    /* The latest successful operation on this logical EA supersedes the
     * initial LIST snapshot. A delete is a tombstone: a subsequent set chooses
     * its own spelling rather than reviving a removed stored name. */
    for (struct smb_create_ea_step *prior = step; prior != attempt->ea_steps;) {
        prior--;
        if (!prior->applied || !chimera_smb_ea_name_eq(prior->entry.name,
                prior->entry.name_len, step->entry.name, step->entry.name_len)) { continue; }
        if (prior->entry.value_len) {
            memcpy(op->name, prior->canonical, prior->canonical_len + 1);
            op->name_len = prior->canonical_len;
        }
        memcpy(step->canonical, op->name, op->name_len + 1);
        step->canonical_len = op->name_len;
        return;
    }
    const struct chimera_vfs_compound_op *list = chimera_vfs_compound_op(compound, attempt->ea_list);
    uint32_t pos = 0;
    while (attempt->ea_list_valid && list->buffer && pos < list->buffer_len) {
        const char *name = (const char *) list->buffer + pos;
        size_t len = strnlen(name, list->buffer_len - pos);
        if (len == list->buffer_len - pos) { break; }
        if (len <= CHIMERA_VFS_COMPOUND_NAME_MAX && chimera_vfs_xattr_is_user(name, len) &&
            chimera_smb_ea_name_eq(name + CHIMERA_VFS_XATTR_USER_PREFIX_LEN,
                len - CHIMERA_VFS_XATTR_USER_PREFIX_LEN, step->entry.name, step->entry.name_len)) {
            memcpy(op->name, name, len); op->name[len] = 0; op->name_len = len;
            break;
        }
        pos += len + 1;
    }
    memcpy(step->canonical, op->name, op->name_len + 1);
    step->canonical_len = op->name_len;
}

static void
smb_create_ea_complete(struct chimera_vfs_compound *compound, uint32_t index,
                       enum chimera_vfs_error *status, void *private_data)
{
    struct smb_create_ea_step *step = private_data;
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);
    if (op->skipped || step->command->status != SMB2_STATUS_SUCCESS) { return; }
    if (*status == CHIMERA_VFS_ENODATA && op->type == CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR) {
        *status = CHIMERA_VFS_OK;
    }
    if (*status != CHIMERA_VFS_OK) { step->command->status = chimera_smb_ea_status(*status); }
    else { step->applied = true; }
}

static int
smb_create_add_admission(struct chimera_vfs_compound *compound, struct smb_vfs_command *command)
{
    struct smb_create_attempt *attempt = command->private_data;
    bool data = smb_create_data_open(command->request);
    int bound = chimera_vfs_compound_add_coordinate(compound, smb_create_pending_bind, command);
    chimera_vfs_compound_set_op_prepare(compound, bound, smb_create_reserve_prepare, command);
    if (data) {
        int before = chimera_vfs_compound_add_coordinate(compound,
            smb_create_recall_start, &attempt->recall[0]);
        chimera_vfs_compound_set_op_prepare(compound, before,
            smb_create_recall_prepare, &attempt->recall[0]);
    }
    attempt->reserve_op = chimera_vfs_compound_add_reserve_access(compound,
        attempt->open_op, &command->open->share_lease);
    chimera_vfs_compound_set_op_callbacks(compound, attempt->reserve_op,
        smb_create_reserve_prepare, smb_create_reserve_complete, command);
    if (data) {
        int after = chimera_vfs_compound_add_coordinate(compound,
            smb_create_recall_start, &attempt->recall[1]);
        chimera_vfs_compound_set_op_prepare(compound, after,
            smb_create_recall_prepare, &attempt->recall[1]);
    }
    if (chimera_smb_disposition_overwrites(command->request->create.create_disposition)) {
        int overwrite = chimera_vfs_compound_add_overwrite(compound, NULL,
            &attempt->overwrite_attr, CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME, NULL);
        chimera_vfs_compound_set_op_callbacks(compound, overwrite,
            smb_create_overwrite_prepare, smb_create_overwrite_result, command);
        int narrow = chimera_vfs_compound_add_narrow_access(compound, attempt->reserve_op, 0, 0);
        chimera_vfs_compound_set_op_prepare(compound, narrow, smb_create_narrow_prepare, command);
    }
    if (attempt->ea_count) {
        int list = chimera_vfs_compound_add_listxattrs(compound, 0, 4096);
        chimera_vfs_compound_set_op_callbacks(compound, list,
            smb_create_ea_list_prepare, smb_create_ea_list_complete, command);
        for (unsigned int i = 0; i < attempt->ea_count; i++) {
            struct smb_create_ea_step *step = &attempt->ea_steps[i];
            int added = step->error ? chimera_vfs_compound_add_checkpoint(compound) :
                step->entry.value_len ? chimera_vfs_compound_add_setxattr(compound,
                    CHIMERA_VFS_XATTR_EITHER, "user.x", 6, step->entry.value, step->entry.value_len) :
                chimera_vfs_compound_add_removexattr(compound, "user.x", 6);
            chimera_vfs_compound_set_op_callbacks(compound, added,
                smb_create_ea_prepare, smb_create_ea_complete, step);
        }
    }
    int ready = chimera_vfs_compound_add_checkpoint(compound);
    chimera_vfs_compound_set_op_callbacks(compound, ready,
        smb_create_reserve_prepare, smb_create_ready_complete, command);
    /* Failed admission is command-local: later independent wire commands must
     * not see a SHARE reservation from a CREATE which never became ready. */
    if (!chimera_vfs_compound_reserve_access_until(compound, attempt->reserve_op, ready)) {
        return -1;
    }
    if (attempt->stream && !chimera_vfs_compound_reserve_access_until(
            compound, attempt->base_reserve_op, ready)) { return -1; }
    return ready;
}

static void
smb_create_root_complete(struct chimera_vfs_compound *compound, uint32_t index,
                          enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    if (*status != CHIMERA_VFS_OK) {
        command->status = SMB2_STATUS_NETWORK_NAME_DELETED;
        return;
    }
    const struct chimera_vfs_attrs *attr = &chimera_vfs_compound_op(compound, index)->attr;
    attempt->root_fh_len = attr->va_fh_len;
    memcpy(attempt->root_fh, attr->va_fh, attr->va_fh_len);
}

static void
smb_create_directory_prepare(struct chimera_vfs_compound *compound, uint32_t index,
                              enum chimera_vfs_error *status, void *private_data)
{
    smb_create_reserve_prepare(compound, index, status, private_data);
    chimera_vfs_compound_op_args(compound, index)->namespace_flags = CHIMERA_VFS_MKDIR_NO_NOTIFY;
    struct smb_vfs_command *command = private_data;
    if (chimera_smb_lease_key_conflict(command->request, NULL, 0)) {
        chimera_vfs_compound_op_skip(compound, index);
    }
}

static void
smb_create_directory_complete(struct chimera_vfs_compound *compound, uint32_t index,
                               enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    if (chimera_vfs_compound_op(compound, index)->skipped) { return; }
    if (*status == CHIMERA_VFS_OK) { attempt->created = 1; }
    else if (*status == CHIMERA_VFS_EEXIST &&
             command->request->create.create_disposition == SMB2_FILE_OPEN_IF) {
        *status = CHIMERA_VFS_OK;
    }
}

static void
smb_create_existing_directory_prepare(struct chimera_vfs_compound *compound, uint32_t index,
                                       enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    smb_create_reserve_prepare(compound, index, status, command);
    if (attempt->created) { chimera_vfs_compound_op_skip(compound, index); }
}

static void
smb_create_directory_reparse_complete(struct chimera_vfs_compound *compound, uint32_t index,
                                      enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    (void) compound; (void) index;
    if (smb_create_bound_missing(command->request, *status)) {
        command->status = SMB2_STATUS_INVALID_PARAMETER;
        *status = CHIMERA_VFS_EINVAL;
        return;
    }
    if (*status == CHIMERA_VFS_ELOOP &&
        (command->request->create.create_options & SMB2_FILE_OPEN_REPARSE_POINT)) {
        chimera_smb_compound_defer(command);
        *status = CHIMERA_VFS_EINTR;
    }
}

static uint32_t
smb_create_stream_base_status(struct smb_vfs_command *command,
                              const struct chimera_vfs_attrs *attr,
                              struct chimera_vfs_open_handle *handle)
{
    struct chimera_smb_request *request = command->request;
    struct smb_create_attempt *attempt = command->private_data;
    if (!(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_NAMED_STREAMS)) {
        return SMB2_STATUS_OBJECT_NAME_INVALID;
    }
    /* The base may be a directory; the named fork is still an ordinary data
     * stream. OPEN_STREAM retains the base mode for shared ACL/metadata. */
    if (!S_ISREG(attr->va_mode) && !S_ISDIR(attr->va_mode)) {
        return SMB2_STATUS_FILE_IS_A_DIRECTORY;
    }
    if (!attempt->base_created) {
        if (chimera_smb_disposition_overwrites(request->create.create_disposition) &&
            !smb_create_overwrite_attrs_allowed(request, attr)) {
            return SMB2_STATUS_ACCESS_DENIED;
        }
        uint32_t access = chimera_smb_create_check_access(request, attr);
        if (access != SMB2_STATUS_SUCCESS) { return access; }
        if (!S_ISDIR(attr->va_mode) &&
            (attr->va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) &&
            (attr->va_dos_attributes & SMB2_FILE_ATTRIBUTE_READONLY) &&
            (smb_create_desired_access(request) & (SMB2_FILE_WRITE_DATA | SMB2_FILE_APPEND_DATA))) {
            return SMB2_STATUS_ACCESS_DENIED;
        }
    }
    struct chimera_vfs_state *state = request->compound->thread->vfs_thread->vfs->vfs_state;
    struct chimera_vfs_file_state *file = chimera_vfs_state_get(state,
        handle->fh, handle->fh_len, handle->fh_hash, false);
    bool pending = file && chimera_vfs_state_is_delete_pending(file);
    if (file) { chimera_vfs_state_put(state, file); }
    return smb_doc_fh_pending(command, handle->fh, handle->fh_len, pending) ?
        SMB2_STATUS_DELETE_PENDING : SMB2_STATUS_SUCCESS;
}

static void
smb_create_stream_base_complete(struct chimera_vfs_compound *compound, uint32_t index,
                                 enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    struct chimera_smb_open_file *open = command->open;
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);
    if (*status != CHIMERA_VFS_OK) {
        if (*status == CHIMERA_VFS_ELOOP) {
            chimera_smb_compound_defer(command);
            *status = CHIMERA_VFS_EINTR;
        }
        return;
    }
    struct chimera_vfs_open_handle *handle = op->out_handle;
    attempt->base_created = handle->r_created;
    command->status = smb_create_stream_base_status(command, &op->attr, handle);
    if (command->status != SMB2_STATUS_SUCCESS) {
        *status = CHIMERA_VFS_EACCES;
        return;
    }
    open->base_fh_len = handle->fh_len;
    memcpy(open->base_fh, handle->fh, handle->fh_len);
    struct chimera_claim_owner owner = {
        .proto = CHIMERA_CLAIM_PROTO_SMB2,
        .client_key = command->request->session_handle->session->client_key,
        .owner_lo = open->file_id.pid, .owner_hi = open->file_id.vid,
    };
    uint8_t used = open->desired_access & (SMB2_DELETE | SMB2_GENERIC_ALL | SMB2_MAXIMUM_ALLOWED) ?
        CHIMERA_CLAIM_D : 0;
    uint8_t denied = open->share_access & SMB2_FILE_SHARE_DELETE ? 0 : CHIMERA_CLAIM_D;
    chimera_vfs_claim_init_smb_open(&open->base_share_lease, used, denied, &owner);
    open->base_share_lease.op_handle = handle;
    open->base_share_lease.policy_tag = open->file_id.pid;
    open->base_share_lease.cb_private = NULL;
}

static void
smb_create_stream_base_reserved(struct chimera_vfs_compound *compound, uint32_t index,
                                 enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);
    if (*status == CHIMERA_VFS_OK) {
        command->state->base_access_owner = op->access_owner;
    } else if (op->claim_conflict.admission_fenced) {
        if (!attempt->base_created &&
            command->request->create.create_disposition == SMB2_FILE_OPEN) {
            /* A retiring last stream peer fences its base while unlinking.
             * Legacy FILE_OPEN resolves the stream first, so it observes
             * DELETE_PENDING/NOT_FOUND instead of this internal base fence.
             * No base or stream mutation has occurred in this discovery. */
            chimera_smb_compound_defer(command);
            *status = CHIMERA_VFS_EINTR;
        } else {
            command->status = SMB2_STATUS_FILE_NOT_AVAILABLE;
        }
    } else if (*status == CHIMERA_VFS_EACCES && !attempt->base_created) {
        /* Legacy reverse base-DELETE conflicts have distinct policy. Retain
         * that boundary before touching the stream, never after base CREATE. */
        chimera_smb_compound_defer(command);
        *status = CHIMERA_VFS_EINTR;
    } else if (*status == CHIMERA_VFS_EACCES) {
        command->status = SMB2_STATUS_SHARING_VIOLATION;
    }
}

static void
smb_create_stream_open_prepare(struct chimera_vfs_compound *compound, uint32_t index,
                                enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    smb_create_reserve_prepare(compound, index, status, private_data);
    smb_doc_command_path(command, &attempt->overwrite_path);
    chimera_vfs_compound_op_args(compound, index)->in_handle =
        chimera_vfs_compound_op(compound, attempt->base_op)->out_handle;
}

static void
smb_create_lease_name_complete(struct chimera_vfs_compound *compound, uint32_t index,
                               enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct chimera_smb_request *request = command->request;
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);
    if (*status == CHIMERA_VFS_OK &&
        !(request->create.create_options & SMB2_FILE_NON_DIRECTORY_FILE) &&
        (op->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) && S_ISDIR(op->attr.va_mode) &&
        !smb_create_directory_lease_supported(request)) {
        /* Preserve unsupported directory policy before creating OPEN. */
        chimera_smb_compound_defer(command);
        *status = CHIMERA_VFS_EINTR;
        return;
    }
    if (*status == CHIMERA_VFS_ENOENT &&
        request->create.create_disposition != SMB2_FILE_OPEN &&
        request->create.create_disposition != SMB2_FILE_OVERWRITE &&
        request->compound->thread->shared->config.leases &&
        chimera_smb_lease_key_conflict(request, NULL, 0)) {
        command->status = SMB2_STATUS_INVALID_PARAMETER;
        *status = CHIMERA_VFS_EINVAL;
        return;
    }
    if (*status == CHIMERA_VFS_OK && request->create.create_disposition == SMB2_FILE_CREATE &&
        chimera_smb_lease_key_conflict(request, NULL, 0)) {
        if ((op->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) && S_ISLNK(op->attr.va_mode)) {
            chimera_smb_compound_defer(command);
            *status = CHIMERA_VFS_EINTR;
        } else {
            command->status = SMB2_STATUS_OBJECT_NAME_COLLISION;
            *status = CHIMERA_VFS_EEXIST;
        }
        return;
    }
    /* OPEN_AT remains authoritative for other lookup errors and identity.
     * This preflight prevents a wrong-key create or unsupported directory
     * lease from crossing into mutation; OPEN_AT revalidates actual identity. */
    *status = CHIMERA_VFS_OK;
}

static int
smb_create_build_pipeline(struct chimera_vfs_compound *compound,
                           struct smb_vfs_command *command)
{
    struct chimera_smb_request *request = command->request;
    struct smb_create_attempt *attempt = command->private_data;
    if (!smb_create_ea_plan(command)) { return -1; }
    /* COORDINATE requires a selected FH; selecting the synthetic root does
     * not acquire a backend handle. Real share/path resolution follows admission. */
    uint8_t admission_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t admission_fh_len;
    chimera_vfs_get_root_fh(admission_fh, &admission_fh_len);
    chimera_vfs_compound_add_putfh(compound, admission_fh, admission_fh_len);
    int admission = chimera_vfs_compound_add_coordinate(compound, smb_create_admission_coordinate, command);
    chimera_vfs_compound_set_op_prepare(compound, admission, smb_create_reserve_prepare, command);
    unsigned int flags = CHIMERA_VFS_OPEN_NO_NOTIFY;
    flags |= ((request->create.create_options & SMB2_FILE_OPEN_REPARSE_POINT) &&
              request->create.create_disposition == SMB2_FILE_OPEN) ?
             CHIMERA_VFS_OPEN_NOFOLLOW : CHIMERA_VFS_OPEN_STOP_SYMLINK;
    if (attempt->stream) {
        /* The disposition targets the named fork. OVERWRITE requires both
         * identities to exist; the create-capable dispositions may create the
         * base, but must never truncate its unnamed data fork. */
        if (request->create.create_disposition != SMB2_FILE_OPEN &&
            request->create.create_disposition != SMB2_FILE_OVERWRITE) {
            flags |= CHIMERA_VFS_OPEN_CREATE;
        }
    } else if (request->create.create_disposition == SMB2_FILE_CREATE) {
        flags |= CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE;
    } else if (request->create.create_disposition == SMB2_FILE_OPEN_IF ||
               request->create.create_disposition == SMB2_FILE_OVERWRITE_IF ||
               request->create.create_disposition == SMB2_FILE_SUPERSEDE) {
        flags |= CHIMERA_VFS_OPEN_CREATE;
    } else if (!smb_create_data_open(request)) {
        flags |= CHIMERA_VFS_OPEN_PATH;
    }
    if (request->create.create_options & SMB2_FILE_DIRECTORY_FILE) { flags |= CHIMERA_VFS_OPEN_DIRECTORY; }
    if (!(smb_create_desired_access(request) & SMB2_WRITE_MASK) &&
        !chimera_smb_disposition_overwrites(request->create.create_disposition)) {
        flags |= CHIMERA_VFS_OPEN_READ_ONLY;
    }
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    if (chimera_timespec_cmp(&now, &request->tree->fh_expiration) > 0) {
        uint8_t root_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t root_fh_len;
        chimera_vfs_get_root_fh(root_fh, &root_fh_len);
        chimera_vfs_compound_add_putfh(compound, root_fh, root_fh_len);
        int root = chimera_vfs_compound_add_lookup_path(compound,
            request->tree->share->path, strlen(request->tree->share->path),
            CHIMERA_VFS_ATTR_FH, CHIMERA_VFS_LOOKUP_FOLLOW);
        chimera_vfs_compound_set_op_callbacks(compound, root, NULL,
                                              smb_create_root_complete, command);
    } else {
        chimera_vfs_compound_add_putfh(compound, request->tree->fh, request->tree->fh_len);
    }
    if (request->create.parent_path_len) {
        int parent = chimera_vfs_compound_add_lookup_path(compound,
            request->create.parent_path, request->create.parent_path_len,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE, 0);
        chimera_vfs_compound_set_op_callbacks(compound, parent, NULL,
                                              smb_create_parent_complete, command);
    }
    int parent_open = chimera_vfs_compound_add_open_current(compound,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY, 0);
    chimera_vfs_compound_set_op_callbacks(compound, parent_open,
        smb_create_reserve_prepare, smb_create_parent_complete, command);
    /* OPEN_CURRENT owns only the cursor. GETHANDLE creates the owning result
     * needed by parent identity capture and symlink recovery after a leaf error. */
    attempt->parent_op = chimera_vfs_compound_add_gethandle(compound);
    chimera_vfs_compound_set_op_callbacks(compound, attempt->parent_op,
        smb_create_reserve_prepare, smb_create_parent_complete, command);
    if (smb_create_lease_request(request) && request->create.name_len &&
        ((flags & CHIMERA_VFS_OPEN_CREATE) ||
         !(request->create.create_options & SMB2_FILE_NON_DIRECTORY_FILE))) {
        int checked = chimera_vfs_compound_add_lookup(compound, request->create.name,
            request->create.name_len, CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE);
        chimera_vfs_compound_set_op_callbacks(compound, checked, smb_create_reserve_prepare,
            smb_create_lease_name_complete, command);
        chimera_vfs_compound_add_puthandle_from(compound, attempt->parent_op, CHIMERA_VFS_OPEN_INFERRED);
    }
    int pending = chimera_vfs_compound_add_coordinate(compound, smb_create_pending_begin, command);
    chimera_vfs_compound_set_op_prepare(compound, pending, smb_create_reserve_prepare, command);
    if ((request->create.create_disposition == SMB2_FILE_CREATE ||
         request->create.create_disposition == SMB2_FILE_OPEN_IF) &&
        (request->create.create_options & SMB2_FILE_DIRECTORY_FILE)) {
        int mkdir = chimera_vfs_compound_add_create(compound, CHIMERA_VFS_COMPOUND_CREATE_DIR,
            request->create.name, request->create.name_len, NULL, 0, &attempt->set_attr,
            CHIMERA_VFS_ATTR_FH);
        if (mkdir >= 0) {
            chimera_vfs_compound_set_op_callbacks(compound, mkdir, smb_create_directory_prepare,
                                                  smb_create_directory_complete, command);
        }
        /* A successful MKDIR leaves the exact new FH selected. Only its
         * EEXIST branch needs a name open; never re-resolve the created inode. */
        int existing = chimera_vfs_compound_add_open_at(compound, request->create.name,
            request->create.name_len, CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_PATH |
            CHIMERA_VFS_OPEN_STOP_SYMLINK, NULL, 0);
        chimera_vfs_compound_set_op_callbacks(compound, existing,
            smb_create_existing_directory_prepare, smb_create_directory_reparse_complete, command);
        int opened = chimera_vfs_compound_add_open_current(compound,
            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY, 0);
        chimera_vfs_compound_set_op_prepare(compound, opened, smb_create_reserve_prepare, command);
        attempt->open_op = chimera_vfs_compound_add_gethandle(compound);
        chimera_vfs_compound_set_op_prepare(compound, attempt->open_op, smb_create_reserve_prepare, command);
        int attrs = chimera_vfs_compound_add_getattr(compound,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME | CHIMERA_VFS_ATTR_ACL);
        chimera_vfs_compound_set_op_callbacks(compound, attrs, smb_create_reserve_prepare,
                                              smb_create_open_complete, command);
    } else if (attempt->stream) {
        attempt->base_op = chimera_vfs_compound_add_open_at(compound, command->open->name,
            command->open->name_len, flags, &attempt->set_attr,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME | CHIMERA_VFS_ATTR_ACL);
        chimera_vfs_compound_set_op_callbacks(compound, attempt->base_op, smb_create_reserve_prepare,
            smb_create_stream_base_complete, command);
        int bind = chimera_vfs_compound_add_coordinate(compound, smb_create_pending_bind, command);
        chimera_vfs_compound_set_op_prepare(compound, bind, smb_create_reserve_prepare, command);
        attempt->base_reserve_op = chimera_vfs_compound_add_reserve_access(compound,
            attempt->base_op, &command->open->base_share_lease);
        chimera_vfs_compound_set_op_callbacks(compound, attempt->base_reserve_op,
            smb_create_reserve_prepare, smb_create_stream_base_reserved, command);
        unsigned int stream_flags = CHIMERA_VFS_OPEN_NO_NOTIFY;
        if (request->create.create_disposition != SMB2_FILE_OPEN &&
            request->create.create_disposition != SMB2_FILE_OVERWRITE) {
            stream_flags |= CHIMERA_VFS_OPEN_CREATE;
        }
        /* Truncation is a separate typed OVERWRITE after both ACCESS
         * reservations and cache coordination, never an OPEN_STREAM flag. */
        if (request->create.create_disposition == SMB2_FILE_CREATE) { stream_flags |= CHIMERA_VFS_OPEN_EXCLUSIVE; }
        attempt->open_op = chimera_vfs_compound_add_open_stream(compound,
            command->open->stream_name, command->open->stream_name_len, stream_flags, &attempt->set_attr,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME | CHIMERA_VFS_ATTR_ACL);
        chimera_vfs_compound_set_op_callbacks(compound, attempt->open_op, smb_create_stream_open_prepare,
            smb_create_open_complete, command);
    } else if (request->create.name_len) {
        attempt->open_op = chimera_vfs_compound_add_open_at(compound, request->create.name,
            request->create.name_len, flags, &attempt->set_attr,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME | CHIMERA_VFS_ATTR_ACL);
        chimera_vfs_compound_set_op_callbacks(compound, attempt->open_op, smb_create_reserve_prepare,
            smb_create_open_complete, command);
    } else {
        int root_open = chimera_vfs_compound_add_open_current(compound, flags, 0);
        chimera_vfs_compound_set_op_prepare(compound, root_open, smb_create_reserve_prepare, command);
        attempt->open_op = chimera_vfs_compound_add_gethandle(compound);
        chimera_vfs_compound_set_op_prepare(compound, attempt->open_op, smb_create_reserve_prepare, command);
        int attrs = chimera_vfs_compound_add_getattr(compound,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME | CHIMERA_VFS_ATTR_ACL);
        chimera_vfs_compound_set_op_callbacks(compound, attrs, smb_create_reserve_prepare,
                                              smb_create_open_complete, command);
    }
    command->state->handle_from = attempt->open_op;
    return smb_create_add_admission(compound, command);
}

static void
smb_create_compound_complete(struct chimera_vfs_compound *compound, uint32_t index,
                              enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_create_attempt *attempt = command->private_data;
    if (!attempt->ea_count || index != (uint32_t) attempt->ea_budget_op) {
        smb_create_ready_complete(compound, index, status, private_data);
        return;
    }
    if (*status != CHIMERA_VFS_OK || command->status != SMB2_STATUS_SUCCESS) { return; }
    /* Every static wire group is now built. Reserve enough room for this
     * entire pipeline and its worst-case dynamic suffix before OPEN can
     * create anything: <=32 ACL refreshes, one repeated admission/EA tail,
     * plus parent/root/directory/symlink handling. Other commands' dynamic
     * suffixes already appended are included in num_ops. */
    uint64_t needed = 2 * (uint64_t) attempt->ea_count +
        CHIMERA_SMB_CREATE_ACCESS_RETRIES + 64;
    if (needed > CHIMERA_VFS_COMPOUND_MAX_OPS - chimera_vfs_compound_num_ops(compound)) {
        chimera_smb_compound_defer(command);
        *status = CHIMERA_VFS_EINTR;
        return;
    }
    if (smb_create_build_pipeline(compound, command) < 0) {
        *status = CHIMERA_VFS_ENOSPC;
    }
}

static int
smb_create_compound_build(struct chimera_vfs_compound *compound,
                          struct smb_vfs_command *command)
{
    struct smb_create_attempt *attempt = command->private_data;
    if (!smb_create_ea_plan(command)) { return -1; }
    if (attempt->ea_count) {
        /* Defer construction, not execution, so later static wire groups
         * cannot consume capacity promised to this CREATE's EA suffix. */
        attempt->ea_budget_op = chimera_vfs_compound_add_checkpoint(compound);
        return attempt->ea_budget_op;
    }
    return smb_create_build_pipeline(compound, command);
}

/* The caller holds the file lock across hash/member publication. Grant
 * admission only examines current claims and may decline/downgrade caching;
 * it cannot fail CREATE, allocate, recall a peer, or dispatch filesystem work. */
static void
smb_create_publish_legacy_cache(struct smb_vfs_command *command,
                                 struct chimera_vfs_file_state *file)
{
    struct chimera_smb_request *request = command->request;
    struct chimera_smb_open_file *open = command->open;
    struct smb_create_attempt *attempt = command->private_data;
    struct chimera_claim_owner owner = {
        .proto = CHIMERA_CLAIM_PROTO_SMB2,
        .client_key = request->session_handle->session->client_key,
        .owner_lo = open->file_id.pid, .owner_hi = open->file_id.vid,
    };
    uint8_t mode = CHIMERA_CLAIM_CR;
    if (request->create.requested_oplock_level != SMB2_OPLOCK_LEVEL_II) {
        mode |= CHIMERA_CLAIM_CW;
    }
    if (request->create.requested_oplock_level == SMB2_OPLOCK_LEVEL_BATCH) {
        mode |= CHIMERA_CLAIM_H;
    }
    if (request->tree->share->force_level2_oplock) { mode &= CHIMERA_CLAIM_CR; }
    /* Cache callbacks are asynchronous: retain the pre-recall client policy
     * even after a required ACK strips its original H bit. Recheck live
     * holders too, so a newer competing lease can only narrow this decision. */
    mode &= attempt->legacy_cache_cap & smb_create_client_cache_cap(file, owner.client_key);
    if (!mode) { return; }
    struct chimera_vfs_claim tmpl;
    chimera_vfs_claim_init_oplock(&tmpl, mode, &owner);
    tmpl.break_cb = chimera_smb_lease_break_cb;
    tmpl.op_handle = open->handle;
    tmpl.policy_tag = open->file_id.pid;
    open->grant_member_next = NULL;
    if (!chimera_vfs_claim_grant_publish_oplock_locked(file,
            attempt->cache_candidate, &tmpl, open)) { return; }
    open->grant = attempt->cache_candidate;
    attempt->cache_candidate = NULL;
    open->caching_file_state = file;
    open->caching_lease_inserted = true;
    chimera_smb_share_claim(open)->own_cache = open->grant;
    open->lease_state = chimera_smb_vfs_to_lease_bits(open->grant->claim.used);
    open->oplock_level = chimera_smb_vfs_to_oplock_level(open->grant->claim.used);
}

static void
smb_create_publish_lease(struct smb_vfs_command *command,
                          struct chimera_vfs_file_state *file)
{
    struct chimera_smb_request *request = command->request;
    struct chimera_smb_open_file *open = command->open;
    struct smb_create_attempt *attempt = command->private_data;
    struct chimera_claim_owner owner = {
        .proto = CHIMERA_CLAIM_PROTO_SMB2,
        .client_key = request->session_handle->session->client_key,
    };
    memcpy(owner.key, request->create.rqls.key, 16);
    memcpy(&owner.owner_lo, owner.key, 8);
    memcpy(&owner.owner_hi, owner.key + 8, 8);
    uint8_t requested = request->compound->thread->shared->config.leases ?
        (request->create.rqls.state & 0x07) : 0;
    /* Preserve the legacy RqLs policy: W/H without R grants NONE, and a
     * force-level-II share admits at most READ. A same-key join still keeps
     * its existing stronger grant and version, including a NONE request. */
    if (!(requested & SMB2_LEASE_READ_CACHING)) { requested = 0; }
    if (request->tree->share->force_level2_oplock) { requested &= SMB2_LEASE_READ_CACHING; }
    uint8_t want = chimera_smb_lease_bits_to_vfs(requested);
    struct chimera_vfs_claim tmpl;
    if (open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) {
        /* Directory content recall is a distinct claim construct; a regular
         * RqLs claim would not be broken by child creation/removal. */
        chimera_vfs_claim_init_dir_lease(&tmpl, want, &owner);
    } else {
        chimera_vfs_claim_init_rqls(&tmpl, want, &owner);
    }
    tmpl.break_cb = chimera_smb_lease_break_cb;
    tmpl.op_handle = open->handle;
    tmpl.policy_tag = open->file_id.pid;
    struct chimera_vfs_claim_grant *grant = chimera_vfs_claim_grant_publish_rqls_locked(
        file, attempt->cache_candidate, &tmpl, request->create.rqls.is_v2,
        request->create.rqls.is_v2 ? (uint16_t) (request->create.rqls.epoch + 1) : 1,
        open, &open->grant_member_next);
    if (!grant) { return; }
    if (grant == attempt->cache_candidate) { attempt->cache_candidate = NULL; }
    open->grant = grant;
    open->caching_file_state = file;
    open->caching_lease_inserted = true;
    chimera_smb_share_claim(open)->own_cache = grant;
    open->lease_state = chimera_smb_vfs_to_lease_bits(grant->claim.used);
    open->lease_epoch = grant->is_v2 ? grant->epoch : 0;
    attempt->reply_grant.is_v2 = grant->is_v2;
    open->lease_flags = grant->claim.break_state == CHIMERA_CLAIM_BREAK_BREAKING ?
        SMB2_LEASE_FLAG_BREAK_IN_PROGRESS : 0;
}

static void
smb_create_compound_publish(struct chimera_vfs_compound *compound,
                            struct smb_vfs_command *command)
{
    struct chimera_smb_request *request = command->request;
    struct chimera_smb_open_file *open = command->open;
    struct smb_create_attempt *attempt = command->private_data;
    uint64_t skip_lo = 0, skip_hi = 0;
    bool has_skip = chimera_smb_parent_lease_skip(open->parent_lease_key, &skip_lo, &skip_hi);
    if (attempt->root_fh_len && command->publish_live && !request->tree->compound_tearing_down) {
        request->tree->fh_len = attempt->root_fh_len;
        memcpy(request->tree->fh, attempt->root_fh, attempt->root_fh_len);
        clock_gettime(CLOCK_MONOTONIC, &request->tree->fh_expiration);
        request->tree->fh_expiration.tv_sec += 60;
    }
    if (command->status == SMB2_STATUS_STOPPED_ON_SYMLINK && attempt->symlink_length) {
        request->create.r_symlink_relative = attempt->symlink_target[0] != '/';
        request->create.r_symlink_target_len = attempt->symlink_length;
        for (unsigned int i = 0; i < attempt->symlink_length; i++) {
            request->create.r_symlink_target[i] = attempt->symlink_target[i] == '/' ? '\\' : attempt->symlink_target[i];
        }
        request->create.r_symlink_unparsed = attempt->symlink_unparsed;
        request->create.r_symlink_error = 1;
    }
    /* Namespace creation is an accepted prefix even if later OPEN/admission
     * fails. Its notification belongs to the mutation, not reply success. */
    if (attempt->base_created) {
        smb_create_notify_lease(request,
            open->parent_fh, open->parent_fh_len, CHIMERA_VFS_NOTIFY_FILE_ADDED | CHIMERA_VFS_NOTIFY_STREAM_NAME,
            open->name, open->name_len, NULL, 0, skip_lo, skip_hi, has_skip);
    }
    if (attempt->created || attempt->overwritten) {
        uint32_t action = attempt->created ?
            (attempt->stream ? CHIMERA_VFS_NOTIFY_STREAM_NAME :
             (request->create.create_options & SMB2_FILE_DIRECTORY_FILE) ?
             CHIMERA_VFS_NOTIFY_DIR_ADDED :
             CHIMERA_VFS_NOTIFY_FILE_ADDED | CHIMERA_VFS_NOTIFY_STREAM_NAME) :
            CHIMERA_VFS_NOTIFY_FILE_MODIFIED | CHIMERA_VFS_NOTIFY_SIZE_CHANGED |
            CHIMERA_VFS_NOTIFY_ATTRS_CHANGED | CHIMERA_VFS_NOTIFY_STREAM_SIZE;
        smb_create_notify_lease(request,
            attempt->created && !attempt->stream ? open->parent_fh : attempt->overwrite_path.parent_fh,
            attempt->created && !attempt->stream ? open->parent_fh_len : attempt->overwrite_path.parent_fh_len, action,
            attempt->created && !attempt->stream ? open->name : attempt->overwrite_path.name,
            attempt->created && !attempt->stream ? open->name_len : attempt->overwrite_path.name_len, NULL, 0, skip_lo, skip_hi, has_skip);
    }
    if (command->status != SMB2_STATUS_SUCCESS) { return; }
    /* Initialize every accepted overlay field before making the new open
     * findable by another channel. Core repeats these merges for all slots. */
    open->flags = (open->flags & ~command->state->flags_dirty) |
                  (command->state->flags & command->state->flags_dirty);
    if (command->state->position_dirty) { open->position = command->state->position; }
    if (command->state->sequence_dirty) {
        open->channel_sequence = command->state->channel_sequence;
        open->channel_sequence_valid = command->state->channel_sequence_valid;
    }
    if (command->state->integrity_dirty) {
        open->integrity_algo = command->state->integrity_algo;
        open->integrity_flags = command->state->integrity_flags;
    }
    for (unsigned int seq = 0; seq < 64; seq++) {
        if (command->state->lock_seq_dirty & (UINT64_C(1) << seq)) {
            open->lock_seq_valid[seq] = command->state->lock_seq_valid[seq];
            open->lock_seq_index[seq] = command->state->lock_seq_index[seq];
            open->lock_seq_status[seq] = command->state->lock_seq_status[seq];
        }
    }
    open->create_action = attempt->created ? SMB2_CREATE_ACTION_CREATED :
        request->create.create_disposition == SMB2_FILE_SUPERSEDE ? SMB2_CREATE_ACTION_SUPERSEDED :
        attempt->overwritten ? SMB2_CREATE_ACTION_OVERWRITTEN : SMB2_CREATE_ACTION_OPENED;
    /* Snapshot the still-private open before namespace/hash publication makes
     * it mutable by peer CLOSE/rename/break callbacks. Cache fields are filled
     * below under the grant's file lock; the reply never borrows the live grant. */
    attempt->reply_open = *open;
    attempt->reply_open.grant = NULL;
    attempt->reply_open.handle = &attempt->reply_handle;
    struct chimera_smb_tree *tree = request->tree;
    struct chimera_server_smb_shared *shared = request->compound->thread->shared;
    /* Serialize publication with the logical teardown marker. A memory pin
     * alone does not prevent another channel disconnecting this tree. */
    pthread_mutex_lock(&shared->trees_lock);
    if (!command->state->closed && command->publish_live &&
        !tree->compound_tearing_down &&
        !(request->session_handle->session->flags & CHIMERA_SMB_SESSION_DELETED)) {
        unsigned int bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
        open->handle = chimera_vfs_compound_take_handle(compound, attempt->open_op);
        chimera_smb_lease_key_attach(request, open, open->handle);
        if (attempt->stream) {
            open->base_handle = chimera_vfs_compound_take_handle(compound, attempt->base_op);
            open->base_access_owner = chimera_vfs_compound_take_access_owner(compound,
                attempt->base_reserve_op, &open->base_share_file_state);
            open->base_share_lease_inserted = true;
            chimera_vfs_state_stream_holder_inc(open->base_share_file_state);
            pthread_mutex_lock(&open->base_share_file_state->lock);
            chimera_smb_base_share_claim(open)->cb_private = open;
            pthread_mutex_unlock(&open->base_share_file_state->lock);
        }
        /* Registry discovery takes file-state locks; publish membership before
         * taking the bucket or file lock, under the tree teardown fence. */
        chimera_smb_open_namespace_attach(request->compound->thread, open);
        pthread_mutex_lock(&tree->open_files_lock[bucket]);
        open->access_owner = chimera_vfs_compound_take_access_owner(compound, attempt->reserve_op, &open->share_file_state);
        if (command->state->range_owner) {
            open->range_owner = chimera_vfs_compound_take_range_owner(compound,
                command->state->range_owner_from);
        }
        open->share_lease_inserted = true;
        /* ACCESS already anchors this state, so this is a nonallocating pin
         * for a possible accepted cache grant. Retire it below if declined. */
        struct chimera_vfs_file_state *cache_file =
            attempt->cache_candidate &&
            (!(open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) ||
             smb_create_lease_request(request)) ?
            chimera_vfs_state_get(shared->vfs->vfs_state, open->handle->fh,
                open->handle->fh_len, open->handle->fh_hash, false) : NULL;
        pthread_mutex_lock(&open->share_file_state->lock);
        open->refcnt++; /* tree */
        if (!tree->open_files[bucket]) {
            tree->open_files[bucket] = attempt->reserved_hash;
            attempt->reserved_hash = NULL;
        } else {
            unsigned int noexpand = tree->open_files[bucket]->hh.tbl->noexpand;
            HASH_CLEAR(hh, attempt->reserved_hash);
            /* Existing-table insertion allocates only on bucket expansion.
             * Bloom insertion only sets preallocated bits. Restore noexpand
             * immediately so subsequent ordinary insertions retain resizing. */
            tree->open_files[bucket]->hh.tbl->noexpand = 1;
            HASH_ADD(hh, tree->open_files[bucket], file_id, sizeof(open->file_id), open);
            tree->open_files[bucket]->hh.tbl->noexpand = noexpand;
        }
        chimera_smb_share_claim(open)->cb_private = open;
        if (cache_file) {
            if (smb_create_lease_request(request)) { smb_create_publish_lease(command, cache_file); }
            else { smb_create_publish_legacy_cache(command, cache_file); }
        }
        attempt->reply_open.oplock_level = open->oplock_level;
        attempt->reply_open.lease_state = open->lease_state;
        attempt->reply_open.lease_epoch = open->lease_epoch;
        attempt->reply_open.lease_flags = open->lease_flags;
        attempt->reply_open.grant = open->grant ? &attempt->reply_grant : NULL;
        command->state->published = 1;
        pthread_mutex_unlock(&open->share_file_state->lock);
        pthread_mutex_unlock(&tree->open_files_lock[bucket]);
        if (cache_file && !open->grant) { chimera_vfs_state_put(shared->vfs->vfs_state, cache_file); }
    }
    pthread_mutex_unlock(&shared->trees_lock);
    request->create.r_open_file = &attempt->reply_open;
    request->create.r_attrs = attempt->attrs;
    request->create.r_created = attempt->created;
    request->create.r_is_directory = !!(attempt->reply_open.flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY);
    request->create.r_granted_access = attempt->reply_open.granted_access;
    request->create.r_maximal_access = attempt->reply_open.maximal_access;
}

static void
smb_create_compound_reset(struct smb_vfs_command *command)
{
    /* All admission coordinates use coordinate_each_attempt; release before
     * retry so no abandoned reservation holds replacement or same-key work. */
    chimera_smb_lease_key_end(command->request);
    if (command->request->namespace_open_token) {
        chimera_smb_namespace_open_end(command->request->namespace_open_token);
    }
    struct smb_create_attempt *attempt = command->private_data;
    if (attempt) { attempt->created = 0; attempt->overwritten = 0; attempt->base_created = false;
        attempt->held_used = attempt->held_denied = 0;
        attempt->legacy_cache_cap = CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW | CHIMERA_CLAIM_H;
        attempt->ea_list_valid = false;
        for (unsigned int i = 0; i < attempt->ea_count; i++) { attempt->ea_steps[i].applied = false; }
        attempt->symlink_length = 0; attempt->symlink_unparsed = 0;
        attempt->access_pending = 0; attempt->access_retries = 0; attempt->root_fh_len = 0; }
}

static void
smb_create_compound_release(struct smb_vfs_command *command)
{
    chimera_smb_lease_key_end(command->request);
    if (command->request->namespace_open_token) {
        chimera_smb_namespace_open_end(command->request->namespace_open_token);
    }
    struct smb_create_attempt *attempt = command->private_data;
    if (!attempt) { return; }
    if (attempt->reserved_hash) { HASH_CLEAR(hh, attempt->reserved_hash); }
    free(attempt->cache_candidate);
    attempt->cache_candidate = NULL;
    if (command->state->published) {
        chimera_smb_open_file_release(command->request, command->open);
    } else {
        chimera_smb_open_namespace_detach(command->open);
        free(command->open);
    }
    command->open = NULL;
}

static void
smb_create_compound_reply_release(struct smb_vfs_command *command)
{
    struct smb_create_attempt *attempt = command->private_data;
    if (attempt) { free(attempt->ea_steps); }
    free(command->private_data);
    command->private_data = NULL;
}

const struct smb_vfs_command_ops chimera_smb_create_compound_ops = {
    .eligible = smb_create_compound_eligible,
    .private_suffix_eligible = smb_create_private_suffix_eligible,
    .private_actor_owner = smb_create_private_actor_owner,
    .creates_open = 1,
    .initialize = smb_create_compound_initialize,
    .map_error = chimera_smb_create_error_status,
    .build = smb_create_compound_build,
    .prepare = smb_create_compound_prepare,
    .complete = smb_create_compound_complete,
    .publish = smb_create_compound_publish,
    .reset = smb_create_compound_reset,
    .release = smb_create_compound_release,
    .reply_release = smb_create_compound_reply_release,
};
