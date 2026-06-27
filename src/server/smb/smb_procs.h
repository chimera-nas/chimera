// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

void
chimera_smb_complete_request(
    struct chimera_smb_request *request,
    unsigned int                status);

int chimera_smb_parse_negotiate(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_negotiate_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

/* Phase 0 test hooks. Exposed so tests/phase0_contexts_test.c can drive the
 * negotiate-context and CREATE-context dispatch paths directly without
 * standing up a full SMB compound + connection. Not used outside tests. */
int chimera_smb_parse_one_negotiate_context(
    struct chimera_smb_request *request,
    uint16_t                    type,
    const uint8_t              *data,
    uint16_t                    data_len);

int chimera_smb_parse_create_contexts(
    const uint8_t              *buf,
    uint32_t                    buf_len,
    struct chimera_smb_request *request);

uint32_t chimera_smb_build_create_response_contexts(
    struct chimera_smb_request *request,
    uint8_t                    *ctx_buf,
    uint32_t                    ctx_buf_size);

int chimera_smb_parse_session_setup(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_session_setup_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_tree_connect(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_tree_connect_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_create(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_create_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

void chimera_smb_create_emit_symlink_error(
    struct evpl_iovec_cursor   *cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_close(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_close_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_write(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_write_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_read(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_read_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_flush(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_flush_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_ioctl(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_ioctl_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_query_info(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_query_directory(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_query_info_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

void chimera_smb_query_directory_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_set_info(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_set_info_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_tree_disconnect(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_tree_disconnect_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_logoff(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_logoff_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

void chimera_smb_negotiate(
    struct chimera_smb_request *request);

void chimera_smb_session_setup(
    struct chimera_smb_request *request);

void chimera_smb_logoff(
    struct chimera_smb_request *request);

void chimera_smb_tree_connect(
    struct chimera_smb_request *request);

void chimera_smb_tree_disconnect(
    struct chimera_smb_request *request);

void chimera_smb_create(
    struct chimera_smb_request *request);

void chimera_smb_close(
    struct chimera_smb_request *request);

void chimera_smb_write(
    struct chimera_smb_request *request);

void chimera_smb_read(
    struct chimera_smb_request *request);

void chimera_smb_flush(
    struct chimera_smb_request *request);

void chimera_smb_ioctl(
    struct chimera_smb_request *request);

void chimera_smb_query_info(
    struct chimera_smb_request *request);

void chimera_smb_query_directory(
    struct chimera_smb_request *request);

void chimera_smb_set_info(
    struct chimera_smb_request *request);

/* Apply a client FILE_FULL_EA_INFORMATION buffer to an open object's xattrs,
 * one EA at a time (shared by SetInfo and CREATE ExtA).  The caller owns ea_buf
 * for the duration; `done` is invoked with the resulting NTSTATUS. */
void chimera_smb_ea_apply(
    struct chimera_server_smb_thread *thread,
    const struct chimera_vfs_cred *cred,
    struct chimera_vfs_open_handle *handle,
    const uint8_t *ea_buf,
    uint32_t ea_buf_len,
    void ( *done )(uint32_t status, void *arg),
    void *arg);

void chimera_smb_set_security(
    struct chimera_smb_request *request);

void chimera_smb_query_security(
    struct chimera_smb_request *request);

void chimera_smb_query_security_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

void chimera_smb_parse_sd_to_attrs(
    const uint8_t            *sd_buf,
    uint32_t                  sd_len,
    struct chimera_vfs_attrs *attrs);

/*
 * Decode a self-relative security descriptor into owner/group/mode AND a full
 * canonical DACL.  The decoded ACL is written into `acl_buf` (capacity
 * `acl_buf_len` bytes) and, when non-empty, attrs->va_acl is pointed at it with
 * the ATTR_ACL set-mask bit raised.
 */
void chimera_smb_parse_sd_to_acl(
    const uint8_t            *sd_buf,
    uint32_t                  sd_len,
    struct chimera_vfs_attrs *attrs,
    void                     *acl_buf,
    uint32_t                  acl_buf_len,
    int                       canonicalize_inherited);

int chimera_smb_parse_echo(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_echo_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

void chimera_smb_echo(
    struct chimera_smb_request *request);

void chimera_smb_ioctl_set_reparse(
    struct chimera_smb_request *request);

void chimera_smb_ioctl_get_reparse(
    struct chimera_smb_request *request);

void chimera_smb_ioctl_set_sparse(
    struct chimera_smb_request *request);

void chimera_smb_ioctl_set_zero_data(
    struct chimera_smb_request *request);

void chimera_smb_ioctl_query_allocated_ranges(
    struct chimera_smb_request *request);

void chimera_smb_ioctl_request_resume_key(
    struct chimera_smb_request *request);

void chimera_smb_ioctl_copychunk(
    struct chimera_smb_request *request);

void chimera_smb_ioctl_duplicate_extents(
    struct chimera_smb_request *request);

void chimera_smb_ioctl_offload_read(
    struct chimera_smb_request *request);

void chimera_smb_ioctl_offload_write(
    struct chimera_smb_request *request);

int chimera_smb_parse_change_notify(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_change_notify(
    struct chimera_smb_request *request);

void chimera_smb_change_notify_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_cancel(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_cancel(
    struct chimera_smb_request *request);

int chimera_smb_parse_lock(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_lock(
    struct chimera_smb_request *request);

void chimera_smb_lock_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

/* Complete a parked blocking byte-range LOCK on its owning thread with `status`
 * (the stashed grant result, SMB2_STATUS_CANCELLED, or
 * SMB2_STATUS_RANGE_NOT_LOCKED).  Cancels the VFS ticket bookkeeping, installs or
 * tears down the entry, drops the open_file reference the park held, and replies. */
void chimera_smb_lock_park_finish(
    struct chimera_smb_request *request,
    uint32_t                    status);

/* Abort a blocking LOCK parked on `open_file` (handle close, tree disconnect,
 * logoff, or connection teardown): cancel its VFS acquire and complete it with
 * SMB2_STATUS_RANGE_NOT_LOCKED.  No-op when no lock is parked.  Must run on the
 * open's owning thread.  Returns the aborted request (whose open_file reference
 * the caller's completion drops), or NULL. */
struct chimera_smb_request *
chimera_smb_lock_abort_parked(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file);

/* Drain (release + free) every byte-range lock entry held by `open_file`.
 * Called at close before the underlying VFS handle is released. */
void chimera_smb_open_file_drain_locks(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file);

/* break_cb wired onto SMB CACHING leases at CREATE time.  Sends an
 * OPLOCK_BREAK Notification on the conn the open was created on, or
 * forcibly revokes if the conn is gone. */
void chimera_smb_lease_break_cb(
    struct chimera_vfs_lease *lease,
    uint8_t                   needed_mode,
    void                     *private_data);

/* Recall every OTHER holder's HANDLE cache on the file backing `open_file`
 * because of a pending namespace mutation (delete-on-close / unlink / rename):
 * an RqLs lease's RH is broken to R, and the operating client's own lease is
 * spared.  Called from the set-info delete-on-close and the close-time remove
 * paths (smb2.lease.unlink). */
void chimera_smb_break_caching_for_namespace(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file);

void chimera_smb_lease_break_thread_init(
    struct chimera_server_smb_thread *thread);

/* Send all lease-break notifications queued for this thread's connections.
 * Called from the request reply path so op-triggered breaks follow the reply. */
void chimera_smb_lease_break_flush(
    struct chimera_server_smb_thread *thread);

void chimera_smb_lease_break_thread_destroy(
    struct chimera_server_smb_thread *thread);

int chimera_smb_parse_oplock_break(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_oplock_break(
    struct chimera_smb_request *request);

/* Resume CREATEs parked whose triggered lease break has now settled.  Called
 * from the OPLOCK_BREAK ack handler after the lease is acked: sweeps the ack's
 * own connection, then broadcasts a resume doorbell to peer threads (a CREATE
 * that triggered the break can be parked on another connection/thread). */
void chimera_smb_create_resume_parked(
    struct chimera_smb_request *ack_request);

/* Ring every peer SMB thread's resume doorbell so each re-scans its own
 * connections for parked CREATEs the just-settled lease break unblocked. */
void chimera_smb_create_resume_parked_broadcast(
    struct chimera_server_smb_thread *origin);

/* Resume doorbell handler (runs on its owning thread). */
void chimera_smb_create_resume_doorbell_callback(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell);

void chimera_smb_oplock_break_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);
